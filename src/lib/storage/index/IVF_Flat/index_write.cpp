/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include "index_io.h"

#include "io.h"
#include "io_macros.h"

#include <cstdio>
#include <cstdlib>

#include <sys/stat.h>
#include <sys/types.h>

#include "InvertedListsIOHook.h"

#include "FaissAssert.h"
#include "hamming.h"
#include "io.h"
#include "io_macros.h"

// #include <faiss/Index2Layer.h>
// #include <faiss/IndexAdditiveQuantizer.h>
// #include <faiss/IndexAdditiveQuantizerFastScan.h>
#include "IndexFlat.h"
#include "IndexHNSW.h"
#include "IndexIVF.h"
// #include <faiss/IndexIVFAdditiveQuantizer.h>
// #include <faiss/IndexIVFAdditiveQuantizerFastScan.h>
#include "IndexIVFFlat.h"
#include "IndexIVFPQ.h"
// #include <faiss/IndexIVFPQFastScan.h>
// #include <faiss/IndexIVFPQR.h>
// #include <faiss/IndexIVFSpectralHash.h>
// #include <faiss/IndexLSH.h>
// #include <faiss/IndexLattice.h>
// #include <faiss/IndexNNDescent.h>
// #include <faiss/IndexNSG.h>
#include "IndexPQ.h"
// #include <faiss/IndexPQFastScan.h>
// #include <faiss/IndexPreTransform.h>
// #include <faiss/IndexRefine.h>
// #include <faiss/IndexRowwiseMinMax.h>
// #include <faiss/IndexScalarQuantizer.h>
// #include <faiss/MetaIndexes.h>
#include "VectorTransform.h"

// #include <faiss/IndexBinaryFlat.h>
// #include <faiss/IndexBinaryFromFloat.h>
// #include <faiss/IndexBinaryHNSW.h>
// #include <faiss/IndexBinaryHash.h>
// #include <faiss/IndexBinaryIVF.h>

/*************************************************************
 * The I/O format is the content of the class. For objects that are
 * inherited, like Index, a 4-character-code (fourcc) indicates which
 * child class this is an instance of.
 *
 * In this case, the fields of the parent class are written first,
 * then the ones for the child classes. Note that this requires
 * classes to be serialized to have a constructor without parameters,
 * so that the fields can be filled in later. The default constructor
 * should set reasonable defaults for all fields.
 *
 * The fourccs are assigned arbitrarily. When the class changed (added
 * or deprecated fields), the fourcc can be replaced. New code should
 * be able to read the old fourcc and fill in new classes.
 *
 * TODO: in this file, the read functions that encouter errors may
 * leak memory.
 **************************************************************/

namespace faiss {

/*************************************************************
 * Write
 **************************************************************/
static void write_index_header(const Index* idx, IOWriter* f) {
  WRITE1(idx->d);
  WRITE1(idx->ntotal);
  Index::idx_t dummy = 1 << 20;
  WRITE1(dummy);
  WRITE1(dummy);
  WRITE1(idx->is_trained);
  WRITE1(idx->metric_type);
  if (idx->metric_type > 1) {
    WRITE1(idx->metric_arg);
  }
}

void write_VectorTransform(const VectorTransform* vt, IOWriter* f) {
  if (const LinearTransform* lt = dynamic_cast<const LinearTransform*>(vt)) {
    if (dynamic_cast<const RandomRotationMatrix*>(lt)) {
      uint32_t h = fourcc("rrot");
      WRITE1(h);
    } else if (const PCAMatrix* pca = dynamic_cast<const PCAMatrix*>(lt)) {
      uint32_t h = fourcc("Pcam");
      WRITE1(h);
      WRITE1(pca->eigen_power);
      WRITE1(pca->epsilon);
      WRITE1(pca->random_rotation);
      WRITE1(pca->balanced_bins);
      WRITEVECTOR(pca->mean);
      WRITEVECTOR(pca->eigenvalues);
      WRITEVECTOR(pca->PCAMat);
    } else if (const ITQMatrix* itqm = dynamic_cast<const ITQMatrix*>(lt)) {
      uint32_t h = fourcc("Viqm");
      WRITE1(h);
      WRITE1(itqm->max_iter);
      WRITE1(itqm->seed);
    } else {
      // generic LinearTransform (includes OPQ)
      uint32_t h = fourcc("LTra");
      WRITE1(h);
    }
    WRITE1(lt->have_bias);
    WRITEVECTOR(lt->A);
    WRITEVECTOR(lt->b);
  } else if (const RemapDimensionsTransform* rdt = dynamic_cast<const RemapDimensionsTransform*>(vt)) {
    uint32_t h = fourcc("RmDT");
    WRITE1(h);
    WRITEVECTOR(rdt->map);
  } else if (const NormalizationTransform* nt = dynamic_cast<const NormalizationTransform*>(vt)) {
    uint32_t h = fourcc("VNrm");
    WRITE1(h);
    WRITE1(nt->norm);
  } else if (const CenteringTransform* ct = dynamic_cast<const CenteringTransform*>(vt)) {
    uint32_t h = fourcc("VCnt");
    WRITE1(h);
    WRITEVECTOR(ct->mean);
  } else if (const ITQTransform* itqt = dynamic_cast<const ITQTransform*>(vt)) {
    uint32_t h = fourcc("Viqt");
    WRITE1(h);
    WRITEVECTOR(itqt->mean);
    WRITE1(itqt->do_pca);
    write_VectorTransform(&itqt->itq, f);
    write_VectorTransform(&itqt->pca_then_itq, f);
  } else {
    FAISS_THROW_MSG("cannot serialize this");
  }
  // common fields
  WRITE1(vt->d_in);
  WRITE1(vt->d_out);
  WRITE1(vt->is_trained);
}

void write_ProductQuantizer(const ProductQuantizer* pq, IOWriter* f) {
  WRITE1(pq->d);
  WRITE1(pq->M);
  WRITE1(pq->nbits);
  WRITEVECTOR(pq->centroids);
}

void write_InvertedLists(const InvertedLists* ils, IOWriter* f) {
  if (ils == nullptr) {
    uint32_t h = fourcc("il00");
    WRITE1(h);
  } else if (const auto& ails = dynamic_cast<const ArrayInvertedLists*>(ils)) {
    uint32_t h = fourcc("ilar");
    WRITE1(h);
    WRITE1(ails->nlist);
    WRITE1(ails->code_size);
    // here we store either as a full or a sparse data buffer
    size_t n_non0 = 0;
    for (size_t i = 0; i < ails->nlist; i++) {
      if (ails->ids[i].size() > 0)
        n_non0++;
    }
    if (n_non0 > ails->nlist / 2) {
      uint32_t list_type = fourcc("full");
      WRITE1(list_type);
      std::vector<size_t> sizes;
      for (size_t i = 0; i < ails->nlist; i++) {
        sizes.push_back(ails->ids[i].size());
      }
      WRITEVECTOR(sizes);
    } else {
      int list_type = fourcc("sprs");  // sparse
      WRITE1(list_type);
      std::vector<size_t> sizes;
      for (size_t i = 0; i < ails->nlist; i++) {
        size_t n = ails->ids[i].size();
        if (n > 0) {
          sizes.push_back(i);
          sizes.push_back(n);
        }
      }
      WRITEVECTOR(sizes);
    }
    // make a single contiguous data buffer (useful for mmapping)
    for (size_t i = 0; i < ails->nlist; i++) {
      size_t n = ails->ids[i].size();
      if (n > 0) {
        WRITEANDCHECK(ails->codes[i].data(), n * ails->code_size);
        WRITEANDCHECK(ails->ids[i].data(), n);
      }
    }

  } else {
    InvertedListsIOHook::lookup_classname(typeid(*ils).name())->write(ils, f);
  }
}

void write_ProductQuantizer(const ProductQuantizer* pq, const char* fname) {
  FileIOWriter writer(fname);
  write_ProductQuantizer(pq, &writer);
}

static void write_HNSW(const HNSW* hnsw, IOWriter* f) {
    WRITEVECTOR(hnsw->assign_probas);
    WRITEVECTOR(hnsw->cum_nneighbor_per_level);
    WRITEVECTOR(hnsw->levels);
    WRITEVECTOR(hnsw->offsets);
    WRITEVECTOR(hnsw->neighbors);
    WRITE1(hnsw->entry_point);
    WRITE1(hnsw->max_level);
    WRITE1(hnsw->efConstruction);
    WRITE1(hnsw->efSearch);
    WRITE1(hnsw->upper_beam);
}

static void write_direct_map(const DirectMap* dm, IOWriter* f) {
  char maintain_direct_map = (char)dm->type;  // for backwards compatibility with bool
  WRITE1(maintain_direct_map);
  WRITEVECTOR(dm->array);
  if (dm->type == DirectMap::Hashtable) {
    using idx_t = Index::idx_t;
    std::vector<std::pair<idx_t, idx_t>> v;
    const std::unordered_map<idx_t, idx_t>& map = dm->hashtable;
    v.resize(map.size());
    std::copy(map.begin(), map.end(), v.begin());
    WRITEVECTOR(v);
  }
}

static void write_ivf_header(const IndexIVF* ivf, IOWriter* f) {
  write_index_header(ivf, f);
  WRITE1(ivf->nlist);
  WRITE1(ivf->nprobe);
  write_index(ivf->quantizer, f);
  write_direct_map(&ivf->direct_map, f);
}

void write_index(const Index* idx, IOWriter* f) {
  if (const IndexFlat* idxf = dynamic_cast<const IndexFlat*>(idx)) {
    uint32_t h = fourcc(idxf->metric_type == METRIC_INNER_PRODUCT ? "IxFI"
                        : idxf->metric_type == METRIC_L2          ? "IxF2"
                                                                  : "IxFl");
    WRITE1(h);
    write_index_header(idx, f);
    WRITEXBVECTOR(idxf->codes);
  } else if (const IndexIVFFlat* ivfl = dynamic_cast<const IndexIVFFlat*>(idx)) {
    uint32_t h = fourcc("IwFl");
    WRITE1(h);
    write_ivf_header(ivfl, f);
    write_InvertedLists(ivfl->invlists, f);
  } else if (const IndexIVFPQ* ivpq = dynamic_cast<const IndexIVFPQ*>(idx)) {
    uint32_t h = fourcc("IwPQ");
    WRITE1(h);
    write_ivf_header(ivpq, f);
    WRITE1(ivpq->by_residual);
    WRITE1(ivpq->code_size);
    write_ProductQuantizer(&ivpq->pq, f);
    write_InvertedLists(ivpq->invlists, f);
  }
  else if (const IndexHNSW* idxhnsw = dynamic_cast<const IndexHNSW*>(idx)) {
      uint32_t h = dynamic_cast<const IndexHNSWFlat*>(idx) ? fourcc("IHNf")
      //         : dynamic_cast<const IndexHNSWPQ*>(idx)      ? fourcc("IHNp")
      //         : dynamic_cast<const IndexHNSWSQ*>(idx)      ? fourcc("IHNs")
      //         : dynamic_cast<const IndexHNSW2Level*>(idx)  ? fourcc("IHN2")
                                                           : 0;
      FAISS_THROW_IF_NOT(h != 0);
      WRITE1(h);
      write_index_header(idxhnsw, f);
      write_HNSW(&idxhnsw->hnsw, f);
      write_index(idxhnsw->storage, f);
  }
  else {
    FAISS_THROW_MSG("don't know how to serialize this type of index");
  }
}

void write_index(const Index* idx, FILE* f) {
  FileIOWriter writer(f);
  write_index(idx, &writer);
}

void write_index(const Index* idx, const char* fname) {
  FileIOWriter writer(fname);
  write_index(idx, &writer);
}

void write_VectorTransform(const VectorTransform* vt, const char* fname) {
  FileIOWriter writer(fname);
  write_VectorTransform(vt, &writer);
}

/*************************************************************
 * Write binary indexes
 **************************************************************/
}  // namespace faiss

// #include <cstdio>
// #include <cstdlib>
// #include "index_io.h"

// #include <sys/stat.h>
// #include <sys/types.h>

// #include "IndexFlat.h"
// #include "IndexIVF.h"
// #include "IndexIVFFlat.h"
// #include "InvertedListsIOHook.h"
// #include "FaissAssert.h"
// #include "io.h"
// #include "io_macros.h"

// #include "IndexIVFPQ.h"
// #include "IndexPQ.h"

// namespace faiss {

// static void write_index_header(const Index* idx, IOWriter* f) {
//   WRITE1(idx->d);
//   WRITE1(idx->ntotal);
//   int64_t dummy = 1 << 20;
//   WRITE1(dummy);
//   WRITE1(dummy);
//   WRITE1(idx->is_trained);
//   WRITE1(idx->metric_type);
//   if (idx->metric_type > 1) {
//     WRITE1(idx->metric_arg);
//   }
// }

// void write_InvertedLists(const InvertedLists* ils, IOWriter* f) {
//   if (ils == nullptr) {
//     uint32_t h = fourcc("il00");
//     WRITE1(h);
//   } else if (const auto& ails = dynamic_cast<const ArrayInvertedLists*>(ils)) {
//     uint32_t h = fourcc("ilar");
//     WRITE1(h);
//     WRITE1(ails->nlist);
//     WRITE1(ails->code_size);
//     // here we store either as a full or a sparse data buffer
//     size_t n_non0 = 0;
//     for (size_t i = 0; i < ails->nlist; i++) {
//       if (ails->ids[i].size() > 0)
//         n_non0++;
//     }
//     if (n_non0 > ails->nlist / 2) {
//       uint32_t list_type = fourcc("full");
//       WRITE1(list_type);
//       std::vector<size_t> sizes;
//       for (size_t i = 0; i < ails->nlist; i++) {
//         sizes.push_back(ails->ids[i].size());
//       }
//       WRITEVECTOR(sizes);
//     } else {
//       int list_type = fourcc("sprs");  // sparse
//       WRITE1(list_type);
//       std::vector<size_t> sizes;
//       for (size_t i = 0; i < ails->nlist; i++) {
//         size_t n = ails->ids[i].size();
//         if (n > 0) {
//           sizes.push_back(i);
//           sizes.push_back(n);
//         }
//       }
//       WRITEVECTOR(sizes);
//     }
//     // make a single contiguous data buffer (useful for mmapping)
//     for (size_t i = 0; i < ails->nlist; i++) {
//       size_t n = ails->ids[i].size();
//       if (n > 0) {
//         WRITEANDCHECK(ails->codes[i].data(), n * ails->code_size);
//         WRITEANDCHECK(ails->ids[i].data(), n);
//       }
//     }
//   } else {
//     InvertedListsIOHook::lookup_classname(typeid(*ils).name())->write(ils, f);
//   }
// }

// static void write_direct_map(const DirectMap* dm, IOWriter* f) {
//   char maintain_direct_map = (char)dm->type;  // for backwards compatibility with bool
//   WRITE1(maintain_direct_map);
//   WRITEVECTOR(dm->array);
//   if (dm->type == DirectMap::Hashtable) {
//     std::vector<std::pair<int64_t, int64_t>> v;
//     const std::unordered_map<int64_t, int64_t>& map = dm->hashtable;
//     v.resize(map.size());
//     std::copy(map.begin(), map.end(), v.begin());
//     WRITEVECTOR(v);
//   }
// }

// static void write_ivf_header(const IndexIVF* ivf, IOWriter* f) {
//   write_index_header(ivf, f);
//   WRITE1(ivf->nlist);
//   WRITE1(ivf->nprobe);
//   // subclasses write by_residual (some of them support only one setting of
//   // by_residual).
//   write_index(ivf->quantizer, f);
//   write_direct_map(&ivf->direct_map, f);
// }

// void write_ProductQuantizer(const ProductQuantizer* pq, IOWriter* f) {
//   WRITE1(pq->d);
//   WRITE1(pq->M);
//   WRITE1(pq->nbits);
//   WRITEVECTOR(pq->centroids);
// }

// void write_ProductQuantizer(const ProductQuantizer* pq, const char* fname) {
//   FileIOWriter writer(fname);
//   write_ProductQuantizer(pq, &writer);
// }

// void write_index(const Index* idx, IOWriter* f) {
//   if (const IndexFlat* idxf = dynamic_cast<const IndexFlat*>(idx)) {
//     uint32_t h = fourcc(idxf->metric_type == METRIC_INNER_PRODUCT ? "IxFI"
//                         : idxf->metric_type == METRIC_L2          ? "IxF2"
//                                                                   : "IxFl");
//     WRITE1(h);
//     write_index_header(idx, f);
//     WRITEXBVECTOR(idxf->codes);
//   } else if (const IndexIVFFlat* ivfl = dynamic_cast<const IndexIVFFlat*>(idx)) {
//     uint32_t h = fourcc("IwFl");
//     WRITE1(h);
//     write_ivf_header(ivfl, f);
//     write_InvertedLists(ivfl->invlists, f);
//   } else if (const IndexIVFPQ* ivpq = dynamic_cast<const IndexIVFPQ*>(idx)) {
//     uint32_t h = fourcc("IwPQ");
//     WRITE1(h);
//     write_ivf_header(ivpq, f);
//     WRITE1(ivpq->by_residual);
//     WRITE1(ivpq->code_size);
//     write_ProductQuantizer(&ivpq->pq, f);
//     write_InvertedLists(ivpq->invlists, f);
//   }
//   else if (const IndexHNSW *idxhnsw = dynamic_cast<const IndexHNSW *>(idx))
//   {
//     uint32_t h = dynamic_cast<const IndexHNSWFlat *>(idx)     ? fourcc("IHNf")
//                 //  : dynamic_cast<const IndexHNSWPQ *>(idx)     ? fourcc("IHNp")
//                 //  : dynamic_cast<const IndexHNSWSQ *>(idx)     ? fourcc("IHNs")
//                 //  : dynamic_cast<const IndexHNSW2Level *>(idx) ? fourcc("IHN2")
//                                                               : 0;
//     FAISS_THROW_IF_NOT(h != 0);
//     WRITE1(h);
//     write_index_header(idxhnsw, f);
//     write_HNSW(&idxhnsw->hnsw, f);
//     write_index(idxhnsw->storage, f);
//   }
//   else {
//     FAISS_THROW_MSG("don't know how to serialize this type of index");
//   }
// }

// void write_index(const Index* idx, FILE* f) {
//   FileIOWriter writer(f);
//   write_index(idx, &writer);
// }

// void write_index(const Index* idx, const char* fname) {
//   FileIOWriter writer(fname);
//   write_index(idx, &writer);
// }
// }  // namespace faiss