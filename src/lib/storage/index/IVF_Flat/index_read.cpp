/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include "index_io.h"

#include "io_macros.h"

#include <cstdio>
#include <cstdlib>

#include <sys/stat.h>
#include <sys/types.h>

#include "FaissAssert.h"
#include "io.h"
#include "io_macros.h"
#include "hamming.h"

#include "InvertedListsIOHook.h"

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

namespace faiss {

/*************************************************************
 * Read
 **************************************************************/

static void read_index_header(Index* idx, IOReader* f) {
    READ1(idx->d);
    READ1(idx->ntotal);
    Index::idx_t dummy;
    READ1(dummy);
    READ1(dummy);
    READ1(idx->is_trained);
    READ1(idx->metric_type);
    if (idx->metric_type > 1) {
        READ1(idx->metric_arg);
    }
    idx->verbose = false;
}

VectorTransform* read_VectorTransform(IOReader* f) {
    uint32_t h;
    READ1(h);
    VectorTransform* vt = nullptr;

    if (h == fourcc("rrot") || h == fourcc("PCAm") || h == fourcc("LTra") ||
        h == fourcc("PcAm") || h == fourcc("Viqm") || h == fourcc("Pcam")) {
        LinearTransform* lt = nullptr;
        if (h == fourcc("rrot")) {
            lt = new RandomRotationMatrix();
        } else if (
                h == fourcc("PCAm") || h == fourcc("PcAm") ||
                h == fourcc("Pcam")) {
            PCAMatrix* pca = new PCAMatrix();
            READ1(pca->eigen_power);
            if (h == fourcc("Pcam")) {
                READ1(pca->epsilon);
            }
            READ1(pca->random_rotation);
            if (h != fourcc("PCAm")) {
                READ1(pca->balanced_bins);
            }
            READVECTOR(pca->mean);
            READVECTOR(pca->eigenvalues);
            READVECTOR(pca->PCAMat);
            lt = pca;
        } else if (h == fourcc("Viqm")) {
            ITQMatrix* itqm = new ITQMatrix();
            READ1(itqm->max_iter);
            READ1(itqm->seed);
            lt = itqm;
        } else if (h == fourcc("LTra")) {
            lt = new LinearTransform();
        }
        READ1(lt->have_bias);
        READVECTOR(lt->A);
        READVECTOR(lt->b);
        FAISS_THROW_IF_NOT(lt->A.size() >= lt->d_in * lt->d_out);
        FAISS_THROW_IF_NOT(!lt->have_bias || lt->b.size() >= lt->d_out);
        lt->set_is_orthonormal();
        vt = lt;
    } else if (h == fourcc("RmDT")) {
        RemapDimensionsTransform* rdt = new RemapDimensionsTransform();
        READVECTOR(rdt->map);
        vt = rdt;
    } else if (h == fourcc("VNrm")) {
        NormalizationTransform* nt = new NormalizationTransform();
        READ1(nt->norm);
        vt = nt;
    } else if (h == fourcc("VCnt")) {
        CenteringTransform* ct = new CenteringTransform();
        READVECTOR(ct->mean);
        vt = ct;
    } else if (h == fourcc("Viqt")) {
        ITQTransform* itqt = new ITQTransform();

        READVECTOR(itqt->mean);
        READ1(itqt->do_pca);
        {
            ITQMatrix* itqm = dynamic_cast<ITQMatrix*>(read_VectorTransform(f));
            FAISS_THROW_IF_NOT(itqm);
            itqt->itq = *itqm;
            delete itqm;
        }
        {
            LinearTransform* pi =
                    dynamic_cast<LinearTransform*>(read_VectorTransform(f));
            FAISS_THROW_IF_NOT(pi);
            itqt->pca_then_itq = *pi;
            delete pi;
        }
        vt = itqt;
    } else {
        FAISS_THROW_FMT(
                "fourcc %ud (\"%s\") not recognized in %s",
                h,
                fourcc_inv_printable(h).c_str(),
                f->name.c_str());
    }
    READ1(vt->d_in);
    READ1(vt->d_out);
    READ1(vt->is_trained);
    return vt;
}

static void read_ArrayInvertedLists_sizes(
        IOReader* f,
        std::vector<size_t>& sizes) {
    uint32_t list_type;
    READ1(list_type);
    if (list_type == fourcc("full")) {
        size_t os = sizes.size();
        READVECTOR(sizes);
        FAISS_THROW_IF_NOT(os == sizes.size());
    } else if (list_type == fourcc("sprs")) {
        std::vector<size_t> idsizes;
        READVECTOR(idsizes);
        for (size_t j = 0; j < idsizes.size(); j += 2) {
            FAISS_THROW_IF_NOT(idsizes[j] < sizes.size());
            sizes[idsizes[j]] = idsizes[j + 1];
        }
    } else {
        FAISS_THROW_FMT(
                "list_type %ud (\"%s\") not recognized",
                list_type,
                fourcc_inv_printable(list_type).c_str());
    }
}

InvertedLists* read_InvertedLists(IOReader* f, int io_flags) {
    uint32_t h;
    READ1(h);
    if (h == fourcc("il00")) {
        fprintf(stderr,
                "read_InvertedLists:"
                " WARN! inverted lists not stored with IVF object\n");
        return nullptr;
    } else if (h == fourcc("ilar") && !(io_flags & IO_FLAG_SKIP_IVF_DATA)) {
        auto ails = new ArrayInvertedLists(0, 0);
        READ1(ails->nlist);
        READ1(ails->code_size);
        ails->ids.resize(ails->nlist);
        ails->codes.resize(ails->nlist);
        std::vector<size_t> sizes(ails->nlist);
        read_ArrayInvertedLists_sizes(f, sizes);
        for (size_t i = 0; i < ails->nlist; i++) {
            ails->ids[i].resize(sizes[i]);
            ails->codes[i].resize(sizes[i] * ails->code_size);
        }
        for (size_t i = 0; i < ails->nlist; i++) {
            size_t n = ails->ids[i].size();
            if (n > 0) {
                READANDCHECK(ails->codes[i].data(), n * ails->code_size);
                READANDCHECK(ails->ids[i].data(), n);
            }
        }
        return ails;

    } else if (h == fourcc("ilar") && (io_flags & IO_FLAG_SKIP_IVF_DATA)) {
        // code is always ilxx where xx is specific to the type of invlists we
        // want so we get the 16 high bits from the io_flag and the 16 low bits
        // as "il"
        int h2 = (io_flags & 0xffff0000) | (fourcc("il__") & 0x0000ffff);
        size_t nlist, code_size;
        READ1(nlist);
        READ1(code_size);
        std::vector<size_t> sizes(nlist);
        read_ArrayInvertedLists_sizes(f, sizes);
        return InvertedListsIOHook::lookup(h2)->read_ArrayInvertedLists(
                f, io_flags, nlist, code_size, sizes);
    } else {
        return InvertedListsIOHook::lookup(h)->read(f, io_flags);
    }
}

static void read_InvertedLists(IndexIVF* ivf, IOReader* f, int io_flags) {
    InvertedLists* ils = read_InvertedLists(f, io_flags);
    if (ils) {
        FAISS_THROW_IF_NOT(ils->nlist == ivf->nlist);
        FAISS_THROW_IF_NOT(
                ils->code_size == InvertedLists::INVALID_CODE_SIZE ||
                ils->code_size == ivf->code_size);
    }
    ivf->invlists = ils;
    ivf->own_invlists = true;
}

static void read_ProductQuantizer(ProductQuantizer* pq, IOReader* f) {
    READ1(pq->d);
    READ1(pq->M);
    READ1(pq->nbits);
    pq->set_derived_values();
    READVECTOR(pq->centroids);
}

static void read_HNSW(HNSW* hnsw, IOReader* f) {
    READVECTOR(hnsw->assign_probas);
    READVECTOR(hnsw->cum_nneighbor_per_level);
    READVECTOR(hnsw->levels);
    READVECTOR(hnsw->offsets);
    READVECTOR(hnsw->neighbors);
    READ1(hnsw->entry_point);
    READ1(hnsw->max_level);
    READ1(hnsw->efConstruction);
    READ1(hnsw->efSearch);
    READ1(hnsw->upper_beam);
}

ProductQuantizer* read_ProductQuantizer(const char* fname) {
    FileIOReader reader(fname);
    return read_ProductQuantizer(&reader);
}

ProductQuantizer* read_ProductQuantizer(IOReader* reader) {
    ProductQuantizer* pq = new ProductQuantizer();
    ScopeDeleter1<ProductQuantizer> del(pq);

    read_ProductQuantizer(pq, reader);
    del.release();
    return pq;
}

static void read_direct_map(DirectMap* dm, IOReader* f) {
    char maintain_direct_map;
    READ1(maintain_direct_map);
    dm->type = (DirectMap::Type)maintain_direct_map;
    READVECTOR(dm->array);
    if (dm->type == DirectMap::Hashtable) {
        using idx_t = Index::idx_t;
        std::vector<std::pair<idx_t, idx_t>> v;
        READVECTOR(v);
        std::unordered_map<idx_t, idx_t>& map = dm->hashtable;
        map.reserve(v.size());
        for (auto it : v) {
            map[it.first] = it.second;
        }
    }
}

static void read_ivf_header(
        IndexIVF* ivf,
        IOReader* f,
        std::vector<std::vector<Index::idx_t>>* ids = nullptr) {
    read_index_header(ivf, f);
    READ1(ivf->nlist);
    READ1(ivf->nprobe);
    ivf->quantizer = read_index(f);
    ivf->own_fields = true;
    if (ids) { // used in legacy "Iv" formats
        ids->resize(ivf->nlist);
        for (size_t i = 0; i < ivf->nlist; i++)
            READVECTOR((*ids)[i]);
    }
    read_direct_map(&ivf->direct_map, f);
}

// used for legacy formats
static ArrayInvertedLists* set_array_invlist(
        IndexIVF* ivf,
        std::vector<std::vector<Index::idx_t>>& ids) {
    ArrayInvertedLists* ail =
            new ArrayInvertedLists(ivf->nlist, ivf->code_size);
    std::swap(ail->ids, ids);
    ivf->invlists = ail;
    ivf->own_invlists = true;
    return ail;
}

static IndexIVFPQ* read_ivfpq(IOReader* f, uint32_t h, int io_flags) {
    bool legacy = h == fourcc("IvQR") || h == fourcc("IvPQ");

    IndexIVFPQ* ivpq = new IndexIVFPQ();

    std::vector<std::vector<Index::idx_t>> ids;
    read_ivf_header(ivpq, f, legacy ? &ids : nullptr);
    READ1(ivpq->by_residual);
    READ1(ivpq->code_size);
    read_ProductQuantizer(&ivpq->pq, f);

    if (legacy) {
        ArrayInvertedLists* ail = set_array_invlist(ivpq, ids);
        for (size_t i = 0; i < ail->nlist; i++)
            READVECTOR(ail->codes[i]);
    } else {
        read_InvertedLists(ivpq, f, io_flags);
    }

    if (ivpq->is_trained) {
        // precomputed table not stored. It is cheaper to recompute it.
        // precompute_table() may be disabled with a flag.
        ivpq->use_precomputed_table = 0;
        if (ivpq->by_residual) {
            if ((io_flags & IO_FLAG_SKIP_PRECOMPUTE_TABLE) == 0) {
                ivpq->precompute_table();
            }
        }
    }
    return ivpq;
}

int read_old_fmt_hack = 0;

Index* read_index(IOReader* f, int io_flags) {
    Index* idx = nullptr;
    uint32_t h;
    READ1(h);
    if (h == fourcc("IxFI") || h == fourcc("IxF2") || h == fourcc("IxFl")) {
        IndexFlat* idxf;
        if (h == fourcc("IxFI")) {
            idxf = new IndexFlatIP();
        } else if (h == fourcc("IxF2")) {
            idxf = new IndexFlatL2();
        } else {
            idxf = new IndexFlat();
        }
        read_index_header(idxf, f);
        idxf->code_size = idxf->d * sizeof(float);
        READXBVECTOR(idxf->codes);
        FAISS_THROW_IF_NOT(
                idxf->codes.size() == idxf->ntotal * idxf->code_size);
        // leak!
        idx = idxf;
    } else if (h == fourcc("IwFl")) {
        IndexIVFFlat* ivfl = new IndexIVFFlat();
        read_ivf_header(ivfl, f);
        ivfl->code_size = ivfl->d * sizeof(float);
        read_InvertedLists(ivfl, f, io_flags);
        idx = ivfl;
    } else if (
            h == fourcc("IvPQ") || h == fourcc("IvQR") || h == fourcc("IwPQ") ||
            h == fourcc("IwQR")) {
        idx = read_ivfpq(f, h, io_flags);
    } else if (
            h == fourcc("IHNf") || h == fourcc("IHNp") || h == fourcc("IHNs") ||
            h == fourcc("IHN2")) {
        IndexHNSW* idxhnsw = nullptr;
        if (h == fourcc("IHNf"))
            idxhnsw = new IndexHNSWFlat();
        // if (h == fourcc("IHNp"))
        //     idxhnsw = new IndexHNSWPQ();
        // if (h == fourcc("IHNs"))
        //     idxhnsw = new IndexHNSWSQ();
        // if (h == fourcc("IHN2"))
        //     idxhnsw = new IndexHNSW2Level();
        read_index_header(idxhnsw, f);
        read_HNSW(&idxhnsw->hnsw, f);
        idxhnsw->storage = read_index(f, io_flags);
        idxhnsw->own_fields = true;
        if (h == fourcc("IHNp")) {
            dynamic_cast<IndexPQ*>(idxhnsw->storage)->pq.compute_sdc_table();
        }
        idx = idxhnsw;
    } else {
        FAISS_THROW_FMT(
                "Index type 0x%08x (\"%s\") not recognized",
                h,
                fourcc_inv_printable(h).c_str());
        idx = nullptr;
    }
    return idx;
}

Index* read_index(FILE* f, int io_flags) {
    FileIOReader reader(f);
    return read_index(&reader, io_flags);
}

Index* read_index(const char* fname, int io_flags) {
    FileIOReader reader(fname);
    Index* idx = read_index(&reader, io_flags);
    return idx;
}

VectorTransform* read_VectorTransform(const char* fname) {
    FileIOReader reader(fname);
    VectorTransform* vt = read_VectorTransform(&reader);
    return vt;
}

/*************************************************************
 * Read binary indexes
 **************************************************************/

} // namespace faiss
