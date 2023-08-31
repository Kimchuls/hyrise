#include "index_io.hpp"
#include <cstdio>
#include <cstdlib>

#include <sys/stat.h>
#include <sys/types.h>

#include "io.hpp"
#include "io_macros.hpp"
#include "IndexFlat.hpp"
#include "IndexIVF.hpp"
#include "IndexIVFFlat.hpp"
#include "InvertedListsIOHook.hpp"
#include "VIndexAssert.hpp"

namespace vindex
{

  static void write_index_header(const Index *idx, IOWriter *f)
  {
    WRITE1(idx->d);
    WRITE1(idx->ntotal);
    int64_t dummy = 1 << 20;
    WRITE1(dummy);
    WRITE1(dummy);
    WRITE1(idx->is_trained);
    WRITE1(idx->metric_type);
    if (idx->metric_type > 1)
    {
      WRITE1(idx->metric_arg);
    }
  }

  void write_InvertedLists(const InvertedLists *ils, IOWriter *f)
  {
    if (ils == nullptr)
    {
      uint32_t h = fourcc("il00");
      WRITE1(h);
    }
    else if (
        const auto &ails = dynamic_cast<const ArrayInvertedLists *>(ils))
    {
      uint32_t h = fourcc("ilar");
      WRITE1(h);
      WRITE1(ails->nlist);
      WRITE1(ails->code_size);
      // here we store either as a full or a sparse data buffer
      size_t n_non0 = 0;
      for (size_t i = 0; i < ails->nlist; i++)
      {
        if (ails->ids[i].size() > 0)
          n_non0++;
      }
      if (n_non0 > ails->nlist / 2)
      {
        uint32_t list_type = fourcc("full");
        WRITE1(list_type);
        std::vector<size_t> sizes;
        for (size_t i = 0; i < ails->nlist; i++)
        {
          sizes.push_back(ails->ids[i].size());
        }
        WRITEVECTOR(sizes);
      }
      else
      {
        int list_type = fourcc("sprs"); // sparse
        WRITE1(list_type);
        std::vector<size_t> sizes;
        for (size_t i = 0; i < ails->nlist; i++)
        {
          size_t n = ails->ids[i].size();
          if (n > 0)
          {
            sizes.push_back(i);
            sizes.push_back(n);
          }
        }
        WRITEVECTOR(sizes);
      }
      // make a single contiguous data buffer (useful for mmapping)
      for (size_t i = 0; i < ails->nlist; i++)
      {
        size_t n = ails->ids[i].size();
        if (n > 0)
        {
          WRITEANDCHECK(ails->codes[i].data(), n * ails->code_size);
          WRITEANDCHECK(ails->ids[i].data(), n);
        }
      }
    }
    else
    {
      InvertedListsIOHook::lookup_classname(typeid(*ils).name())
          ->write(ils, f);
    }
  }
  
  static void write_direct_map(const DirectMap *dm, IOWriter *f)
  {
    char maintain_direct_map =
        (char)dm->type; // for backwards compatibility with bool
    WRITE1(maintain_direct_map);
    WRITEVECTOR(dm->array);
    if (dm->type == DirectMap::Hashtable)
    {
      std::vector<std::pair<int64_t, int64_t>> v;
      const std::unordered_map<int64_t, int64_t> &map = dm->hashtable;
      v.resize(map.size());
      std::copy(map.begin(), map.end(), v.begin());
      WRITEVECTOR(v);
    }
  }

  static void write_ivf_header(const IndexIVF *ivf, IOWriter *f)
  {
    write_index_header(ivf, f);
    WRITE1(ivf->nlist);
    WRITE1(ivf->nprobe);
    // subclasses write by_residual (some of them support only one setting of
    // by_residual).
    write_index(ivf->quantizer, f);
    write_direct_map(&ivf->direct_map, f);
  }

  void write_index(const Index *idx, IOWriter *f)
  {
    if (const IndexFlat *idxf = dynamic_cast<const IndexFlat *>(idx))
    {
      uint32_t h =
          fourcc(idxf->metric_type == METRIC_INNER_PRODUCT ? "IxFI"
                 : idxf->metric_type == METRIC_L2          ? "IxF2"
                                                           : "IxFl");
      WRITE1(h);
      write_index_header(idx, f);
      WRITEXBVECTOR(idxf->codes);
    }
    else if (const IndexIVFFlat *ivfl = dynamic_cast<const IndexIVFFlat *>(idx))
    {
      uint32_t h = fourcc("IwFl");
      WRITE1(h);
      write_ivf_header(ivfl, f);
      write_InvertedLists(ivfl->invlists, f);
    }
    // else if (const IndexIVFPQ *ivpq = dynamic_cast<const IndexIVFPQ *>(idx))
    // {
    //   const IndexIVFPQR *ivfpqr = dynamic_cast<const IndexIVFPQR *>(idx);

    //   uint32_t h = fourcc(ivfpqr ? "IwQR" : "IwPQ");
    //   WRITE1(h);
    //   write_ivf_header(ivpq, f);
    //   WRITE1(ivpq->by_residual);
    //   WRITE1(ivpq->code_size);
    //   write_ProductQuantizer(&ivpq->pq, f);
    //   write_InvertedLists(ivpq->invlists, f);
    //   if (ivfpqr)
    //   {
    //     write_ProductQuantizer(&ivfpqr->refine_pq, f);
    //     WRITEVECTOR(ivfpqr->refine_codes);
    //     WRITE1(ivfpqr->k_factor);
    //   }
    // }
    // else if (const IndexHNSW *idxhnsw = dynamic_cast<const IndexHNSW *>(idx))
    // {
    //   uint32_t h = dynamic_cast<const IndexHNSWFlat *>(idx)     ? fourcc("IHNf")
    //                : dynamic_cast<const IndexHNSWPQ *>(idx)     ? fourcc("IHNp")
    //                : dynamic_cast<const IndexHNSWSQ *>(idx)     ? fourcc("IHNs")
    //                : dynamic_cast<const IndexHNSW2Level *>(idx) ? fourcc("IHN2")
    //                                                             : 0;
    //   FAISS_THROW_IF_NOT(h != 0);
    //   WRITE1(h);
    //   write_index_header(idxhnsw, f);
    //   write_HNSW(&idxhnsw->hnsw, f);
    //   write_index(idxhnsw->storage, f);
    // }
    else
    {
      VINDEX_THROW_MSG("don't know how to serialize this type of index");
    }
  }
  void write_index(const Index *idx, FILE *f)
  {
    FileIOWriter writer(f);
    write_index(idx, &writer);
  }

  void write_index(const Index *idx, const char *fname)
  {
    FileIOWriter writer(fname);
    write_index(idx, &writer);
  }
} // namespace vindex
