
#include "index_io.hpp"

#include <cstdio>
#include <cstdlib>

#include <sys/stat.h>
#include <sys/types.h>

#include "VIndexAssert.hpp"
#include "io.hpp"
#include "io_macros.hpp"
#include "hamming.hpp"

#include "InvertedListsIOHook.hpp"

// #include "Index.hpp"
#include "IndexFlat.hpp"
#include "IndexIVF.hpp"
#include "IndexIVFFlat.hpp"
#include "IndexIVFPQ.hpp"
#include "IndexPQ.hpp"


namespace vindex {

/*************************************************************
 * Read
 **************************************************************/

static void read_index_header(Index* idx, IOReader* f) {
    READ1(idx->d);
    READ1(idx->ntotal);
    int64_t dummy;
    READ1(dummy);
    READ1(dummy);
    READ1(idx->is_trained);
    READ1(idx->metric_type);
    if (idx->metric_type > 1) {
        READ1(idx->metric_arg);
    }
    idx->verbose = false;
}

static void read_ArrayInvertedLists_sizes(
        IOReader* f,
        std::vector<size_t>& sizes) {
    uint32_t list_type;
    READ1(list_type);
    if (list_type == fourcc("full")) {//qwq
        size_t os = sizes.size();
        READVECTOR(sizes);
        VINDEX_THROW_IF_NOT(os == sizes.size());
    } else if (list_type == fourcc("sprs")) {//qwq
        std::vector<size_t> idsizes;
        READVECTOR(idsizes);
        for (size_t j = 0; j < idsizes.size(); j += 2) {
            VINDEX_THROW_IF_NOT(idsizes[j] < sizes.size());
            sizes[idsizes[j]] = idsizes[j + 1];
        }
    } else {
        VINDEX_THROW_FMT(
                "list_type %ud (\"%s\") not recognized",
                list_type,
                fourcc_inv_printable(list_type).c_str());
    }
}

InvertedLists* read_InvertedLists(IOReader* f, int io_flags) {
    uint32_t h;
    READ1(h);
    if (h == fourcc("il00")) {//qwq
        fprintf(stderr,
                "read_InvertedLists:"
                " WARN! inverted lists not stored with IVF object\n");
        return nullptr;
    } else if (h == fourcc("ilar") && !(io_flags & IO_FLAG_SKIP_IVF_DATA)) {//qwq
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
        VINDEX_THROW_IF_NOT(ils->nlist == ivf->nlist);
        VINDEX_THROW_IF_NOT(
                ils->code_size == InvertedLists::INVALID_CODE_SIZE ||
                ils->code_size == ivf->code_size);
    }
    ivf->invlists = ils;
    ivf->own_invlists = true;
}


static void read_direct_map(DirectMap* dm, IOReader* f) {
    char maintain_direct_map;
    READ1(maintain_direct_map);
    dm->type = (DirectMap::Type)maintain_direct_map;
    READVECTOR(dm->array);
    if (dm->type == DirectMap::Hashtable) {
        std::vector<std::pair<int64_t, int64_t>> v;
        READVECTOR(v);
        std::unordered_map<int64_t, int64_t>& map = dm->hashtable;
        map.reserve(v.size());
        for (auto it : v) {
            map[it.first] = it.second;
        }
    }
}

static void read_ivf_header(
        IndexIVF* ivf,
        IOReader* f,
        std::vector<std::vector<int64_t>>* ids = nullptr) {
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
        std::vector<std::vector<int64_t>>& ids) {
    ArrayInvertedLists* ail =
            new ArrayInvertedLists(ivf->nlist, ivf->code_size);
    std::swap(ail->ids, ids);
    ivf->invlists = ail;
    ivf->own_invlists = true;
    return ail;
}

static void read_ProductQuantizer(ProductQuantizer* pq, IOReader* f) {
    READ1(pq->d);
    READ1(pq->M);
    READ1(pq->nbits);
    pq->set_derived_values();
    READVECTOR(pq->centroids);
}

ProductQuantizer* read_ProductQuantizer(IOReader* reader) {
    ProductQuantizer* pq = new ProductQuantizer();
    ScopeDeleter1<ProductQuantizer> del(pq);

    read_ProductQuantizer(pq, reader);
    del.release();
    return pq;
}

ProductQuantizer* read_ProductQuantizer(const char* fname) {
    FileIOReader reader(fname);
    return read_ProductQuantizer(&reader);
}
static IndexIVFPQ* read_ivfpq(IOReader* f, uint32_t h, int io_flags) {
    bool legacy = h == fourcc("IvQR") || h == fourcc("IvPQ");

    IndexIVFPQ* ivpq = new IndexIVFPQ();

    std::vector<std::vector<int64_t>> ids;
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
    if ( h == fourcc("IxF2") || h == fourcc("IxFl")) {//qwq
        IndexFlat* idxf;
        if (h == fourcc("IxFl")) {
            idxf = new IndexFlat();
        } else if (h == fourcc("IxF2")) {
            idxf = new IndexFlatL2();
        }
        read_index_header(idxf, f);
        idxf->code_size = idxf->d * sizeof(float);
        READXBVECTOR(idxf->codes);
        VINDEX_THROW_IF_NOT(
                idxf->codes.size() == idxf->ntotal * idxf->code_size);
        // leak!
        idx = idxf;
    } else if (h == fourcc("IwFl")) {//qwq
        IndexIVFFlat* ivfl = new IndexIVFFlat();
        read_ivf_header(ivfl, f);
        ivfl->code_size = ivfl->d * sizeof(float);
        read_InvertedLists(ivfl, f, io_flags);
        idx = ivfl;
    } else if (
            h == fourcc("IvPQ")  || h == fourcc("IwPQ")) {
        idx = read_ivfpq(f, h, io_flags);
    } else {
        VINDEX_THROW_FMT(
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



} // namespace vindex
