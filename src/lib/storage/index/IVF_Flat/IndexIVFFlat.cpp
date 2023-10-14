#include "IndexIVFFlat.hpp"

#include <cstring>
#include <omp.h>

#include <cinttypes>
#include <cstdio>

#include "AuxIndexStructures.hpp"
#include "IDSelector.hpp"
#include "utils.hpp"
#include "distances.hpp"
#include "IndexFlat.hpp"
#include "VIndexAssert.hpp"
namespace vindex
{
    IndexIVFFlat::IndexIVFFlat(
        Index *quantizer,
        size_t d,
        size_t nlist,
        MetricType metric)
        : IndexIVF(quantizer, d, nlist, sizeof(float) * d, metric)
    {
        code_size = sizeof(float) * d;
        by_residual = false;
    }

    IndexIVFFlat::IndexIVFFlat()
    {
        by_residual = false;
    }

    void IndexIVFFlat::add_core(
        int64_t n,
        const float *x,
        const int64_t *xids,
        const int64_t *coarse_idx)
    {
        VINDEX_THROW_IF_NOT(is_trained);
        VINDEX_THROW_IF_NOT(coarse_idx);
        VINDEX_THROW_IF_NOT(!by_residual);
        assert(invlists);
        direct_map.check_can_add(xids);

        int64_t n_add = 0;

        DirectMapAdd dm_adder(direct_map, n, xids);
    // double t1=getmillisecs();
    // double time0=0.0,time1=0.0;
#pragma omp parallel reduction(+ : n_add)
        {
            int nt = omp_get_num_threads();
            int rank = omp_get_thread_num();

            // each thread takes care of a subset of lists
            for (size_t i = 0; i < n; i++)
            {
                int64_t list_no = coarse_idx[i];

                if (list_no >= 0 && list_no % nt == rank)
                {
                    int64_t id = xids ? xids[i] : ntotal + i;
                    const float *xi = x + i * d;
                    size_t offset =
                        invlists->add_entry(list_no, id, (const uint8_t *)xi);
                    dm_adder.add(i, list_no, offset);
                    n_add++;
                }
                else if (rank == 0 && list_no == -1)
                {
                    dm_adder.add(i, -1, 0);
                }
            }
        }
    // double t2=getmillisecs();
    // printf("IndexIVFFlat::add_core timestamp1: %.4f\n",(t2-t1)/1000);
        if (verbose)
        {
            printf("IndexIVFFlat::add_core: added %" PRId64 " / %" PRId64
                   " vectors\n",
                   n_add,
                   n);
        }
        ntotal += n;
    }

    void IndexIVFFlat::encode_vectors(
        int64_t n,
        const float *x,
        const int64_t *list_nos,
        uint8_t *codes,
        bool include_listnos) const
    {
        VINDEX_THROW_IF_NOT(!by_residual);
        if (!include_listnos)
        {
            memcpy(codes, x, code_size * n);
        }
        else
        {
            size_t coarse_size = coarse_code_size();
            for (size_t i = 0; i < n; i++)
            {
                int64_t list_no = list_nos[i];
                uint8_t *code = codes + i * (code_size + coarse_size);
                const float *xi = x + i * d;
                if (list_no >= 0)
                {
                    encode_listno(list_no, code);
                    memcpy(code + coarse_size, xi, code_size);
                }
                else
                {
                    memset(code, 0, code_size + coarse_size);
                }
            }
        }
    }

    void IndexIVFFlat::sa_decode(int64_t n, const uint8_t *bytes, float *x) const
    {
        size_t coarse_size = coarse_code_size();
        for (size_t i = 0; i < n; i++)
        {
            const uint8_t *code = bytes + i * (code_size + coarse_size);
            float *xi = x + i * d;
            memcpy(xi, code + coarse_size, code_size);
        }
    }

    namespace
    {

        template <MetricType metric, class C, bool use_sel>
        struct IVFFlatScanner : InvertedListScanner
        {
            size_t d;

            IVFFlatScanner(size_t d, bool store_pairs, const IDSelector *sel)
                : InvertedListScanner(store_pairs, sel), d(d) {}

            const float *xi;
            void set_query(const float *query) override
            {
                this->xi = query;
            }

            void set_list(int64_t list_no, float /* coarse_dis */) override
            {
                this->list_no = list_no;
            }

            float distance_to_code(const uint8_t *code) const override
            {
                const float *yj = (float *)code;
                float dis = metric == METRIC_INNER_PRODUCT
                                ? fvec_inner_product(xi, yj, d)
                                : fvec_L2sqr(xi, yj, d);
                return dis;
            }

            size_t scan_codes(
                size_t list_size,
                const uint8_t *codes,
                const int64_t *ids,
                float *simi,
                int64_t *idxi,
                size_t k, double* cost_time=nullptr) const override
            {
                const float *list_vecs = (const float *)codes;
                size_t nup = 0;
                for (size_t j = 0; j < list_size; j++)
                {
                    const float *yj = list_vecs + d * j;
                    if (use_sel && !sel->is_member(ids[j]))
                    {
                        continue;
                    }
                    // double t0=getmillisecs();
                    float dis = metric == METRIC_INNER_PRODUCT
                                    ? fvec_inner_product(xi, yj, d)
                                    : fvec_L2sqr(xi, yj, d);
                    // ivfflat_scanner_time0+=
                    // *cost_time+=(getmillisecs()-t0)/1000;
                    if (C::cmp(simi[0], dis))
                    {
                        int64_t id = store_pairs ? lo_build(list_no, j) : ids[j];
                        heap_replace_top<C>(k, simi, idxi, dis, id);
                        nup++;
                    }
                }
                return nup;
            }

            void scan_codes_range(
                size_t list_size,
                const uint8_t *codes,
                const int64_t *ids,
                float radius,
                RangeQueryResult &res) const override
            {
                const float *list_vecs = (const float *)codes;
                for (size_t j = 0; j < list_size; j++)
                {
                    const float *yj = list_vecs + d * j;
                    if (use_sel && !sel->is_member(ids[j]))
                    {
                        continue;
                    }
                    float dis = metric == METRIC_INNER_PRODUCT
                                    ? fvec_inner_product(xi, yj, d)
                                    : fvec_L2sqr(xi, yj, d);
                    if (C::cmp(radius, dis))
                    {
                        int64_t id = store_pairs ? lo_build(list_no, j) : ids[j];
                        res.add(dis, id);
                    }
                }
            }
        };

        template <bool use_sel>
        InvertedListScanner *get_InvertedListScanner1(
            const IndexIVFFlat *ivf,
            bool store_pairs,
            const IDSelector *sel)
        {
            if (ivf->metric_type == METRIC_INNER_PRODUCT)
            {
                return new IVFFlatScanner<
                    METRIC_INNER_PRODUCT,
                    CMin<float, int64_t>,
                    use_sel>(ivf->d, store_pairs, sel);
            }
            else if (ivf->metric_type == METRIC_L2)
            {
                return new IVFFlatScanner<METRIC_L2, CMax<float, int64_t>, use_sel>(
                    ivf->d, store_pairs, sel);
            }
            else
            {
                VINDEX_THROW_MSG("metric type not supported");
            }
        }

    } // anonymous namespace

    InvertedListScanner *IndexIVFFlat::get_InvertedListScanner(
        bool store_pairs,
        const IDSelector *sel) const
    {
        if (sel)
        {
            return get_InvertedListScanner1<true>(this, store_pairs, sel);
        }
        else
        {
            return get_InvertedListScanner1<false>(this, store_pairs, sel);
        }
    }

    void IndexIVFFlat::reconstruct_from_offset(
        int64_t list_no,
        int64_t offset,
        float *recons) const
    {
        memcpy(recons, invlists->get_single_code(list_no, offset), code_size);
    }

} // namespace vindex
