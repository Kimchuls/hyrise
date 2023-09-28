#include "IndexFlat.hpp"
#include <cstring>

#include "AuxIndexStructures.hpp"
#include "distances.hpp"
#include "extra_distances.hpp"
#include "Heap.hpp"
#include "prefetch.hpp"
#include "sorting.hpp"
#include "utils.hpp"
#include "VIndexAssert.hpp"
namespace vindex
{
IndexFlat::IndexFlat(int64_t d, MetricType metric)
        : IndexFlatCodes(sizeof(float) * d, d, metric) {}

void IndexFlat::search(
        int64_t n,
        const float* x,
        int64_t k,
        float* distances,
        int64_t* labels,
        const SearchParameters* params) const {
    IDSelector* sel = params ? params->sel : nullptr;
    VINDEX_THROW_IF_NOT(k > 0);

    // we see the distances and labels as heaps
    if (metric_type == METRIC_INNER_PRODUCT) {
        float_minheap_array_t res = {size_t(n), size_t(k), labels, distances};
        knn_inner_product(x, get_xb(), d, n, ntotal, &res, sel);
    } else if (metric_type == METRIC_L2) {
        float_maxheap_array_t res = {size_t(n), size_t(k), labels, distances};
        knn_L2sqr(x, get_xb(), d, n, ntotal, &res, nullptr, sel);
    } else if (is_similarity_metric(metric_type)) {
        float_minheap_array_t res = {size_t(n), size_t(k), labels, distances};
        knn_extra_metrics(
                x, get_xb(), d, n, ntotal, metric_type, metric_arg, &res);
    } else {
        VINDEX_THROW_IF_NOT(!sel);
        float_maxheap_array_t res = {size_t(n), size_t(k), labels, distances};
        knn_extra_metrics(
                x, get_xb(), d, n, ntotal, metric_type, metric_arg, &res);
    }
}

void IndexFlat::range_search(
        int64_t n,
        const float* x,
        float radius,
        RangeSearchResult* result,
        const SearchParameters* params) const {
    IDSelector* sel = params ? params->sel : nullptr;

    switch (metric_type) {
        case METRIC_INNER_PRODUCT:
            range_search_inner_product(
                    x, get_xb(), d, n, ntotal, radius, result, sel);
            break;
        case METRIC_L2:
            range_search_L2sqr(x, get_xb(), d, n, ntotal, radius, result, sel);
            break;
        default:
            VINDEX_THROW_MSG("metric type not supported");
    }
}

void IndexFlat::compute_distance_subset(
        int64_t n,
        const float* x,
        int64_t k,
        float* distances,
        const int64_t* labels) const {
    switch (metric_type) {
        case METRIC_INNER_PRODUCT:
            fvec_inner_products_by_idx(distances, x, get_xb(), labels, d, n, k);
            break;
        case METRIC_L2:
            fvec_L2sqr_by_idx(distances, x, get_xb(), labels, d, n, k);
            break;
        default:
            VINDEX_THROW_MSG("metric type not supported");
    }
}

void IndexFlat::reconstruct(int64_t key, float* recons) const {
    memcpy(recons, &(codes[key * code_size]), code_size);
}

void IndexFlat::sa_encode(int64_t n, const float* x, uint8_t* bytes) const {
    if (n > 0) {
        memcpy(bytes, x, sizeof(float) * d * n);
    }
}

void IndexFlat::sa_decode(int64_t n, const uint8_t* bytes, float* x) const {
    if (n > 0) {
        memcpy(x, bytes, sizeof(float) * d * n);
    }
}
} // namespace vindex
