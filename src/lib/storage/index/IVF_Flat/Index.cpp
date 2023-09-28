#include "Index.hpp"

#include <cstring>

#include "AuxIndexStructures.hpp"
#include "distances.hpp"
#include "DistanceComputer.hpp"
#include "VIndexAssert.hpp"
namespace vindex
{
  Index::~Index() {}

void Index::train(int64_t /*n*/, const float* /*x*/) {
    // does nothing by default
    printf("Index::train\n");
}

void Index::range_search(
        int64_t,
        const float*,
        float,
        RangeSearchResult*,
        const SearchParameters* params) const {
    VINDEX_THROW_MSG("range search not implemented");
}

void Index::assign(int64_t n, const float* x, int64_t* labels, int64_t k) const {
    std::vector<float> distances(n * k);
    search(n, x, k, distances.data(), labels);
}

void Index::add_with_ids(
        int64_t /*n*/,
        const float* /*x*/,
        const int64_t* /*xids*/) {
    VINDEX_THROW_MSG("add_with_ids not implemented for this type of index");
}

size_t Index::remove_ids(const IDSelector& /*sel*/) {
    VINDEX_THROW_MSG("remove_ids not implemented for this type of index");
    return -1;
}

void Index::reconstruct(int64_t, float*) const {
    VINDEX_THROW_MSG("reconstruct not implemented for this type of index");
}

void Index::reconstruct_batch(int64_t n, const int64_t* keys, float* recons) const {
    std::mutex exception_mutex;
    std::string exception_string;
#pragma omp parallel for if (n > 1000)
    for (int64_t i = 0; i < n; i++) {
        try {
            reconstruct(keys[i], &recons[i * d]);
        } catch (const std::exception& e) {
            std::lock_guard<std::mutex> lock(exception_mutex);
            exception_string = e.what();
        }
    }
    if (!exception_string.empty()) {
        VINDEX_THROW_MSG(exception_string.c_str());
    }
}

void Index::reconstruct_n(int64_t i0, int64_t ni, float* recons) const {
#pragma omp parallel for if (ni > 1000)
    for (int64_t i = 0; i < ni; i++) {
        reconstruct(i0 + i, recons + i * d);
    }
}

void Index::search_and_reconstruct(
        int64_t n,
        const float* x,
        int64_t k,
        float* distances,
        int64_t* labels,
        float* recons,
        const SearchParameters* params) const {
    VINDEX_THROW_IF_NOT(k > 0);

    search(n, x, k, distances, labels, params);
    for (int64_t i = 0; i < n; ++i) {
        for (int64_t j = 0; j < k; ++j) {
            int64_t ij = i * k + j;
            int64_t key = labels[ij];
            float* reconstructed = recons + ij * d;
            if (key < 0) {
                // Fill with NaNs
                memset(reconstructed, -1, sizeof(*reconstructed) * d);
            } else {
                reconstruct(key, reconstructed);
            }
        }
    }
}

void Index::compute_residual(const float* x, float* residual, int64_t key) const {
    reconstruct(key, residual);
    for (size_t i = 0; i < d; i++) {
        residual[i] = x[i] - residual[i];
    }
}

void Index::compute_residual_n(
        int64_t n,
        const float* xs,
        float* residuals,
        const int64_t* keys) const {
#pragma omp parallel for
    for (int64_t i = 0; i < n; ++i) {
        compute_residual(&xs[i * d], &residuals[i * d], keys[i]);
    }
}

size_t Index::sa_code_size() const {
    VINDEX_THROW_MSG("standalone codec not implemented for this type of index");
}

void Index::sa_encode(int64_t, const float*, uint8_t*) const {
    VINDEX_THROW_MSG("standalone codec not implemented for this type of index");
}

void Index::sa_decode(int64_t, const uint8_t*, float*) const {
    VINDEX_THROW_MSG("standalone codec not implemented for this type of index");
}

namespace {

// storage that explicitly reconstructs vectors before computing distances
struct GenericDistanceComputer : DistanceComputer {
    size_t d;
    const Index& storage;
    std::vector<float> buf;
    const float* q;

    explicit GenericDistanceComputer(const Index& storage) : storage(storage) {
        d = storage.d;
        buf.resize(d * 2);
    }

    float operator()(int64_t i) override {
        storage.reconstruct(i, buf.data());
        return fvec_L2sqr(q, buf.data(), d);
    }

    float symmetric_dis(int64_t i, int64_t j) override {
        storage.reconstruct(i, buf.data());
        storage.reconstruct(j, buf.data() + d);
        return fvec_L2sqr(buf.data() + d, buf.data(), d);
    }

    void set_query(const float* x) override {
        q = x;
    }
};

} // namespace

DistanceComputer* Index::get_distance_computer() const {
    if (metric_type == METRIC_L2) {
        return new GenericDistanceComputer(*this);
    } else {
        VINDEX_THROW_MSG("get_distance_computer() not implemented");
    }
}

void Index::merge_from(Index& /* otherIndex */, int64_t /* add_id */) {
    VINDEX_THROW_MSG("merge_from() not implemented");
}

void Index::check_compatible_for_merge(const Index& /* otherIndex */) const {
    VINDEX_THROW_MSG("check_compatible_for_merge() not implemented");
}


} // namespace vindex
