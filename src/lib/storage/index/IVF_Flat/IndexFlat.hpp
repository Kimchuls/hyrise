#ifndef INDEX_FLAT_HPP
#define INDEX_FLAT_HPP
#include "IndexFlatCodes.hpp"
#include <vector>
namespace vindex
{
struct IndexFlat : IndexFlatCodes {
    explicit IndexFlat(int64_t d, MetricType metric = METRIC_L2);

    void search(
            int64_t n,
            const float* x,
            int64_t k,
            float* distances,
            int64_t* labels,
            const SearchParameters* params = nullptr) const override;

    void range_search(
            int64_t n,
            const float* x,
            float radius,
            RangeSearchResult* result,
            const SearchParameters* params = nullptr) const override;

    void reconstruct(int64_t key, float* recons) const override;

    /** compute distance with a subset of vectors
     *
     * @param x       query vectors, size n * d
     * @param labels  indices of the vectors that should be compared
     *                for each query vector, size n * k
     * @param distances
     *                corresponding output distances, size n * k
     */
    void compute_distance_subset(
            int64_t n,
            const float* x,
            int64_t k,
            float* distances,
            const int64_t* labels) const;

    // get pointer to the floating point data
    float* get_xb() {
        return (float*)codes.data();
    }
    const float* get_xb() const {
        return (const float*)codes.data();
    }

    IndexFlat() {}

    // FlatCodesDistanceComputer* get_FlatCodesDistanceComputer() const override;

    /* The stanadlone codec interface (just memcopies in this case) */
    void sa_encode(int64_t n, const float* x, uint8_t* bytes) const override;

    void sa_decode(int64_t n, const uint8_t* bytes, float* x) const override;
};


  struct IndexFlatL2 : public IndexFlat
  {
  public:
    // std::vector<float> cached_l2norms;
    explicit IndexFlatL2(int64_t d) : IndexFlat(d, METRIC_L2) {}
    IndexFlatL2() {}

    // override for l2 norms cache.
    // FlatCodesDistanceComputer *get_FlatCodesDistanceComputer() const override;

    // compute L2 norms
    // void sync_l2norms();
    // clear L2 norms
    // void clear_l2norms();
  };
} // namespace vindex
#endif
