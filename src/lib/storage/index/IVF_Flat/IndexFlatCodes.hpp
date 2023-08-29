#pragma once

#include "Index.hpp"
#include <vector>
namespace vindex
{
  struct IndexFlatCodes : public Index
  {
  public:
    size_t code_size;
    std::vector<uint8_t> codes;

    IndexFlatCodes();
    IndexFlatCodes(size_t code_size, int64_t d, MetricType metric = METRIC_L2);

    void add(int64_t n, const float *x) override;
    void reset() override;
    // void reconstruct_n(idx_t i0, idx_t ni, float *recons) const override;
    // void reconstruct(idx_t key, float *recons) const override;
    // size_t sa_code_size() const override;
    // size_t remove_ids(const IDSelector &sel) override;
    // virtual FlatCodesDistanceComputer *get_FlatCodesDistanceComputer() const;
    // DistanceComputer *get_distance_computer() const override
    // {
    //   return get_FlatCodesDistanceComputer();
    // }
    // CodePacker *get_CodePacker() const;
    // void check_compatible_for_merge(const Index &otherIndex) const override;
    // virtual void merge_from(Index &otherIndex, idx_t add_id = 0) override;
    // void permute_entries(const idx_t *perm);
  };
} // namespace vindex
