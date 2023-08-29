#ifndef VINDEX_INDEX_IVF_FLAT_HPP
#define VINDEX_INDEX_IVF_FLAT_HPP
#include "IndexIVF.hpp"

namespace vindex
{
  struct IndexIVFFlat : public IndexIVF
  {
  public:
IndexIVFFlat(
            Index* quantizer,
            size_t d,
            size_t nlist_,
            MetricType = METRIC_L2);

    void add_core(
            int64_t n,
            const float* x,
            const int64_t* xids,
            const int64_t* precomputed_idx) override;

    void encode_vectors(
            int64_t n,
            const float* x,
            const int64_t* list_nos,
            uint8_t* codes,
            bool include_listnos = false) const override;

    InvertedListScanner* get_InvertedListScanner(
            bool store_pairs,
            const IDSelector* sel) const override;

    void reconstruct_from_offset(int64_t list_no, int64_t offset, float* recons)
            const override;

    void sa_decode(int64_t n, const uint8_t* bytes, float* x) const override;

    IndexIVFFlat();
  };
} // namespace vindex
#endif