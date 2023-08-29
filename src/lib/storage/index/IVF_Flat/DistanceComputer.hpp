#pragma once
#include "Index.hpp"
namespace vindex
{
  struct DistanceComputer
  {
    /// called before computing distances. Pointer x should remain valid
    /// while operator () is called
    virtual void set_query(const float *x) = 0;

    /// compute distance of vector i to current query
    virtual float operator()(int64_t i) = 0;

    /// compute distances of current query to 4 stored vectors.
    /// certain DistanceComputer implementations may benefit
    /// heavily from this.
    virtual void distances_batch_4(
        const int64_t idx0,
        const int64_t idx1,
        const int64_t idx2,
        const int64_t idx3,
        float &dis0,
        float &dis1,
        float &dis2,
        float &dis3)
    {
      // compute first, assign next
      const float d0 = this->operator()(idx0);
      const float d1 = this->operator()(idx1);
      const float d2 = this->operator()(idx2);
      const float d3 = this->operator()(idx3);
      dis0 = d0;
      dis1 = d1;
      dis2 = d2;
      dis3 = d3;
    }

    /// compute distance between two stored vectors
    virtual float symmetric_dis(int64_t i, int64_t j) = 0;

    virtual ~DistanceComputer() {}
  };
  
  struct FlatCodesDistanceComputer : DistanceComputer
  {
    const uint8_t *codes;
    size_t code_size;

    FlatCodesDistanceComputer(const uint8_t *codes, size_t code_size)
        : codes(codes), code_size(code_size) {}

    FlatCodesDistanceComputer() : codes(nullptr), code_size(0) {}

    float operator()(int64_t i) override
    {
      return distance_to_code(codes + i * code_size);
    }

    /// compute distance of current query to an encoded vector
    virtual float distance_to_code(const uint8_t *code) = 0;

    virtual ~FlatCodesDistanceComputer() {}
  };
} // namespace vindex
