#ifndef VINDEX_UTILS_HPP
#define VINDEX_UTILS_HPP
#include <stdint.h>

#include <set>
#include <string>
#include <vector>

#include "platform_macros.hpp"
#include "Heap.hpp"
namespace vindex
{
  double getmillisecs();
  /// a balanced assignment has a IF of 1
  double imbalance_factor(int n, int k, const int64_t *assign);
  /// same, takes a histogram as input
  double imbalance_factor(int k, const int *hist);
  const float* fvecs_maybe_subsample(
        size_t d,
        size_t* n,
        size_t nmax,
        const float* x,
        bool verbose = false,
        int64_t seed = 1234);
} // namespace vindex

#endif