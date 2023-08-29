#pragma once
#include <random>

namespace vindex
{
  struct RandomGenerator
  {
    public:
    std::mt19937 mt;

    /// random positive integer
    int rand_int();
    /// random int64_t
    int64_t rand_int64();
    /// generate random integer between 0 and max-1
    int rand_int(int max);
    /// between 0 and 1
    float rand_float();
    double rand_double();
    explicit RandomGenerator(int64_t seed = 1234);
  };

  /* random permutation */
  void rand_perm(int *perm, size_t n, int64_t seed);
} // namespace vindex
