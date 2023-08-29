#include "random.hpp"

namespace vindex
{
  /**************************************************
   * Random data generation functions
   **************************************************/

  RandomGenerator::RandomGenerator(int64_t seed) : mt((unsigned int)seed) {}

  int RandomGenerator::rand_int()
  {
    return mt() & 0x7fffffff;
  }

  int64_t RandomGenerator::rand_int64()
  {
    return int64_t(rand_int()) | int64_t(rand_int()) << 31;
  }

  int RandomGenerator::rand_int(int max)
  {
    return mt() % max;
  }

  float RandomGenerator::rand_float()
  {
    return mt() / float(mt.max());
  }

  double RandomGenerator::rand_double()
  {
    return mt() / double(mt.max());
  }
  void rand_perm(int *perm, size_t n, int64_t seed)
  {
    for (size_t i = 0; i < n; i++)
      perm[i] = i;

    RandomGenerator rng(seed);

    for (size_t i = 0; i + 1 < n; i++)
    {
      int i2 = i + rng.rand_int(n - i);
      std::swap(perm[i], perm[i2]);
    }
  }
} // namespace vindex
