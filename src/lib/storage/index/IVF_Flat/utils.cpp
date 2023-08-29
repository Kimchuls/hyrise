#include "utils.hpp"

#include <cassert>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <sys/time.h>
#include <unistd.h>
#include <omp.h>

#include <algorithm>
#include <set>
#include <type_traits>
#include <vector>
#include <sys/types.h>
#include "random.hpp"
namespace vindex
{
  double getmillisecs()
  {
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    return tv.tv_sec * 1e3 + tv.tv_usec * 1e-3;
  }
  double imbalance_factor(int k, const int *hist)
  {
    double tot = 0, uf = 0;

    for (int i = 0; i < k; i++)
    {
      tot += hist[i];
      uf += hist[i] * (double)hist[i];
    }
    uf = uf * k / (tot * tot);

    return uf;
  }

  double imbalance_factor(int n, int k, const int64_t *assign)
  {
    std::vector<int> hist(k, 0);
    for (int i = 0; i < n; i++)
    {
      hist[assign[i]]++;
    }

    return imbalance_factor(k, hist.data());
  }
  const float* fvecs_maybe_subsample(
        size_t d,
        size_t* n,
        size_t nmax,
        const float* x,
        bool verbose,
        int64_t seed) {
    if (*n <= nmax)
        return x; // nothing to do

    size_t n2 = nmax;
    if (verbose) {
        printf("  Input training set too big (max size is %zd), sampling "
               "%zd / %zd vectors\n",
               nmax,
               n2,
               *n);
    }
    std::vector<int> subset(*n);
    rand_perm(subset.data(), *n, seed);
    float* x_subset = new float[n2 * d];
    for (int64_t i = 0; i < n2; i++)
        memcpy(&x_subset[i * d], &x[subset[i] * size_t(d)], sizeof(x[0]) * d);
    *n = n2;
    return x_subset;
}
} // namespace vindex
