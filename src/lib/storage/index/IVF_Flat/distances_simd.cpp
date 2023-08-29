#include "distances.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdio>
#include <cstring>

#include "VIndexAssert.hpp"
#include "platform_macros.hpp"
// #include <faiss/utils/simdlib.h>

#ifdef __SSE3__
#include <immintrin.h>
#endif

// #ifdef __AVX2__
// #include <faiss/utils/transpose/transpose-avx2-inl.h>
// #endif

#ifdef __aarch64__
#include <arm_neon.h>
#endif

namespace vindex
{

#ifdef __AVX__
#define USE_AVX
#endif
  float fvec_L1_ref(const float *x, const float *y, size_t d)
  {
    size_t i;
    float res = 0;
    for (i = 0; i < d; i++)
    {
      const float tmp = x[i] - y[i];
      res += fabs(tmp);
    }
    return res;
  }

  float fvec_Linf_ref(const float *x, const float *y, size_t d)
  {
    size_t i;
    float res = 0;
    for (i = 0; i < d; i++)
    {
      res = fmax(res, fabs(x[i] - y[i]));
    }
    return res;
  }

  float fvec_L1(const float *x, const float *y, size_t d)
  {
    return fvec_L1_ref(x, y, d);
  }

  float fvec_Linf(const float *x, const float *y, size_t d)
  {
    return fvec_Linf_ref(x, y, d);
  }

  VINDEX_PRAGMA_IMPRECISE_FUNCTION_BEGIN
  float fvec_norm_L2sqr(const float *x, size_t d)
  {
    // the double in the _ref is suspected to be a typo. Some of the manual
    // implementations this replaces used float.
    float res = 0;
    VINDEX_PRAGMA_IMPRECISE_LOOP
    for (size_t i = 0; i != d; ++i)
    {
      res += x[i] * x[i];
    }

    return res;
  }
  VINDEX_PRAGMA_IMPRECISE_FUNCTION_END

  void fvec_renorm_L2(size_t d, size_t nx, float *__restrict x)
  {
#pragma omp parallel for schedule(guided)
    for (int64_t i = 0; i < nx; i++)
    {
      float *__restrict xi = x + i * d;

      float nr = fvec_norm_L2sqr(xi, d);

      if (nr > 0)
      {
        size_t j;
        const float inv_nr = 1.0 / sqrtf(nr);
        for (j = 0; j < d; j++)
          xi[j] *= inv_nr;
      }
    }
  }

  VINDEX_PRAGMA_IMPRECISE_FUNCTION_BEGIN
  float fvec_inner_product(const float *x, const float *y, size_t d)
  {
    float res = 0.F;
    VINDEX_PRAGMA_IMPRECISE_LOOP
    for (size_t i = 0; i != d; ++i)
    {
      res += x[i] * y[i];
    }
    return res;
  }
  VINDEX_PRAGMA_IMPRECISE_FUNCTION_END

  VINDEX_PRAGMA_IMPRECISE_FUNCTION_BEGIN
  float fvec_L2sqr(const float *x, const float *y, size_t d)
  {
    size_t i;
    float res = 0;
    VINDEX_PRAGMA_IMPRECISE_LOOP
    for (i = 0; i < d; i++)
    {
      const float tmp = x[i] - y[i];
      res += tmp * tmp;
    }
    return res;
  }
  VINDEX_PRAGMA_IMPRECISE_FUNCTION_END

} // namespace vindex