#include "extra_distances.hpp"

#include <omp.h>

#include "AuxIndexStructures.hpp"
#include "VIndexAssert.hpp"

namespace vindex
{

  namespace
  {
    template <class VD, class C>
    void knn_extra_metrics_template(
        VD vd,
        const float *x,
        const float *y,
        size_t nx,
        size_t ny,
        HeapArray<C> *res)
    {
      size_t k = res->k;
      size_t d = vd.d;
      size_t check_period = InterruptCallback::get_period_hint(ny * d);
      check_period *= omp_get_max_threads();

      for (size_t i0 = 0; i0 < nx; i0 += check_period)
      {
        size_t i1 = std::min(i0 + check_period, nx);

#pragma omp parallel for
        for (int64_t i = i0; i < i1; i++)
        {
          const float *x_i = x + i * d;
          const float *y_j = y;
          size_t j;
          float *simi = res->get_val(i);
          int64_t *idxi = res->get_ids(i);

          // maxheap_heapify(k, simi, idxi);
          heap_heapify<C>(k, simi, idxi);
          for (j = 0; j < ny; j++)
          {
            float disij = vd(x_i, y_j);

            // if (disij < simi[0]) {
            if ((!vd.is_similarity && (disij < simi[0])) ||
                (vd.is_similarity && (disij > simi[0])))
            {
              // maxheap_replace_top(k, simi, idxi, disij, j);
              heap_replace_top<C>(k, simi, idxi, disij, j);
            }
            y_j += d;
          }
          // maxheap_reorder(k, simi, idxi);
          heap_reorder<C>(k, simi, idxi);
        }
        InterruptCallback::check();
      }
    }
  } // anonymous namespace

  template <class C>
  void knn_extra_metrics(
      const float *x,
      const float *y,
      size_t d,
      size_t nx,
      size_t ny,
      MetricType mt,
      float metric_arg,
      HeapArray<C> *res)
  {
    switch (mt)
    {
#define HANDLE_VAR(kw)                                        \
  case METRIC_##kw:                                           \
  {                                                           \
    VectorDistance<METRIC_##kw> vd = {(size_t)d, metric_arg}; \
    knn_extra_metrics_template(vd, x, y, nx, ny, res);        \
    break;                                                    \
  }
      HANDLE_VAR(L2);
      HANDLE_VAR(L1);
      HANDLE_VAR(Linf);
      HANDLE_VAR(Canberra);
      HANDLE_VAR(BrayCurtis);
      HANDLE_VAR(JensenShannon);
      HANDLE_VAR(Lp);
      HANDLE_VAR(Jaccard);
#undef HANDLE_VAR
    default:
      VINDEX_THROW_MSG("metric type not implemented");
    }
  }

  template void knn_extra_metrics<CMax<float, int64_t>>(
      const float *x,
      const float *y,
      size_t d,
      size_t nx,
      size_t ny,
      MetricType mt,
      float metric_arg,
      HeapArray<CMax<float, int64_t>> *res);

  template void knn_extra_metrics<CMin<float, int64_t>>(
      const float *x,
      const float *y,
      size_t d,
      size_t nx,
      size_t ny,
      MetricType mt,
      float metric_arg,
      HeapArray<CMin<float, int64_t>> *res);
} // namespace vindex
