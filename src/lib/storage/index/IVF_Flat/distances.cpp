#include "distances.hpp"
#include <cstdio>
#include <memory>
#include <omp.h>
#include <cmath>

#include "AuxIndexStructures.hpp"
#include "IDSelector.hpp"
#include "platform_macros.hpp"
#include "ResultHandler.hpp"
#include "VIndexAssert.hpp"
#ifndef FINTEGER
#define FINTEGER long
#endif
extern "C"
{
  /* declare BLAS functions, see http://www.netlib.org/clapack/cblas/ */

  int sgemm_(
      const char *transa,
      const char *transb,
      FINTEGER *m,
      FINTEGER *n,
      FINTEGER *k,
      const float *alpha,
      const float *a,
      FINTEGER *lda,
      const float *b,
      FINTEGER *ldb,
      float *beta,
      float *c,
      FINTEGER *ldc);
}

namespace vindex
{
  int distance_compute_blas_threshold = 20;
  int distance_compute_blas_query_bs = 4096;
  int distance_compute_blas_database_bs = 1024;
  int distance_compute_min_k_reservoir = 100;

  void fvec_norms_L2sqr(
      float *__restrict nr,
      const float *__restrict x,
      size_t d,
      size_t nx)
  {
#pragma omp parallel for schedule(guided)
    for (int64_t i = 0; i < nx; i++)
      nr[i] = fvec_norm_L2sqr(x + i * d, d);
  }

  namespace
  {
    template <class ResultHandler, bool use_sel = false>
    void exhaustive_inner_product_seq(
        const float *x,
        const float *y,
        size_t d,
        size_t nx,
        size_t ny,
        ResultHandler &res,
        const IDSelector *sel = nullptr)
    {
      using SingleResultHandler = typename ResultHandler::SingleResultHandler;
      int nt = std::min(int(nx), omp_get_max_threads());

      VINDEX_ASSERT(use_sel == (sel != nullptr));

#pragma omp parallel num_threads(nt)
      {
        SingleResultHandler resi(res);
#pragma omp for
        for (int64_t i = 0; i < nx; i++)
        {
          const float *x_i = x + i * d;
          const float *y_j = y;

          resi.begin(i);

          for (size_t j = 0; j < ny; j++, y_j += d)
          {
            if (use_sel && !sel->is_member(j))
            {
              continue;
            }
            float ip = fvec_inner_product(x_i, y_j, d);
            resi.add_result(ip, j);
          }
          resi.end();
        }
      }
    }
    template <class ResultHandler, bool use_sel = false>

    void exhaustive_L2sqr_seq(
        const float *x,
        const float *y,
        size_t d,
        size_t nx,
        size_t ny,
        ResultHandler &res,
        const IDSelector *sel = nullptr)
    {
      using SingleResultHandler = typename ResultHandler::SingleResultHandler;
      int nt = std::min(int(nx), omp_get_max_threads());

      VINDEX_ASSERT(use_sel == (sel != nullptr));

#pragma omp parallel num_threads(nt)
      {
        SingleResultHandler resi(res);
#pragma omp for
        for (int64_t i = 0; i < nx; i++)
        {
          const float *x_i = x + i * d;
          const float *y_j = y;
          resi.begin(i);
          for (size_t j = 0; j < ny; j++, y_j += d)
          {
            if (use_sel && !sel->is_member(j))
            {
              continue;
            }
            float disij = fvec_L2sqr(x_i, y_j, d);
            resi.add_result(disij, j);
          }
          resi.end();
        }
      }
    }

    template <class ResultHandler>
    void exhaustive_inner_product_blas(
        const float *x,
        const float *y,
        size_t d,
        size_t nx,
        size_t ny,
        ResultHandler &res)
    {
      // BLAS does not like empty matrices
      if (nx == 0 || ny == 0)
        return;

      /* block sizes */
      const size_t bs_x = distance_compute_blas_query_bs;
      const size_t bs_y = distance_compute_blas_database_bs;
      std::unique_ptr<float[]> ip_block(new float[bs_x * bs_y]);

      for (size_t i0 = 0; i0 < nx; i0 += bs_x)
      {
        size_t i1 = i0 + bs_x;
        if (i1 > nx)
          i1 = nx;

        res.begin_multiple(i0, i1);

        for (size_t j0 = 0; j0 < ny; j0 += bs_y)
        {
          size_t j1 = j0 + bs_y;
          if (j1 > ny)
            j1 = ny;
          /* compute the actual dot products */
          {
            float one = 1, zero = 0;
            FINTEGER nyi = j1 - j0, nxi = i1 - i0, di = d;
            sgemm_("Transpose",
                   "Not transpose",
                   &nyi,
                   &nxi,
                   &di,
                   &one,
                   y + j0 * d,
                   &di,
                   x + i0 * d,
                   &di,
                   &zero,
                   ip_block.get(),
                   &nyi);
          }

          res.add_results(j0, j1, ip_block.get());
        }
        res.end_multiple();
        InterruptCallback::check();
      }
    }

    template <class ResultHandler>
    void exhaustive_L2sqr_blas_default_impl(
        const float *x,
        const float *y,
        size_t d,
        size_t nx,
        size_t ny,
        ResultHandler &res,
        const float *y_norms = nullptr)
    {
      // BLAS does not like empty matrices
      if (nx == 0 || ny == 0)
        return;

      /* block sizes */
      const size_t bs_x = distance_compute_blas_query_bs;
      const size_t bs_y = distance_compute_blas_database_bs;
      // const size_t bs_x = 16, bs_y = 16;
      std::unique_ptr<float[]> ip_block(new float[bs_x * bs_y]);
      std::unique_ptr<float[]> x_norms(new float[nx]);
      std::unique_ptr<float[]> del2;

      fvec_norms_L2sqr(x_norms.get(), x, d, nx);

      if (!y_norms)
      {
        float *y_norms2 = new float[ny];
        del2.reset(y_norms2);
        fvec_norms_L2sqr(y_norms2, y, d, ny);
        y_norms = y_norms2;
      }

      for (size_t i0 = 0; i0 < nx; i0 += bs_x)
      {
        size_t i1 = i0 + bs_x;
        if (i1 > nx)
          i1 = nx;

        res.begin_multiple(i0, i1);

        for (size_t j0 = 0; j0 < ny; j0 += bs_y)
        {
          size_t j1 = j0 + bs_y;
          if (j1 > ny)
            j1 = ny;
          /* compute the actual dot products */
          {
            float one = 1, zero = 0;
            FINTEGER nyi = j1 - j0, nxi = i1 - i0, di = d;
            sgemm_("Transpose",
                   "Not transpose",
                   &nyi,
                   &nxi,
                   &di,
                   &one,
                   y + j0 * d,
                   &di,
                   x + i0 * d,
                   &di,
                   &zero,
                   ip_block.get(),
                   &nyi);
          }
#pragma omp parallel for
          for (int64_t i = i0; i < i1; i++)
          {
            float *ip_line = ip_block.get() + (i - i0) * (j1 - j0);

            for (size_t j = j0; j < j1; j++)
            {
              float ip = *ip_line;
              float dis = x_norms[i] + y_norms[j] - 2 * ip;

              // negative values can occur for identical vectors
              // due to roundoff errors
              if (dis < 0)
                dis = 0;

              *ip_line = dis;
              ip_line++;
            }
          }
          res.add_results(j0, j1, ip_block.get());
        }
        res.end_multiple();
        InterruptCallback::check();
      }
    }
    template <class ResultHandler>
    void exhaustive_L2sqr_blas(
        const float *x,
        const float *y,
        size_t d,
        size_t nx,
        size_t ny,
        ResultHandler &res,
        const float *y_norms = nullptr)
    {
      exhaustive_L2sqr_blas_default_impl(x, y, d, nx, ny, res);
    }

    // an override if only a single closest point is needed
    template <>
    void exhaustive_L2sqr_blas<SingleBestResultHandler<CMax<float, int64_t>>>(
        const float *x,
        const float *y,
        size_t d,
        size_t nx,
        size_t ny,
        SingleBestResultHandler<CMax<float, int64_t>> &res,
        const float *y_norms)
    {
      // #if defined(__AVX2__)
      //     // use a faster fused kernel if available
      //     if (exhaustive_L2sqr_fused_cmax(x, y, d, nx, ny, res, y_norms)) {
      //         // the kernel is available and it is complete, we're done.
      //         return;
      //     }

      //     // run the specialized AVX2 implementation
      //     exhaustive_L2sqr_blas_cmax_avx2(x, y, d, nx, ny, res, y_norms);

      // #elif defined(__aarch64__)
      //     // use a faster fused kernel if available
      //     if (exhaustive_L2sqr_fused_cmax(x, y, d, nx, ny, res, y_norms)) {
      //         // the kernel is available and it is complete, we're done.
      //         return;
      //     }

      //     // run the default implementation
      //     exhaustive_L2sqr_blas_default_impl<
      //             SingleBestResultHandler<CMax<float, int64_t>>>(
      //             x, y, d, nx, ny, res, y_norms);
      // #else
      // run the default implementation
      exhaustive_L2sqr_blas_default_impl<
          SingleBestResultHandler<CMax<float, int64_t>>>(
          x, y, d, nx, ny, res, y_norms);
      // #endif
    }

    template <class ResultHandler>
    void knn_L2sqr_select(
        const float *x,
        const float *y,
        size_t d,
        size_t nx,
        size_t ny,
        ResultHandler &res,
        const float *y_norm2,
        const IDSelector *sel)
    {
      if (sel)
      {
        exhaustive_L2sqr_seq<ResultHandler, true>(x, y, d, nx, ny, res, sel);
      }
      else if (nx < distance_compute_blas_threshold)
      {
        exhaustive_L2sqr_seq(x, y, d, nx, ny, res);
      }
      else
      {
        exhaustive_L2sqr_blas(x, y, d, nx, ny, res, y_norm2);
      }
    }

  } // namespace

  // int distance_compute_blas_threshold = 20;
  // int distance_compute_blas_query_bs = 4096;
  // int distance_compute_blas_database_bs = 1024;
  // int distance_compute_min_k_reservoir = 100;

  void knn_inner_product(
      const float *x,
      const float *y,
      size_t d,
      size_t nx,
      size_t ny,
      size_t k,
      float *val,
      int64_t *ids,
      const IDSelector *sel)
  {
    int64_t imin = 0;
    if (auto selr = dynamic_cast<const IDSelectorRange *>(sel))
    {
      imin = std::max(selr->imin, int64_t(0));
      int64_t imax = std::min(selr->imax, int64_t(ny));
      ny = imax - imin;
      y += d * imin;
      sel = nullptr;
    }
    if (auto sela = dynamic_cast<const IDSelectorArray *>(sel))
    {
      knn_inner_products_by_idx(
          x, y, sela->ids, d, nx, sela->n, k, val, ids, 0);
      return;
    }
    if (k < distance_compute_min_k_reservoir)
    {
      using RH = HeapResultHandler<CMin<float, int64_t>>;
      RH res(nx, val, ids, k);
      if (sel)
      {
        exhaustive_inner_product_seq<RH, true>(x, y, d, nx, ny, res, sel);
      }
      else if (nx < distance_compute_blas_threshold)
      {
        exhaustive_inner_product_seq(x, y, d, nx, ny, res);
      }
      else
      {
        exhaustive_inner_product_blas(x, y, d, nx, ny, res);
      }
    }
    else
    {
      using RH = ReservoirResultHandler<CMin<float, int64_t>>;
      RH res(nx, val, ids, k);
      if (sel)
      {
        exhaustive_inner_product_seq<RH, true>(x, y, d, nx, ny, res, sel);
      }
      else if (nx < distance_compute_blas_threshold)
      {
        exhaustive_inner_product_seq(x, y, d, nx, ny, res, nullptr);
      }
      else
      {
        exhaustive_inner_product_blas(x, y, d, nx, ny, res);
      }
    }
    if (imin != 0)
    {
      for (size_t i = 0; i < nx * k; i++)
      {
        if (ids[i] >= 0)
        {
          ids[i] += imin;
        }
      }
    }
  }
  void knn_inner_product(
      const float *x,
      const float *y,
      size_t d,
      size_t nx,
      size_t ny,
      float_minheap_array_t *res,
      const IDSelector *sel)
  {
    VINDEX_THROW_IF_NOT(nx == res->nh);
    knn_inner_product(x, y, d, nx, ny, res->k, res->val, res->ids, sel);
  }

  void knn_inner_products_by_idx(
      const float *x,
      const float *y,
      const int64_t *ids,
      size_t d,
      size_t nx,
      size_t ny,
      size_t k,
      float *res_vals,
      int64_t *res_ids,
      int64_t ld_ids)
  {
    if (ld_ids < 0)
    {
      ld_ids = ny;
    }

#pragma omp parallel for if (nx > 100)
    for (int64_t i = 0; i < nx; i++)
    {
      const float *x_ = x + i * d;
      const int64_t *idsi = ids + i * ld_ids;
      size_t j;
      float *__restrict simi = res_vals + i * k;
      int64_t *__restrict idxi = res_ids + i * k;
      minheap_heapify(k, simi, idxi);

      for (j = 0; j < ny; j++)
      {
        if (idsi[j] < 0)
          break;
        float ip = fvec_inner_product(x_, y + d * idsi[j], d);

        if (ip > simi[0])
        {
          minheap_replace_top(k, simi, idxi, ip, idsi[j]);
        }
      }
      minheap_reorder(k, simi, idxi);
    }
  }

  void knn_L2sqr(
      const float *x,
      const float *y,
      size_t d,
      size_t nx,
      size_t ny,
      size_t k,
      float *vals,
      int64_t *ids,
      const float *y_norm2,
      const IDSelector *sel)
  {
    int64_t imin = 0;
    if (auto selr = dynamic_cast<const IDSelectorRange *>(sel))
    {
      imin = std::max(selr->imin, int64_t(0));
      int64_t imax = std::min(selr->imax, int64_t(ny));
      ny = imax - imin;
      y += d * imin;
      sel = nullptr;
    }
    if (auto sela = dynamic_cast<const IDSelectorArray *>(sel))
    {
      knn_L2sqr_by_idx(x, y, sela->ids, d, nx, sela->n, k, vals, ids, 0);
      return;
    }
    if (k == 1)
    {
      SingleBestResultHandler<CMax<float, int64_t>> res(nx, vals, ids);
      knn_L2sqr_select(x, y, d, nx, ny, res, y_norm2, sel);
    }
    else if (k < distance_compute_min_k_reservoir)
    {
      HeapResultHandler<CMax<float, int64_t>> res(nx, vals, ids, k);
      knn_L2sqr_select(x, y, d, nx, ny, res, y_norm2, sel);
    }
    else
    {
      ReservoirResultHandler<CMax<float, int64_t>> res(nx, vals, ids, k);
      knn_L2sqr_select(x, y, d, nx, ny, res, y_norm2, sel);
    }
    if (imin != 0)
    {
      for (size_t i = 0; i < nx * k; i++)
      {
        if (ids[i] >= 0)
        {
          ids[i] += imin;
        }
      }
    }
  }

  void knn_L2sqr(
      const float *x,
      const float *y,
      size_t d,
      size_t nx,
      size_t ny,
      float_maxheap_array_t *res,
      const float *y_norm2,
      const IDSelector *sel)
  {
    VINDEX_THROW_IF_NOT(res->nh == nx);
    knn_L2sqr(x, y, d, nx, ny, res->k, res->val, res->ids, y_norm2, sel);
  }
  void knn_L2sqr_by_idx(
      const float *x,
      const float *y,
      const int64_t *__restrict ids,
      size_t d,
      size_t nx,
      size_t ny,
      size_t k,
      float *res_vals,
      int64_t *res_ids,
      int64_t ld_ids)
  {
    if (ld_ids < 0)
    {
      ld_ids = ny;
    }
#pragma omp parallel for if (nx > 100)
    for (int64_t i = 0; i < nx; i++)
    {
      const float *x_ = x + i * d;
      const int64_t *__restrict idsi = ids + i * ld_ids;
      float *__restrict simi = res_vals + i * k;
      int64_t *__restrict idxi = res_ids + i * k;
      maxheap_heapify(k, simi, idxi);
      for (size_t j = 0; j < ny; j++)
      {
        float disij = fvec_L2sqr(x_, y + d * idsi[j], d);

        if (disij < simi[0])
        {
          maxheap_replace_top(k, simi, idxi, disij, idsi[j]);
        }
      }
      maxheap_reorder(k, simi, idxi);
    }
  }
  /***************************************************************************
   * Range search
   ***************************************************************************/

  void range_search_L2sqr(
      const float *x,
      const float *y,
      size_t d,
      size_t nx,
      size_t ny,
      float radius,
      RangeSearchResult *res,
      const IDSelector *sel)
  {
    using RH = RangeSearchResultHandler<CMax<float, int64_t>>;
    RH resh(res, radius);
    if (sel)
    {
      exhaustive_L2sqr_seq<RH, true>(x, y, d, nx, ny, resh, sel);
    }
    else if (nx < distance_compute_blas_threshold)
    {
      exhaustive_L2sqr_seq(x, y, d, nx, ny, resh, sel);
    }
    else
    {
      exhaustive_L2sqr_blas(x, y, d, nx, ny, resh);
    }
  }
  void range_search_inner_product(
      const float *x,
      const float *y,
      size_t d,
      size_t nx,
      size_t ny,
      float radius,
      RangeSearchResult *res,
      const IDSelector *sel)
  {
    using RH = RangeSearchResultHandler<CMin<float, int64_t>>;
    RH resh(res, radius);
    if (sel)
    {
      exhaustive_inner_product_seq<RH, true>(x, y, d, nx, ny, resh, sel);
    }
    else if (nx < distance_compute_blas_threshold)
    {
      exhaustive_inner_product_seq(x, y, d, nx, ny, resh);
    }
    else
    {
      exhaustive_inner_product_blas(x, y, d, nx, ny, resh);
    }
  }
  /***************************************************************************
   * compute a subset of  distances
   ***************************************************************************/

  /* compute the inner product between x and a subset y of ny vectors,
     whose indices are given by idy.  */
  void fvec_inner_products_by_idx(
      float *__restrict ip,
      const float *x,
      const float *y,
      const int64_t *__restrict ids, /* for y vecs */
      size_t d,
      size_t nx,
      size_t ny)
  {
#pragma omp parallel for
    for (int64_t j = 0; j < nx; j++)
    {
      const int64_t *__restrict idsj = ids + j * ny;
      const float *xj = x + j * d;
      float *__restrict ipj = ip + j * ny;
      for (size_t i = 0; i < ny; i++)
      {
        if (idsj[i] < 0)
          continue;
        ipj[i] = fvec_inner_product(xj, y + d * idsj[i], d);
      }
    }
  }

  /* compute the inner product between x and a subset y of ny vectors,
     whose indices are given by idy.  */
  void fvec_L2sqr_by_idx(
      float *__restrict dis,
      const float *x,
      const float *y,
      const int64_t *__restrict ids, /* ids of y vecs */
      size_t d,
      size_t nx,
      size_t ny)
  {
#pragma omp parallel for
    for (int64_t j = 0; j < nx; j++)
    {
      const int64_t *__restrict idsj = ids + j * ny;
      const float *xj = x + j * d;
      float *__restrict disj = dis + j * ny;
      for (size_t i = 0; i < ny; i++)
      {
        if (idsj[i] < 0)
          continue;
        disj[i] = fvec_L2sqr(xj, y + d * idsj[i], d);
      }
    }
  }
} // namespace vindex
