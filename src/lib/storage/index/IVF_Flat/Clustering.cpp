#include "Clustering.hpp"
#include <assert.h>
#include <cinttypes>
#include <cmath>
#include <cstdio>
#include <memory>
#include <omp.h>

#include "distances.hpp"
#include "random.hpp"
#include "utils.hpp"
#include "VIndexAssert.hpp"

namespace vindex
{
  ClusteringParameters::ClusteringParameters()
      : niter(25),
        nredo(1),
        verbose(false),
        spherical(false),
        int_centroids(false),
        update_index(false),
        frozen_centroids(false),
        min_points_per_centroid(39),
        max_points_per_centroid(256),
        seed(1234),
        decode_block_size(32768) {}

  Clustering::Clustering(int d, int k) : d(d), k(k) {}
  Clustering::Clustering(int d, int k, const ClusteringParameters &cp)
      : ClusteringParameters(cp), d(d), k(k) {}

  void Clustering::train(int64_t n, const float *x, Index &index, const float *x_weight)
  {
    // printf("Clustering::train\n");
    // double t0=getmillisecs();
    // total_time_clear();
    train_encoded(n, reinterpret_cast<const uint8_t *>(x), nullptr, index, x_weight);
    // printf("Clustering::train (%.4lf s)\n",(getmillisecs()-t0)/1000);
  }
  namespace
  {
    int64_t subsample_training_set(const Clustering &clus, int64_t nx, const uint8_t *x, size_t line_size, const float *weights, uint8_t **x_out, float **weights_out)
    {
      if (clus.verbose)
      {
        printf("Sampling a subset of %zd / %" PRId64 " for training\n",
               clus.k * clus.max_points_per_centroid, nx);
      }
      std::vector<int> perm(nx);
      rand_perm(perm.data(), nx, clus.seed);
      nx = clus.k * clus.max_points_per_centroid;
      uint8_t *x_new = new uint8_t[nx * line_size];
      *x_out = x_new;
      for (int64_t i = 0; i < nx; i++)
        memcpy(x_new + i * line_size, x + perm[i] * line_size, line_size);
      if (weights)
      {
        float *weights_new = new float[nx];
        for (int64_t i = 0; i < nx; i++)
          weights_new[i] = weights[perm[i]];
        *weights_out = weights_new;
      }
      else
      {
        *weights_out = nullptr;
      }
      return nx;
    }

    void compute_centroids(size_t d, size_t k, size_t n, size_t k_frozen, const uint8_t *x, const Index *codec, const int64_t *assign, const float *weights, float *hassign, float *centroids)
    {
      k -= k_frozen;
      centroids += k_frozen * d;
      memset(centroids, 0, sizeof(*centroids) * d * k);
      size_t line_size = codec ? codec->sa_code_size() : d * sizeof(float);
#pragma omp parallel
      {
        int nt = omp_get_num_threads();
        int rank = omp_get_thread_num();
        // this thread is taking care of centroids c0:c1
        size_t c0 = (k * rank) / nt;
        size_t c1 = (k * (rank + 1)) / nt;
        std::vector<float> decode_buffer(d);

        for (size_t i = 0; i < n; i++)
        {
          int64_t ci = assign[i];
          assert(ci >= 0 && ci < k + k_frozen);
          ci -= k_frozen;
          if (ci >= c0 && ci < c1)
          {
            float *c = centroids + ci * d;
            const float *xi;
            if (!codec)
              xi = reinterpret_cast<const float *>(x + i * line_size);
            else
            {
              float *xif = decode_buffer.data();
              codec->sa_decode(1, x + i * line_size, xif);
              xi = xif;
            }
            if (weights)
            {
              float w = weights[i];
              hassign[ci] += w;
              for (size_t j = 0; j < d; j++)
                c[j] += xi[j] * w;
            }
            else
            {
              hassign[ci] += 1.0;
              for (size_t j = 0; j < d; j++)
                c[j] += xi[j];
            }
          }
        }
      }

#pragma omp parallel for
      for (int64_t ci = 0; ci < k; ci++)
      {
        if (hassign[ci] == 0)
          continue;
        float norm = 1 / hassign[ci];
        float *c = centroids + ci * d;
        for (size_t j = 0; j < d; j++)
          c[j] *= norm;
      }
    }
#define EPS (1 / 1024.)
    int split_clusters(size_t d, size_t k, size_t n, size_t k_frozen, float *hassign, float *centroids)
    {
      k -= k_frozen;
      centroids += k_frozen * d;

      /* Take care of void clusters */
      size_t nsplit = 0;
      RandomGenerator rng(1234);
      for (size_t ci = 0; ci < k; ci++)
      {
        if (hassign[ci] == 0)
        { /* need to redefine a centroid */
          size_t cj;
          for (cj = 0; 1; cj = (cj + 1) % k)
          {
            /* probability to pick this cluster for split */
            float p = (hassign[cj] - 1.0) / (float)(n - k);
            float r = rng.rand_float();
            if (r < p)
              break; /* found our cluster to be split */
          }
          memcpy(centroids + ci * d, centroids + cj * d, sizeof(*centroids) * d);

          /* small symmetric pertubation */
          for (size_t j = 0; j < d; j++)
            if (j % 2 == 0)
            {
              centroids[ci * d + j] *= 1 + EPS;
              centroids[cj * d + j] *= 1 - EPS;
            }
            else
            {
              centroids[ci * d + j] *= 1 - EPS;
              centroids[cj * d + j] *= 1 + EPS;
            }
          /* assume even split of the cluster */
          hassign[ci] = hassign[cj] / 2;
          hassign[cj] -= hassign[ci];
          nsplit++;
        }
      }

      return nsplit;
    }
  } // namespace

  void Clustering::train_encoded(int64_t nx, const uint8_t *x_in, const Index *codec, Index &index, const float *weights)
  {
    // printf("train_encode\n");
    VINDEX_THROW_IF_NOT_FMT(nx >= k,
                            "Number of training points (%" PRId64 ") should be at least as large as number of clusters (%zd)",
                            nx, k);

    VINDEX_THROW_IF_NOT_FMT(
        (!codec || codec->d == d),
        "Codec dimension %d not the same as data dimension %d", int(codec->d), int(d));

    VINDEX_THROW_IF_NOT_FMT(
        index.d == d,
        "Index dimension %d not the same as data dimension %d", int(index.d), int(d));
    // double t0=getmillisecs();
    if (!codec)
    {
      const float *x = reinterpret_cast<const float *>(x_in);
      for (size_t i = 0; i < nx * d; i++)
      {
        VINDEX_THROW_IF_NOT_MSG(std::isfinite(x[i]), "input contains NaN's or Inf's");
      }
    }
    const uint8_t *x = x_in;
    std::unique_ptr<uint8_t[]> del1;
    std::unique_ptr<float[]> del3;
    size_t line_size = codec ? codec->sa_code_size() : sizeof(float) * d;
    // printf("nx %ld, k %ld, max_points_per_centroid %d\n", nx, k, max_points_per_centroid);
    if (nx > k * max_points_per_centroid)
    {
      uint8_t *x_new;
      float *weights_new;
      nx = subsample_training_set(*this, nx, x, line_size, weights, &x_new, &weights_new);
      del1.reset(x_new);
      x = x_new;
      del3.reset(weights_new);
      weights = weights_new;
    }
    else if (nx < k * min_points_per_centroid)
    {
      fprintf(stderr, "WARNING clustering %" PRId64 " points to %zd centroids: "
                      "please provide at least %" PRId64 " training points\n",
              nx, k, int64_t(k) * min_points_per_centroid);
    }
    if (nx == k)
    {
      if (verbose)
        printf("Number of training points (%" PRId64 ") same as number of clusters, just copying\n", nx);
      centroids.resize(d * k);
      if (!codec)
        memcpy(centroids.data(), x_in, sizeof(float) * d * k);
      else
        codec->sa_decode(nx, x_in, centroids.data());
      // ClusteringIterationStats stats = {0.0, 0.0, 0.0, 1.0, 0};
      // iteration_stats.push_back(stats);
      index.reset();
      index.add(k, centroids.data());
      return;
    }
    if (verbose)
    {
      printf("Clustering %" PRId64 " points in %zdD to %zd clusters, redo %d times, %d iterations\n", nx, d, k, nredo, niter);
      if (codec)
        printf("Input data encoded in %zd bytes per vector\n", codec->sa_code_size());
    }
    // printf("train_encode2\n");
    std::unique_ptr<int64_t[]> assign(new int64_t[nx]);
    std::unique_ptr<float[]> dis(new float[nx]);

    // remember best iteration for redo
    bool lower_is_better = !is_similarity_metric(index.metric_type);
    float best_object = lower_is_better ? HUGE_VALF : -HUGE_VALF;
    // std::vector<ClusteringIterationStats> best_iteration_stats;
    std::vector<float> best_centroids;
    VINDEX_THROW_IF_NOT_MSG(centroids.size() % d == 0, "size of provided input centroids not a multiple of dimension");
    size_t n_input_centroids = centroids.size() / d;
    if (verbose && n_input_centroids > 0)
      printf("  Using %zd centroids provided as input (%sfrozen)\n", n_input_centroids, frozen_centroids ? "" : "not ");
    double t_search_tot = 0;
    // if (verbose)
    //   printf("  Preprocessing in %.2f s\n", (getmillisecs() - t0) / 1000.);
    // temporary buffer to decode vectors during the optimization
    std::vector<float> decode_buffer(codec ? d * decode_block_size : 0);
    // printf("train_encode3, timestamp1: %.4f\n",(getmillisecs()-t0)/1000);
    for (int redo = 0; redo < nredo; redo++)
    {
      // double t1=getmillisecs();
      if (verbose && nredo > 1)
        printf("Outer iteration %d / %d\n", redo, nredo);
      centroids.resize(d * k);
      std::vector<int> perm(nx);
      rand_perm(perm.data(), nx, seed + 1 + redo * 15486557L);
      if (!codec)
        for (int i = n_input_centroids; i < k; i++)
          memcpy(&centroids[i * d], x + perm[i] * line_size, line_size);
      else
        for (int i = n_input_centroids; i < k; i++)
          codec->sa_decode(1, x + perm[i] * line_size, &centroids[i * d]);
      post_process_centroids();
      if (index.ntotal != 0)
        index.reset();
      if (!index.is_trained)
        index.train(k, centroids.data());
      // printf("checkpoint: index.add(k, centroids.data());, is_trained: %d\n",index.is_trained);
      index.add(k, centroids.data());
      // double t2=getmillisecs();
      // printf("timestamp2: %.4f\n",(t2-t1)/1000);
      // k-means iterations
      float obj = 0;
      for (int i = 0; i < niter; i++)
      {
        // double t0s = getmillisecs();
        if (!codec)
          index.search(nx, reinterpret_cast<const float *>(x), 1, dis.get(), assign.get());
        else
        {
          size_t code_size = codec->sa_code_size();
          for (size_t i0 = 0; i0 < nx; i0 += decode_block_size)
          {
            size_t i1 = i0 + decode_block_size;
            if (i1 > nx)
              i1 = nx;
            codec->sa_decode(i1 - i0, x + code_size * i0, decode_buffer.data());
            index.search(i1 - i0, decode_buffer.data(), 1, dis.get() + i0, assign.get() + i0);
          }
        }
        // InterruptCallback::check();
        // t_search_tot += getmillisecs() - t0s;
        // accumulate objective
        obj = 0;
        for (int j = 0; j < nx; j++)
          obj += dis[j];
        std::vector<float> hassign(k);

        size_t k_frozen = frozen_centroids ? n_input_centroids : 0;
        compute_centroids(d, k, nx, k_frozen, x, codec, assign.get(), weights, hassign.data(), centroids.data());
        int nsplit = split_clusters(d, k, nx, k_frozen, hassign.data(), centroids.data());

        // ClusteringIterationStats stats = {obj, (getmillisecs() - t0) / 1000.0, t_search_tot / 1000, imbalance_factor(nx, k, assign.get()), nsplit};
        // iteration_stats.push_back(stats);
        // if (verbose)
        // {
        //   printf("  Iteration %d (%.2f s, search %.2f s): objective=%g imbalance=%.3f nsplit=%d       \r", i, stats.time, stats.time_search, stats.obj, stats.imbalance_factor, nsplit);
        //   fflush(stdout);
        // }

        post_process_centroids();
        // add centroids to index for the next iteration (or for output)
        index.reset();
        if (update_index)
          index.train(k, centroids.data());
        index.add(k, centroids.data());
        // InterruptCallback::check();
      }
      if (verbose)
        printf("\n");
      if (nredo > 1)
      {
        if ((lower_is_better && obj < best_object) ||
            (!lower_is_better && obj > best_object))
        {
          if (verbose)
          {
            printf("Objective improved: keep new clusters\n");
          }
          best_centroids = centroids;
          // best_iteration_stats = iteration_stats;
          best_object = obj;
        }
        index.reset();
      }
      // double t3=getmillisecs();
      // printf("timestamp3: %.4f\n",(t3-t2)/1000);
    }
    if (nredo > 1)
    {
      centroids = best_centroids;
      // iteration_stats = best_iteration_stats;
      index.reset();
      index.add(k, best_centroids.data());
    }
  }

  void Clustering::post_process_centroids()
  {
    if (spherical)
      fvec_renorm_L2(d, k, centroids.data());
    if (int_centroids)
      for (size_t i = 0; i < centroids.size(); i++)
        centroids[i] = roundf(centroids[i]);
  }
} // namespace vindex
