#ifndef VINDEX_CLUSTERING_HPP
#define VINDEX_CLUSTERING_HPP
#include "Index.hpp"
#include <vector>
namespace vindex
{
  struct ClusteringParameters
  {
  public:
    int niter; ///< clustering iterations
    int nredo; ///< redo clustering this many times and keep best

    bool verbose;
    bool spherical;        ///< do we want normalized centroids?
    bool int_centroids;    ///< round centroids coordinates to integer
    bool update_index;     ///< re-train index after each iteration?
    bool frozen_centroids; ///< use the centroids provided as input and do not
                           ///< change them during iterations

    int min_points_per_centroid; ///< otherwise you get a warning
    int max_points_per_centroid; ///< to limit size of dataset

    int seed; ///< seed for the random number generator

    size_t decode_block_size; ///< how many vectors at a time to decode

    /// sets reasonable defaults
    ClusteringParameters();
  };

  struct ClusteringInterationStats
  {
  public:
  //   float object;            //< objective values (sum of distances reported by index)
  //   double time;             ///< seconds for iteration
  //   double time_search;      ///< seconds for just search
  //   double imbalance_factor; ///< imbalance factor of iteration
  //   int nsplit;              ///< number of cluster splits
  };

  struct Clustering : public ClusteringParameters
  {
  public:
    size_t d;                     ///< dimension of the vectors
    size_t k;                     ///< nb of centroids
    std::vector<float> centroids; ///(k*d), if centroids are set on input to train, they will be used as initialization
  //   // std::vector<ClusteringIterationStats> iteration_stats; /// stats at every iteration of clustering
    Clustering(int d, int k);
    Clustering(int d, int k, const ClusteringParameters &cp);
    virtual ~Clustering(){}

  //   /// @brief run k-means
  //   /// @param x vector
  //   /// @param index
  //   /// @param x_weight
    virtual void train(int64_t n, const float *x, Index &index, const float *x_weight = nullptr);
  //   /// win addition to train()'s parameters takes a codec as parameterto decode the input vectors.
  //   /// codec used to decode the vectors (nullptr = vectors are in fact floats)
    void train_encoded(int64_t nx, const uint8_t* x_in, const Index* codec, Index& index, const float* weights=nullptr);

  //   /// @brief Post-process the centroids after each centroid update. includes optional L2 normalization and nearest integer rounding
    void post_process_centroids();

  };
} // namespace vindex

#endif