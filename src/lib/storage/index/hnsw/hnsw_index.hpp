#pragma once

#include <tsl/sparse_map.h>
#include <tsl/sparse_set.h>
#include "hnswlib.h"
#include "storage/index/abstract_vector_index.hpp"
#include "types.hpp"
#define MAX_ELES_LONGLONG 1ll << 62
// #define MAX_ELES_UINT (int)((1ll << 31) - 1)
#define MAX_ELES_UINT (1ll << 25)

namespace hyrise {
using SimilarKPair = std::priority_queue<std::pair<float, size_t>>;

class HNSWIndex : public AbstractVectorIndex {
 public:
  HNSWIndex() = delete;

  HNSWIndex(const HNSWIndex&) = delete;
  HNSWIndex& operator=(const HNSWIndex&) = delete;
  // HNSWIndex(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index, ColumnID column_id, int dim,
  //           long long max_elements, int M, int ef_construction, int ef);
  HNSWIndex(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index, ColumnID column_id,
            int dim);
  HNSWIndex(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index, ColumnID column_id, int dim,
            int testing_data);
  HNSWIndex(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index, ColumnID column_id, int dim,
            int max_elements, int M, int ef_construction, int ef);

  void insert(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>&);
  size_t remove(const std::vector<ChunkID>&);
  void train(int64_t n, const float* data);
  void train(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>&);

  // SimilarKPair similar_k(const AllTypeVariant& query, int k);
  void similar_k(const float* query, int64_t* I, float* D, int k);
  void range_similar_k(size_t n, const float* queries, int64_t* I, float* D, int k);
  void save_index(const std::string& save_path);

  void change_param(const int param) {
    _alg_hnsw->setEf(param);
    std::cout<<"hnsw ef: "<<_alg_hnsw->ef_<<std::endl;
  }

  bool is_index_for(const ColumnID) const;

  // tsl::sparse_set<ChunkID> get_indexed_chunk_ids() const;

  ColumnID get_indexed_column_id() const;

  size_t estimate_memory_usage() const;

  int get_dim() const {
    return _dim;
  }

  int get_max_elements() const {
    return _max_elements;
  }

  int get_M() const {
    return _M;
  }

  int get_ef_construction() const {
    return _ef_construction;
  }

  hnswlib::HierarchicalNSW<float>* _alg_hnsw;

 protected:
  mutable std::shared_mutex _data_access_mutex;
  //TODO-kcj: this library only support elements less than 2^31
 private:
  ColumnID _column_id;
  int _dim;
  long long _max_elements;
  int _M;
  int _ef_construction;
  hnswlib::L2Space* _space;
  tsl::sparse_set<ChunkID> _indexed_chunk_ids = {};
};

}  // namespace hyrise
