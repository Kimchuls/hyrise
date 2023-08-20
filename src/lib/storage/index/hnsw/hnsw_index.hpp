#pragma once

#include <tsl/sparse_map.h>
#include <tsl/sparse_set.h>
#include "hnswlib.h"
#include "types.hpp"
#define MAX_ELES_LONGLONG 1ll << 62
// #define MAX_ELES_UINT (int)((1ll << 31) - 1)
#define MAX_ELES_UINT (1ll << 23)

namespace hyrise {
using SimilarKPair = std::vector<std::pair<ChunkID, ChunkOffset>>;

class HNSWIndex {
 public:
  HNSWIndex() = delete;
  HNSWIndex(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index, ColumnID column_id, int dim,
            long long max_elements, int M, int ef_construction, int ef);
  HNSWIndex(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index, ColumnID column_id, int dim,
            int max_elements, int M, int ef_construction, int ef);

  size_t insert(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>&);
  size_t remove(const std::vector<ChunkID>&);

  // SimilarKPair similar_k(const AllTypeVariant& query, int k);
  SimilarKPair similar_k(float_array& query, int k);

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
