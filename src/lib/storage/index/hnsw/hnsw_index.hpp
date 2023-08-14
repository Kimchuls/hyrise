#pragma once

#include "hnswlib.h"
#include "types.hpp"

namespace hyrise {

class HNSWIndex {
 public:
  HNSWIndex() = delete;
  HNSWIndex(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index, const ColumnID);

  size_t insert(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>&);
  size_t remove(const std::vector<ChunkID>&);

  bool is_index_for(const ColumnID column_id) const {
    return column_id == _column_id;
  }

  //   tsl::sparse_set<ChunkID> get_indexed_chunk_ids() const;

  ColumnID get_indexed_column_id() const;

  size_t estimate_memory_usage() const;

  int get_dim() const {
    return _dim;
  }

  long long int get_max_element() const {
    return _max_element;
  }

  int get_M() const {
    return _M;
  }

  int get_ef_construction() const {
    return _ef_construction;
  }

 protected:
  mutable std::shared_mutex _data_access_mutex;

 private:
  const ColumnID _column_id;
  int _dim;
  long long int _max_element;
  int _M;
  int _ef_construction;
  hnswlib::L2Space _space;
  hnswlib::HierarchicalNSW<float>* _alg_hnsw;
};

}  // namespace hyrise
