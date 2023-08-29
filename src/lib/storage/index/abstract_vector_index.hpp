#pragma once

#include <memory>
#include <vector>

#include "all_type_variant.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "vector_index_type.hpp"

namespace hyrise {

using SimilarKPair = std::priority_queue<std::pair<float, size_t>>;

class AbstractVectorIndex : private Noncopyable {
 public:
  AbstractVectorIndex() = delete;
  explicit AbstractVectorIndex(const VectorIndexType type);
  AbstractVectorIndex(AbstractVectorIndex&&) = default;
  virtual ~AbstractVectorIndex() = default;

  virtual void similar_k(const float* query, int64_t* I, float* D, int k) = 0;
  virtual void range_similar_k(size_t n, const float* query, int64_t* I, float* D, int k) = 0;

  virtual bool is_index_for(const ColumnID) const = 0;
  virtual ColumnID get_indexed_column_id() const = 0;

  virtual void train(int64_t, const float*) = 0;
  virtual void train(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>&) = 0;

  VectorIndexType type() const {
    return _type;
  }

 private:
  const VectorIndexType _type;
};
}  // namespace hyrise
