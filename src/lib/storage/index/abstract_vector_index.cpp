#include "abstract_vector_index.hpp"

#include <memory>
#include <vector>

#include "storage/index/hnsw/hnsw_index.hpp"

namespace hyrise
{
  AbstractVectorIndex::AbstractVectorIndex(const VectorIndexType type) : _type{type} {}
} // namespace hyrise
