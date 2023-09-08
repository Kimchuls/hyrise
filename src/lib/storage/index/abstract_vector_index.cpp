#include "abstract_vector_index.hpp"

#include <memory>
#include <string>
#include <vector>

namespace hyrise {
AbstractVectorIndex::AbstractVectorIndex(const VectorIndexType type, const std::string name)
    : _type{type}, _name{name} {}
}  // namespace hyrise
