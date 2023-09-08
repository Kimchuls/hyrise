#include "table_vector_index_definition.hpp"

namespace hyrise {

TableVectorIndexDefinition::TableVectorIndexDefinition(const std::string& init_name, const int init_value)
    : name(init_name), value(init_value) {}

// bool TableColumnDefinition::operator==(const TableColumnDefinition& rhs) const {
//   return name == rhs.name && data_type == rhs.data_type && nullable == rhs.nullable;
// }

// size_t TableColumnDefinition::hash() const {
//   auto hash = boost::hash_value(name);
//   boost::hash_combine(hash, data_type);
//   boost::hash_combine(hash, nullable);
//   return hash;
// }
}  // namespace hyrise
