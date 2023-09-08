#pragma once

#include "all_type_variant.hpp"
#include "types.hpp"

namespace hyrise {

struct TableVectorIndexDefinition final {
  TableVectorIndexDefinition() = default;
  TableVectorIndexDefinition(const std::string& init_name, const int init_value);

  // bool operator==(const TableColumnDefinition& rhs) const;
  // size_t hash() const;

  std::string name;
  int value;
};

// So that google test, e.g., prints readable error messages
// inline std::ostream& operator<<(std::ostream& stream, const TableColumnDefinition& definition) {
//   stream << definition.name << " ";
//   stream << definition.data_type << " ";
//   stream << (definition.nullable ? "nullable" : "not nullable");
//   return stream;
// }

using TableVectorIndexDefinitions = std::vector<TableVectorIndexDefinition>;

}  // namespace hyrise
