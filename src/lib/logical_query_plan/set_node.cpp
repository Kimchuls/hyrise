#include "set_node.hpp"

#include <sstream>

namespace hyrise {

SetNode::SetNode(const std::string& init_table_name, const std::string& init_index_name,
                 const std::string& init_parameter_name, const int init_value)
    : AbstractNonQueryNode(LQPNodeType::SetVectorIndex),
      table_name(init_table_name),
      index_name(init_index_name),
      parameter_name(init_parameter_name),
      value(init_value) {}

std::string SetNode::description(const DescriptionMode /*mode*/) const {
  std::ostringstream stream;
  stream << "[Set] Name: '" << table_name << "', '" << index_name << "', '" << parameter_name << "' = " << value;
  return stream.str();
}

size_t SetNode::_on_shallow_hash() const {
  auto hash = boost::hash_value(table_name);
  boost::hash_combine(hash, index_name);
  boost::hash_combine(hash, parameter_name);
  boost::hash_combine(hash, value);
  return hash;
}

std::shared_ptr<AbstractLQPNode> SetNode::_on_shallow_copy(LQPNodeMapping& /*node_mapping*/) const {
  return SetNode::make(table_name, index_name, parameter_name, value);
}

bool SetNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& /*node_mapping*/) const {
  const auto& set_node = static_cast<const SetNode&>(rhs);
  return (table_name == set_node.table_name) && (index_name == set_node.index_name) &&
             (parameter_name == set_node.parameter_name) && (value == set_node.value);
}

}  // namespace hyrise
