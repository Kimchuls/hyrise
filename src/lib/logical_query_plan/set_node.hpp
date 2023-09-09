#pragma once

#include <string>

#include "abstract_non_query_node.hpp"
#include "enable_make_for_lqp_node.hpp"
// #include "import_export/file_type.hpp"
#include "storage/table_column_definition.hpp"

namespace hyrise {

/**
 * This node type represents the IMPORT / COPY FROM management command.
 */
class SetNode : public EnableMakeForLQPNode<SetNode>, public AbstractNonQueryNode {
 public:
  SetNode(const std::string& init_table_name, const std::string& init_index_name, const std::string& init_parameter_name, const int init_value);

  std::string description(const DescriptionMode mode = DescriptionMode::Short) const override;

  const std::string table_name;
  const std::string index_name;
  const std::string parameter_name;
  const int value;

 protected:
  size_t _on_shallow_hash() const override;
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& /*node_mapping*/) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& /*node_mapping*/) const override;
};

}  // namespace hyrise
