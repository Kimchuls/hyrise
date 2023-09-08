#pragma once

#include <string>

#include "abstract_non_query_node.hpp"
#include "enable_make_for_lqp_node.hpp"
#include "storage/table_vector_index_definition.hpp"

// #include "storage/table_column_definition.hpp"

namespace hyrise {

/**
 * This node type represents the CREATE TABLE management command.
 */
class CreateVectorIndexNode : public EnableMakeForLQPNode<CreateVectorIndexNode>, public AbstractNonQueryNode {
 public:
  CreateVectorIndexNode(const std::string& init_table_name, const std::string& init_index_name,
                        const bool init_if_not_exists, const std::vector<std::string> init_column_names,const TableVectorIndexDefinitions init_vector_index_definitions);

  std::string description(const DescriptionMode mode = DescriptionMode::Short) const override;

  const std::string table_name;
  const std::vector<std::string> column_names;
  const std::string index_name;
  const bool if_not_exists;
  const TableVectorIndexDefinitions vector_index_definitions;
 protected:
  size_t _on_shallow_hash() const override;
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& /*node_mapping*/) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& /*node_mapping*/) const override;
};

}  // namespace hyrise
