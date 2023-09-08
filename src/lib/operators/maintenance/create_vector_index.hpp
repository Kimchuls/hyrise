#pragma once


#include "operators/abstract_read_only_operator.hpp"
// #include "operators/insert.hpp"
// #include "storage/table_column_definition.hpp"
#include "storage/table_vector_index_definition.hpp"

namespace hyrise {
struct TableVectorIndexDefinition;
using TableVectorIndexDefinitions = std::vector<TableVectorIndexDefinition>;

// maintenance operator for the "CREATE TABLE" sql statement
class CreateVectorIndex : public AbstractReadOnlyOperator {
 public:
  CreateVectorIndex(const std::string& init_table_name, const std::string& init_index_name, bool init_if_not_exists,
              const std::vector<std::string> init_column_names, const TableVectorIndexDefinitions init_vector_index_definitions);

  const std::string& name() const override;
  std::string description(DescriptionMode description_mode) const override;
  // const TableColumnDefinitions& column_definitions() const;

  const std::string table_name;
  const std::string index_name;
  const std::vector<std::string> column_names;
  const bool if_not_exists;
  const TableVectorIndexDefinitions vector_index_definitions;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const override;

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  // Commit happens in Insert operator
  // void _on_commit_records(const CommitID cid) override {}

  // Rollback happens in Insert operator
  // void _on_rollback_records() override {}

  // std::shared_ptr<Insert> _insert;
};
}  // namespace hyrise
