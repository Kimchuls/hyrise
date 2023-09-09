#pragma once


#include "operators/abstract_read_only_operator.hpp"
// #include "operators/insert.hpp"
// #include "storage/table_column_definition.hpp"
#include "storage/table_vector_index_definition.hpp"

namespace hyrise {

// maintenance operator for the "CREATE TABLE" sql statement
class SetVectorIndex : public AbstractReadOnlyOperator {
 public:
  SetVectorIndex(const std::string& init_table_name, const std::string& init_index_name,
              const std::string init_parameter_name, const int init_value);

  const std::string& name() const override;
  std::string description(DescriptionMode description_mode) const override;
  // const TableColumnDefinitions& column_definitions() const;

  const std::string table_name;
  const std::string index_name;
  const std::string parameter_name;
  const int value;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const override;

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

};
}  // namespace hyrise
