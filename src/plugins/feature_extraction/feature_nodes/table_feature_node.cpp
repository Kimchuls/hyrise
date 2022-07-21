#include "table_feature_node.hpp"

#include "feature_extraction/util/feature_extraction_utils.hpp"

namespace opossum {

TableFeatureNode::TableFeatureNode(const TableType table_type, const uint64_t row_count, const uint64_t chunk_count,
                                   const uint16_t column_count, const std::shared_ptr<AbstractFeatureNode>& input_node)
    : AbstractFeatureNode{FeatureNodeType::Table, input_node},
      _table_type{table_type},
      _row_count{row_count},
      _chunk_count{chunk_count},
      _column_count{column_count} {}

TableFeatureNode::TableFeatureNode(const TableType table_type, const uint64_t row_count, const uint64_t chunk_count,
                                   const uint16_t column_count, const std::shared_ptr<Table>& table,
                                   const std::string& table_name)
    : AbstractFeatureNode{FeatureNodeType::Table, nullptr},
      _table_type{table_type},
      _row_count{row_count},
      _chunk_count{chunk_count},
      _column_count{column_count},
      _table{table},
      _table_name{table_name} {
  _columns.resize(column_count);
}

std::shared_ptr<TableFeatureNode> TableFeatureNode::from_performance_data(
    const AbstractOperatorPerformanceData& performance_data, const std::shared_ptr<AbstractFeatureNode>& input_node) {
  return std::make_shared<TableFeatureNode>(TableType::References, performance_data.output_row_count,
                                            performance_data.output_chunk_count, performance_data.output_column_count,
                                            input_node);
}

std::shared_ptr<TableFeatureNode> TableFeatureNode::from_table(const std::shared_ptr<Table>& table,
                                                               const std::string& table_name) {
  return std::make_shared<TableFeatureNode>(TableType::Data, table->row_count(), table->chunk_count(),
                                            table->column_count(), table, table_name);
}

size_t TableFeatureNode::_on_shallow_hash() const {
  Assert(!_left_input && _table_type == TableType::Data, "_on_shallow_hash() called on output table");
  auto hash = _left_input ? size_t{0} : boost::hash_value(*_table_name);
  boost::hash_combine(hash, _table_type);
  return hash;
}

std::shared_ptr<FeatureVector> TableFeatureNode::_on_to_feature_vector() const {
  auto feature_vector = one_hot_encoding<TableType>(_table_type);
  feature_vector->reserve(feature_vector->size() + 3);
  feature_vector->emplace_back(static_cast<Feature>(_row_count));
  feature_vector->emplace_back(static_cast<Feature>(_chunk_count));
  feature_vector->emplace_back(static_cast<Feature>(_column_count));
  return feature_vector;
}

const std::vector<std::string>& TableFeatureNode::feature_headers() const {
  return headers();
}

const std::vector<std::string>& TableFeatureNode::headers() {
  static auto ohe_headers_type = one_hot_headers<TableType>("table_type.");
  static const auto headers = std::vector<std::string>{"row_count", "chunk_count", "column_count"};
  if (ohe_headers_type.size() == magic_enum::enum_count<TableType>()) {
    ohe_headers_type.insert(ohe_headers_type.end(), headers.begin(), headers.end());
  }
  return ohe_headers_type;
}

const std::optional<std::string>& TableFeatureNode::table_name() const {
  return _table_name;
}

bool TableFeatureNode::is_base_table() const {
  return _table_type == TableType::Data && !_left_input;
}

bool TableFeatureNode::registered_column(ColumnID column_id) const {
  Assert(_columns.size() > column_id, "Invalid column ID");
  return !_columns[column_id].expired();
}

std::shared_ptr<ColumnFeatureNode> TableFeatureNode::get_column(ColumnID column_id) const {
  Assert(_columns.size() > column_id, "Invalid column ID");
  Assert(!_columns[column_id].expired(), "Column was not set");
  return _columns[column_id].lock();
}

void TableFeatureNode::register_column(const std::shared_ptr<ColumnFeatureNode>& column) {
  const auto column_id = column->column_id();
  Assert(_columns.size() > column_id, "Invalid column ID");
  Assert(_columns[column_id].expired(), "Column was already set");
  _columns[column_id] = column;
}

std::shared_ptr<Table> TableFeatureNode::table() const {
  Assert(is_base_table(), "Only base tables link to the original table");
  return _table;
}

}  // namespace opossum
