#include "create_vector_index.hpp"

#include <assert.h>
#include <sstream>
#include <unordered_map>

#include "hyrise.hpp"
#include "types.hpp"
// #include "operators/insert.hpp"
#include "storage/index/IVF_Flat/ivf_flat_index.hpp"
#include "storage/index/hnsw/hnsw_index.hpp"
#include "storage/table.hpp"

// #include "utils/print_utils.hpp"

namespace hyrise {

CreateVectorIndex::CreateVectorIndex(const std::string& init_table_name, const std::string& init_index_name,
                                     bool init_if_not_exists, const std::vector<std::string> init_column_names,
                                     const TableVectorIndexDefinitions init_vector_index_definitions)
    : AbstractReadOnlyOperator(OperatorType::CreateVectorIndex),
      table_name(init_table_name),
      index_name(init_index_name),
      if_not_exists(init_if_not_exists),
      column_names(init_column_names),
      vector_index_definitions(init_vector_index_definitions) {
  // printf("enter CreateVectorIndex::CreateVectorIndex\n");
  }

const std::string& CreateVectorIndex::name() const {
  static const auto name = std::string{"CreateVectorIndex"};
  return name;
}

std::string CreateVectorIndex::description(DescriptionMode description_mode) const {
  std::ostringstream stream;
  const auto* const separator = description_mode == DescriptionMode::SingleLine ? ", " : "\n";
  stream << AbstractOperator::description(description_mode) << " '" << table_name << "'";
  for (auto iter = size_t{0}; iter < column_names.size(); ++iter) {
    stream << "'" << column_names[iter] << "', ";
    if (iter + 1u < column_names.size()) {
      stream << separator;
    }
  }
  stream << "/'" << index_name << "' (";
  for (auto iter = size_t{0}; iter < vector_index_definitions.size(); ++iter) {
    const auto& vector_index_definition = vector_index_definitions[iter];
    stream << "'" << vector_index_definition.name << "': " << vector_index_definition.value << " ";
    if (iter + 1u < vector_index_definitions.size()) {
      stream << separator;
    }
  }
  stream << ")";
  return stream.str();
}

std::shared_ptr<const Table> CreateVectorIndex::_on_execute() {
  // printf("enter CreateVectorIndex::_on_execute\n");
  Assert(Hyrise::get().storage_manager.has_table(table_name),
         "Table \"" + table_name + "\" is not existed. Replacing it.\n");
  const auto& table = Hyrise::get().storage_manager.get_table(table_name);
  const auto& column_name = column_names[0];
  const auto column_id = table->column_id_by_name(column_name);
  Assert(table->column_data_type(column_id) == DataType::Vector,
         "Table \"" + table_name + "\" column \"" + column_name + "\" is not existed. Replacing it.\n");
  std::cout << "- Creating table indexes" << std::endl;
  auto chunk_ids = std::vector<ChunkID>(table->chunk_count());
  std::iota(chunk_ids.begin(), chunk_ids.end(), ChunkID{0});
  int float_array_dim = (table->get_value<float_array>(column_name, 0)).value().size();
  for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
    const auto& chunk = table->get_chunk(chunk_id);
    Assert(chunk, "Requested index on deleted chunk.");
    Assert(!chunk->is_mutable(), "Cannot index mutable chunk.");
  }
  std::unordered_map<std::string, int> parameters;
  parameters["dim"] = float_array_dim;
  for (auto iter = size_t{0}; iter < vector_index_definitions.size(); ++iter) {
    const auto& vector_index_definition = vector_index_definitions[iter];
    parameters[vector_index_definition.name] = vector_index_definition.value;
  }
  // printf("checkpoint2.1.5\n");
  if (index_name == "hnsw") {
    table->create_float_array_index<HNSWIndex>(table->column_id_by_name(column_name), chunk_ids, parameters);
  } else if (index_name == "ivfflat") {
    table->create_float_array_index<IVFFlatIndex>(table->column_id_by_name(column_name), chunk_ids, parameters);
  } else {
    std::cout << "other index type is not supported." << std::endl;
  }
  // printf("exit CreateVectorIndex::_on_execute\n");
  // const auto float_array_index = table->get_table_indexes_vector(column_id)[0];
  // std::string index_save_path = index_type + "_" + table_name + ".bin";
  // float_array_index->save_index(index_save_path);
  return nullptr;
}

std::shared_ptr<AbstractOperator> CreateVectorIndex::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& /*copied_left_input*/,
    const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  return std::make_shared<CreateVectorIndex>(table_name, index_name, if_not_exists, column_names,
                                             vector_index_definitions);
}

void CreateVectorIndex::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {
  // No parameters possible for CREATE TABLE
}

}  // namespace hyrise
