#include "set_vector_index.hpp"

#include <assert.h>
#include <sstream>
#include <unordered_map>

#include "hyrise.hpp"
#include "types.hpp"
// #include "operators/insert.hpp"
// #include "storage/index/IVF_Flat/ivf_flat_index.hpp"
// #include "storage/index/hnsw/hnsw_index.hpp"
#include "storage/index/abstract_vector_index.hpp"
#include "storage/table.hpp"

// #include "utils/print_utils.hpp"

namespace hyrise {

SetVectorIndex::SetVectorIndex(const std::string& init_table_name, const std::string& init_index_name,
                               const std::string init_parameter_name, const int init_value)
    : AbstractReadOnlyOperator(OperatorType::SetVectorIndex),
      table_name(init_table_name),
      index_name(init_index_name),
      parameter_name(init_parameter_name),
      value(init_value) {
  // printf("enter SetVectorIndex::SetVectorIndex\n");
}

const std::string& SetVectorIndex::name() const {
  static const auto name = std::string{"SetVectorIndex"};
  return name;
}

std::string SetVectorIndex::description(DescriptionMode description_mode) const {
  std::ostringstream stream;
  const auto* const separator = description_mode == DescriptionMode::SingleLine ? ", " : "\n";
  stream << AbstractOperator::description(description_mode) << " '" << table_name << "' ";
  stream << index_name << "' (" << parameter_name << ": " << value << ")";
  return stream.str();
}

std::shared_ptr<const Table> SetVectorIndex::_on_execute() {
  printf("*************************reset parameter**************************\n");
  Assert(Hyrise::get().storage_manager.has_table(table_name),
         "Table \"" + table_name + "\" is not existed. Replacing it.\n");
  const auto& table = Hyrise::get().storage_manager.get_table(table_name);
  const auto float_array_indexs = table->get_table_indexes_vector();
  // std::cout<<"index_name: "<<index_name<<std::endl;
  for (auto& float_array_index : float_array_indexs) {
    std::cout<<"float_array_index->name(): "<<float_array_index->name()<<std::endl;
    if (float_array_index->name() == index_name) {
      if (index_name == "ivfflat") {
        if (parameter_name == "nprobe") {
          float_array_index->change_param(value);
          return nullptr;
        } else {
          Assert(false, "wrong parameter for index::ivfflat\n");
        }
      } else if (index_name == "hnsw") {
        if (parameter_name == "efs") {
          float_array_index->change_param(value);
          return nullptr;
        } else {
          Assert(false, "wrong parameter for index::hnsw\n");
        }
      } else {
        Assert(false, "wrong index name\n");
      }
      break;
    }
  }
  Assert(false, "no such index in the table " + table_name);
  return nullptr;
}

std::shared_ptr<AbstractOperator> SetVectorIndex::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& /*copied_left_input*/,
    const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  return std::make_shared<SetVectorIndex>(table_name, index_name, parameter_name, value);
}

void SetVectorIndex::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {
  // No parameters possible for CREATE TABLE
}

}  // namespace hyrise
