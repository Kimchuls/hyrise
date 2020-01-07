
#include <logical_query_plan/mock_node.hpp>
#include <synthetic_table_generator.hpp>
#include <cost_calibration/table_generator.hpp>
#include <cost_calibration/measurement_export.hpp>
#include <logical_query_plan/lqp_translator.hpp>
#include <scheduler/operator_task.hpp>
#include <cost_calibration/lqp_generator.hpp>
#include <fstream>
#include "types.hpp"
#include "hyrise.hpp"

using namespace opossum;  // NOLINT

// START FIX
// The following needs to stay here in order to be able to compile with GCC
// TODO find you actual problem and fix it
std::vector<DataType> data_types_collection;
std::vector<SegmentEncodingSpec> segment_encoding_spec_collection;
std::vector<ColumnDataDistribution> column_data_distribution_collection;
std::vector<int> chunk_offsets;
std::vector<int> row_counts;
std::vector<std::string> column_names;

std::vector<std::shared_ptr<opossum::Table>> generate() {
    auto tables = std::vector<std::shared_ptr<opossum::Table>>();
    auto table_generator = std::make_shared<SyntheticTableGenerator>();

    for (int chunk_size : chunk_offsets) {
        for (int row_count : row_counts){
            const auto table = table_generator->generate_table(
                    column_data_distribution_collection,
                    data_types_collection,
                    row_count,
                    chunk_size,
                    {segment_encoding_spec_collection},
                    {column_names},
                    UseMvcc::Yes    // MVCC = Multiversion concurrency control
            );
            tables.emplace_back(table);
        }
    }
    return tables;
}
// END FIX


int main() {
  auto table_config = std::make_shared<TableGeneratorConfig>(TableGeneratorConfig{
          {DataType::Double, DataType::Float, DataType::Int, DataType::Long, DataType::String, DataType::Null},
          {EncodingType::Dictionary, EncodingType::FixedStringDictionary, EncodingType ::FrameOfReference, EncodingType::LZ4, EncodingType::RunLength, EncodingType::Unencoded},
          {ColumnDataDistribution::make_uniform_config(0.0, 1000.0)},
          {1000},
          {100, 1000, 10000, 100000}
  });
  auto table_generator = TableGenerator(table_config);
  const auto tables = table_generator.generate();

  auto const path = ".";
  auto measurement_export = MeasurementExport(path);
  auto lqp_generator = LQPGenerator();

  for (const auto &table : tables){
    Hyrise::get().storage_manager.add_table(table->get_name(), table->get_table());

    const auto lqps = lqp_generator.generate(OperatorType::TableScan, table);

    //Execution of lpqs; In the future a good scheduler as replacement for following code would be awesome.
    for (const std::shared_ptr<AbstractLQPNode>& lqp : lqps) {
      const auto pqp = LQPTranslator{}.translate_node(lqp);
      const auto tasks = OperatorTask::make_tasks_from_operator(pqp, CleanupTemporaries::Yes);
      Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);

      //Execute LQP directly after generation
      measurement_export.export_to_csv(pqp);
    }
    Hyrise::get().storage_manager.drop_table(table->get_name());
  }

  // Export table information
  // Extract this in a new class
  std::fstream table_meta;
  table_meta.open("./table_meta.csv", std::ofstream::out | std::ofstream::app);
  table_meta << "TABLE_NAME,";
  table_meta << "ROW_COUNT,";
  table_meta << "CHUNK_SIZE\n";

  std::fstream column_meta;
  column_meta.open("./column_meta.csv", std::ofstream::out | std::ofstream::app);
  column_meta << "TABLE_NAME,";
  column_meta << "COLUMN_NAME,";
  column_meta << "COLUMN_DATA_TYPE\n";

  for (const auto &table : tables){
    std::cout << table_meta.is_open();
    table_meta << table->export_table_meta_data();
    column_meta << table->export_column_meta_data();
  }

  table_meta.close();
  column_meta.close();
}