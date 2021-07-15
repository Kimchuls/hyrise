
#include <memory>

#include "base_test.hpp"
#include "utils/assert.hpp"

#include "concurrency/transaction_context.hpp"
#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "hyrise.hpp"
#include "operators/get_table.hpp"
#include "operators/maintenance/create_index.hpp"
#include "operators/projection.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/validate.hpp"
#include "storage/table.hpp"
#include "tasks/chunk_compression_task.hpp"

namespace opossum {

using namespace opossum::expression_functional;  // NOLINT

class CreateIndexTest : public BaseTest {
 public:
  void SetUp() override {
    test_table = load_table("resources/test_data/tbl/string_int_index.tbl", 3);
    Hyrise::get().storage_manager.add_table(table_name, test_table);
    column_ids->emplace_back(test_table->column_id_by_name("b"));
    create_index = std::make_shared<CreateIndex>(index_name, true, table_name, column_ids);
  }

  std::shared_ptr<Table> test_table;
  std::shared_ptr<CreateIndex> create_index;
  std::string index_name = "TestIndex";
  std::shared_ptr<std::vector<ColumnID>> column_ids = std::make_shared<std::vector<ColumnID>>();
  SegmentIndexType index_type;
  std::string table_name = "TestTable";
};

TEST_F(CreateIndexTest, NameAndDescription) { EXPECT_EQ(create_index->name(), "CreateIndex"); }

TEST_F(CreateIndexTest, Execute) {
  ChunkEncoder::encode_all_chunks(test_table);

  const auto context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  create_index->set_transaction_context(context);

  create_index->execute();
  context->commit();

  auto actual_index = test_table->indexes_statistics().at(0);

  EXPECT_TRUE(actual_index.name == index_name);
  EXPECT_TRUE(actual_index.column_ids == *column_ids);
}

TEST_F(CreateIndexTest, TableExists) {
  create_index = std::make_shared<CreateIndex>(index_name, true, table_name, column_ids);

  const auto context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  create_index->set_transaction_context(context);

  EXPECT_THROW(create_index->execute(), std::logic_error);
  context->rollback(RollbackReason::Conflict);
}

TEST_F(CreateIndexTest, ExecuteWithIfNotExists) {
  ChunkEncoder::encode_all_chunks(test_table);

  const auto context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  create_index->set_transaction_context(context);

  create_index->execute();
  context->commit();

  auto another_index = std::make_shared<CreateIndex>(index_name, true, table_name, column_ids);
  const auto another_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);

  another_index->set_transaction_context(another_context);

  EXPECT_THROW(another_index->execute(), std::logic_error);
  another_context->rollback(RollbackReason::Conflict);
}

TEST_F(CreateIndexTest, ExecuteWithIfNotExistsWithoutName) {}
  // TODO implement test case
}  // namespace opossum
