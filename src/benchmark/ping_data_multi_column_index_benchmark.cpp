#include <fstream>

#include "micro_benchmark_basic_fixture.hpp"

#include "benchmark_config.hpp"
#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "operators/index_scan.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/encoding_type.hpp"
#include "storage/index/group_key/composite_group_key_index.hpp"

#include "utils/assert.hpp"
#include "utils/load_table.hpp"
#include "operators/print.hpp"
#include "operators/sort.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace {
using namespace opossum;

///////////////////////////////
// benchmark seetings
///////////////////////////////

// input and output settings 
///////////////////////////////
constexpr auto INDEX_META_DATA_FILE = "../../out/400mio/index_meta_data_multi_index.csv";
constexpr auto TBL_FILE = "../../data/10mio_pings_no_id_int.tbl";

// table and compression settings
///////////////////////////////
constexpr auto TABLE_NAME_PREFIX = "ping";
const auto CHUNK_SIZE = size_t{40'000'000};
const auto SCAN_COLUMNS = std::vector{"captain_id", "latitude", "longitude", "timestamp", "captain_status"};
const auto ORDER_COLUMNS = std::vector{"captain_id", "latitude", "longitude", "timestamp", "captain_status", "unsorted"};

// 400 mio pings table

// quantile benchmark values (int table)
// timestamp values --> unix timestamp
// [0.0001, 0.01, 0.02, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.4, 0.5, 0.6, 0.7, 0.75, 0.8, 0.9, 1.0]
///////////////////////////////
const auto BM_VAL_CAPTAIN_ID = std::vector{4, 511, 1051, 2156, 5075, 11309, 26152, 51264, 71463, 153884, 261690, 444765, 681250, 830979, 951600, 1209929, 1419878};
const auto BM_VAL_CAPTAIN_STATUS = std::vector{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2};
const auto BM_VAL_LATITUDE = std::vector{243318475, 249128864, 249867308, 250278868, 250441577, 250552193, 250681596, 250763657, 250831026, 251008836, 251205836, 251695319, 251959544, 252079334, 252166598, 252442895, 601671321};
const auto BM_VAL_LONGITUDE = std::vector{543464250, 550118701, 551136315, 551335287, 551413500, 551467322, 551533330, 551663587, 551771444, 552061945, 552444439, 552686763, 552805030, 552905709, 553158162, 553615144, 2137369825};
const auto BM_VAL_TIMESTAMP = std::vector{1541029693, 1541117270, 1541190288, 1541473344, 1541871716, 1542301289, 1542701374, 1543132978, 1543613596, 1544417407, 1545152896, 1545879041, 1546772391, 1547148186, 1547525107, 1548196911, 1548975682};

const auto BM_SCAN_VALUES = BM_VAL_CAPTAIN_ID.size();

// quantile between benchmark values (int table)
// timestamp values --> unix timestamp
// [0.0001, 0.01, 0.02, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.4, 0.5, 0.6, 0.7, 0.75, 0.8, 0.9, 1.0]
///////////////////////////////
const std::vector<std::vector<int>> BM_BETWEEN_VAL_CAPTAIN_ID {{261690, 261690}, {257105, 265387}, {249105, 274829}, {232076, 313448}, {206962, 391070}, {172746, 413552}, {153884, 444765}, {135981, 491021}, {111718, 538905}, {71463, 681250}, {51264, 830979}, {26152, 951600}, {11309, 1157854}, {7739, 1188073}, {5075, 1209929}, {2156, 1267757}, {4, 1419878}};
const std::vector<std::vector<int>> BM_BETWEEN_VAL_CAPTAIN_STATUS {{1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 2}, {1, 2}, {1, 2}, {1, 2}, {1, 2}};
const std::vector<std::vector<int>> BM_BETWEEN_VAL_LATITUDE {{251205594, 251206088}, {251186996, 251229239}, {251170200, 251244361}, {251150524, 251307385}, {251111686, 251424324}, {251063392, 251567452}, {251008836, 251695319}, {250959444, 251817667}, {250916446, 251875782}, {250831026, 251959544}, {250763657, 252079334}, {250681596, 252166598}, {250552193, 252294631}, {250485765, 252349413}, {250441577, 252442895}, {250278868, 252584668}, {-348142571, 601671321}};
const std::vector<std::vector<int>> BM_BETWEEN_VAL_LONGITUDE {{552444244, 552444647}, {552421152, 552466309}, {552403773, 552484039}, {552334887, 552512683}, {552219192, 552585334}, {552133587, 552638424}, {552061945, 552686763}, {552006222, 552727166}, {551944417, 552758956}, {551771444, 552805030}, {551663587, 552905709}, {551533330, 553158162}, {551467322, 553438739}, {551440083, 553526687}, {551413500, 553615144}, {551335287, 553972207}, {-2144769047, 2137369825}};
const std::vector<std::vector<int>> BM_BETWEEN_VAL_TIMESTAMP {{1545152588, 1545153213}, {1545125220, 1545195923}, {1545101636, 1545220279}, {1544986222, 1545315256}, {1544806072, 1545537953}, {1544611869, 1545697161}, {1544417407, 1545879041}, {1544199073, 1546078740}, {1544001704, 1546335968}, {1543613596, 1546772391}, {1543132978, 1547148186}, {1542701374, 1547525107}, {1542301289, 1547873717}, {1542094512, 1548046293}, {1541871716, 1548196911}, {1541473344, 1548555299}, {1541026811, 1548975682}};

//const std::vector<std::vector<int>> MULTI_COLUMN_INDEXES {{0, 4}, {1, 2}, {1, 2, 3}, {3, 1, 2}, {2, 1, 3}, {1, 2, 3, 4}, {1, 2, 0}};
const std::vector<std::vector<int>> MULTI_COLUMN_INDEXES {{0, 4}, {1, 2}};
std::map<std::vector<int> ,std::shared_ptr<AbstractIndex>> multi_indexes; 
//const std::vector<int> SCAN_VALUES = {4, 1, 1, 1, 1};
const auto INDEX_VALUES = MULTI_COLUMN_INDEXES.size();
const auto INDEX_LENGTH = 2;

///////////////////////////////
// methods
///////////////////////////////

Segments get_segments_of_chunk(const std::shared_ptr<const Table>& input_table, ChunkID chunk_id){
  Segments segments{};
  for (auto column_id = ColumnID{0}; column_id < input_table->column_count(); ++column_id) {
    segments.emplace_back(input_table->get_chunk(chunk_id)->get_segment(column_id));
  }
  return segments;
} 

std::shared_ptr<Table> sort_table_chunk_wise(const std::shared_ptr<const Table>& input_table,
    const std::string order_by_column_name, const size_t chunk_size, const std::optional<ChunkEncodingSpec>& chunk_encoding_spec = std::nullopt,
    const SortMode sort_mode = SortMode::Ascending) {
  // empty table to which we iteratively add the sorted chunks
  auto sorted_table = std::make_shared<Table>(input_table->column_definitions(), TableType::Data, chunk_size, UseMvcc::No);

  const auto chunk_count = input_table->chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    // create new single chunk and create a new table with that chunk
    auto new_chunk = std::make_shared<Chunk>(get_segments_of_chunk(input_table, chunk_id));
    std::vector<std::shared_ptr<Chunk>> single_chunk_to_sort_as_vector = {new_chunk};
    auto single_chunk_table = std::make_shared<Table>(input_table->column_definitions(), TableType::Data, std::move(single_chunk_to_sort_as_vector), UseMvcc::No);

    // call sort operator on single-chunk table
    auto table_wrapper = std::make_shared<TableWrapper>(single_chunk_table);
    table_wrapper->execute();

    auto sort = std::make_shared<Sort>(
      table_wrapper, std::vector<SortColumnDefinition>{
        SortColumnDefinition{single_chunk_table->column_id_by_name(order_by_column_name), sort_mode}},
      chunk_size, Sort::ForceMaterialization::Yes);
    sort->execute();
    const auto immutable_sorted_table = sort->get_output();

    // add sorted chunk to output table
    // Note: we do not care about MVCC at all at the moment
    sorted_table->append_chunk(get_segments_of_chunk(immutable_sorted_table, ChunkID{0}));
  
    const auto& added_chunk = sorted_table->get_chunk(chunk_id);
    added_chunk->finalize();
    added_chunk->set_individually_sorted_by(SortColumnDefinition(sorted_table->column_id_by_name(order_by_column_name), sort_mode));

    // in case a chunk encoding spec is provided, encode chunk
    if (chunk_encoding_spec) {
      ChunkEncoder::encode_chunk(added_chunk, immutable_sorted_table->column_data_types(), *chunk_encoding_spec);
    }
  }

  return sorted_table;
}

std::string get_table_name(const std::string table_name, const std::string order_by_column , const std::string encoding) {
  return table_name + "_orderby_" + order_by_column + "_encoding_" + encoding;
} 

}  // namespace

///////////////////////////////
// Fixtures
///////////////////////////////

namespace opossum {

class TableWrapper;

// Defining the base fixture class
class PingDataMultiIndexBenchmarkFixture : public MicroBenchmarkBasicFixture {
 public:
  void SetUp(::benchmark::State& state) override {
    auto& storage_manager = Hyrise::get().storage_manager;

    // Generate tables
    if (!_data_generated) {

      // file for table stats

      std::ofstream index_meta_data_csv_file(INDEX_META_DATA_FILE);
      index_meta_data_csv_file << "TABLE_NAME,COLUMN_ID,SECOND_COLUMN_ID,ORDER_BY,ENCODING,CHUNK_ID,ROW_COUNT,SIZE_IN_BYTES\n"; 
      
      // Sort table and add sorted tables to the storage manager
      // Load origninal table from tbl file with specified chunk size
      std::cout << "Load initial table form tbl file '" << TBL_FILE << "' with chunk size: " << CHUNK_SIZE << "." << std::endl;
      auto loaded_table = load_table(TBL_FILE, CHUNK_SIZE);

      const auto encoding = SegmentEncodingSpec{EncodingType::Dictionary};
      const auto encoding_name= encoding_type_to_string.left.at(encoding.encoding_type);

      for (const auto order_by_column : ORDER_COLUMNS) {

        const auto new_table_name = get_table_name(TABLE_NAME_PREFIX, order_by_column, encoding_name);
        
        auto table_wrapper = std::make_shared<TableWrapper>(loaded_table);
        table_wrapper->execute();
        const auto chunk_encoding_spec = ChunkEncodingSpec(table_wrapper->get_output()->column_count(), encoding);

        auto new_table = loaded_table;

        if (strcmp(order_by_column, "unsorted") == 0) {
          new_table = load_table(TBL_FILE, CHUNK_SIZE);
          ChunkEncoder::encode_all_chunks(new_table, chunk_encoding_spec);
        } else {
          new_table = sort_table_chunk_wise(loaded_table, order_by_column, CHUNK_SIZE, chunk_encoding_spec);
        }

        storage_manager.add_table(new_table_name, new_table);
        std::cout << "Created table: " << new_table_name << std::endl;

        // create index for each chunk and each segment 
        if (encoding.encoding_type == EncodingType::Dictionary) {
          for (size_t index_config_id = 0; index_config_id < INDEX_VALUES; ++index_config_id) {

            std::cout << "Creating indexes: ";

            const auto chunk_count = new_table->chunk_count();
            auto column_ids = std::vector<ColumnID>{};

            for (const auto& index_column : MULTI_COLUMN_INDEXES[index_config_id]) {
              column_ids.emplace_back(index_column);
            }
              
            for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
              const auto& index = new_table->get_chunk(chunk_id)->create_index<CompositeGroupKeyIndex>(column_ids);
              multi_indexes.insert({MULTI_COLUMN_INDEXES[index_config_id], index});
              index_meta_data_csv_file << new_table_name << "," << new_table->column_name(column_ids[0]) << "," << new_table->column_name(column_ids[1]) << "," << order_by_column << ","<< encoding << ","<< chunk_id << "," << CHUNK_SIZE << "," << index->memory_consumption() << "\n";
            }
            std::cout << "done " << std::endl;
          }
        }
      }

      index_meta_data_csv_file.close();
    }

    _data_generated = true;
  }

  // Required to avoid resetting of StorageManager in MicroBenchmarkBasicFixture::TearDown()
  void TearDown(::benchmark::State&) override {}

  inline static bool _data_generated = false;

};

///////////////////////////////
// benchmarks
///////////////////////////////

BENCHMARK_DEFINE_F(PingDataMultiIndexBenchmarkFixture, BM_MultiColumnIndexScan)(benchmark::State& state) {
  auto& storage_manager = Hyrise::get().storage_manager;

  const auto order_by_column = ORDER_COLUMNS[state.range(0)];
  const auto index_config = MULTI_COLUMN_INDEXES[state.range(1)];
  const auto search_value_index = state.range(2);
  const auto scan_op = state.range(3);

  const auto encoding = SegmentEncodingSpec{EncodingType::Dictionary};
  const auto encoding_type = encoding_type_to_string.left.at(encoding.encoding_type);
  const auto table_name = get_table_name(TABLE_NAME_PREFIX, order_by_column, encoding_type);
  
  auto table = storage_manager.get_table(table_name);
  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  auto scan_column_ids = std::vector<ColumnID>{};
  if (scan_op == 0) {
    scan_column_ids.emplace_back(index_config[0]);
  } else {
    for (const auto& index_column : index_config) {
      scan_column_ids.emplace_back(index_column);
    }
  }

  // setting up right value (i.e., the search value)
  std::vector<AllTypeVariant> right_values;
  for (const auto& scan_column_index : scan_column_ids) {
    if (scan_column_index == 0) { right_values.emplace_back(BM_VAL_CAPTAIN_ID[search_value_index]); }
    if (scan_column_index == 1) { right_values.emplace_back(BM_VAL_LATITUDE[search_value_index]); }
    if (scan_column_index == 2) { right_values.emplace_back(BM_VAL_LONGITUDE[search_value_index]); }
    if (scan_column_index == 3) { right_values.emplace_back(BM_VAL_TIMESTAMP[search_value_index]); }
    if (scan_column_index == 4) { right_values.emplace_back(BM_VAL_CAPTAIN_STATUS[search_value_index]); }
  }

  std::vector<ChunkID> indexed_chunks;
  for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
    indexed_chunks.emplace_back(chunk_id);
  }

  for (auto _ : state) {
    const auto index_scan = std::make_shared<IndexScan>(table_wrapper, SegmentIndexType::CompositeGroupKey, scan_column_ids, PredicateCondition::LessThanEquals, right_values);
    index_scan->included_chunk_ids = indexed_chunks;
    index_scan->execute();
  }
}

BENCHMARK_DEFINE_F(PingDataMultiIndexBenchmarkFixture, BM_MultiColumnIndexScan_04)(benchmark::State& state) {
  std::vector<std::vector<std::tuple<ColumnID, int32_t, int32_t>>> workload{
                                                                            {{ColumnID{0}, 0, 4},
                                                                             {ColumnID{4}, 0, 1}
                                                                            },
                                                                            {{ColumnID{1}, 251111686, 251424324},
                                                                             {ColumnID{2}, 552219192, 552585334},
                                                                             {ColumnID{3}, 1544417407, 1545879041},
                                                                             {ColumnID{4}, 0, 1}
                                                                            },
                                                                            {{ColumnID{0}, 0, 511},
                                                                             {ColumnID{1}, 250916446, 251875782},
                                                                             {ColumnID{2}, 551944417, 552758956}
                                                                            }};

  for (const auto& multi_column_def : MULTI_COLUMN_INDEXES) {
    std::cout << "Multi column index: ";
    for (const auto column_id : multi_column_def) {
      std::cout << column_id << " ";
    }
    std::cout << std::endl;

    const auto& index = multi_indexes[multi_column_def];
    std::cout << "Memory consumption is: " << index->memory_consumption() << std::endl;

    for (const auto& query : workload) {
      std::cout << "############\n############ Query: ";
      for (const auto& scan : query) std::cout << std::get<0>(scan) << " ";
      std::cout << "\n############" << std::endl;

      std::vector<AllTypeVariant> lower_bounds;
      std::vector<AllTypeVariant> upper_bounds;

      // Starting from the index' first column, check how many scans we can cover.
      auto found_match = false;
      for (const auto column_id : multi_column_def) {
        // For every scan column, check if it is the currently checked index column. If so, append search values and continue
        for (const auto& scan : query) {
          const auto scan_column_id = std::get<0>(scan);
          if (column_id == scan_column_id) {
            const auto lower_bound = std::get<1>(scan);
            const auto upper_bound = std::get<2>(scan);

            found_match = true;
            lower_bounds.push_back(lower_bound);
            upper_bounds.push_back(upper_bound);
          }
        }

        if (!found_match) {
          // If the currently scan column is not part of the index, neglect all other scans
          // (composite index has to be used from start without "holes"). 
          break;
        }
      }

      std::cout << "For the current query, we found the following search values:\n\tlower: ";
      for (const auto lower : lower_bounds) std::cout << lower << " ";
      std::cout << "\n\tupper: ";
      for (const auto upper : upper_bounds) std::cout << upper << " ";
      std::cout << std::endl;
      
      Assert(lower_bounds.size() == upper_bounds.size(), "Narf");
      if (!lower_bounds.empty()) {
        auto range_begin = index->lower_bound(lower_bounds);
        auto range_end = index->upper_bound(upper_bounds);
   
        // TODO: for fair evaluation, push_back to std::vector<RowID> ...
        //for (; range_begin < range_end; ++range_begin) {
         // std::cout << *range_begin << std::endl;
        //}
        std::cout << "Found " << std::distance(range_begin, range_end) << " matches." << std::endl;
      }
    }
  }

}

static void MultiIndexCustomArguments(benchmark::internal::Benchmark* b) {
  for (size_t order_by_column_id = 0; order_by_column_id < ORDER_COLUMNS.size(); ++order_by_column_id) {
    for (size_t scan_value_id = 0; scan_value_id < BM_SCAN_VALUES; ++scan_value_id) {
      for (size_t scan_op = 0; scan_op < INDEX_LENGTH; ++scan_op) {
        b->Args({static_cast<long long>(order_by_column_id), static_cast<long long>(scan_value_id), static_cast<long long>(scan_op)});
      }
    }
  }
}

BENCHMARK_REGISTER_F(PingDataMultiIndexBenchmarkFixture, BM_MultiColumnIndexScan_04)->Apply(MultiIndexCustomArguments);
//BENCHMARK_REGISTER_F(PingDataMultiIndexBenchmarkFixture, BM_MultiColumnIndexScan)->Apply(MultiIndexCustomArguments);

}  // namespace opossum
