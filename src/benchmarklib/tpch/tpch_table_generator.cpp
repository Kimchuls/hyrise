#include "tpch_table_generator.hpp"

extern "C" {
#include <dss.h>
#include <dsstypes.h>
#include <rnd.h>
}

#include <filesystem>
#include <utility>

#include "benchmark_config.hpp"
#include "benchmark_table_encoder.hpp"
#include "hyrise.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "storage/chunk.hpp"
#include "storage/table_key_constraint.hpp"
#include "table_builder.hpp"
#include "utils/timer.hpp"

extern char** asc_date;
extern seed_t seed[];  // NOLINT

#pragma clang diagnostic ignored "-Wshorten-64-to-32"
#pragma clang diagnostic ignored "-Wfloat-conversion"

namespace {

using namespace opossum;  // NOLINT

// clang-format off
const auto customer_column_types = boost::hana::tuple      <int32_t,    pmr_string,  pmr_string,  int32_t,       pmr_string,  float,       pmr_string,     pmr_string>();  // NOLINT
const auto customer_column_names = boost::hana::make_tuple("c_custkey", "c_name",    "c_address", "c_nationkey", "c_phone",   "c_acctbal", "c_mktsegment", "c_comment"); // NOLINT

const auto order_column_types = boost::hana::tuple      <int32_t,     int32_t,     pmr_string,      float,          pmr_string,    pmr_string,        pmr_string,  int32_t,          pmr_string>();  // NOLINT
const auto order_column_names = boost::hana::make_tuple("o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk",   "o_shippriority", "o_comment");  // NOLINT

const auto lineitem_column_types = boost::hana::tuple      <int32_t,     int32_t,     int32_t,     int32_t,        float,        float,             float,        float,   pmr_string,     pmr_string,     pmr_string,   pmr_string,     pmr_string,      pmr_string,       pmr_string,   pmr_string>();  // NOLINT
const auto lineitem_column_names = boost::hana::make_tuple("l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment");  // NOLINT

const auto part_column_types = boost::hana::tuple      <int32_t,    pmr_string,  pmr_string,  pmr_string,  pmr_string,  int32_t,  pmr_string,    float,        pmr_string>();  // NOLINT
const auto part_column_names = boost::hana::make_tuple("p_partkey", "p_name",    "p_mfgr",    "p_brand",   "p_type",    "p_size", "p_container", "p_retailsize", "p_comment");  // NOLINT

const auto partsupp_column_types = boost::hana::tuple<     int32_t,      int32_t,      int32_t,       float,           pmr_string>();  // NOLINT
const auto partsupp_column_names = boost::hana::make_tuple("ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment");  // NOLINT

const auto supplier_column_types = boost::hana::tuple<     int32_t,     pmr_string,  pmr_string,  int32_t,       pmr_string,  float,       pmr_string>();  // NOLINT
const auto supplier_column_names = boost::hana::make_tuple("s_suppkey", "s_name",    "s_address", "s_nationkey", "s_phone",   "s_acctbal", "s_comment");  // NOLINT

const auto nation_column_types = boost::hana::tuple<     int32_t,       pmr_string,  int32_t,       pmr_string>();  // NOLINT
const auto nation_column_names = boost::hana::make_tuple("n_nationkey", "n_name",    "n_regionkey", "n_comment");  // NOLINT

const auto region_column_types = boost::hana::tuple<     int32_t,       pmr_string,  pmr_string>();  // NOLINT
const auto region_column_names = boost::hana::make_tuple("r_regionkey", "r_name",    "r_comment");  // NOLINT
// clang-format on

std::unordered_map<opossum::TPCHTable, std::underlying_type_t<opossum::TPCHTable>> tpch_table_to_dbgen_id = {
    {opossum::TPCHTable::Part, PART},     {opossum::TPCHTable::PartSupp, PSUPP}, {opossum::TPCHTable::Supplier, SUPP},
    {opossum::TPCHTable::Customer, CUST}, {opossum::TPCHTable::Orders, ORDER},   {opossum::TPCHTable::LineItem, LINE},
    {opossum::TPCHTable::Nation, NATION}, {opossum::TPCHTable::Region, REGION}};

template <typename DSSType, typename MKRetType, typename... Args>
DSSType call_dbgen_mk(size_t idx, MKRetType (*mk_fn)(DSS_HUGE, DSSType* val, Args...), opossum::TPCHTable table,
                      Args... args) {
  /**
   * Preserve calling scheme (row_start(); mk...(); row_stop(); as in dbgen's gen_tbl())
   */

  const auto dbgen_table_id = tpch_table_to_dbgen_id.at(table);

  row_start(dbgen_table_id);

  DSSType value{};
  mk_fn(idx, &value, std::forward<Args>(args)...);

  row_stop(dbgen_table_id);

  return value;
}

float convert_money(DSS_HUGE cents) {
  const auto dollars = cents / 100;
  cents %= 100;
  return static_cast<float>(dollars) + (static_cast<float>(cents)) / 100.0f;
}

/**
 * Call this after using dbgen to avoid memory leaks
 */
void dbgen_cleanup() {
  for (auto* distribution : {&nations,     &regions,        &o_priority_set, &l_instruct_set,
                             &l_smode_set, &l_category_set, &l_rflag_set,    &c_mseg_set,
                             &colors,      &p_types_set,    &p_cntr_set,     &articles,
                             &nouns,       &adjectives,     &adverbs,        &prepositions,
                             &verbs,       &terminators,    &auxillaries,    &np,
                             &vp,          &grammar}) {
    free(distribution->permute);  // NOLINT
    distribution->permute = nullptr;
  }

  if (asc_date) {
    for (size_t idx = 0; idx < TOTDATE; ++idx) {
      free(asc_date[idx]);  // NOLINT
    }
    free(asc_date);  // NOLINT
  }
  asc_date = nullptr;
}

void encode_tables(const std::shared_ptr<BenchmarkConfig>& benchmark_config,
                   std::vector<std::pair<std::string, std::shared_ptr<Table>&>>& names_and_tables,
		   std::atomic<bool>& table_generation_finished) {
  auto last_encoded_chunk_id_per_table_map = std::map<std::string, int32_t>{};
  auto encoding_job_list = std::list<std::vector<std::shared_ptr<opossum::AbstractTask>>>{};

  auto is_last_run = false;
  auto runs_without_encoding = size_t{0};
  while (!is_last_run) {
    is_last_run = table_generation_finished;

    auto reencoding_took_place = std::atomic<bool>{false};
    auto encoder_was_called = false;
    encoding_job_list.emplace_back();
    for (const auto& [table_name, table] : names_and_tables) {
      if (!last_encoded_chunk_id_per_table_map.contains(table_name)) {
        last_encoded_chunk_id_per_table_map[table_name] = -1;
      }

      for (auto chunk_id = opossum::ChunkID{static_cast<uint32_t>(last_encoded_chunk_id_per_table_map[table_name] + 1)}; chunk_id < table->chunk_count();
           ++chunk_id) {
        const auto chunk = table->get_chunk(chunk_id);
        if (!chunk) {
          break;
        }

        if (chunk->is_mutable()) {
          chunk->finalize();
        }

        last_encoded_chunk_id_per_table_map[table_name] = chunk_id;
	encoding_job_list.back().emplace_back(std::make_shared<JobTask>([&, chunk_id, table_name=table_name, table=table]() {
          reencoding_took_place = BenchmarkTableEncoder::encode(benchmark_config->encoding_config, table_name, table, chunk_id);
        }));
        encoder_was_called = true;
      }
    }

    // Start jobs, but don't wait for now. We'll wait later.
    Hyrise::get().scheduler()->schedule_tasks(encoding_job_list.back());

    runs_without_encoding = static_cast<size_t>(!encoder_was_called) * (runs_without_encoding + static_cast<size_t>(!encoder_was_called));
    std::this_thread::sleep_for(std::chrono::milliseconds(10 * runs_without_encoding));
  }

  std::cout << "Waiting for all to finish ..." << std::endl;
  for (const auto& encoding_jobs : encoding_job_list) {
    Hyrise::get().scheduler()->wait_for_tasks(encoding_jobs);
  }
}

}  // namespace

namespace opossum {

std::unordered_map<TPCHTable, std::string> tpch_table_names = {
    {TPCHTable::Part, "part"},         {TPCHTable::PartSupp, "partsupp"}, {TPCHTable::Supplier, "supplier"},
    {TPCHTable::Customer, "customer"}, {TPCHTable::Orders, "orders"},     {TPCHTable::LineItem, "lineitem"},
    {TPCHTable::Nation, "nation"},     {TPCHTable::Region, "region"}};

TPCHTableGenerator::TPCHTableGenerator(float scale_factor, ClusteringConfiguration clustering_configuration,
                                       ChunkOffset chunk_size)
    : TPCHTableGenerator(scale_factor, clustering_configuration, create_benchmark_config_with_chunk_size(chunk_size)) {}

TPCHTableGenerator::TPCHTableGenerator(float scale_factor, ClusteringConfiguration clustering_configuration,
                                       const std::shared_ptr<BenchmarkConfig>& benchmark_config)
    : AbstractTableGenerator(benchmark_config),
      _scale_factor(scale_factor),
      _clustering_configuration(clustering_configuration) {}

std::unordered_map<std::string, BenchmarkTableInfo> TPCHTableGenerator::generate() {
  Assert(_scale_factor < 1.0f || std::round(_scale_factor) == _scale_factor,
         "Due to tpch_dbgen limitations, only scale factors less than one can have a fractional part.");

  const auto cache_directory = std::string{"tpch_cached_tables/sf-"} + std::to_string(_scale_factor);  // NOLINT
  if (_benchmark_config->cache_binary_tables && std::filesystem::is_directory(cache_directory)) {
    return _load_binary_tables_from_path(cache_directory);
  }

  // Init tpch_dbgen - it is important this is done before any data structures from tpch_dbgen are read.
  dbgen_reset_seeds();
  dbgen_init_scale_factor(_scale_factor);

  const auto order_count = static_cast<ChunkOffset>(tdefs[ORDER].base * scale);
  const auto customer_count = static_cast<ChunkOffset>(tdefs[CUST].base * scale);
  const auto part_count = static_cast<ChunkOffset>(tdefs[PART].base * scale);
  const auto supplier_count = static_cast<ChunkOffset>(tdefs[SUPP].base * scale);
  const auto nation_count = static_cast<ChunkOffset>(tdefs[NATION].base);
  const auto region_count = static_cast<ChunkOffset>(tdefs[REGION].base);

  // The `* 4` part is defined in the TPC-H specification.
  TableBuilder order_builder{_benchmark_config->chunk_size, order_column_types, order_column_names, order_count};
  TableBuilder lineitem_builder{_benchmark_config->chunk_size, lineitem_column_types, lineitem_column_names,
                                ChunkOffset{order_count * 4}};
  TableBuilder customer_builder{_benchmark_config->chunk_size, customer_column_types, customer_column_names,
                                customer_count};
  TableBuilder part_builder{_benchmark_config->chunk_size, part_column_types, part_column_names, part_count};
  TableBuilder partsupp_builder{_benchmark_config->chunk_size, partsupp_column_types, partsupp_column_names,
                                ChunkOffset{part_count * 4}};
  TableBuilder supplier_builder{_benchmark_config->chunk_size, supplier_column_types, supplier_column_names,
                                supplier_count};
  TableBuilder nation_builder{_benchmark_config->chunk_size, nation_column_types, nation_column_names, nation_count};
  TableBuilder region_builder{_benchmark_config->chunk_size, region_column_types, region_column_names, region_count};

  // TODO(anyone): put all builders in a map above and use that one all over the place here.
  auto table_builders = std::vector<std::pair<std::string, std::shared_ptr<Table>&>>{};
  table_builders.reserve(8);

  table_builders.emplace_back("orders", order_builder._table);
  table_builders.emplace_back("lineitem", lineitem_builder._table);

  auto data_generation_done = std::atomic<bool>{false};
  auto encoder_thread = std::thread{[&] {
    encode_tables(_benchmark_config, table_builders, data_generation_done);
  }};

  auto& storage_manager = Hyrise::get().storage_manager;
  auto table_info_by_name = std::unordered_map<std::string, BenchmarkTableInfo>{};

  auto print_generation_duration_and_add_table = [&] (Timer generation_timer, const std::string& table_name,
                                                      std::shared_ptr<Table>& table) {
    auto generation_output = std::string{"- Generated table '"} + table_name + "' (" + generation_timer.lap_formatted() + ")\n";
    std::cout << generation_output << std::flush;
    if (storage_manager.has_table(table_name)) storage_manager.drop_table(table_name);
    Timer adding_timer;
    storage_manager.add_table(table_name, table);
    const auto adding_output = std::string{"-  Added '"} + table_name + "' (" + adding_timer.lap_formatted() + ")\n";
    std::cout << adding_output << std::flush;
  };

  /**
   * ORDER and LINEITEM
   */

  Timer orders_table_timer;
  for (size_t order_idx = 0; order_idx < order_count; ++order_idx) {
    const auto order = call_dbgen_mk<order_t>(order_idx + 1, mk_order, TPCHTable::Orders, 0l);

    order_builder.append_row(order.okey, order.custkey, pmr_string(1, order.orderstatus),
                             convert_money(order.totalprice), order.odate, order.opriority, order.clerk,
                             order.spriority, order.comment);

    for (auto line_idx = 0; line_idx < order.lines; ++line_idx) {
      const auto& lineitem = order.l[line_idx];

      lineitem_builder.append_row(lineitem.okey, lineitem.partkey, lineitem.suppkey, lineitem.lcnt, lineitem.quantity,
                                  convert_money(lineitem.eprice), convert_money(lineitem.discount),
                                  convert_money(lineitem.tax), pmr_string(1, lineitem.rflag[0]),
                                  pmr_string(1, lineitem.lstatus[0]), lineitem.sdate, lineitem.cdate, lineitem.rdate,
                                  lineitem.shipinstruct, lineitem.shipmode, lineitem.comment);
    }
  }
  
  // Lineitem first to minimmize tables already loaded when creating its histograms.
  auto lineitem_table = lineitem_builder.finish_table();
  table_info_by_name["lineitem"].table = lineitem_table;

  auto orders_table = order_builder.finish_table();
  table_info_by_name["orders"].table = orders_table;

  data_generation_done = true;
  std::cout << "Waiting for lineitem and orders table encoding." << std::endl;
  encoder_thread.join();
  std::cout << "Encoding done." << std::endl;

  std::cout << "Adding to storage manager." << std::endl;
  print_generation_duration_and_add_table(orders_table_timer, "orders", orders_table);
  print_generation_duration_and_add_table(orders_table_timer, "lineitem", lineitem_table);
  std::cout << "Done." << std::endl;

  std::cout << "Processing remaining tables." << std::endl;
  table_builders.clear();
  table_builders.emplace_back("customer", customer_builder._table);
  table_builders.emplace_back("part", part_builder._table);
  table_builders.emplace_back("partsupp", partsupp_builder._table);
  table_builders.emplace_back("supplier", supplier_builder._table);
  table_builders.emplace_back("nation", nation_builder._table);
  table_builders.emplace_back("region", region_builder._table);

  data_generation_done = false;
  encoder_thread = std::thread{[&] {
    encode_tables(_benchmark_config, table_builders, data_generation_done);
  }};

  /**
   * CUSTOMER
   */

  Timer customer_table_timer;
  for (size_t row_idx = 0; row_idx < customer_count; row_idx++) {
    auto customer = call_dbgen_mk<customer_t>(row_idx + 1, mk_cust, TPCHTable::Customer);
    customer_builder.append_row(customer.custkey, customer.name, customer.address, customer.nation_code, customer.phone,
                                convert_money(customer.acctbal), customer.mktsegment, customer.comment);
  }

  auto customer_table = customer_builder.finish_table();
  table_info_by_name["customer"].table = customer_table;
  print_generation_duration_and_add_table(customer_table_timer, "customer", customer_table);


  /**
   * PART and PARTSUPP
   */

  Timer part_table_timer;
  for (size_t part_idx = 0; part_idx < part_count; ++part_idx) {
    const auto part = call_dbgen_mk<part_t>(part_idx + 1, mk_part, TPCHTable::Part);

    part_builder.append_row(part.partkey, part.name, part.mfgr, part.brand, part.type, part.size, part.container,
                            convert_money(part.retailprice), part.comment);

    // Some scale factors (e.g., 0.05) are not supported by tpch-dbgen as they produce non-unique partkey/suppkey
    // combinations. The reason is probably somewhere in the magic in PART_SUPP_BRIDGE. As the partkey is
    // ascending, those are easy to identify:

    DSS_HUGE last_partkey = {};
    auto suppkeys = std::vector<DSS_HUGE>{};

    for (const auto& partsupp : part.s) {
      {
        // Make sure we do not generate non-unique combinations (see above)
        if (partsupp.partkey != last_partkey) {
          Assert(partsupp.partkey > last_partkey, "Expected partkey to be generated in ascending order");
          last_partkey = partsupp.partkey;
          suppkeys.clear();
        }
        Assert(std::find(suppkeys.begin(), suppkeys.end(), partsupp.suppkey) == suppkeys.end(),
               "Scale factor unsupported by tpch-dbgen. Consider choosing a \"round\" number.");
        suppkeys.emplace_back(partsupp.suppkey);
      }

      partsupp_builder.append_row(partsupp.partkey, partsupp.suppkey, partsupp.qty, convert_money(partsupp.scost),
                                  partsupp.comment);
    }
  }

  auto part_table = part_builder.finish_table();
  table_info_by_name["part"].table = part_table;
  print_generation_duration_and_add_table(part_table_timer, "part", part_table);

  auto partsupp_table = partsupp_builder.finish_table();
  table_info_by_name["partsupp"].table = partsupp_table;
  print_generation_duration_and_add_table(part_table_timer, "partsupp", partsupp_table);

  /**
   * SUPPLIER
   */

  Timer supplier_table_timer;
  for (size_t supplier_idx = 0; supplier_idx < supplier_count; ++supplier_idx) {
    const auto supplier = call_dbgen_mk<supplier_t>(supplier_idx + 1, mk_supp, TPCHTable::Supplier);

    supplier_builder.append_row(supplier.suppkey, supplier.name, supplier.address, supplier.nation_code, supplier.phone,
                                convert_money(supplier.acctbal), supplier.comment);
  }

  auto supplier_table = supplier_builder.finish_table();
  table_info_by_name["supplier"].table = supplier_table;
  print_generation_duration_and_add_table(supplier_table_timer, "supplier", supplier_table);

  /**
   * NATION
   */

  Timer nation_table_timer;
  for (size_t nation_idx = 0; nation_idx < nation_count; ++nation_idx) {
    const auto nation = call_dbgen_mk<code_t>(nation_idx + 1, mk_nation, TPCHTable::Nation);
    nation_builder.append_row(nation.code, nation.text, nation.join, nation.comment);
  }

  auto nation_table = nation_builder.finish_table();
  table_info_by_name["nation"].table = nation_table;
  print_generation_duration_and_add_table(nation_table_timer, "nation", nation_table);

  /**
   * REGION
   */

  Timer region_table_timer;
  for (size_t region_idx = 0; region_idx < region_count; ++region_idx) {
    const auto region = call_dbgen_mk<code_t>(region_idx + 1, mk_region, TPCHTable::Region);
    region_builder.append_row(region.code, region.text, region.comment);
  }

  /**
   * Clean up dbgen every time we finish table generation to avoid memory leaks in dbgen
   */
  dbgen_cleanup();

  /**
   * Return
   */

  auto region_table = region_builder.finish_table();
  table_info_by_name["region"].table = region_table;
  print_generation_duration_and_add_table(region_table_timer, "region", region_table);

  if (_benchmark_config->cache_binary_tables) {
    std::filesystem::create_directories(cache_directory);
    for (auto& [table_name, table_info] : table_info_by_name) {
      table_info.binary_file_path = cache_directory + "/" + table_name + ".bin";  // NOLINT
    }
  }

  data_generation_done = true;
  encoder_thread.join();
  return table_info_by_name;
}

AbstractTableGenerator::IndexesByTable TPCHTableGenerator::_indexes_by_table() const {
  return {
      {"part", {{"p_partkey"}}},
      {"supplier", {{"s_suppkey"}, {"s_nationkey"}}},
      // TODO(anyone): multi-column indexes are currently not used by the index scan rule and the translator
      {"partsupp", {{"ps_partkey", "ps_suppkey"}, {"ps_suppkey"}}},  // ps_partkey is subset of {ps_partkey, ps_suppkey}
      {"customer", {{"c_custkey"}, {"c_nationkey"}}},
      {"orders", {{"o_orderkey"}, {"o_custkey"}}},
      {"lineitem", {{"l_orderkey", "l_linenumber"}, {"l_partkey", "l_suppkey"}}},
      {"nation", {{"n_nationkey"}, {"n_regionkey"}}},
      {"region", {{"r_regionkey"}}},
  };
}

AbstractTableGenerator::SortOrderByTable TPCHTableGenerator::_sort_order_by_table() const {
  if (_clustering_configuration == ClusteringConfiguration::Pruning) {
    // This clustering improves the pruning of chunks for the two largest tables in TPC-H, lineitem and orders. Both
    // tables are frequently filtered by the sorted columns, which improves the pruning rate significantly.
    // Allowed as per TPC-H Specification, paragraph 1.5.2.
    return {{"lineitem", "l_shipdate"}, {"orders", "o_orderdate"}};
  }

  // Even though the generated TPC-H data is implicitly sorted by the primary keys, we do neither set the corresponding
  // flags in the table nor in the chunks. This is done on purpose, as the non-clustered mode is designed to pass as
  // little extra information into Hyrise as possible. In the future, these sort orders might be automatically
  // identified with flags being set automatically.
  return {};
}

void TPCHTableGenerator::_add_constraints(
    std::unordered_map<std::string, BenchmarkTableInfo>& table_info_by_name) const {
  const auto& customer_table = table_info_by_name.at("customer").table;
  customer_table->add_soft_key_constraint(
      {{customer_table->column_id_by_name("c_custkey")}, KeyConstraintType::PRIMARY_KEY});

  const auto& orders_table = table_info_by_name.at("orders").table;
  const auto orders_pk_constraint =
      TableKeyConstraint{{orders_table->column_id_by_name("o_orderkey")}, KeyConstraintType::PRIMARY_KEY};
  orders_table->add_soft_key_constraint(orders_pk_constraint);

  const auto& lineitem_table = table_info_by_name.at("lineitem").table;
  const auto lineitem_pk_constraint = TableKeyConstraint{
      {lineitem_table->column_id_by_name("l_orderkey"), lineitem_table->column_id_by_name("l_linenumber")},
      KeyConstraintType::PRIMARY_KEY};
  lineitem_table->add_soft_key_constraint(lineitem_pk_constraint);

  const auto& part_table = table_info_by_name.at("part").table;
  const auto part_table_pk_constraint =
      TableKeyConstraint{{part_table->column_id_by_name("p_partkey")}, KeyConstraintType::PRIMARY_KEY};
  part_table->add_soft_key_constraint(part_table_pk_constraint);

  const auto& partsupp_table = table_info_by_name.at("partsupp").table;
  const auto partsupp_pk_constraint = TableKeyConstraint{
      {partsupp_table->column_id_by_name("ps_partkey"), partsupp_table->column_id_by_name("ps_suppkey")},
      KeyConstraintType::PRIMARY_KEY};
  partsupp_table->add_soft_key_constraint(partsupp_pk_constraint);

  const auto& supplier_table = table_info_by_name.at("supplier").table;
  const auto supplier_pk_constraint =
      TableKeyConstraint{{supplier_table->column_id_by_name("s_suppkey")}, KeyConstraintType::PRIMARY_KEY};
  supplier_table->add_soft_key_constraint(supplier_pk_constraint);

  const auto& nation_table = table_info_by_name.at("nation").table;
  const auto nation_pk_constraint =
      TableKeyConstraint{{nation_table->column_id_by_name("n_nationkey")}, KeyConstraintType::PRIMARY_KEY};
  nation_table->add_soft_key_constraint(nation_pk_constraint);

  const auto& region_table = table_info_by_name.at("region").table;
  const auto region_pk_constraint =
      TableKeyConstraint{{region_table->column_id_by_name("r_regionkey")}, KeyConstraintType::PRIMARY_KEY};
  region_table->add_soft_key_constraint(region_pk_constraint);
}

}  // namespace opossum
