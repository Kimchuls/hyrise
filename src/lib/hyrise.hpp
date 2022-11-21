#pragma once

#include <boost/container/pmr/memory_resource.hpp>

#include "concurrency/transaction_manager.hpp"
#include "rdma/rdma.h"
#include "scheduler/immediate_execution_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "sql/sql_plan_cache.hpp"
#include "storage/storage_manager.hpp"
#include "utils/log_manager.hpp"
#include "utils/meta_table_manager.hpp"
#include "utils/plugin_manager.hpp"
#include "utils/settings_manager.hpp"
#include "utils/singleton.hpp"

namespace hyrise {

class AbstractScheduler;
class BenchmarkRunner;

struct index_info {
  std::string table_name;
  uint32_t chunk_id;
  uint8_t index_type;
  std::vector<uint16_t> column_ids;

  inline bool operator<(const index_info& p) const {
    if (table_name < p.table_name)
      return true;
    else if (table_name == p.table_name && chunk_id < p.chunk_id)
      return true;
    else if (table_name == p.table_name && chunk_id == p.chunk_id && index_type < p.index_type)
      return true;
    else if (table_name == p.table_name && chunk_id == p.chunk_id && index_type == p.index_type &&
             column_ids.size() < p.column_ids.size())
      return true;
    else if (table_name == p.table_name && chunk_id == p.chunk_id && index_type == p.index_type &&
             column_ids.size() == p.column_ids.size()) {
      for (int i = 0; i < p.column_ids.size(); i++) {
        if (column_ids[i] < p.column_ids[i])
          return true;
        if (column_ids[i] > p.column_ids[i])
          return false;
      }
    }
    return false;
  }
};

// This should be the only singleton in the src/lib world. It provides a unified way of accessing components like the
// storage manager, the transaction manager, and more. Encapsulating this in one class avoids the static initialization
// order fiasco, which would otherwise make the initialization/destruction order hard to control.
class Hyrise : public Singleton<Hyrise> {
 public:
  // Resets the Hyrise state by deleting its members (e.g., StorageManager) and
  // creating new ones. This is used especially in tests and can lead to a lot of
  // issues if there are still running tasks / threads that want to access a resource.
  // You should be very sure that this is what you want.
  static void reset();

  // The scheduler is always set. However, the ImmediateExecutionScheduler does not involve any multi-threading. This
  // can be tested with is_multi_threaded.
  const std::shared_ptr<AbstractScheduler>& scheduler() const;
  bool is_multi_threaded() const;

  void set_scheduler(const std::shared_ptr<AbstractScheduler>& new_scheduler);

  char* RDMA_Read(char* data1, uint64_t length1);
  void RDMA_Write(char* data1, uint64_t length1, char* data2, uint64_t length2);

  // The order of these members is important because it defines in which order their destructors are called.
  // For example, the StorageManager's destructor should not be called before the PluginManager's destructor.
  // The latter stops all plugins which, in turn, might access tables during their shutdown procedure. This
  // could not work without the StorageManager still in place.
  StorageManager storage_manager;
  PluginManager plugin_manager;
  TransactionManager transaction_manager;
  MetaTableManager meta_table_manager;
  SettingsManager settings_manager;
  LogManager log_manager;
  Topology topology;
  RDMA_Manager rdma_manager_1;
  RDMA_Manager rdma_manager_2;
  std::map<index_info, std::shared_ptr<AbstractIndex>> memory_index;

  // Plan caches used by the SQLPipelineBuilder if `with_{l/p}qp_cache()` are not used. Both default caches can be
  // nullptr themselves. If both default_{l/p}qp_cache and _{l/p}qp_cache are nullptr, no plan caching is used.
  std::shared_ptr<SQLPhysicalPlanCache> default_pqp_cache;
  std::shared_ptr<SQLLogicalPlanCache> default_lqp_cache;

  // The BenchmarkRunner is available here so that non-benchmark components can add information to the benchmark
  // result JSON.
  std::weak_ptr<BenchmarkRunner> benchmark_runner;

 private:
  Hyrise();
  friend class Singleton;

  // (Re-)setting the scheduler requires more than just replacing the pointer. To make sure that set_scheduler is used,
  // the scheduler is private.
  std::shared_ptr<AbstractScheduler> _scheduler;
};

}  // namespace hyrise
