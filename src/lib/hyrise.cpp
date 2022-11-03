#include "hyrise.hpp"

namespace hyrise {

Hyrise::Hyrise() {
  // The default_memory_resource must be initialized before Hyrise's members so that
  // it is destructed after them and remains accessible during their deconstruction.
  // For example, when the StorageManager is destructed, it causes its stored tables
  // to be deconstructed, too. As these might call deallocate on the
  // default_memory_resource, it is important that the resource has not been
  // destructed before. As objects are destructed in the reverse order of their
  // construction, explicitly initializing the resource first means that it is
  // destructed last.
  boost::container::pmr::get_default_resource();

  storage_manager = StorageManager{};
  plugin_manager = PluginManager{};
  transaction_manager = TransactionManager{};
  meta_table_manager = MetaTableManager{};
  settings_manager = SettingsManager{};
  log_manager = LogManager{};
  topology = Topology{};
  _scheduler = std::make_shared<ImmediateExecutionScheduler>();
  rdma_manager_1 = RDMA_Manager{};
  rdma_manager_2 = RDMA_Manager{};
  struct hyrise::config_t config = {NULL,           /* dev_name */
                                    "10.145.21.35", /* server_name */
                                    19876,          /* tcp_port */
                                    1,              /* ib_port */
                                    -1 /* gid_idx */};
  rdma_manager_1.init(config);
  rdma_manager_2.init(config);
}

char * Hyrise::RDMA_Read() {
  char temp_char;
  char temp_send[] = "R";
  fprintf(stdout, "------------------------------------------\nread order solved\n");
  // char* out_chars = const_cast<char*>(data.c_str());

  strcpy(Hyrise::get().rdma_manager_1.res->buf, "read,");
  // memcpy(rdma_manager_1.res->buf, data, )
  fprintf(stdout, "going to send the message: '%s'\n", Hyrise::get().rdma_manager_1.res->buf);
  if (Hyrise::get().rdma_manager_1.RDMA_Send()) {
    fprintf(stderr, "failed to rdma_mg_1 RDMA_Send\n");
    return nullptr;
  }
  if (Hyrise::get().rdma_manager_1.RDMA_Receive()) {
    fprintf(stderr, "failed to rdma_mg_1 RDMA_Receive\n");
    return nullptr;
  }
  if (0 == strcmp(Hyrise::get().rdma_manager_1.res->buf, "already")) {
    fprintf(stdout, "Message is: '%s', %ld\n", Hyrise::get().rdma_manager_1.res->buf,
            strlen(Hyrise::get().rdma_manager_1.res->buf));
  } else {
    fprintf(stderr, "failed receive already\n");
    return nullptr;
  }
  if (Hyrise::get().rdma_manager_2.RDMA_Read()) {
    fprintf(stderr, "failed to rdma_mg_2 RDMA_Read\n");
    return nullptr;
  }
  // fprintf(stdout, "Contents of server's buffer: '%s'\n", Hyrise::get().rdma_manager_2.res->buf);
  if (Hyrise::get().rdma_manager_1.sock_sync_data(Hyrise::get().rdma_manager_1.res->sock, 1, temp_send,
                                                  &temp_char)) /* just send a dummy char back and forth */
  {
    fprintf(stderr, "sync error after RDMA ops\n");
    return nullptr;
  }
  if (Hyrise::get().rdma_manager_2.sock_sync_data(Hyrise::get().rdma_manager_2.res->sock, 1, temp_send,
                                                  &temp_char)) /* just send a dummy char back and forth */
  {
    fprintf(stderr, "sync error after RDMA ops\n");
    return nullptr;
  }
  return Hyrise::get().rdma_manager_2.res->buf;
}

void Hyrise::RDMA_Write(char* data, uint64_t length) {
  char temp_char;
  char temp_send[] = "R";
  fprintf(stdout, "------------------------------------------\nwrite order solved\n");

  strcpy(Hyrise::get().rdma_manager_1.res->buf, "write,");
  fprintf(stdout, "rdma_manager_1 going to send the message: '%s'\n", Hyrise::get().rdma_manager_1.res->buf);
  if (Hyrise::get().rdma_manager_1.RDMA_Send()) {
    fprintf(stderr, "failed to rdma_mg_1 RDMA_Send\n");
    return;
  }

  // char* out_chars = const_cast<char*>(data.c_str());
  // strcpy(Hyrise::get().rdma_manager_2.res->buf, data);
  memcpy(Hyrise::get().rdma_manager_2.res->buf, data, length);
  // fprintf(stdout, "rdma_manager_2 going to send the message: '%s'\n", Hyrise::get().rdma_manager_2.res->buf);
  if (Hyrise::get().rdma_manager_2.RDMA_Write()) {
    fprintf(stderr, "failed to rdma_mg_2 RDMA_Write\n");
    return;
  }
  if (Hyrise::get().rdma_manager_1.sock_sync_data(Hyrise::get().rdma_manager_1.res->sock, 1, temp_send,
                                                  &temp_char)) /* just send a dummy char back and forth */
  {
    fprintf(stderr, "sync error after RDMA ops\n");
    return;
  }
  if (Hyrise::get().rdma_manager_2.sock_sync_data(Hyrise::get().rdma_manager_2.res->sock, 1, temp_send,
                                                  &temp_char)) /* just send a dummy char back and forth */
  {
    fprintf(stderr, "sync error after RDMA ops\n");
    return;
  }
}

void Hyrise::reset() {
  Hyrise::get().scheduler()->finish();
  get() = Hyrise{};
}

const std::shared_ptr<AbstractScheduler>& Hyrise::scheduler() const {
  return _scheduler;
}

bool Hyrise::is_multi_threaded() const {
  return std::dynamic_pointer_cast<ImmediateExecutionScheduler>(_scheduler) == nullptr;
}

void Hyrise::set_scheduler(const std::shared_ptr<AbstractScheduler>& new_scheduler) {
  _scheduler->finish();
  _scheduler = new_scheduler;
  _scheduler->begin();
}

}  // namespace hyrise
