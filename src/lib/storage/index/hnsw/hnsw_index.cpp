#include "hnsw_index.hpp"
#include <boost/lexical_cast.hpp>
#include <unordered_map>
#include "storage/chunk.hpp"
#include "storage/index/abstract_vector_index.hpp"
#include "storage/segment_iterate.hpp"
#include "types.hpp"
#include "utils/timer.hpp"
#define DEFAULT_DIM 128
#define DEFAULT_M 16
#define DEFAULT_EF_CONSTRUCTION 40
#define DEFAULT_EFS 200

namespace hyrise {
HNSWIndex::HNSWIndex(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index, ColumnID column_id,
                     std::unordered_map<std::string, int> parameters)
    : AbstractVectorIndex{get_vector_index_type_of<HNSWIndex>(), "hnsw"} {
  Assert(!chunks_to_index.empty(), "HNSWIndex requires chunks_to_index not to be empty.");
  _column_id = column_id;

  std::unordered_map<std::string, int>::iterator get_item;
  if (get_item = parameters.find("dim"), get_item == parameters.end())
    _dim = DEFAULT_DIM;
  else
    _dim = get_item->second;

  if (get_item = parameters.find("max_elements"), get_item == parameters.end())
    _max_elements = MAX_ELES_UINT;
  else
    _max_elements = get_item->second;

  if (get_item = parameters.find("M"), get_item == parameters.end())
    _M = DEFAULT_M;
  else
    _M = get_item->second;

  if (get_item = parameters.find("ef_construction"), get_item == parameters.end())
    _ef_construction = DEFAULT_EF_CONSTRUCTION;
  else
    _ef_construction = get_item->second;

  _space = new hnswlib::L2Space(_dim);
  _alg_hnsw = new hnswlib::HierarchicalNSW<float>(_space, _max_elements, _M, _ef_construction);

  if (get_item = parameters.find("efs"), get_item == parameters.end())
    _alg_hnsw->setEf(DEFAULT_EFS);
  else
    _alg_hnsw->setEf(get_item->second);
  _indexed_chunk_ids = {};
  insert(chunks_to_index);
}

bool HNSWIndex::is_index_for(const ColumnID column_id) const {
  return column_id == _column_id;
}

ColumnID HNSWIndex::get_indexed_column_id() const {
  return _column_id;
}

void HNSWIndex::insert(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index) {
  std::cout << "insert into HNSWIndex: " << std::endl;
  auto per_table_index_timer = Timer{};
  auto indexed_chunks = size_t{0};
  auto times = Chunk::DEFAULT_SIZE;
  int give_label = 0;
  for (const auto& chunk : chunks_to_index) {
    if (_indexed_chunk_ids.contains(chunk.first)) {
      continue;
    }
    _indexed_chunk_ids.insert(chunk.first);
    ++indexed_chunks;
    // int idx = chunk.first;
    // std::cout<<"idx: "<<idx<<std::endl;
    const auto abstract_segment = chunk.second->get_segment(_column_id);
    const auto segment_size = abstract_segment->size();
    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < segment_size; ++chunk_offset) {
      const auto value = boost::get<float_array>((*abstract_segment)[chunk_offset]);
      // auto label = (int)(times * idx) + chunk_offset;
      // if(chunk_offset%100==0){
      //   std::cout<<"chunk_offset: "<<chunk_offset<<", label: "<<label<<std::endl;
      // }
      // _alg_hnsw->addPoint(value.data(), label);
      _alg_hnsw->addPoint(value.data(), give_label++);
    }
  }
  std::cout << per_table_index_timer.lap_formatted() << std::endl;
  // return indexed_chunks;
}

void HNSWIndex::similar_k(const float* query, int64_t* I, float* D, int k = 1) {
  range_similar_k(1, query, I, D, k);
  return;
}

void HNSWIndex::range_similar_k(size_t n, const float* queries, int64_t* I, float* D, int k = 1) {
  std::cout << "HNSWIndex::range_similar_k: " << std::endl;
  auto per_table_index_timer = Timer{};
  for (size_t i = 0; i < n; i++) {
    SimilarKPair result = _alg_hnsw->searchKnn(queries + i * _dim, k);
    int iter = result.size() - 1;
    while (!result.empty()) {
      I[i * k + iter] = (int64_t)(result.top().second);
      D[i * k + iter] = result.top().first;
      iter--;
      result.pop();
    }
  }
  std::cout << per_table_index_timer.lap_formatted() << std::endl;
}

void HNSWIndex::train(int64_t n, const float* data) {
  Assert(true, "This index is not support training.\n");
}

void HNSWIndex::train(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index) {
  Assert(true, "This index is not support training.\n");
}

void HNSWIndex::save_index(const std::string& save_path) {
  _alg_hnsw->saveIndex(save_path);
}
}  // namespace hyrise
