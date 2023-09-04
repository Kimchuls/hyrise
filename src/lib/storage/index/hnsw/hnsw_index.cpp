#include "hnsw_index.hpp"
#include <boost/lexical_cast.hpp>
#include "storage/chunk.hpp"
#include "storage/index/abstract_vector_index.hpp"
#include "storage/segment_iterate.hpp"
#include "types.hpp"
#include "utils/timer.hpp"

namespace hyrise {

// HNSWIndex::HNSWIndex(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index, ColumnID column_id,
//                      int dim, long long max_elements = MAX_ELES_LONGLONG, int M = 16, int ef_construction = 40,
//                      int ef = 200)
//     : AbstractVectorIndex{get_vector_index_type_of<HNSWIndex>()} {
//   Assert(!chunks_to_index.empty(), "HNSWIndex requires chunks_to_index not to be empty.");
// }
HNSWIndex::HNSWIndex(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index, ColumnID column_id,
                     int dim)
    : AbstractVectorIndex{get_vector_index_type_of<HNSWIndex>()} {
  Assert(!chunks_to_index.empty(), "HNSWIndex requires chunks_to_index not to be empty.");
  _column_id = column_id;
  _dim = dim;
  _max_elements = MAX_ELES_UINT;
  _M = 32;
  _ef_construction = 40;
  _space = new hnswlib::L2Space(_dim);
  _alg_hnsw = new hnswlib::HierarchicalNSW<float>(_space, _max_elements, _M, _ef_construction);
  _alg_hnsw->setEf(200);
  _indexed_chunk_ids = {};
  insert(chunks_to_index);
}

HNSWIndex::HNSWIndex(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index, ColumnID column_id,
                     int dim, int testing_data=200)
    : AbstractVectorIndex{get_vector_index_type_of<HNSWIndex>()} {
  Assert(!chunks_to_index.empty(), "HNSWIndex requires chunks_to_index not to be empty.");
  _column_id = column_id;
  _dim = dim;
  _max_elements = MAX_ELES_UINT;
  _M = 32;
  _ef_construction = 40;
  _space = new hnswlib::L2Space(_dim);
  _alg_hnsw = new hnswlib::HierarchicalNSW<float>(_space, _max_elements, _M, _ef_construction);
  _alg_hnsw->setEf(testing_data);
  _indexed_chunk_ids = {};
  insert(chunks_to_index);
}

HNSWIndex::HNSWIndex(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index, ColumnID column_id,
                     int dim, int max_elements = MAX_ELES_UINT, int M = 32, int ef_construction = 40, int ef = 200)
    : AbstractVectorIndex{get_vector_index_type_of<HNSWIndex>()} {
  Assert(!chunks_to_index.empty(), "HNSWIndex requires chunks_to_index not to be empty.");
  _column_id = column_id;
  _dim = dim;
  _max_elements = max_elements;
  _M = M;
  _ef_construction = ef_construction;
  _space = new hnswlib::L2Space(_dim);
  _alg_hnsw = new hnswlib::HierarchicalNSW<float>(_space, max_elements, M, ef_construction);
  _alg_hnsw->setEf(ef);
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
  for (const auto& chunk : chunks_to_index) {
    if (_indexed_chunk_ids.contains(chunk.first)) {
      continue;
    }
    _indexed_chunk_ids.insert(chunk.first);
    ++indexed_chunks;
    int idx = chunk.first;
    // std::cout<<"idx: "<<idx<<std::endl;
    const auto abstract_segment = chunk.second->get_segment(_column_id);
    const auto segment_size = abstract_segment->size();
    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < segment_size; ++chunk_offset) {
      const auto value = boost::get<float_array>((*abstract_segment)[chunk_offset]);
      auto label = (int)(times * idx) + chunk_offset;
      // if(chunk_offset%100==0){
      //   std::cout<<"chunk_offset: "<<chunk_offset<<", label: "<<label<<std::endl;
      // }
      _alg_hnsw->addPoint(value.data(), label);
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

void HNSWIndex::save_index(const std::string& save_path ) {
  _alg_hnsw->saveIndex(save_path);
}
}  // namespace hyrise
