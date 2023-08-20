#include "hnsw_index.hpp"
#include <boost/lexical_cast.hpp>
#include "storage/chunk.hpp"
#include "storage/segment_iterate.hpp"
#include "types.hpp"
#include "utils/timer.hpp"

namespace hyrise {

HNSWIndex::HNSWIndex(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index, ColumnID column_id,
                     int dim, long long max_elements = MAX_ELES_LONGLONG, int M = 16, int ef_construction = 40,
                     int ef = 200) {
  Assert(!chunks_to_index.empty(), "HNSWIndex requires chunks_to_index not to be empty.");
}

HNSWIndex::HNSWIndex(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index, ColumnID column_id,
                     int dim, int max_elements = MAX_ELES_UINT, int M = 16, int ef_construction = 40, int ef = 200) {
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

size_t HNSWIndex::insert(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index) {
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
  return indexed_chunks;
}

// SimilarKPair HNSWIndex::similar_k(const AllTypeVariant& query, int k = 1) {
SimilarKPair HNSWIndex::similar_k(float_array& query, int k = 1) {
  printf("data: ");
  // std::cout << query.length() << std::endl;
  // std::cout << (&q) << std::endl;
  // std::cout << (q.data()[0]) << std::endl;
  // std::cout << _dim << std::endl;
  // for (int i = 0; i < _dim; i++) {
  //   printf("%f ", query.data()[i]);
  // }
  // printf("\n");
  float xx[16]={0.1,0.2,0.1,0.2,0.1,0.2,0.1,0.2,0.1,0.2,0.1,0.2,0.1,0.2,0.1,0.2};
  // for (int x = 0; x < 100; x++) {
    std::priority_queue<std::pair<float, hnswlib::labeltype>> result = _alg_hnsw->searchKnn(xx, k);
  // }
  SimilarKPair returnResult;
  // while (!result.empty()) {
  //   hnswlib::labeltype label = result.top().second;
  //   auto times = Chunk::DEFAULT_SIZE;
  //   ChunkID cid = ChunkID{(unsigned int)(label / times)};
  //   ChunkOffset cot = ChunkOffset{(unsigned int)(label % times)};
  //   returnResult.push_back(std::pair<ChunkID, ChunkOffset>(cid, cot));
  //   result.pop();
  // }
  printf("HNSWIndex::similar_k\n");
  return returnResult;
}

}  // namespace hyrise
