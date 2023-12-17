#include "hnsw_flat_index.hpp"
#include <boost/lexical_cast.hpp>
#include <memory>
#include <unordered_map>
#include "index_io.h"
#include "storage/chunk.hpp"
#include "storage/index/abstract_vector_index.hpp"
#include "storage/segment_iterate.hpp"
#include "types.hpp"
#include "utils/timer.hpp"
#define DEFAULT_DIM 128
#define DEFAULT_EFS 200
#define DEFAULT_M 16

namespace hyrise {

                                                            
HNSWFlatIndex::HNSWFlatIndex(const std::string& path,const std::unordered_map<std::string, int> parameters)
    : AbstractVectorIndex{get_vector_index_type_of<HNSWFlatIndex>(), "hnsw"} {
      // printf("create ivfpq index-2\n");
  faiss::Index* new_index = faiss::read_index(path.c_str());
  _index = dynamic_cast<faiss::IndexHNSWFlat*>(new_index);
}

HNSWFlatIndex::HNSWFlatIndex(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index,
                           ColumnID column_id, std::unordered_map<std::string, int> parameters)
    : AbstractVectorIndex{get_vector_index_type_of<HNSWFlatIndex>(), "hnsw"} {
      // printf("create ivfpq index-3\n");
  auto per_table_index_timer = Timer{};
  Assert(!chunks_to_index.empty(), "HNSWFlatIndex requires chunks_to_index not to be empty.");
  _column_id = column_id;
  // printf("checkpoint2.2.1\n");
  std::unordered_map<std::string, int>::iterator get_item;
  if (get_item = parameters.find("dim"), get_item == parameters.end())
    _d = DEFAULT_DIM;
  else
    _d = get_item->second;

  if (get_item = parameters.find("m"), get_item == parameters.end())
    _m = DEFAULT_M;
  else
    _m = get_item->second;
  _index = new faiss::IndexHNSWFlat(_d, _m);

  // printf("checkpoint2.2.2\n");
  if (get_item = parameters.find("efs"), get_item == parameters.end())
    _index->hnsw.efSearch = DEFAULT_EFS;
  else
    _index->hnsw.efSearch = get_item->second;

  // printf("checkpoint2.2.3\n");
  train_and_insert(chunks_to_index);
  std::cout << "HNSWFlatIndex time(" << per_table_index_timer.lap_formatted() << ")" << std::endl;
}

bool HNSWFlatIndex::is_index_for(const ColumnID column_id) const {
  return column_id == _column_id;
}

ColumnID HNSWFlatIndex::get_indexed_column_id() const {
  return _column_id;
}

void HNSWFlatIndex::train_and_insert(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index) {
  // if (_index->is_trained) {
  //   std::cout << "is_trained = true" << std::endl;
  //   return;
  // }
  auto indexed_chunks = size_t{0};
  float* data = new float[chunks_to_index.size() * Chunk::DEFAULT_SIZE * _d];
  int nb = 0;

  auto per_table_index_timer = Timer{};
  for (const auto& chunk : chunks_to_index) {
    ++indexed_chunks;
    const auto abstract_segment = chunk.second->get_segment(_column_id);
    const auto segment_size = abstract_segment->size();
    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < segment_size; ++chunk_offset) {
      const auto value = boost::get<float_array>((*abstract_segment)[chunk_offset]).data();
      memcpy(data + nb * _d, value, sizeof(float) * _d);
      nb++;
    }
  }
  // std::cout << "cut data(" << per_table_index_timer.lap_formatted() << ")" << std::endl;
  // srand((int)time(0));
  // float* trainvecs = new float[nb / 100 * _d];
  // for (int i = 0; i < nb / 100; i++) {
  //   int rng = (rand() % (nb + 1));
  //   memcpy(trainvecs + i * _d, data + rng * _d, sizeof(float) * _d);
  // }

  // std::cout << "cut train data(" << per_table_index_timer.lap_formatted() << ")" << std::endl;
  // _index->train(nb / 100, trainvecs);
  // _index->train(nb, data);
  // std::cout << "cut finish train(" << per_table_index_timer.lap_formatted() << ")" << std::endl;
  // delete[] trainvecs;
  // std::cout << "cut delete vec(" << per_table_index_timer.lap_formatted() << ")" << std::endl;
  _index->add(nb, data);
  // std::cout << "cut finish add(" << per_table_index_timer.lap_formatted() << ")" << std::endl;
  // delete[] data;
  // std::cout << "cut delete data(" << per_table_index_timer.lap_formatted() << ")" << std::endl;
}

void HNSWFlatIndex::similar_k(const float* query, int64_t* I, float* D, int k) {
  std::cout << "We do not recommend you to search for single queries." << std::endl;
  _index->search(1, query, k, D, I);
}

void HNSWFlatIndex::range_similar_k(size_t n, const float* query, int64_t* I, float* D, int k) {
  // std::cout << "HNSWFlatIndex::range_similar_k" << std::endl;
  auto per_table_index_timer = Timer{};
  _index->search(n, query, k, D, I);
  // std::cout << per_table_index_timer.lap_formatted() << std::endl;
}

void HNSWFlatIndex::save_index(const std::string& save_path) {
  // std::cout<<"IVFPQ save index"<<std::endl;
  faiss::write_index(_index, save_path.c_str());
}
}  // namespace hyrise
