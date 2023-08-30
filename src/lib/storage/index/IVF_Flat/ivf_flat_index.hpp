#pragma once

#include <stdio.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <tsl/sparse_map.h>
#include <tsl/sparse_set.h>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <queue>
#include <random>
#include "IndexFlat.hpp"
#include "IndexIVFFlat.hpp"

#include "storage/index/abstract_vector_index.hpp"
#include "types.hpp"

#define TRAIN_SIZE 6000000
#define ADD_SIZE

namespace hyrise {
using SimilarKPair = std::priority_queue<std::pair<float, size_t>>;

class IVFFlatIndex : public AbstractVectorIndex {
 public:
  IVFFlatIndex() = delete;
  IVFFlatIndex(const IVFFlatIndex&) = delete;
  IVFFlatIndex(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index, ColumnID column_id,
               int d);
  IVFFlatIndex(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index, ColumnID column_id,
               int dim, int testing_data);
  IVFFlatIndex(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index, ColumnID column_id,
               int d, int nlist, size_t nprobe);
  void train(int64_t n, const float* data);
  void train(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>&);
  void insert(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>&, const int64_t* = nullptr);
  void insert(int64_t n, const float* data, const int64_t* = nullptr);
  size_t remove(const std::vector<ChunkID>&);

  void similar_k(const float* query, int64_t* I, float* D, int k = 1);
  void range_similar_k(size_t n, const float* query, int64_t* I, float* D, int k = 1);

  void save_index(const std::string& save_path);
  bool is_index_for(const ColumnID) const;
  ColumnID get_indexed_column_id() const;

  int get_dim() const {
    return _d;
  }

  int get_nlist() const {
    return _nlist;
  }

 private:
  ColumnID _column_id;
  int _d;
  int _nlist;
  bool* _is_trained;
  vindex::IndexFlatL2* _quantizer;
  vindex::IndexIVFFlat* _index;
  tsl::sparse_set<ChunkID> _indexed_chunk_ids = {};
};

}  // namespace hyrise
