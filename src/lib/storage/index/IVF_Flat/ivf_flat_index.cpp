#include "ivf_flat_index.hpp"
#include <boost/lexical_cast.hpp>
#include <memory>
#include "storage/chunk.hpp"
#include "storage/index/abstract_vector_index.hpp"
#include "storage/segment_iterate.hpp"
#include "types.hpp"
#include "utils/timer.hpp"

namespace hyrise {

float* fvecs_read(const char* fname, size_t* d_out, size_t* n_out) {
  FILE* f = fopen(fname, "r");
  if (!f) {
    fprintf(stderr, "could not open %s\n", fname);
    perror("");
    abort();
  }
  int d;
  fread(&d, 1, sizeof(int), f);
  assert((d > 0 && d < 1000000) || !"unreasonable dimension");
  fseek(f, 0, SEEK_SET);
  struct stat st;
  fstat(fileno(f), &st);
  size_t sz = st.st_size;
  assert(sz % ((d + 1) * 4) == 0 || !"weird file size");
  size_t n = sz / ((d + 1) * 4);

  *d_out = d;
  *n_out = n;
  float* x = new float[n * (d + 1)];
  size_t nr = fread(x, sizeof(float), n * (d + 1), f);
  assert(nr == n * (d + 1) || !"could not read whole file");

  // shift array to remove row headers
  for (size_t i = 0; i < n; i++)
    memmove(x + i * d, x + 1 + i * (d + 1), d * sizeof(*x));

  fclose(f);
  return x;
}

int* ivecs_read(const char* fname, size_t* d_out, size_t* n_out) {
  return (int*)fvecs_read(fname, d_out, n_out);
}

void load_data(char* filename, float*& data, int num, int dim) {
  std::ifstream in(filename, std::ios::binary);  //open file in binary
  if (!in.is_open()) {
    std::cout << "open file error" << std::endl;
    exit(-1);
  }
  data = new float[(size_t)num * (size_t)dim];
  in.seekg(0, std::ios::beg);  //shift to start
                               //       printf("shift successfully\n");
  for (size_t i = 0; i < num; i++) {
    in.seekg(4, std::ios::cur);  //right shift 4 bytes

    for (int j = 0; j < dim; j++)
      in.read((char*)(data + i * dim + j), 4);  //load data
                                                //in.read((char*)(data + i*dim), dim );
  }
}

IVFFlatIndex::IVFFlatIndex(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index,
                           ColumnID column_id, int d = 128)
    : AbstractVectorIndex{get_vector_index_type_of<IVFFlatIndex>()} {
  Assert(!chunks_to_index.empty(), "IVFFlatIndex requires chunks_to_index not to be empty.");
  _column_id = column_id;
  _d = d;
  _nlist = 1000;
  _quantizer = new vindex::IndexFlatL2(_d);
  _index = new vindex::IndexIVFFlat(_quantizer, _d, _nlist);
  _index->nprobe = 20;
  _is_trained = &(_index->is_trained);

  printf("self training\n");
  int nb = 1000000;
  char* base_filepath = "/home/jin467/github_download/hyrise/scripts/vector_test/sift/sift_base.fvecs";
  float* xb = new float[d * nb];
  load_data(base_filepath, xb, nb, d);
  srand((int)time(0));
  std::vector<float> trainvecs(nb / 100 * d);
  for (int i = 0; i < nb / 100; i++) {
    int rng = (rand() % (nb + 1));
    for (int j = 0; j < d; j++) {
      trainvecs[d * i + j] = xb[rng * d + j];
    }
  }
  auto per_table_index_timer = Timer{};
  _index->train(nb / 100, trainvecs.data());
  std::cout << "(" << per_table_index_timer.lap_formatted() << ")" << std::endl;
  delete[] xb;

  insert(chunks_to_index);
}

IVFFlatIndex::IVFFlatIndex(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index,
                           ColumnID column_id, int d = 128, int nlist = 1000, size_t nprobe = 20)
    : AbstractVectorIndex{get_vector_index_type_of<IVFFlatIndex>()} {
  Assert(!chunks_to_index.empty(), "IVFFlatIndex requires chunks_to_index not to be empty.");
  _column_id = column_id;
  _d = d;
  _nlist = nlist;
  _quantizer = new vindex::IndexFlatL2(_d);
  _index = new vindex::IndexIVFFlat(_quantizer, _d, _nlist);
  _index->nprobe = nprobe;
  _is_trained = &(_index->is_trained);

  //TODO: training and inserting
}

bool IVFFlatIndex::is_index_for(const ColumnID column_id) const {
  return column_id == _column_id;
}

ColumnID IVFFlatIndex::get_indexed_column_id() const {
  return _column_id;
}

void IVFFlatIndex::train(int64_t n, const float* data) {
  std::cout << "train IVFFlatIndex: const float* data" << std::endl;
  if (_is_trained) {
    std::cout << "is_trained = true" << std::endl;
    return;
  }
  auto per_table_index_timer = Timer{};
  _index->train(n, data);
  std::cout << per_table_index_timer.lap_formatted() << std::endl;
}

void IVFFlatIndex::train(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index) {
  std::cout << "train IVFFlatIndex: chunks_to_index" << std::endl;
  if (_is_trained) {
    std::cout << "is_trained = true" << std::endl;
    return;
  }
  auto indexed_chunks = size_t{0};
  float* data = new float[TRAIN_SIZE];
  int iter = 0;
  auto per_table_index_timer = Timer{};
  for (const auto& chunk : chunks_to_index) {
    ++indexed_chunks;
    const auto abstract_segment = chunk.second->get_segment(_column_id);
    const auto segment_size = abstract_segment->size();
    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < segment_size; ++chunk_offset) {
      if ((iter + 1) * _d >= TRAIN_SIZE) {
        Assert((iter + 1) * _d >= TRAIN_SIZE, "training data is too lage for IVFFlat Index.\n");
      }
      const auto value = boost::get<float_array>((*abstract_segment)[chunk_offset]).data();
      memcpy(data + iter * _d, value, sizeof(float) * _d);
      iter++;
    }
  }
  std::cout << "get training data: " << per_table_index_timer.lap_formatted() << std::endl;
  train(iter, data);
  delete[] data;
  // return indexed_chunks;
}

void IVFFlatIndex::insert(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index,
                          const int64_t* labels) {
  std::cout << "insert IVFFlatIndex: chunks_to_index" << std::endl;
  auto indexed_chunks = size_t{0};
  float* data = new float[chunks_to_index.size() * Chunk::DEFAULT_SIZE * _d];
  int iter = 0;
  auto per_table_index_timer = Timer{};
  for (const auto& chunk : chunks_to_index) {
    if (_indexed_chunk_ids.contains(chunk.first)) {
      continue;
    }
    _indexed_chunk_ids.insert(chunk.first);
    ++indexed_chunks;
    const auto abstract_segment = chunk.second->get_segment(_column_id);
    const auto segment_size = abstract_segment->size();
    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < segment_size; ++chunk_offset) {
      if ((iter + 1) * _d >= TRAIN_SIZE) {
        Assert((iter + 1) * _d >= TRAIN_SIZE, "training data is too lage for IVFFlat Index.\n");
      }
      const auto value = boost::get<float_array>((*abstract_segment)[chunk_offset]).data();
      memcpy(data + iter * _d, value, sizeof(float) * _d);
      iter++;
    }
  }
  std::cout << "get adding data: " << per_table_index_timer.lap_formatted() << std::endl;
  insert(iter, data, labels);
  delete[] data;
  // return indexed_chunks;
}

void IVFFlatIndex::insert(int64_t n, const float* data, const int64_t* labels) {
  std::cout << "insert IVFFlatIndex: const float* data" << std::endl;
  auto per_table_index_timer = Timer{};
  if (labels == nullptr) {
    _index->add(n, data);
  } else {
    _index->add_with_ids(n, data, labels);
  }
  std::cout << per_table_index_timer.lap_formatted() << std::endl;
}

// size_t IVFFlatIndex::remove(const std::vector<ChunkID>&){

// }

void IVFFlatIndex::similar_k(const float* query, int64_t* I, float* D, int k) {
  std::cout << "We do not recommend you to search for single queries." << std::endl;
  _index->search(1, query, k, D, I);
}

void IVFFlatIndex::range_similar_k(size_t n, const float* query, int64_t* I, float* D, int k) {
  _index->search(n, query, k, D, I);
}
}  // namespace hyrise
