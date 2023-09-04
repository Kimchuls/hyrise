#include "ivf_flat_index.hpp"
#include <boost/lexical_cast.hpp>
#include <memory>
#include "index_io.hpp"
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

float* bvecs_read(const char* input_file, int num_vectors_to_read, size_t* d_out, size_t* n_out) {
  float* result = nullptr;
  unsigned char* tmp = nullptr;
  FILE* file = fopen(input_file, "r");  // Open the file in binary read mode
  if (!file) {
    std::cerr << "Error: Unable to open file " << input_file << std::endl;
    return result;
  }

  int32_t d;
  int32_t n;
  fread(&d, sizeof(int32_t), 1, file);
  *d_out = d;
  fseek(file, 0, SEEK_END);
  long file_size = ftell(file);
  long total_n = file_size / (d + 4);

  num_vectors_to_read = std::min(num_vectors_to_read, (int)total_n);
  *n_out = total_n;
  n = num_vectors_to_read;
  int64_t tmp_elements = (int64_t)n * (d + 4);
  int64_t num_elements = (int64_t)num_vectors_to_read * d;
  result = new float[num_elements];
  tmp = new unsigned char[tmp_elements];

  fseek(file, 0, SEEK_SET);
  fread(tmp, n * (d + 4), 1, file);

  for (int64_t i = 0; i < num_vectors_to_read; ++i)
    for (int j = 0; j < d; ++j)
      result[i * d + j] = static_cast<float>(tmp[i * (d + 4) + 4 + j]);

  fclose(file);
  delete[] tmp;
  return result;
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
                           ColumnID column_id, int d = 128, int testing_data = 20)
    : AbstractVectorIndex{get_vector_index_type_of<IVFFlatIndex>()} {
  Assert(!chunks_to_index.empty(), "IVFFlatIndex requires chunks_to_index not to be empty.");
  _column_id = column_id;
  _d = d;
  // _nlist = 1000;
  _nlist = 3162;
  _quantizer = new vindex::IndexFlatL2(_d);
  _index = new vindex::IndexIVFFlat(_quantizer, _d, _nlist);
  _index->nprobe = testing_data;
  // _is_trained = &(_index->is_trained);

  // float* xb = new float[d * nb];
  // load_data(base_filepath, xb, nb, d);
  // srand((int)time(0));
  // std::vector<float> trainvecs(nb / 100 * d);
  // for (int i = 0; i < nb / 100; i++) {
  //   int rng = (rand() % (nb + 1));
  //   for (int j = 0; j < d; j++) {
  //     trainvecs[d * i + j] = xb[rng * d + j];
  //   }
  // }
  // printf("IVFFLAT\n");
  // train(nb / 100, trainvecs.data());
  // delete[] xb;

  // std::cout<<"_d: "<<_d<<std::endl;

  nb = 10'000'000;
  base_filepath = "/home/jin467/dataset/bigann_10m_base.bvecs";
  float* xb = new float[d * nb];
  size_t dout, nout;
  xb = bvecs_read(base_filepath, nb, &dout, &nout);
  srand((int)time(0));
  std::vector<float> trainvecs(nb / 100 * d);
  for (int i = 0; i < nb / 100; i++) {
    int rng = (rand() % (nb + 1));
    for (int j = 0; j < d; j++) {
      trainvecs[d * i + j] = xb[rng * d + j];
    }
  }
  printf("IVFFLAT\n");
  train(nb / 100, trainvecs.data());
  delete[] xb;

  std::cout << "_d: " << _d << std::endl;
  insert(chunks_to_index);
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
  // _is_trained = &(_index->is_trained);

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
  train(nb / 100, trainvecs.data());
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
  // _is_trained = &(_index->is_trained);

  //TODO: training and inserting
}

bool IVFFlatIndex::is_index_for(const ColumnID column_id) const {
  return column_id == _column_id;
}

ColumnID IVFFlatIndex::get_indexed_column_id() const {
  return _column_id;
}

void IVFFlatIndex::train(int64_t n, const float* data) {
  // std::cout << "train IVFFlatIndex: const float* data" << std::endl;
  if (_index->is_trained) {
    std::cout << "is_trained = true" << std::endl;
    return;
  }
  printf("training\n");
  auto per_table_index_timer = Timer{};
  _index->train(n, data);
  std::cout << per_table_index_timer.lap_formatted() << std::endl;
}

void IVFFlatIndex::train(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index) {
  if (_index->is_trained) {
    std::cout << "is_trained = true" << std::endl;
    return;
  }
  auto indexed_chunks = size_t{0};
  float* data = new float[TRAIN_SIZE];
  int iter = 0;
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
  train(iter, data);
  delete[] data;
}

void IVFFlatIndex::insert(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index,
                          const int64_t* labels) {
  // std::cout << "insert IVFFlatIndex: chunks_to_index" << std::endl;
  auto indexed_chunks = size_t{0};
  float* data = new float[chunks_to_index.size() * Chunk::DEFAULT_SIZE * _d];
  int iter = 0;
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
  std::cout << "IVFFlatIndex::range_similar_k" << std::endl;
  auto per_table_index_timer = Timer{};
  _index->search(n, query, k, D, I);
  std::cout << per_table_index_timer.lap_formatted() << std::endl;
}

void IVFFlatIndex::save_index(const std::string& save_path) {
  // std::cout<<"not supported"<<std::endl;
  vindex::write_index(_index, save_path.c_str());
}
}  // namespace hyrise
