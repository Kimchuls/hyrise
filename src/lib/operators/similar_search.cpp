#include "similar_search.hpp"

#include <algorithm>
#include <cmath>
#include <fstream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <string>
#include "all_type_variant.hpp"
#include "expression/evaluation/expression_evaluator.hpp"
#include "expression/expression_utils.hpp"
#include "storage/index/abstract_vector_index.hpp"
#include "storage/reference_segment.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#define MIN(a, b) (((a) < (b)) ? (a) : (b))

namespace hyrise {

SimilarSearch::SimilarSearch(const std::shared_ptr<const AbstractOperator>& input_operator,
                             const int init_k_for_similar_search, const std::string init_column_name,
                             const std::string init_index_name, const int init_dim, const int init_nq,
                             float* init_queries)
    : AbstractReadOnlyOperator(OperatorType::SimilarSearch, input_operator),
      k_for_similar_search(init_k_for_similar_search),
      column_name(init_column_name),
      index_name(init_index_name),
      dim(init_dim),
      nq(init_nq)
// queries(init_queries)
{
  queries = new float[dim * nq];
  memcpy(queries, init_queries, dim * nq * sizeof(float));
}

const std::string& SimilarSearch::name() const {
  static const auto name = std::string{"SimilarSearch"};
  return name;
}

// std::shared_ptr<AbstractExpression> SimilarSearch::row_count_expression() const {
//   return _row_count_expression;
// }

std::shared_ptr<AbstractOperator> SimilarSearch::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  return std::make_shared<SimilarSearch>(copied_left_input, k_for_similar_search, column_name, index_name, dim, nq,
                                         queries);
}

std::shared_ptr<const Table> SimilarSearch::_on_execute() {
  //TODO: printf("This similar search does not consider complex query like filter.");
  const auto input_table = left_input_table();
  const auto meta_chunk = input_table->get_chunk(ChunkID{0});
  const auto ref_segment_in = std::dynamic_pointer_cast<const ReferenceSegment>(meta_chunk->get_segment(ColumnID{0}));
  auto referenced_table = std::shared_ptr<const Table>{};
  referenced_table = ref_segment_in->referenced_table();
  // printf("referenced table %d\n",referenced_table->type());
  // printf("%ld %ld\n",referenced_table->chunk_count(),referenced_table->get_chunk(ChunkID{0})->column_count());

  const auto float_array_indexs = referenced_table->get_table_indexes_vector();
  // printf("vector index size: %ld\n", float_array_indexs.size());
  std::shared_ptr<AbstractVectorIndex> vector_index;
  for (auto& float_array_index : float_array_indexs) {
    // std::cout << "float_array_index->name(): " << float_array_index->name() << std::endl;
    // std::cout << "index name(): " << index_name << std::endl;
    if (float_array_index->name() == index_name) {
      if (index_name == "ivfflat") {
        vector_index = float_array_index;
        break;
      } else if (index_name == "hnsw") {
        vector_index = float_array_index;
// std::cout << "index name(): " << index_name << std::endl;
        break;
      }
    }
  }
  Assert(vector_index, "no such index in the table");
  // const auto column_id = referenced_table->column_id_by_name(column_name);
  // if (referenced_table->column_data_type(column_id) != DataType::Vector) {
  //   printf("\" column \"%s\" is not existed. Replacing it.\n", column_name);
  //   return nullptr;
  // }

  int64_t* I = new int64_t[k_for_similar_search * nq];
  float* D = new float[k_for_similar_search * nq];
  vector_index->range_similar_k(nq, queries, I, D, k_for_similar_search);
  // FILE* writes = fopen("outputs.txt", "w");
  // fprintf(writes, "print\n");
  // for (int i = 0; i < 128 * nq; i++) {
  //   fprintf(writes, "%f, ", queries[i]);
  //   if ((i + 1) % 128 == 0)
  //     fprintf(writes, "\n");
  // }
  // for (int i = 0; i < k_for_similar_search * nq; i++) {
  //   fprintf(writes, "%ld, ", I[i]);
  //   if ((i + 1) % k_for_similar_search == 0)
  //     fprintf(writes, "\n");
  // }
  // fclose(writes);

  auto per_table_index_timer = Timer{};
  // FILE* writes = fopen("output.txt", "w");
  const auto chunk_count = referenced_table->chunk_count();
  const auto column_count = referenced_table->column_count();
  const auto column_definitions = referenced_table->column_definitions();
  // printf("count: %d, %d\n", chunk_count, column_count);

  auto output_chunks = std::vector<std::shared_ptr<Chunk>>{};
  int times_for_chunk = std::ceil(1.0 * k_for_similar_search * nq / Chunk::DEFAULT_SIZE);
  // std::cout << times_for_chunk << std::endl;

  for (int round_for_chunk = 0; round_for_chunk < times_for_chunk; round_for_chunk++) {
    Segments output_segments;

    for (auto column_id = ColumnID{0}; column_id < column_count; column_id++) {
      resolve_data_type(column_definitions[column_id].data_type, [&](auto type) {
        using ColumnDataType = typename decltype(type)::type;
        // output_segments.push_back();

        const auto target_value_segment =
            std::make_shared<ValueSegment<ColumnDataType>>(column_definitions[column_id].nullable, Chunk::DEFAULT_SIZE);
        // const auto target_value_segment = std::dynamic_pointer_cast<ValueSegment<ColumnDataType>>(target_segment);
        auto offset_for_this_round =
            MIN((k_for_similar_search * nq - Chunk::DEFAULT_SIZE * round_for_chunk), (Chunk::DEFAULT_SIZE));
        // printf("offset_for_this_round: %d, Chunk::DEFAULT_SIZE: %d, round_for_chunk:%d\n", offset_for_this_round,
        //        Chunk::DEFAULT_SIZE, round_for_chunk);
        for (size_t i = 0; i < offset_for_this_round; i++) {
          auto chunk_id = ChunkID{(unsigned int)(I[i + Chunk::DEFAULT_SIZE * round_for_chunk] / Chunk::DEFAULT_SIZE)};
          auto chunk_offset =
              ChunkOffset{(unsigned int)(I[i + Chunk::DEFAULT_SIZE * round_for_chunk] % Chunk::DEFAULT_SIZE)};
          // if (chunk_offset == INVALID_CHUNK_OFFSET) {
          //   printf("i: %d, Chunk::DEFAULT_SIZE: %d, round_for_chunk:%d\n", i, Chunk::DEFAULT_SIZE, round_for_chunk);
          //   printf("%d %d %d\n", I[i + Chunk::DEFAULT_SIZE * round_for_chunk],
          //          (I[i + Chunk::DEFAULT_SIZE * round_for_chunk] % Chunk::DEFAULT_SIZE), chunk_offset);
          // }
          auto searched_chunk = referenced_table->get_chunk(chunk_id);
          auto searched_segment = searched_chunk->get_segment(column_id);
          target_value_segment->append((*searched_segment)[chunk_offset]);
          // auto x = get_AllTypeVariant_to_string<std::string>((*searched_segment)[chunk_offset]);
          // fprintf(writes, "%s\n", x.c_str());
        }
        // std::cout << "size: " << target_value_segment->values().size() << std::endl;
        output_segments.push_back(target_value_segment);
      });
    }
    auto output_chunk = std::make_shared<Chunk>(std::move(output_segments));
    output_chunk->finalize();
    output_chunks.emplace_back(output_chunk);
  }
  // fclose(writes);
  std::cout << "(" << per_table_index_timer.lap_formatted() << ")" << std::endl;
  auto result =
      std::make_shared<Table>(referenced_table->column_definitions(), TableType::Data, std::move(output_chunks));
  // printf("chunk_count %d %d\n",result->chunk_count(),result->column_count());
  delete[] I;
  delete[] D;
  return result;
  // return referenced_table;
}

void SimilarSearch::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {
  // expression_set_parameters(_row_count_expression, parameters);
}

void SimilarSearch::_on_set_transaction_context(const std::weak_ptr<TransactionContext>& transaction_context) {
  // expression_set_transaction_context(_row_count_expression, transaction_context);
}

}  // namespace hyrise
