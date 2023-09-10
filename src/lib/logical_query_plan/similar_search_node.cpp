#include "similar_search_node.hpp"

#include <cmath>
#include <sstream>
#include <string>
#include <vector>

#include "expression/expression_utils.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

// SimilarSearchNode::SimilarSearchNode(const int init_k_for_similar_search, const std::string init_column_name,
//                                      const std::string init_index_name,
//                                      std::vector<std::vector<float>*>* init_vectorQueries)
//     : AbstractLQPNode(LQPNodeType::SimilarSearch),
//       k_for_similar_search(init_k_for_similar_search),
//       column_name(init_column_name),
//       index_name(init_index_name) {
//   Assert(init_vectorQueries->size() > 0, "Expected more than zero queries");
//   dim = (*init_vectorQueries)[0]->size();
//   nq = init_vectorQueries->size();
//   queries = new float[dim * nq];
//   printf("query data length: %d", dim);
//   for (size_t iter_vectorQueries = 0; iter_vectorQueries < nq; iter_vectorQueries++) {
//     memcpy(queries + iter_vectorQueries * dim, (*init_vectorQueries)[iter_vectorQueries]->data(), sizeof(float) * dim);
//     if (iter_vectorQueries < 2) {
//       printf("origin data /first 20: ");
//       for (int x = 0; x < 20; x++)
//         printf("%f ", (*(*init_vectorQueries)[iter_vectorQueries])[x]);
//       printf("\n");
//     }
//   }
//   for (int y = 0; y < 2; y++) {
//     printf("extract data /first 20: ");
//     for (int x = 0; x < 20; x++)
//       printf("%f ", queries[y * dim + x]);
//     printf("\n");
//   }
// }

SimilarSearchNode::SimilarSearchNode(const std::vector<std::shared_ptr<AbstractExpression>>& expressions,
                                     const int init_k_for_similar_search, const std::string init_column_name,
                                     const std::string init_index_name, const int init_dim, const int init_nq,
                                     float* init_queries)
    : AbstractLQPNode(LQPNodeType::SimilarSearch),
      k_for_similar_search(init_k_for_similar_search),
      column_name(init_column_name),
      index_name(init_index_name),
      dim(init_dim),
      nq(init_nq),
      queries(init_queries) {
  node_expressions.resize(expressions.size());
  std::copy(expressions.begin(), expressions.end(), node_expressions.begin());
}

std::string SimilarSearchNode::description(const DescriptionMode mode) const {
  const auto expression_mode = _expression_description_mode(mode);
  std::stringstream stream;
  stream << "[Similar Search] " << column_name << ", " << index_name << ", dim: " << dim
         << ", k: " << k_for_similar_search << "; ";
  stream << "SELECT from: [";
  for (auto expression_idx = ColumnID{0}; expression_idx < node_expressions.size(); ++expression_idx) {
    stream << node_expressions[expression_idx]->description(expression_mode);
    if (expression_idx + 1u < node_expressions.size()) {
      stream << ", ";
    }
  }
  stream << "] ";
  return stream.str();
}

UniqueColumnCombinations SimilarSearchNode::unique_column_combinations() const {
  return _forward_left_unique_column_combinations();
}

size_t SimilarSearchNode::_on_shallow_hash() const {
  auto hash = size_t{0};
  boost::hash_combine(hash, k_for_similar_search);
  boost::hash_combine(hash, column_name);
  boost::hash_combine(hash, index_name);
  boost::hash_combine(hash, dim);
  boost::hash_combine(hash, queries);
  return hash;
}

std::shared_ptr<AbstractLQPNode> SimilarSearchNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  const auto select_from_expressions =
      std::vector<std::shared_ptr<AbstractExpression>>{node_expressions.begin(), node_expressions.end()};
  return SimilarSearchNode::make(expressions_copy_and_adapt_to_different_lqp(select_from_expressions, node_mapping),
                                 k_for_similar_search, column_name, index_name, dim, nq, queries);
}

bool SimilarSearchNode::compare_float_array(const float* queries1, const float* queries2) const {
  size_t length = dim * nq;
  for (size_t i = 0; i < length; i++) {
    if (std::fabs(queries1[i] - queries2[i]) > 1e-6)
      return false;
  }
  return true;
}

bool SimilarSearchNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& similar_search_node = static_cast<const SimilarSearchNode&>(rhs);

  return k_for_similar_search == similar_search_node.k_for_similar_search && nq == similar_search_node.nq &&
         column_name == similar_search_node.column_name && index_name == similar_search_node.index_name &&
         dim == similar_search_node.dim && compare_float_array(queries, similar_search_node.queries) &&
         expressions_equal_to_expressions_in_different_lqp(node_expressions, similar_search_node.node_expressions,
                                                           node_mapping);
}

}  // namespace hyrise
