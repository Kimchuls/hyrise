#pragma once

#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"
#include "types.hpp"

namespace hyrise {

/**
 * This node type represents sorting operations as defined in ORDER BY clauses.
 */
class SimilarSearchNode : public EnableMakeForLQPNode<SimilarSearchNode>, public AbstractLQPNode {
 public:
  // explicit SimilarSearchNode(const std::vector<std::shared_ptr<AbstractExpression>>& expressions,
  //                            const int init_k_for_similar_search, const std::string init_column_name,
  //                            const std::string init_index_name, std::vector<std::vector<float>*>* init_vectorQueries);
  SimilarSearchNode(
    const std::vector<std::shared_ptr<AbstractExpression>>& expressions,
                    const int init_k_for_similar_search, const std::string init_column_name,
                    const std::string init_index_name, const int init_dim, const int init_nq, float* init_queries);

  std::string description(const DescriptionMode mode = DescriptionMode::Short) const override;
  bool compare_float_array(const float* queries1, const float* queries2) const;

  // Forwards unique column combinations from the left input node.
  UniqueColumnCombinations unique_column_combinations() const override;

  const int k_for_similar_search;
  const std::string column_name;
  const std::string index_name;
  int dim;
  int nq;
  float* queries;

 protected:
  size_t _on_shallow_hash() const override;
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace hyrise
