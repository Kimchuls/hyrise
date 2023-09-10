#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "expression/abstract_expression.hpp"

namespace hyrise {
// operator to SimilarSearch the input to n rows
class SimilarSearch : public AbstractReadOnlyOperator {
 public:
  SimilarSearch(const std::shared_ptr<const AbstractOperator>& input_operator, const int init_k_for_similar_search,
                const std::string init_column_name, const std::string init_index_name, const int init_dim,
                const int init_nq, float* init_queries);

  const std::string& name() const override;

  // std::shared_ptr<AbstractExpression> row_count_expression() const;
  const int k_for_similar_search;
  const std::string column_name;
  const std::string index_name;
  const int dim;
  const int nq;
  float* queries;

 protected:
  std::shared_ptr<const Table> _on_execute() override;
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  void _on_set_transaction_context(const std::weak_ptr<TransactionContext>& transaction_context) override;

 private:
  // std::shared_ptr<AbstractExpression> _row_count_expression;
};
}  // namespace hyrise
