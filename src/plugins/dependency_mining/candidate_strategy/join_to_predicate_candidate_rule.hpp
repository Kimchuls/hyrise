#pragma once

#include "abstract_dependency_candidate_rule.hpp"

namespace opossum {

class JoinToPredicateCandidateRule : public AbstractDependencyCandidateRule {
 public:
  JoinToPredicateCandidateRule();
  std::vector<DependencyCandidate> apply_to_node(const std::shared_ptr<const AbstractLQPNode>& lqp_node,
                                                 const size_t priority) const final;
};

}  // namespace opossum
