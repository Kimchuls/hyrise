#include "join_index.hpp"

#include <map>
#include <memory>
#include <numeric>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "join_nested_loop.hpp"
#include "multi_predicate_join/multi_predicate_join_evaluator.hpp"
#include "resolve_type.hpp"
#include "storage/segment_iterate.hpp"
#include "type_comparison.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"
#include "utils/timer.hpp"

namespace opossum {

/*
 * This is an index join implementation. It expects to find an index on the index side column.
 * It can be used for all join modes except JoinMode::Cross.
 * For the remaining join types or if no index is found it falls back to a nested loop join.
 */

bool JoinIndex::supports(const JoinConfiguration config) {
  if (!config.left_table_type || !config.right_table_type || !config.index_side) {
    Fail("Table types and index side are required to make support decisions for the index join.");
  } else {
    TableType index_side_table_type;
    if (*config.index_side == IndexSide::Left) {
      index_side_table_type = *config.left_table_type;
    } else {
      index_side_table_type = *config.right_table_type;
    }

    if (index_side_table_type == TableType::References && config.join_mode != JoinMode::Inner) {
      return false;  // non-inner index joins on reference tables are not supported
    }

    return !config.secondary_predicates;  // multi predicate index joins are not supported
  }
}

JoinIndex::JoinIndex(const std::shared_ptr<const AbstractOperator>& left,
                     const std::shared_ptr<const AbstractOperator>& right, const JoinMode mode,
                     const OperatorJoinPredicate& primary_predicate,
                     const std::vector<OperatorJoinPredicate>& secondary_predicates, const IndexSide index_side,
                     const std::vector<ColumnID>& pruned_column_ids)
    : AbstractJoinOperator(OperatorType::JoinIndex, left, right, mode, primary_predicate, secondary_predicates,
                           std::make_unique<JoinIndex::PerformanceData>()),
      _index_side(index_side),
      _adjusted_primary_predicate(primary_predicate) {
  if (_index_side == IndexSide::Left) {
    _adjusted_primary_predicate.flip();
  }


  // If some columns have been pruned, the ColumnID to join on may differ from the original index ColumnID.
  // In this step the original index ColumnID is calculated.
  _index_column_id_before_pruning = _adjusted_primary_predicate.column_ids.second;
  for (const auto pruned_column_id : pruned_column_ids) {
    if (pruned_column_id > _index_column_id_before_pruning) {
      break;
    }
    ++_index_column_id_before_pruning;
  }
}

const std::string& JoinIndex::name() const {
  static const auto name = std::string{"JoinIndex"};
  return name;
}

std::string JoinIndex::description(DescriptionMode description_mode) const {
  const auto separator = (description_mode == DescriptionMode::SingleLine ? ' ' : '\n');
  const auto* const index_side_str = _index_side == IndexSide::Left ? "Left" : "Right";

  std::ostringstream stream(AbstractJoinOperator::description(description_mode), std::ios_base::ate);
  stream << separator << "Index side: " << index_side_str;

  return stream.str();
}

const std::vector<ColumnID> JoinIndex::get_pruned_column_ids() const {
  const auto number_of_pruned_column_ids = _index_column_id_before_pruning - _adjusted_primary_predicate.column_ids.second;
  auto pruned_column_ids = std::vector<ColumnID>{static_cast<size_t>(number_of_pruned_column_ids)};

  std::iota(std::begin(pruned_column_ids), std::end(pruned_column_ids), 0);

  return pruned_column_ids;
}

std::shared_ptr<AbstractOperator> JoinIndex::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& copied_right_input,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  const auto copied_pruned_column_ids = get_pruned_column_ids();

  return std::make_shared<JoinIndex>(copied_left_input, copied_right_input, _mode, _primary_predicate,
                                     _secondary_predicates, _index_side, copied_pruned_column_ids);
}

std::shared_ptr<const Table> JoinIndex::_on_execute() {
  Assert(
      supports({_mode, _primary_predicate.predicate_condition,
                left_input_table()->column_data_type(_primary_predicate.column_ids.first),
                right_input_table()->column_data_type(_primary_predicate.column_ids.second),
                !_secondary_predicates.empty(), left_input_table()->type(), right_input_table()->type(), _index_side}),
      "JoinIndex doesn't support these parameters");

  if (_index_side == IndexSide::Left) {
    _probe_input_table = right_input_table();
    _index_input_table = left_input_table();
  } else {
    _probe_input_table = left_input_table();
    _index_input_table = right_input_table();
  }

  _index_matches.resize(_index_input_table->chunk_count());
  _probe_matches.resize(_probe_input_table->chunk_count());

  const auto is_semi_or_anti_join =
      _mode == JoinMode::Semi || _mode == JoinMode::AntiNullAsFalse || _mode == JoinMode::AntiNullAsTrue;

  const auto track_probe_matches = _mode == JoinMode::FullOuter ||
                                   (_mode == JoinMode::Left && _index_side == IndexSide::Right) ||
                                   (_mode == JoinMode::Right && _index_side == IndexSide::Left) ||
                                   (is_semi_or_anti_join && _index_side == IndexSide::Right);
  const auto track_index_matches = _mode == JoinMode::FullOuter ||
                                   (_mode == JoinMode::Left && _index_side == IndexSide::Left) ||
                                   (_mode == JoinMode::Right && _index_side == IndexSide::Right) ||
                                   (is_semi_or_anti_join && _index_side == IndexSide::Left);

  if (track_probe_matches) {
    const auto chunk_count = _probe_input_table->chunk_count();
    for (ChunkID probe_chunk_id{0}; probe_chunk_id < chunk_count; ++probe_chunk_id) {
      const auto chunk = _probe_input_table->get_chunk(probe_chunk_id);
      Assert(chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

      _probe_matches[probe_chunk_id].resize(chunk->size());
    }
  }

  if (track_index_matches) {
    const auto chunk_count = _index_input_table->chunk_count();
    for (ChunkID index_chunk_id{0}; index_chunk_id < chunk_count; ++index_chunk_id) {
      const auto chunk = _index_input_table->get_chunk(index_chunk_id);
      Assert(chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

      _index_matches[index_chunk_id].resize(chunk->size());
    }
  }

  _probe_pos_list = std::make_shared<RowIDPosList>();
  _index_pos_list = std::make_shared<RowIDPosList>();

  auto pos_list_size_to_reserve =
      std::max(uint64_t{100}, std::min(_probe_input_table->row_count(), _index_input_table->row_count()));

  _probe_pos_list->reserve(pos_list_size_to_reserve);
  _index_pos_list->reserve(pos_list_size_to_reserve);

  auto& join_index_performance_data = static_cast<PerformanceData&>(*performance_data);

  auto secondary_predicate_evaluator = MultiPredicateJoinEvaluator{*_probe_input_table, *_index_input_table, _mode, {}};

  auto index_joining_duration = std::chrono::nanoseconds{0};
  auto nested_loop_joining_duration = std::chrono::nanoseconds{0};
  Timer timer;
  if (_mode == JoinMode::Inner && _index_input_table->type() == TableType::References &&
      _secondary_predicates.empty()) {  // INNER REFERENCE JOIN
    // Scan all chunks for index input
    const auto chunk_count_index_input_table = _index_input_table->chunk_count();
    for (ChunkID index_chunk_id{0}; index_chunk_id < chunk_count_index_input_table; ++index_chunk_id) {
      const auto index_chunk = _index_input_table->get_chunk(index_chunk_id);
      Assert(index_chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

      const auto& reference_segment =
          std::dynamic_pointer_cast<ReferenceSegment>(index_chunk->get_segment(_primary_predicate.column_ids.second));
      Assert(reference_segment != nullptr,
             "Non-empty index input table (reference table) has to have only reference segments.");
      auto index_data_table = reference_segment->referenced_table();
      const std::vector<ColumnID> index_data_table_column_ids{reference_segment->referenced_column_id()};
      const auto& reference_segment_pos_list = reference_segment->pos_list();

      if (reference_segment_pos_list->references_single_chunk()) {
        const auto index_data_table_chunk = index_data_table->get_chunk((*reference_segment_pos_list)[0].chunk_id);
        Assert(index_data_table_chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");
        const auto& indexes = index_data_table_chunk->get_indexes(index_data_table_column_ids);

        if (!indexes.empty()) {
          // We assume the first index to be efficient for our join
          // as we do not want to spend time on evaluating the best index inside of this join loop

          if (indexes.size() > 1) {
            PerformanceWarning("There are multiple indexes available, but only the first one is used.");
          }

          const auto& index = indexes.front();

          _scan_probe_side_input([&](auto probe_iter, const auto probe_end, const auto probe_chunk_id) {
            _reference_join_two_segments_using_chunk_index(probe_iter, probe_end, probe_chunk_id, index_chunk_id, index,
                                                     reference_segment_pos_list);
          });

          index_joining_duration += timer.lap();
          join_index_performance_data.chunks_scanned_with_index++;
        } else {
          _fallback_nested_loop(index_chunk_id, track_probe_matches, track_index_matches, is_semi_or_anti_join,
                                secondary_predicate_evaluator);
          nested_loop_joining_duration += timer.lap();
        }
      } else {
        _fallback_nested_loop(index_chunk_id, track_probe_matches, track_index_matches, is_semi_or_anti_join,
                              secondary_predicate_evaluator);
        nested_loop_joining_duration += timer.lap();
      }
    }
  } else {  // DATA JOIN since only inner joins are supported for a reference table on the index side
    // Here we prefer to use table indexes if the join supports them. If no table index exists or other predicates than
    // Equals or NotEquals are requested, chunk indexes are used. If no chunk index exists, NestedLoopJoin is used as a
    // fallback solution.
    const auto& table_indexes = _index_input_table->get_table_indexes(_index_column_id_before_pruning);

    if (!table_indexes.empty() &&
        (_adjusted_primary_predicate.predicate_condition == PredicateCondition::Equals ||
         _adjusted_primary_predicate.predicate_condition == PredicateCondition::NotEquals)) {  // table-based index join
      const auto chunk_count_index_input_table = _index_input_table->chunk_count();
      auto total_indexed_chunk_ids = std::set<ChunkID>{};

      // We do not take multiple table indexes created for the same column into account. For now, we only use the first
      // available table index. Theoretically, multiple table index storing index entries for different chunk subsets,
      // e.g., ChunkIDs{1,3} and ChunkIDs{0,2,7}, can be used in combination.
      if (table_indexes.size() > 1) {
        PerformanceWarning("There are multiple table indexes available, but only the first one is used.");
      }

      const auto& table_index = table_indexes[0];
      const auto& indexed_chunk_ids = table_index->get_indexed_chunk_ids();
      total_indexed_chunk_ids.insert(indexed_chunk_ids.begin(), indexed_chunk_ids.end());

      _scan_probe_side_input([&](auto probe_iter, const auto probe_end, const auto probe_chunk_id) {
        _data_join_two_segments_using_table_index(probe_iter, probe_end, probe_chunk_id, table_index);
      });

      index_joining_duration += timer.lap();
      ++join_index_performance_data.chunks_scanned_with_index;

      // Check if chunk was indexed in one of the table indexes, thus no need to join it again. Otherwise perform
      // NestedLoopJoin on the not-indexed chunk.
      auto total_indexed_iter = total_indexed_chunk_ids.begin();
      for (auto index_side_chunk_id = ChunkID{0}; index_side_chunk_id < chunk_count_index_input_table;
           ++index_side_chunk_id) {
        if (*total_indexed_iter == index_side_chunk_id && total_indexed_iter != total_indexed_chunk_ids.end()) {
          ++total_indexed_iter;
          continue;
        } else {
          _fallback_nested_loop(index_side_chunk_id, track_probe_matches, track_index_matches, is_semi_or_anti_join,
                                secondary_predicate_evaluator);
          nested_loop_joining_duration += timer.lap();
        }
      }
    } else {  // chunk-based index join
      const auto chunk_count_index_input_table = _index_input_table->chunk_count();
      for (ChunkID index_chunk_id{0}; index_chunk_id < chunk_count_index_input_table; ++index_chunk_id) {
        const auto index_chunk = _index_input_table->get_chunk(index_chunk_id);
        Assert(index_chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

        const auto& indexes =
            index_chunk->get_indexes(std::vector<ColumnID>{_adjusted_primary_predicate.column_ids.second});
        if (!indexes.empty()) {
          // We assume the first index to be efficient for our join as we do not want to spend time on evaluating the
          // best index inside of this join loop.

          if (indexes.size() > 1) {
            PerformanceWarning("There are multiple indexes available, but only the first one is used.");
          }
          
          const auto& index = indexes.front();

          _scan_probe_side_input([&](auto probe_iter, const auto probe_end, const auto probe_chunk_id) {
            _data_join_two_segments_using_chunk_index(probe_iter, probe_end, probe_chunk_id, index_chunk_id, index);
          });

          index_joining_duration += timer.lap();
          join_index_performance_data.chunks_scanned_with_index++;
        } else {
          _fallback_nested_loop(index_chunk_id, track_probe_matches, track_index_matches, is_semi_or_anti_join,
                                secondary_predicate_evaluator);
          nested_loop_joining_duration += timer.lap();
        }
      }
    }
    _append_matches_non_inner(is_semi_or_anti_join);
  }

  // write output chunks
  Segments output_segments;

  if (_index_side == IndexSide::Left) {
    _write_output_segments(output_segments, _index_input_table, _index_pos_list);
  } else {
    _write_output_segments(output_segments, _probe_input_table, _probe_pos_list);
  }

  if (!is_semi_or_anti_join) {
    if (_index_side == IndexSide::Left) {
      _write_output_segments(output_segments, _probe_input_table, _probe_pos_list);
    } else {
      _write_output_segments(output_segments, _index_input_table, _index_pos_list);
    }
  }
  join_index_performance_data.set_step_runtime(OperatorSteps::OutputWriting, timer.lap());
  join_index_performance_data.set_step_runtime(OperatorSteps::IndexJoining, index_joining_duration);
  join_index_performance_data.set_step_runtime(OperatorSteps::NestedLoopJoining, nested_loop_joining_duration);

  if (join_index_performance_data.chunks_scanned_with_index <
      join_index_performance_data.chunks_scanned_without_index) {
    PerformanceWarning(std::string("Only ") + std::to_string(join_index_performance_data.chunks_scanned_with_index) +
                       " of " +
                       std::to_string(join_index_performance_data.chunks_scanned_with_index +
                                      join_index_performance_data.chunks_scanned_without_index) +
                       " chunks processed using an index.");
  }

  auto chunks = std::vector<std::shared_ptr<Chunk>>{};
  if (output_segments.at(0)->size() > 0) {
    chunks.emplace_back(std::make_shared<Chunk>(std::move(output_segments)));
  }
  return _build_output_table(std::move(chunks));
}

template <typename Functor>
void JoinIndex::_scan_probe_side_input(const Functor& functor) {
  // Scan all chunks from the probe side input
  const auto chunk_count_probe_input_table = _probe_input_table->chunk_count();
  for (ChunkID probe_chunk_id{0}; probe_chunk_id < chunk_count_probe_input_table; ++probe_chunk_id) {
    const auto chunk = _probe_input_table->get_chunk(probe_chunk_id);
    Assert(chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

    const auto& probe_segment = chunk->get_segment(_adjusted_primary_predicate.column_ids.first);
    segment_with_iterators(
        *probe_segment, [&](auto probe_iter, const auto probe_end) { functor(probe_iter, probe_end, probe_chunk_id); });
  }
}

void JoinIndex::_fallback_nested_loop(const ChunkID index_chunk_id, const bool track_probe_matches,
                                      const bool track_index_matches, const bool is_semi_or_anti_join,
                                      MultiPredicateJoinEvaluator& secondary_predicate_evaluator) {
  PerformanceWarning("Fallback nested loop used.");
  auto& join_index_performance_data = static_cast<PerformanceData&>(*performance_data);

  const auto index_chunk = _index_input_table->get_chunk(index_chunk_id);
  Assert(index_chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

  const auto& index_segment = index_chunk->get_segment(_adjusted_primary_predicate.column_ids.second);
  const auto& index_pos_list_size_pre_fallback = _index_pos_list->size();

  const auto chunk_count = _probe_input_table->chunk_count();
  for (ChunkID probe_chunk_id{0}; probe_chunk_id < chunk_count; ++probe_chunk_id) {
    const auto chunk = _probe_input_table->get_chunk(probe_chunk_id);
    Assert(chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

    const auto& probe_segment = chunk->get_segment(_adjusted_primary_predicate.column_ids.first);
    JoinNestedLoop::JoinParams params{*_probe_pos_list,
                                      *_index_pos_list,
                                      _probe_matches[probe_chunk_id],
                                      _index_matches[index_chunk_id],
                                      track_probe_matches,
                                      track_index_matches,
                                      _mode,
                                      _adjusted_primary_predicate.predicate_condition,
                                      secondary_predicate_evaluator,
                                      !is_semi_or_anti_join};
    JoinNestedLoop::_join_two_untyped_segments(*probe_segment, *index_segment, probe_chunk_id, index_chunk_id, params);
  }
  const auto& index_pos_list_size_post_fallback = _index_pos_list->size();
  const auto& count_index_positions = index_pos_list_size_post_fallback - index_pos_list_size_pre_fallback;
  std::fill_n(std::back_inserter(_index_pos_dereferenced), count_index_positions, false);
  join_index_performance_data.chunks_scanned_without_index++;
}

// Join loop that joins two segments of two columns using an iterator for the probe side,
// and a table index for the index side.
template <typename ProbeIterator>
void JoinIndex::_data_join_two_segments_using_table_index(ProbeIterator probe_iter, ProbeIterator probe_end,
                                                          const ChunkID probe_chunk_id,
                                                          const std::shared_ptr<AbstractTableIndex>& table_index) {
  for (; probe_iter != probe_end; ++probe_iter) {
    const auto probe_side_position = *probe_iter;

    std::vector<TableIndexRange> index_ranges{};
    // Index ranges can consist of a single range (e.g. Equals) or two independent ranges at max (e.g. NotEquals).
    index_ranges.reserve(2);

    // AntiNullAsTrue is the only join mode in which comparisons with null-values are evaluated as "true".
    // If the probe side value is null or at least one null value exists in the indexed join segment, the probe value
    // has a match.
    if (_mode == JoinMode::AntiNullAsTrue) {
      const auto indexed_null_values = table_index->null_cbegin() != table_index->null_cend();
      if (probe_side_position.is_null() || indexed_null_values) {
        index_ranges.emplace_back(TableIndexRange{table_index->cbegin(), table_index->cend()});
        index_ranges.emplace_back(TableIndexRange{table_index->null_cbegin(), table_index->null_cend()});
      }
    } else {
      if (!probe_side_position.is_null()) {
        switch (_adjusted_primary_predicate.predicate_condition) {
          case PredicateCondition::Equals: {
            const auto [index_begin, index_end] = table_index->range_equals(probe_side_position.value());
            index_ranges.emplace_back(TableIndexRange{index_begin, index_end});
            break;
          }
          case PredicateCondition::NotEquals: {
            const auto [not_equals_range_left, not_equals_range_right] =
                table_index->range_not_equals(probe_side_position.value());
            index_ranges.emplace_back(TableIndexRange{not_equals_range_left.first, not_equals_range_left.second});
            index_ranges.emplace_back(TableIndexRange{not_equals_range_right.first, not_equals_range_right.second});
            break;
          }
          default: {
            Fail("Unsupported comparison type encountered");
          }
        }
      }
    }
    for (const auto& [index_begin, index_end] : index_ranges) {
      _append_matches_table_index(index_begin, index_end, probe_side_position.chunk_offset(), probe_chunk_id);
    }
  }
}

// Join loop that joins two segments of two columns using an iterator for the probe side,
// and an index for the index side.
template <typename ProbeIterator>
void JoinIndex::_data_join_two_segments_using_chunk_index(ProbeIterator probe_iter, ProbeIterator probe_end,
                                                    const ChunkID probe_chunk_id, const ChunkID index_chunk_id,
                                                    const std::shared_ptr<AbstractChunkIndex>& chunk_index) {
  for (; probe_iter != probe_end; ++probe_iter) {
    const auto probe_side_position = *probe_iter;
    const auto index_ranges = _chunk_index_ranges_for_value(probe_side_position, chunk_index);
    for (const auto& [index_begin, index_end] : index_ranges) {
      _append_matches_chunk_index(index_begin, index_end, probe_side_position.chunk_offset(), probe_chunk_id, index_chunk_id);
    }
  }
}

template <typename ProbeIterator>
void JoinIndex::_reference_join_two_segments_using_chunk_index(
    ProbeIterator probe_iter, ProbeIterator probe_end, const ChunkID probe_chunk_id, const ChunkID index_chunk_id,
    const std::shared_ptr<AbstractChunkIndex>& chunk_index,
    const std::shared_ptr<const AbstractPosList>& reference_segment_pos_list) {
  for (; probe_iter != probe_end; ++probe_iter) {
    RowIDPosList index_scan_pos_list;
    const auto probe_side_position = *probe_iter;
    const auto index_ranges = _chunk_index_ranges_for_value(probe_side_position, chunk_index);
    for (const auto& [index_begin, index_end] : index_ranges) {
      std::transform(index_begin, index_end, std::back_inserter(index_scan_pos_list),
                     [index_chunk_id](ChunkOffset index_chunk_offset) {
                       return RowID{index_chunk_id, index_chunk_offset};
                     });
    }

    RowIDPosList mutable_ref_seg_pos_list(reference_segment_pos_list->size());
    std::copy(reference_segment_pos_list->begin(), reference_segment_pos_list->end(), mutable_ref_seg_pos_list.begin());
    std::sort(mutable_ref_seg_pos_list.begin(), mutable_ref_seg_pos_list.end());
    std::sort(index_scan_pos_list.begin(), index_scan_pos_list.end());

    RowIDPosList index_table_matches{};
    std::set_intersection(mutable_ref_seg_pos_list.begin(), mutable_ref_seg_pos_list.end(), index_scan_pos_list.begin(),
                          index_scan_pos_list.end(), std::back_inserter(index_table_matches));
    _append_matches_dereferenced(probe_chunk_id, probe_side_position.chunk_offset(), index_table_matches);
  }
}

template <typename SegmentPosition>
std::vector<ChunkIndexRange> JoinIndex::_chunk_index_ranges_for_value(const SegmentPosition probe_side_position,
                                                           const std::shared_ptr<AbstractChunkIndex>& chunk_index) const {
  std::vector<ChunkIndexRange> index_ranges{};
  index_ranges.reserve(2);

  // AntiNullAsTrue is the only join mode in which comparisons with null-values are evaluated as "true".
  // If the probe side value is null or at least one null value exists in the indexed join segment, the probe value
  // has a match.
  if (_mode == JoinMode::AntiNullAsTrue) {
    const auto indexed_null_values = chunk_index->null_cbegin() != chunk_index->null_cend();
    if (probe_side_position.is_null() || indexed_null_values) {
      index_ranges.emplace_back(ChunkIndexRange{chunk_index->cbegin(), chunk_index->cend()});
      index_ranges.emplace_back(ChunkIndexRange{chunk_index->null_cbegin(), chunk_index->null_cend()});
      return index_ranges;
    }
  }

  if (!probe_side_position.is_null()) {
    auto range_begin = AbstractChunkIndex::Iterator{};
    auto range_end = AbstractChunkIndex::Iterator{};

    switch (_adjusted_primary_predicate.predicate_condition) {
      case PredicateCondition::Equals: {
        range_begin = chunk_index->lower_bound({probe_side_position.value()});
        range_end = chunk_index->upper_bound({probe_side_position.value()});
        break;
      }
      case PredicateCondition::NotEquals: {
        // first, get all values less than the search value
        range_begin = chunk_index->cbegin();
        range_end = chunk_index->lower_bound({probe_side_position.value()});
        index_ranges.emplace_back(ChunkIndexRange{range_begin, range_end});

        // set range for second half to all values greater than the search value
        range_begin = chunk_index->upper_bound({probe_side_position.value()});
        range_end = chunk_index->cend();
        break;
      }
      case PredicateCondition::GreaterThan: {
        range_begin = chunk_index->cbegin();
        range_end = chunk_index->lower_bound({probe_side_position.value()});
        break;
      }
      case PredicateCondition::GreaterThanEquals: {
        range_begin = chunk_index->cbegin();
        range_end = chunk_index->upper_bound({probe_side_position.value()});
        break;
      }
      case PredicateCondition::LessThan: {
        range_begin = chunk_index->upper_bound({probe_side_position.value()});
        range_end = chunk_index->cend();
        break;
      }
      case PredicateCondition::LessThanEquals: {
        range_begin = chunk_index->lower_bound({probe_side_position.value()});
        range_end = chunk_index->cend();
        break;
      }
      default: {
        Fail("Unsupported comparison type encountered");
      }
    }
    index_ranges.emplace_back(ChunkIndexRange{range_begin, range_end});
  }
  return index_ranges;
}

void JoinIndex::_append_matches_chunk_index(const AbstractChunkIndex::Iterator& range_begin,
                                const AbstractChunkIndex::Iterator& range_end, const ChunkOffset probe_chunk_offset,
                                const ChunkID probe_chunk_id, const ChunkID index_chunk_id) {
  const auto num_index_matches = std::distance(range_begin, range_end);

  if (num_index_matches == 0) {
    return;
  }

  const auto is_semi_or_anti_join =
      _mode == JoinMode::Semi || _mode == JoinMode::AntiNullAsFalse || _mode == JoinMode::AntiNullAsTrue;

  // Remember the matches for non-inner joins.
  if (((is_semi_or_anti_join || _mode == JoinMode::Left) && _index_side == IndexSide::Right) ||
      (_mode == JoinMode::Right && _index_side == IndexSide::Left) || _mode == JoinMode::FullOuter) {
    _probe_matches[probe_chunk_id][probe_chunk_offset] = true;
  }

  if (!is_semi_or_anti_join) {
    // We replicate the probe side value for each index side value.
    std::fill_n(std::back_inserter(*_probe_pos_list), num_index_matches, RowID{probe_chunk_id, probe_chunk_offset});

    std::transform(range_begin, range_end, std::back_inserter(*_index_pos_list),
                   [index_chunk_id](ChunkOffset index_chunk_offset) {
                     return RowID{index_chunk_id, index_chunk_offset};
                   });
  }

  if ((_mode == JoinMode::Left && _index_side == IndexSide::Left) ||
      (_mode == JoinMode::Right && _index_side == IndexSide::Right) || _mode == JoinMode::FullOuter ||
      (is_semi_or_anti_join && _index_side == IndexSide::Left)) {
    std::for_each(range_begin, range_end, [this, index_chunk_id](ChunkOffset index_chunk_offset) {
      _index_matches[index_chunk_id][index_chunk_offset] = true;
    });
  }
}

void JoinIndex::_append_matches_table_index(const AbstractTableIndex::Iterator& range_begin,
                                            const AbstractTableIndex::Iterator& range_end,
                                            const ChunkOffset probe_chunk_offset, const ChunkID probe_chunk_id) {
  const auto num_index_matches = std::distance(range_begin, range_end);

  if (num_index_matches == 0) {
    return;
  }

  const auto is_semi_or_anti_join =
      _mode == JoinMode::Semi || _mode == JoinMode::AntiNullAsFalse || _mode == JoinMode::AntiNullAsTrue;

  // Remember the matches for non-inner joins.
  if (((is_semi_or_anti_join || _mode == JoinMode::Left) && _index_side == IndexSide::Right) ||
      (_mode == JoinMode::Right && _index_side == IndexSide::Left) || _mode == JoinMode::FullOuter) {
    _probe_matches[probe_chunk_id][probe_chunk_offset] = true;
  }

  if (!is_semi_or_anti_join) {
    // We replicate the probe side value for each index side value.
    std::fill_n(std::back_inserter(*_probe_pos_list), num_index_matches, RowID{probe_chunk_id, probe_chunk_offset});

    std::copy(range_begin, range_end, std::back_inserter(*_index_pos_list));
  }

  if ((_mode == JoinMode::Left && _index_side == IndexSide::Left) ||
      (_mode == JoinMode::Right && _index_side == IndexSide::Right) || _mode == JoinMode::FullOuter ||
      (is_semi_or_anti_join && _index_side == IndexSide::Left)) {
    std::for_each(range_begin, range_end, [this](RowID index_row_id) {
      _index_matches[index_row_id.chunk_id][index_row_id.chunk_offset] = true;
    });
  }
}

void JoinIndex::_append_matches_dereferenced(const ChunkID& probe_chunk_id, const ChunkOffset& probe_chunk_offset,
                                             const RowIDPosList& index_table_matches) {
  for (const auto& index_side_row_id : index_table_matches) {
    _probe_pos_list->emplace_back(RowID{probe_chunk_id, probe_chunk_offset});
    _index_pos_list->emplace_back(index_side_row_id);
    _index_pos_dereferenced.emplace_back(true);
  }
}

void JoinIndex::_append_matches_non_inner(const bool is_semi_or_anti_join) {
  // For Full Outer and Left Join we need to add all unmatched rows for the probe side
  if ((_mode == JoinMode::Left && _index_side == IndexSide::Right) ||
      (_mode == JoinMode::Right && _index_side == IndexSide::Left) || _mode == JoinMode::FullOuter) {
    const auto chunk_count = _probe_input_table->chunk_count();
    for (ChunkID probe_chunk_id{0}; probe_chunk_id < chunk_count; ++probe_chunk_id) {
      for (ChunkOffset chunk_offset{0}; chunk_offset < static_cast<ChunkOffset>(_probe_matches[probe_chunk_id].size());
           ++chunk_offset) {
        if (!_probe_matches[probe_chunk_id][chunk_offset]) {
          _probe_pos_list->emplace_back(RowID{probe_chunk_id, chunk_offset});
          _index_pos_list->emplace_back(NULL_ROW_ID);
        }
      }
    }
  }

  // For Full Outer and Right Join we need to add all unmatched rows for the index side.
  if ((_mode == JoinMode::Left && _index_side == IndexSide::Left) ||
      (_mode == JoinMode::Right && _index_side == IndexSide::Right) || _mode == JoinMode::FullOuter) {
    const auto chunk_count = _index_matches.size();
    for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
      for (ChunkOffset chunk_offset{0}; chunk_offset < static_cast<ChunkOffset>(_index_matches[chunk_id].size());
           ++chunk_offset) {
        if (!_index_matches[chunk_id][chunk_offset]) {
          _index_pos_list->emplace_back(RowID{chunk_id, chunk_offset});
          _probe_pos_list->emplace_back(NULL_ROW_ID);
        }
      }
    }
  }

  _probe_pos_list->shrink_to_fit();
  _index_pos_list->shrink_to_fit();

  // Write PosLists for Semi/Anti Joins, which so far haven't written any results to the PosLists
  // We use `_probe_matches` to determine whether a tuple from the probe side found a match.
  if (is_semi_or_anti_join) {
    const auto invert = _mode == JoinMode::AntiNullAsFalse || _mode == JoinMode::AntiNullAsTrue;
    if (_index_side == IndexSide::Right) {
      const auto chunk_count = _probe_input_table->chunk_count();
      for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
        const auto chunk = _probe_input_table->get_chunk(chunk_id);
        Assert(chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

        const auto chunk_size = chunk->size();
        for (ChunkOffset chunk_offset{0}; chunk_offset < chunk_size; ++chunk_offset) {
          if (_probe_matches[chunk_id][chunk_offset] ^ invert) {
            _probe_pos_list->emplace_back(RowID{chunk_id, chunk_offset});
          }
        }
      }
    } else {  // INDEX SIDE LEFT
      const auto chunk_count = _index_input_table->chunk_count();
      for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
        const auto chunk = _index_input_table->get_chunk(chunk_id);
        Assert(chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

        const auto chunk_size = chunk->size();
        for (ChunkOffset chunk_offset{0}; chunk_offset < chunk_size; ++chunk_offset) {
          if (_index_matches[chunk_id][chunk_offset] ^ invert) {
            _index_pos_list->emplace_back(RowID{chunk_id, chunk_offset});
          }
        }
      }
    }
  }
}

void JoinIndex::_write_output_segments(Segments& output_segments, const std::shared_ptr<const Table>& input_table,
                                       const std::shared_ptr<RowIDPosList>& pos_list) {
  // Add segments from table to output chunk
  const auto column_count = input_table->column_count();
  for (ColumnID column_id{0}; column_id < column_count; ++column_id) {
    std::shared_ptr<AbstractSegment> segment;

    if (input_table->type() == TableType::References) {
      if (input_table->chunk_count() > 0) {
        auto new_pos_list = std::make_shared<RowIDPosList>();

        ChunkID current_chunk_id{0};

        const auto first_chunk_input_table = input_table->get_chunk(ChunkID{0});
        Assert(first_chunk_input_table, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");
        auto reference_segment =
            std::static_pointer_cast<const ReferenceSegment>(first_chunk_input_table->get_segment(column_id));

        // de-reference to the correct RowID so the output can be used in a Multi Join
        for (ChunkOffset pos_list_offset{0}; pos_list_offset < static_cast<ChunkOffset>(pos_list->size());
             ++pos_list_offset) {
          const auto& row = (*pos_list)[pos_list_offset];
          if (row.is_null()) {
            new_pos_list->push_back(NULL_ROW_ID);
            continue;
          }
          if (pos_list == _index_pos_list && _index_pos_dereferenced[pos_list_offset]) {
            new_pos_list->push_back(row);
          } else {
            if (row.chunk_id != current_chunk_id) {
              current_chunk_id = row.chunk_id;

              const auto chunk = input_table->get_chunk(current_chunk_id);
              Assert(chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

              reference_segment = std::dynamic_pointer_cast<const ReferenceSegment>(chunk->get_segment(column_id));
            }
            new_pos_list->push_back((*reference_segment->pos_list())[row.chunk_offset]);
          }
        }
        segment = std::make_shared<ReferenceSegment>(reference_segment->referenced_table(),
                                                     reference_segment->referenced_column_id(), new_pos_list);
      } else {
        // If there are no Chunks in the input_table, we can't deduce the Table that input_table is referencing to.
        // pos_list will contain only NULL_ROW_IDs anyway, so it doesn't matter which Table the ReferenceSegment that
        // we output is referencing. HACK, but works fine: we create a dummy table and let the ReferenceSegment ref
        // it.
        const auto dummy_table = Table::create_dummy_table(input_table->column_definitions());
        segment = std::make_shared<ReferenceSegment>(dummy_table, column_id, pos_list);
      }
    } else {
      segment = std::make_shared<ReferenceSegment>(input_table, column_id, pos_list);
    }

    output_segments.push_back(segment);
  }
}

void JoinIndex::_on_cleanup() {
  _output_table.reset();
  _probe_pos_list.reset();
  _index_pos_list.reset();
  _probe_matches.clear();
  _index_matches.clear();
}

void JoinIndex::PerformanceData::output_to_stream(std::ostream& stream, DescriptionMode description_mode) const {
  OperatorPerformanceData<OperatorSteps>::output_to_stream(stream, description_mode);

  const auto chunk_count = chunks_scanned_with_index + chunks_scanned_without_index;
  stream << (description_mode == DescriptionMode::SingleLine ? " " : "\n") << "Indexes used for "
         << chunks_scanned_with_index << " of " << chunk_count << " chunk" << (chunk_count > 1 ? "s" : "") << ".";
}

}  // namespace opossum
