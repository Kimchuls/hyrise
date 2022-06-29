#include "index_scan.hpp"

#include <algorithm>

#include <boost/sort/sort.hpp>

#include "expression/between_expression.hpp"
#include "hyrise.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "storage/index/abstract_index.hpp"
#include "storage/reference_segment.hpp"
#include "utils/assert.hpp"

namespace opossum {

IndexScan::IndexScan(const std::shared_ptr<const AbstractOperator>& in, const SegmentIndexType index_type,
                     const std::vector<ColumnID>& left_column_ids, const PredicateCondition predicate_condition,
                     const std::vector<AllTypeVariant>& right_values, const std::vector<AllTypeVariant>& right_values2)
    : AbstractReadOnlyOperator{OperatorType::IndexScan, in},
      _index_type{index_type},
      _left_column_ids{left_column_ids},
      _predicate_condition{predicate_condition},
      _right_values{right_values},
      _right_values2{right_values2} {}

const std::string& IndexScan::name() const {
  static const auto name = std::string{"IndexScan"};
  return name;
}

std::string IndexScan::description(DescriptionMode description_mode) const {
  const auto separator = (description_mode == DescriptionMode::SingleLine ? ' ' : '\n');

  std::stringstream stream;

  stream << AbstractOperator::description(description_mode) << separator;
  stream << separator << "IndexType: " << magic_enum::enum_name(_index_type);
  stream << separator << "Predicate: " << magic_enum::enum_name(_predicate_condition);
  for (auto column_id = ColumnID{0}; column_id < _left_column_ids.size(); ++column_id) {
    stream << separator << "Column #" << _left_column_ids[column_id] << ": " << _right_values[column_id];
    if (!_right_values2.empty()) {
      stream << "-" << _right_values2[column_id] << separator;
    }
  }

  return stream.str();
}

std::shared_ptr<const Table> IndexScan::_on_execute() {
  _in_table = left_input_table();

  _validate_input();

  _out_table = std::make_shared<Table>(_in_table->column_definitions(), TableType::References);

  std::mutex output_mutex;

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  if (included_chunk_ids.empty()) {
    const auto chunk_count = _in_table->chunk_count();
    jobs.reserve(chunk_count);
    for (auto chunk_id = ChunkID{0u}; chunk_id < chunk_count; ++chunk_id) {
      const auto chunk = _in_table->get_chunk(chunk_id);
      Assert(chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");
      // Commented out by Keven to fix the problem of empty include lists for several configurations
      // Martin addressed the problem in issue #2187   
      //jobs.push_back(_create_job_and_schedule(chunk_id, output_mutex));
    }
  } else {
    jobs.reserve(included_chunk_ids.size());
    for (auto chunk_id : included_chunk_ids) {
      if (_in_table->get_chunk(chunk_id)) {
        jobs.push_back(_create_job(chunk_id, output_mutex));
      }
    }
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

  return _out_table;
}

std::shared_ptr<AbstractOperator> IndexScan::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& copied_right_input) const {
  return std::make_shared<IndexScan>(copied_left_input, _index_type, _left_column_ids, _predicate_condition,
                                     _right_values, _right_values2);
}

void IndexScan::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<AbstractTask> IndexScan::_create_job(const ChunkID chunk_id, std::mutex& output_mutex) {
  auto job_task = std::make_shared<JobTask>([this, chunk_id, &output_mutex]() {
    // The output chunk is allocated on the same NUMA node as the input chunk.
    const auto chunk = _in_table->get_chunk(chunk_id);
    if (!chunk) {
      return;
    }

    const auto matches_out = _scan_chunk(chunk_id);
    if (matches_out->empty()) {
      return;
    }

    Segments segments;

    for (ColumnID column_id{0u}; column_id < _in_table->column_count(); ++column_id) {
      auto ref_segment_out = std::make_shared<ReferenceSegment>(_in_table, column_id, matches_out);
      segments.push_back(ref_segment_out);
    }

    std::lock_guard<std::mutex> lock(output_mutex);
    _out_table->append_chunk(segments);
    if (!chunk->individually_sorted_by().empty()) {
       _out_table->last_chunk()->finalize();
       _out_table->last_chunk()->set_individually_sorted_by(chunk->individually_sorted_by());
    }  
  });

  return job_task;
}

void IndexScan::_validate_input() {
  Assert(_predicate_condition != PredicateCondition::Like, "Predicate condition not supported by index scan.");
  Assert(_predicate_condition != PredicateCondition::NotLike, "Predicate condition not supported by index scan.");

  Assert(_left_column_ids.size() == _right_values.size(),
         "Count mismatch: left column IDs and right values don’t have same size.");
  if (is_between_predicate_condition(_predicate_condition)) {
    Assert(_left_column_ids.size() == _right_values2.size(),
           "Count mismatch: left column IDs and right values don’t have same size.");
  }

  Assert(_in_table->type() == TableType::Data, "IndexScan only supports persistent tables right now.");
}

std::shared_ptr<AbstractPosList> IndexScan::_scan_chunk(const ChunkID chunk_id) {
  const auto to_row_id = [chunk_id](ChunkOffset chunk_offset) { return RowID{chunk_id, chunk_offset}; };

  auto range_begin = AbstractIndex::Iterator{};
  auto range_end = AbstractIndex::Iterator{};

  const auto chunk = _in_table->get_chunk(chunk_id);
  auto matches_out = std::make_shared<RowIDPosList>();

  const auto index = chunk->get_index(_index_type, _left_column_ids);
  if (!index) {
    auto sstream = std::stringstream{};
    sstream << "Index of specified type not found for segment (vector). Index type: ";
    sstream << magic_enum::enum_name(_index_type);
    sstream << ". Column IDs: ";
    for (const auto column_id : _left_column_ids) sstream << column_id << " ";
    sstream << std::endl;
    Assert(index, sstream.str());
  }

  switch (_predicate_condition) {
    case PredicateCondition::Equals: {
      range_begin = index->lower_bound(_right_values);
      range_end = index->upper_bound(_right_values);
      break;
    }
    case PredicateCondition::NotEquals: {
      // first, get all values less than the search value
      range_begin = index->cbegin();
      range_end = index->lower_bound(_right_values);

      matches_out->reserve(std::distance(range_begin, range_end));
      std::transform(range_begin, range_end, std::back_inserter(*matches_out), to_row_id);

      // set range for second half to all values greater than the search value
      range_begin = index->upper_bound(_right_values);
      range_end = index->cend();
      break;
    }
    case PredicateCondition::LessThan: {
      range_begin = index->cbegin();
      range_end = index->lower_bound(_right_values);
      break;
    }
    case PredicateCondition::LessThanEquals: {
      range_begin = index->cbegin();
      range_end = index->upper_bound(_right_values);
      break;
    }
    case PredicateCondition::GreaterThan: {
      range_begin = index->upper_bound(_right_values);
      range_end = index->cend();
      break;
    }
    case PredicateCondition::GreaterThanEquals: {
      range_begin = index->lower_bound(_right_values);
      range_end = index->cend();
      break;
    }
    case PredicateCondition::BetweenInclusive: {
      range_begin = index->lower_bound(_right_values);
      range_end = index->upper_bound(_right_values2);
      break;
    }
    case PredicateCondition::BetweenLowerExclusive: {
      range_begin = index->upper_bound(_right_values);
      range_end = index->upper_bound(_right_values2);
      break;
    }
    case PredicateCondition::BetweenUpperExclusive: {
      range_begin = index->lower_bound(_right_values);
      range_end = index->lower_bound(_right_values2);
      break;
    }
    case PredicateCondition::BetweenExclusive: {
      range_begin = index->upper_bound(_right_values);
      range_end = index->lower_bound(_right_values2);
      break;
    }
    default:
      Fail("Unsupported comparison type encountered");
  }

  DebugAssert(_in_table->type() == TableType::Data, "Cannot guarantee single chunk PosList for non-data tables.");
  matches_out->guarantee_single_chunk();

  const auto match_count = static_cast<size_t>(std::distance(range_begin, range_end));

  if (match_count == chunk->size()) {
    return std::make_shared<EntireChunkPosList>(chunk_id, chunk->size());
  }

  const auto current_matches_size = matches_out->size();
  const auto final_matches_size = current_matches_size + match_count;
  matches_out->resize(final_matches_size);

  auto& matches_out_vector = *matches_out;
  for (auto matches_position = current_matches_size; matches_position < final_matches_size; ++matches_position) {
    matches_out_vector[matches_position] = RowID{chunk_id, *range_begin};
    ++range_begin;
  }

  boost::sort::pdqsort(matches_out->begin(), matches_out->end());

  return matches_out;
}

}  // namespace opossum
