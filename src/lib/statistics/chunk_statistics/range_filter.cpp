#include "range_filter.hpp"

#include <algorithm>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

#include "statistics/abstract_statistics_object.hpp"
#include "statistics/chunk_statistics/min_max_filter.hpp"
#include "statistics/empty_statistics_object.hpp"
#include "type_cast.hpp"
#include "resolve_type.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
RangeFilter<T>::RangeFilter(std::vector<std::pair<T, T>> ranges)
    : AbstractStatisticsObject(data_type_from_type<T>()), _ranges(std::move(ranges)) {}

template <typename T>
CardinalityEstimate RangeFilter<T>::estimate_cardinality(const PredicateCondition predicate_type,
                                                         const AllTypeVariant& variant_value,
                                                         const std::optional<AllTypeVariant>& variant_value2) const {
  if (_does_not_contain(predicate_type, variant_value, variant_value2)) {
    return {Cardinality{0}, EstimateType::MatchesNone};
  } else {
    return {Cardinality{0}, EstimateType::MatchesApproximately};
  }
}

template <typename T>
std::shared_ptr<AbstractStatisticsObject> RangeFilter<T>::slice_with_predicate(
    const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
    const std::optional<AllTypeVariant>& variant_value2) const {
  if (_does_not_contain(predicate_type, variant_value, variant_value2)) {
    return std::make_shared<EmptyStatisticsObject>(data_type);
  }

  std::vector<std::pair<T, T>> ranges;
  const auto value = type_cast_variant<T>(variant_value);

  // If value is on range edge, we do not take the opportunity to slightly improve the new object.
  // The impact should be small.
  switch (predicate_type) {
    case PredicateCondition::Equals:
      return std::make_shared<MinMaxFilter<T>>(value, value);
    case PredicateCondition::LessThan:
    case PredicateCondition::LessThanEquals: {
      auto end_it = std::lower_bound(_ranges.cbegin(), _ranges.cend(), value,
                                     [](const auto& a, const auto& b) { return a.second < b; });

      // Copy all the ranges before the value.
      auto it = _ranges.cbegin();
      for (; it != end_it; it++) {
        ranges.emplace_back(*it);
      }

      DebugAssert(it != _ranges.cend(), "_does_not_contain() should have caught that.");

      // If value is not in a gap, limit the last range's upper bound to value.
      if (value >= it->first) {
        ranges.emplace_back(std::pair<T, T>{it->first, value});
      }
    } break;
    case PredicateCondition::GreaterThan:
    case PredicateCondition::GreaterThanEquals: {
      auto it = std::lower_bound(_ranges.cbegin(), _ranges.cend(), value,
                                 [](const auto& a, const auto& b) { return a.second < b; });

      DebugAssert(it != _ranges.cend(), "_does_not_contain() should have caught that.");

      // If value is in a gap, use the next range, otherwise limit the next range's upper bound to value.
      if (value <= it->first) {
        ranges.emplace_back(*it);
      } else {
        ranges.emplace_back(std::pair<T, T>{value, it->second});
      }
      it++;

      // Copy all following ranges.
      for (; it != _ranges.cend(); it++) {
        ranges.emplace_back(*it);
      }
    } break;
    case PredicateCondition::Between: {
      DebugAssert(variant_value2, "BETWEEN needs a second value.");
      const auto value2 = type_cast_variant<T>(*variant_value2);
      return slice_with_predicate(PredicateCondition::GreaterThanEquals, value)
          ->slice_with_predicate(PredicateCondition::LessThanEquals, value2);
    }
    default:
      ranges = _ranges;
  }

  return std::make_shared<RangeFilter<T>>(ranges);
}

template <typename T>
std::shared_ptr<AbstractStatisticsObject> RangeFilter<T>::scale_with_selectivity(const float /*selectivity*/) const {
  return std::make_shared<RangeFilter<T>>(_ranges);
}

template <typename T>
std::unique_ptr<RangeFilter<T>> RangeFilter<T>::build_filter(const pmr_vector<T>& dictionary,
                                                             uint32_t max_ranges_count) {
  static_assert(std::is_arithmetic_v<T>, "Range filters are only allowed on arithmetic types.");
  DebugAssert(!dictionary.empty(), "The dictionary should not be empty.");
  DebugAssert(max_ranges_count > 0, "Number of ranges to create needs to be larger zero.");
  DebugAssert(std::is_sorted(dictionary.begin(), dictionary.cend()), "Dictionary must be sorted in ascending order.");

  if (dictionary.size() == 1) {
    std::vector<std::pair<T, T>> ranges;
    ranges.emplace_back(dictionary.front(), dictionary.front());
    return std::make_unique<RangeFilter<T>>(std::move(ranges));
  }

  /*
  * In case more than one value is present, first the elements are checked for potential overflows (e.g., when calculating
  * the distince between INT::MIN() and INT::MAX(), the resulting distance might be to large for signed types).
  * While being rather unlikely for doubles, it's more likely to happen when Opossum includes tinyint etc.
  * std::make_unsigned<T>::type would be possible to use for signed int types, but not for floating types.
  * Approach: take the min and max values and simply check if the distance between both might overflow. In this case,
  * fall back to a single range filter.
  */
  const auto min_max = std::minmax_element(dictionary.cbegin(), dictionary.cend());
  if ((*min_max.first < 0) &&
      (*min_max.second > std::numeric_limits<T>::max() + *min_max.first)) {  // min_value is negative
    return std::make_unique<RangeFilter<T>>(std::vector<std::pair<T, T>>{{*min_max.first, *min_max.second}});
  }

  // calculate distances by taking the difference between two neighbouring elements
  // vector stores <distance to next element, dictionary index>
  std::vector<std::pair<T, size_t>> distances;
  distances.reserve(dictionary.size() - 1);
  for (auto dict_it = dictionary.cbegin(); dict_it + 1 != dictionary.cend(); ++dict_it) {
    auto dict_it_next = dict_it + 1;
    distances.emplace_back(*dict_it_next - *dict_it, std::distance(dictionary.cbegin(), dict_it));
  }

  std::sort(distances.begin(), distances.end(),
            [](const auto& pair1, const auto& pair2) { return pair1.first > pair2.first; });

  if ((max_ranges_count - 1) < distances.size()) {
    distances.erase(distances.cbegin() + (max_ranges_count - 1), distances.cend());
  }

  std::sort(distances.begin(), distances.end(),
            [](const auto& pair1, const auto& pair2) { return pair1.second < pair2.second; });
  // we want a range until the last element in the dictionary
  distances.emplace_back(T{}, dictionary.size() - 1);

  // derive intervals from distances where items exist
  //
  // start   end  next_startpoint
  // v       v    v
  // 1 2 3 4 5    10 11     15 16
  //         ^
  //         distance 5, index 4
  //
  // next_startpoint is the start of the next range

  std::vector<std::pair<T, T>> ranges;
  size_t next_startpoint = 0u;
  for (const auto& distance_index_pair : distances) {
    const auto index = distance_index_pair.second;
    ranges.emplace_back(dictionary[next_startpoint], dictionary[index]);
    next_startpoint = index + 1;
  }

  return std::make_unique<RangeFilter<T>>(std::move(ranges));
}

template <typename T>
bool RangeFilter<T>::_does_not_contain(const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
                                       const std::optional<AllTypeVariant>& variant_value2) const {
  /*
      * Early exit for NULL-checking predicates and NULL variants. Predicates with one or
      * more variant parameter being NULL are not prunable. Malformed predicates such as
      * can_prune(PredicateCondition::LessThan, {5}, NULL_VALUE) are not pruned either,
      * the caller is expected to call the function correctly.
      */
  if (variant_is_null(variant_value) || (variant_value2.has_value() && variant_is_null(variant_value2.value())) ||
      predicate_type == PredicateCondition::IsNull || predicate_type == PredicateCondition::IsNotNull) {
    return false;
  }

  const auto value = type_cast_variant<T>(variant_value);
  // Operators work as follows: value_from_table <operator> value
  // e.g. OpGreaterThan: value_from_table > value
  // thus we can exclude chunk if value >= _max since then no value from the table can be greater than value
  switch (predicate_type) {
    case PredicateCondition::GreaterThan: {
      auto& max = _ranges.back().second;
      return value >= max;
    }
    case PredicateCondition::GreaterThanEquals: {
      auto& max = _ranges.back().second;
      return value > max;
    }
    case PredicateCondition::LessThan: {
      auto& min = _ranges.front().first;
      return value <= min;
    }
    case PredicateCondition::LessThanEquals: {
      auto& min = _ranges.front().first;
      return value < min;
    }
    case PredicateCondition::Equals: {
      for (const auto& bounds : _ranges) {
        const auto& [min, max] = bounds;

        if (value >= min && value <= max) {
          return false;
        }
      }
      return true;
    }
    case PredicateCondition::NotEquals: {
      return _ranges.size() == 1 && _ranges.front().first == value && _ranges.front().second == value;
    }
    case PredicateCondition::Between: {
      /* There are two scenarios where a between predicate can be pruned:
       *    - both bounds are "outside" (not spanning) the segment's value range (i.e., either both are smaller than
       *      the minimum or both are larger than the maximum
       *    - both bounds are within the same gap
       */

      Assert(variant_value2.has_value(), "Between operator needs two values.");
      const auto value2 = type_cast_variant<T>(*variant_value2);

      // a BETWEEN 5 AND 4 will always be empty
      if (value2 < value) return true;

      // Smaller than the segment's minimum.
      if (_does_not_contain(PredicateCondition::LessThanEquals, std::max(value, value2))) {
        return true;
      }

      // Larger than the segment's maximum.
      if (_does_not_contain(PredicateCondition::GreaterThanEquals, std::min(value, value2))) {
        return true;
      }

      const auto range_comp = [](std::pair<T, T> range, T compare_value) -> bool {
        return range.second < compare_value;
      };
      // Get value range or next larger value range if searched value is in a gap.
      const auto start_lower = std::lower_bound(_ranges.cbegin(), _ranges.cend(), value, range_comp);
      const auto end_lower = std::lower_bound(_ranges.cbegin(), _ranges.cend(), value2, range_comp);

      const bool start_in_value_range =
          (start_lower != _ranges.cend()) && (*start_lower).first <= value && value <= (*start_lower).second;
      const bool end_in_value_range =
          (end_lower != _ranges.cend()) && (*end_lower).first <= value2 && value2 <= (*end_lower).second;

      // Check if both bounds are within the same gap.
      if (!start_in_value_range && !end_in_value_range && start_lower == end_lower) {
        return true;
      }

      return false;
    }
    default:
      return false;
  }
}

template class RangeFilter<int32_t>;
template class RangeFilter<int64_t>;
template class RangeFilter<float>;
template class RangeFilter<double>;

}  // namespace opossum
