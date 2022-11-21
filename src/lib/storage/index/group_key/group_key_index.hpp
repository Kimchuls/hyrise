#pragma once

#include <memory>
#include <numeric>
#include <utility>
#include <vector>

#include <iterator>
#include "storage/index/abstract_index.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

#include "storage/vector_compression/bitpacking/bitpacking_vector.hpp"

namespace hyrise {
#define ART_INDEX_SIZE 1024 * 1024 * 1024
class AbstractSegment;
class BaseDictionarySegment;
class GroupKeyIndexTest;

/**
 *
 * The GroupKeyIndex works on a single dictionary compressed segment.
 * Besides the AbstractIndex's positions list containing record positions for NULL values (`_null_positions`),
 * this specialized index uses two additional structures. The first is a positions list containing record positions
 * (ie ChunkOffsets) for non-NULL values in the attribute vector, the second is a structure mapping non-NULL-value-ids
 * to the start offsets in the positions list.
 * Since the AbstractIndex's NULL position list only contains NULL value positions, a structure for mapping
 * NULL-value-ids to offsets isn't needed. 
 *
 * An example structure along with the corresponding dictionary segment might look like this:
 *  +----+-----------+------------+---------+----------------+----------------+
 *  | (i)| Attribute | Dictionary |  Index  | Index Postings | NULL Positions |
 *  |    |  Vector   |            | Offsets | (non-NULL)     | [x²]           |
 *  +----+-----------+------------+---------+----------------+----------------+
 *  |  0 |         6 | apple    ------->  0 ------------>  7 |              0 | ie NULL can be found at i = 0, 5, 6 and 11
 *  |  1 |         4 | charlie  ------->  1 ------------>  8 |              5 |
 *  |  2 |         2 | delta    ------->  3 ----------|    9 |              6 |
 *  |  3 |         3 | frank    ------->  5 --------| |->  2 |             11 |
 *  |  4 |         2 | hotel    ------->  6 ------| |      4 |                |       
 *  |  5 |         6 | inbox    ------->  7 ----| | |--->  3 |                |
 *  |  6 |         6 |            | [x¹]  8 |   | |----->  1 |                |
 *  |  7 |         0 |            |         |   |-------> 10 |                |  
 *  |  8 |         1 |            |         |                |                |
 *  |  9 |         1 |            |         |                |                |
 *  | 10 |         5 |            |         |                |                |
 *  | 11 |         6 |            |         |                |                |  
 *  +----+-----------+------------+---------+----------------+----------------+
 * 
 * NULL is represented in the Attribute Vector by ValueID{dictionary.size()}, i.e., ValueID{6} in this example.
 * x¹: Mark for the ending position.
 * x²: NULL positions are stored in `_null_positions` of the AbstractIndex
 *
 * Find more information about this in our Wiki: https://github.com/hyrise/hyrise/wiki/GroupKey
 */
class GroupKeyIndex : public AbstractIndex {
  friend class GroupKeyIndexTest;

 public:
  /**
   * Predicts the memory consumption in bytes of creating this index.
   * See AbstractIndex::estimate_memory_consumption()
   */
  static size_t estimate_memory_consumption(ChunkOffset row_count, ChunkOffset distinct_count, uint32_t value_bytes);

  GroupKeyIndex() = delete;

  GroupKeyIndex(const GroupKeyIndex&) = delete;
  GroupKeyIndex& operator=(const GroupKeyIndex&) = delete;

  GroupKeyIndex(GroupKeyIndex&&) = default;

  explicit GroupKeyIndex(const std::vector<std::shared_ptr<const AbstractSegment>>& segments_to_index);
  explicit GroupKeyIndex(const std::string& table_name, const ChunkID& chunk_id, SegmentIndexType index_type,
                         const std::vector<ColumnID>& column_ids);
  void _serialization();
  void _deserialization();
  void send_RDMA(const std::string& table_name, const ChunkID& chunk_id, SegmentIndexType index_type,
                 const std::vector<ColumnID>& column_ids);
  char* _serialize;
  uint64_t _serialize_length;

  template <typename T>
  void export_value(char* pointer, uint64_t& length, const T& value);
  template <typename T, typename Alloc>
  void export_values(char* pointer, uint64_t& length, const std::vector<T, Alloc>& values);
  void export_values(char* pointer, uint64_t& length, const FixedStringVector& values);
  void export_string_values(char* pointer, uint64_t& length, const pmr_vector<pmr_string>& values);
  template <typename T>
  void export_values(char* pointer, uint64_t& length, const pmr_vector<pmr_string>& values);
  template <typename Alloc>
  void export_values(char* pointer, uint64_t& length, const std::vector<bool, Alloc>& values);
  void _export_compact_vector(char* pointer, uint64_t& length, const pmr_compact_vector& values);
  template <typename T>
  pmr_vector<T> _read_values(char* pointer, uint64_t& length, const size_t count);
  template <typename T>
  T _read_value(char* pointer, uint64_t& length);
  pmr_vector<pmr_string> _read_string_values(char* pointer, uint64_t& length, const size_t count);
  template <typename T>
  pmr_compact_vector _read_values_compact_vector(char* pointer, uint64_t& length, const size_t count);
  std::shared_ptr<BaseCompressedVector> _import_attribute_vector(
      char* pointer, uint64_t& length, const size_t row_count, const CompressedVectorTypeID compressed_vector_type_id);
  template <typename T>
  std::shared_ptr<ValueSegment<T>> _import_value_segment(char* pointer, uint64_t& index);
  template <typename T>
  std::shared_ptr<DictionarySegment<T>> _import_dictionary_segment(char* pointer, uint64_t& index);
  std::shared_ptr<FixedStringDictionarySegment<pmr_string>> _import_fixed_string_dictionary_segment(char* pointer,
                                                                                                    uint64_t& index);
  template <typename T>
  std::shared_ptr<RunLengthSegment<T>> _import_run_length_segment(char* pointer, uint64_t& index);
  template <typename T>
  std::shared_ptr<FrameOfReferenceSegment<T>> _import_frame_of_reference_segment(char* pointer, uint64_t& index);
  template <typename T>
  std::shared_ptr<LZ4Segment<T>> _import_lz4_segment(char* pointer, uint64_t& index);
  template <typename ColumnDataType>
  std::shared_ptr<AbstractSegment> _import_segment(char* pointer, uint64_t& index, EncodingType column_type);
  template <typename T>
  CompressedVectorTypeID _compressed_vector_type_id(const AbstractEncodedSegment& abstract_encoded_segment);
  void _export_compressed_vector(char* pointer, uint64_t& length, const CompressedVectorType type,
                                 const BaseCompressedVector& compressed_vector);
  template <typename T>
  void _write_segment(char* _serialize, uint64_t& _serialize_length, const DictionarySegment<T>& dictionary_segment);
  template <typename T>
  void _write_segment(char* _serialize, uint64_t& _serialize_length, const ValueSegment<T>& value_segment);
  void _write_segment(char* _serialize, uint64_t& _serialize_length, const ReferenceSegment& reference_segment);
  template <typename T>
  void _write_segment(char* _serialize, uint64_t& _serialize_length, const LZ4Segment<T>& lz4_segment);
  template <typename T>
  void _write_segment(char* _serialize, uint64_t& _serialize_length, const RunLengthSegment<T>& run_length_segment);
  template <typename T>
  void _write_segment(char* _serialize, uint64_t& _serialize_length,
                      const FrameOfReferenceSegment<T>& frame_of_reference_segment);
  template <typename T>
  void _write_segment(char* _serialize, uint64_t& _serialize_length,
                      const FixedStringDictionarySegment<T>& fixed_string_dictionary_segment);

 private:
  Iterator _lower_bound(const std::vector<AllTypeVariant>& values) const final;

  Iterator _upper_bound(const std::vector<AllTypeVariant>& values) const final;

  Iterator _cbegin() const final;

  Iterator _cend() const final;

  /**
   *
   * @returns an iterator pointing to the the first ChunkOffset in the positions-vector
   * that belongs to a given value-id.
   */
  Iterator _get_positions_iterator_at(ValueID value_id) const;

  std::vector<std::shared_ptr<const AbstractSegment>> _get_indexed_segments() const override;

  size_t _memory_consumption() const final;

 private:
  std::shared_ptr<const BaseDictionarySegment> _indexed_segment;
  std::vector<ChunkOffset> _value_start_offsets;  // maps value-ids to offsets in _positions
  std::vector<ChunkOffset> _positions;            // non-NULL record positions in the attribute vector
};
}  // namespace hyrise
