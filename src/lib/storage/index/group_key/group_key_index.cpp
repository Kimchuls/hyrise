#include "group_key_index.hpp"

#include <memory>
#include <vector>

#include "resolve_type.hpp"
#include "storage/base_dictionary_segment.hpp"
#include "storage/index/abstract_index.hpp"
// #include "storage/index/serialize_utils.h"
#include "storage/vector_compression/resolve_compressed_vector_type.hpp"

#include <algorithm>
#include <iterator>
#include <limits>
#include <numeric>
#include <string>
#include <utility>

#include "hyrise.hpp"
#include "storage/vector_compression/bitpacking/bitpacking_vector.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

// Writes a shallow copy of the given value to the ofstream
template <typename T>
void GroupKeyIndex::export_value(char* pointer, uint64_t& length, const T& value) {
  // printf("export_values::T\n");
  // std::cout<<value<<std::endl;
  memcpy(pointer + length, &value, sizeof(value));
  length += sizeof(value);
}

template <typename T, typename Alloc>
void GroupKeyIndex::export_values(char* pointer, uint64_t& length, const std::vector<T, Alloc>& values) {
  // printf("export_values::vector<T, Alloc>\n");
  memcpy(pointer + length, values.data(), values.size() * sizeof(T));
  // for (int i = 0; i < values.size(); i++) {
  //   printf("value %d:%d, ", i, values[i]);
  // }
  // std::cout << std::endl;
  length += values.size() * sizeof(T);
}

void GroupKeyIndex::export_values(char* pointer, uint64_t& length, const FixedStringVector& values) {
  printf("export_values::FixedString\n");
  memcpy(pointer + length, values.data(), static_cast<int64_t>(values.size() * values.string_length()));
  // for (int i = 0; i < values.size(); i++) {
  //   printf("value %d:%s, ", i, values.data()[i]);
  // }
  // std::cout << std::endl;
  length += static_cast<int64_t>(values.size() * values.string_length());
}

void GroupKeyIndex::export_string_values(char* pointer, uint64_t& length, const pmr_vector<pmr_string>& values) {
  pmr_vector<size_t> string_lengths(values.size());
  size_t total_length = 0;
  // printf("export_string_values::values size : %d\n", values.size());
  for (size_t i = 0; i < values.size(); ++i) {
    // std::cout << "value " << i << ": " << values[i] << ",";
    string_lengths[i] = values[i].size();
    total_length += values[i].size();
  }
  // std::cout << std::endl;
  export_values(pointer, length, string_lengths);
  if (total_length == 0)
    return;

  pmr_vector<char> buffer(total_length);
  size_t start = 0;
  for (const auto& str : values) {
    std::memcpy(buffer.data() + start, str.data(), str.size());
    start += str.size();
  }
  export_values(pointer, length, buffer);
}

// specialized implementation for string values

template <>
void GroupKeyIndex::export_values(char* pointer, uint64_t& length, const pmr_vector<pmr_string>& values) {
  // printf("export_values::pmr_vector<pmr_string>\n");
  export_string_values(pointer, length, values);
}

// specialized implementation for bool values
template <typename Alloc>
void GroupKeyIndex::export_values(char* pointer, uint64_t& length, const std::vector<bool, Alloc>& values) {
  // printf("export_values::pmr_vector<bool>\n");
  // Cast to fixed-size format used in binary file
  const auto writable_bools = pmr_vector<BoolAsByteType>(values.begin(), values.end());
  export_values(pointer, length, writable_bools);
}

void GroupKeyIndex::_export_compact_vector(char* pointer, uint64_t& length, const pmr_compact_vector& values) {
  // printf("export_compact_vector\n");
  export_value(pointer, length, static_cast<uint8_t>(values.bits()));
  memcpy(pointer + length, values.get(), static_cast<int64_t>(values.bytes()));
}

template <typename T>
pmr_vector<T> GroupKeyIndex::_read_values(char* pointer, uint64_t& length, const size_t count) {
  // printf("read_values::pmr_vector<T>\n");
  pmr_vector<T> values(count);
  memcpy(values.data(), pointer + length, values.size() * sizeof(T));
  length += values.size() * sizeof(T);
  // for (int i = 0; i < values.size(); i++) {
  //   printf("value %d:%d, ", i, values[i]);
  // }
  // std::cout << std::endl;
  return values;
}

// specialized implementation for bool values
template <>
pmr_vector<bool> GroupKeyIndex::_read_values(char* pointer, uint64_t& length, const size_t count) {
  // printf("read_values::pmr_vector<bool>\n");
  pmr_vector<BoolAsByteType> readable_bools(count);
  memcpy(readable_bools.data(), pointer + length, static_cast<int64_t>(readable_bools.size() * sizeof(BoolAsByteType)));
  length += static_cast<int64_t>(readable_bools.size() * sizeof(BoolAsByteType));
  return {readable_bools.begin(), readable_bools.end()};
}

template <typename T>
T GroupKeyIndex::_read_value(char* pointer, uint64_t& length) {
  // printf("read_values::T\n");
  T result;
  memcpy(&result, pointer + length, sizeof(T));
  length += sizeof(T);
  return result;
}

pmr_vector<pmr_string> GroupKeyIndex::_read_string_values(char* pointer, uint64_t& length, const size_t count) {
  // printf("_read_string_values\n");
  const auto string_lengths = _read_values<size_t>(pointer, length, count);
  const auto total_length = std::accumulate(string_lengths.cbegin(), string_lengths.cend(), static_cast<size_t>(0));
  const auto buffer = _read_values<char>(pointer, length, total_length);
  auto values = pmr_vector<pmr_string>{count};
  auto start = size_t{0};
  for (auto index = size_t{0}; index < count; ++index) {
    values[index] = pmr_string{buffer.data() + start, buffer.data() + start + string_lengths[index]};
    start += string_lengths[index];
  }
  // printf("%d %d \n", values.size(), count);
  // for (int i = 0; i < values.size(); i++) {
  // std::cout << "value " << i << ": " << values[i] << ", ";
  // }
  // std::cout << std::endl;
  return values;
}

// specialized implementation for string values
template <>
pmr_vector<pmr_string> GroupKeyIndex::_read_values(char* pointer, uint64_t& length, const size_t count) {
  // printf("read_values::pmr_vector<pmr_string>\n");
  return _read_string_values(pointer, length, count);
}

template <typename T>
pmr_compact_vector GroupKeyIndex::_read_values_compact_vector(char* pointer, uint64_t& length, const size_t count) {
  // printf("_read_values_compact_vector\n");
  const auto bit_width = _read_value<uint8_t>(pointer, length);
  auto values = pmr_compact_vector(bit_width, count);
  memcpy(values.get(), pointer + length, static_cast<int64_t>(values.bytes()));
  length += static_cast<int64_t>(values.bytes());
  return values;
}

std::shared_ptr<BaseCompressedVector> GroupKeyIndex::_import_attribute_vector(
    char* pointer, uint64_t& length, const size_t row_count, const CompressedVectorTypeID compressed_vector_type_id) {
  const auto compressed_vector_type = static_cast<CompressedVectorType>(compressed_vector_type_id);
  switch (compressed_vector_type) {
    case CompressedVectorType::BitPacking:
      return std::make_shared<BitPackingVector>(_read_values_compact_vector<uint32_t>(pointer, length, row_count));
    case CompressedVectorType::FixedWidthInteger1Byte:
      return std::make_shared<FixedWidthIntegerVector<uint8_t>>(_read_values<uint8_t>(pointer, length, row_count));
    case CompressedVectorType::FixedWidthInteger2Byte:
      return std::make_shared<FixedWidthIntegerVector<uint16_t>>(_read_values<uint16_t>(pointer, length, row_count));
    case CompressedVectorType::FixedWidthInteger4Byte:
      return std::make_shared<FixedWidthIntegerVector<uint32_t>>(_read_values<uint32_t>(pointer, length, row_count));
    default:
      Fail("Cannot import attribute vector with compressed vector type id: " +
           std::to_string(compressed_vector_type_id));
  }
}

template <typename T>
std::shared_ptr<ValueSegment<T>> GroupKeyIndex::_import_value_segment(char* pointer, uint64_t& index) {
  std::cout << "_import_value_segment" << std::endl;
  exit(0);
}

template <typename T>
std::shared_ptr<DictionarySegment<T>> GroupKeyIndex::_import_dictionary_segment(char* pointer, uint64_t& index) {
  // std::cout << "_import_dictionary_segment" << std::endl;
  const auto compressed_vector_type_id = _read_value<CompressedVectorTypeID>(pointer, index);
  // printf("compressed_vector_type_id: %ld\n", compressed_vector_type_id);
  const auto dictionary_size = _read_value<ValueID>(pointer, index);
  // printf("dictionary_size: %ld\n", dictionary_size);
  auto dictionary = std::make_shared<pmr_vector<T>>(_read_values<T>(pointer, index, dictionary_size));
  const auto row_count = _read_value<ValueID>(pointer, index);
  // printf("row_count: %ld\n", row_count);
  auto attribute_vector = _import_attribute_vector(pointer, index, row_count, compressed_vector_type_id);

  return std::make_shared<DictionarySegment<T>>(dictionary, attribute_vector);
}

std::shared_ptr<FixedStringDictionarySegment<pmr_string>> GroupKeyIndex::_import_fixed_string_dictionary_segment(
    char* pointer, uint64_t& index) {
  std::cout << "_import_fixed_string_dictionary_segment" << std::endl;
  exit(0);
}

template <typename T>
std::shared_ptr<RunLengthSegment<T>> GroupKeyIndex::_import_run_length_segment(char* pointer, uint64_t& index) {
  std::cout << "_import_run_length_segment" << std::endl;
  exit(0);
}

template <typename T>
std::shared_ptr<FrameOfReferenceSegment<T>> GroupKeyIndex::_import_frame_of_reference_segment(char* pointer,
                                                                                              uint64_t& index) {
  std::cout << "_import_frame_of_reference_segment" << std::endl;
  exit(0);
}

template <typename T>
std::shared_ptr<LZ4Segment<T>> GroupKeyIndex::_import_lz4_segment(char* pointer, uint64_t& index) {
  std::cout << "_import_lz4_segment" << std::endl;
  exit(0);
}

template <typename ColumnDataType>
std::shared_ptr<AbstractSegment> GroupKeyIndex::_import_segment(char* pointer, uint64_t& index,
                                                                EncodingType column_type) {
  switch (column_type) {
    case EncodingType::Unencoded:
      return _import_value_segment<ColumnDataType>(pointer, index);
      break;
    case EncodingType::Dictionary:
      return _import_dictionary_segment<ColumnDataType>(pointer, index);
      break;
    case EncodingType::FixedStringDictionary:
      if constexpr (encoding_supports_data_type(enum_c<EncodingType, EncodingType::FixedStringDictionary>,
                                                hana::type_c<ColumnDataType>)) {
        return _import_fixed_string_dictionary_segment(pointer, index);
      } else {
        Fail("Unsupported data type for FixedStringDictionary encoding");
      }
      break;
    case EncodingType::RunLength:
      return _import_run_length_segment<ColumnDataType>(pointer, index);
      break;
    case EncodingType::FrameOfReference:
      if constexpr (encoding_supports_data_type(enum_c<EncodingType, EncodingType::FrameOfReference>,
                                                hana::type_c<ColumnDataType>)) {
        return _import_frame_of_reference_segment<ColumnDataType>(pointer, index);
      } else {
        Fail("Unsupported data type for FOR encoding");
      }
      break;
    case EncodingType::LZ4:
      return _import_lz4_segment<ColumnDataType>(pointer, index);
      break;
  }

  Fail("Invalid EncodingType");
}

template <typename T>
CompressedVectorTypeID GroupKeyIndex::_compressed_vector_type_id(
    const AbstractEncodedSegment& abstract_encoded_segment) {
  uint8_t compressed_vector_type_id = 0u;
  resolve_encoded_segment_type<T>(abstract_encoded_segment, [&compressed_vector_type_id](auto& typed_segment) {
    const auto compressed_vector_type = typed_segment.compressed_vector_type();
    Assert(compressed_vector_type, "Expected Segment to use vector compression");
    switch (*compressed_vector_type) {
      case CompressedVectorType::FixedWidthInteger4Byte:
      case CompressedVectorType::FixedWidthInteger2Byte:
      case CompressedVectorType::FixedWidthInteger1Byte:
      case CompressedVectorType::BitPacking:
        compressed_vector_type_id = static_cast<uint8_t>(*compressed_vector_type);
        break;
      default:
        Fail("Export of specified CompressedVectorType is not yet supported");
    }
  });
  return compressed_vector_type_id;
}

void GroupKeyIndex::_export_compressed_vector(char* pointer, uint64_t& length, const CompressedVectorType type,
                                              const BaseCompressedVector& compressed_vector) {
  switch (type) {
    case CompressedVectorType::FixedWidthInteger4Byte:
      export_values(pointer, length, dynamic_cast<const FixedWidthIntegerVector<uint32_t>&>(compressed_vector).data());
      return;
    case CompressedVectorType::FixedWidthInteger2Byte:
      export_values(pointer, length, dynamic_cast<const FixedWidthIntegerVector<uint16_t>&>(compressed_vector).data());
      return;
    case CompressedVectorType::FixedWidthInteger1Byte:
      export_values(pointer, length, dynamic_cast<const FixedWidthIntegerVector<uint8_t>&>(compressed_vector).data());
      return;
    case CompressedVectorType::BitPacking:
      _export_compact_vector(pointer, length, dynamic_cast<const BitPackingVector&>(compressed_vector).data());
      return;
    default:
      Fail("Any other type should have been caught before.");
  }
}

// std::unique_ptr<const BaseCompressedVector> _import_offset_value_vector(
//     std::ifstream& file, const ChunkOffset row_count, const CompressedVectorTypeID compressed_vector_type_id) {
//   return nullptr;
// }

// std::shared_ptr<FixedStringVector> _import_fixed_string_vector(std::ifstream& file, const size_t count) {
//   return nullptr;
// }

template <typename T>
void GroupKeyIndex::_write_segment(char* _serialize, uint64_t& _serialize_length,
                                   const DictionarySegment<T>& dictionary_segment) {
  // std::cout << "*-------------------------------*" << std::endl;
  // std::cout << "_write_segment::DictionarySegment" << std::endl;
  // std::cout << "-------------encode--------------" << std::endl;
  const auto encodingtype = dictionary_segment.encoding_type();
  export_value(_serialize, _serialize_length, encodingtype);
  // printf("encodingtype: %ld, %d, %ld\n", encodingtype, sizeof(encodingtype), _serialize_length);
  const auto column_datatype = dictionary_segment.data_type();
  export_value(_serialize, _serialize_length, column_datatype);
  // printf("column_datatype: %ld\n", column_datatype);
  const auto compressed_vector_type_id = _compressed_vector_type_id<T>(dictionary_segment);
  export_value(_serialize, _serialize_length, compressed_vector_type_id);
  // printf("compressed_vector_type_id: \'%d\'\n", sizeof(compressed_vector_type_id));
  export_value(_serialize, _serialize_length, static_cast<ValueID::base_type>(dictionary_segment.dictionary()->size()));
  // printf("dictionary_segment.dictionary()->size(): %ld\n", dictionary_segment.dictionary()->size());
  export_values(_serialize, _serialize_length, *dictionary_segment.dictionary());
  export_value(_serialize, _serialize_length,
               static_cast<ValueID::base_type>(dictionary_segment.attribute_vector()->size()));
  // printf("dictionary_segment.attribute_vector()->size(): %ld\n", dictionary_segment.attribute_vector()->size());

  _export_compressed_vector(_serialize, _serialize_length, *dictionary_segment.compressed_vector_type(),
                            *dictionary_segment.attribute_vector());
  // std::cout << "*-------------------------------*" << std::endl;
}

template <typename T>
void GroupKeyIndex::_write_segment(char* _serialize, uint64_t& _serialize_length,
                                   const ValueSegment<T>& value_segment) {
  std::cout << "_write_segment::ValueSegment" << std::endl;
  exit(0);
}

void GroupKeyIndex::_write_segment(char* _serialize, uint64_t& _serialize_length,
                                   const ReferenceSegment& reference_segment) {
  std::cout << "_write_segment::ReferenceSegment" << std::endl;
  exit(0);
}

template <typename T>
void GroupKeyIndex::_write_segment(char* _serialize, uint64_t& _serialize_length, const LZ4Segment<T>& lz4_segment) {
  std::cout << "_write_segment::LZ4Segment" << std::endl;
  exit(0);
}

template <typename T>
void GroupKeyIndex::_write_segment(char* _serialize, uint64_t& _serialize_length,
                                   const RunLengthSegment<T>& run_length_segment) {
  std::cout << "_write_segment::RunLengthSegment" << std::endl;
  exit(0);
}

template <typename T>
void GroupKeyIndex::_write_segment(char* _serialize, uint64_t& _serialize_length,
                                   const FrameOfReferenceSegment<T>& frame_of_reference_segment) {
  std::cout << "_write_segment::FrameOfReferenceSegment" << std::endl;
  exit(0);
}

template <typename T>
void GroupKeyIndex::_write_segment(char* _serialize, uint64_t& _serialize_length,
                                   const FixedStringDictionarySegment<T>& fixed_string_dictionary_segment) {
  std::cout << "_write_segment::FixedStringDictionarySegment" << std::endl;
  exit(0);
}
}  // namespace hyrise

namespace hyrise {

size_t GroupKeyIndex::estimate_memory_consumption(ChunkOffset row_count, ChunkOffset distinct_count,
                                                  uint32_t value_bytes) {
  return row_count * sizeof(ChunkOffset) + distinct_count * sizeof(std::size_t);
}

GroupKeyIndex::GroupKeyIndex(const std::vector<std::shared_ptr<const AbstractSegment>>& segments_to_index)
    : AbstractIndex{get_index_type_of<GroupKeyIndex>()},
      _indexed_segment(segments_to_index.empty()  // Empty segment list is illegal
                           ? nullptr              // but range check needed for accessing the first segment
                           : std::dynamic_pointer_cast<const BaseDictionarySegment>(segments_to_index[0])) {
  Assert(static_cast<bool>(_indexed_segment), "GroupKeyIndex only works with dictionary segments_to_index.");
  Assert((segments_to_index.size() == 1), "GroupKeyIndex only works with a single segment.");

  // 1) Creating a value histogram:
  //    Create a value histogram with a size of the dictionary + 1 (plus one to mark the ending position)
  //    and set all bins to 0.
  //    With this histogram, we want to count the occurences of each ValueID of the attribute vector.
  //    The ValueID for NULL in an attribute vector is the highest available ValueID in the dictionary + 1
  //    which is also the size of the dictionary.
  //    `unique_values_count` returns the size of dictionary which does not store a ValueID for NULL.
  //    Therefore we have `unique_values_count` ValueIDs (NULL-value-id is not included)
  //    for which we want to count the occurrences.
  auto value_histogram = std::vector<ChunkOffset>{
      _indexed_segment->unique_values_count() + 1u /*to mark the ending position */, ChunkOffset{0}};

  // 2) Count the occurrences of value-ids: Iterate once over the attribute vector (i.e. value ids)
  //    and count the occurrences of each value id at their respective position in the dictionary,
  //    i.e. the position in the value histogram.
  const auto null_value_id = _indexed_segment->null_value_id();
  auto null_count = 0u;

  resolve_compressed_vector_type(*_indexed_segment->attribute_vector(), [&](auto& attribute_vector) {
    for (const auto value_id : attribute_vector) {
      if (static_cast<ValueID>(value_id) != null_value_id) {
        value_histogram[value_id + 1u]++;
      } else {
        ++null_count;
      }
    }
  });

  const auto non_null_count = _indexed_segment->size() - null_count;

  // 3) Set the _positions and _null_positions
  _positions = std::vector<ChunkOffset>(non_null_count);
  _null_positions = std::vector<ChunkOffset>(null_count);

  // 4) Create start offsets for the positions in _value_start_offsets
  _value_start_offsets = std::move(value_histogram);
  std::partial_sum(_value_start_offsets.begin(), _value_start_offsets.end(), _value_start_offsets.begin());

  // 5) Create the positions
  // 5a) Copy _value_start_offsets to use the copy as a write counter
  auto value_write_offsets = std::vector<ChunkOffset>(_value_start_offsets);

  // 5b) Iterate over the attribute vector to obtain the write-offsets and
  //     to finally insert the positions
  resolve_compressed_vector_type(*_indexed_segment->attribute_vector(), [&](auto& attribute_vector) {
    auto value_id_iter = attribute_vector.cbegin();
    auto null_positions_iter = _null_positions.begin();
    auto position = ChunkOffset{0};
    for (; value_id_iter != attribute_vector.cend(); ++value_id_iter, ++position) {
      const auto& value_id = static_cast<ValueID>(*value_id_iter);

      if (value_id != null_value_id) {
        _positions[value_write_offsets[value_id]] = position;
      } else {
        *null_positions_iter = position;
        ++null_positions_iter;
      }

      // increase the write-offset by one to ensure that further writes
      // are directed to the next position in `_positions`
      ++value_write_offsets[value_id];
    }
  });
  _serialization();
}

void GroupKeyIndex::_serialization() {
  // std::cout << "*-------------------------------*" << std::endl;
  // std::cout << "------GroupKeyIndex::encode------" << std::endl;
  // std::cout << "---------------------------------" << std::endl;
  _serialize = (char*)malloc(ART_INDEX_SIZE);
  _serialize_length = 0;
  resolve_data_and_segment_type(*(std::dynamic_pointer_cast<const AbstractSegment>(_indexed_segment)),
                                [&](const auto data_type_t, const auto& resolved_segment) {
                                  _write_segment(_serialize, _serialize_length, resolved_segment);
                                });
  const auto _value_start_offsets_length = _value_start_offsets.size();
  export_value(_serialize, _serialize_length, static_cast<ValueID::base_type>(_value_start_offsets_length));
  // printf("_value_start_offsets_length: %ld\n", _value_start_offsets_length);
  export_values(_serialize, _serialize_length, _value_start_offsets);

  const auto _positions_length = _positions.size();
  export_value(_serialize, _serialize_length, static_cast<ValueID::base_type>(_positions_length));
  // printf("_positions_length: %ld, address=%ld\n", _positions_length,_serialize_length);
  export_values(_serialize, _serialize_length, _positions);

  memmove(_serialize + sizeof(_serialize_length), _serialize, _serialize_length);
  memcpy(_serialize, &_serialize_length, sizeof(_serialize_length));
  printf("_serialize_length: %ld\n", _serialize_length);
  _serialize_length += sizeof(_serialize_length);
  // printf("checkpoint1\n");
  // _serialize_length = 0;
  // _deserialization();
}

void GroupKeyIndex::send_RDMA(const std::string& table_name, const ChunkID& chunk_id, SegmentIndexType index_type,
                              const std::vector<ColumnID>& column_ids) {
  // std::cout << table_name << std::endl;
  // printf("%d %d\n", chunk_id, index_type);
  // for (int ii = 0; ii < column_ids.size(); ii++)
  //   printf("columnid %d:%d\n", ii, column_ids[ii]);

  char* data1 = (char*)malloc(SIZE_DATA1);
  uint64_t length1 = 0ll;
  std::string f = "write";
  memcpy(data1 + length1, f.data(), f.size());
  length1 += f.size();

  export_value(data1, length1, static_cast<uint64_t>(table_name.size()));
  memcpy(data1 + length1, table_name.data(), table_name.size());
  length1 += table_name.size();

  export_value(data1, length1, chunk_id);
  export_value(data1, length1, static_cast<uint8_t>(index_type));

  export_value(data1, length1, static_cast<uint64_t>(column_ids.size()));
  export_values(data1, length1, column_ids);
  Hyrise::get().RDMA_Write(data1, length1, _serialize, _serialize_length);
}

GroupKeyIndex::GroupKeyIndex(const std::string& table_name, const ChunkID& chunk_id, SegmentIndexType index_type,
                             const std::vector<ColumnID>& column_ids)
    : AbstractIndex{get_index_type_of<GroupKeyIndex>()}, _serialize_length(0) {
  // std::cout << table_name << std::endl;
  // printf("%d %d\n", chunk_id, index_type);
  // for (int ii = 0; ii < column_ids.size(); ii++)
  //   printf("columnid %d:%d\n", ii, column_ids[ii]);

  char* data1 = (char*)malloc(SIZE_DATA1);
  uint64_t length1 = 0ll;
  std::string f = "read ";
  memcpy(data1 + length1, f.data(), f.size());
  length1 += f.size();

  export_value(data1, length1, static_cast<uint64_t>(table_name.size()));
  memcpy(data1 + length1, table_name.data(), table_name.size());
  length1 += table_name.size();

  export_value(data1, length1, chunk_id);
  export_value(data1, length1, static_cast<uint8_t>(index_type));

  export_value(data1, length1, static_cast<uint64_t>(column_ids.size()));
  export_values(data1, length1, column_ids);
  _serialize = Hyrise::get().RDMA_Read(data1, length1);
  if (_serialize != nullptr) {
    _deserialization();
  }
  // printf("compare : %d\n",strcmp(pp,_serialize));
}

void GroupKeyIndex::_deserialization() {
  // std::cout << "*-------------------------------*" << std::endl;
  // std::cout << "------GroupKeyIndex::decode------" << std::endl;
  // std::cout << "---------------------------------" << std::endl;
  uint64_t index = 0;
  const auto pointer_length = _read_value<uint64_t>(_serialize, _serialize_length);
  // printf("pointer_length: %ld\n", pointer_length);
  // TODO: read segment
  const auto column_type = _read_value<EncodingType>(_serialize, _serialize_length);
  // printf("column_type: %ld\n", column_type);
  const auto column_datatype = _read_value<DataType>(_serialize, _serialize_length);
  // printf("column_datatype: %ld\n", column_datatype);
  std::shared_ptr<AbstractSegment> result;
  resolve_data_type(column_datatype, [&](auto type) {
    using ColumnDataType = typename decltype(type)::type;
    result = _import_segment<ColumnDataType>(_serialize, _serialize_length, column_type);
    _indexed_segment = std::dynamic_pointer_cast<const BaseDictionarySegment>(result);
  });
  // std::cout << "*-------------------------------*" << std::endl;
  const auto _value_start_offsets_length = _read_value<ValueID::base_type>(_serialize, _serialize_length);
  // printf("value_start_offsets_length: %ld\n", _value_start_offsets_length);
  _value_start_offsets = std::vector<ChunkOffset>(_value_start_offsets_length);
  memcpy(_value_start_offsets.data(), _serialize + _serialize_length,
         _value_start_offsets.size() * sizeof(ChunkOffset));
  _serialize_length += _value_start_offsets.size() * sizeof(ChunkOffset);
  // printf("value_start_offsets \n");
  // for (int i = 0; i < _value_start_offsets.size(); i++) {
  //   printf("[%d]:%d, ", i, _value_start_offsets[i]);
  // }
  // std::cout<<std::endl;
  const auto _positions_length = _read_value<ValueID::base_type>(_serialize, _serialize_length);
  // printf("_positions_length: %ld, address=%ld\n", _positions_length,_serialize_length);
  auto _positions = std::vector<ChunkOffset>(_positions_length);
  memcpy(_positions.data(), _serialize + _serialize_length, _positions.size() * sizeof(ChunkOffset));
  _serialize_length += _positions.size() * sizeof(ChunkOffset);
  // for (int i = 0; i < _positions.size(); i++) {
  //   printf("[%d]:%d, ", i, _positions[i]);
  // }
  // std::cout<<std::endl;
}

GroupKeyIndex::Iterator GroupKeyIndex::_lower_bound(const std::vector<AllTypeVariant>& values) const {
  Assert((values.size() == 1), "Group Key Index expects exactly one input value");
  // the caller is responsible for not passing a NULL value
  Assert(!variant_is_null(values[0]), "Null was passed to lower_bound().");

  ValueID value_id = _indexed_segment->lower_bound(values[0]);
  return _get_positions_iterator_at(value_id);
}

GroupKeyIndex::Iterator GroupKeyIndex::_upper_bound(const std::vector<AllTypeVariant>& values) const {
  Assert((values.size() == 1), "Group Key Index expects exactly one input value");
  // the caller is responsible for not passing a NULL value
  Assert(!variant_is_null(values[0]), "Null was passed to upper_bound().");

  ValueID value_id = _indexed_segment->upper_bound(values[0]);
  return _get_positions_iterator_at(value_id);
}

GroupKeyIndex::Iterator GroupKeyIndex::_cbegin() const {
  return _positions.cbegin();
}

GroupKeyIndex::Iterator GroupKeyIndex::_cend() const {
  return _positions.cend();
}

/**
   *
   * @returns an iterator pointing to the the first ChunkOffset in the positions-vector
   * that belongs to a given non -NULL value-id.
   */
GroupKeyIndex::Iterator GroupKeyIndex::_get_positions_iterator_at(ValueID value_id) const {
  if (value_id == INVALID_VALUE_ID) {
    return _cend();
  }

  // get the start position in the position-vector, ie the offset, by looking up the index_offset at value_id
  auto start_pos = _value_start_offsets[value_id];

  // get an iterator pointing to start_pos
  auto iter = _positions.cbegin();
  std::advance(iter, start_pos);
  return iter;
}

std::vector<std::shared_ptr<const AbstractSegment>> GroupKeyIndex::_get_indexed_segments() const {
  return {_indexed_segment};
}

size_t GroupKeyIndex::_memory_consumption() const {
  size_t bytes = sizeof(_indexed_segment);
  bytes += sizeof(std::vector<ChunkOffset>);  // _value_start_offsets
  bytes += sizeof(ChunkOffset) * _value_start_offsets.capacity();
  bytes += sizeof(std::vector<ChunkOffset>);  // _positions
  bytes += sizeof(ChunkOffset) * _positions.capacity();
  return bytes;
}

}  // namespace hyrise
