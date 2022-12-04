#include "composite_group_key_index.hpp"

#include <algorithm>
#include <climits>
#include <cstdint>
#include <iterator>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "storage/base_dictionary_segment.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"
#include "storage/vector_compression/base_vector_decompressor.hpp"
#include "storage/vector_compression/fixed_width_integer/fixed_width_integer_utils.hpp"
#include "utils/assert.hpp"
#include "variable_length_key_proxy.hpp"

#include "hyrise.hpp"
#include "resolve_type.hpp"
#include "storage/index/abstract_index.hpp"
#include "storage/vector_compression/bitpacking/bitpacking_vector.hpp"
#include "storage/vector_compression/resolve_compressed_vector_type.hpp"
#include "types.hpp"

namespace hyrise {

// Writes a shallow copy of the given value to the ofstream
template <typename T>
void CompositeGroupKeyIndex::export_value(char* pointer, uint64_t& length, const T& value) {
  // printf("export_values::T\n");
  // std::cout << value << std::endl;
  memcpy(pointer + length, &value, sizeof(value));
  length += sizeof(value);
}

template <typename T, typename Alloc>
void CompositeGroupKeyIndex::export_values(char* pointer, uint64_t& length, const std::vector<T, Alloc>& values) {
  // printf("export_values::vector<T, Alloc>\n");
  memcpy(pointer + length, values.data(), values.size() * sizeof(T));
  // for (int i = 0; i < values.size(); i++) {
  //   printf("value %d:%d, ", i, values[i]);
  // }
  // std::cout << std::endl;
  length += values.size() * sizeof(T);
}

void CompositeGroupKeyIndex::export_values(char* pointer, uint64_t& length, const FixedStringVector& values) {
  // printf("export_values::FixedString\n");
  memcpy(pointer + length, values.data(), static_cast<int64_t>(values.size() * values.string_length()));
  // for (int i = 0; i < values.size(); i++) {
  //   printf("value %d:%s, ", i, values.data()[i]);
  // }
  // std::cout << std::endl;
  length += static_cast<int64_t>(values.size() * values.string_length());
}

void CompositeGroupKeyIndex::export_string_values(char* pointer, uint64_t& length,
                                                  const pmr_vector<pmr_string>& values) {
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
void CompositeGroupKeyIndex::export_values(char* pointer, uint64_t& length, const pmr_vector<pmr_string>& values) {
  // printf("export_values::pmr_vector<pmr_string>\n");
  export_string_values(pointer, length, values);
}

// specialized implementation for bool values
template <typename Alloc>
void CompositeGroupKeyIndex::export_values(char* pointer, uint64_t& length, const std::vector<bool, Alloc>& values) {
  // printf("export_values::pmr_vector<bool>\n");
  // Cast to fixed-size format used in binary file
  const auto writable_bools = pmr_vector<BoolAsByteType>(values.begin(), values.end());
  export_values(pointer, length, writable_bools);
}

void CompositeGroupKeyIndex::_export_compact_vector(char* pointer, uint64_t& length, const pmr_compact_vector& values) {
  // printf("export_compact_vector\n");
  export_value(pointer, length, static_cast<uint8_t>(values.bits()));
  memcpy(pointer + length, values.get(), static_cast<int64_t>(values.bytes()));
}

template <typename T>
pmr_vector<T> CompositeGroupKeyIndex::_read_values(char* pointer, uint64_t& length, const size_t count) {
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
pmr_vector<bool> CompositeGroupKeyIndex::_read_values(char* pointer, uint64_t& length, const size_t count) {
  // printf("read_values::pmr_vector<bool>\n");
  pmr_vector<BoolAsByteType> readable_bools(count);
  memcpy(readable_bools.data(), pointer + length, static_cast<int64_t>(readable_bools.size() * sizeof(BoolAsByteType)));
  length += static_cast<int64_t>(readable_bools.size() * sizeof(BoolAsByteType));
  return {readable_bools.begin(), readable_bools.end()};
}

template <typename T>
T CompositeGroupKeyIndex::_read_value(char* pointer, uint64_t& length) {
  // printf("read_values::T\n");
  T result;
  memcpy(&result, pointer + length, sizeof(T));
  length += sizeof(T);
  return result;
}

pmr_vector<pmr_string> CompositeGroupKeyIndex::_read_string_values(char* pointer, uint64_t& length,
                                                                   const size_t count) {
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
pmr_vector<pmr_string> CompositeGroupKeyIndex::_read_values(char* pointer, uint64_t& length, const size_t count) {
  // printf("read_values::pmr_vector<pmr_string>\n");
  return _read_string_values(pointer, length, count);
}

template <typename T>
pmr_compact_vector CompositeGroupKeyIndex::_read_values_compact_vector(char* pointer, uint64_t& length,
                                                                       const size_t count) {
  // printf("_read_values_compact_vector\n");
  const auto bit_width = _read_value<uint8_t>(pointer, length);
  auto values = pmr_compact_vector(bit_width, count);
  memcpy(values.get(), pointer + length, static_cast<int64_t>(values.bytes()));
  length += static_cast<int64_t>(values.bytes());
  return values;
}

std::shared_ptr<BaseCompressedVector> CompositeGroupKeyIndex::_import_attribute_vector(
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
std::shared_ptr<ValueSegment<T>> CompositeGroupKeyIndex::_import_value_segment(char* pointer, uint64_t& index) {
  std::cout << "_import_value_segment" << std::endl;
  exit(0);
}

template <typename T>
std::shared_ptr<DictionarySegment<T>> CompositeGroupKeyIndex::_import_dictionary_segment(char* pointer,
                                                                                         uint64_t& index) {
  std::cout << "_import_dictionary_segment" << std::endl;
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

std::shared_ptr<FixedStringDictionarySegment<pmr_string>>
CompositeGroupKeyIndex::_import_fixed_string_dictionary_segment(char* pointer, uint64_t& index) {
  std::cout << "_import_fixed_string_dictionary_segment" << std::endl;
  exit(0);
}

template <typename T>
std::shared_ptr<RunLengthSegment<T>> CompositeGroupKeyIndex::_import_run_length_segment(char* pointer,
                                                                                        uint64_t& index) {
  std::cout << "_import_run_length_segment" << std::endl;
  exit(0);
}

template <typename T>
std::shared_ptr<FrameOfReferenceSegment<T>> CompositeGroupKeyIndex::_import_frame_of_reference_segment(
    char* pointer, uint64_t& index) {
  std::cout << "_import_frame_of_reference_segment" << std::endl;
  exit(0);
}

template <typename T>
std::shared_ptr<LZ4Segment<T>> CompositeGroupKeyIndex::_import_lz4_segment(char* pointer, uint64_t& index) {
  std::cout << "_import_lz4_segment" << std::endl;
  exit(0);
}

template <typename ColumnDataType>
std::shared_ptr<AbstractSegment> CompositeGroupKeyIndex::_import_segment(char* pointer, uint64_t& index,
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
CompressedVectorTypeID CompositeGroupKeyIndex::_compressed_vector_type_id(
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

void CompositeGroupKeyIndex::_export_compressed_vector(char* pointer, uint64_t& length, const CompressedVectorType type,
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
void CompositeGroupKeyIndex::_write_segment(char* _serialize, uint64_t& _serialize_length,
                                            const DictionarySegment<T>& dictionary_segment) {
  // std::cout << "*-------------------------------*" << std::endl;
  // std::cout << "_write_segment::DictionarySegment" << std::endl;
  // std::cout << "-------------encode--------------" << std::endl;
  const auto encodingtype = dictionary_segment.encoding_type();
  export_value(_serialize, _serialize_length, encodingtype);
  // printf("encodingtype: %ld\n", encodingtype);
  const auto column_datatype = dictionary_segment.data_type();
  export_value(_serialize, _serialize_length, column_datatype);
  // printf("column_datatype: %ld\n", column_datatype);
  const auto compressed_vector_type_id = _compressed_vector_type_id<T>(dictionary_segment);
  export_value(_serialize, _serialize_length, compressed_vector_type_id);
  // printf("compressed_vector_type_id: \'%d\'\n", compressed_vector_type_id);
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
void CompositeGroupKeyIndex::_write_segment(char* _serialize, uint64_t& _serialize_length,
                                            const ValueSegment<T>& value_segment) {
  std::cout << "_write_segment::ValueSegment" << std::endl;
  exit(0);
}

void CompositeGroupKeyIndex::_write_segment(char* _serialize, uint64_t& _serialize_length,
                                            const ReferenceSegment& reference_segment) {
  std::cout << "_write_segment::ReferenceSegment" << std::endl;
  exit(0);
}

template <typename T>
void CompositeGroupKeyIndex::_write_segment(char* _serialize, uint64_t& _serialize_length,
                                            const LZ4Segment<T>& lz4_segment) {
  std::cout << "_write_segment::LZ4Segment" << std::endl;
  exit(0);
}

template <typename T>
void CompositeGroupKeyIndex::_write_segment(char* _serialize, uint64_t& _serialize_length,
                                            const RunLengthSegment<T>& run_length_segment) {
  std::cout << "_write_segment::RunLengthSegment" << std::endl;
  exit(0);
}

template <typename T>
void CompositeGroupKeyIndex::_write_segment(char* _serialize, uint64_t& _serialize_length,
                                            const FrameOfReferenceSegment<T>& frame_of_reference_segment) {
  std::cout << "_write_segment::FrameOfReferenceSegment" << std::endl;
  exit(0);
}

template <typename T>
void CompositeGroupKeyIndex::_write_segment(char* _serialize, uint64_t& _serialize_length,
                                            const FixedStringDictionarySegment<T>& fixed_string_dictionary_segment) {
  std::cout << "_write_segment::FixedStringDictionarySegment" << std::endl;
  exit(0);
}
}  // namespace hyrise

namespace hyrise {

size_t CompositeGroupKeyIndex::estimate_memory_consumption(ChunkOffset row_count, ChunkOffset distinct_count,
                                                           uint32_t value_bytes) {
  return (static_cast<size_t>(row_count) + distinct_count) * sizeof(ChunkOffset) +
         static_cast<size_t>(distinct_count * value_bytes);
}

CompositeGroupKeyIndex::CompositeGroupKeyIndex(
    const std::vector<std::shared_ptr<const AbstractSegment>>& segments_to_index)
    : AbstractIndex{get_index_type_of<CompositeGroupKeyIndex>()} {
  Assert(!segments_to_index.empty(), "CompositeGroupKeyIndex requires at least one segment to be indexed.");

  if constexpr (HYRISE_DEBUG) {
    auto first_size = segments_to_index.front()->size();
    auto all_segments_have_same_size =
        std::all_of(segments_to_index.cbegin(), segments_to_index.cend(),
                    [first_size](const auto& segment) { return segment->size() == first_size; });

    Assert(all_segments_have_same_size,
           "CompositeGroupKey requires same length of all segments that should be indexed.");
  }

  // cast and check segments
  _indexed_segments.reserve(segments_to_index.size());
  for (const auto& segment : segments_to_index) {
    auto dict_segment = std::dynamic_pointer_cast<const BaseDictionarySegment>(segment);
    Assert(static_cast<bool>(dict_segment), "CompositeGroupKeyIndex only works with dictionary segments.");
    Assert(dict_segment->compressed_vector_type(),
           "Expected DictionarySegment to use vector compression for attribute vector");
    Assert(is_fixed_width_integer(*dict_segment->compressed_vector_type()),
           "CompositeGroupKeyIndex only works with Fixed-width integer compressed attribute vectors.");
    _indexed_segments.emplace_back(dict_segment);
  }

  // retrieve memory consumption by each concatenated key
  auto bytes_per_key =
      std::accumulate(_indexed_segments.begin(), _indexed_segments.end(), CompositeKeyLength{0u},
                      [](auto key_length, const auto& segment) {
                        return key_length + byte_width_for_fixed_width_integer_type(*segment->compressed_vector_type());
                      });

  // create concatenated keys and save their positions
  // at this point duplicated keys may be created, they will be handled later
  auto segment_size = _indexed_segments.front()->size();
  auto keys = std::vector<VariableLengthKey>(segment_size);
  _position_list.resize(segment_size);

  auto attribute_vector_widths_and_decompressors = [&]() {
    auto decompressors =
        std::vector<std::pair<size_t, std::unique_ptr<BaseVectorDecompressor>>>(_indexed_segments.size());

    std::transform(
        _indexed_segments.cbegin(), _indexed_segments.cend(), decompressors.begin(), [](const auto& segment) {
          const auto byte_width = byte_width_for_fixed_width_integer_type(*segment->compressed_vector_type());
          auto decompressor = segment->attribute_vector()->create_base_decompressor();
          return std::make_pair(byte_width, std::move(decompressor));
        });

    return decompressors;
  }();

  for (auto chunk_offset = ChunkOffset{0}; chunk_offset < static_cast<ChunkOffset>(segment_size); ++chunk_offset) {
    auto concatenated_key = VariableLengthKey(bytes_per_key);
    for (const auto& [byte_width, decompressor] : attribute_vector_widths_and_decompressors) {
      concatenated_key.shift_and_set(decompressor->get(chunk_offset), static_cast<uint8_t>(byte_width * CHAR_BIT));
    }
    keys[chunk_offset] = std::move(concatenated_key);
    _position_list[chunk_offset] = chunk_offset;
  }

  // sort keys and their positions
  std::sort(_position_list.begin(), _position_list.end(),
            [&keys](auto left, auto right) { return keys[left] < keys[right]; });

  _keys = VariableLengthKeyStore(static_cast<ChunkOffset>(segment_size), bytes_per_key);
  for (auto chunk_offset = ChunkOffset{0}; chunk_offset < static_cast<ChunkOffset>(segment_size); ++chunk_offset) {
    _keys[chunk_offset] = keys[_position_list[chunk_offset]];
  }

  // create offsets to unique keys
  _key_offsets.reserve(segment_size);
  _key_offsets.emplace_back(0);
  for (auto chunk_offset = ChunkOffset{1}; chunk_offset < static_cast<ChunkOffset>(segment_size); ++chunk_offset) {
    if (_keys[chunk_offset] != _keys[ChunkOffset{chunk_offset - 1}]) {
      _key_offsets.emplace_back(chunk_offset);
    }
  }
  _key_offsets.shrink_to_fit();

  // remove duplicated keys
  auto unique_keys_end = std::unique(_keys.begin(), _keys.end());
  _keys.erase(unique_keys_end, _keys.end());
  _keys.shrink_to_fit();

  _serialization();
}

void CompositeGroupKeyIndex::_serialization() {
  // std::cout << "*-------------------------------*" << std::endl;
  // std::cout << "--CompositeGroupKeyIndex::encode-" << std::endl;
  // std::cout << "---------------------------------" << std::endl;
  _serialize = (char*)malloc(ART_INDEX_SIZE);
  _serialize_length = 0;
  const auto _indexed_segments_length = _indexed_segments.size();
  export_value(_serialize, _serialize_length, static_cast<size_t>(_indexed_segments_length));
  // printf("_indexed_segments_length: %ld\n", _indexed_segments_length);

  for (auto iter = 0; iter < _indexed_segments_length; iter++) {
    resolve_data_and_segment_type(*(std::dynamic_pointer_cast<const AbstractSegment>(_indexed_segments[iter])),
                                  [&](const auto data_type_t, const auto& resolved_segment) {
                                    _write_segment(_serialize, _serialize_length, resolved_segment);
                                  });
  }

  const auto _keys_bytes_per_key = _keys._bytes_per_key;
  export_value(_serialize, _serialize_length, static_cast<size_t>(_keys_bytes_per_key));
  // printf("_keys_bytes_per_key: %ld\n", _keys_bytes_per_key);

  const auto _keys_key_alignment = _keys._key_alignment;
  export_value(_serialize, _serialize_length, static_cast<size_t>(_keys_key_alignment));
  // printf("_keys_key_alignment: %ld\n", _keys_key_alignment);

  const auto _keys_data_length = _keys._data.size();
  export_value(_serialize, _serialize_length, static_cast<size_t>(_keys_data_length));
  // printf("_keys_data_length: %ld\n", _keys_data_length);
  export_values(_serialize, _serialize_length, _keys._data);
  // for (auto i = 0; i < 10; i++) {
  // printf("value %d:%ld ", i, _keys._data.data()[i]);
  // }
  // printf("\n");
  // for (auto i = _keys._data.size() - 10; i < _keys._data.size(); i++) {
  // printf("value %d:%ld ", i, _keys._data.data()[i]);
  // }
  // printf("\n");
  // exit(0);
  const auto _key_offsets_length = _key_offsets.size();
  export_value(_serialize, _serialize_length, static_cast<size_t>(_key_offsets_length));
  // printf("_key_offsets_length: %ld\n", _key_offsets_length);
  export_values(_serialize, _serialize_length, _key_offsets);

  const auto _position_list_length = _position_list.size();
  export_value(_serialize, _serialize_length, static_cast<size_t>(_position_list_length));
  // printf("_position_list_length: %ld, address=%ld\n", _position_list_length, _serialize_length);
  export_values(_serialize, _serialize_length, _position_list);

  memmove(_serialize + sizeof(_serialize_length), _serialize, _serialize_length);
  memcpy(_serialize, &_serialize_length, sizeof(_serialize_length));
  // printf("_serialize_length: %ld\n", _serialize_length);
  _serialize_length += sizeof(_serialize_length);
  // printf("checkpoint1\n");
  // _serialize_length = 0;
  // _deserialization();
}

void CompositeGroupKeyIndex::send_RDMA(const std::string& table_name, const ChunkID& chunk_id,
                                       SegmentIndexType index_type, const std::vector<ColumnID>& column_ids) {
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
  // printf("%ld\n", static_cast<uint64_t>(table_name.size()));
  memcpy(data1 + length1, table_name.data(), table_name.size());

  length1 += table_name.size();

  export_value(data1, length1, chunk_id);
  export_value(data1, length1, static_cast<uint8_t>(index_type));

  export_value(data1, length1, static_cast<uint64_t>(column_ids.size()));
  export_values(data1, length1, column_ids);
  Hyrise::get().RDMA_Write(data1, length1, _serialize, _serialize_length);
}

CompositeGroupKeyIndex::CompositeGroupKeyIndex(const std::string& table_name, const ChunkID& chunk_id,
                                               SegmentIndexType index_type, const std::vector<ColumnID>& column_ids)
    : AbstractIndex{get_index_type_of<CompositeGroupKeyIndex>()}, _serialize_length(0) {
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
  // auto x = length1;
  // printf("%ld\n",length1);
  export_values(data1, length1, column_ids);
  // std::vector<uint16_t> y(column_ids.size());
  // memcpy(y.data(), data1 + x, sizeof(uint16_t) * column_ids.size());
  // printf("ydata_: %d %d\n",y[0],y[1]);
  _serialize = Hyrise::get().RDMA_Read(data1, length1);
  // printf("composite group key\n");
  if (_serialize != nullptr) {
    _deserialization();
  } else {
    // printf("null pointer!\n");
  }
  // printf("compare : %d\n",strcmp(pp,_serialize));
}

void CompositeGroupKeyIndex::_deserialization() {
  // std::cout << "*-------------------------------*" << std::endl;
  // std::cout << "--CompositeGroupKeyIndex::decode-" << std::endl;
  // std::cout << "---------------------------------" << std::endl;
  uint64_t index = 0;
  const auto pointer_length = _read_value<size_t>(_serialize, _serialize_length);
  // printf("pointer_length: %ld\n", pointer_length);
  // TODO: read segment
  const auto indexed_segments_length = _read_value<size_t>(_serialize, _serialize_length);
  // printf("indexed_segments_length: %ld\n", indexed_segments_length);
  for (auto i = 0; i < indexed_segments_length; i++) {
    const auto column_type = _read_value<EncodingType>(_serialize, _serialize_length);
    // printf("column_type: %ld\n", column_type);
    const auto column_datatype = _read_value<DataType>(_serialize, _serialize_length);
    // printf("column_datatype: %ld\n", column_datatype);
    std::shared_ptr<AbstractSegment> result;
    resolve_data_type(column_datatype, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      result = _import_segment<ColumnDataType>(_serialize, _serialize_length, column_type);
      auto dict_segment = std::dynamic_pointer_cast<const BaseDictionarySegment>(result);
      _indexed_segments.emplace_back(dict_segment);
    });
  }
  // std::cout << "*-------------------------------*" << std::endl;
  //TODO: read _keys

  const auto bytes_per_key = _read_value<size_t>(_serialize, _serialize_length);
  // printf("bytes_per_key: %ld\n", bytes_per_key);

  const auto key_alignment = _read_value<size_t>(_serialize, _serialize_length);
  // printf("key_alignment: %ld\n", key_alignment);

  const auto keys_data_length = _read_value<size_t>(_serialize, _serialize_length);
  // printf("keys_data_length: %ld\n", keys_data_length);
  _keys = VariableLengthKeyStore(bytes_per_key, key_alignment, keys_data_length);
  memcpy(_keys._data.data(), _serialize + _serialize_length, _keys._data.size() * sizeof(VariableLengthKeyWord));
  _serialize_length += _keys._data.size() * sizeof(VariableLengthKeyWord);

  // for (auto i = 0; i < 10; i++) {
  //   printf("value %d:%ld ", i, _keys._data.data()[i]);
  // }
  // printf("\n");
  // for (auto i = _keys._data.size() - 10; i < _keys._data.size(); i++) {
  //   printf("value %d:%ld ", i, _keys._data.data()[i]);
  // }
  // printf("\n");

  //TODO: read other vectors
  const auto _key_offsets_length = _read_value<size_t>(_serialize, _serialize_length);
  // printf("_key_offsets_length: %ld\n", _key_offsets_length);
  _key_offsets = std::vector<ChunkOffset>(_key_offsets_length);
  memcpy(_key_offsets.data(), _serialize + _serialize_length, _key_offsets.size() * sizeof(ChunkOffset));
  _serialize_length += _key_offsets.size() * sizeof(ChunkOffset);
  // printf("value_start_offsets \n");
  // for (int i = 0; i < value_start_offsets.size(); i++) {
  //   printf("[%d]:%d, ", i, value_start_offsets[i]);
  // }
  // std::cout<<std::endl;
  const auto _position_list_length = _read_value<size_t>(_serialize, _serialize_length);
  // printf("_position_list_length: %ld, address=%ld\n", _position_list_length, _serialize_length);
  _position_list = std::vector<ChunkOffset>(_position_list_length);
  memcpy(_position_list.data(), _serialize + _serialize_length, _position_list.size() * sizeof(ChunkOffset));
  _serialize_length += _position_list.size() * sizeof(ChunkOffset);
  // for (int i = 0; i < positions.size(); i++) {
  //   printf("[%d]:%d, ", i, positions[i]);
  // }
  // std::cout<<std::endl;
}

AbstractIndex::Iterator CompositeGroupKeyIndex::_cbegin() const {
  return _position_list.cbegin();
}

AbstractIndex::Iterator CompositeGroupKeyIndex::_cend() const {
  return _position_list.cend();
}

AbstractIndex::Iterator CompositeGroupKeyIndex::_lower_bound(const std::vector<AllTypeVariant>& values) const {
  auto composite_key = _create_composite_key(values, false);
  return _get_position_iterator_for_key(composite_key);
}

AbstractIndex::Iterator CompositeGroupKeyIndex::_upper_bound(const std::vector<AllTypeVariant>& values) const {
  auto composite_key = _create_composite_key(values, true);
  return _get_position_iterator_for_key(composite_key);
}

VariableLengthKey CompositeGroupKeyIndex::_create_composite_key(const std::vector<AllTypeVariant>& values,
                                                                bool is_upper_bound) const {
  auto result = VariableLengthKey(_keys.key_size());

  // retrieve the partial keys for every value except for the last one and append them into one partial-key
  for (auto column_id = ColumnID{0}; column_id < values.size() - 1; ++column_id) {
    Assert(!variant_is_null(values[column_id]), "CompositeGroupKeyIndex doesn't support NULL handling yet.");
    auto partial_key = _indexed_segments[column_id]->lower_bound(values[column_id]);
    auto bits_of_partial_key =
        byte_width_for_fixed_width_integer_type(*_indexed_segments[column_id]->compressed_vector_type()) * CHAR_BIT;
    result.shift_and_set(partial_key, static_cast<uint8_t>(bits_of_partial_key));
  }

  // retrieve the partial key for the last value (depending on whether we have a lower- or upper-bound-query)
  // and append it to the previously created partial key to obtain the key containing all provided values
  const auto& segment_for_last_value = _indexed_segments[values.size() - 1];
  auto&& partial_key = is_upper_bound ? segment_for_last_value->upper_bound(values.back())
                                      : segment_for_last_value->lower_bound(values.back());
  auto bits_of_partial_key =
      byte_width_for_fixed_width_integer_type(*segment_for_last_value->compressed_vector_type()) * CHAR_BIT;
  result.shift_and_set(partial_key, static_cast<uint8_t>(bits_of_partial_key));

  // fill empty space of key with zeros if less values than segments were provided
  auto empty_bits = std::accumulate(
      _indexed_segments.cbegin() + static_cast<int64_t>(values.size()), _indexed_segments.cend(), uint8_t{0},
      [](const auto& value, const auto& segment) {
        return value + byte_width_for_fixed_width_integer_type(*segment->compressed_vector_type()) * CHAR_BIT;
      });
  result <<= empty_bits;

  return result;
}

AbstractIndex::Iterator CompositeGroupKeyIndex::_get_position_iterator_for_key(const VariableLengthKey& key) const {
  // get an iterator pointing to the search-key in the keystore
  // (use always lower_bound() since the search method is already handled within creation of composite key)
  auto key_it = std::lower_bound(_keys.cbegin(), _keys.cend(), key);
  if (key_it == _keys.cend()) {
    return _position_list.cend();
  }

  // get the start position in the position-vector, ie the offset, by getting the offset_iterator for the key
  // (which is at the same position as the iterator for the key in the keystore)
  auto offset_it = _key_offsets.cbegin();
  std::advance(offset_it, std::distance(_keys.cbegin(), key_it));

  // get an iterator pointing to that start position
  auto position_it = _position_list.cbegin();
  std::advance(position_it, *offset_it);

  return position_it;
}

std::vector<std::shared_ptr<const AbstractSegment>> CompositeGroupKeyIndex::_get_indexed_segments() const {
  auto result = std::vector<std::shared_ptr<const AbstractSegment>>();
  result.reserve(_indexed_segments.size());
  for (auto&& indexed_segment : _indexed_segments) {
    result.emplace_back(indexed_segment);
  }
  return result;
}

size_t CompositeGroupKeyIndex::_memory_consumption() const {
  auto byte_count = static_cast<size_t>(_keys.size() * _keys.key_size());
  byte_count += _key_offsets.size() * sizeof(ChunkOffset);
  byte_count += _position_list.size() * sizeof(ChunkOffset);
  return byte_count;
}

}  // namespace hyrise
