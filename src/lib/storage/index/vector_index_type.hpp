#pragma once

#include <cstdint>

#include <boost/hana/at_key.hpp>

#include "all_type_variant.hpp"

namespace hyrise {

namespace hana = boost::hana;

enum class VectorIndexType : uint8_t { HNSW, IVFFlat };
enum class VectorTestBase : uint8_t { sift1m, gist1m, sift10m };

class HNSWIndex;
class IVFFlatIndex;

namespace detail {

constexpr auto vector_index_map =
    hana::make_map(hana::make_pair(hana::type_c<HNSWIndex>, VectorIndexType::HNSW),
    hana::make_pair(hana::type_c<IVFFlatIndex>, VectorIndexType::IVFFlat));

}  // namespace detail

template <typename IndexType>
VectorIndexType get_vector_index_type_of() {
  return detail::vector_index_map[hana::type_c<IndexType>];
}

}  // namespace hyrise
