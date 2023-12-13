#pragma once

#include <cstdint>

#include <boost/hana/at_key.hpp>

#include "all_type_variant.hpp"

namespace hyrise {

namespace hana = boost::hana;

enum class VectorIndexType : uint8_t { HNSW, IVFFlat, IVFPQ };

// class HNSWIndex;
class HNSWFlatIndex;
class IVFFlatIndex;
class IVFPQIndex;

namespace detail {

constexpr auto vector_index_map =
    // hana::make_map(hana::make_pair(hana::type_c<HNSWIndex>, VectorIndexType::HNSW),
    hana::make_map(hana::make_pair(hana::type_c<HNSWFlatIndex>, VectorIndexType::HNSW),
    hana::make_pair(hana::type_c<IVFFlatIndex>, VectorIndexType::IVFFlat),
    hana::make_pair(hana::type_c<IVFPQIndex>, VectorIndexType::IVFPQ)
    );

}  // namespace detail

template <typename IndexType>
VectorIndexType get_vector_index_type_of() {
  return detail::vector_index_map[hana::type_c<IndexType>];
}

}  // namespace hyrise
