#include "hnsw_index.hpp"
#include "types.hpp"
#include "storage/segment_iterate.hpp"

namespace hyrise {

HNSWIndex::HNSWIndex(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index,
                     const ColumnID column_id)
    : _column_id{column_id} {
  Assert(!chunks_to_index.empty(), "HNSWIndex requires chunks_to_index not to be empty.");
}

// bool HNSWIndex::is_index_for(const ColumnID column_id) const {
//   return column_id == _column_id;
// }

}  // namespace hyrise
