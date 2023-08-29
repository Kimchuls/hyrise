#ifndef DIRECT_MAP_HPP
#define DIRECT_MAP_HPP
#include "InvertedLists.hpp"
#include <unordered_map>
namespace vindex
{
  struct IDSelector;
  inline uint64_t lo_build(uint64_t list_id, uint64_t offset)
  {
    return list_id << 32 | offset;
  }

  inline uint64_t lo_listno(uint64_t lo)
  {
    return lo >> 32;
  }

  inline uint64_t lo_offset(uint64_t lo)
  {
    return lo & 0xffffffff;
  }
  struct DirectMap
  {
  public:
    enum Type
    {
      NoMap = 0,    // default
      Array = 1,    // sequential ids (only for add, no add_with_ids)
      Hashtable = 2 // arbitrary ids
    };
    Type type;
    std::vector<int64_t> array;
    std::unordered_map<int64_t, int64_t> hashtable;

    DirectMap();
    void set_type(Type new_type, const InvertedLists *invlists, size_t ntotal);
    int64_t get(int64_t id) const;
    bool no() const
    {
      return type == NoMap;
    }

    /**
     * update the direct_map
     */

    void check_can_add(const int64_t *ids);
    void add_single_id(int64_t id, int64_t list_no, size_t offset);
    void clear();

    /**
     * operations on inverted lists that require translation with a DirectMap
     */
    size_t remove_ids(const IDSelector &sel, InvertedLists *invlists);
    void update_codes(InvertedLists *invlists, int n, const int64_t *ids, const int64_t *list_nos, const uint8_t *codes);
  };

  struct DirectMapAdd
  {
    using Type = DirectMap::Type;

    DirectMap &direct_map;
    DirectMap::Type type;
    size_t ntotal;
    size_t n;
    const int64_t *xids;

    std::vector<int64_t> all_ofs;

    DirectMapAdd(DirectMap &direct_map, size_t n, const int64_t *xids);

    /// add vector i (with id xids[i]) at list_no and offset
    void add(size_t i, int64_t list_no, size_t offset);

    ~DirectMapAdd();
  };
} // namespace vindex

#endif