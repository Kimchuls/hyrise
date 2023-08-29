#pragma once
#include "Index.hpp"
namespace vindex
{
  struct IDSelector
  {
  public:
    virtual bool is_member(int64_t id) const = 0;
    virtual ~IDSelector() {}
  };
  /** ids between [imin, imax) */
  struct IDSelectorRange : IDSelector
  {
    int64_t imin, imax;
    bool assume_sorted;
    IDSelectorRange(int64_t imin, int64_t imax, bool assume_sorted = false);
    bool is_member(int64_t id) const final;
    void find_sorted_ids_bounds(
        size_t list_size,
        const int64_t *ids,
        size_t *jmin,
        size_t *jmax) const;

    ~IDSelectorRange() override {}
  };
  struct IDSelectorArray : IDSelector
  {
    size_t n;
    const int64_t *ids;

    /** Construct with an array of ids to process
     *
     * @param n number of ids to store
     * @param ids elements to store. The pointer should remain valid during
     *            IDSelectorArray's lifetime
     */
    IDSelectorArray(size_t n, const int64_t *ids);
    bool is_member(int64_t id) const final;
    ~IDSelectorArray() override {}
  };

} // namespace vindex
