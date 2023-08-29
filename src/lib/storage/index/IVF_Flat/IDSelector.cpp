#include "IDSelector.hpp"
#include "VIndexAssert.hpp"

namespace vindex
{
  IDSelectorRange::IDSelectorRange(int64_t imin, int64_t imax, bool assume_sorted)
      : imin(imin), imax(imax), assume_sorted(assume_sorted) {}

  bool IDSelectorRange::is_member(int64_t id) const
  {
    return id >= imin && id < imax;
  }

  void IDSelectorRange::find_sorted_ids_bounds(
      size_t list_size,
      const int64_t *ids,
      size_t *jmin_out,
      size_t *jmax_out) const
  {
    VINDEX_ASSERT(assume_sorted);
    if (list_size == 0 || imax <= ids[0] || imin > ids[list_size - 1])
    {
      *jmin_out = *jmax_out = 0;
      return;
    }
    // bissection to find imin
    if (ids[0] >= imin)
    {
      *jmin_out = 0;
    }
    else
    {
      size_t j0 = 0, j1 = list_size;
      while (j1 > j0 + 1)
      {
        size_t jmed = (j0 + j1) / 2;
        if (ids[jmed] >= imin)
        {
          j1 = jmed;
        }
        else
        {
          j0 = jmed;
        }
      }
      *jmin_out = j1;
    }
    // bissection to find imax
    if (*jmin_out == list_size || ids[*jmin_out] >= imax)
    {
      *jmax_out = *jmin_out;
    }
    else
    {
      size_t j0 = *jmin_out, j1 = list_size;
      while (j1 > j0 + 1)
      {
        size_t jmed = (j0 + j1) / 2;
        if (ids[jmed] >= imax)
        {
          j1 = jmed;
        }
        else
        {
          j0 = jmed;
        }
      }
      *jmax_out = j1;
    }
  }
  IDSelectorArray::IDSelectorArray(size_t n, const int64_t *ids) : n(n), ids(ids) {}

  bool IDSelectorArray::is_member(int64_t id) const
  {
    for (int64_t i = 0; i < n; i++)
    {
      if (ids[i] == id)
        return true;
    }
    return false;
  }
} // namespace vindex
