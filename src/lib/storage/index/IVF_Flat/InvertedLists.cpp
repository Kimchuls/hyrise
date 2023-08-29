#include "InvertedLists.hpp"
#include <assert.h>
#include <cstdio>
#include <memory>

#include "utils.hpp"
#include "VIndexAssert.hpp"

namespace vindex
{
  /*****************************************
   * InvertedLists implementation
   ******************************************/

  InvertedLists::InvertedLists(size_t nlist, size_t code_size)
      : nlist(nlist), code_size(code_size), use_iterator(false) {}

  InvertedLists::~InvertedLists() {}

  bool InvertedLists::is_empty(size_t list_no) const
  {
    return use_iterator
               ? !std::unique_ptr<InvertedListsIterator>(get_iterator(list_no))
                      ->is_available()
               : list_size(list_no) == 0;
  }

  int64_t InvertedLists::get_single_id(size_t list_no, size_t offset) const
  {
    assert(offset < list_size(list_no));
    const int64_t *ids = get_ids(list_no);
    int64_t id = ids[offset];
    release_ids(list_no, ids);
    return id;
  }

  void InvertedLists::release_codes(size_t, const uint8_t *) const {}

  void InvertedLists::release_ids(size_t, const int64_t *) const {}

  void InvertedLists::prefetch_lists(const int64_t *, int) const {}

  const uint8_t *InvertedLists::get_single_code(size_t list_no, size_t offset)
      const
  {
    assert(offset < list_size(list_no));
    return get_codes(list_no) + offset * code_size;
  }

  size_t InvertedLists::add_entry(
      size_t list_no,
      int64_t theid,
      const uint8_t *code)
  {
    return add_entries(list_no, 1, &theid, code);
  }

  void InvertedLists::update_entry(
      size_t list_no,
      size_t offset,
      int64_t id,
      const uint8_t *code)
  {
    update_entries(list_no, offset, 1, &id, code);
  }

  void InvertedLists::reset()
  {
    for (size_t i = 0; i < nlist; i++)
    {
      resize(i, 0);
    }
  }

  InvertedListsIterator *InvertedLists::get_iterator(size_t /*list_no*/) const
  {
    VINDEX_THROW_MSG("get_iterator is not supported");
  }

  void InvertedLists::merge_from(InvertedLists *oivf, size_t add_id)
  {
#pragma omp parallel for
    for (int64_t i = 0; i < nlist; i++)
    {
      size_t list_size = oivf->list_size(i);
      ScopedIds ids(oivf, i);
      if (add_id == 0)
      {
        add_entries(i, list_size, ids.get(), ScopedCodes(oivf, i).get());
      }
      else
      {
        std::vector<int64_t> new_ids(list_size);

        for (size_t j = 0; j < list_size; j++)
        {
          new_ids[j] = ids[j] + add_id;
        }
        add_entries(
            i, list_size, new_ids.data(), ScopedCodes(oivf, i).get());
      }
      oivf->resize(i, 0);
    }
  }

  size_t InvertedLists::copy_subset_to(
      InvertedLists &oivf,
      subset_type_t subset_type,
      int64_t a1,
      int64_t a2) const
  {
    VINDEX_THROW_IF_NOT(nlist == oivf.nlist);
    VINDEX_THROW_IF_NOT(code_size == oivf.code_size);
    VINDEX_THROW_IF_NOT_FMT(
        subset_type >= 0 && subset_type <= 4,
        "subset type %d not implemented",
        subset_type);
    size_t accu_n = 0;
    size_t accu_a1 = 0;
    size_t accu_a2 = 0;
    size_t n_added = 0;

    size_t ntotal = 0;
    if (subset_type == 2)
    {
      ntotal = compute_ntotal();
    }

    for (int64_t list_no = 0; list_no < nlist; list_no++)
    {
      size_t n = list_size(list_no);
      ScopedIds ids_in(this, list_no);

      if (subset_type == SUBSET_TYPE_ID_RANGE)
      {
        for (int64_t i = 0; i < n; i++)
        {
          int64_t id = ids_in[i];
          if (a1 <= id && id < a2)
          {
            oivf.add_entry(
                list_no,
                get_single_id(list_no, i),
                ScopedCodes(this, list_no, i).get());
            n_added++;
          }
        }
      }
      else if (subset_type == SUBSET_TYPE_ID_MOD)
      {
        for (int64_t i = 0; i < n; i++)
        {
          int64_t id = ids_in[i];
          if (id % a1 == a2)
          {
            oivf.add_entry(
                list_no,
                get_single_id(list_no, i),
                ScopedCodes(this, list_no, i).get());
            n_added++;
          }
        }
      }
      else if (subset_type == SUBSET_TYPE_ELEMENT_RANGE)
      {
        // see what is allocated to a1 and to a2
        size_t next_accu_n = accu_n + n;
        size_t next_accu_a1 = next_accu_n * a1 / ntotal;
        size_t i1 = next_accu_a1 - accu_a1;
        size_t next_accu_a2 = next_accu_n * a2 / ntotal;
        size_t i2 = next_accu_a2 - accu_a2;

        for (int64_t i = i1; i < i2; i++)
        {
          oivf.add_entry(
              list_no,
              get_single_id(list_no, i),
              ScopedCodes(this, list_no, i).get());
        }

        n_added += i2 - i1;
        accu_a1 = next_accu_a1;
        accu_a2 = next_accu_a2;
      }
      else if (subset_type == SUBSET_TYPE_INVLIST_FRACTION)
      {
        size_t i1 = n * a2 / a1;
        size_t i2 = n * (a2 + 1) / a1;

        for (int64_t i = i1; i < i2; i++)
        {
          oivf.add_entry(
              list_no,
              get_single_id(list_no, i),
              ScopedCodes(this, list_no, i).get());
        }

        n_added += i2 - i1;
      }
      else if (subset_type == SUBSET_TYPE_INVLIST)
      {
        if (list_no >= a1 && list_no < a2)
        {
          oivf.add_entries(
              list_no,
              n,
              ScopedIds(this, list_no).get(),
              ScopedCodes(this, list_no).get());
          n_added += n;
        }
      }
      accu_n += n;
    }
    return n_added;
  }

  double InvertedLists::imbalance_factor() const
  {
    std::vector<int> hist(nlist);

    for (size_t i = 0; i < nlist; i++)
    {
      hist[i] = list_size(i);
    }

    return vindex::imbalance_factor(nlist, hist.data());
  }

  void InvertedLists::print_stats() const
  {
    std::vector<int> sizes(40);
    for (size_t i = 0; i < nlist; i++)
    {
      for (size_t j = 0; j < sizes.size(); j++)
      {
        if ((list_size(i) >> j) == 0)
        {
          sizes[j]++;
          break;
        }
      }
    }
    for (size_t i = 0; i < sizes.size(); i++)
    {
      if (sizes[i])
      {
        printf("list size in < %zu: %d instances\n",
               static_cast<size_t>(1) << i,
               sizes[i]);
      }
    }
  }

  size_t InvertedLists::compute_ntotal() const
  {
    size_t tot = 0;
    for (size_t i = 0; i < nlist; i++)
    {
      tot += list_size(i);
    }
    return tot;
  }

  /*****************************************
   * ArrayInvertedLists implementation
   ******************************************/

  ArrayInvertedLists::ArrayInvertedLists(size_t nlist, size_t code_size)
      : InvertedLists(nlist, code_size)
  {
    ids.resize(nlist);
    codes.resize(nlist);
  }

  size_t ArrayInvertedLists::add_entries(
      size_t list_no,
      size_t n_entry,
      const int64_t *ids_in,
      const uint8_t *code)
  {
    if (n_entry == 0)
      return 0;
    assert(list_no < nlist);
    size_t o = ids[list_no].size();
    ids[list_no].resize(o + n_entry);
    memcpy(&ids[list_no][o], ids_in, sizeof(ids_in[0]) * n_entry);
    codes[list_no].resize((o + n_entry) * code_size);
    memcpy(&codes[list_no][o * code_size], code, code_size * n_entry);
    return o;
  }

  size_t ArrayInvertedLists::list_size(size_t list_no) const
  {
    assert(list_no < nlist);
    return ids[list_no].size();
  }

  const uint8_t *ArrayInvertedLists::get_codes(size_t list_no) const
  {
    assert(list_no < nlist);
    return codes[list_no].data();
  }

  const int64_t *ArrayInvertedLists::get_ids(size_t list_no) const
  {
    assert(list_no < nlist);
    return ids[list_no].data();
  }

  void ArrayInvertedLists::resize(size_t list_no, size_t new_size)
  {
    ids[list_no].resize(new_size);
    codes[list_no].resize(new_size * code_size);
  }

  void ArrayInvertedLists::update_entries(
      size_t list_no,
      size_t offset,
      size_t n_entry,
      const int64_t *ids_in,
      const uint8_t *codes_in)
  {
    assert(list_no < nlist);
    assert(n_entry + offset <= ids[list_no].size());
    memcpy(&ids[list_no][offset], ids_in, sizeof(ids_in[0]) * n_entry);
    memcpy(&codes[list_no][offset * code_size], codes_in, code_size * n_entry);
  }

  void ArrayInvertedLists::permute_invlists(const int64_t *map)
  {
    std::vector<std::vector<uint8_t>> new_codes(nlist);
    std::vector<std::vector<int64_t>> new_ids(nlist);

    for (size_t i = 0; i < nlist; i++)
    {
      size_t o = map[i];
      VINDEX_THROW_IF_NOT(o < nlist);
      std::swap(new_codes[i], codes[o]);
      std::swap(new_ids[i], ids[o]);
    }
    std::swap(codes, new_codes);
    std::swap(ids, new_ids);
  }

  ArrayInvertedLists::~ArrayInvertedLists() {}
} // namespace vindex
