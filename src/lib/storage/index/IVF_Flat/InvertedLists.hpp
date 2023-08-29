#ifndef VINDEX_INVERTED_LISTS_HPP
#define VINDEX_INVERTED_LISTS_HPP
#include <vector>

#include "MetricType.hpp"
namespace vindex
{
  struct InvertedListsIterator
  {
    virtual ~InvertedListsIterator();
    virtual bool is_available() const = 0;
    virtual void next() = 0;
    virtual std::pair<int64_t, const uint8_t *> get_id_and_codes() = 0;
  };
  
  struct InvertedLists
  {
  public:
    size_t nlist;     ///< number of possible key values
    size_t code_size; ///< code size per vector in bytes
    bool use_iterator;

    InvertedLists(size_t nlist, size_t code_size);
    virtual ~InvertedLists();

    static const size_t INVALID_CODE_SIZE = static_cast<size_t>(-1);
    /*************************
     *  Read only functions */
    bool is_empty(size_t list_no) const;
    virtual size_t list_size(size_t list_no) const = 0;
    virtual InvertedListsIterator *get_iterator(size_t list_no) const;
    virtual const uint8_t *get_codes(size_t list_no) const = 0;
    virtual const int64_t *get_ids(size_t list_no) const = 0;
    virtual void release_codes(size_t list_no, const uint8_t *codes) const;
    virtual void release_ids(size_t list_no, const int64_t *ids) const;
    virtual int64_t get_single_id(size_t list_no, size_t offset) const;
    virtual const uint8_t *get_single_code(size_t list_no, size_t offset) const;
    virtual void prefetch_lists(const int64_t *list_nos, int nlist) const;

    /*************************
     * writing functions     */
    virtual size_t add_entry(size_t list_no, int64_t theid, const uint8_t *code);
    virtual size_t add_entries(size_t list_no, size_t n_entry, const int64_t *ids, const uint8_t *code) = 0;
    virtual void update_entry(size_t list_no, size_t offset, int64_t id, const uint8_t *code);
    virtual void update_entries(size_t list_no, size_t offset, size_t n_entry, const int64_t *ids, const uint8_t *code) = 0;
    virtual void resize(size_t list_no, size_t new_size) = 0;
    virtual void reset();

    /*************************
     * high level functions  */
    void merge_from(InvertedLists *oivf, size_t add_id);
    enum subset_type_t : int
    {
      SUBSET_TYPE_ID_RANGE = 0,
      SUBSET_TYPE_ID_MOD = 1,
      SUBSET_TYPE_ELEMENT_RANGE = 2,
      SUBSET_TYPE_INVLIST_FRACTION = 3,
      SUBSET_TYPE_INVLIST = 4
    };
    size_t copy_subset_to(InvertedLists &other, subset_type_t subset_type, int64_t a1, int64_t a2) const;

    /*************************
     * statistics            */
    double imbalance_factor() const;
    void print_stats() const;
    size_t compute_ntotal() const;

    /**************************************
     * Scoped inverted lists (for automatic deallocation)
     *
     * instead of writing:
     *
     *     uint8_t * codes = invlists->get_codes (10);
     *     ... use codes
     *     invlists->release_codes(10, codes)
     *
     * write:
     *
     *    ScopedCodes codes (invlists, 10);
     *    ... use codes.get()
     *    // release called automatically when codes goes out of scope
     *
     * the following function call also works:
     *
     *    foo (123, ScopedCodes (invlists, 10).get(), 456);
     *
     */

    struct ScopedIds
    {
      const InvertedLists *il;
      const int64_t *ids;
      size_t list_no;

      ScopedIds(const InvertedLists *il, size_t list_no)
          : il(il), ids(il->get_ids(list_no)), list_no(list_no) {}

      const int64_t *get()
      {
        return ids;
      }

      int64_t operator[](size_t i) const
      {
        return ids[i];
      }

      ~ScopedIds()
      {
        il->release_ids(list_no, ids);
      }
    };

    struct ScopedCodes
    {
      const InvertedLists *il;
      const uint8_t *codes;
      size_t list_no;

      ScopedCodes(const InvertedLists *il, size_t list_no)
          : il(il), codes(il->get_codes(list_no)), list_no(list_no) {}

      ScopedCodes(const InvertedLists *il, size_t list_no, size_t offset)
          : il(il),
            codes(il->get_single_code(list_no, offset)),
            list_no(list_no) {}

      const uint8_t *get()
      {
        return codes;
      }

      ~ScopedCodes()
      {
        il->release_codes(list_no, codes);
      }
    };
  };
  struct ArrayInvertedLists : public InvertedLists
  {
  public:
    std::vector<std::vector<uint8_t>> codes; // binary codes, size nlist
    std::vector<std::vector<int64_t>> ids;   ///< Inverted lists for indexes

    ArrayInvertedLists(size_t nlist, size_t code_size);
    ~ArrayInvertedLists() override;
    size_t list_size(size_t list_no) const override;
    const uint8_t *get_codes(size_t list_no) const override;
    const int64_t *get_ids(size_t list_no) const override;
    size_t add_entries(size_t list_no, size_t n_entry, const int64_t *ids, const uint8_t *code) override;
    void update_entries(size_t list_no, size_t offset, size_t n_entry, const int64_t *ids, const uint8_t *code) override;
    void resize(size_t list_no, size_t new_size) override;
    void permute_invlists(const int64_t *map);
  };
} // namespace vindex

#endif