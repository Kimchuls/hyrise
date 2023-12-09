#ifndef VINDEX_INDEX_IVF_HPP
#define VINDEX_INDEX_IVF_HPP
#include <stdint.h>
#include <memory>
#include <unordered_map>
#include <vector>

#include "Clustering.hpp"
#include "DirectMap.hpp"
#include "Heap.hpp"
#include "IDSelector.hpp"
#include "Index.hpp"
#include "InvertedLists.hpp"
#include "platform_macros.hpp"

namespace vindex
{
  extern double time_stamp0,time_stamp1;
  void indexivf_time_clear();
  void indexivf_print_time();
  struct Level1Quantizer
  {
  public:
    Index *quantizer = nullptr;
    size_t nlist = 0;
    char quantizer_trains_alone = 0;
    bool own_fields = false;
    ClusteringParameters cp;
    Index *clustering_index = nullptr;

    void train_q1(size_t n, const float *x, bool verbose, MetricType metric_type);
    size_t coarse_code_size() const;
    void encode_listno(int64_t list_no, uint8_t *code) const;
    int64_t decode_listno(const uint8_t *code) const;

    Level1Quantizer(Index *quantizer, size_t nlist);
    Level1Quantizer();
    ~Level1Quantizer();
  };
  struct SearchParametersIVF : SearchParameters
  {
    size_t nprobe = 1;    ///< number of probes at query time
    size_t max_codes = 0; ///< max nb of codes to visit to do a query
    SearchParameters *quantizer_params = nullptr;

    virtual ~SearchParametersIVF() {}
  };

  // the new convention puts the index type after SearchParameters
  using IVFSearchParameters = SearchParametersIVF;
  struct InvertedListScanner;
  struct IndexIVFStats;
  struct CodePacker;
  struct IndexIVFInterface : public Level1Quantizer
  {
  public:
    size_t nprobe = 1;    ///< number of probes at query time
    size_t max_codes = 0; ///< max nb of codes to visit to do a query

    explicit IndexIVFInterface(Index *quantizer = nullptr, size_t nlist = 0)
        : Level1Quantizer(quantizer, nlist) {}

    virtual void search_preassigned(int64_t n, const float *x, int64_t k, const int64_t *assign, const float *centroid_dis, float *distances, int64_t *labels, bool store_pairs, const IVFSearchParameters *params = nullptr, IndexIVFStats *stats = nullptr) const = 0;
    virtual void range_search_preassigned(int64_t nx, const float *x, float radius, const int64_t *keys, const float *coarse_dis, RangeSearchResult *result, bool store_pairs = false, const IVFSearchParameters *params = nullptr, IndexIVFStats *stats = nullptr) const = 0;

    virtual ~IndexIVFInterface() {}
  };
  struct IndexIVF : public Index, public IndexIVFInterface
  {
  public:
    InvertedLists *invlists = nullptr;
    bool own_invlists = false;
    size_t code_size = 0;
    int parallel_mode = 0;
    const int PARALLEL_MODE_NO_HEAP_INIT = 1024;
    DirectMap direct_map;
    bool by_residual = true;
    IndexIVF(Index *quantizer, size_t d, size_t nlist, size_t code_size, MetricType metric = METRIC_L2);
    IndexIVF();
    ~IndexIVF() override;

    void reset() override;
    void train(int64_t n, const float *x) override;
    void add(int64_t n, const float *x) override;
    void add_with_ids(int64_t n, const float *x, const int64_t *xids) override;
    virtual void add_core(int64_t n, const float *x, const int64_t *xids, const int64_t *precomputed_idx);
    virtual void encode_vectors(int64_t n, const float *x, const int64_t *list_nos, uint8_t *codes, bool include_listno = false) const = 0;

    void add_sa_codes(int64_t n, const uint8_t *codes, const int64_t *xids);
    virtual void train_encoder(int64_t n, const float *x, const int64_t *assign);
    virtual int64_t train_encoder_num_vectors() const;
    void search_preassigned(int64_t n, const float *x, int64_t k, const int64_t *assign, const float *centroid_dis, float *distances, int64_t *labels, bool store_pairs, const IVFSearchParameters *params = nullptr, IndexIVFStats *stats = nullptr) const override;
    void range_search_preassigned(int64_t nx, const float *x, float radius, const int64_t *keys, const float *coarse_dis, RangeSearchResult *result, bool store_pairs = false, const IVFSearchParameters *params = nullptr, IndexIVFStats *stats = nullptr) const override;
    void search(int64_t n, const float *x, int64_t k, float *distances, int64_t *labels, const SearchParameters *params = nullptr) const override;
    void range_search(int64_t n, const float *x, float radius, RangeSearchResult *result, const SearchParameters *params = nullptr) const override;

    virtual InvertedListScanner *get_InvertedListScanner(bool store_pairs = false, const IDSelector *sel = nullptr) const;
    void reconstruct(int64_t key, float *recons) const override;
    virtual void update_vectors(int nv, const int64_t *idx, const float *v);
    void reconstruct_n(int64_t i0, int64_t ni, float *recons) const override;
    void search_and_reconstruct(int64_t n, const float *x, int64_t k, float *distances, int64_t *labels, float *recons, const SearchParameters *params = nullptr) const override;

    virtual void reconstruct_from_offset(int64_t list_no, int64_t offset, float *recons) const;
    size_t remove_ids(const IDSelector &sel) override;
    void check_compatible_for_merge(const Index &otherIndex) const override;
    virtual void merge_from(Index &otherIndex, int64_t add_id) override;
    virtual CodePacker *get_CodePacker() const;
    virtual void copy_subset_to(IndexIVF &other, InvertedLists::subset_type_t subset_type, int64_t a1, int64_t a2) const;

    size_t get_list_size(size_t list_no) const
    {
      return invlists->list_size(list_no);
    }
    bool check_ids_sorted() const;
    void make_direct_map(bool new_maintain_direct_map = true);
    void set_direct_map_type(DirectMap::Type type);
    void replace_invlists(InvertedLists *il, bool own = false);
    size_t sa_code_size() const override;
    void sa_encode(int64_t n, const float *x, uint8_t *bytes) const override;
  };

  struct RangeQueryResult;

  /** Object that handles a query. The inverted lists to scan are
   * provided externally. The object has a lot of state, but
   * distance_to_code and scan_codes can be called in multiple
   * threads */
  struct InvertedListScanner
  {
    int64_t list_no = -1;  ///< remember current list
    bool keep_max = false; ///< keep maximum instead of minimum
    /// store positions in invlists rather than labels
    bool store_pairs;

    /// search in this subset of ids
    const IDSelector *sel;

    InvertedListScanner(
        bool store_pairs = false,
        const IDSelector *sel = nullptr)
        : store_pairs(store_pairs), sel(sel) {}

    /// used in default implementation of scan_codes
    size_t code_size = 0;

    /// from now on we handle this query.
    virtual void set_query(const float *query_vector) = 0;

    /// following codes come from this inverted list
    virtual void set_list(int64_t list_no, float coarse_dis) = 0;

    /// compute a single query-to-code distance
    virtual float distance_to_code(const uint8_t *code) const = 0;

    /** scan a set of codes, compute distances to current query and
     * update heap of results if necessary. Default implemetation
     * calls distance_to_code.
     *
     * @param n      number of codes to scan
     * @param codes  codes to scan (n * code_size)
     * @param ids        corresponding ids (ignored if store_pairs)
     * @param distances  heap distances (size k)
     * @param labels     heap labels (size k)
     * @param k          heap size
     * @return number of heap updates performed
     */
    virtual size_t scan_codes(
        size_t n,
        const uint8_t *codes,
        const int64_t *ids,
        float *distances,
        int64_t *labels,
        size_t k) const;

    // same as scan_codes, using an iterator
    virtual size_t iterate_codes(
        InvertedListsIterator *iterator,
        float *distances,
        int64_t *labels,
        size_t k,
        size_t &list_size) const;

    /** scan a set of codes, compute distances to current query and
     * update results if distances are below radius
     *
     * (default implementation fails) */
    virtual void scan_codes_range(
        size_t n,
        const uint8_t *codes,
        const int64_t *ids,
        float radius,
        RangeQueryResult &result) const;

    // same as scan_codes_range, using an iterator
    virtual void iterate_codes_range(
        InvertedListsIterator *iterator,
        float radius,
        RangeQueryResult &result,
        size_t &list_size) const;

    virtual ~InvertedListScanner() {}
  };

  struct IndexIVFStats
  {
    size_t nq;                // nb of queries run
    size_t nlist;             // nb of inverted lists scanned
    size_t ndis;              // nb of distances computed
    size_t nheap_updates;     // nb of times the heap was updated
    double quantization_time; // time spent quantizing vectors (in ms)
    double search_time;       // time spent searching lists (in ms)

    IndexIVFStats()
    {
      reset();
    }
    void reset();
    void add(const IndexIVFStats &other);
  };
  extern IndexIVFStats indexIVF_stats;
} // namespace vindex

#endif