#include <stdint.h>
#include <stdio.h>

#include "platform_macros.hpp"

namespace vindex
{
  /** partitions the table into 0:q and q:n where all elements above q are >= all
 * elements below q (for C = CMax, for CMin comparisons are reversed)
 *
 * Returns the partition threshold. The elements q:n are destroyed on output.
 */
template <class C>
typename C::T partition_fuzzy(
        typename C::T* vals,
        typename C::TI* ids,
        size_t n,
        size_t q_min,
        size_t q_max,
        size_t* q_out);

/** simplified interface for when the parition is not fuzzy */
// template <class C>
// inline typename C::T partition(
//         typename C::T* vals,
//         typename C::TI* ids,
//         size_t n,
//         size_t q) {
//     return partition_fuzzy<C>(vals, ids, n, q, q, nullptr);
// }

/** low level SIMD histogramming functions */

/** 8-bin histogram of (x - min) >> shift
 * values outside the range are ignored.
 * the data table should be aligned on 32 bytes */
// void simd_histogram_8(
//         const uint16_t* data,
//         int n,
//         uint16_t min,
//         int shift,
//         int* hist);

/** same for 16-bin histogram */
// void simd_histogram_16(
//         const uint16_t* data,
//         int n,
//         uint16_t min,
//         int shift,
//         int* hist);

// struct PartitionStats {
//     uint64_t bissect_cycles;
//     uint64_t compress_cycles;

//     PartitionStats() {
//         reset();
//     }
//     void reset();
// };
} // namespace vindex
