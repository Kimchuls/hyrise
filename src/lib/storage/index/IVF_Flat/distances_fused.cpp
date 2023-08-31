/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "distances_fused.hpp"

#include "platform_macros.hpp"

#include "avx512.hpp"
#include "simdlib_based.hpp"

namespace vindex {

bool exhaustive_L2sqr_fused_cmax(
        const float* x,
        const float* y,
        size_t d,
        size_t nx,
        size_t ny,
        SingleBestResultHandler<CMax<float, int64_t>>& res,
        const float* y_norms) {
    if (nx == 0 || ny == 0) {
        // nothing to do
        return true;
    }

#ifdef __AVX512__
    // avx512 kernel
    return exhaustive_L2sqr_fused_cmax_AVX512(x, y, d, nx, ny, res, y_norms);
#elif defined(__AVX2__) || defined(__aarch64__)
    // avx2 or arm neon kernel
    return exhaustive_L2sqr_fused_cmax_simdlib(x, y, d, nx, ny, res, y_norms);
#else
    // not supported, please use a general-purpose kernel
    return false;
#endif
}

} // namespace vindex
