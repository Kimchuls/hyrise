/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "IndexFlatCodes.hpp"

#include "AuxIndexStructures.hpp"
#include "CodePacker.hpp"
#include "DistanceComputer.hpp"
#include "VIndexAssert.hpp"
#include "IDSelector.hpp"

namespace vindex {

IndexFlatCodes::IndexFlatCodes(size_t code_size, int64_t d, MetricType metric)
        : Index(d, metric), code_size(code_size) {}

IndexFlatCodes::IndexFlatCodes() : code_size(0) {}

void IndexFlatCodes::add(int64_t n, const float* x) {
    VINDEX_THROW_IF_NOT(is_trained);
    if (n == 0) {
        return;
    }
    codes.resize((ntotal + n) * code_size);
    sa_encode(n, x, codes.data() + (ntotal * code_size));
    ntotal += n;
}

void IndexFlatCodes::reset() {
    codes.clear();
    ntotal = 0;
}

size_t IndexFlatCodes::sa_code_size() const {
    return code_size;
}

size_t IndexFlatCodes::remove_ids(const IDSelector& sel) {
    int64_t j = 0;
    for (int64_t i = 0; i < ntotal; i++) {
        if (sel.is_member(i)) {
            // should be removed
        } else {
            if (i > j) {
                memmove(&codes[code_size * j],
                        &codes[code_size * i],
                        code_size);
            }
            j++;
        }
    }
    size_t nremove = ntotal - j;
    if (nremove > 0) {
        ntotal = j;
        codes.resize(ntotal * code_size);
    }
    return nremove;
}

void IndexFlatCodes::reconstruct_n(int64_t i0, int64_t ni, float* recons) const {
    VINDEX_THROW_IF_NOT(ni == 0 || (i0 >= 0 && i0 + ni <= ntotal));
    sa_decode(ni, codes.data() + i0 * code_size, recons);
}

void IndexFlatCodes::reconstruct(int64_t key, float* recons) const {
    reconstruct_n(key, 1, recons);
}

FlatCodesDistanceComputer* IndexFlatCodes::get_FlatCodesDistanceComputer()
        const {
    VINDEX_THROW_MSG("not implemented");
}

void IndexFlatCodes::check_compatible_for_merge(const Index& otherIndex) const {
    // minimal sanity checks
    const IndexFlatCodes* other =
            dynamic_cast<const IndexFlatCodes*>(&otherIndex);
    VINDEX_THROW_IF_NOT(other);
    VINDEX_THROW_IF_NOT(other->d == d);
    VINDEX_THROW_IF_NOT(other->code_size == code_size);
    VINDEX_THROW_IF_NOT_MSG(
            typeid(*this) == typeid(*other),
            "can only merge indexes of the same type");
}

void IndexFlatCodes::merge_from(Index& otherIndex, int64_t add_id) {
    VINDEX_THROW_IF_NOT_MSG(add_id == 0, "cannot set ids in FlatCodes index");
    check_compatible_for_merge(otherIndex);
    IndexFlatCodes* other = static_cast<IndexFlatCodes*>(&otherIndex);
    codes.resize((ntotal + other->ntotal) * code_size);
    memcpy(codes.data() + (ntotal * code_size),
           other->codes.data(),
           other->ntotal * code_size);
    ntotal += other->ntotal;
    other->reset();
}

CodePacker* IndexFlatCodes::get_CodePacker() const {
    return new CodePackerFlat(code_size);
}

void IndexFlatCodes::permute_entries(const int64_t* perm) {
    std::vector<uint8_t> new_codes(codes.size());

    for (int64_t i = 0; i < ntotal; i++) {
        memcpy(new_codes.data() + i * code_size,
               codes.data() + perm[i] * code_size,
               code_size);
    }
    std::swap(codes, new_codes);
}

} // namespace vindex
