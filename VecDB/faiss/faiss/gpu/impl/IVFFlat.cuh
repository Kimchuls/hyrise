/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <faiss/gpu/impl/GpuScalarQuantizer.cuh>
#include <faiss/gpu/impl/IVFBase.cuh>

namespace faiss {
namespace gpu {

class IVFFlat : public IVFBase {
   public:
    IVFFlat(GpuResources* resources,
            int dim,
            int nlist,
            faiss::MetricType metric,
            float metricArg,
            bool useResidual,
            /// Optional ScalarQuantizer
            faiss::ScalarQuantizer* scalarQ,
            bool interleavedLayout,
            IndicesOptions indicesOptions,
            MemorySpace space);

    ~IVFFlat() override;

    /// Find the approximate k nearest neigbors for `queries` against
    /// our database
    void search(
            Index* coarseQuantizer,
            Tensor<float, 2, true>& queries,
            int nprobe,
            int k,
            Tensor<float, 2, true>& outDistances,
            Tensor<Index::idx_t, 2, true>& outIndices) override;

    /// Performs search when we are already given the IVF cells to look at
    /// (GpuIndexIVF::search_preassigned implementation)
    void searchPreassigned(
            Index* coarseQuantizer,
            Tensor<float, 2, true>& vecs,
            Tensor<float, 2, true>& ivfDistances,
            Tensor<Index::idx_t, 2, true>& ivfAssignments,
            int k,
            Tensor<float, 2, true>& outDistances,
            Tensor<Index::idx_t, 2, true>& outIndices,
            bool storePairs) override;

   protected:
    /// Returns the number of bytes in which an IVF list containing numVecs
    /// vectors is encoded on the device. Note that due to padding this is not
    /// the same as the encoding size for a subset of vectors in an IVF list;
    /// this is the size for an entire IVF list
    size_t getGpuVectorsEncodingSize_(int numVecs) const override;
    size_t getCpuVectorsEncodingSize_(int numVecs) const override;

    /// Translate to our preferred GPU encoding
    std::vector<uint8_t> translateCodesToGpu_(
            std::vector<uint8_t> codes,
            size_t numVecs) const override;

    /// Translate from our preferred GPU encoding
    std::vector<uint8_t> translateCodesFromGpu_(
            std::vector<uint8_t> codes,
            size_t numVecs) const override;

    /// Encode the vectors that we're adding and append to our IVF lists
    void appendVectors_(
            Tensor<float, 2, true>& vecs,
            Tensor<float, 2, true>& ivfCentroidResiduals,
            Tensor<Index::idx_t, 1, true>& indices,
            Tensor<Index::idx_t, 1, true>& uniqueLists,
            Tensor<int, 1, true>& vectorsByUniqueList,
            Tensor<int, 1, true>& uniqueListVectorStart,
            Tensor<int, 1, true>& uniqueListStartOffset,
            Tensor<Index::idx_t, 1, true>& listIds,
            Tensor<int, 1, true>& listOffset,
            cudaStream_t stream) override;

    /// Shared IVF search implementation, used by both search and
    /// searchPreassigned
    void searchImpl_(
            Tensor<float, 2, true>& queries,
            Tensor<float, 2, true>& coarseDistances,
            Tensor<Index::idx_t, 2, true>& coarseIndices,
            Tensor<float, 3, true>& ivfCentroids,
            int k,
            Tensor<float, 2, true>& outDistances,
            Tensor<Index::idx_t, 2, true>& outIndices,
            bool storePairs);

   protected:
    /// Scalar quantizer for encoded vectors, if any
    std::unique_ptr<GpuScalarQuantizer> scalarQ_;
};

} // namespace gpu
} // namespace faiss
