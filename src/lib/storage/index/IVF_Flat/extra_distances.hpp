#pragma once
#include <stdint.h>

#include "Heap.hpp"
#include "MetricType.hpp"
#include "extra_distances-inl.hpp"

namespace vindex
{
  template <class C>
void knn_extra_metrics(
        const float* x,
        const float* y,
        size_t d,
        size_t nx,
        size_t ny,
        MetricType mt,
        float metric_arg,
        HeapArray<C>* res);
} // namespace vindex
