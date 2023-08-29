#include "IndexFlatCodes.hpp"

#include "VIndexAssert.hpp"
namespace vindex
{
  IndexFlatCodes::IndexFlatCodes(size_t code_size, int64_t d, MetricType metric)
      : Index(d, metric), code_size(code_size) {}

  IndexFlatCodes::IndexFlatCodes() : code_size(0) {}

  void IndexFlatCodes::reset()
  {
    codes.clear();
    ntotal = 0;
  }
  void IndexFlatCodes::add(int64_t n, const float *x)
  {
    VINDEX_THROW_IF_NOT(is_trained);
    if (n == 0)
    {
      return;
    }
    codes.resize((ntotal + n) * code_size);
    sa_encode(n, x, codes.data() + (ntotal * code_size));
    ntotal += n;
  }
} // namespace vindex
