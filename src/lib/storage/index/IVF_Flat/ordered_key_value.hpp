#pragma once
#include <stdint.h>
#include <climits>
#include <cmath>

#include <limits>
namespace vindex
{
  template <typename T_, typename TI_>
  struct CMax;

  template <typename T>
  inline T cmin_nextafter(T x);
  template <typename T>
  inline T cmax_nextafter(T x);

  // traits of minheaps = heaps where the minimum value is stored on top
  // useful to find the *max* values of an array
  template <typename T_, typename TI_>
  struct CMin
  {
    typedef T_ T;
    typedef TI_ TI;
    typedef CMax<T_, TI_> Crev; // reference to reverse comparison
    inline static bool cmp(T a, T b)
    {
      return a < b;
    }
    // Similar to cmp(), but also breaks ties
    // by comparing the second pair of arguments.
    inline static bool cmp2(T a1, T b1, TI a2, TI b2)
    {
      return (a1 < b1) || ((a1 == b1) && (a2 < b2));
    }
    inline static T neutral()
    {
      return std::numeric_limits<T>::lowest();
    }
    static const bool is_max = false;

    inline static T nextafter(T x)
    {
      return cmin_nextafter(x);
    }
  };

  template <typename T_, typename TI_>
  struct CMax
  {
    typedef T_ T;
    typedef TI_ TI;
    typedef CMin<T_, TI_> Crev;
    inline static bool cmp(T a, T b)
    {
      return a > b;
    }
    // Similar to cmp(), but also breaks ties
    // by comparing the second pair of arguments.
    inline static bool cmp2(T a1, T b1, TI a2, TI b2)
    {
      return (a1 > b1) || ((a1 == b1) && (a2 > b2));
    }
    inline static T neutral()
    {
      return std::numeric_limits<T>::max();
    }
    static const bool is_max = true;
    inline static T nextafter(T x)
    {
      return cmax_nextafter(x);
    }
  };

  template <>
  inline float cmin_nextafter<float>(float x)
  {
    return std::nextafterf(x, -HUGE_VALF);
  }

  template <>
  inline float cmax_nextafter<float>(float x)
  {
    return std::nextafterf(x, HUGE_VALF);
  }

  template <>
  inline uint16_t cmin_nextafter<uint16_t>(uint16_t x)
  {
    return x - 1;
  }

  template <>
  inline uint16_t cmax_nextafter<uint16_t>(uint16_t x)
  {
    return x + 1;
  }
} // namespace vindex
