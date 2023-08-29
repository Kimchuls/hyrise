#ifndef VINDEX_EXCEPTION_HPP
#define VINDEX_EXCEPTION_HPP
#include <exception>
#include <string>
#include <utility>
#include <vector>
namespace vindex
{
  struct VINDEXException : public std::exception {
   public:
    explicit VINDEXException(const std::string& msg);

    VINDEXException(
            const std::string& msg,
            const char* funcName,
            const char* file,
            int line);

    /// from std::exception
    const char* what() const noexcept override;

    std::string msg;
};

void handleExceptions(
        std::vector<std::pair<int, std::exception_ptr>>& exceptions);

/** bare-bones unique_ptr
 * this one deletes with delete [] */
template <class T>
struct ScopeDeleter {
    const T* ptr;
    explicit ScopeDeleter(const T* ptr = nullptr) : ptr(ptr) {}
    void release() {
        ptr = nullptr;
    }
    void set(const T* ptr_in) {
        ptr = ptr_in;
    }
    void swap(ScopeDeleter<T>& other) {
        std::swap(ptr, other.ptr);
    }
    ~ScopeDeleter() {
        delete[] ptr;
    }
};

/** same but deletes with the simple delete (least common case) */
template <class T>
struct ScopeDeleter1 {
    const T* ptr;
    explicit ScopeDeleter1(const T* ptr = nullptr) : ptr(ptr) {}
    void release() {
        ptr = nullptr;
    }
    void set(const T* ptr_in) {
        ptr = ptr_in;
    }
    void swap(ScopeDeleter1<T>& other) {
        std::swap(ptr, other.ptr);
    }
    ~ScopeDeleter1() {
        delete ptr;
    }
};

/** RAII object for a set of possibly transformed vectors (deallocated only if
 * they are indeed transformed)
 */
struct TransformedVectors {
    const float* x;
    bool own_x;
    TransformedVectors(const float* x_orig, const float* x) : x(x) {
        own_x = x_orig != x;
    }

    ~TransformedVectors() {
        if (own_x) {
            delete[] x;
        }
    }
};

/// make typeids more readable
std::string demangle_cpp_symbol(const char* name);
} // namespace vindex

#endif