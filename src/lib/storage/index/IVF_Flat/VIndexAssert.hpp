#ifndef VINDEX_ASSERT_HPP
#define VINDEX_ASSERT_HPP
#include <cstdio>
#include <cstring>
#include "VIndexException.hpp"
#define VINDEX_ASSERT(X)                                  \
    do                                                    \
    {                                                     \
        if (!(X))                                         \
        {                                                 \
            fprintf(stderr,                               \
                    "VINDEX assertion '%s' failed in %s " \
                    "at %s:%d\n",                         \
                    #X,                                   \
                    __PRETTY_FUNCTION__,                  \
                    __FILE__,                             \
                    __LINE__);                            \
            abort();                                      \
        }                                                 \
    } while (false)

#define VINDEX_ASSERT_MSG(X, MSG)                         \
    do                                                    \
    {                                                     \
        if (!(X))                                         \
        {                                                 \
            fprintf(stderr,                               \
                    "VINDEX assertion '%s' failed in %s " \
                    "at %s:%d; details: " MSG "\n",       \
                    #X,                                   \
                    __PRETTY_FUNCTION__,                  \
                    __FILE__,                             \
                    __LINE__);                            \
            abort();                                      \
        }                                                 \
    } while (false)

#define VINDEX_ASSERT_FMT(X, FMT, ...)                    \
    do                                                    \
    {                                                     \
        if (!(X))                                         \
        {                                                 \
            fprintf(stderr,                               \
                    "VINDEX assertion '%s' failed in %s " \
                    "at %s:%d; details: " FMT "\n",       \
                    #X,                                   \
                    __PRETTY_FUNCTION__,                  \
                    __FILE__,                             \
                    __LINE__,                             \
                    __VA_ARGS__);                         \
            abort();                                      \
        }                                                 \
    } while (false)

///
/// Exceptions for returning user errors
///

#define VINDEX_THROW_MSG(MSG)                              \
    do                                                     \
    {                                                      \
        throw vindex::VINDEXException(                     \
            MSG, __PRETTY_FUNCTION__, __FILE__, __LINE__); \
    } while (false)

#define VINDEX_THROW_FMT(FMT, ...)                           \
    do                                                       \
    {                                                        \
        std::string __s;                                     \
        int __size = snprintf(nullptr, 0, FMT, __VA_ARGS__); \
        __s.resize(__size + 1);                              \
        snprintf(&__s[0], __s.size(), FMT, __VA_ARGS__);     \
        throw vindex::VINDEXException(                       \
            __s, __PRETTY_FUNCTION__, __FILE__, __LINE__);   \
    } while (false)

///
/// Exceptions thrown upon a conditional failure
///

#define VINDEX_THROW_IF_NOT(X)                          \
    do                                                  \
    {                                                   \
        if (!(X))                                       \
        {                                               \
            VINDEX_THROW_FMT("Error: '%s' failed", #X); \
        }                                               \
    } while (false)

#define VINDEX_THROW_IF_NOT_MSG(X, MSG)                       \
    do                                                        \
    {                                                         \
        if (!(X))                                             \
        {                                                     \
            VINDEX_THROW_FMT("Error: '%s' failed: " MSG, #X); \
        }                                                     \
    } while (false)

#define VINDEX_THROW_IF_NOT_FMT(X, FMT, ...)                               \
    do                                                                     \
    {                                                                      \
        if (!(X))                                                          \
        {                                                                  \
            VINDEX_THROW_FMT("Error: '%s' failed: " FMT, #X, __VA_ARGS__); \
        }                                                                  \
    } while (false)

#endif