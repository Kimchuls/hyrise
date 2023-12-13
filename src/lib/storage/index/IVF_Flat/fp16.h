#pragma once

#include <cstdint>

#include "platform_macros.h"

#if defined(__F16C__)
#include "fp16-fp16c.h"
#else
#include "fp16-inl.h"
#endif
