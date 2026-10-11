#pragma once

#if defined(__i386__) || defined(__x86_64__)
#include <immintrin.h>
inline void cpu_relax() { _mm_pause(); }
#else
inline void cpu_relax() {}
#endif
