#pragma once

// 主机 / 设备两用的函数标注: CUDA 编译器下 __host__ __device__, 纯 C++ 下为空 (factor 契约公式与 define/Label.hpp 的协议函数同用)
#ifndef FACTOR_HD
#if defined(__CUDACC__)
#define FACTOR_HD __host__ __device__
#else
#define FACTOR_HD
#endif
#endif
