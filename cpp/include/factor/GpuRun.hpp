#pragma once

// =============================================================================
// GPU 后端的对拍入口 (把 CUDA 编译单元与 op_check 的宿主代码隔开)
// =============================================================================
//   op_check 用纯 C++ 编译, Gpu.cuh 用 clang -x cuda 编译; 两边只通过这个声明相见.
//   这里收的是**宿主指针**: 拷入拷出由 .cu 侧包办 —— 对拍要的是正确性, 不是吞吐.
//   未开 CUDA 时链接 op_check_gpu_stub.cpp, available() = false, op_check 退化成两方对拍.
// =============================================================================

#include "factor/Contract.hpp"

#include <cstdint>

namespace factor::gpu {

bool available();

// 0 号设备型号 (UI 显示用, 首次调用探测后缓存); 未编译 CUDA 或无设备 → nullptr
const char *device_name();

// name 必须在 OpTable 里; 不在则断言死 (三后端同名是硬约束)
// kernel_ms 非空 → 回填纯 kernel 耗时 (cudaEvent), 不含 cudaMalloc / H2D / D2H (搬运是对拍接口的成本, 不是算子的)
void run_ts(const char *name, const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym,
            const float *zv, const uint8_t *zm, float *ov, uint8_t *om, int T, int A, const Param &p,
            double *kernel_ms = nullptr);
void run_cs(const char *name, const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym,
            const float *zv, const uint8_t *zm, float *ov, uint8_t *om, int T, int A, const Param &p,
            double *kernel_ms = nullptr);

} // namespace factor::gpu
