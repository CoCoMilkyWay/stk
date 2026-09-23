// =============================================================================
// GpuRun 的空实现 (未开 FACTOR_CUDA 时链接这份, op_check 退化成 cpu / stream 两方对拍)
// =============================================================================

#include "factor/GpuRun.hpp"

#include <cassert>

namespace factor::gpu {

bool available() { return false; }

const char *device_name() { return nullptr; }

void run_ts(const char *, const float *, const uint8_t *, const float *, const uint8_t *, const float *,
            const uint8_t *, float *, uint8_t *, int, int, const Param &, double *) {
  assert(false && "GPU 后端未编译 (cmake -DFACTOR_CUDA=ON)");
}
void run_cs(const char *, const float *, const uint8_t *, const float *, const uint8_t *, const float *,
            const uint8_t *, float *, uint8_t *, int, int, const Param &, double *) {
  assert(false && "GPU 后端未编译 (cmake -DFACTOR_CUDA=ON)");
}
void run_stat(const float *, const uint8_t *, int, int, const factor::stat::Holds &, const StatLabelHost *,
              factor::stat::Row *, double *, double *) {
  assert(false && "GPU 后端未编译 (cmake -DFACTOR_CUDA=ON)");
}

} // namespace factor::gpu
