// =============================================================================
// GpuRun 的空实现 (GPU_BACKEND=Stub 时链接这份)
// =============================================================================

#include "factor/GpuRun.hpp"

#include <cassert>

namespace factor::gpu {

bool available() { return false; }
const char *device_name() { return nullptr; }
void mem_info(size_t *, size_t *) { assert(false && "GPU 后端未编译 (cmake -DGPU_BACKEND=CUDA)"); }
int stat_max_batch() { return 0; }

Session *session_open(size_t) {
  assert(false && "GPU 后端未编译 (cmake -DGPU_BACKEND=CUDA)");
  return nullptr;
}
void session_close(Session *) { assert(false && "GPU 后端未编译 (cmake -DGPU_BACKEND=CUDA)"); }
size_t session_bytes(const Session *) { return 0; }
DevPlane *upload(Session *, const float *, const uint8_t *) {
  assert(false && "GPU 后端未编译 (cmake -DGPU_BACKEND=CUDA)");
  return nullptr;
}
DevPlane *upload_mask(Session *, const uint8_t *) {
  assert(false && "GPU 后端未编译 (cmake -DGPU_BACKEND=CUDA)");
  return nullptr;
}
void pin(void *, size_t) { assert(false && "GPU 后端未编译 (cmake -DGPU_BACKEND=CUDA)"); }
void unpin(void *) { assert(false && "GPU 后端未编译 (cmake -DGPU_BACKEND=CUDA)"); }

void run(Session *, int, const DevPlane *const *, int, const DevPlane *, float *, uint8_t *, int, int, int, const Param &, double *) {
  assert(false && "GPU 后端未编译 (cmake -DGPU_BACKEND=CUDA)");
}
void run(int, int, const float *const *, const uint8_t *const *, const uint8_t *, float *, uint8_t *, int, int, int, const Param &, double *) {
  assert(false && "GPU 后端未编译 (cmake -DGPU_BACKEND=CUDA)");
}
DevPlane *plane_new(Session *) {
  assert(false && "GPU 后端未编译 (cmake -DGPU_BACKEND=CUDA)");
  return nullptr;
}
DevPlane *mask_new(Session *) {
  assert(false && "GPU 后端未编译 (cmake -DGPU_BACKEND=CUDA)");
  return nullptr;
}
void plane_del(Session *, DevPlane *) { assert(false && "GPU 后端未编译 (cmake -DGPU_BACKEND=CUDA)"); }
void upload_to(Session *, DevPlane *, const float *, const uint8_t *) { assert(false && "GPU 后端未编译 (cmake -DGPU_BACKEND=CUDA)"); }
void download(Session *, const DevPlane *, float *, uint8_t *) { assert(false && "GPU 后端未编译 (cmake -DGPU_BACKEND=CUDA)"); }
void run_dev(Session *, int, const DevPlane *const *, int, const DevPlane *, DevPlane *, int, int, int, const Param &, double *) {
  assert(false && "GPU 后端未编译 (cmake -DGPU_BACKEND=CUDA)");
}
void run_fused(Session *, const Fuse &, const DevPlane *const *, int, DevPlane *, int, int, int, double *) {
  assert(false && "GPU 后端未编译 (cmake -DGPU_BACKEND=CUDA)");
}

StatSession *stat_open(int, int, int, int, const Holds &, double) {
  assert(false && "GPU 后端未编译 (cmake -DGPU_BACKEND=CUDA)");
  return nullptr;
}
void stat_load(StatSession *, const factor::stat::Label *, double *) { assert(false && "GPU 后端未编译 (cmake -DGPU_BACKEND=CUDA)"); }
void stat_eval(StatSession *, const DevPlane *const *, int, const DevPlane *, factor::stat::Frame, factor::stat::Dir, int, int, int, factor::stat::Accum *,
               factor::stat::Day *, factor::stat::Row *, uint16_t *, double *) {
  assert(false && "GPU 后端未编译 (cmake -DGPU_BACKEND=CUDA)");
}
void stat_close(StatSession *) { assert(false && "GPU 后端未编译 (cmake -DGPU_BACKEND=CUDA)"); }
size_t stat_bytes(const StatSession *) { return 0; }
void run_stat(int, const float *const *, const uint8_t *const *, const uint8_t *, factor::stat::Frame, factor::stat::Dir, int, int, int, int, int, int,
              const Holds &, double, const factor::stat::Label *, factor::stat::Accum *, factor::stat::Day *, factor::stat::Row *, double *,
              double *) {
  assert(false && "GPU 后端未编译 (cmake -DGPU_BACKEND=CUDA)");
}

} // namespace factor::gpu
