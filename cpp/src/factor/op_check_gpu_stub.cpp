// =============================================================================
// GpuRun 的空实现 (GPU_BACKEND=Stub 时链接这份, op_check 退化成 cpu / stream 两方对拍)
// =============================================================================

#include "factor/GpuRun.hpp"

#include <cassert>

namespace factor::gpu {

bool available() { return false; }

const char *device_name() { return nullptr; }

void mem_info(size_t *, size_t *) { assert(false && "GPU 后端未编译 (cmake -DGPU_BACKEND=CUDA)"); }
int search_max_batch() { return 0; }

Session *session_open(size_t) {
  assert(false && "GPU 后端未编译 (cmake -DGPU_BACKEND=CUDA)");
  return nullptr;
}
void session_close(Session *) { assert(false && "GPU 后端未编译 (cmake -DGPU_BACKEND=CUDA)"); }
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

void run(Session *, int, const DevPlane *, const DevPlane *, const DevPlane *, const DevPlane *, float *, uint8_t *, int, int, int,
         const Param &, double *) {
  assert(false && "GPU 后端未编译 (cmake -DGPU_BACKEND=CUDA)");
}
void run(int, const float *, const uint8_t *, const float *, const uint8_t *, const float *, const uint8_t *, const uint8_t *, float *,
         uint8_t *, int, int, int, const Param &, double *) {
  assert(false && "GPU 后端未编译 (cmake -DGPU_BACKEND=CUDA)");
}
void run_stat(factor::stat::Plane, const uint8_t *, factor::stat::Frame, int, int, int, const factor::stat::Labels &, factor::stat::Accum *,
              factor::stat::Row *, double *, double *) {
  assert(false && "GPU 后端未编译 (cmake -DGPU_BACKEND=CUDA)");
}

DevPlane *plane_new(Session *) {
  assert(false && "GPU 后端未编译 (cmake -DGPU_BACKEND=CUDA)");
  return nullptr;
}
void plane_del(Session *, DevPlane *) { assert(false && "GPU 后端未编译 (cmake -DGPU_BACKEND=CUDA)"); }
void download(Session *, const DevPlane *, float *, uint8_t *) { assert(false && "GPU 后端未编译 (cmake -DGPU_BACKEND=CUDA)"); }
void run_dev(Session *, int, const DevPlane *, const DevPlane *, const DevPlane *, const DevPlane *, DevPlane *, int, int, int,
             const Param &, double *) {
  assert(false && "GPU 后端未编译 (cmake -DGPU_BACKEND=CUDA)");
}
void run_fused(Session *, const Fuse &, const DevPlane *const *, int, DevPlane *, int, int, int, double *) {
  assert(false && "GPU 后端未编译 (cmake -DGPU_BACKEND=CUDA)");
}
StatSession *stat_open(int, int, int, const factor::stat::Labels &, double *) {
  assert(false && "GPU 后端未编译 (cmake -DGPU_BACKEND=CUDA)");
  return nullptr;
}
void stat_eval(StatSession *, const DevPlane *, const DevPlane *, factor::stat::Frame, factor::stat::Dir, int, int, int, factor::stat::Accum *,
               factor::stat::Row *, uint8_t *, uint16_t *, double *) {
  assert(false && "GPU 后端未编译 (cmake -DGPU_BACKEND=CUDA)");
}
void stat_close(StatSession *) { assert(false && "GPU 后端未编译 (cmake -DGPU_BACKEND=CUDA)"); }

SearchSession *search_open(int, int, int, int, const factor::stat::Holds &, double) {
  assert(false && "GPU 后端未编译 (cmake -DGPU_BACKEND=CUDA)");
  return nullptr;
}
void search_load(SearchSession *, const factor::stat::Label *, double *) { assert(false && "GPU 后端未编译 (cmake -DGPU_BACKEND=CUDA)"); }
void search_eval(SearchSession *, const DevPlane *const *, int, const DevPlane *, factor::stat::Frame, int, int, int, factor::search::Result *,
                 double *) {
  assert(false && "GPU 后端未编译 (cmake -DGPU_BACKEND=CUDA)");
}
void search_ranks(SearchSession *, int, int, int, int, uint16_t *) { assert(false && "GPU 后端未编译 (cmake -DGPU_BACKEND=CUDA)"); }
void search_close(SearchSession *) { assert(false && "GPU 后端未编译 (cmake -DGPU_BACKEND=CUDA)"); }
void run_search(int, const float *const *, const uint8_t *const *, const uint8_t *, factor::stat::Frame, int, int, int, int, int, int,
                const factor::stat::Holds &, double, const factor::stat::Label *, factor::search::Result *, double *, double *) {
  assert(false && "GPU 后端未编译 (cmake -DGPU_BACKEND=CUDA)");
}

} // namespace factor::gpu
