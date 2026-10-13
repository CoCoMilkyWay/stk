// =============================================================================
// GpuRun 的空实现 (未开 FACTOR_CUDA 时链接这份, op_check 退化成 cpu / stream 两方对拍)
// =============================================================================

#include "factor/GpuRun.hpp"

#include <cassert>

namespace factor::gpu {

bool available() { return false; }

const char *device_name() { return nullptr; }

void mem_info(size_t *, size_t *) { assert(false && "GPU 后端未编译 (cmake -DFACTOR_CUDA=ON)"); }
int search_max_batch() { return 0; }

Session *session_open(size_t) {
  assert(false && "GPU 后端未编译 (cmake -DFACTOR_CUDA=ON)");
  return nullptr;
}
void session_close(Session *) { assert(false && "GPU 后端未编译 (cmake -DFACTOR_CUDA=ON)"); }
DevPlane *upload(Session *, const float *, const uint8_t *) {
  assert(false && "GPU 后端未编译 (cmake -DFACTOR_CUDA=ON)");
  return nullptr;
}
DevPlane *upload_mask(Session *, const uint8_t *) {
  assert(false && "GPU 后端未编译 (cmake -DFACTOR_CUDA=ON)");
  return nullptr;
}
void pin(void *, size_t) { assert(false && "GPU 后端未编译 (cmake -DFACTOR_CUDA=ON)"); }
void unpin(void *) { assert(false && "GPU 后端未编译 (cmake -DFACTOR_CUDA=ON)"); }

void run_ts(Session *, const char *, const DevPlane *, const DevPlane *, const DevPlane *, float *, uint8_t *, int, int,
            const Param &, double *) {
  assert(false && "GPU 后端未编译 (cmake -DFACTOR_CUDA=ON)");
}
void run_cs(Session *, const char *, const DevPlane *, const DevPlane *, const DevPlane *, const DevPlane *, float *, uint8_t *, int,
            int, const Param &, double *) {
  assert(false && "GPU 后端未编译 (cmake -DFACTOR_CUDA=ON)");
}

void run_ts(const char *, const float *, const uint8_t *, const float *, const uint8_t *, const float *,
            const uint8_t *, float *, uint8_t *, int, int, const Param &, double *) {
  assert(false && "GPU 后端未编译 (cmake -DFACTOR_CUDA=ON)");
}
void run_cs(const char *, const float *, const uint8_t *, const float *, const uint8_t *, const float *,
            const uint8_t *, const uint8_t *, float *, uint8_t *, int, int, const Param &, double *) {
  assert(false && "GPU 后端未编译 (cmake -DFACTOR_CUDA=ON)");
}
void run_stat(const float *, const uint8_t *, const uint8_t *, factor::stat::Frame, int, int, const factor::stat::Holds &,
              const factor::stat::Label *, factor::stat::Row *, double *, double *) {
  assert(false && "GPU 后端未编译 (cmake -DFACTOR_CUDA=ON)");
}

DevPlane *plane_new(Session *) {
  assert(false && "GPU 后端未编译 (cmake -DFACTOR_CUDA=ON)");
  return nullptr;
}
void plane_del(Session *, DevPlane *) { assert(false && "GPU 后端未编译 (cmake -DFACTOR_CUDA=ON)"); }
void download(Session *, const DevPlane *, float *, uint8_t *) { assert(false && "GPU 后端未编译 (cmake -DFACTOR_CUDA=ON)"); }
void run_ts_dev(Session *, const char *, const DevPlane *, const DevPlane *, const DevPlane *, DevPlane *, int, int,
                const Param &, double *) {
  assert(false && "GPU 后端未编译 (cmake -DFACTOR_CUDA=ON)");
}
void run_cs_dev(Session *, const char *, const DevPlane *, const DevPlane *, const DevPlane *, const DevPlane *, DevPlane *, int,
                int, const Param &, double *) {
  assert(false && "GPU 后端未编译 (cmake -DFACTOR_CUDA=ON)");
}
StatSession *stat_open(int, int, const factor::stat::Holds &, const factor::stat::Label *, double *) {
  assert(false && "GPU 后端未编译 (cmake -DFACTOR_CUDA=ON)");
  return nullptr;
}
void stat_eval(StatSession *, const DevPlane *, const DevPlane *, factor::stat::Frame, factor::stat::Row *, double *, uint16_t *) {
  assert(false && "GPU 后端未编译 (cmake -DFACTOR_CUDA=ON)");
}
void stat_close(StatSession *) { assert(false && "GPU 后端未编译 (cmake -DFACTOR_CUDA=ON)"); }

SearchSession *search_open(int, int, int, const factor::stat::Holds &) {
  assert(false && "GPU 后端未编译 (cmake -DFACTOR_CUDA=ON)");
  return nullptr;
}
void search_load(SearchSession *, const factor::stat::Label *, double *) { assert(false && "GPU 后端未编译 (cmake -DFACTOR_CUDA=ON)"); }
void search_eval(SearchSession *, const DevPlane *const *, int, const DevPlane *, factor::stat::Frame, int, int, factor::search::Result *,
                 double *) {
  assert(false && "GPU 后端未编译 (cmake -DFACTOR_CUDA=ON)");
}
void search_close(SearchSession *) { assert(false && "GPU 后端未编译 (cmake -DFACTOR_CUDA=ON)"); }
void run_search(int, const float *const *, const uint8_t *const *, const uint8_t *, factor::stat::Frame, int, int, int, int,
                const factor::stat::Holds &, const factor::stat::Label *, factor::search::Result *, double *, double *) {
  assert(false && "GPU 后端未编译 (cmake -DFACTOR_CUDA=ON)");
}

} // namespace factor::gpu
