// =============================================================================
// GpuRun 的 CUDA 实现: 收宿主指针 → 拷进显存 → 跑 Gpu.cuh 的算子 → 拷回
// =============================================================================
//   只服务 op_check 的正确性对拍, 不是挖掘的性能路径 (挖掘侧数据常驻显存, 不走这里).
//   分派表由 OpTable.hpp 展开: 表里有名字而 Gpu.cuh 无同名 struct → 此处编译错.
// =============================================================================

#include "factor/CS/Gpu.cuh" // IWYU pragma: keep
#include "factor/TS/Gpu.cuh" // IWYU pragma: keep

#include "factor/GpuRun.hpp"
#include "factor/OpTable.hpp"

#include <cassert>
#include <cstdio>
#include <cstring>

#define CU(call)                                         \
  do {                                                   \
    const cudaError_t e_ = (call);                       \
    assert(e_ == cudaSuccess && cudaGetErrorString(e_)); \
    (void)e_;                                            \
  } while (0)

namespace factor::gpu {

namespace {

// 一对 (值, 掩码) 平面的显存搬运; 宿主指针为空 → 设备指针也为空
struct DevPlane {
  float *v = nullptr;
  uint8_t *m = nullptr;
  void up(const float *hv, const uint8_t *hm, size_t n) {
    if (!hv)
      return;
    CU(cudaMalloc(&v, n * sizeof(float)));
    CU(cudaMalloc(&m, n));
    CU(cudaMemcpy(v, hv, n * sizeof(float), cudaMemcpyHostToDevice));
    CU(cudaMemcpy(m, hm, n, cudaMemcpyHostToDevice));
  }
  void alloc(size_t n) {
    CU(cudaMalloc(&v, n * sizeof(float)));
    CU(cudaMalloc(&m, n));
    CU(cudaMemset(v, 0, n * sizeof(float)));
    CU(cudaMemset(m, 0, n));
  }
  void down(float *hv, uint8_t *hm, size_t n) const {
    CU(cudaMemcpy(hv, v, n * sizeof(float), cudaMemcpyDeviceToHost));
    CU(cudaMemcpy(hm, m, n, cudaMemcpyDeviceToHost));
  }
  void free_() {
    if (v)
      CU(cudaFree(v));
    if (m)
      CU(cudaFree(m));
  }
};

template <class Op>
void call(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, const float *zv,
          const uint8_t *zm, float *ov, uint8_t *om, int T, int A, const Param &p, double *kernel_ms) {
  const size_t n = static_cast<size_t>(T) * A;
  DevPlane x, y, z, o;
  x.up(xv, xm, n), y.up(yv, ym, n), z.up(zv, zm, n), o.alloc(n);

  void *ws = nullptr;
  const size_t wsn = Op::workspace(T, A, p);
  if (wsn)
    CU(cudaMalloc(&ws, wsn));

  // 纯 kernel 计时 (cudaEvent 夹 Op::run): malloc / 搬运是本接口的成本, 不算进算子
  cudaEvent_t e0, e1;
  CU(cudaEventCreate(&e0));
  CU(cudaEventCreate(&e1));
  CU(cudaEventRecord(e0));
  Op::run(x.v, x.m, y.v, y.m, z.v, z.m, o.v, o.m, T, A, p, ws, nullptr);
  CU(cudaEventRecord(e1));
  CU(cudaEventSynchronize(e1));
  CU(cudaGetLastError());
  if (kernel_ms) {
    float f = 0.f;
    CU(cudaEventElapsedTime(&f, e0, e1));
    *kernel_ms = f;
  }
  CU(cudaEventDestroy(e0));
  CU(cudaEventDestroy(e1));

  o.down(ov, om, n);
  if (ws)
    CU(cudaFree(ws));
  x.free_(), y.free_(), z.free_(), o.free_();
}

} // namespace

bool available() {
  int n = 0;
  return cudaGetDeviceCount(&n) == cudaSuccess && n > 0;
}

const char *device_name() {
  static char buf[256] = {0};
  static bool probed = false;
  if (!probed) {
    probed = true;
    int n = 0;
    if (cudaGetDeviceCount(&n) == cudaSuccess && n > 0) {
      cudaDeviceProp prop;
      CU(cudaGetDeviceProperties(&prop, 0));
      std::snprintf(buf, sizeof(buf), "%s", prop.name);
    }
  }
  return buf[0] ? buf : nullptr;
}

void run_ts(const char *name, const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym,
            const float *zv, const uint8_t *zm, float *ov, uint8_t *om, int T, int A, const Param &p,
            double *kernel_ms) {
#define G_TS(Name, ar, win, scope, kern, prm, tex, note) \
  if (std::strcmp(name, #Name) == 0)                     \
    return call<ts::Name>(xv, xm, yv, ym, zv, zm, ov, om, T, A, p, kernel_ms);
  OP_TS(G_TS)
#undef G_TS
  assert(false && "算子不在 OpTable 的 TS 组");
}

void run_cs(const char *name, const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym,
            const float *zv, const uint8_t *zm, float *ov, uint8_t *om, int T, int A, const Param &p,
            double *kernel_ms) {
#define G_CS(Name, ar, win, scope, kern, prm, tex, note) \
  if (std::strcmp(name, #Name) == 0)                     \
    return call<cs::Name>(xv, xm, yv, ym, zv, zm, ov, om, T, A, p, kernel_ms);
  OP_CS(G_CS)
#undef G_CS
  assert(false && "算子不在 OpTable 的 CS 组");
}

} // namespace factor::gpu
