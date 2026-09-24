// =============================================================================
// GpuRun 的 CUDA 实现: 输入拷进显存 (会话内一次) → 跑 Gpu.cuh 的算子 → 输出拷回
// =============================================================================
//   只服务对拍的正确性 + 纯 kernel 计时, 不是挖掘的性能路径 (挖掘侧数据常驻显存, 不走这里).
//   分派表由 OpTable.hpp 展开: 表里有名字而 Gpu.cuh 无同名 struct → 此处编译错.
// =============================================================================

#include "factor/CS/Gpu.cuh" // IWYU pragma: keep
#include "factor/TS/Gpu.cuh" // IWYU pragma: keep

#include "factor/Stat/Gpu.cuh" // IWYU pragma: keep

#include "factor/GpuRun.hpp"
#include "factor/OpTable.hpp"

#include <cassert>
#include <cstdio>
#include <cstring>
#include <vector>

#define CU(call)                                         \
  do {                                                   \
    const cudaError_t e_ = (call);                       \
    assert(e_ == cudaSuccess && cudaGetErrorString(e_)); \
    (void)e_;                                            \
  } while (0)

namespace factor::gpu {

// 显存里一对 (值, 掩码) 平面 (GpuRun.hpp 只前置声明)
struct DevPlane {
  float *v = nullptr;
  uint8_t *m = nullptr;
};

// 常驻会话: 输入平面 (归会话) / 输出平面 / 按需长大的工作区 / 计时事件. n 开会话时定死
struct Session {
  size_t n = 0;
  std::vector<DevPlane *> in; // upload 出去的, close 时统一释放
  DevPlane o;
  void *ws = nullptr;
  size_t ws_cap = 0;
  cudaEvent_t e0 = nullptr, e1 = nullptr;
};

namespace {

void plane_alloc(DevPlane &p, size_t n) {
  CU(cudaMalloc(&p.v, n * sizeof(float)));
  CU(cudaMalloc(&p.m, n));
}
void plane_free(DevPlane &p) {
  CU(cudaFree(p.v));
  CU(cudaFree(p.m));
}

// 会话内跑一个算子, 输出留在设备平面 out: 工作区不够才重分配; cudaEvent 夹 Op::run 计纯 kernel
template <class Op>
void call_dev(Session *s, const DevPlane *x, const DevPlane *y, const DevPlane *z, DevPlane *out, int T, int A,
              const Param &p, double *kernel_ms) {
  assert(s && s->n == static_cast<size_t>(T) * A && "会话形状与本次调用不符");
  assert(out && out->v && out->m);
  assert(out != x && out != y && out != z && "算子不支持原地");
  const size_t wsn = Op::workspace(T, A, p);
  if (wsn > s->ws_cap) {
    if (s->ws)
      CU(cudaFree(s->ws));
    CU(cudaMalloc(&s->ws, wsn));
    s->ws_cap = wsn;
  }
  const float *xv = x ? x->v : nullptr, *yv = y ? y->v : nullptr, *zv = z ? z->v : nullptr;
  const uint8_t *xm = x ? x->m : nullptr, *ym = y ? y->m : nullptr, *zm = z ? z->m : nullptr;
  CU(cudaEventRecord(s->e0));
  Op::run(xv, xm, yv, ym, zv, zm, out->v, out->m, T, A, p, wsn ? s->ws : nullptr, nullptr);
  CU(cudaEventRecord(s->e1));
  CU(cudaEventSynchronize(s->e1));
  CU(cudaGetLastError());
  if (kernel_ms) {
    float f = 0.f;
    CU(cudaEventElapsedTime(&f, s->e0, s->e1));
    *kernel_ms = f;
  }
}

// 会话版: 写会话输出平面后拷回宿主 (宿主 pin 过则走 DMA)
template <class Op>
void call(Session *s, const DevPlane *x, const DevPlane *y, const DevPlane *z, float *ov, uint8_t *om, int T, int A,
          const Param &p, double *kernel_ms) {
  call_dev<Op>(s, x, y, z, &s->o, T, A, p, kernel_ms);
  CU(cudaMemcpy(ov, s->o.v, s->n * sizeof(float), cudaMemcpyDeviceToHost));
  CU(cudaMemcpy(om, s->o.m, s->n, cudaMemcpyDeviceToHost));
}

// 一次性版本的公共体: 临时会话, 上传 → 跑 → 关
template <class Op>
void call_once(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, const float *zv, const uint8_t *zm,
               float *ov, uint8_t *om, int T, int A, const Param &p, double *kernel_ms) {
  Session *s = session_open(static_cast<size_t>(T) * A);
  const DevPlane *x = xv ? upload(s, xv, xm) : nullptr;
  const DevPlane *y = yv ? upload(s, yv, ym) : nullptr;
  const DevPlane *z = zv ? upload(s, zv, zm) : nullptr;
  call<Op>(s, x, y, z, ov, om, T, A, p, kernel_ms);
  session_close(s);
}

} // namespace

Session *session_open(size_t n) {
  assert(n > 0);
  Session *s = new Session;
  s->n = n;
  plane_alloc(s->o, n); // 输出不清零: 契约是后端写满每格
  CU(cudaEventCreate(&s->e0));
  CU(cudaEventCreate(&s->e1));
  return s;
}

void session_close(Session *s) {
  assert(s);
  for (DevPlane *p : s->in) {
    plane_free(*p);
    delete p;
  }
  plane_free(s->o);
  if (s->ws)
    CU(cudaFree(s->ws));
  CU(cudaEventDestroy(s->e0));
  CU(cudaEventDestroy(s->e1));
  delete s;
}

DevPlane *upload(Session *s, const float *v, const uint8_t *m) {
  assert(s && v && m);
  DevPlane *p = new DevPlane;
  plane_alloc(*p, s->n);
  CU(cudaMemcpy(p->v, v, s->n * sizeof(float), cudaMemcpyHostToDevice));
  CU(cudaMemcpy(p->m, m, s->n, cudaMemcpyHostToDevice));
  s->in.push_back(p);
  return p;
}

DevPlane *plane_new(Session *s) {
  assert(s);
  DevPlane *p = new DevPlane;
  plane_alloc(*p, s->n);
  return p;
}

void plane_del(Session *s, DevPlane *p) {
  assert(s && p);
  plane_free(*p);
  delete p;
}

void download(Session *s, const DevPlane *p, float *v, uint8_t *m) {
  assert(s && p && (v || m));
  if (v)
    CU(cudaMemcpy(v, p->v, s->n * sizeof(float), cudaMemcpyDeviceToHost));
  if (m)
    CU(cudaMemcpy(m, p->m, s->n, cudaMemcpyDeviceToHost));
}

void pin(void *host, size_t bytes) { CU(cudaHostRegister(host, bytes, cudaHostRegisterDefault)); }
void unpin(void *host) { CU(cudaHostUnregister(host)); }

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

// 分派: 六个入口 (ts / cs × 会话 / 一次性 / 设备输出) 共用 OpTable 展开, 只差调哪个 call
void run_ts_dev(Session *s, const char *name, const DevPlane *x, const DevPlane *y, const DevPlane *z, DevPlane *out, int T,
                int A, const Param &p, double *kernel_ms) {
#define G_TS(Name, c_name, ar, t, a, kern, kdom, in_dom, out_dom, op, note) \
  if (std::strcmp(name, #Name) == 0)                                \
    return call_dev<ts::Name>(s, x, y, z, out, T, A, p, kernel_ms);
  OP_TS(G_TS)
#undef G_TS
  assert(false && "算子不在 OpTable 的 TS 组");
}

void run_cs_dev(Session *s, const char *name, const DevPlane *x, const DevPlane *y, const DevPlane *z, DevPlane *out, int T,
                int A, const Param &p, double *kernel_ms) {
#define G_CS(Name, c_name, ar, t, a, kern, kdom, in_dom, out_dom, op, note) \
  if (std::strcmp(name, #Name) == 0)                                \
    return call_dev<cs::Name>(s, x, y, z, out, T, A, p, kernel_ms);
  OP_CS(G_CS)
#undef G_CS
  assert(false && "算子不在 OpTable 的 CS 组");
}

void run_ts(Session *s, const char *name, const DevPlane *x, const DevPlane *y, const DevPlane *z, float *ov, uint8_t *om,
            int T, int A, const Param &p, double *kernel_ms) {
#define G_TS(Name, c_name, ar, t, a, kern, kdom, in_dom, out_dom, op, note) \
  if (std::strcmp(name, #Name) == 0)                                \
    return call<ts::Name>(s, x, y, z, ov, om, T, A, p, kernel_ms);
  OP_TS(G_TS)
#undef G_TS
  assert(false && "算子不在 OpTable 的 TS 组");
}

void run_cs(Session *s, const char *name, const DevPlane *x, const DevPlane *y, const DevPlane *z, float *ov, uint8_t *om,
            int T, int A, const Param &p, double *kernel_ms) {
#define G_CS(Name, c_name, ar, t, a, kern, kdom, in_dom, out_dom, op, note) \
  if (std::strcmp(name, #Name) == 0)                                \
    return call<cs::Name>(s, x, y, z, ov, om, T, A, p, kernel_ms);
  OP_CS(G_CS)
#undef G_CS
  assert(false && "算子不在 OpTable 的 CS 组");
}

void run_ts(const char *name, const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym,
            const float *zv, const uint8_t *zm, float *ov, uint8_t *om, int T, int A, const Param &p,
            double *kernel_ms) {
#define G_TS(Name, c_name, ar, t, a, kern, kdom, in_dom, out_dom, op, note) \
  if (std::strcmp(name, #Name) == 0)                                \
    return call_once<ts::Name>(xv, xm, yv, ym, zv, zm, ov, om, T, A, p, kernel_ms);
  OP_TS(G_TS)
#undef G_TS
  assert(false && "算子不在 OpTable 的 TS 组");
}

void run_cs(const char *name, const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym,
            const float *zv, const uint8_t *zm, float *ov, uint8_t *om, int T, int A, const Param &p,
            double *kernel_ms) {
#define G_CS(Name, c_name, ar, t, a, kern, kdom, in_dom, out_dom, op, note) \
  if (std::strcmp(name, #Name) == 0)                                \
    return call_once<cs::Name>(xv, xm, yv, ym, zv, zm, ov, om, T, A, p, kernel_ms);
  OP_CS(G_CS)
#undef G_CS
  assert(false && "算子不在 OpTable 的 CS 组");
}

namespace {

// 设备缓冲 (任意元素类型): 拷入 / 分配 / 拷出 / 释放
template <class E>
struct DevBuf {
  E *p = nullptr;
  void up(const E *h, size_t n) {
    CU(cudaMalloc(&p, n * sizeof(E)));
    CU(cudaMemcpy(p, h, n * sizeof(E), cudaMemcpyHostToDevice));
  }
  void alloc(size_t n) {
    CU(cudaMalloc(&p, n * sizeof(E)));
    CU(cudaMemset(p, 0, n * sizeof(E)));
  }
  void down(E *h, size_t n) const { CU(cudaMemcpy(h, p, n * sizeof(E), cudaMemcpyDeviceToHost)); }
  void free_() {
    if (p)
      CU(cudaFree(p));
  }
};

struct Timer {
  cudaEvent_t e0, e1;
  Timer() {
    CU(cudaEventCreate(&e0));
    CU(cudaEventCreate(&e1));
  }
  ~Timer() {
    CU(cudaEventDestroy(e0));
    CU(cudaEventDestroy(e1));
  }
  void begin() { CU(cudaEventRecord(e0)); }
  double end() { // ms
    CU(cudaEventRecord(e1));
    CU(cudaEventSynchronize(e1));
    CU(cudaGetLastError());
    float f = 0.f;
    CU(cudaEventElapsedTime(&f, e0, e1));
    return f;
  }
};

} // namespace

// Stat 常驻会话: 标签 (原值 / 短标签 / 掩码 / rank 预处理) + 工作区 + 行输出, open 时全部就位
struct StatSession {
  int T = 0, A = 0;
  factor::stat::Holds hd;
  DevBuf<uint16_t> lv[factor::stat::kMaxHold], sv[factor::stat::kMaxHold], ry[factor::stat::kMaxHold];
  DevBuf<uint8_t> lm[factor::stat::kMaxHold];
  stat::LabelSet L;
  DevBuf<uint16_t> ws;
  DevBuf<factor::stat::Row> out;
  Timer tm;
};

StatSession *stat_open(int T, int A, const factor::stat::Holds &hd, const StatLabelHost *lab, double *prep_ms) {
  factor::stat::assert_holds(hd);
  assert(lab && T > 0 && A > 0);
  const size_t n = static_cast<size_t>(T) * A;
  const int H = hd.n;
  StatSession *s = new StatSession;
  s->T = T, s->A = A, s->hd = hd;
  s->L.n = H;
  for (int i = 0; i < H; ++i) {
    s->lv[i].up(lab[i].lv, n), s->sv[i].up(lab[i].sv, n), s->lm[i].up(lab[i].m, n), s->ry[i].alloc(n);
    s->L.l[i] = {s->lv[i].p, s->sv[i].p, s->lm[i].p, s->ry[i].p};
  }
  s->ws.alloc(n);
  s->out.alloc(static_cast<size_t>(H) * T);
  s->tm.begin();
  for (int i = 0; i < H; ++i)
    stat::prep_label(s->lv[i].p, s->lm[i].p, T, A, s->ry[i].p, nullptr);
  const double pms = s->tm.end();
  if (prep_ms)
    *prep_ms = pms;
  return s;
}

void stat_eval(StatSession *s, const DevPlane *x, factor::stat::Frame f, factor::stat::Row *rows, double *eval_ms) {
  assert(s && x && x->v && x->m && rows);
  s->tm.begin();
  stat::eval(x->v, x->m, f, s->T, s->A, s->hd, s->L, s->ws.p, s->out.p, nullptr);
  const double ems = s->tm.end();
  if (eval_ms)
    *eval_ms = ems;
  s->out.down(rows, static_cast<size_t>(s->hd.n) * s->T);
}

void stat_close(StatSession *s) {
  assert(s);
  s->out.free_(), s->ws.free_();
  for (int i = 0; i < s->hd.n; ++i)
    s->lv[i].free_(), s->sv[i].free_(), s->lm[i].free_(), s->ry[i].free_();
  delete s;
}

// 一次性版: 临时会话 + 上传 x
void run_stat(const float *xv, const uint8_t *xm, factor::stat::Frame f, int T, int A, const factor::stat::Holds &hd,
              const StatLabelHost *lab, factor::stat::Row *rows, double *prep_ms, double *eval_ms) {
  const size_t n = static_cast<size_t>(T) * A;
  DevBuf<float> x;
  DevBuf<uint8_t> m;
  x.up(xv, n), m.up(xm, n);
  const DevPlane xp{x.p, m.p};
  StatSession *s = stat_open(T, A, hd, lab, prep_ms);
  stat_eval(s, &xp, f, rows, eval_ms);
  stat_close(s);
  x.free_(), m.free_();
}

} // namespace factor::gpu
