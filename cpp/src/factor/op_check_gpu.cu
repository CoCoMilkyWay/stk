// =============================================================================
// GpuRun 的 CUDA 实现: 输入拷进显存 (会话内一次) → 跑 Gpu.cuh 的算子 → 输出拷回
// =============================================================================
//   只服务对拍的正确性 + 纯 kernel 计时, 不是挖掘的性能路径 (挖掘侧数据常驻显存, 不走这里).
//   分派表由 OpTable.hpp 展开: 表里有名字而 Gpu.cuh 无同名 struct → 此处编译错.
// =============================================================================

#include "factor/CS/Gpu.cuh" // IWYU pragma: keep
#include "factor/TS/Gpu.cuh" // IWYU pragma: keep

#include "factor/Search/Gpu.cuh" // IWYU pragma: keep
#include "factor/Stat/Gpu.cuh"   // IWYU pragma: keep

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
void plane_free(DevPlane &p) { // v 可空 (upload_mask 的掩码平面)
  if (p.v)
    CU(cudaFree(p.v));
  CU(cudaFree(p.m));
}

// 会话内跑一个算子, 输出留在设备平面 out: 工作区不够才重分配; cudaEvent 夹 Op::run 计纯 kernel.
// CS 算子多收池掩码 g (只用 g->m); TS 传 nullptr
template <class Op, bool CS>
void call_dev(Session *s, const DevPlane *x, const DevPlane *y, const DevPlane *z, const DevPlane *g, DevPlane *out, int T, int A,
              const Param &p, double *kernel_ms) {
  assert(s && s->n == static_cast<size_t>(T) * A && "会话形状与本次调用不符");
  assert(out && out->v && out->m);
  assert(out != x && out != y && out != z && "算子不支持原地");
  assert(CS == (g != nullptr) && (!CS || g->m) && "CS 算子必须给池掩码 g, TS 不收");
  const size_t wsn = Op::workspace(T, A, p);
  if (wsn > s->ws_cap) {
    if (s->ws)
      CU(cudaFree(s->ws));
    CU(cudaMalloc(&s->ws, wsn));
    s->ws_cap = wsn;
  }
  const float *xv = x ? x->v : nullptr, *yv = y ? y->v : nullptr, *zv = z ? z->v : nullptr;
  const uint8_t *xm = x ? x->m : nullptr, *ym = y ? y->m : nullptr, *zm = z ? z->m : nullptr;
  // 只有要计时才夹事件并同步: 搜索路径 (kernel_ms = nullptr) 整条 DAG 异步排队, 不逐算子做 CPU↔GPU 往返
  if (kernel_ms)
    CU(cudaEventRecord(s->e0));
  if constexpr (CS)
    Op::run(xv, xm, yv, ym, zv, zm, g->m, out->v, out->m, T, A, p, wsn ? s->ws : nullptr, nullptr);
  else
    Op::run(xv, xm, yv, ym, zv, zm, out->v, out->m, T, A, p, wsn ? s->ws : nullptr, nullptr);
  CU(cudaGetLastError());
  if (kernel_ms) {
    CU(cudaEventRecord(s->e1));
    CU(cudaEventSynchronize(s->e1));
    CU(cudaGetLastError());
    float f = 0.f;
    CU(cudaEventElapsedTime(&f, s->e0, s->e1));
    *kernel_ms = f;
  }
}

// 会话版: 写会话输出平面后拷回宿主 (宿主 pin 过则走 DMA)
template <class Op, bool CS>
void call(Session *s, const DevPlane *x, const DevPlane *y, const DevPlane *z, const DevPlane *g, float *ov, uint8_t *om, int T, int A,
          const Param &p, double *kernel_ms) {
  call_dev<Op, CS>(s, x, y, z, g, &s->o, T, A, p, kernel_ms);
  CU(cudaMemcpy(ov, s->o.v, s->n * sizeof(float), cudaMemcpyDeviceToHost));
  CU(cudaMemcpy(om, s->o.m, s->n, cudaMemcpyDeviceToHost));
}

// 一次性版本的公共体: 临时会话, 上传 → 跑 → 关 (gm 非空 ⟺ CS)
template <class Op, bool CS>
void call_once(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, const float *zv, const uint8_t *zm,
               const uint8_t *gm, float *ov, uint8_t *om, int T, int A, const Param &p, double *kernel_ms) {
  Session *s = session_open(static_cast<size_t>(T) * A);
  const DevPlane *x = xv ? upload(s, xv, xm) : nullptr;
  const DevPlane *y = yv ? upload(s, yv, ym) : nullptr;
  const DevPlane *z = zv ? upload(s, zv, zm) : nullptr;
  const DevPlane *g = gm ? upload_mask(s, gm) : nullptr;
  call<Op, CS>(s, x, y, z, g, ov, om, T, A, p, kernel_ms);
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

DevPlane *upload_mask(Session *s, const uint8_t *m) {
  assert(s && m);
  DevPlane *p = new DevPlane;
  CU(cudaMalloc(&p->m, s->n));
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

void mem_info(size_t *free_bytes, size_t *total_bytes) {
  assert(free_bytes && total_bytes);
  CU(cudaMemGetInfo(free_bytes, total_bytes));
}

int search_max_batch() { return stat::kMaxBatch; }

// 分派: 六个入口 (ts / cs × 会话 / 一次性 / 设备输出) 共用 OpTable 展开, 只差调哪个 call
void run_ts_dev(Session *s, const char *name, const DevPlane *x, const DevPlane *y, const DevPlane *z, DevPlane *out, int T,
                int A, const Param &p, double *kernel_ms) {
#define G_TS(Name, c_name, ar, t, a, kern, kdom, in_dom, out_dom, op, note) \
  if (std::strcmp(name, #Name) == 0)                                        \
    return call_dev<ts::Name, false>(s, x, y, z, nullptr, out, T, A, p, kernel_ms);
  OP_TS(G_TS)
#undef G_TS
  assert(false && "算子不在 OpTable 的 TS 组");
}

void run_cs_dev(Session *s, const char *name, const DevPlane *x, const DevPlane *y, const DevPlane *z, const DevPlane *g,
                DevPlane *out, int T, int A, const Param &p, double *kernel_ms) {
#define G_CS(Name, c_name, ar, t, a, kern, kdom, in_dom, out_dom, op, note) \
  if (std::strcmp(name, #Name) == 0)                                        \
    return call_dev<cs::Name, true>(s, x, y, z, g, out, T, A, p, kernel_ms);
  OP_CS(G_CS)
#undef G_CS
  assert(false && "算子不在 OpTable 的 CS 组");
}

void run_ts(Session *s, const char *name, const DevPlane *x, const DevPlane *y, const DevPlane *z, float *ov, uint8_t *om,
            int T, int A, const Param &p, double *kernel_ms) {
#define G_TS(Name, c_name, ar, t, a, kern, kdom, in_dom, out_dom, op, note) \
  if (std::strcmp(name, #Name) == 0)                                        \
    return call<ts::Name, false>(s, x, y, z, nullptr, ov, om, T, A, p, kernel_ms);
  OP_TS(G_TS)
#undef G_TS
  assert(false && "算子不在 OpTable 的 TS 组");
}

void run_cs(Session *s, const char *name, const DevPlane *x, const DevPlane *y, const DevPlane *z, const DevPlane *g,
            float *ov, uint8_t *om, int T, int A, const Param &p, double *kernel_ms) {
#define G_CS(Name, c_name, ar, t, a, kern, kdom, in_dom, out_dom, op, note) \
  if (std::strcmp(name, #Name) == 0)                                        \
    return call<cs::Name, true>(s, x, y, z, g, ov, om, T, A, p, kernel_ms);
  OP_CS(G_CS)
#undef G_CS
  assert(false && "算子不在 OpTable 的 CS 组");
}

void run_ts(const char *name, const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym,
            const float *zv, const uint8_t *zm, float *ov, uint8_t *om, int T, int A, const Param &p,
            double *kernel_ms) {
#define G_TS(Name, c_name, ar, t, a, kern, kdom, in_dom, out_dom, op, note) \
  if (std::strcmp(name, #Name) == 0)                                        \
    return call_once<ts::Name, false>(xv, xm, yv, ym, zv, zm, nullptr, ov, om, T, A, p, kernel_ms);
  OP_TS(G_TS)
#undef G_TS
  assert(false && "算子不在 OpTable 的 TS 组");
}

void run_cs(const char *name, const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym,
            const float *zv, const uint8_t *zm, const uint8_t *gm, float *ov, uint8_t *om, int T, int A, const Param &p,
            double *kernel_ms) {
  assert(gm && "CS 算子必须给池掩码");
#define G_CS(Name, c_name, ar, t, a, kern, kdom, in_dom, out_dom, op, note) \
  if (std::strcmp(name, #Name) == 0)                                        \
    return call_once<cs::Name, true>(xv, xm, yv, ym, zv, zm, gm, ov, om, T, A, p, kernel_ms);
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
  void put(const E *h, size_t n) const { CU(cudaMemcpy(p, h, n * sizeof(E), cudaMemcpyHostToDevice)); } // 已分配, 只拷入
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

// 一组设备常驻标签 (lv / sv / m 拷入, ry 预处理), Stat / Search 会话共用; L = 给算子的设备指针视图
struct DevLabels {
  int H = 0;
  DevBuf<uint16_t> lv[factor::stat::kMaxHold], sv[factor::stat::kMaxHold], ry[factor::stat::kMaxHold];
  DevBuf<uint8_t> m[factor::stat::kMaxHold];
  factor::stat::Label L[factor::stat::kMaxHold];
  void alloc(int H_, size_t n) {
    H = H_;
    for (int i = 0; i < H; ++i) {
      lv[i].alloc(n), sv[i].alloc(n), m[i].alloc(n), ry[i].alloc(n);
      L[i] = {lv[i].p, sv[i].p, m[i].p, ry[i].p};
    }
  }
  // 宿主标签拷入 + ry 预处理 (纯 kernel 计时)
  double load(const factor::stat::Label *lab, int T, int A, Timer &tm) {
    const size_t n = static_cast<size_t>(T) * A;
    for (int i = 0; i < H; ++i) {
      assert(lab[i].lv && lab[i].sv && lab[i].m);
      lv[i].put(lab[i].lv, n), sv[i].put(lab[i].sv, n), m[i].put(lab[i].m, n);
    }
    tm.begin();
    for (int i = 0; i < H; ++i)
      stat::prep_label(lv[i].p, m[i].p, T, A, ry[i].p, nullptr);
    return tm.end();
  }
  void free_() {
    for (int i = 0; i < H; ++i)
      lv[i].free_(), sv[i].free_(), m[i].free_(), ry[i].free_();
  }
};

} // namespace

// Stat 常驻会话: 标签 + 工作区 + 行输出, open 时全部就位
struct StatSession {
  int T = 0, A = 0;
  factor::stat::Holds hd;
  DevLabels lab;
  DevBuf<uint16_t> ws;
  DevBuf<factor::stat::Row> out;
  Timer tm;
};

StatSession *stat_open(int T, int A, const factor::stat::Holds &hd, const factor::stat::Label *lab, double *prep_ms) {
  factor::stat::assert_holds(hd);
  assert(lab && T > 0 && A > 0);
  const size_t n = static_cast<size_t>(T) * A;
  StatSession *s = new StatSession;
  s->T = T, s->A = A, s->hd = hd;
  s->lab.alloc(hd.n, n);
  s->ws.alloc(n);
  s->out.alloc(static_cast<size_t>(hd.n) * T);
  const double pms = s->lab.load(lab, T, A, s->tm);
  if (prep_ms)
    *prep_ms = pms;
  return s;
}

void stat_eval(StatSession *s, const DevPlane *x, const DevPlane *g, factor::stat::Frame f, factor::stat::Row *rows, double *eval_ms, uint16_t *rx) {
  assert(s && x && x->v && x->m && g && g->m && rows);
  if (eval_ms)
    s->tm.begin();
  stat::eval(x->v, x->m, g->m, f, s->T, s->A, s->hd, s->lab.L, s->ws.p, s->out.p, nullptr);
  if (eval_ms)
    *eval_ms = s->tm.end();
  s->out.down(rows, static_cast<size_t>(s->hd.n) * s->T); // D2H 自带同步
  if (rx)
    s->ws.down(rx, static_cast<size_t>(s->T) * s->A);
}

void stat_close(StatSession *s) {
  assert(s);
  s->out.free_(), s->ws.free_(), s->lab.free_();
  delete s;
}

// 一次性版: 临时会话 + 上传 x / g
void run_stat(const float *xv, const uint8_t *xm, const uint8_t *gm, factor::stat::Frame f, int T, int A, const factor::stat::Holds &hd,
              const factor::stat::Label *lab, factor::stat::Row *rows, double *prep_ms, double *eval_ms) {
  const size_t n = static_cast<size_t>(T) * A;
  DevBuf<float> x;
  DevBuf<uint8_t> m, g;
  x.up(xv, n), m.up(xm, n), g.up(gm, n);
  const DevPlane xp{x.p, m.p}, gp{nullptr, g.p};
  StatSession *s = stat_open(T, A, hd, lab, prep_ms);
  stat_eval(s, &xp, &gp, f, rows, eval_ms);
  stat_close(s);
  x.free_(), m.free_(), g.free_();
}

// Search 会话: 一块的标签 + Cmax 候选的 rx 工作区 + 分片 / 结果, open 时全部分配; load 只拷入 + 预处理
struct SearchSession {
  int T = 0, A = 0, Cmax = 0;
  factor::stat::Holds hd;
  DevLabels lab;
  DevBuf<uint16_t> rx;
  DevBuf<search::Slice> part;
  DevBuf<factor::search::Result> out;
  bool loaded = false;
  Timer tm;
};

SearchSession *search_open(int T, int A, int Cmax, const factor::stat::Holds &hd) {
  factor::stat::assert_holds(hd);
  assert(T > 0 && A > 0 && A <= factor::stat::kMaxA && Cmax >= 1 && Cmax <= stat::kMaxBatch);
  const size_t n = static_cast<size_t>(T) * A;
  SearchSession *s = new SearchSession;
  s->T = T, s->A = A, s->Cmax = Cmax, s->hd = hd;
  s->lab.alloc(hd.n, n);
  const search::Work w = search::plan(Cmax, T, A, hd.n, 0); // t_lo = 0 是分片数上界
  s->rx.alloc(w.rx_elems());
  s->part.alloc(w.part_elems());
  s->out.alloc(w.out_elems());
  return s;
}

void search_load(SearchSession *s, const factor::stat::Label *lab, double *prep_ms) {
  assert(s && lab);
  const double pms = s->lab.load(lab, s->T, s->A, s->tm);
  if (prep_ms)
    *prep_ms = pms;
  s->loaded = true;
}

void search_eval(SearchSession *s, const DevPlane *const *x, int C, const DevPlane *g, factor::stat::Frame f, int t0_abs, int t_lo,
                 factor::search::Result *out, double *eval_ms) {
  assert(s && s->loaded && x && C >= 1 && C <= s->Cmax && g && g->m && out);
  stat::Batch B;
  B.C = C;
  for (int c = 0; c < C; ++c) {
    assert(x[c] && x[c]->v && x[c]->m);
    B.v[c] = x[c]->v, B.m[c] = x[c]->m;
  }
  const search::Work w = search::plan(C, s->T, s->A, s->hd.n, t_lo);
  if (eval_ms)
    s->tm.begin();
  search::eval(B, g->m, f, w, t0_abs, s->hd, s->lab.L, s->rx.p, s->part.p, s->out.p, nullptr);
  if (eval_ms)
    *eval_ms = s->tm.end();
  s->out.down(out, w.out_elems()); // D2H 自带同步; Result 是 POD, 设备格式 = 宿主格式
}

void search_close(SearchSession *s) {
  assert(s);
  s->out.free_(), s->part.free_(), s->rx.free_();
  s->lab.free_();
  delete s;
}

void run_search(int C, const float *const *xv, const uint8_t *const *xm, const uint8_t *gm, factor::stat::Frame f, int T, int A, int t0_abs,
                int t_lo, const factor::stat::Holds &hd, const factor::stat::Label *lab, factor::search::Result *out, double *prep_ms,
                double *eval_ms) {
  assert(C >= 1 && C <= stat::kMaxBatch && xv && xm && gm);
  const size_t n = static_cast<size_t>(T) * A;
  std::vector<DevBuf<float>> x(static_cast<size_t>(C));
  std::vector<DevBuf<uint8_t>> m(static_cast<size_t>(C));
  std::vector<DevPlane> xp(static_cast<size_t>(C));
  std::vector<const DevPlane *> xpp(static_cast<size_t>(C));
  for (int c = 0; c < C; ++c) {
    x[static_cast<size_t>(c)].up(xv[c], n), m[static_cast<size_t>(c)].up(xm[c], n);
    xp[static_cast<size_t>(c)] = {x[static_cast<size_t>(c)].p, m[static_cast<size_t>(c)].p};
    xpp[static_cast<size_t>(c)] = &xp[static_cast<size_t>(c)];
  }
  DevBuf<uint8_t> g;
  g.up(gm, n);
  const DevPlane gp{nullptr, g.p};
  SearchSession *s = search_open(T, A, C, hd);
  search_load(s, lab, prep_ms);
  search_eval(s, xpp.data(), C, &gp, f, t0_abs, t_lo, out, eval_ms);
  search_close(s);
  for (int c = 0; c < C; ++c)
    x[static_cast<size_t>(c)].free_(), m[static_cast<size_t>(c)].free_();
  g.free_();
}

} // namespace factor::gpu
