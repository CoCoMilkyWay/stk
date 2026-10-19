// =============================================================================
// GpuRun 的 CUDA 实现: 输入拷进显存 (会话内一次) → 跑 Gpu.cuh 的算子 → 输出拷回
// =============================================================================
//   只服务对拍的正确性 + 纯 kernel 计时, 不是挖掘的性能路径 (挖掘侧数据常驻显存, 不走这里).
//   分派表由 OpTable.hpp 展开 (下标 = expr::kOps 下标): 表里有名字而 Gpu.cuh 无同名 struct → 此处编译错.
//   逐点链融合节点 (Dag fuse) 的解释核也在这里 (run_fused).
// =============================================================================

#include "factor/CS/Gpu.cuh" // IWYU pragma: keep
#include "factor/TS/Gpu.cuh" // IWYU pragma: keep

#include "factor/Search/Gpu.cuh" // IWYU pragma: keep
#include "factor/Stat/Gpu.cuh"   // IWYU pragma: keep

#include "factor/GpuRun.hpp"
#include "factor/OpTable.hpp"

#include <cassert>
#include <cstdio>
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

// 算子分派表 (下标 = expr::kOps 下标): A 域 token 粘贴选命名空间 (SELF → ts, ALL / GROUP → cs); 表里有名字而 Gpu.cuh 无同名 struct → 编译错
struct Kernel {
  size_t (*scratch)(const Call &);
  void (*run)(const Call &, void *, cudaStream_t);
  bool cs; // 收池掩码 g
};
const Kernel &kernel(int op) {
#define G_SELF(Name) Kernel{&ts::Name::scratch, &ts::Name::run, false}
#define G_ALL(Name) Kernel{&cs::Name::scratch, &cs::Name::run, true}
#define G_GROUP G_ALL
#define G_ROW(Name, c_name, ar, t, a, kern, kdom, in_dom, out_dom, op_tex, note) G_##a(Name),
  static const Kernel tbl[] = {OP_ALL(G_ROW)};
#undef G_ROW
#undef G_GROUP
#undef G_ALL
#undef G_SELF
  assert(op >= 0 && op < static_cast<int>(sizeof(tbl) / sizeof(tbl[0])));
  return tbl[op];
}

// 只有要计时才夹事件并同步: 搜索路径 (kernel_ms = nullptr) 整条 DAG 异步排队, 不逐算子做 CPU↔GPU 往返
template <class Fn>
void timed(Session *s, double *kernel_ms, Fn &&launch) {
  if (kernel_ms)
    CU(cudaEventRecord(s->e0));
  launch();
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

// ---- 逐点链融合核: 一线程一格, 逐步调各 POINT 算子的 F::apply, 每步结果过 mk (= dev::store 口径) 与不融合逐位同 ----
// OpTable 行序 → 编译期下标 (case 标号)
#define G_IDX(Name, c_name, ar, t, a, kern, kdom, in_dom, out_dom, op_tex, note) kIdx##Name,
enum : int { OP_ALL(G_IDX) kIdxEnd };
#undef G_IDX

struct FuseArgs { // kernel 参数区 (< 4 KB)
  In in[kFuseIn];
  Fuse f;
};

template <int NARY, class F>
__device__ __forceinline__ DVal fuse_step(const FuseStep &st, const In *in, const DVal *r, int i, int t_seg) {
  DVal u[3] = {{0.f, 0}, {0.f, 0}, {0.f, 0}};
#pragma unroll
  for (int j = 0; j < NARY; ++j) {
    const int q = st.arg[j];
    u[j] = q >= 0 ? DVal{in[q].v[i], in[q].m[i]} : r[~q];
  }
  float v = 0.f;
  bool m = false;
  F::apply(u[0], u[1], u[2], t_seg, st.p, v, m);
  const bool ok = m && dev::fin(v);
  return DVal{ok ? v : 0.f, static_cast<uint8_t>(ok ? 1 : 0)};
}

__global__ void fused(const FuseArgs a, float *ov, uint8_t *om, int T, int A, int S) {
  const int col = blockIdx.x * blockDim.x + threadIdx.x;
  if (col >= A)
    return;
  for (int t = blockIdx.y; t < T; t += gridDim.y) {
    const int i = t * A + col;
    DVal r[kFuseLen];
    for (int s = 0; s < a.f.n; ++s) {
      const FuseStep &st = a.f.s[s];
      switch (st.op) {
#define G_FUSE_SELF_POINT(Name, ar)                           \
  case kIdx##Name:                                            \
    r[s] = fuse_step<ar, ts::Name::F>(st, a.in, r, i, t % S); \
    break;
#define G_FUSE_SELF_EXPAND(Name, ar)
#define G_FUSE_SELF_ROLL(Name, ar)
#define G_FUSE_SELF_EXPO(Name, ar)
#define G_FUSE_ALL_POINT(Name, ar)
#define G_FUSE_ALL_EXPAND(Name, ar)
#define G_FUSE_ALL_ROLL(Name, ar)
#define G_FUSE_ALL_EXPO(Name, ar)
#define G_FUSE_GROUP_POINT(Name, ar)
#define G_FUSE_GROUP_EXPAND(Name, ar)
#define G_FUSE_GROUP_ROLL(Name, ar)
#define G_FUSE_GROUP_EXPO(Name, ar)
#define G_FUSE(Name, c_name, ar, t, a, kern, kdom, in_dom, out_dom, op_tex, note) G_FUSE_##a##_##t(Name, ar)
        OP_ALL(G_FUSE)
#undef G_FUSE
      default:
        assert(false && "融合步只装 POINT × SELF 算子");
      }
    }
    ov[i] = r[a.f.n - 1].v;
    om[i] = r[a.f.n - 1].m;
  }
}

} // namespace

// 会话内跑一个算子, 输出留在设备平面 out: 工作区不够才重分配 (只增不缩); cudaEvent 夹 run 计纯 kernel
void run_dev(Session *s, int op, const DevPlane *x, const DevPlane *y, const DevPlane *z, const DevPlane *g, DevPlane *out, int T, int A,
             int seg, const Param &p, double *kernel_ms) {
  assert(s && s->n == static_cast<size_t>(T) * A && "会话形状与本次调用不符");
  assert(out && out->v && out->m);
  assert(out != x && out != y && out != z && "算子不支持原地");
  assert(is_seg(seg) && T % seg == 0 && "须给合法段长, T 为整段数");
  const Kernel &k = kernel(op);
  assert(k.cs == (g != nullptr) && (!k.cs || g->m) && "CS 算子必须给池掩码 g, TS 不收");
  Call c;
  c.x = x ? In{x->v, x->m} : In{};
  c.y = y ? In{y->v, y->m} : In{};
  c.z = z ? In{z->v, z->m} : In{};
  c.g = g ? g->m : nullptr;
  c.o = {out->v, out->m};
  c.T = T, c.A = A, c.geo = geo_of(seg), c.p = p;
  const size_t wsn = k.scratch(c);
  if (wsn > s->ws_cap) {
    if (s->ws)
      CU(cudaFree(s->ws));
    CU(cudaMalloc(&s->ws, wsn));
    s->ws_cap = wsn;
  }
  timed(s, kernel_ms, [&] { k.run(c, wsn ? s->ws : nullptr, nullptr); });
}

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

// 会话版: 写会话输出平面后拷回宿主 (宿主 pin 过则走 DMA)
void run(Session *s, int op, const DevPlane *x, const DevPlane *y, const DevPlane *z, const DevPlane *g, float *ov, uint8_t *om, int T,
         int A, int seg, const Param &p, double *kernel_ms) {
  run_dev(s, op, x, y, z, g, &s->o, T, A, seg, p, kernel_ms);
  CU(cudaMemcpy(ov, s->o.v, s->n * sizeof(float), cudaMemcpyDeviceToHost));
  CU(cudaMemcpy(om, s->o.m, s->n, cudaMemcpyDeviceToHost));
}

// 一次性版: 临时会话, 上传 → 跑 → 关
void run(int op, const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, const float *zv, const uint8_t *zm,
         const uint8_t *gm, float *ov, uint8_t *om, int T, int A, int seg, const Param &p, double *kernel_ms) {
  Session *s = session_open(static_cast<size_t>(T) * A);
  const DevPlane *x = xv ? upload(s, xv, xm) : nullptr;
  const DevPlane *y = yv ? upload(s, yv, ym) : nullptr;
  const DevPlane *z = zv ? upload(s, zv, zm) : nullptr;
  const DevPlane *g = gm ? upload_mask(s, gm) : nullptr;
  run(s, op, x, y, z, g, ov, om, T, A, seg, p, kernel_ms);
  session_close(s);
}

void run_fused(Session *s, const Fuse &f, const DevPlane *const *in, int n_in, DevPlane *out, int T, int A, int seg, double *kernel_ms) {
  assert(s && s->n == static_cast<size_t>(T) * A && "会话形状与本次调用不符");
  assert(out && out->v && out->m);
  assert(f.n >= 2 && f.n <= kFuseLen && n_in >= 0 && n_in <= kFuseIn);
  assert(is_seg(seg) && T % seg == 0 && "须给合法段长, T 为整段数");
  FuseArgs a;
  a.f = f;
  for (int q = 0; q < n_in; ++q) {
    assert(in[q] && in[q]->v && in[q]->m && in[q] != out && "融合节点不支持原地");
    a.in[q] = {in[q]->v, in[q]->m};
  }
  timed(s, kernel_ms, [&] { fused<<<ts::k::g_pt(T, A), ts::kPB>>>(a, out->v, out->m, T, A, seg); });
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

// 一组设备常驻标签 (两腿 v / m 拷入, ry 预处理), Stat / Search 会话共用; L = 给算子的设备指针视图 (契约 Labels)
struct DevLabels {
  static constexpr int kLegs = factor::stat::kLegs, kLong = factor::stat::kLong, kShort = factor::stat::kShort;
  int H = 0;
  DevBuf<uint16_t> v[kLegs][factor::stat::kMaxHold], ry[factor::stat::kMaxHold];
  DevBuf<uint8_t> m[factor::stat::kMaxHold];
  factor::stat::Labels L;
  void alloc(const factor::stat::Holds &hd, int seg, size_t n) {
    factor::stat::assert_holds(hd, seg);
    H = hd.n, L.hd = hd;
    for (int i = 0; i < H; ++i) {
      for (int l = 0; l < kLegs; ++l)
        v[l][i].alloc(n);
      m[i].alloc(n), ry[i].alloc(n);
      L.l[i] = {{v[kLong][i].p, v[kShort][i].p}, m[i].p, ry[i].p};
    }
  }
  // 宿主标签 (持有期须与 alloc 同) 拷入 + ry 预处理 (纯 kernel 计时)
  double load(const factor::stat::Labels &host, const factor::stat::Span &s, Timer &tm) {
    factor::stat::assert_labels(host, s.g.S, false);
    assert(host.hd.n == H);
    for (int i = 0; i < H; ++i)
      assert(host.hd.h[i] == L.hd.h[i] && "标签持有期与会话不符");
    L.cap = host.cap;
    for (int i = 0; i < H; ++i) {
      for (int l = 0; l < kLegs; ++l)
        v[l][i].put(host.l[i].v[l], s.n());
      m[i].put(host.l[i].m, s.n());
    }
    tm.begin();
    for (int i = 0; i < H; ++i)
      stat::prep_label(L.l[i], s, ry[i].p, nullptr);
    return tm.end();
  }
  void free_() {
    for (int i = 0; i < H; ++i) {
      for (int l = 0; l < kLegs; ++l)
        v[l][i].free_();
      m[i].free_(), ry[i].free_();
    }
  }
};

} // namespace

// Stat 常驻会话: 标签 + rx / lc 工作区 + 一级 / 分片 / 出口, open 时全部就位
struct StatSession {
  factor::stat::Span s;
  DevLabels lab;
  DevBuf<uint16_t> rx;
  DevBuf<uint8_t> lc;
  DevBuf<factor::stat::Row> rows;
  DevBuf<factor::stat::Accum> part, acc;
  stat::Workspace w;
  Timer tm;
};

StatSession *stat_open(int T, int A, int seg, const factor::stat::Labels &L, double *prep_ms) {
  StatSession *s = new StatSession;
  s->s = {T, A, geo_of(seg)};
  factor::stat::assert_span(s->s);
  const size_t n = s->s.n();
  const int H = L.hd.n, NB = stat::n_parts(s->s);
  s->lab.alloc(L.hd, seg, n);
  s->rx.alloc(n), s->lc.alloc(n);
  s->rows.alloc(static_cast<size_t>(H) * T);
  s->part.alloc(static_cast<size_t>(H) * NB), s->acc.alloc(static_cast<size_t>(H));
  s->w = {s->rows.p, s->part.p, s->acc.p, H, NB};
  const double pms = s->lab.load(L, s->s, s->tm);
  if (prep_ms)
    *prep_ms = pms;
  return s;
}

void stat_eval(StatSession *s, const DevPlane *x, const DevPlane *g, factor::stat::Frame f, factor::stat::Accum *acc, factor::stat::Row *rows,
               uint8_t *lc, uint16_t *rx, double *eval_ms) {
  assert(s && x && x->v && x->m && g && g->m && acc);
  if (eval_ms)
    s->tm.begin();
  stat::run(factor::stat::Plane{x->v, x->m}, g->m, f, s->s, s->lab.L, s->rx.p, s->w, nullptr);
  if (eval_ms)
    *eval_ms = s->tm.end();
  const int H = s->lab.L.hd.n;
  s->acc.down(acc, static_cast<size_t>(H)); // D2H 自带同步
  if (rows)
    s->rows.down(rows, static_cast<size_t>(H) * s->s.T);
  if (lc) {
    stat::leg_codes(s->rx.p, s->s, s->lc.p, nullptr);
    s->lc.down(lc, s->s.n());
  }
  if (rx)
    s->rx.down(rx, s->s.n());
}

void stat_close(StatSession *s) {
  assert(s);
  s->acc.free_(), s->part.free_(), s->rows.free_(), s->lc.free_(), s->rx.free_(), s->lab.free_();
  delete s;
}

// 一次性版: 临时会话 + 上传 x / g
void run_stat(factor::stat::Plane x, const uint8_t *gm, factor::stat::Frame f, int T, int A, int seg, const factor::stat::Labels &L,
              factor::stat::Accum *acc, factor::stat::Row *rows, double *prep_ms, double *eval_ms) {
  assert(x.v && x.m && gm);
  const size_t n = static_cast<size_t>(T) * A;
  DevBuf<float> xv;
  DevBuf<uint8_t> xm, g;
  xv.up(x.v, n), xm.up(x.m, n), g.up(gm, n);
  const DevPlane xp{xv.p, xm.p}, gp{nullptr, g.p};
  StatSession *s = stat_open(T, A, seg, L, prep_ms);
  stat_eval(s, &xp, &gp, f, acc, rows, nullptr, nullptr, eval_ms);
  stat_close(s);
  xv.free_(), xm.free_(), g.free_();
}

// Search 会话: 一块的标签 + Cmax 候选的 rx 工作区 + 分片 / 结果, open 时全部分配; load 只拷入 + 预处理
struct SearchSession {
  int T = 0, A = 0, seg = 0, Cmax = 0;
  factor::stat::Holds hd;
  double cap = 0.0;
  DevLabels lab;
  DevBuf<uint16_t> rx;
  DevBuf<search::Slice> part;
  DevBuf<factor::search::Result> out;
  bool loaded = false;
  Timer tm;
};

SearchSession *search_open(int T, int A, int seg, int Cmax, const factor::stat::Holds &hd, double cap) {
  factor::stat::assert_holds(hd, seg);
  assert(T > 0 && A > 0 && A <= factor::stat::kMaxA && Cmax >= 1 && Cmax <= stat::kMaxBatch && cap > 0.0);
  const size_t n = static_cast<size_t>(T) * A;
  SearchSession *s = new SearchSession;
  s->T = T, s->A = A, s->seg = seg, s->Cmax = Cmax, s->hd = hd, s->cap = cap;
  s->lab.alloc(hd, seg, n);
  const search::Work w = search::plan(Cmax, T, A, seg, hd.n, 0); // t_lo = 0 是分片数上界
  s->rx.alloc(w.rx_elems());
  s->part.alloc(w.part_elems());
  s->out.alloc(w.out_elems());
  return s;
}

void search_load(SearchSession *s, const factor::stat::Label *lab, double *prep_ms) {
  assert(s && lab);
  factor::stat::Labels L;
  L.hd = s->hd;
  L.cap = s->cap;
  for (int i = 0; i < s->hd.n; ++i)
    L.l[i] = lab[i];
  const double pms = s->lab.load(L, factor::stat::Span{s->T, s->A, geo_of(s->seg)}, s->tm);
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
    B.x[c] = {x[c]->v, x[c]->m};
  }
  const search::Work w = search::plan(C, s->T, s->A, s->seg, s->hd.n, t_lo);
  if (eval_ms)
    s->tm.begin();
  search::eval(B, g->m, f, w, t0_abs, s->lab.L, s->rx.p, s->part.p, s->out.p, nullptr);
  if (eval_ms)
    *eval_ms = s->tm.end();
  s->out.down(out, w.out_elems()); // D2H 自带同步; Result 是 POD, 设备格式 = 宿主格式
}

void search_ranks(SearchSession *s, int c, int t0, int stride, int n_rows, uint16_t *out) {
  assert(s && s->loaded && c >= 0 && c < s->Cmax && t0 >= 0 && stride >= 1 && n_rows >= 1 && out);
  assert(t0 + static_cast<long long>(n_rows - 1) * stride < s->T && "抽样行越界");
  const size_t A = static_cast<size_t>(s->A);
  const uint16_t *src = s->rx.p + (static_cast<size_t>(c) * s->T + static_cast<size_t>(t0)) * A;
  CU(cudaMemcpy2D(out, A * sizeof(uint16_t), src, static_cast<size_t>(stride) * A * sizeof(uint16_t), A * sizeof(uint16_t),
                  static_cast<size_t>(n_rows), cudaMemcpyDeviceToHost));
}

void search_close(SearchSession *s) {
  assert(s);
  s->out.free_(), s->part.free_(), s->rx.free_();
  s->lab.free_();
  delete s;
}

void run_search(int C, const float *const *xv, const uint8_t *const *xm, const uint8_t *gm, factor::stat::Frame f, int T, int A, int seg,
                int t0_abs, int t_lo, const factor::stat::Holds &hd, double cap, const factor::stat::Label *lab, factor::search::Result *out,
                double *prep_ms, double *eval_ms) {
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
  SearchSession *s = search_open(T, A, seg, C, hd, cap);
  search_load(s, lab, prep_ms);
  search_eval(s, xpp.data(), C, &gp, f, t0_abs, t_lo, out, eval_ms);
  search_close(s);
  for (int c = 0; c < C; ++c)
    x[static_cast<size_t>(c)].free_(), m[static_cast<size_t>(c)].free_();
  g.free_();
}

} // namespace factor::gpu
