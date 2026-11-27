// =============================================================================
// GpuRun 的 CUDA 实现: 常驻会话 / 设备平面 / Stat 会话
// =============================================================================
//   分派表由 OpTable.hpp 展开 (下标 = expr::kOps 下标): 表里有名字而 Gpu.cuh 无同名 struct → 此处编译错.
//   逐点链融合节点 (Dag fuse) 的解释核在 run_fused.
// =============================================================================

#include "factor/CS/Gpu.cuh" // IWYU pragma: keep
#include "factor/TS/Gpu.cuh" // IWYU pragma: keep

#include "factor/Stat/Gpu.cuh" // IWYU pragma: keep

#include "factor/GpuRun.hpp"
#include "factor/OpTable.hpp"

#include <algorithm>
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
// 只展开定元 TS 块 (OP_TS0..3): 融合步只装定元 POINT × SELF (Dag pointwise), N 元与 CS 不进
#define G_FUSE_POINT(Name, ar)                                \
  case kIdx##Name:                                            \
    r[s] = fuse_step<ar, ts::Name::F>(st, a.in, r, i, t % S); \
    break;
#define G_FUSE_EXPAND(Name, ar)
#define G_FUSE_ROLL(Name, ar)
#define G_FUSE_EXPO(Name, ar)
#define G_FUSE(Name, c_name, ar, t, a, kern, kdom, in_dom, out_dom, op_tex, note) G_FUSE_##t(Name, ar)
        OP_TS0(G_FUSE)
        OP_TS1(G_FUSE)
        OP_TS2(G_FUSE)
        OP_TS3(G_FUSE)
#undef G_FUSE
#undef G_FUSE_EXPO
#undef G_FUSE_ROLL
#undef G_FUSE_EXPAND
#undef G_FUSE_POINT
      default:
        assert(false && "融合步只装定元 POINT × SELF 算子");
      }
    }
    ov[i] = r[a.f.n - 1].v;
    om[i] = r[a.f.n - 1].m;
  }
}

} // namespace

// 会话内跑一个算子, 输出留在设备平面 out: 工作区不够才重分配 (只增不缩); cudaEvent 夹 run 计纯 kernel
void run_dev(Session *s, int op, const DevPlane *const *in, int n_in, const DevPlane *g, DevPlane *out, int T, int A, int seg,
             const Param &p, double *kernel_ms) {
  assert(s && s->n == static_cast<size_t>(T) * A && "会话形状与本次调用不符");
  assert(out && out->v && out->m);
  assert(n_in >= 0 && n_in <= kMaxIn);
  assert(is_seg(seg) && T % seg == 0 && "须给合法段长, T 为整段数");
  const Kernel &k = kernel(op);
  assert(k.cs == (g != nullptr) && (!k.cs || g->m) && "CS 算子必须给池掩码 g, TS 不收");
  In xs[kMaxIn];
  for (int i = 0; i < n_in; ++i) {
    assert(in[i] && in[i]->v && in[i]->m && in[i] != out && "算子不支持原地");
    xs[i] = {in[i]->v, in[i]->m};
  }
  Call c;
  bind_in(c, xs, n_in);
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
  if (s->o.m)
    plane_free(s->o);
  if (s->ws)
    CU(cudaFree(s->ws));
  CU(cudaEventDestroy(s->e0));
  CU(cudaEventDestroy(s->e1));
  delete s;
}

size_t session_bytes(const Session *s) {
  if (!s)
    return 0;
  size_t b = s->ws_cap + (s->o.m ? (sizeof(float) + 1) * s->n : 0);
  for (const DevPlane *p : s->in)
    b += ((p->v ? sizeof(float) : 0) + 1) * s->n;
  return b;
}

DevPlane *upload(Session *s, const float *v, const uint8_t *m) {
  assert(s && v && m);
  DevPlane *p = plane_new(s);
  upload_to(s, p, v, m);
  s->in.push_back(p);
  return p;
}

DevPlane *upload_mask(Session *s, const uint8_t *m) {
  assert(s && m);
  DevPlane *p = mask_new(s);
  upload_to(s, p, nullptr, m);
  s->in.push_back(p);
  return p;
}

DevPlane *plane_new(Session *s) {
  assert(s);
  DevPlane *p = new DevPlane;
  plane_alloc(*p, s->n);
  return p;
}

DevPlane *mask_new(Session *s) {
  assert(s);
  DevPlane *p = new DevPlane;
  CU(cudaMalloc(&p->m, s->n));
  return p;
}

void plane_del(Session *s, DevPlane *p) {
  assert(s && p);
  plane_free(*p);
  delete p;
}

void upload_to(Session *s, DevPlane *p, const float *v, const uint8_t *m) {
  assert(s && p && (v || m));
  if (v) {
    assert(p->v);
    CU(cudaMemcpy(p->v, v, s->n * sizeof(float), cudaMemcpyHostToDevice));
  }
  if (m)
    CU(cudaMemcpy(p->m, m, s->n, cudaMemcpyHostToDevice));
}

void download(Session *s, const DevPlane *p, float *v, uint8_t *m) {
  assert(s && p && (v || m));
  if (v) {
    assert(p->v);
    CU(cudaMemcpy(v, p->v, s->n * sizeof(float), cudaMemcpyDeviceToHost));
  }
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

int stat_max_batch() { return stat::kMaxBatch; }

// 会话版: 写会话输出平面后拷回宿主 (宿主 pin 过则走 DMA)
void run(Session *s, int op, const DevPlane *const *in, int n_in, const DevPlane *g, float *ov, uint8_t *om, int T, int A, int seg,
         const Param &p, double *kernel_ms) {
  assert(s);
  if (!s->o.m)
    plane_alloc(s->o, s->n);
  run_dev(s, op, in, n_in, g, &s->o, T, A, seg, p, kernel_ms);
  download(s, &s->o, ov, om);
}

// 一次性版: 临时会话, 上传 → 跑 → 关
void run(int op, int n_in, const float *const *v, const uint8_t *const *m, const uint8_t *gm, float *ov, uint8_t *om, int T, int A,
         int seg, const Param &p, double *kernel_ms) {
  assert(n_in >= 0 && n_in <= kMaxIn);
  Session *s = session_open(static_cast<size_t>(T) * A);
  const DevPlane *in[kMaxIn] = {};
  for (int i = 0; i < n_in; ++i)
    in[i] = upload(s, v[i], m[i]);
  const DevPlane *g = gm ? upload_mask(s, gm) : nullptr;
  run(s, op, in, n_in, g, ov, om, T, A, seg, p, kernel_ms);
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

// 设备缓冲 (任意元素类型): alloc 只分配 (整张覆写的平面), zalloc 分配并清零 (带未写尾段的汇总缓冲)
template <class E>
struct DevBuf {
  E *p = nullptr;
  size_t n = 0;
  size_t bytes() const { return n * sizeof(E); }
  void alloc(size_t n_) {
    n = n_;
    CU(cudaMalloc(&p, bytes()));
  }
  void zalloc(size_t n_) {
    alloc(n_);
    CU(cudaMemset(p, 0, bytes()));
  }
  void up(const E *h, size_t n_) {
    alloc(n_);
    put(h, n_);
  }
  void put(const E *h, size_t n_) const { CU(cudaMemcpy(p, h, n_ * sizeof(E), cudaMemcpyHostToDevice)); }
  void down(E *h, size_t n_) const { CU(cudaMemcpy(h, p, n_ * sizeof(E), cudaMemcpyDeviceToHost)); }
  void free_() {
    if (p)
      CU(cudaFree(p));
    p = nullptr, n = 0;
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

// 一组设备常驻标签 (lv / nv (只偏移型) / m / 可成交码 eo / 逐行池基准 mk 与满仓 cap 拷入, ry 预处理); L = 给算子的设备指针视图 (契约 Labels)
struct DevLabels {
  int H = 0;
  DevBuf<uint16_t> lv[kHoldCount], nv[kHoldCount], ry[kHoldCount];
  DevBuf<uint8_t> m[kHoldCount], eo;             // eo 各持有期共用一张
  DevBuf<float> mk[kHoldCount], cap[kHoldCount]; // [T] 池基准 / 满仓 (宿主装载时算好, 原样拷入)
  factor::stat::Labels L;
  void alloc(const Holds &hd, int seg, int T, size_t n) {
    factor::stat::assert_holds(hd, seg);
    H = hd.n, L.hd = hd;
    eo.alloc(n);
    L.eo = eo.p;
    for (int i = 0; i < H; ++i) {
      lv[i].alloc(n), m[i].alloc(n), ry[i].alloc(n), mk[i].alloc(static_cast<size_t>(T)), cap[i].alloc(static_cast<size_t>(T));
      if (hold_offset(hd.k[i]))
        nv[i].alloc(n);
      L.l[i] = {lv[i].p, nv[i].p, m[i].p, ry[i].p, mk[i].p, cap[i].p};
    }
  }
  // 宿主标签 (与 alloc 同持有期) 拷入 + ry 预处理 (纯 kernel 计时)
  double load(const factor::stat::Labels &host, const factor::stat::Span &s, Timer &tm) {
    assert(host.hd == L.hd && host.eo && "宿主标签与会话的持有期不符 / 缺可成交码");
    for (int i = 0; i < H; ++i)
      assert(host.l[i].lv && host.l[i].m && host.l[i].mk && host.l[i].cap && (!nv[i].p || host.l[i].nv) && "宿主标签不全");
    eo.put(host.eo, s.n());
    for (int i = 0; i < H; ++i) {
      const factor::stat::Label &h = host.l[i];
      lv[i].put(h.lv, s.n());
      m[i].put(h.m, s.n());
      mk[i].put(h.mk, static_cast<size_t>(s.T));
      cap[i].put(h.cap, static_cast<size_t>(s.T));
      if (nv[i].p)
        nv[i].put(h.nv, s.n());
    }
    tm.begin();
    for (int i = 0; i < H; ++i)
      stat::prep_label(L.l[i], s, ry[i].p, nullptr);
    return tm.end();
  }
  void free_() {
    for (int i = 0; i < H; ++i)
      lv[i].free_(), nv[i].free_(), m[i].free_(), ry[i].free_(), mk[i].free_(), cap[i].free_();
    eo.free_();
  }
  size_t bytes() const {
    size_t b = eo.bytes();
    for (int i = 0; i < H; ++i)
      b += lv[i].bytes() + nv[i].bytes() + m[i].bytes() + ry[i].bytes() + mk[i].bytes() + cap[i].bytes();
    return b;
  }
};

} // namespace

// Stat 常驻会话: 标签 + rx[Cmax] 工作区 + 一级 / 出口, open 时全部就位, load 可按同形状新块覆盖标签
struct StatSession {
  factor::stat::Span s;
  int Cmax = 0;
  DevLabels lab;
  DevBuf<uint16_t> rx;
  DevBuf<factor::stat::Row> rows;
  DevBuf<factor::stat::Sum> day;
  stat::Workspace w;
  bool loaded = false;
  Timer tm;
};

StatSession *stat_open(const factor::stat::Span &sh, int Cmax, const Holds &hd) {
  assert(sh.T > 0 && sh.A > 0 && sh.A <= factor::stat::kMaxA && Cmax >= 1 && Cmax <= stat::kMaxBatch);
  StatSession *s = new StatSession;
  s->s = {sh.T, sh.A, sh.g};
  s->Cmax = Cmax;
  const size_t n = s->s.n();
  const int H = hd.n, D = s->s.days();
  s->lab.alloc(hd, sh.g.S, sh.T, n);
  s->rx.alloc(static_cast<size_t>(Cmax) * n);
  s->rows.zalloc(static_cast<size_t>(H) * sh.T);
  s->day.zalloc(static_cast<size_t>(Cmax) * H * D);
  s->w = {s->rows.p, s->day.p, H, Cmax, D};
  return s;
}

void stat_load(StatSession *s, const factor::stat::Labels &lab, double *prep_ms) {
  assert(s);
  const double pms = s->lab.load(lab, s->s, s->tm);
  if (prep_ms)
    *prep_ms = pms;
  s->loaded = true;
}

void stat_eval(StatSession *ss, const DevPlane *const *x, int C, const DevPlane *g, const factor::stat::Mode &m, const factor::stat::Span &s, factor::stat::Sum *day,
               factor::stat::Row *rows, uint16_t *rx, double *eval_ms) {
  assert(ss && ss->loaded && x && C >= 1 && C <= ss->Cmax && g && g->m && day);
  assert((C == 1 || (!rows && !rx)) && "一级 / r16 只单候选可拷回");
  assert(s.T == ss->s.T && s.A == ss->s.A && s.g.S == ss->s.g.S && "块形与会话不符"); // 统计日数随 t_lo / t_back 只减 (open 按两者 = 0 的上界分配)
  factor::stat::assert_span(s);
  stat::Batch B;
  B.C = C;
  for (int c = 0; c < C; ++c) {
    assert(x[c] && x[c]->v && x[c]->m);
    B.x[c] = {x[c]->v, x[c]->m};
  }
  if (eval_ms)
    ss->tm.begin();
  stat::rank_x(B, g->m, m, s, ss->rx.p, nullptr);
  stat::accum(s, ss->rx.p, C, ss->lab.L, m.r, ss->w, rows != nullptr, nullptr);
  if (eval_ms)
    *eval_ms = ss->tm.end();
  const size_t H = static_cast<size_t>(ss->lab.L.hd.n), D = static_cast<size_t>(s.days());
  ss->day.down(day, static_cast<size_t>(C) * H * D); // D2H 自带同步; Sum / Row 是 POD, 设备格式 = 宿主格式; [C][H][D] 按本次统计日数密排
  if (rows)
    ss->rows.down(rows, H * static_cast<size_t>(s.T));
  if (rx)
    ss->rx.down(rx, s.n());
}

void stat_close(StatSession *s) {
  assert(s);
  s->day.free_(), s->rows.free_(), s->rx.free_(), s->lab.free_();
  delete s;
}

size_t stat_bytes(const StatSession *s) {
  return s ? s->lab.bytes() + s->rx.bytes() + s->rows.bytes() + s->day.bytes() : 0;
}

void run_stat(int C, const float *const *xv, const uint8_t *const *xm, const uint8_t *gm, const factor::stat::Mode &m, const factor::stat::Span &s,
              const factor::stat::Labels &lab, factor::stat::Sum *day, factor::stat::Row *rows, double *prep_ms, double *eval_ms) {
  assert(C >= 1 && C <= stat::kMaxBatch && xv && xm && gm);
  const size_t n = s.n();
  std::vector<DevBuf<float>> dv(static_cast<size_t>(C));
  std::vector<DevBuf<uint8_t>> dm(static_cast<size_t>(C));
  std::vector<DevPlane> xp(static_cast<size_t>(C));
  std::vector<const DevPlane *> xpp(static_cast<size_t>(C));
  for (int c = 0; c < C; ++c) {
    dv[static_cast<size_t>(c)].up(xv[c], n), dm[static_cast<size_t>(c)].up(xm[c], n);
    xp[static_cast<size_t>(c)] = {dv[static_cast<size_t>(c)].p, dm[static_cast<size_t>(c)].p};
    xpp[static_cast<size_t>(c)] = &xp[static_cast<size_t>(c)];
  }
  DevBuf<uint8_t> g;
  g.up(gm, n);
  const DevPlane gp{nullptr, g.p};
  StatSession *ss = stat_open(s, C, lab.hd);
  stat_load(ss, lab, prep_ms);
  stat_eval(ss, xpp.data(), C, &gp, m, s, day, rows, nullptr, eval_ms);
  stat_close(ss);
  for (int c = 0; c < C; ++c)
    dv[static_cast<size_t>(c)].free_(), dm[static_cast<size_t>(c)].free_();
  g.free_();
}

} // namespace factor::gpu
