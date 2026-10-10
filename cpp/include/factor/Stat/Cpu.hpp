#pragma once

// =============================================================================
// Stat 评估算子的 CPU 后端 (factor::cpu::stat; 语义契约见 factor/Stat/Contract.hpp)
// =============================================================================
//   定位: 挖掘无 GPU 时的评估路径, 与 GPU 后端对拍 (无 golden, 两边必须 match). 独立实现, 不 include Gpu.cuh.
//
//   【分层】算子 = 行区间核 + 累加器, 无线程 / 无分配 / 无所有权; 流程 = 一份 run<Exec>, 谁出线程由 Exec 定 (factor/Exec.hpp):
//     核 (单线程, 绝对 t, 调用方给 Scratch):
//       rank_x_rows   [t0,t1) 行: CS 有效 x → 保序键 → LSD 基数排序 (位宽按 n 自适应: n ≤ 1024 用 8 位 × 4 趟, 否则 11 位 × 3 趟;
//                     任何正确的稳定排序给出同一 r16) → 等值段均秩 → ws; TS 有效 x (分位) → r16_quant 直接量化
//       rank_y_rows   同上对做多标签 (fp16 位) → ry. 与因子无关, 常驻期算一次
//       ac_row        rank-AC: Pearson(rx_t, rx_{t−h}) 整数五和, 与标签无关
//       label_row     标签侧: J = rx ∧ ry 有效, 一遍 A 轴累加 (整数五和 / 标签浮点和 / 分组和), branchless select; 收尾 finish_row
//       row_stat      一 (t, hold) 完整行 = ac (t ≥ h) + label (非段末尾部)
//     流程 (Exec 出线程; 阶段间一次屏障):
//       prep_label    ry[T][A]                                   ← exec.rows(rank_y_rows)
//       rank_x        ws[T][A]                                   ← exec.rows(rank_x_rows)
//       eval_rows     ws × 标签集 → Row[H][T] (可不落地) + HoldStat[H]  ← exec.rows(每 t 每 h row_stat → sink + Accum[tid]) → merge
//       run           = rank_x + eval_rows (一个标签集的完整评估)
//     绝对 t 的意义: tail_masked (t % kSegLen) 与 ac 的 t−h 在任何切块下成立; 超大张量沿 T 分块只需外面带 h_max 行 rx + 跨块 Accum.
//     Workspace 由调用方持有 (ws / Scratch[tid] / Accum[tid][H]), ensure 幂等 → 稳态零分配; 搜索每 worker 一份, 交互一份常驻.
//
//   【标签】fp16 位 → float 用 _Float16 (clang, -march=native 有 F16C), 精确转换, 与 GPU __half2float 逐位一致.
//   出错策略: 只 assert. 【precise-math】编进 -fno-fast-math TU.
// =============================================================================

#include "factor/Stat/Contract.hpp"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <vector>

namespace factor::cpu::stat {

using factor::stat::Accum;
using factor::stat::Frame;
using factor::stat::Holds;
using factor::stat::HoldStat;
using factor::stat::kGroups;
using factor::stat::kMaxA;
using factor::stat::kMaxHold;
using factor::stat::kNoKey;
using factor::stat::kRankNone;
using factor::stat::Row;

// 一组常驻标签 (宿主指针, 全部 [T][A]); ry 由 prep_label 填
struct Label {
  const uint16_t *lv = nullptr; // 做多收益 fp16 位 (毛 / 扣冲击后的净, 由调用方定)
  const uint16_t *sv = nullptr; // 做空收益 fp16 位
  const uint8_t *m = nullptr;   // 有效位 (long / short 共用)
  const uint16_t *ry = nullptr; // 做多标签逐行 r16 (prep_label 输出)
};

// 每线程排序暂存 (2 × A × 8B + 桶计数), 跨行 / 跨因子复用
inline constexpr int kRadixMaxBits = 11;
struct Scratch {
  std::vector<uint64_t> a, b; // (key << 16) | idx
  int cnt[1 << kRadixMaxBits];
  void reserve(int A) {
    assert(A >= 1 && A <= kMaxA);
    a.resize(static_cast<size_t>(A));
    b.resize(static_cast<size_t>(A));
  }
};

namespace detail {

inline float h2f(uint16_t b) {
  _Float16 h;
  std::memcpy(&h, &b, sizeof(h));
  return static_cast<float>(h);
}

// 一行的精确并列均秩: key(a) 给保序键或 kNoKey; out[a] = r16 / kRankNone. n < 2 或全并列 → 整行 kRankNone
template <class Key>
inline void rank_row(const Key &key, int A, uint16_t *out, Scratch &sc) {
  assert(sc.a.size() >= static_cast<size_t>(A));
  int n = 0;
  for (int a = 0; a < A; ++a) {
    const unsigned k = key(a);
    sc.a[static_cast<size_t>(n)] = (static_cast<uint64_t>(k) << 16) | static_cast<uint64_t>(a);
    n += k != kNoKey;
  }
  for (int a = 0; a < A; ++a)
    out[a] = kRankNone;
  if (n < 2)
    return;
  // LSD 基数排序: 键在位 16..47 (32 位), 稳定. 小 n 时桶清零 / 前缀和是固定开销, 用窄桶多趟
  const int bits = n <= 1024 ? 8 : kRadixMaxBits, passes = (32 + bits - 1) / bits, radix = 1 << bits, mask = radix - 1;
  uint64_t *src = sc.a.data(), *dst = sc.b.data();
  for (int p = 0; p < passes; ++p) {
    const int shift = 16 + bits * p;
    std::fill(sc.cnt, sc.cnt + radix, 0);
    for (int i = 0; i < n; ++i)
      ++sc.cnt[(src[i] >> shift) & mask];
    int run = 0;
    for (int d = 0; d < radix; ++d) {
      const int c = sc.cnt[d];
      sc.cnt[d] = run;
      run += c;
    }
    for (int i = 0; i < n; ++i)
      dst[sc.cnt[(src[i] >> shift) & mask]++] = src[i];
    std::swap(src, dst);
  }
  const auto key_at = [&](int i) { return static_cast<unsigned>(src[i] >> 16); };
  if (!(key_at(n - 1) > key_at(0))) // 全并列 = spread 为假
    return;
  for (int i = 0; i < n;) {
    int j = i + 1;
    while (j < n && key_at(j) == key_at(i))
      ++j;
    const uint16_t r = factor::stat::r16_of(i, j - i, n);
    for (int p = i; p < j; ++p)
      out[src[p] & 0xFFFFu] = r;
    i = j;
  }
}

} // namespace detail

// -----------------------------------------------------------------------------
// 行区间核 (单线程, 绝对 t)
// -----------------------------------------------------------------------------

// 阶段 1: x (口径 f, 只看池内 g ∧ xm 的格; 契约【截面池 g】) 行 [t0,t1) → r16 → ws (同 [T][A] 布局的绝对下标)
inline void rank_x_rows(const float *xv, const uint8_t *xm, const uint8_t *g, Frame f, int t0, int t1, int A, uint16_t *ws, Scratch &sc) {
  if (f == Frame::CS) {
    for (int t = t0; t < t1; ++t) {
      const size_t base = static_cast<size_t>(t) * A;
      detail::rank_row([&](int a) { return (xm[base + a] & g[base + a]) ? factor::stat::ord(xv[base + a]) : kNoKey; }, A, ws + base, sc);
    }
    return;
  }
  for (size_t i = static_cast<size_t>(t0) * A, e = static_cast<size_t>(t1) * A; i < e; ++i)
    ws[i] = (xm[i] & g[i]) ? factor::stat::r16_quant(xv[i]) : kRankNone;
}

// 做多标签 (fp16 位 + 有效位) 行 [t0,t1) → r16 → ry
inline void rank_y_rows(const uint16_t *lv, const uint8_t *m, int t0, int t1, int A, uint16_t *ry, Scratch &sc) {
  for (int t = t0; t < t1; ++t) {
    const size_t base = static_cast<size_t>(t) * A;
    detail::rank_row([&](int a) { return m[base + a] ? factor::stat::ord(detail::h2f(lv[base + a])) : kNoKey; }, A, ry + base, sc);
  }
}

// rank-AC: rx = 本行 r16, rlag = t−h 行 r16 → r.ac / r.ok_ac
inline void ac_row(const uint16_t *rx, const uint16_t *rlag, int A, Row &r) {
  using ull = unsigned long long;
  ull n = 0, sx = 0, sy = 0, sxx = 0, syy = 0, sxy = 0;
  for (int a = 0; a < A; ++a) {
    const ull x = rx[a], y = rlag[a];
    const bool ok = rx[a] != kRankNone && rlag[a] != kRankNone;
    n += ok;
    sx += ok ? x : 0;
    sy += ok ? y : 0;
    sxx += ok ? x * x : 0;
    syy += ok ? y * y : 0;
    sxy += ok ? x * y : 0;
  }
  bool ok = false;
  r.ac = factor::stat::pearson_int(n, sx, sy, sxx, syy, sxy, ok);
  r.ok_ac = ok ? 1u : 0u;
}

// 标签侧: rx = 本行 r16, t = 绝对行 (取标签行) → r 的 ic / mkt / grp / cnt / ls / ok. 调用方已排除段末尾部
inline void label_row(Frame f, const uint16_t *rx, int t, int A, const Label &L, Row &r) {
  using ull = unsigned long long;
  const size_t base = static_cast<size_t>(t) * A;
  const uint16_t *ry = L.ry + base, *lv = L.lv + base, *sv = L.sv + base;
  ull n = 0, sx = 0, sy = 0, sxx = 0, syy = 0, sxy = 0;
  double smk = 0.0, ssv = 0.0, s0 = 0.0, gs[kGroups] = {};
  int gc[kGroups] = {};
  for (int a = 0; a < A; ++a) {
    const ull x = rx[a], y = ry[a];
    const bool ok = rx[a] != kRankNone && ry[a] != kRankNone;
    n += ok;
    sx += ok ? x : 0;
    sy += ok ? y : 0;
    sxx += ok ? x * x : 0;
    syy += ok ? y * y : 0;
    sxy += ok ? x * y : 0;
    const float yl = detail::h2f(lv[a]), ys = detail::h2f(sv[a]);
    const int g = ok ? factor::stat::grp_of(rx[a]) : 0;
    smk += ok ? yl : 0.f;
    ssv += ok ? ys : 0.f;
    s0 += (ok && g == 0) ? ys : 0.f;
    gs[g] += ok ? yl : 0.f;
    gc[g] += ok;
  }
  factor::stat::finish_row(f, n, sx, sy, sxx, syy, sxy, smk, ssv, gs, gc, s0, r);
}

// 一 (t, hold) 完整行. ws = 全部 r16 [T][A] (ac 取 t−h 行, h = hold_minutes(hold))
inline Row row_stat(Frame f, const uint16_t *ws, int t, int A, int hold, const Label &L) {
  Row r;
  const int h = factor::stat::hold_minutes(hold);
  const uint16_t *rx = ws + static_cast<size_t>(t) * A;
  if (t >= h)
    ac_row(rx, ws + static_cast<size_t>(t - h) * A, A, r);
  if (!factor::stat::tail_masked(t, hold))
    label_row(f, rx, t, A, L, r);
  return r;
}

// -----------------------------------------------------------------------------
// 流程 (Exec 出线程, 见 factor/Exec.hpp)
// -----------------------------------------------------------------------------

// 调用方持有; ensure 幂等 (形状对得上零分配)
struct Workspace {
  std::vector<uint16_t> ws; // [T][A] r16(x)
  std::vector<Scratch> sc;  // [tid]
  std::vector<Accum> acc;   // [tid][H]
  int T = 0, A = 0, H = 0, threads = 0;
  void ensure(int T_, int A_, int H_, int threads_) {
    assert(T_ >= 1 && A_ >= 1 && A_ <= kMaxA && static_cast<long long>(T_) * A_ < (1LL << 31));
    assert(H_ >= 1 && H_ <= kMaxHold && threads_ >= 1);
    if (ws.size() != static_cast<size_t>(T_) * A_)
      ws.resize(static_cast<size_t>(T_) * A_);
    if (sc.size() < static_cast<size_t>(threads_))
      sc.resize(static_cast<size_t>(threads_));
    for (Scratch &s : sc)
      if (s.a.size() != static_cast<size_t>(A_))
        s.reserve(A_);
    if (acc.size() < static_cast<size_t>(threads_) * H_)
      acc.resize(static_cast<size_t>(threads_) * H_);
    T = T_, A = A_, H = H_, threads = threads_;
  }
};

// 标签预处理: 做多标签 → ry [T][A]. 常驻期算一次 (用 w.sc)
template <class Exec>
inline void prep_label(Exec &ex, const uint16_t *lv, const uint8_t *m, int T, int A, uint16_t *ry, Workspace &w) {
  assert(w.A == A && w.threads >= ex.threads() && "Workspace 未 ensure 到位");
  ex.rows(T, [&](int t0, int t1, int tid) { rank_y_rows(lv, m, t0, t1, A, ry, w.sc[static_cast<size_t>(tid)]); });
}

// 阶段 1: r16(x) → w.ws
template <class Exec>
inline void rank_x(Exec &ex, const float *xv, const uint8_t *xm, const uint8_t *g, Frame f, int T, int A, Workspace &w) {
  assert(g && w.T == T && w.A == A && w.threads >= ex.threads());
  ex.rows(T, [&](int t0, int t1, int tid) { rank_x_rows(xv, xm, g, f, t0, t1, A, w.ws.data(), w.sc[static_cast<size_t>(tid)]); });
}

// 阶段 2 + 二级: w.ws × hd.n 组标签 → rows[hd.n][T] (nullptr = 不落地) + out[hd.n]
template <class Exec>
inline void eval_rows(Exec &ex, Frame f, int T, int A, const Holds &hd, const Label *lab, Workspace &w, Row *rows, HoldStat *out) {
  assert(w.T == T && w.A == A && w.H >= hd.n && w.threads >= ex.threads() && out);
  factor::stat::assert_holds(hd);
  const int H = hd.n;
  for (int i = 0; i < H; ++i)
    assert(lab[i].lv && lab[i].sv && lab[i].m && lab[i].ry && "标签未预处理 (prep_label)");
  const int nt = ex.threads();
  std::fill(w.acc.begin(), w.acc.begin() + static_cast<size_t>(nt) * H, Accum{});
  ex.rows(T, [&](int t0, int t1, int tid) {
    Accum *acc = w.acc.data() + static_cast<size_t>(tid) * H;
    for (int t = t0; t < t1; ++t)
      for (int i = 0; i < H; ++i) {
        const Row r = row_stat(f, w.ws.data(), t, A, hd.h[i], lab[i]);
        if (rows)
          rows[static_cast<size_t>(i) * T + t] = r;
        acc[i].add(r);
      }
  });
  for (int i = 0; i < H; ++i) {
    Accum a = w.acc[static_cast<size_t>(i)];
    for (int tid = 1; tid < nt; ++tid)
      a.merge(w.acc[static_cast<size_t>(tid) * H + i]);
    out[i] = a.finish(hd.h[i]);
  }
}

// 一个标签集的完整评估 = rank_x + eval_rows
template <class Exec>
inline void run(Exec &ex, const float *xv, const uint8_t *xm, const uint8_t *g, Frame f, int T, int A, const Holds &hd, const Label *lab,
                Workspace &w, Row *rows, HoldStat *out) {
  rank_x(ex, xv, xm, g, f, T, A, w);
  eval_rows(ex, f, T, A, hd, lab, w, rows, out);
}

} // namespace factor::cpu::stat
