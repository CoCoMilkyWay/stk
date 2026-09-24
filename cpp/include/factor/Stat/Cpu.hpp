#pragma once

// =============================================================================
// Stat 评估算子的 CPU 后端 (factor::cpu::stat; 语义契约见 factor/Stat/Contract.hpp)
// =============================================================================
//   定位: 挖掘无 GPU 时的评估路径, 与 GPU 后端对拍 (无 golden, 两边必须 match). 独立实现, 不 include Gpu.cuh.
//
//   【并行】行 (时刻) 之间完全独立 → 按 t 切块给 threads 个线程 (threads = 1 即单线程内联).
//   两个阶段各自并行, 阶段间 join 一次: 阶段 2 的 rank-AC 要读 t−h 行的 rank, 可能落在别的线程的块里.
//     阶段 1  每行: 有效 x → 保序键 → 3 趟 11 位 LSD 基数排序 (A ≤ 5120, 键 32 位) → 走一遍等值段给 r16 → ws[T][A]
//     阶段 2  每 (t, h): 一遍 A 轴累加 (整数五和 / 标签浮点和 / 分组和), 全 branchless select, 自动向量化
//   每线程一份排序暂存 (2 × A × 8B + 2048 计数), 跨行复用, 不逐行分配.
//
//   【标签】fp16 位 → float 用 _Float16 (clang, -march=native 有 F16C), 精确转换, 与 GPU __half2float 逐位一致.
//   出错策略: 只 assert. 【precise-math】编进 -fno-fast-math TU.
// =============================================================================

#include "factor/Stat/Contract.hpp"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <thread>
#include <vector>

namespace factor::cpu::stat {

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
  const uint16_t *lv = nullptr; // 做多净收益 fp16 位
  const uint16_t *sv = nullptr; // 做空净收益 fp16 位
  const uint8_t *m = nullptr;   // 有效位 (long / short 共用)
  const uint16_t *ry = nullptr; // 做多标签逐行 r16 (prep_label 输出)
};

namespace detail {

inline float h2f(uint16_t b) {
  _Float16 h;
  std::memcpy(&h, &b, sizeof(h));
  return static_cast<float>(h);
}

// ---- 每线程排序暂存 ----
inline constexpr int kRadixBits = 11;
inline constexpr int kRadix = 1 << kRadixBits;
inline constexpr int kRadixPasses = 3; // 3 × 11 = 33 ≥ 32 位键

struct Scratch {
  std::vector<uint64_t> a, b; // (key << 16) | idx
  int cnt[kRadix];
  void reserve(int A) {
    a.resize(static_cast<size_t>(A));
    b.resize(static_cast<size_t>(A));
  }
};

// 一行的精确并列均秩: key(a) 给保序键或 kNoKey; out[a] = r16 / kRankNone
//   n < 2 或全并列 → 整行 kRankNone
template <class Key>
inline void rank_row(const Key &key, int A, uint16_t *out, Scratch &sc) {
  int n = 0;
  for (int a = 0; a < A; ++a) {
    const unsigned k = key(a);
    sc.a[n] = (static_cast<uint64_t>(k) << 16) | static_cast<uint64_t>(a);
    n += k != kNoKey;
  }
  for (int a = 0; a < A; ++a)
    out[a] = kRankNone;
  if (n < 2)
    return;
  // LSD 基数排序: 键在位 16..47, 每趟 11 位, 稳定
  uint64_t *src = sc.a.data(), *dst = sc.b.data();
  for (int p = 0; p < kRadixPasses; ++p) {
    const int shift = 16 + kRadixBits * p;
    std::fill(sc.cnt, sc.cnt + kRadix, 0);
    for (int i = 0; i < n; ++i)
      ++sc.cnt[(src[i] >> shift) & (kRadix - 1)];
    int run = 0;
    for (int d = 0; d < kRadix; ++d) {
      const int c = sc.cnt[d];
      sc.cnt[d] = run;
      run += c;
    }
    for (int i = 0; i < n; ++i)
      dst[sc.cnt[(src[i] >> shift) & (kRadix - 1)]++] = src[i];
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

// 行区间并行: fn(t0, t1, tid). threads = 1 → 内联单线程
template <class Fn>
inline void par_rows(int T, int threads, Fn &&fn) {
  assert(threads >= 1 && T >= 1);
  if (threads == 1 || T < threads) {
    fn(0, T, 0);
    return;
  }
  const int chunk = (T + threads - 1) / threads;
  std::vector<std::thread> th;
  th.reserve(static_cast<size_t>(threads));
  for (int i = 0; i < threads; ++i) {
    const int t0 = i * chunk, t1 = std::min(T, t0 + chunk);
    if (t0 < t1)
      th.emplace_back([&fn, t0, t1, i] { fn(t0, t1, i); });
  }
  for (std::thread &t : th)
    t.join();
}

// 一 (t, hold) 行的统计. rx = 本行 r16, ws = 全部 r16 (取 t−h 行, h = hold_minutes(hold))
inline Row row_stat(const uint16_t *rx, int t, int A, int hold, const Label &L, const uint16_t *ws) {
  using ull = unsigned long long;
  Row r;
  const size_t base = static_cast<size_t>(t) * A;
  const int h = factor::stat::hold_minutes(hold);
  // ---- rank-AC (lag = h), 与标签无关 ----
  if (t >= h) {
    const uint16_t *rp = ws + static_cast<size_t>(t - h) * A;
    ull n = 0, sx = 0, sy = 0, sxx = 0, syy = 0, sxy = 0;
    for (int a = 0; a < A; ++a) {
      const ull x = rx[a], y = rp[a];
      const bool ok = rx[a] != kRankNone && rp[a] != kRankNone;
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
  // ---- 标签侧: 段末尾部掩掉 (仅分钟档) ----
  if (factor::stat::tail_masked(t, hold))
    return r;
  const uint16_t *ry = L.ry + base, *lv = L.lv + base, *sv = L.sv + base;
  ull n = 0, sx = 0, sy = 0, sxx = 0, syy = 0, sxy = 0;
  double smk = 0.0, s0 = 0.0, gs[kGroups] = {};
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
    const float yl = h2f(lv[a]), ys = h2f(sv[a]);
    const int g = ok ? factor::stat::grp_of(rx[a]) : 0;
    smk += ok ? yl : 0.f;
    s0 += (ok && g == 0) ? ys : 0.f;
    gs[g] += ok ? yl : 0.f;
    gc[g] += ok;
  }
  bool icok = false;
  const float ic = factor::stat::pearson_int(n, sx, sy, sxx, syy, sxy, icok);
  bool gok = true;
  for (int k = 0; k < kGroups; ++k)
    gok = gok && gc[k] >= 1;
  if (!(icok && gok))
    return r;
  r.ok = 1;
  r.ic = ic;
  r.mkt = static_cast<float>(smk / static_cast<double>(n));
  for (int k = 0; k < kGroups; ++k)
    r.grp[k] = static_cast<float>(gs[k] / gc[k]);
  r.ls = r.grp[kGroups - 1] + static_cast<float>(s0 / gc[0]);
  return r;
}

} // namespace detail

// 标签预处理: 做多标签 (fp16 位 + 有效位) 逐行 r16 → ry [T][A]. 常驻期算一次
inline void prep_label(const uint16_t *lv, const uint8_t *m, int T, int A, uint16_t *ry, int threads) {
  assert(T >= 1 && A >= 1 && A <= kMaxA);
  std::vector<detail::Scratch> sc(static_cast<size_t>(threads));
  for (detail::Scratch &s : sc)
    s.reserve(A);
  detail::par_rows(T, threads, [&](int t0, int t1, int tid) {
    for (int t = t0; t < t1; ++t) {
      const size_t base = static_cast<size_t>(t) * A;
      detail::rank_row([&](int a) { return m[base + a] ? factor::stat::ord(detail::h2f(lv[base + a])) : kNoKey; }, A,
                       ry + base, sc[static_cast<size_t>(tid)]);
    }
  });
}

// 评估: x + hd.n 组预处理标签 → rows[hd.n][T]. ws = 工作区 [T][A] uint16 (调用方分配, 跨因子复用)
inline void eval(const float *xv, const uint8_t *xm, int T, int A, const Holds &hd, const Label *lab, uint16_t *ws,
                 Row *rows, int threads) {
  assert(T >= 1 && A >= 1 && A <= kMaxA && static_cast<long long>(T) * A < (1LL << 31));
  factor::stat::assert_holds(hd);
  for (int i = 0; i < hd.n; ++i)
    assert(lab[i].lv && lab[i].sv && lab[i].m && lab[i].ry && "标签未预处理 (prep_label)");
  // 阶段 1: rank(x) → ws
  {
    std::vector<detail::Scratch> sc(static_cast<size_t>(threads));
    for (detail::Scratch &s : sc)
      s.reserve(A);
    detail::par_rows(T, threads, [&](int t0, int t1, int tid) {
      for (int t = t0; t < t1; ++t) {
        const size_t base = static_cast<size_t>(t) * A;
        detail::rank_row([&](int a) { return xm[base + a] ? factor::stat::ord(xv[base + a]) : kNoKey; }, A, ws + base,
                         sc[static_cast<size_t>(tid)]);
      }
    });
  }
  // 阶段 2: 每 (t, h) 一行统计
  detail::par_rows(T, threads, [&](int t0, int t1, int) {
    for (int t = t0; t < t1; ++t) {
      const uint16_t *rx = ws + static_cast<size_t>(t) * A;
      for (int i = 0; i < hd.n; ++i)
        rows[static_cast<size_t>(i) * T + t] = detail::row_stat(rx, t, A, hd.h[i], lab[i], ws);
    }
  });
}

} // namespace factor::cpu::stat
