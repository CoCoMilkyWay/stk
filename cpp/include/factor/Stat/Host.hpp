#pragma once

// =============================================================================
// Stat 行核的宿主版 (CPU; 契约 factor/Stat/Contract.hpp): 一行的 rank / 一 (t, hold) 的 Row
// =============================================================================
//   Stat/Cpu.hpp 与 Search/Cpu.hpp 两个 CPU 算子共用, 与 Stat/Dev.cuh 对仗 (同一组行核同名同参序, 一边宿主一边设备). 单线程, 无流程 / 无分配.
//     KeyX / KeyY 取键器 (行视图, 与 Dev.cuh 同名同形): 因子一行 (只看池内 g ∧ m 的格; g = m 即不看池) / 做多标签一行 (fp16 位) → 保序键 / kNoKey
//     rank_row    精确并列均秩: key(a) → LSD 基数排序 (位宽按 n 自适应: n ≤ 1024 用 8 位 × 4 趟, 否则 11 位 × 3 趟;
//                 任何正确的稳定排序给出同一 r16) → 等值段均秩 → out. n < 2 或全并列 → 整行 kRankNone. 标签行 = rank_row(KeyY)
//     rank_x_row  因子一行: CS rank_row(KeyX) / TS 直接量化 —— 口径分叉只在这里
//     row_stat    一 (t, hold) 完整行 = 换手侧 lag_row (t−h → t, 给了 lag 行才算) + 标签侧 label_row (t → t+h, 非段末尾部才算; 满档 cap 进持仓口径);
//                 整数五和走契约 pair_add, 收尾 finish_row. label_row 一遍 A 轴: 整数五和 / 两腿标签浮点和 / 分组和, branchless select;
//                 逐格扩展 Ext (调用方专属的附加统计, 同一遍同一个 J; NoExt 编译期消掉, Inspect 尾部分层用)
//     cost_row    一行 t 的换手成本 + cover, H 个持有期 × C 个金额档一遍 A 轴 (契约腿码 lc 逐格) → CostAcc[C][H] + cover[1 + C].
//                 Run (stat.costs) / Inspect 期限结构净线共用, 不进 Row
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

using factor::stat::Cost;
using factor::stat::CostAcc;
using factor::stat::CostLeg;
using factor::stat::Frame;
using factor::stat::kGroups;
using factor::stat::kLcMid;
using factor::stat::kLcVoid;
using factor::stat::kLegs;
using factor::stat::kLong;
using factor::stat::kMaxA;
using factor::stat::kNoKey;
using factor::stat::kRankNone;
using factor::stat::kShort;
using factor::stat::kSums;
using factor::stat::Label;
using factor::stat::LegSum;
using factor::stat::Row;

inline float h2f(uint16_t b) {
  _Float16 h;
  std::memcpy(&h, &b, sizeof(h));
  return static_cast<float>(h);
}

// ---- 取键器: 格 a (指针已偏到行首) → 保序键 / kNoKey ----
struct KeyX { // 因子一行, 只看池内 g ∧ m 的格 (契约【截面池 g】); g = m 即不看池
  const float *v;
  const uint8_t *m, *g;
  bool on(int a) const { return (m[a] & g[a]) != 0; }
  unsigned key(int a) const { return on(a) ? factor::stat::ord(v[a]) : kNoKey; }
};
struct KeyY { // 做多标签一行 (fp16 位)
  const uint16_t *v;
  const uint8_t *m;
  unsigned key(int a) const { return m[a] ? factor::stat::ord(h2f(v[a])) : kNoKey; }
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

// 一行的精确并列均秩: key.key(a) 给保序键或 kNoKey; out[a] = r16 / kRankNone
template <class Key>
inline void rank_row(const Key &key, int A, uint16_t *out, Scratch &sc) {
  assert(sc.a.size() >= static_cast<size_t>(A));
  int n = 0;
  for (int a = 0; a < A; ++a) {
    const unsigned k = key.key(a);
    sc.a[static_cast<size_t>(n)] = (static_cast<uint64_t>(k) << 16) | static_cast<uint64_t>(a);
    n += k != kNoKey;
  }
  std::fill(out, out + A, kRankNone);
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

// 因子一行 (口径 f) → r16
inline void rank_x_row(const KeyX &key, Frame f, int A, uint16_t *out, Scratch &sc) {
  if (f == Frame::CS) {
    rank_row(key, A, out, sc);
    return;
  }
  for (int a = 0; a < A; ++a)
    out[a] = key.on(a) ? factor::stat::r16_quant(key.v[a]) : kRankNone;
}

// 换手侧 (t−h → t): rx = 本行 r16, rlag = t−h 行 r16 → r.rac / r.ok_rac. 也是两候选同行截面相关的核 (rlag 换成另一候选)
inline void lag_row(const uint16_t *rx, const uint16_t *rlag, int A, Row &r) {
  unsigned long long s[kSums] = {};
  for (int a = 0; a < A; ++a)
    factor::stat::pair_add(s, rx[a], rlag[a], rx[a] != kRankNone && rlag[a] != kRankNone);
  bool ok = false;
  r.rac = factor::stat::pearson_int(s, ok);
  r.ok_rac = ok ? 1u : 0u;
}

// 换手成本 + cover (契约【换手成本】): lc = 本行腿码; lag[i] = t−h_i 行腿码 (没有 → nullptr, 该持有期不攒); c[k] 已偏到行首; cap = 满档
//   → acc[k·H + i] (腿计数, 新进者累建仓冲击; 本行先攒进 row 再按 max(腿样本数, cap) 折进加权和); cov[0] += 有秩格数, cov[1 + k] += 其中成本 k 有效
//   (与 h 无关, 每行一次)
inline void cost_row(const uint8_t *lc, const uint8_t *const *lag, int H, const Cost *c, int C, int A, double cap, CostAcc *acc, long long *cov) {
  CostLeg row[factor::stat::kMaxCost * factor::stat::kMaxHold][kLegs] = {};
  for (int i = 0; i < H; ++i)
    if (lag[i])
      for (int k = 0; k < C; ++k)
        ++acc[k * H + i].rows;
  for (int a = 0; a < A; ++a) {
    const int l = lc[a];
    if (l == kLcVoid)
      continue;
    ++cov[0];
    for (int k = 0; k < C; ++k)
      cov[1 + k] += c[k].m[a] != 0;
    if (l == kLcMid)
      continue;
    for (int i = 0; i < H; ++i) {
      if (!lag[i])
        continue;
      const bool ent = lag[i][a] != l;
      for (int k = 0; k < C; ++k) {
        if (!c[k].m[a])
          continue;
        CostLeg &g = row[k * H + i][l];
        ++g.n;
        if (ent)
          ++g.e, g.imp += static_cast<double>(h2f(c[k].imp[l][a]));
      }
    }
  }
  for (int j = 0; j < C * H; ++j)
    for (int l = 0; l < kLegs; ++l) {
      const CostLeg &r = row[j][l];
      CostLeg &g = acc[j].leg[l];
      const double den = factor::stat::book_den(static_cast<double>(r.n), cap);
      g.n += r.n, g.e += r.e, g.imp += r.imp;
      g.wimp += r.imp / den, g.we += static_cast<double>(r.e) / den;
    }
}

// label_row 的逐格扩展 (见文件头 row_stat): cell(x, ok, y) 每格一次, x = 本格 r16, ok = 格在 J, y[kLegs] = 两腿标签值
struct NoExt {
  void cell(unsigned, bool, const float *) {}
};

// 标签侧 (t → t+h): rx = 本行 r16, lb 已偏到行首, cap = 满档 → r 的 ric / mkt / grp / cnt / ls / ok. 调用方已排除段末尾部
template <class Ext>
inline void label_row(const uint16_t *rx, const Label &lb, int A, double cap, Row &r, Ext &ext) {
  unsigned long long s[kSums] = {};
  double sm[kLegs] = {}, gs[kGroups] = {};
  int gc[kGroups] = {};
  double sv_bot = 0.0; // 空腿 (底组) 的 Σsv (多腿的 Σlv 与两腿计数就是 gs / gc)
  for (int a = 0; a < A; ++a) {
    const bool ok = rx[a] != kRankNone && lb.ry[a] != kRankNone;
    factor::stat::pair_add(s, rx[a], lb.ry[a], ok);
    const float y[kLegs] = {h2f(lb.v[kLong][a]), h2f(lb.v[kShort][a])};
    for (int l = 0; l < kLegs; ++l)
      sm[l] += ok ? y[l] : 0.f;
    const int g = ok ? factor::stat::grp_of(rx[a]) : 0;
    gs[g] += ok ? y[kLong] : 0.f;
    gc[g] += ok;
    sv_bot += (ok && g == factor::stat::leg_group(kShort)) ? y[kShort] : 0.f;
    ext.cell(rx[a], ok, y);
  }
  constexpr int gl = factor::stat::leg_group(kLong), gb = factor::stat::leg_group(kShort);
  const LegSum leg[kLegs] = {{static_cast<double>(gc[gl]), gs[gl]}, {static_cast<double>(gc[gb]), sv_bot}};
  factor::stat::finish_row(s, sm, gs, gc, leg, cap, r);
}

// 一 (t, hold) 完整行: rlag = t−h 行 r16 (没有 → nullptr, 无 rAC); lb 已偏到行首; tail = 段末尾部 (tail_masked, 为真则 ext 不动)
template <class Ext>
inline Row row_stat(const uint16_t *rx, const uint16_t *rlag, const Label &lb, int A, double cap, bool tail, Ext &ext) {
  Row r;
  if (rlag)
    lag_row(rx, rlag, A, r);
  if (!tail)
    label_row(rx, lb, A, cap, r, ext);
  return r;
}

} // namespace factor::cpu::stat
