#pragma once

// =============================================================================
// Stat 行核的宿主版 (CPU; 契约 factor/Stat/Contract.hpp): 一行的 rank / 一 (t, hold) 的 Row
// =============================================================================
//   Stat/Cpu.hpp 与 Search/Cpu.hpp 两个 CPU 算子共用, 与 Stat/Dev.cuh 对仗 (同一组行核同名同参序, 一边宿主一边设备). 单线程, 无流程 / 无分配.
//     KeyX / KeyY 取键器 (行视图, 与 Dev.cuh 同名同形): 因子一行 (只看池内 g ∧ m 的格; g = m 即不看池) / 做多标签一行 (fp16 位) → 保序键 / kNoKey
//     rank_row    精确并列均秩: key(a) → LSD 基数排序 (位宽按 n 自适应: n ≤ 1024 用 8 位 × 4 趟, 否则 11 位 × 3 趟;
//                 任何正确的稳定排序给出同一 r16) → 等值段均秩 → out. n < 2 或全并列 → 整行 kRankNone. 标签行 = rank_row(KeyY)
//     rank_x_row  因子一行: CS rank_row(KeyX) / TS 直接量化 —— 口径分叉只在这里
//     row_stat    一 (t, hold) 完整行 = 换手侧 lag_row (t−h → t, 给了 lag 行才算) + 标签侧 label_row (t → t+h, 非段末尾部才算; 满仓 cap 进持仓口径);
//                 整数五和走契约 pair_add, 收尾 finish_row. label_row 一遍 A 轴: 整数五和 / 基准标签浮点和 / 分组和 (按契约【仓位感知】);
//                 逐格扩展 Ext (调用方专属的附加统计, 同一遍同一个 J; NoExt 编译期消掉, Inspect 尾部分层用)
//     cost_row    一行 t 的换手成本 (Δw), 每持有期两遍 A 轴 (端只数 → 权; 逐格差额) × C ≥ 1 个金额段 (契约端码 ec ∧ 标签有效位 ∧ 真持有) → CostAcc[C][H].
//                 Run (stat.costs) / Inspect 期限结构净线 / Portfolio 逐行成本共用, 不进 Row
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
using factor::stat::CostBook;
using factor::stat::Frame;
using factor::stat::kEcMid;
using factor::stat::kEnds;
using factor::stat::kGroups;
using factor::stat::kLegs;
using factor::stat::kLong;
using factor::stat::kMaxA;
using factor::stat::kNoKey;
using factor::stat::kRankNone;
using factor::stat::kShort;
using factor::stat::kSums;
using factor::stat::Label;
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

// 一行一个持有期的换手输入 (契约【换手成本】; 指针已偏到行首): m = 本行标签有效位 (nullptr = 本行尾部不开仓, 不攒); eo = 本行建仓可成交码;
// c = 本行的交接链 (c.nets(c.t) = 与行 t−h 的批交接, 只交易差额; 否则本批整建); settle = 本批平仓不与下一批交接 (建仓时一并记平仓量)
struct CostHold {
  const uint8_t *m = nullptr, *eo = nullptr;
  factor::stat::Chain c;
  bool settle = false;
};

// 换手成本: ec = 本行端码 (块形平面偏到行首, 交接链回看前行); ch[i] = 持有期 i 的输入; c[k] 已偏到行首; cap = 一端满仓 → acc[k·H + i].
//   端成员 = 端码 ∧ m ∧ 真持有 (契约 held, 与 Row 的端组同一集合); 两批各端只数 → 每只权 1 / max(只数, cap);
//   逐格 Δ = 本批权 − 前一批权: Δ⁺ 增仓 (付买入冲击; 本格不可买 (续持者封板) → 加不了仓, 记 0), Δ⁻ 减仓.
//   固定段 (c[k].flat()) 无买入冲击, 增仓量记进固定量 (契约【换手成本】)
inline void cost_row(const uint8_t *ec, const CostHold *ch, int H, const Cost *c, int C, int A, double cap, CostAcc *acc) {
  assert(C >= 1 && C <= factor::stat::kMaxCost);
  for (int i = 0; i < H; ++i) {
    const CostHold &q = ch[i];
    if (!q.m)
      continue;
    const bool net = q.c.nets(q.c.t);
    factor::stat::Chain pc = q.c; // 前一批 (行 t − h) 的链
    pc.t -= q.c.h;
    const ptrdiff_t back = -static_cast<ptrdiff_t>(q.c.h) * A;
    // 偏 o 行的格 a 在链 ch 上的端 (−1 = 不是真持有的端成员)
    const auto end_at = [&](const factor::stat::Chain &cn, ptrdiff_t o, int a) -> int8_t {
      const ptrdiff_t j = o + a;
      const uint8_t x = ec[j];
      if (x >= kEcMid || !q.m[j])
        return -1;
      return factor::stat::held(cn, ec + j, q.m + j, q.eo + j, [x](uint8_t v) { return v == x; }) ? static_cast<int8_t>(x) : -1;
    };
    int8_t en[kMaxA], pen[kMaxA];
    int n[kEnds] = {}, pn[kEnds] = {};
    for (int a = 0; a < A; ++a) {
      en[a] = end_at(q.c, 0, a);
      pen[a] = net ? end_at(pc, back, a) : -1;
      if (en[a] >= 0)
        ++n[en[a]];
      if (pen[a] >= 0)
        ++pn[pen[a]];
    }
    double w[kEnds], pw[kEnds];
    for (int e = 0; e < kEnds; ++e) {
      w[e] = 1.0 / factor::stat::book_den(n[e], cap);
      pw[e] = 1.0 / factor::stat::book_den(pn[e], cap);
    }
    double imp[factor::stat::kMaxCost][kEnds] = {}, buy[kEnds] = {}, sell[kEnds] = {};
    for (int a = 0; a < A; ++a) {
      const int x = en[a], px = pen[a];
      if (x < 0 && px < 0)
        continue;
      for (int e = 0; e < kEnds; ++e) {
        double d = (x == e ? w[e] : 0.0) - (px == e ? pw[e] : 0.0);
        if (d > 0.0 && !(q.eo[a] & factor::stat::kEoBuy))
          d = 0.0;
        if (d > 0.0) {
          buy[e] += d;
          for (int k = 0; k < C; ++k)
            if (!c[k].flat())
              imp[k][e] += d * static_cast<double>(h2f(c[k].imp[a]));
        } else {
          sell[e] -= d;
        }
      }
    }
    for (int k = 0; k < C; ++k) {
      CostAcc &o = acc[k * H + i];
      ++o.rows;
      for (int e = 0; e < kEnds; ++e) {
        CostBook &b = o.book[e];
        const double we = n[e] * w[e], out = sell[e] + (q.settle ? we : 0.0);
        b.imp += imp[k][e], b.buy += buy[e], b.sell += out, b.fix += out + (c[k].flat() ? buy[e] : 0.0);
        b.w += we, b.n += n[e];
      }
    }
  }
}

// label_row 的逐格扩展 (见文件头 row_stat): cell(cl) 每个 J 内的格一次; cl = 本格的交接链视图 (code = rx, *cl.code = 本格 r16, cl.y = 两侧标签),
// 扩展按自己的成员判据问 cl.held / cl.continues / cl.ret (契约【仓位感知】)
using RxCell = factor::stat::Cell<uint16_t>;
struct NoExt {
  void cell(const RxCell &) {}
};

// 标签侧 (t → t+h): rx = 本行 r16, lb 已偏到行首 (两者交接链回看 / 前看同块其他行), c = 本行交接链, cap = 满仓
//   → r 的 ric / mkt / grp / cnt / ok. ric / mkt 在 J 上 (与可成交无关); 组只收真持有的格, 收益按续持取名义值 (契约【仓位感知】).
//   调用方已排除段末尾部
template <class Ext>
inline void label_row(const uint16_t *rx, const Label &lb, const factor::stat::Chain &c, double cap, Row &r, Ext &ext) {
  unsigned long long s[kSums] = {};
  double sm = 0.0, gs[kGroups] = {};
  int gc[kGroups] = {};
  for (int a = 0; a < c.A; ++a) {
    const bool ok = rx[a] != kRankNone && lb.ry[a] != kRankNone;
    factor::stat::pair_add(s, rx[a], lb.ry[a], ok);
    const float y[kLegs] = {h2f(lb.v[kLong][a]), h2f(lb.v[kShort][a])};
    sm += ok ? y[kLong] : 0.f;
    if (!ok)
      continue;
    const int g = factor::stat::grp_of(rx[a]);
    const auto in = [g](uint16_t x) { return x != kRankNone && factor::stat::grp_of(x) == g; };
    const RxCell cl{c, rx + a, lb.m + a, lb.eo + a, y};
    if (cl.held(in)) {
      gs[g] += cl.ret(cl.continues(in));
      ++gc[g];
    }
    ext.cell(cl);
  }
  factor::stat::finish_row(s, sm, gs, gc, cap, r);
}

// 一 (t, hold) 完整行: rlag = t−h 行 r16 (没有 → nullptr, 无 rAC); lb 已偏到行首; c = 本行交接链; tail = 段末尾部 (tail_masked, 为真则 ext 不动)
template <class Ext>
inline Row row_stat(const uint16_t *rx, const uint16_t *rlag, const Label &lb, const factor::stat::Chain &c, double cap, bool tail, Ext &ext) {
  Row r;
  if (rlag)
    lag_row(rx, rlag, c.A, r);
  if (!tail)
    label_row(rx, lb, c, cap, r, ext);
  return r;
}

} // namespace factor::cpu::stat
