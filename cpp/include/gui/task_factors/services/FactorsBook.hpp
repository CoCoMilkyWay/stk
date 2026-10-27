// FactorsBook — 净值引擎 (Stat 契约【净值】; Inspect 分层 / Portfolio 因子线 / 池篮子 / 池整体线同一份): 装载层 (FactorsLoad) 之上.
//   逐行批收益 RowLines (落线探针 / 篮子写) → fold_book (每批资金 1/h̄, 收益在平仓行实现 / 成本在建仓行即付; 按 stride 求和) → 期收益 BookLines
//   → compound 净值 (UI). 池篮子 baskets (日层: 指数 / 估值 / 换手, 吃 Y 的门控 + T+1C 标签与 B 的原料); pool_line (池整体等权线, 任意层, 只吃 Y).
#pragma once

#include "features/Method/Fundamental.hpp"           // fund::SW2021_L1_COUNT
#include "gui/task_factors/services/FactorsLoad.hpp" // YPlanes / BPlanes
#include "misc/exec.hpp"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace GUI::Factors {

// 期收益表 (fold_book 出): [n][P], 线 i 点 p
struct BookLines {
  int n = 0, P = 0; // 线数 / 每线点数 (= T / stride)
  std::vector<float> v;
  const float *at(int i) const { return v.data() + static_cast<size_t>(i) * static_cast<size_t>(P); }
  // 线 [i0, i0 + m) 拷成一张
  BookLines slice(int i0, int m) const {
    assert(i0 >= 0 && m >= 0 && i0 + m <= n);
    BookLines o;
    o.n = m, o.P = P;
    o.v.assign(at(i0), at(i0) + static_cast<size_t>(m) * static_cast<size_t>(P));
    return o;
  }
};
// 逐行批收益 (fold_book 入): x [n][T] = 线 i 行 t 这一批的收益 (占这批资金; 不 ok 记 0); hold[i] = 线 i 的持有期键 (各线可不同: 多持有期一次折完);
// at_exit[i]: 1 = 收益 (平仓行 hold_exit_row 实现, 平仓越过 T 的批不计) / 0 = 成本 (建仓行即付)
struct RowLines {
  int n = 0, T = 0;
  std::vector<int> hold;        // [n]
  std::vector<uint8_t> at_exit; // [n]
  std::vector<float> x;         // [n][T]
  void reset(int n_, int T_) {
    n = n_, T = T_;
    hold.assign(static_cast<size_t>(n), 0);
    at_exit.assign(static_cast<size_t>(n), 1);
    x.assign(static_cast<size_t>(n) * static_cast<size_t>(T), 0.f);
  }
  float *at(int i) { return x.data() + static_cast<size_t>(i) * static_cast<size_t>(T); }
};
// 线 i 行 t 的 x / h̄ (h̄ = hold_span) 记到实现行 (at_exit ? 平仓行 : t) → 每 stride 行求和一点 (stride 1 = 逐行 / S = 日). 线间并行
BookLines fold_book(misc::exec::ForkJoin &ex, int seg, const RowLines &r, int stride);
// 期收益 → 净值 (起点 1 不含): put(p, Π_{q ≤ p} (1 + r_q)). 一切净值 / 指数的复利只此一处 (UI: Inspect 分层 / Portfolio 组合 / 池指数)
template <class R, class Put>
void compound(const R *r, size_t n, Put &&put) {
  double l = 1.0;
  for (size_t p = 0; p < n; ++p) {
    l *= 1.0 + static_cast<double>(r[p]);
    put(p, l);
  }
}

inline constexpr int kBasketSeg = 1; // 篮子的层 (日层: 行 D 成员位 = D−1 收盘定的池, D 收盘换仓)
// 日层篮子 (baskets; 池整体与行业同形): 篮子 0 = 池整体, k = 1..kBaskets−1 = SW2021 一级行业 k (本池成分按行业切, 全A 池 = 申万一级;
//   行业未知的只进篮子 0); 线 index_line(k, w) = kWeights · k + w
struct Baskets {
  static constexpr size_t kBaskets = fund::SW2021_L1_COUNT;
  static constexpr int index_line(size_t k, int w) { return kWeights * static_cast<int>(k) + w; }
  BookLines index;                             // 指数的逐行收益 (stride 1; 净值 = compound, 调用方) = fold_book: 行 t 一批 = 成分 (行 t 在池 ∧ T+1C
                                               //   标签有效; ∧ 该行 ind_l1 = k) 做多标签按权平均 (等权 1 / 总市值加权 = 行 t 的 close_day × total_shares, 不复权;
                                               //   缺股本权 0), 平仓行实现 1/h̄. 日层 T+1C = C(D) → C(D+1) 复权, 顺延在标签里, 建仓 / 续持仓位感知 (Stat 契约)
  BookLines index_px;                          // 收盘价指数的逐行收益 (线序 / 权同 index; 不看可成交): 行 t 成分 = 上一有效 close 行 p 在池 (∧ 行 p ind_l1 = k),
                                               //   收益 = 后复权 close_t / close_p − 1, 权 = 等权 1 / 行 p 总市值; 无成分 → 0. 与 index 之差 = 可成交 / 顺延的拖累
  std::vector<float> val[kWeights][kPoolVals]; // [Weight][PoolVal] 估值: 行 t 成分 = 上一行在池 (∧ 本行 ind_l1 = k); 市值 = 本行收盘价 (停牌带上一有效价) × 本行总股本;
                                               //   个股收益率 = 分子 / 市值, 按权平均: 等权 = 算术平均 (= 等权组合每元的分子) / 加权 (权 = 市值) = 整体法 Σ分子 / Σ市值;
                                               //   分子: EP = 归母净利TTM, BP = 归母权益MRQ, SP = 营业总收入TTM, CP = 经营现金流TTM, DY = 分红,
                                               //   分红 = 本行 dy_ttm × 上一行收盘价 × 本行总股本 (= Fund 的分母口径还原近 365 日税前分红额);
                                               //   股本 / 财务 / 行业是盘后列, 日层 as-of 读到的本行 = 当日适用; 每口只收该口输入有限的成分 (亏损 / 负权益 / 负现金流照计为负);
                                               //   Σ权 ≤ 0 / 无成分 → NaN
  double turn[kWeights] = {};                  // [Weight] 篮子 0 的每行单边换手 ½ Σ|w_t − w̃_{t−1}| (w̃ = 上一成分行的权按本行收益漂移; 含再平衡 + 成分进出)
                                               //   沿成分行均值, 首行建仓不计
};
// 篮子 (日层; Y = 块的门控 + T+1C 标签, B = 同块的原料) → 指数 / 估值 / 换手 (Baskets 注)
Baskets baskets(misc::exec::ForkJoin &ex, const YPlanes &Y, const BPlanes &B);
// 池整体等权线 (= 篮子 0 等权指数; 线 i = 持有期 y.hd.k[i]): 行 t 一批 = 行 t 标签有效格做多标签的等权平均. 任意层, 只吃 Y
BookLines pool_line(misc::exec::ForkJoin &ex, const YPlanes &y);

} // namespace GUI::Factors
