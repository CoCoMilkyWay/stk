// FactorsBook — 净值引擎 (Stat 契约【净值】; Inspect 分层 / Portfolio 因子线 / 池篮子 / 池整体线同一份): 装载层 (FactorsLoad) 之上.
//   逐行批收益 RowLines (落线探针 / 篮子写) → fold_book (每批资金 1/h̄, 收益在平仓行实现 / 成本在建仓行即付; 按 stride 求和) → 期收益 BookLines
//   → compound 净值 (UI). 池篮子 baskets (全部池一趟: 指数 / 估值 / 换手, 吃 B); pool_line (池整体等权线, 只吃 Y, 任意层). 两者同一个指数核.
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

// 篮子 (baskets; 全部池一趟, 各篮子同形): 篮子 b ∈ [0, count()): [0, kAllBaskets) = all 池整体 + SW2021 一级行业 k (all 内按行业切, 行业未知的只进篮子 0),
//   [kAllBaskets, count()) = 其余各池整体 (池 p → of_pool(p)). 成分判据 = 成员位 (BPlanes::in): 池整体 = 含该池位, 行业 k = 含 kAllPool 位 ∧ ind_l1 = k.
//   线 index_line(b, w) = kWeights · b + w
struct Baskets {
  static constexpr size_t kAllBaskets = fund::SW2021_L1_COUNT; // all 池的篮子数 (整体 + 行业)
  static size_t count() { return kAllBaskets + pool_count() - 1; }
  static size_t of_pool(PoolId p) { return p == kAllPool ? 0 : kAllBaskets + p - 1; }
  static constexpr int index_line(size_t b, int w) { return kWeights * static_cast<int>(b) + w; }
  BookLines index;                             // 指数的逐行收益 (stride 1; 净值 = compound, 调用方) = fold_book: 行 t 一批 = 成分 (行 t 成员位合判据 ∧ T+1C
                                               //   标签有效) 做多标签按权平均 (等权 1 / 总市值加权 = 行 t 的 close × total_shares, 不复权;
                                               //   缺股本权 0), 平仓行实现 1/h̄. T+1C = C(行) → C(D+1) 复权, 顺延在标签里, 建仓 / 续持仓位感知 (Stat 契约)
  BookLines index_px;                          // 收盘价指数的逐行收益 (线序 / 权同 index; 不看可成交): 行 t 成分 = 上一有效 close 行 p 合判据 (行业按行 p),
                                               //   收益 = 后复权 close_t / close_p − 1, 权 = 等权 1 / 行 p 总市值; 无成分 → 0. 与 index 之差 = 可成交 / 顺延的拖累
  std::vector<float> val[kWeights][kPoolVals]; // [Weight][PoolVal][b · T] 估值: 行 t 成分 = 上一行成员位合判据 (行业按本行); 市值 = 本行收盘价 (停牌带上一有效价) × 本行总股本;
                                               //   个股收益率 = 分子 / 市值, 按权平均: 等权 = 算术平均 (= 等权组合每元的分子) / 加权 (权 = 市值) = 整体法 Σ分子 / Σ市值;
                                               //   分子: EP = 归母净利TTM, BP = 归母权益MRQ, SP = 营业总收入TTM, CP = 经营现金流TTM, DY = 分红,
                                               //   分红 = 本行 dy_ttm × 前一交易日末行收盘价 × 本行总股本 (= Fund 的分母口径还原近 365 日税前分红额; 评估期首日无 → NaN);
                                               //   股本 / 财务 / 行业是日频盘后列, as-of 读到的本行 = 当日适用; 每口只收该口输入有限的成分 (亏损 / 负权益 / 负现金流照计为负);
                                               //   Σ权 ≤ 0 / 无成分 → NaN
  std::vector<double> turn[kWeights];          // [Weight][PoolId] 各池整体 (篮子 of_pool(p)) 的每行单边换手 ½ Σ|w_t − w̃_{t−1}| (w̃ = 上一成分行的权按本行收益漂移;
                                               //   含再平衡 + 成分进出) 沿成分行均值, 首行建仓不计
};
// 池篮子输入 B (load_b) → 全部篮子的指数 / 估值 / 换手 (Baskets 注)
Baskets baskets(misc::exec::ForkJoin &ex, const BPlanes &B);
// 池整体等权线 (= 篮子 0 等权指数; 线 i = 持有期 y.hd.k[i]): 行 t 一批 = 行 t 标签有效格做多标签的等权平均. 任意层, 只吃 Y
BookLines pool_line(misc::exec::ForkJoin &ex, const YPlanes &y);

} // namespace GUI::Factors
