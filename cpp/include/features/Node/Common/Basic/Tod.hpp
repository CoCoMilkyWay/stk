#pragma once

// =============================================================================
// TodT<Bars, SLOTS> - 同时段历史基准 z 值 (feature_list.md 1.8, 前 D 日版, D ∈ {5, 10}; 20 / 60 跨周版不做)
// =============================================================================
//   槽 = 本层行 (Bars::index): Tod = 分钟版 (compute=onMinute, 255 槽; Lob); Bar 档用 5 分钟箱版 (compute=on5Min, 49 槽, 读 min5_data).
//   下文"分钟"对箱版即"箱".
//   x_t = 本分钟成交额 / 成交量 (Bars 买 + 卖; Bar: bar1m 总量, 与 Trade 同源; Lob: 主动买 + 卖, 与 Flow 同源),
//   μ_t / σ_t = 前 D 个交易日 同一分钟槽 的均值 / 样本标准差 (不含今天; 因果):
//     amt_todz_{D}d = (amt_t − μ_t) / σ_t      vol_todz_{D}d = (vol_t − μ_t) / σ_t
//   槽内有效样本 < 3 或 σ = 0 → 0 (z 的中性值 = 与历史均值一致; 不产 NaN, 下游多日序列无缺口).
//   无成交的分钟 x = 0 照常入样 (每分钟都 flush): 基准含"没成交"这一事实, 竞价空窗槽历史全 0 → σ = 0 → 中性 0.
//   状态: [SLOTS 槽][D_MAX] 环 + 有效位图 (位 = 该日有数据, 无数据日 / 上市前为缺), 每日 reset 清掉被覆盖的一列; D=5 取环上最近 5 列 (位掩码).
//   智臾 调整后成交量相关性 / 日内持续异常交易量; 开源 029 TGD 基准.
//   两档共用类; 节点实例 / 落盘列见 Node/{Lob,Bar}/TS/Basic/Tod.hpp (Lob 列名 lob_*).
// =============================================================================

#include "features/DataDefine.hpp"
#include "features/TimeIndex.hpp"
#include <bit>
#include <cassert>
#include <cmath>
#include <cstdint>

template <class Bars, size_t SLOTS>
class TodT {
  static constexpr size_t D_MAX = 10;
  static constexpr size_t D_SHORT = 5;
  static constexpr uint32_t MIN_N = 3;
  static_assert(D_MAX <= 16, "valid_ 位图 uint16_t");

public:
  enum Out : size_t { amt_todz_5d,
                      vol_todz_5d,
                      amt_todz_10d,
                      vol_todz_10d,
                      kCount };
  float y[kCount] = {};

  explicit TodT(const Bars &md) : md_(md) { update_short_mask(); }

  inline void compute() {
    const size_t slot = md_.index;
    assert(slot < SLOTS && "Tod: 本层行超出槽数");
    const float xa = static_cast<float>(md_.bid_amount.back() + md_.ask_amount.back());
    const float xv = static_cast<float>(md_.bid_volume.back() + md_.ask_volume.back());
    const uint16_t v_all = valid_[slot];
    const uint16_t v_short = static_cast<uint16_t>(v_all & short_mask_);
    y[amt_todz_5d] = zscore(ring_a_[slot], v_short, xa);
    y[vol_todz_5d] = zscore(ring_v_[slot], v_short, xv);
    y[amt_todz_10d] = zscore(ring_a_[slot], v_all, xa);
    y[vol_todz_10d] = zscore(ring_v_[slot], v_all, xv);
    ring_a_[slot][col_] = xa;
    ring_v_[slot][col_] = xv;
    valid_[slot] |= static_cast<uint16_t>(1u << col_);
  }

  // 跨日: 推进列游标, 清掉即将被覆盖的一列 (D_MAX 日前的样本), 更新最近 D_SHORT 列掩码
  void reset() {
    col_ = (col_ + 1) % D_MAX;
    const uint16_t mask = static_cast<uint16_t>(~(1u << col_));
    for (size_t s = 0; s < SLOTS; ++s)
      valid_[s] &= mask;
    update_short_mask();
  }

private:
  // 最近 D_SHORT 个已写列 = col_-1 .. col_-D_SHORT (mod D_MAX); 今天的 col_ 在 compute 前尚未写入, 不在掩码内
  void update_short_mask() {
    short_mask_ = 0;
    for (size_t k = 1; k <= D_SHORT; ++k)
      short_mask_ |= static_cast<uint16_t>(1u << ((col_ + D_MAX - k) % D_MAX));
  }

  // 两遍算方差: 成交额 ~1e6..1e7, 单遍 Σx²−nμ² 在 fp32 下抵消到只剩舍入噪声, σ 假小 → z 上万.
  // 缺位按选择取 +0 (非乘掩码: 被清位的列仍留旧值), 累加序不变; 有效位随日轮换, 分支预测不住
  static inline float zscore(const float (&ring)[D_MAX], uint16_t valid, float x) {
    const uint32_t n = static_cast<uint32_t>(std::popcount(valid));
    if (n < MIN_N)
      return 0.0f; // 槽内样本不足 (上市初期 / 稀疏槽): z 的中性值 = 0 (与历史均值一致), 不产 NaN
    float s = 0.0f;
    for (size_t i = 0; i < D_MAX; ++i)
      s += (valid >> i & 1u) ? ring[i] : 0.0f;
    const float nf = static_cast<float>(n);
    const float mean = s / nf;
    float ss = 0.0f;
    for (size_t i = 0; i < D_MAX; ++i) {
      const float d = ring[i] - mean;
      ss += (valid >> i & 1u) ? d * d : 0.0f;
    }
    const float var = ss / (nf - 1.0f);
    return var > 0.0f ? (x - mean) / std::sqrt(var) : 0.0f; // σ = 0 (历史全同值): 无离散度可归一, 同样落中性 0
  }

  const Bars &md_;
  float ring_a_[SLOTS][D_MAX] = {};
  float ring_v_[SLOTS][D_MAX] = {};
  uint16_t valid_[SLOTS] = {};
  uint16_t short_mask_ = 0;
  size_t col_ = 0;
};
using Tod = TodT<MinuteData, TRADE_MINUTES_PER_DAY>;
