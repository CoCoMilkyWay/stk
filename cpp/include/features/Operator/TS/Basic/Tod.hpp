#pragma once

// =============================================================================
// Tod - 同分钟历史基准 z 值 (compute=onMinute; feature_list.md 1.8, 前 D 日版, D ∈ {5, 10}; 20 / 60 跨周版不做)
// =============================================================================
//   x_t = 本分钟成交额 / 成交量 (MinuteData 买 + 卖, 与 Flow 的 taker 口同源), μ_t / σ_t = 前 D 个交易日 同一分钟槽 的均值 / 样本标准差 (不含今天; 因果):
//     amt_todz_{D}d = (amt_t − μ_t) / σ_t      vol_todz_{D}d = (vol_t − μ_t) / σ_t
//   槽内有效样本 < 3 或 σ = 0 → 0 (z 的中性值 = 与历史均值一致; 不产 NaN, 下游多日序列无缺口). 无成交的分钟不 flush → 该槽当天缺样本 (位图标缺).
//   状态: [255 槽][D_MAX] 环 + 有效位图, 每日 reset 清掉被覆盖的一列; D=5 取环上最近 5 列 (位掩码).
//   智臾 调整后成交量相关性 / 日内持续异常交易量; 开源 029 TGD 基准.
// =============================================================================

#include "features/DataDefine.hpp"
#include "features/TimeIndex.hpp"
#include <cmath>
#include <cstdint>

class Tod {
  static constexpr size_t D_MAX = 10;
  static constexpr size_t D_SHORT = 5;
  static constexpr size_t SLOTS = TRADE_MINUTES_PER_DAY;
  static constexpr uint32_t MIN_N = 3;
  static_assert(D_MAX <= 16, "valid_ 位图 uint16_t");

public:
  enum Out : size_t { amt_todz_5d,
                      vol_todz_5d,
                      amt_todz_10d,
                      vol_todz_10d,
                      kCount };
  float y[kCount] = {};

  explicit Tod(const MinuteData &md) : md_(md) { update_short_mask(); }

  inline void compute() {
    const size_t slot = md_.l1_index;
    const float xa = md_.bid_amount.back() + md_.ask_amount.back();
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

  // 两遍算方差: 成交额 ~1e6..1e7, 单遍 Σx²−nμ² 在 fp32 下抵消到只剩舍入噪声, σ 假小 → z 上万
  static inline float zscore(const float (&ring)[D_MAX], uint16_t valid, float x) {
    float s = 0.0f;
    uint32_t n = 0;
    for (size_t i = 0; i < D_MAX; ++i)
      if (valid & (1u << i))
        s += ring[i], ++n;
    if (n < MIN_N)
      return 0.0f; // 槽内样本不足 (上市初期 / 稀疏槽): z 的中性值 = 0 (与历史均值一致), 不产 NaN
    const float nf = static_cast<float>(n);
    const float mean = s / nf;
    float ss = 0.0f;
    for (size_t i = 0; i < D_MAX; ++i)
      if (valid & (1u << i)) {
        const float d = ring[i] - mean;
        ss += d * d;
      }
    const float var = ss / (nf - 1.0f);
    return var > 0.0f ? (x - mean) / std::sqrt(var) : 0.0f; // σ = 0 (历史全同值): 无离散度可归一, 同样落中性 0
  }

  const MinuteData &md_;
  float ring_a_[SLOTS][D_MAX] = {};
  float ring_v_[SLOTS][D_MAX] = {};
  uint16_t valid_[SLOTS] = {};
  uint16_t short_mask_ = 0;
  size_t col_ = 0;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Tod(N) N(Tod, (Tod), (minute_data), onMinute)

#define TOD_ROWS(X, CAT1, D)                                                                                                                                                                                                                                                                             \
  X(amt_todz_##D##d, CAT1, AUTO, "Amount TOD Z " #D "d", "成交额同分钟z值" #D "日", "本分钟成交额对前" #D "日同分钟均值/标准差的z值(样本<3或σ=0→NaN)", R"(\frac{\mathrm{Amt}_t - \mu^{()" #D R"(d)}_{\mathrm{tod}(t)}}{\sigma^{()" #D R"(d)}_{\mathrm{tod}(t)}})", OP(Tod, amt_todz_##D##d, None, None)) \
  X(vol_todz_##D##d, CAT1, AUTO, "Volume TOD Z " #D "d", "成交量同分钟z值" #D "日", "本分钟成交量对前" #D "日同分钟均值/标准差的z值(样本<3或σ=0→NaN)", R"(\frac{\mathrm{Vol}_t - \mu^{()" #D R"(d)}_{\mathrm{tod}(t)}}{\sigma^{()" #D R"(d)}_{\mathrm{tod}(t)}})", OP(Tod, vol_todz_##D##d, None, None))

#define FIELDS_L1_Tod(X, CAT1) \
  TOD_ROWS(X, CAT1, 5)         \
  TOD_ROWS(X, CAT1, 10)
