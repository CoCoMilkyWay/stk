#pragma once

// =============================================================================
// Realized - 已实现测度族的秒级降频 (compute=onTaker, flush=onMinute; feature_list.md 1.0)
// =============================================================================
//   Δ 网格 (3s / 15s) 上按"前一笔价" (previous-tick) 采样成交价, 格收益 r_c = 1e4·ln(P_c / P_{c-1}) (基点);
//   无成交的格收益 = 0 (固定网格口径, 零收益进和式, 也打断 BPV/TPV 滞后链). 网格与分钟边界对齐 (60 | Δ).
//   分钟内可加统计量 (日级 = 因子层按行求和, 精确). 每个 Δ 同一口序 (RetMoments 7 口 + 网格独有 4 口):
//     rv / rv_up / rv_dn / rm3 / rm4 / r_max / r_min   收益幂和 + 极值 (math/distribution/RetMoments.hpp; 偏度 = rm3 / rv^{3/2}, 峰度 = rm4 / rv², 因子层做)
//     bpv     = Σ |r_c||r_{c-1}|                    tpv = Σ (|r_c||r_{c-1}||r_{c-2}|)^{2/3}   (滞后链跨分钟连续)
//     rv_bigup / rv_bigdn = Σ_{r>θ} r² / Σ_{r<-θ} r²,  θ = α·σ_Δ,  σ_Δ² = (π/2)·Σ_{前一日} bpv_Δ / N_Δ  (α = 4, N_Δ = 14400/Δ)
//               (原文用当日 IV 定阈, 非因果; 这里用前一日, 首日 θ 未定 → 大跳跃列落 0)
//   逐笔:
//     path_len = Σ |1e4·Δln p| (基点, Δln p 复用 TakerRet 节点, 与 Flow dlogp_taker_* / TradeSize 共享一次 log)
//   均价 (vwap / twap) 在 Flow; 盘口更新中间价的同族幂和在 RealizedMid.
//   fp16 落盘: 幂和用 Log Tf (基点量纲下 Σr² ~ 1e2..1e6), 极值 / 路径长 原值.
//   跨分钟状态: 网格 (p_prev / 滞后链 / last_cell); 未 flush 的无成交分钟并入下一有效分钟.
// =============================================================================

#include "features/DataDefine.hpp"
#include "features/TimeIndex.hpp"
#include "math/distribution/RetMoments.hpp"
#include <cmath>
#include <cstdint>

class Realized {
public:
  static constexpr float kBp = 1e4f; // 收益量纲: 基点 (fp16 下 Σr² 在 ratio 量纲会落进次正规区)

private:
  static constexpr float ALPHA = 4.0f;               // 跳跃阈值倍数
  static constexpr float BPV_TO_IV = 1.5707963f;     // μ₁⁻² = π/2
  static constexpr uint32_t CONT_SECONDS = 4 * 3600; // 连续竞价秒数 (阈值归一用)
  static constexpr uint32_t NO_CELL = 0xFFFFFFFFu;
  static constexpr size_t NG = RetMoments::kCount + 4; // 每 Δ 口数

  template <uint32_t DELTA>
  struct Grid {
    static constexpr uint32_t N_DAY = CONT_SECONDS / DELTA;
    // 跨分钟状态
    float p_prev = 0.0f;          // P_{c-1}: 最近已闭合格的收盘价 (0 = 尚无)
    float a1 = 0.0f, a2 = 0.0f;   // |r_{c-1}|, |r_{c-2}|
    float b1 = 0.0f, b2 = 0.0f;   // |r_{c-1}|^{2/3}, |r_{c-2}|^{2/3}
    uint32_t last_cell = NO_CELL; // 最近已闭合格 (含)
    // 跨日: 阈值
    float theta = 0.0f; // 0 = 无前日 IV (首日)
    float day_bpv = 0.0f;
    // 分钟累计
    RetMoments mom;
    float bpv = 0, tpv = 0, big_up = 0, big_dn = 0;

    inline void add(float r) {
      mom.add(r);
      const float r2 = r * r;
      const float ar = std::fabs(r);
      bpv += ar * a1;
      const float c = std::cbrt(r2); // |r|^{2/3}
      tpv += c * b1 * b2;
      big_up += r > theta ? r2 : 0.0f;
      big_dn += r < -theta ? r2 : 0.0f;
      a2 = a1, a1 = ar;
      b2 = b1, b1 = c;
    }
    // 闭合 (last_cell, upto]: 首格以 px 收 (对 p_prev 记收益), 其余无成交 → 零收益
    inline void close_upto(uint32_t upto, float px) {
      if (upto == NO_CELL || (last_cell != NO_CELL && upto <= last_cell))
        return;
      const uint32_t first = last_cell == NO_CELL ? upto : last_cell + 1;
      if (p_prev > 0.0f)
        add(kBp * std::log(px / p_prev));
      p_prev = px;
      const uint32_t n_zero = upto - first;
      if (n_zero >= 1) {
        mom.ext(0.0f); // 零收益格: 幂和不变, 极值计入 0
        mom.n += n_zero - 1;
        a2 = a1, a1 = 0.0f, b2 = b1, b1 = 0.0f;
        if (n_zero >= 2)
          a2 = 0.0f, b2 = 0.0f;
      }
      last_cell = upto;
    }
    inline void on_trade(uint32_t l0, float px_last) {
      const uint32_t k = l0 / DELTA;
      if (last_cell == NO_CELL) [[unlikely]]
        last_cell = k > 0 ? k - 1 : 0; // 首笔: 从本格开始待闭合 (k=0 只在盘前钳零的脏数据出现)
      else if (k - 1 > last_cell)
        close_upto(k - 1, px_last);
    }
    inline void flush_minute(uint32_t minute_end_l0, float px_last) {
      if (last_cell != NO_CELL)
        close_upto(minute_end_l0 / DELTA - 1, px_last);
      day_bpv += bpv;
    }
    inline void clear_minute() {
      mom.clear();
      bpv = tpv = big_up = big_dn = 0.0f;
    }
    inline void reset_day() {
      // 前一日 IV → 今日跳跃阈值 (基点)
      theta = day_bpv > 0.0f ? ALPHA * std::sqrt(BPV_TO_IV * day_bpv / static_cast<float>(N_DAY)) : 0.0f;
      day_bpv = 0.0f;
      p_prev = 0.0f;
      a1 = a2 = b1 = b2 = 0.0f;
      last_cell = NO_CELL;
      clear_minute();
    }
  };

public:
  // 布局: Δ 外层 × (RetMoments 7 口 + bpv, tpv, rv_bigup, rv_bigdn) 内层 —— y[g·NG + i]; 其后 path_len
  enum Out : size_t {
    rv_3s,
    rv_up_3s,
    rv_dn_3s,
    rm3_3s,
    rm4_3s,
    r_max_3s,
    r_min_3s,
    bpv_3s,
    tpv_3s,
    rv_bigup_3s,
    rv_bigdn_3s,
    rv_15s,
    rv_up_15s,
    rv_dn_15s,
    rm3_15s,
    rm4_15s,
    r_max_15s,
    r_min_15s,
    bpv_15s,
    tpv_15s,
    rv_bigup_15s,
    rv_bigdn_15s,
    path_len,
    kCount
  };
  static_assert(rv_15s == NG && path_len == 2 * NG);
  float y[kCount] = {};

  Realized(const TickData &td, const MinuteData &md, const Series &taker_dlogp) : td_(td), md_(md), taker_dlogp_(taker_dlogp) {}

  inline void compute() {
    const float p = td_.lob.price;
    if (p <= 0.0f) [[unlikely]]
      return;
    const uint32_t l0 = td_.l0_index;
    path_len_ += std::fabs(taker_dlogp_.back()); // TakerRet 同域已 flush; 首笔 = 0, 加零无影响
    g3_.on_trade(l0, px_last_ > 0.0f ? px_last_ : p);
    g15_.on_trade(l0, px_last_ > 0.0f ? px_last_ : p);
    px_last_ = p;
  }

  inline void flush() {
    const uint32_t end_l0 = static_cast<uint32_t>(L1_to_L0(md_.l1_index)) + 60u;
    g3_.flush_minute(end_l0, px_last_);
    g15_.flush_minute(end_l0, px_last_);
    write(g3_, rv_3s);
    write(g15_, rv_15s);
    y[path_len] = path_len_;
    g3_.clear_minute();
    g15_.clear_minute();
    path_len_ = 0.0f;
  }

  void reset() {
    g3_.reset_day();
    g15_.reset_day();
    px_last_ = path_len_ = 0.0f;
  }

private:
  template <uint32_t D>
  inline void write(const Grid<D> &g, size_t base) {
    g.mom.write(&y[base]); // n = 0 (分钟内无已闭合格) 时极值本就是 0 = 无变动观测
    y[base + RetMoments::kCount + 0] = g.bpv;
    y[base + RetMoments::kCount + 1] = g.tpv;
    // θ 未定 (首日 / 前日零波动) → 判不出大跳跃, 落 0 = 无大跳跃; 与 rv 族空分钟同样落 0 的约定一致, 不产 NaN
    const bool has_theta = g.theta > 0.0f;
    y[base + RetMoments::kCount + 2] = has_theta ? g.big_up : 0.0f;
    y[base + RetMoments::kCount + 3] = has_theta ? g.big_dn : 0.0f;
  }

  const TickData &td_;
  const MinuteData &md_;
  const Series &taker_dlogp_; // TakerRet 输出口 (同域 onTaker, 拓扑序在前, back() 即本笔值)
  Grid<3> g3_;
  Grid<15> g15_;
  float px_last_ = 0.0f;
  float path_len_ = 0.0f;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Realized(N) N(Realized, (Realized), (tick_data, minute_data, TakerRet.out()), onTaker, onMinute)

// 一个 Δ 网格的 11 行 (d = 网格 token, dd = 网格秒数字面)
#define REALIZED_GRID_ROWS(X, CAT1, d, dd)                                                                                                                                                                                                                                                                                                     \
  X(rv_##d, CAT1, AUTO, "Realized Variance " #dd "s", #dd "秒已实现方差", #dd "秒网格格收益(基点)平方分钟和; 日级=行求和", R"(\sum_{c \in \Delta t} r_c^2,\; r_c = 10^4 \ln\frac{P_c}{P_{c-1}},\; \Delta=)" #dd R"(\mathrm{s})", OP(Realized, rv_##d, Log, None))                                                                              \
  X(rv_up_##d, CAT1, AUTO, "Realized Upside Variance " #dd "s", #dd "秒上行已实现方差", #dd "秒网格正收益平方分钟和(基点²)", R"(\sum_{c \in \Delta t} r_c^2 \mathbf{1}[r_c>0],\; \Delta=)" #dd R"(\mathrm{s})", OP(Realized, rv_up_##d, Log, None))                                                                                            \
  X(rv_dn_##d, CAT1, AUTO, "Realized Downside Variance " #dd "s", #dd "秒下行已实现方差", #dd "秒网格负收益平方分钟和(基点²)", R"(\sum_{c \in \Delta t} r_c^2 \mathbf{1}[r_c<0],\; \Delta=)" #dd R"(\mathrm{s})", OP(Realized, rv_dn_##d, Log, None))                                                                                          \
  X(rm3_##d, CAT1, AUTO, "Realized Third Moment " #dd "s", #dd "秒已实现三阶矩", #dd "秒网格收益立方分钟和(基点³); 偏度=rm3/rv^1.5", R"(\sum_{c \in \Delta t} r_c^3,\; \Delta=)" #dd R"(\mathrm{s})", OP(Realized, rm3_##d, Log, None))                                                                                                        \
  X(rm4_##d, CAT1, AUTO, "Realized Fourth Moment " #dd "s", #dd "秒已实现四阶矩", #dd "秒网格收益四次方分钟和(基点⁴); 峰度=rm4/rv²", R"(\sum_{c \in \Delta t} r_c^4,\; \Delta=)" #dd R"(\mathrm{s})", OP(Realized, rm4_##d, Log, None))                                                                                                        \
  X(r_max_##d, CAT1, AUTO, "Max Grid Return " #dd "s", #dd "秒格收益最大值", "分钟内" #dd "秒网格格收益极大值(基点, 含零收益格)", R"(\max_{c \in \Delta t} r_c,\; \Delta=)" #dd R"(\mathrm{s})", OP(Realized, r_max_##d, None, None))                                                                                                          \
  X(r_min_##d, CAT1, AUTO, "Min Grid Return " #dd "s", #dd "秒格收益最小值", "分钟内" #dd "秒网格格收益极小值(基点, 含零收益格)", R"(\min_{c \in \Delta t} r_c,\; \Delta=)" #dd R"(\mathrm{s})", OP(Realized, r_min_##d, None, None))                                                                                                          \
  X(bpv_##d, CAT1, AUTO, "Bipower Variation " #dd "s", #dd "秒已实现双幂次变差", "相邻格|收益|乘积分钟和(基点²), 滞后链跨分钟连续; 跳跃=rv-π/2·bpv", R"(\sum_{c \in \Delta t} |r_c||r_{c-1}|,\; \Delta=)" #dd R"(\mathrm{s})", OP(Realized, bpv_##d, Log, None))                                                                               \
  X(tpv_##d, CAT1, AUTO, "Tripower Variation " #dd "s", #dd "秒已实现三幂次变差", "连续三格|收益|^(2/3)乘积分钟和(基点²)", R"(\sum_{c \in \Delta t} (|r_c||r_{c-1}||r_{c-2}|)^{2/3},\; \Delta=)" #dd R"(\mathrm{s})", OP(Realized, tpv_##d, Log, None))                                                                                        \
  X(rv_bigup_##d, CAT1, AUTO, "Big Upside Jump Variance " #dd "s", #dd "秒大上行跳跃方差", "超过阈值θ=4σ_Δ的正收益平方和(基点²), σ_Δ由前一日BPV推出; 首日θ未定→0", R"(\sum_{c \in \Delta t} r_c^2 \mathbf{1}[r_c > 4\sigma_\Delta],\; \sigma_\Delta^2 = \frac{\pi}{2}\frac{\sum_{D-1} bpv}{N_\Delta})", OP(Realized, rv_bigup_##d, Log, None)) \
  X(rv_bigdn_##d, CAT1, AUTO, "Big Downside Jump Variance " #dd "s", #dd "秒大下行跳跃方差", "低于阈值-θ的负收益平方和(基点²); 首日θ未定→0", R"(\sum_{c \in \Delta t} r_c^2 \mathbf{1}[r_c < -4\sigma_\Delta])", OP(Realized, rv_bigdn_##d, Log, None))

#define FIELDS_L1_Realized(X, CAT1)    \
  REALIZED_GRID_ROWS(X, CAT1, 3s, 3)   \
  REALIZED_GRID_ROWS(X, CAT1, 15s, 15) \
  X(path_len, CAT1, AUTO, "Trade Path Length", "逐笔路径长", "分钟内逐笔|对数价变动|之和(基点); 趋势占比=|ln(C/C₋₁)|/path_len", R"(\sum_{\tau \in \Delta t} |10^4 \ln\frac{P_\tau}{P_{\tau-1}}|)", OP(Realized, path_len, None, None))
