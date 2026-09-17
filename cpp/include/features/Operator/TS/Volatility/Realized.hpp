#pragma once

// =============================================================================
// Realized - 已实现测度族的秒级降频 (compute=onTaker, flush=onMinute; feature_list.md 1.0)
// =============================================================================
//   Δ 网格 (3s / 15s) 上按"前一笔价" (previous-tick) 采样成交价, 格收益 r_c = 1e4·ln(P_c / P_{c-1}) (基点);
//   无成交的格收益 = 0 (固定网格口径, 零收益进和式, 也打断 BPV/TPV 滞后链). 网格与分钟边界对齐 (60 | Δ).
//   分钟内可加统计量 (日级 = 因子层按行求和, 精确):
//     rv      = Σ r²           rv_up / rv_dn = Σ_{r>0} r² / Σ_{r<0} r²
//     rm3/rm4 = Σ r³ / Σ r⁴    (偏度 = rm3 / rv^{3/2}, 峰度 = rm4 / rv², 因子层做)
//     bpv     = Σ |r_c||r_{c-1}|                    tpv = Σ (|r_c||r_{c-1}||r_{c-2}|)^{2/3}   (滞后链跨分钟连续)
//     rv_bigup / rv_bigdn = Σ_{r>θ} r² / Σ_{r<-θ} r²,  θ = α·σ_Δ,  σ_Δ² = (π/2)·Σ_{前一日} bpv_Δ / N_Δ  (α = 4, N_Δ = 14400/Δ)
//               (原文用当日 IV 定阈, 非因果; 这里用前一日, 首日 NaN)
//     r_max / r_min (3s 网格分钟内极值, 基点)
//   逐笔 / 时间加权:
//     path_len = Σ |1e4·Δln p| 逐笔 (基点);  twap = 分钟内成交价的时间加权均值 (元, 末笔持有到分钟末)
//   fp16 落盘: 幂和用 Log Tf (基点量纲下 Σr² ~ 1e2..1e6), 极值 / 路径长 / twap 原值.
//   跨分钟状态: 网格 (p_prev / 滞后链 / last_cell) 与 twap 持有价; 未 flush 的无成交分钟并入下一有效分钟.
// =============================================================================

#include "features/DataDefine.hpp"
#include "features/TimeIndex.hpp"
#include <algorithm>
#include <cmath>
#include <cstdint>

class Realized {
public:
  static constexpr float kBp = 1e4f; // 收益量纲: 基点 (fp16 下 Σr² 在 ratio 量纲会落进次正规区)
  // 事件时刻: 交易时段毫秒 (l0_index × 1000 + ms; 分钟边界 = 60000 整数倍, 午休不计时)
  static inline uint32_t tick_ms(const TickData &td) { return td.l0_index * 1000u + td.lob.millisecond * 10u; }

private:
  static constexpr float ALPHA = 4.0f;               // 跳跃阈值倍数
  static constexpr float BPV_TO_IV = 1.5707963f;     // μ₁⁻² = π/2
  static constexpr uint32_t CONT_SECONDS = 4 * 3600; // 连续竞价秒数 (阈值归一用)
  static constexpr uint32_t NO_CELL = 0xFFFFFFFFu;

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
    float rv = 0, rv_up = 0, rv_dn = 0, m3 = 0, m4 = 0, bpv = 0, tpv = 0, big_up = 0, big_dn = 0;
    float rmax = 0.0f, rmin = 0.0f;
    uint32_t n_cells = 0; // 分钟内已闭合格数 (极值有效性; 不用 ±inf 哨兵: fast-math TU)

    inline void ext(float r) {
      if (n_cells++ == 0)
        rmax = rmin = r;
      else
        rmax = std::max(rmax, r), rmin = std::min(rmin, r);
    }
    inline void add(float r) {
      const float r2 = r * r;
      const float ar = std::fabs(r);
      rv += r2;
      rv_up += r > 0.0f ? r2 : 0.0f;
      rv_dn += r < 0.0f ? r2 : 0.0f;
      m3 += r2 * r;
      m4 += r2 * r2;
      bpv += ar * a1;
      const float c = std::cbrt(r2); // |r|^{2/3}
      tpv += c * b1 * b2;
      big_up += r > theta ? r2 : 0.0f;
      big_dn += r < -theta ? r2 : 0.0f;
      ext(r);
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
        ext(0.0f);
        n_cells += n_zero - 1;
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
      rv = rv_up = rv_dn = m3 = m4 = bpv = tpv = big_up = big_dn = 0.0f;
      rmax = rmin = 0.0f;
      n_cells = 0;
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
  enum Out : size_t {
    rv_3s,
    rv_up_3s,
    rv_dn_3s,
    rm3_3s,
    rm4_3s,
    bpv_3s,
    tpv_3s,
    rv_bigup_3s,
    rv_bigdn_3s,
    r_max_3s,
    r_min_3s,
    rv_15s,
    rv_up_15s,
    rv_dn_15s,
    rm3_15s,
    rm4_15s,
    bpv_15s,
    tpv_15s,
    rv_bigup_15s,
    rv_bigdn_15s,
    path_len,
    twap,
    kCount
  };
  float y[kCount] = {};

  Realized(const TickData &td, const MinuteData &md) : td_(td), md_(md) {}

  inline void compute() {
    const float p = td_.lob.price;
    if (p <= 0.0f) [[unlikely]]
      return;
    const uint32_t l0 = td_.l0_index;
    const uint32_t t = tick_ms(td_);
    if (px_last_ > 0.0f) {
      path_len_ += std::fabs(kBp * std::log(p / px_last_));
      twap_acc_ += px_last_ * static_cast<float>(t > t_last_ ? t - t_last_ : 0u); // 哨兵秒内 ms 回绕 → 钳零 (同 Book)
    } else {
      t_start_ = t; // 当日首笔: 时间加权从这里起算
    }
    g3_.on_trade(l0, px_last_ > 0.0f ? px_last_ : p);
    g15_.on_trade(l0, px_last_ > 0.0f ? px_last_ : p);
    px_last_ = p;
    t_last_ = t;
  }

  inline void flush() {
    const uint32_t end_l0 = static_cast<uint32_t>(L1_to_L0(md_.l1_index)) + 60u;
    const uint32_t t_end = end_l0 * 1000u;
    g3_.flush_minute(end_l0, px_last_);
    g15_.flush_minute(end_l0, px_last_);

    write(g3_, rv_3s, true);
    write(g15_, rv_15s, false);

    y[path_len] = path_len_;
    // twap: 末笔持有到分钟末; 时段长 0 (收盘竞价整段映射同一秒) → NaN
    twap_acc_ += px_last_ * static_cast<float>(t_end > t_last_ ? t_end - t_last_ : 0u);
    const float span = static_cast<float>(t_end > t_start_ ? t_end - t_start_ : 0u);
    y[twap] = span > 0.0f ? twap_acc_ / span : kNaN;

    g3_.clear_minute();
    g15_.clear_minute();
    path_len_ = twap_acc_ = 0.0f;
    t_start_ = t_last_ = t_end;
  }

  void reset() {
    g3_.reset_day();
    g15_.reset_day();
    px_last_ = path_len_ = twap_acc_ = 0.0f;
    t_start_ = t_last_ = 0;
  }

private:
  template <uint32_t D>
  inline void write(const Grid<D> &g, size_t base, bool extremes) {
    const bool has_theta = g.theta > 0.0f;
    y[base + 0] = g.rv;
    y[base + 1] = g.rv_up;
    y[base + 2] = g.rv_dn;
    y[base + 3] = g.m3;
    y[base + 4] = g.m4;
    y[base + 5] = g.bpv;
    y[base + 6] = g.tpv;
    y[base + 7] = has_theta ? g.big_up : kNaN;
    y[base + 8] = has_theta ? g.big_dn : kNaN;
    if (extremes) {
      const bool any = g.n_cells > 0; // 分钟内无已闭合格 → NaN
      y[base + 9] = any ? g.rmax : kNaN;
      y[base + 10] = any ? g.rmin : kNaN;
    }
  }

  const TickData &td_;
  const MinuteData &md_;
  Grid<3> g3_;
  Grid<15> g15_;
  float px_last_ = 0.0f;
  float path_len_ = 0.0f;
  float twap_acc_ = 0.0f;
  uint32_t t_start_ = 0, t_last_ = 0;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Realized(N) N(Realized, (Realized), (tick_data, minute_data), onTaker, onMinute)

// 一个 Δ 网格的幂和族 (d = 网格 token, dd = 网格秒数字面)
#define REALIZED_GRID_ROWS(X, CAT1, d, dd)                                                                                                                                                                                                                                                                                                \
  X(rv_##d, CAT1, RAW, "Realized Variance " #dd "s", #dd "秒已实现方差", #dd "秒网格格收益(基点)平方分钟和; 日级=行求和", R"(\sum_{c \in \Delta t} r_c^2,\; r_c = 10^4 \ln\frac{P_c}{P_{c-1}},\; \Delta=)" #dd R"(\mathrm{s})", OP(Realized, rv_##d, Log, None))                                                                          \
  X(rv_up_##d, CAT1, RAW, "Realized Upside Variance " #dd "s", #dd "秒上行已实现方差", #dd "秒网格正收益平方分钟和(基点²)", R"(\sum_{c \in \Delta t} r_c^2 \mathbf{1}[r_c>0],\; \Delta=)" #dd R"(\mathrm{s})", OP(Realized, rv_up_##d, Log, None))                                                                                        \
  X(rv_dn_##d, CAT1, RAW, "Realized Downside Variance " #dd "s", #dd "秒下行已实现方差", #dd "秒网格负收益平方分钟和(基点²)", R"(\sum_{c \in \Delta t} r_c^2 \mathbf{1}[r_c<0],\; \Delta=)" #dd R"(\mathrm{s})", OP(Realized, rv_dn_##d, Log, None))                                                                                      \
  X(rm3_##d, CAT1, RAW, "Realized Third Moment " #dd "s", #dd "秒已实现三阶矩", #dd "秒网格收益立方分钟和(基点³); 偏度=rm3/rv^1.5", R"(\sum_{c \in \Delta t} r_c^3,\; \Delta=)" #dd R"(\mathrm{s})", OP(Realized, rm3_##d, Log, None))                                                                                                    \
  X(rm4_##d, CAT1, RAW, "Realized Fourth Moment " #dd "s", #dd "秒已实现四阶矩", #dd "秒网格收益四次方分钟和(基点⁴); 峰度=rm4/rv²", R"(\sum_{c \in \Delta t} r_c^4,\; \Delta=)" #dd R"(\mathrm{s})", OP(Realized, rm4_##d, Log, None))                                                                                                    \
  X(bpv_##d, CAT1, RAW, "Bipower Variation " #dd "s", #dd "秒已实现双幂次变差", "相邻格|收益|乘积分钟和(基点²), 滞后链跨分钟连续; 跳跃=rv-π/2·bpv", R"(\sum_{c \in \Delta t} |r_c||r_{c-1}|,\; \Delta=)" #dd R"(\mathrm{s})", OP(Realized, bpv_##d, Log, None))                                                                           \
  X(tpv_##d, CAT1, RAW, "Tripower Variation " #dd "s", #dd "秒已实现三幂次变差", "连续三格|收益|^(2/3)乘积分钟和(基点²)", R"(\sum_{c \in \Delta t} (|r_c||r_{c-1}||r_{c-2}|)^{2/3},\; \Delta=)" #dd R"(\mathrm{s})", OP(Realized, tpv_##d, Log, None))                                                                                    \
  X(rv_bigup_##d, CAT1, RAW, "Big Upside Jump Variance " #dd "s", #dd "秒大上行跳跃方差", "超过阈值θ=4σ_Δ的正收益平方和(基点²), σ_Δ由前一日BPV推出; 首日NaN", R"(\sum_{c \in \Delta t} r_c^2 \mathbf{1}[r_c > 4\sigma_\Delta],\; \sigma_\Delta^2 = \frac{\pi}{2}\frac{\sum_{D-1} bpv}{N_\Delta})", OP(Realized, rv_bigup_##d, Log, None)) \
  X(rv_bigdn_##d, CAT1, RAW, "Big Downside Jump Variance " #dd "s", #dd "秒大下行跳跃方差", "低于阈值-θ的负收益平方和(基点²); 首日NaN", R"(\sum_{c \in \Delta t} r_c^2 \mathbf{1}[r_c < -4\sigma_\Delta])", OP(Realized, rv_bigdn_##d, Log, None))

#define FIELDS_L1_Realized(X, CAT1)                                                                                                                                                                                    \
  REALIZED_GRID_ROWS(X, CAT1, 3s, 3)                                                                                                                                                                                   \
  X(r_max_3s, CAT1, RAW, "Max Grid Return 3s", "3秒格收益最大值", "分钟内3秒网格格收益极大值(基点, 含零收益格)", R"(\max_{c \in \Delta t} r_c,\; \Delta = 3\mathrm{s})", OP(Realized, r_max_3s, None, None))           \
  X(r_min_3s, CAT1, RAW, "Min Grid Return 3s", "3秒格收益最小值", "分钟内3秒网格格收益极小值(基点, 含零收益格)", R"(\min_{c \in \Delta t} r_c,\; \Delta = 3\mathrm{s})", OP(Realized, r_min_3s, None, None))           \
  REALIZED_GRID_ROWS(X, CAT1, 15s, 15)                                                                                                                                                                                 \
  X(path_len, CAT1, RAW, "Trade Path Length", "逐笔路径长", "分钟内逐笔|对数价变动|之和(基点); 趋势占比=|ret|/path_len", R"(\sum_{i \in \Delta t} |10^4 \ln\frac{p_i}{p_{i-1}}|)", OP(Realized, path_len, None, None)) \
  X(twap, CAT1, RAW, "TWAP", "时间加权成交价", "分钟内成交价时间加权均值(元, 末笔持有到分钟末)", R"(\frac{\sum_i p_i (t_{i+1}-t_i)}{\sum_i (t_{i+1}-t_i)})", OP(Realized, twap, None, None))
