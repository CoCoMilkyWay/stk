#pragma once

// =============================================================================
// CTR (Cumulative Trade Ratio) - 累计成交比率
// =============================================================================
// 计算从开盘到当前时刻的累计成交统计
//
// 【公式定义】
//   cc_r = |O^{T,CA}| / |O^T|                             (连续竞价成交占比)
//   ctr_xl/l/m/s = Σ|O^{T,c}| / Σ|O^T|                   (按大小单分类的累计成交占比)
//   cnbi = (Σ|O^{T,B}| - Σ|O^{T,A}|) / (Σ|O^{T,B}| + Σ|O^{T,A}|)  (累计净买入比率)
//   cnbi_xl/l/m/s = N^c / Σ|N^c|                          (按大小单的净买入贡献)
//   cnbi_am/pm = 早盘/尾盘净买入比率
//
// 【触发域】
//   compute: onTaker
//   flush:   onDepth (按秒输出)
//
// 【输入输出】
//   输入: TickData.lob.{order_dir, volume, price, market_state} (onTaker)
//   输出: cc_r, ctr_xl, ctr_l, ctr_m, ctr_s, cnbi, cnbi_xl, cnbi_l, cnbi_m, cnbi_s, cnbi_am, cnbi_pm (onDepth)
//
// 【备注】
//   - 大单分类动态分位数: S(<P50), M(P50-P80), L(P80-P95), XL(>=P95)
//   - 使用KLL Sketch滚动4周历史计算阈值
//   - 需要reset()更新跨天阈值
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"
#include "math/distribution/KLLcache.hpp"
#include <algorithm>
#include <cmath>
#include <vector>

// compute: 每笔订单时累计 (内部过滤非TAKER), flush: 按秒输出
class CTR {
  static constexpr size_t N_WEEKS = 4;       // 保留4周KLL历史
  static constexpr size_t DAYS_PER_WEEK = 5; // 每周5个交易日
  static constexpr size_t KLL_K = 128;       // KLL每层容量
  static constexpr size_t KLL_RECON = 64;    // KLL重建点数 (P80精度<1%足够)

  // 默认阈值（预热阶段无历史数据时使用, 万元）
  static constexpr float DEFAULT_XL = 100.0f; // 特大单 >= 100万
  static constexpr float DEFAULT_L = 20.0f;   // 大单 20-100万
  static constexpr float DEFAULT_M = 4.0f;    // 中单 4-20万

public:
  CTR(TickData &td,
      CBuffer<float, L2::BLEN> &cc_r,
      CBuffer<float, L2::BLEN> &ctr_xl,
      CBuffer<float, L2::BLEN> &ctr_l,
      CBuffer<float, L2::BLEN> &ctr_m,
      CBuffer<float, L2::BLEN> &ctr_s,
      CBuffer<float, L2::BLEN> &cnbi,
      CBuffer<float, L2::BLEN> &cnbi_xl,
      CBuffer<float, L2::BLEN> &cnbi_l,
      CBuffer<float, L2::BLEN> &cnbi_m,
      CBuffer<float, L2::BLEN> &cnbi_s,
      CBuffer<float, L2::BLEN> &cnbi_am,
      CBuffer<float, L2::BLEN> &cnbi_pm)
      : td_(td),
        cc_r_(cc_r), ctr_xl_(ctr_xl), ctr_l_(ctr_l), ctr_m_(ctr_m), ctr_s_(ctr_s),
        cnbi_(cnbi), cnbi_xl_(cnbi_xl), cnbi_l_(cnbi_l), cnbi_m_(cnbi_m), cnbi_s_(cnbi_s),
        cnbi_am_(cnbi_am), cnbi_pm_(cnbi_pm),
        current_kll_(KLL_K, KLL_RECON) {
    for (auto &w : weekly_kll_)
      w = KLLcache(KLL_K, KLL_RECON);
    amt_buffer_.reserve(1024);
  }

  inline void compute() {
    // 每笔订单时，只处理成交订单（TAKER）
    if (td_.lob.order_type != L2::OrderType::TAKER)
      return;

    // 计算成交金额（万元）
    const float amt = static_cast<float>(td_.lob.volume) * td_.lob.price / 10000.0f;
    const bool is_buy = (td_.lob.order_dir == L2::OrderDirection::BID); // 主动买入
    const auto mkt_state = td_.lob.market_state;

    // 1. 累计总成交金额和连续竞价成交金额
    cum_total_ += amt;
    if (mkt_state == L2::MarketState::CONTINUOUS_TRADING_MORNING ||
        mkt_state == L2::MarketState::CONTINUOUS_TRADING_AFTERNOON)
      cum_ca_ += amt;

    // 2. 按买卖方向累计成交金额
    if (is_buy)
      cum_buy_ += amt;
    else
      cum_sell_ += amt;

    // 3. 缓冲成交金额供KLL采样
    amt_buffer_.push_back(amt);

    // 4. 按大小单分类（动态分位数阈值，小单不累加，flush时推算）
    const float signed_amt = is_buy ? amt : -amt;
    if (amt >= threshold_xl_) { // 特大单 >= P95
      cum_xl_ += amt;
      net_xl_ += signed_amt;
    } else if (amt >= threshold_l_) { // 大单 P80-P95
      cum_l_ += amt;
      net_l_ += signed_amt;
    } else if (amt >= threshold_m_) { // 中单 P50-P80
      cum_m_ += amt;
      net_m_ += signed_amt;
    }
    // else: 小单 < P50, 由 cum_total - xl - l - m 推算

    // 5. 按时段分类（早盘/尾盘）
    if (mkt_state == L2::MarketState::CONTINUOUS_TRADING_MORNING) {
      // 早盘时段 (9:30-11:30)
      if (is_buy)
        cum_am_buy_ += amt;
      else
        cum_am_sell_ += amt;
    } else if (mkt_state == L2::MarketState::CONTINUOUS_TRADING_AFTERNOON) {
      // 尾盘时段 (13:00-14:57)
      if (is_buy)
        cum_pm_buy_ += amt;
      else
        cum_pm_sell_ += amt;
    }
  }

  // 每秒输出
  inline void flush() {
    // 0. 将缓冲的成交金额批量写入当前周KLL
    if (!amt_buffer_.empty()) {
      current_kll_.addBatch(amt_buffer_);
      amt_buffer_.clear();
    }

    // 1. cc_r：连续竞价成交占比 = 连续竞价成交 / 总成交
    cc_r_.push_back(cum_total_ > 1e-6f ? cum_ca_ / cum_total_ : 0.0f);

    // 2. ctr_*：各类大小单的累计成交占比（小单由总量推算）
    const float inv_total = cum_total_ > 1e-6f ? 1.0f / cum_total_ : 0.0f;
    const float r_xl = cum_xl_ * inv_total;
    const float r_l = cum_l_ * inv_total;
    const float r_m = cum_m_ * inv_total;
    ctr_xl_.push_back(r_xl);
    ctr_l_.push_back(r_l);
    ctr_m_.push_back(r_m);
    ctr_s_.push_back(std::max(0.0f, 1.0f - r_xl - r_l - r_m)); // 推算

    // 3. cnbi：累计净买入比率
    float sum_bs = cum_buy_ + cum_sell_;
    cnbi_.push_back(sum_bs > 1e-6f ? (cum_buy_ - cum_sell_) / sum_bs : 0.0f);

    // 4. cnbi_*：各类大小单的净买入贡献度（小单由总净买入推算）
    const float net_s = (cum_buy_ - cum_sell_) - net_xl_ - net_l_ - net_m_;
    float sum_abs_net = std::abs(net_xl_) + std::abs(net_l_) + std::abs(net_m_) + std::abs(net_s);
    const float inv_abs = sum_abs_net > 1e-6f ? 1.0f / sum_abs_net : 0.0f;
    cnbi_xl_.push_back(net_xl_ * inv_abs);
    cnbi_l_.push_back(net_l_ * inv_abs);
    cnbi_m_.push_back(net_m_ * inv_abs);
    cnbi_s_.push_back(net_s * inv_abs); // 推算

    // 5. cnbi_am/pm：早盘/尾盘净买入比率
    // 反映不同时段的资金流向差异
    float sum_am = cum_am_buy_ + cum_am_sell_;
    float sum_pm = cum_pm_buy_ + cum_pm_sell_;
    cnbi_am_.push_back(sum_am > 1e-6f ? (cum_am_buy_ - cum_am_sell_) / sum_am : 0.0f); // 早盘净买入
    cnbi_pm_.push_back(sum_pm > 1e-6f ? (cum_pm_buy_ - cum_pm_sell_) / sum_pm : 0.0f); // 尾盘净买入
  }

  // 跨天重置: 周轮换 + 重新计算阈值 + 重置日内累计
  void reset() {
    // 1. 周轮换（当前周满5天时，将current_kll_归档到历史）
    if (day_in_week_ >= DAYS_PER_WEEK) {
      weekly_kll_[write_idx_] = std::move(current_kll_);
      current_kll_ = KLLcache(KLL_K, KLL_RECON);
      write_idx_ = (write_idx_ + 1) % N_WEEKS;
      if (n_weeks_ < N_WEEKS)
        ++n_weeks_;
      day_in_week_ = 0;
    }

    // 2. 重新计算大小单阈值（各KLL分别查询分位数取均值）
    updateThresholds();

    // 3. 重置日内累计
    cum_total_ = cum_ca_ = 0.0f;
    cum_buy_ = cum_sell_ = 0.0f;
    cum_xl_ = cum_l_ = cum_m_ = 0.0f;
    net_xl_ = net_l_ = net_m_ = 0.0f;
    cum_am_buy_ = cum_am_sell_ = 0.0f;
    cum_pm_buy_ = cum_pm_sell_ = 0.0f;
    amt_buffer_.clear();

    ++day_in_week_;
  }

private:
  // 从ICDF曲线线性插值查询分位数值
  static float queryQuantileFromICDF(const KLLcache::LinePtr &icdf, float p) {
    float u0 = icdf.x[0], u1 = icdf.x[icdf.n - 1];
    if (u1 <= u0)
      return icdf.y[0];
    float t = (p - u0) / (u1 - u0) * static_cast<float>(icdf.n - 1);
    t = std::clamp(t, 0.0f, static_cast<float>(icdf.n - 1));
    size_t i = static_cast<size_t>(t);
    if (i >= icdf.n - 1)
      return icdf.y[icdf.n - 1];
    float frac = t - static_cast<float>(i);
    return icdf.y[i] + frac * (icdf.y[i + 1] - icdf.y[i]);
  }

  // 分别查询所有可用KLL的分位数，取均值作为阈值
  void updateThresholds() {
    // 收集所有可用的ICDF（避免重复调用exportICDF）
    std::vector<KLLcache::LinePtr> icdfs;
    icdfs.reserve(N_WEEKS + 1);

    if (!current_kll_.empty())
      icdfs.push_back(current_kll_.exportICDF());

    for (size_t i = 0; i < n_weeks_; ++i) {
      size_t idx = (write_idx_ + N_WEEKS - 1 - i) % N_WEEKS;
      if (!weekly_kll_[idx].empty())
        icdfs.push_back(weekly_kll_[idx].exportICDF());
    }

    if (icdfs.empty()) {
      threshold_xl_ = DEFAULT_XL;
      threshold_l_ = DEFAULT_L;
      threshold_m_ = DEFAULT_M;
      return;
    }

    // 对每个ICDF查询3个分位数，累加求均值
    float sum_m = 0.0f, sum_l = 0.0f, sum_xl = 0.0f;
    for (const auto &icdf : icdfs) {
      sum_m += queryQuantileFromICDF(icdf, 0.50f);
      sum_l += queryQuantileFromICDF(icdf, 0.80f);
      sum_xl += queryQuantileFromICDF(icdf, 0.95f);
    }

    float inv = 1.0f / static_cast<float>(icdfs.size());
    threshold_m_ = sum_m * inv;
    threshold_l_ = sum_l * inv;
    threshold_xl_ = sum_xl * inv;
  }

  TickData &td_;

  // 输出 CBuffer
  CBuffer<float, L2::BLEN> &cc_r_, &ctr_xl_, &ctr_l_, &ctr_m_, &ctr_s_;
  CBuffer<float, L2::BLEN> &cnbi_, &cnbi_xl_, &cnbi_l_, &cnbi_m_, &cnbi_s_;
  CBuffer<float, L2::BLEN> &cnbi_am_, &cnbi_pm_;

  // 动态阈值（万元, 每日开盘前从KLL历史计算）
  float threshold_xl_ = DEFAULT_XL;
  float threshold_l_ = DEFAULT_L;
  float threshold_m_ = DEFAULT_M;

  // KLL分布缓存（动态大小单阈值）
  KLLcache current_kll_;         // 当前周KLL（累积中）
  KLLcache weekly_kll_[N_WEEKS]; // 历史周KLL (circular buffer)
  size_t write_idx_ = 0;         // circular buffer下一个写入位置
  size_t n_weeks_ = 0;           // 已有的完整周数 (0..N_WEEKS)
  size_t day_in_week_ = 0;       // 当前周内已过天数

  // 秒内成交金额缓冲（批量写入KLL）
  std::vector<float> amt_buffer_;

  // 日内累计统计
  float cum_total_ = 0.0f, cum_ca_ = 0.0f;
  float cum_buy_ = 0.0f, cum_sell_ = 0.0f;
  float cum_xl_ = 0.0f, cum_l_ = 0.0f, cum_m_ = 0.0f;
  float net_xl_ = 0.0f, net_l_ = 0.0f, net_m_ = 0.0f;
  float cum_am_buy_ = 0.0f, cum_am_sell_ = 0.0f;
  float cum_pm_buy_ = 0.0f, cum_pm_sell_ = 0.0f;
};
