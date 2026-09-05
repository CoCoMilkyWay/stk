#pragma once

// =============================================================================
// CTR (Cumulative Trade Ratio) - 开盘至今的累计成交统计
// =============================================================================
//   cc_r = |O^{T,CA}| / |O^T|                                     (连续竞价成交占比)
//   ctr_xl/l/m/s = Σ|O^{T,c}| / Σ|O^T|                            (按大小单分类的累计成交占比)
//   cnbi = (Σ|O^{T,B}| - Σ|O^{T,A}|) / (Σ|O^{T,B}| + Σ|O^{T,A}|)  (累计净买入比率)
//   cnbi_xl/l/m/s = N^c / Σ|N^c|                                  (按大小单的净买入贡献)
//   cnbi_am/pm = 早盘/尾盘净买入比率
//   大小单阈值: S(<P50), M(P50-P80), L(P80-P95), XL(>=P95), KLL Sketch 滚动 4 周历史, reset() 时更新
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "features/DataDefine.hpp"
#include "math/distribution/KLLcache.hpp"
#include <algorithm>
#include <cmath>
#include <vector>

// compute: 每笔订单时累计 (内部过滤非TAKER), flush: 分钟末结算
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
  enum Out : size_t { cc_r,
                      ctr_xl,
                      ctr_l,
                      ctr_m,
                      ctr_s,
                      cnbi,
                      cnbi_xl,
                      cnbi_l,
                      cnbi_m,
                      cnbi_s,
                      cnbi_am,
                      cnbi_pm,
                      kCount };
  float y[kCount] = {};

  explicit CTR(TickData &td) : td_(td), current_kll_(KLL_K, KLL_RECON) {
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

  // 分钟末结算到 y
  inline void flush() {
    // 0. 缓冲的成交金额批量写入当前周KLL
    if (!amt_buffer_.empty()) {
      current_kll_.addBatch(amt_buffer_);
      amt_buffer_.clear();
    }

    // 1. cc_r：连续竞价成交 / 总成交
    y[cc_r] = cum_total_ > 1e-6f ? cum_ca_ / cum_total_ : 0.0f;

    // 2. ctr_*：各类大小单的累计成交占比（小单由总量推算）
    const float inv_total = cum_total_ > 1e-6f ? 1.0f / cum_total_ : 0.0f;
    y[ctr_xl] = cum_xl_ * inv_total;
    y[ctr_l] = cum_l_ * inv_total;
    y[ctr_m] = cum_m_ * inv_total;
    y[ctr_s] = std::max(0.0f, 1.0f - y[ctr_xl] - y[ctr_l] - y[ctr_m]);

    // 3. cnbi：累计净买入比率
    float sum_bs = cum_buy_ + cum_sell_;
    y[cnbi] = sum_bs > 1e-6f ? (cum_buy_ - cum_sell_) / sum_bs : 0.0f;

    // 4. cnbi_*：各类大小单的净买入贡献度（小单由总净买入推算）
    const float net_s = (cum_buy_ - cum_sell_) - net_xl_ - net_l_ - net_m_;
    float sum_abs_net = std::abs(net_xl_) + std::abs(net_l_) + std::abs(net_m_) + std::abs(net_s);
    const float inv_abs = sum_abs_net > 1e-6f ? 1.0f / sum_abs_net : 0.0f;
    y[cnbi_xl] = net_xl_ * inv_abs;
    y[cnbi_l] = net_l_ * inv_abs;
    y[cnbi_m] = net_m_ * inv_abs;
    y[cnbi_s] = net_s * inv_abs;

    // 5. cnbi_am/pm：早盘/尾盘净买入比率
    float sum_am = cum_am_buy_ + cum_am_sell_;
    float sum_pm = cum_pm_buy_ + cum_pm_sell_;
    y[cnbi_am] = sum_am > 1e-6f ? (cum_am_buy_ - cum_am_sell_) / sum_am : 0.0f;
    y[cnbi_pm] = sum_pm > 1e-6f ? (cum_pm_buy_ - cum_pm_sell_) / sum_pm : 0.0f;
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

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Ctr(N) N(Ctr, (CTR), (tick_data), onTick, onMinute)

#define FIELDS_L1_Ctr(X, CAT1)                                                                                                                                                                                                                                                                              \
  X(cc_r, CAT1, RATIO, NONE, "CC Trade Ratio", "连续竞价成交占比", "连续竞价成交额占全天成交额的比例(降频)", R"(\frac{|O_t^{T,\mathrm{CA}}|}{|O_t^{T}|})", OP(Ctr, cc_r))                                                                                                                                   \
  X(ctr_xl, CAT1, RATIO, NONE, "Cumulative XL Trade Ratio", "特大单累计成交占比", "从开盘到t时刻,特大单成交额占总成交额比例(降频)", R"(\frac{\sum_{\tau=t_0}^{t}(|O_{\tau}^{T,B,\mathrm{XL}}|+|O_{\tau}^{T,A,\mathrm{XL}}|)}{\sum_{\tau=t_0}^{t}(|O_{\tau}^{T,B}|+|O_{\tau}^{T,A}|)})", OP(Ctr, ctr_xl))    \
  X(ctr_l, CAT1, RATIO, NONE, "Cumulative L Trade Ratio", "大单累计成交占比", "从开盘到t时刻,大单成交额占总成交额比例(降频)", R"(\frac{\sum_{\tau=t_0}^{t}(|O_{\tau}^{T,B,\mathrm{L}}|+|O_{\tau}^{T,A,\mathrm{L}}|)}{\sum_{\tau=t_0}^{t}(|O_{\tau}^{T,B}|+|O_{\tau}^{T,A}|)})", OP(Ctr, ctr_l))             \
  X(ctr_m, CAT1, RATIO, NONE, "Cumulative M Trade Ratio", "中单累计成交占比", "从开盘到t时刻,中单成交额占总成交额比例(降频)", R"(\frac{\sum_{\tau=t_0}^{t}(|O_{\tau}^{T,B,\mathrm{M}}|+|O_{\tau}^{T,A,\mathrm{M}}|)}{\sum_{\tau=t_0}^{t}(|O_{\tau}^{T,B}|+|O_{\tau}^{T,A}|)})", OP(Ctr, ctr_m))             \
  X(ctr_s, CAT1, RATIO, NONE, "Cumulative S Trade Ratio", "小单累计成交占比", "从开盘到t时刻,小单成交额占总成交额比例(降频)", R"(\frac{\sum_{\tau=t_0}^{t}(|O_{\tau}^{T,B,\mathrm{S}}|+|O_{\tau}^{T,A,\mathrm{S}}|)}{\sum_{\tau=t_0}^{t}(|O_{\tau}^{T,B}|+|O_{\tau}^{T,A}|)})", OP(Ctr, ctr_s))             \
  X(cnbi, CAT1, RATIO, NONE, "Cumulative Net Buy Ratio", "累计净买入比率", "从开盘到t,主动买入成交额减主动卖出成交额(降频)", R"(\frac{\sum_{\tau=t_0}^{t}(|O_{\tau}^{T,B}|-|O_{\tau}^{T,A}|)}{\sum_{\tau=t_0}^{t}(|O_{\tau}^{T,B}|+|O_{\tau}^{T,A}|)})", OP(Ctr, cnbi))                                     \
  X(cnbi_xl, CAT1, RATIO, NONE, "Cumulative Net Buy Ratio XL", "特大单累计净买入比率", "特大单对累计净买入失衡的贡献(降频)", R"(\frac{N_t^{\mathrm{XL}}}{\sum_{c}|N_t^{c}|}, \quad N_t^{c}=\sum_{\tau=t_0}^{t}(|O_{\tau}^{T,B,c}|-|O_{\tau}^{T,A,c}|), \quad c\in\{\mathrm{XL,L,M,S}\})", OP(Ctr, cnbi_xl)) \
  X(cnbi_l, CAT1, RATIO, NONE, "Cumulative Net Buy Ratio L", "大单累计净买入比率", "大单对累计净买入失衡的贡献(降频)", R"(\frac{N_t^{\mathrm{L}}}{\sum_{c}|N_t^{c}|}, \quad N_t^{c}=\sum_{\tau=t_0}^{t}(|O_{\tau}^{T,B,c}|-|O_{\tau}^{T,A,c}|), \quad c\in\{\mathrm{XL,L,M,S}\})", OP(Ctr, cnbi_l))         \
  X(cnbi_m, CAT1, RATIO, NONE, "Cumulative Net Buy Ratio M", "中单累计净买入比率", "中单对累计净买入失衡的贡献(降频)", R"(\frac{N_t^{\mathrm{M}}}{\sum_{c}|N_t^{c}|}, \quad N_t^{c}=\sum_{\tau=t_0}^{t}(|O_{\tau}^{T,B,c}|-|O_{\tau}^{T,A,c}|), \quad c\in\{\mathrm{XL,L,M,S}\})", OP(Ctr, cnbi_m))         \
  X(cnbi_s, CAT1, RATIO, NONE, "Cumulative Net Buy Ratio S", "小单累计净买入比率", "小单对累计净买入失衡的贡献(降频)", R"(\frac{N_t^{\mathrm{S}}}{\sum_{c}|N_t^{c}|}, \quad N_t^{c}=\sum_{\tau=t_0}^{t}(|O_{\tau}^{T,B,c}|-|O_{\tau}^{T,A,c}|), \quad c\in\{\mathrm{XL,L,M,S}\})", OP(Ctr, cnbi_s))         \
  X(cnbi_am, CAT1, RATIO, NONE, "Net Buy Ratio AM", "早盘净买入比率", "上午有符号净主动成交额(降频)", R"(\frac{\sum_{\tau\in\mathcal{T}_{\mathrm{AM}}}(|O_{\tau}^{T,B}|-|O_{\tau}^{T,A}|)}{\sum_{\tau\in\mathcal{T}_{\mathrm{AM}}}(|O_{\tau}^{T,B}|+|O_{\tau}^{T,A}|)})", OP(Ctr, cnbi_am))                 \
  X(cnbi_pm, CAT1, RATIO, NONE, "Net Buy Ratio PM", "尾盘净买入比率", "下午有符号净主动成交额(降频)", R"(\frac{\sum_{\tau\in\mathcal{T}_{\mathrm{PM}}}(|O_{\tau}^{T,B}|-|O_{\tau}^{T,A}|)}{\sum_{\tau\in\mathcal{T}_{\mathrm{PM}}}(|O_{\tau}^{T,B}|+|O_{\tau}^{T,A}|)})", OP(Ctr, cnbi_pm))
