#pragma once

// =============================================================================
// CTR (Cumulative Trade Ratio) - 累计成交比率
// =============================================================================
// 计算从开盘到当前时刻的累计成交统计
//   cc_r = |O^T,CA| / |O^T|                          (连续竞价成交占比)
//   ctr_xl/l/m/s = Σ|O^T,c| / Σ|O^T|                 (按大小单分类的累计成交占比)
//   cnbi = (Σ|O^T,B| - Σ|O^T,A|) / (Σ|O^T,B| + Σ|O^T,A|)  (累计净买入比率)
//   cnbi_xl/l/m/s = N^c / Σ|N^c|                      (按大小单的净买入贡献)
//   cnbi_am/pm = 早盘/尾盘净买入比率
//
// 大单分类 (以成交金额分):
//   XL (特大单): >= 100万
//   L  (大单):   20万 - 100万
//   M  (中单):   4万 - 20万
//   S  (小单):   < 4万
//
// 输入频率: PER_TAKER (每笔成交)
// 输出频率: per sec
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"
#include "features/FeaturesDefine.hpp"

// compute: 每笔订单时累计 (内部过滤非TAKER), flush: 按秒输出
class CTR {
  static constexpr float THRESHOLD_XL = 100.0f; // 特大单 >= 100万
  static constexpr float THRESHOLD_L = 20.0f;   // 大单 20-100万
  static constexpr float THRESHOLD_M = 4.0f;    // 中单 4-20万

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
        cnbi_am_(cnbi_am), cnbi_pm_(cnbi_pm) {}

  inline void compute() {
    // 每笔订单时，只处理成交订单（TAKER）
    if (td_.lob.order_type != L2::OrderType::TAKER) return;

    // 计算成交金额（万元）
    const float amt = static_cast<float>(td_.lob.volume) * td_.lob.price / 10000.0f;
    const bool is_buy = (td_.lob.order_dir == L2::OrderDirection::BID);  // 主动买入
    const bool is_continuous = (td_.l0_index >= MORNING_SECONDS / 60 * 60 || td_.l0_index >= 900); // 9:30后连续竞价

    // 1. 累计总成交金额和连续竞价成交金额
    cum_total_ += amt;
    if (is_continuous) cum_ca_ += amt;

    // 2. 按买卖方向累计成交金额
    if (is_buy) cum_buy_ += amt;
    else cum_sell_ += amt;

    // 3. 按大小单分类（以成交金额划分）
    float *cum_cat = nullptr;  // 该类别累计成交
    float *net_cat = nullptr;  // 该类别净买入
    if (amt >= THRESHOLD_XL) {         // 特大单 >= 100万
      cum_cat = &cum_xl_;
      net_cat = &net_xl_;
    } else if (amt >= THRESHOLD_L) {   // 大单 20-100万
      cum_cat = &cum_l_;
      net_cat = &net_l_;
    } else if (amt >= THRESHOLD_M) {   // 中单 4-20万
      cum_cat = &cum_m_;
      net_cat = &net_m_;
    } else {                            // 小单 < 4万
      cum_cat = &cum_s_;
      net_cat = &net_s_;
    }

    *cum_cat += amt;  // 累加该类别成交量
    *net_cat += is_buy ? amt : -amt;  // 累加净买入（买为正，卖为负）

    // 4. 按时段分类（早盘/尾盘）
    if (td_.l0_index < MORNING_SECONDS) {
      // 上午时段
      if (is_buy) cum_am_buy_ += amt;
      else cum_am_sell_ += amt;
    } else {
      // 下午时段
      if (is_buy) cum_pm_buy_ += amt;
      else cum_pm_sell_ += amt;
    }
  }

  // 每秒输出
  inline void flush() {
    // 1. cc_r：连续竞价成交占比 = 连续竞价成交 / 总成交
    cc_r_.push_back(cum_total_ > 1e-6f ? cum_ca_ / cum_total_ : 0.0f);

    // 2. ctr_*：各类大小单的累计成交占比
    // 反映不同规模资金的活跃度
    ctr_xl_.push_back(cum_total_ > 1e-6f ? cum_xl_ / cum_total_ : 0.0f);  // 特大单占比
    ctr_l_.push_back(cum_total_ > 1e-6f ? cum_l_ / cum_total_ : 0.0f);    // 大单占比
    ctr_m_.push_back(cum_total_ > 1e-6f ? cum_m_ / cum_total_ : 0.0f);    // 中单占比
    ctr_s_.push_back(cum_total_ > 1e-6f ? cum_s_ / cum_total_ : 0.0f);    // 小单占比

    // 3. cnbi：累计净买入比率 = (累计买入 - 累计卖出) / (累计买入 + 累计卖出)
    // 值域[-1,1]，正值表示资金净流入，负值表示资金净流出
    float sum_bs = cum_buy_ + cum_sell_;
    cnbi_.push_back(sum_bs > 1e-6f ? (cum_buy_ - cum_sell_) / sum_bs : 0.0f);

    // 4. cnbi_*：各类大小单的净买入贡献度
    // 计算各类别净买入占所有类别净买入绝对值和的比例
    // 反映哪种规模的资金在主导方向
    float sum_abs_net = std::abs(net_xl_) + std::abs(net_l_) + std::abs(net_m_) + std::abs(net_s_);
    cnbi_xl_.push_back(sum_abs_net > 1e-6f ? net_xl_ / sum_abs_net : 0.0f);  // 特大单贡献
    cnbi_l_.push_back(sum_abs_net > 1e-6f ? net_l_ / sum_abs_net : 0.0f);    // 大单贡献
    cnbi_m_.push_back(sum_abs_net > 1e-6f ? net_m_ / sum_abs_net : 0.0f);    // 中单贡献
    cnbi_s_.push_back(sum_abs_net > 1e-6f ? net_s_ / sum_abs_net : 0.0f);    // 小单贡献

    // 5. cnbi_am/pm：早盘/尾盘净买入比率
    // 反映不同时段的资金流向差异
    float sum_am = cum_am_buy_ + cum_am_sell_;
    float sum_pm = cum_pm_buy_ + cum_pm_sell_;
    cnbi_am_.push_back(sum_am > 1e-6f ? (cum_am_buy_ - cum_am_sell_) / sum_am : 0.0f);  // 早盘净买入
    cnbi_pm_.push_back(sum_pm > 1e-6f ? (cum_pm_buy_ - cum_pm_sell_) / sum_pm : 0.0f);  // 尾盘净买入
  }

  // 跨天重置
  void reset() {
    cum_total_ = cum_ca_ = 0.0f;
    cum_buy_ = cum_sell_ = 0.0f;
    cum_xl_ = cum_l_ = cum_m_ = cum_s_ = 0.0f;
    net_xl_ = net_l_ = net_m_ = net_s_ = 0.0f;
    cum_am_buy_ = cum_am_sell_ = 0.0f;
    cum_pm_buy_ = cum_pm_sell_ = 0.0f;
  }

private:
  TickData &td_;

  // 输出 CBuffer
  CBuffer<float, L2::BLEN> &cc_r_, &ctr_xl_, &ctr_l_, &ctr_m_, &ctr_s_;
  CBuffer<float, L2::BLEN> &cnbi_, &cnbi_xl_, &cnbi_l_, &cnbi_m_, &cnbi_s_;
  CBuffer<float, L2::BLEN> &cnbi_am_, &cnbi_pm_;

  // 累计统计
  float cum_total_ = 0.0f, cum_ca_ = 0.0f;
  float cum_buy_ = 0.0f, cum_sell_ = 0.0f;
  float cum_xl_ = 0.0f, cum_l_ = 0.0f, cum_m_ = 0.0f, cum_s_ = 0.0f;
  float net_xl_ = 0.0f, net_l_ = 0.0f, net_m_ = 0.0f, net_s_ = 0.0f;
  float cum_am_buy_ = 0.0f, cum_am_sell_ = 0.0f;
  float cum_pm_buy_ = 0.0f, cum_pm_sell_ = 0.0f;
};
