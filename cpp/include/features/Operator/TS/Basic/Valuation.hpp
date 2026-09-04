#pragma once

// =============================================================================
// Valuation - 实时估值
// =============================================================================
// 分钟最新价 × 当日基本面输入行 (fund::kCount floats, PIT 预计算, 缺失=NaN)
//
// 【公式定义】
//   mcap  = P_t × S_total   (亿元)       fmcap = P_t × S_float   (亿元)
//   pe    = mcap / NP_ttm                pb    = mcap / EQ_mrq
//   ps    = mcap / REV_ttm               pcf   = mcap / CF_ttm
//   limit_up = 1[P_t ≥ P_up - 1e-4]      limit_dn = 1[P_t ≤ P_dn + 1e-4]
//   low_p    = 1[P_t < 1]                low_mc   = 1[mcap < thr]
//
// 【触发域】
//   compute: onMinute
//   flush:   onMinute
//
// 【输入输出】
//   输入: MinuteData.close (onMinute), fund_row (onDay, begin_day 设置)
//   输出: mcap, fmcap, pe, pb, ps, pcf, limit_up, limit_dn, low_p, low_mc
//
// 【fast-math 契约】
//   本 TU 走 -ffast-math, 不做 isnan/isfinite: 缺失 (NaN) 靠硬件算术透传
//   (NaN 参与 ± × ÷ 仍是 NaN); 比较遇 NaN 恒 false → 标记列自然落 0
//   (无涨跌停限制 → 未触板, 股本缺失 → 非低市值), 与 qmt 口径一致.
//   sat: 比率极值 (分母趋零 → 超 fp16 上限) 与 NaN 一样比较 false → NaN.
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"
#include "features/Fundamental/FundamentalDaily.hpp"

#include <cassert>
#include <limits>

// 触发域: ON_MINUTE (分钟级, 每分钟触发一次)
class Valuation {
public:
  enum Out : size_t { mcap,
                      fmcap,
                      pe,
                      pb,
                      ps,
                      pcf,
                      limit_up,
                      limit_dn,
                      low_p,
                      low_mc,
                      kCount };

  Valuation(const MinuteData &md, const float *const &fund_row,
            CBuffer<float, ::L2::BLEN> (&out)[kCount])
      : md_(md), fund_row_(fund_row),
        mcap_buf_(out[mcap]), fmcap_buf_(out[fmcap]),
        pe_buf_(out[pe]), pb_buf_(out[pb]), ps_buf_(out[ps]), pcf_buf_(out[pcf]),
        limit_up_buf_(out[limit_up]), limit_dn_buf_(out[limit_dn]),
        low_p_buf_(out[low_p]), low_mc_buf_(out[low_mc]) {}

  // 触发域: ON_MINUTE
  void compute() {
    assert(fund_row_ != nullptr && "begin_day 未设置当日基本面行");
    const float close = md_.close.back(); // [元]
    const float *f = fund_row_;

    const float mcap = close * f[fund::total_shares]; // [亿元]
    mcap_ = sat(mcap);
    fmcap_ = sat(close * f[fund::float_shares]);
    pe_ = sat(mcap / f[fund::net_profit_ttm]);
    pb_ = sat(mcap / f[fund::equity_mrq]);
    ps_ = sat(mcap / f[fund::revenue_ttm]);
    pcf_ = sat(mcap / f[fund::cffoa_ttm]);
    limit_up_ = (close >= f[fund::up_lim] - 1e-4f) ? 1.0f : 0.0f;
    limit_dn_ = (close <= f[fund::dn_lim] + 1e-4f) ? 1.0f : 0.0f;
    low_p_ = (close < 1.0f) ? 1.0f : 0.0f;
    low_mc_ = (mcap < f[fund::low_mc_thr]) ? 1.0f : 0.0f;
  }

  // 触发域: ON_MINUTE
  void flush() {
    mcap_buf_.push_back(mcap_);
    fmcap_buf_.push_back(fmcap_);
    pe_buf_.push_back(pe_);
    pb_buf_.push_back(pb_);
    ps_buf_.push_back(ps_);
    pcf_buf_.push_back(pcf_);
    limit_up_buf_.push_back(limit_up_);
    limit_dn_buf_.push_back(limit_dn_);
    low_p_buf_.push_back(low_p_);
    low_mc_buf_.push_back(low_mc_);
  }

  void reset() {}

private:
  // Float16 饱和: 存不下的极值 → NaN (NaN 输入同样落到 NaN 分支)
  static float sat(float v) {
    constexpr float kFp16Max = 65504.0f;
    return (v > -kFp16Max && v < kFp16Max)
               ? v
               : std::numeric_limits<float>::quiet_NaN();
  }

  const MinuteData &md_;
  const float *const &fund_row_; // 指向 DAG::fund_row_, begin_day 更新

  CBuffer<float, ::L2::BLEN> &mcap_buf_;
  CBuffer<float, ::L2::BLEN> &fmcap_buf_;
  CBuffer<float, ::L2::BLEN> &pe_buf_;
  CBuffer<float, ::L2::BLEN> &pb_buf_;
  CBuffer<float, ::L2::BLEN> &ps_buf_;
  CBuffer<float, ::L2::BLEN> &pcf_buf_;
  CBuffer<float, ::L2::BLEN> &limit_up_buf_;
  CBuffer<float, ::L2::BLEN> &limit_dn_buf_;
  CBuffer<float, ::L2::BLEN> &low_p_buf_;
  CBuffer<float, ::L2::BLEN> &low_mc_buf_;

  float mcap_ = 0.0f, fmcap_ = 0.0f;
  float pe_ = 0.0f, pb_ = 0.0f, ps_ = 0.0f, pcf_ = 0.0f;
  float limit_up_ = 0.0f, limit_dn_ = 0.0f, low_p_ = 0.0f, low_mc_ = 0.0f;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Val(N) N(Val, (Valuation), (minute_data, fund_row_), onMinute, onMinute)

#define FIELDS_L1_Val(X)                                                                                                                                                                             \
  X(mcap, 1, DATA, TS, BASIC, RAW, LOG_ZSCORE, "00/010/00", "Market Cap RT", "实时总市值", "分钟最新价×总股本(亿元,不复权真市值)", R"(\frac{P_t \cdot S^{total}_{D}}{10^{8}})", OP(Val, mcap))       \
  X(fmcap, 1, DATA, TS, BASIC, RAW, LOG_ZSCORE, "00/010/00", "Float Market Cap RT", "实时流通市值", "分钟最新价×A股流通股本(亿元)", R"(\frac{P_t \cdot S^{float}_{D}}{10^{8}})", OP(Val, fmcap))     \
  X(pe, 1, DATA, TS, BASIC, RATIO, NONE, "00/010/00", "PE TTM RT", "实时市盈率TTM", "实时市值/归母净利TTM(亏损→负PE保留,分母0→NaN)", R"(\frac{P_t S^{total}_{D}}{NP^{TTM}_{D}})", OP(Val, pe))       \
  X(pb, 1, DATA, TS, BASIC, RATIO, NONE, "00/010/00", "PB MRQ RT", "实时市净率MRQ", "实时市值/归母权益MRQ(负权益→负PB保留,分母0→NaN)", R"(\frac{P_t S^{total}_{D}}{EQ^{MRQ}_{D}})", OP(Val, pb))     \
  X(ps, 1, DATA, TS, BASIC, RATIO, NONE, "00/010/00", "PS TTM RT", "实时市销率TTM", "实时市值/营业总收入TTM(营收≤0为脏值→NaN)", R"(\frac{P_t S^{total}_{D}}{REV^{TTM}_{D}})", OP(Val, ps))           \
  X(pcf, 1, DATA, TS, BASIC, RATIO, NONE, "00/010/00", "PCF TTM RT", "实时市现率TTM", "实时市值/经营现金流TTM(烧钱→负PCF保留,分母0→NaN)", R"(\frac{P_t S^{total}_{D}}{CF^{TTM}_{D}})", OP(Val, pcf)) \
  X(limit_up, 1, DATA, TS, BASIC, RAW, NONE, "00/010/00", "Limit Up RT", "实时涨停标记", "分钟最新价触及当日涨停价", R"(\mathbf{1}[P_t \geq P^{up}_{D} - 10^{-4}])", OP(Val, limit_up))              \
  X(limit_dn, 1, DATA, TS, BASIC, RAW, NONE, "00/010/00", "Limit Down RT", "实时跌停标记", "分钟最新价触及当日跌停价", R"(\mathbf{1}[P_t \leq P^{dn}_{D} + 10^{-4}])", OP(Val, limit_dn))            \
  X(low_p, 1, DATA, TS, BASIC, RAW, NONE, "00/010/00", "Low Price RT", "实时低价标记", "分钟最新价 < 1元(面值退市风险)", R"(\mathbf{1}[P_t < 1])", OP(Val, low_p))                                   \
  X(low_mc, 1, DATA, TS, BASIC, RAW, NONE, "00/010/00", "Low Market Cap RT", "实时低市值标记", "实时市值 < 阈值(主板5亿/其他3亿)", R"(\mathbf{1}[P_t S^{total}_{D} < \theta])", OP(Val, low_mc))
