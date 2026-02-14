#pragma once

#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"
// Basic
#include "features/Operator/TS/Basic/MicroPrice.hpp"
#include "features/Operator/TS/Basic/MidPrice.hpp"
#include "features/Operator/TS/Basic/MinuteIndex.hpp"
#include "features/Operator/TS/Basic/Spread.hpp"
#include "features/Operator/TS/Basic/TickIndex.hpp"
// Imbalance
#include "features/Operator/TS/Imbalance/CI.hpp"
#include "features/Operator/TS/Imbalance/CI_all.hpp"
#include "features/Operator/TS/Imbalance/CWI.hpp"
#include "features/Operator/TS/Imbalance/DDI.hpp"
#include "features/Operator/TS/Imbalance/EntropyImba.hpp"
#include "features/Operator/TS/Imbalance/GradImba.hpp"
#include "features/Operator/TS/Imbalance/ParaImba.hpp"
// OrderFlow
#include "features/Operator/TS/OrderFlow/CTR.hpp"
#include "features/Operator/TS/OrderFlow/FlowRate.hpp"
#include "features/Operator/TS/OrderFlow/HLA.hpp"
#include "features/Operator/TS/OrderFlow/OA.hpp"
#include "features/Operator/TS/OrderFlow/OFI.hpp"
#include "features/Operator/TS/OrderFlow/OrderInfo.hpp"
#include "features/Operator/TS/OrderFlow/ToxicCr.hpp"
// Behavioral
#include "features/Operator/TS/Behavioral/Behav.hpp"
#include "features/Operator/TS/Behavioral/Manip.hpp"
// Resilience
#include "features/Operator/TS/Resilience/Resiliency.hpp"
// Liquidity
#include "features/Operator/TS/Liquidity/Cost.hpp"
#include "features/Operator/TS/Liquidity/TLR.hpp"
// Shape
#include "features/Operator/TS/Shape/DepthRepresentation.hpp"
#include "features/Operator/TS/Shape/Entropy.hpp"
#include "features/Operator/TS/Shape/Grad.hpp"
#include "features/Operator/TS/Shape/Para.hpp"
#include "features/Operator/TS/Shape/Peak.hpp"
// Meta
#include "features/Operator/TS/Meta/DepthData.hpp"
#include "features/Operator/TS/Meta/DepthIndex.hpp"
// Label
#include "features/Operator/TS/Label/LabelReturn.hpp"

// DAG: (静态多级)有向无环计算图 (Directed Acyclic Graph) ( L0 (Tick) -> L1 (Minute) )
class DAG {
public:
  // ===========================================================================
  // 事件/时间驱动: 底层数据结构 (按计算层级排列, 作为计算图的"驱动时钟")
  // ===========================================================================
  TickData &tick_data;     // L0 输入（外部传入）
  MinuteData minute_data;  // L1 输入（内部管理，由 resampler 填充）
  std::string asset_code_; // 股票代码（用于涨跌幅判断）

  // ===========================================================================
  // L0: Tick 级别 - CBuffer + 算子
  // ===========================================================================
  struct L0 {
    const std::string &asset_code_; // 股票代码 (用于判断涨跌幅限制)
    TickData &td;                   // 逐笔数据

    // -------------------------------------------------------------------------
    // [ON TAKER] 成交更新 - order_type == TAKER 时触发
    // -------------------------------------------------------------------------

    // --- OrderInfo (Taker) ---
    CBuffer<float, L2::BLEN> Taker_price_;
    CBuffer<float, L2::BLEN> Taker_timestamp_;
    CBuffer<float, L2::BLEN> Taker_tickindex_;
    CBuffer<float, L2::BLEN> Taker_volume_;
    CBuffer<float, L2::BLEN> Taker_dir_;
    OrderInfo Taker{td, Taker_price_, Taker_timestamp_, Taker_tickindex_, Taker_volume_, Taker_dir_};

    // -------------------------------------------------------------------------
    // [ON MAKER] 挂单更新 - order_type == MAKER 时触发
    // -------------------------------------------------------------------------

    // --- OrderInfo (Maker) ---
    CBuffer<float, L2::BLEN> Maker_price_;
    CBuffer<float, L2::BLEN> Maker_timestamp_;
    CBuffer<float, L2::BLEN> Maker_tickindex_;
    CBuffer<float, L2::BLEN> Maker_volume_;
    CBuffer<float, L2::BLEN> Maker_dir_;
    OrderInfo Maker{td, Maker_price_, Maker_timestamp_, Maker_tickindex_, Maker_volume_, Maker_dir_};

    // -------------------------------------------------------------------------
    // [ON CANCEL] 撤单更新 - order_type == CANCEL 时触发
    // -------------------------------------------------------------------------

    // --- OrderInfo (Cancel) ---
    CBuffer<float, L2::BLEN> Cancel_price_;
    CBuffer<float, L2::BLEN> Cancel_timestamp_;
    CBuffer<float, L2::BLEN> Cancel_tickindex_;
    CBuffer<float, L2::BLEN> Cancel_volume_;
    CBuffer<float, L2::BLEN> Cancel_dir_;
    OrderInfo Cancel{td, Cancel_price_, Cancel_timestamp_, Cancel_tickindex_, Cancel_volume_, Cancel_dir_};

    // -------------------------------------------------------------------------
    // [ON TICK] 逐笔更新 - 每个订单(增/删/改/成交)都触发
    // -------------------------------------------------------------------------

    // --- TickIndex ---
    CBuffer<float, L2::BLEN> Sec_;
    CBuffer<float, L2::BLEN> TickIndex_;
    TickIndex TickIndex{td, Sec_, TickIndex_};

    // -------------------------------------------------------------------------
    // [ON DEPTH] 盘口更新 - depth_updated == true 时触发:
    // 1. depth更新间隔大于L2::L2_MIN_TIME_INTERVAL_MS (一般是一秒)
    // 2. tob_price_已经被前面的taker单更新
    // 3. 深度挂单已满(depth_buffer.size() >= L2::LOB_DEPTH) (集合竞价数据之前会插入极端价位的多档sentinel, 所以自动满足)
    // 注意:
    //    9:25:00集合竞价成交的第一个taker单会被故意过滤(tob_price_正要被更新), 那一秒尾随的大量burst taker单也会被L2_MIN_TIME_INTERVAL_MS时间故意过滤
    //    9:25:00-9:30:00期间为订单真空期
    //    9:30:00开始的任意"增/删/改/成交"订单才会触发全天第一个depth_valid
    // -------------------------------------------------------------------------

    // --- DepthIndex ---
    CBuffer<float, L2::BLEN> DepthIndex_;
    DepthIndex DepthIndex{td, DepthIndex_};

    // --- DepthData ---
    CBuffer<float, L2::BLEN> BidPrice_[L2::LOB_DEPTH];
    CBuffer<float, L2::BLEN> AskPrice_[L2::LOB_DEPTH];
    CBuffer<float, L2::BLEN> BidQty_[L2::LOB_DEPTH];
    CBuffer<float, L2::BLEN> AskQty_[L2::LOB_DEPTH];
    CBuffer<float, L2::BLEN> BidAmt_[L2::LOB_DEPTH];
    CBuffer<float, L2::BLEN> AskAmt_[L2::LOB_DEPTH];
    DepthData<L2::LOB_DEPTH> DepthData{td, Taker_price_, BidPrice_, AskPrice_, BidQty_, AskQty_, BidAmt_, AskAmt_, asset_code_};

    // --- MidPrice ---
    CBuffer<float, L2::BLEN> MidPrice_;
    MidPrice MidPrice{BidPrice_[0], AskPrice_[0], MidPrice_};

    // --- MicroPrice ---
    CBuffer<float, L2::BLEN> MicroPrice_;
    MicroPrice MicroPrice{BidPrice_[0], AskPrice_[0], BidQty_[0], AskQty_[0], MicroPrice_};

    // --- Spread ---
    CBuffer<float, L2::BLEN> Spread_;
    Spread Spread{BidPrice_[0], AskPrice_[0], Spread_};

    // --- CI ---
    CBuffer<float, L2::BLEN> Ci_1_;
    CI<1> Ci_1{BidQty_, AskQty_, Ci_1_};

    // --- OFI ---
    CBuffer<float, L2::BLEN> Ofi_1_;
    CBuffer<float, L2::BLEN> Ofi_5_;
    OFI<1> Ofi_1{BidQty_, AskQty_, BidPrice_, AskPrice_, Ofi_1_};
    OFI<5> Ofi_5{BidQty_, AskQty_, BidPrice_, AskPrice_, Ofi_5_};

    // --- LabelReturn (吃单收益标签，单算子计算全部组合) ---
    // 输出顺序: [5min×5w long, 5min×5w short, 5min×20w long, 5min×20w short, 10min×5w long, ...]
    LabelReturnOp LabelReturn{BidPrice_, AskPrice_, BidQty_, AskQty_};

    explicit L0(TickData &t, const std::string &code) : td(t), asset_code_(code) {}
  };
  L0 l0;

  // ===========================================================================
  // L1: Minute 级别 - 暂保持 deque（后续迁移到 CBuffer + 算子）
  // ===========================================================================
  struct L1 {
    MinuteData &md;
    L0 &l0;

    // --- 基础数据 CBuffer ---
    CBuffer<float, ::L2::BLEN> Min_;
    CBuffer<float, ::L2::BLEN> MinuteIndex_;
    MinuteIndex MinuteIndex{md, Min_, MinuteIndex_};

    CBuffer<float, ::L2::BLEN> Ci_5_;
    CBuffer<float, ::L2::BLEN> Ci_10_;
    CBuffer<float, ::L2::BLEN> Ci_30_;
    CBuffer<float, ::L2::BLEN> Ci_all_;
    CI<5> Ci_5{l0.BidQty_, l0.AskQty_, Ci_5_};
    CI<10> Ci_10{l0.BidQty_, l0.AskQty_, Ci_10_};
    CI<30> Ci_30{l0.BidQty_, l0.AskQty_, Ci_30_};
    CI_all Ci_all{l0.td, Ci_all_};

    CBuffer<float, ::L2::BLEN> Cwi_1_;
    CBuffer<float, ::L2::BLEN> Cwi_2_;
    CWI<10> Cwi_1{l0.BidQty_, l0.AskQty_, Cwi_1_};
    CWI<20> Cwi_2{l0.BidQty_, l0.AskQty_, Cwi_2_};

    CBuffer<float, ::L2::BLEN> Ddi_1_;
    CBuffer<float, ::L2::BLEN> Ddi_2_;
    DDI<1> Ddi_1{l0.BidQty_, l0.AskQty_, l0.BidPrice_, l0.AskPrice_, Ddi_1_};
    DDI<2> Ddi_2{l0.BidQty_, l0.AskQty_, l0.BidPrice_, l0.AskPrice_, Ddi_2_};

    CBuffer<float, ::L2::BLEN> Tbr_5_;
    CBuffer<float, ::L2::BLEN> Tar_5_;
    TLR<5, true> Tbr_5{l0.BidQty_, l0.AskQty_, l0.td, Tbr_5_};
    TLR<5, false> Tar_5{l0.BidQty_, l0.AskQty_, l0.td, Tar_5_};

    // --- Para (降频) ---
    CBuffer<float, ::L2::BLEN> Para_b_c0_;
    CBuffer<float, ::L2::BLEN> Para_b_c1_;
    CBuffer<float, ::L2::BLEN> Para_b_c2_;
    CBuffer<float, ::L2::BLEN> Para_a_c0_;
    CBuffer<float, ::L2::BLEN> Para_a_c1_;
    CBuffer<float, ::L2::BLEN> Para_a_c2_;
    Para<true, 0> Para_b_c0{l0.BidQty_, l0.AskQty_, Para_b_c0_};
    Para<true, 1> Para_b_c1{l0.BidQty_, l0.AskQty_, Para_b_c1_};
    Para<true, 2> Para_b_c2{l0.BidQty_, l0.AskQty_, Para_b_c2_};
    Para<false, 0> Para_a_c0{l0.BidQty_, l0.AskQty_, Para_a_c0_};
    Para<false, 1> Para_a_c1{l0.BidQty_, l0.AskQty_, Para_a_c1_};
    Para<false, 2> Para_a_c2{l0.BidQty_, l0.AskQty_, Para_a_c2_};

    // --- ParaImba (降频) ---
    CBuffer<float, ::L2::BLEN> ParaImba_c0_;
    CBuffer<float, ::L2::BLEN> ParaImba_c1_;
    CBuffer<float, ::L2::BLEN> ParaImba_c2_;
    ParaImba<0> ParaImba_c0{Para_b_c0_, Para_a_c0_, ParaImba_c0_};
    ParaImba<1> ParaImba_c1{Para_b_c1_, Para_a_c1_, ParaImba_c1_};
    ParaImba<2> ParaImba_c2{Para_b_c2_, Para_a_c2_, ParaImba_c2_};

    // --- Grad (降频) ---
    CBuffer<float, ::L2::BLEN> Grad_b_5_c1_;
    CBuffer<float, ::L2::BLEN> Grad_a_5_c1_;
    Grad<5, true> Grad_b_5_c1{l0.BidQty_, l0.AskQty_, Grad_b_5_c1_};
    Grad<5, false> Grad_a_5_c1{l0.BidQty_, l0.AskQty_, Grad_a_5_c1_};

    // --- GradImba (降频) ---
    CBuffer<float, ::L2::BLEN> GradImba_5_c1_;
    GradImba GradImba_5_c1{Grad_b_5_c1_, Grad_a_5_c1_, GradImba_5_c1_};

    // --- Entropy (降频) ---
    CBuffer<float, ::L2::BLEN> Entropy_b_5_;
    CBuffer<float, ::L2::BLEN> Entropy_a_5_;
    CBuffer<float, ::L2::BLEN> Entropy_b_30_;
    CBuffer<float, ::L2::BLEN> Entropy_a_30_;
    Entropy<5, true> Entropy_b_5{l0.BidQty_, l0.AskQty_, Entropy_b_5_};
    Entropy<5, false> Entropy_a_5{l0.BidQty_, l0.AskQty_, Entropy_a_5_};
    Entropy<30, true> Entropy_b_30{l0.BidQty_, l0.AskQty_, Entropy_b_30_};
    Entropy<30, false> Entropy_a_30{l0.BidQty_, l0.AskQty_, Entropy_a_30_};

    // --- EntropyImba (降频) ---
    CBuffer<float, ::L2::BLEN> EntropyImba_5_;
    CBuffer<float, ::L2::BLEN> EntropyImba_30_;
    EntropyImba EntropyImba_5{Entropy_b_5_, Entropy_a_5_, EntropyImba_5_};
    EntropyImba EntropyImba_30{Entropy_b_30_, Entropy_a_30_, EntropyImba_30_};

    // --- DepthRepresentation (降频) ---
    CBuffer<float, ::L2::BLEN> DepthRepresentation_;
    DepthRepresentation DepthRepresentation{DepthRepresentation_};

    // --- CTR (降频) ---
    CBuffer<float, ::L2::BLEN> Ctr_cc_r_;
    CBuffer<float, ::L2::BLEN> Ctr_xl_;
    CBuffer<float, ::L2::BLEN> Ctr_l_;
    CBuffer<float, ::L2::BLEN> Ctr_m_;
    CBuffer<float, ::L2::BLEN> Ctr_s_;
    CBuffer<float, ::L2::BLEN> Ctr_cnbi_;
    CBuffer<float, ::L2::BLEN> Ctr_cnbi_xl_;
    CBuffer<float, ::L2::BLEN> Ctr_cnbi_l_;
    CBuffer<float, ::L2::BLEN> Ctr_cnbi_m_;
    CBuffer<float, ::L2::BLEN> Ctr_cnbi_s_;
    CBuffer<float, ::L2::BLEN> Ctr_cnbi_am_;
    CBuffer<float, ::L2::BLEN> Ctr_cnbi_pm_;
    CTR Ctr{l0.td, Ctr_cc_r_, Ctr_xl_, Ctr_l_, Ctr_m_, Ctr_s_,
            Ctr_cnbi_, Ctr_cnbi_xl_, Ctr_cnbi_l_, Ctr_cnbi_m_, Ctr_cnbi_s_, Ctr_cnbi_am_, Ctr_cnbi_pm_};

    // --- OA (降频) ---
    CBuffer<float, ::L2::BLEN> Oa_bcr_;
    CBuffer<float, ::L2::BLEN> Oa_acr_;
    CBuffer<float, ::L2::BLEN> Oa_btr_;
    CBuffer<float, ::L2::BLEN> Oa_atr_;
    OA Oa{l0.td, Oa_bcr_, Oa_acr_, Oa_btr_, Oa_atr_};

    // --- HLA (降频) ---
    CBuffer<float, ::L2::BLEN> Hla_imba_;
    HLA Hla{l0.td, l0.BidQty_, l0.AskQty_, Hla_imba_};

    // --- ToxicCr (降频) ---
    CBuffer<float, ::L2::BLEN> ToxicCr_;
    ToxicCr ToxicCr{l0.td, ToxicCr_};

    // --- FlowRate (降频) ---
    CBuffer<float, ::L2::BLEN> FlowRate_mk_bid_;
    CBuffer<float, ::L2::BLEN> FlowRate_mk_ask_;
    CBuffer<float, ::L2::BLEN> FlowRate_cn_bid_;
    CBuffer<float, ::L2::BLEN> FlowRate_cn_ask_;
    CBuffer<float, ::L2::BLEN> FlowRate_tk_bid_;
    CBuffer<float, ::L2::BLEN> FlowRate_tk_ask_;
    CBuffer<float, ::L2::BLEN> FlowRate_net_ord_;
    CBuffer<float, ::L2::BLEN> FlowRate_foi_;
    FlowRate FlowRate{l0.td, FlowRate_mk_bid_, FlowRate_mk_ask_, FlowRate_cn_bid_, FlowRate_cn_ask_,
                      FlowRate_tk_bid_, FlowRate_tk_ask_, FlowRate_net_ord_, FlowRate_foi_};

    // --- Behav (降频) ---
    CBuffer<float, ::L2::BLEN> Behav_agg_buy_;
    CBuffer<float, ::L2::BLEN> Behav_agg_sell_;
    CBuffer<float, ::L2::BLEN> Behav_agg_dif_;
    CBuffer<float, ::L2::BLEN> Behav_cpr_;
    CBuffer<float, ::L2::BLEN> Behav_agg_trd_;
    CBuffer<float, ::L2::BLEN> Behav_ord_size_;
    Behav Behav{l0.td, Behav_agg_buy_, Behav_agg_sell_, Behav_agg_dif_, Behav_cpr_, Behav_agg_trd_, Behav_ord_size_};

    // --- Manip (降频) ---
    CBuffer<float, ::L2::BLEN> Manip_ptc_rt_;
    CBuffer<float, ::L2::BLEN> Manip_fleet_rt_;
    CBuffer<float, ::L2::BLEN> Manip_spoof_int_;
    CBuffer<float, ::L2::BLEN> Manip_stale_ratio_bid_;
    CBuffer<float, ::L2::BLEN> Manip_stale_ratio_ask_;
    Manip Manip{l0.td, l0.BidQty_, l0.AskQty_, Manip_ptc_rt_, Manip_fleet_rt_, Manip_spoof_int_, Manip_stale_ratio_bid_, Manip_stale_ratio_ask_};

    // --- Resiliency (降频) ---
    CBuffer<float, ::L2::BLEN> Resil_ratio_bid_;
    CBuffer<float, ::L2::BLEN> Resil_ratio_ask_;
    CBuffer<float, ::L2::BLEN> Resil_imba_;
    CBuffer<float, ::L2::BLEN> Resil_dev_bid_;
    CBuffer<float, ::L2::BLEN> Resil_dev_ask_;
    CBuffer<float, ::L2::BLEN> Resil_mr_bid_;
    CBuffer<float, ::L2::BLEN> Resil_mr_ask_;
    CBuffer<float, ::L2::BLEN> Resil_recovery_bid_;
    CBuffer<float, ::L2::BLEN> Resil_recovery_ask_;
    Resiliency Resiliency{l0.td, l0.BidQty_, l0.AskQty_,
                          Resil_ratio_bid_, Resil_ratio_ask_, Resil_imba_, Resil_dev_bid_, Resil_dev_ask_,
                          Resil_mr_bid_, Resil_mr_ask_, Resil_recovery_bid_, Resil_recovery_ask_};

    // --- Cost (降频) ---
    CBuffer<float, ::L2::BLEN> Cost_buy_1_;
    CBuffer<float, ::L2::BLEN> Cost_buy_5_;
    CBuffer<float, ::L2::BLEN> Cost_buy_10_;
    CBuffer<float, ::L2::BLEN> Cost_sell_1_;
    CBuffer<float, ::L2::BLEN> Cost_sell_5_;
    CBuffer<float, ::L2::BLEN> Cost_sell_10_;
    Cost<1, true> Cost_buy_1{l0.AskPrice_, l0.AskQty_, l0.MidPrice_, Cost_buy_1_};
    Cost<5, true> Cost_buy_5{l0.AskPrice_, l0.AskQty_, l0.MidPrice_, Cost_buy_5_};
    Cost<10, true> Cost_buy_10{l0.AskPrice_, l0.AskQty_, l0.MidPrice_, Cost_buy_10_};
    Cost<1, false> Cost_sell_1{l0.BidPrice_, l0.BidQty_, l0.MidPrice_, Cost_sell_1_};
    Cost<5, false> Cost_sell_5{l0.BidPrice_, l0.BidQty_, l0.MidPrice_, Cost_sell_5_};
    Cost<10, false> Cost_sell_10{l0.BidPrice_, l0.BidQty_, l0.MidPrice_, Cost_sell_10_};

    // --- Peak (降频) ---
    CBuffer<float, ::L2::BLEN> Peak_loc_bid_;
    CBuffer<float, ::L2::BLEN> Peak_loc_ask_;
    CBuffer<float, ::L2::BLEN> Peak_ratio_bid_;
    CBuffer<float, ::L2::BLEN> Peak_ratio_ask_;
    Peak<true, true> Peak_loc_bid{l0.BidQty_, Peak_loc_bid_};
    Peak<false, true> Peak_loc_ask{l0.AskQty_, Peak_loc_ask_};
    Peak<true, false> Peak_ratio_bid{l0.BidQty_, Peak_ratio_bid_};
    Peak<false, false> Peak_ratio_ask{l0.AskQty_, Peak_ratio_ask_};

    explicit L1(MinuteData &m, L0 &l0) : md(m), l0(l0) {}
  };
  L1 l1;

  // ===========================================================================
  // 构造函数
  // ===========================================================================
  explicit DAG(TickData &td, const std::string &code) : tick_data(td), asset_code_(code), l0(tick_data, asset_code_), l1(minute_data, l0) {}

  // ===========================================================================
  // 盘前重置
  // ===========================================================================
  void at_day_start() {
    l0.Taker.reset();
    l0.Maker.reset();
    l0.Cancel.reset();
    l0.DepthData.reset();
    l0.Ofi_1.reset();
    l0.Ofi_5.reset();
    l0.LabelReturn.reset();
    l1.Ctr.reset();
    l1.Oa.reset();
    l1.Hla.reset();
    l1.ToxicCr.reset();
    l1.FlowRate.reset();
    l1.Behav.reset();
    l1.Manip.reset();
    l1.Resiliency.reset();
  }

  // ===========================================================================
  // 盘尾计算 (主要给标签类特征用)
  // ===========================================================================
  void at_day_end() {
    // 例如: l0.LabelReturn 可能需要在此做最后的计算
  }
};
