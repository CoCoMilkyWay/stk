#pragma once

#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"
#include "features/FeaturesMinute/TS/MinuteIndex.hpp"
// base
#include "features/FeaturesTick/TS/base/DeltaT.hpp"
#include "features/FeaturesTick/TS/base/DepthData.hpp"
#include "features/FeaturesTick/TS/base/DepthIndex.hpp"
#include "features/FeaturesTick/TS/base/MicroPrice.hpp"
#include "features/FeaturesTick/TS/base/MidPrice.hpp"
#include "features/FeaturesTick/TS/base/Spread.hpp"
#include "features/FeaturesTick/TS/base/TickIndex.hpp"
#include "features/FeaturesTick/TS/base/TradePrice.hpp"
// features
#include "features/FeaturesTick/TS/BEHAV.hpp"
#include "features/FeaturesTick/TS/CI.hpp"
#include "features/FeaturesTick/TS/COST.hpp"
#include "features/FeaturesTick/TS/CTR.hpp"
#include "features/FeaturesTick/TS/CWI.hpp"
#include "features/FeaturesTick/TS/DDI.hpp"
#include "features/FeaturesTick/TS/ENTROPY.hpp"
#include "features/FeaturesTick/TS/FLOW_RATE.hpp"
#include "features/FeaturesTick/TS/GRAD.hpp"
#include "features/FeaturesTick/TS/HLA.hpp"
#include "features/FeaturesTick/TS/LABEL.hpp"
#include "features/FeaturesTick/TS/OA.hpp"
#include "features/FeaturesTick/TS/OFI.hpp"
#include "features/FeaturesTick/TS/PARA.hpp"
#include "features/FeaturesTick/TS/PEAK.hpp"
#include "features/FeaturesTick/TS/REPRE.hpp"
#include "features/FeaturesTick/TS/RESIL.hpp"
#include "features/FeaturesTick/TS/TLR.hpp"
#include "features/FeaturesTick/TS/TOXIC.hpp"
#include <deque>

// DAG: (静态多级)有向无环计算图 (Directed Acyclic Graph) ( L0 (Tick) -> L1 (Minute) )
class DAG {
public:
  // ===========================================================================
  // 事件/时间驱动: 底层数据结构 (按计算层级排列, 作为计算图的"驱动时钟")
  // ===========================================================================
  TickData &tick_data;    // L0 输入（外部传入）
  MinuteData minute_data; // L1 输入（内部管理，由 resampler 填充）

  // ===========================================================================
  // L0: Tick 级别 - CBuffer + 算子
  // ===========================================================================
  struct L0 {
    TickData &td; // 唯一需要构造函数初始化的引用

    // -------------------------------------------------------------------------
    // [EVERY TICK] 逐笔更新 - 每个订单(增/删/改/成交)都触发
    // -------------------------------------------------------------------------

    // --- DeltaT ---
    CBuffer<float, L2::BLEN> DeltaTMaker_;
    CBuffer<float, L2::BLEN> DeltaTTaker_;
    CBuffer<float, L2::BLEN> DeltaTCancel_;
    DeltaT DeltaT{td, DeltaTMaker_, DeltaTTaker_, DeltaTCancel_};

    // --- TickIndex ---
    CBuffer<float, L2::BLEN> Sec_;       // 秒相位 [-1,1] (特征: sec)
    CBuffer<float, L2::BLEN> TickIndex_; // 原始tick索引 (供其他算子使用)
    TickIndex TickIndex{td, Sec_, TickIndex_};

    // -------------------------------------------------------------------------
    // [ON TAKER] 成交时更新 - order_type == TAKER 时触发
    // -------------------------------------------------------------------------

    // --- TradePrice ---
    CBuffer<float, L2::BLEN> TradePrice_; // 成交价 (元单位)
    TradePrice TradePrice{td, TradePrice_};

    // -------------------------------------------------------------------------
    // [ON DEPTH] 盘口更新时 - depth_updated == true 时触发:
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
    CBuffer<float, L2::BLEN> BidPrice_[L2::LOB_DEPTH]; // 买1-N价 (元)
    CBuffer<float, L2::BLEN> AskPrice_[L2::LOB_DEPTH]; // 卖1-N价 (元)
    CBuffer<float, L2::BLEN> BidQty_[L2::LOB_DEPTH];   // 买1-N量 (股, 正值)
    CBuffer<float, L2::BLEN> AskQty_[L2::LOB_DEPTH];   // 卖1-N量 (股, 负值)
    CBuffer<float, L2::BLEN> BidAmt_[L2::LOB_DEPTH];   // 买1-N金额 (万元, 正值)
    CBuffer<float, L2::BLEN> AskAmt_[L2::LOB_DEPTH];   // 卖1-N金额 (万元, 负值)
    DepthData<L2::LOB_DEPTH> DepthData{td, BidPrice_, AskPrice_, BidQty_, AskQty_, BidAmt_, AskAmt_};

    // --- MidPrice ---
    CBuffer<float, L2::BLEN> MidPrice_;
    MidPrice MidPrice{BidPrice_[0], AskPrice_[0], MidPrice_};

    // --- MicroPrice ---
    CBuffer<float, L2::BLEN> MicroPrice_;
    MicroPrice MicroPrice{td, MicroPrice_};

    // --- Spread ---
    CBuffer<float, L2::BLEN> Spread_;
    Spread Spread{BidPrice_[0], AskPrice_[0], Spread_};

    // --- CI ---
    CBuffer<float, L2::BLEN> ci_1_;
    CBuffer<float, L2::BLEN> ci_5_;
    CBuffer<float, L2::BLEN> ci_10_;
    CBuffer<float, L2::BLEN> ci_30_;
    CBuffer<float, L2::BLEN> ci_all_;
    CI<1> ci_1{BidQty_, AskQty_, ci_1_};
    CI<5> ci_5{BidQty_, AskQty_, ci_5_};
    CI<10> ci_10{BidQty_, AskQty_, ci_10_};
    CI<30> ci_30{BidQty_, AskQty_, ci_30_};
    CI<L2::LOB_DEPTH> ci_all{BidQty_, AskQty_, ci_all_};

    // --- CWI ---
    CBuffer<float, L2::BLEN> cwi_1_;
    CBuffer<float, L2::BLEN> cwi_2_;
    CWI<10> cwi_1{BidQty_, AskQty_, cwi_1_};
    CWI<20> cwi_2{BidQty_, AskQty_, cwi_2_};

    // --- DDI ---
    CBuffer<float, L2::BLEN> ddi_1_;
    CBuffer<float, L2::BLEN> ddi_2_;
    DDI<1> ddi_1{BidQty_, AskQty_, BidPrice_, AskPrice_, ddi_1_};
    DDI<2> ddi_2{BidQty_, AskQty_, BidPrice_, AskPrice_, ddi_2_};

    // --- TLR ---
    CBuffer<float, L2::BLEN> tbr_5_;
    CBuffer<float, L2::BLEN> tar_5_;
    TLR<5, true> tbr_5{BidQty_, AskQty_, tbr_5_};
    TLR<5, false> tar_5{BidQty_, AskQty_, tar_5_};

    // --- PARA (Layer 1) ---
    CBuffer<float, L2::BLEN> b_para_c0_;
    CBuffer<float, L2::BLEN> b_para_c1_;
    CBuffer<float, L2::BLEN> b_para_c2_;
    CBuffer<float, L2::BLEN> a_para_c0_;
    CBuffer<float, L2::BLEN> a_para_c1_;
    CBuffer<float, L2::BLEN> a_para_c2_;
    PARA<true, 0> b_para_c0{BidQty_, AskQty_, b_para_c0_};
    PARA<true, 1> b_para_c1{BidQty_, AskQty_, b_para_c1_};
    PARA<true, 2> b_para_c2{BidQty_, AskQty_, b_para_c2_};
    PARA<false, 0> a_para_c0{BidQty_, AskQty_, a_para_c0_};
    PARA<false, 1> a_para_c1{BidQty_, AskQty_, a_para_c1_};
    PARA<false, 2> a_para_c2{BidQty_, AskQty_, a_para_c2_};
    // --- PARA (Layer 2: 失衡，依赖Layer 1) ---
    CBuffer<float, L2::BLEN> imba_para_c0_;
    CBuffer<float, L2::BLEN> imba_para_c1_;
    CBuffer<float, L2::BLEN> imba_para_c2_;
    PARA_IMBA<0> imba_para_c0{b_para_c0_, a_para_c0_, imba_para_c0_};
    PARA_IMBA<1> imba_para_c1{b_para_c1_, a_para_c1_, imba_para_c1_};
    PARA_IMBA<2> imba_para_c2{b_para_c2_, a_para_c2_, imba_para_c2_};

    // --- GRAD (Layer 1) ---
    CBuffer<float, L2::BLEN> b_5_c1_;
    CBuffer<float, L2::BLEN> a_5_c1_;
    GRAD<5, true> b_5_c1{BidQty_, AskQty_, b_5_c1_};
    GRAD<5, false> a_5_c1{BidQty_, AskQty_, a_5_c1_};
    // --- GRAD (Layer 2) ---
    CBuffer<float, L2::BLEN> imba_5_c1_;
    GRAD_IMBA imba_5_c1{b_5_c1_, a_5_c1_, imba_5_c1_};

    // --- ENTROPY (Layer 1) ---
    CBuffer<float, L2::BLEN> b_5_entropy_;
    CBuffer<float, L2::BLEN> a_5_entropy_;
    CBuffer<float, L2::BLEN> b_30_entropy_;
    CBuffer<float, L2::BLEN> a_30_entropy_;
    ENTROPY<5, true> b_5_entropy{BidQty_, AskQty_, b_5_entropy_};
    ENTROPY<5, false> a_5_entropy{BidQty_, AskQty_, a_5_entropy_};
    ENTROPY<30, true> b_30_entropy{BidQty_, AskQty_, b_30_entropy_};
    ENTROPY<30, false> a_30_entropy{BidQty_, AskQty_, a_30_entropy_};
    // --- ENTROPY (Layer 2) ---
    CBuffer<float, L2::BLEN> imba_5_entropy_;
    CBuffer<float, L2::BLEN> imba_30_entropy_;
    ENTROPY_IMBA imba_5_entropy{b_5_entropy_, a_5_entropy_, imba_5_entropy_};
    ENTROPY_IMBA imba_30_entropy{b_30_entropy_, a_30_entropy_, imba_30_entropy_};

    // --- OFI ---
    CBuffer<float, L2::BLEN> ofi_1_;
    CBuffer<float, L2::BLEN> ofi_5_;
    OFI<1> ofi_1{BidQty_, AskQty_, BidPrice_, AskPrice_, ofi_1_};
    OFI<5> ofi_5{BidQty_, AskQty_, BidPrice_, AskPrice_, ofi_5_};

    // --- COST ---
    CBuffer<float, L2::BLEN> cost_buy_1_;
    CBuffer<float, L2::BLEN> cost_buy_5_;
    CBuffer<float, L2::BLEN> cost_buy_10_;
    CBuffer<float, L2::BLEN> cost_sell_1_;
    CBuffer<float, L2::BLEN> cost_sell_5_;
    CBuffer<float, L2::BLEN> cost_sell_10_;
    COST<1, true> cost_buy_1{AskPrice_, AskQty_, MidPrice_, cost_buy_1_};
    COST<5, true> cost_buy_5{AskPrice_, AskQty_, MidPrice_, cost_buy_5_};
    COST<10, true> cost_buy_10{AskPrice_, AskQty_, MidPrice_, cost_buy_10_};
    COST<1, false> cost_sell_1{BidPrice_, BidQty_, MidPrice_, cost_sell_1_};
    COST<5, false> cost_sell_5{BidPrice_, BidQty_, MidPrice_, cost_sell_5_};
    COST<10, false> cost_sell_10{BidPrice_, BidQty_, MidPrice_, cost_sell_10_};

    // --- PEAK ---
    CBuffer<float, L2::BLEN> peak_loc_bid_;
    CBuffer<float, L2::BLEN> peak_loc_ask_;
    CBuffer<float, L2::BLEN> peak_ratio_bid_;
    CBuffer<float, L2::BLEN> peak_ratio_ask_;
    PEAK<true, true> peak_loc_bid{BidQty_, peak_loc_bid_};
    PEAK<false, true> peak_loc_ask{AskQty_, peak_loc_ask_};
    PEAK<true, false> peak_ratio_bid{BidQty_, peak_ratio_bid_};
    PEAK<false, false> peak_ratio_ask{AskQty_, peak_ratio_ask_};

    // --- LABEL ---
    CBuffer<float, L2::BLEN> next_tick_ret_;
    NextTickReturn next_tick_ret{MidPrice_, next_tick_ret_};

    // --- REPRE (DUMMY) ---
    CBuffer<float, L2::BLEN> depth_repre_;
    DepthRepresentation depth_repre{depth_repre_};

    // -------------------------------------------------------------------------
    // [跨触发域] compute=EVERY TICK, flush=ON DEPTH
    // -------------------------------------------------------------------------

    // --- FLOW_RATE ---
    CBuffer<float, L2::BLEN> arr_bid_;
    CBuffer<float, L2::BLEN> arr_ask_;
    CBuffer<float, L2::BLEN> can_bid_;
    CBuffer<float, L2::BLEN> can_ask_;
    CBuffer<float, L2::BLEN> trd_buy_;
    CBuffer<float, L2::BLEN> trd_sell_;
    CBuffer<float, L2::BLEN> net_ord_;
    CBuffer<float, L2::BLEN> foi_;
    FlowRate flow_rate{td, arr_bid_, arr_ask_, can_bid_, can_ask_, trd_buy_, trd_sell_, net_ord_, foi_};

    // --- ToxicCR ---
    CBuffer<float, L2::BLEN> toxic_cr_;
    ToxicCR toxic_cr{td, toxic_cr_};

    // --- RESIL ---
    CBuffer<float, L2::BLEN> ratio_bid_;
    CBuffer<float, L2::BLEN> ratio_ask_;
    CBuffer<float, L2::BLEN> imba_;
    CBuffer<float, L2::BLEN> dev_bid_;
    CBuffer<float, L2::BLEN> dev_ask_;
    CBuffer<float, L2::BLEN> mr_bid_;
    CBuffer<float, L2::BLEN> mr_ask_;
    CBuffer<float, L2::BLEN> recovery_bid_;
    CBuffer<float, L2::BLEN> recovery_ask_;
    Resiliency resil{td, BidQty_, AskQty_,
                     ratio_bid_, ratio_ask_, imba_, dev_bid_, dev_ask_,
                     mr_bid_, mr_ask_, recovery_bid_, recovery_ask_};

    // --- CTR ---
    CBuffer<float, L2::BLEN> cc_r_;
    CBuffer<float, L2::BLEN> ctr_xl_;
    CBuffer<float, L2::BLEN> ctr_l_;
    CBuffer<float, L2::BLEN> ctr_m_;
    CBuffer<float, L2::BLEN> ctr_s_;
    CBuffer<float, L2::BLEN> cnbi_;
    CBuffer<float, L2::BLEN> cnbi_xl_;
    CBuffer<float, L2::BLEN> cnbi_l_;
    CBuffer<float, L2::BLEN> cnbi_m_;
    CBuffer<float, L2::BLEN> cnbi_s_;
    CBuffer<float, L2::BLEN> cnbi_am_;
    CBuffer<float, L2::BLEN> cnbi_pm_;
    CTR ctr{td, cc_r_, ctr_xl_, ctr_l_, ctr_m_, ctr_s_,
            cnbi_, cnbi_xl_, cnbi_l_, cnbi_m_, cnbi_s_, cnbi_am_, cnbi_pm_};

    // --- BEHAV ---
    CBuffer<float, L2::BLEN> agg_buy_;
    CBuffer<float, L2::BLEN> agg_sell_;
    CBuffer<float, L2::BLEN> agg_dif_;
    CBuffer<float, L2::BLEN> cpr_;
    CBuffer<float, L2::BLEN> agg_trd_;
    CBuffer<float, L2::BLEN> ord_size_;
    Behav behav{td, agg_buy_, agg_sell_, agg_dif_, cpr_, agg_trd_, ord_size_};

    // --- OA ---
    CBuffer<float, L2::BLEN> oa_bcr_;
    CBuffer<float, L2::BLEN> oa_acr_;
    CBuffer<float, L2::BLEN> oa_btr_;
    CBuffer<float, L2::BLEN> oa_atr_;
    OA oa{td, oa_bcr_, oa_acr_, oa_btr_, oa_atr_};

    // --- HLA ---
    CBuffer<float, L2::BLEN> hla_imba_;
    HLA hla{td, BidQty_, AskQty_, hla_imba_};

    // --- TOXIC ---
    CBuffer<float, L2::BLEN> ptc_rt_;
    CBuffer<float, L2::BLEN> fleet_rt_;
    CBuffer<float, L2::BLEN> spoof_int_;
    CBuffer<float, L2::BLEN> stale_ratio_bid_;
    CBuffer<float, L2::BLEN> stale_ratio_ask_;
    Toxic toxic{td, BidQty_, AskQty_, ptc_rt_, fleet_rt_, spoof_int_, stale_ratio_bid_, stale_ratio_ask_};

    explicit L0(TickData &t) : td(t) {} // 构造函数 (只需初始化引用成员)
  };
  L0 l0;

  // ===========================================================================
  // L1: Minute 级别 - 暂保持 deque（后续迁移到 CBuffer + 算子）
  // ===========================================================================
  struct L1 {
    MinuteData &md;

    // --- 基础数据 CBuffer ---
    CBuffer<float, ::L2::BLEN> Min_;         // 分钟数 [0-59] (特征)
    CBuffer<float, ::L2::BLEN> MinuteIndex_; // 原始minute索引 (供其他算子使用)

    // --- 基础数据算子 ---
    MinuteIndex MinuteIndex{md, Min_, MinuteIndex_};

    // Rolling windows for TS features
    std::deque<float> minute_return_window;

    explicit L1(MinuteData &m) : md(m) {}
  };
  L1 l1;

  // ===========================================================================
  // 构造函数
  // ===========================================================================
  explicit DAG(TickData &td) : tick_data(td), l0(td), l1(minute_data) {} // 创建时传入TickData

  // ===========================================================================
  // 跨天重置 (统一维护)
  // ===========================================================================
  void reset_for_new_day() {
    // 用前一天收盘价(最后成交价)设置depth的涨跌停保护
    float prev_close = l0.TradePrice_.size() > 0 ? l0.TradePrice_.back() : 0.0f;
    if (prev_close > 0) {
      l0.DepthData.set_prev_close(prev_close);
    }
    // TODO: 后续新增算子的跨天重置逻辑统一加在这里
  }
};
