#pragma once

#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"
#include "features/FeaturesHour/TS/HourIndex.hpp"
#include "features/FeaturesMinute/TS/MinuteIndex.hpp"
#include "features/FeaturesTick/TS/DeltaT.hpp"
#include "features/FeaturesTick/TS/DepthData.hpp"
#include "features/FeaturesTick/TS/MPB.hpp"
#include "features/FeaturesTick/TS/MPC.hpp"
#include "features/FeaturesTick/TS/MPCStat.hpp"
#include "features/FeaturesTick/TS/MicroPrice.hpp"
#include "features/FeaturesTick/TS/MidPrice.hpp"
#include "features/FeaturesTick/TS/OIR.hpp"
#include "features/FeaturesTick/TS/SOIR.hpp"
#include "features/FeaturesTick/TS/Spread.hpp"
#include "features/FeaturesTick/TS/TickIndex.hpp"
#include "features/FeaturesTick/TS/TradePrice.hpp"
#include "features/FeaturesTick/TS/VOI.hpp"
#include <deque>

// DAG: (静态多级)有向无环计算图 (Directed Acyclic Graph) ( L0 (Tick) -> L1 (Minute) -> L2 (Hour))
class DAG {
public:
  // ===========================================================================
  // 事件/时间驱动: 底层数据结构 (按计算层级排列, 作为计算图的"驱动时钟")
  // ===========================================================================
  TickData &tick_data;    // L0 输入（外部传入）
  MinuteData minute_data; // L1 输入（内部管理，由 resampler 填充）
  HourData hour_data;     // L2 输入（内部管理，由 resampler 填充）

  // ===========================================================================
  // L0: Tick 级别 - CBuffer + 算子
  // ===========================================================================
  struct L0 {
    TickData &td; // 唯一需要构造函数初始化的引用

    // -------------------------------------------------------------------------
    // [EVERY TICK] 逐笔更新 - 每个订单(增/删/改/成交)都触发
    // -------------------------------------------------------------------------
    CBuffer<float, L2::BLEN> DeltaTMaker_;
    CBuffer<float, L2::BLEN> DeltaTTaker_;
    CBuffer<float, L2::BLEN> DeltaTCancel_;
    DeltaT DeltaT{td, DeltaTMaker_, DeltaTTaker_, DeltaTCancel_};

    // -------------------------------------------------------------------------
    // [ON DEPTH] 盘口更新时 - depth_updated == true 时触发:
    // 1. depth更新间隔大于L2::L2_MIN_TIME_INTERVAL_MS (一般是一秒)
    // 2. 当天的第一个depth更新由taker订单驱动(交易所撮合机器在9:25:00释放早盘集合竞价成交订单, 持续几秒)
    // 3. 后续depth更新由"增/删/改/成交"任意订单触发(9:30开始, 9:25-9:30期间为订单真空期)
    // -------------------------------------------------------------------------

    // --- 基础数据 CBuffer ---
    CBuffer<float, L2::BLEN> Sec_;                     // 秒数 [0-59] (特征)
    CBuffer<float, L2::BLEN> TickIndex_;               // 原始tick索引 (供其他算子使用)
    CBuffer<float, L2::BLEN> BidPrice_[L2::LOB_DEPTH]; // 买1-N价 (元)
    CBuffer<float, L2::BLEN> AskPrice_[L2::LOB_DEPTH]; // 卖1-N价 (元)
    CBuffer<float, L2::BLEN> BidQty_[L2::LOB_DEPTH];   // 买1-N量 (股, 正值)
    CBuffer<float, L2::BLEN> AskQty_[L2::LOB_DEPTH];   // 卖1-N量 (股, 负值)
    CBuffer<float, L2::BLEN> BidAmt_[L2::LOB_DEPTH];   // 买1-N金额 (万元, 正值)
    CBuffer<float, L2::BLEN> AskAmt_[L2::LOB_DEPTH];   // 卖1-N金额 (万元, 负值)
    CBuffer<float, L2::BLEN> MidPrice_;                // 中间价 (元)
    CBuffer<float, L2::BLEN> MicroPrice_;              // 微价格 (元)
    CBuffer<float, L2::BLEN> Spread_;                  // 买卖价差 (元)

    // --- 基础数据算子 ---
    TickIndex TickIndex{td, Sec_, TickIndex_};
    DepthData<L2::LOB_DEPTH> DepthData{td, BidPrice_, AskPrice_, BidQty_, AskQty_, BidAmt_, AskAmt_};
    MidPrice MidPrice{BidPrice_[0], AskPrice_[0], MidPrice_};
    MicroPrice MicroPrice{td, MicroPrice_};
    Spread Spread{BidPrice_[0], AskPrice_[0], Spread_};

    // --- 因子 CBuffer ---
    CBuffer<float, L2::BLEN> VOI1_;      // VOI 1档
    CBuffer<float, L2::BLEN> VOI30_;     // VOI 30档加权 (线性衰减)
    CBuffer<float, L2::BLEN> OIR5_;      // OIR 5档加权比率
    CBuffer<float, L2::BLEN> OIR10_;     // OIR 10档加权
    CBuffer<float, L2::BLEN> SOIR5_;     // SOIR 5档加权
    CBuffer<float, L2::BLEN> SOIR5s_;    // SOIR 第5档单独 (研报:单档效果更好)
    CBuffer<float, L2::BLEN> SOIR10s_;   // SOIR 第10档单独
    CBuffer<float, L2::BLEN> SOIR30s_;   // SOIR 第30档单独 (深度信息)
    CBuffer<float, L2::BLEN> MPC1_;      // 中间价变化率 lag=1
    CBuffer<float, L2::BLEN> MPC5_;      // 中间价变化率 lag=5
    CBuffer<float, L2::BLEN> MPC5_Max_;  // MPC5日内最大值 (IC -9.39%)
    CBuffer<float, L2::BLEN> MPC5_Skew_; // MPC5日内偏度 (夏普3.07)

    // --- 因子算子 ---
    VOI<1> VOI1{BidPrice_, AskPrice_, BidAmt_, AskAmt_, TickIndex_, VOI1_};
    VOI<30> VOI30{BidPrice_, AskPrice_, BidAmt_, AskAmt_, TickIndex_, VOI30_};
    OIR<5> OIR5{BidAmt_, AskAmt_, OIR5_};
    OIR<10> OIR10{BidAmt_, AskAmt_, OIR10_};
    SOIR<5, false> SOIR5{BidAmt_, AskAmt_, SOIR5_};
    SOIR<5, true> SOIR5s{BidAmt_, AskAmt_, SOIR5s_};
    SOIR<10, true> SOIR10s{BidAmt_, AskAmt_, SOIR10s_};
    SOIR<30, true> SOIR30s{BidAmt_, AskAmt_, SOIR30s_};
    MPC<1> MPC1{MidPrice_, MPC1_};
    MPCWithStats<5> MPC5{MidPrice_, MPC5_, MPC5_Max_, MPC5_Skew_};

    // -------------------------------------------------------------------------
    // [ON TAKER] 成交时更新 - order_type == TAKER 时触发
    // -------------------------------------------------------------------------
    CBuffer<float, L2::BLEN> TradePrice_; // 成交价 (元单位)
    CBuffer<float, L2::BLEN> MPB_;        // 市价偏离度
    TradePrice TradePrice{td, TradePrice_};
    MPB MPB{MidPrice_, TradePrice_, MPB_};

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
    std::deque<float> rv_window;
    std::deque<float> vwap_window;
    std::deque<float> momentum_window;
    std::deque<float> momentum_returns;
    std::deque<std::pair<float, float>> range_window; // {range, close}

    explicit L1(MinuteData &m) : md(m) {}
  };
  L1 l1;

  // ===========================================================================
  // L2: Hour 级别 - 暂保持 deque（后续迁移到 CBuffer + 算子）
  // ===========================================================================
  struct L2 {
    HourData &hd;

    // --- 基础数据 CBuffer ---
    CBuffer<float, ::L2::BLEN> Hour_;      // 小时数 [0-23] (特征)
    CBuffer<float, ::L2::BLEN> HourIndex_; // 原始hour索引 (供其他算子使用)

    // --- 基础数据算子 ---
    HourIndex HourIndex{hd, Hour_, HourIndex_};

    // Rolling windows for TS features
    std::deque<float> hour_return_window;
    std::deque<float> hour_vol_window;
    std::deque<float> pivot_window;
    std::deque<float> dominant_window;

    explicit L2(HourData &h) : hd(h) {}
  };
  L2 l2;

  // ===========================================================================
  // 构造函数
  // ===========================================================================
  explicit DAG(TickData &td) : tick_data(td), l0(td), l1(minute_data), l2(hour_data) {} // 创建时传入TickData

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
