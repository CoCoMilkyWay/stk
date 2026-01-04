#pragma once

#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"
#include "features/FeaturesTick/DeltaT.hpp"
#include "features/FeaturesTick/DepthData.hpp"
#include "features/FeaturesTick/MPB.hpp"
#include "features/FeaturesTick/MPC.hpp"
#include "features/FeaturesTick/MPCStat.hpp"
#include "features/FeaturesTick/MicroPrice.hpp"
#include "features/FeaturesTick/MidPrice.hpp"
#include "features/FeaturesTick/OIR.hpp"
#include "features/FeaturesTick/SOIR.hpp"
#include "features/FeaturesTick/Spread.hpp"
#include "features/FeaturesTick/TradePrice.hpp"
#include "features/FeaturesTick/VOI.hpp"
#include <deque>

// DAG: 静态多级有向无环计算图 (Directed Acyclic Graph) ( L0 (Tick) -> L1 (Minute) -> L2 (Hour))
class DAG {
public:
  // ===========================================================================
  // 事件/时间驱动: 底层数据结构 (按计算层级排列, 作为计算图的"时钟")
  // ===========================================================================
  TickData &tick_data;    // L0 输入（外部传入）
  MinuteData minute_data; // L1 输入（内部管理，由 resampler 填充）
  HourData hour_data;     // L2 输入（内部管理，由 resampler 填充）

  // ===========================================================================
  // L0: Tick 级别 - CBuffer + 算子
  // ===========================================================================
  struct L0 {
    // -------------------------------------------------------------------------
    // [EVERY TICK] 逐笔更新 - 每个订单都触发
    // -------------------------------------------------------------------------
    CBuffer<float, L2::BLEN> DeltaTMaker;
    CBuffer<float, L2::BLEN> DeltaTTaker;
    CBuffer<float, L2::BLEN> DeltaTCancel;
    DeltaT delta_t;

    // -------------------------------------------------------------------------
    // [ON DEPTH] 盘口更新时 - depth_updated == true 时触发:
    // 1. depth更新间隔大于L2::L2_MIN_TIME_INTERVAL_MS
    // 2. 当天的第一个depth更新由taker订单驱动(9:25:00早盘集合竞价成交订单释放, 最多持续十几秒)
    // 3. 后续depth更新由增删改成交任意订单触发(9:30开始, 9:25-9:30期间为订单真空期)
    // -------------------------------------------------------------------------

    // --- 基础数据 CBuffer ---
    CBuffer<float, L2::BLEN> BidPrice[L2::LOB_DEPTH]; // 买1-N价 (元)
    CBuffer<float, L2::BLEN> AskPrice[L2::LOB_DEPTH]; // 卖1-N价 (元)
    CBuffer<float, L2::BLEN> BidQty[L2::LOB_DEPTH];   // 买1-N量 (股, 正值)
    CBuffer<float, L2::BLEN> AskQty[L2::LOB_DEPTH];   // 卖1-N量 (股, 负值)
    CBuffer<float, L2::BLEN> BidAmt[L2::LOB_DEPTH];   // 买1-N金额 (万元, 正值)
    CBuffer<float, L2::BLEN> AskAmt[L2::LOB_DEPTH];   // 卖1-N金额 (万元, 负值)
    CBuffer<float, L2::BLEN> MidPrice;                // 中间价 (元)
    CBuffer<float, L2::BLEN> MicroPrice;              // 微观价格
    CBuffer<float, L2::BLEN> Spread;                  // 买卖价差 (元)

    // --- 基础数据算子 ---
    DepthData<L2::LOB_DEPTH> depth_data;
    class MidPrice mid_price;
    class MicroPrice micro_price;
    class Spread spread;

    // --- 因子 CBuffer ---
    CBuffer<float, L2::BLEN> VOI1;      // VOI 1档
    CBuffer<float, L2::BLEN> VOI30;     // VOI 30档加权 (线性衰减)
    CBuffer<float, L2::BLEN> OIR5;      // OIR 5档加权比率
    CBuffer<float, L2::BLEN> OIR10;     // OIR 10档加权
    CBuffer<float, L2::BLEN> SOIR5;     // SOIR 5档加权
    CBuffer<float, L2::BLEN> SOIR5s;    // SOIR 第5档单独 (研报:单档效果更好)
    CBuffer<float, L2::BLEN> SOIR10s;   // SOIR 第10档单独
    CBuffer<float, L2::BLEN> SOIR30s;   // SOIR 第30档单独 (深度信息)
    CBuffer<float, L2::BLEN> MPB;       // 市价偏离度
    CBuffer<float, L2::BLEN> MPC1;      // 中间价变化率 lag=1
    CBuffer<float, L2::BLEN> MPC5;      // 中间价变化率 lag=5
    CBuffer<float, L2::BLEN> MPC5_Max;  // MPC5日内最大值 (IC -9.39%)
    CBuffer<float, L2::BLEN> MPC5_Skew; // MPC5日内偏度 (夏普3.07)

    // --- 因子算子 ---
    VOI<1> voi1;
    VOI<30> voi30;
    OIR<5> oir5;
    OIR<10> oir10;
    SOIR<5, false> soir5;   // 加权
    SOIR<5, true> soir5s;   // 单档
    SOIR<10, true> soir10s; // 单档
    SOIR<30, true> soir30s; // 单档深度
    MPC<1> mpc1;
    MPCWithStats<5> mpc5;

    // -------------------------------------------------------------------------
    // [ON TAKER] 成交时更新 - order_type == TAKER 时触发
    // -------------------------------------------------------------------------
    CBuffer<float, L2::BLEN> TradePrice; // 成交价 (元单位)
    class TradePrice trade_price;
    class MPB mpb; // 依赖 TradePrice

    // -------------------------------------------------------------------------
    // 构造函数 (按声明顺序初始化)
    // -------------------------------------------------------------------------
    explicit L0(TickData &td)
        : // [EVERY TICK]
          delta_t{td, DeltaTMaker, DeltaTTaker, DeltaTCancel},
          // [ON DEPTH] 基础数据算子
          depth_data{td, BidPrice, AskPrice, BidQty, AskQty, BidAmt, AskAmt},
          mid_price{BidPrice[0], AskPrice[0], MidPrice},
          micro_price{td, MicroPrice},
          spread{BidPrice[0], AskPrice[0], Spread},
          // [ON DEPTH] 因子算子
          voi1{BidPrice, AskPrice, BidAmt, AskAmt, VOI1},
          voi30{BidPrice, AskPrice, BidAmt, AskAmt, VOI30},
          oir5{BidAmt, AskAmt, OIR5},
          oir10{BidAmt, AskAmt, OIR10},
          soir5{BidAmt, AskAmt, SOIR5},
          soir5s{BidAmt, AskAmt, SOIR5s},
          soir10s{BidAmt, AskAmt, SOIR10s},
          soir30s{BidAmt, AskAmt, SOIR30s},
          mpc1{MidPrice, MPC1},
          mpc5{MidPrice, MPC5, MPC5_Max, MPC5_Skew},
          // [ON TAKER]
          trade_price{td, TradePrice},
          mpb{MidPrice, TradePrice, MPB} {}
  };
  L0 l0;

  // ===========================================================================
  // L1: Minute 级别 - 暂保持 deque（后续迁移到 CBuffer + 算子）
  // ===========================================================================
  struct L1 {
    // Rolling windows for TS features
    std::deque<float> minute_return_window;
    std::deque<float> rv_window;
    std::deque<float> vwap_window;
    std::deque<float> momentum_window;
    std::deque<float> momentum_returns;
    std::deque<std::pair<float, float>> range_window; // {range, close}
  };
  L1 l1;

  // ===========================================================================
  // L2: Hour 级别 - 暂保持 deque（后续迁移到 CBuffer + 算子）
  // ===========================================================================
  struct L2 {
    // Rolling windows for TS features
    std::deque<float> hour_return_window;
    std::deque<float> hour_vol_window;
    std::deque<float> pivot_window;
    std::deque<float> dominant_window;
  };
  L2 l2;

  // ===========================================================================
  // 构造函数
  // ===========================================================================
  explicit DAG(TickData &td) : tick_data(td), l0(td) {} // 创建时传入TickData

  // ===========================================================================
  // 跨天重置
  // ===========================================================================
  void set_prev_close(float prev_close) {
    l0.depth_data.set_prev_close(prev_close);
  }

  float get_last_trade_price() const {
    return l0.TradePrice.size() > 0 ? l0.TradePrice.back() : 0.0f;
  }
};
