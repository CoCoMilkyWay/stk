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
#include "features/backend/FeatureStore.hpp"
#include "features/backend/FeatureStoreConfig.hpp"
// #include <iostream>

// Tick-level sequential feature computation
class Tick_Sequential {
public:
  Tick_Sequential(const TickData &tick_data,
                  GlobalFeatureStore &store,
                  size_t asset_id,
                  size_t worker_id)
      : tick_data_(tick_data),
        store_(&store),
        asset_id_(asset_id),
        worker_id_(worker_id) {}

  void set_date(const std::string &date_str) {
    if (TradePrice_.size() > 0) {
      depth_data_.set_prev_close(TradePrice_.back()); // 跨天: 用前一天收盘价(最后成交价)设置涨跌停保护
    }
    date_str_ = date_str;
  }

  // Main computation entry (called by CoreSequential)
  void compute_and_store() {

    // Compute and write tick-level TS features
    compute_ts_tick(tick_data_.l0_index);

    // Write LOB depth snapshot for GUI (META features)
    write_lob_depth(tick_data_.l0_index);
  }

private:
  // Level 0: Tick-level TS features computation
  void compute_ts_tick(size_t t) {
    delta_t_.compute();

    if (tick_data_.lob.depth_updated) {
      // =============================================================
      // 数据层: 基础数据提取 (每tick只算一次, 供下游因子复用)
      // =============================================================
      depth_data_.compute();  // N档price/qty → 4N个CBuffer
      mid_price_.compute();   // 中间价
      micro_price_.compute(); // 微观价格
      spread_.compute();      // 买卖价差 (依赖BidPrice_[0], AskPrice_[0])
      trade_price_.compute(); // 成交价

      // =============================================================
      // 因子层: 从共享CBuffer读取, 无重复计算
      // =============================================================
      // 订单量类因子
      voi1_.compute();    // VOI 1档
      voi30_.compute();   // VOI 30档
      oir5_.compute();    // OIR 5档比率
      oir10_.compute();   // OIR 10档
      soir5_.compute();   // SOIR 5档加权
      soir5s_.compute();  // SOIR 第5档单独
      soir10s_.compute(); // SOIR 第10档单独
      soir30s_.compute(); // SOIR 第30档单独

      // 价格类因子
      mpb_.compute();  // 市价偏离度 (从MidPrice/TradePrice读)
      mpc1_.compute(); // 中间价变化率 lag=1 (从MidPrice读)
      mpc5_.compute(); // 中间价变化率 lag=5 + 日内max/skew

      // 写入因子到输出缓冲区 (顺序与 LEVEL_0_FIELDS 定义一致)
      // 订单量类因子 [0-7]
      ts_features_buffer_[0] = VOI1_Buffer_.back();
      ts_features_buffer_[1] = VOI30_Buffer_.back();
      ts_features_buffer_[2] = OIR5_Buffer_.back();
      ts_features_buffer_[3] = OIR10_Buffer_.back();
      ts_features_buffer_[4] = SOIR5_Buffer_.back();
      ts_features_buffer_[5] = SOIR5s_Buffer_.back();
      ts_features_buffer_[6] = SOIR10s_Buffer_.back();
      ts_features_buffer_[7] = SOIR30s_Buffer_.back();
      ts_features_buffer_[8] = MPB_Buffer_.back();
      ts_features_buffer_[9] = MPC1_Buffer_.back();
      ts_features_buffer_[10] = MPC5_Buffer_.back();
      ts_features_buffer_[11] = MPC5_Max_Buffer_.back();
      ts_features_buffer_[12] = MPC5_Skew_Buffer_.back();
    };

    // Write TS features [voi1, mpc5_skew]
    TS_WRITE_FEATURES(store_, date_str_, 0, t, asset_id_, 0, L0_FieldOffset::mpc5_skew, ts_features_buffer_, worker_id_);

    // Write data validity flag (event-driven sparsity marker)
    TS_WRITE_SINGLE(store_, date_str_, 0, t, L0_FieldOffset::_data_valid, asset_id_, 1.0f, worker_id_);
    DEPTH_WRITE_SINGLE(store_, date_str_, t, DepthFieldOffset::_data_valid, asset_id_, 1.0f, worker_id_);
  }

  // Write LOB depth snapshot (N levels bid/ask price/volume for GUI)
  // 从已提取的 CBuffer 读取，无重复访问 depth_buffer
  // 输出单位: 价格=元, 数量=手
  void write_lob_depth(size_t t) {
    if (!tick_data_.lob.depth_updated)
      return;

    constexpr size_t N = L2::LOB_DEPTH;
    constexpr float VOLUME_TO_LOT = 0.01f; // 股 → 手 (1手=100股)

    // 从 CBuffer 读取 (depth_data_.compute() 已在 compute_ts_tick 中调用)
    // 价格已是元, 数量需转为手
    for (size_t i = 0; i < N; ++i) {
      lob_depth_buffer_[i] = BidPrice_[i].back();
      lob_depth_buffer_[N + i] = AskPrice_[i].back();
      lob_depth_buffer_[2 * N + i] = BidQty_[i].back() * VOLUME_TO_LOT;
      lob_depth_buffer_[3 * N + i] = AskQty_[i].back() * VOLUME_TO_LOT;
    }

    // Mid_price: 已是元单位
    lob_depth_buffer_[4 * N] = MidPrice_.back();
    lob_depth_buffer_[4 * N + 1] = 1.0f;

    // Batch write [_bid_price, _depth_valid]
    DEPTH_WRITE_FEATURES(store_, date_str_, t, asset_id_, 0, DepthFieldOffset::_depth_valid, lob_depth_buffer_, worker_id_);
  }

  // Get mid price from CBuffer (元单位, 已在数据层转换)
  float get_mid_price() const {
    if (MidPrice_.size() == 0)
      return 0.0f;
    return MidPrice_.back();
  }

  // Get spread from CBuffer (元单位, 已在数据层转换)
  float get_spread() const {
    if (Spread_.size() == 0)
      return 0.0f;
    return Spread_.back();
  }

  // ===========================================================================
  // 静态配置 (初始化时设置)
  // ===========================================================================
  const TickData &tick_data_;
  GlobalFeatureStore *store_ = nullptr;
  size_t asset_id_ = 0;
  size_t worker_id_ = 0;
  std::string date_str_;

  // ===========================================================================
  // [EVERY TICK] 逐笔更新 - 每个订单都触发
  // ===========================================================================
  CBuffer<float, L2::BLEN> DeltaTMaker_;
  CBuffer<float, L2::BLEN> DeltaTTaker_;
  CBuffer<float, L2::BLEN> DeltaTCancel_;
  DeltaT delta_t_{tick_data_, DeltaTMaker_, DeltaTTaker_, DeltaTCancel_};

  // ===========================================================================
  // [ON DEPTH] 盘口更新时 - depth_updated == true 时触发
  // ===========================================================================

  // --- 基础数据类 ---
  CBuffer<float, L2::BLEN> BidPrice_[L2::LOB_DEPTH]; // 买1-N价 (元)
  CBuffer<float, L2::BLEN> AskPrice_[L2::LOB_DEPTH]; // 卖1-N价 (元)
  CBuffer<float, L2::BLEN> BidQty_[L2::LOB_DEPTH];   // 买1-N量 (股, 正值)
  CBuffer<float, L2::BLEN> AskQty_[L2::LOB_DEPTH];   // 卖1-N量 (股, 负值)
  CBuffer<float, L2::BLEN> BidAmt_[L2::LOB_DEPTH];   // 买1-N金额 (万元, 正值)
  CBuffer<float, L2::BLEN> AskAmt_[L2::LOB_DEPTH];   // 卖1-N金额 (万元, 负值)
  CBuffer<float, L2::BLEN> MidPrice_;                // 中间价 (元)
  CBuffer<float, L2::BLEN> MicroPrice_;              // 微观价格
  CBuffer<float, L2::BLEN> Spread_;                  // 买卖价差 (元)

  DepthData<L2::LOB_DEPTH> depth_data_{tick_data_, BidPrice_, AskPrice_, BidQty_, AskQty_, BidAmt_, AskAmt_};
  MidPrice mid_price_{BidPrice_[0], AskPrice_[0], MidPrice_};
  MicroPrice micro_price_{tick_data_, MicroPrice_};
  Spread spread_{BidPrice_[0], AskPrice_[0], Spread_};

  // --- 因子计算类 ---
  CBuffer<float, L2::BLEN> VOI1_Buffer_;      // VOI 1档
  CBuffer<float, L2::BLEN> VOI30_Buffer_;     // VOI 30档加权 (线性衰减)
  CBuffer<float, L2::BLEN> OIR5_Buffer_;      // OIR 5档加权比率
  CBuffer<float, L2::BLEN> OIR10_Buffer_;     // OIR 10档加权
  CBuffer<float, L2::BLEN> SOIR5_Buffer_;     // SOIR 5档加权
  CBuffer<float, L2::BLEN> SOIR5s_Buffer_;    // SOIR 第5档单独 (研报:单档效果更好)
  CBuffer<float, L2::BLEN> SOIR10s_Buffer_;   // SOIR 第10档单独
  CBuffer<float, L2::BLEN> SOIR30s_Buffer_;   // SOIR 第30档单独 (深度信息)
  CBuffer<float, L2::BLEN> MPB_Buffer_;       // 市价偏离度
  CBuffer<float, L2::BLEN> MPC1_Buffer_;      // 中间价变化率 lag=1
  CBuffer<float, L2::BLEN> MPC5_Buffer_;      // 中间价变化率 lag=5
  CBuffer<float, L2::BLEN> MPC5_Max_Buffer_;  // MPC5日内最大值 (IC -9.39%)
  CBuffer<float, L2::BLEN> MPC5_Skew_Buffer_; // MPC5日内偏度 (夏普3.07)

  VOI<1> voi1_{BidPrice_, AskPrice_, BidAmt_, AskAmt_, VOI1_Buffer_};
  VOI<30> voi30_{BidPrice_, AskPrice_, BidAmt_, AskAmt_, VOI30_Buffer_};
  OIR<5> oir5_{BidAmt_, AskAmt_, OIR5_Buffer_};
  OIR<10> oir10_{BidAmt_, AskAmt_, OIR10_Buffer_};
  SOIR<5, false> soir5_{BidAmt_, AskAmt_, SOIR5_Buffer_};     // 加权
  SOIR<5, true> soir5s_{BidAmt_, AskAmt_, SOIR5s_Buffer_};    // 单档
  SOIR<10, true> soir10s_{BidAmt_, AskAmt_, SOIR10s_Buffer_}; // 单档
  SOIR<30, true> soir30s_{BidAmt_, AskAmt_, SOIR30s_Buffer_}; // 单档深度
  MPC<1> mpc1_{MidPrice_, MPC1_Buffer_};
  MPCWithStats<5> mpc5_{MidPrice_, MPC5_Buffer_, MPC5_Max_Buffer_, MPC5_Skew_Buffer_};

  // ===========================================================================
  // [ON TAKER] 成交时更新 - order_type == TAKER 时触发
  // ===========================================================================
  CBuffer<float, L2::BLEN> TradePrice_; // 成交价 (元单位)
  TradePrice trade_price_{tick_data_, TradePrice_};
  MPB mpb_{MidPrice_, TradePrice_, MPB_Buffer_}; // 依赖 TradePrice

  // ===========================================================================
  // 输出缓冲区 (批量写入优化)
  // ===========================================================================
  // TS因子数量: 从 FeaturesDefine.hpp 自动推导 (L0中所有TS类型字段)
  float ts_features_buffer_[L0_TS_WIDTH];
  float lob_depth_buffer_[4 * L2::LOB_DEPTH + 2];
};
