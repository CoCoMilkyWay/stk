#pragma once

// =============================================================================
// DepthData - 盘口数据提取层: depth_buffer 的 N 档 → 6 组独立 CBuffer, 供下游算子和盘口快照落盘复用
// =============================================================================
//   单位: 价格(元), 数量(股), 金额(万元); 卖方数量/金额存负值
//   布局: depth_buffer [0:N-1]=ask(N→1), [N:2N-1]=bid(1→N); 本类 bid_*[i]/ask_*[i] = 买/卖 i+1 档
//   涨跌停保护: 按代码推断涨跌幅 (主板10%, 科创/创业20%, 北交所30%), 超限档强制为边界价, qty=1股, amt=0.01万
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"

template <size_t N_LEVELS = L2::LOB_DEPTH>
class DepthData {
public:
  static constexpr float PRICE_SCALE = 0.01f; // Level->price 是0.01元(分)单位 → 转为元
  static constexpr float AMT_SCALE = 1e-4f;   // 元 → 万元
  static constexpr float LIMIT_QTY = 1.0f;    // 超限档位数量: 1股
  static constexpr float LIMIT_AMT = 0.01f;   // 超限档位金额: 0.01万元

  // 源层节点: 无标量输出口 (kCount = 0), 自持 6 组 N 档 CBuffer, 下游按 DepthData.bid_qty 等直接引用
  enum Out : size_t { kCount = 0 };

  DepthData(const TickData &tick_data,
            const CBuffer<float, L2::BLEN> &taker_price,
            const std::string &asset_code)
      : tick_data_(tick_data),
        limit_pct_(L2::infer_pct_limit(asset_code)),
        taker_price_(taker_price) {}

  CBuffer<float, L2::BLEN> bid_price[N_LEVELS];
  CBuffer<float, L2::BLEN> ask_price[N_LEVELS];
  CBuffer<float, L2::BLEN> bid_qty[N_LEVELS];
  CBuffer<float, L2::BLEN> ask_qty[N_LEVELS];
  CBuffer<float, L2::BLEN> bid_amt[N_LEVELS];
  CBuffer<float, L2::BLEN> ask_amt[N_LEVELS];

  // 跨天: 用前一天最后成交价设置涨跌停边界
  void reset() {
    float prev_close = taker_price_.size() > 0 ? taker_price_.back() : 0.0f;
    if (prev_close > 0.0f) {
      limit_up_ = prev_close * (1.0f + limit_pct_);
      limit_down_ = prev_close * (1.0f - limit_pct_);
      initialized_ = true;
    }
  }

  // compute 与 flush 同域 (onDepth) 且相邻, 直接推入 CBuffer
  inline void compute() {
    const auto &depth = tick_data_.lob.depth_buffer;
    // Level::price 是档位下标, 加上基准才是绝对价 (分). 低价股基准为 0.
    const float base = static_cast<float>(tick_data_.lob.price_base);

    // 第一天没有前收盘价: 用 mid 初始化涨跌停边界
    if (!initialized_) [[unlikely]] {
      const Level *bid1 = depth[L2::LOB_DEPTH];
      const Level *ask1 = depth[L2::LOB_DEPTH - 1];
      float mid = (base + (bid1->price + ask1->price) * 0.5f) * PRICE_SCALE;
      limit_up_ = mid * (1.0f + limit_pct_);
      limit_down_ = mid * (1.0f - limit_pct_);
      initialized_ = true;
    }

    for (size_t i = 0; i < N_LEVELS; ++i) {
      const Level *bid_level = depth[L2::LOB_DEPTH + i];     // 买i+1档
      const Level *ask_level = depth[L2::LOB_DEPTH - 1 - i]; // 卖i+1档

      float bp = (base + static_cast<float>(bid_level->price)) * PRICE_SCALE;
      float ap = (base + static_cast<float>(ask_level->price)) * PRICE_SCALE;
      float bq = static_cast<float>(bid_level->net_quantity);
      float aq = static_cast<float>(ask_level->net_quantity); // 负值
      float ba, aa;

      if (bp > limit_up_) [[unlikely]] {
        bp = limit_up_, bq = LIMIT_QTY, ba = LIMIT_AMT;
      } else if (bp < limit_down_) [[unlikely]] {
        bp = limit_down_, bq = LIMIT_QTY, ba = LIMIT_AMT;
      } else {
        ba = bp * bq * AMT_SCALE;
      }

      if (ap > limit_up_) [[unlikely]] {
        ap = limit_up_, aq = -LIMIT_QTY, aa = -LIMIT_AMT;
      } else if (ap < limit_down_) [[unlikely]] {
        ap = limit_down_, aq = -LIMIT_QTY, aa = -LIMIT_AMT;
      } else {
        aa = ap * aq * AMT_SCALE;
      }

      bid_price[i].push_back(bp);
      ask_price[i].push_back(ap);
      bid_qty[i].push_back(bq);
      ask_qty[i].push_back(aq);
      bid_amt[i].push_back(ba);
      ask_amt[i].push_back(aa);
    }
  }

private:
  const TickData &tick_data_;
  const float limit_pct_;
  float limit_up_ = 0.0f;
  float limit_down_ = 0.0f;
  bool initialized_ = false;
  const CBuffer<float, L2::BLEN> &taker_price_; // 前收盘价来源
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_DepthData(N) N(DepthData, (DepthData<L2::LOB_DEPTH>), (tick_data, Taker.out(Taker.price), asset_code_), onDepth, onDepth)
