#pragma once

// =============================================================================
// Auction - 开盘集合竞价两阶段分账 (compute=onTick, flush=onMinute 全日广播)
// =============================================================================
//   𝒯₁ = [09:15, 09:20) 可撤单阶段, 𝒯₂ = [09:20, 09:25) 不可撤单阶段 (沪深皆不接受撤单申报, 故无二阶段撤单口).
//   竞价期无成交 → 09:15-09:24 分钟从不 flush, Flow 的委托 / 撤单全部并入 09:25 行 (l1=10), 两阶段与 09:20 时刻状态在那里不可分;
//   本节点按阶段分账, 09:25 起冻结, 之后每分钟原值广播 (与 Fund 日频广播同法; 全日任一行可读当日竞价特征).
//   订单流 (分阶段, 分侧):
//     {vol,n}_maker_{bid,ask}_auc{1,2}   新增委托 量 / 笔
//     {vol,n}_cancel_{bid,ask}_auc1      撤单 量 / 笔 (仅 𝒯₁)
//   盘口 (j = 竞价期盘口更新: LOB 每秒至多一次整簿重建 + 预撮合; P^{ref} = 最大成交量价位, 仅簿交叉时有值):
//     n_snap_auc{1,2}                    盘口更新次数 (活跃秒数代理; 长江 003 "快照数量")
//     n_snap_{up,dn}_auc{1,2}            参考价较上一次有值更新 上行 / 下行 次数 (跨阶段连续比较)
//     px_auc{1,2}                        阶段末参考价 (元; 09:20 / 09:25 时刻价)
//     px_{max,min}_auc{1,2}              阶段内参考价极值 (元; 振幅 = max / min − 1, 因子层)
//     qty_match_auc{1,2}                 阶段末参考价位虚拟匹配量 (股)
//     qty_imb_auc{1,2}                   阶段末参考价位未匹配量 (股; 正 = 买剩, 负 = 卖剩)
//   阶段内无交叉盘口 (单边簿 / 未交叉) → px / qty 五口 NaN (无定义), 计数口自然为 0.
//   不单列: 开盘成交量 / 额 = Flow 09:25 行 taker 口; 收盘竞价 14:57-15:00 = Flow 末行 (l1=254, 哨兵秒映射恰为该时段);
//   成交率 / 撤单率 / 涨跌幅 / 振幅 / 委比 = 因子层按口相除 (广发 海量level二 004, 长江 003, 广发 048 集合竞价族原料在此齐备).
//   fp16 落盘: 量 / 笔 / 股 用 Log Tf (保号, qty_imb 可负); 价 原值.
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "features/DataDefine.hpp"
#include "features/TimeIndex.hpp"
#include <algorithm>
#include <cstdint>

class Auction {
  static constexpr uint32_t PHASE_L0 = Clock_to_L0(9, 20, 0); // 𝒯₂ 起点
  static constexpr uint32_t END_L0 = Clock_to_L0(9, 25, 0);   // 竞价结束, 之后状态冻结
  static_assert(PHASE_L0 == 300 && END_L0 == 600);

public:
  // 布局: 同口两阶段相邻 (auc1, auc2); 分侧口内层 (bid, ask) —— y[base + k·2 + s]; 单口 y[base + k]
  enum Out : size_t {
    vol_maker_bid_auc1,
    vol_maker_ask_auc1,
    vol_maker_bid_auc2,
    vol_maker_ask_auc2,
    n_maker_bid_auc1,
    n_maker_ask_auc1,
    n_maker_bid_auc2,
    n_maker_ask_auc2,
    vol_cancel_bid_auc1,
    vol_cancel_ask_auc1,
    n_cancel_bid_auc1,
    n_cancel_ask_auc1,
    n_snap_auc1,
    n_snap_auc2,
    n_snap_up_auc1,
    n_snap_up_auc2,
    n_snap_dn_auc1,
    n_snap_dn_auc2,
    px_auc1,
    px_auc2,
    px_max_auc1,
    px_max_auc2,
    px_min_auc1,
    px_min_auc2,
    qty_match_auc1,
    qty_match_auc2,
    qty_imb_auc1,
    qty_imb_auc2,
    kCount
  };
  static_assert(vol_maker_ask_auc2 == vol_maker_bid_auc1 + 3 && n_maker_ask_auc2 == n_maker_bid_auc1 + 3);
  static_assert(qty_imb_auc2 == n_snap_auc1 + 15);
  float y[kCount] = {};

  explicit Auction(const TickData &td) : td_(td) {}

  inline void compute() {
    const uint32_t l0 = td_.l0_index;
    if (l0 >= END_L0) [[likely]]
      return;                                // 09:25 起冻结 (收盘竞价 14:57-15:00 映射到哨兵秒, 亦在此外)
    const size_t k = l0 >= PHASE_L0 ? 1 : 0; // 阶段下标
    const auto &lob = td_.lob;
    const size_t s = lob.order_dir == L2::OrderDirection::BID ? 0 : 1;
    const float v = static_cast<float>(lob.volume);

    switch (lob.order_type) {
    case L2::OrderType::MAKER:
      maker_vol_[k][s] += v, maker_n_[k][s] += 1.0f;
      break;
    case L2::OrderType::CANCEL:
      if (k == 0) // 𝒯₂ 交易所不接受撤单申报
        cancel_vol_[s] += v, cancel_n_[s] += 1.0f;
      break;
    default:
      break;
    }

    if (lob.depth_updated) { // 本笔触发了竞价整簿重建 (update_depth 先于本域, 预撮合口已是本次值)
      snap_n_[k] += 1.0f;
      const float ref = lob.auction_ref_price;
      if (ref > 0.0f) { // 簿交叉才有参考价
        if (px_prev_ > 0.0f) {
          snap_up_[k] += ref > px_prev_ ? 1.0f : 0.0f;
          snap_dn_[k] += ref < px_prev_ ? 1.0f : 0.0f;
        }
        px_prev_ = px_end_[k] = ref;
        px_max_[k] = has_px_[k] ? std::max(px_max_[k], ref) : ref;
        px_min_[k] = has_px_[k] ? std::min(px_min_[k], ref) : ref;
        has_px_[k] = true;
        match_[k] = static_cast<float>(lob.auction_matched_qty);
        imb_[k] = static_cast<float>(lob.auction_imbalance);
      }
    }
  }

  // 每分钟原值广播 (09:25 后状态不再变化)
  inline void flush() {
    for (size_t k = 0; k < 2; ++k) {
      for (size_t s = 0; s < 2; ++s) {
        y[vol_maker_bid_auc1 + k * 2 + s] = maker_vol_[k][s];
        y[n_maker_bid_auc1 + k * 2 + s] = maker_n_[k][s];
      }
      y[n_snap_auc1 + k] = snap_n_[k];
      y[n_snap_up_auc1 + k] = snap_up_[k];
      y[n_snap_dn_auc1 + k] = snap_dn_[k];
      const bool ok = has_px_[k];
      y[px_auc1 + k] = ok ? px_end_[k] : kNaN;
      y[px_max_auc1 + k] = ok ? px_max_[k] : kNaN;
      y[px_min_auc1 + k] = ok ? px_min_[k] : kNaN;
      y[qty_match_auc1 + k] = ok ? match_[k] : kNaN;
      y[qty_imb_auc1 + k] = ok ? imb_[k] : kNaN;
    }
    for (size_t s = 0; s < 2; ++s) {
      y[vol_cancel_bid_auc1 + s] = cancel_vol_[s];
      y[n_cancel_bid_auc1 + s] = cancel_n_[s];
    }
  }

  void reset() {
    for (size_t k = 0; k < 2; ++k) {
      maker_vol_[k][0] = maker_vol_[k][1] = maker_n_[k][0] = maker_n_[k][1] = 0.0f;
      snap_n_[k] = snap_up_[k] = snap_dn_[k] = 0.0f;
      px_end_[k] = px_max_[k] = px_min_[k] = match_[k] = imb_[k] = 0.0f;
      has_px_[k] = false;
    }
    cancel_vol_[0] = cancel_vol_[1] = cancel_n_[0] = cancel_n_[1] = 0.0f;
    px_prev_ = 0.0f;
  }

private:
  const TickData &td_;

  float maker_vol_[2][2] = {}, maker_n_[2][2] = {}; // [阶段][侧]
  float cancel_vol_[2] = {}, cancel_n_[2] = {};     // [侧], 仅 𝒯₁
  float snap_n_[2] = {}, snap_up_[2] = {}, snap_dn_[2] = {};
  float px_end_[2] = {}, px_max_[2] = {}, px_min_[2] = {};
  float match_[2] = {}, imb_[2] = {};
  bool has_px_[2] = {};
  float px_prev_ = 0.0f; // 上一次有值参考价 (跨阶段连续, 供 up / dn 计数)
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Auction(N) N(Auction, (Auction), (tick_data), onTick, onMinute)

// 一个阶段 k (数字 token; K = 阶段中文, T = 时段字面) 的新增委托 量 / 笔 × 买 / 卖 4 行
#define AUCTION_MAKER_ROWS(X, CAT1, k, K, T)                                                                                                                                                                                                                          \
  X(vol_maker_bid_auc##k, CAT1, AUTO, "Auction P" #k " Maker Bid Volume", "竞价" K "阶段买委托量", "开盘集合竞价" K "阶段(" T ")新增买委托量(股); 全日广播", R"(\sum_{\tau \in \mathcal{T}_)" #k R"(} |O_\tau^{M,B}|)", OP(Auction, vol_maker_bid_auc##k, Log, None)) \
  X(vol_maker_ask_auc##k, CAT1, AUTO, "Auction P" #k " Maker Ask Volume", "竞价" K "阶段卖委托量", "开盘集合竞价" K "阶段(" T ")新增卖委托量(股); 全日广播", R"(\sum_{\tau \in \mathcal{T}_)" #k R"(} |O_\tau^{M,A}|)", OP(Auction, vol_maker_ask_auc##k, Log, None)) \
  X(n_maker_bid_auc##k, CAT1, AUTO, "Auction P" #k " Maker Bid Count", "竞价" K "阶段买委托笔数", "开盘集合竞价" K "阶段(" T ")新增买委托笔数; 全日广播", R"(\#O_{\mathcal{T}_)" #k R"(}^{M,B})", OP(Auction, n_maker_bid_auc##k, Log, None))                         \
  X(n_maker_ask_auc##k, CAT1, AUTO, "Auction P" #k " Maker Ask Count", "竞价" K "阶段卖委托笔数", "开盘集合竞价" K "阶段(" T ")新增卖委托笔数; 全日广播", R"(\#O_{\mathcal{T}_)" #k R"(}^{M,A})", OP(Auction, n_maker_ask_auc##k, Log, None))

// 一个阶段 k 的盘口 8 行: 更新计数 3 + 参考价 3 + 匹配量 2
#define AUCTION_SNAP_ROWS(X, CAT1, k, K, T)                                                                                                                                                                                                                                                  \
  X(n_snap_auc##k, CAT1, AUTO, "Auction P" #k " Snapshot Count", "竞价" K "阶段盘口更新数", "开盘集合竞价" K "阶段(" T ")盘口更新次数(每秒至多1次, 活跃秒数代理); 全日广播", R"(\#\{j \in \mathcal{T}_)" #k R"(\})", OP(Auction, n_snap_auc##k, Log, None))                                  \
  X(n_snap_up_auc##k, CAT1, AUTO, "Auction P" #k " Ref-Price Up Count", "竞价" K "阶段参考价上行数", "开盘集合竞价" K "阶段(" T ")参考价高于上一次有值更新的次数; 全日广播", R"(\#\{j \in \mathcal{T}_)" #k R"( : P^{ref}_j > P^{ref}_{j-1}\})", OP(Auction, n_snap_up_auc##k, Log, None))   \
  X(n_snap_dn_auc##k, CAT1, AUTO, "Auction P" #k " Ref-Price Down Count", "竞价" K "阶段参考价下行数", "开盘集合竞价" K "阶段(" T ")参考价低于上一次有值更新的次数; 全日广播", R"(\#\{j \in \mathcal{T}_)" #k R"( : P^{ref}_j < P^{ref}_{j-1}\})", OP(Auction, n_snap_dn_auc##k, Log, None)) \
  X(px_auc##k, CAT1, AUTO, "Auction P" #k " Ref Price", "竞价" K "阶段末参考价", "开盘集合竞价" K "阶段(" T ")末次盘口更新的预撮合参考价(元, 最大成交量价位); 阶段内无交叉盘口→NaN; 全日广播", R"(P^{ref}_{j^{end}_)" #k R"(})", OP(Auction, px_auc##k, None, None))                         \
  X(px_max_auc##k, CAT1, AUTO, "Auction P" #k " Ref Price Max", "竞价" K "阶段参考价最高", "开盘集合竞价" K "阶段(" T ")预撮合参考价极大值(元); 无交叉→NaN; 全日广播", R"(\max_{j \in \mathcal{T}_)" #k R"(} P^{ref}_j)", OP(Auction, px_max_auc##k, None, None))                            \
  X(px_min_auc##k, CAT1, AUTO, "Auction P" #k " Ref Price Min", "竞价" K "阶段参考价最低", "开盘集合竞价" K "阶段(" T ")预撮合参考价极小值(元); 无交叉→NaN; 全日广播", R"(\min_{j \in \mathcal{T}_)" #k R"(} P^{ref}_j)", OP(Auction, px_min_auc##k, None, None))                            \
  X(qty_match_auc##k, CAT1, AUTO, "Auction P" #k " Matched Qty", "竞价" K "阶段末匹配量", "开盘集合竞价" K "阶段(" T ")末次更新参考价位的虚拟匹配量(股); 无交叉→NaN; 全日广播", R"(V^{match}_{j^{end}_)" #k R"(})", OP(Auction, qty_match_auc##k, Log, None))                                \
  X(qty_imb_auc##k, CAT1, AUTO, "Auction P" #k " Unmatched Qty", "竞价" K "阶段末未匹配量", "开盘集合竞价" K "阶段(" T ")末次更新参考价位的未匹配量(股, 正=买剩 负=卖剩); 无交叉→NaN; 全日广播", R"(V^{B}_{j^{end}_)" #k R"(} - V^{A}_{j^{end}_)" #k R"(})", OP(Auction, qty_imb_auc##k, Log, None))

#define FIELDS_L1_Auction(X, CAT1)                                                                                                                                                                                                                                   \
  AUCTION_MAKER_ROWS(X, CAT1, 1, "一", "09:15-09:20")                                                                                                                                                                                                                \
  AUCTION_MAKER_ROWS(X, CAT1, 2, "二", "09:20-09:25")                                                                                                                                                                                                                \
  X(vol_cancel_bid_auc1, CAT1, AUTO, "Auction P1 Cancel Bid Volume", "竞价一阶段买撤单量", "开盘集合竞价一阶段(09:15-09:20)买撤单量(股; 09:20后不可撤单); 全日广播", R"(\sum_{\tau \in \mathcal{T}_1} |O_\tau^{C,B}|)", OP(Auction, vol_cancel_bid_auc1, Log, None)) \
  X(vol_cancel_ask_auc1, CAT1, AUTO, "Auction P1 Cancel Ask Volume", "竞价一阶段卖撤单量", "开盘集合竞价一阶段(09:15-09:20)卖撤单量(股; 09:20后不可撤单); 全日广播", R"(\sum_{\tau \in \mathcal{T}_1} |O_\tau^{C,A}|)", OP(Auction, vol_cancel_ask_auc1, Log, None)) \
  X(n_cancel_bid_auc1, CAT1, AUTO, "Auction P1 Cancel Bid Count", "竞价一阶段买撤单笔数", "开盘集合竞价一阶段(09:15-09:20)买撤单笔数; 全日广播", R"(\#O_{\mathcal{T}_1}^{C,B})", OP(Auction, n_cancel_bid_auc1, Log, None))                                          \
  X(n_cancel_ask_auc1, CAT1, AUTO, "Auction P1 Cancel Ask Count", "竞价一阶段卖撤单笔数", "开盘集合竞价一阶段(09:15-09:20)卖撤单笔数; 全日广播", R"(\#O_{\mathcal{T}_1}^{C,A})", OP(Auction, n_cancel_ask_auc1, Log, None))                                          \
  AUCTION_SNAP_ROWS(X, CAT1, 1, "一", "09:15-09:20")                                                                                                                                                                                                                 \
  AUCTION_SNAP_ROWS(X, CAT1, 2, "二", "09:20-09:25")
