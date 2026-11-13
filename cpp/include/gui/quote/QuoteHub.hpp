// 行情: 新浪当日 + 历史价格库, Market 任务各页 (ASSET 个股 / NAV · VAL · FLOW · CTA 指数 / 行业) 与 Live 持仓页共用一份;
//   Database 的 Sync 页只读新浪状态. CreateAllTasks 建, 各任务各持一个 shared_ptr.
//   首次用到才起 (之前不碰网络); 价格库的交接交给新浪 ⇒ 依赖者后建先拆 (成员序即析构序)
//   一帧一份 (frame 取, 各处只读): 新浪状态 / 当日 (Live) / 历史 (PriceView); 读行情只经 joint / of (接续判据只此一处)
//   要的历史各处每帧登记 (want_kline / want_minutes), 下帧首并成一个价格库请求 (连同基本面段末日 / 行情日), 变了才发;
//     价格库发布的是它留着的全部 (不只本帧要的), 切页不闪
#pragma once

#include "gui/quote/PriceLib.hpp"
#include "gui/quote/services/QuoteService.hpp"

#include <cstdint>
#include <memory>
#include <optional>
#include <span>

struct SharedData;

namespace GUI::Quote {

struct QuoteHub {
  // 本帧 (GUI 线程, frame 取)
  QuoteStatus status;
  std::shared_ptr<const Live> live;      // 空 = 新浪未到
  std::shared_ptr<const PriceView> view; // 空 = 价格库未发布
  // 每帧用前调 (AssetInfo 已建, task_state.database.asset_info_ready; 一帧多调只首次生效): 首次起全部服务 (全市场 = 在市股);
  //   上帧登记的要求 + 基本面段末日 + 行情日 → 价格库请求; 基本面段末日 → 新浪; 取本帧三份
  void frame(const SharedData &data);
  void want_kline(uint32_t id); // 本帧要个股 id 的全历史
  void want_minutes(Ref r);     // 本帧要 r 的分钟价

  Joint joint(Side s) const { return {s, view, live}; }
  Source of(Ref r) const { return joint(r.side).of(r.key); }
  const Tape *tape(Side s) const { return joint(s).tape(); } // 价格库用不了 / 未发布 → nullptr

  // 服务 (任意时刻; 以下除 started 须已起)
  bool started() const { return quote_ != nullptr; }
  QuoteStatus sina() const { return quote_->status(); }                                        // 新浪此刻的状态 (不等 frame)
  const Progress &progress() const { return *price_; }                                         // 价格库的进度
  std::span<const misc::Ymd> trading() const { return quote_->trading(); }                     // 交易日升序
  bool listed(uint32_t id) const { return quote_->axis().sub_of(id) < quote_->axis().size(); } // 在新浪全市场
  void select(uint32_t id) { quote_->select(id); }                                             // 选中标的 (~0u = 无): 新浪先补它的分钟 K

private:
  std::unique_ptr<QuoteService> quote_;
  std::unique_ptr<PriceLib> price_;
  int frame_ = -1;
  PriceRequest want_;
  std::optional<PriceRequest> sent_;
  misc::Ymd mark_ = 0; // 交给新浪的基本面段末日
};

} // namespace GUI::Quote
