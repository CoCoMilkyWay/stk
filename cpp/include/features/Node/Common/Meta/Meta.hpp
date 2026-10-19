#pragma once
// MetaTracker: 基建标志 ts_valid / cs_valid 的状态机 (两档共用; 落盘列 FIELDS_*_Meta 各档自写, 编码/解码 → features/MetaFlag.hpp)
//   CoreTs 持有: begin_day 重置 + 给当日成员位, close_minute 取 min_flag(traded) + on_minute 并入各分钟箱, close_bin 取 bin_flag(i);
//   Lob 档每笔另喂 on_tick (L_sec 行 + 分钟 depth OR).
//   Bar 档无逐笔 (从不 on_tick) → min_depth_ 恒 false, min_flag(traded) = traded ? pack(false, 1) : 0, 符号位 (depth) 恒不置.
//
//   ts_valid 单槽三态编码: 0 = 该行无事件 (L_sec) / 本分钟无成交 (L_min) / 本箱无成交 (分钟箱层); 非 0 = 有 (data 有效); 负 = 盘口有更新 (depth 有效)
//   幅值: L_sec = 当时 micro price (量加权中间价, 元; 开盘竞价盘口未发布时退化为最近事件价); L_min / 分钟箱层 = 1 (纯标志)
//   依据: 盘口更新必来自事件 → depth ⟹ data, 三态刚好用 零/符号 编进一个 Float16 槽, 价格精度无损
//   L_min 行稠密 (每分钟都写, 特征列 ALL 不受本列门控), 本列在 L_min 只是"有成交"信息位 + 标签分钟型 exit 的推进条件.
//
//   cs_valid = 当日成员 ? 1 : 0 —— 纯日频成员位, 整日常量, 与事件无关 (盘前一次写满本档全部层全部行). 两列对仗:
//     ts_valid  本资产这一行有没有事件 / 成交 (TS 语义, 与池无关; 全轴资产永远算, 日后进池不用补历史)
//     cs_valid  本资产今天在不在截面里 (CS 语义). 截面 (CoreCs gather / 因子层 CS 节点 / Stat) 只看它, 不看 ts_valid:
//               CS 本就该在平稳可比的信息上算, 池内资产在有效行上理应全部有值 (Fund 族 PIT 列除外) —— 因子层对喂 CS 的叶
//               assert 掩码 ⊇ cs_valid, 不满足就是数据脏或用法错 (拿 CS 套 PIT 事件), 让人 think again
//   成员位是日频 PIT 输入, 各档自定 (CoreTs 档钩子 member): Lob = 票池 (pool::Panel, 第 D 日由 Bar 库 D−1 收盘行定),
//   Bar = 在市 (Fund::in_market). 对本类只是一个 (资产, 日) 的 bool —— TS 行仍只看本资产 + 日频 PIT, 不破一致性红线.

#include "features/MetaFlag.hpp"  // fmeta:: 编码/解码 (稳定头, 消费端直接用它, 不依赖本节点文件)
#include "features/TimeIndex.hpp" // L_sec_to_L_min / BIN_COUNT
#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>

// ts_valid 状态机: 同秒 (L_sec 行) / 同分钟 (L_min 行) 多笔的 depth 位 OR 累积 + micro price 维护.
// 逐笔覆盖写同一槽位, 最后一笔的累积值 = 该行终值 (免读改写). close_minute 在跨分钟的那笔
// 进入 on_tick 之前调 (CoreTs::advance 顺序), 所以 min_flag() 读到的 min_depth_ 就是刚完结分钟的 OR.
// cs(): 当日 cs_valid 值 = 成员 ? 1 : 0 (成员位 begin_day 给, 日内不变; CoreTs 盘前写满整列).
class MetaTracker {
public:
  // 盘前: 当日成员位 (日频 PIT 输入, 日内恒定; CoreTs::begin_day 经档钩子 member 传入)
  void begin_day(bool member) { member_ = member; }
  float cs() const { return member_ ? 1.0f : 0.0f; }

  // 每笔一次 (run<L_sec> 末尾, onDepth 已跑完): micro = MicroPrice 节点值 (单边盘口公式给 0);
  // tick_price = 该笔事件价 (开盘竞价盘口未发布, 幅值退化为最近事件价, 首个盘口价一到永久切回)
  inline void on_tick(size_t t, bool depth_updated, float micro, float bid0, float ask0, float tick_price) {
    if (t != sec_) {
      sec_ = t;
      sec_depth_ = false;
    }
    const size_t m = L_sec_to_L_min(t);
    if (m != min_) {
      min_ = m;
      min_depth_ = false;
    }
    sec_depth_ |= depth_updated;
    min_depth_ |= depth_updated;
    if (depth_updated) {
      // 双边盘口 → micro; 单边 (涨跌停) → 有价一侧最优价; 空簿 → 沿用日内最近价
      const float p = (bid0 > 0.0f && ask0 > 0.0f) ? micro : (bid0 > 0.0f ? bid0 : ask0);
      if (p > 0.0f) {
        price_ = p;
        book_priced_ = true;
      }
    }
    if (!book_priced_ && tick_price > 0.0f)
      price_ = tick_price; // 集合竞价阶段: 限价委托/撮合成交价必 > 0 (市价单只在连续竞价, 彼时盘口已建立)
    assert(price_ > 0.0f && "日内首笔必有价: 竞价阶段只有限价单, 撤单前必有挂单");
  }

  float sec_flag() const { return fmeta::pack(sec_depth_, price_); }                          // L_sec 行 (当前秒)
  float min_flag(bool traded) const { return traded ? fmeta::pack(min_depth_, 1.0f) : 0.0f; } // L_min 行 (刚完结分钟; 跨分钟那笔尚未 on_tick); 无成交 → 0

  // 每分钟结算一次 (close_minute): 刚完结分钟的成交 / depth 位 OR 进各分钟箱层的当前箱
  // (depth 只随有成交分钟并入: 无事件分钟不会 on_tick, min_depth_ 是更早分钟的残值)
  void on_minute(bool traded) {
    for (size_t i = 0; i < BIN_COUNT; ++i) {
      bin_traded_[i] |= traded;
      bin_depth_[i] |= traded && min_depth_;
    }
  }
  // 第 i 个分钟箱层的行 (刚完结箱, 箱末分钟 on_minute 之后) 并开该层新箱; 箱内无成交 → 0
  float bin_flag(size_t i) {
    assert(i < BIN_COUNT);
    const float f = bin_traded_[i] ? fmeta::pack(bin_depth_[i], 1.0f) : 0.0f;
    bin_traded_[i] = bin_depth_[i] = false;
    return f;
  }

  void reset() {
    sec_ = min_ = SIZE_MAX;
    sec_depth_ = min_depth_ = book_priced_ = false;
    bin_traded_.fill(false);
    bin_depth_.fill(false);
    price_ = 0.0f;
  }

private:
  size_t sec_ = SIZE_MAX, min_ = SIZE_MAX;
  bool sec_depth_ = false, min_depth_ = false;
  std::array<bool, BIN_COUNT> bin_traded_{}, bin_depth_{}; // 各分钟箱层当前箱 (close_minute 逐分钟 OR, bin_flag 取走即清)
  bool book_priced_ = false;                               // 日内是否已有盘口价 (之前幅值退化为事件价)
  bool member_ = false;                                    // 当日成员位 (begin_day 给; 日内不变)
  float price_ = 0.0f;                                     // 最近价 (元, 日内): 盘口 micro price, 竞价阶段为事件价
};
