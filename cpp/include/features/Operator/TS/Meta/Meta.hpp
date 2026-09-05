#pragma once
// 元数据列: 基建标志 _meta (FLAG), 由 CoreSequential 手工写.
// 无算子 (不进 DAG: L1 行落不落还取决于分钟有效性), 但全部逻辑收在本文件:
//   fmeta        _meta 编码/解码唯一事实源 (写入端 pack, 消费端 data_valid/depth_valid/price/valid)
//   MetaTracker  秒内/分钟内 depth OR + micro price 状态机 (CoreSequential 每笔喂一次)
//
//   _meta 单槽三态编码: 0 = 该行无事件; 非 0 = 有事件写入 (data 有效); 负 = 盘口有更新 (depth 有效)
//   幅值: L0 = 当时 micro price (量加权中间价, 元; 开盘竞价盘口未发布时退化为最近事件价); L1 = 1 (纯标志)
//   依据: 盘口更新必来自事件 → depth ⟹ data, 三态刚好用 零/符号 编进一个 Float16 槽, 价格精度无损

#include "codec/L2_DataType.hpp"  // L2::ValidType (fmeta::valid)
#include "features/TimeIndex.hpp" // L0_to_L1
#include <cassert>
#include <cstddef>

namespace fmeta {
inline constexpr bool data_valid(float v) { return v != 0.0f; }
inline constexpr bool depth_valid(float v) { return v < 0.0f; }
inline constexpr float price(float v) { return v < 0.0f ? -v : v; }
inline constexpr float pack(bool depth, float mag) { return depth ? -mag : mag; }
// 某特征列按其 valid_type 的门控判定 (消费端读同层 _meta 后调用)
inline constexpr bool valid(float v, L2::ValidType vt) {
  return vt == L2::ValidType::ALL || (vt == L2::ValidType::DEPTH ? depth_valid(v) : data_valid(v));
}
} // namespace fmeta

// _meta 状态机: 同秒 (L0 行) / 同分钟 (L1 行) 多笔的 depth 位 OR 累积 + micro price 维护.
// 逐笔覆盖写同一槽位, 最后一笔的累积值 = 该行终值 (免读改写). 分钟翻转与 ResamplerTick2Min
// 由同一笔 tick 驱动 (同一个 L0_to_L1(l0_index)), 所以 run_minute 时 prev = 刚完结的分钟.
class MetaTracker {
public:
  // 每笔一次 (run_tick 末尾, onDepth 已跑完): micro = MicroPrice 节点值 (单边盘口公式给 0);
  // tick_price = 该笔事件价 (开盘竞价盘口未发布, 幅值退化为最近事件价, 首个盘口价一到永久切回)
  inline void on_tick(size_t t, bool depth_updated, float micro, float bid0, float ask0, float tick_price) {
    if (t != sec_) {
      sec_ = t;
      sec_depth_ = false;
    }
    const size_t m = L0_to_L1(t);
    if (m != min_) {
      prev_min_depth_ = min_depth_;
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

  float l0() const { return fmeta::pack(sec_depth_, price_); }                                           // L0 行 (当前秒)
  float l1(bool minute_valid) const { return minute_valid ? fmeta::pack(prev_min_depth_, 1.0f) : 0.0f; } // L1 行 (刚完结分钟)

  void reset() {
    sec_ = min_ = SIZE_MAX;
    sec_depth_ = min_depth_ = prev_min_depth_ = book_priced_ = false;
    price_ = 0.0f;
  }

private:
  size_t sec_ = SIZE_MAX, min_ = SIZE_MAX;
  bool sec_depth_ = false, min_depth_ = false, prev_min_depth_ = false;
  bool book_priced_ = false; // 日内是否已有盘口价 (之前幅值退化为事件价)
  float price_ = 0.0f;       // 最近价 (元, 日内): 盘口 micro price, 竞价阶段为事件价
};

// ---- 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define FIELDS_L0_Meta(X, CAT1) \
  X(_meta, CAT1, RAW, NONE, "Meta Flags", "元数据", "0=无事件; ±micro price, 负=该秒盘口有更新", R"(\pm P_t^{micro} \cdot \mathbf{1}_{\mathrm{data}})", FLAG)

#define FIELDS_L1_Meta(X, CAT1) \
  X(_meta, CAT1, RAW, NONE, "Meta Flags", "元数据", "0=无效分钟; ±1, 负=该分钟盘口有更新", R"(\pm\mathbf{1}_{\mathrm{data}})", FLAG)
