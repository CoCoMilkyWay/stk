#pragma once
// 元数据列: 基建标志 ts_valid / cs_valid (FLAG), 由 CoreSequential 手工写, 两层都落.
// 无算子 (不进 DAG: L1 行落不落还取决于分钟有效性):
//   fmeta        编码/解码唯一事实源 → features/MetaFlag.hpp (稳定头, 消费端不依赖本算子文件)
//   MetaTracker  秒内/分钟内 depth OR + micro price 状态机 (CoreSequential 每笔喂一次) + 当日池成员位
//
//   ts_valid 单槽三态编码: 0 = 该行无事件; 非 0 = 有事件写入 (data 有效); 负 = 盘口有更新 (depth 有效)
//   幅值: L0 = 当时 micro price (量加权中间价, 元; 开盘竞价盘口未发布时退化为最近事件价); L1 = 1 (纯标志)
//   依据: 盘口更新必来自事件 → depth ⟹ data, 三态刚好用 零/符号 编进一个 Float16 槽, 价格精度无损
//
//   cs_valid = 当日在池 ? 1 : 0 —— 纯日频成员位, 整日常量, 与事件无关 (盘前一次写满两层全部行). 两列对仗:
//     ts_valid  本资产这一行有没有数据 (TS 语义, 与池无关; 全轴资产永远算, 日后进池不用补历史)
//     cs_valid  本资产今天在不在截面里 (CS 语义). 截面 (CoreCrosssection gather / 因子层 CS 节点 / Stat) 只看它, 不看 ts_valid:
//               CS 本就该在平稳可比的信息上算, 池内资产在有效行上理应全部有值 (Fund 族 PIT 列除外) —— 因子层对喂 CS 的叶
//               assert 掩码 ⊇ cs_valid, 不满足就是数据脏或用法错 (拿 CS 套 PIT 事件), 让人 think again
//   池成员是日频 PIT 输入 (universe::Pool, 见 shared/Universe.hpp: py/universe/<name>.py 盘后产出逐日 diff json,
//   第 D 日名单由 ≤ D−1 数据定), 对本算子只是一个 (资产, 日) 的 bool, 与 Fund 吃 fund_pool 同形 ——
//   TS 行仍只看本资产 + 日频 PIT, 不破一致性红线. 选池不与本项目耦合: 选池可能要用特征而特征此刻还没算 (鸡生蛋).

#include "features/MetaFlag.hpp"  // fmeta:: 编码/解码 (稳定头, 消费端直接用它, 不依赖本算子文件)
#include "features/TimeIndex.hpp" // L0_to_L1
#include <cassert>
#include <cstddef>

// ts_valid 状态机: 同秒 (L0 行) / 同分钟 (L1 行) 多笔的 depth 位 OR 累积 + micro price 维护.
// 逐笔覆盖写同一槽位, 最后一笔的累积值 = 该行终值 (免读改写). run_minute 在跨分钟的那笔
// 进入 on_tick 之前调 (CoreSequential 顺序), 所以 l1() 读到的 min_depth_ 就是刚完结分钟的 OR.
// cs(): 当日 cs_valid 值 = 在池 ? 1 : 0 (池成员位 begin_day 给, 日内不变; CoreSequential 盘前写满整列).
class MetaTracker {
public:
  // 盘前: 当日池成员位 (日频 PIT 输入, 日内恒定; CoreSequential::begin_day 查 universe::Pool 传入)
  void begin_day(bool in_pool) { in_pool_ = in_pool; }
  float cs() const { return in_pool_ ? 1.0f : 0.0f; }

  // 每笔一次 (run_tick 末尾, onDepth 已跑完): micro = MicroPrice 节点值 (单边盘口公式给 0);
  // tick_price = 该笔事件价 (开盘竞价盘口未发布, 幅值退化为最近事件价, 首个盘口价一到永久切回)
  inline void on_tick(size_t t, bool depth_updated, float micro, float bid0, float ask0, float tick_price) {
    if (t != sec_) {
      sec_ = t;
      sec_depth_ = false;
    }
    const size_t m = L0_to_L1(t);
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

  float l0() const { return fmeta::pack(sec_depth_, price_); }                                      // L0 行 (当前秒)
  float l1(bool minute_valid) const { return minute_valid ? fmeta::pack(min_depth_, 1.0f) : 0.0f; } // L1 行 (刚完结分钟; 跨分钟那笔尚未 on_tick)

  void reset() {
    sec_ = min_ = SIZE_MAX;
    sec_depth_ = min_depth_ = book_priced_ = false;
    price_ = 0.0f;
  }

private:
  size_t sec_ = SIZE_MAX, min_ = SIZE_MAX;
  bool sec_depth_ = false, min_depth_ = false;
  bool book_priced_ = false; // 日内是否已有盘口价 (之前幅值退化为事件价)
  bool in_pool_ = false;     // 当日池成员位 (begin_day 给; 日内不变)
  float price_ = 0.0f;       // 最近价 (元, 日内): 盘口 micro price, 竞价阶段为事件价
};

// ---- 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define FIELDS_L0_Meta(X, CAT1)                                                                                                                             \
  X(ts_valid, CAT1, AUTO, "TS Valid", "时序有效", "0=无事件; ±micro price, 负=该秒盘口有更新", R"(\pm P_t^{micro} \cdot \mathbf{1}_{\mathrm{data}})", FLAG) \
  X(cs_valid, CAT1, AUTO, "CS Valid", "截面有效", "当日在池=1 否则 0 (整日常量, 与事件无关); 截面只看此列", R"(\mathbf{1}_{\mathrm{pool},D})", FLAG)

#define FIELDS_L1_Meta(X, CAT1)                                                                                                    \
  X(ts_valid, CAT1, AUTO, "TS Valid", "时序有效", "0=无效分钟; ±1, 负=该分钟盘口有更新", R"(\pm\mathbf{1}_{\mathrm{data}})", FLAG) \
  X(cs_valid, CAT1, AUTO, "CS Valid", "截面有效", "当日在池=1 否则 0 (整日常量, 与事件无关); 截面只看此列", R"(\mathbf{1}_{\mathrm{pool},D})", FLAG)
