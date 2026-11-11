// fund — 日频 PIT 基本面 (估值 / 因子 / filter 链路): Fund 节点的数据源 + 逐日推进方法
//
//   Panel    数据源. Compute 开跑前构建一次: 扫 output/data/fundamental/ 月度 parquet → 全交易日历 D 轴 × 全局 A 轴 (asset_axis, 全市场) 的
//           原始网格 (close/股本/涨跌停/状态/两融, cutoff 已套, ffill 已做) + per-A 事件链 (ttm/资产负债/年报/分红/预告/行业).
//           网格只保留 [首回测日 − 15, 末] (trading_st 连续 15 日计数所需), 历史依赖全在事件链上; 只读, 全部 TS worker 共享.
//           与库无关: 任一池任一档的库都 build 同一份全市场面板, 节点按全局 id 取行 (TS 不随池变).
//           行情派生 (Market): 切片前在全史网格上一次扫出, 只留切片窗口 —— 长窗时序 (12−1 月动量 / 换手比)
//           与全市场截面 (活跃等权市场收益 → 特质波动 / β; 均线截面回归 → 趋势预期收益; 三项波动合成正交 → 残差波动).
//           截面一律取活跃: 切片前让状态机沿全 D 轴只推事件 + 状态口, 出全史活跃掩码 (经 Fund live 口供节点读,
//           另经 live() 给 all 池逐日成员).
//           每格只用 ≤ T 收盘的数据 (行 T+1 起可见), 对 TS 是与基本面同级的日频 PIT 输入; 截面归约按固定资产块定序,
//           结果与线程数无关. 各窗按可得史扩张 (数据起点 / 新股同规则), 有价次日起即有值.
//   Stream  per-资产日频状态机 (Fund 节点各持一个). advance_to(d) 沿 D 轴逐日推进到 d (回测日之间的非回测日也走,
//           状态才对; 回测日前只推事件指针, 不碰网格), 然后产出当日 Fund::kCount 列 (布局 Fund::Out, 缺失 = NaN,
//           fp16 存不下的极值也归 NaN). 总算力 = per-A 沿 D 轴一遍, 与预计算相同.
//
// PIT 口径 (row D = D 日盘前可知; 逐表见 .cpp GridScan / EventScan, Fund 节点前另列 L_1day 对齐):
//   cutoff=-1 (T 盘后, row T+1 起可见): bar1d.close / shares.total 收盘口 (收盘市值) / margin_trading_detail / industry / dividend / financial_ttm / balance / income / forecast / 行情派生
//   cutoff=0  (T 盘前可知): status(st/susp), shares, limit_price, bar1d.pre_close / adjust_factor (交易所盘前公布前收 / 除权事先公告)
//   涨跌停价: row T 即 "T 当日适用涨跌停" (由 pre_close(T) 推出), 直接配盘中实时价判触板.
//   D 轴 = 交易日历截到 today. 库末日 ≤ last_day: 行 D 所读各表本地已齐到 D 盘后 / D+1 盘前, 且 D+1 在轴内
//   (Fund 收盘推进到 d+1). 缺尾日的表不默认补 (status 缺行 = 非 ST / 两融缺行 = 非标的, 会被库追加落盘), 库止于源齐处.
//   三表 raw: 利润表 / 现金流量表按 YTD 链差分出 Q 单季 / TTM / LYR, 资产负债表取 MRQ; 定义见 .cpp YtdState.
//
// fast-math 契约: 输出行被 -ffast-math TU (Valuation 节点/CoreLob) 盲算消费 — 只做算术 (NaN 硬件透传) 与
//   "NaN 恒 false" 语义的比较, 不做 isnan/isfinite. 本文件 .cpp 在 MATH_PRECISE 列表里, 类型细节全在 .cpp.
#pragma once

#include "misc/date.hpp"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string_view>
#include <vector>

namespace fundamental {
class Catalog;
}

namespace fund {

// ============================================================================
// SW2021 一级行业 (下标即落盘 ID, 顺序不得改动)
//   落盘列 ind_l1 的取值域: 0 = 未知, 1..31; GUI 行业筛选按此显示中文名
// ============================================================================
inline constexpr std::string_view SW2021_L1_NAMES[32] = {
    "未知", "交通运输", "传媒", "公用事业", "农林牧渔", "医药生物", "商贸零售",
    "国防军工", "基础化工", "家用电器", "建筑材料", "建筑装饰", "房地产",
    "有色金属", "机械设备", "汽车", "煤炭", "环保", "电力设备", "电子",
    "石油石化", "社会服务", "纺织服饰", "综合", "美容护理", "计算机",
    "轻工制造", "通信", "钢铁", "银行", "非银金融", "食品饮料"};
inline constexpr std::size_t SW2021_L1_COUNT = 32;

inline std::uint8_t sw2021_l1_name_to_id(std::string_view name) {
  for (std::uint8_t i = 1; i < SW2021_L1_COUNT; ++i)
    if (SW2021_L1_NAMES[i] == name)
      return i;
  return 0;
}

// 次新股: 上市龄 < kNewListDays 日历日 (过滤表列 flt_new; 不进活跃判定 ⇒ all 库 cs_valid 含次新, 子池过滤维 nonew 剔).
// 覆盖上市初期连板炒作与 6 / 12 个月限售解禁窗口
inline constexpr float kNewListDays = 365.0f;

// 板块 (代码判定, 静态): 落盘列 board 的取值域; 其余 (B 股等) 记未知
enum Board : std::uint8_t { kBoardUnknown,
                            kBoardShMain,  // 沪市主板 600 / 601 / 603 / 605
                            kBoardSzMain,  // 深市主板 000 / 001 / 002 / 003 / 004
                            kBoardStar,    // 科创板 688 / 689
                            kBoardChiNext, // 创业板 300 / 301 / 302 / 309
                            kBoardBj,      // 北交所 (代码 .BJ)
                            kBoardCount };
inline constexpr std::string_view BOARD_NAMES[kBoardCount] = {"未知", "沪市主板", "深市主板", "科创板", "创业板", "北交所"};

// code = 带交易所后缀的代码 (如 600000.SH)
inline Board board_of(std::string_view code) {
  if (code.ends_with(".BJ"))
    return kBoardBj;
  const std::string_view p = code.substr(0, 3);
  for (const std::string_view s : {"600", "601", "603", "605"})
    if (p == s)
      return kBoardShMain;
  for (const std::string_view s : {"000", "001", "002", "003", "004"})
    if (p == s)
      return kBoardSzMain;
  for (const std::string_view s : {"688", "689"})
    if (p == s)
      return kBoardStar;
  for (const std::string_view s : {"300", "301", "302", "309"})
    if (p == s)
      return kBoardChiNext;
  return kBoardUnknown;
}

// 库可算到的末日: 最晚的交易日 D, 使行 D (= D+1 盘前态) 所读各表本地都已齐到所需数据日 (逐表见 .cpp kSources),
// 且 D+1 在 cal 内. cal = 交易日历 (升序, 截到 today); 无 → 0. Bar 源日历截到此日, Panel::build 断言
misc::Ymd last_day(const fundamental::Catalog &cat, std::span<const misc::Ymd> cal);

struct Data;  // 轴 / meta / 网格切片 / 事件链 (定义在 .cpp)
struct State; // Stream 内部状态 (定义在 .cpp)

class Panel {
public:
  Panel();
  ~Panel();
  Panel(const Panel &) = delete;
  Panel &operator=(const Panel &) = delete;

  // A 轴 = asset_axis 全轴 (查码表漏收); dates: 库日期轴升序 (决定网格切片起点, 必须全部命中交易日历).
  // 本地 parquet 缺失 → assert (前置条件: 基本面 Sync 已完成).
  void build(const std::vector<misc::Ymd> &dates);

  int date_index(misc::Ymd d) const; // D 轴下标; 不是交易日 → -1
  // 第 d 日活跃位 (d 日盘前态, 与 Fund live 口同一份) → out[全局 id]; out.size() = A 轴长, d 不在网格切片 → assert
  void live(misc::Ymd d, std::span<std::uint8_t> out) const;

  const Data &data() const {
    assert(d_ != nullptr && "fund::Panel 未 build");
    return *d_;
  }

private:
  std::unique_ptr<Data> d_;
};

class Stream {
public:
  Stream(const Panel &panel, std::size_t asset_id);
  ~Stream();
  Stream(const Stream &) = delete;
  Stream &operator=(const Stream &) = delete;

  // 推进到 D 轴日 d (必须单调不减, 首次可跨全部 warmup), 写 out[Fund::kCount]
  void advance_to(int d, float *out);

private:
  std::unique_ptr<State> s_;
};

} // namespace fund
