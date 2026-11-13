// Tab Board - 持仓 · 调仓页: 连接 (客户端适配表 live/Clients.hpp 一行一个按钮) / 左栏 控件分组 + 持仓表 (并调仓名单) + 事务日志 / 右栏选中行的行情图
//   左栏小一号字 (kFontScale)、紧凑间距, 一屏放下持仓与调仓两面; 各表格内边距随栏宽收放 (放得下用满, 放不下压到 1px, 仍放不下则长表头折两行)
//   控件分组一行一组, 状态行定高 (不随内容增减, 免得持仓表的按钮在手底下挪):
//     数据: 刷新 / 定时 / 两融条款 / 隐私, 刷新进度; 下单: 委托价 / 买入方式, 最近两单的步骤条;
//     调仓: 名单 / 最小单元 / 行使比例 / 执行, 试算状态 + 试算表与事务;
//     账户: 现 / 调仓后 / 变动%, 调仓后与现不同的格标色, 变动按该列好坏着色 (好红坏绿)
//   持仓表: 持仓 × 当日成交 × 行情 (live/Board.hpp), 今日清仓的排在后、名称灰; 在当前调仓名单里的名称青.
//     名单里未持有的列在表末 (按排名, 不随排序), 只出名称、融、调仓金额与成本、折算; 这次不入的名称灰.
//     点表头排序 (标识与数值列, 三态, 不排 = 帧序; 今日清仓、空值恒在后).
//     状态列: ● 今日成交 · ○ 挂单, 买红卖绿, 悬停看逐笔. 融列与卖出、金额、成本各列取自调仓试算 (试算未出即空).
//     金额 = 现金买,融资买/卖 [万] (卖 = 拖到的股数 × 现价), 买红卖绿, 0 占位不着色; 名单股的排名、保证金比例、
//     不补 / 不成单 / 不可买的原因与成本拆分放悬停 (名称、金额、成本格).
//     合计行当日% = 当日盈亏 / 昨净资产 (现金 + 总市值 − 负债 − 当日盈亏).
//   下单: 一列滑点一个按钮 (kSlipCols, 自左而右 大买 … 小买、小卖 … 大卖), 填充 = 该额的滑点色
//     (绿 < kSlipOk ≤ 黄 < kSlipHi ≤ 红, 竞价 / 无行情不填); 只有绿的 (5 档吃满且低滑点) 直接下, 其余先确认.
//     默认锁住 (勾上该列表头的「解锁」才下得了单); 买入方式只管买入 (默认融资), 卖出一律担保品卖出; 有单或调仓在跑即都不收
//   卖出滑杆按股数线性走 (0 … 全部可卖), 拖动时就地 snap 到能成单的股数: 0 不卖、
//     first 起按步长递增 (first 够起买股数且够最小调仓单元)、全部可卖 (够最小调仓单元才有), 取最近的
//   调仓: 名单先融资标的、后其余, 各按排名入选, 等权补齐; 试算与执行的口径见 live/Rebal.hpp, 事务的步骤见 LiveHub.hpp
//   行情图 = Market 的个股面板 (QuotesPanel), 本地资产库就绪才有
//   隐私 (LiveHub::priv): 代码 / 名称 / 状态 / 盈亏 / 市值各列 (含合计, 合计只数照显)、账户对照与试算摘要的金额、
//     名单股的代码 / 名称 / 调仓金额、各单与挂单的代码名称、事务日志、行情图标题遮成 kMask; 比例、只数、下单与卖出各列照显
#pragma once

#include "gui/task_live/LiveHub.hpp"
#include "gui/task_market/ui/QuotesPanel.hpp"

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

struct SharedData;

namespace GUI::Live {

inline constexpr double kSlipOk = 10, kSlipHi = 20; // [万分之一]
inline constexpr float kFontScale = 0.85f;          // 左栏字号 / 基准字号

// 自适应表的上帧量度: 各列格内容宽 (不含表头), 表头除字宽外的占宽 (排序箭头 / 表头内控件); 表头是否折行
struct Fit {
  std::vector<float> body, extra;
  bool wrap = false;
};

struct BoardState {
  std::string sel;            // 选中的代码 (行情图看它)
  bool margin = true;         // 买入走融资
  bool armed = false;         // 解锁买卖
  bool run = false;           // 执行调仓待确认
  std::optional<int> every;   // 定时刷新 [s] 输入框编辑中的值 (无 = 显示当前周期)
  std::optional<double> unit; // 最小调仓单元输入框编辑中的值 (无 = 显示当前配置)
  struct Ask {
    std::string code, name, slip;
    int side = live::kBuy;
    double size = 0;
    live::Credit credit = live::Credit::Plain;
  };
  std::optional<Ask> ask; // 待确认的单
  Market::StockState stock;
  std::unordered_map<std::string, uint32_t> ids; // "000001.SZ" → 全局 id
  float split = 0.66f;                           // 左右分栏比
  Fit pos_fit, acct_fit, plan_fit;               // 持仓 / 账户 / 试算表
};

void RenderTabBoard(SharedData &data, LiveHub &hub, Quote::QuoteHub &quote, BoardState &st);

} // namespace GUI::Live
