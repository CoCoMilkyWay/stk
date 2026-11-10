// Tab Board - 持仓页: 连接 (客户端适配表 live/Clients.hpp 一行一个按钮) / 刷新进度 / 持仓表 (行内闪电下单) / 右栏选中持仓的行情图
//   持仓表: 持仓 × 当日成交 × 行情 (live/Board.hpp), 今日清仓的排在后、名称灰; 在当前调仓名单里的名称青.
//     状态列: ● 今日成交 · ○ 挂单, 买绿卖红, 悬停看逐笔. 融列取自调仓试算 (试算未出即空).
//   下单: 一列滑点一个按钮 (kSlipCols, 自左而右 大买 … 小买、小卖 … 大卖), 填充 = 该额的滑点色
//     (绿 < kSlipOk ≤ 黄 < kSlipHi ≤ 红, 竞价 / 无行情不填); 只有绿的 (5 档吃满且低滑点) 直接下, 其余先确认.
//     默认锁住 (解锁买卖才下得了单); 买入方式只管买入, 卖出一律担保品卖出; 有单或调仓在跑即都不收
//   行情图 = Market 的个股面板 (QuotesPanel), 本地资产库就绪才有
//   隐私 (LiveHub::priv): 代码 / 名称 / 状态 / 盈亏 / 市值各列 (含合计, 合计只数照显)、各单与挂单的代码名称、行情图标题遮成 kMask
#pragma once

#include "gui/task_live/LiveHub.hpp"
#include "gui/task_market/ui/QuotesPanel.hpp"

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>

struct SharedData;

namespace GUI::Live {

inline constexpr double kSlipOk = 10, kSlipHi = 20; // [万分之一]

struct BoardState {
  std::string sel;          // 选中的代码 (行情图看它)
  bool margin = false;      // 买入走融资
  bool armed = false;       // 解锁买卖
  std::optional<int> every; // 定时刷新 [s] 输入框编辑中的值 (无 = 显示当前周期)
  struct Ask {
    std::string code, name, slip;
    int side = live::kBuy;
    double size = 0;
    live::Credit credit = live::Credit::Plain;
  };
  std::optional<Ask> ask; // 待确认的单
  Market::StockState stock;
  std::unordered_map<std::string, uint32_t> ids; // "000001.SZ" → 全局 id
  float split = 0.62f;                           // 左右分栏比
};

void RenderTabBoard(SharedData &data, LiveHub &hub, Quote::QuoteHub &quote, BoardState &st);

} // namespace GUI::Live
