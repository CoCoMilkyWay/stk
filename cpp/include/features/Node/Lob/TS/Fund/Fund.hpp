#pragma once

// Lob 档 Fund 节点: 只供盘中节点 (Kline / Book / Auction 前收兜底, Depth / Flow / Book / LabelLob 涨跌停) 取口, 不落盘 —— 基本面列在 Bar 档
#include "features/Node/Common/Fund/Fund.hpp"

#define NODE_Fund(N) N(Fund, (Fund), (fund_panel, asset_id_, date_), onSettle)
