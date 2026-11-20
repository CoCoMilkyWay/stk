// 通达信适配: 信用账户 — 两融条款 (标的证券 × 担保品) 与账户 (信用资产 + 融资负债), 均经请求层直发
//   条款: 两表各按定位字串翻页到底 (担保品近万只, 约十余页); 以 (市场, 代码) 为键, 合成时按代码归一 (跨市场同号的只留 market_of 那只)
//   账户: 信用资产 (各项「名称 → 数值」) + 各只融资负债
//   须插件已发过各表的功能 (tables::seen; Driver 先经界面引导)
#pragma once

#include "live/Broker.hpp"

namespace live::tdx::credit {

Terms terms(const Report &r);
Account acct();

} // namespace live::tdx::credit
