// 实盘: 两融条款, 券商无关 (与资金流水账本 live/Flows.hpp 同形: 拉 → 存 → 用)
//   缓存 output/live/terms.csv (live/Pulled.hpp): 一行一只; 不论哪天拉的启动即用
//   拉: 客户端重查全表, 整份换掉缓存
//   用: 与本地库合: 本地库有的个股名称 / 市场取本地 (AssetInfo), 融资标的 = 券商融资状态正常 ∧ 本地两融明细最新一日有它,
//     担保品 = 券商担保状态正常 ∧ 本地在市; 保证金比例 / 折算率只有券商有; 本地库没有的 (ETF / 债券) 照用券商的整行
#pragma once

#include "live/Broker.hpp"
#include "misc/date.hpp"

#include <string>
#include <unordered_map>
#include <vector>

struct AssetInfo;

namespace live::terms {

struct Cache {
  misc::Ymd pulled = 0; // 拉于 (0 = 还没有缓存)
  Terms terms;
};

Cache load();          // 没有缓存 → 空
Cache pull(Broker &b); // 客户端重查全表 → 存; 失败 → Fail

// 本地库的一只个股
struct Stock {
  std::string name;
  Mkt mkt = Mkt::SZ;
  bool listed = false; // 在市
  bool margin = false; // 两融明细最新一日有它 (交易所两融标的)
};
using Local = std::unordered_map<std::string, Stock>; // 代码 → 个股

Local local(const AssetInfo &info);                 // AssetInfo + 本地两融明细; AssetInfo 还没建 → 空; 本地库没有两融明细 → Fail
Terms merged(const Terms &raw, const Local &local); // 券商条款合本地库
// 要用条款的代码 (名单 / 账户里的各只) 有缓存里没有的; 本地库认得且北交所或已退市的本就不在券商表里, 不算
bool missing(const Terms &raw, const Local &local, const std::vector<std::string> &codes);

} // namespace live::terms
