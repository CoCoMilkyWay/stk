// 实盘: 两融条款, 券商无关 (与资金流水账本 account/Ledger.hpp 同形: 拉 → 存 → 用)
//   缓存 output/live/terms.csv (live/Pulled.hpp); 用时与本地库合 (名称 / 市场 / 两融明细 / 在市)
#pragma once

#include "live/Broker.hpp"
#include "misc/date.hpp"

#include <string>
#include <unordered_map>
#include <vector>

struct AssetInfo;

namespace live::terms {

struct Cache {
  misc::Ymd pulled = 0;
  Terms terms;
};

Cache load();
void save(const Cache &c);
Cache pull(Broker &b); // 客户端重查全表 → 存; 失败 → Fail

struct Stock {
  std::string name;
  Mkt mkt = Mkt::SZ;
  bool listed = false;
  bool margin = false;
};
using Local = std::unordered_map<std::string, Stock>;

Local local(const AssetInfo &info);
Terms merged(const Terms &raw, const Local &local);
bool missing(const Terms &raw, const Local &local, const std::vector<std::string> &codes, const Snapshot *snap);

} // namespace live::terms
