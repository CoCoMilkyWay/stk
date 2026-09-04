// Asset Loader Implementation
#include "gui/task_database/services/AssetLoader.hpp"
#include "shared/AssetAxis.hpp"
#include "shared/SharedData.hpp"

#include <algorithm>
#include <cassert>
#include <vector>

namespace GUI::Database {

void AssetLoader::load(SharedData &data) {
  // date_info 按资产下标原样保留 (轴 append-only, items[i].asset_id == i 不变);
  // 日期轴在 Asset 上, 不随 items 重建
  std::vector<std::vector<DateInfo>> old_date_info;
  old_date_info.reserve(data.asset.items.size());
  for (auto &item : data.asset.items)
    old_date_info.push_back(std::move(item.date_info));

  data.asset.items.clear();

  const auto &stock_info_map = data.assetinfo.get_stock_info();
  AssetAxis &axis = asset_axis(); // 首次访问即 load + 自校验

  // 基本面股票全量 → intern. stock_info 是 std::map, 首次建轴时顺序即
  // (exchange, code) 字典序; 之后新上市按发现顺序追加尾部.
  for (const auto &[key, info] : stock_info_map) {
    std::string code, exchange;
    split_stock_key(key, code, exchange);
    axis.intern(code + "." + exchange);
  }
  axis.save();

  // items 与轴下标密集对齐: items[i].asset_id == i.
  // 轴里可能有本次基本面查不到的老代码 (注册表 append-only, 从不删) — 那些
  // 保留占位, 名称留空、区间给全开, encode 时 archive 里没有自然跳过.
  const std::size_t n = axis.size();
  data.asset.items.reserve(n);

  for (std::size_t i = 0; i < n; ++i) {
    const std::string &code_ex = axis.code(i);
    const std::size_t dot = code_ex.find('.');
    assert(dot != std::string::npos && "AssetAxis 资产码缺少 .EX 后缀");
    const std::string code = code_ex.substr(0, dot);
    const std::string exchange = code_ex.substr(dot + 1);

    std::string exchange_lower = exchange;
    std::transform(exchange_lower.begin(), exchange_lower.end(), exchange_lower.begin(), ::tolower);

    std::string asset_name;
    std::string start_date = "19900101";
    std::string end_date = "20991231";

    auto it = stock_info_map.find(exchange_lower + "." + code);
    if (it != stock_info_map.end()) {
      const auto &info = it->second;
      asset_name = info.name;

      std::string ipo_date = info.ipoDate;
      if (!ipo_date.empty()) {
        ipo_date.erase(std::remove(ipo_date.begin(), ipo_date.end(), '-'), ipo_date.end());
        start_date = ipo_date;
      }

      std::string out_date = info.outDate;
      if (!out_date.empty()) {
        out_date.erase(std::remove(out_date.begin(), out_date.end(), '-'), out_date.end());
        end_date = out_date;
      }
    }

    AssetItem item(i, code, asset_name, exchange, start_date, end_date);
    if (i < old_date_info.size())
      item.date_info = std::move(old_date_info[i]);
    data.asset.items.push_back(std::move(item));
  }
}

void AssetLoader::split_stock_key(const std::string &key, std::string &code, std::string &exchange) {
  const std::size_t dot = key.find('.');
  assert(dot != std::string::npos && "基本面 stock_info key 缺少 . 分隔 (期望 sz.000001)");

  exchange = key.substr(0, dot);
  std::transform(exchange.begin(), exchange.end(), exchange.begin(), ::toupper);
  code = key.substr(dot + 1);

  assert(!code.empty() && "基本面 stock_info key 代码段为空");
}

} // namespace GUI::Database
