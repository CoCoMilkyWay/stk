// Asset Loader — A 轴物化 (注册表 → Asset::items)
//
// universe 不再由人工名单 (config/assets.json) 决定: encode 与 features 两级
// cache 都是全市场日频. 来源 = 基本面 cn_stock_basic_info 的股票全量 (含已退市,
// 天然不含 ETF/基金 — archive 里那 ~2200 个基金目录不进 A 轴).
//
// items[i].asset_id == i == AssetAxis 下标, 全流程按此下标寻址.
#pragma once

#include <string>

// Forward declaration
struct SharedData;

namespace GUI::Database {

class AssetLoader {
public:
  // 基本面股票全量 → AssetAxis (append-only intern) → Asset::items (下标对齐).
  // 前置: 基本面已就绪 (StateManager 的 update_all 先跑).
  static void load(SharedData &data);

private:
  // "sz.000001" (基本面 key) → "000001", "SZ"
  static void split_stock_key(const std::string &key, std::string &code, std::string &exchange);
};

} // namespace GUI::Database
