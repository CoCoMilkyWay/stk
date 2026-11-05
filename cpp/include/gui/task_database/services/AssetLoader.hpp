// Asset Loader — A 轴物化 (注册表 → Asset::items)
//
// 资产集不由人工名单决定: encode 与 features 两级
// cache 都是全市场日频. 来源 = 基本面 cn_stock_basic_info 的股票全量 (含已退市,
// 天然不含 ETF/基金 — archive 里那 ~2200 个基金目录不进 A 轴).
//
// items[i].asset_id == i == AssetAxis 下标, 全流程按此下标寻址.
#pragma once

// Forward declaration
struct SharedData;

namespace GUI::Database {

class AssetLoader {
public:
  // AssetInfo::codes → AssetAxis (append-only intern, 下标逐个对上) → Asset::items (下标对齐).
  // 前置: 基本面已就绪 (StateManager 收下 ok 的基本面交接件后调).
  static void load(SharedData &data);
};

} // namespace GUI::Database
