// Asset Loader Implementation
#include "gui/task_database/services/AssetLoader.hpp"
#include "fundamental/AssetAxis.hpp"
#include "shared/SharedData.hpp"

#include <cassert>
#include <string>

namespace GUI::Database {

void AssetLoader::load(SharedData &data) {
  // 帧目录 (Asset::catalog) 按资产下标定址, 不随 items 重建: 轴 append-only,
  // items[i].asset_id == i 不变
  data.asset.items.clear();

  const AssetInfo &info = data.asset_info;
  AssetAxis &axis = asset_axis(); // 首次访问即 load + 自校验

  // 码表 = 构建时的轴 + 档案新见代码 (追加在尾) ⇒ 逐个 intern 恰好落在自己的下标上
  for (std::size_t i = 0; i < info.codes.size(); ++i) {
    [[maybe_unused]] const std::size_t id = axis.intern(info.codes[i]);
    assert(id == i && "AssetInfo 码表须是 A 轴的延伸");
  }
  axis.save();
  assert(axis.size() == info.stocks.size() && "AssetInfo 未覆盖 A 轴");

  // 轴里可能有档案不再收录的老代码 (注册表 append-only, 从不删) — 那些
  // 保留占位, 名称留空、区间给全开, encode 时 archive 里没有自然跳过.
  const std::size_t n = axis.size();
  data.asset.items.reserve(n);
  for (std::size_t i = 0; i < n; ++i) {
    const std::string &code_ex = axis.code(i);
    const std::size_t dot = code_ex.find('.');
    assert(dot != std::string::npos && "AssetAxis 资产码缺少 .EX 后缀");
    const StockInfo &s = info.stocks[i];
    data.asset.items.emplace_back(i, code_ex.substr(0, dot), s.name, code_ex.substr(dot + 1),
                                  s.ipo_date ? s.ipo_date : 19900101, s.out_date ? s.out_date : 20991231);
  }
}

} // namespace GUI::Database
