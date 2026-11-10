#include "features/Storage/FeatureLib.hpp"

#include "nlohmann/json.hpp"

#include <algorithm>
#include <cassert>

// ============================================================================
// 特征库清单: <库>/lib.json = {"tier", "dates": [YYYYMMDD...], "codes": ["CODE.EX"...], "outside": [...] (非空才有)}
// ============================================================================
disk::Key lib_key(LibId id) { return {.pool = pool_def(lib_pool(id)).name, .tier = std::string(tier_name(lib_tier(id)))}; }

std::string lib_dir(LibId id) { return disk::path(disk::Lib, lib_key(id)).string(); }

std::string lib_day_file(LibId id, misc::Ymd d, size_t lvl) {
  disk::Key k = lib_key(id);
  k.lvl = level_name(lvl);
  k.date = d;
  return disk::path(disk::LibDayFile, k).string();
}

FeatureLib load_lib(LibId id) {
  FeatureLib lib;
  lib.id = id;
  lib.dir = lib_dir(id);
  if (!disk::exists(disk::LibManifest, lib_key(id)))
    return lib; // 未建库

  const nlohmann::json j = nlohmann::json::parse(disk::read(disk::LibManifest, lib_key(id)));
  assert(j.at("tier").get<std::string>() == tier_name(lib.tier()) && "库清单的档与目录不符");
  lib.dates = j.at("dates").get<std::vector<misc::Ymd>>();
  assert(!lib.dates.empty() && std::is_sorted(lib.dates.begin(), lib.dates.end()) && "库清单日期轴为空 / 非升序");

  const AssetAxis &table = asset_axis();
  const auto &codes = j.at("codes");
  std::vector<uint32_t> ids;
  ids.reserve(codes.size());
  for (const auto &c : codes) {
    const std::size_t id = table.find(c.get_ref<const std::string &>());
    assert(id < table.size() && "库清单代码不在 A 轴上");
    ids.push_back(static_cast<uint32_t>(id));
  }
  lib.axis = pool_axis(std::move(ids));

  if (const auto it = j.find("outside"); it != j.end())
    for (const auto &c : *it) {
      const std::size_t id = table.find(c.get_ref<const std::string &>());
      assert(id < table.size() && "库清单轴外代码不在 A 轴上");
      lib.outside.push_back(static_cast<uint32_t>(id));
    }
  assert(std::is_sorted(lib.outside.begin(), lib.outside.end()) && "库清单轴外须按全局 id 升序");
  return lib;
}

void save_lib(const FeatureLib &lib) {
  assert(lib.ready() && !lib.dates.empty() && "空库不落清单");
  const AssetAxis &table = asset_axis();
  std::vector<std::string> codes;
  codes.reserve(lib.axis.size());
  for (const uint32_t a : lib.axis.ids)
    codes.push_back(table.code(a));

  nlohmann::json j;
  j["tier"] = tier_name(lib.tier());
  j["dates"] = lib.dates;
  j["codes"] = codes;
  if (!lib.outside.empty()) {
    std::vector<std::string> outside;
    outside.reserve(lib.outside.size());
    for (const uint32_t a : lib.outside)
      outside.push_back(table.code(a));
    j["outside"] = outside;
  }

  disk::write(disk::LibManifest, lib_key(lib.id), j.dump(0)); // 原子替换: 读端要么见旧清单要么见新清单
}
