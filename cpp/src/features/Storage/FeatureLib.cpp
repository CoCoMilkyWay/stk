#include "features/Storage/FeatureLib.hpp"

#include "nlohmann/json.hpp"

#include <algorithm>
#include <cassert>
#include <filesystem>
#include <fstream>

// ============================================================================
// 特征库清单: <dir>/lib.json = {"tier", "dates": [YYYYMMDD...], "codes": ["CODE.EX"...], "outside": [...] (非空才有)}
// ============================================================================
namespace {
std::string lib_manifest(const std::string &dir) { return dir + "/lib.json"; }
} // namespace

std::string lib_dir(const std::string &feature_dir, LibId id) { return feature_dir + "/" + pool_def(lib_pool(id)).name + "/" + tier_name(lib_tier(id)); }

FeatureLib load_lib(const std::string &feature_dir, LibId id) {
  FeatureLib lib;
  lib.id = id;
  lib.dir = lib_dir(feature_dir, id);
  std::ifstream file(lib_manifest(lib.dir));
  if (!file.is_open())
    return lib; // 未建库

  const nlohmann::json j = nlohmann::json::parse(file);
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

  std::filesystem::create_directories(lib.dir);
  const std::string path = lib_manifest(lib.dir);
  const std::string tmp = path + ".tmp";
  {
    std::ofstream file(tmp);
    assert(file.is_open() && "save_lib: 临时文件打不开");
    file << j.dump(0);
    assert(file.good() && "save_lib: 写入失败");
  }
  std::filesystem::rename(tmp, path); // 原子替换: 读端要么见旧清单要么见新清单
}
