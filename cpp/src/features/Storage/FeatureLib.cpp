#include "features/Storage/FeatureLib.hpp"

#include "nlohmann/json.hpp"

#include <algorithm>
#include <cassert>

// ============================================================================
// 池表 (FeatureLib.hpp 声明): [0] = all; 其余 = 子池表 (config/pools.json), 进程内首次用到时读一次
// ============================================================================
namespace {
const std::vector<PoolDef> &pools() {
  static const std::vector<PoolDef> v = [] {
    const std::vector<pool::Spec> specs = pool::load_pools();
    assert(specs.size() + 1 <= kMaxPools && "子池表超出库号 / 库集位宽");
    const std::vector<std::string> cn = pool::aliases(specs);
    std::vector<PoolDef> out{{kAllPoolName, "全市场", "all = 全市场: 当日活跃 (在市 ∧ 上市满 365 日 ∧ 行业已知, 含 ST)", {}}};
    for (size_t i = 0; i < specs.size(); ++i) {
      assert(pool::code(specs[i]) != kAllPoolName);
      out.push_back({pool::code(specs[i]), cn[i], pool::detail_cn(specs[i]), specs[i]});
    }
    return out;
  }();
  return v;
}
} // namespace

std::span<const PoolDef> feature_pools() { return pools(); }
size_t pool_count() { return pools().size(); }
uint64_t pool_hash() {
  static const uint64_t h = [] {
    uint64_t x = 0xcbf29ce484222325ULL; // FNV-1a, 各名以 '\0' 收尾
    for (const PoolDef &d : pools())
      for (const char c : d.name + '\0')
        x = (x ^ static_cast<uint8_t>(c)) * 0x100000001b3ULL;
    return x;
  }();
  return h;
}
std::string lib_name(LibId lib) { return pool_def(lib_pool(lib)).name + "." + tier_name(lib_tier(lib)); }

const PoolDef &pool_def(PoolId pool) {
  assert(pool < pools().size() && "池下标越界");
  return pools()[pool];
}
const PoolDef &pool_def(std::string_view name) { return pools()[pool_id(name)]; }
PoolId pool_id(std::string_view name) {
  for (size_t i = 0; i < pools().size(); ++i)
    if (name == pools()[i].name)
      return static_cast<PoolId>(i);
  assert(false && "不是池名 (all / 子池表 config/pools.json)");
  return kAllPool;
}

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
