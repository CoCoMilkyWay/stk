#include "fundamental/AssetAxis.hpp"

#include "disk/Disk.hpp"
#include "misc/epoch.hpp"
#include "nlohmann/json.hpp"

#include <algorithm>
#include <cassert>
#include <functional>

namespace {

constexpr std::uint64_t kFnvOffset = 1469598103934665603ULL;
constexpr std::uint64_t kFnvPrime = 1099511628211ULL;

// 累积一条资产码进 hash 链. 每条码末尾多走一轮纯乘法作分隔, 否则
// "00"+"0001" 与 "000"+"001" 会落在同一条链上.
std::uint64_t fnv_append(std::uint64_t h, const std::string &s) {
  for (char c : s) {
    h ^= static_cast<std::uint8_t>(c);
    h *= kFnvPrime;
  }
  return h * kFnvPrime;
}

} // namespace

void AssetAxis::load() {
  loaded_ = true;
  codes_.clear();
  index_.clear();
  prefix_hash_.assign(1, kFnvOffset);
  dirty_ = false;

  if (!disk::exists(disk::AssetAxisFile))
    return; // 首次建库, 空轴

  const nlohmann::json j = nlohmann::json::parse(disk::read(disk::AssetAxisFile));

  assert(j.at("version").get<int>() == kVersion && "AssetAxis: 注册表版本不认识");

  const auto &assets = j.at("assets");
  assert(assets.is_array() && "AssetAxis: assets 不是数组");

  const std::size_t count = j.at("count").get<std::size_t>();
  assert(count == assets.size() && "AssetAxis: count 与 assets 长度不符 (注册表损坏)");

  codes_.reserve(count);
  index_.reserve(count * 2);
  prefix_hash_.reserve(count + 1);

  for (const auto &e : assets) {
    std::string code_ex = e.get<std::string>();
    assert(!code_ex.empty() && "AssetAxis: 空资产码 (注册表损坏)");
    const std::size_t idx = codes_.size();
    index_.emplace(code_ex, idx);
    assert(index_.size() == idx + 1 && "AssetAxis: 注册表内重复资产码 (损坏)");
    prefix_hash_.push_back(fnv_append(prefix_hash_.back(), code_ex));
    codes_.push_back(std::move(code_ex));
  }

  // 全量累积 hash 自校验: 名单被外部改动/截断/重排 → 立刻暴露
  assert(j.at("hash").get<std::uint64_t>() == prefix_hash_.back() &&
         "AssetAxis: 注册表 hash 不符 (内容被改动或损坏)");
}

std::size_t AssetAxis::intern(const std::string &code_ex) {
  assert(!code_ex.empty() && "AssetAxis::intern: 空资产码");
  auto it = index_.find(code_ex);
  if (it != index_.end())
    return it->second;

  const std::size_t idx = codes_.size();
  index_.emplace(code_ex, idx);
  prefix_hash_.push_back(fnv_append(prefix_hash_.back(), code_ex));
  codes_.push_back(code_ex);
  dirty_ = true;
  return idx;
}

void AssetAxis::save() const {
  if (!dirty_)
    return;
  assert(loaded_ && "AssetAxis::save: 未 load");

  nlohmann::json j;
  j["version"] = kVersion;
  j["count"] = codes_.size();
  j["hash"] = prefix_hash_.back();
  j["assets"] = codes_;

  disk::write(disk::AssetAxisFile, {}, j.dump(0)); // 原子替换: 崩在中途也不会留半截注册表
  dirty_ = false;
}

const std::string &AssetAxis::code(std::size_t i) const {
  assert(i < codes_.size() && "AssetAxis::code: 下标越界");
  return codes_[i];
}

std::uint64_t AssetAxis::hash_at(std::size_t n) const {
  assert(n < prefix_hash_.size() && "AssetAxis::hash_at: n 超过轴长度");
  return prefix_hash_[n];
}

std::size_t AssetAxis::find(const std::string &code_ex) const {
  auto it = index_.find(code_ex);
  return it == index_.end() ? codes_.size() : it->second;
}

AssetAxis &asset_axis() {
  struct Loaded : AssetAxis {
    Loaded() { load(); }
  };
  static misc::Epochal<Loaded> instance;
  return instance.get();
}

PoolAxis pool_axis(std::vector<uint32_t> ids) {
  const AssetAxis &table = asset_axis();
  assert(!ids.empty() && "池轴为空");
  assert(std::adjacent_find(ids.begin(), ids.end(), std::greater_equal<>()) == ids.end() && "池轴 ids 须升序去重");
  assert(ids.back() < table.size() && "池轴资产不在 A 轴上");

  PoolAxis out;
  out.ids = std::move(ids);
  out.hash = kFnvOffset;
  for (const uint32_t a : out.ids)
    out.hash = fnv_append(out.hash, table.code(a));
  return out;
}
