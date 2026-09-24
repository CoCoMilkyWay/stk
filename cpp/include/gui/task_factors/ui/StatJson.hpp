// Stat 二级汇总 (factor::stat::HoldStat) 的 JSON 编解码: operators.json 的 stat.holds[] 与因子文件的 stat.holds[]
// 同一格式 (键名 = HoldStat 字段名, 数值 4 位有效). load_hold 是 Save 的逆: 键缺 / 类型不对 → false (不 assert:
// 文件是上次运行留下的, 可能来自旧代码 / 半截崩的).
#pragma once

#include "factor/Stat/Contract.hpp"

#include "nlohmann/json.hpp"

#include <cstdio>
#include <cstdlib>

namespace GUI::Factors {

// 4 位有效数字 (人读; 耗时 / 误差全精度只添噪)
inline double sig4(double v) {
  char buf[32];
  std::snprintf(buf, sizeof(buf), "%.4g", v);
  return std::strtod(buf, nullptr);
}

inline bool json_num(const nlohmann::json &j, const char *key, double &out) {
  const auto it = j.find(key);
  if (it == j.end() || !it->is_number())
    return false;
  out = it->get<double>();
  return true;
}

inline bool json_int(const nlohmann::json &j, const char *key, int &out) {
  const auto it = j.find(key);
  if (it == j.end() || !it->is_number_integer())
    return false;
  out = it->get<int>();
  return true;
}

inline bool json_str_is(const nlohmann::json &j, const char *key, const char *want) {
  const auto it = j.find(key);
  return it != j.end() && it->is_string() && it->get_ref<const std::string &>() == want;
}

inline constexpr const char *kHoldStatKeys[] = {"ic_mean", "ic_std", "icir", "ic_t", "ic_pos", "ic_skew", "ic_kurt",
                                                "ls_mean", "ls_t", "ls_pos", "sharpe", "beta", "mono", "rank_ac"};
inline constexpr int kHoldStatKeyCount = static_cast<int>(sizeof(kHoldStatKeys) / sizeof(kHoldStatKeys[0]));

namespace detail {
// 键 ↔ 字段 (与 kHoldStatKeys 同序)
template <class H>
auto hold_stat_field(H &h, int q) -> decltype(&h.ic_mean) {
  decltype(&h.ic_mean) f[kHoldStatKeyCount] = {&h.ic_mean, &h.ic_std, &h.icir, &h.ic_t, &h.ic_pos, &h.ic_skew, &h.ic_kurt,
                                               &h.ls_mean, &h.ls_t, &h.ls_pos, &h.sharpe, &h.beta, &h.mono, &h.rank_ac};
  return f[q];
}
} // namespace detail

inline nlohmann::ordered_json hold_json(const factor::stat::HoldStat &h) {
  nlohmann::ordered_json o;
  o["hold"] = h.hold;
  o["n"] = h.n;
  o["n_ac"] = h.n_ac;
  for (int q = 0; q < kHoldStatKeyCount; ++q)
    o[kHoldStatKeys[q]] = sig4(*detail::hold_stat_field(h, q));
  nlohmann::ordered_json g = nlohmann::ordered_json::array();
  for (int k = 0; k < factor::stat::kGroups; ++k)
    g.push_back(sig4(h.grp[k]));
  o["grp"] = g;
  return o;
}

inline bool load_hold(const nlohmann::json &o, factor::stat::HoldStat &dst) {
  if (!o.is_object())
    return false;
  factor::stat::HoldStat h;
  if (!json_int(o, "hold", h.hold) || !json_int(o, "n", h.n) || !json_int(o, "n_ac", h.n_ac))
    return false;
  for (int q = 0; q < kHoldStatKeyCount; ++q) {
    double v = 0;
    if (!json_num(o, kHoldStatKeys[q], v))
      return false;
    *detail::hold_stat_field(h, q) = static_cast<float>(v);
  }
  const auto git = o.find("grp");
  if (git == o.end() || !git->is_array() || git->size() != static_cast<size_t>(factor::stat::kGroups))
    return false;
  for (int k = 0; k < factor::stat::kGroups; ++k) {
    const nlohmann::json &e = (*git)[static_cast<size_t>(k)];
    if (!e.is_number())
      return false;
    h.grp[k] = e.get<float>();
  }
  dst = h;
  return true;
}

} // namespace GUI::Factors
