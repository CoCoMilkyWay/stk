// Stat 二级汇总 (factor::stat::HoldStat) 的 JSON 编解码: operators.json 的 stat.holds[] 与因子文件的 stat.holds[]
// 同一格式 (键名 = HoldStat 字段名: 三个逐行量的 Moments 展平为 <ric|rac|ls>_<矩>, 标量 beta / mono / leg_n, 数值 4 位有效).
// load_hold 是 Save 的逆: 键缺 / 类型不对 → false (不 assert: 文件是上次运行留下的, 可能来自旧代码 / 半截崩的; 旧键名的 stat 即作废重算).
// 换手成本累加器 (CostAcc, 因子文件 stat.costs[].acc[]) 同款 cost_acc_json / load_cost_acc: 两腿各一个对象 {imp, n, e} + rows.
#pragma once

#include "factor/Stat/Contract.hpp"

#include "nlohmann/json.hpp"

#include <cstdio>
#include <cstdlib>
#include <string>

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

inline bool json_ll(const nlohmann::json &j, const char *key, long long &out) {
  const auto it = j.find(key);
  if (it == j.end() || !it->is_number_integer())
    return false;
  out = it->get<long long>();
  return true;
}

// HoldStat 的浮点字段 = 三个逐行量 × 七个矩 + 三个标量 (键与字段同序)
inline constexpr const char *kSeriesKeys[] = {"ric", "rac", "ls"};
inline constexpr const char *kMomentKeys[] = {"mean", "sd", "ir", "t", "pos", "skew", "kurt"};
inline constexpr const char *kScalarKeys[] = {"beta", "mono", "leg_n"};
// CostAcc 的两腿 (与 factor::stat::Leg 同序)
inline constexpr const char *kLegKeys[factor::stat::kLegs] = {"long", "short"};

namespace detail {
template <class M>
auto moment_field(M &m, int q) -> decltype(&m.mean) {
  decltype(&m.mean) f[] = {&m.mean, &m.sd, &m.ir, &m.t, &m.pos, &m.skew, &m.kurt};
  static_assert(sizeof(f) / sizeof(f[0]) == sizeof(kMomentKeys) / sizeof(kMomentKeys[0]));
  return f[q];
}
template <class H>
auto series_of(H &h, int s) -> decltype(&h.ric) {
  decltype(&h.ric) f[] = {&h.ric, &h.rac, &h.ls};
  static_assert(sizeof(f) / sizeof(f[0]) == sizeof(kSeriesKeys) / sizeof(kSeriesKeys[0]));
  return f[s];
}
template <class H>
auto scalar_field(H &h, int q) -> decltype(&h.mono) {
  decltype(&h.mono) f[] = {&h.beta, &h.mono, &h.leg_n};
  static_assert(sizeof(f) / sizeof(f[0]) == sizeof(kScalarKeys) / sizeof(kScalarKeys[0]));
  return f[q];
}
// 逐个 (键, 字段) 访问 HoldStat 的全部浮点字段; H = HoldStat 或 const HoldStat
template <class H, class F>
void for_each_field(H &h, F &&f) {
  for (int s = 0; s < static_cast<int>(sizeof(kSeriesKeys) / sizeof(kSeriesKeys[0])); ++s)
    for (int q = 0; q < static_cast<int>(sizeof(kMomentKeys) / sizeof(kMomentKeys[0])); ++q)
      f(std::string(kSeriesKeys[s]) + "_" + kMomentKeys[q], *moment_field(*series_of(h, s), q));
  for (int q = 0; q < static_cast<int>(sizeof(kScalarKeys) / sizeof(kScalarKeys[0])); ++q)
    f(std::string(kScalarKeys[q]), *scalar_field(h, q));
}
} // namespace detail

inline nlohmann::ordered_json hold_json(const factor::stat::HoldStat &h) {
  nlohmann::ordered_json o;
  o["hold"] = h.hold;
  o["n"] = h.n;
  o["n_rac"] = h.n_rac;
  detail::for_each_field(h, [&](const std::string &k, const float &v) { o[k] = sig4(v); });
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
  if (!json_int(o, "hold", h.hold) || !json_int(o, "n", h.n) || !json_int(o, "n_rac", h.n_rac))
    return false;
  bool ok = true;
  detail::for_each_field(h, [&](const std::string &k, float &v) {
    double d = 0;
    ok = ok && json_num(o, k.c_str(), d);
    v = static_cast<float>(d);
  });
  if (!ok)
    return false;
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

// 换手成本累加器 (契约 CostAcc; 因子文件 stat.costs[].acc[]): 计数整数精确 (util / 换手占比的分子分母), 冲击和 4 位有效
inline nlohmann::ordered_json cost_acc_json(const factor::stat::CostAcc &c) {
  nlohmann::ordered_json o;
  for (int l = 0; l < factor::stat::kLegs; ++l) {
    nlohmann::ordered_json g;
    g["imp"] = sig4(c.leg[l].imp);
    g["n"] = c.leg[l].n;
    g["e"] = c.leg[l].e;
    o[kLegKeys[l]] = g;
  }
  o["rows"] = c.rows;
  return o;
}

inline bool load_cost_acc(const nlohmann::json &o, factor::stat::CostAcc &dst) {
  if (!o.is_object())
    return false;
  factor::stat::CostAcc c;
  for (int l = 0; l < factor::stat::kLegs; ++l) {
    const auto it = o.find(kLegKeys[l]);
    if (it == o.end() || !it->is_object())
      return false;
    factor::stat::CostLeg &g = c.leg[l];
    if (!json_num(*it, "imp", g.imp) || !json_ll(*it, "n", g.n) || !json_ll(*it, "e", g.e))
      return false;
  }
  if (!json_ll(o, "rows", c.rows))
    return false;
  dst = c;
  return true;
}

} // namespace GUI::Factors
