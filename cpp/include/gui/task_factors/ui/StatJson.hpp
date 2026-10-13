// Stat 二级汇总 (factor::stat::HoldStat) 的 JSON 编解码: operators.json 的 stat.holds[] 与因子文件的 stat.holds[]
// 同一格式 (键名 = HoldStat 字段名, 嵌套 Moments 展平为 ric_* / rac_*, 数值 4 位有效). load_hold 是 Save 的逆: 键缺 / 类型不对 → false (不 assert:
// 文件是上次运行留下的, 可能来自旧代码 / 半截崩的). 换手成本累加器 (CostAcc, 因子文件 stat.costs[].acc[]) 同款 cost_acc_json / load_cost_acc.
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

inline constexpr const char *kHoldStatKeys[] = {"ric_mean", "ric_sd", "ric_ir", "ric_t", "ric_pos", "ric_skew", "ric_kurt",
                                                "rac_mean", "rac_sd", "rac_ir", "rac_t", "rac_pos", "rac_skew", "rac_kurt",
                                                "ls_mean", "ls_t", "ls_pos", "sharpe", "beta", "mono", "leg"};
inline constexpr int kHoldStatKeyCount = static_cast<int>(sizeof(kHoldStatKeys) / sizeof(kHoldStatKeys[0]));

namespace detail {
// 键 ↔ 字段 (与 kHoldStatKeys 同序; 嵌套 Moments 的键 = <ric|rac>_<字段>)
template <class H>
auto hold_stat_field(H &h, int q) -> decltype(&h.mono) {
  decltype(&h.mono) f[kHoldStatKeyCount] = {&h.ric.mean, &h.ric.sd, &h.ric.ir, &h.ric.t, &h.ric.pos, &h.ric.skew, &h.ric.kurt,
                                            &h.rac.mean, &h.rac.sd, &h.rac.ir, &h.rac.t, &h.rac.pos, &h.rac.skew, &h.rac.kurt,
                                            &h.ls_mean, &h.ls_t, &h.ls_pos, &h.sharpe, &h.beta, &h.mono, &h.leg};
  return f[q];
}
} // namespace detail

inline nlohmann::ordered_json hold_json(const factor::stat::HoldStat &h) {
  nlohmann::ordered_json o;
  o["hold"] = h.hold;
  o["n"] = h.n;
  o["n_rac"] = h.n_rac;
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
  if (!json_int(o, "hold", h.hold) || !json_int(o, "n", h.n) || !json_int(o, "n_rac", h.n_rac))
    return false;
  if (o.contains("sign")) // 旧格式: 值按自动符号镜像过, 作废重算
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

// 换手成本累加器 (契约 CostAcc; 因子文件 stat.costs[].acc[]): 计数整数精确 (util / 换手占比的分子分母), 两个冲击和 4 位有效
inline constexpr const char *kCostAccKeys[] = {"n_hi", "n_lo", "e_hi", "e_lo", "rows"};
inline constexpr int kCostAccKeyCount = static_cast<int>(sizeof(kCostAccKeys) / sizeof(kCostAccKeys[0]));
namespace detail {
template <class C>
auto cost_acc_field(C &c, int q) -> decltype(&c.rows) {
  decltype(&c.rows) f[kCostAccKeyCount] = {&c.n_hi, &c.n_lo, &c.e_hi, &c.e_lo, &c.rows};
  return f[q];
}
} // namespace detail

inline nlohmann::ordered_json cost_acc_json(const factor::stat::CostAcc &c) {
  nlohmann::ordered_json o;
  o["buy_hi"] = sig4(c.buy_hi);
  o["sell_lo"] = sig4(c.sell_lo);
  for (int q = 0; q < kCostAccKeyCount; ++q)
    o[kCostAccKeys[q]] = *detail::cost_acc_field(c, q);
  return o;
}

inline bool load_cost_acc(const nlohmann::json &o, factor::stat::CostAcc &dst) {
  if (!o.is_object())
    return false;
  factor::stat::CostAcc c;
  if (!json_num(o, "buy_hi", c.buy_hi) || !json_num(o, "sell_lo", c.sell_lo))
    return false;
  for (int q = 0; q < kCostAccKeyCount; ++q) {
    const auto it = o.find(kCostAccKeys[q]);
    if (it == o.end() || !it->is_number_integer())
      return false;
    *detail::cost_acc_field(c, q) = it->get<long long>();
  }
  dst = c;
  return true;
}

} // namespace GUI::Factors
