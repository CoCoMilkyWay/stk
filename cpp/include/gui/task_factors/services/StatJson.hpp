// Stat 二级汇总 (factor::stat::HoldStat) 的 JSON 编解码: 因子文件 stats[i].holds[] 每项 = 按 hold_cols() 列序的数组
// (列名 = HoldStat 字段名: 计数 hold / n / n_rac / n_all / n_trig, 三个逐行量的 Moments 展平为 <ric|rac|ls>_<矩>, 标量 beta / mono / leg_n,
// 末列 grp 数组; 数值 4 位有效). 列名只在文件顶层 hold_cols 写一次, 载入先比表头 (json_cols_are), 对不上整份 stat 作废.
// load_hold 是 Save 的逆: 长度 / 类型不对 → false (不 assert: 文件是上次运行留下的, 可能半截崩的 / 人手改的; 不合的 stat 即作废重算).
// 换手成本累加器 (CostAcc, 因子文件 stats[i].costs[].acc[]) 同款 cost_acc_json / load_cost_acc: 一行 = acc_cols() 列序 (两腿各 imp, buy, sell, w, n + rows);
// 一个金额段 (CostTier) cost_json / load_cost; 因子表迷你 PDF (MiniPdf) mini_pdf_json / load_mini_pdf.
#pragma once

#include "factor/Stat/Contract.hpp"
#include "gui/task_factors/services/FactorsTable.hpp" // CostTier / MiniPdf

#include "nlohmann/json.hpp"

#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

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
// CostAcc 的两腿 (与 factor::stat::Leg 同序) × 每腿字段 (与 CostLeg 同序)
inline constexpr const char *kLegKeys[factor::stat::kLegs] = {"long", "short"};
inline constexpr const char *kCostLegKeys[] = {"imp", "buy", "sell", "w", "n"};

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

// 行式表的列名 (因子文件顶层 hold_cols / acc_cols): 与 hold_json / cost_acc_json 的元素一一对应
inline std::vector<std::string> hold_cols() {
  std::vector<std::string> c = {"hold", "n", "n_rac", "n_all", "n_trig"};
  factor::stat::HoldStat h{};
  detail::for_each_field(h, [&](const std::string &k, const float &) { c.push_back(k); });
  c.push_back("grp");
  return c;
}
inline std::vector<std::string> acc_cols() {
  std::vector<std::string> c;
  for (const char *l : kLegKeys)
    for (const char *k : kCostLegKeys)
      c.push_back(std::string(l) + "." + k);
  c.push_back("rows");
  return c;
}
inline nlohmann::ordered_json cols_json(const std::vector<std::string> &cols) {
  nlohmann::ordered_json a = nlohmann::ordered_json::array();
  for (const std::string &c : cols)
    a.push_back(c);
  return a;
}
// j[key] 是否恰为当前代码的列名表 (J = json / ordered_json: 扫描读 / 回写比)
template <class J>
bool json_cols_are(const J &j, const char *key, const std::vector<std::string> &want) {
  const auto it = j.find(key);
  if (it == j.end() || !it->is_array() || it->size() != want.size())
    return false;
  for (size_t i = 0; i < want.size(); ++i)
    if (!(*it)[i].is_string() || (*it)[i].template get_ref<const std::string &>() != want[i])
      return false;
  return true;
}

inline nlohmann::ordered_json hold_json(const factor::stat::HoldStat &h) {
  nlohmann::ordered_json o = nlohmann::ordered_json::array();
  for (int v : {h.hold, h.n, h.n_rac, h.n_all, h.n_trig})
    o.push_back(v);
  detail::for_each_field(h, [&](const std::string &, const float &v) { o.push_back(sig4(v)); });
  nlohmann::ordered_json g = nlohmann::ordered_json::array();
  for (int k = 0; k < factor::stat::kGroups; ++k)
    g.push_back(sig4(h.grp[k]));
  o.push_back(g);
  return o;
}

// seg = 因子目录所在层 (HoldStat::seg 不落文件)
inline bool load_hold(const nlohmann::json &o, int seg, factor::stat::HoldStat &dst) {
  if (!o.is_array() || o.size() != hold_cols().size())
    return false;
  factor::stat::HoldStat h;
  size_t c = 0;
  for (int *p : {&h.hold, &h.n, &h.n_rac, &h.n_all, &h.n_trig}) {
    const nlohmann::json &e = o[c++];
    if (!e.is_number_integer())
      return false;
    *p = e.get<int>();
  }
  bool ok = true;
  detail::for_each_field(h, [&](const std::string &, float &v) {
    const nlohmann::json &e = o[c++];
    ok = ok && e.is_number();
    v = ok ? e.get<float>() : 0.f;
  });
  if (!ok)
    return false;
  const nlohmann::json &g = o[c];
  if (!g.is_array() || g.size() != static_cast<size_t>(factor::stat::kGroups))
    return false;
  for (int k = 0; k < factor::stat::kGroups; ++k) {
    const nlohmann::json &e = g[static_cast<size_t>(k)];
    if (!e.is_number())
      return false;
    h.grp[k] = e.get<float>();
  }
  h.seg = seg;
  dst = h;
  return true;
}

// 换手成本累加器 (契约 CostAcc; 因子文件 stat.costs[].acc[]): 计数整数精确, 量 / 冲击和 4 位有效
inline nlohmann::ordered_json cost_acc_json(const factor::stat::CostAcc &c) {
  nlohmann::ordered_json o = nlohmann::ordered_json::array();
  for (const factor::stat::CostLeg &g : c.leg) {
    o.push_back(sig4(g.imp));
    o.push_back(sig4(g.buy));
    o.push_back(sig4(g.sell));
    o.push_back(sig4(g.w));
    o.push_back(g.n);
  }
  o.push_back(c.rows);
  return o;
}

inline bool load_cost_acc(const nlohmann::json &o, factor::stat::CostAcc &dst) {
  if (!o.is_array() || o.size() != acc_cols().size())
    return false;
  factor::stat::CostAcc c;
  size_t i = 0;
  const auto num = [&](double &v) {
    const nlohmann::json &e = o[i++];
    v = e.is_number() ? e.get<double>() : 0.0;
    return e.is_number();
  };
  const auto ll = [&](long long &v) {
    const nlohmann::json &e = o[i++];
    v = e.is_number_integer() ? e.get<long long>() : 0;
    return e.is_number_integer();
  };
  for (factor::stat::CostLeg &g : c.leg)
    if (!num(g.imp) || !num(g.buy) || !num(g.sell) || !num(g.w) || !ll(g.n))
      return false;
  if (!ll(c.rows))
    return false;
  dst = c;
  return true;
}

// 一个金额段的换手成本 ↔ JSON {amt, acc: [n_hold 个 CostAcc]}
inline nlohmann::ordered_json cost_json(const CostTier &c, int n_hold) {
  nlohmann::ordered_json o;
  o["amt"] = c.amt;
  nlohmann::ordered_json acc = nlohmann::ordered_json::array();
  for (int k = 0; k < n_hold; ++k)
    acc.push_back(cost_acc_json(c.acc[k]));
  o["acc"] = acc;
  return o;
}
inline bool load_cost(const nlohmann::json &o, int n_hold, CostTier &dst) {
  if (!o.is_object())
    return false;
  CostTier t;
  if (!json_int(o, "amt", t.amt))
    return false;
  const auto ait = o.find("acc");
  if (ait == o.end() || !ait->is_array() || ait->size() != static_cast<size_t>(n_hold))
    return false;
  for (int k = 0; k < n_hold; ++k)
    if (!load_cost_acc((*ait)[static_cast<size_t>(k)], t.acc[k]))
      return false;
  dst = t;
  return true;
}

// 因子表迷你 PDF ↔ JSON {lo, hi, y: [kMiniPdfPts]} (y 已归一到 [0, 1], 3 位小数够画)
inline nlohmann::ordered_json mini_pdf_json(const MiniPdf &p) {
  nlohmann::ordered_json o;
  o["lo"] = sig4(p.lo);
  o["hi"] = sig4(p.hi);
  nlohmann::ordered_json y = nlohmann::ordered_json::array();
  for (const float v : p.y)
    y.push_back(std::round(v * 1000.f) / 1000.0);
  o["y"] = y;
  return o;
}
inline bool load_mini_pdf(const nlohmann::json &o, MiniPdf &dst) {
  if (!o.is_object())
    return false;
  MiniPdf t;
  double lo = 0.0, hi = 0.0;
  if (!json_num(o, "lo", lo) || !json_num(o, "hi", hi))
    return false;
  t.lo = static_cast<float>(lo), t.hi = static_cast<float>(hi);
  const auto yit = o.find("y");
  if (yit == o.end() || !yit->is_array() || yit->size() != static_cast<size_t>(kMiniPdfPts))
    return false;
  for (int i = 0; i < kMiniPdfPts; ++i) {
    const nlohmann::json &v = (*yit)[static_cast<size_t>(i)];
    if (!v.is_number())
      return false;
    t.y[i] = v.get<float>();
  }
  dst = t;
  return true;
}

} // namespace GUI::Factors
