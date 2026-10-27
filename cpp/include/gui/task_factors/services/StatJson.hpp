// Stat 二级汇总 (factor::stat::HoldStat) 的 JSON 编解码: 因子文件 stats[i].holds[] 每项 = 按 hold_cols() 列序的数组
// (列名 = HoldStat 字段名: 计数 hold / n / n_rac / n_all / n_trig, 三个逐行量的 Moments 展平为 <ric|rac|ls>_<矩>, 标量 beta / mono / leg_n,
// 末列 grp 数组; 数值 4 位有效). 列名只在文件顶层 hold_cols 写一次, 载入先比表头 (json_cols_are), 对不上整份 stat 作废.
// load_hold 是 Save 的逆: 长度 / 类型不对 → false (不 assert: 文件是上次运行留下的, 可能半截崩的 / 人手改的; 不合的 stat 即作废重算).
// 换手成本累加器 (CostAcc, 因子文件 stats[i].costs[].acc[]) 同款 cost_acc_json / load_cost_acc: 一行 = acc_cols() 列序 (两腿各 imp, buy, sell, w, n + rows);
// 一个金额段 (CostTier) cost_json / load_cost; 因子表迷你 PDF (MiniPdf) mini_pdf_json / load_mini_pdf;
// 一个根的评估结果 (EvalStat) 两半写 scope_json (文件顶层 run) + eval_json (stats[i]), 一次读 load_eval.
#pragma once

#include "factor/Stat/Contract.hpp"
#include "gui/task_factors/services/FactorsTable.hpp" // CostTier / MiniPdf / EvalStat
#include "misc/date.hpp"

#include "nlohmann/json.hpp"

#include <cassert>
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

// 结果作用域头 ↔ 因子文件顶层 run (一轮评估内全部权重同一份; f = 根口径; time 各权重各自, 在 eval_json)
inline nlohmann::ordered_json scope_json(const StatScope &s, factor::stat::Frame f) {
  nlohmann::ordered_json j;
  j["stat_ver"] = factor::stat::kStatSemver;
  j["pool"] = s.pool;
  j["start_date"] = misc::dash(s.start_date);
  j["end_date"] = misc::dash(s.end_date);
  j["days"] = s.days;
  j["T"] = s.T;
  j["A"] = s.A;
  j["backend"] = s.backend;
  j["frame"] = factor::stat::frame_name(f);
  j["max_impact"] = s.max_impact;
  j["n_full"] = sig4(s.n_full);
  return j;
}

// 一个根的评估结果 ↔ stats[i] (scope_json 之外的全部): 标量首段, holds / turn / costs / 小图各一段
inline nlohmann::ordered_json eval_json(const EvalStat &st) {
  nlohmann::ordered_json j;
  j["time"] = st.scope.time;
  j["valid_pct"] = sig4(st.valid_pct);
  j["rank_pct"] = sig4(st.rank_pct);
  j["eval_ms"] = sig4(st.eval_ms);
  nlohmann::ordered_json holds = nlohmann::ordered_json::array(), turn = nlohmann::ordered_json::array(), ic = nlohmann::ordered_json::array();
  for (int k = 0; k < st.n_hold; ++k) {
    holds.push_back(hold_json(st.hold[k]));
    turn.push_back(cost_acc_json(st.turn[k]));
    ic.push_back(mini_pdf_json(st.ic_pdf[k]));
  }
  j["holds"] = holds;
  j["turn"] = turn;
  nlohmann::ordered_json costs = nlohmann::ordered_json::array();
  for (const CostTier &c : st.costs)
    costs.push_back(cost_json(c, st.n_hold));
  j["costs"] = costs;
  j["ic_pdf"] = ic;
  j["alpha_pdf"] = mini_pdf_json(st.alpha_pdf);
  return j;
}

// scope_json + eval_json 的逆: run 口径与 f 不符 / stat_ver ≠ kStatSemver / 任一处不合 → false (dst 不动). 小图可选 (老文件没有 → 空)
inline bool load_eval(const nlohmann::json &run, const nlohmann::json &j, int seg, factor::stat::Frame f, EvalStat &dst) {
  if (!run.is_object() || !j.is_object() || !json_str_is(run, "frame", factor::stat::frame_name(f)))
    return false;
  if (int ver = 0; !json_int(run, "stat_ver", ver) || ver != factor::stat::kStatSemver)
    return false;
  EvalStat t;
  const auto str = [](const nlohmann::json &o, const char *key, std::string &out) {
    const auto it = o.find(key);
    if (it == o.end() || !it->is_string())
      return false;
    out = it->get<std::string>();
    return true;
  };
  const auto ymd = [&](const nlohmann::json &o, const char *key, misc::Ymd &out) {
    std::string s;
    if (!str(o, key, s))
      return false;
    out = misc::parse_ymd(s);
    assert(out && "因子文件 run 日期非 YYYY-MM-DD");
    return true;
  };
  StatScope &sc = t.scope;
  if (!str(run, "pool", sc.pool) || !ymd(run, "start_date", sc.start_date) || !ymd(run, "end_date", sc.end_date) || !str(run, "backend", sc.backend) ||
      !str(j, "time", sc.time))
    return false;
  if (!json_int(run, "days", sc.days) || !json_int(run, "T", sc.T) || !json_int(run, "A", sc.A))
    return false;
  if (!json_num(run, "max_impact", sc.max_impact) || !json_num(run, "n_full", sc.n_full) || !(sc.n_full > 0.0))
    return false;
  double v = 0;
  if (!json_num(j, "valid_pct", v))
    return false;
  t.valid_pct = static_cast<float>(v);
  if (!json_num(j, "rank_pct", v))
    return false;
  t.rank_pct = static_cast<float>(v);
  if (!json_num(j, "eval_ms", t.eval_ms))
    return false;
  const auto hit = j.find("holds");
  if (hit == j.end() || !hit->is_array() || hit->empty() || hit->size() > static_cast<size_t>(factor::stat::kMaxHold))
    return false;
  t.n_hold = static_cast<int>(hit->size());
  const size_t H = hit->size();
  const auto tit = j.find("turn");
  if (tit == j.end() || !tit->is_array() || tit->size() != H)
    return false;
  for (size_t k = 0; k < H; ++k)
    if (!load_hold((*hit)[k], seg, t.hold[k]) || !load_cost_acc((*tit)[k], t.turn[k]))
      return false;
  const auto cit = j.find("costs");
  if (cit == j.end() || !cit->is_array())
    return false;
  t.costs.resize(cit->size());
  for (size_t c = 0; c < cit->size(); ++c)
    if (!load_cost((*cit)[c], t.n_hold, t.costs[c]))
      return false;
  if (const auto pit = j.find("ic_pdf"); pit != j.end()) {
    if (!pit->is_array() || pit->size() != H)
      return false;
    for (size_t k = 0; k < H; ++k)
      if (!load_mini_pdf((*pit)[k], t.ic_pdf[k]))
        return false;
  }
  if (const auto pit = j.find("alpha_pdf"); pit != j.end() && !load_mini_pdf(*pit, t.alpha_pdf))
    return false;
  dst = std::move(t);
  return true;
}

} // namespace GUI::Factors
