// FactorsFile — 见头文件. include 因子契约头 (Expr → Contract.hpp: isfinite), CMake 列入 PRECISE_MATH 源.
#include "gui/task_factors/services/FactorsFile.hpp"

#include "factor/Dag.hpp" // build (n_feats / n_slots 列)
#include "gui/task_factors/services/StatJson.hpp"

#include "nlohmann/json.hpp"
#include "utfcpp/utf8.hpp"

#include <algorithm>
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string_view>

namespace GUI::Factors {

namespace {

using json = nlohmann::json;
using ojson = nlohmann::ordered_json;

// 评估派生数据 (run + 表头 + stats): 只描述当前 weights, 权重整体换掉 / 清空时一起删
void erase_stats(ojson &j) {
  j.erase("run");
  j.erase("hold_cols");
  j.erase("acc_cols");
  j.erase("stats");
}

// 一个 stat: 标量 (time / valid_pct / …) 首行, 其余结构值各一行 (holds 一行), costs 每金额段一行
std::string stat_block(const ojson &st) {
  if (!st.is_object())
    return st.dump();
  std::string head, body;
  for (auto it = st.begin(); it != st.end(); ++it) {
    const ojson &v = it.value();
    const std::string kv = ojson(it.key()).dump() + ":";
    if (it.key() == "costs" && v.is_array() && !v.empty()) {
      body += ",\n   " + kv + "[";
      for (size_t c = 0; c < v.size(); ++c)
        body += "\n    " + v[c].dump() + (c + 1 < v.size() ? "," : "");
      body += "]";
    } else if (v.is_structured()) {
      body += ",\n   " + kv + v.dump();
    } else {
      head += (head.empty() ? "" : ",") + kv + v.dump();
    }
  }
  if (head.empty() && !body.empty())
    body.erase(0, 1); // 无标量: 去掉首段前的逗号
  return "{" + head + body + "}";
}

// 因子文件排版 (给人看, 合法 JSON): 顶层一键一行, 人写的键 (type / name_cn / expr / note / …) 在前, 之后 weights → 评估派生
// (run / hold_cols / acc_cols / stats); weights 一元素一行, stats 一元素一块 (stat_block; stats[i] ↔ weights[i]), 数值表密排不展开
std::string format_factor(const ojson &j) {
  assert(j.is_object());
  static constexpr std::string_view kTail[] = {"weights", "run", "hold_cols", "acc_cols", "stats"};
  std::vector<std::string> top; // 顶层一键一段 (段间 ",\n")
  const auto add = [&](const std::string &k, const ojson &v) {
    std::string s = " " + ojson(k).dump() + ": ";
    if ((k == "weights" || k == "stats") && v.is_array() && !v.empty()) {
      s += "[\n";
      for (size_t i = 0; i < v.size(); ++i)
        s += "  " + (k == "stats" ? stat_block(v[i]) : v[i].dump()) + (i + 1 < v.size() ? ",\n" : "\n");
      s += " ]";
    } else {
      s += v.dump();
    }
    top.push_back(s);
  };
  for (auto it = j.begin(); it != j.end(); ++it)
    if (std::find(std::begin(kTail), std::end(kTail), it.key()) == std::end(kTail))
      add(it.key(), it.value());
  for (std::string_view k : kTail)
    if (const auto it = j.find(std::string(k)); it != j.end())
      add(it.key(), it.value());
  std::string out = "{\n";
  for (size_t i = 0; i < top.size(); ++i)
    out += top[i] + (i + 1 < top.size() ? ",\n" : "\n");
  return out + "}\n";
}

// 已扫描过的文件读回 (须是对象: 扫描时已解析过, 不该变)
ojson read_json(const std::filesystem::path &path) {
  std::ifstream f(path);
  assert(f.is_open() && "因子文件不存在");
  ojson j = ojson::parse(f, nullptr, /*allow_exceptions=*/false);
  assert(j.is_object() && "因子文件扫描时已解析过, 不该变");
  return j;
}

// tmp + rename 原子落盘 (同 operators.json / features.json)
void write_json(const std::filesystem::path &path, const ojson &j) {
  std::filesystem::create_directories(path.parent_path());
  const std::filesystem::path tmp = path.string() + ".tmp";
  {
    std::ofstream f(tmp);
    assert(f.is_open() && "因子文件: 临时文件打不开");
    f << format_factor(j);
    assert(f.good() && "因子文件: 写入失败");
  }
  std::filesystem::rename(tmp, path);
}

// 模板源串 → 规范串 (parse + 格点 + root_frame 过); 不合法 → false + err
bool parse_template(const FeatureTable &feats, std::string_view src, factor::expr::Expr &e, std::string &err) {
  factor::stat::Frame fr;
  return factor::expr::parse(src, feats.lookup(), feats.seg, e, err) && feats.check_grid(e, err) && factor::expr::root_frame(e, feats.seg, fr, err);
}

} // namespace

// ============================================================================
// 名字 / 目标
// ============================================================================

bool ValidFactorName(std::string_view name) {
  if (name.empty() || name.size() > 64)
    return false;
  for (char c : name)
    if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'))
      return false;
  return true;
}

bool ValidFactorNameCn(std::string_view name) {
  if (name.empty() || !utf8::is_valid(name.begin(), name.end()))
    return false;
  int n = 0;
  for (auto it = name.begin(); it != name.end(); ++n) {
    const utf8::utfchar32_t cp = utf8::next(it, name.end());
    if (cp < 0x4E00 || cp > 0x9FFF)
      return false;
  }
  return n <= 10;
}

factor::search::Score RowScore(const FactorRow &r, int hold) {
  const factor::stat::HoldStat *h = r.has_stat ? r.find_hold(hold) : nullptr;
  if (!h)
    return {};
  return factor::search::score_of(*h);
}

// ============================================================================
// 读: ScanFactorFile
// ============================================================================

void ScanFactorFile(const std::filesystem::path &p, const FeatureTable &ft, std::vector<FactorRow> &out) {
  const factor::expr::FeatureLookup fl = ft.lookup();
  const auto emit = [&](FactorRow &r) {
    r.key = row_key(r.file, r.widx);
    out.push_back(r);
  };
  FactorRow r;
  r.file = p.filename().string();
  r.name_en = p.stem().string();
  r.status = RowStatus::Pending;
  std::ifstream f(p);
  json j(json::value_t::discarded);
  if (f.is_open())
    j = json::parse(f, nullptr, /*allow_exceptions=*/false);
  if (j.is_discarded() || !j.is_object()) {
    r.error = "JSON 解析失败 / 顶层不是对象";
    emit(r);
    return;
  }
  if (const auto it = j.find("note"); it != j.end() && it->is_string())
    r.note = it->get<std::string>();
  if (const auto it = j.find("name_cn"); it != j.end() && it->is_string())
    r.name_cn = it->get<std::string>();
  if (!ValidFactorNameCn(r.name_cn)) {
    r.error = "缺 name_cn 键或值不合 (纯汉字, 1..10 字)";
    emit(r);
    return;
  }
  const auto eit = j.find("expr");
  if (eit == j.end() || !eit->is_string()) {
    r.error = "缺 expr 键或不是字串";
    emit(r);
    return;
  }
  r.expr_raw = eit->get<std::string>();
  // type: 必填; beta 只留位
  if (json_str_is(j, "type", kind_name(FactorKind::Beta))) {
    r.kind = FactorKind::Beta;
    r.error = "beta 类因子未实现 (留位)";
    emit(r);
    return;
  }
  if (!json_str_is(j, "type", kind_name(FactorKind::Alpha))) {
    r.error = "缺 type 键或值不合 (alpha | beta)";
    emit(r);
    return;
  }
  factor::expr::Expr t; // 模板
  if (!factor::expr::parse(r.expr_raw, fl, ft.seg, t, r.error) || !ft.check_grid(t, r.error)) {
    emit(r);
    return;
  }
  r.tmpl = t.canon; // 表里显示规范串, 便于看根
  if (!factor::expr::root_frame(t, ft.seg, r.frame, r.error)) {
    emit(r);
    return;
  }
  // 权重: weights 数组每项一行; 无 weights 时, 模板具体 → 手填唯一权重, 否则 "未搜索" 一行
  struct W {
    int hold = 0, sign = 1;
    std::string expr, err;
  };
  std::vector<W> ws;
  if (const auto wit = j.find("weights"); wit != j.end() && !(wit->is_array() && wit->empty())) {
    if (!wit->is_array()) {
      r.error = "weights 须是数组";
      emit(r);
      return;
    }
    for (size_t i = 0; i < wit->size(); ++i) {
      const json &o = (*wit)[i];
      W w;
      if (!o.is_object() || !json_int(o, "hold", w.hold)) {
        w.err = "weights[" + std::to_string(i) + "] 须是对象且有整数 hold";
      } else if (w.hold != 0 && !find_leg(ft.y.labels, w.hold)) {
        w.err = "weights[" + std::to_string(i) + "] 的 hold " + std::to_string(w.hold) + " 不在本层持有期里 (已废的持有期, 作废)";
      } else if (o.find("sign") != o.end() && !(json_int(o, "sign", w.sign) && (w.sign == 1 || w.sign == -1))) {
        w.err = "weights[" + std::to_string(i) + "] 的 sign 须是 1 或 -1";
      } else if (const auto xit = o.find("expr"); xit == o.end() || !xit->is_string()) {
        w.err = "weights[" + std::to_string(i) + "] 缺 expr 键或不是字串";
      } else {
        w.expr = xit->get<std::string>();
      }
      ws.push_back(std::move(w));
    }
  } else if (!t.is_template()) {
    ws.push_back(W{0, 1, t.canon, {}});
  }
  if (ws.empty()) { // 模板未搜索
    r.status = RowStatus::Done;
    emit(r);
    return;
  }
  // run + 表头对得上当前代码且 stats 与 weights 同长才吃 stats[i] (否则全部忽略, 下次 Run 覆盖)
  const json *run = nullptr, *stats = nullptr;
  if (const auto rit = j.find("run"), sit = j.find("stats"); rit != j.end() && sit != j.end() && sit->is_array() && sit->size() == ws.size() &&
                                                             json_cols_are(j, "hold_cols", hold_cols()) && json_cols_are(j, "acc_cols", acc_cols()))
    run = &*rit, stats = &*sit;
  for (size_t i = 0; i < ws.size(); ++i) {
    FactorRow w = r;
    w.widx = static_cast<int>(i);
    w.hold_src = ws[i].hold;
    if (!ws[i].err.empty()) {
      w.error = ws[i].err;
      emit(w);
      continue;
    }
    factor::expr::Expr e;
    if (!factor::expr::parse(ws[i].expr, fl, ft.seg, e, w.error)) {
      w.error = "weights[" + std::to_string(i) + "]: " + w.error;
      emit(w);
      continue;
    }
    if (!factor::expr::instance_of(t, e)) {
      w.error = "weights[" + std::to_string(i) + "] 不是模板的实例 (结构 / 固定参数须同模板, 自由参数须落值)";
      emit(w);
      continue;
    }
    w.sign = ws[i].sign;
    if (w.sign < 0) // 评估对象 = 根下插 TsNeg
      e = ft.reparse(factor::expr::negated_canon(e));
    w.expr = e.canon;
    w.n_ops = e.n_ops;
    const factor::Dag d = factor::build(e);
    w.n_feats = static_cast<int>(d.feats.size());
    w.n_slots = d.n_slots;
    // stat 是派生数据: 不合 / 口径与当前根不符 → 整块忽略 (不算 BROKEN, 下次 Run 覆盖)
    if (stats && load_eval(*run, (*stats)[i], ft.seg, w.frame, w))
      w.has_stat = w.stat_from_file = true;
    emit(w);
  }
}

// ============================================================================
// 写: 构建器 Add / Update / Delete, Search WriteFactorWeights, Run WriteFactorStats
// ============================================================================

std::string AddFactorFile(const std::string &factor_dir, const FeatureTable &feats, std::string_view name, std::string_view name_cn,
                          std::string_view expr_src, std::string_view note, std::string &err) {
  assert(ValidFactorName(name) && ValidFactorNameCn(name_cn));
  factor::expr::Expr e;
  if (!parse_template(feats, expr_src, e, err))
    return "";
  const std::string file = std::string(name) + ".json";
  const std::filesystem::path path = std::filesystem::path(factor_dir) / file;
  if (std::filesystem::exists(path)) {
    err = "已存在 " + file;
    return "";
  }
  ojson j;
  j["type"] = kind_name(FactorKind::Alpha);
  j["name_cn"] = std::string(name_cn);
  j["expr"] = e.canon;
  if (!note.empty())
    j["note"] = std::string(note);
  write_json(path, j);
  return file;
}

std::string UpdateFactorFile(const std::string &factor_dir, const FeatureTable &feats, std::string_view file, std::string_view new_name,
                             std::string_view name_cn, std::string_view expr_src, std::string_view note, std::string &err) {
  assert(ValidFactorName(new_name) && ValidFactorNameCn(name_cn));
  factor::expr::Expr e;
  if (!parse_template(feats, expr_src, e, err))
    return "";
  const std::filesystem::path from = std::filesystem::path(factor_dir) / file;
  const std::string new_file = std::string(new_name) + ".json";
  const std::filesystem::path to = std::filesystem::path(factor_dir) / new_file;
  assert(std::filesystem::exists(from));
  if (to != from && std::filesystem::exists(to)) {
    err = "已存在 " + new_file;
    return "";
  }
  // 原文件可能是 BROKEN (非 JSON / 非对象): 那就从空对象重建; 否则保留人加的其他键
  ojson j(ojson::value_t::discarded);
  {
    std::ifstream f(from);
    assert(f.is_open());
    j = ojson::parse(f, nullptr, /*allow_exceptions=*/false);
  }
  if (!j.is_object())
    j = ojson::object();
  j["type"] = kind_name(FactorKind::Alpha); // 构建器只造 alpha
  j["name_cn"] = std::string(name_cn);
  bool expr_changed = true; // 按规范串比 (文件原串可能是非规范写法)
  if (const auto old_expr = j.find("expr"); old_expr != j.end() && old_expr->is_string()) {
    factor::expr::Expr old_e;
    std::string old_err;
    if (factor::expr::parse(old_expr->get<std::string>(), feats.lookup(), feats.seg, old_e, old_err))
      expr_changed = old_e.canon != e.canon;
  }
  j["expr"] = e.canon;
  if (note.empty())
    j.erase("note");
  else
    j["note"] = std::string(note);
  if (expr_changed) {
    j.erase("weights"); // 旧权重是别的模板的实例
    erase_stats(j);
  }
  write_json(from, j);
  if (to != from)
    std::filesystem::rename(from, to);
  return new_file;
}

void DeleteFactorFile(const std::string &factor_dir, std::string_view file) {
  const std::filesystem::path path = std::filesystem::path(factor_dir) / file;
  assert(std::filesystem::exists(path));
  std::filesystem::remove(path);
}

void WriteFactorWeights(const std::string &factor_dir, std::string_view file, int hold, const std::vector<SearchWeight> &ws) {
  assert(!ws.empty());
  const std::filesystem::path path = std::filesystem::path(factor_dir) / file;
  ojson j = read_json(path);
  erase_stats(j);
  ojson arr = ojson::array();
  if (j.contains("weights")) {
    assert(j["weights"].is_array() && "搜索落盘: weights 扫描时已校验过");
    for (const ojson &o : j["weights"])
      if (!o.is_object() || !o.contains("hold") || !o["hold"].is_number_integer() || o["hold"].get<int>() != hold)
        arr.push_back(o);
  }
  for (const SearchWeight &w : ws) {
    assert(w.hold == hold && (w.sign == 1 || w.sign == -1));
    ojson o = ojson::object();
    o["hold"] = w.hold;
    o["sign"] = w.sign;
    o["expr"] = w.expr;
    arr.push_back(std::move(o));
  }
  j["weights"] = std::move(arr);
  write_json(path, j);
}

void WriteFactorStats(const std::filesystem::path &path, const std::vector<const FactorRow *> &rs) {
  assert(!rs.empty());
  const FactorRow &r0 = *rs.front();
  ojson j = read_json(path);
  if (const auto wit = j.find("weights"); wit == j.end() || !wit->is_array() || wit->empty()) {
    assert(rs.size() == 1 && r0.widx == 0 && r0.hold_src == 0 && r0.sign == 1 && "无 weights 的文件只可能有手填权重 (widx 0, 正向)");
    ojson w = ojson::object();
    w["hold"] = 0;
    w["expr"] = r0.expr;
    j["weights"] = ojson::array({w});
  }
  const size_t nw = j["weights"].size();
  const ojson run = scope_json(r0.scope, r0.frame);
  const auto old_run = j.find("run");
  const auto old_stats = j.find("stats");
  const bool keep = old_run != j.end() && *old_run == run && json_cols_are(j, "hold_cols", hold_cols()) && json_cols_are(j, "acc_cols", acc_cols()) &&
                    old_stats != j.end() && old_stats->is_array() && old_stats->size() == nw;
  ojson stats = keep ? *old_stats : ojson::array();
  while (stats.size() < nw)
    stats.push_back(nullptr);
  for (const FactorRow *r : rs) {
    const size_t w = static_cast<size_t>(r->widx);
    assert(r->widx >= 0 && !r->expr.empty() && r->has_stat && !r->stat_from_file && r->file == r0.file);
    assert(w < nw && j["weights"][w].is_object() && "权重项与扫描时不符");
    assert(scope_json(r->scope, r->frame) == run && "同文件一轮评估同一 run 口径");
    stats[w] = eval_json(*r);
  }
  j["run"] = run;
  j["hold_cols"] = cols_json(hold_cols());
  j["acc_cols"] = cols_json(acc_cols());
  j["stats"] = std::move(stats);
  write_json(path, j);
}

} // namespace GUI::Factors
