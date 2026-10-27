// FactorsService — 见头文件. 本 TU include 因子 CPU 后端头 (EvalCpu → TS/CS Cpu.hpp, Stat/Cpu.hpp), 依赖受控浮点:
// CMake 里已列入 PRECISE_MATH 源 (-fno-fast-math), 与 OperatorsService 同待遇.
#include "gui/task_factors/services/FactorsService.hpp"

#include "gui/task_factors/services/FactorsEngine.hpp" // eval_job / Probes / with_engine (四服务同一条管线)
#include "gui/task_factors/services/FactorsLoad.hpp"   // stat_scope / Rc
#include "gui/task_factors/services/StatJson.hpp"
#include "misc/profiler.hpp"
#include "shared/SharedData.hpp"

#include "nlohmann/json.hpp"
#include "utfcpp/utf8.hpp"

#include <algorithm>
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
#include <string_view>

namespace GUI::Factors {

bool FactorWorker::abort_out(Rc rc, FactorFlow &F) {
  assert(aborted(rc) && "只收中断结局");
  if (rc == Rc::Cancel)
    return false;
  F.c.reset(); // 库换了, 缓存的一切不可信
  std::lock_guard<std::mutex> lock(mutex);
  message = kStaleMsg;
  return false;
}

namespace {

using json = nlohmann::json;
using ojson = nlohmann::ordered_json;

// Run 的探针: 二级 / 成本 / 有效率 / 可秩率 / alpha 分布 / rIC 分布 (一个根一份, 同根的只评首个)
using RunProbes = Probes<HoldP, CostP, ValidP, RankP, AlphaP, IcPdfP>;

// 评估派生数据 (run + 表头 + stats): 只描述当前 weights, 权重整体换掉 / 清空时一起删
void erase_stats(ojson &j) {
  j.erase("run");
  j.erase("hold_cols");
  j.erase("acc_cols");
  j.erase("stats");
}

// 文件顶层 run (全部权重共用的口径) + 权重 stat → 行 (载入显示用); 任一处不合 / 口径与当前根不符 → 整块忽略
// (不算 BROKEN: stat 是本服务写的派生数据, 下次评估覆盖)
void load_stat(const json &run, const json &j, int seg, FactorRow &r) {
  if (!run.is_object() || !j.is_object())
    return;
  FactorRow t;
  const auto str = [](const json &o, const char *key, std::string &dst) {
    const auto it = o.find(key);
    if (it == o.end() || !it->is_string())
      return false;
    dst = it->get<std::string>();
    return true;
  };
  const auto ymd = [&](const json &o, const char *key, misc::Ymd &dst) {
    std::string s;
    if (!str(o, key, s))
      return false;
    dst = misc::parse_ymd(s);
    assert(dst && "因子文件 run 日期非 YYYY-MM-DD");
    return true;
  };
  if (!json_str_is(run, "frame", factor::stat::frame_name(r.frame)))
    return;
  if (!str(run, "pool", t.scope.pool) || !ymd(run, "start_date", t.scope.start_date) || !ymd(run, "end_date", t.scope.end_date) ||
      !str(run, "backend", t.scope.backend) || !str(j, "time", t.scope.time))
    return;
  if (!json_int(run, "days", t.scope.days) || !json_int(run, "T", t.scope.T) || !json_int(run, "A", t.scope.A))
    return;
  if (!json_num(run, "max_impact", t.scope.max_impact) || !json_num(run, "n_full", t.scope.n_full) || !(t.scope.n_full > 0.0))
    return;
  double v = 0;
  if (!json_num(j, "valid_pct", v))
    return;
  t.valid_pct = static_cast<float>(v);
  if (!json_num(j, "rank_pct", v))
    return;
  t.rank_pct = static_cast<float>(v);
  if (!json_num(j, "eval_ms", t.eval_ms))
    return;
  const auto hit = j.find("holds");
  if (hit == j.end() || !hit->is_array() || hit->empty() || hit->size() > static_cast<size_t>(factor::stat::kMaxHold))
    return;
  t.n_hold = static_cast<int>(hit->size());
  for (int k = 0; k < t.n_hold; ++k)
    if (!load_hold((*hit)[static_cast<size_t>(k)], seg, t.hold[k]))
      return;
  const auto tit = j.find("turn"); // [n_hold] 只量 CostAcc
  if (tit == j.end() || !tit->is_array() || tit->size() != static_cast<size_t>(t.n_hold))
    return;
  for (int k = 0; k < t.n_hold; ++k)
    if (!load_cost_acc((*tit)[static_cast<size_t>(k)], t.turn[k]))
      return;
  const auto cit = j.find("costs"); // [amt] 每金额段 [n_hold] CostAcc (cost_json 的逆)
  if (cit == j.end() || !cit->is_array())
    return;
  t.costs.resize(cit->size());
  for (size_t c = 0; c < cit->size(); ++c)
    if (!load_cost((*cit)[c], t.n_hold, t.costs[c]))
      return;
  if (const auto pit = j.find("ic_pdf"); pit != j.end()) { // 小图可选 (老文件没有 → 空)
    if (!pit->is_array() || pit->size() != static_cast<size_t>(t.n_hold))
      return;
    for (int k = 0; k < t.n_hold; ++k)
      if (!load_mini_pdf((*pit)[static_cast<size_t>(k)], t.ic_pdf[k]))
        return;
  }
  if (const auto pit = j.find("alpha_pdf"); pit != j.end() && !load_mini_pdf(*pit, t.alpha_pdf))
    return;
  r.scope = t.scope;
  r.valid_pct = t.valid_pct;
  r.rank_pct = t.rank_pct;
  r.eval_ms = t.eval_ms;
  r.n_hold = t.n_hold;
  for (int k = 0; k < t.n_hold; ++k) {
    r.hold[k] = t.hold[k];
    r.turn[k] = t.turn[k];
    r.ic_pdf[k] = t.ic_pdf[k];
  }
  r.alpha_pdf = t.alpha_pdf;
  r.costs = std::move(t.costs);
  r.has_stat = true;
  r.stat_from_file = true;
}

// 顶层 run: 一轮评估内全部权重相同的口径 (time 每权重各自回写时取, 不进 run)
ojson run_json(const FactorRow &r) {
  ojson j;
  j["pool"] = r.scope.pool;
  j["start_date"] = misc::dash(r.scope.start_date);
  j["end_date"] = misc::dash(r.scope.end_date);
  j["days"] = r.scope.days;
  j["T"] = r.scope.T;
  j["A"] = r.scope.A;
  j["backend"] = r.scope.backend;
  j["frame"] = factor::stat::frame_name(r.frame);
  j["max_impact"] = r.scope.max_impact;
  j["n_full"] = sig4(r.scope.n_full);
  return j;
}

ojson stat_json(const FactorRow &r) {
  ojson j;
  j["time"] = r.scope.time;
  j["valid_pct"] = sig4(r.valid_pct);
  j["rank_pct"] = sig4(r.rank_pct);
  j["eval_ms"] = sig4(r.eval_ms);
  ojson holds = ojson::array();
  for (int k = 0; k < r.n_hold; ++k)
    holds.push_back(hold_json(r.hold[k]));
  j["holds"] = holds;
  ojson turn = ojson::array();
  for (int k = 0; k < r.n_hold; ++k)
    turn.push_back(cost_acc_json(r.turn[k]));
  j["turn"] = turn;
  ojson costs = ojson::array();
  for (const CostTier &c : r.costs)
    costs.push_back(cost_json(c, r.n_hold));
  j["costs"] = costs;
  ojson ic = ojson::array();
  for (int k = 0; k < r.n_hold; ++k)
    ic.push_back(mini_pdf_json(r.ic_pdf[k]));
  j["ic_pdf"] = ic;
  j["alpha_pdf"] = mini_pdf_json(r.alpha_pdf);
  return j;
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

// 评估后回写: 只动 stats[widx] + 顶层 run / 表头, 其他键 (expr / note / 人加的任何东西) 原样保留.
// stats 与 weights 同长同序 (没评的位置 null); run / 表头与已有的不同 (换了区间 / 池 / 后端…) → 别的权重的 stat 不再
// 对应 run, 一并清成 null (下次 Run 重算). 手填权重 (文件无 weights, hold_src = 0) 首次回写时补出 weights = [{hold 0, expr}]
void write_back(const std::filesystem::path &path, const FactorRow &r) {
  assert(r.widx >= 0 && !r.expr.empty());
  std::ifstream f(path);
  assert(f.is_open());
  ojson j = ojson::parse(f, nullptr, /*allow_exceptions=*/false);
  f.close();
  assert(j.is_object() && "回写的文件扫描时已解析过, 不该变");
  if (const auto wit = j.find("weights"); wit == j.end() || !wit->is_array() || wit->empty()) {
    assert(r.widx == 0 && r.hold_src == 0 && r.sign == 1 && "无 weights 的文件只可能有手填权重 (widx 0, 正向)");
    ojson w = ojson::object();
    w["hold"] = 0;
    w["expr"] = r.expr;
    j["weights"] = ojson::array({w});
  }
  const size_t nw = j["weights"].size();
  assert(static_cast<size_t>(r.widx) < nw && j["weights"][static_cast<size_t>(r.widx)].is_object() && "权重项与扫描时不符");
  const ojson run = run_json(r);
  const auto old_run = j.find("run");
  const auto old_stats = j.find("stats");
  const bool keep = old_run != j.end() && *old_run == run && json_cols_are(j, "hold_cols", hold_cols()) && json_cols_are(j, "acc_cols", acc_cols()) &&
                    old_stats != j.end() && old_stats->is_array() && old_stats->size() == nw;
  ojson stats = keep ? *old_stats : ojson::array();
  while (stats.size() < nw)
    stats.push_back(nullptr);
  stats[static_cast<size_t>(r.widx)] = stat_json(r);
  j["run"] = run;
  j["hold_cols"] = cols_json(hold_cols());
  j["acc_cols"] = cols_json(acc_cols());
  j["stats"] = std::move(stats);
  write_json(path, j);
}

// 根评估结果 → 行 (EvalStat 整块 + 小图)
void fill_stat(FactorRow &r, const EvalStat &st, const RunProbes &p) {
  static_cast<EvalStat &>(r) = st;
  for (int hi = 0; hi < st.n_hold; ++hi)
    r.ic_pdf[hi] = mini_pdf_of(p.at<IcPdfP>().pdf[hi]);
  r.alpha_pdf = mini_pdf_of(p.at<AlphaP>().d.pdf);
  r.has_stat = true;
  r.stat_from_file = false;
}

} // namespace

bool MakeFactorsRequest(const SharedData &data, std::shared_ptr<const FeatureTable> ft, const std::string &pool, bool evaluate, bool gpu,
                        FactorsRequest &req) {
  req = FactorsRequest{};
  req.spec.evaluate = evaluate;
  return MakeFactorScope(data, std::move(ft), pool, gpu, req.scope) || !evaluate;
}

// ============================================================================
// AddFactorFile
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

std::string AddFactorFile(const std::string &factor_dir, const FeatureTable &feats, std::string_view name, std::string_view name_cn,
                          std::string_view expr_src, std::string_view note, std::string &err) {
  assert(ValidFactorName(name) && ValidFactorNameCn(name_cn));
  factor::expr::Expr e;
  factor::stat::Frame fr;
  if (!factor::expr::parse(expr_src, feats.lookup(), feats.seg, e, err) || !feats.check_grid(e, err) ||
      !factor::expr::root_frame(e, feats.seg, fr, err))
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
  factor::stat::Frame fr;
  if (!factor::expr::parse(expr_src, feats.lookup(), feats.seg, e, err) || !feats.check_grid(e, err) ||
      !factor::expr::root_frame(e, feats.seg, fr, err))
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

factor::search::Score RowScore(const FactorRow &r, int hold) {
  const factor::stat::HoldStat *h = r.has_stat ? r.find_hold(hold) : nullptr;
  if (!h)
    return {};
  return factor::search::score_of(*h);
}

void WriteFactorWeights(const std::string &factor_dir, std::string_view file, int hold, const std::vector<SearchWeight> &ws) {
  assert(!ws.empty());
  const std::filesystem::path path = std::filesystem::path(factor_dir) / file;
  std::ifstream f(path);
  assert(f.is_open() && "搜索落盘: 因子文件不存在");
  ojson j = ojson::parse(f, nullptr, /*allow_exceptions=*/false);
  f.close();
  assert(j.is_object() && "搜索落盘: 因子文件扫描时已解析过, 不该变");
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

// ============================================================================
// Service
// ============================================================================

FactorsService::FactorsService() { Start(); }
FactorsService::~FactorsService() { Stop(); }

bool FactorsService::run(FactorsRequest &req) {
  std::vector<FactorRow> rs;
  scan(req, rs);
  int nb = 0;
  for (const FactorRow &r : rs)
    nb += !r.error.empty();
  {
    std::lock_guard<std::mutex> lock(mutex);
    rows = std::move(rs);
    current = req;
    message.clear();
  }
  broken_.store(nb, std::memory_order_relaxed);
  publish();

  if (!req.spec.evaluate)
    return true;
  if (!flow_)
    flow_ = std::make_unique<FactorFlow>();
  const bool ok = eval(req, *flow_);
  flow_->c.reset(); // 作用域块只活一轮 (Y 跨文件复用)
  return ok;
}

void ScanFactorFile(const std::filesystem::path &p, const FeatureTable &ft, std::vector<FactorRow> &out) {
  const factor::expr::FeatureLookup fl = ft.lookup();
  const auto emit = [&](FactorRow &r) {
    r.key = row_key(r.file, r.widx);
    out.push_back(r);
  };
  {
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
      if (stats)
        load_stat(*run, (*stats)[i], ft.seg, w);
      emit(w);
    }
  }
}

// 扫目录: 每个 *.json 的每个权重一行 (文件级问题一行 widx −1); 解析 / 校验失败 → error (文件不动)
void FactorsService::scan(const FactorsRequest &req, std::vector<FactorRow> &out) {
  TraceN("FactorsScan");
  out.clear();
  const FeatureTable &ft = *req.scope.ft;
  for (const std::filesystem::path &p : list_factor_files(req.scope.factor_dir))
    ScanFactorFile(p, ft, out);
  std::map<std::pair<int, std::string>, std::string> seen; // (hold, 具体规范串) → 首行 key
  for (FactorRow &w : out) {
    if (w.expr.empty())
      continue;
    if (const auto it = seen.find({w.hold_src, w.expr}); it != seen.end())
      w.dup_of = it->second;
    else
      seen.emplace(std::pair{w.hold_src, w.expr}, w.key);
  }
  if (!req.spec.evaluate) // 只扫描: BROKEN 行也算"处理完"
    for (FactorRow &r : out)
      r.status = RowStatus::Done;
}

// 装载 + 评估 (worker 线程; 返回 false = 取消 / 判废): 逐因子文件 eval_file
bool FactorsService::eval(const FactorsRequest &req, FactorFlow &F) {
  TraceN("FactorsEvaluate");
  // 有效权重行 → Expr (scan 已校验过, 这里必成功), 按文件分组 (scan 出的行同文件相邻)
  std::vector<std::vector<size_t>> idx;
  std::vector<std::vector<factor::expr::Expr>> exprs;
  std::vector<std::vector<factor::stat::Frame>> frames; // [k] 口径 (同根的因子规范串相同 → 口径相同)
  {
    std::lock_guard<std::mutex> lock(mutex);
    for (size_t i = 0; i < rows.size(); ++i) {
      if (!rows[i].error.empty() || rows[i].expr.empty() || (!req.spec.only_file.empty() && rows[i].file != req.spec.only_file)) {
        rows[i].status = RowStatus::Done; // BROKEN / 未搜索 / 不在 only_file 的行本轮不算
        continue;
      }
      if (idx.empty() || rows[idx.back().front()].file != rows[i].file)
        idx.emplace_back(), exprs.emplace_back(), frames.emplace_back();
      idx.back().push_back(i);
      exprs.back().push_back(req.scope.ft->reparse(rows[i].expr));
      frames.back().push_back(rows[i].frame);
    }
  }
  if (idx.empty()) {
    std::lock_guard<std::mutex> lock(mutex);
    for (FactorRow &r : rows)
      r.status = RowStatus::Done;
    message = "无有效因子";
    return true;
  }
  return with_engine(req.scope.gpu, F.ex, cancel_, 1, [&](auto &eng) {
    for (size_t f = 0; f < idx.size(); ++f) {
      const Rc rc = eval_file(req, F, eng, idx[f], exprs[f], frames[f]);
      if (rc == Rc::Ok)
        continue;
      if (rc == Rc::Stale) {
        std::lock_guard<std::mutex> lock(mutex);
        for (FactorRow &r : rows)
          r.status = RowStatus::Done;
      }
      return abort_out(rc, F);
    }
    return true;
  });
}

template <class E>
Rc FactorsService::eval_file(const FactorsRequest &req, FactorFlow &F, E &eng, const std::vector<size_t> &idx,
                             const std::vector<factor::expr::Expr> &exprs, const std::vector<factor::stat::Frame> &frames) {
  TraceN("FactorsEvaluateFile");
  // 该文件的权重 → 一张共享森林 (roots[k] ↔ idx[k]); 同根的只评首个 (探针 ps[k0])
  std::vector<const factor::expr::Expr *> eptr;
  for (const factor::expr::Expr &e : exprs)
    eptr.push_back(&e);
  // Y = 全部持有期 + 全部金额段, 同作用域跨文件复用; X = 本文件的叶 (retain)
  const YSpec ys = y_full(*req.scope.ft, req.scope.max_impact);
  std::vector<RunProbes> ps(idx.size());
  JobOut o;
  const auto mark = [&](size_t k, const std::string &why) { // 行 BROKEN (调用方持锁)
    rows[idx[k]].error = why;
    rows[idx[k]].status = RowStatus::Done;
    broken_.fetch_add(1, std::memory_order_relaxed);
  };
  const std::filesystem::path dir(req.scope.factor_dir);
  const Rc rc = eval_job(
      eng, F, req.scope, eptr, frames.data(), ys, /*retain=*/true, "Run", [&](int k) -> RunProbes & { return ps[static_cast<size_t>(k)]; },
      cancel_, *this, o,
      [&](const std::vector<std::string> &why) { // 值域已查: 坏根 BROKEN, 好根 Running
        std::lock_guard<std::mutex> lock(mutex);
        for (size_t k = 0; k < idx.size(); ++k)
          if (!why[k].empty())
            mark(k, why[k]);
          else
            rows[idx[k]].status = RowStatus::Running;
      },
      [&](int k0) { // 发布该根的全部权重 (dup 共根: 同一子树, eval_ms 同)
        const RunProbes &p = ps[static_cast<size_t>(k0)];
        EvalStat st;
        fill_eval(p, stat_scope(req.scope, o.es, block_of(F, o, 0).y, E::kName), o.eval_ms(k0), st);
        for (size_t k = 0; k < idx.size(); ++k) {
          if (o.d.first_root(static_cast<int>(k)) != k0)
            continue;
          FactorRow r;
          {
            std::lock_guard<std::mutex> lock(mutex);
            r = rows[idx[k]];
          }
          fill_stat(r, st, p);
          r.status = RowStatus::Done;
          {
            std::lock_guard<std::mutex> lock(mutex);
            rows[idx[k]] = r;
          }
          write_back(dir / r.file, r);
          publish();
        }
      });
  if (rc == Rc::Empty) { // 评估期 / A 轴不成 → 该文件各行 BROKEN
    std::lock_guard<std::mutex> lock(mutex);
    for (size_t k = 0; k < idx.size(); ++k)
      mark(k, o.why);
  }
  publish();
  return rc == Rc::Empty || rc == Rc::Broken ? Rc::Ok : rc;
}

} // namespace GUI::Factors
