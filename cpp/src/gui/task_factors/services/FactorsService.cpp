// FactorsService — 见头文件. 本 TU include 因子 CPU 后端头 (EvalCpu → TS/CS Cpu.hpp, Stat/Cpu.hpp), 依赖受控浮点:
// CMake 里已列入 PRECISE_MATH 源 (-fno-fast-math), 与 OperatorsService 同待遇.
#include "gui/task_factors/services/FactorsService.hpp"

#include "factor/EvalCpu.hpp"
#include "factor/EvalGpu.hpp"
#include "factor/GpuRun.hpp"
#include "factor/Stat/Cpu.hpp"
#include "features/Backend/FeatureRead.hpp"
#include "gui/task_factors/services/FactorsLoad.hpp" // Loaded / load_planes / check_leaf_domains (与 InspectService 共用)
#include "gui/task_factors/ui/StatJson.hpp"
#include "misc/profiler.hpp"
#include "shared/SharedData.hpp"

#include "nlohmann/json.hpp"
#include "utfcpp/utf8.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <map>
#include <set>
#include <thread>

namespace GUI::Factors {

namespace {

using json = nlohmann::json;
using ojson = nlohmann::ordered_json;
using Clock = std::chrono::steady_clock;
double ms_since(Clock::time_point t0) { return std::chrono::duration<double, std::milli>(Clock::now() - t0).count(); }

constexpr size_t kLevel = analysis::kLevel; // 因子只在 L1 算

std::string now_string() {
  const std::time_t t = std::time(nullptr);
  char buf[32];
  std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M", std::localtime(&t));
  return buf;
}

// 算子节点前序 → params 数组 (只落该算子声明的字段)
ojson params_json(const factor::expr::Expr &e) {
  ojson arr = ojson::array();
  for (const factor::expr::Node &n : e.nodes) {
    if (n.op < 0)
      continue;
    const factor::expr::OpInfo &o = factor::expr::kOps[n.op];
    ojson p = ojson::object();
    if (factor::expr::declares(o.params, "d"))
      p["d"] = n.p.d;
    if (factor::expr::declares(o.params, "k"))
      p["k"] = n.p.k;
    if (factor::expr::declares(o.params, "k2"))
      p["k2"] = n.p.k2;
    arr.push_back(p);
  }
  return arr;
}

// 文件 params → 补丁; 格式不合 → false + err
bool parse_patches(const json &j, std::vector<factor::expr::ParamPatch> &out, std::string &err) {
  if (!j.is_array()) {
    err = "params 须是数组 (每算子节点一项, 前序)";
    return false;
  }
  out.clear();
  for (size_t i = 0; i < j.size(); ++i) {
    const json &o = j[i];
    if (!o.is_object()) {
      err = "params[" + std::to_string(i) + "] 须是对象";
      return false;
    }
    factor::expr::ParamPatch p;
    for (auto it = o.begin(); it != o.end(); ++it) {
      if (!it.value().is_number()) {
        err = "params[" + std::to_string(i) + "]." + it.key() + " 须是数";
        return false;
      }
      const double v = it.value().get<double>();
      if (it.key() == "d") {
        if (!it.value().is_number_integer()) {
          err = "params[" + std::to_string(i) + "].d 须是整数";
          return false;
        }
        p.has_d = true, p.d = it.value().get<int>();
      } else if (it.key() == "k") {
        p.has_k = true, p.k = static_cast<float>(v);
      } else if (it.key() == "k2") {
        p.has_k2 = true, p.k2 = static_cast<float>(v);
      } else {
        err = "params[" + std::to_string(i) + "] 未知键 " + it.key() + " (只许 d / k / k2)";
        return false;
      }
    }
    out.push_back(p);
  }
  return true;
}

// 文件 stat → 行 (载入显示用); 任一处不合 / 口径与当前根不符 → 整块忽略 (不算 BROKEN: stat 是本服务写的派生数据, 下次评估覆盖)
void load_stat(const json &j, FactorRow &r) {
  if (!j.is_object())
    return;
  FactorRow t;
  const auto str = [&](const char *key, std::string &dst) {
    const auto it = j.find(key);
    if (it == j.end() || !it->is_string())
      return false;
    dst = it->get<std::string>();
    return true;
  };
  if (!json_str_is(j, "frame", factor::stat::frame_name(r.frame)))
    return;
  if (!str("universe", t.scope.universe) || !str("start_date", t.scope.start_date) || !str("end_date", t.scope.end_date) ||
      !str("backend", t.scope.backend) || !str("time", t.scope.time))
    return;
  if (!json_int(j, "days", t.scope.days) || !json_int(j, "T", t.scope.T) || !json_int(j, "A", t.scope.A))
    return;
  double v = 0;
  if (!json_num(j, "valid_pct", v))
    return;
  t.valid_pct = static_cast<float>(v);
  if (!json_num(j, "eval_ms", t.eval_ms))
    return;
  const auto hit = j.find("holds"); // 旧格式 ("amts": [{amt, holds}]) 无此键 → 整块忽略, 下次 Run 覆盖
  if (hit == j.end() || !hit->is_array() || hit->empty() || hit->size() > static_cast<size_t>(factor::stat::kMaxHold))
    return;
  t.n_hold = static_cast<int>(hit->size());
  for (int k = 0; k < t.n_hold; ++k)
    if (!load_hold((*hit)[static_cast<size_t>(k)], t.hold[k]))
      return;
  r.scope = t.scope;
  r.valid_pct = t.valid_pct;
  r.eval_ms = t.eval_ms;
  r.n_hold = t.n_hold;
  for (int k = 0; k < t.n_hold; ++k)
    r.hold[k] = t.hold[k];
  r.has_stat = true;
  r.stat_from_file = true;
}

ojson stat_json(const FactorRow &r) {
  ojson j;
  j["universe"] = r.scope.universe;
  j["start_date"] = r.scope.start_date;
  j["end_date"] = r.scope.end_date;
  j["days"] = r.scope.days;
  j["T"] = r.scope.T;
  j["A"] = r.scope.A;
  j["backend"] = r.scope.backend;
  j["time"] = r.scope.time;
  j["frame"] = factor::stat::frame_name(r.frame);
  j["valid_pct"] = sig4(r.valid_pct);
  j["eval_ms"] = sig4(r.eval_ms);
  ojson holds = ojson::array();
  for (int k = 0; k < r.n_hold; ++k)
    holds.push_back(hold_json(r.hold[k]));
  j["holds"] = holds;
  return j;
}

// tmp + rename 原子落盘 (同 operators.json / features.json)
void write_json(const std::filesystem::path &path, const ojson &j) {
  std::filesystem::create_directories(path.parent_path());
  const std::filesystem::path tmp = path.string() + ".tmp";
  {
    std::ofstream f(tmp);
    assert(f.is_open() && "因子文件: 临时文件打不开");
    f << j.dump(1) << "\n";
    assert(f.good() && "因子文件: 写入失败");
  }
  std::filesystem::rename(tmp, path);
}

// 评估后回写: 只动 params / stat 两键, 其他键 (expr / note / 人加的任何东西) 原样保留
void write_back(const std::filesystem::path &path, const factor::expr::Expr &e, const FactorRow &r) {
  std::ifstream f(path);
  assert(f.is_open());
  ojson j = ojson::parse(f, nullptr, /*allow_exceptions=*/false);
  f.close();
  assert(j.is_object() && "回写的文件扫描时已解析过, 不该变");
  j["params"] = params_json(e);
  j["stat"] = stat_json(r);
  write_json(path, j);
}

// 评估结果 → 行 (scope / 二级汇总); rows_out = [hold][T]
void fill_stat(FactorRow &r, const FactorsRequest &req, const Loaded &L, const factor::stat::Row *rows_out, const char *backend) {
  r.scope.universe = req.universe;
  r.scope.start_date = req.start_date;
  r.scope.end_date = req.end_date;
  r.scope.days = L.days, r.scope.T = L.T, r.scope.A = L.A;
  r.scope.backend = backend;
  r.scope.time = now_string();
  r.n_hold = L.n_hold;
  for (int hi = 0; hi < L.n_hold; ++hi)
    r.hold[hi] = factor::stat::summarize(rows_out + static_cast<size_t>(hi) * L.T, L.T, L.hd.h[hi]);
  r.has_stat = true;
  r.stat_from_file = false;
}

} // namespace

// ============================================================================
// FeatureTable
// ============================================================================

const FeatCol *FeatureTable::find(std::string_view code) const {
  for (const FeatCol &c : cols)
    if (c.code == code)
      return &c;
  return nullptr;
}

factor::expr::FeatureLookup FeatureTable::lookup() const {
  return [this](std::string_view code) {
    const FeatCol *c = find(code);
    if (!c)
      return factor::expr::FeatState::MISSING;
    return c->allowed ? factor::expr::FeatState::OK : factor::expr::FeatState::FORBIDDEN;
  };
}

FeatureTable BuildFeatureTable(const Feature::Metadata &meta) {
  FeatureTable t;
  const auto &L1 = meta.features[kLevel];
  t.ts_col = static_cast<uint32_t>(meta.col_of(kLevel, "ts_valid"));
  t.cs_col = static_cast<uint32_t>(meta.col_of(kLevel, "cs_valid"));
  struct Lab {
    bool is_long;
    int hold;
    uint32_t col;
  };
  struct Cost {
    bool is_buy;
    int amt;
    uint32_t col;
  };
  std::vector<Lab> labs;
  std::vector<Cost> costs;
  std::set<int> holds, amts;
  for (size_t i = 0; i < L1.size(); ++i) {
    const FeatureMetadata &f = L1[i];
    FeatCol c;
    c.code = f.code;
    c.col = static_cast<uint32_t>(i);
    c.vt = f.valid_type;
    c.allowed = f.data_type == FeatureDataType::TS || f.data_type == FeatureDataType::CS;
    t.cols.push_back(c);
    if (f.data_type != FeatureDataType::LB)
      continue;
    char side[8] = {}, name[16] = {};
    int amt = 0;
    if (std::sscanf(f.code, "lb_cost_%7[a-z]_%dw", side, &amt) == 2) {
      assert((std::string_view(side) == "buy" || std::string_view(side) == "sell") && amt > 0 && "冲击成本列命名不合 lb_cost_<buy|sell>_<amt>w");
      costs.push_back({std::string_view(side) == "buy", amt, static_cast<uint32_t>(i)});
      amts.insert(amt);
      continue;
    }
    const int got = std::sscanf(f.code, "lb_%7[a-z]_%15[a-z0-9]", side, name);
    assert(got == 2 && (std::string_view(side) == "long" || std::string_view(side) == "short") && "标签列命名不合 lb_<long|short>_<name>");
    const int h = factor::stat::hold_from_name(name); // <n>m / close / t<N> → 持有期键 (Stat/Contract.hpp 【持有期键】)
    labs.push_back({std::string_view(side) == "long", h, static_cast<uint32_t>(i)});
    holds.insert(h);
  }
  assert(!labs.empty() && "字段表无标签列");
  for (int h : holds) {
    LabelCol lc;
    lc.hold = h;
    lc.long_col = lc.short_col = UINT32_MAX;
    for (const Lab &l : labs)
      if (l.hold == h)
        (l.is_long ? lc.long_col : lc.short_col) = l.col;
    assert(lc.long_col != UINT32_MAX && lc.short_col != UINT32_MAX && "标签列不齐: 某 hold 缺 long 或 short");
    t.labels.push_back(lc);
  }
  for (int a : amts) {
    CostCol cc;
    cc.amt = a;
    cc.buy_col = cc.sell_col = UINT32_MAX;
    for (const Cost &c : costs)
      if (c.amt == a)
        (c.is_buy ? cc.buy_col : cc.sell_col) = c.col;
    assert(cc.buy_col != UINT32_MAX && cc.sell_col != UINT32_MAX && "冲击成本列不齐: 某金额档缺 buy 或 sell");
    t.costs.push_back(cc);
  }
  return t;
}

bool MakeFactorsRequest(const SharedData &data, bool evaluate, bool gpu, FactorsRequest &req) {
  req = FactorsRequest{};
  req.factor_dir = data.config.factor_dir + "/" + data.config.universe;
  req.evaluate = evaluate;
  req.gpu = gpu;
  if (!evaluate)
    return true;
  if (data.asset.items.empty())
    return false; // 资产轴未就绪 (数据库未扫), 读不了特征库
  req.scope = analysis::read_scope(data.config, data.asset.items.size());
  if (req.scope.months.empty())
    return false;
  req.universe = data.config.universe;
  req.start_date = data.config.start_date;
  req.end_date = data.config.end_date;
  return true;
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
  if (!factor::expr::parse(expr_src, feats.lookup(), e, err) || !factor::expr::root_frame(e, fr, err))
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
  j["params"] = params_json(e);
  write_json(path, j);
  return file;
}

std::string UpdateFactorFile(const std::string &factor_dir, const FeatureTable &feats, std::string_view file, std::string_view new_name,
                             std::string_view name_cn, std::string_view expr_src, std::string_view note, std::string &err) {
  assert(ValidFactorName(new_name) && ValidFactorNameCn(name_cn));
  factor::expr::Expr e;
  factor::stat::Frame fr;
  if (!factor::expr::parse(expr_src, feats.lookup(), e, err) || !factor::expr::root_frame(e, fr, err))
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
    if (factor::expr::parse(old_expr->get<std::string>(), feats.lookup(), old_e, old_err))
      expr_changed = old_e.canon != e.canon;
  }
  j["expr"] = e.canon;
  if (note.empty())
    j.erase("note");
  else
    j["note"] = std::string(note);
  j["params"] = params_json(e);
  if (expr_changed)
    j.erase("stat"); // 旧 stat 是别的表达式算的
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

// ============================================================================
// Service
// ============================================================================

void FactorsService::Request(const FactorsRequest &req) {
  {
    std::lock_guard<std::mutex> lock(req_mutex_);
    pending_ = req;
    cancel_.store(true, std::memory_order_relaxed);
  }
  req_cv_.notify_all();
  if (!thread_.joinable()) {
    stop_.store(false, std::memory_order_relaxed);
    thread_ = std::thread(&FactorsService::worker_loop, this);
  }
}

void FactorsService::Stop() {
  if (!thread_.joinable())
    return;
  {
    std::lock_guard<std::mutex> lock(req_mutex_);
    stop_.store(true, std::memory_order_relaxed);
    cancel_.store(true, std::memory_order_relaxed);
  }
  req_cv_.notify_all();
  thread_.join();
}

void FactorsService::worker_loop() {
  TraceThread("FactorsWorker");
  while (true) {
    FactorsRequest req;
    {
      std::unique_lock<std::mutex> lock(req_mutex_);
      req_cv_.wait(lock, [&] { return stop_.load() || pending_.has_value(); });
      if (stop_.load())
        return;
      req = *pending_;
      pending_.reset();
      cancel_.store(false, std::memory_order_relaxed);
    }
    done_.store(0, std::memory_order_relaxed);
    total_.store(0, std::memory_order_relaxed);
    status_.store(FactorsStatus::Scanning, std::memory_order_release);
    epoch_.fetch_add(1, std::memory_order_relaxed);

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
    epoch_.fetch_add(1, std::memory_order_relaxed);

    if (!req.evaluate) {
      status_.store(FactorsStatus::Done, std::memory_order_release);
      continue;
    }
    const bool ok = evaluate(req);
    status_.store(ok ? FactorsStatus::Done : FactorsStatus::Cancelled, std::memory_order_release);
    epoch_.fetch_add(1, std::memory_order_relaxed);
  }
}

// 扫目录: 每个 *.json 一行; 解析 / 校验失败 → error (文件不动)
void FactorsService::scan(const FactorsRequest &req, std::vector<FactorRow> &out) {
  TraceN("FactorsScan");
  out.clear();
  std::vector<std::filesystem::path> files;
  if (std::filesystem::is_directory(req.factor_dir))
    for (const auto &ent : std::filesystem::directory_iterator(req.factor_dir))
      if (ent.is_regular_file() && ent.path().extension() == ".json")
        files.push_back(ent.path());
  std::sort(files.begin(), files.end());

  const factor::expr::FeatureLookup fl = feats_.lookup();
  std::map<std::string, std::string> seen; // canon → 首个文件
  for (const std::filesystem::path &p : files) {
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
      out.push_back(r);
      continue;
    }
    if (const auto it = j.find("note"); it != j.end() && it->is_string())
      r.note = it->get<std::string>();
    if (const auto it = j.find("name_cn"); it != j.end() && it->is_string())
      r.name_cn = it->get<std::string>();
    if (!ValidFactorNameCn(r.name_cn)) {
      r.error = "缺 name_cn 键或值不合 (纯汉字, 1..10 字)";
      out.push_back(r);
      continue;
    }
    const auto eit = j.find("expr");
    if (eit == j.end() || !eit->is_string()) {
      r.error = "缺 expr 键或不是字串";
      out.push_back(r);
      continue;
    }
    r.expr_raw = eit->get<std::string>();
    // type: 必填; beta 只留位
    if (json_str_is(j, "type", kind_name(FactorKind::Beta))) {
      r.kind = FactorKind::Beta;
      r.error = "beta 类因子未实现 (留位)";
      out.push_back(r);
      continue;
    }
    if (!json_str_is(j, "type", kind_name(FactorKind::Alpha))) {
      r.error = "缺 type 键或值不合 (alpha | beta)";
      out.push_back(r);
      continue;
    }
    factor::expr::Expr e;
    if (!factor::expr::parse(r.expr_raw, fl, e, r.error)) {
      out.push_back(r);
      continue;
    }
    if (const auto pit = j.find("params"); pit != j.end()) {
      std::vector<factor::expr::ParamPatch> patches;
      if (!parse_patches(*pit, patches, r.error) || !factor::expr::apply_params(e, patches, r.error)) {
        out.push_back(r);
        continue;
      }
    }
    if (!factor::expr::root_frame(e, r.frame, r.error)) {
      r.expr = e.canon; // 表里仍显示规范串, 便于看根
      out.push_back(r);
      continue;
    }
    r.expr = e.canon;
    r.n_ops = e.n_ops;
    const factor::Dag d = factor::build(e);
    r.n_feats = static_cast<int>(d.feats.size());
    r.n_slots = d.n_slots;
    if (const auto it = seen.find(e.canon); it != seen.end())
      r.dup_of = it->second;
    else
      seen.emplace(e.canon, r.file);
    if (const auto sit = j.find("stat"); sit != j.end())
      load_stat(*sit, r);
    out.push_back(r);
  }
  if (!req.evaluate) // 只扫描: BROKEN 行也算"处理完"
    for (FactorRow &r : out)
      r.status = RowStatus::Done;
}

// 装载 + 评估 (worker 线程; 返回 false = 取消)
bool FactorsService::evaluate(const FactorsRequest &req) {
  TraceN("FactorsEvaluate");
  // 有效行 → Expr (scan 已校验过, 这里必成功) → 一张共享 DAG (F.roots[k] ↔ idx[k])
  std::vector<size_t> idx;
  std::vector<factor::expr::Expr> exprs;
  std::vector<factor::stat::Frame> frames; // [k] 口径 (同根的因子规范串相同 → 口径相同)
  {
    std::lock_guard<std::mutex> lock(mutex);
    const factor::expr::FeatureLookup fl = feats_.lookup();
    for (size_t i = 0; i < rows.size(); ++i) {
      if (!rows[i].error.empty()) {
        rows[i].status = RowStatus::Done; // BROKEN 行本轮不算
        continue;
      }
      factor::expr::Expr e;
      std::string err;
      const bool ok = factor::expr::parse(rows[i].expr, fl, e, err);
      assert(ok && "scan 过的规范串必可再解析");
      (void)ok;
      idx.push_back(i);
      exprs.push_back(std::move(e));
      frames.push_back(rows[i].frame);
    }
  }
  const auto finish_all_pending = [&](const char *msg) {
    std::lock_guard<std::mutex> lock(mutex);
    for (FactorRow &r : rows)
      r.status = RowStatus::Done;
    message = msg;
  };
  if (idx.empty()) {
    finish_all_pending("无有效因子");
    return true;
  }
  std::vector<const factor::expr::Expr *> eptr;
  for (const factor::expr::Expr &e : exprs)
    eptr.push_back(&e);
  const factor::Dag F = factor::build_forest(eptr);
  const int N = static_cast<int>(F.nodes.size());
  // 每因子的子树节点集 (eval_ms 归因 / 组 id 检查 / 跳过没人要的节点)
  std::vector<std::vector<int>> sub(idx.size());
  for (size_t k = 0; k < idx.size(); ++k) {
    std::vector<uint8_t> seen(static_cast<size_t>(N), 0);
    std::vector<int> stack{F.roots[k]};
    while (!stack.empty()) {
      const int i = stack.back();
      stack.pop_back();
      if (seen[static_cast<size_t>(i)])
        continue;
      seen[static_cast<size_t>(i)] = 1;
      sub[k].push_back(i);
      const factor::DagNode &nd = F.nodes[static_cast<size_t>(i)];
      for (int a = 0; nd.op >= 0 && a < factor::expr::kOps[nd.op].arity; ++a)
        stack.push_back(nd.in[a]);
    }
  }

  // ---- 装载 ----
  status_.store(FactorsStatus::Loading, std::memory_order_release);
  FeatureRead reader(req.scope.features_dir, req.scope.uni.size(), req.scope.uni.hash, &cancel_);
  const std::vector<std::string> dates = analysis::enumerate_dates(reader, req.scope.months).dates;
  if (dates.empty()) {
    finish_all_pending("特征库在该区间无数据 (先算特征)");
    return true;
  }
  Loaded L;
  init_loaded(feats_, static_cast<int>(dates.size()), static_cast<int>(req.scope.uni.size()), L);
  L.feat_codes = F.feats; // 共享 DAG 的输入平面下标 = feats 下标

  total_.store(L.days, std::memory_order_relaxed);
  done_.store(0, std::memory_order_relaxed);
  if (!load_planes(feats_, reader, dates, L, cancel_, done_)) {
    if (reader.stale())
      finish_all_pending("特征库判废 (子轴/字段表与当前不符), 需重算特征");
    return false;
  }

  // ---- 特征叶元的值域数据检查 (每算子节点一次, 按 OpTable in 列; 子树含坏节点的因子 → BROKEN, 跳过) ----
  std::vector<std::string> node_err(static_cast<size_t>(N));
  for (int i = 0; i < N; ++i)
    check_leaf_domains(F, i, L, node_err[static_cast<size_t>(i)]);
  std::vector<uint8_t> runnable(idx.size(), 1), needed(static_cast<size_t>(N), 0);
  for (size_t k = 0; k < idx.size(); ++k) {
    for (int i : sub[k]) {
      if (node_err[static_cast<size_t>(i)].empty())
        continue;
      runnable[k] = 0;
      std::lock_guard<std::mutex> lock(mutex);
      rows[idx[k]].error = node_err[static_cast<size_t>(i)];
      rows[idx[k]].status = RowStatus::Done;
      broken_.fetch_add(1, std::memory_order_relaxed);
      break;
    }
    if (runnable[k])
      for (int i : sub[k])
        needed[static_cast<size_t>(i)] = 1;
  }

  // ---- 评估 ----
  status_.store(FactorsStatus::Running, std::memory_order_release);
  {
    int n_need = 0;
    for (int i = 0; i < N; ++i)
      n_need += needed[static_cast<size_t>(i)] && F.nodes[static_cast<size_t>(i)].op >= 0;
    total_.store(n_need, std::memory_order_relaxed); // Running 期进度 = 共享 DAG 的算子节点
    done_.store(0, std::memory_order_relaxed);
    std::lock_guard<std::mutex> lock(mutex);
    for (size_t k = 0; k < idx.size(); ++k)
      if (runnable[k])
        rows[idx[k]].status = RowStatus::Running;
  }
  epoch_.fetch_add(1, std::memory_order_relaxed);
  const size_t n = L.n(), H = static_cast<size_t>(L.hd.n);
  const int threads = static_cast<int>(hw_threads());
  const std::filesystem::path dir(req.factor_dir);
  std::vector<double> node_ms(static_cast<size_t>(N), 0.0);
  std::vector<factor::stat::Row> srows(H * static_cast<size_t>(L.T));

  // 共享 DAG 顺序走节点: run_node(i) → 该算子节点 wall ms; 到根: stat_root(i, frame, valid_pct) 填 srows → 发布该根的全部因子 (dup 共根)
  const auto walk = [&](const char *backend, auto &&run_node, auto &&stat_root) -> bool {
    for (int i = 0; i < N; ++i) {
      if (cancel_.load(std::memory_order_relaxed))
        return false;
      if (!needed[static_cast<size_t>(i)])
        continue;
      if (F.nodes[static_cast<size_t>(i)].op >= 0) {
        node_ms[static_cast<size_t>(i)] = run_node(i);
        done_.fetch_add(1, std::memory_order_relaxed);
        epoch_.fetch_add(1, std::memory_order_relaxed);
      }
      if (!F.is_root(i))
        continue;
      float valid_pct = 0.f;
      bool stat_done = false;
      for (size_t k = 0; k < idx.size(); ++k) {
        if (!runnable[k] || F.roots[k] != i)
          continue;
        if (!stat_done) {
          stat_root(i, frames[k], valid_pct);
          stat_done = true;
        }
        FactorRow r;
        {
          std::lock_guard<std::mutex> lock(mutex);
          r = rows[idx[k]];
        }
        r.eval_ms = 0.0;
        for (int j : sub[k])
          r.eval_ms += node_ms[static_cast<size_t>(j)];
        r.valid_pct = valid_pct;
        fill_stat(r, req, L, srows.data(), backend);
        r.status = RowStatus::Done;
        {
          std::lock_guard<std::mutex> lock(mutex);
          rows[idx[k]] = r;
        }
        write_back(dir / r.file, exprs[k], r);
        epoch_.fetch_add(1, std::memory_order_relaxed);
      }
    }
    return true;
  };

  if (!req.gpu) {
    // 标签 rank 预处理 (全核, 一次)
    std::vector<std::vector<uint16_t>> ry(H, std::vector<uint16_t>(n));
    std::vector<factor::cpu::stat::Label> lab(H);
    for (size_t h = 0; h < H; ++h) {
      factor::cpu::stat::prep_label(L.labels[h].lv.data(), L.labels[h].m.data(), L.T, L.A, ry[h].data(), threads);
      lab[h] = {L.labels[h].lv.data(), L.labels[h].sv.data(), L.labels[h].m.data(), ry[h].data()};
    }
    factor::cpu::Pool pool;
    pool.prepare(F.n_slots, n); // 内存 = 峰值活槽 × n × 5B, 一次分配
    std::vector<factor::cpu::ParScratch> sc(static_cast<size_t>(threads));
    std::vector<uint16_t> ws(n);
    const auto plane_of = [&](int i) -> const factor::check::Plane * {
      const factor::DagNode &nd = F.nodes[static_cast<size_t>(i)];
      return nd.op < 0 ? &L.planes[static_cast<size_t>(nd.feat)] : &pool.slots[static_cast<size_t>(nd.slot)];
    };
    return walk(
        "cpu",
        [&](int i) {
          const factor::DagNode &nd = F.nodes[static_cast<size_t>(i)];
          const factor::check::Plane *in[3] = {};
          for (int a = 0; a < factor::expr::kOps[nd.op].arity; ++a)
            in[a] = plane_of(nd.in[a]);
          const Clock::time_point t0 = Clock::now();
          factor::cpu::run_node_par(nd, in, pool.slots[static_cast<size_t>(nd.slot)], L.T, L.A, threads, sc, L.cs.m.data());
          return ms_since(t0);
        },
        [&](int i, factor::stat::Frame fr, float &valid_pct) {
          const factor::check::Plane *root = plane_of(i); // Stat 只看池内 (g 在算子内部)
          valid_pct = valid_pct_of(root->m.data(), L.cs.m.data(), n);
          factor::cpu::stat::eval(root->v.data(), root->m.data(), L.cs.m.data(), fr, L.T, L.A, L.hd, lab.data(), ws.data(), srows.data(),
                                  threads);
        });
  }

  assert(factor::gpu::available() && "GPU 后端不可用");
  factor::gpu::Session *sess = factor::gpu::session_open(n);
  std::vector<const factor::gpu::DevPlane *> dev(L.planes.size());
  for (size_t i = 0; i < L.planes.size(); ++i)
    dev[i] = factor::gpu::upload(sess, L.planes[i].v.data(), L.planes[i].m.data());     // 输入只上传一次
  const factor::gpu::DevPlane *cs_gate = factor::gpu::upload_mask(sess, L.cs.m.data()); // 截面池掩码平面 (随会话释放)
  std::vector<factor::gpu::StatLabelHost> lab(H);
  for (size_t h = 0; h < H; ++h)
    lab[h] = {L.labels[h].lv.data(), L.labels[h].sv.data(), L.labels[h].m.data()};
  factor::gpu::StatSession *ss = factor::gpu::stat_open(L.T, L.A, L.hd, lab.data(), nullptr);
  factor::gpu::DevPool pool;
  pool.prepare(sess, F.n_slots); // 中间量常驻显存, 不回宿主
  std::vector<uint8_t> mask(n);
  const auto dplane_of = [&](int i) -> const factor::gpu::DevPlane * {
    const factor::DagNode &nd = F.nodes[static_cast<size_t>(i)];
    return nd.op < 0 ? dev[static_cast<size_t>(nd.feat)] : pool.slots[static_cast<size_t>(nd.slot)];
  };
  const bool ok = walk(
      "gpu",
      [&](int i) {
        const factor::DagNode &nd = F.nodes[static_cast<size_t>(i)];
        const factor::expr::OpInfo &o = factor::expr::kOps[nd.op];
        const factor::gpu::DevPlane *in[3] = {};
        for (int a = 0; a < o.arity; ++a)
          in[a] = dplane_of(nd.in[a]);
        factor::gpu::DevPlane *out = pool.slots[static_cast<size_t>(nd.slot)];
        double ms = 0.0; // 纯 kernel (与 Operators 页 GPU 列同口径)
        if (o.a == factor::A::SELF)
          factor::gpu::run_ts_dev(sess, o.name, in[0], in[1], in[2], out, L.T, L.A, nd.p, &ms);
        else
          factor::gpu::run_cs_dev(sess, o.name, in[0], in[1], in[2], cs_gate, out, L.T, L.A, nd.p, &ms);
        return ms;
      },
      [&](int i, factor::stat::Frame fr, float &valid_pct) {
        const factor::gpu::DevPlane *root = dplane_of(i);        // Stat 只看池内 (g 在算子内部)
        factor::gpu::download(sess, root, nullptr, mask.data()); // 只回掩码 (n 字节) 算 valid%
        valid_pct = valid_pct_of(mask.data(), L.cs.m.data(), n);
        factor::gpu::stat_eval(ss, root, cs_gate, fr, srows.data());
      });
  pool.release();
  factor::gpu::stat_close(ss);
  factor::gpu::session_close(sess); // 上传的输入平面随会话释放
  return ok;
}

} // namespace GUI::Factors
