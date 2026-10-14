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

std::string now_string() {
  const std::time_t t = std::time(nullptr);
  char buf[32];
  std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M", std::localtime(&t));
  return buf;
}

namespace {

using json = nlohmann::json;
using ojson = nlohmann::ordered_json;
using Clock = std::chrono::steady_clock;
double ms_since(Clock::time_point t0) { return std::chrono::duration<double, std::milli>(Clock::now() - t0).count(); }

constexpr size_t kLevel = analysis::kLevel; // 因子只在 L1 算

// 旧格式残留键 (params / 顶层 stat): 任何回写都顺手删
void erase_legacy(ojson &j) {
  j.erase("params");
  j.erase("stat");
}

// 一个金额档的换手成本 ↔ JSON {amt, cover, acc: [n_hold 个 CostAcc]} (StatJson cost_acc_json / load_cost_acc)
ojson cost_json(const CostTier &c, int n_hold) {
  ojson o;
  o["amt"] = c.amt;
  o["cover"] = sig4(c.cover);
  ojson acc = ojson::array();
  for (int k = 0; k < n_hold; ++k)
    acc.push_back(cost_acc_json(c.acc[k]));
  o["acc"] = acc;
  return o;
}
bool load_cost(const json &o, int n_hold, CostTier &dst) {
  if (!o.is_object())
    return false;
  CostTier t;
  double cover = 0.0;
  if (!json_int(o, "amt", t.amt) || !json_num(o, "cover", cover))
    return false;
  t.cover = static_cast<float>(cover);
  const auto ait = o.find("acc");
  if (ait == o.end() || !ait->is_array() || ait->size() != static_cast<size_t>(n_hold))
    return false;
  for (int k = 0; k < n_hold; ++k)
    if (!load_cost_acc((*ait)[static_cast<size_t>(k)], t.acc[k]))
      return false;
  dst = t;
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
  if (!json_num(j, "rank_pct", v))
    return;
  t.rank_pct = static_cast<float>(v);
  if (!json_num(j, "eval_ms", t.eval_ms))
    return;
  if (!json_num(j, "max_impact", t.scope.max_impact) || !json_num(j, "n_full", t.scope.n_full) || !(t.scope.n_full > 0.0))
    return;
  const auto hit = j.find("holds"); // 旧格式 ("amts": [{amt, holds}]) 无此键 → 整块忽略, 下次 Run 覆盖
  if (hit == j.end() || !hit->is_array() || hit->empty() || hit->size() > static_cast<size_t>(factor::stat::kMaxHold))
    return;
  t.n_hold = static_cast<int>(hit->size());
  for (int k = 0; k < t.n_hold; ++k)
    if (!load_hold((*hit)[static_cast<size_t>(k)], t.hold[k]))
      return;
  const auto cit = j.find("costs"); // [amt] 每档 cover + [n_hold] CostAcc (cost_json 的逆)
  if (cit == j.end() || !cit->is_array())
    return;
  t.costs.resize(cit->size());
  for (size_t c = 0; c < cit->size(); ++c)
    if (!load_cost((*cit)[c], t.n_hold, t.costs[c]))
      return;
  r.scope = t.scope;
  r.valid_pct = t.valid_pct;
  r.rank_pct = t.rank_pct;
  r.eval_ms = t.eval_ms;
  r.n_hold = t.n_hold;
  for (int k = 0; k < t.n_hold; ++k)
    r.hold[k] = t.hold[k];
  r.costs = std::move(t.costs);
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
  j["rank_pct"] = sig4(r.rank_pct);
  j["eval_ms"] = sig4(r.eval_ms);
  j["max_impact"] = r.scope.max_impact;
  j["n_full"] = sig4(r.scope.n_full);
  ojson holds = ojson::array();
  for (int k = 0; k < r.n_hold; ++k)
    holds.push_back(hold_json(r.hold[k]));
  j["holds"] = holds;
  ojson costs = ojson::array();
  for (const CostTier &c : r.costs)
    costs.push_back(cost_json(c, r.n_hold));
  j["costs"] = costs;
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

// 评估后回写: 只动 weights[widx].stat, 其他键 (expr / note / 人加的任何东西) 原样保留.
// 手填权重 (文件无 weights, hold_src = 0) 首次回写时补出 weights = [{hold 0, expr}]
void write_back(const std::filesystem::path &path, const FactorRow &r) {
  assert(r.widx >= 0 && !r.expr.empty());
  std::ifstream f(path);
  assert(f.is_open());
  ojson j = ojson::parse(f, nullptr, /*allow_exceptions=*/false);
  f.close();
  assert(j.is_object() && "回写的文件扫描时已解析过, 不该变");
  erase_legacy(j);
  auto wit = j.find("weights");
  if (wit == j.end() || !wit->is_array() || wit->empty()) {
    assert(r.widx == 0 && r.hold_src == 0 && "无 weights 的文件只可能有手填权重 (widx 0)");
    ojson w = ojson::object();
    w["hold"] = 0;
    w["expr"] = r.expr;
    j["weights"] = ojson::array({w});
    wit = j.find("weights");
  }
  assert(static_cast<size_t>(r.widx) < wit->size());
  ojson &w = (*wit)[static_cast<size_t>(r.widx)];
  assert(w.is_object() && "权重项与扫描时不符");
  w["stat"] = stat_json(r);
  write_json(path, j);
}

// 评估结果 → 行 (scope / 二级); hold = [n_hold]
void fill_stat(FactorRow &r, const FactorsRequest &req, const Loaded &L, double n_full, const factor::stat::HoldStat *hold,
               const std::vector<CostTier> &costs, const char *backend) {
  r.scope.universe = req.universe;
  r.scope.start_date = req.start_date;
  r.scope.end_date = req.end_date;
  r.scope.days = L.days, r.scope.T = L.T, r.scope.A = L.A;
  r.scope.max_impact = req.max_impact;
  r.scope.n_full = n_full;
  r.scope.backend = backend;
  r.scope.time = now_string();
  r.n_hold = L.n_hold;
  for (int hi = 0; hi < L.n_hold; ++hi)
    r.hold[hi] = hold[hi];
  r.costs = costs;
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
    lc.col[factor::stat::kLong] = lc.col[factor::stat::kShort] = UINT32_MAX;
    for (const Lab &l : labs)
      if (l.hold == h)
        lc.col[l.is_long ? factor::stat::kLong : factor::stat::kShort] = l.col;
    assert(lc.col[factor::stat::kLong] != UINT32_MAX && lc.col[factor::stat::kShort] != UINT32_MAX && "标签列不齐: 某 hold 缺 long 或 short");
    t.labels.push_back(lc);
  }
  for (int a : amts) {
    CostCol cc;
    cc.amt = a;
    cc.col[factor::stat::kLong] = cc.col[factor::stat::kShort] = UINT32_MAX;
    for (const Cost &c : costs)
      if (c.amt == a)
        cc.col[c.is_buy ? factor::stat::kLong : factor::stat::kShort] = c.col;
    assert(cc.col[factor::stat::kLong] != UINT32_MAX && cc.col[factor::stat::kShort] != UINT32_MAX && "冲击成本列不齐: 某金额档缺 buy 或 sell");
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
  req.max_impact = data.config.max_impact;
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
  erase_legacy(j);
  if (expr_changed)
    j.erase("weights"); // 旧权重是别的模板的实例
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
  return factor::search::score_of(*h, factor::search::Objective{});
}

void WriteFactorWeights(const std::string &factor_dir, std::string_view file, const std::vector<SearchWeight> &ws) {
  assert(!ws.empty());
  const std::filesystem::path path = std::filesystem::path(factor_dir) / file;
  std::ifstream f(path);
  assert(f.is_open() && "搜索落盘: 因子文件不存在");
  ojson j = ojson::parse(f, nullptr, /*allow_exceptions=*/false);
  f.close();
  assert(j.is_object() && "搜索落盘: 因子文件扫描时已解析过, 不该变");
  erase_legacy(j);
  ojson arr = ojson::array();
  for (const SearchWeight &w : ws) {
    ojson o = ojson::object();
    o["hold"] = w.hold;
    o["expr"] = w.expr;
    arr.push_back(std::move(o));
  }
  j["weights"] = std::move(arr);
  write_json(path, j);
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
  factor::exec::ForkJoin ex(static_cast<int>(std::max(1u, std::thread::hardware_concurrency()))); // 常驻, 随 worker 活
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
    const bool ok = evaluate(req, ex);
    status_.store(ok ? FactorsStatus::Done : FactorsStatus::Cancelled, std::memory_order_release);
    epoch_.fetch_add(1, std::memory_order_relaxed);
  }
}

// 扫目录: 每个 *.json 的每个权重一行 (文件级问题一行 widx −1); 解析 / 校验失败 → error (文件不动)
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
  std::map<std::pair<int, std::string>, std::string> seen; // (hold, 具体规范串) → 首行 key
  const auto emit = [&](FactorRow &r) {
    r.key = row_key(r.file, r.widx);
    out.push_back(r);
  };
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
      emit(r);
      continue;
    }
    if (const auto it = j.find("note"); it != j.end() && it->is_string())
      r.note = it->get<std::string>();
    if (const auto it = j.find("name_cn"); it != j.end() && it->is_string())
      r.name_cn = it->get<std::string>();
    if (!ValidFactorNameCn(r.name_cn)) {
      r.error = "缺 name_cn 键或值不合 (纯汉字, 1..10 字)";
      emit(r);
      continue;
    }
    const auto eit = j.find("expr");
    if (eit == j.end() || !eit->is_string()) {
      r.error = "缺 expr 键或不是字串";
      emit(r);
      continue;
    }
    r.expr_raw = eit->get<std::string>();
    // type: 必填; beta 只留位
    if (json_str_is(j, "type", kind_name(FactorKind::Beta))) {
      r.kind = FactorKind::Beta;
      r.error = "beta 类因子未实现 (留位)";
      emit(r);
      continue;
    }
    if (!json_str_is(j, "type", kind_name(FactorKind::Alpha))) {
      r.error = "缺 type 键或值不合 (alpha | beta)";
      emit(r);
      continue;
    }
    factor::expr::Expr t; // 模板
    if (!factor::expr::parse(r.expr_raw, fl, t, r.error)) {
      emit(r);
      continue;
    }
    r.tmpl = t.canon; // 表里显示规范串, 便于看根
    if (!factor::expr::root_frame(t, r.frame, r.error)) {
      emit(r);
      continue;
    }
    // 权重: weights 数组每项一行; 无 weights 时, 模板具体 → 手填唯一权重, 否则 "未搜索" 一行
    struct W {
      int hold = 0;
      std::string expr, err;
      const json *stat = nullptr;
    };
    std::vector<W> ws;
    if (const auto wit = j.find("weights"); wit != j.end() && !(wit->is_array() && wit->empty())) {
      if (!wit->is_array()) {
        r.error = "weights 须是数组";
        emit(r);
        continue;
      }
      for (size_t i = 0; i < wit->size(); ++i) {
        const json &o = (*wit)[i];
        W w;
        if (!o.is_object() || !json_int(o, "hold", w.hold)) {
          w.err = "weights[" + std::to_string(i) + "] 须是对象且有整数 hold";
        } else if (const auto xit = o.find("expr"); xit == o.end() || !xit->is_string()) {
          w.err = "weights[" + std::to_string(i) + "] 缺 expr 键或不是字串";
        } else {
          w.expr = xit->get<std::string>();
          if (const auto sit = o.find("stat"); sit != o.end())
            w.stat = &*sit;
        }
        ws.push_back(std::move(w));
      }
    } else if (!t.is_template()) {
      ws.push_back(W{0, t.canon, {}, nullptr});
    }
    if (ws.empty()) { // 模板未搜索
      r.status = RowStatus::Done;
      emit(r);
      continue;
    }
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
      if (!factor::expr::parse(ws[i].expr, fl, e, w.error)) {
        w.error = "weights[" + std::to_string(i) + "]: " + w.error;
        emit(w);
        continue;
      }
      if (!factor::expr::instance_of(t, e)) {
        w.error = "weights[" + std::to_string(i) + "] 不是模板的实例 (结构 / 固定参数须同模板, 自由参数须落值)";
        emit(w);
        continue;
      }
      w.expr = e.canon;
      w.n_ops = e.n_ops;
      const factor::Dag d = factor::build(e);
      w.n_feats = static_cast<int>(d.feats.size());
      w.n_slots = d.n_slots;
      w.key = row_key(w.file, w.widx);
      if (const auto it = seen.find({w.hold_src, e.canon}); it != seen.end())
        w.dup_of = it->second;
      else
        seen.emplace(std::pair{w.hold_src, e.canon}, w.key);
      if (ws[i].stat)
        load_stat(*ws[i].stat, w);
      emit(w);
    }
  }
  if (!req.evaluate) // 只扫描: BROKEN 行也算"处理完"
    for (FactorRow &r : out)
      r.status = RowStatus::Done;
}

// 装载 + 评估 (worker 线程; 返回 false = 取消)
bool FactorsService::evaluate(const FactorsRequest &req, factor::exec::ForkJoin &ex) {
  TraceN("FactorsEvaluate");
  // 有效权重行 → Expr (scan 已校验过, 这里必成功) → 一张共享 DAG (F.roots[k] ↔ idx[k])
  std::vector<size_t> idx;
  std::vector<factor::expr::Expr> exprs;
  std::vector<factor::stat::Frame> frames; // [k] 口径 (同根的因子规范串相同 → 口径相同)
  {
    std::lock_guard<std::mutex> lock(mutex);
    const factor::expr::FeatureLookup fl = feats_.lookup();
    for (size_t i = 0; i < rows.size(); ++i) {
      if (!rows[i].error.empty() || rows[i].expr.empty()) {
        rows[i].status = RowStatus::Done; // BROKEN / 未搜索 行本轮不算
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
  L.max_impact = static_cast<float>(req.max_impact);

  total_.store(L.days, std::memory_order_relaxed);
  done_.store(0, std::memory_order_relaxed);
  if (!load_planes(feats_, reader, dates, L, ex, cancel_, done_, /*with_labels=*/true, /*with_costs=*/true)) {
    if (reader.stale())
      finish_all_pending("特征库判废 (子轴/字段表与当前不符), 需重算特征");
    return false;
  }
  const double n_full = n_full_of(L); // 满档 cap (池宽), 全部因子共用

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
  const size_t n = L.n();
  const std::filesystem::path dir(req.factor_dir);
  std::vector<double> node_ms(static_cast<size_t>(N), 0.0);
  factor::stat::HoldStat hold[factor::stat::kMaxHold];
  factor::stat::Accum acc[factor::stat::kMaxHold]; // 该根的一级累加 (后端出), 二级 finish_all → hold
  std::vector<uint8_t> lc(n);                      // 该根的腿码 (后端出), 评估与成本共用
  std::vector<CostTier> costs;                     // [amt] 该根的换手成本 (cost_tiers, 两后端同一份宿主代码, 腿码由后端给)

  // 共享 DAG 顺序走节点: run_node(i) → 该算子节点 wall ms; 到根: stat_root(i, frame, valid_pct, rank_pct) 填 hold[] + costs → 发布该根的全部因子 (dup 共根)
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
      float valid_pct = 0.f, rank_pct = 0.f;
      bool stat_done = false;
      for (size_t k = 0; k < idx.size(); ++k) {
        if (!runnable[k] || F.roots[k] != i)
          continue;
        if (!stat_done) {
          stat_root(i, frames[k], valid_pct, rank_pct);
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
        r.rank_pct = rank_pct;
        fill_stat(r, req, L, n_full, hold, costs, backend);
        r.status = RowStatus::Done;
        {
          std::lock_guard<std::mutex> lock(mutex);
          rows[idx[k]] = r;
        }
        write_back(dir / r.file, r);
        epoch_.fetch_add(1, std::memory_order_relaxed);
      }
    }
    return true;
  };

  if (!req.gpu) {
    // 标签 rank 预处理 (全核, 一次)
    factor::cpu::stat::Workspace w;
    w.ensure(L.A, L.hd.n, static_cast<int>(feats_.costs.size()), ex.threads());
    LabelsHost lab;
    prep_labels(ex, L, w, lab);
    std::vector<uint16_t> rx(n); // r16(x), 评估与腿码共用
    factor::cpu::Pool pool;
    pool.prepare(F.n_slots, n); // 内存 = 峰值活槽 × n × 5B, 一次分配
    std::vector<factor::cpu::ParScratch> sc(static_cast<size_t>(ex.threads()));
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
          factor::cpu::run_node(ex, nd, in, pool.slots[static_cast<size_t>(nd.slot)], L.T, L.A, sc, L.cs.m.data());
          return ms_since(t0);
        },
        [&](int i, factor::stat::Frame fr, float &valid_pct, float &rank_pct) {
          const factor::check::Plane *root = plane_of(i); // Stat 只看池内 (g 在算子内部)
          valid_pct = valid_pct_of(root->m.data(), L.cs.m.data(), n);
          factor::cpu::stat::run(ex, factor::stat::Plane{root->v.data(), root->m.data()}, L.cs.m.data(), fr, L.span(), lab.L, rx.data(), w, acc,
                                 factor::cpu::stat::DropRows{});
          rank_pct = rank_pct_of(ex, rx.data(), root->m.data(), L.cs.m.data(), L.T, L.A);
          factor::stat::finish_all(acc, L.hd, hold);
          factor::cpu::stat::leg_codes(ex, rx.data(), L.span(), lc.data());
          cost_tiers(ex, lc.data(), L, feats_, w, costs);
        });
  }

  assert(factor::gpu::available() && "GPU 后端不可用");
  factor::gpu::Session *sess = factor::gpu::session_open(n);
  std::vector<const factor::gpu::DevPlane *> dev(L.planes.size());
  for (size_t i = 0; i < L.planes.size(); ++i)
    dev[i] = factor::gpu::upload(sess, L.planes[i].v.data(), L.planes[i].m.data());     // 输入只上传一次
  const factor::gpu::DevPlane *cs_gate = factor::gpu::upload_mask(sess, L.cs.m.data()); // 截面池掩码平面 (随会话释放)
  factor::gpu::StatSession *ss = factor::gpu::stat_open(L.T, L.A, labels_raw(L), nullptr);
  factor::cpu::stat::Workspace w; // 只给 cost_tiers (宿主成本代码与 CPU 后端同一份)
  w.ensure(L.A, L.hd.n, static_cast<int>(feats_.costs.size()), ex.threads());
  factor::gpu::DevPool pool;
  pool.prepare(sess, F.n_slots); // 中间量常驻显存, 不回宿主
  std::vector<uint8_t> mask(n);
  std::vector<uint16_t> rx(n); // r16(x) 拷回 (rank%)
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
      [&](int i, factor::stat::Frame fr, float &valid_pct, float &rank_pct) {
        const factor::gpu::DevPlane *root = dplane_of(i);        // Stat 只看池内 (g 在算子内部)
        factor::gpu::download(sess, root, nullptr, mask.data()); // 只回掩码 (n 字节) 算 valid% / rank%
        valid_pct = valid_pct_of(mask.data(), L.cs.m.data(), n);
        factor::gpu::stat_eval(ss, root, cs_gate, fr, acc, nullptr, lc.data(), rx.data());
        rank_pct = rank_pct_of(ex, rx.data(), mask.data(), L.cs.m.data(), L.T, L.A);
        factor::stat::finish_all(acc, L.hd, hold);
        cost_tiers(ex, lc.data(), L, feats_, w, costs);
      });
  pool.release();
  factor::gpu::stat_close(ss);
  factor::gpu::session_close(sess); // 上传的输入平面随会话释放
  return ok;
}

} // namespace GUI::Factors
