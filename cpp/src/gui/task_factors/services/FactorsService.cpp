// FactorsService — 见头文件. 本 TU include 因子 CPU 后端头 (EvalCpu → TS/CS Cpu.hpp, Stat/Cpu.hpp), 依赖受控浮点:
// CMake 里已列入 PRECISE_MATH 源 (-fno-fast-math), 与 OperatorsService 同待遇.
#include "gui/task_factors/services/FactorsService.hpp"

#include "factor/EvalCpu.hpp"
#include "factor/EvalGpu.hpp"
#include "factor/GpuRun.hpp"
#include "factor/Stat/Cpu.hpp"
#include "features/Backend/FeatureRead.hpp"
#include "features/MetaFlag.hpp" // fmeta::valid
#include "features/TimeIndex.hpp"
#include "gui/task_factors/ui/StatJson.hpp"
#include "misc/profiler.hpp"
#include "shared/SharedData.hpp"

#include "nlohmann/json.hpp"

#include <algorithm>
#include <bit>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <set>
#include <thread>

// 因子契约的段 = 一个交易日的 L1 行数 (含 09:15 起的集合竞价 15 分钟); 特征库 L1 有效行数与之对账
static_assert(factor::kSegLen == static_cast<int>(TRADE_MINUTES_PER_DAY), "factor::kSegLen 必须等于 L1 每日分钟数");

namespace GUI::Factors {

namespace {

using json = nlohmann::json;
using ojson = nlohmann::ordered_json;
using Clock = std::chrono::steady_clock;
double ms_since(Clock::time_point t0) { return std::chrono::duration<double, std::milli>(Clock::now() - t0).count(); }

constexpr size_t kLevel = analysis::kLevel; // 因子只在 L1 算
// CPU 因子并行的内存预算 (每线程: 槽池 n_slots × T·A × 5B + Stat 暂存 T·A × 2B): 线程数 = min(核数, 预算 / 每线程)
constexpr size_t kCpuThreadBudget = size_t(6) << 30;

size_t hw_threads() { return std::max<size_t>(1, std::thread::hardware_concurrency()); }

// 一波线程抢任务 (同 Correlation.cpp 的 parallel_for; fn(i, tid))
void parallel_for(size_t n_tasks, size_t n_threads, const std::atomic<bool> &cancel, const std::function<void(size_t, size_t)> &fn) {
  if (n_tasks == 0)
    return;
  const size_t n = std::min(n_threads, n_tasks);
  std::atomic<size_t> next{0};
  std::vector<std::thread> threads;
  threads.reserve(n);
  for (size_t t = 0; t < n; ++t)
    threads.emplace_back([&, t] {
      for (;;) {
        const size_t i = next.fetch_add(1, std::memory_order_relaxed);
        if (i >= n_tasks || cancel.load(std::memory_order_relaxed))
          return;
        fn(i, t);
      }
    });
  for (auto &th : threads)
    th.join();
}

uint32_t fnv1a(std::string_view s) {
  uint32_t h = 2166136261u;
  for (unsigned char c : s)
    h = (h ^ c) * 16777619u;
  return h;
}

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

// 文件 stat → 行 (载入显示用); 任一处不合 → 整块忽略 (不算 BROKEN: stat 是本服务写的派生数据, 下次评估覆盖)
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
  if (!str("universe", t.scope.universe) || !str("start_date", t.scope.start_date) || !str("end_date", t.scope.end_date) ||
      !str("backend", t.scope.backend) || !str("time", t.scope.time))
    return;
  if (!json_int(j, "days", t.scope.days) || !json_int(j, "T", t.scope.T) || !json_int(j, "A", t.scope.A) ||
      !json_int(j, "amt", t.scope.amt))
    return;
  double v = 0;
  if (!json_num(j, "valid_pct", v))
    return;
  t.valid_pct = static_cast<float>(v);
  if (!json_num(j, "eval_ms", t.eval_ms) || !json_num(j, "stat_ms", t.stat_ms))
    return;
  const auto hit = j.find("holds");
  if (hit == j.end() || !hit->is_array() || hit->empty() || hit->size() > static_cast<size_t>(factor::stat::kMaxHold))
    return;
  t.n_hold = static_cast<int>(hit->size());
  for (int i = 0; i < t.n_hold; ++i)
    if (!load_hold((*hit)[static_cast<size_t>(i)], t.hold[i]))
      return;
  r.scope = t.scope;
  r.valid_pct = t.valid_pct;
  r.eval_ms = t.eval_ms, r.stat_ms = t.stat_ms;
  r.n_hold = t.n_hold;
  for (int i = 0; i < t.n_hold; ++i)
    r.hold[i] = t.hold[i];
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
  j["amt"] = r.scope.amt;
  j["backend"] = r.scope.backend;
  j["time"] = r.scope.time;
  j["valid_pct"] = sig4(r.valid_pct);
  j["eval_ms"] = sig4(r.eval_ms);
  j["stat_ms"] = sig4(r.stat_ms);
  ojson holds = ojson::array();
  for (int i = 0; i < r.n_hold; ++i)
    holds.push_back(hold_json(r.hold[i]));
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

// ---- 装载好的一轮数据 (worker 栈上) ----
struct LabelPlane {
  std::vector<uint16_t> lv, sv; // 做多 / 做空净收益 fp16 位 (与落盘同格式, Stat 直接吃)
  std::vector<uint8_t> m;
};
struct Loaded {
  int T = 0, A = 0, days = 0;
  std::vector<std::string> feat_codes;      // 去重特征 (平面下标)
  std::vector<factor::check::Plane> planes; // [feat] 值 + 掩码
  std::vector<LabelPlane> labels;           // [hold]
  factor::stat::Holds hd;
  size_t n() const { return static_cast<size_t>(T) * A; }
};

// 逐天并行读: 特征列 + 标签列 + _meta 一次 load_day_columns, 门控后散进平面
bool load_planes(const FactorsRequest &req, const FeatureTable &ft, FeatureRead &reader, const std::vector<std::string> &dates,
                 Loaded &L, std::atomic<bool> &cancel, std::atomic<int> &done) {
  TraceN("FactorsLoad");
  const size_t A = static_cast<size_t>(L.A), n = L.n();
  const size_t VR = level_valid_rows(kLevel);
  assert(VR == static_cast<size_t>(factor::kSegLen));
  const size_t H = ft.labels.size();
  const size_t nf = L.feat_codes.size();
  // 列表: [特征 nf][标签 long/short × H][_meta]
  std::vector<size_t> cols;
  std::vector<L2::ValidType> vts;
  for (const std::string &c : L.feat_codes) {
    const FeatCol *fc = ft.find(c);
    assert(fc && fc->allowed);
    cols.push_back(fc->col), vts.push_back(fc->vt);
  }
  for (const LabelCol &lc : ft.labels) {
    cols.push_back(lc.long_col[static_cast<size_t>(req.amt_idx)]);
    cols.push_back(lc.short_col[static_cast<size_t>(req.amt_idx)]);
    vts.push_back(ft.cols[lc.long_col[static_cast<size_t>(req.amt_idx)]].vt);
    vts.push_back(ft.cols[lc.short_col[static_cast<size_t>(req.amt_idx)]].vt);
  }
  cols.push_back(ft.meta_col);
  const size_t meta_i = cols.size() - 1;

  L.planes.resize(nf);
  for (factor::check::Plane &p : L.planes)
    p.resize(n);
  L.labels.resize(H);
  for (LabelPlane &lb : L.labels)
    lb.lv.assign(n, 0), lb.sv.assign(n, 0), lb.m.assign(n, 0);

  const size_t n_threads = std::min(hw_threads(), dates.size());
  std::vector<FeatureRead::DayColumns> staging(n_threads);
  for (FeatureRead::DayColumns &dc : staging)
    dc.preallocate(A, kLevel, cols.size());
  const size_t nc = cols.size();

  parallel_for(dates.size(), n_threads, cancel, [&](size_t d, size_t tid) {
    FeatureRead::DayColumns &dc = staging[tid];
    reader.load_day_columns(dates[d], cols, dc);
    if (reader.stale())
      return;
    for (size_t t = 0; t < VR; ++t) {
      const size_t row = (d * VR + t) * A;
      const feature_storage_t *gate = dc.data.data() + (t * nc + meta_i) * A;
      for (size_t i = 0; i < nf; ++i) {
        const feature_storage_t *src = dc.data.data() + (t * nc + i) * A;
        float *v = L.planes[i].v.data() + row;
        uint8_t *m = L.planes[i].m.data() + row;
        for (size_t a = 0; a < A; ++a) {
          const float x = static_cast<float>(src[a]);
          const bool ok = fmeta::valid(static_cast<float>(gate[a]), vts[i]) && std::isfinite(x);
          v[a] = ok ? x : 0.f;
          m[a] = ok;
        }
      }
      for (size_t h = 0; h < H; ++h) {
        const size_t il = nf + 2 * h, is = il + 1;
        const feature_storage_t *sl = dc.data.data() + (t * nc + il) * A;
        const feature_storage_t *ss = dc.data.data() + (t * nc + is) * A;
        LabelPlane &lb = L.labels[h];
        for (size_t a = 0; a < A; ++a) {
          const float lv = static_cast<float>(sl[a]), sv = static_cast<float>(ss[a]);
          const bool ok = fmeta::valid(static_cast<float>(gate[a]), vts[il]) && fmeta::valid(static_cast<float>(gate[a]), vts[is]) &&
                          std::isfinite(lv) && std::isfinite(sv);
          lb.lv[row + a] = ok ? std::bit_cast<uint16_t>(sl[a]) : 0;
          lb.sv[row + a] = ok ? std::bit_cast<uint16_t>(ss[a]) : 0;
          lb.m[row + a] = ok;
        }
      }
    }
    done.fetch_add(1, std::memory_order_relaxed);
  });
  return !cancel.load(std::memory_order_relaxed) && !reader.stale();
}

// GROUP 域算子的组 id 元若是特征叶: 数据须全为 [0, kMaxGroup) 的整数 (CS 算子内部 max_gid 断言炸不得由数据触发)
bool check_group_leaves(const factor::Dag &d, const Loaded &L, std::string &err) {
  for (const factor::DagNode &nd : d.nodes) {
    if (nd.op < 0 || factor::expr::kOps[nd.op].a != factor::A::GROUP)
      continue;
    const factor::DagNode &g = d.nodes[static_cast<size_t>(nd.in[factor::expr::group_arg(factor::expr::kOps[nd.op])])];
    if (g.op >= 0)
      continue;
    const factor::check::Plane &p = L.planes[static_cast<size_t>(g.feat)];
    for (size_t i = 0; i < p.v.size(); ++i) {
      if (!p.m[i])
        continue;
      const float v = p.v[i];
      if (!(factor::expr::is_int(v) && v >= 0.f && v < static_cast<float>(factor::kMaxGroup))) {
        char buf[64];
        std::snprintf(buf, sizeof(buf), "%g", static_cast<double>(v));
        err = "组 id 特征 " + L.feat_codes[static_cast<size_t>(g.feat)] + " 含非整数 / 越界值 " + buf + " (须为 0..1023 整数)";
        return false;
      }
    }
  }
  return true;
}

// 评估结果 → 行 (scope / 二级汇总); rows_out = [H][T]
void fill_stat(FactorRow &r, const FactorsRequest &req, const Loaded &L, const factor::stat::Row *rows_out, const char *backend) {
  r.scope.universe = req.universe;
  r.scope.start_date = req.start_date;
  r.scope.end_date = req.end_date;
  r.scope.days = L.days, r.scope.T = L.T, r.scope.A = L.A;
  r.scope.backend = backend;
  r.scope.time = now_string();
  r.n_hold = L.hd.n;
  for (int i = 0; i < L.hd.n; ++i)
    r.hold[i] = factor::stat::summarize(rows_out + static_cast<size_t>(i) * L.T, L.T, L.hd.h[i]);
  r.has_stat = true;
  r.stat_from_file = false;
}

float valid_pct_of(const uint8_t *m, size_t n) {
  size_t c = 0;
  for (size_t i = 0; i < n; ++i)
    c += m[i];
  return n ? 100.f * static_cast<float>(c) / static_cast<float>(n) : 0.f;
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
  t.meta_col = static_cast<uint32_t>(meta.col_of(kLevel, "_meta"));
  struct Lab {
    bool is_long;
    int hold, amt;
    uint32_t col;
  };
  std::vector<Lab> labs;
  std::set<int> holds, amts;
  for (size_t i = 0; i < L1.size(); ++i) {
    const FeatureMetadata &f = L1[i];
    FeatCol c;
    c.code = f.code;
    c.col = static_cast<uint32_t>(i);
    c.vt = f.valid_type;
    c.allowed = f.data_type == FeatureDataType::TS || f.data_type == FeatureDataType::CS;
    t.cols.push_back(c);
    if (f.data_type == FeatureDataType::LB) {
      char side[8] = {};
      int h = 0, amt = 0;
      const int got = std::sscanf(f.code, "lb_%7[a-z]_%dm_%dw", side, &h, &amt);
      assert(got == 3 && (std::string_view(side) == "long" || std::string_view(side) == "short") && "标签列命名不合 lb_<side>_<h>m_<amt>w");
      labs.push_back({std::string_view(side) == "long", h, amt, static_cast<uint32_t>(i)});
      holds.insert(h), amts.insert(amt);
    }
  }
  assert(!labs.empty() && "字段表无标签列");
  t.amts.assign(amts.begin(), amts.end());
  for (int h : holds) {
    LabelCol lc;
    lc.hold = h;
    lc.long_col.assign(t.amts.size(), UINT32_MAX);
    lc.short_col.assign(t.amts.size(), UINT32_MAX);
    for (const Lab &l : labs) {
      if (l.hold != h)
        continue;
      const size_t ai = static_cast<size_t>(std::find(t.amts.begin(), t.amts.end(), l.amt) - t.amts.begin());
      (l.is_long ? lc.long_col : lc.short_col)[ai] = l.col;
    }
    for (size_t ai = 0; ai < t.amts.size(); ++ai)
      assert(lc.long_col[ai] != UINT32_MAX && lc.short_col[ai] != UINT32_MAX && "标签列不齐: 某 (hold, amt) 缺 long 或 short");
    t.labels.push_back(lc);
  }
  return t;
}

bool MakeFactorsRequest(const SharedData &data, bool evaluate, bool gpu, int amt_idx, FactorsRequest &req) {
  req = FactorsRequest{};
  req.factor_dir = data.config.factor_dir + "/" + data.config.universe;
  req.evaluate = evaluate;
  req.gpu = gpu;
  req.amt_idx = amt_idx;
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

std::string AddFactorFile(const std::string &factor_dir, const FeatureTable &feats, std::string_view expr_src, std::string &err) {
  factor::expr::Expr e;
  if (!factor::expr::parse(expr_src, feats.lookup(), e, err))
    return "";
  char name[32];
  std::snprintf(name, sizeof(name), "f_%08x.json", fnv1a(e.canon));
  const std::filesystem::path path = std::filesystem::path(factor_dir) / name;
  if (std::filesystem::exists(path)) {
    err = std::string("已存在 ") + name + " (同一规范串)";
    return "";
  }
  ojson j;
  j["expr"] = e.canon;
  j["params"] = params_json(e);
  write_json(path, j);
  return name;
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
    const auto eit = j.find("expr");
    if (eit == j.end() || !eit->is_string()) {
      r.error = "缺 expr 键或不是字串";
      out.push_back(r);
      continue;
    }
    r.expr_raw = eit->get<std::string>();
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
  // 有效行 → Expr (scan 已校验过, 这里必成功)
  std::vector<size_t> idx;
  std::vector<factor::expr::Expr> exprs;
  std::vector<factor::Dag> dags;
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
      dags.push_back(factor::build(exprs.back()));
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

  // ---- 装载 ----
  status_.store(FactorsStatus::Loading, std::memory_order_release);
  FeatureRead reader(req.scope.features_dir, req.scope.uni.size(), req.scope.uni.hash, &cancel_);
  const std::vector<std::string> dates = analysis::enumerate_dates(reader, req.scope.months).dates;
  if (dates.empty()) {
    finish_all_pending("特征库在该区间无数据 (先算特征)");
    return true;
  }
  Loaded L;
  L.days = static_cast<int>(dates.size());
  L.A = static_cast<int>(req.scope.uni.size());
  L.T = L.days * factor::kSegLen;
  assert(L.A >= 2 && L.A <= factor::stat::kMaxA && "资产轴超 Stat 容量 (kMaxA)");
  assert(static_cast<long long>(L.T) * L.A < (1LL << 31));
  {
    std::set<std::string> u;
    for (const factor::Dag &d : dags)
      for (const std::string &c : d.feats)
        if (u.insert(c).second)
          L.feat_codes.push_back(c);
  }
  std::map<std::string, int> feat_idx;
  for (size_t i = 0; i < L.feat_codes.size(); ++i)
    feat_idx.emplace(L.feat_codes[i], static_cast<int>(i));
  L.hd.n = static_cast<int>(feats_.labels.size());
  for (int i = 0; i < L.hd.n; ++i)
    L.hd.h[i] = feats_.labels[static_cast<size_t>(i)].hold;
  factor::stat::assert_holds(L.hd);

  total_.store(L.days, std::memory_order_relaxed);
  done_.store(0, std::memory_order_relaxed);
  if (!load_planes(req, feats_, reader, dates, L, cancel_, done_)) {
    if (reader.stale())
      finish_all_pending("特征库判废 (字段表指纹不符), 需重算特征");
    return false;
  }

  // ---- 组 id 特征叶的数据检查 (每因子一次; 不合 → BROKEN, 跳过) ----
  std::vector<uint8_t> runnable(idx.size(), 1);
  for (size_t k = 0; k < idx.size(); ++k) {
    std::string err;
    if (!check_group_leaves(dags[k], L, err)) {
      runnable[k] = 0;
      std::lock_guard<std::mutex> lock(mutex);
      rows[idx[k]].error = err;
      rows[idx[k]].status = RowStatus::Done;
      broken_.fetch_add(1, std::memory_order_relaxed);
    }
  }

  // ---- 评估 ----
  status_.store(FactorsStatus::Running, std::memory_order_release);
  total_.store(static_cast<int>(idx.size()), std::memory_order_relaxed);
  done_.store(0, std::memory_order_relaxed);
  epoch_.fetch_add(1, std::memory_order_relaxed);
  const size_t n = L.n(), H = static_cast<size_t>(L.hd.n);
  const std::filesystem::path dir(req.factor_dir);

  const auto publish = [&](size_t k, FactorRow &r) {
    r.status = RowStatus::Done;
    {
      std::lock_guard<std::mutex> lock(mutex);
      rows[idx[k]] = r;
    }
    write_back(dir / r.file, exprs[k], r);
    done_.fetch_add(1, std::memory_order_relaxed);
    epoch_.fetch_add(1, std::memory_order_relaxed);
  };
  const auto inputs_of = [&](const factor::Dag &d, auto &&plane_of) {
    std::vector<decltype(plane_of(0))> in;
    for (const std::string &c : d.feats)
      in.push_back(plane_of(feat_idx.at(c)));
    return in;
  };

  if (!req.gpu) {
    // 标签 rank 预处理 (全核, 一次)
    std::vector<std::vector<uint16_t>> ry(H, std::vector<uint16_t>(n));
    std::vector<factor::cpu::stat::Label> lab(H);
    for (size_t h = 0; h < H; ++h) {
      factor::cpu::stat::prep_label(L.labels[h].lv.data(), L.labels[h].m.data(), L.T, L.A, ry[h].data(), static_cast<int>(hw_threads()));
      lab[h] = {L.labels[h].lv.data(), L.labels[h].sv.data(), L.labels[h].m.data(), ry[h].data()};
    }
    // 因子间并行: 每线程独立槽池 + Stat 暂存; 线程数按内存预算封顶
    int max_slots = 1;
    for (const factor::Dag &d : dags)
      max_slots = std::max(max_slots, d.n_slots);
    const size_t per_thread = n * (static_cast<size_t>(max_slots) * 5 + 2) + H * static_cast<size_t>(L.T) * sizeof(factor::stat::Row);
    const size_t n_threads = std::clamp<size_t>(kCpuThreadBudget / std::max<size_t>(per_thread, 1), 1, std::min(hw_threads(), idx.size()));
    struct Scratch {
      factor::cpu::Pool pool;
      std::vector<uint16_t> ws;
      std::vector<factor::stat::Row> rows;
    };
    std::vector<Scratch> sc(n_threads);
    for (Scratch &s : sc)
      s.ws.resize(n), s.rows.resize(H * static_cast<size_t>(L.T));
    parallel_for(idx.size(), n_threads, cancel_, [&](size_t k, size_t tid) {
      if (!runnable[k])
        return;
      Scratch &s = sc[tid];
      FactorRow r;
      {
        std::lock_guard<std::mutex> lock(mutex);
        rows[idx[k]].status = RowStatus::Running;
        r = rows[idx[k]];
      }
      epoch_.fetch_add(1, std::memory_order_relaxed);
      const factor::Dag &d = dags[k];
      const auto in = inputs_of(d, [&](int i) -> const factor::check::Plane * { return &L.planes[static_cast<size_t>(i)]; });
      Clock::time_point t0 = Clock::now();
      const factor::check::Plane *root = factor::cpu::eval(d, in, s.pool, L.T, L.A);
      r.eval_ms = ms_since(t0);
      r.valid_pct = valid_pct_of(root->m.data(), n);
      t0 = Clock::now();
      factor::cpu::stat::eval(root->v.data(), root->m.data(), L.T, L.A, L.hd, lab.data(), s.ws.data(), s.rows.data(), 1);
      r.stat_ms = ms_since(t0);
      fill_stat(r, req, L, s.rows.data(), "cpu");
      r.scope.amt = feats_.amts[static_cast<size_t>(req.amt_idx)];
      publish(k, r);
    });
  } else {
    assert(factor::gpu::available() && "GPU 后端不可用");
    factor::gpu::Session *sess = factor::gpu::session_open(n);
    std::vector<const factor::gpu::DevPlane *> dev(L.planes.size());
    for (size_t i = 0; i < L.planes.size(); ++i)
      dev[i] = factor::gpu::upload(sess, L.planes[i].v.data(), L.planes[i].m.data());
    std::vector<factor::gpu::StatLabelHost> lab(H);
    for (size_t h = 0; h < H; ++h)
      lab[h] = {L.labels[h].lv.data(), L.labels[h].sv.data(), L.labels[h].m.data()};
    factor::gpu::StatSession *ss = factor::gpu::stat_open(L.T, L.A, L.hd, lab.data(), nullptr);
    factor::gpu::DevPool pool;
    std::vector<uint8_t> mask(n);
    std::vector<factor::stat::Row> srows(H * static_cast<size_t>(L.T));
    for (size_t k = 0; k < idx.size() && !cancel_.load(std::memory_order_relaxed); ++k) {
      if (!runnable[k])
        continue;
      FactorRow r;
      {
        std::lock_guard<std::mutex> lock(mutex);
        rows[idx[k]].status = RowStatus::Running;
        r = rows[idx[k]];
      }
      epoch_.fetch_add(1, std::memory_order_relaxed);
      const factor::Dag &d = dags[k];
      const auto in = inputs_of(d, [&](int i) -> const factor::gpu::DevPlane * { return dev[static_cast<size_t>(i)]; });
      double kms = 0.0;
      const factor::gpu::DevPlane *root = factor::gpu::eval(d, in, pool, sess, L.T, L.A, &kms);
      r.eval_ms = kms; // 纯 kernel 和 (与 Operators 页 GPU 列同口径)
      factor::gpu::download(sess, root, nullptr, mask.data());
      r.valid_pct = valid_pct_of(mask.data(), n);
      double sms = 0.0;
      factor::gpu::stat_eval(ss, root, srows.data(), &sms);
      r.stat_ms = sms;
      fill_stat(r, req, L, srows.data(), "gpu");
      r.scope.amt = feats_.amts[static_cast<size_t>(req.amt_idx)];
      publish(k, r);
    }
    pool.release();
    factor::gpu::stat_close(ss);
    factor::gpu::session_close(sess); // 上传的输入平面随会话释放
  }
  return !cancel_.load(std::memory_order_relaxed);
}

} // namespace GUI::Factors
