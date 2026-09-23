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
  if (!json_int(j, "days", t.scope.days) || !json_int(j, "T", t.scope.T) || !json_int(j, "A", t.scope.A))
    return;
  double v = 0;
  if (!json_num(j, "valid_pct", v))
    return;
  t.valid_pct = static_cast<float>(v);
  if (!json_num(j, "eval_ms", t.eval_ms))
    return;
  const auto ait = j.find("amts");
  if (ait == j.end() || !ait->is_array() || ait->empty() || ait->size() > static_cast<size_t>(kMaxAmt))
    return;
  t.n_amt = static_cast<int>(ait->size());
  t.n_hold = -1;
  for (int i = 0; i < t.n_amt; ++i) {
    const json &a = (*ait)[static_cast<size_t>(i)];
    if (!a.is_object() || !json_int(a, "amt", t.amt[i]))
      return;
    const auto hit = a.find("holds");
    if (hit == a.end() || !hit->is_array() || hit->empty() || hit->size() > static_cast<size_t>(factor::stat::kMaxHold))
      return;
    if (t.n_hold >= 0 && static_cast<size_t>(t.n_hold) != hit->size()) // 每档金额的持有期数须一致
      return;
    t.n_hold = static_cast<int>(hit->size());
    for (int k = 0; k < t.n_hold; ++k)
      if (!load_hold((*hit)[static_cast<size_t>(k)], t.hold[i][k]))
        return;
  }
  r.scope = t.scope;
  r.valid_pct = t.valid_pct;
  r.eval_ms = t.eval_ms;
  r.n_amt = t.n_amt, r.n_hold = t.n_hold;
  for (int i = 0; i < t.n_amt; ++i) {
    r.amt[i] = t.amt[i];
    for (int k = 0; k < t.n_hold; ++k)
      r.hold[i][k] = t.hold[i][k];
  }
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
  j["valid_pct"] = sig4(r.valid_pct);
  j["eval_ms"] = sig4(r.eval_ms);
  ojson amts = ojson::array();
  for (int i = 0; i < r.n_amt; ++i) {
    ojson a;
    a["amt"] = r.amt[i];
    ojson holds = ojson::array();
    for (int k = 0; k < r.n_hold; ++k)
      holds.push_back(hold_json(r.hold[i][k]));
    a["holds"] = holds;
    amts.push_back(a);
  }
  j["amts"] = amts;
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
  std::vector<LabelPlane> labels;           // [amt × hold] 展平: labels[ai * n_hold + hi]
  factor::stat::Holds hd;                   // hd.h[ai * n_hold + hi] = hold
  int n_amt = 0, n_hold = 0;
  size_t n() const { return static_cast<size_t>(T) * A; }
};

// 逐天并行读: 特征列 + 全部标签列 + _meta 一次 load_day_columns, 门控后散进平面
bool load_planes(const FeatureTable &ft, FeatureRead &reader, const std::vector<std::string> &dates, Loaded &L, std::atomic<bool> &cancel,
                 std::atomic<int> &done) {
  TraceN("FactorsLoad");
  const size_t A = static_cast<size_t>(L.A), n = L.n();
  const size_t VR = level_valid_rows(kLevel);
  assert(VR == static_cast<size_t>(factor::kSegLen));
  const size_t H = static_cast<size_t>(L.n_amt) * L.n_hold;
  const size_t nf = L.feat_codes.size();
  // 列表: [特征 nf][标签 long/short × (amt × hold)][_meta]
  std::vector<size_t> cols;
  std::vector<L2::ValidType> vts;
  for (const std::string &c : L.feat_codes) {
    const FeatCol *fc = ft.find(c);
    assert(fc && fc->allowed);
    cols.push_back(fc->col), vts.push_back(fc->vt);
  }
  for (int ai = 0; ai < L.n_amt; ++ai)
    for (const LabelCol &lc : ft.labels) {
      const uint32_t lcol = lc.long_col[static_cast<size_t>(ai)], scol = lc.short_col[static_cast<size_t>(ai)];
      cols.push_back(lcol), vts.push_back(ft.cols[lcol].vt);
      cols.push_back(scol), vts.push_back(ft.cols[scol].vt);
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

// GROUP 域算子节点的组 id 元若是特征叶: 数据须全为 [0, kMaxGroup) 的整数 (CS 算子内部 max_gid 断言炸不得由数据触发)
void check_group_leaf(const factor::Dag &d, int node, const Loaded &L, std::string &err) {
  const factor::DagNode &nd = d.nodes[static_cast<size_t>(node)];
  if (nd.op < 0 || factor::expr::kOps[nd.op].a != factor::A::GROUP)
    return;
  const factor::DagNode &g = d.nodes[static_cast<size_t>(nd.in[factor::expr::group_arg(factor::expr::kOps[nd.op])])];
  if (g.op >= 0)
    return;
  const factor::check::Plane &p = L.planes[static_cast<size_t>(g.feat)];
  for (size_t i = 0; i < p.v.size(); ++i) {
    if (!p.m[i])
      continue;
    const float v = p.v[i];
    if (!(factor::expr::is_int(v) && v >= 0.f && v < static_cast<float>(factor::kMaxGroup))) {
      char buf[64];
      std::snprintf(buf, sizeof(buf), "%g", static_cast<double>(v));
      err = "组 id 特征 " + L.feat_codes[static_cast<size_t>(g.feat)] + " 含非整数 / 越界值 " + buf + " (须为 0..1023 整数)";
      return;
    }
  }
}

// 评估结果 → 行 (scope / 二级汇总); rows_out = [amt × hold][T]
void fill_stat(FactorRow &r, const FactorsRequest &req, const FeatureTable &ft, const Loaded &L, const factor::stat::Row *rows_out,
               const char *backend) {
  r.scope.universe = req.universe;
  r.scope.start_date = req.start_date;
  r.scope.end_date = req.end_date;
  r.scope.days = L.days, r.scope.T = L.T, r.scope.A = L.A;
  r.scope.backend = backend;
  r.scope.time = now_string();
  r.n_amt = L.n_amt, r.n_hold = L.n_hold;
  for (int ai = 0; ai < L.n_amt; ++ai) {
    r.amt[ai] = ft.amts[static_cast<size_t>(ai)];
    for (int hi = 0; hi < L.n_hold; ++hi) {
      const size_t i = static_cast<size_t>(ai) * L.n_hold + hi;
      r.hold[ai][hi] = factor::stat::summarize(rows_out + i * L.T, L.T, L.hd.h[i]);
    }
  }
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

std::string AddFactorFile(const std::string &factor_dir, const FeatureTable &feats, std::string_view name, std::string_view expr_src,
                          std::string_view note, std::string &err) {
  assert(ValidFactorName(name));
  factor::expr::Expr e;
  if (!factor::expr::parse(expr_src, feats.lookup(), e, err))
    return "";
  const std::string file = std::string(name) + ".json";
  const std::filesystem::path path = std::filesystem::path(factor_dir) / file;
  if (std::filesystem::exists(path)) {
    err = "已存在 " + file;
    return "";
  }
  ojson j;
  j["expr"] = e.canon;
  if (!note.empty())
    j["note"] = std::string(note);
  j["params"] = params_json(e);
  write_json(path, j);
  return file;
}

std::string UpdateFactorFile(const std::string &factor_dir, const FeatureTable &feats, std::string_view file, std::string_view new_name,
                             std::string_view expr_src, std::string_view note, std::string &err) {
  assert(ValidFactorName(new_name));
  factor::expr::Expr e;
  if (!factor::expr::parse(expr_src, feats.lookup(), e, err))
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
  // 有效行 → Expr (scan 已校验过, 这里必成功) → 一张共享 DAG (F.roots[k] ↔ idx[k])
  std::vector<size_t> idx;
  std::vector<factor::expr::Expr> exprs;
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
  L.days = static_cast<int>(dates.size());
  L.A = static_cast<int>(req.scope.uni.size());
  L.T = L.days * factor::kSegLen;
  assert(L.A >= 2 && L.A <= factor::stat::kMaxA && "资产轴超 Stat 容量 (kMaxA)");
  assert(static_cast<long long>(L.T) * L.A < (1LL << 31));
  L.feat_codes = F.feats; // 共享 DAG 的输入平面下标 = feats 下标
  L.n_amt = static_cast<int>(feats_.amts.size());
  L.n_hold = static_cast<int>(feats_.labels.size());
  assert(L.n_amt >= 1 && L.n_amt <= kMaxAmt && "金额档数超 kMaxAmt");
  assert(L.n_amt * L.n_hold <= factor::stat::kMaxHold && "amt × hold 超 Stat 标签组容量 (kMaxHold)");
  L.hd.n = L.n_amt * L.n_hold;
  for (int ai = 0; ai < L.n_amt; ++ai)
    for (int hi = 0; hi < L.n_hold; ++hi)
      L.hd.h[ai * L.n_hold + hi] = feats_.labels[static_cast<size_t>(hi)].hold;
  factor::stat::assert_holds(L.hd);

  total_.store(L.days, std::memory_order_relaxed);
  done_.store(0, std::memory_order_relaxed);
  if (!load_planes(feats_, reader, dates, L, cancel_, done_)) {
    if (reader.stale())
      finish_all_pending("特征库判废 (字段表指纹不符), 需重算特征");
    return false;
  }

  // ---- 组 id 特征叶的数据检查 (每 GROUP 节点一次; 子树含坏节点的因子 → BROKEN, 跳过) ----
  std::vector<std::string> node_err(static_cast<size_t>(N));
  for (int i = 0; i < N; ++i)
    check_group_leaf(F, i, L, node_err[static_cast<size_t>(i)]);
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

  // 共享 DAG 顺序走节点: run_node(i) → 该算子节点 wall ms; 到根: stat_root(i, valid_pct) 填 srows → 发布该根的全部因子 (dup 共根)
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
          stat_root(i, valid_pct);
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
        fill_stat(r, req, feats_, L, srows.data(), backend);
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
          factor::cpu::run_node_par(nd, in, pool.slots[static_cast<size_t>(nd.slot)], L.T, L.A, threads, sc);
          return ms_since(t0);
        },
        [&](int i, float &valid_pct) {
          const factor::check::Plane *root = plane_of(i);
          valid_pct = valid_pct_of(root->m.data(), n);
          factor::cpu::stat::eval(root->v.data(), root->m.data(), L.T, L.A, L.hd, lab.data(), ws.data(), srows.data(), threads);
        });
  }

  assert(factor::gpu::available() && "GPU 后端不可用");
  factor::gpu::Session *sess = factor::gpu::session_open(n);
  std::vector<const factor::gpu::DevPlane *> dev(L.planes.size());
  for (size_t i = 0; i < L.planes.size(); ++i)
    dev[i] = factor::gpu::upload(sess, L.planes[i].v.data(), L.planes[i].m.data()); // 输入只上传一次
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
          factor::gpu::run_cs_dev(sess, o.name, in[0], in[1], in[2], out, L.T, L.A, nd.p, &ms);
        return ms;
      },
      [&](int i, float &valid_pct) {
        const factor::gpu::DevPlane *root = dplane_of(i);
        factor::gpu::download(sess, root, nullptr, mask.data()); // 只回掩码 (n 字节) 算 valid%
        valid_pct = valid_pct_of(mask.data(), n);
        factor::gpu::stat_eval(ss, root, srows.data());
      });
  pool.release();
  factor::gpu::stat_close(ss);
  factor::gpu::session_close(sess); // 上传的输入平面随会话释放
  return ok;
}

} // namespace GUI::Factors
