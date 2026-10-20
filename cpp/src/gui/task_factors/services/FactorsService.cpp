// FactorsService — 见头文件. 本 TU include 因子 CPU 后端头 (EvalCpu → TS/CS Cpu.hpp, Stat/Cpu.hpp), 依赖受控浮点:
// CMake 里已列入 PRECISE_MATH 源 (-fno-fast-math), 与 OperatorsService 同待遇.
#include "gui/task_factors/services/FactorsService.hpp"

#include "gui/task_factors/services/FactorsEngine.hpp" // Engine<Cpu | Gpu> / with_engine (与 Inspect / Search 共用)
#include "gui/task_factors/services/FactorsLoad.hpp"   // ScopeCache / ensure_y / ensure_x / check_leaf_domains
#include "gui/task_factors/ui/StatJson.hpp"
#include "misc/profiler.hpp"
#include "shared/SharedData.hpp"

#include "nlohmann/json.hpp"
#include "utfcpp/utf8.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdio>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <map>
#include <set>
#include <string_view>

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

// 评估派生数据 (run + 表头 + stats): 只描述当前 weights, 权重整体换掉 / 清空时一起删
void erase_stats(ojson &j) {
  j.erase("run");
  j.erase("hold_cols");
  j.erase("acc_cols");
  j.erase("stats");
}

// 一个金额段的换手成本 ↔ JSON {amt, cover, acc: [n_hold 个 CostAcc]} (StatJson cost_acc_json / load_cost_acc)
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

// 因子表迷你 PDF ↔ JSON {lo, hi, y: [kMiniPdfPts]} (y 已归一到 [0, 1], 3 位小数够画)
ojson mini_pdf_json(const MiniPdf &p) {
  ojson o;
  o["lo"] = sig4(p.lo);
  o["hi"] = sig4(p.hi);
  ojson y = ojson::array();
  for (const float v : p.y)
    y.push_back(std::round(v * 1000.f) / 1000.0);
  o["y"] = y;
  return o;
}
bool load_mini_pdf(const json &o, MiniPdf &dst) {
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
    const json &v = (*yit)[static_cast<size_t>(i)];
    if (!v.is_number())
      return false;
    t.y[i] = v.get<float>();
  }
  dst = t;
  return true;
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
  const auto cit = j.find("costs"); // [amt] 每金额段 cover + [n_hold] CostAcc (cost_json 的逆)
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

// 根评估结果 → 行 (EvalStat 整块 + scope / 小图; eval_ms 归调用方); ic_pdf = [y.hd.n]
void fill_stat(FactorRow &r, const FactorsRequest &req, const EvalScope &es, const YPlanes &y, const EvalStat &st, const AlphaDist &adist,
               const MiniPdf *ic_pdf, const char *backend) {
  static_cast<EvalStat &>(r) = st;
  r.scope.pool = req.scope.pool;
  r.scope.start_date = es.dates.front();
  r.scope.end_date = es.dates.back();
  r.scope.days = y.sh.days, r.scope.T = y.sh.T, r.scope.A = y.sh.A;
  r.scope.max_impact = req.scope.max_impact;
  r.scope.n_full = y.n_full;
  r.scope.backend = backend;
  r.scope.time = now_string();
  for (int hi = 0; hi < y.hd.n; ++hi)
    r.ic_pdf[hi] = ic_pdf[hi];
  r.alpha_pdf = mini_pdf_of(adist.pdf);
  r.has_stat = true;
  r.stat_from_file = false;
}

} // namespace

// ============================================================================
// FeatureTable
// ============================================================================

const FeatCol *XCols::find(std::string_view code) const {
  for (const FeatCol &c : leaves)
    if (c.code == code)
      return &c;
  return nullptr;
}

const LegCol &leg_of(const std::vector<LegCol> &cols, int key) {
  for (const LegCol &c : cols)
    if (c.key == key)
      return c;
  assert(false && "key 不在字段表两腿列里");
  __builtin_unreachable();
}

std::vector<int> keys_of(const std::vector<LegCol> &cols) {
  std::vector<int> keys;
  for (const LegCol &c : cols)
    keys.push_back(c.key);
  return keys;
}

factor::expr::FeatureLookup FeatureTable::lookup() const {
  return [this](std::string_view code) {
    if (const FeatCol *c = x.find(code))
      return c->allowed ? factor::expr::FeatState::OK : factor::expr::FeatState::FORBIDDEN;
    // 档.层.列 的层比网格细 (字段表只收不细于网格的层)
    const size_t p = code.find('.'), q = p == std::string_view::npos ? p : code.find('.', p + 1);
    if (q != std::string_view::npos)
      for (size_t lv = 0; lv < factor_level(seg).lvl; ++lv)
        if (code.substr(p + 1, q - p - 1) == kLevelToken[lv])
          return factor::expr::FeatState::FINER;
    return factor::expr::FeatState::MISSING;
  };
}

bool FeatureTable::check_grid(const factor::expr::Expr &e, std::string &err) const {
  const FactorLevel &lv = factor_level(seg);
  size_t finest = LEVEL_COUNT;
  for (const factor::expr::Node &n : e.nodes)
    if (n.op < 0) {
      const FeatCol *c = x.find(n.feat);
      assert(c && "parse 过的叶须在字段表里");
      finest = std::min(finest, c->level);
    }
  if (finest == LEVEL_COUNT || finest == lv.lvl) // 无特征叶 (纯生成算子) 随目录层
    return true;
  assert(finest > lv.lvl);
  err = std::string("最细的特征层是 ") + kLevelToken[finest] + ", 比因子层 " + lv.name + " 粗: 因子层 = 所用特征的最细层, 放到对应层目录";
  return false;
}

const FactorLevel &factor_level(int seg) {
  for (const FactorLevel &l : kFactorLevels)
    if (l.seg == seg)
      return l;
  assert(false && "seg 不是因子层");
  __builtin_unreachable();
}

std::string factor_dir_of(const SharedData &data, const std::string &pool, int seg) {
  const FactorLevel &l = factor_level(seg);
  assert(level_for_pool(l, pool_def(pool).tier) && "该池不能用这个因子层");
  return data.config.factor_dir + "/" + pool + "/" + l.name;
}

PerPool<FeatureLib> factor_libs(const SharedData &data, const std::string &pool, int seg) {
  const FactorLevel &l = factor_level(seg);
  assert(level_for_pool(l, pool_def(pool).tier) && "该池不能用这个因子层");
  PerPool<FeatureLib> libs;
  libs[kAllPool] = load_lib(data.config, pool_def(kAllPool).name);
  if (const PoolId lab = l.lab(pool_id(pool)); lab != kAllPool)
    libs[lab] = load_lib(data.config, pool);
  return libs;
}

std::vector<std::filesystem::path> list_factor_files(const std::string &dir) {
  std::vector<std::filesystem::path> files;
  if (std::filesystem::is_directory(dir))
    for (const auto &ent : std::filesystem::directory_iterator(dir))
      if (ent.is_regular_file() && ent.path().extension() == ".json")
        files.push_back(ent.path());
  std::sort(files.begin(), files.end());
  return files;
}

FeatureTable BuildFeatureTable(const Feature &feature, int seg) {
  const FactorLevel &lv = factor_level(seg);
  assert(level_valid_rows(lv.lvl) == static_cast<size_t>(seg) && "因子层段长与特征库层有效行数不符");
  FeatureTable t;
  t.seg = seg;
  t.feature = &feature;
  for (const TierId tier : {TierId::Lob, TierId::Bar}) {
    if (tier == TierId::Lob && lv.tier != TierId::Lob) // lob.* = 因子池的库, 只有 Lob 标签层的池是 Lob 池
      continue;
    for (size_t l = lv.lvl; l < LEVEL_COUNT; ++l) {
      if (!has_level(tier, l))
        continue;
      const auto &list = feature.meta(tier).features[l];
      const std::string pre = std::string(tier_name(tier)) + "." + kLevelToken[l] + ".";
      for (size_t i = 0; i < list.size(); ++i) {
        const FeatureMetadata &f = list[i];
        t.x.leaves.push_back({{tier, l, static_cast<uint32_t>(i)}, pre + f.code, f.valid_type, f.data_type == FeatureDataType::TS || f.data_type == FeatureDataType::CS});
      }
    }
  }
  const Feature::Metadata &meta = feature.meta(lv.tier);
  const auto &level = meta.features[lv.lvl];
  const auto own = [&](uint32_t c) { return Col{lv.tier, lv.lvl, c}; }; // 标签档网格层本层列
  t.y.ts = own(static_cast<uint32_t>(meta.col_of(lv.lvl, "ts_valid")));
  t.y.cs = own(static_cast<uint32_t>(meta.col_of(lv.lvl, "cs_valid")));
  const char *ohlc[4] = {"open", "high", "low", "close"};
  for (int i = 0; i < 4; ++i) {
    const std::string code = lv.tier == TierId::Lob ? std::string("lob_") + ohlc[i]
                             : lv.lvl == L_1day     ? std::string(ohlc[i]) + "_day"
                                                    : std::string(ohlc[i]) + "_" + std::to_string(level_bin_minutes(lv.lvl)) + "min";
    t.y.ohlc[i] = own(static_cast<uint32_t>(meta.col_of(lv.lvl, code.c_str())));
  }
  {
    const std::string code = lv.tier == TierId::Lob ? std::string("lob_close_adj")
                             : lv.lvl == L_1day     ? std::string("close_adj_day")
                                                    : "close_adj_" + std::to_string(level_bin_minutes(lv.lvl)) + "min";
    t.y.close_adj = own(static_cast<uint32_t>(meta.col_of(lv.lvl, code.c_str())));
  }
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
  for (size_t i = 0; i < level.size(); ++i) {
    const FeatureMetadata &f = level[i];
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
  const auto legs = [&](int key) {
    LegCol lc;
    lc.key = key;
    lc.leg[factor::stat::kLong] = lc.leg[factor::stat::kShort] = own(UINT32_MAX);
    return lc;
  };
  const auto full = [](const LegCol &lc) { return lc.leg[factor::stat::kLong].col != UINT32_MAX && lc.leg[factor::stat::kShort].col != UINT32_MAX; };
  for (int h : holds) {
    if (!factor::stat::hold_ok(h, factor::geo_of(seg)))
      continue;
    LegCol lc = legs(h);
    for (const Lab &l : labs)
      if (l.hold == h)
        lc.leg[l.is_long ? factor::stat::kLong : factor::stat::kShort].col = l.col;
    assert(full(lc) && "标签列不齐: 某 hold 缺 long 或 short");
    t.y.labels.push_back(lc);
  }
  assert(!t.y.labels.empty() && "本层无合法持有期");
  for (int a : amts) {
    LegCol lc = legs(a);
    for (const Cost &c : costs)
      if (c.amt == a)
        lc.leg[c.is_buy ? factor::stat::kLong : factor::stat::kShort].col = c.col;
    assert(full(lc) && "冲击成本列不齐: 某金额段缺 buy 或 sell");
    t.y.costs.push_back(lc);
  }
  return t;
}

bool MakeFactorScope(const SharedData &data, const std::string &pool, int seg, bool gpu, FactorScope &sc) {
  sc = FactorScope{};
  sc.seg = seg;
  sc.pool = pool;
  sc.factor_dir = factor_dir_of(data, pool, seg);
  sc.max_impact = data.config.max_impact;
  sc.gpu = gpu;
  if (data.asset.items.empty())
    return false; // 资产轴未就绪 (数据库未扫), 读不了特征库
  sc.libs = factor_libs(data, pool, seg);
  return sc.libs[factor_level(seg).lab(pool_id(pool))].ready(); // 标签库未建 → false
}

bool MakeFactorsRequest(const SharedData &data, const std::string &pool, int seg, bool evaluate, bool gpu, FactorsRequest &req) {
  req = FactorsRequest{};
  req.spec.evaluate = evaluate;
  return MakeFactorScope(data, pool, seg, gpu, req.scope) || !evaluate;
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
  erase_stats(j);
  ojson arr = ojson::array();
  for (const SearchWeight &w : ws) {
    assert(w.sign == 1 || w.sign == -1);
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
  return evaluate(req, misc::exec::ensure(ex_, misc::exec::hw()));
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
      if (w.sign < 0) { // 评估对象 = 根下插 TsNeg
        std::string err;
        const bool ok = factor::expr::parse(factor::expr::negated_canon(e), fl, ft.seg, e, err);
        assert(ok && "合法实例的取反串必可解析");
        (void)ok;
      }
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
  const FeatureTable &ft = feats();
  assert(req.scope.seg == ft.seg && "请求层与字段表层不符");
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

// 装载 + 评估 (worker 线程; 返回 false = 取消): 逐因子文件 evaluate_file
bool FactorsService::evaluate(const FactorsRequest &req, misc::exec::ForkJoin &ex) {
  TraceN("FactorsEvaluate");
  // 有效权重行 → Expr (scan 已校验过, 这里必成功), 按文件分组 (scan 出的行同文件相邻)
  std::vector<std::vector<size_t>> idx;
  std::vector<std::vector<factor::expr::Expr>> exprs;
  std::vector<std::vector<factor::stat::Frame>> frames; // [k] 口径 (同根的因子规范串相同 → 口径相同)
  {
    std::lock_guard<std::mutex> lock(mutex);
    const factor::expr::FeatureLookup fl = feats().lookup();
    for (size_t i = 0; i < rows.size(); ++i) {
      if (!rows[i].error.empty() || rows[i].expr.empty() || (!req.spec.only_file.empty() && rows[i].file != req.spec.only_file)) {
        rows[i].status = RowStatus::Done; // BROKEN / 未搜索 / 不在 only_file 的行本轮不算
        continue;
      }
      factor::expr::Expr e;
      std::string err;
      const bool ok = factor::expr::parse(rows[i].expr, fl, feats().seg, e, err);
      assert(ok && "scan 过的规范串必可再解析");
      (void)ok;
      if (idx.empty() || rows[idx.back().front()].file != rows[i].file)
        idx.emplace_back(), exprs.emplace_back(), frames.emplace_back();
      idx.back().push_back(i);
      exprs.back().push_back(std::move(e));
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
  LibReaders lr;
  open_libs(req.scope, lr, &cancel_);
  ScopeCache sc; // 本轮作用域块: Y 跨文件复用 (同作用域), X 每文件 retain
  return with_engine(req.scope.gpu, ex, cancel_, 1, [&](auto &eng) {
    for (size_t f = 0; f < idx.size(); ++f)
      if (!evaluate_file(req, ex, lr, sc, eng, idx[f], exprs[f], frames[f])) {
        if (lr.stale()) {
          std::lock_guard<std::mutex> lock(mutex);
          for (FactorRow &r : rows)
            r.status = RowStatus::Done;
          message = "特征库判废 (池轴/字段表与当前不符), 需重算特征";
        }
        return false;
      }
    return true;
  });
}

template <class E>
bool FactorsService::evaluate_file(const FactorsRequest &req, misc::exec::ForkJoin &ex, LibReaders &lr, ScopeCache &sc, E &eng,
                                   const std::vector<size_t> &idx, const std::vector<factor::expr::Expr> &exprs,
                                   const std::vector<factor::stat::Frame> &frames) {
  TraceN("FactorsEvaluateFile");
  // 该文件的权重 → 一张共享 DAG (F.roots[k] ↔ idx[k])
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
      for (int a = 0; a < nd.n_in; ++a)
        stack.push_back(nd.in[a]);
    }
  }

  // ---- 评估期 / A 轴 (所用库落盘日交集; 不成 → 该文件各行 BROKEN) ----
  const FeatureTable &ft = feats();
  EvalScope es;
  std::string why;
  if (!eval_scope(ft, req.scope, F.feats, es, why)) {
    std::lock_guard<std::mutex> lock(mutex);
    for (const size_t i : idx) {
      rows[i].error = why;
      rows[i].status = RowStatus::Done;
      broken_.fetch_add(1, std::memory_order_relaxed);
    }
    publish();
    return true;
  }
  // ---- 装载 (整张量): Y = 全部持有期 + 全部金额段, 同作用域跨文件复用; X = 本文件的叶 (retain, 共享 DAG 输入下标 = feats 下标) ----
  //      宿主峰值 = Y + X + rx / lc + CPU 活槽 + 标签秩 | GPU 掩码 / alpha 拷回, + Row[H][T]
  const YSpec ys{holds_of(keys_of(ft.y.labels)), amts_of(keys_of(ft.y.costs)), static_cast<float>(req.scope.max_impact)};
  {
    const int days = static_cast<int>(es.days.size()), A = static_cast<int>(es.rs.axis.size());
    const double H = ys.hd.n;
    const double cells = static_cast<double>(days) * ft.seg * A, rows_T = static_cast<double>(days) * ft.seg;
    const double per = cell_bytes(ys) + cell_bytes(XSpec{F.feats}) + 3.0 + (req.scope.gpu ? kPlaneBytes : kPlaneBytes * F.n_slots + 2.0 * H);
    assert_fits("Run", cells * per + H * rows_T * sizeof(factor::stat::Row), days, A, ft.seg);
  }
  if (!ensure_y(sc, ft, es, lr, 0, es.days.size(), ys, ex, cancel_, *this) || !ensure_x(sc, ft, es, lr, F.feats, /*retain=*/true, ex, cancel_, *this))
    return false;
  const YPlanes &y = sc.y;

  // ---- 特征叶元的值域数据检查 (每算子节点一次, 按 OpTable in 列; 子树含坏节点的因子 → BROKEN, 跳过) ----
  std::vector<std::string> node_err(static_cast<size_t>(N));
  {
    const std::vector<const factor::check::Plane *> in = leaves(sc, F.feats);
    for (int i = 0; i < N; ++i)
      check_leaf_domains(F, i, in, node_err[static_cast<size_t>(i)]);
  }
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
  {
    size_t n_need = 0;
    for (int i = 0; i < N; ++i)
      n_need += needed[static_cast<size_t>(i)] && F.nodes[static_cast<size_t>(i)].op >= 0;
    std::lock_guard<std::mutex> lock(mutex);
    for (size_t k = 0; k < idx.size(); ++k)
      if (runnable[k])
        rows[idx[k]].status = RowStatus::Running;
    begin(Phase::Running, n_need); // Running 期进度 = 共享 DAG 的算子节点
  }
  const int H = y.hd.n, T = y.sh.T;
  const std::filesystem::path dir(req.scope.factor_dir);
  std::vector<double> node_ms(static_cast<size_t>(N), 0.0);
  std::vector<factor::stat::Row> lrows(static_cast<size_t>(H) * T); // 该根的一级 Row[H][T] (引擎落地), 只给 ic_pdf
  EvalStat st;                                                      // 该根的宿主收尾
  AlphaDist adist;
  MiniPdf ic_pdf[factor::stat::kMaxHold]; // 该根的因子表小图 (与 Inspect 同一组样本)

  // 共享 DAG 走过节点 i (ms = 算子耗时, 叶 0): 记时 + 进度; 到根: eng.stat → 发布该根的全部因子 (dup 共根)
  eng.bind(sc);
  return eng.walk(F, needed.data(), [&](int i, double ms) {
    if (cancel_.load(std::memory_order_relaxed))
      return false;
    if (F.nodes[static_cast<size_t>(i)].op >= 0) {
      node_ms[static_cast<size_t>(i)] = ms;
      publish(1);
    }
    if (!F.is_root(i))
      return true;
    bool stat_done = false;
    for (size_t k = 0; k < idx.size(); ++k) {
      if (!runnable[k] || F.roots[k] != i)
        continue;
      if (!stat_done) {
        eng.stat(i, frames[k], lrows.data(), st, adist);
        ic_mini_pdfs(lrows.data(), H, T, ic_pdf);
        stat_done = true;
      }
      FactorRow r;
      {
        std::lock_guard<std::mutex> lock(mutex);
        r = rows[idx[k]];
      }
      fill_stat(r, req, es, y, st, adist, ic_pdf, E::kName);
      r.eval_ms = 0.0;
      for (int j : sub[k])
        r.eval_ms += node_ms[static_cast<size_t>(j)];
      r.status = RowStatus::Done;
      {
        std::lock_guard<std::mutex> lock(mutex);
        rows[idx[k]] = r;
      }
      write_back(dir / r.file, r);
      publish();
    }
    return true;
  });
}

} // namespace GUI::Factors
