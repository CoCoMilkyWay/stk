// Bridge — 见头文件. task_agent/ 里唯一 include 项目因子 / 特征 / 服务头的 TU (CMake PRECISE: 因子契约头的消费者)
#include "gui/task_agent/Bridge.hpp"
#include "gui/task_agent/Config.hpp" // kMaxOps / kGoodOps
#include "gui/task_agent/Text.hpp"

#include "factor/Expr.hpp"                 // kOps / operand_tex
#include "factor/GpuRun.hpp"               // available
#include "factor/Search/Contract.hpp"      // search_hold / Score / kMinMono
#include "factor/Search/Space.hpp"         // slots_of (曲面的槽)
#include "features/Storage/FeatureLib.hpp" // feature_pools
#include "features/TimeIndex.hpp"          // bin_to_Clock (行的收盘时刻)
#include "fundamental/AssetAxis.hpp"       // 码 ↔ 全局资产下标
#include "gui/engine/File.hpp"
#include "gui/engine/Hold.hpp"  // hold_desc
#include "gui/engine/Scope.hpp" // span_of
#include "gui/engine/services/LensService.hpp"
#include "gui/engine/services/RunService.hpp"
#include "gui/engine/services/SearchService.hpp"
#include "gui/task_agent/services/ReplayService.hpp"
#include "gui/util/PoolTip.hpp" // move_pool
#include "misc/date.hpp"        // dash
#include "misc/epoch.hpp"
#include "shared/SharedData.hpp"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <deque>
#include <map>
#include <mutex>
#include <tuple>

namespace GUI::Agent {

static_assert(kGroups == factor::stat::kGroups && kRankNone == factor::stat::kRankNone && kRankMax == factor::stat::kRankMax);
static_assert(kTails == Engine::kTails && kTailOfGroup[0] == Engine::kTailOfGroup[0] && kTailOfGroup[1] == Engine::kTailOfGroup[1] && kTailOfGroup[2] == Engine::kTailOfGroup[2] &&
              factor::stat::kTop == 0 && factor::stat::kEnds == 2);
static_assert(static_cast<int>(BookKind::New) == static_cast<int>(ReplayKind::New) &&
              static_cast<int>(BookKind::Cancel) == static_cast<int>(ReplayKind::Cancel) &&
              static_cast<int>(BookKind::Filled) == static_cast<int>(ReplayKind::Filled) &&
              static_cast<int>(BookKind::Sweep) == static_cast<int>(ReplayKind::Sweep) && kBigQuantile == 0.99f);

double Mono(const double *g, int n) { return factor::stat::mono_of(g, n); }
int TradeSec(int clock) { return static_cast<int>(trade_sec(static_cast<uint32_t>(clock))); }

namespace {

using text::num;

Engine::FactorLevelId level_id(int level) {
  assert(level >= 0 && static_cast<size_t>(level) < Engine::kFactorLevelCount);
  return static_cast<Engine::FactorLevelId>(level);
}

// 字段表视图: 每进程代每层建一次 (GUI 线程独占)
std::shared_ptr<const Engine::FeatureTable> table(const SharedData &data, int level) {
  static misc::Epochal<Engine::FeatureTables> tables;
  return Engine::table_of(tables.get(), data.feature, level_id(level));
}

// 表达式的算子节点数 (解析不了 = 0, 由落文件报原因)
int ops_of(const Engine::FeatureTable &ft, const std::string &src) {
  factor::expr::Expr e;
  std::string error;
  return factor::expr::parse(src, ft.lookup(), ft.seg, e, error) ? e.n_ops : 0;
}

// 同式键: 规范串, 截面根 (CsRank / CsNormRank / CsZ 只看截面排序, 互换不改分组与 rIC) 去掉根名; 解析不了 = 空
std::string twin_key(const Engine::FeatureTable &ft, const std::string &src) {
  factor::expr::Expr e;
  std::string error;
  if (!factor::expr::parse(src, ft.lookup(), ft.seg, e, error))
    return {};
  const factor::expr::Node &r = e.root();
  const std::string_view nm = r.op >= 0 ? std::string_view(factor::expr::kOps[r.op].name) : std::string_view();
  return nm == "CsRank" || nm == "CsNormRank" || nm == "CsZ" ? "Cs:" + factor::expr::to_string(e, r.args[0]) : e.canon;
}

// 与 src 同式的可见候选名; 无 = 空
std::string twin_of(const Engine::FeatureTable &ft, const std::vector<Result> &results, const std::string &src) {
  const std::string key = twin_key(ft, src);
  if (key.empty())
    return {};
  for (const Result &r : results)
    if (r.visible && !r.tmpl.empty() && twin_key(ft, r.tmpl) == key)
      return r.name;
  return {};
}

// ---- 改动归类 (Lab::delta) ----
namespace fx = factor::expr;

std::string sub_of(const fx::Expr &e, int i) { return fx::to_string(e, i); }
int arity_of(const fx::Node &n) { return n.n_args; }
bool op_in(const fx::Node &n, std::initializer_list<std::string_view> names) {
  return n.op >= 0 && std::find(names.begin(), names.end(), std::string_view(fx::kOps[n.op].name)) != names.end();
}
bool smooth_op(const fx::Node &n) { return op_in(n, {"TsMeanRoll", "TsWmaRoll", "TsMeanEma", "TsMeanCum", "TsSumRoll", "TsSumCum"}); }
bool squash_op(const fx::Node &n) { return op_in(n, {"TsLog", "TsSqrt", "TsClip", "TsZRoll", "CsWinsor"}); }

// 同算子两节点的参数相同 (自由位相同, 固定值相同)
bool same_params(const fx::Node &a, const fx::Node &b) {
  const fx::OpInfo &o = fx::kOps[a.op];
  return a.free == b.free && (!fx::declares(o.params, "d") || (a.free & fx::kFreeD) || a.p.d == b.p.d) &&
         (!fx::declares(o.params, "k") || (a.free & fx::kFreeK) || a.p.k == b.p.k) && (!fx::declares(o.params, "k2") || (a.free & fx::kFreeK2) || a.p.k2 == b.p.k2) &&
         (!fx::declares(o.params, "h") || (a.free & fx::kFreeH) || a.p.h == b.p.h) &&
         (!fx::declares(o.params, "w") || (a.free & fx::kFreeW) || std::equal(a.p.w, a.p.w + a.n_args, b.p.w));
}

// p 有一元的子式 = s
bool has_kid(const fx::Expr &e, const fx::Node &p, const std::string &s) {
  for (int a = 0; a < arity_of(p); ++a)
    if (sub_of(e, p.args[a]) == s)
      return true;
  return false;
}

// 一处差异: base 的节点 i ↔ expr 的节点 j; param = 同算子同子树, 只差参数.
// 同算子而一边整个是另一边的一元 (TsAdd(a, b) → TsAdd(TsAdd(a, b), c)) 也是一处, 不逐元比
struct Region {
  int i, j;
  bool param;
};
void regions_of(const fx::Expr &b, int i, const fx::Expr &n, int j, std::vector<Region> &out) {
  const std::string sb = sub_of(b, i), sn = sub_of(n, j);
  if (sb == sn)
    return;
  const fx::Node &x = b.nodes[static_cast<size_t>(i)], &y = n.nodes[static_cast<size_t>(j)];
  if (x.op < 0 || x.op != y.op || x.n_args != y.n_args || has_kid(n, y, sb) || has_kid(b, x, sn)) {
    out.push_back({i, j, false});
    return;
  }
  if (!same_params(x, y))
    out.push_back({i, j, true});
  for (int a = 0; a < arity_of(x); ++a)
    regions_of(b, x.args[a], n, y.args[a], out);
}

// 计量 / 阈值词: 额 / 量 / 笔, 带数字的 (1s / p93 / 20w / 5), all
bool measure_word(const std::string &w) {
  return w == "amt" || w == "vol" || w == "n" || w == "all" || std::any_of(w.begin(), w.end(), [](char ch) { return ch >= '0' && ch <= '9'; });
}
// 两叶之换: 同档层同词数只差一个词 → "旧词→新词" 作签名 (口径 = 两词都是计量 / 阈值词且同族); 否则签名 = 两个全名
struct Swap {
  std::string sig;
  bool caliber = false;
};
Swap swap_of(const std::vector<Leaf> &leaves, const std::string &a, const std::string &b) {
  const auto fam = [&](const std::string &c) {
    const auto it = std::find_if(leaves.begin(), leaves.end(), [&](const Leaf &l) { return l.code == c; });
    return it == leaves.end() ? std::string() : it->family;
  };
  const auto [ga, wa] = LeafWords(a);
  const auto [gb, wb] = LeafWords(b);
  if (ga == gb && wa.size() == wb.size()) {
    size_t diff = 0, at = 0;
    for (size_t k = 0; k < wa.size(); ++k)
      if (wa[k] != wb[k])
        ++diff, at = k;
    if (diff == 1)
      return {wa[at] + "→" + wb[at], measure_word(wa[at]) && measure_word(wb[at]) && fam(a) == fam(b)};
  }
  return {a + "→" + b, false};
}

Delta classify(const std::vector<Leaf> &leaves, const fx::Expr &b, const fx::Expr &n) {
  std::vector<Region> rs;
  regions_of(b, 0, n, 0, rs);
  const auto shown = [&](const Region &r) { return "`" + text::head(sub_of(b, r.i), 80) + "` → `" + text::head(sub_of(n, r.j), 80) + "`"; };
  std::string what;
  for (const Region &r : rs)
    what += (what.empty() ? "" : "; ") + shown(r);
  if (rs.empty())
    return {Change::Same, ""};
  if (std::all_of(rs.begin(), rs.end(), [](const Region &r) { return r.param; }))
    return {Change::Params, what};
  const auto leafy = [&](const Region &r) { return !r.param && b.nodes[static_cast<size_t>(r.i)].op < 0 && n.nodes[static_cast<size_t>(r.j)].op < 0; };
  if (std::all_of(rs.begin(), rs.end(), leafy)) {
    const Swap s0 = swap_of(leaves, b.nodes[static_cast<size_t>(rs[0].i)].feat, n.nodes[static_cast<size_t>(rs[0].j)].feat);
    const bool one = std::all_of(rs.begin(), rs.end(), [&](const Region &r) {
      return swap_of(leaves, b.nodes[static_cast<size_t>(r.i)].feat, n.nodes[static_cast<size_t>(r.j)].feat).sig == s0.sig;
    });
    if (one)
      return {s0.caliber ? Change::Caliber : Change::Leaf, what};
  }
  if (rs.size() > 1)
    return {Change::Multi, what};
  const fx::Node &x = b.nodes[static_cast<size_t>(rs[0].i)], &y = n.nodes[static_cast<size_t>(rs[0].j)];
  const std::string sb = sub_of(b, rs[0].i), sn = sub_of(n, rs[0].j);
  Change k = Change::Struct;
  if (arity_of(y) == 1 && sub_of(n, y.args[0]) == sb)
    k = smooth_op(y) ? Change::Smooth : squash_op(y) ? Change::Squash
                                                     : Change::Wrap;
  else if (arity_of(x) == 1 && sub_of(b, x.args[0]) == sn)
    k = smooth_op(x) ? Change::Smooth : squash_op(x) ? Change::Squash
                                                     : Change::Unwrap;
  else if (arity_of(x) == 1 && arity_of(y) == 1 && sub_of(b, x.args[0]) == sub_of(n, y.args[0]))
    k = smooth_op(x) && smooth_op(y) ? Change::Smooth : squash_op(x) && squash_op(y) ? Change::Squash
                                                                                     : Change::Struct;
  else if (arity_of(y) >= 2 && has_kid(n, y, sb))
    k = op_in(y, {"TsMask", "TsWhere"}) ? Change::Cond : Change::Add;
  else if (arity_of(x) >= 2 && has_kid(b, x, sn))
    k = Change::Drop;
  return {k, what};
}

// 具体式 (无自由参数) 的解析; 不成 → false + error
bool concrete(const Engine::FeatureTable &ft, const std::string &src, factor::expr::Expr &e, std::string &error) {
  if (!factor::expr::parse(src, ft.lookup(), ft.seg, e, error))
    return false;
  if (e.is_template()) {
    error = "含自由参数 (裸 d / k / k2 / h / w): 观察只接具体式, 参数写成数字 (h 写持有期 token, w 写 [数字, …])";
    return false;
  }
  return true;
}

// 具体特征式 (任意子式) → CsRank 包装的规范串; 不成 → false + error
bool feature_root(const Engine::FeatureTable &ft, const std::string &src, std::string &root, std::string &error) {
  factor::expr::Expr e;
  if (!concrete(ft, src, e, error))
    return false;
  [[maybe_unused]] const bool ok = concrete(ft, "CsRank(" + e.canon + ")", e, error);
  assert(ok && "具体式包一层 CsRank 必可解析");
  root = e.canon;
  return true;
}

const FeatureMetadata &meta_of(const Engine::FeatureTable &ft, const Engine::FeatCol &c) { return ft.feature->meta(c.tier).features[c.level][c.col]; }

// 叶的特征族: 节点文件 features/Node/<族>.hpp → <族>
std::string family_of(const FeatureMetadata &m) {
  const std::string_view f = m.file, pre = "features/Node/", ext = ".hpp";
  assert(f.starts_with(pre) && f.ends_with(ext) && "节点文件在 features/Node/ 下");
  return std::string(f.substr(pre.size(), f.size() - pre.size() - ext.size()));
}
// 叶的来源 = 所在档
Src src_of(const Engine::FeatCol &c) { return c.tier == TierId::Lob ? Src::Lob : Src::Bar; }

// 开放给 agent 的叶 (叶名) / 算子 ([kOps 下标]): 叶 = 来源开放且人勾了, 算子 = 人勾了
struct Opened {
  std::set<std::string> leaves;
  std::vector<bool> ops;
  bool has(const fx::Node &n) const { return n.op >= 0 ? ops[static_cast<size_t>(n.op)] : leaves.contains(n.feat); }
};
Opened opened_of(const Engine::FeatureTable &ft, const Scope &sc) {
  Opened o;
  const Open &pick = sc.leaves_of(Engine::factor_level(ft.lv).name);
  for (const Engine::FeatCol &l : ft.x.leaves)
    if (l.allowed && (sc.srcs() & src_bit(src_of(l))) && pick.has(l.code))
      o.leaves.insert(l.code);
  for (const fx::OpInfo &x : fx::kOps)
    o.ops.push_back(sc.ops.has(x.name));
  return o;
}

// TS 叶落盘前套的元素变换 (ts::Tf) 的定义式, 下标 = ts::TfId; CS 叶的变换写在其公式里
constexpr const char *kTfDef[] = {"", "sign(x)·ln(1+|x|)", "asinh(x)", "tanh(x)", "sign(x)·√|x|"};
static_assert(std::size(kTfDef) == static_cast<size_t>(ts::TfId::kCount));

// 落盘值 → 原量 (ts::Tf 的逆)
float raw_of(ts::TfId tf, float y) {
  switch (tf) {
  case ts::TfId::None:
    return y;
  case ts::TfId::Log:
    return std::copysign(std::expm1(std::fabs(y)), y);
  case ts::TfId::Asinh:
    return std::sinh(y);
  case ts::TfId::Tanh:
    return std::atanh(y);
  case ts::TfId::Sqrt:
    return std::copysign(y * y, y);
  case ts::TfId::kCount:
    break;
  }
  assert(false && "未知 ts::TfId");
  __builtin_unreachable();
}

// 层 lvl 的叶 档.层.col (字段表里有且可作输入; 没有 → 空)
std::string leaf(const Engine::FeatureTable &ft, size_t lvl, const std::string &col) {
  for (const Engine::FeatCol &c : ft.x.leaves)
    if (c.allowed && c.level == lvl && c.code.size() > col.size() && c.code.ends_with("." + col))
      return c.code;
  return {};
}
// 叶 code 的落盘变换 (须在字段表里)
ts::TfId tf_of(const Engine::FeatureTable &ft, const std::string &code) {
  const auto it = std::find_if(ft.x.leaves.begin(), ft.x.leaves.end(), [&](const Engine::FeatCol &c) { return c.code == code; });
  assert(it != ft.x.leaves.end());
  return meta_of(ft, *it).ts_tf;
}
PerBar<std::string> kline_of(const Engine::FeatureTable &ft) {
  const Engine::FactorLevel &l = Engine::factor_level(ft.lv);
  const auto bar = [&](const char *f) {
    char b[64];
    std::snprintf(b, sizeof(b), l.kline, f);
    return leaf(ft, l.lvl, b);
  };
  // 量额: K 线列名式里没有的层 (1min 的 K 线在盘口档) 取本层别档的同名列
  const auto flow = [&](const char *f) {
    const std::string s = bar(f);
    return s.empty() ? leaf(ft, l.lvl, f) : s;
  };
  PerBar<std::string> k;
  k[Bar::Close] = bar("close"), k[Bar::High] = bar("high"), k[Bar::Low] = bar("low"), k[Bar::Volume] = flow("volume"), k[Bar::Amount] = flow("amount");
  k[Bar::PreClose] = leaf(ft, L_1day, "pre_close"), k[Bar::LimUp] = leaf(ft, L_1day, "lim_up"), k[Bar::LimDn] = leaf(ft, L_1day, "lim_dn");
  return k;
}

// 层的每行收盘时刻 HH:MM: 日层 = 收盘; 分钟层 = 该分钟末; 分钟箱层 = 箱收盘
std::vector<std::string> clocks_of(const Engine::FactorLevel &l) {
  std::vector<std::string> out;
  for (int k = 0; k < l.seg; ++k) {
    ClockTime c{15, 0, 0};
    if (l.lvl == L_1min) {
      const ClockTime m = L_1min_to_Clock(static_cast<size_t>(k));
      const int total = m.hour * 60 + m.minute + 1;
      c = {static_cast<uint8_t>(total / 60), static_cast<uint8_t>(total % 60), 0};
    } else if (l.seg > 1) {
      c = bin_to_Clock(static_cast<size_t>(240 / (l.seg - 1)), static_cast<size_t>(k));
    }
    char b[16];
    format_time_hm(b, sizeof(b), c);
    out.emplace_back(b);
  }
  return out;
}
// 每日开头属集合竞价段的行数: 日内层收盘时刻不晚于 09:30 的行 (分钟箱 1 行, 1min 15 行); 日层 0
int open_of(const std::vector<std::string> &clock) {
  if (clock.size() == 1)
    return 0;
  const int n = static_cast<int>(std::count_if(clock.begin(), clock.end(), [](const std::string &c) { return c <= "09:30"; }));
  assert(n >= 1 && clock[static_cast<size_t>(n - 1)] == "09:30");
  return n;
}

// Lens 的落地 → Look (r16 原样, 行 / 格换成 agent 的形); 因子根与 labels 对齐;
// 特征根先是 LookSpec::feats 里成的式 (feat_err 与 LookSpec::feats 对齐, 非空 = 不成, 没算), 其后按 kBars 序 = bars 里取了的 K 线叶 (kline = 其全名, 逆落盘变换成原量)
Look look_of(Engine::LensResult &&r, const Engine::FeatureTable &ft, const std::vector<std::string> &labels, std::vector<std::string> &&feat_err, const PerBar<bool> &bars,
             const PerBar<std::string> &kline) {
  Look o;
  o.error = std::move(r.error);
  if (!o.error.empty())
    return o;
  o.dates.assign(r.dates.begin(), r.dates.end());
  o.seg = ft.seg;
  o.clock = clocks_of(Engine::factor_level(ft.lv));
  o.open = open_of(o.clock);
  for (const uint32_t id : r.ids)
    o.codes.push_back(asset_axis().code(id));
  o.gate = std::move(r.gate), o.label = std::move(r.label), o.mkt = std::move(r.mkt);
  assert(r.roots.size() == labels.size());
  for (size_t j = 0; j < r.roots.size(); ++j) {
    Engine::LensRoot &x = r.roots[j];
    LookRoot y;
    y.label = labels[j];
    y.error = std::move(x.error), y.rx = std::move(x.rx);
    assert(x.tails.size() == x.rows.size());
    for (size_t t = 0; t < x.rows.size(); ++t) {
      const factor::stat::Row &w = x.rows[t];
      const Engine::TailRow &tr = x.tails[t];
      LookRow z;
      z.ok = w.ok, z.ric = w.ric, z.mkt = w.mkt;
      std::copy_n(w.grp, kGroups, z.grp);
      std::copy_n(w.cnt, kGroups, z.cnt);
      for (int e = 0; e < 2; ++e)
        for (int j = 0; j < kTails; ++j)
          z.tail[e][j] = tr.sum[e][j], z.tcnt[e][j] = tr.cnt[e][j];
      y.rows.push_back(z);
    }
    for (const Engine::LensCell &c : x.cells)
      y.cells.push_back({c.t, c.a, c.e == factor::stat::kTop, c.ret});
    o.factors.push_back(std::move(y));
  }
  size_t i = 0;
  for (std::string &e : feat_err)
    if (!e.empty()) {
      o.feats.push_back({std::move(e), {}});
    } else {
      o.feats.push_back({std::move(r.feats[i].error), std::move(r.feats[i].v)});
      ++i;
    }
  for (const Bar b : kBars)
    if (bars[b]) {
      LookFeat &k = o.kline[b] = {std::move(r.feats[i].error), std::move(r.feats[i].v)};
      if (const ts::TfId tf = tf_of(ft, kline[b]); tf != ts::TfId::None)
        for (float &x : k.v)
          x = raw_of(tf, x);
      ++i;
    }
  assert(i == r.feats.size());
  return o;
}

bool searchable(const Engine::FactorRow &r) { return r.error.empty() && !r.tmpl.empty(); }

// Search 评过的点 → 曲面: 各点的具体式按模板的槽 (前序节点) 读回参数值
Surface surface_of(const Engine::FeatureTable &ft, const Engine::SearchResult &r) {
  namespace sp = factor::search::space;
  Surface s;
  if (r.slots.empty())
    return s;
  const factor::expr::Expr t = ft.reparse_at(r.tmpl, r.hold); // 与 Search 同: 自由 h = 所搜持有期
  const std::vector<sp::Slot> slots = sp::slots_of(t, ft.seg, r.n_full * kGroups);
  assert(slots.size() == r.slots.size());
  s.slots = r.slots;
  for (const Engine::SearchCand &c : r.cands) {
    if (!c.sc.ok)
      continue;
    const factor::expr::Expr e = ft.reparse(c.expr);
    assert(e.nodes.size() == t.nodes.size() && "具体式与模板同构");
    Probe p;
    p.st = {c.sc.mono, c.sc.ls, c.sc.ric};
    for (const sp::Slot &x : slots) {
      const auto &q = e.nodes[static_cast<size_t>(x.node)].p;
      p.v.push_back(x.key == sp::Key::D    ? static_cast<double>(q.d)
                    : x.key == sp::Key::K  ? q.k
                    : x.key == sp::Key::K2 ? q.k2
                                           : q.w[x.wi]);
    }
    s.pts.push_back(std::move(p));
  }
  return s;
}

using Fails = std::map<std::string, std::string>; // 文件 → Search 中途放弃的原因 (本次运行里, 同作用域跨会话留着)

// 作用域此刻的数据: 未就绪 → 空 (此时一切统计都不可见)
std::optional<Engine::FactorScope> fscope_of(const SharedData &data, const std::shared_ptr<const Engine::FeatureTable> &ft, const std::string &pool) {
  Engine::FactorScope fsc;
  if (!Engine::MakeFactorScope(data, ft, pool, /*gpu=*/false, fsc))
    return std::nullopt;
  return fsc;
}

// 评估期 (同池 + 首末 + 天数); days = 0 → 不成 (与任何统计都不同窗)
struct Period {
  std::string pool;
  misc::Ymd start = 0, end = 0;
  int days = 0;
  bool same(const std::string &p, misc::Ymd s, misc::Ymd e, int n) const { return days > 0 && p == pool && s == start && e == end && n == days; }
  std::string text() const { return misc::dash(start) + " ~ " + misc::dash(end) + ", " + std::to_string(days) + " 天"; }
};
// 模板的特征叶 (首见序, 去重; 回归目标叶在标签库, 不计)
std::vector<Engine::LeafId> leaves_in(const Engine::FeatureTable &ft, const std::string &tmpl) {
  std::vector<std::string> codes;
  for (const factor::expr::Node &n : ft.reparse(tmpl).nodes)
    if (n.op < 0 && !n.target && std::find(codes.begin(), codes.end(), n.feat) == codes.end())
      codes.push_back(n.feat);
  return ft.x.ids(codes);
}
// 叶为 leaves (空 = 只有标签库) 时此刻的评估期
Period period_of(const std::optional<Engine::FactorScope> &fsc, const Engine::FeatureTable &ft, const std::vector<Engine::LeafId> &leaves) {
  Period s;
  if (!fsc)
    return s;
  Engine::Domain dm;
  std::string why;
  if (!Engine::span_of(ft, *fsc, leaves, SIZE_MAX, dm, why))
    return s;
  s.pool = fsc->pool, s.start = dm.dates.front(), s.end = dm.dates.back(), s.days = static_cast<int>(dm.dates.size());
  return s;
}

// 因子文件 search[] 的代表点 → Top (落选原因按统计定)
Top top_of(const Engine::SearchTop &t) {
  Top x;
  x.expr = t.expr, x.sign = t.sign;
  x.st = {t.mono, t.ls, t.ric};
  x.g = t.g, x.se = t.se, x.mu = t.mu;
  x.rep = t.peak ? Rep::Peak : factor::search::passes(t.ls, t.ric, t.mono) ? Rep::Gated
                                                                           : Rep::Mono;
  if (x.rep == Rep::Peak)
    x.miss = "过门槛的峰, 但曲面估计 " + num(t.mu) + " ≤ 标准误 " + num(t.se) + " (不显著)";
  else if (x.rep == Rep::Gated)
    x.miss = t.mu > 0.0 ? "过门槛的点都不成峰: 曲面估计更高处不过门槛 (多是 mono 不够)" : "过门槛的点曲面估计都 ≤ 0 (最高 " + num(t.mu) + ")";
  else if (t.mono < factor::search::kMinMono)
    x.miss = "没有点 mono ≥ " + num(factor::search::kMinMono) + " (最高 " + num(t.mono) + ")";
  else
    x.miss = "mono 过门槛的点每期毛多空或 rIC 都 ≤ 0";
  return x;
}

// 一个因子文件 → 本持有期下的结果 (权重按 score 降序, 结局随之定; 可见性按此刻评估期与开放的叶 / 算子)
Result result_of(const Engine::FeatureTable &ft, const std::optional<Engine::FactorScope> &fsc, const Opened &open, const Engine::FactorDir &dir, const std::string &file, int hold,
                 const Fails &fails) {
  std::vector<Engine::FactorRow> rows;
  Engine::ScanFactorFile(dir, file, ft, rows);
  assert(!rows.empty());
  const Engine::FactorRow &r0 = rows.front();
  Result r;
  r.file = r0.file, r.name = r0.name_en, r.name_cn = r0.name_cn, r.tmpl = r0.tmpl, r.note = r0.note;
  r.ops = ops_of(ft, r0.tmpl);
  const std::vector<Engine::LeafId> leaves = r0.tmpl.empty() ? std::vector<Engine::LeafId>{} : leaves_in(ft, r0.tmpl);
  const Period now = r0.tmpl.empty() ? Period{} : period_of(fsc, ft, leaves);
  bool fresh = true; // 证据都在此刻评估期上
  const auto mk = std::find_if(r0.searched.begin(), r0.searched.end(), [&](const Engine::SearchMark &m) { return m.hold == hold; });
  if (mk != r0.searched.end()) {
    fresh = now.same(now.pool, mk->start_date, mk->end_date, mk->days);
    if (mk->top)
      r.top = top_of(*mk->top);
  }
  if (const auto it = fails.find(r.file); it != fails.end())
    r.error = "Search 放弃: " + it->second;
  bool run = true; // 权重都 Run 过
  for (const Engine::FactorRow &w : rows) {
    if (!w.error.empty()) {
      if (r.error.empty())
        r.error = w.error;
      continue;
    }
    if (w.expr.empty())
      continue;
    run = run && w.has_stat;
    if (w.has_stat)
      fresh = fresh && now.same(w.scope.pool, w.scope.start_date, w.scope.end_date, w.scope.days);
    Weight x;
    x.expr = w.expr, x.sign = w.sign, x.hold_src = w.hold_src;
    const factor::search::Score s = Engine::RowScore(w, hold);
    x.has = s.ok;
    if (s.ok) {
      x.st = {s.mono, s.ls, s.ric};
      x.score = s.score, x.se = s.se, x.valid = w.valid_pct / 100.0;
      const factor::stat::HoldStat *h = w.find_hold(hold);
      assert(h && "RowScore ok 须有该持有期的 HoldStat");
      double mid = 0.0;
      for (const float g : h->grp)
        mid += g / static_cast<double>(kGroups);
      for (const float g : h->grp)
        x.grp.push_back(g - mid);
    }
    r.ws.push_back(std::move(x));
  }
  std::stable_sort(r.ws.begin(), r.ws.end(), [](const Weight &a, const Weight &b) { return a.has != b.has ? a.has : a.score > b.score; });
  const bool opened = r0.tmpl.empty() || std::ranges::all_of(ft.reparse(r0.tmpl).nodes, [&](const fx::Node &n) { return n.target || open.has(n); });
  r.visible = fresh && r.ops <= kMaxOps && opened;
  const bool null = r.ws.empty() ? mk != r0.searched.end() : run; // 搜过无显著 / 权重都 Run 过却无本持有期统计
  r.verdict = !r.error.empty() ? Verdict::Broken : r.best() ? Verdict::Weights
                                               : null       ? Verdict::Null
                                                            : Verdict::Pending;
  return r;
}

// 结果序: 有统计的在前, 最佳权重 score 降序
void sort_results(std::vector<Result> &v) {
  std::stable_sort(v.begin(), v.end(), [](const Result &a, const Result &b) {
    const Weight *x = a.best(), *y = b.best();
    return (x != nullptr) != (y != nullptr) ? x != nullptr : x && x->score > y->score;
  });
}

std::vector<Result> scan(const Engine::FeatureTable &ft, const std::optional<Engine::FactorScope> &fsc, const Opened &open, const Engine::FactorDir &dir, int hold, const Fails &fails) {
  std::vector<Result> out;
  for (const std::string &file : dir.files())
    out.push_back(result_of(ft, fsc, open, dir, file, hold, fails));
  sort_results(out);
  return out;
}

// 一道 (Search / Run / Lens / Replay 同构): 服务 + 发请求时的 epoch
template <class Svc>
struct Lane {
  std::unique_ptr<Svc> svc;
  uint64_t e0 = 0;

  template <class Req>
  void send(Req req) {
    if (!svc)
      svc = std::make_unique<Svc>();
    e0 = svc->epoch.load(std::memory_order_acquire);
    svc->Request(std::move(req));
  }
  // 取走跑完那轮的结果
  auto take() {
    std::lock_guard<std::mutex> lock(svc->mutex);
    return std::move(svc->result);
  }
  std::string note() const {
    std::lock_guard<std::mutex> lock(svc->mutex);
    return svc->message;
  }
  // 请求发出后服务跑完一轮: epoch 越过发请求时的值 (已 begin) 且阶段落到终态 (已 end); 先读 epoch 再读阶段
  bool finished() const {
    if (svc->epoch.load(std::memory_order_acquire) <= e0)
      return false;
    const Phase ph = svc->phase.load(std::memory_order_acquire);
    return ph == Phase::Done || ph == Phase::Cancelled;
  }
  bool cancelled() const { return svc->phase.load(std::memory_order_acquire) == Phase::Cancelled; }
  void cancel() const { svc->RequestCancel(); }
  void read(Lab::Live &l) const {
    const size_t n = svc->total.load(std::memory_order_relaxed);
    l.frac = n > 0 ? static_cast<double>(svc->done.load(std::memory_order_relaxed)) / static_cast<double>(n) : 0.0;
    l.message = note();
  }
};

Context context_of(const SharedData &data, const Scope &sc, const Opened &open) {
  const std::shared_ptr<const Engine::FeatureTable> ft = table(data, sc.level);
  Context c;
  c.pool = sc.pool;
  c.pool_cn = catalog::pool_cn(sc.pool);
  c.level = Engine::factor_level(level_id(sc.level)).name;
  c.hold = factor::stat::hold_name(sc.hold);
  c.hold_desc = catalog::hold_desc(sc.hold, sc.level);
  const std::optional<Engine::FactorScope> fsc = fscope_of(data, ft, sc.pool);
  std::array<bool, kSrcCount> has{}, on{}; // [Src] 本层有可作输入的叶 / 有开放的叶
  std::vector<Engine::LeafId> opened;      // 开放的叶
  for (size_t i = 0; i < ft->x.leaves.size(); ++i) {
    const Engine::FeatCol &l = ft->x.leaves[i];
    if (!l.allowed)
      continue;
    const Src src = src_of(l);
    has[static_cast<size_t>(src)] = true;
    const FeatureMetadata &m = meta_of(*ft, l);
    const size_t tf = static_cast<size_t>(m.ts_tf);
    Leaf x{l.code, m.name_cn, m.formula, m.description, m.ts_tf == ts::TfId::None ? "" : ts::TF_NAMES[tf], kTfDef[tf], family_of(m), src};
    x.kin = LeafKin(x.code) + "|" + x.tf;
    x.hay = text::lower(x.code + "\n" + x.name_cn + "\n" + x.formula + "\n" + x.desc + "\n" + x.tf + "\n" + x.family);
    if (open.leaves.contains(l.code)) {
      on[static_cast<size_t>(src)] = true;
      opened.push_back(static_cast<Engine::LeafId>(i));
      c.leaves.push_back(x);
    }
    c.all_leaves.push_back(std::move(x));
  }
  for (size_t s = 0; s < kSrcCount; ++s)
    if (on[s])
      c.srcs += (c.srcs.empty() ? "" : " / ") + std::string(kSrcs[s].name);
  c.has_lob = has[static_cast<size_t>(Src::Lob)];
  const Period base = period_of(fsc, *ft, opened);
  c.window = base.days > 0 ? base.text() : "未就绪";
  c.first = static_cast<int>(base.start), c.last = static_cast<int>(base.end);
  c.seg = ft->seg;
  c.clock = clocks_of(Engine::factor_level(ft->lv));
  c.open = open_of(c.clock);
  c.ts_root_d = factor::stat::ts_norm_d(ft->seg);
  c.max_d = factor::kMaxD;
  c.min_mono = factor::search::kMinMono;
  for (size_t i = 0; i < std::size(fx::kOps); ++i) {
    const fx::OpInfo &o = fx::kOps[i];
    Op x{o.name, o.c_name, fx::operand_tex(o), factor::dom_name(o.out), o.op, o.note};
    if (open.ops[i])
      c.ops.push_back(x);
    c.all_ops.push_back(std::move(x));
  }
  c.kline = kline_of(*ft);
  c.library = scan(*ft, fsc, open, Engine::factor_dir_of(sc.pool, level_id(sc.level)), sc.hold, {});
  return c;
}

} // namespace

std::pair<std::string, std::vector<std::string>> LeafWords(const std::string &code) {
  const size_t dot = code.find('.', code.find('.') + 1);
  assert(dot != std::string::npos && "叶名 = 档.层.列");
  std::vector<std::string> ws;
  for (size_t a = dot + 1, b; a <= code.size(); a = b + 1) {
    b = std::min(code.find('_', a), code.size());
    ws.push_back(code.substr(a, b - a));
  }
  return {code.substr(0, dot), ws};
}

std::string LeafKin(const std::string &code) {
  const auto [g, ws] = LeafWords(code);
  std::string s = g + ".";
  for (size_t k = 0; k < ws.size(); ++k)
    s += (k ? "_" : "") + (measure_word(ws[k]) ? std::string("*") : ws[k]);
  return s;
}

std::string GateMiss(const Gate &g, const Result &r) {
  if (r.verdict == Verdict::Broken)
    return "BROKEN: " + r.error;
  for (const Weight &w : r.ws)
    if (w.has && w.st.mono >= g.mono && w.st.ric >= g.ric)
      return "";
  const Weight *w = r.best();
  if (!w)
    return "本持有期无显著权重";
  std::string s = "最佳权重未达入库门槛:";
  if (w->st.mono < g.mono)
    s += " mono " + num(w->st.mono) + " < " + num(g.mono);
  if (w->st.ric < g.ric)
    s += " rIC " + num(w->st.ric) + " < " + num(g.ric);
  return s;
}

// ---- catalog ----
namespace catalog {

std::vector<Choice> pools() {
  std::vector<Choice> out;
  for (size_t i = 0; i < feature_pools().size(); ++i)
    out.push_back({static_cast<int>(i), feature_pools()[i].name});
  return out;
}
void move_pool(SharedData &data, int from, int to) { GUI::move_pool(data, {from, to}); }
std::string pool_cn(const std::string &pool) { return pool_def(pool).name_cn; }
std::vector<Choice> levels() {
  std::vector<Choice> out;
  for (const Engine::FactorLevel &l : Engine::kFactorLevels)
    out.push_back({l.id, l.name});
  return out;
}
std::vector<Choice> holds(const SharedData &data, int level) {
  std::vector<Choice> out;
  for (const Engine::LegCol &lg : table(data, level)->y.labels)
    if (factor::search::search_hold(lg.key))
      out.push_back({lg.key, factor::stat::hold_name(lg.key)});
  return out;
}
std::string hold_desc(int hold, int level) { return Engine::hold_desc(hold, level_id(level)); }
bool gpu() { return factor::gpu::available(); }

bool ready(const SharedData &data, const Scope &sc, bool gpu) {
  Engine::FactorScope probe;
  return Engine::MakeFactorScope(data, table(data, sc.level), sc.pool, gpu, probe);
}

std::string label(const Scope &sc) {
  return sc.pool + " / " + Engine::factor_level(level_id(sc.level)).name + " / " + factor::stat::hold_name(sc.hold);
}
disk::Key key(const Scope &sc, bool test) {
  return {.pool = sc.pool, .level = Engine::factor_level(level_id(sc.level)).name, .hold = factor::stat::hold_token(sc.hold), .test = test};
}

} // namespace catalog

namespace {
// 作用域的候选因子 factors/ (+ Search 日志, 在 run/agent/ 下同构位置)
Engine::FactorDir candidates(const Scope &sc, bool test) { return {.file = disk::AgentFactorFile, .log = disk::RunAgentSearchLog, .key = catalog::key(sc, test)}; }
} // namespace

// ---- Lab ----
struct Lab::Impl {
  // 作用域
  const SharedData *data = nullptr;
  Scope sc;
  bool test = false;
  Engine::FactorDir dir; // 候选目录
  Opened open;
  Context ctx;
  std::vector<Result> results;
  Fails fails; // 同作用域跨会话留着
  // 四道 (下标 = Step), 首次用到才建
  static_assert(static_cast<size_t>(Step::Search) == 0 && static_cast<size_t>(Step::Run) == 1 && static_cast<size_t>(Step::Lens) == 2 &&
                static_cast<size_t>(Step::Replay) == 3);
  bool gpu = false;
  std::tuple<Lane<Engine::SearchService>, Lane<Engine::RunService>, Lane<Engine::LensService>, Lane<ReplayService>> lanes;
  // 在做的那件事
  bool running = false;              // send 起, 到 End 入队
  bool halt = false;                 // 已 cancel: 在跑那步收尾即 End
  Step at = Step::End;               // 在跑那步的道; End = 无
  std::string file;                  // eval: 在评的文件
  Engine::FactorRow row;             // eval: 其首行 (Search 的模板)
  Surface surf;                      // eval: Search 落了盘时的曲面 (随定论交出)
  std::optional<Eval> graded;        // eval: 定论 (代表点观察完才交出)
  std::vector<std::string> labels;   // look: 因子的名字
  std::vector<std::string> feat_err; // look: 与 LookSpec::feats 对齐, 非空 = 该式不成 (没发)
  PerBar<bool> bars;                 // look: 取了的 K 线叶
  BookSpec spec;                     // book: 在放的那段
  std::deque<Tick> ticks;            // 未取走的步

  std::shared_ptr<const Engine::FeatureTable> ft() const { return table(*data, sc.level); }
  std::optional<Engine::FactorScope> fsc() const { return fscope_of(*data, ft(), sc.pool); }
  template <Step S>
  auto &lane() { return std::get<static_cast<size_t>(S)>(lanes); }
  // 在跑那步的道
  template <class Fn>
  decltype(auto) on_lane(Fn &&fn) const {
    switch (at) {
    case Step::Search:
      return fn(std::get<0>(lanes));
    case Step::Run:
      return fn(std::get<1>(lanes));
    case Step::Lens:
      return fn(std::get<2>(lanes));
    case Step::Replay:
      return fn(std::get<3>(lanes));
    case Step::End:
      break;
    }
    assert(false && "无在跑的道");
    __builtin_unreachable();
  }

  // ---- 进道 / 收尾 ----
  template <Step S, class Req>
  void enter(Req req) {
    lane<S>().send(std::move(req));
    at = S;
    ticks.push_back({S, {}});
  }
  void close(Got got = {}) {
    file.clear(), graded.reset();
    at = Step::End;
    running = false;
    ticks.push_back({Step::End, std::move(got)});
  }

  // ---- 三件事的起点 ----
  // 模板合法 → Search (定方向 + 搜参, 落了盘随后 Run); BROKEN → 直接评完
  void start(EvalSpec s) {
    gpu = s.gpu;
    file = std::move(s.file);
    std::vector<Engine::FactorRow> rows;
    Engine::ScanFactorFile(dir, file, *ft(), rows);
    assert(!rows.empty());
    row = std::move(rows.front());
    surf = {};
    if (searchable(row))
      return enter<Step::Search>(search_req());
    settle();
  }
  void start(LookSpec s) { enter<Step::Lens>(lens_req(std::move(s))); }
  void start(BookSpec s) { enter<Step::Replay>(replay_req(std::move(s))); }

  // ---- 各道的请求 ----
  Engine::SearchRequest search_req() const {
    Engine::SearchRequest req;
    [[maybe_unused]] const bool ok = Engine::MakeSearchRequest(*data, ft(), sc.pool, row, sc.hold, /*fresh=*/true, gpu, req);
    assert(ok && "作用域须 ready");
    req.scope.dir = dir;
    return req;
  }
  Engine::RunRequest run_req() const {
    Engine::RunRequest req;
    [[maybe_unused]] const bool ok = Engine::MakeRunRequest(*data, ft(), sc.pool, /*evaluate=*/true, gpu, req);
    assert(ok && "作用域须 ready");
    req.scope.dir = dir;
    req.spec.only_file = file;
    return req;
  }
  // 记下成文要的 labels / feat_err / bars
  Engine::LensRequest lens_req(LookSpec s) {
    assert(!s.factors.empty() || std::any_of(s.feats.begin(), s.feats.end(), [](const LookExpr &x) { return x.error.empty(); }) || s.kline);
    Engine::LensRequest req;
    [[maybe_unused]] const bool ok = Engine::MakeFactorScope(*data, ft(), sc.pool, /*gpu=*/false, req.scope);
    assert(ok && "作用域须 ready");
    req.spec.hold = sc.hold;
    labels.clear();
    for (const LookFactor &f : s.factors) {
      req.spec.roots.push_back({f.expr, f.ts ? factor::stat::Frame::TS : factor::stat::Frame::CS, factor::stat::dir_of(f.sign)});
      labels.push_back(f.label);
    }
    feat_err.clear();
    for (LookExpr &x : s.feats) {
      if (x.error.empty())
        req.spec.feats.push_back({std::move(x.root), factor::stat::Frame::CS, factor::stat::kFwd});
      feat_err.push_back(std::move(x.error));
    }
    bars = {};
    if (s.kline)
      for (const Bar b : kBars)
        if (const std::string &src = ctx.kline[b]; !src.empty()) {
          std::string root, error;
          [[maybe_unused]] const bool ok_leaf = feature_root(*ft(), src, root, error);
          assert(ok_leaf && "K 线叶必可作特征");
          req.spec.feats.push_back({std::move(root), factor::stat::Frame::CS, factor::stat::kFwd});
          bars[b] = true;
        }
    if (!s.code.empty()) {
      const size_t id = asset_axis().find(s.code);
      assert(id < asset_axis().size() && "码须经 code_of");
      req.spec.asset = static_cast<uint32_t>(id);
    }
    return req;
  }
  // 记下在放的那段
  ReplayRequest replay_req(BookSpec s) {
    assert(s.from < s.to && s.at >= s.from && s.at < s.to);
    const size_t id = asset_axis().find(s.code);
    assert(id < asset_axis().size() && id < data->asset.items.size() && "码须经 code_of");
    ReplayRequest req;
    req.orders = data->asset.catalog();
    assert(req.orders && "逐笔帧目录未扫描");
    req.asset = static_cast<uint32_t>(id);
    req.exchange = data->asset.items[id].exchange_type;
    req.date = s.date;
    req.s0 = trade_sec(static_cast<uint32_t>(s.from));
    req.s1 = trade_sec(static_cast<uint32_t>(s.to - 1)) + 1;
    req.at = trade_sec(static_cast<uint32_t>(s.at));
    req.big = static_cast<float>(s.big);
    spec = std::move(s);
    return req;
  }

  // ---- 各道跑完 ----
  // Search: 落了盘 → 取曲面, 进 Run; 中途放弃 → 记下原因 (该文件 BROKEN), 评完
  void searched() {
    Lane<Engine::SearchService> &l = lane<Step::Search>();
    if (const Engine::SearchResult r = l.take(); r.wrote) {
      fails.erase(file), surf = surface_of(*ft(), r);
      return enter<Step::Run>(run_req());
    }
    fails[file] = l.note();
    settle();
  }

  // 重扫一个文件, 结果表保序
  const Result &refresh(const std::string &f) {
    const auto find = [&] { return std::find_if(results.begin(), results.end(), [&](const Result &x) { return x.file == f; }); };
    Result r = result_of(*ft(), fsc(), open, dir, f, sc.hold, fails);
    if (const auto it = find(); it == results.end())
      results.push_back(std::move(r));
    else
      *it = std::move(r);
    sort_results(results);
    return *find();
  }
  // Run (或模板坏的不进道): 定论带上曲面; 无显著而有代表点 → 接着观察它, 观察完一并交出
  void settle() {
    Result r = refresh(file);
    r.surf = std::move(surf), surf = {};
    const bool rep = r.verdict == Verdict::Null && r.top && r.visible;
    graded = Eval{std::move(r), {}};
    if (!rep)
      return close(std::move(*graded));
    LookFactor f;
    std::string error;
    [[maybe_unused]] const bool ok = factor_of(graded->res.name, f, error);
    assert(ok && "代表点是 Search 评过的具体式");
    enter<Step::Lens>(lens_req({.factors = {std::move(f)}}));
  }
  // Look: 观察本身, 或在评那个的代表点
  void looked() {
    Look l = look_of(lane<Step::Lens>().take(), *ft(), labels, std::move(feat_err), bars, ctx.kline);
    if (!graded)
      return close(std::move(l));
    graded->rep = std::move(l);
    close(std::move(*graded));
  }
  // Book: 交易秒 → 钟面秒
  void booked() {
    ReplayResult r = lane<Step::Replay>().take();
    Book b{.spec = spec,
           .error = std::move(r.error),
           .orders = static_cast<int>(r.orders),
           .open = r.open,
           .high = r.high,
           .low = r.low,
           .close = r.close,
           .amount = r.amount,
           .big = r.big};
    const uint32_t s0 = trade_sec(static_cast<uint32_t>(spec.from));
    for (size_t i = 0; i < r.secs.size(); ++i) {
      const ReplaySec &x = r.secs[i];
      BookSec &y = b.secs.emplace_back();
      y.clock = static_cast<int>(trade_clock(s0 + static_cast<uint32_t>(i)));
      y.bid = x.bid, y.ask = x.ask, y.ref = x.ref, y.matched = x.matched, y.imbalance = x.imbalance, y.last = x.last;
      for (int s = 0; s < 2; ++s)
        y.depth[s] = x.depth[s], y.taker[s] = x.taker[s], y.maker[s] = x.maker[s], y.cancel[s] = x.cancel[s];
    }
    for (const ReplayEvent &e : r.events)
      b.events.push_back({L2::tick_hour(e.tick) * 3600 + L2::tick_minute(e.tick) * 60 + L2::tick_second(e.tick), L2::tick_millisecond(e.tick), static_cast<BookKind>(e.kind), e.side,
                          e.levels, e.price, e.first, e.amount, e.life});
    b.book = std::move(r.book);
    close(std::move(b));
  }
  // 在跑那步的道跑完一轮 → 下一步
  void pump() {
    if (at == Step::End || !on_lane([](const auto &l) { return l.finished(); }))
      return;
    if (halt || on_lane([](const auto &l) { return l.cancelled(); }))
      return close();
    switch (at) {
    case Step::Search:
      return searched();
    case Step::Run:
      return settle();
    case Step::Lens:
      return looked();
    case Step::Replay:
      return booked();
    case Step::End:
      break;
    }
  }

  bool factor_of(const std::string &arg, LookFactor &out, std::string &error) const {
    const auto it = std::find_if(results.begin(), results.end(), [&](const Result &r) { return r.name == arg; });
    std::string src = arg, label = arg;
    int sign = 1;
    if (it != results.end()) {
      if (!it->visible) {
        error = "候选 " + arg + " 对本会话不可见 (统计不在当前评估期上, 或用了未开放的叶 / 算子)";
        return false;
      }
      if (const Weight *w = it->best()) {
        src = w->expr, sign = w->sign;
      } else if (it->top) {
        src = it->top->expr, sign = it->top->sign, label = arg + " (无显著, 用代表点)";
      } else {
        error = "候选 " + arg + " 没有本持有期的权重或代表点 (" + def(it->verdict).cn + "); 可直接给具体根式";
        return false;
      }
    }
    const std::shared_ptr<const Engine::FeatureTable> t = ft();
    factor::expr::Expr e;
    factor::stat::Frame f;
    if (!concrete(*t, src, e, error) || !factor::expr::root_frame(e, t->seg, f, error) || !t->check_grid(e, error) || !(error = closed(e)).empty()) {
      error = (it != results.end() ? "" : "不是候选英文名, 按根式解析: ") + error;
      return false;
    }
    out = {e.canon, it != results.end() ? label : e.canon, f == factor::stat::Frame::TS, sign};
    return true;
  }

  bool parse(const std::string &src, fx::Expr &e) const {
    const std::shared_ptr<const Engine::FeatureTable> t = ft();
    std::string error;
    return fx::parse(src, t->lookup(), t->seg, e, error);
  }

  // 式里未开放的叶 / 算子 → 原因; 都开放 = 空
  std::string closed(const fx::Expr &e) const {
    const std::shared_ptr<const Engine::FeatureTable> t = ft();
    std::vector<std::string> seen;
    std::string leaves, ops;
    for (const fx::Node &x : e.nodes) {
      const std::string name = x.op >= 0 ? fx::kOps[x.op].name : x.feat;
      if (x.target || open.has(x) || std::find(seen.begin(), seen.end(), name) != seen.end())
        continue;
      seen.push_back(name);
      if (x.op >= 0) {
        ops += (ops.empty() ? "" : ", ") + name;
        continue;
      }
      const Src s = src_of(*t->x.find(x.feat));
      leaves += (leaves.empty() ? "" : ", ") + name + ((sc.srcs() & src_bit(s)) ? "" : " (" + std::string(def(s).name) + " 来源未开放)");
    }
    if (leaves.empty() && ops.empty())
      return {};
    return "用了本会话未开放的" + (leaves.empty() ? "" : " 叶: " + leaves + ";") + (ops.empty() ? "" : " 算子: " + ops + ";") + " 只能用系统提示特征表与算子表里列的";
  }
  // 模板里未开放的叶 / 算子 (解析不了 = 空, 由落文件报原因)
  std::string closed(const std::string &src) const {
    fx::Expr e;
    return parse(src, e) ? closed(e) : std::string();
  }
};

Lab::Lab() : p_(std::make_unique<Impl>()) {}
Lab::~Lab() = default;

void Lab::bind(const SharedData &data, const Scope &sc, bool test) {
  Impl &I = *p_;
  assert(!I.running && "评估队列忙");
  const bool same = sc.pool == I.sc.pool && sc.level == I.sc.level && sc.hold == I.sc.hold; // 来源与开放的叶 / 算子只定上下文与可见性, 不碰数据
  if (!same || test != I.test)
    I.fails.clear();
  I.data = &data;
  I.sc = sc;
  I.test = test;
  I.dir = candidates(sc, test);
  I.open = opened_of(*I.ft(), sc);
  I.ctx = context_of(data, sc, I.open);
  I.results = scan(*I.ft(), I.fsc(), I.open, I.dir, sc.hold, I.fails);
  I.ticks.clear();
}

const Scope &Lab::scope() const { return p_->sc; }
bool Lab::test() const { return p_->test; }
const Context &Lab::ctx() const { return p_->ctx; }
const std::vector<Result> &Lab::results() const { return p_->results; }

Staged Lab::stage(Cand c) {
  Impl &I = *p_;
  Staged s{.cand = std::move(c)};
  const Cand &x = s.cand;
  std::string error;
  if (!Engine::ValidFactorName(x.name))
    error = "英文名须非空, ≤ 64, 仅 [A-Za-z0-9_]";
  else if (!Engine::ValidFactorNameCn(x.name_cn))
    error = "中文名须 1..10 个汉字";
  else if (std::string cl = I.closed(x.expr); !cl.empty())
    error = std::move(cl);
  else if (const int n = ops_of(*I.ft(), x.expr); n > kMaxOps)
    error = "算子 " + std::to_string(n) + " 个, 超过上限 " + std::to_string(kMaxOps) + " (建议 " + std::to_string(kGoodOps) + " 个左右)";
  else if (const std::string tw = twin_of(*I.ft(), I.results, x.expr); !tw.empty())
    error = "与候选 " + tw + " 同式 (截面根 CsRank / CsNormRank / CsZ 互换也算同式), 回测结果必同";
  else
    s.file = Engine::AddFactorFile(I.dir, *I.ft(), x.name, x.name_cn, x.expr, x.note, error);
  if (s.ok())
    s.res = I.refresh(s.file);
  else
    s.res.error = std::move(error), s.res.verdict = Verdict::Unstaged;
  return s;
}

Adopted Lab::adopt(std::string name, const Gate &g) {
  const Impl &I = *p_;
  Adopted s{.name = std::move(name)};
  const auto it = std::find_if(I.results.begin(), I.results.end(), [&](const Result &r) { return r.name == s.name; });
  if (it == I.results.end() || !it->visible) {
    s.error = "会话目录里没有这个候选";
    return s;
  }
  s.error = GateMiss(g, *it);
  if (!s.ok())
    return s;
  const Engine::FactorDir to = Engine::factor_dir_of(I.sc.pool, level_id(I.sc.level));
  if (disk::exists(to.file, to.of(it->file))) {
    s.error = "正式目录已有 " + it->file;
    return s;
  }
  if (!I.test)
    disk::write(to.file, to.of(it->file), disk::read(I.dir.file, I.dir.of(it->file)));
  return s;
}

const Result *Lab::tried(const std::string &name) const {
  const std::vector<Result> &rs = p_->results;
  const auto it = std::find_if(rs.begin(), rs.end(), [&](const Result &r) { return r.visible && r.name == name; });
  return it == rs.end() ? nullptr : &*it;
}

Delta Lab::delta(const std::string &base, const std::string &expr) const {
  const Impl &I = *p_;
  fx::Expr n;
  if (!I.parse(expr, n))
    return {Change::Unparsed, "", base};
  const auto against = [&](const Result &r) {
    fx::Expr b;
    Delta d = I.parse(r.tmpl, b) ? classify(I.ctx.leaves, b, n) : Delta{Change::Unparsed, ""};
    d.base = r.name;
    return d;
  };
  if (!base.empty()) {
    const Result *r = tried(base);
    assert(r && "base 须是可见的已试候选");
    return against(*r);
  }
  for (const Result &r : I.results)
    if (r.visible && !r.tmpl.empty())
      if (Delta d = against(r); d.kind == Change::Same || d.kind == Change::Params || def(d.kind).knob)
        return d;
  return {};
}

std::vector<std::string> Lab::leaves_of(const std::string &expr) const {
  fx::Expr e;
  std::vector<std::string> out;
  if (p_->parse(expr, e))
    for (const fx::Node &x : e.nodes)
      if (x.op < 0 && std::find(out.begin(), out.end(), x.feat) == out.end())
        out.push_back(x.feat);
  return out;
}

std::string Lab::parts_miss(const std::string &expr, const std::vector<Part> &parts) const {
  const Engine::FeatureTable &ft = *p_->ft();
  fx::Expr e;
  if (!p_->parse(expr, e))
    return {};
  std::vector<std::string> subs;
  for (size_t i = 0; i < e.nodes.size(); ++i)
    subs.push_back(sub_of(e, static_cast<int>(i)));
  std::vector<std::string> have;
  for (const Part &p : parts) {
    fx::Expr q;
    std::string why;
    if (!fx::parse(p.expr, ft.lookup(), ft.seg, q, why))
      return "功能块 `" + p.expr + "` 解析不了: " + why;
    if (std::find(subs.begin(), subs.end(), q.canon) == subs.end())
      return "功能块 `" + p.expr + "` 不是模板里的子式 (从模板里逐字抄一段完整的子式, 自由参数照模板写裸键)";
    have.push_back(q.canon);
  }
  // 要说明的: 根的输入 (整个因子在衡量什么) + 每个多元算子的非叶元 (各成分各衡量什么)
  std::vector<int> need = {arity_of(e.root()) >= 1 ? e.root().args[0] : 0};
  for (const fx::Node &x : e.nodes)
    for (int a = 0; arity_of(x) >= 2 && a < arity_of(x); ++a)
      if (e.nodes[static_cast<size_t>(x.args[a])].op >= 0)
        need.push_back(x.args[a]);
  std::vector<std::string> want;
  for (const int i : need)
    if (std::find(want.begin(), want.end(), subs[static_cast<size_t>(i)]) == want.end())
      want.push_back(subs[static_cast<size_t>(i)]);
  std::string all;
  int miss = 0;
  for (const std::string &w : want) {
    const bool ok = std::find(have.begin(), have.end(), w) != have.end();
    miss += !ok;
    all += std::string("\n- ") + (ok ? "有" : "缺") + " `" + w + "`";
  }
  if (!miss)
    return {};
  return "功能块缺 " + std::to_string(miss) + " 块. 这个模板要说明的子式共 " + std::to_string(want.size()) +
         " 个 (根的输入 + 每个多元算子的每个非叶元, 叶相加的组合也算; 每个一块, 一句话说清它衡量什么; 另加的块不限):" + all;
}

bool Lab::factor_of(const std::string &arg, LookFactor &out, std::string &error) const { return p_->factor_of(arg, out, error); }

bool Lab::feature_of(const std::string &src, std::string &root, std::string &error) const {
  fx::Expr e;
  if (!concrete(*p_->ft(), src, e, error) || !(error = p_->closed(e)).empty())
    return false;
  return feature_root(*p_->ft(), src, root, error);
}

// 池轴 = 标签库的资产轴 (子池的成员都在其内): 不在轴上的必观察不成, 先拦; 在轴上而评估期内没进过子池的由观察结果的在池行数说
bool Lab::code_of(const std::string &arg, std::string &code, std::string &error) const {
  const Impl &I = *p_;
  const AssetAxis &ax = asset_axis();
  for (const char *ex : {"", ".SZ", ".SH", ".BJ"})
    if (const std::string c = arg + ex; ax.find(c) < ax.size()) {
      const std::optional<Engine::FactorScope> fsc = I.fsc();
      assert(fsc && "作用域须 ready");
      const PoolAxis &pa = fsc->libs[Engine::label_lib(pool_id(I.sc.pool))].axis;
      if (pa.sub_of(static_cast<uint32_t>(ax.find(c))) == pa.size()) {
        error = c + " 不在本池 (" + I.sc.pool + ") 特征库的资产轴上, 从没进过池; 挑池内的票 (如 view_trades / view_holders 结果里的代码)";
        return false;
      }
      code = c;
      return true;
    }
  error = "码表里没有 " + arg + " (写 6 位代码或 代码.交易所, 如 000001.SZ)";
  return false;
}

void Lab::send(Job job) {
  Impl &I = *p_;
  assert(!I.running && I.data && "评估忙或未 bind");
  I.running = true, I.halt = false;
  std::visit([&](auto &&s) { I.start(std::move(s)); }, std::move(job));
}

void Lab::cancel() {
  Impl &I = *p_;
  if (!I.running)
    return;
  I.halt = true;
  I.on_lane([](const auto &l) { l.cancel(); });
}

bool Lab::busy() const { return p_->running; }

std::optional<Lab::Tick> Lab::poll() {
  Impl &I = *p_;
  if (I.ticks.empty())
    I.pump();
  if (I.ticks.empty())
    return std::nullopt;
  Tick t = std::move(I.ticks.front());
  I.ticks.pop_front();
  return t;
}

void Lab::live(Live &l) const {
  const Impl &I = *p_;
  if (I.at == Step::End)
    l = {};
  else
    I.on_lane([&](const auto &x) { x.read(l); });
}

} // namespace GUI::Agent
