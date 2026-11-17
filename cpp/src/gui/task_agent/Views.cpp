// Views — 见头文件. 因子 CPU 后端与统计契约的消费者: CMake MATH_PRECISE 源.
#include "gui/task_agent/Views.hpp"
#include "gui/task_agent/Config.hpp"
#include "gui/task_agent/Text.hpp"

#include "factor/Expr.hpp"
#include "factor/Search/Space.hpp"
#include "factor/Stat/Host.hpp"
#include "features/OrderBook/LimitOrderBook.hpp"
#include "fundamental/AssetAxis.hpp"
#include "gui/engine/Engine.hpp"
#include "misc/profiler.hpp"
#include "orders/OrderFrame.hpp"
#include "orders/OrdersDay.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <cctype>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <map>
#include <numeric>
#include <random>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

namespace GUI::Agent {

struct ViewRunner::BookRes {
  L2::FrameDecoder decoder;
  LimitOrderBook lob;
  TickData tick_data;
  BookRes() : lob(L2::LOB_ORDER_CAPACITY) {}
};

ViewRunner::ViewRunner() = default;
ViewRunner::~ViewRunner() = default;

namespace {

namespace fx = factor::expr;
namespace st = factor::stat;
using factor::cpu::stat::h2f;
using text::bars;
using text::bp;
using text::chg;
using text::fmt;
using text::num;
using text::pct;
using text::r3;
using text::spark;

constexpr int kGroups = st::kGroups;
constexpr int kG1 = 0, kG20 = kGroups - 1;
constexpr int kTails = Engine::kTails;
constexpr double kNaN = NAN;
constexpr size_t kSample = 200000;
constexpr size_t kCsCells = 2000000;
constexpr int kCorrRows = 400;
constexpr size_t kBase = 40000;
constexpr int kMinCs = 20;
constexpr size_t kEvents = 20000;
constexpr int kExplainRows = 3000;
constexpr double kIntraNone = 0.005;
constexpr size_t kGistExpr = 40;

// ---- 格式与日期 ----
std::string pos2(double x) { return std::isfinite(x) ? fmt("%.2f", x) : "—"; }
std::string rk(double x) { return std::isfinite(x) ? fmt("%.0f", x * 100.0) : "—"; }
std::string str(size_t n) { return std::to_string(n); }
std::string gname(int k) { return "G" + std::to_string(k + 1); }

std::string date_of(int ymd) {
  char b[16];
  std::snprintf(b, sizeof(b), "%04d-%02d-%02d", ymd / 10000, ymd / 100 % 100, ymd % 100);
  return b;
}
int ymd_of(const std::string &s) {
  std::string d;
  for (const char ch : s) {
    if (ch == '-')
      continue;
    if (ch < '0' || ch > '9')
      return 0;
    d += ch;
  }
  if (d.size() != 8)
    return 0;
  const int v = std::stoi(d), m = v / 100 % 100, day = v % 100;
  return m >= 1 && m <= 12 && day >= 1 && day <= 31 ? v : 0;
}
int day_index(const std::vector<misc::Ymd> &dates, const std::string &arg, std::string &err) {
  const int ymd = ymd_of(arg);
  const auto it = std::find(dates.begin(), dates.end(), ymd);
  if (it == dates.end()) {
    err = "日期 " + arg + " 不是观察的日段 (" + date_of(dates.front()) + " ~ " + date_of(dates.back()) + ") 里的交易日";
    return -1;
  }
  return static_cast<int>(it - dates.begin());
}

float rank_of_r16(uint16_t rx) { return rx == st::kRankNone ? NAN : static_cast<float>(rx) / st::kRankMax; }
int grp_of_r16(uint16_t rx) { return rx == st::kRankNone ? -1 : st::grp_of(rx); }

// ---- 统计公共件 ----
double mean_of(const std::vector<double> &v) {
  double s = 0.0;
  size_t n = 0;
  for (const double x : v)
    if (std::isfinite(x))
      s += x, ++n;
  return n ? s / static_cast<double>(n) : kNaN;
}
double median_of(std::vector<double> v) {
  std::erase_if(v, [](double x) { return !std::isfinite(x); });
  if (v.empty())
    return kNaN;
  const auto mid = v.begin() + static_cast<std::ptrdiff_t>(v.size() / 2);
  std::nth_element(v.begin(), mid, v.end());
  return *mid;
}
double quant(const std::vector<double> &sorted, double p) {
  if (sorted.empty())
    return kNaN;
  const double x = p * static_cast<double>(sorted.size() - 1);
  const size_t i = static_cast<size_t>(x);
  return i + 1 < sorted.size() ? sorted[i] + (sorted[i + 1] - sorted[i]) * (x - static_cast<double>(i)) : sorted[i];
}
std::vector<double> ranks_of(const std::vector<double> &x) {
  std::vector<size_t> ix(x.size());
  std::iota(ix.begin(), ix.end(), size_t{0});
  std::sort(ix.begin(), ix.end(), [&](size_t a, size_t b) { return x[a] < x[b]; });
  std::vector<double> r(x.size());
  for (size_t i = 0; i < ix.size();) {
    size_t j = i;
    while (j + 1 < ix.size() && x[ix[j + 1]] == x[ix[i]])
      ++j;
    for (size_t k = i; k <= j; ++k)
      r[ix[k]] = 0.5 * static_cast<double>(i + j);
    i = j + 1;
  }
  return r;
}
double pearson(const std::vector<double> &a, const std::vector<double> &b) {
  const double ma = std::accumulate(a.begin(), a.end(), 0.0) / static_cast<double>(a.size());
  const double mb = std::accumulate(b.begin(), b.end(), 0.0) / static_cast<double>(b.size());
  double sab = 0.0, saa = 0.0, sbb = 0.0;
  for (size_t i = 0; i < a.size(); ++i)
    sab += (a[i] - ma) * (b[i] - mb), saa += (a[i] - ma) * (a[i] - ma), sbb += (b[i] - mb) * (b[i] - mb);
  return saa > 0.0 && sbb > 0.0 ? sab / std::sqrt(saa * sbb) : kNaN;
}
double spearman(const std::vector<double> &a, const std::vector<double> &b) { return a.size() >= 3 ? pearson(ranks_of(a), ranks_of(b)) : kNaN; }

struct Mean {
  double sum = 0.0, sq = 0.0;
  size_t n = 0, pos = 0;
  void add(double x) {
    if (!std::isfinite(x))
      return;
    sum += x, sq += x * x, ++n, pos += x > 0.0;
  }
  void merge(const Mean &o) { sum += o.sum, sq += o.sq, n += o.n, pos += o.pos; }
  double mean() const { return n ? sum / static_cast<double>(n) : kNaN; }
  double t() const {
    if (n < 2)
      return kNaN;
    const double m = mean(), var = (sq - static_cast<double>(n) * m * m) / static_cast<double>(n - 1);
    return var > 0.0 ? m / std::sqrt(var) * std::sqrt(static_cast<double>(n)) : kNaN;
  }
  double up() const { return n ? static_cast<double>(pos) / static_cast<double>(n) : kNaN; }
};
std::string wan(const Mean &m) { return m.n ? fmt("%.0f", m.sum / 1e4) : "—"; }

// ---- 日内时段 ----
struct Slot {
  int k0, k1;
  std::string name;
};
int part_rows(int seg, int open) { return std::max(1, (seg - open) / 8); }
std::vector<Slot> parts_of(int seg, int open, const std::vector<std::string> &clock) {
  if (seg == 1)
    return {{0, 1, "收盘"}};
  const int w = part_rows(seg, open);
  std::vector<Slot> p{{0, open, "09:15–09:30 竞价"}};
  for (int k = open; k < seg; k += w) {
    const int k1 = seg - (k + w) < w ? seg : k + w;
    const std::string &from = clock[static_cast<size_t>(k - 1)];
    p.push_back({k, k1, (from == "11:30" ? std::string("13:00") : from) + "–" + clock[static_cast<size_t>(k1 - 1)]});
    if (k1 == seg)
      break;
  }
  return p;
}
std::vector<int> part_of_row(const std::vector<Slot> &ps, int seg) {
  std::vector<int> o(static_cast<size_t>(seg));
  for (size_t i = 0; i < ps.size(); ++i)
    for (int k = ps[i].k0; k < ps[i].k1; ++k)
      o[static_cast<size_t>(k)] = static_cast<int>(i);
  return o;
}

bool hm_shape(const std::string &s, size_t at = 0) {
  return s.size() >= at + 5 && s[at + 2] == ':' && std::isdigit(static_cast<unsigned char>(s[at])) && std::isdigit(static_cast<unsigned char>(s[at + 1])) &&
         std::isdigit(static_cast<unsigned char>(s[at + 3])) && std::isdigit(static_cast<unsigned char>(s[at + 4]));
}
bool rows_in(const char *key, const std::string &s, const std::vector<std::string> &clock, int cap, int &k0, int &k1, std::string &error) {
  if (s.size() != 11 || !hm_shape(s) || s[5] != '-' || !hm_shape(s, 6)) {
    error = std::string(key) + ": 写成 HH:MM-HH:MM (如 10:00-10:30)";
    return false;
  }
  const std::string a = s.substr(0, 5), b = s.substr(6, 5);
  const int seg = static_cast<int>(clock.size());
  k0 = 0;
  while (k0 < seg && clock[static_cast<size_t>(k0)] < a)
    ++k0;
  k1 = k0;
  while (k1 < seg && clock[static_cast<size_t>(k1)] <= b)
    ++k1;
  if (k1 == k0) {
    error = std::string(key) + ": " + s + " 里没有行 (本层行末时刻 " + clock.front() + ", " + clock[1] + " … " + clock.back() + ")";
    return false;
  }
  if (cap > 0 && k1 - k0 > cap) {
    error = std::string(key) + ": " + s + " 有 " + std::to_string(k1 - k0) + " 行, 至多 " + std::to_string(cap) + " 行; 收窄时段";
    return false;
  }
  return true;
}
int row_at(const std::vector<std::string> &clock, const std::string &hm) {
  if (clock.size() == 1)
    return 0;
  const auto it = std::find(clock.begin(), clock.end(), hm);
  return it == clock.end() ? -1 : static_cast<int>(it - clock.begin());
}

// ---- 分层累加器 ----
struct Layer {
  int rows = 0, ok = 0;
  double g[kGroups] = {}, cnt[kGroups] = {};
  double tl[2][kTails] = {}, tc[2][kTails] = {};
  Mean ric;
  void add(const st::Row &w, const Engine::TailRow *tr = nullptr) {
    ++rows;
    if (!w.ok)
      return;
    ++ok;
    for (int k = 0; k < kGroups; ++k)
      g[k] += w.grp[k], cnt[k] += w.cnt[k];
    if (tr)
      for (int e = 0; e < 2; ++e)
        for (int j = 0; j < kTails; ++j)
          tl[e][j] += tr->sum[e][j], tc[e][j] += tr->cnt[e][j];
    ric.add(w.ric);
  }
  void merge(const Layer &o) {
    rows += o.rows, ok += o.ok;
    for (int k = 0; k < kGroups; ++k)
      g[k] += o.g[k], cnt[k] += o.cnt[k];
    for (int e = 0; e < 2; ++e)
      for (int j = 0; j < kTails; ++j)
        tl[e][j] += o.tl[e][j], tc[e][j] += o.tc[e][j];
    ric.merge(o.ric);
  }
  double grp(int k) const { return rows ? g[k] / rows : kNaN; }
  double tail(int e, int j) const { return rows ? tl[e][j] / rows : kNaN; }
  double tail_held(int e, int j) const { return ok ? tc[e][j] / ok : kNaN; }
  std::vector<double> grps() const {
    std::vector<double> v(kGroups);
    for (int k = 0; k < kGroups; ++k)
      v[static_cast<size_t>(k)] = grp(k);
    return v;
  }
  double mono() const { return ok ? st::mono_of(grps().data(), kGroups) : kNaN; }
  double ls() const { return (grp(kG20) - grp(kG1)) / 2.0; }
  double held(int k) const { return ok ? cnt[k] / ok : kNaN; }
};

bool approx_row(const std::vector<double> &x, const std::vector<double> &y, st::Row &w) {
  const size_t n = x.size();
  if (n < static_cast<size_t>(kMinCs))
    return false;
  const std::vector<double> rx = ranks_of(x);
  w = {};
  double s[kGroups] = {};
  for (size_t i = 0; i < n; ++i) {
    const int k = std::min(static_cast<int>(rx[i] * kGroups / static_cast<double>(n)), kG20);
    s[k] += y[i], ++w.cnt[k];
  }
  for (int k = 0; k < kGroups; ++k)
    w.grp[k] = static_cast<float>(w.cnt[k] ? s[k] / w.cnt[k] : 0.0);
  w.ric = static_cast<float>(pearson(rx, ranks_of(y)));
  w.ok = 1;
  return true;
}

void add_layer_row(text::Table &tbl, const std::string &name, const Layer &b) {
  tbl.add({name, std::to_string(b.rows), std::to_string(b.ok), r3(b.mono()), bp(b.ls()), r3(b.ric.mean()), bp(b.grp(kG1)), bp(b.grp(kGroups / 2 - 1)),
           bp(b.grp(kG20)), "`" + bars(b.grps()) + "`"});
}

// ---- K 线直读视图 (直接读 Block::x 的叶平面并按需做逆落盘变换, 零平面拷贝) ----
enum Bar : uint8_t { Close,
                     High,
                     Low,
                     Volume,
                     Amount,
                     PreClose,
                     LimUp,
                     LimDn,
                     kBarCount };
constexpr uint8_t bit(Bar b) { return static_cast<uint8_t>(1u << b); }
constexpr uint8_t kAllBars = 0xFFu;

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

struct BarMeta {
  struct Col {
    std::string code;
    Engine::LeafId id = Engine::kNoCol;
    ts::TfId tf = ts::TfId::None;
  } col[kBarCount];

  explicit BarMeta(const Engine::FeatureTable &ft) {
    const Engine::FactorLevel &l = Engine::factor_level(ft.lv);
    const auto find_leaf = [&](size_t lvl, const std::string &name) -> Col {
      for (size_t i = 0; i < ft.x.leaves.size(); ++i) {
        const Engine::FeatCol &c = ft.x.leaves[i];
        if (c.allowed && c.level == lvl && c.code.size() > name.size() && c.code.ends_with("." + name))
          return {c.code, static_cast<Engine::LeafId>(i), ft.feature->meta(c.tier).features[c.level][c.col].ts_tf};
      }
      return {};
    };
    const auto bar = [&](const char *f) {
      char b[64];
      std::snprintf(b, sizeof(b), l.kline, f);
      return find_leaf(l.lvl, b);
    };
    const auto flow = [&](const char *f) {
      Col c = bar(f);
      return c.code.empty() ? find_leaf(l.lvl, f) : c;
    };
    col[Close] = bar("close"), col[High] = bar("high"), col[Low] = bar("low");
    col[Volume] = flow("volume"), col[Amount] = flow("amount");
    col[PreClose] = find_leaf(L_1day, "pre_close"), col[LimUp] = find_leaf(L_1day, "lim_up"), col[LimDn] = find_leaf(L_1day, "lim_dn");
  }
};

struct Px {
  struct Binding {
    const float *v = nullptr;
    const uint8_t *m = nullptr;
    ts::TfId tf = ts::TfId::None;
  } b[kBarCount];
  int A = 0, seg = 1, open = 0;

  Px(const Engine::Block &blk, const BarMeta &bm, uint8_t mask, int open_) : A(blk.y.sh.A), seg(blk.y.sh.seg), open(open_) {
    for (uint8_t i = 0; i < kBarCount; ++i)
      if ((mask & (1u << i)) && bm.col[i].id != Engine::kNoCol)
        if (const Engine::Leaf *lf = blk.x.find(bm.col[i].id))
          b[i] = {lf->v.data(), lf->m.data(), bm.col[i].tf};
  }
  double operator()(Bar bar, int t, int a) const {
    const Binding &p = b[bar];
    if (!p.v)
      return kNaN;
    const size_t k = static_cast<size_t>(t) * static_cast<size_t>(A) + static_cast<size_t>(a);
    return p.m[k] ? static_cast<double>(raw_of(p.tf, p.v[k])) : kNaN;
  }
  bool traded(int t, int a) const { return !b[Volume].v || (*this)(Volume, t, a) > 0.0; }
  double pre(int t, int a) const {
    const double p = seg > 1 ? (*this)(PreClose, t, a) : t > 0 ? (*this)(Close, t - 1, a)
                                                               : kNaN;
    return p > 0.0 ? p : kNaN;
  }
};

// ---- 交割单与快照的盘面标签 (只看块内同日行 [t - t%seg, t], 恒在当前块内) ----
enum class Tag : uint8_t { Open,
                           Last,
                           NearUp,
                           NearDn,
                           Up,
                           Down,
                           High,
                           Low,
                           Surge };
struct TagDef {
  const char *cn, *brief;
};
constexpr TagDef kTags[] = {{"竞价段 (09:15–09:30 的行)", "竞价"},
                            {"尾盘最后一段", "尾盘"},
                            {"近涨停 (收 ≥ 涨停价 × 0.995)", "近涨停"},
                            {"近跌停 (收 ≤ 跌停价 × 1.005)", "近跌停"},
                            {"当日涨 > 5%", "大涨"},
                            {"当日跌 > 5%", "大跌"},
                            {"日内高位 (当日高低区间 ≥ 0.8)", "高位"},
                            {"日内低位 (≤ 0.2)", "低位"},
                            {"放量 (≥ 当日此前有成交行均量 3 倍)", "放量"}};
constexpr int kTagCount = static_cast<int>(std::size(kTags));

struct Mark {
  uint32_t m = 0;
  double chg = kNaN, pos = kNaN;
  void set(Tag t) { m |= 1u << static_cast<int>(t); }
};
Mark mark_of(const Px &px, int t, int a) {
  const int seg = px.seg, k = t % seg, t0 = t - k;
  Mark r;
  const double c = px(Close, t, a);
  r.chg = c / px.pre(t, a) - 1.0;
  if (r.chg > 0.05)
    r.set(Tag::Up);
  if (r.chg < -0.05)
    r.set(Tag::Down);
  if (seg == 1)
    return r;
  if (k < px.open)
    r.set(Tag::Open);
  if (k >= seg - part_rows(seg, px.open))
    r.set(Tag::Last);
  if (c >= px(LimUp, t, a) * 0.995)
    r.set(Tag::NearUp);
  if (c <= px(LimDn, t, a) * 1.005)
    r.set(Tag::NearDn);
  double hi = -INFINITY, lo = INFINITY, vs = 0.0;
  int vn = 0;
  for (int u = t0; u <= t; ++u) {
    if (px.traded(u, a))
      hi = std::fmax(hi, px(High, u, a)), lo = std::fmin(lo, px(Low, u, a));
    if (const double v = px(Volume, u, a); u < t && v > 0.0)
      vs += v, ++vn;
  }
  if (hi > lo && std::isfinite(c)) {
    r.pos = (c - lo) / (hi - lo);
    if (r.pos >= 0.8)
      r.set(Tag::High);
    if (r.pos <= 0.2)
      r.set(Tag::Low);
  }
  if (vn >= 3 && vs > 0.0 && px(Volume, t, a) >= 3.0 * vs / vn)
    r.set(Tag::Surge);
  return r;
}
std::string tags_of(uint32_t m) {
  std::string s;
  for (int j = 0; j < kTagCount; ++j)
    if (m >> j & 1u)
      s += (s.empty() ? "" : " ") + std::string(kTags[j].brief);
  return s;
}

// ---- 参数校验与解析 ----
std::string closed_err(const ViewEnv &env, const Engine::FeatureTable &ft, const fx::Expr &e) {
  std::vector<std::string> seen;
  std::string leaves, ops;
  for (const fx::Node &x : e.nodes) {
    const std::string name = x.op >= 0 ? fx::kOps[x.op].name : x.feat;
    const bool ok = x.target || (x.op >= 0 ? env.open_ops[static_cast<size_t>(x.op)] : env.open_leaves.contains(x.feat));
    if (ok || std::find(seen.begin(), seen.end(), name) != seen.end())
      continue;
    seen.push_back(name);
    if (x.op >= 0) {
      ops += (ops.empty() ? "" : ", ") + name;
    } else {
      const bool is_lob = ft.x.find(x.feat)->tier == TierId::Lob;
      leaves += (leaves.empty() ? "" : ", ") + name + (is_lob && !env.lob_open ? " (lob 来源未开放)" : "");
    }
  }
  if (leaves.empty() && ops.empty())
    return {};
  return "用了本会话未开放的" + (leaves.empty() ? "" : " 叶: " + leaves + ";") + (ops.empty() ? "" : " 算子: " + ops + ";") + " 只能用系统提示特征表与算子表里列的";
}

bool parse_concrete(const Engine::FeatureTable &ft, const std::string &src, fx::Expr &e, std::string &err) {
  if (!fx::parse(src, ft.lookup(), ft.seg, e, err))
    return false;
  if (e.is_template()) {
    err = "含自由参数 (裸 d / k / k2 / h / w): 观察只接具体式, 参数写成数字 (h 写持有期 token, w 写 [数字, …])";
    return false;
  }
  return true;
}

struct FactorSpec {
  fx::Expr e;
  st::Frame frame = st::Frame::CS;
  st::Dir dir = st::kFwd;
  std::string label;
  bool tails = false, ends = false;
  int only_end = -1; // -1 = 两端, 0 = kTop, 1 = kBot
};

bool resolve_factor(const std::string &arg, const ViewEnv &env, FactorSpec &out, std::string &err) {
  const Engine::FeatureTable &ft = *env.fsc.ft;
  std::string src = arg, label = arg;
  int sign = 1;
  const auto it = env.cands.find(arg);
  if (it != env.cands.end()) {
    if (!it->second.error.empty()) {
      err = "factor: " + it->second.error;
      return false;
    }
    src = it->second.expr, label = it->second.label, sign = it->second.sign;
  }
  if (!parse_concrete(ft, src, out.e, err) || !fx::root_frame(out.e, ft.seg, out.frame, err) || !ft.check_grid(out.e, err) ||
      !(err = closed_err(env, ft, out.e)).empty()) {
    err = std::string("factor: ") + (it != env.cands.end() ? "" : "不是候选英文名, 按根式解析: ") + err;
    return false;
  }
  out.dir = st::dir_of(sign);
  out.label = it != env.cands.end() ? label : out.e.canon;
  return true;
}

struct FeatSpec {
  std::string raw;
  fx::Expr e;
  std::string err;
  int root_idx = -1; // dag.roots 下标 (-1 = 解析失败没入森林)
};

bool resolve_feats(const std::vector<std::string> &xs, const ViewEnv &env, std::vector<FeatSpec> &out) {
  const Engine::FeatureTable &ft = *env.fsc.ft;
  bool any = false;
  for (const std::string &x : xs) {
    FeatSpec f{.raw = x};
    if (parse_concrete(ft, x, f.e, f.err))
      f.err = closed_err(env, ft, f.e);
    any |= f.err.empty();
    out.push_back(std::move(f));
  }
  return any;
}

bool resolve_code(const std::string &arg, const ViewEnv &env, std::string &code, uint32_t &asset_id, std::string &err) {
  const AssetAxis &ax = asset_axis();
  const PoolAxis &pa = env.fsc.libs[Engine::label_lib(pool_id(env.fsc.pool))].axis;
  for (const char *ex : {"", ".SZ", ".SH", ".BJ"}) {
    const std::string c = arg + ex;
    const size_t id = ax.find(c);
    if (id >= ax.size())
      continue;
    if (pa.sub_of(static_cast<uint32_t>(id)) == pa.size()) {
      err = "code: " + c + " 不在本池 (" + env.fsc.pool + ") 特征库的资产轴上, 从没进过池; 挑池内的票 (如 view_trades / view_holders 结果里的代码)";
      return false;
    }
    code = c, asset_id = static_cast<uint32_t>(id);
    return true;
  }
  err = "code: 码表里没有 " + arg + " (写 6 位代码或 代码.交易所, 如 000001.SZ)";
  return false;
}

bool date_arg_ok(const Call &c, const char *k, std::string &err) {
  if (const std::string v = c.str(k); !v.empty() && !ymd_of(v)) {
    err = std::string(k) + ": 日期写成 YYYY-MM-DD";
    return false;
  }
  return true;
}

// ---- 分块流式观察核: 装载前核日期 → 跳过无关块 → 块内全核 DAG + Stat → 原地归约 ----
struct HeldCell {
  int t = 0, a = 0; // t = 块内行, a = 池轴列
  uint8_t e = 0;    // st::kTop / kBot (已定向)
  float ret = 0.f;
};

struct ChunkView {
  const Engine::Job &job;
  const Engine::Block &blk;
  const BarMeta &bm;
  const std::vector<misc::Ymd> &dates; // stat_dates
  int u0 = 0, t_lo = 0, t_hi = 0;      // 本块统计行 [t_lo, t_hi) 对应全局统计行 u0 + (t - t_lo)
  const uint16_t *rx = nullptr;        // [blk.T][A] 已定向 (高 = 做多端)
  std::span<const st::Row> rows;       // [t_hi - t_lo] 已定向
  std::span<const Engine::TailRow> tails;
  std::span<const std::vector<HeldCell>> cells;    // [t_hi - t_lo]
  std::span<const factor::check::Plane *> feat_pl; // 与 feats 对齐 (坏式或解析失败为 nullptr)
};

// 一行逐行池内标签均值 mkt(t)
float row_mkt(const Engine::Block &blk, int t) {
  const int A = blk.y.sh.A;
  const size_t base = static_cast<size_t>(t) * static_cast<size_t>(A);
  const Engine::LegPlane &lb = *blk.y.labels[0];
  const uint16_t *lv = lb.v[st::kLong].data();
  double sum = 0.0;
  size_t cnt = 0;
  for (int a = 0; a < A; ++a)
    if (blk.y.gate[base + static_cast<size_t>(a)] && lb.m[base + static_cast<size_t>(a)])
      sum += h2f(lv[base + static_cast<size_t>(a)]), ++cnt;
  return cnt ? static_cast<float>(sum / static_cast<double>(cnt)) : NAN;
}

struct StreamCfg {
  const FactorSpec *fac = nullptr;
  std::vector<FeatSpec> *feats = nullptr;
  uint8_t bars = 0;
  int extra_front = 0, extra_back = 0;
  double extra_work = 0.0;
};

// 返回: Ok = 跑完 (body 由调用方填); Empty / Broken = 正常输入错误 (err 已填); Cancel / Stale = 中断
template <class RangeFn, class ChunkFn>
Engine::Rc run_stream(const ViewEnv &env, Engine::FactorFlow &F, const StreamCfg &cfg, std::vector<misc::Ymd> &stat_dates, Engine::Job &job,
                      std::string &err, RangeFn &&range_fn, ChunkFn &&chunk_fn) {
  TraceN("AgentViewStream");
  const Engine::FeatureTable &ft = *env.fsc.ft;
  const BarMeta bm(ft);
  std::vector<Engine::Root> fac_roots;
  std::vector<const fx::Expr *> plain_es;
  if (cfg.fac)
    fac_roots.push_back({&cfg.fac->e, cfg.fac->frame, cfg.fac->dir});
  if (cfg.feats)
    for (FeatSpec &f : *cfg.feats)
      if (f.err.empty()) {
        f.root_idx = static_cast<int>(fac_roots.size() + plain_es.size());
        plain_es.push_back(&f.e);
      }
  const bool has_dag = !fac_roots.empty() || !plain_es.empty();
  Engine::Forest fr = has_dag ? Engine::plant_forest(ft, fac_roots, /*keep_roots=*/true, plain_es) : Engine::Forest{};
  std::vector<Engine::LeafId> leaves = fr.leaves;
  for (uint8_t b = 0; b < kBarCount; ++b)
    if ((cfg.bars & (1u << b)) && bm.col[b].id != Engine::kNoCol && std::find(leaves.begin(), leaves.end(), bm.col[b].id) == leaves.end())
      leaves.push_back(bm.col[b].id);

  Engine::Lease lease(F);
  if (!lease)
    return Engine::Rc::Cancel;
  if (!Engine::open_job(F, env.fsc, leaves, job)) {
    err = "没算成: " + job.why;
    return Engine::Rc::Empty;
  }
  stat_dates = job.dm.stat_dates();
  size_t s_lo = 0, s_hi = stat_dates.size();
  if (!range_fn(stat_dates, s_lo, s_hi, err))
    return Engine::Rc::Empty;
  assert(s_lo < s_hi && s_hi <= stat_dates.size());

  const Engine::Holds hd = Engine::keys_of<Engine::Holds>(std::vector<int>{env.hold});
  job.spec.y = Engine::y_spec(ft, job.dm, hd, Engine::Segs::None, env.fsc.max_impact);
  job.spec.ry = cfg.fac != nullptr;
  int look = 0;
  if (cfg.fac)
    look = std::max(look, factor::search::space::lookback(cfg.fac->e, ft.seg));
  for (const fx::Expr *e : plain_es)
    look = std::max(look, factor::search::space::lookback(*e, ft.seg));
  job.bands = cfg.fac ? Engine::bands_of(look, hd, ft.seg) : Engine::Bands{(look + ft.seg - 1) / ft.seg, 0};
  job.bands.front = std::max(job.bands.front, cfg.extra_front);
  job.bands.back = std::max(job.bands.back, cfg.extra_back);
  job.b = Engine::budget_of<Engine::Backend<Engine::Cpu>, Engine::ProbeBase>(job, fr.d, job.spec.x.size(), 1, 0);
  job.b.work += cfg.extra_work;
  job.P = Engine::plan_chunks(job.dm, ft.seg, job.b, job.bands, F.S.budget());

  Engine::Backend<Engine::Cpu> &eng = Engine::engine_space(F.S).cpu;
  const size_t want_d0 = job.dm.warm + s_lo, want_d1 = job.dm.warm + s_hi;
  std::vector<std::string> dead(fr.d.roots.size());

  for (size_t ci = 0; ci < job.P.chunks.size(); ++ci) {
    const Engine::Chunk &ck = job.P.chunks[ci];
    if (ck.s1 <= want_d0 || ck.s0 >= want_d1)
      continue;
    Engine::Block blk;
    if (const Engine::Rc rc = Engine::load_block(F, job, ci, blk); rc != Engine::Rc::Ok) {
      if (rc == Engine::Rc::Stale)
        err = std::string("没算成: ") + Engine::kStaleMsg;
      return rc;
    }
    eng.bind(blk);
    const Engine::Shape &sh = blk.y.sh;
    if (has_dag) {
      std::vector<std::string> why = dead;
      std::vector<uint8_t> need;
      const int good = Engine::plan_forest(eng.ex(), blk, fr, why, need);
      dead = std::move(why);
      if (cfg.fac && !dead[0].empty()) {
        err = "因子没算成: " + dead[0];
        return Engine::Rc::Broken;
      }
      if (cfg.feats)
        for (FeatSpec &f : *cfg.feats)
          if (f.root_idx >= 0 && !dead[static_cast<size_t>(f.root_idx)].empty())
            f.err = dead[static_cast<size_t>(f.root_idx)];
      size_t n_ops = 0;
      for (size_t i = 0; i < fr.d.nodes.size(); ++i)
        n_ops += need[i] && fr.d.nodes[i].op >= 0;
      F.prog.begin(Phase::Running, n_ops + 1);
      if (good > 0 && !eng.walk_dag(F.cancel, fr.d, fr.leaves, need.data(), [&](int i, double) {
            if (fr.d.nodes[static_cast<size_t>(i)].op >= 0)
              F.prog.publish(1);
            return true;
          }))
        return Engine::Rc::Cancel;
    }

    const st::Span span = blk.span();
    const int t_lo = span.t_lo, t_hi = span.t_hi(), n_stat = t_hi - t_lo;
    std::vector<st::Row> rows;
    std::vector<Engine::TailRow> tails;
    std::vector<std::vector<HeldCell>> cells;
    uint16_t *rx = nullptr;

    if (cfg.fac) {
      rows.assign(static_cast<size_t>(n_stat), {});
      if (cfg.fac->tails)
        tails.assign(static_cast<size_t>(n_stat), {});
      if (cfg.fac->ends)
        cells.assign(static_cast<size_t>(n_stat), {});
      const st::Labels L = Engine::labels_of(blk.y);
      const uint8_t *m0 = L.l[0].m;
      const st::Dir dir = cfg.fac->dir;

      struct Ext {
        const uint8_t *m0;
        st::Dir d;
        bool want_tails, want_ends;
        int only_end;
        Engine::TailExt tl;
        std::vector<std::tuple<size_t, uint8_t, float>> got;
        void cell(const factor::cpu::stat::RxCell &cl) {
          if (want_tails)
            tl.cell(cl);
          if (!want_ends)
            return;
          const int g = st::grp_of(*cl.code);
          for (int e = 0; e < st::kEnds; ++e) {
            if ((only_end >= 0 && e != only_end) || g != st::end_group(e ^ d))
              continue;
            const auto in = [g](uint16_t v) { return v != st::kRankNone && st::grp_of(v) == g; };
            if (cl.held(in))
              got.emplace_back(static_cast<size_t>(cl.m - m0), static_cast<uint8_t>(e), cl.ret(cl.continues(in)));
          }
        }
      };
      struct Sink {
        const uint8_t *m0;
        double cap;
        st::Dir d;
        int t_lo, A;
        bool want_tails, want_ends;
        int only_end;
        std::vector<st::Row> &rows;
        std::vector<Engine::TailRow> &tails;
        std::vector<std::vector<HeldCell>> &cells;
        using Ext = Ext;
        Ext ext() const { return {m0, d, want_tails, want_ends, only_end, {Engine::kTailThr.v, cap, d}, {}}; }
        void row(int, int t, const st::Row &raw, const Ext &e) const {
          const size_t idx = static_cast<size_t>(t - t_lo), base = static_cast<size_t>(t) * static_cast<size_t>(A);
          const st::Row w = st::oriented(raw, d);
          rows[idx] = w;
          if (want_tails)
            e.tl.finish(w, tails[idx]);
          if (want_ends)
            for (const auto &[k, side, ret] : e.got) {
              assert(k >= base && k < base + static_cast<size_t>(A));
              cells[idx].push_back({t, static_cast<int>(k - base), side, ret});
            }
        }
      };
      const factor::check::Plane *rp = eng.plane(fr.d.roots[0]);
      st::Accum acc[st::kMaxHold];
      rx = eng.stat_run(rp, cfg.fac->frame, span, L, acc,
                        Sink{m0, L.cap, dir, t_lo, sh.A, cfg.fac->tails, cfg.fac->ends, cfg.fac->only_end, rows, tails, cells});
      if (dir == st::kRev)
        F.S.ex.rows(sh.T, [&](int r0, int r1, int) {
          for (size_t k = static_cast<size_t>(r0) * static_cast<size_t>(sh.A), end = static_cast<size_t>(r1) * static_cast<size_t>(sh.A); k < end; ++k)
            rx[k] = st::oriented(rx[k], st::kRev);
        });
    }

    std::vector<const factor::check::Plane *> feat_pl;
    if (cfg.feats)
      for (const FeatSpec &f : *cfg.feats)
        feat_pl.push_back(f.root_idx >= 0 && f.err.empty() ? eng.plane(fr.d.roots[static_cast<size_t>(f.root_idx)]) : nullptr);

    chunk_fn(ChunkView{job, blk, bm, stat_dates, blk.land0() + t_lo, t_lo, t_hi, rx, rows, tails, cells, feat_pl});
    F.prog.publish(1);
    if (F.cancel.load(std::memory_order_relaxed))
      return Engine::Rc::Cancel;
  }
  return Engine::Rc::Ok;
}

std::string period_note(const std::vector<misc::Ymd> &dates, const ViewEnv &env) {
  if (dates.front() == env.first && dates.back() == env.last)
    return {};
  return "注意: 本观察的日段为 " + date_of(dates.front()) + " ~ " + date_of(dates.back()) + " (" + str(dates.size()) +
         " 天), 与评估期不同 (用到的叶落盘日更短), 与别的观察不可直接比\n\n";
}

std::string expr_line(size_t i, const FeatSpec &f, const char *miss = "没算成") {
  return "E" + str(i + 1) + " = `" + f.raw + "`" + (f.err.empty() ? "" : std::string(" (") + miss + ": " + f.err + ")") + "\n";
}

// =============================================================================
// 1. view_feature
// =============================================================================
ViewOut feature_view(const Call &c, const ViewEnv &env, Engine::FactorFlow &F, ViewRunner &) {
  std::vector<FeatSpec> feats;
  if (!resolve_feats(c.strs("exprs"), env, feats)) {
    std::string err;
    for (size_t i = 0; i < feats.size(); ++i)
      err += (i ? "; " : "") + ("exprs[" + str(i) + "]: " + feats[i].err);
    return {false, std::move(err), {}};
  }
  const int seg = env.fsc.ft->seg;
  const std::vector<Slot> ps = parts_of(seg, env.open, env.clock);
  const std::vector<int> pr = part_of_row(ps, seg);
  const std::vector<std::pair<int, const char *>> lags =
      seg > 1 ? std::vector<std::pair<int, const char *>>{{1, "隔 1 行"}, {seg, "隔 1 日"}, {5 * seg, "隔 5 日"}}
              : std::vector<std::pair<int, const char *>>{{1, "隔 1 日"}, {5, "隔 5 日"}, {20, "隔 20 日"}};

  struct LagAcc {
    double xy = 0.0, x = 0.0, y = 0.0, xx = 0.0, yy = 0.0;
    size_t n = 0;
  };
  struct Acc {
    size_t in = 0, ok = 0;
    std::vector<double> smp;
    double lo = -INFINITY, hi = INFINITY;
    bool bounds_ready = false;
    double sx = 0.0, sxx = 0.0, sum_day_sq = 0.0;
    size_t n_val = 0;
    std::vector<double> sa;
    std::vector<size_t> na;
    std::array<std::vector<LagAcc>, 3> lag;
    std::vector<double> med;
    Mean ric;
    std::vector<Mean> ric_part;
  };
  const size_t F_n = feats.size();
  std::vector<Acc> accs(F_n);
  std::vector<Mean> pair_corr(F_n * F_n);

  std::vector<misc::Ymd> dates;
  Engine::Job job;
  std::string err;
  StreamCfg cfg{.feats = &feats, .extra_front = seg > 1 ? 5 : 20};
  const Engine::Rc rc = run_stream(
      env, F, cfg, dates, job, err,
      [&](const std::vector<misc::Ymd> &, size_t &, size_t &, std::string &) { return true; },
      [&](const ChunkView &cv) {
        const int A = cv.blk.y.sh.A, n_stat = cv.t_hi - cv.t_lo, days_stat = n_stat / seg;
        const int total_T = static_cast<int>(cv.dates.size()) * seg;
        const size_t total_N = static_cast<size_t>(total_T) * static_cast<size_t>(A);
        const size_t st_smp = std::max<size_t>(1, total_N / kSample);
        const int rstep = std::max(1, static_cast<int>(total_N / kCsCells));
        const int cstep = std::max(1, total_T / kCorrRows);
        const Engine::LegPlane &lb = *cv.blk.y.labels[0];
        const uint16_t *lv = lb.v[st::kLong].data();
        const int nt = F.S.ex.threads();

        for (size_t i = 0; i < F_n; ++i) {
          const factor::check::Plane *pl = cv.feat_pl[i];
          if (!pl)
            continue;
          Acc &ac = accs[i];
          if (ac.sa.empty()) {
            ac.sa.assign(static_cast<size_t>(A), 0.0);
            ac.na.assign(static_cast<size_t>(A), 0);
            for (auto &lg : ac.lag)
              lg.assign(static_cast<size_t>(A), {});
            ac.med.assign(static_cast<size_t>(total_T), kNaN);
            ac.ric_part.resize(ps.size());
          }
          // 1) 池内有效格计数与等距抽样 (首块即定 1%~99% 截尾界)
          const size_t base0 = static_cast<size_t>(cv.t_lo) * static_cast<size_t>(A), end0 = static_cast<size_t>(cv.t_hi) * static_cast<size_t>(A);
          for (size_t k = base0; k < end0; ++k)
            if (cv.blk.y.gate[k]) {
              ++ac.in;
              if (pl->m[k] && std::isfinite(pl->v[k]) && ac.ok++ % st_smp == 0)
                ac.smp.push_back(pl->v[k]);
            }
          if (!ac.bounds_ready && ac.smp.size() >= 20) {
            std::vector<double> tmp = ac.smp;
            std::sort(tmp.begin(), tmp.end());
            ac.lo = quant(tmp, 0.01), ac.hi = quant(tmp, 0.99);
            ac.bounds_ready = true;
          }
          if (!ac.bounds_ready)
            continue;

          // 2) 按资产列全核并行: 单趟攒方差分解 (ANOVA 恒等式) + 三档滞后自相关
          std::vector<double> t_sx(static_cast<size_t>(nt), 0.0), t_sxx(static_cast<size_t>(nt), 0.0), t_dsq(static_cast<size_t>(nt), 0.0);
          std::vector<size_t> t_n(static_cast<size_t>(nt), 0);
          const auto clamp_v = [&](float v) { return std::clamp(static_cast<double>(v), ac.lo, ac.hi); };
          F.S.ex.rows(A, [&](int a0, int a1, int tid) {
            double lsx = 0.0, lsxx = 0.0, ldsq = 0.0;
            size_t ln = 0;
            for (int a = a0; a < a1; ++a) {
              const size_t ua = static_cast<size_t>(a);
              for (int d = 0; d < days_stat; ++d) {
                double ds = 0.0;
                int dn = 0;
                for (int r = 0; r < seg; ++r) {
                  const int t = cv.t_lo + d * seg + r, u = cv.u0 + d * seg + r;
                  const size_t k1 = static_cast<size_t>(t) * static_cast<size_t>(A) + ua;
                  if (!cv.blk.y.gate[k1] || !pl->m[k1] || !std::isfinite(pl->v[k1]))
                    continue;
                  const double x = clamp_v(pl->v[k1]);
                  ac.sa[ua] += x, ++ac.na[ua];
                  ds += x, ++dn;
                  lsx += x, lsxx += x * x, ++ln;
                  for (size_t li = 0; li < lags.size(); ++li) {
                    const int lag = lags[li].first;
                    if (u < lag)
                      continue;
                    const size_t k0 = static_cast<size_t>(t - lag) * static_cast<size_t>(A) + ua;
                    if (!cv.blk.y.gate[k0] || !pl->m[k0] || !std::isfinite(pl->v[k0]))
                      continue;
                    const double y = clamp_v(pl->v[k0]);
                    LagAcc &la = ac.lag[li][ua];
                    la.xy += x * y, la.x += x, la.y += y, la.xx += x * x, la.yy += y * y, ++la.n;
                  }
                }
                if (dn > 0)
                  ldsq += ds * ds / dn;
              }
            }
            const size_t ut = static_cast<size_t>(tid);
            t_sx[ut] += lsx, t_sxx[ut] += lsxx, t_dsq[ut] += ldsq, t_n[ut] += ln;
          });
          for (int tid = 0; tid < nt; ++tid) {
            const size_t ut = static_cast<size_t>(tid);
            ac.sx += t_sx[ut], ac.sxx += t_sxx[ut], ac.sum_day_sq += t_dsq[ut], ac.n_val += t_n[ut];
          }

          // 3) 按统计行全核并行: 截面中位数 + 抽样行 rIC
          std::vector<Mean> t_ric(static_cast<size_t>(nt));
          std::vector<std::vector<Mean>> t_part(static_cast<size_t>(nt), std::vector<Mean>(ps.size()));
          F.S.ex.rows(n_stat, [&](int r0, int r1, int tid) {
            std::vector<double> cs, xa, ya;
            const size_t ut = static_cast<size_t>(tid);
            for (int r = r0; r < r1; ++r) {
              const int t = cv.t_lo + r, u = cv.u0 + r;
              const size_t base = static_cast<size_t>(t) * static_cast<size_t>(A);
              cs.clear();
              for (int a = 0; a < A; ++a)
                if (const size_t k = base + static_cast<size_t>(a); cv.blk.y.gate[k] && pl->m[k] && std::isfinite(pl->v[k]))
                  cs.push_back(pl->v[k]);
              if (cs.size() >= 5)
                ac.med[static_cast<size_t>(u)] = median_of(std::move(cs));
              if (u % rstep != 0)
                continue;
              xa.clear(), ya.clear();
              for (int a = 0; a < A; ++a)
                if (const size_t k = base + static_cast<size_t>(a); cv.blk.y.gate[k] && pl->m[k] && std::isfinite(pl->v[k]) && lb.m[k])
                  xa.push_back(pl->v[k]), ya.push_back(h2f(lv[k]));
              if (xa.size() >= static_cast<size_t>(kMinCs)) {
                const double rho = spearman(xa, ya);
                t_ric[ut].add(rho);
                t_part[ut][static_cast<size_t>(pr[static_cast<size_t>(u % seg)])].add(rho);
              }
            }
          });
          for (int tid = 0; tid < nt; ++tid) {
            ac.ric.merge(t_ric[static_cast<size_t>(tid)]);
            for (size_t j = 0; j < ps.size(); ++j)
              ac.ric_part[j].merge(t_part[static_cast<size_t>(tid)][j]);
          }
        }

        // 4) 两两截面相关 (抽样行全核并行)
        std::vector<std::vector<Mean>> t_corr(static_cast<size_t>(nt), std::vector<Mean>(F_n * F_n));
        F.S.ex.rows(n_stat, [&](int r0, int r1, int tid) {
          std::vector<double> xa, ya;
          std::vector<Mean> &mc = t_corr[static_cast<size_t>(tid)];
          for (int r = r0; r < r1; ++r) {
            const int u = cv.u0 + r;
            if (u % cstep != 0)
              continue;
            const size_t base = static_cast<size_t>(cv.t_lo + r) * static_cast<size_t>(A);
            for (size_t i = 0; i < F_n; ++i)
              for (size_t j = i + 1; j < F_n; ++j) {
                const factor::check::Plane *px = cv.feat_pl[i], *py = cv.feat_pl[j];
                if (!px || !py)
                  continue;
                xa.clear(), ya.clear();
                for (int a = 0; a < A; ++a)
                  if (const size_t k = base + static_cast<size_t>(a);
                      cv.blk.y.gate[k] && px->m[k] && py->m[k] && std::isfinite(px->v[k]) && std::isfinite(py->v[k]))
                    xa.push_back(px->v[k]), ya.push_back(py->v[k]);
                if (xa.size() >= static_cast<size_t>(kMinCs))
                  mc[i * F_n + j].add(spearman(xa, ya));
              }
          }
        });
        for (int tid = 0; tid < nt; ++tid)
          for (size_t idx = 0; idx < F_n * F_n; ++idx)
            pair_corr[idx].merge(t_corr[static_cast<size_t>(tid)][idx]);
      });

  if (rc != Engine::Rc::Ok)
    return {false, std::move(err), {}};

  const int D = static_cast<int>(dates.size());
  std::string s = period_note(dates, env), gist;
  std::vector<size_t> good;
  for (size_t i = 0; i < F_n; ++i) {
    s += "## E" + str(i + 1) + " `" + feats[i].raw + "`\n";
    if (!feats[i].err.empty()) {
      s += "没算成: " + feats[i].err + "\n\n";
      continue;
    }
    Acc &ac = accs[i];
    if (ac.ok < 100) {
      s += "池内有效格太少 (" + str(ac.ok) + " / " + str(ac.in) + ")\n\n";
      continue;
    }
    good.push_back(i);
    std::sort(ac.smp.begin(), ac.smp.end());
    Mean mv;
    for (const double x : ac.smp)
      mv.add(x);
    const double sd = mv.n > 1 ? std::sqrt(std::max(0.0, (mv.sq - static_cast<double>(mv.n) * mv.mean() * mv.mean()) / static_cast<double>(mv.n - 1))) : kNaN;
    s += "池内有效 " + pct(static_cast<double>(ac.ok) / static_cast<double>(ac.in)) + " (" + str(ac.ok) + " 格) · 均值 " + num(mv.mean()) + " · 标准差 " + num(sd) + "\n";
    s += "分位 1/5/25/50/75/95/99%:";
    for (const double p : {0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99})
      s += " " + num(quant(ac.smp, p));
    s += "\n";
    std::vector<double> u = ac.smp;
    u.erase(std::unique(u.begin(), u.end()), u.end());
    if (u.size() <= 20)
      s += "离散: 抽样里只有 " + str(u.size()) + " 种取值\n";
    if (const double z = static_cast<double>(std::count(ac.smp.begin(), ac.smp.end(), 0.0)) / static_cast<double>(ac.smp.size()); z >= 0.05)
      s += "零值占 " + pct(z) + "\n";

    const double mu = ac.sx / static_cast<double>(ac.n_val);
    const double tot = ac.sxx - static_cast<double>(ac.n_val) * mu * mu;
    double ssa = 0.0, sum_asset_sq = 0.0;
    for (size_t a = 0; a < ac.sa.size(); ++a)
      if (ac.na[a]) {
        const double ma = ac.sa[a] / static_cast<double>(ac.na[a]);
        ssa += static_cast<double>(ac.na[a]) * (ma - mu) * (ma - mu);
        sum_asset_sq += ac.sa[a] * ma;
      }
    const double ssd = std::max(0.0, ac.sum_day_sq - sum_asset_sq);
    const double intra = tot > 0.0 ? std::max(0.0, tot - ssa - ssd) / tot : 0.0;
    if (tot > 0.0)
      s += "方差来自 (截到 1~99% 分位): 资产间 " + pct(ssa / tot) + " · 日间 (同一资产) " + pct(ssd / tot) + (seg > 1 ? " · 日内 " + pct(intra) : std::string()) + "\n";

    double day1 = kNaN;
    s += "持续性 (去资产均值, 同一资产自相关):";
    for (size_t li = 0; li < lags.size(); ++li) {
      double xy = 0.0, xx = 0.0, yy = 0.0;
      for (size_t a = 0; a < ac.sa.size(); ++a) {
        const LagAcc &la = ac.lag[li][a];
        if (!ac.na[a] || !la.n)
          continue;
        const double ma = ac.sa[a] / static_cast<double>(ac.na[a]), n = static_cast<double>(la.n);
        xy += la.xy - ma * (la.x + la.y) + n * ma * ma;
        xx += la.xx - 2.0 * ma * la.x + n * ma * ma;
        yy += la.yy - 2.0 * ma * la.y + n * ma * ma;
      }
      const double rho = xx > 0.0 && yy > 0.0 ? xy / std::sqrt(xx * yy) : kNaN;
      if (lags[li].first == seg)
        day1 = rho;
      s += std::string(" ") + lags[li].second + " " + r3(rho);
    }
    s += "\n";

    if (seg > 1 && intra < kIntraNone) {
      s += "日内轮廓: 日内方差不到 " + fmt("%g%%", kIntraNone * 100.0) + " (日频慢变量), 不列\n";
    } else if (seg > 1) {
      std::vector<double> prof(static_cast<size_t>(seg)), col(static_cast<size_t>(D));
      for (int k = 0; k < seg; ++k) {
        for (int d = 0; d < D; ++d)
          col[static_cast<size_t>(d)] = ac.med[static_cast<size_t>(d * seg + k)];
        prof[static_cast<size_t>(k)] = median_of(col);
      }
      s += "日内轮廓 (各行截面中位数, 取日间中位数): " + spark(prof) + "\n  ";
      for (const Slot &p : ps)
        s += " " + env.clock[static_cast<size_t>(p.k1 - 1)] + " " + num(prof[static_cast<size_t>(p.k1 - 1)]);
      s += "\n";
    } else {
      s += "截面中位数逐日: " + spark(ac.med) + "\n";
    }
    s += "与本持有期收益的截面 rIC (原向): 均值 " + r3(ac.ric.mean()) + " · t " + fmt("%.1f", ac.ric.t()) + " (行间重叠未折算, 偏大) · 正 rIC 行 " + pct(ac.ric.up()) + "\n";
    gist += (gist.empty() ? "" : "; ") + text::head(feats[i].raw, kGistExpr) + " rIC " + r3(ac.ric.mean()) + " · 隔 1 日自相关 " + r3(day1);
    if (seg > 1) {
      s += "  分时段:";
      for (size_t j = 0; j < ps.size(); ++j)
        s += " " + ps[j].name + " " + r3(ac.ric_part[j].mean());
      s += "\n";
    }
    s += "\n";
  }

  if (good.size() >= 2) {
    std::vector<std::string> hs{""};
    for (const size_t idx : good)
      hs.push_back("E" + str(idx + 1));
    text::Table tbl(hs);
    for (size_t i = 0; i < good.size(); ++i) {
      std::vector<std::string> row{"E" + str(good[i] + 1)};
      for (size_t j = 0; j < good.size(); ++j)
        row.push_back(j == i ? "1" : r3(pair_corr[std::min(good[i], good[j]) * F_n + std::max(good[i], good[j])].mean()));
      tbl.add(row);
    }
    s += "## 两两截面秩相关 (逐行 Spearman 的均值)\n" + tbl.str();
  }
  return {true, std::move(s), std::move(gist)};
}

// =============================================================================
// 2. view_layers
// =============================================================================
ViewOut layers_view(const Call &c, const ViewEnv &env, Engine::FactorFlow &F, ViewRunner &) {
  std::string err;
  FactorSpec fac;
  fac.tails = true;
  if (!resolve_factor(c.str("factor"), env, fac, err))
    return {false, std::move(err), {}};
  const std::string split = c.str("split"), cond = c.str("cond");
  std::vector<FeatSpec> feats;
  if (!cond.empty()) {
    resolve_feats({cond}, env, feats);
    if (!feats[0].err.empty())
      return {false, "cond: " + feats[0].err, {}};
  }
  const int seg = env.fsc.ft->seg;
  const std::vector<Slot> ps = parts_of(seg, env.open, env.clock);
  const std::vector<int> pr = part_of_row(ps, seg);

  Layer all;
  std::vector<Layer> by_tod(ps.size());
  std::map<int, Layer> by_month;
  std::array<Layer, 4> cond_b;
  Mean cond_rho, cond_ric;

  std::vector<misc::Ymd> dates;
  Engine::Job job;
  StreamCfg cfg{.fac = &fac, .feats = feats.empty() ? nullptr : &feats};
  const Engine::Rc rc = run_stream(
      env, F, cfg, dates, job, err,
      [&](const std::vector<misc::Ymd> &, size_t &, size_t &, std::string &) { return true; },
      [&](const ChunkView &cv) {
        const int n_stat = cv.t_hi - cv.t_lo;
        for (int r = 0; r < n_stat; ++r) {
          const int u = cv.u0 + r;
          all.add(cv.rows[static_cast<size_t>(r)], &cv.tails[static_cast<size_t>(r)]);
          if (split == "tod" && seg > 1)
            by_tod[static_cast<size_t>(pr[static_cast<size_t>(u % seg)])].add(cv.rows[static_cast<size_t>(r)]);
          else if (split == "month")
            by_month[cv.dates[static_cast<size_t>(u / seg)] / 100].add(cv.rows[static_cast<size_t>(r)]);
        }
        if (cond.empty() || !cv.feat_pl[0])
          return;
        const int A = cv.blk.y.sh.A, total_T = static_cast<int>(cv.dates.size()) * seg;
        const int rstep = std::max(1, static_cast<int>(static_cast<size_t>(total_T) * static_cast<size_t>(A) / kCsCells));
        const factor::check::Plane *cp = cv.feat_pl[0];
        const Engine::LegPlane &lb = *cv.blk.y.labels[0];
        const uint16_t *lv = lb.v[st::kLong].data();
        const int nt = F.S.ex.threads();
        std::vector<std::array<Layer, 4>> tb(static_cast<size_t>(nt));
        std::vector<Mean> trho(static_cast<size_t>(nt)), tric(static_cast<size_t>(nt));

        F.S.ex.rows(n_stat, [&](int r0, int r1, int tid) {
          std::vector<double> cval, fxv, ey;
          std::array<std::vector<double>, 3> px, py;
          st::Row w;
          const size_t ut = static_cast<size_t>(tid);
          for (int r = r0; r < r1; ++r) {
            const int t = cv.t_lo + r, u = cv.u0 + r;
            if (u % rstep != 0)
              continue;
            const float mkt = row_mkt(cv.blk, t);
            if (!std::isfinite(mkt))
              continue;
            const size_t base = static_cast<size_t>(t) * static_cast<size_t>(A);
            cval.clear(), fxv.clear(), ey.clear();
            for (int a = 0; a < A; ++a) {
              const size_t k = base + static_cast<size_t>(a);
              if (cv.blk.y.gate[k] && cv.rx[k] != st::kRankNone && lb.m[k] && cp->m[k] && std::isfinite(cp->v[k]))
                cval.push_back(cp->v[k]), fxv.push_back(rank_of_r16(cv.rx[k])), ey.push_back(static_cast<double>(h2f(lv[k])) - mkt);
            }
            const size_t n = cval.size();
            if (n < 3 * static_cast<size_t>(kMinCs))
              continue;
            const std::vector<double> rc_v = ranks_of(cval);
            trho[ut].add(pearson(rc_v, ranks_of(fxv))), tric[ut].add(pearson(rc_v, ranks_of(ey)));
            for (size_t j = 0; j < 3; ++j)
              px[j].clear(), py[j].clear();
            for (size_t i = 0; i < n; ++i) {
              const size_t j = std::min<size_t>(2, static_cast<size_t>(rc_v[i] * 3.0 / static_cast<double>(n)));
              px[j].push_back(fxv[i]), py[j].push_back(ey[i]);
            }
            if (approx_row(fxv, ey, w))
              tb[ut][0].add(w);
            for (size_t j = 0; j < 3; ++j)
              if (approx_row(px[j], py[j], w))
                tb[ut][j + 1].add(w);
          }
        });
        for (int tid = 0; tid < nt; ++tid) {
          const size_t ut = static_cast<size_t>(tid);
          cond_rho.merge(trho[ut]), cond_ric.merge(tric[ut]);
          for (size_t j = 0; j < 4; ++j)
            cond_b[j].merge(tb[ut][j]);
        }
      });

  if (rc != Engine::Rc::Ok)
    return {false, std::move(err), {}};

  std::string s = period_note(dates, env) + "## 因子 `" + fac.label + "` (组号已定向: G20 = 做多端)\n";
  s += "有统计行 " + std::to_string(all.ok) + " / " + std::to_string(all.rows) + " · mono " + r3(all.mono()) + " · 每期多空 " + bp(all.ls()) + "bp · rIC 均值 " +
       r3(all.ric.mean()) + " (t " + fmt("%.1f", all.ric.t()) + ", 正 " + pct(all.ric.up()) + ")\n";
  s += "20 组每期超额 G1 → G20: " + spark(all.grps(), bp) + " bp\n\n";

  std::vector<std::string> g_head{""}, g_ex{"每期超额 bp"}, g_cnt{"行均真持有只数"};
  double lo = INFINITY, hi = -INFINITY;
  for (int k = 0; k < kGroups; ++k) {
    g_head.push_back(gname(k));
    g_ex.push_back(bp(all.grp(k)));
    g_cnt.push_back(fmt("%.1f", all.held(k)));
    lo = std::min(lo, all.held(k)), hi = std::max(hi, all.held(k));
  }
  text::Table gtbl(g_head);
  gtbl.add(g_ex);
  if (hi - lo > 0.1 * hi)
    gtbl.add(g_cnt);
  s += gtbl.str();
  if (!(hi - lo > 0.1 * hi))
    s += "\n各组行均真持有 " + fmt("%.1f", lo) + "–" + fmt("%.1f", hi) + " 只\n";

  double mid = 0.0;
  for (int k = 5; k < 15; ++k)
    mid += all.grp(k) / 10.0;
  s += "\n做多端 G20 " + bp(all.grp(kG20)) + "bp · 最低组 G1 " + bp(all.grp(kG1)) + "bp · 中段 G6–G15 均值 " + bp(mid) + "bp\n";

  constexpr int kEnd = kTails - 1;
  std::string gist = "mono " + r3(all.mono()) + " · 多空 " + bp(all.ls()) + "bp · rIC " + r3(all.ric.mean()) + " · 组内最极端 " +
                     fmt("%g%%", Engine::kTailOfGroup[kEnd] * 100.0) + ": G20 " + bp(all.tail(0, kEnd)) + " / G1 " + bp(all.tail(1, kEnd)) + "bp";

  text::Table ttbl({"截取", "占截面", "G20 端 bp", "G20 行均只数", "G1 端 bp", "G1 行均只数", "多空 bp"});
  ttbl.add({"整组", fmt("%g%%", 100.0 / kGroups), bp(all.grp(kG20)), fmt("%.1f", all.held(kG20)), bp(all.grp(kG1)), fmt("%.1f", all.held(kG1)), bp(all.ls())});
  for (int j = 0; j < kTails; ++j)
    ttbl.add({"组内最极端 " + fmt("%g%%", Engine::kTailOfGroup[j] * 100.0), fmt("%g%%", Engine::kTailOfGroup[j] * 100.0 / kGroups), bp(all.tail(0, j)),
              fmt("%.1f", all.tail_held(0, j)), bp(all.tail(1, j)), fmt("%.1f", all.tail_held(1, j)), bp((all.tail(0, j) - all.tail(1, j)) / 2.0)});
  s += "\n两端组内按池内秩再截更极端的一截 (同一份真持有判据, 满仓按宽度折算, 每期超额口径同上):\n" + ttbl.str();

  if (split == "tod" && seg == 1) {
    s += "\n(日层每天一行, 没有日内时段可切)\n";
  } else if (split != "none") {
    text::Table stbl({split == "tod" ? "时段" : "月份", "行", "有统计", "mono", "多空 bp", "rIC", "G1 bp", "G10 bp", "G20 bp", "20 组"});
    if (split == "tod")
      for (size_t i = 0; i < ps.size(); ++i)
        add_layer_row(stbl, ps[i].name, by_tod[i]);
    else
      for (const auto &[m, b] : by_month)
        add_layer_row(stbl, date_of(m * 100 + 1).substr(0, 7), b);
    s += "\n" + stbl.str();
  }

  if (!cond.empty()) {
    s += "\n## 按条件三等分: `" + cond + "`\n";
    if (!feats[0].err.empty()) {
      s += "条件没算成: " + feats[0].err + "\n";
    } else if (!cond_b[0].ok) {
      s += "没有可用的行 (每行至少要 " + std::to_string(3 * kMinCs) + " 个在池且因子 / 标签 / 条件都有效的格)\n";
    } else {
      s += "条件与因子的截面秩相关 " + r3(cond_rho.mean()) + " · 条件自身 rIC " + r3(cond_ric.mean()) + "\n";
      s += "每行按条件值把池子截面三等分 (条件离散时并列落同一段, 三段不均), 段内按因子分位重新分 20 组; 组超额 = 组内 (标签 − 本行在池均值) 等权均值, 建不了仓的格不剔, "
           "与上面的真持有口径略有出入, 只作这四行之间对照\n\n";
      text::Table ctbl({"条件段", "行", "有统计", "mono", "多空 bp", "rIC", "G1 bp", "G10 bp", "G20 bp", "20 组"});
      add_layer_row(ctbl, "全体 (同口径)", cond_b[0]);
      add_layer_row(ctbl, "低 1/3", cond_b[1]);
      add_layer_row(ctbl, "中 1/3", cond_b[2]);
      add_layer_row(ctbl, "高 1/3", cond_b[3]);
      s += ctbl.str();
      gist += "; 按 " + text::head(cond, kGistExpr) + " 低/中/高 mono " + r3(cond_b[1].mono()) + "/" + r3(cond_b[2].mono()) + "/" + r3(cond_b[3].mono()) + " · 多空 " +
              bp(cond_b[1].ls()) + "/" + bp(cond_b[2].ls()) + "/" + bp(cond_b[3].ls()) + "bp";
    }
  }
  return {true, std::move(s), std::move(gist)};
}

// =============================================================================
// 3. view_trades
// =============================================================================
ViewOut trades_view(const Call &c, const ViewEnv &env, Engine::FactorFlow &F, ViewRunner &) {
  std::string err;
  if (!date_arg_ok(c, "from", err) || !date_arg_ok(c, "to", err))
    return {false, std::move(err), {}};
  const int seg = env.fsc.ft->seg;
  const std::string from = c.str("from"), to = c.str("to"), tod = c.str("tod");
  int k0 = 0, k1 = seg;
  if (!tod.empty()) {
    if (seg == 1)
      return {false, "tod: 日层每天只有一行, 没有日内时段可选", {}};
    if (!rows_in("tod", tod, env.clock, 0, k0, k1, err))
      return {false, std::move(err), {}};
  }
  const bool top = c.str("side") == "top";
  FactorSpec fac;
  fac.ends = true, fac.only_end = top ? st::kTop : st::kBot;
  if (!resolve_factor(c.str("factor"), env, fac, err))
    return {false, std::move(err), {}};

  const int ymd0 = from.empty() ? 0 : ymd_of(from), ymd1 = to.empty() ? 99999999 : ymd_of(to);
  const std::string merge = c.str("merge"), sort = c.str("sort");
  const std::string side_cn = top ? "做多端 G20" : "最低组 G1";

  struct CellRec {
    int a, u;
    float ex, rank;
    Mark mk;
  };
  std::vector<CellRec> cs;
  std::array<size_t, kTagCount> base_tags{};
  size_t nb = 0;
  int win_rows = 0;
  std::mt19937_64 rng(7);

  std::vector<misc::Ymd> dates;
  Engine::Job job;
  StreamCfg cfg{.fac = &fac, .bars = bit(Close) | bit(High) | bit(Low) | bit(Volume) | bit(PreClose) | bit(LimUp) | bit(LimDn), .extra_front = seg == 1 ? 1 : 0};
  const Engine::Rc rc = run_stream(
      env, F, cfg, dates, job, err,
      [&](const std::vector<misc::Ymd> &ds, size_t &lo, size_t &hi, std::string &why) {
        size_t a = ds.size(), b = 0;
        for (size_t d = 0; d < ds.size(); ++d)
          if (ds[d] >= ymd0 && ds[d] <= ymd1)
            a = std::min(a, d), b = d + 1;
        if (a >= b || k0 >= k1) {
          std::string win = (!from.empty() || !to.empty() ? (from.empty() ? "…" : from) + " ~ " + (to.empty() ? "…" : to) + " " : "") +
                            (!tod.empty() ? "行末 " + tod + " " : "");
          why = "## 因子 `" + fac.label + "` · " + side_cn + " · 合并 " + merge + " · 只看 " + win + "的行 (0 行)\n窗口里没有行 (观察的日段 " +
                date_of(ds.front()) + " ~ " + date_of(ds.back()) + ")";
          return false;
        }
        lo = a, hi = b;
        win_rows = static_cast<int>(b - a) * (k1 - k0);
        return true;
      },
      [&](const ChunkView &cv) {
        const int A = cv.blk.y.sh.A, n_stat = cv.t_hi - cv.t_lo;
        const Px px(cv.blk, cv.bm, cfg.bars, env.open);
        const Engine::LegPlane &lb = *cv.blk.y.labels[0];

        std::vector<std::pair<int, HeldCell>> flat;
        for (int r = 0; r < n_stat; ++r) {
          const int u = cv.u0 + r, ymd = cv.dates[static_cast<size_t>(u / seg)], k = u % seg;
          if (ymd < ymd0 || ymd > ymd1 || k < k0 || k >= k1)
            continue;
          for (const HeldCell &hc : cv.cells[static_cast<size_t>(r)])
            flat.push_back({r, hc});
        }
        const size_t base_idx = cs.size();
        cs.resize(base_idx + flat.size());
        F.S.ex.rows(static_cast<int>(flat.size()), [&](int i0, int i1, int) {
          for (int i = i0; i < i1; ++i) {
            const auto &[r, hc] = flat[static_cast<size_t>(i)];
            const size_t k = static_cast<size_t>(hc.t) * static_cast<size_t>(A) + static_cast<size_t>(hc.a);
            cs[base_idx + static_cast<size_t>(i)] = {hc.a, cv.u0 + r, hc.ret - cv.rows[static_cast<size_t>(r)].mkt, rank_of_r16(cv.rx[k]), mark_of(px, hc.t, hc.a)};
          }
        });

        // 池内基准抽样 (按本块所占统计行比例抽)
        const size_t want_b = std::max<size_t>(1, kBase * static_cast<size_t>(n_stat) / std::max<size_t>(1, cv.dates.size() * static_cast<size_t>(seg)));
        std::uniform_int_distribution<int> pick_r(0, n_stat - 1), pick_a(0, A - 1);
        for (size_t got = 0, tries = 0; got < want_b && nb < kBase && tries < want_b * 20; ++tries) {
          const int r = pick_r(rng), u = cv.u0 + r, ymd = cv.dates[static_cast<size_t>(u / seg)], kr = u % seg;
          if (ymd < ymd0 || ymd > ymd1 || kr < k0 || kr >= k1)
            continue;
          const int t = cv.t_lo + r, a = pick_a(rng);
          const size_t k = static_cast<size_t>(t) * static_cast<size_t>(A) + static_cast<size_t>(a);
          if (!cv.blk.y.gate[k] || !lb.m[k])
            continue;
          const Mark m = mark_of(px, t, a);
          ++got, ++nb;
          for (int j = 0; j < kTagCount; ++j)
            base_tags[static_cast<size_t>(j)] += m.m >> j & 1u;
        }
      });

  if (rc != Engine::Rc::Ok)
    return {false, std::move(err), {}};

  std::string win;
  if (!from.empty() || !to.empty())
    win += (from.empty() ? "…" : from) + " ~ " + (to.empty() ? "…" : to) + " ";
  if (!tod.empty())
    win += "行末 " + tod + " ";
  if (!win.empty())
    win = " · 只看 " + win + "的行 (" + std::to_string(win_rows) + " 行)";
  std::string s = period_note(dates, env) + "## 因子 `" + fac.label + "` · " + side_cn + " · 合并 " + merge + win + "\n";
  if (cs.empty())
    return {true, s + "本端没有真持有过的格\n", {}};

  std::sort(cs.begin(), cs.end(), [](const CellRec &x, const CellRec &y) { return x.a != y.a ? x.a < y.a : x.u < y.u; });
  struct Trade {
    int a, u0, u1, n;
    double ex;
    float rank;
    Mark mk;
  };
  std::vector<Trade> ts;
  Mean cell_m;
  for (const CellRec &x : cs) {
    cell_m.add(x.ex);
    Trade *b = ts.empty() ? nullptr : &ts.back();
    const bool join = b && b->a == x.a && b->u1 / seg == x.u / seg && (merge == "day" || (merge == "run" && x.u == b->u1 + 1));
    if (join)
      b->ex = (b->ex * b->n + x.ex) / (b->n + 1), b->u1 = x.u, ++b->n;
    else
      ts.push_back({x.a, x.u, x.u, 1, x.ex, x.rank, x.mk});
  }

  const size_t M = ts.size();
  std::vector<double> ex(M), contrib(M);
  Mean seg_ex;
  double gain = 0.0, loss = 0.0;
  for (size_t i = 0; i < M; ++i) {
    ex[i] = ts[i].ex, seg_ex.add(ex[i]);
    contrib[i] = ts[i].ex * ts[i].n;
    (contrib[i] > 0 ? gain : loss) += contrib[i];
  }
  std::vector<double> sx = ex;
  std::sort(sx.begin(), sx.end());
  std::sort(contrib.begin(), contrib.end());
  const size_t tenth = std::max<size_t>(1, M / 10);
  double worst = 0.0, best = 0.0;
  for (size_t i = 0; i < tenth; ++i)
    worst += std::min(0.0, contrib[i]), best += std::max(0.0, contrib[M - 1 - i]);
  s += "真持有 " + str(cs.size()) + " 格 (每行平均 " + fmt("%.1f", static_cast<double>(cs.size()) / win_rows) + " 只) → " + str(M) + " 段, 段均 " +
       fmt("%.1f", static_cast<double>(cs.size()) / M) + " 行\n";
  s += "格超额均值 " + bp(cell_m.mean()) + "bp (胜率 " + pct(cell_m.up()) + ") · 段超额: 均值 " + bp(seg_ex.mean()) + " · 中位 " + bp(quant(sx, 0.5)) + " · p10 " +
       bp(quant(sx, 0.1)) + " · p90 " + bp(quant(sx, 0.9)) + " · 胜率 " + pct(seg_ex.up()) + "\n";
  s += "集中度 (段贡献 = 段超额 × 行数): 最差 10% 的段占全部亏损 " + pct(loss < 0.0 ? worst / loss : kNaN) + " · 最好 10% 的段占全部盈利 " +
       pct(gain > 0.0 ? best / gain : kNaN) + "\n";

  if (seg > 1) {
    const std::vector<Slot> ps = parts_of(seg, env.open, env.clock);
    const std::vector<int> pr = part_of_row(ps, seg);
    std::vector<Mean> by(ps.size());
    for (size_t i = 0; i < M; ++i)
      by[static_cast<size_t>(pr[static_cast<size_t>(ts[i].u0 % seg)])].add(ex[i]);
    text::Table ptbl({"开仓时段", "段数", "占比", "段超额均值 bp", "胜率"});
    for (size_t j = 0; j < ps.size(); ++j)
      if (by[j].n)
        ptbl.add({ps[j].name, str(by[j].n), pct(static_cast<double>(by[j].n) / M), bp(by[j].mean()), pct(by[j].up())});
    s += "\n" + ptbl.str();
  }

  std::array<size_t, kTagCount> held{};
  size_t nh = 0;
  for (size_t i = 0, step = std::max<size_t>(1, cs.size() / kBase); i < cs.size(); i += step, ++nh)
    for (int j = 0; j < kTagCount; ++j)
      held[static_cast<size_t>(j)] += cs[i].mk.m >> j & 1u;

  text::Table ttbl({"开仓那行的盘面", "本端格占比", "池内格占比", "提升", "开仓带此标签的段超额 bp", "不带的 bp"});
  bool any_tag = false;
  for (int j = 0; j < kTagCount; ++j) {
    Mean with, without;
    for (size_t i = 0; i < M; ++i)
      (ts[i].mk.m >> j & 1u ? with : without).add(ex[i]);
    const double fs = static_cast<double>(held[static_cast<size_t>(j)]) / nh, fb = nb ? static_cast<double>(base_tags[static_cast<size_t>(j)]) / nb : kNaN;
    if (fs < 0.01 && !(fb >= 0.01))
      continue;
    any_tag = true;
    ttbl.add({kTags[j].cn, pct(fs), pct(fb), fb > 0.0 ? fmt("%.2f", fs / fb) : "—", bp(with.mean()), bp(without.mean())});
  }
  if (any_tag)
    s += "\n" + ttbl.str();

  std::vector<size_t> ix(M);
  std::iota(ix.begin(), ix.end(), size_t{0});
  if (sort == "random")
    std::shuffle(ix.begin(), ix.end(), std::mt19937_64(7));
  else
    std::stable_sort(ix.begin(), ix.end(), [&](size_t x, size_t y) { return sort == "worst" ? ex[x] < ex[y] : ex[x] > ex[y]; });
  ix.resize(std::min<size_t>(M, static_cast<size_t>(c.num("n"))));

  const AssetAxis &ax = asset_axis();
  const PoolAxis &pa = job.dm.rs.axis;
  text::Table dtbl({"#", "代码", "日期", "时刻", "行", "开仓分位", "段超额 bp", "开仓时当日涨跌", "开仓时日内位置", "盘面"});
  for (size_t q = 0; q < ix.size(); ++q) {
    const Trade &x = ts[ix[q]];
    const std::string &c0 = env.clock[static_cast<size_t>(x.u0 % seg)], &c1 = env.clock[static_cast<size_t>(x.u1 % seg)];
    dtbl.add({str(q + 1), ax.code(pa.global(static_cast<size_t>(x.a))), date_of(dates[static_cast<size_t>(x.u0 / seg)]), x.n > 1 ? c0 + "–" + c1 : c0,
              std::to_string(x.n), rk(x.rank), bp(x.ex), chg(x.mk.chg), pos2(x.mk.pos), tags_of(x.mk.m)});
  }
  s += "\n明细 (" + sort + (sort == "random" ? "" : "; 尾部个例, 多是个股事件或数据缺失, 不代表信号") + "):\n" + dtbl.str();
  return {true, std::move(s), {}};
}

// =============================================================================
// 4. view_holders
// =============================================================================
ViewOut holders_view(const Call &c, const ViewEnv &env, Engine::FactorFlow &F, ViewRunner &) {
  std::string err;
  const bool top = c.str("side") == "top";
  FactorSpec fac;
  fac.ends = true, fac.only_end = top ? st::kTop : st::kBot;
  if (!resolve_factor(c.str("factor"), env, fac, err))
    return {false, std::move(err), {}};

  const int g_want = top ? kG20 : kG1, seg = env.fsc.ft->seg;
  struct Who {
    int a = 0, in = 0, side = 0, days = 0, runs = 0;
    Mean ex;
  };
  std::vector<Who> ws;
  std::vector<int> last_day;

  std::vector<misc::Ymd> dates;
  Engine::Job job;
  StreamCfg cfg{.fac = &fac};
  const Engine::Rc rc = run_stream(
      env, F, cfg, dates, job, err,
      [&](const std::vector<misc::Ymd> &, size_t &, size_t &, std::string &) { return true; },
      [&](const ChunkView &cv) {
        const int A = cv.blk.y.sh.A, n_stat = cv.t_hi - cv.t_lo;
        if (ws.empty()) {
          ws.resize(static_cast<size_t>(A));
          for (int a = 0; a < A; ++a)
            ws[static_cast<size_t>(a)].a = a;
          last_day.assign(static_cast<size_t>(A), -1);
        }
        F.S.ex.rows(A, [&](int a0, int a1, int) {
          for (int a = a0; a < a1; ++a) {
            Who &w = ws[static_cast<size_t>(a)];
            int &ld = last_day[static_cast<size_t>(a)];
            for (int r = 0; r < n_stat; ++r) {
              const int t = cv.t_lo + r, u = cv.u0 + r;
              const size_t k = static_cast<size_t>(t) * static_cast<size_t>(A) + static_cast<size_t>(a);
              w.in += cv.blk.y.gate[k];
              if (grp_of_r16(cv.rx[k]) != g_want)
                continue;
              ++w.side;
              if (u / seg != ld)
                ++w.days, ld = u / seg;
              if (u % seg == 0 || grp_of_r16(cv.rx[static_cast<size_t>(t - 1) * static_cast<size_t>(A) + static_cast<size_t>(a)]) != g_want)
                ++w.runs;
            }
          }
        });
        for (int r = 0; r < n_stat; ++r) {
          const float mkt = cv.rows[static_cast<size_t>(r)].mkt;
          for (const HeldCell &hc : cv.cells[static_cast<size_t>(r)])
            ws[static_cast<size_t>(hc.a)].ex.add(static_cast<double>(hc.ret) - mkt);
        }
      });

  if (rc != Engine::Rc::Ok)
    return {false, std::move(err), {}};

  int active_A = 0;
  for (const Who &w : ws)
    active_A += w.in > 0;
  std::sort(ws.begin(), ws.end(), [](const Who &x, const Who &y) { return x.side != y.side ? x.side > y.side : x.a < y.a; });
  size_t total = 0, any = 0;
  for (const Who &w : ws)
    total += static_cast<size_t>(w.side), any += w.side > 0;
  const std::string side_cn = top ? "做多端 G20" : "最低组 G1";
  std::string s = period_note(dates, env) + "## 因子 `" + fac.label + "` · " + side_cn + " 的常客\n";
  if (!total)
    return {true, s + "本端没有行\n", {}};

  size_t acc = 0, half = 0, top10 = 0;
  for (size_t i = 0; i < ws.size(); ++i) {
    if (i < 10)
      top10 += static_cast<size_t>(ws[i].side);
    if (acc * 2 < total)
      acc += static_cast<size_t>(ws[i].side), ++half;
  }
  s += "本端共 " + str(total) + " 行 · 落过本端的资产 " + str(any) + " / " + std::to_string(active_A) + " · 前 10 只占 " + pct(static_cast<double>(top10) / total) +
       " · 凑满一半要 " + str(half) + " 只\n\n";

  const AssetAxis &ax = asset_axis();
  const PoolAxis &pa = job.dm.rs.axis;
  text::Table tbl({"代码", "本端行", "在池行", "占其在池", "天数", "平均连续行", "本端持仓格", "持仓格均超额 bp"});
  for (size_t i = 0; i < std::min<size_t>(ws.size(), static_cast<size_t>(c.num("n"))); ++i) {
    const Who &w = ws[i];
    if (!w.side)
      break;
    tbl.add({ax.code(pa.global(static_cast<size_t>(w.a))), std::to_string(w.side), std::to_string(w.in), pct(w.in ? static_cast<double>(w.side) / w.in : kNaN),
             std::to_string(w.days), fmt("%.1f", static_cast<double>(w.side) / w.runs), str(w.ex.n), bp(w.ex.mean())});
  }
  s += tbl.str();
  return {true, std::move(s), {}};
}

// =============================================================================
// 5. view_response
// =============================================================================
// 块内逐格超额对数步长 (F.S.ex 全核并行)
std::vector<float> block_excess_steps(misc::exec::ForkJoin &ex, const Engine::Block &blk, const Px &px, int u_blk0) {
  const int T = blk.y.sh.T, A = blk.y.sh.A, seg = blk.y.sh.seg;
  std::vector<float> e(static_cast<size_t>(T) * static_cast<size_t>(A));
  ex.rows(T, [&](int t0, int t1, int) {
    std::vector<double> stp(static_cast<size_t>(A));
    for (int t = t0; t < t1; ++t) {
      Mean m;
      const size_t base = static_cast<size_t>(t) * static_cast<size_t>(A);
      const int u = u_blk0 + t;
      for (int a = 0; a < A; ++a) {
        const double c = px(Close, t, a);
        const double b = t % seg == 0 ? (seg == 1 && u <= 0 ? kNaN : px.pre(t, a)) : px(Close, t - 1, a);
        stp[static_cast<size_t>(a)] = c > 0.0 && b > 0.0 ? std::log(c / b) : kNaN;
        if (blk.y.gate[base + static_cast<size_t>(a)])
          m.add(stp[static_cast<size_t>(a)]);
      }
      const double mk = m.n ? m.mean() : 0.0;
      for (int a = 0; a < A; ++a)
        e[base + static_cast<size_t>(a)] = static_cast<float>((std::isfinite(stp[static_cast<size_t>(a)]) ? stp[static_cast<size_t>(a)] : 0.0) - mk);
    }
  });
  return e;
}

double cum_step(const std::vector<float> &e, int A, int a, int t0, int to) {
  double s = 0.0;
  for (int u = std::min(t0, to) + 1; u <= std::max(t0, to); ++u)
    s += e[static_cast<size_t>(u) * static_cast<size_t>(A) + static_cast<size_t>(a)];
  return s;
}

double cluster_se(const Mean &all, const std::vector<Mean> &by) {
  if (all.n < 2)
    return kNaN;
  const double m = all.mean();
  double v = 0.0;
  int nd = 0;
  for (const Mean &d : by)
    if (d.n)
      v += (d.sum - static_cast<double>(d.n) * m) * (d.sum - static_cast<double>(d.n) * m), ++nd;
  return nd >= 2 ? std::sqrt(v) / static_cast<double>(all.n) : kNaN;
}

struct RespPt {
  std::string name;
  bool close;
  int n;
};
std::vector<RespPt> points_of(int seg) {
  if (seg == 1)
    return {{"-5 日", false, -5}, {"-1 日", false, -1}, {"0 (开仓)", false, 0}, {"+1 日", false, 1}, {"+2 日", false, 2}, {"+3 日", false, 3}, {"+5 日", false, 5}};
  std::vector<RespPt> p{{"前 1 日同一行", false, -seg}};
  for (const int n : {-30, -10, -5, -1})
    if (-n < seg)
      p.push_back({std::to_string(n) + " 行", false, n});
  p.push_back({"0 (开仓)", false, 0});
  for (const int n : {1, 5, 10, 30})
    if (n < seg)
      p.push_back({"+" + std::to_string(n) + " 行", false, n});
  p.push_back({"当日收盘", true, 0});
  p.push_back({"T+1 收盘", true, 1});
  p.push_back({"T+2 收盘", true, 2});
  return p;
}

ViewOut response_view(const Call &c, const ViewEnv &env, Engine::FactorFlow &F, ViewRunner &) {
  std::string err;
  const bool top = c.str("side") == "top";
  FactorSpec fac;
  fac.ends = true, fac.only_end = top ? st::kTop : st::kBot;
  if (!resolve_factor(c.str("factor"), env, fac, err))
    return {false, std::move(err), {}};

  const int seg = env.fsc.ft->seg;
  const bool tod = c.str("split") == "tod" && seg > 1;
  const std::vector<RespPt> pts = points_of(seg);
  const size_t P = pts.size();
  const std::vector<Slot> ps = tod ? parts_of(seg, env.open, env.clock) : std::vector<Slot>{};
  const std::vector<int> pr = tod ? part_of_row(ps, seg) : std::vector<int>{};

  std::vector<std::pair<std::string, int>> lags;
  if (seg == 1) {
    lags = {{"前 1 日", 1}, {"前 5 日", 5}, {"前 20 日", 20}};
  } else {
    for (const int w : {5, 30})
      if (w < seg)
        lags.push_back({"前 " + std::to_string(w) + " 行", w});
    lags.push_back({"前 1 日", seg});
  }
  std::vector<Mean> lag_rho(lags.size());

  struct EvRec {
    int d0, part;
    std::array<float, 16> v, rk;
  };
  assert(P <= 16);
  std::vector<EvRec> all_ev;

  std::vector<misc::Ymd> dates;
  Engine::Job job;
  StreamCfg cfg{.fac = &fac, .bars = bit(Close) | bit(PreClose), .extra_front = seg == 1 ? 20 : 1, .extra_back = seg == 1 ? 5 : 2, .extra_work = sizeof(float)};
  const Engine::Rc rc = run_stream(
      env, F, cfg, dates, job, err,
      [&](const std::vector<misc::Ymd> &, size_t &, size_t &, std::string &) { return true; },
      [&](const ChunkView &cv) {
        const int A = cv.blk.y.sh.A, n_stat = cv.t_hi - cv.t_lo, total_T = static_cast<int>(cv.dates.size()) * seg;
        const Px px(cv.blk, cv.bm, cfg.bars, env.open);
        const std::vector<float> e = block_excess_steps(F.S.ex, cv.blk, px, cv.blk.land0());

        // 段首事件 (段在日界处天然断开, 不跨块)
        std::vector<std::pair<int, int>> raw; // (a, t)
        for (int r = 0; r < n_stat; ++r)
          for (const HeldCell &hc : cv.cells[static_cast<size_t>(r)])
            raw.push_back({hc.a, hc.t});
        std::sort(raw.begin(), raw.end());
        std::vector<std::pair<int, int>> ev;
        for (size_t i = 0; i < raw.size(); ++i) {
          const int u = cv.u0 + (raw[i].second - cv.t_lo);
          const int u_prev = i > 0 ? cv.u0 + (raw[i - 1].second - cv.t_lo) : -2;
          if (i == 0 || raw[i - 1].first != raw[i].first || u_prev + 1 != u || u_prev / seg != u / seg)
            ev.push_back(raw[i]);
        }
        const size_t base_idx = all_ev.size();
        all_ev.resize(base_idx + ev.size());
        F.S.ex.rows(static_cast<int>(ev.size()), [&](int i0, int i1, int) {
          for (int i = i0; i < i1; ++i) {
            const auto [a, t] = ev[static_cast<size_t>(i)];
            const int u = cv.u0 + (t - cv.t_lo), d0 = u / seg;
            EvRec rec{.d0 = d0, .part = tod ? pr[static_cast<size_t>(u % seg)] : 0};
            rec.v.fill(NAN), rec.rk.fill(NAN);
            for (size_t j = 0; j < P; ++j) {
              const int u_to = pts[j].close ? (d0 + pts[j].n) * seg + seg - 1 : u + pts[j].n;
              if (u_to < 0 || u_to >= total_T)
                continue;
              const int t_to = t + (u_to - u);
              assert(t_to >= 0 && t_to < cv.blk.y.sh.T);
              rec.v[j] = static_cast<float>(cum_step(e, A, a, t, t_to));
              const size_t k_to = static_cast<size_t>(t_to) * static_cast<size_t>(A) + static_cast<size_t>(a);
              if (cv.blk.y.gate[k_to])
                rec.rk[j] = rank_of_r16(cv.rx[k_to]);
            }
            all_ev[base_idx + static_cast<size_t>(i)] = rec;
          }
        });

        // 开仓行因子分位与此前收益的截面秩相关
        const int nt = F.S.ex.threads();
        for (size_t li = 0; li < lags.size(); ++li) {
          const int w = lags[li].second, rstep = std::max(1, (total_T - w) / kCorrRows);
          std::vector<Mean> tr(static_cast<size_t>(nt));
          F.S.ex.rows(n_stat, [&](int r0, int r1, int tid) {
            std::vector<double> xs, ys;
            for (int r = r0; r < r1; ++r) {
              const int t = cv.t_lo + r, u = cv.u0 + r;
              if (u < w || (u - w) % rstep != 0)
                continue;
              const size_t base = static_cast<size_t>(t) * static_cast<size_t>(A);
              xs.clear(), ys.clear();
              for (int a = 0; a < A; ++a)
                if (cv.blk.y.gate[base + static_cast<size_t>(a)] && cv.rx[base + static_cast<size_t>(a)] != st::kRankNone)
                  xs.push_back(rank_of_r16(cv.rx[base + static_cast<size_t>(a)])), ys.push_back(cum_step(e, A, a, t, t - w));
              if (static_cast<int>(xs.size()) >= kMinCs)
                tr[static_cast<size_t>(tid)].add(spearman(xs, ys));
            }
          });
          for (int tid = 0; tid < nt; ++tid)
            lag_rho[li].merge(tr[static_cast<size_t>(tid)]);
        }
      });

  if (rc != Engine::Rc::Ok)
    return {false, std::move(err), {}};

  const std::string side_cn = top ? "做多端 G20" : "最低组 G1";
  std::string s = period_note(dates, env) + "## 因子 `" + fac.label + "` · " + side_cn + " · 开仓前后的累计超额\n";
  if (all_ev.empty())
    return {true, s + "本端没有真持有过的格\n", {}};

  // 按全局资产序再按时间排以保证等距抽样确定性
  const size_t n_ev = all_ev.size(), step = std::max<size_t>(1, n_ev / kEvents);
  const size_t D = dates.size();
  std::vector<Mean> ex(P), rank(P);
  std::vector<std::vector<Mean>> by_day(P, std::vector<Mean>(D));
  std::vector<std::vector<Mean>> by_part(ps.size(), std::vector<Mean>(P));
  std::vector<size_t> n_part(ps.size(), 0);
  size_t kept = 0;
  for (size_t i = 0; i < n_ev; i += step, ++kept) {
    const EvRec &ev = all_ev[i];
    if (tod)
      ++n_part[static_cast<size_t>(ev.part)];
    for (size_t j = 0; j < P; ++j) {
      if (std::isfinite(ev.v[j])) {
        ex[j].add(ev.v[j]), by_day[j][static_cast<size_t>(ev.d0)].add(ev.v[j]);
        if (tod)
          by_part[static_cast<size_t>(ev.part)][j].add(ev.v[j]);
      }
      if (std::isfinite(ev.rk[j]))
        rank[j].add(ev.rk[j]);
    }
  }

  s += "事件 = 真持有段 (同 view_trades merge=run) 的开仓行, 共 " + str(n_ev) + " 段" + (step > 1 ? " (等距抽 " + str(kept) + " 段)" : "") + " · 零点 = 开仓行收盘价\n";
  s += "累计超额 = 对数收益减同行在池均值逐行累计 (跨日按昨收接, 不含除权缺口; 没价的行记 0); 开仓前的点 = 该点到开仓的涨跌, 开仓后的点 = 开仓到该点; 标准误按开仓日聚类\n\n";
  text::Table ptbl({"时点", "段数", "累计超额 bp", "± 标准误", "为正占比", "因子分位 (高 = 做多端)"});
  for (size_t j = 0; j < P; ++j) {
    const bool zero = !pts[j].close && pts[j].n == 0;
    ptbl.add({pts[j].name, str(ex[j].n), zero ? "0" : bp(ex[j].mean()), zero ? "—" : fmt("%.1f", cluster_se(ex[j], by_day[j]) * 1e4),
              zero ? "—" : pct(ex[j].up()), rk(rank[j].mean())});
  }
  s += ptbl.str();

  s += "\n开仓行的因子分位与此前收益的截面 rank 相关 (抽 " + std::to_string(kCorrRows) + " 行取均值; 接近 −1 = 因子主要是短期反转的代理, 接近 +1 = 动量的代理):";
  for (size_t i = 0; i < lags.size(); ++i)
    s += (i ? " · " : " ") + lags[i].first + " " + r3(lag_rho[i].mean());
  s += "\n";

  if (tod) {
    std::vector<std::string> hs{"开仓时段", "段数"};
    for (const RespPt &p : pts)
      hs.push_back(p.name);
    text::Table ttbl(hs);
    for (size_t q = 0; q < ps.size(); ++q) {
      if (!n_part[q])
        continue;
      std::vector<std::string> row{ps[q].name, str(n_part[q])};
      for (size_t j = 0; j < P; ++j)
        row.push_back(!pts[j].close && pts[j].n == 0 ? "0" : bp(by_part[q][j].mean()));
      ttbl.add(row);
    }
    s += "\n按开仓时段 (各时点的累计超额 bp):\n" + ttbl.str();
  }
  return {true, std::move(s), {}};
}

// =============================================================================
// 6. view_explain
// =============================================================================
std::vector<double> centered_ranks(const std::vector<double> &x) {
  std::vector<double> r = ranks_of(x);
  const double m = 0.5 * static_cast<double>(x.size() - 1);
  for (double &v : r)
    v -= m;
  return r;
}
double dot(const std::vector<double> &a, const std::vector<double> &b) { return std::inner_product(a.begin(), a.end(), b.begin(), 0.0); }

std::vector<double> resid_of(const std::vector<std::vector<double>> &X, const std::vector<double> &y) {
  const size_t m = X.size();
  std::vector<double> g(m * m), h(m), beta(m);
  double tr = 0.0;
  for (size_t i = 0; i < m; ++i) {
    for (size_t j = 0; j <= i; ++j)
      g[i * m + j] = g[j * m + i] = dot(X[i], X[j]);
    h[i] = dot(X[i], y), tr += g[i * m + i];
  }
  for (size_t i = 0; i < m; ++i)
    g[i * m + i] += 1e-9 * tr / static_cast<double>(m) + 1e-12;
  for (size_t p = 0; p < m; ++p)
    for (size_t i = p + 1; i < m; ++i) {
      const double f = g[i * m + p] / g[p * m + p];
      for (size_t j = p; j < m; ++j)
        g[i * m + j] -= f * g[p * m + j];
      h[i] -= f * h[p];
    }
  for (size_t p = m; p-- > 0;) {
    double s = h[p];
    for (size_t j = p + 1; j < m; ++j)
      s -= g[p * m + j] * beta[j];
    beta[p] = s / g[p * m + p];
  }
  std::vector<double> res = y;
  for (size_t j = 0; j < m; ++j)
    for (size_t i = 0; i < res.size(); ++i)
      res[i] -= beta[j] * X[j][i];
  return res;
}

ViewOut explain_view(const Call &c, const ViewEnv &env, Engine::FactorFlow &F, ViewRunner &) {
  std::string err;
  FactorSpec fac;
  if (!resolve_factor(c.str("factor"), env, fac, err))
    return {false, std::move(err), {}};
  std::vector<FeatSpec> feats;
  resolve_feats(c.strs("exprs"), env, feats);

  const int seg = env.fsc.ft->seg;
  const int lag = seg > 1 ? 6 * seg : 21;
  std::vector<std::string> b_names;
  std::vector<int> ret_lags; // 内置近 w 行收益基准
  if (seg > 1) {
    for (const int w : {5, 30})
      if (w < seg)
        b_names.push_back("近 " + std::to_string(w) + " 行收益"), ret_lags.push_back(w);
    for (const char *nm : {"当日至今收益", "前 1 日收益", "前 5 日收益", "当日至今成交额", "当日至今振幅", "日内位置", "价格"})
      b_names.emplace_back(nm);
  } else {
    for (const int w : {1, 5, 20})
      b_names.push_back(w == 1 ? "当日收益" : "近 " + std::to_string(w) + " 日收益"), ret_lags.push_back(w);
    for (const char *nm : {"成交额", "振幅", "价格"})
      b_names.emplace_back(nm);
  }
  const size_t n_builtin = b_names.size();
  for (size_t i = 0; i < feats.size(); ++i)
    if (feats[i].err.empty())
      b_names.push_back("E" + str(i + 1));

  // 由于运行期可能有坏根特征, 在首块 plan_forest 后按实际有效特征定列
  Mean fric, r2, rric;
  std::vector<Mean> corr, bric, sric;
  Layer orig, rest;
  std::vector<size_t> active_feats;
  bool cols_fixed = false;

  std::vector<misc::Ymd> dates;
  Engine::Job job;
  StreamCfg cfg{.fac = &fac,
                .feats = &feats,
                .bars = bit(Close) | bit(High) | bit(Low) | bit(Volume) | bit(Amount) | bit(PreClose),
                .extra_front = seg > 1 ? 7 : 22,
                .extra_work = sizeof(double)};
  const Engine::Rc rc = run_stream(
      env, F, cfg, dates, job, err,
      [&](const std::vector<misc::Ymd> &ds, size_t &, size_t &, std::string &why) {
        if (static_cast<int>(ds.size()) * seg <= lag) {
          why = "## 因子 `" + fac.label + "` 的成分\n观察的日段太短: 基准要回看 " + std::to_string(lag) + " 行\n";
          return false;
        }
        return true;
      },
      [&](const ChunkView &cv) {
        if (!cols_fixed) {
          b_names.resize(n_builtin);
          for (size_t i = 0; i < feats.size(); ++i)
            if (cv.feat_pl[i]) {
              active_feats.push_back(i);
              b_names.push_back("E" + str(i + 1));
            }
          const size_t m = b_names.size();
          corr.resize(m), bric.resize(m), sric.resize(m);
          cols_fixed = true;
        }
        const size_t m = b_names.size();
        const int T = cv.blk.y.sh.T, A = cv.blk.y.sh.A, n_stat = cv.t_hi - cv.t_lo, total_T = static_cast<int>(cv.dates.size()) * seg;
        const int rstep = std::max(1, (total_T - lag) / kExplainRows);
        const Px px(cv.blk, cv.bm, cfg.bars, env.open);
        const std::vector<float> e = block_excess_steps(F.S.ex, cv.blk, px, cv.blk.land0());
        std::vector<double> cum(e.size());
        F.S.ex.rows(A, [&](int a0, int a1, int) {
          for (int a = a0; a < a1; ++a) {
            double acc = 0.0;
            for (int t = 0; t < T; ++t)
              cum[static_cast<size_t>(t) * static_cast<size_t>(A) + static_cast<size_t>(a)] = acc += e[static_cast<size_t>(t) * static_cast<size_t>(A) + static_cast<size_t>(a)];
          }
        });
        const auto ret = [&](int a, int t1, int t0) {
          return cum[static_cast<size_t>(t1) * static_cast<size_t>(A) + static_cast<size_t>(a)] -
                 cum[static_cast<size_t>(t0) * static_cast<size_t>(A) + static_cast<size_t>(a)];
        };

        const Engine::LegPlane &lb = *cv.blk.y.labels[0];
        const uint16_t *lv = lb.v[st::kLong].data();
        const int nt = F.S.ex.threads();
        struct ThreadAcc {
          Mean fric, r2, rric;
          std::vector<Mean> corr, bric, sric;
          Layer orig, rest;
        };
        std::vector<ThreadAcc> ta(static_cast<size_t>(nt));
        for (ThreadAcc &x : ta)
          x.corr.resize(m), x.bric.resize(m), x.sric.resize(m);

        F.S.ex.rows(n_stat, [&](int r0, int r1, int tid) {
          ThreadAcc &loc = ta[static_cast<size_t>(tid)];
          std::vector<double> fxv, ey, one(m);
          std::vector<std::vector<double>> bv(m), X(m);
          st::Row w;
          for (int r = r0; r < r1; ++r) {
            const int t = cv.t_lo + r, u = cv.u0 + r;
            if (u < lag || (u - lag) % rstep != 0)
              continue;
            const float mkt = row_mkt(cv.blk, t);
            if (!std::isfinite(mkt))
              continue;
            assert(t >= lag);
            const int t_day0 = t - t % seg;
            const size_t base = static_cast<size_t>(t) * static_cast<size_t>(A);
            fxv.clear(), ey.clear();
            for (std::vector<double> &v : bv)
              v.clear();

            for (int a = 0; a < A; ++a) {
              const size_t k = base + static_cast<size_t>(a);
              if (!cv.blk.y.gate[k] || cv.rx[k] == st::kRankNone || !lb.m[k])
                continue;
              size_t col = 0;
              for (const int rw : ret_lags)
                one[col++] = ret(a, t, t - rw);
              if (seg > 1) {
                double damt = 0.0, dhi = -INFINITY, dlo = INFINITY;
                for (int q = t_day0; q <= t; ++q) {
                  if (const double av = px(Amount, q, a); std::isfinite(av))
                    damt += av;
                  if (px.traded(q, a))
                    dhi = std::fmax(dhi, px(High, q, a)), dlo = std::fmin(dlo, px(Low, q, a));
                }
                const double pre = px.pre(t, a), cl = px(Close, t, a);
                one[col++] = ret(a, t, t_day0 - 1);
                one[col++] = ret(a, t_day0 - 1, t_day0 - 1 - seg);
                one[col++] = ret(a, t_day0 - 1, t_day0 - 1 - 5 * seg);
                one[col++] = damt;
                one[col++] = dhi > dlo ? (dhi - dlo) / pre : 0.0;
                one[col++] = dhi > dlo ? (cl - dlo) / (dhi - dlo) : 0.5;
                one[col++] = cl;
              } else {
                one[col++] = px(Amount, t, a);
                one[col++] = (px(High, t, a) - px(Low, t, a)) / px.pre(t, a);
                one[col++] = px(Close, t, a);
              }
              bool ok = true;
              for (const size_t fi : active_feats) {
                const factor::check::Plane *fp = cv.feat_pl[fi];
                one[col++] = fp->m[k] ? static_cast<double>(fp->v[k]) : kNaN;
              }
              assert(col == m);
              for (size_t j = 0; j < m && ok; ++j)
                ok = std::isfinite(one[j]);
              if (!ok)
                continue;
              fxv.push_back(rank_of_r16(cv.rx[k]));
              ey.push_back(static_cast<double>(h2f(lv[k])) - mkt);
              for (size_t j = 0; j < m; ++j)
                bv[j].push_back(one[j]);
            }
            if (fxv.size() < static_cast<size_t>(kMinCs))
              continue;
            const std::vector<double> rf = centered_ranks(fxv), ry = ranks_of(ey);
            loc.fric.add(pearson(rf, ry));
            for (size_t j = 0; j < m; ++j) {
              X[j] = centered_ranks(bv[j]);
              loc.corr[j].add(pearson(rf, X[j])), loc.bric[j].add(pearson(X[j], ry));
              const double xx = dot(X[j], X[j]), beta = xx > 0.0 ? dot(X[j], rf) / xx : 0.0;
              std::vector<double> res = rf;
              for (size_t i = 0; i < res.size(); ++i)
                res[i] -= beta * X[j][i];
              loc.sric[j].add(pearson(ranks_of(res), ry));
            }
            const std::vector<double> res = resid_of(X, rf);
            const double tot = dot(rf, rf);
            if (tot > 0.0)
              loc.r2.add(1.0 - dot(res, res) / tot);
            loc.rric.add(pearson(ranks_of(res), ry));
            if (approx_row(fxv, ey, w))
              loc.orig.add(w);
            if (approx_row(res, ey, w))
              loc.rest.add(w);
          }
        });
        for (const ThreadAcc &loc : ta) {
          fric.merge(loc.fric), r2.merge(loc.r2), rric.merge(loc.rric);
          orig.merge(loc.orig), rest.merge(loc.rest);
          for (size_t j = 0; j < m; ++j)
            corr[j].merge(loc.corr[j]), bric[j].merge(loc.bric[j]), sric[j].merge(loc.sric[j]);
        }
      });

  if (rc != Engine::Rc::Ok)
    return {false, std::move(err), {}};

  std::string s = period_note(dates, env) + "## 因子 `" + fac.label + "` 的成分\n";
  for (size_t i = 0; i < feats.size(); ++i)
    s += expr_line(i, feats[i], "没算成, 不进基准");
  if (!fric.n)
    return {false, s + "没有可用的行 (每行至少要 " + std::to_string(kMinCs) + " 个在池、标签有效且全部基准有值的格)\n", {}};

  s += "抽 " + str(fric.n) + " 行, 每行取在池、标签有效且全部基准有值的格; 收益类基准是对数收益减同行在池均值 (跨日按昨收接); 相关与 rIC 都是逐行截面 Spearman 的均值\n";
  s += "因子 rIC (同口径) " + r3(fric.mean()) + "\n\n";
  text::Table btbl({"基准", "与因子的截面秩相关", "基准自身 rIC", "只剥它后的因子 rIC"});
  for (size_t j = 0; j < b_names.size(); ++j)
    btbl.add({b_names[j], r3(corr[j].mean()), r3(bric[j].mean()), r3(sric[j].mean())});
  s += btbl.str();

  s += "\n全部基准一起剥 (每行把因子秩对全部基准秩做截面回归, 取残差): 平均 R² " + r3(r2.mean()) + " · 因子 rIC " + r3(fric.mean()) + " → 剩余部分 " + r3(rric.mean()) + "\n";
  s += "20 组按标签逐行近似分 (组超额 = 组内 (标签 − 本行在池均值) 等权均值, 建不了仓的格不剔), 与回测 / view_layers 的真持有口径略有出入, 只作两行之间对照\n\n";
  text::Table ltbl({"", "行", "有统计", "mono", "多空 bp", "rIC", "G1 bp", "G10 bp", "G20 bp", "20 组"});
  add_layer_row(ltbl, "原因子", orig);
  add_layer_row(ltbl, "剩余部分", rest);
  s += ltbl.str();
  std::string gist = "rIC " + r3(fric.mean()) + " → 剥全部基准剩 " + r3(rric.mean()) + " (R² " + r3(r2.mean()) + ") · 剩余部分 mono " + r3(rest.mono()) + " · 多空 " +
                     bp(rest.ls()) + "bp";
  return {true, std::move(s), std::move(gist)};
}

// =============================================================================
// 7. view_snapshot (只装载目标那 1 天所在块)
// =============================================================================
ViewOut snapshot_view(const Call &c, const ViewEnv &env, Engine::FactorFlow &F, ViewRunner &) {
  std::string err;
  if (!date_arg_ok(c, "date", err))
    return {false, std::move(err), {}};
  const int seg = env.fsc.ft->seg;
  const int k_row = row_at(env.clock, c.str("time"));
  if (seg > 1 && (c.str("time").empty() || k_row < 0))
    return {false, "time: 写成本层某一行的行末时刻 HH:MM (本层 " + env.clock.front() + ", " + env.clock[1] + " … " + env.clock.back() + ")", {}};

  FactorSpec fac;
  fac.ends = true;
  if (!resolve_factor(c.str("factor"), env, fac, err))
    return {false, std::move(err), {}};
  std::vector<FeatSpec> feats;
  resolve_feats(c.strs("exprs"), env, feats);

  int target_d = -1;
  std::string body;
  bool ok_snap = false;
  std::vector<misc::Ymd> dates;
  Engine::Job job;
  StreamCfg cfg{.fac = &fac, .feats = &feats, .bars = kAllBars, .extra_front = seg == 1 ? 1 : 0};
  const Engine::Rc rc = run_stream(
      env, F, cfg, dates, job, err,
      [&](const std::vector<misc::Ymd> &ds, size_t &lo, size_t &hi, std::string &why) {
        target_d = day_index(ds, c.str("date"), why);
        if (target_d < 0)
          return false;
        lo = static_cast<size_t>(target_d), hi = lo + 1;
        return true;
      },
      [&](const ChunkView &cv) {
        const int u_want = target_d * seg + k_row;
        if (u_want < cv.u0 || u_want >= cv.u0 + (cv.t_hi - cv.t_lo))
          return;
        const int r_idx = u_want - cv.u0, t = cv.t_lo + r_idx, t0 = t - k_row, A = cv.blk.y.sh.A;
        const Px px(cv.blk, cv.bm, cfg.bars, env.open);
        const Engine::LegPlane &lb = *cv.blk.y.labels[0];
        const uint16_t *lv = lb.v[st::kLong].data();
        const float mkt = row_mkt(cv.blk, t);

        struct Cell {
          int a = 0, grp = -1, held = 0;
          double rank = kNaN, amt = 0.0, step = kNaN, ex = kNaN;
          Mark m;
          std::vector<double> fv;
        };
        std::vector<int> held(static_cast<size_t>(A), 0);
        for (const HeldCell &hc : cv.cells[static_cast<size_t>(r_idx)])
          held[static_cast<size_t>(hc.a)] = hc.e == st::kTop ? 1 : -1;

        std::vector<Cell> cs;
        for (int a = 0; a < A; ++a) {
          const size_t q = static_cast<size_t>(t) * static_cast<size_t>(A) + static_cast<size_t>(a);
          if (!cv.blk.y.gate[q] || cv.rx[q] == st::kRankNone)
            continue;
          Cell x{.a = a, .grp = grp_of_r16(cv.rx[q]), .held = held[static_cast<size_t>(a)], .rank = rank_of_r16(cv.rx[q])};
          for (int q_t = t0; q_t <= t; ++q_t)
            if (const double v = px(Amount, q_t, a); std::isfinite(v))
              x.amt += v;
          const double cl = px(Close, t, a), prev = k_row == 0 ? px.pre(t, a) : px(Close, t - 1, a);
          x.step = cl / prev - 1.0;
          x.ex = lb.m[q] ? static_cast<double>(h2f(lv[q])) - mkt : kNaN;
          x.m = mark_of(px, t, a);
          for (size_t i = 0; i < feats.size(); ++i)
            x.fv.push_back(cv.feat_pl[i] && cv.feat_pl[i]->m[q] ? static_cast<double>(cv.feat_pl[i]->v[q]) : kNaN);
          cs.push_back(std::move(x));
        }

        const std::string when = date_of(dates[static_cast<size_t>(target_d)]) + (seg > 1 ? " " + env.clock[static_cast<size_t>(k_row)] : "");
        std::string s = "## 因子 `" + fac.label + "` · " + when + " 那一行的截面 (" + env.level + "; 分位 / 组号已定向: G20 = 做多端)\n";
        if (cs.size() < static_cast<size_t>(kMinCs)) {
          body = s + "本行在池且因子有秩的只有 " + str(cs.size()) + " 只";
          return;
        }
        ok_snap = true;
        std::sort(cs.begin(), cs.end(), [](const Cell &x, const Cell &y) { return x.rank > y.rank; });
        const st::Row &w = cv.rows[static_cast<size_t>(r_idx)];
        s += "在池且有秩 " + str(cs.size()) + " 只 · 本行" +
             (w.ok ? "有统计: rIC " + r3(w.ric) + " · G20 超额 " + bp(w.grp[kG20]) + "bp (真持有 " + std::to_string(w.cnt[kG20]) + " 只) · G1 超额 " +
                         bp(w.grp[kG1]) + "bp (" + std::to_string(w.cnt[kG1]) + " 只)"
                   : std::string("无统计 (建不了仓 / 标签无效)")) +
             " · 后续超额 = 本行开仓的持有期收益 − 在池均值\n";
        for (size_t i = 0; i < feats.size(); ++i)
          s += expr_line(i, feats[i]);

        const auto corr_fn = [&](auto &&fn) {
          std::vector<double> u, v;
          for (const Cell &x : cs)
            if (const double y = fn(x); std::isfinite(y))
              u.push_back(x.rank), v.push_back(y);
          return u.size() >= static_cast<size_t>(kMinCs) ? spearman(u, v) : kNaN;
        };
        s += "与因子分位的截面秩相关: 当日涨跌 " + r3(corr_fn([](const Cell &x) { return x.m.chg; })) +
             (seg > 1 ? " · 本行涨跌 " + r3(corr_fn([](const Cell &x) { return x.step; })) : std::string()) + " · 日内位置 " +
             r3(corr_fn([](const Cell &x) { return x.m.pos; })) + " · 当日至此成交额 " + r3(corr_fn([](const Cell &x) { return x.amt; })) + " · 后续超额 " +
             r3(corr_fn([](const Cell &x) { return x.ex; }));
        for (size_t i = 0; i < feats.size(); ++i)
          s += " · E" + str(i + 1) + " " + r3(corr_fn([i](const Cell &x) { return x.fv[i]; }));
        s += "\n\n";

        std::vector<std::string> hs{"端", "代码", "分位", "组", "真持有", "当日涨跌"};
        if (seg > 1)
          hs.push_back("本行涨跌"), hs.push_back("日内位置");
        hs.push_back("至此额 万"), hs.push_back("后续超额 bp"), hs.push_back("盘面");
        for (size_t i = 0; i < feats.size(); ++i)
          hs.push_back("E" + str(i + 1));
        text::Table tbl(hs);

        const AssetAxis &ax = asset_axis();
        const PoolAxis &pa = job.dm.rs.axis;
        const auto add_line = [&](const std::string &end_nm, const Cell &x) {
          std::vector<std::string> row{end_nm, ax.code(pa.global(static_cast<size_t>(x.a))), rk(x.rank), gname(x.grp),
                                       x.held > 0 ? "多" : x.held < 0 ? "空"
                                                                      : "",
                                       chg(x.m.chg)};
          if (seg > 1)
            row.push_back(chg(x.step)), row.push_back(pos2(x.m.pos));
          row.push_back(fmt("%.0f", x.amt / 1e4)), row.push_back(bp(x.ex)), row.push_back(tags_of(x.m.m));
          for (const double v : x.fv)
            row.push_back(num(v));
          tbl.add(row);
        };
        const size_t n = std::min(cs.size() / 2, static_cast<size_t>(c.num("n")));
        for (size_t i = 0; i < n; ++i)
          add_line("高", cs[i]);
        {
          std::vector<double> chs, sts, pss, ams, exs;
          for (const Cell &x : cs)
            chs.push_back(x.m.chg), sts.push_back(x.step), pss.push_back(x.m.pos), ams.push_back(x.amt), exs.push_back(x.ex);
          std::vector<std::string> mid_row{"池中位", "", "50", "", "", chg(median_of(chs))};
          if (seg > 1)
            mid_row.push_back(chg(median_of(sts))), mid_row.push_back(pos2(median_of(pss)));
          mid_row.push_back(fmt("%.0f", median_of(ams) / 1e4)), mid_row.push_back(bp(median_of(exs))), mid_row.push_back("");
          for (size_t i = 0; i < feats.size(); ++i) {
            std::vector<double> fv;
            for (const Cell &x : cs)
              fv.push_back(x.fv[i]);
            mid_row.push_back(num(median_of(fv)));
          }
          tbl.add(mid_row);
        }
        for (size_t i = 0; i < n; ++i)
          add_line("低", cs[cs.size() - 1 - i]);
        body = s + tbl.str();
      });

  if (rc != Engine::Rc::Ok)
    return {false, std::move(err), {}};
  return {ok_snap, ok_snap ? period_note(dates, env) + body : body, {}};
}

// =============================================================================
// 8. view_stock_day & 9. view_stock_days
// =============================================================================
ViewOut stock_day_view(const Call &c, const ViewEnv &env, Engine::FactorFlow &F, ViewRunner &) {
  const int seg = env.fsc.ft->seg;
  if (seg == 1)
    return {false, "日层每天只有一行, 看个股用 view_stock_days", {}};
  std::string err, code;
  uint32_t asset_id = 0;
  if (!resolve_code(c.str("code"), env, code, asset_id, err) || !date_arg_ok(c, "date", err))
    return {false, std::move(err), {}};
  int rk0 = 0, rk1 = 0;
  if (!c.str("rows").empty() && !rows_in("rows", c.str("rows"), env.clock, kStockRows, rk0, rk1, err))
    return {false, std::move(err), {}};

  FactorSpec fac;
  const bool has_fac = !c.str("factor").empty();
  if (has_fac && !resolve_factor(c.str("factor"), env, fac, err))
    return {false, std::move(err), {}};
  std::vector<FeatSpec> feats;
  resolve_feats(c.strs("exprs"), env, feats);

  int target_d = -1;
  std::string body;
  std::vector<misc::Ymd> dates;
  Engine::Job job;
  StreamCfg cfg{.fac = has_fac ? &fac : nullptr, .feats = &feats, .bars = kAllBars};
  const Engine::Rc rc = run_stream(
      env, F, cfg, dates, job, err,
      [&](const std::vector<misc::Ymd> &ds, size_t &lo, size_t &hi, std::string &why) {
        target_d = day_index(ds, c.str("date"), why);
        if (target_d < 0)
          return false;
        lo = static_cast<size_t>(target_d), hi = lo + 1;
        return true;
      },
      [&](const ChunkView &cv) {
        const int u0 = target_d * seg;
        if (u0 < cv.u0 || u0 >= cv.u0 + (cv.t_hi - cv.t_lo))
          return;
        const size_t col_a = job.dm.rs.axis.sub_of(asset_id);
        assert(col_a < static_cast<size_t>(cv.blk.y.sh.A));
        const int a = static_cast<int>(col_a), A = cv.blk.y.sh.A, t0 = cv.t_lo + (u0 - cv.u0);
        const Px px(cv.blk, cv.bm, cfg.bars, env.open);
        const Engine::LegPlane &lb = *cv.blk.y.labels[0];
        const uint16_t *lv = lb.v[st::kLong].data();

        std::vector<double> close(static_cast<size_t>(seg)), high(static_cast<size_t>(seg)), low(static_cast<size_t>(seg)), vol(static_cast<size_t>(seg)),
            amt(static_cast<size_t>(seg)), exs(static_cast<size_t>(seg)), rks(static_cast<size_t>(seg), kNaN);
        std::vector<int> grps(static_cast<size_t>(seg), -1);
        std::vector<std::vector<double>> fvals(feats.size(), std::vector<double>(static_cast<size_t>(seg), kNaN));

        const double pre = px.pre(t0, a);
        double hi = -INFINITY, lo = INFINITY, vsum = 0.0, last = kNaN;
        int in = 0, first = -1;
        Mean amean;
        for (int k = 0; k < seg; ++k) {
          const int t = t0 + k;
          const size_t q = static_cast<size_t>(k), cell_k = static_cast<size_t>(t) * static_cast<size_t>(A) + static_cast<size_t>(a);
          close[q] = px(Close, t, a), high[q] = px(High, t, a), low[q] = px(Low, t, a), vol[q] = px(Volume, t, a), amt[q] = px(Amount, t, a);
          if (px.traded(t, a)) {
            hi = std::fmax(hi, high[q]), lo = std::fmin(lo, low[q]);
            first = first < 0 ? k : first;
          }
          vsum += std::isfinite(vol[q]) ? vol[q] : 0.0, amean.add(amt[q]);
          last = std::isfinite(close[q]) ? close[q] : last;
          in += cv.blk.y.gate[cell_k];
          const float mkt = row_mkt(cv.blk, t);
          exs[q] = lb.m[cell_k] ? static_cast<double>(h2f(lv[cell_k])) - mkt : kNaN;
          if (has_fac)
            rks[q] = rank_of_r16(cv.rx[cell_k]), grps[q] = grp_of_r16(cv.rx[cell_k]);
          for (size_t i = 0; i < feats.size(); ++i)
            if (cv.feat_pl[i] && cv.feat_pl[i]->m[cell_k])
              fvals[i][q] = cv.feat_pl[i]->v[cell_k];
        }

        std::string s = "## " + code + " · " + date_of(dates[static_cast<size_t>(target_d)]) + " · " + env.level + " (每天 " + std::to_string(seg) + " 行)\n";
        const std::string open_str = first < 0 ? "当日无成交" : "首个有成交行 (" + env.clock[static_cast<size_t>(first)] + ") 收 " + num(close[static_cast<size_t>(first)]) + " (" + chg(close[static_cast<size_t>(first)] / pre - 1.0) + ")";
        s += "昨收 " + num(pre) + " · " + open_str + " · 高 " + num(hi) + " (" + chg(hi / pre - 1.0) + ") · 低 " + num(lo) + " (" + chg(lo / pre - 1.0) + ") · 收 " +
             num(last) + " (" + chg(last / pre - 1.0) + ") · 涨停 " + num(px(LimUp, t0, a)) + " / 跌停 " + num(px(LimDn, t0, a)) + " · 成交额 " + wan(amean) +
             " 万 · 在池 " + std::to_string(in) + " / " + std::to_string(seg) + " 行\n";
        for (size_t i = 0; i < feats.size(); ++i)
          s += expr_line(i, feats[i]);
        s += "\n- 收盘价 " + spark(close) + "\n- 成交量 " + spark(vol) + "\n";
        if (has_fac)
          s += "- 因子 `" + fac.label + "` 分位 (高 = 做多端) " + spark(rks, rk) + "\n";
        for (size_t i = 0; i < feats.size(); ++i)
          s += "- E" + str(i + 1) + " " + spark(fvals[i]) + "\n";

        std::vector<std::string> phs{"时段", "涨跌", "段内高", "段内低", "量占比"};
        if (has_fac)
          phs.push_back("因子分位"), phs.push_back("多/空行");
        phs.push_back("后续超额 bp");
        for (size_t i = 0; i < feats.size(); ++i)
          phs.push_back("E" + str(i + 1) + " 段末");
        text::Table ptbl(phs);
        for (const Slot &p : parts_of(seg, env.open, env.clock)) {
          double ph = -INFINITY, pl = INFINITY, pv = 0.0;
          std::vector<double> prks, pexs;
          int up = 0, dn = 0;
          for (int k = p.k0; k < p.k1; ++k) {
            const size_t q = static_cast<size_t>(k);
            if (px.traded(t0 + k, a))
              ph = std::fmax(ph, high[q]), pl = std::fmin(pl, low[q]);
            pv += std::isfinite(vol[q]) ? vol[q] : 0.0;
            pexs.push_back(exs[q]);
            if (has_fac) {
              prks.push_back(rks[q]);
              up += grps[q] == kG20, dn += grps[q] == kG1;
            }
          }
          const double pfrom = p.k0 == 0 ? pre : close[static_cast<size_t>(p.k0 - 1)];
          std::vector<std::string> row{p.name, chg(close[static_cast<size_t>(p.k1 - 1)] / pfrom - 1.0), chg(ph / pre - 1.0), chg(pl / pre - 1.0),
                                       pct(vsum > 0.0 ? pv / vsum : kNaN)};
          if (has_fac)
            row.push_back(rk(mean_of(prks))), row.push_back(std::to_string(up) + "/" + std::to_string(dn));
          row.push_back(bp(mean_of(pexs)));
          for (size_t i = 0; i < feats.size(); ++i)
            row.push_back(num(fvals[i][static_cast<size_t>(p.k1 - 1)]));
          ptbl.add(row);
        }
        s += "\n" + ptbl.str();

        if (!c.str("rows").empty()) {
          const auto whole = [](double x) { return std::isfinite(x) ? fmt("%.0f", x) : std::string("—"); };
          std::vector<std::string> rhs{"时刻", "收", "涨跌", "当日", "量", "额"};
          if (has_fac)
            rhs.push_back("因子分位"), rhs.push_back("组");
          rhs.push_back("后续超额 bp");
          for (size_t i = 0; i < feats.size(); ++i)
            rhs.push_back("E" + str(i + 1));
          text::Table rtbl(rhs);
          for (int k = rk0; k < rk1; ++k) {
            const size_t q = static_cast<size_t>(k);
            const double prev = k == 0 ? pre : close[q - 1];
            std::vector<std::string> row{env.clock[q], num(close[q]), chg(close[q] / prev - 1.0), chg(close[q] / pre - 1.0), whole(vol[q]),
                                         std::isfinite(amt[q]) ? fmt("%.1f", amt[q] / 1e4) : std::string("—")};
            if (has_fac)
              row.push_back(rk(rks[q])), row.push_back(grps[q] < 0 ? "—" : gname(grps[q]));
            row.push_back(bp(exs[q]));
            for (size_t i = 0; i < feats.size(); ++i)
              row.push_back(num(fvals[i][q]));
            rtbl.add(row);
          }
          s += "\n逐行 " + c.str("rows") + " (涨跌 = 对上一行收盘; 当日 = 对昨收; 额 万元):\n" + rtbl.str();
        }
        body = std::move(s);
      });

  if (rc != Engine::Rc::Ok)
    return {false, std::move(err), {}};
  return {true, period_note(dates, env) + body, {}};
}

ViewOut stock_days_view(const Call &c, const ViewEnv &env, Engine::FactorFlow &F, ViewRunner &) {
  std::string err, code;
  uint32_t asset_id = 0;
  if (!resolve_code(c.str("code"), env, code, asset_id, err) || !date_arg_ok(c, "from", err) || !date_arg_ok(c, "to", err))
    return {false, std::move(err), {}};

  FactorSpec fac;
  const bool has_fac = !c.str("factor").empty();
  if (has_fac && !resolve_factor(c.str("factor"), env, fac, err))
    return {false, std::move(err), {}};
  std::vector<FeatSpec> feats;
  resolve_feats(c.strs("exprs"), env, feats);

  const int seg = env.fsc.ft->seg;
  int d0 = 0, d1 = 0, n_days = 0;
  struct DayRec {
    double close = kNaN, chg_d = kNaN, amp = kNaN, close_ex = kNaN, fac_mean = kNaN, fac_close = kNaN;
    Mean amean;
    int in = 0, up = 0, dn = 0;
    std::vector<double> fv;
  };
  std::vector<DayRec> recs;

  std::vector<misc::Ymd> dates;
  Engine::Job job;
  StreamCfg cfg{.fac = has_fac ? &fac : nullptr, .feats = &feats, .bars = bit(Close) | bit(High) | bit(Low) | bit(Volume) | bit(Amount) | bit(PreClose), .extra_front = seg == 1 ? 1 : 0};
  const Engine::Rc rc = run_stream(
      env, F, cfg, dates, job, err,
      [&](const std::vector<misc::Ymd> &ds, size_t &lo, size_t &hi, std::string &why) {
        const int D = static_cast<int>(ds.size());
        d1 = c.str("to").empty() ? D - 1 : day_index(ds, c.str("to"), why);
        if (d1 < 0)
          return false;
        d0 = c.str("from").empty() ? std::max(0, d1 - 19) : day_index(ds, c.str("from"), why);
        if (d0 < 0)
          return false;
        if (d0 > d1 || d1 - d0 + 1 > kStockDays) {
          why = "from ~ to 须是先后有序且至多 " + std::to_string(kStockDays) + " 个交易日 (这里是 " + std::to_string(d1 - d0 + 1) + " 个)";
          return false;
        }
        n_days = d1 - d0 + 1;
        recs.resize(static_cast<size_t>(n_days));
        lo = static_cast<size_t>(d0), hi = static_cast<size_t>(d1 + 1);
        return true;
      },
      [&](const ChunkView &cv) {
        const size_t col_a = job.dm.rs.axis.sub_of(asset_id);
        assert(col_a < static_cast<size_t>(cv.blk.y.sh.A));
        const int a = static_cast<int>(col_a), A = cv.blk.y.sh.A, days_stat = (cv.t_hi - cv.t_lo) / seg, chunk_d0 = cv.u0 / seg;
        const Px px(cv.blk, cv.bm, cfg.bars, env.open);
        const Engine::LegPlane &lb = *cv.blk.y.labels[0];
        const uint16_t *lv = lb.v[st::kLong].data();

        for (int cd = 0; cd < days_stat; ++cd) {
          const int d = chunk_d0 + cd;
          if (d < d0 || d > d1)
            continue;
          const int t0 = cv.t_lo + cd * seg, tl = t0 + seg - 1;
          DayRec &rec = recs[static_cast<size_t>(d - d0)];
          double hi = -INFINITY, lo = INFINITY;
          std::vector<double> rks;
          for (int t = t0; t <= tl; ++t) {
            const size_t cell_k = static_cast<size_t>(t) * static_cast<size_t>(A) + static_cast<size_t>(a);
            if (px.traded(t, a))
              hi = std::fmax(hi, px(High, t, a)), lo = std::fmin(lo, px(Low, t, a));
            rec.amean.add(px(Amount, t, a));
            if (const double x = px(Close, t, a); std::isfinite(x))
              rec.close = x;
            rec.in += cv.blk.y.gate[cell_k];
            if (has_fac) {
              rks.push_back(rank_of_r16(cv.rx[cell_k]));
              const int g = grp_of_r16(cv.rx[cell_k]);
              rec.up += g == kG20, rec.dn += g == kG1;
            }
          }
          const double pre = px.pre(t0, a);
          rec.chg_d = rec.close / pre - 1.0;
          rec.amp = hi > lo ? (hi - lo) / pre : kNaN;
          const size_t kl = static_cast<size_t>(tl) * static_cast<size_t>(A) + static_cast<size_t>(a);
          const float mkt = row_mkt(cv.blk, tl);
          rec.close_ex = lb.m[kl] ? static_cast<double>(h2f(lv[kl])) - mkt : kNaN;
          if (has_fac)
            rec.fac_mean = mean_of(rks), rec.fac_close = rank_of_r16(cv.rx[kl]);
          for (size_t j = 0; j < feats.size(); ++j)
            rec.fv.push_back(cv.feat_pl[j] && cv.feat_pl[j]->m[kl] ? static_cast<double>(cv.feat_pl[j]->v[kl]) : kNaN);
        }
      });

  if (rc != Engine::Rc::Ok)
    return {false, std::move(err), {}};

  std::vector<double> sc(static_cast<size_t>(n_days)), sg(static_cast<size_t>(n_days)), sf(static_cast<size_t>(n_days), kNaN);
  std::vector<std::vector<double>> se(feats.size(), std::vector<double>(static_cast<size_t>(n_days), kNaN));
  std::vector<std::string> hs{"日期", "涨跌", "振幅", "成交额 万", "在池行"};
  if (has_fac)
    hs.push_back("因子日均分位"), hs.push_back("收盘分位"), hs.push_back("多/空行");
  hs.push_back("收盘开仓超额 bp");
  for (size_t j = 0; j < feats.size(); ++j)
    hs.push_back("E" + str(j + 1) + " 收盘");
  text::Table tbl(hs);

  for (int i = 0; i < n_days; ++i) {
    const size_t ui = static_cast<size_t>(i);
    const DayRec &r = recs[ui];
    sc[ui] = r.close, sg[ui] = r.chg_d, sf[ui] = r.fac_mean;
    std::vector<std::string> row{date_of(dates[static_cast<size_t>(d0 + i)]), chg(r.chg_d), pct(r.amp), wan(r.amean), std::to_string(r.in)};
    if (has_fac)
      row.push_back(rk(r.fac_mean)), row.push_back(rk(r.fac_close)), row.push_back(std::to_string(r.up) + "/" + std::to_string(r.dn));
    row.push_back(bp(r.close_ex));
    for (size_t j = 0; j < feats.size(); ++j) {
      const double v = j < r.fv.size() ? r.fv[j] : kNaN;
      se[j][ui] = v;
      row.push_back(num(v));
    }
    tbl.add(row);
  }

  std::string s = period_note(dates, env) + "## " + code + " · " + date_of(dates[static_cast<size_t>(d0)]) + " ~ " + date_of(dates[static_cast<size_t>(d1)]) + " (" +
                  std::to_string(n_days) + " 日) · " + env.level + "\n";
  for (size_t i = 0; i < feats.size(); ++i)
    s += expr_line(i, feats[i]);
  s += "\n- 收盘价 " + spark(sc) + "\n- 当日涨跌 " + spark(sg, chg) + "\n";
  if (has_fac)
    s += "- 因子 `" + fac.label + "` 日均分位 (高 = 做多端) " + spark(sf, rk) + "\n";
  for (size_t j = 0; j < feats.size(); ++j)
    s += "- E" + str(j + 1) + " 收盘值 " + spark(se[j]) + "\n";
  s += "\n" + tbl.str();
  return {true, std::move(s), {}};
}

// =============================================================================
// 10. view_book (逐笔重放进常驻订单簿, 原地成文, 不暴露中间结构)
// =============================================================================
constexpr int kAmOpen = 9 * 3600 + 15 * 60, kAmClose = 11 * 3600 + 30 * 60, kPmOpen = 13 * 3600, kPmClose = 15 * 3600;
constexpr int kOpenCall = 9 * 3600 + 25 * 60, kCloseCall = 14 * 3600 + 57 * 60;
constexpr int kTradeSecs = (kAmClose - kAmOpen) + (kPmClose - kPmOpen);
constexpr float kBigQuantile = 0.99f;
constexpr size_t kDepthLevels = 5;
constexpr size_t kCheckEvery = 1 << 16;
constexpr size_t kProfile = 8;
constexpr int kBarCells = 12;
constexpr int kSteps[] = {1, 2, 3, 5, 10, 15, 20, 30, 60, 120, 300, 600};

constexpr bool in_session(int clock) { return (clock >= kAmOpen && clock < kAmClose) || (clock >= kPmOpen && clock < kPmClose); }
constexpr int trade_sec(int clock) {
  return clock < kAmOpen    ? 0
         : clock < kAmClose ? clock - kAmOpen
         : clock < kPmOpen  ? kAmClose - kAmOpen
         : clock < kPmClose ? kAmClose - kAmOpen + clock - kPmOpen
                            : kTradeSecs - 1;
}
constexpr int trade_clock(int sec) { return sec < kAmClose - kAmOpen ? kAmOpen + sec : kPmOpen + sec - (kAmClose - kAmOpen); }

int parse_clock(const std::string &s) {
  if (!hm_shape(s) || !(s.size() == 5 || (s.size() == 8 && s[5] == ':' && std::isdigit(static_cast<unsigned char>(s[6])) && std::isdigit(static_cast<unsigned char>(s[7])))))
    return -1;
  const int h = std::stoi(s.substr(0, 2)), m = std::stoi(s.substr(3, 2)), x = s.size() == 8 ? std::stoi(s.substr(6, 2)) : 0;
  return h < 24 && m < 60 && x < 60 ? h * 3600 + m * 60 + x : -1;
}
std::string hms(int c) {
  char b[16];
  std::snprintf(b, sizeof(b), "%02d:%02d:%02d", c / 3600, c / 60 % 60, c % 60);
  return b;
}
int step_of(int want, int secs) {
  if (want > 0)
    return want;
  for (const int s : kSteps)
    if ((secs + s - 1) / s <= kBookRows)
      return s;
  return kSteps[std::size(kSteps) - 1];
}
std::string w4(double yuan) {
  const double v = yuan / 1e4;
  return v == 0.0 ? "0" : std::fabs(v) < 10.0 ? fmt("%.1f", v)
                                              : fmt("%.0f", v);
}
std::string px2(double p) { return p > 0.0 ? fmt("%.2f", p) : "—"; }

int tick_clock(uint32_t tick) { return L2::tick_hour(tick) * 3600 + L2::tick_minute(tick) * 60 + L2::tick_second(tick); }
int tick_cs(uint32_t tick) { return L2::tick_millisecond(tick); }
int tick_sec(uint32_t tick) { return trade_sec(tick_clock(tick)); }
float life_of(uint32_t a, uint32_t b) { return std::max(0.f, static_cast<float>(tick_sec(b) - tick_sec(a)) + 0.01f * static_cast<float>(tick_cs(b) - tick_cs(a))); }
bool continuous(L2::MarketState s) { return s == L2::MarketState::CONTINUOUS_TRADING_MORNING || s == L2::MarketState::CONTINUOUS_TRADING_AFTERNOON; }

float quantile_of(const L2::FrameView &v, float q) {
  std::vector<float> amt;
  for (size_t i = 0; i < v.head.count; ++i)
    if (const L2::Order &o = v.orders[i]; o.order_type == L2::OrderType::MAKER && o.price > 0)
      amt.push_back(static_cast<float>(o.price) * 0.01f * static_cast<float>(o.volume));
  if (amt.empty())
    return std::numeric_limits<float>::max();
  const auto k = amt.begin() + static_cast<std::ptrdiff_t>(q * static_cast<float>(amt.size() - 1));
  std::nth_element(amt.begin(), k, amt.end());
  return *k;
}

enum BookKind : uint8_t { BkNew,
                          BkCancel,
                          BkFilled,
                          BkSweep,
                          kBookKindCount };
constexpr const char *kBookKindCn[kBookKindCount] = {"新挂", "撤单", "被吃完", "扫单"};

ViewOut book_view(const Call &c, const ViewEnv &env, Engine::FactorFlow &F, ViewRunner::BookRes &R) {
  TraceN("AgentBookReplay");
  std::string err, code;
  uint32_t asset_id = 0;
  if (!resolve_code(c.str("code"), env, code, asset_id, err))
    return {false, std::move(err), {}};
  const int date = ymd_of(c.str("date"));
  if (date == 0)
    return {false, "date: 日期写成 YYYY-MM-DD", {}};
  const std::string w = c.str("time");
  const size_t dash = w.find('-');
  const int from = dash == std::string::npos ? -1 : parse_clock(w.substr(0, dash));
  const int to = dash == std::string::npos ? -1 : parse_clock(w.substr(dash + 1));
  if (from < 0 || to < 0)
    return {false, "time: 写成 HH:MM-HH:MM 或 HH:MM:SS-HH:MM:SS (如 10:00-10:30)", {}};
  if (from >= to || !in_session(from) || !in_session(to - 1))
    return {false, "time: " + w + " 须起 < 止, 两端在 09:15–11:30 / 13:00–15:00 内 (止是开区间, 可写 11:30 / 15:00)", {}};
  const int n_secs = trade_sec(to - 1) + 1 - trade_sec(from);
  if (n_secs > kBookMins * 60)
    return {false, "time: " + w + " 有 " + std::to_string((n_secs + 59) / 60) + " 分钟交易时间, 至多 " + std::to_string(kBookMins) + " 分钟; 收窄窗口", {}};
  if (const int st = c.num("step"); st > 0 && (n_secs + st - 1) / st > kBookRows)
    return {false, "step: 窗口 " + std::to_string(n_secs) + " 秒按每行 " + std::to_string(st) + " 秒有 " + std::to_string((n_secs + st - 1) / st) + " 行, 至多 " + std::to_string(kBookRows) + " 行; 加大 step 或给 0 自动", {}};
  int at_clock = to - 1;
  if (const std::string a = c.str("at"); !a.empty()) {
    at_clock = parse_clock(a);
    if (at_clock < from || at_clock >= to || !in_session(at_clock))
      return {false, "at: 写成窗口内交易时段里的 HH:MM:SS", {}};
  }

  assert(env.orders && "逐笔帧目录未扫描");
  std::string head_s = "## " + code + " · " + date_of(date) + " · " + hms(from) + "–" + hms(to) + " 逐笔重放";
  const size_t didx = env.orders->day_idx(date);
  const FrameRef &ref = env.orders->at(didx, asset_id);
  L2::FrameView view;
  if (!ref.empty())
    view = DayReader(date).get(ref, R.decoder);
  if (view.head.count == 0) {
    const char *why = didx == OrdersCatalog::kNoDay ? "逐笔日文件里没有这一天 (非交易日, 或该日未编码)"
                      : ref.tombstone()             ? "当日该股逐笔为空 (停牌或没有委托)"
                                                    : "当日日文件里没有该股 (未上市 / 已退市, 或未编码)";
    return {false, head_s + "\n没重放成: " + why, {}};
  }

  const int s0 = trade_sec(from), s1 = trade_sec(to - 1) + 1, at_s = trade_sec(at_clock);
  const size_t n_ord = view.head.count;
  F.prog.begin(Phase::Running, n_ord);
  const float big = c.num("big") > 0 ? static_cast<float>(c.num("big") * 1e4) : quantile_of(view, kBigQuantile);

  struct BookSec {
    int clock = 0;
    float bid = 0, ask = 0, depth[2] = {}, ref = 0, matched = 0, imbalance = 0;
    float taker[2] = {}, maker[2] = {}, cancel[2] = {}, last = 0;
  };
  struct BookEvent {
    int clock = 0, cs = 0;
    BookKind kind = BkNew;
    int side = 0, levels = 0;
    float price = 0, first = 0, amount = 0, life = -1;
  };

  float day_open = 0, day_high = 0, day_low = 0, day_close = 0;
  double day_amt = 0;
  std::vector<BookSec> secs(static_cast<size_t>(s1 - s0));
  for (int t = s0; t < s1; ++t)
    secs[static_cast<size_t>(t - s0)].clock = trade_clock(t);
  std::vector<BookEvent> events;
  std::vector<std::pair<float, float>> profile;

  LimitOrderBook &lob = R.lob;
  lob.bind(&R.tick_data, asset_id, env.exchange);
  lob.begin_frame(view);
  constexpr size_t N = L2::LOB_DEPTH;
  const LOB_Feature &lf = R.tick_data.lob;
  const auto sec_at = [&](int t) -> BookSec & { return secs[static_cast<size_t>(t - s0)]; };

  BookSec cur;
  int settled = s0;
  bool booked = false;
  const auto settle_until = [&](int t) {
    if (!booked && t > at_s) {
      lob.for_each_visible_level([&](float price, int32_t net_qty) {
        if (net_qty != 0)
          profile.emplace_back(price, price * static_cast<float>(net_qty));
      });
      booked = true;
    }
    for (; settled < std::min(t, s1); ++settled) {
      BookSec &x = sec_at(settled);
      x.bid = cur.bid, x.ask = cur.ask, x.depth[0] = cur.depth[0], x.depth[1] = cur.depth[1];
      x.ref = cur.ref, x.matched = cur.matched, x.imbalance = cur.imbalance;
    }
  };
  const auto refresh = [&] {
    const Level *b1 = lf.depth_buffer[N], *a1 = lf.depth_buffer[N - 1];
    cur.bid = LOB_Feature::is_real(b1) ? lf.price_of(b1) : 0.f;
    cur.ask = LOB_Feature::is_real(a1) ? lf.price_of(a1) : 0.f;
    for (size_t sd = 0; sd < 2; ++sd) {
      cur.depth[sd] = 0.f;
      for (size_t i = 0; i < kDepthLevels; ++i)
        if (const Level *l = lf.depth_buffer[sd == 0 ? N + i : N - 1 - i]; LOB_Feature::is_real(l))
          cur.depth[sd] += lf.price_of(l) * static_cast<float>(std::abs(l->net_quantity));
    }
    cur.ref = lf.auction_ref_price;
    cur.matched = cur.ref * static_cast<float>(lf.auction_matched_qty);
    cur.imbalance = cur.ref * static_cast<float>(lf.auction_imbalance);
  };
  const auto in_win = [&](int t) { return t >= s0 && t < s1; };
  const auto push_ev = [&](uint32_t tick, BookKind k, int side, int levels, float price, float first, float amount, float life) {
    events.push_back({tick_clock(tick), tick_cs(tick), k, side, levels, price, first, amount, life});
  };
  struct Sweep {
    bool on = false;
    uint32_t id = 0, tick = 0;
    int side = 0, levels = 0;
    float first = 0.f, last = 0.f;
    double amount = 0.0;
  } sw;
  const auto close_sweep = [&] {
    if (sw.on && in_win(tick_sec(sw.tick)) && (sw.levels >= 2 || sw.amount >= big))
      push_ev(sw.tick, BkSweep, sw.side, sw.levels, sw.last, sw.first, static_cast<float>(sw.amount), -1.f);
    sw.on = false;
  };

  for (size_t i = 0; i < n_ord; ++i) {
    if (i > 0 && i % kCheckEvery == 0) {
      if (F.cancel.load(std::memory_order_relaxed)) {
        lob.unbind();
        return {};
      }
      F.prog.publish(kCheckEvery);
    }
    const L2::Order &o = view.orders[i];
    const int t = tick_sec(o.tick);
    settle_until(t);
    lob.process(o);
    if (lf.depth_updated)
      refresh();
    const bool win = in_win(t);
    const uint8_t sd = static_cast<uint8_t>(o.order_dir);
    const float px = static_cast<float>(o.price) * 0.01f, vol = static_cast<float>(o.volume);
    BookSec *x = win ? &sec_at(t) : nullptr;
    switch (o.order_type) {
    case L2::OrderType::MAKER:
      close_sweep();
      if (win) {
        x->maker[sd] += px * vol;
        if (px > 0.f && px * vol >= big)
          push_ev(o.tick, BkNew, sd, 0, px, px, px * vol, -1.f);
      }
      break;
    case L2::OrderType::CANCEL: {
      close_sweep();
      if (!win)
        break;
      const float p = px > 0.f ? px : lf.ord_price[sd];
      x->cancel[sd] += p * vol;
      if (p > 0.f && p * vol >= big)
        push_ev(o.tick, BkCancel, sd, 0, p, p, p * vol, ord_reliable(lf, sd) ? life_of(lf.ord_tick[sd], o.tick) : -1.f);
      break;
    }
    case L2::OrderType::TAKER: {
      day_open = day_open > 0.f ? day_open : px;
      day_high = std::max(day_high, px);
      day_low = day_low > 0.f ? std::min(day_low, px) : px;
      day_close = px;
      day_amt += static_cast<double>(px) * vol;
      const uint8_t p = static_cast<uint8_t>(1 - sd);
      if (win) {
        x->taker[sd] += px * vol, x->last = px;
        if (ord_reliable(lf, p) && lf.ord_rest[p] <= o.volume)
          if (const float a = lf.ord_price[p] * static_cast<float>(lf.ord_orig[p]); a >= big)
            push_ev(o.tick, BkFilled, p, 0, lf.ord_price[p], lf.ord_price[p], a, life_of(lf.ord_tick[p], o.tick));
      }
      const uint32_t aid = sd == 0 ? o.bid_order_id : o.ask_order_id;
      if (sw.on && sw.id != aid)
        close_sweep();
      if (aid == 0 || !continuous(lf.market_state))
        break;
      if (!sw.on)
        sw = {true, aid, o.tick, sd, 1, px, px, 0.0};
      else if (px != sw.last)
        ++sw.levels, sw.last = px;
      sw.amount += static_cast<double>(px) * vol;
      break;
    }
    default:
      break;
    }
  }
  close_sweep();
  settle_until(s1);
  lob.unbind();
  std::stable_sort(events.begin(), events.end(), [](const BookEvent &a, const BookEvent &b) { return std::pair(a.clock, a.cs) < std::pair(b.clock, b.cs); });

  // ---- 成文 ----
  const int n = static_cast<int>(secs.size()), st = step_of(c.num("step"), n), R_rows = (n + st - 1) / st;
  std::string s = head_s + " (额 万元; 价差 / 距盘口按 0.01 元一跳)\n";
  s += "当日: 逐笔 " + str(n_ord) + " 条 · " +
       (day_close > 0 ? "开 " + px2(day_open) + " 高 " + px2(day_high) + " 低 " + px2(day_low) + " 收 " + px2(day_close) + " · 成交额 " + w4(day_amt) : std::string("无成交")) +
       " · 大单门槛 " + w4(big) + (c.num("big") > 0 ? " (给定)" : " (当日新挂单额 " + fmt("%.0f", kBigQuantile * 100.0) + " 分位)") + "\n";

  double tk[2] = {}, mk[2] = {}, cx[2] = {}, spread = 0.0, cum = 0.0;
  int nsp = 0;
  float first = 0.f, carry = 0.f;
  std::vector<double> lastp(static_cast<size_t>(n), kNaN), net(static_cast<size_t>(n));
  for (int i = 0; i < n; ++i) {
    const BookSec &x = secs[static_cast<size_t>(i)];
    for (int e = 0; e < 2; ++e)
      tk[e] += x.taker[e], mk[e] += x.maker[e], cx[e] += x.cancel[e];
    if (x.last > 0.f)
      first = first > 0.f ? first : x.last, carry = x.last;
    lastp[static_cast<size_t>(i)] = carry > 0.f ? carry : kNaN;
    cum += x.taker[0] - x.taker[1], net[static_cast<size_t>(i)] = cum / 1e4;
    if (x.bid > 0.f && x.ask > x.bid)
      spread += (x.ask - x.bid) * 100.0, ++nsp;
  }
  s += "窗口: 主动买 " + w4(tk[0]) + " / 主动卖 " + w4(tk[1]) + " · 新挂 买 " + w4(mk[0]) + " / 卖 " + w4(mk[1]) + " · 撤单 买 " + w4(cx[0]) + " / 卖 " + w4(cx[1]) +
       (carry > 0.f ? " · 成交价 " + px2(first) + " → " + px2(carry) : std::string(" · 无成交")) + (nsp ? " · 平均价差 " + fmt("%.1f", spread / nsp) + " 跳" : std::string()) + "\n";
  s += "- 逐秒成交价 " + spark(lastp) + "\n- 累计主动买 − 主动卖 (万) " + spark(net) + "\n";

  const bool call = from < kOpenCall || to > kCloseCall;
  s += "\n逐时表 (每行 " + std::to_string(st) + " 秒; 主买 / 主卖 = 主动成交额, 挂 / 撤 = 新挂 / 撤单额, 五档 = 前 5 价位挂单额" +
       (call ? std::string("; 竞价段: 参考价 / 匹配额 / 未匹配额 (+ 买剩 − 卖剩)") : std::string()) + ")\n";
  std::vector<std::string> ths{"起", "末价", "买一", "卖一", "价差", "主买", "主卖", "挂买", "挂卖", "撤买", "撤卖", "买五档", "卖五档"};
  if (call)
    ths.push_back("参考价"), ths.push_back("匹配"), ths.push_back("未匹配");
  text::Table ttbl(ths);
  for (int r = 0; r < R_rows; ++r) {
    const int i0 = r * st, i1 = std::min(n, i0 + st);
    double t2[2] = {}, m2[2] = {}, c2[2] = {};
    for (int i = i0; i < i1; ++i)
      for (int e = 0; e < 2; ++e) {
        const BookSec &x = secs[static_cast<size_t>(i)];
        t2[e] += x.taker[e], m2[e] += x.maker[e], c2[e] += x.cancel[e];
      }
    const BookSec &z = secs[static_cast<size_t>(i1 - 1)];
    std::vector<std::string> row{hms(secs[static_cast<size_t>(i0)].clock), px2(lastp[static_cast<size_t>(i1 - 1)]), px2(z.bid), px2(z.ask),
                                 z.bid > 0.f && z.ask > 0.f ? fmt("%.0f", std::round((z.ask - z.bid) * 100.0)) : std::string("—"),
                                 w4(t2[0]), w4(t2[1]), w4(m2[0]), w4(m2[1]), w4(c2[0]), w4(c2[1]), w4(z.depth[0]), w4(z.depth[1])};
    if (call)
      row.push_back(px2(z.ref)), row.push_back(w4(z.matched)), row.push_back(w4(z.imbalance));
    ttbl.add(row);
  }
  s += ttbl.str();

  int cnt[kBookKindCount][2] = {};
  double amt[kBookKindCount][2] = {}, lv[2] = {};
  std::vector<double> life[2];
  for (const BookEvent &e : events) {
    ++cnt[e.kind][e.side], amt[e.kind][e.side] += e.amount;
    if (e.kind == BkSweep)
      lv[e.side] += e.levels;
    if (e.kind == BkCancel && e.life >= 0.f)
      life[e.side].push_back(e.life);
  }
  s += "\n大单事件 (窗口内共 " + str(events.size()) + " 起; 侧 = 委托所在侧, 扫单 = 主动方; 寿命 = 挂出到撤 / 吃完的交易秒):\n";
  for (size_t k = 0; k < kBookKindCount; ++k) {
    s += "- " + std::string(kBookKindCn[k]) + ": 买 " + std::to_string(cnt[k][0]) + " 起 " + w4(amt[k][0]) + " / 卖 " + std::to_string(cnt[k][1]) + " 起 " + w4(amt[k][1]);
    if (k == BkSweep && cnt[k][0] + cnt[k][1] > 0)
      s += " · 平均吃 买 " + (cnt[k][0] ? fmt("%.1f", lv[0] / cnt[k][0]) : std::string("—")) + " / 卖 " + (cnt[k][1] ? fmt("%.1f", lv[1] / cnt[k][1]) : std::string("—")) + " 价位";
    if (k == BkCancel && (!life[0].empty() || !life[1].empty()))
      s += " · 寿命中位 买 " + (life[0].empty() ? std::string("—") : fmt("%.1f", median_of(life[0]))) + " / 卖 " +
           (life[1].empty() ? std::string("—") : fmt("%.1f", median_of(life[1]))) + " 秒";
    s += "\n";
  }

  std::vector<size_t> ix(events.size());
  std::iota(ix.begin(), ix.end(), size_t{0});
  const size_t m = std::min(ix.size(), static_cast<size_t>(c.num("n")));
  std::partial_sort(ix.begin(), ix.begin() + static_cast<std::ptrdiff_t>(m), ix.end(), [&](size_t x, size_t y) { return events[x].amount > events[y].amount; });
  ix.resize(m);
  std::sort(ix.begin(), ix.end());
  if (m > 0) {
    s += "\n额最大的 " + str(m) + " 起 (按时序; 距盘口 = 与上一秒末同侧最优价差几跳: 0 在盘口, + 在盘口后方, − 越过盘口):\n";
    text::Table etbl({"时刻", "事件", "侧", "价", "距盘口", "额", "寿命 秒"});
    for (const size_t i : ix) {
      const BookEvent &e = events[i];
      const BookSec &z = secs[static_cast<size_t>(std::clamp(trade_sec(e.clock) - s0 - 1, 0, n - 1))];
      const float best = e.side == 0 ? z.bid : z.ask;
      const bool sweep = e.kind == BkSweep;
      const std::string dist = sweep        ? "吃 " + std::to_string(e.levels) + " 价位"
                               : best > 0.f ? fmt("%+.0f", std::round((e.side == 0 ? best - e.price : e.price - best) * 100.0))
                                            : std::string("—");
      etbl.add({hms(e.clock) + fmt(".%02.0f", e.cs), kBookKindCn[e.kind], e.side == 0 ? "买" : "卖",
                sweep ? px2(e.first) + "→" + px2(e.price) : px2(e.price), dist, w4(e.amount), e.life >= 0.f ? fmt("%.1f", e.life) : "—"});
    }
    s += etbl.str();
  }

  std::vector<std::pair<float, float>> bids, asks;
  double tb = 0.0, ta = 0.0;
  for (const auto &[p, a] : profile)
    if (a > 0.f)
      bids.emplace_back(p, a), tb += a;
    else
      asks.emplace_back(p, -a), ta -= a;
  std::reverse(bids.begin(), bids.end());
  bids.resize(std::min(bids.size(), kProfile)), asks.resize(std::min(asks.size(), kProfile));
  s += "\n全簿剖面 @ " + hms(at_clock) + " 秒末 (全簿 买 " + w4(tb) + " / 卖 " + w4(ta) + "; 列盘口两侧各至多 " + str(kProfile) + " 价位):\n";
  if (bids.empty() && asks.empty())
    return {true, s + "簿空\n", {}};

  float mx = 0.f;
  for (const auto &[p, a] : bids)
    mx = std::max(mx, a);
  for (const auto &[p, a] : asks)
    mx = std::max(mx, a);
  text::Table ptbl({"档", "价", "额", ""});
  const auto add_lvl = [&](const char *side, size_t i, const std::pair<float, float> &x) {
    std::string bar;
    for (long k = std::lround(x.second / mx * kBarCells); k > 0; --k)
      bar += "█";
    ptbl.add({std::string(side) + str(i + 1), px2(x.first), w4(x.second), bar});
  };
  for (size_t i = asks.size(); i-- > 0;)
    add_lvl("卖", i, asks[i]);
  for (size_t i = 0; i < bids.size(); ++i)
    add_lvl("买", i, bids[i]);
  s += ptbl.str();
  return {true, std::move(s), {}};
}

// ---- 观察工具分派表 (下标 = Tool, ViewFeature .. ViewBook 一一对应) ----
struct ViewDef {
  Tool id;
  ViewOut (*fn)(const Call &, const ViewEnv &, Engine::FactorFlow &, ViewRunner &);
};

} // namespace

ViewOut ViewRunner::run(const Call &c, const ViewEnv &env, Engine::FactorFlow &F) {
  static constexpr auto book_entry = +[](const Call &call, const ViewEnv &ve, Engine::FactorFlow &flow, ViewRunner &vr) {
    if (!vr.book_)
      vr.book_ = std::make_unique<BookRes>();
    return book_view(call, ve, flow, *vr.book_);
  };
  static constexpr ViewDef kViews[] = {
      {Tool::ViewFeature, feature_view},
      {Tool::ViewLayers, layers_view},
      {Tool::ViewTrades, trades_view},
      {Tool::ViewHolders, holders_view},
      {Tool::ViewResponse, response_view},
      {Tool::ViewExplain, explain_view},
      {Tool::ViewSnapshot, snapshot_view},
      {Tool::ViewStockDay, stock_day_view},
      {Tool::ViewStockDays, stock_days_view},
      {Tool::ViewBook, book_entry},
  };
  const size_t idx = static_cast<size_t>(c.tool);
  assert(idx < std::size(kViews) && kViews[idx].id == c.tool && "不是观察工具");
  return kViews[idx].fn(c, env, F, *this);
}

std::string ViewRunner::rep_layers(const std::string &expr, int sign, const ViewEnv &env, Engine::FactorFlow &F) {
  const Engine::FeatureTable &ft = *env.fsc.ft;
  FactorSpec fac{.e = ft.reparse(expr), .dir = st::dir_of(sign), .label = expr};
  std::string dummy;
  [[maybe_unused]] const bool ok_rf = fx::root_frame(fac.e, ft.seg, fac.frame, dummy);
  assert(ok_rf);
  const int seg = ft.seg;
  const std::vector<Slot> ps = parts_of(seg, env.open, env.clock);
  const std::vector<int> pr = part_of_row(ps, seg);
  Layer all;
  std::vector<Layer> bs(ps.size());
  std::vector<misc::Ymd> dates;
  Engine::Job job;
  std::string err;
  StreamCfg cfg{.fac = &fac};
  const Engine::Rc rc = run_stream(
      env, F, cfg, dates, job, err,
      [&](const std::vector<misc::Ymd> &, size_t &, size_t &, std::string &) { return true; },
      [&](const ChunkView &cv) {
        for (int r = 0; r < cv.t_hi - cv.t_lo; ++r) {
          all.add(cv.rows[static_cast<size_t>(r)]);
          if (seg > 1)
            bs[static_cast<size_t>(pr[static_cast<size_t>((cv.u0 + r) % seg)])].add(cv.rows[static_cast<size_t>(r)]);
        }
      });
  if (rc != Engine::Rc::Ok)
    return err.empty() ? std::string() : "- 代表点" + err + "\n";
  std::string s = "- 代表点的 20 组每期超额 G1 → G20: " + spark(all.grps(), bp) + " bp · mono " + r3(all.mono()) + " · 多空 " + bp(all.ls()) + "bp\n";
  if (seg > 1) {
    s += "- 代表点分时段 mono:";
    for (size_t j = 0; j < ps.size(); ++j)
      s += (j ? " · " : " ") + ps[j].name + " " + r3(bs[j].mono());
    s += "\n";
  }
  return s + "- 观察工具的 factor 写本候选名即看代表点\n";
}

} // namespace GUI::Agent
