#pragma once

// =============================================================================
// Kernel — 观察核的公共件 (10 个 view_* 共用): 格式与日期 / 统计 / 日内时段 / 分层累加器 / K 线直读 / 盘面标签 / 参数解析 / 分块流式观察核
// =============================================================================
//   run_stream: 装载前核日期 → 跳过无关块 → 块内全核 DAG + Stat → 交 ChunkView 原地归约 (零中间 [T][A] 平面搬运)
//   各 view_* 的入口 (Call, ViewEnv, FactorFlow, ViewRunner) → ToolOut, 结果结构见 tools/Look.hpp; 分派表在 Views.cpp
// =============================================================================

#include "gui/task_agent/Config.hpp"
#include "gui/task_agent/Text.hpp"
#include "gui/task_agent/Views.hpp"
#include "gui/task_agent/tools/Look.hpp"

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
#include <iterator>
#include <numeric>
#include <span>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

namespace GUI::Agent {

// 逐笔重放的常驻资源 (view_book 首次用时建)
struct ViewRunner::Replay {
  L2::FrameDecoder decoder;
  LimitOrderBook lob;
  TickData tick_data;
  Replay() : lob(L2::LOB_ORDER_CAPACITY) {}
};

ToolOut ViewFeature(const Call &c, const ViewEnv &env, Engine::FactorFlow &F);
ToolOut ViewLayers(const Call &c, const ViewEnv &env, Engine::FactorFlow &F);
ToolOut ViewTrades(const Call &c, const ViewEnv &env, Engine::FactorFlow &F);
ToolOut ViewHolders(const Call &c, const ViewEnv &env, Engine::FactorFlow &F);
ToolOut ViewResponse(const Call &c, const ViewEnv &env, Engine::FactorFlow &F);
ToolOut ViewExplain(const Call &c, const ViewEnv &env, Engine::FactorFlow &F);
ToolOut ViewSnapshot(const Call &c, const ViewEnv &env, Engine::FactorFlow &F);
ToolOut ViewStockDay(const Call &c, const ViewEnv &env, Engine::FactorFlow &F);
ToolOut ViewStockDays(const Call &c, const ViewEnv &env, Engine::FactorFlow &F);
ToolOut ViewBook(const Call &c, const ViewEnv &env, Engine::FactorFlow &F, ViewRunner::Replay &R);

namespace look {

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

inline constexpr int kGroups = st::kGroups;
static_assert(kGroups == kGroupN);
inline constexpr int kG1 = 0, kG20 = kGroups - 1;
inline constexpr int kTails = Engine::kTails;
inline constexpr double kNaN = NAN;
inline constexpr size_t kSample = 200000;
inline constexpr size_t kCsCells = 2000000;
inline constexpr int kCorrRows = 400;
inline constexpr size_t kBase = 40000;
inline constexpr int kMinCs = 20;
inline constexpr size_t kEvents = 20000;
inline constexpr int kExplainRows = 3000;
inline constexpr double kIntraNone = 0.005;
inline constexpr size_t kGistExpr = 40;
inline constexpr int kHistBins = 40;

// ---- 格式与日期 ----
inline std::string pos2(double x) { return std::isfinite(x) ? fmt("%.2f", x) : "—"; }
inline std::string rk(double x) { return std::isfinite(x) ? fmt("%.0f", x * 100.0) : "—"; }
inline std::string str(size_t n) { return std::to_string(n); }
inline std::string gname(int k) { return "G" + std::to_string(k + 1); }

inline std::string date_of(int ymd) {
  char b[16];
  std::snprintf(b, sizeof(b), "%04d-%02d-%02d", ymd / 10000, ymd / 100 % 100, ymd % 100);
  return b;
}
inline int ymd_of(const std::string &s) {
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
inline int day_index(const std::vector<misc::Ymd> &dates, const std::string &arg, std::string &err) {
  const int ymd = ymd_of(arg);
  const auto it = std::find(dates.begin(), dates.end(), ymd);
  if (it == dates.end()) {
    err = "日期 " + arg + " 不是观察的日段 (" + date_of(dates.front()) + " ~ " + date_of(dates.back()) + ") 里的交易日";
    return -1;
  }
  return static_cast<int>(it - dates.begin());
}

inline float rank_of_r16(uint16_t rx) { return rx == st::kRankNone ? NAN : static_cast<float>(rx) / st::kRankMax; }
inline int grp_of_r16(uint16_t rx) { return rx == st::kRankNone ? -1 : st::grp_of(rx); }

// ---- 统计公共件 ----
inline double mean_of(const std::vector<double> &v) {
  double s = 0.0;
  size_t n = 0;
  for (const double x : v)
    if (std::isfinite(x))
      s += x, ++n;
  return n ? s / static_cast<double>(n) : kNaN;
}
inline double median_of(std::vector<double> v) {
  std::erase_if(v, [](double x) { return !std::isfinite(x); });
  if (v.empty())
    return kNaN;
  const auto mid = v.begin() + static_cast<std::ptrdiff_t>(v.size() / 2);
  std::nth_element(v.begin(), mid, v.end());
  return *mid;
}
inline double quant(const std::vector<double> &sorted, double p) {
  if (sorted.empty())
    return kNaN;
  const double x = p * static_cast<double>(sorted.size() - 1);
  const size_t i = static_cast<size_t>(x);
  return i + 1 < sorted.size() ? sorted[i] + (sorted[i + 1] - sorted[i]) * (x - static_cast<double>(i)) : sorted[i];
}
inline std::vector<double> ranks_of(const std::vector<double> &x) {
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
inline double pearson(const std::vector<double> &a, const std::vector<double> &b) {
  const double ma = std::accumulate(a.begin(), a.end(), 0.0) / static_cast<double>(a.size());
  const double mb = std::accumulate(b.begin(), b.end(), 0.0) / static_cast<double>(b.size());
  double sab = 0.0, saa = 0.0, sbb = 0.0;
  for (size_t i = 0; i < a.size(); ++i)
    sab += (a[i] - ma) * (b[i] - mb), saa += (a[i] - ma) * (a[i] - ma), sbb += (b[i] - mb) * (b[i] - mb);
  return saa > 0.0 && sbb > 0.0 ? sab / std::sqrt(saa * sbb) : kNaN;
}
inline double spearman(const std::vector<double> &a, const std::vector<double> &b) { return a.size() >= 3 ? pearson(ranks_of(a), ranks_of(b)) : kNaN; }

// 升序样本在 [p1, p99] 间等宽分箱的频数 (界写进 lo / hi)
inline std::vector<double> hist_of(const std::vector<double> &sorted, double &lo, double &hi) {
  lo = quant(sorted, 0.01), hi = quant(sorted, 0.99);
  std::vector<double> h(static_cast<size_t>(kHistBins), 0.0);
  if (!(hi > lo))
    return h;
  for (const double x : sorted)
    if (x >= lo && x <= hi)
      h[static_cast<size_t>(std::min(kHistBins - 1, static_cast<int>((x - lo) / (hi - lo) * kHistBins)))] += 1.0;
  return h;
}

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
inline std::string wan(double sum, size_t n) { return n ? fmt("%.0f", sum / 1e4) : "—"; }

// ---- 日内时段 ----
struct Slot {
  int k0, k1;
  std::string name;
};
inline int part_rows(int seg, int open) { return std::max(1, (seg - open) / 8); }
inline std::vector<Slot> parts_of(int seg, int open, const std::vector<std::string> &clock) {
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
inline std::vector<int> part_of_row(const std::vector<Slot> &ps, int seg) {
  std::vector<int> o(static_cast<size_t>(seg));
  for (size_t i = 0; i < ps.size(); ++i)
    for (int k = ps[i].k0; k < ps[i].k1; ++k)
      o[static_cast<size_t>(k)] = static_cast<int>(i);
  return o;
}

inline bool hm_shape(const std::string &s, size_t at = 0) {
  return s.size() >= at + 5 && s[at + 2] == ':' && std::isdigit(static_cast<unsigned char>(s[at])) && std::isdigit(static_cast<unsigned char>(s[at + 1])) &&
         std::isdigit(static_cast<unsigned char>(s[at + 3])) && std::isdigit(static_cast<unsigned char>(s[at + 4]));
}
inline bool rows_in(const char *key, const std::string &s, const std::vector<std::string> &clock, int cap, int &k0, int &k1, std::string &error) {
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
inline int row_at(const std::vector<std::string> &clock, const std::string &hm) {
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

inline LayerRow row_of(std::string name, const Layer &b) { return {std::move(name), b.rows, b.ok, b.mono(), b.ls(), b.ric.mean(), b.grps()}; }

inline bool approx_row(const std::vector<double> &x, const std::vector<double> &y, st::Row &w) {
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

// 分层行的原文表 (列: 名 / 行 / 有统计 / mono / 多空 / rIC / G1 / G10 / G20 / 20 组火花线)
inline text::Table layer_table(const char *first) { return text::Table({first, "行", "有统计", "mono", "多空 bp", "rIC", "G1 bp", "G10 bp", "G20 bp", "20 组"}); }
inline void add_layer_row(text::Table &tbl, const LayerRow &b) {
  tbl.add({b.name, std::to_string(b.rows), std::to_string(b.ok), r3(b.mono), bp(b.ls), r3(b.ric), bp(b.grp[kG1]), bp(b.grp[kGroups / 2 - 1]), bp(b.grp[kG20]),
           "`" + bars(b.grp) + "`"});
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
inline constexpr uint8_t bit(Bar b) { return static_cast<uint8_t>(1u << b); }
inline constexpr uint8_t kAllBars = 0xFFu;

inline float raw_of(ts::TfId tf, float y) {
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
inline constexpr TagDef kTags[] = {{"竞价段 (09:15–09:30 的行)", "竞价"},
                                   {"尾盘最后一段", "尾盘"},
                                   {"近涨停 (收 ≥ 涨停价 × 0.995)", "近涨停"},
                                   {"近跌停 (收 ≤ 跌停价 × 1.005)", "近跌停"},
                                   {"当日涨 > 5%", "大涨"},
                                   {"当日跌 > 5%", "大跌"},
                                   {"日内高位 (当日高低区间 ≥ 0.8)", "高位"},
                                   {"日内低位 (≤ 0.2)", "低位"},
                                   {"放量 (≥ 当日此前有成交行均量 3 倍)", "放量"}};
inline constexpr int kTagCount = static_cast<int>(std::size(kTags));

struct Mark {
  uint32_t m = 0;
  double chg = kNaN, pos = kNaN;
  void set(Tag t) { m |= 1u << static_cast<int>(t); }
};
inline Mark mark_of(const Px &px, int t, int a) {
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
inline std::string tags_of(uint32_t m) {
  std::string s;
  for (int j = 0; j < kTagCount; ++j)
    if (m >> j & 1u)
      s += (s.empty() ? "" : " ") + std::string(kTags[j].brief);
  return s;
}

// ---- 参数校验与解析 ----
inline std::string closed_err(const ViewEnv &env, const Engine::FeatureTable &ft, const fx::Expr &e) {
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

inline bool parse_concrete(const Engine::FeatureTable &ft, const std::string &src, fx::Expr &e, std::string &err) {
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

inline bool resolve_factor(const std::string &arg, const ViewEnv &env, FactorSpec &out, std::string &err) {
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

inline bool resolve_feats(const std::vector<std::string> &xs, const ViewEnv &env, std::vector<FeatSpec> &out) {
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

inline bool resolve_code(const std::string &arg, const ViewEnv &env, std::string &code, uint32_t &asset_id, std::string &err) {
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

inline bool date_arg_ok(const Call &c, const char *k, std::string &err) {
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
inline float row_mkt(const Engine::Block &blk, int t) {
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

// 全期观察的整期范围 (不截日段)
inline bool whole(const std::vector<misc::Ymd> &, size_t &, size_t &, std::string &) { return true; }

inline std::string period_note(const std::vector<misc::Ymd> &dates, const ViewEnv &env) {
  if (dates.front() == env.first && dates.back() == env.last)
    return {};
  return "注意: 本观察的日段为 " + date_of(dates.front()) + " ~ " + date_of(dates.back()) + " (" + str(dates.size()) +
         " 天), 与评估期不同 (用到的叶落盘日更短), 与别的观察不可直接比\n\n";
}

inline std::string expr_line(size_t i, const FeatSpec &f, const char *miss = "没算成") {
  return "E" + str(i + 1) + " = `" + f.raw + "`" + (f.err.empty() ? "" : std::string(" (") + miss + ": " + f.err + ")") + "\n";
}

// 块内逐格超额对数步长 (F.S.ex 全核并行; view_response / view_explain)
inline std::vector<float> block_excess_steps(misc::exec::ForkJoin &ex, const Engine::Block &blk, const Px &px, int u_blk0) {
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

inline double cum_step(const std::vector<float> &e, int A, int a, int t0, int to) {
  double s = 0.0;
  for (int u = std::min(t0, to) + 1; u <= std::max(t0, to); ++u)
    s += e[static_cast<size_t>(u) * static_cast<size_t>(A) + static_cast<size_t>(a)];
  return s;
}

} // namespace look
} // namespace GUI::Agent
