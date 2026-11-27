#pragma once

// =============================================================================
// Kernel — 观察核的公共件 (10 个 view_* 共用): 格式与日期 / 统计 / 日内时段 / 分层累加器 / K 线直读 / 盘面标签 / 参数解析与日段 / 分块流式观察核
// =============================================================================
//   run_stream: 装载前核日段 → 跳过无关块 → 块内 Engine::ForestPass (全核 DAG + Stat 进 LookP) → 段内的行交 ChunkView 原地归约 (零中间 [T][A] 平面搬运)
//   各 view_* 的入口 (Call, Bay) → ToolOut 与结果结构见 tools/Look.hpp, 实现表在 tools/Kit.cpp
// =============================================================================

#include "gui/task_agent/Text.hpp"
#include "gui/task_agent/tools/Bay.hpp"
#include "gui/task_agent/tools/Look.hpp"

#include "factor/Expr.hpp"
#include "factor/Search/Space.hpp"
#include "factor/Stat/Fold.hpp"
#include "factor/Stat/Host.hpp"
#include "factor/Stat/Pos.hpp"
#include "fundamental/AssetAxis.hpp"
#include "gui/engine/Engine.hpp"
#include "misc/date.hpp"
#include "misc/profiler.hpp"

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
#include <utility>
#include <vector>

namespace GUI::Agent {

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
inline constexpr int kAllGroups = -2; // FactorSpec::only_end: 全部组的真持有格
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
    err = "日期 " + arg + " 不是观察的日段 (" + misc::dash(dates.front()) + " ~ " + misc::dash(dates.back()) + ") 里的交易日";
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
inline double pearson(std::span<const double> a, std::span<const double> b) {
  const double ma = std::accumulate(a.begin(), a.end(), 0.0) / static_cast<double>(a.size());
  const double mb = std::accumulate(b.begin(), b.end(), 0.0) / static_cast<double>(b.size());
  double sab = 0.0, saa = 0.0, sbb = 0.0;
  for (size_t i = 0; i < a.size(); ++i)
    sab += (a[i] - ma) * (b[i] - mb), saa += (a[i] - ma) * (a[i] - ma), sbb += (b[i] - mb) * (b[i] - mb);
  return saa > 0.0 && sbb > 0.0 ? sab / std::sqrt(saa * sbb) : kNaN;
}

// 子截面的并列均秩 rv (rank_into, 0 起: rv = less + (eq − 1) / 2) → Stat 同口径组号 (r16_of 的同一整数式 → grp_of)
inline int grp_of_rank(double rv, size_t n) {
  assert(n >= 2);
  const long long num = std::llround(2.0 * rv) * st::kRankMax, den = 2LL * (static_cast<long long>(n) - 1);
  return st::grp_of(static_cast<unsigned>((2 * num + den) / (2 * den)));
}

// 线程内复用的零堆分配秩 / 相关 / 近似分层工作区
struct RankScratch {
  std::vector<size_t> ix;
  std::vector<double> a, b, tmp;

  void rank_into(std::span<const double> x, std::vector<double> &out, bool center = false) {
    const size_t n = x.size();
    ix.resize(n), out.resize(n);
    std::iota(ix.begin(), ix.end(), size_t{0});
    std::sort(ix.begin(), ix.end(), [&](size_t i, size_t j) { return x[i] < x[j]; });
    const double shift = center ? 0.5 * static_cast<double>(n - 1) : 0.0;
    for (size_t i = 0; i < n;) {
      size_t j = i;
      while (j + 1 < n && x[ix[j + 1]] == x[ix[i]])
        ++j;
      const double rv = 0.5 * static_cast<double>(i + j) - shift;
      for (size_t k = i; k <= j; ++k)
        out[ix[k]] = rv;
      i = j + 1;
    }
  }
  double spearman(std::span<const double> x, std::span<const double> y) {
    if (x.size() < 3)
      return kNaN;
    rank_into(x, a), rank_into(y, b);
    return pearson(a, b);
  }
  // 子截面分层: 格按 rx 的并列均秩 (rank_into, 0 起) 经 Stat 同一式 (r16_of → grp_of) 分 20 组, 组超额只收全截面分组下真持有的格
  //   (held; hy = 其做多收益 − mkt, 续持取名义值, Stat 仓位感知); 组收益 = Σ hy / max(真持有只数, 子截面满仓 n / 20) (与 Stat【持仓口径】同式);
  //   ric 用全部格 (同 Row)
  bool approx_ranked(std::span<const double> rx, std::span<const double> hy, std::span<const double> ry, std::span<const uint8_t> held, st::Row &w) const {
    const size_t n = rx.size();
    assert(hy.size() == n && ry.size() == n && held.size() == n);
    if (n < static_cast<size_t>(kMinCs))
      return false;
    w = {};
    double s[kGroups] = {};
    for (size_t i = 0; i < n; ++i) {
      const int k = grp_of_rank(rx[i], n);
      if (held[i])
        s[k] += hy[i], ++w.cnt[k];
    }
    const double cap = static_cast<double>(n) / kGroups;
    for (int k = 0; k < kGroups; ++k)
      w.grp[k] = static_cast<float>(s[k] / st::book_den(w.cnt[k], cap));
    w.ric = static_cast<float>(pearson(rx, ry));
    w.ok = 1;
    return true;
  }
  // y = 做多标签超额 (只定 ric 的秩), hy / held 同 approx_ranked
  bool approx_row(std::span<const double> x, std::span<const double> y, std::span<const double> hy, std::span<const uint8_t> held, st::Row &w) {
    if (x.size() < static_cast<size_t>(kMinCs))
      return false;
    rank_into(x, a), rank_into(y, b);
    return approx_ranked(a, hy, b, held, w);
  }
};

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
// 抽样行的步长: 至少 want, 且与每日行数 seg 互素 (有公因子时抽到的行只落在固定几个时段上)
inline int row_step(int want, int seg) {
  int s = std::max(1, want);
  while (std::gcd(s, seg) != 1)
    ++s;
  return s;
}
// 全局统计行 u 开仓: 段末尾部 (清仓点越过收盘竞价前最后可成交锚点, Hold::opens) 的行回测不开仓, 观察里的标签统计同样不计
inline bool opens(int u, const ViewEnv &env) { return hold_on(env.hold, geo_of(env.fsc.ft->seg)).opens(u); }
inline int row_at(const std::vector<std::string> &clock, const std::string &hm) {
  if (clock.size() == 1)
    return 0;
  const auto it = std::find(clock.begin(), clock.end(), hm);
  return it == clock.end() ? -1 : static_cast<int>(it - clock.begin());
}

// 分层行 (行和 = Stat 的唯一累加 Sum, 与三级同分母)
inline LayerRow row_of(std::string name, const st::Sum &b) {
  std::vector<double> g(kGroups);
  for (int k = 0; k < kGroups; ++k)
    g[static_cast<size_t>(k)] = b.grp_of(k);
  return {std::move(name), b.h.rows, b.ok(), b.mono(), b.ls_of(), b.ric_mean(), std::move(g)};
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
  bool tails = false, cells = false; // cells = 要真持有的格 (行核 walk 时同出)
  int only_end = -1;                 // 格的范围: kAllGroups = 全部组, -1 = 两端, 0 = kTop, 1 = kBot
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
      !(err = env.opened.closed_err(ft, out.e)).empty()) {
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
      f.err = env.opened.closed_err(ft, f.e);
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

// 观察日段 from / to (统计全期的 view_* 同一对参数; 含两端, 不必是交易日): 作 run_stream 的日段核定, 定下统计日下标 [lo, hi)
struct Window {
  std::string from, to;
  int ymd0 = 0, ymd1 = 99999999;
  size_t lo = 0, hi = 0;
  bool cut() const { return !from.empty() || !to.empty(); }
  std::string text() const { return (from.empty() ? "…" : from) + " ~ " + (to.empty() ? "…" : to); }
  bool operator()(const std::vector<misc::Ymd> &ds, size_t &l, size_t &h, std::string &why) {
    l = ds.size(), h = 0;
    for (size_t d = 0; d < ds.size(); ++d)
      if (ds[d] >= ymd0 && ds[d] <= ymd1)
        l = std::min(l, d), h = d + 1;
    if (l >= h) {
      why = "from / to: " + text() + " 里没有交易日 (观察的日段 " + misc::dash(ds.front()) + " ~ " + misc::dash(ds.back()) + ")";
      return false;
    }
    lo = l, hi = h;
    return true;
  }
};
inline bool window_of(const Call &c, Window &w, std::string &err) {
  if (!date_arg_ok(c, "from", err) || !date_arg_ok(c, "to", err))
    return false;
  w.from = c.str("from"), w.to = c.str("to");
  w.ymd0 = w.from.empty() ? 0 : ymd_of(w.from), w.ymd1 = w.to.empty() ? 99999999 : ymd_of(w.to);
  if (w.ymd0 > w.ymd1) {
    err = "from / to: " + w.text() + " 须先后有序";
    return false;
  }
  return true;
}

// ---- 分块流式观察核: 装载前核日期 → 跳过无关块 → 块内全核 DAG + Stat → 原地归约 ----
struct HeldCell {
  int t = 0, a = 0; // t = 块内行, a = 池轴列
  int g = 0;        // 组号 (已定向)
  float ret = 0.f;  // 做多收益 (续持取名义值)
};

struct ChunkView {
  const Engine::Job &job;
  const Engine::Block &blk;
  const BarMeta &bm;
  const std::vector<misc::Ymd> &dates;             // stat_dates
  int u0 = 0, t_lo = 0, t_hi = 0;                  // 本块落在日段内的统计行 [t_lo, t_hi) 对应全局统计行 u0 + (t - t_lo)
  const uint16_t *rx = nullptr;                    // [blk.T][A] 已定向 (高 = 做多端)
  std::span<const st::Row> rows;                   // [t_hi - t_lo] 已定向 (含尾部 Row::tail)
  std::span<const std::vector<HeldCell>> cells;    // [t_hi - t_lo] 真持有的格 (行内 a 升序; 不要格 = 空)
  std::span<const factor::check::Plane *> feat_pl; // 与 feats 对齐 (坏式或解析失败为 nullptr)
  int w0 = 0, w1 = 0;                              // 日段的全局统计行 [w0, w1) (抽样步长按它定)
  int w_rows() const { return w1 - w0; }
};

// 本行 (cells = ChunkView::cells 的一行) 里格 a 是否在全截面分组 (rx 已定向) 下真持有; 是 → ret = 其做多收益 (续持取名义值).
//   格由行核 walk 时同出 (Stat 仓位感知唯一出口 factor/Stat/Pos.hpp cell), 这里只查
inline bool held_cell(const std::vector<HeldCell> &cells, int a, float &ret) {
  const auto it = std::lower_bound(cells.begin(), cells.end(), a, [](const HeldCell &c, int x) { return c.a < x; });
  if (it == cells.end() || it->a != a)
    return false;
  ret = it->ret;
  return true;
}

// 一行的超额基准 = 池基准 (契约 Label::mk: 标签有效格的做多收益均值, 与因子无关); 有因子时行不 ok (rIC 不可算) = NaN
inline float mkt_of(const ChunkView &cv, int t) {
  const float mk = cv.blk.y.labels[0]->mk[static_cast<size_t>(t)];
  if (cv.rows.empty())
    return mk;
  const int r = t - cv.t_lo;
  assert(r >= 0 && r < static_cast<int>(cv.rows.size()) && "行须在本块的日段内");
  return cv.rows[static_cast<size_t>(r)].ok ? mk : NAN;
}

// 观察流的因子根探针 (块级, Engine::ForestPass 进, 只 CPU): 本块统计行的 Row (已定向, 含尾部) + rx + 真持有的格
//   (cells 时; 行核 walk 时同出 sink.cell, 不另走链)
struct LookP : Engine::ProbeBase {
  static constexpr uint8_t need = Engine::kNeedRows | Engine::kNeedRx;
  static constexpr bool kCells = true;
  bool want = false; // 要真持有的格
  int only = -1;     // 格的范围 (FactorSpec::only_end)
  int t_lo = 0;
  std::vector<st::Row> rows;                // [本块统计行]
  std::vector<std::vector<HeldCell>> cells; // [本块统计行] (不要格 = 空)
  const uint16_t *rx = nullptr;             // [blk.T][A] 已定向 (高 = 做多端)
  static constexpr Engine::Bytes scratch(int) { return {.cell = sizeof(HeldCell), .row = sizeof(st::Row)}; }
  void open(const Engine::Ctx &c) {
    const st::Span s = c.s();
    t_lo = s.t_lo;
    rows.assign(static_cast<size_t>(s.t_hi() - s.t_lo), {});
    cells.assign(want ? rows.size() : 0, {});
  }
  void row(int, int t, const st::Row &r) { rows[static_cast<size_t>(t - t_lo)] = r; }
  void cell(int, int t, int a, int g, const st::Pos &p, const float *y) { // 观察只一个持有期; 行内格按 a 升序到 (同行同线程)
    if (want && (only == kAllGroups || (only < 0 ? g == st::end_group(st::kTop) || g == st::end_group(st::kBot) : g == st::end_group(only))))
      cells[static_cast<size_t>(t - t_lo)].push_back({t, a, g, st::ret_at(p, 0, y)});
  }
  void close(const Engine::Ctx &, const Engine::RootView &v) {
    rx = v.rx;
    for (size_t r = 0; r < cells.size(); ++r) // 行不 ok (rIC 不可算) 不收格
      if (!rows[r].ok)
        cells[r].clear();
  }
};
using LookProbes = Engine::Probes<LookP>;

// 观察另加的前后带 (日) / 每格宿主工作量 (字节): 并进作业的 Need
struct Extra {
  int front = 0, back = 0;
  double work = 0.0;
};
struct StreamCfg {
  const FactorSpec *fac = nullptr;
  std::vector<FeatSpec> *feats = nullptr;
  uint8_t bars = 0;
  Extra extra;
};

// range_fn 核定统计日段 [s_lo, s_hi) (Window 或按目标日); 只装与它相交的块 (Hooks::want), 块内 ForestPass 评完后 (Hooks::after) 只把段内的行交给 chunk_fn
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
    fac_roots.push_back({&cfg.fac->e, {cfg.fac->frame, cfg.fac->dir}});
  if (cfg.feats)
    for (FeatSpec &f : *cfg.feats)
      if (f.err.empty()) {
        f.root_idx = static_cast<int>(fac_roots.size() + plain_es.size());
        plain_es.push_back(&f.e);
      }
  const Engine::Forest fr = !fac_roots.empty() || !plain_es.empty() ? Engine::plant_forest(ft, fac_roots, /*keep_roots=*/true, plain_es) : Engine::Forest{};

  Engine::Lease lease(F);
  if (!lease)
    return Engine::Rc::Cancel;
  using Cpu = Engine::Backend<Engine::Cpu>;
  const int R = static_cast<int>(fac_roots.size());
  Engine::Need need = Engine::forest_need<Cpu, LookProbes>(ft, fr, R, Engine::look_of(fac_roots, plain_es, ft.seg), Holds::of({env.hold}));
  for (uint8_t b = 0; b < kBarCount; ++b)
    if ((cfg.bars & (1u << b)) && bm.col[b].id != Engine::kNoCol && std::find(need.x.begin(), need.x.end(), bm.col[b].id) == need.x.end())
      need.x.push_back(bm.col[b].id);
  need.bands.front = std::max(need.bands.front, cfg.extra.front), need.bands.back = std::max(need.bands.back, cfg.extra.back);
  need.cell.work += cfg.extra.work;
  if (const Engine::Rc rc = Engine::open_job(F, env.fsc, std::move(need), job); rc != Engine::Rc::Ok) {
    err = "没算成: " + job.why;
    return rc;
  }
  stat_dates = job.dm.stat_dates();
  size_t s_lo = 0, s_hi = stat_dates.size();
  if (!range_fn(stat_dates, s_lo, s_hi, err))
    return Engine::Rc::Empty;
  assert(s_lo < s_hi && s_hi <= stat_dates.size());

  Cpu &eng = Engine::engine_space(F.S).cpu;
  const size_t want_d0 = job.dm.warm + s_lo, want_d1 = job.dm.warm + s_hi;
  std::vector<LookProbes> ps(static_cast<size_t>(R));
  const LookP *fp = cfg.fac ? &ps[0].at<LookP>() : nullptr;
  if (cfg.fac)
    ps[0].at<LookP>().want = cfg.fac->cells, ps[0].at<LookP>().only = cfg.fac->only_end;
  Engine::Fate fate;
  const Engine::Hooks hk{.want = [&](const Engine::Chunk &ck) { return ck.s1 > want_d0 && ck.s0 < want_d1; },
                         .after = [&](const Engine::Block &blk) {
                           std::vector<const factor::check::Plane *> feat_pl;
                           if (cfg.feats)
                             for (FeatSpec &f : *cfg.feats) {
                               if (f.root_idx >= 0 && !fate.dead[static_cast<size_t>(f.root_idx)].empty())
                                 f.err = fate.dead[static_cast<size_t>(f.root_idx)];
                               feat_pl.push_back(f.root_idx >= 0 && f.err.empty() ? eng.plane(fr.d.roots[static_cast<size_t>(f.root_idx)]) : nullptr);
                             }
                           const st::Span span = blk.span();
                           const int t_lo = span.t_lo, n_stat = span.t_hi() - t_lo;
                           const int w0 = static_cast<int>(s_lo) * ft.seg, w1 = static_cast<int>(s_hi) * ft.seg, u_lo = blk.land0() + t_lo;
                           const int c0 = std::max(0, w0 - u_lo), c1 = std::min(n_stat, w1 - u_lo);
                           assert(c0 < c1 && "只装与日段相交的块");
                           const size_t n_in = static_cast<size_t>(c1 - c0);
                           const std::span<const st::Row> rows = fp ? std::span<const st::Row>(fp->rows).subspan(static_cast<size_t>(c0), n_in) : std::span<const st::Row>{};
                           const std::span<const std::vector<HeldCell>> cells =
                               fp && fp->want ? std::span<const std::vector<HeldCell>>(fp->cells).subspan(static_cast<size_t>(c0), n_in) : std::span<const std::vector<HeldCell>>{};
                           chunk_fn(ChunkView{job, blk, bm, stat_dates, u_lo + c0, t_lo + c0, t_lo + c1, fp ? fp->rx : nullptr, rows, cells, feat_pl, w0, w1}); }};
  const Engine::Rc rc = Engine::run_job(F, job, Engine::ForestPass<Cpu, LookProbes>(eng, F, fr, std::span(ps), fate, hk));
  if (rc == Engine::Rc::Broken)
    err = "因子没算成: " + job.why;
  else if (rc == Engine::Rc::Stale)
    err = std::string("没算成: ") + Engine::kStaleMsg;
  return rc;
}

// 日段提示: 给了 from / to 写截下的一段; 用到的叶落盘日更短 (观察的日段与评估期不同) 另写两端
inline std::string period_note(const std::vector<misc::Ymd> &dates, const ViewEnv &env, const Window &w = {}) {
  const bool short_ = dates.front() != env.first || dates.back() != env.last;
  const std::string eval = "评估期 " + misc::dash(env.first) + " ~ " + misc::dash(env.last);
  const std::string lib = "观察的日段 " + misc::dash(dates.front()) + " ~ " + misc::dash(dates.back()) + " (" + str(dates.size()) + " 天; 用到的叶落盘日更短, 与" + eval + "不同)";
  if (w.cut())
    return "注意: 本观察只看 " + misc::dash(dates[w.lo]) + " ~ " + misc::dash(dates[w.hi - 1]) + " (" + str(w.hi - w.lo) + " 天), 是" + (short_ ? lib : eval) +
           " 里的一段, 只与同段的结果比\n\n";
  return short_ ? "注意: " + lib + ", 与别的观察不可直接比\n\n" : std::string();
}

inline std::string expr_line(size_t i, const FeatSpec &f, const char *miss = "没算成") {
  return "E" + str(i + 1) + " = `" + f.raw + "`" + (f.err.empty() ? "" : std::string(" (") + miss + ": " + f.err + ")");
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
