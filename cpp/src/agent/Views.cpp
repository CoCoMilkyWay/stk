// Views — 见头文件
#include "agent/Views.hpp"
#include "agent/Config.hpp"
#include "agent/Text.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <cctype>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <map>
#include <numeric>
#include <random>
#include <string>
#include <vector>

namespace agent {

namespace {

constexpr int kG1 = 0, kG20 = kGroups - 1;
constexpr double kNaN = NAN;
constexpr size_t kSample = 200000;   // 分位的抽样上限 (格)
constexpr size_t kCsCells = 2000000; // 逐行截面相关 (rIC) 的用格上限
constexpr int kCorrRows = 400;       // 特征间相关取的行数上限
constexpr size_t kBase = 40000;      // 交割单标签的池内基准抽样 (格)
constexpr int kMinCs = 20;           // 截面相关至少的格数
constexpr size_t kEvents = 20000;    // 响应曲线的开仓段抽样上限
constexpr int kExplainRows = 3000;   // 成分剥离取的行数上限

// ---- 数的写法 ----
using text::fmt;
std::string num(double v) { return std::isfinite(v) ? fmt("%.4g", v) : "—"; }
std::string bp(double r) { return std::isfinite(r) ? fmt("%+.1f", r * 1e4) : "—"; }
std::string pct(double x) { return std::isfinite(x) ? fmt("%.0f%%", x * 100.0) : "—"; }
std::string chg(double x) { return std::isfinite(x) ? fmt("%+.2f%%", x * 100.0) : "—"; }
std::string r3(double x) { return std::isfinite(x) ? fmt("%.3f", x) : "—"; }
std::string pos(double x) { return std::isfinite(x) ? fmt("%.2f", x) : "—"; }
std::string rk(double x) { return std::isfinite(x) ? fmt("%.0f", x * 100.0) : "—"; } // 分位 ×100
std::string str(size_t n) { return std::to_string(n); }
std::string date_of(int ymd) {
  char b[16];
  std::snprintf(b, sizeof(b), "%04d-%02d-%02d", ymd / 10000, ymd / 100 % 100, ymd % 100);
  return b;
}
// YYYY-MM-DD / YYYYMMDD → YYYYMMDD; 不成 → 0
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

using text::bars;
// 火花线 + [最小, 最大] (原值的) + 分箱时每字几个值
std::string spark(const std::vector<double> &v, std::string (*f)(double) = num) {
  double lo = INFINITY, hi = -INFINITY;
  for (const double x : v)
    if (std::isfinite(x))
      lo = std::min(lo, x), hi = std::max(hi, x);
  const size_t w = text::spark_bin(v.size());
  return "`" + bars(v) + "`" + (lo <= hi ? "  [" + f(lo) + ", " + f(hi) + "]" : "") + (w > 1 ? " (每字 = 相邻 " + std::to_string(w) + " 个的均值)" : "");
}

// ---- 统计 ----
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
std::vector<double> ranks_of(const std::vector<double> &x) { // 并列取平均秩
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
// 均值 / t (= 均值 / 标准差 · √n) / 正占比
struct Mean {
  double sum = 0.0, sq = 0.0;
  size_t n = 0, pos = 0;
  void add(double x) {
    if (!std::isfinite(x))
      return;
    sum += x, sq += x * x, ++n, pos += x > 0.0;
  }
  double mean() const { return n ? sum / static_cast<double>(n) : kNaN; }
  double t() const {
    if (n < 2)
      return kNaN;
    const double m = mean(), var = (sq - static_cast<double>(n) * m * m) / static_cast<double>(n - 1);
    return var > 0.0 ? m / std::sqrt(var) * std::sqrt(static_cast<double>(n)) : kNaN;
  }
  double up() const { return n ? static_cast<double>(pos) / static_cast<double>(n) : kNaN; }
};
std::string wan(const Mean &m) { return m.n ? fmt("%.0f", m.sum / 1e4) : "—"; } // 成交额之和 (万); 无有效行 —

// 日内时段: 竞价段 (每日开头 L.open 行) 单列, 其余每段约 30 分钟 (8 段); 日层只一段
// 段名 = 起止时刻: 起 = 上一行收盘时刻, 午休不占行, 午后首段起于 13:00
struct Slot {
  int k0, k1; // 日内行 [k0, k1)
  std::string name;
};
int part_rows(const Look &L) { return std::max(1, (L.seg - L.open) / 8); }
std::vector<Slot> parts_of(const Look &L) {
  if (L.seg == 1)
    return {{0, 1, "收盘"}};
  const int w = part_rows(L);
  std::vector<Slot> p{{0, L.open, "09:15–09:30 竞价"}};
  for (int k = L.open; k < L.seg; k += w) {
    const int k1 = L.seg - (k + w) < w ? L.seg : k + w;
    const std::string &from = L.clock[static_cast<size_t>(k - 1)];
    p.push_back({k, k1, (from == "11:30" ? std::string("13:00") : from) + "–" + L.clock[static_cast<size_t>(k1 - 1)]});
    if (k1 == L.seg)
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

// "HH:MM" 的写法
bool hm_shape(const std::string &s, size_t at = 0) {
  return s.size() >= at + 5 && s[at + 2] == ':' && std::isdigit(static_cast<unsigned char>(s[at])) && std::isdigit(static_cast<unsigned char>(s[at + 1])) &&
         std::isdigit(static_cast<unsigned char>(s[at + 3])) && std::isdigit(static_cast<unsigned char>(s[at + 4]));
}
// 参数 key 的 "HH:MM-HH:MM" → 行末时刻落在其中 (含两端) 的日内行 [k0, k1); 写法不对 / 没有行 / 超过 cap 行 (cap > 0) → false + err
bool rows_in(const char *key, const std::string &s, const std::vector<std::string> &clock, int cap, int &k0, int &k1, std::string &err) {
  if (s.size() != 11 || !hm_shape(s) || s[5] != '-' || !hm_shape(s, 6)) {
    err = std::string(key) + ": 写成 HH:MM-HH:MM (如 10:00-10:30)";
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
    err = std::string(key) + ": " + s + " 里没有行 (本层行末时刻 " + clock.front() + ", " + clock[1] + " … " + clock.back() + ")";
    return false;
  }
  if (cap > 0 && k1 - k0 > cap) {
    err = std::string(key) + ": " + s + " 有 " + std::to_string(k1 - k0) + " 行, 至多 " + std::to_string(cap) + " 行; 收窄时段";
    return false;
  }
  return true;
}

// 观察所在的日段与作用域评估期不同 (用到的叶落盘日更短) 才写
std::string head(const Look &L, const Context &ctx) {
  if (L.dates.front() == ctx.first && L.dates.back() == ctx.last)
    return "";
  return "注意: 本观察用到的叶落盘日更短, 日段为 " + date_of(L.dates.front()) + " ~ " + date_of(L.dates.back()) + " (" + str(L.dates.size()) + " 天), 不是作用域评估期\n\n";
}

// ---- K 线取值 (没有 / 没算成的叶 → NaN) ----
struct Px {
  const Look &L;
  PerBar<const float *> v{};
  explicit Px(const Look &l) : L(l) {
    for (const Bar b : kBars)
      if (const LookFeat &f = L.kline[b]; f.error.empty() && !f.v.empty())
        v[b] = f.v.data();
  }
  double operator()(Bar b, int t, int a) const { return v[b] ? v[b][L.at(t, a)] : kNaN; }
  // 该行有成交 (无量叶则都算有): 无成交行 (如竞价段未撮合的分钟) 的高低是占位价, 不进当日高低
  bool traded(int t, int a) const { return !v[Bar::Volume] || (*this)(Bar::Volume, t, a) > 0.0; }
  // 当日涨跌的基准: 分钟层 = 当日昨收 (除权); 日层 = 上一行收盘
  double pre(int t, int a) const {
    const double p = L.seg > 1 ? (*this)(Bar::PreClose, t, a) : t > 0 ? (*this)(Bar::Close, t - 1, a)
                                                                      : kNaN;
    return p > 0.0 ? p : kNaN;
  }
};

// ---- 交割单的标签 (开仓那行的盘面) ----
namespace tag {
enum : uint8_t { Open,
                 Last,
                 NearUp,
                 NearDn,
                 Up,
                 Down,
                 High,
                 Low,
                 Surge,
                 N };
}
constexpr const char *kTagNames[tag::N] = {"竞价段 (09:15–09:30 的行)",
                                           "尾盘最后一段",
                                           "近涨停 (收 ≥ 涨停价 × 0.995)",
                                           "近跌停 (收 ≤ 跌停价 × 1.005)",
                                           "当日涨 > 5%",
                                           "当日跌 > 5%",
                                           "日内高位 (当日高低区间 ≥ 0.8)",
                                           "日内低位 (≤ 0.2)",
                                           "放量 (≥ 当日此前有成交行均量 3 倍)"};
struct Mark {
  uint32_t m = 0;
  double chg = kNaN, pos = kNaN; // 开仓那行收盘时: 当日涨跌 / 收在当日至此高低区间的位置
};
Mark mark_of(const Px &px, int t, int a) {
  const int seg = px.L.seg, k = t % seg, t0 = t - k;
  Mark r;
  const double c = px(Bar::Close, t, a);
  r.chg = c / px.pre(t, a) - 1.0;
  if (r.chg > 0.05)
    r.m |= 1u << tag::Up;
  if (r.chg < -0.05)
    r.m |= 1u << tag::Down;
  if (seg == 1)
    return r;
  if (k < px.L.open)
    r.m |= 1u << tag::Open;
  if (k >= seg - part_rows(px.L))
    r.m |= 1u << tag::Last;
  if (c >= px(Bar::LimUp, t, a) * 0.995)
    r.m |= 1u << tag::NearUp;
  if (c <= px(Bar::LimDn, t, a) * 1.005)
    r.m |= 1u << tag::NearDn;
  double hi = -INFINITY, lo = INFINITY, vs = 0.0;
  int vn = 0;
  for (int u = t0; u <= t; ++u) {
    if (px.traded(u, a))
      hi = std::fmax(hi, px(Bar::High, u, a)), lo = std::fmin(lo, px(Bar::Low, u, a));
    if (const double v = px(Bar::Volume, u, a); u < t && v > 0.0)
      vs += v, ++vn;
  }
  if (hi > lo && std::isfinite(c)) {
    r.pos = (c - lo) / (hi - lo);
    if (r.pos >= 0.8)
      r.m |= 1u << tag::High;
    if (r.pos <= 0.2)
      r.m |= 1u << tag::Low;
  }
  if (vn >= 3 && vs > 0.0 && px(Bar::Volume, t, a) >= 3.0 * vs / vn)
    r.m |= 1u << tag::Surge;
  return r;
}
std::string tags_of(uint32_t m) {
  static constexpr const char *kShort[tag::N] = {"竞价", "尾盘", "近涨停", "近跌停", "大涨", "大跌", "高位", "低位", "放量"};
  std::string s;
  for (int j = 0; j < tag::N; ++j)
    if (m >> j & 1u)
      s += (s.empty() ? "" : " ") + std::string(kShort[j]);
  return s;
}

// ---- 分层汇总 (口径同分层回测: 组的每期超额 = 有统计行的组超额之和 / 全部行数) ----
struct Layer {
  int rows = 0, ok = 0;
  double g[kGroups] = {}, cnt[kGroups] = {};
  double tl[2][kTails] = {}, tc[2][kTails] = {}; // 尾部 [端][j] (端 0 = G20 内 / 1 = G1 内)
  Mean ric;
  void add(const LookRow &w) {
    ++rows;
    if (!w.ok)
      return;
    ++ok;
    for (int k = 0; k < kGroups; ++k)
      g[k] += w.grp[k], cnt[k] += w.cnt[k];
    for (int e = 0; e < 2; ++e)
      for (int j = 0; j < kTails; ++j)
        tl[e][j] += w.tail[e][j], tc[e][j] += w.tcnt[e][j];
    ric.add(w.ric);
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
  double mono() const { return ok ? Mono(grps().data(), kGroups) : kNaN; }
  double ls() const { return (grp(kG20) - grp(kG1)) / 2.0; }
  double held(int k) const { return ok ? cnt[k] / ok : kNaN; }
};
std::string gname(int k) { return "G" + std::to_string(k + 1); }

// 标签口径的近似一行: 按分数 x 把格等分 20 组, 组超额 = 组内超额 y 的等权均值, rIC = x 与 y 的 Spearman; 格不足 → false
bool approx_row(const std::vector<double> &x, const std::vector<double> &y, LookRow &w) {
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
  w.ok = true;
  return true;
}

const LookRoot *root_of(const Look &L, std::string &out) {
  const LookRoot &r = L.factors.front();
  if (!r.error.empty()) {
    out = "因子没算成: " + r.error;
    return nullptr;
  }
  return &r;
}

// ---- view_feature ----
std::string feature_view(const Call &c, const Look &L) {
  const std::vector<std::string> xs = c.strs("exprs");
  assert(xs.size() == L.feats.size());
  const int A = L.A(), T = L.T(), seg = L.seg, D = static_cast<int>(L.dates.size());
  const size_t N = static_cast<size_t>(T) * static_cast<size_t>(A);
  const int rstep = std::max(1, static_cast<int>(N / kCsCells));
  const std::vector<Slot> ps = parts_of(L);
  const std::vector<int> pr = part_of_row(ps, seg);
  std::string s;
  std::vector<size_t> good;
  for (size_t i = 0; i < xs.size(); ++i) {
    s += "## E" + str(i + 1) + " `" + xs[i] + "`\n";
    const LookFeat &f = L.feats[i];
    if (!f.error.empty()) {
      s += "没算成: " + f.error + "\n\n";
      continue;
    }
    const std::vector<float> &v = f.v;
    size_t in = 0, ok = 0;
    std::vector<double> smp;
    const size_t st = std::max<size_t>(1, N / kSample);
    for (size_t k = 0; k < N; ++k)
      if (L.gate[k]) {
        ++in;
        if (std::isfinite(v[k]) && ok++ % st == 0)
          smp.push_back(v[k]);
      }
    if (ok < 100) {
      s += "池内有效格太少 (" + str(ok) + " / " + str(in) + ")\n\n";
      continue;
    }
    good.push_back(i);
    std::sort(smp.begin(), smp.end());
    const double lo = quant(smp, 0.01), hi = quant(smp, 0.99);
    Mean mv;
    for (const double x : smp)
      mv.add(x);
    const double sd = mv.n > 1 ? std::sqrt(std::max(0.0, (mv.sq - static_cast<double>(mv.n) * mv.mean() * mv.mean()) / static_cast<double>(mv.n - 1))) : kNaN;
    s += "池内有效 " + pct(static_cast<double>(ok) / static_cast<double>(in)) + " (" + str(ok) + " 格) · 均值 " + num(mv.mean()) + " · 标准差 " + num(sd) + "\n";
    s += "分位 1/5/25/50/75/95/99%:";
    for (const double p : {0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99})
      s += " " + num(quant(smp, p));
    s += "\n";
    std::vector<double> u = smp;
    u.erase(std::unique(u.begin(), u.end()), u.end());
    if (u.size() <= 20)
      s += "离散: 抽样里只有 " + str(u.size()) + " 种取值\n";
    if (const double z = static_cast<double>(std::count(smp.begin(), smp.end(), 0.0)) / static_cast<double>(smp.size()); z >= 0.05)
      s += "零值占 " + pct(z) + "\n";

    // 截到 1%..99% 再做方差分解 / 持续性
    const auto w = [&](float x) { return std::clamp(static_cast<double>(x), lo, hi); };
    std::vector<double> sa(static_cast<size_t>(A)), sad(static_cast<size_t>(D) * A);
    std::vector<int> na(static_cast<size_t>(A)), nad(static_cast<size_t>(D) * A);
    double sx = 0.0, sxx = 0.0;
    size_t n = 0;
    for (int t = 0; t < T; ++t)
      for (int a = 0; a < A; ++a)
        if (const size_t k = L.at(t, a); L.gate[k] && std::isfinite(v[k])) {
          const double x = w(v[k]);
          const size_t da = static_cast<size_t>(t / seg) * A + a;
          sa[a] += x, ++na[a], sad[da] += x, ++nad[da], sx += x, sxx += x * x, ++n;
        }
    const double mu = sx / n, tot = sxx - n * mu * mu;
    std::vector<double> ma(static_cast<size_t>(A), kNaN);
    double ssa = 0.0, ssd = 0.0;
    for (int a = 0; a < A; ++a)
      if (na[a])
        ma[a] = sa[a] / na[a], ssa += na[a] * (ma[a] - mu) * (ma[a] - mu);
    for (size_t da = 0; da < sad.size(); ++da)
      if (nad[da]) {
        const double m = sad[da] / nad[da] - ma[da % A];
        ssd += nad[da] * m * m;
      }
    if (tot > 0.0)
      s += "方差来自 (截到 1~99% 分位): 资产间 " + pct(ssa / tot) + " · 日间 (同一资产) " + pct(ssd / tot) +
           (seg > 1 ? " · 日内 " + pct(std::max(0.0, tot - ssa - ssd) / tot) : std::string()) + "\n";

    // 持续性: 去资产均值后同一资产的自相关
    const std::vector<std::pair<int, const char *>> lags =
        seg > 1 ? std::vector<std::pair<int, const char *>>{{1, "隔 1 行"}, {seg, "隔 1 日"}, {5 * seg, "隔 5 日"}}
                : std::vector<std::pair<int, const char *>>{{1, "隔 1 日"}, {5, "隔 5 日"}, {20, "隔 20 日"}};
    s += "持续性 (去资产均值, 同一资产自相关):";
    for (const auto &[lag, name] : lags) {
      double xy = 0.0, xx = 0.0, yy = 0.0;
      for (int t = lag; t < T; ++t)
        for (int a = 0; a < A; ++a) {
          const size_t k1 = L.at(t, a), k0 = L.at(t - lag, a);
          if (!L.gate[k1] || !L.gate[k0] || !std::isfinite(v[k1]) || !std::isfinite(v[k0]))
            continue;
          const double x = w(v[k1]) - ma[a], y = w(v[k0]) - ma[a];
          xy += x * y, xx += x * x, yy += y * y;
        }
      s += std::string(" ") + name + " " + r3(xx > 0.0 && yy > 0.0 ? xy / std::sqrt(xx * yy) : kNaN);
    }
    s += "\n";

    // 日内轮廓 + rIC
    std::vector<double> med(static_cast<size_t>(T), kNaN), cs, xa, ya;
    Mean ric;
    std::vector<Mean> ric_part(ps.size());
    for (int t = 0; t < T; ++t) {
      cs.clear();
      for (int a = 0; a < A; ++a)
        if (const size_t k = L.at(t, a); L.gate[k] && std::isfinite(v[k]))
          cs.push_back(v[k]);
      if (cs.size() >= 5)
        med[t] = median_of(cs);
      if (t % rstep)
        continue;
      xa.clear(), ya.clear();
      for (int a = 0; a < A; ++a)
        if (const size_t k = L.at(t, a); L.gate[k] && std::isfinite(v[k]) && std::isfinite(L.label[k]))
          xa.push_back(v[k]), ya.push_back(L.label[k]);
      if (xa.size() >= static_cast<size_t>(kMinCs)) {
        const double r = spearman(xa, ya);
        ric.add(r), ric_part[static_cast<size_t>(pr[t % seg])].add(r);
      }
    }
    if (seg > 1) {
      std::vector<double> prof(static_cast<size_t>(seg)), col(static_cast<size_t>(D));
      for (int k = 0; k < seg; ++k) {
        for (int d = 0; d < D; ++d)
          col[d] = med[static_cast<size_t>(d) * seg + k];
        prof[k] = median_of(col);
      }
      s += "日内轮廓 (各行截面中位数, 取日间中位数): " + spark(prof) + "\n  ";
      for (const Slot &p : ps)
        s += " " + L.clock[static_cast<size_t>(p.k1 - 1)] + " " + num(prof[static_cast<size_t>(p.k1 - 1)]);
      s += "\n";
    } else {
      s += "截面中位数逐日: " + spark(med) + "\n";
    }
    s += "与本持有期收益的截面 rIC (原向): 均值 " + r3(ric.mean()) + " · t " + fmt("%.1f", ric.t()) + " (行间重叠未折算, 偏大) · 正 rIC 行 " + pct(ric.up()) + "\n";
    if (seg > 1) {
      s += "  分时段:";
      for (size_t j = 0; j < ps.size(); ++j)
        s += " " + ps[j].name + " " + r3(ric_part[j].mean());
      s += "\n";
    }
    s += "\n";
  }

  if (good.size() >= 2) {
    const int cstep = std::max(1, T / kCorrRows);
    const size_t G = good.size();
    std::vector<Mean> m(G * G);
    std::vector<double> xa, ya;
    for (int t = 0; t < T; t += cstep)
      for (size_t i = 0; i < G; ++i)
        for (size_t j = i + 1; j < G; ++j) {
          const std::vector<float> &x = L.feats[good[i]].v, &y = L.feats[good[j]].v;
          xa.clear(), ya.clear();
          for (int a = 0; a < A; ++a)
            if (const size_t k = L.at(t, a); L.gate[k] && std::isfinite(x[k]) && std::isfinite(y[k]))
              xa.push_back(x[k]), ya.push_back(y[k]);
          if (xa.size() >= static_cast<size_t>(kMinCs))
            m[i * G + j].add(spearman(xa, ya));
        }
    s += "## 两两截面秩相关 (逐行 Spearman 的均值)\n|  |";
    for (size_t j = 0; j < G; ++j)
      s += " E" + str(good[j] + 1) + " |";
    s += "\n|---|";
    for (size_t j = 0; j < G; ++j)
      s += "---|";
    s += "\n";
    for (size_t i = 0; i < G; ++i) {
      s += "| E" + str(good[i] + 1) + " |";
      for (size_t j = 0; j < G; ++j)
        s += " " + (j == i ? std::string("1") : r3(m[std::min(i, j) * G + std::max(i, j)].mean())) + " |";
      s += "\n";
    }
  }
  return s;
}

// ---- view_layers ----
std::string layers_row(const std::string &name, const Layer &b) {
  return "| " + name + " | " + std::to_string(b.rows) + " | " + std::to_string(b.ok) + " | " + r3(b.mono()) + " | " + bp(b.ls()) + " | " + r3(b.ric.mean()) + " | " +
         bp(b.grp(kG1)) + " | " + bp(b.grp(kGroups / 2 - 1)) + " | " + bp(b.grp(kG20)) + " | `" + bars(b.grps()) + "` |\n";
}

// 条件三等分: 每行把 在池 ∧ 因子 / 标签 / 条件有效 的格按条件值截面三等分, 段内按因子分位近似分层 (抽行)
std::string cond_view(const std::string &cond, const LookRoot &r, const Look &L) {
  std::string s = "\n## 按条件三等分: `" + cond + "`\n";
  assert(L.feats.size() == 1 && "LookOf 只为 cond 取一个特征式");
  const LookFeat &f = L.feats.front();
  if (!f.error.empty())
    return s + "条件没算成: " + f.error + "\n";
  const int T = L.T(), A = L.A();
  const int rstep = std::max(1, static_cast<int>(static_cast<size_t>(T) * static_cast<size_t>(A) / kCsCells));
  std::array<Layer, 4> b; // 全体 / 低 / 中 / 高
  Mean rho, cric;
  std::vector<double> cv, fx, ey;
  std::array<std::vector<double>, 3> px, py;
  LookRow w;
  for (int t = 0; t < T; t += rstep) {
    if (!std::isfinite(L.mkt[static_cast<size_t>(t)]))
      continue;
    cv.clear(), fx.clear(), ey.clear();
    for (int a = 0; a < A; ++a)
      if (const size_t k = L.at(t, a); L.gate[k] && std::isfinite(r.rank(k)) && std::isfinite(L.label[k]) && std::isfinite(f.v[k]))
        cv.push_back(f.v[k]), fx.push_back(r.rank(k)), ey.push_back(static_cast<double>(L.label[k]) - L.mkt[static_cast<size_t>(t)]);
    const size_t n = cv.size();
    if (n < 3 * static_cast<size_t>(kMinCs))
      continue;
    const std::vector<double> rc = ranks_of(cv);
    rho.add(pearson(rc, ranks_of(fx))), cric.add(pearson(rc, ranks_of(ey)));
    for (size_t j = 0; j < 3; ++j)
      px[j].clear(), py[j].clear();
    for (size_t i = 0; i < n; ++i) {
      const size_t j = std::min<size_t>(2, static_cast<size_t>(rc[i] * 3.0 / static_cast<double>(n)));
      px[j].push_back(fx[i]), py[j].push_back(ey[i]);
    }
    if (approx_row(fx, ey, w))
      b[0].add(w);
    for (size_t j = 0; j < 3; ++j)
      if (approx_row(px[j], py[j], w))
        b[j + 1].add(w);
  }
  if (!b[0].ok)
    return s + "没有可用的行 (每行至少要 " + std::to_string(3 * kMinCs) + " 个在池且因子 / 标签 / 条件都有效的格)\n";
  s += "条件与因子的截面秩相关 " + r3(rho.mean()) + " · 条件自身 rIC " + r3(cric.mean()) + "\n";
  s += "每行按条件值把池子截面三等分 (条件离散时并列落同一段, 三段不均), 段内按因子分位重新分 20 组; 组超额 = 组内 (标签 − 本行在池均值) 等权均值, 建不了仓的格不剔, "
       "与上面的真持有口径略有出入, 只作这四行之间对照\n\n";
  s += "| 条件段 | 行 | 有统计 | mono | 多空 bp | rIC | G1 bp | G10 bp | G20 bp | 20 组 |\n|---|---|---|---|---|---|---|---|---|---|\n";
  s += layers_row("全体 (同口径)", b[0]) + layers_row("低 1/3", b[1]) + layers_row("中 1/3", b[2]) + layers_row("高 1/3", b[3]);
  return s;
}

bool layers_view(const Call &c, const Look &L, std::string &out) {
  const LookRoot *r = root_of(L, out);
  if (!r)
    return false;
  const int T = L.T(), seg = L.seg;
  Layer all;
  for (const LookRow &w : r->rows)
    all.add(w);
  std::string s = "## 因子 `" + r->label + "` (组号已定向: G20 = 做多端)\n";
  s += "有统计行 " + std::to_string(all.ok) + " / " + std::to_string(all.rows) + " · mono " + r3(all.mono()) + " · 每期多空 " + bp(all.ls()) + "bp · rIC 均值 " +
       r3(all.ric.mean()) + " (t " + fmt("%.1f", all.ric.t()) + ", 正 " + pct(all.ric.up()) + ")\n";
  s += "20 组每期超额 G1 → G20: " + spark(all.grps(), bp) + " bp\n\n|  |";
  for (int k = 0; k < kGroups; ++k)
    s += " " + gname(k) + " |";
  s += "\n|---|";
  for (int k = 0; k < kGroups; ++k)
    s += "---|";
  s += "\n| 每期超额 bp |";
  for (int k = 0; k < kGroups; ++k)
    s += " " + bp(all.grp(k)) + " |";
  s += "\n| 行均真持有只数 |";
  for (int k = 0; k < kGroups; ++k)
    s += " " + fmt("%.1f", all.held(k)) + " |";
  double mid = 0.0;
  for (int k = 5; k < 15; ++k)
    mid += all.grp(k) / 10.0;
  s += "\n\n做多端 G20 " + bp(all.grp(kG20)) + "bp · 最低组 G1 " + bp(all.grp(kG1)) + "bp · 中段 G6–G15 均值 " + bp(mid) + "bp\n";
  s += "\n两端组内按池内秩再截更极端的一截 (同一份真持有判据, 满仓按宽度折算, 每期超额口径同上):\n| 截取 | 占截面 | G20 端 bp | G20 行均只数 | G1 端 bp | G1 行均只数 | 多空 bp |\n|---|---|---|---|---|---|---|\n";
  s += "| 整组 | " + fmt("%g%%", 100.0 / kGroups) + " | " + bp(all.grp(kG20)) + " | " + fmt("%.1f", all.held(kG20)) + " | " + bp(all.grp(kG1)) + " | " + fmt("%.1f", all.held(kG1)) +
       " | " + bp(all.ls()) + " |\n";
  for (int j = 0; j < kTails; ++j)
    s += "| 组内最极端 " + fmt("%g%%", kTailOfGroup[j] * 100.0) + " | " + fmt("%g%%", kTailOfGroup[j] * 100.0 / kGroups) + " | " + bp(all.tail(0, j)) + " | " +
         fmt("%.1f", all.tail_held(0, j)) + " | " + bp(all.tail(1, j)) + " | " + fmt("%.1f", all.tail_held(1, j)) + " | " + bp((all.tail(0, j) - all.tail(1, j)) / 2.0) + " |\n";

  const std::string split = c.str("split"), cond = c.str("cond");
  if (split == "tod" && seg == 1) {
    s += "\n(日层每天一行, 没有日内时段可切)\n";
  } else if (split != "none") {
    std::vector<std::pair<std::string, Layer>> bs;
    if (split == "tod") {
      const std::vector<Slot> ps = parts_of(L);
      const std::vector<int> pr = part_of_row(ps, seg);
      for (const Slot &p : ps)
        bs.push_back({p.name, {}});
      for (int t = 0; t < T; ++t)
        bs[static_cast<size_t>(pr[t % seg])].second.add(r->rows[t]);
    } else {
      std::map<int, Layer> by;
      for (int t = 0; t < T; ++t)
        by[L.dates[static_cast<size_t>(t / seg)] / 100].add(r->rows[t]);
      for (auto &[m, b] : by)
        bs.push_back({date_of(m * 100 + 1).substr(0, 7), b});
    }
    s += "\n| " + std::string(split == "tod" ? "时段" : "月份") + " | 行 | 有统计 | mono | 多空 bp | rIC | G1 bp | G10 bp | G20 bp | 20 组 |\n|---|---|---|---|---|---|---|---|---|---|\n";
    for (const auto &[name, b] : bs)
      s += layers_row(name, b);
  }
  if (!cond.empty())
    s += cond_view(cond, *r, L);
  out = std::move(s);
  return true;
}

// ---- view_trades ----
struct Trade {
  int a, t0, t1, n; // 资产 / 首末行 / 行数
  double ex;        // 各行超额的均值
};

bool trades_view(const Call &c, const Look &L, std::string &out) {
  const LookRoot *r = root_of(L, out);
  if (!r)
    return false;
  const bool top = c.str("side") == "top";
  const std::string merge = c.str("merge"), sort = c.str("sort");
  const int seg = L.seg, T = L.T(), A = L.A();
  // 行窗: 日期 [from, to] ∧ 日内行 [k0, k1) (tod); 格与池内基准格都只取窗内的行
  const std::string from = c.str("from"), to = c.str("to"), tod = c.str("tod");
  const int ymd0 = from.empty() ? 0 : ymd_of(from), ymd1 = to.empty() ? 99999999 : ymd_of(to);
  int k0 = 0, k1 = seg;
  if (!tod.empty()) {
    std::string err;
    [[maybe_unused]] const bool ok = rows_in("tod", tod, L.clock, 0, k0, k1, err);
    assert(ok && "LookOf 已查过");
  }
  const auto in_win = [&](int t) {
    const int ymd = L.dates[static_cast<size_t>(t / seg)], k = t % seg;
    return ymd >= ymd0 && ymd <= ymd1 && k >= k0 && k < k1;
  };
  int rows = 0;
  for (int t = 0; t < T; ++t)
    rows += in_win(t);
  const std::string side = top ? "做多端 G20" : "最低组 G1";
  std::string win;
  if (!from.empty() || !to.empty())
    win += (from.empty() ? "…" : from) + " ~ " + (to.empty() ? "…" : to) + " ";
  if (!tod.empty())
    win += "行末 " + tod + " ";
  if (!win.empty())
    win = " · 只看 " + win + "的行 (" + std::to_string(rows) + " 行)";
  std::string s = "## 因子 `" + r->label + "` · " + side + " · 合并 " + merge + win + "\n";
  if (!rows) {
    out = s + "窗口里没有行 (评估期 " + date_of(L.dates.front()) + " ~ " + date_of(L.dates.back()) + ")";
    return false;
  }
  std::vector<LookCell> cs;
  for (const LookCell &x : r->cells)
    if (x.top == top && in_win(x.t))
      cs.push_back(x);
  if (cs.empty()) {
    out = s + "本端没有真持有过的格\n";
    return true;
  }
  std::sort(cs.begin(), cs.end(), [](const LookCell &x, const LookCell &y) { return x.a != y.a ? x.a < y.a : x.t < y.t; });
  const auto ex_of = [&](const LookCell &x) { return static_cast<double>(x.ret) - r->rows[static_cast<size_t>(x.t)].mkt; };
  std::vector<Trade> ts;
  Mean cell;
  for (const LookCell &x : cs) {
    const double e = ex_of(x);
    cell.add(e);
    Trade *b = ts.empty() ? nullptr : &ts.back();
    const bool join = b && b->a == x.a && b->t1 / seg == x.t / seg && (merge == "day" || (merge == "run" && x.t == b->t1 + 1));
    if (join) {
      b->ex = (b->ex * b->n + e) / (b->n + 1), b->t1 = x.t, ++b->n;
    } else {
      ts.push_back({x.a, x.t, x.t, 1, e});
    }
  }
  const size_t M = ts.size();
  std::vector<double> ex(M);
  Mean seg_ex;
  double gain = 0.0, loss = 0.0;
  std::vector<double> contrib(M);
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
  s += "真持有 " + str(cs.size()) + " 格 (每行平均 " + fmt("%.1f", static_cast<double>(cs.size()) / rows) + " 只) → " + str(M) + " 段, 段均 " +
       fmt("%.1f", static_cast<double>(cs.size()) / M) + " 行\n";
  s += "格超额均值 " + bp(cell.mean()) + "bp (胜率 " + pct(cell.up()) + ") · 段超额: 均值 " + bp(seg_ex.mean()) + " · 中位 " + bp(quant(sx, 0.5)) + " · p10 " + bp(quant(sx, 0.1)) +
       " · p90 " + bp(quant(sx, 0.9)) + " · 胜率 " + pct(seg_ex.up()) + "\n";
  s += "集中度 (段贡献 = 段超额 × 行数): 最差 10% 的段占全部亏损 " + pct(loss < 0.0 ? worst / loss : kNaN) + " · 最好 10% 的段占全部盈利 " + pct(gain > 0.0 ? best / gain : kNaN) +
       "\n";

  const Px px(L);
  std::vector<Mark> mk(M);
  for (size_t i = 0; i < M; ++i)
    mk[i] = mark_of(px, ts[i].t0, ts[i].a);

  if (seg > 1) {
    const std::vector<Slot> ps = parts_of(L);
    const std::vector<int> pr = part_of_row(ps, seg);
    std::vector<Mean> by(ps.size());
    for (size_t i = 0; i < M; ++i)
      by[static_cast<size_t>(pr[ts[i].t0 % seg])].add(ex[i]);
    s += "\n| 开仓时段 | 段数 | 占比 | 段超额均值 bp | 胜率 |\n|---|---|---|---|---|\n";
    for (size_t j = 0; j < ps.size(); ++j)
      if (by[j].n)
        s += "| " + ps[j].name + " | " + str(by[j].n) + " | " + pct(static_cast<double>(by[j].n) / M) + " | " + bp(by[j].mean()) + " | " + pct(by[j].up()) + " |\n";
  }

  // 标签: 本端真持有格 (等距抽样) vs 池内格 (随机抽样, 固定种子), 同在格上数, 提升与合并方式无关; 段超额按段开仓那行分
  std::array<size_t, tag::N> held{}, base{};
  size_t nh = 0, nb = 0;
  for (size_t i = 0, st = std::max<size_t>(1, cs.size() / kBase); i < cs.size(); i += st, ++nh) {
    const Mark m = mark_of(px, cs[i].t, cs[i].a);
    for (int j = 0; j < tag::N; ++j)
      held[static_cast<size_t>(j)] += m.m >> j & 1u;
  }
  {
    std::mt19937_64 rng(7);
    std::uniform_int_distribution<size_t> pick(0, static_cast<size_t>(T) * A - 1);
    for (size_t tries = 0; nb < kBase && tries < kBase * 20; ++tries) {
      const size_t k = pick(rng);
      if (!L.gate[k] || !std::isfinite(L.label[k]) || !in_win(static_cast<int>(k / A)))
        continue;
      const Mark m = mark_of(px, static_cast<int>(k / A), static_cast<int>(k % A));
      ++nb;
      for (int j = 0; j < tag::N; ++j)
        base[static_cast<size_t>(j)] += m.m >> j & 1u;
    }
  }
  s += "\n| 开仓那行的盘面 | 本端格占比 | 池内格占比 | 提升 | 开仓带此标签的段超额 bp | 不带的 bp |\n|---|---|---|---|---|---|\n";
  for (int j = 0; j < tag::N; ++j) {
    Mean with, without;
    for (size_t i = 0; i < M; ++i)
      (mk[i].m >> j & 1u ? with : without).add(ex[i]);
    const double fs = static_cast<double>(held[static_cast<size_t>(j)]) / nh, fb = nb ? static_cast<double>(base[static_cast<size_t>(j)]) / nb : kNaN;
    if (fs < 0.01 && !(fb >= 0.01))
      continue;
    s += "| " + std::string(kTagNames[j]) + " | " + pct(fs) + " | " + pct(fb) + " | " + (fb > 0.0 ? fmt("%.2f", fs / fb) : "—") + " | " + bp(with.mean()) + " | " +
         bp(without.mean()) + " |\n";
  }

  std::vector<size_t> ix(M);
  std::iota(ix.begin(), ix.end(), size_t{0});
  if (sort == "random")
    std::shuffle(ix.begin(), ix.end(), std::mt19937_64(7));
  else
    std::stable_sort(ix.begin(), ix.end(), [&](size_t x, size_t y) { return sort == "worst" ? ex[x] < ex[y] : ex[x] > ex[y]; });
  ix.resize(std::min<size_t>(M, static_cast<size_t>(c.num("n"))));
  s += "\n明细 (" + sort + (sort == "random" ? "" : "; 尾部个例, 多是个股事件或数据缺失, 不代表信号") + "):\n| # | 代码 | 日期 | 时刻 | 行 | 开仓分位 | 段超额 bp | 开仓时当日涨跌 | 开仓时日内位置 | 盘面 |\n|---|---|---|---|---|---|---|---|---|---|\n";
  for (size_t q = 0; q < ix.size(); ++q) {
    const Trade &x = ts[ix[q]];
    const Mark &m = mk[ix[q]];
    const std::string &k0 = L.clock[static_cast<size_t>(x.t0 % seg)], &k1 = L.clock[static_cast<size_t>(x.t1 % seg)];
    s += "| " + str(q + 1) + " | " + L.codes[static_cast<size_t>(x.a)] + " | " + date_of(L.dates[static_cast<size_t>(x.t0 / seg)]) + " | " + (x.n > 1 ? k0 + "–" + k1 : k0) + " | " +
         std::to_string(x.n) + " | " + rk(r->rank(L.at(x.t0, x.a))) + " | " + bp(x.ex) + " | " + chg(m.chg) + " | " + pos(m.pos) + " | " + tags_of(m.m) + " |\n";
  }
  out = std::move(s);
  return true;
}

// ---- view_holders ----
bool holders_view(const Call &c, const Look &L, std::string &out) {
  const LookRoot *r = root_of(L, out);
  if (!r)
    return false;
  const bool top = c.str("side") == "top";
  const int g = top ? kG20 : kG1, seg = L.seg, T = L.T(), A = L.A();
  struct Who {
    int a = 0, in = 0, side = 0, days = 0, runs = 0;
    Mean ex;
  };
  std::vector<Who> ws(static_cast<size_t>(A));
  for (int a = 0; a < A; ++a)
    ws[a].a = a;
  std::vector<int> last_day(static_cast<size_t>(A), -1);
  for (int t = 0; t < T; ++t)
    for (int a = 0; a < A; ++a) {
      const size_t k = L.at(t, a);
      Who &w = ws[a];
      w.in += L.gate[k];
      if (r->grp(k) != g)
        continue;
      ++w.side;
      if (t / seg != last_day[a])
        ++w.days, last_day[a] = t / seg;
      if (t % seg == 0 || r->grp(L.at(t - 1, a)) != g)
        ++w.runs;
    }
  for (const LookCell &x : r->cells)
    if (x.top == top)
      ws[static_cast<size_t>(x.a)].ex.add(static_cast<double>(x.ret) - r->rows[static_cast<size_t>(x.t)].mkt);
  std::sort(ws.begin(), ws.end(), [](const Who &x, const Who &y) { return x.side != y.side ? x.side > y.side : x.a < y.a; });
  size_t total = 0, any = 0;
  for (const Who &w : ws)
    total += static_cast<size_t>(w.side), any += w.side > 0;
  const std::string side = top ? "做多端 G20" : "最低组 G1";
  std::string s = "## 因子 `" + r->label + "` · " + side + " 的常客\n";
  if (!total) {
    out = s + "本端没有行\n";
    return true;
  }
  size_t acc = 0, half = 0, top10 = 0;
  for (size_t i = 0; i < ws.size(); ++i) {
    if (i < 10)
      top10 += static_cast<size_t>(ws[i].side);
    if (acc * 2 < total)
      acc += static_cast<size_t>(ws[i].side), ++half;
  }
  s += "本端共 " + str(total) + " 行 · 落过本端的资产 " + str(any) + " / " + std::to_string(A) + " · 前 10 只占 " + pct(static_cast<double>(top10) / total) + " · 凑满一半要 " +
       str(half) + " 只\n\n| 代码 | 本端行 | 在池行 | 占其在池 | 天数 | 平均连续行 | 本端持仓格 | 持仓格均超额 bp |\n|---|---|---|---|---|---|---|---|\n";
  for (size_t i = 0; i < std::min<size_t>(ws.size(), static_cast<size_t>(c.num("n"))); ++i) {
    const Who &w = ws[i];
    if (!w.side)
      break;
    s += "| " + L.codes[static_cast<size_t>(w.a)] + " | " + std::to_string(w.side) + " | " + std::to_string(w.in) + " | " + pct(w.in ? static_cast<double>(w.side) / w.in : kNaN) +
         " | " + std::to_string(w.days) + " | " + fmt("%.1f", static_cast<double>(w.side) / w.runs) + " | " + str(w.ex.n) + " | " + bp(w.ex.mean()) + " |\n";
  }
  out = std::move(s);
  return true;
}

// ---- view_response ----
// 逐格的超额对数步长: 本行收盘对上一行收盘 (日首行对昨收, 已除权) 的对数 − 同行在池均值; 没价的格 (停牌 / 缺数据) 原步长记 0
Plane<float> excess_steps(const Look &L, const Px &px) {
  const int T = L.T(), A = L.A(), seg = L.seg;
  Plane<float> e(static_cast<size_t>(T) * static_cast<size_t>(A));
  std::vector<double> st(static_cast<size_t>(A));
  for (int t = 0; t < T; ++t) {
    Mean m;
    for (int a = 0; a < A; ++a) {
      const double c = px(Bar::Close, t, a), b = t % seg == 0 ? px.pre(t, a) : px(Bar::Close, t - 1, a);
      st[static_cast<size_t>(a)] = c > 0.0 && b > 0.0 ? std::log(c / b) : kNaN;
      if (L.gate[L.at(t, a)])
        m.add(st[static_cast<size_t>(a)]);
    }
    const double mk = m.n ? m.mean() : 0.0;
    for (int a = 0; a < A; ++a)
      e[L.at(t, a)] = static_cast<float>((std::isfinite(st[static_cast<size_t>(a)]) ? st[static_cast<size_t>(a)] : 0.0) - mk);
  }
  return e;
}
// 开仓行 t0 与时点 to 之间的累计超额: 之前的点 = 该点 → 开仓 (到开仓为止的涨跌), 之后的点 = 开仓 → 该点
double cum_of(const Plane<float> &e, const Look &L, int a, int t0, int to) {
  double s = 0.0;
  for (int u = std::min(t0, to) + 1; u <= std::max(t0, to); ++u)
    s += e[L.at(u, a)];
  return s;
}
// 均值的标准误, 按开仓日聚类 (同日的段共享截面冲击, 不独立)
double se_of(const Mean &all, const std::vector<Mean> &by) {
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
// 时点: close = 开仓日后第 n 个交易日的收盘行; 否则 = 开仓行偏移 n 行
struct Pt {
  std::string name;
  bool close;
  int n;
};
std::vector<Pt> points_of(int seg) {
  if (seg == 1)
    return {{"-5 日", false, -5}, {"-1 日", false, -1}, {"0 (开仓)", false, 0}, {"+1 日", false, 1}, {"+2 日", false, 2}, {"+3 日", false, 3}, {"+5 日", false, 5}};
  std::vector<Pt> p{{"前 1 日同一行", false, -seg}};
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

bool response_view(const Call &c, const Look &L, std::string &out) {
  const LookRoot *r = root_of(L, out);
  if (!r)
    return false;
  const bool top = c.str("side") == "top", tod = c.str("split") == "tod" && L.seg > 1;
  const int seg = L.seg, T = L.T(), D = static_cast<int>(L.dates.size());
  const std::string side = top ? "做多端 G20" : "最低组 G1";
  std::string s = "## 因子 `" + r->label + "` · " + side + " · 开仓前后的累计超额\n";
  // 事件 = 真持有段 (同资产同日相邻行连成一段, 同 view_trades merge=run) 的开仓行
  std::vector<LookCell> cs;
  for (const LookCell &x : r->cells)
    if (x.top == top)
      cs.push_back(x);
  std::sort(cs.begin(), cs.end(), [](const LookCell &x, const LookCell &y) { return x.a != y.a ? x.a < y.a : x.t < y.t; });
  std::vector<LookCell> ev;
  for (size_t i = 0; i < cs.size(); ++i)
    if (i == 0 || cs[i - 1].a != cs[i].a || cs[i - 1].t + 1 != cs[i].t || cs[i - 1].t / seg != cs[i].t / seg)
      ev.push_back(cs[i]);
  if (ev.empty()) {
    out = s + "本端没有真持有过的格\n";
    return true;
  }
  const size_t n_ev = ev.size(), st = std::max<size_t>(1, n_ev / kEvents);
  if (st > 1) {
    std::vector<LookCell> sub;
    for (size_t i = 0; i < n_ev; i += st)
      sub.push_back(ev[i]);
    ev.swap(sub);
  }
  const Px px(L);
  const Plane<float> e = excess_steps(L, px);
  const std::vector<Pt> pts = points_of(seg);
  const size_t P = pts.size();
  const std::vector<Slot> ps = tod ? parts_of(L) : std::vector<Slot>{};
  const std::vector<int> pr = tod ? part_of_row(ps, seg) : std::vector<int>{};
  std::vector<Mean> ex(P), rank(P);
  std::vector<std::vector<Mean>> by_day(P, std::vector<Mean>(static_cast<size_t>(D)));
  std::vector<std::vector<Mean>> by_part(ps.size(), std::vector<Mean>(P));
  std::vector<size_t> n_part(ps.size());
  for (const LookCell &x : ev) {
    const int d0 = x.t / seg;
    if (tod)
      ++n_part[static_cast<size_t>(pr[x.t % seg])];
    for (size_t j = 0; j < P; ++j) {
      const int to = pts[j].close ? (d0 + pts[j].n) * seg + seg - 1 : x.t + pts[j].n;
      if (to < 0 || to >= T)
        continue;
      const double v = cum_of(e, L, x.a, x.t, to);
      ex[j].add(v), by_day[j][static_cast<size_t>(d0)].add(v);
      if (L.gate[L.at(to, x.a)])
        rank[j].add(r->rank(L.at(to, x.a)));
      if (tod)
        by_part[static_cast<size_t>(pr[x.t % seg])][j].add(v);
    }
  }
  s += "事件 = 真持有段 (同 view_trades merge=run) 的开仓行, 共 " + str(n_ev) + " 段" + (st > 1 ? " (等距抽 " + str(ev.size()) + " 段)" : "") + " · 零点 = 开仓行收盘价\n";
  s += "累计超额 = 对数收益减同行在池均值逐行累计 (跨日按昨收接, 不含除权缺口; 没价的行记 0); 开仓前的点 = 该点到开仓的涨跌, 开仓后的点 = 开仓到该点; 标准误按开仓日聚类\n";
  s += "\n| 时点 | 段数 | 累计超额 bp | ± 标准误 | 为正占比 | 因子分位 (高 = 做多端) |\n|---|---|---|---|---|---|\n";
  for (size_t j = 0; j < P; ++j) {
    const bool zero = !pts[j].close && pts[j].n == 0;
    s += "| " + pts[j].name + " | " + str(ex[j].n) + " | " + (zero ? "0" : bp(ex[j].mean())) + " | " + (zero ? "—" : fmt("%.1f", se_of(ex[j], by_day[j]) * 1e4)) + " | " +
         (zero ? "—" : pct(ex[j].up())) + " | " + rk(rank[j].mean()) + " |\n";
  }

  // 因子分位与此前收益的截面 rank 相关: 抽 kCorrRows 行, 每行在池且分位有效的资产, 取各行的均值
  std::vector<std::pair<std::string, int>> ws;
  if (seg == 1)
    ws = {{"前 1 日", 1}, {"前 5 日", 5}, {"前 20 日", 20}};
  else {
    for (const int w : {5, 30})
      if (w < seg)
        ws.push_back({"前 " + std::to_string(w) + " 行", w});
    ws.push_back({"前 1 日", seg});
  }
  s += "\n开仓行的因子分位与此前收益的截面 rank 相关 (抽 " + std::to_string(kCorrRows) + " 行取均值; 接近 −1 = 因子主要是短期反转的代理, 接近 +1 = 动量的代理):";
  for (size_t i = 0; i < ws.size(); ++i) {
    const int w = ws[i].second, rstep = std::max(1, (T - w) / kCorrRows);
    Mean rho;
    for (int t = w; t < T; t += rstep) {
      std::vector<double> xs, ys;
      for (int a = 0; a < L.A(); ++a)
        if (const size_t k = L.at(t, a); L.gate[k] && std::isfinite(r->rank(k)))
          xs.push_back(r->rank(k)), ys.push_back(cum_of(e, L, a, t, t - w));
      if (static_cast<int>(xs.size()) >= kMinCs)
        rho.add(spearman(xs, ys));
    }
    s += (i ? " · " : " ") + ws[i].first + " " + r3(rho.mean());
  }
  s += "\n";

  if (tod) {
    s += "\n按开仓时段 (各时点的累计超额 bp):\n| 开仓时段 | 段数 |";
    for (const Pt &p : pts)
      s += " " + p.name + " |";
    s += "\n|---|---|";
    for (size_t j = 0; j < P; ++j)
      s += "---|";
    s += "\n";
    for (size_t q = 0; q < ps.size(); ++q) {
      if (!n_part[q])
        continue;
      s += "| " + ps[q].name + " | " + str(n_part[q]) + " |";
      for (size_t j = 0; j < P; ++j)
        s += " " + (!pts[j].close && pts[j].n == 0 ? std::string("0") : bp(by_part[q][j].mean())) + " |";
      s += "\n";
    }
  }
  out = std::move(s);
  return true;
}

// ---- view_explain ----
std::vector<double> centered_ranks(const std::vector<double> &x) {
  std::vector<double> r = ranks_of(x);
  const double m = 0.5 * static_cast<double>(x.size() - 1);
  for (double &v : r)
    v -= m;
  return r;
}
double dot(const std::vector<double> &a, const std::vector<double> &b) { return std::inner_product(a.begin(), a.end(), b.begin(), 0.0); }
// y 对各列 X (都已去均值) 的截面回归残差; 共线 / 常数列靠微小岭项定成 0 方向
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
  for (size_t p = 0; p < m; ++p) // 对称正定: 不选主元
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

bool explain_view(const Call &c, const Look &L, std::string &out) {
  const LookRoot *r = root_of(L, out);
  if (!r)
    return false;
  const std::vector<std::string> xs = c.strs("exprs");
  assert(xs.size() == L.feats.size());
  const int A = L.A(), T = L.T(), seg = L.seg;
  const Px px(L);
  // 逐资产累计超额对数收益: 区间收益 = 两端之差
  const Plane<float> e = excess_steps(L, px);
  std::vector<double> cum(e.size());
  for (int a = 0; a < A; ++a) {
    double acc = 0.0;
    for (int t = 0; t < T; ++t)
      cum[L.at(t, a)] = acc += e[L.at(t, a)];
  }
  const auto ret = [&](int a, int t1, int t0) { return cum[L.at(t1, a)] - cum[L.at(t0, a)]; };
  std::vector<double> amt(static_cast<size_t>(A)), hi(static_cast<size_t>(A)), lo(static_cast<size_t>(A)); // 当日至今 (含本行)
  std::vector<std::pair<std::string, std::function<double(int, int)>>> bs;
  int lag = 0; // 基准要回看的行数
  if (seg > 1) {
    for (const int w : {5, 30})
      if (w < seg)
        bs.push_back({"近 " + std::to_string(w) + " 行收益", [&, w](int t, int a) { return ret(a, t, t - w); }});
    bs.push_back({"当日至今收益", [&](int t, int a) { return ret(a, t, t - t % seg - 1); }});
    bs.push_back({"前 1 日收益", [&](int t, int a) { return ret(a, t - t % seg - 1, t - t % seg - 1 - seg); }});
    bs.push_back({"前 5 日收益", [&](int t, int a) { return ret(a, t - t % seg - 1, t - t % seg - 1 - 5 * seg); }});
    bs.push_back({"当日至今成交额", [&](int, int a) { return amt[a]; }});
    bs.push_back({"当日至今振幅", [&](int t, int a) { return hi[a] > lo[a] ? (hi[a] - lo[a]) / px.pre(t, a) : 0.0; }});
    bs.push_back({"日内位置", [&](int t, int a) { return hi[a] > lo[a] ? (px(Bar::Close, t, a) - lo[a]) / (hi[a] - lo[a]) : 0.5; }});
    lag = 6 * seg;
  } else {
    for (const int w : {1, 5, 20})
      bs.push_back({w == 1 ? std::string("当日收益") : "近 " + std::to_string(w) + " 日收益", [&, w](int t, int a) { return ret(a, t, t - w); }});
    bs.push_back({"成交额", [&](int t, int a) { return px(Bar::Amount, t, a); }});
    bs.push_back({"振幅", [&](int t, int a) { return (px(Bar::High, t, a) - px(Bar::Low, t, a)) / px.pre(t, a); }});
    lag = 21;
  }
  bs.push_back({"价格", [&](int t, int a) { return px(Bar::Close, t, a); }});
  std::string s = "## 因子 `" + r->label + "` 的成分\n";
  for (size_t i = 0; i < xs.size(); ++i) {
    s += "E" + str(i + 1) + " = `" + xs[i] + "`" + (L.feats[i].error.empty() ? "" : " (没算成, 不进基准: " + L.feats[i].error + ")") + "\n";
    if (L.feats[i].error.empty())
      bs.push_back({"E" + str(i + 1), [&, i](int t, int a) { return static_cast<double>(L.feats[i].v[L.at(t, a)]); }});
  }
  const size_t m = bs.size();
  if (T <= lag) {
    out = s + "评估期太短: 基准要回看 " + std::to_string(lag) + " 行\n";
    return false;
  }
  const int rstep = std::max(1, (T - lag) / kExplainRows);

  Mean fric, r2, rric;
  std::vector<Mean> corr(m), bric(m), sric(m);
  Layer orig, rest;
  LookRow w;
  std::vector<double> fx, ey, one(m);
  std::vector<std::vector<double>> bv(m), X(m);
  for (int t = 0; t < T; ++t) {
    for (int a = 0; a < A && seg > 1; ++a) {
      if (t % seg == 0)
        amt[a] = 0.0, hi[a] = -INFINITY, lo[a] = INFINITY;
      if (const double v = px(Bar::Amount, t, a); std::isfinite(v))
        amt[a] += v;
      if (px.traded(t, a))
        hi[a] = std::fmax(hi[a], px(Bar::High, t, a)), lo[a] = std::fmin(lo[a], px(Bar::Low, t, a));
    }
    if (t < lag || (t - lag) % rstep || !std::isfinite(L.mkt[static_cast<size_t>(t)]))
      continue;
    fx.clear(), ey.clear();
    for (std::vector<double> &v : bv)
      v.clear();
    for (int a = 0; a < A; ++a) {
      const size_t k = L.at(t, a);
      if (!L.gate[k] || !std::isfinite(r->rank(k)) || !std::isfinite(L.label[k]))
        continue;
      bool ok = true;
      for (size_t j = 0; j < m && ok; ++j)
        ok = std::isfinite(one[j] = bs[j].second(t, a));
      if (!ok)
        continue;
      fx.push_back(r->rank(k)), ey.push_back(static_cast<double>(L.label[k]) - L.mkt[static_cast<size_t>(t)]);
      for (size_t j = 0; j < m; ++j)
        bv[j].push_back(one[j]);
    }
    if (fx.size() < static_cast<size_t>(kMinCs))
      continue;
    const std::vector<double> rf = centered_ranks(fx), ry = ranks_of(ey);
    fric.add(pearson(rf, ry));
    for (size_t j = 0; j < m; ++j) {
      X[j] = centered_ranks(bv[j]);
      corr[j].add(pearson(rf, X[j])), bric[j].add(pearson(X[j], ry));
      const double xx = dot(X[j], X[j]), b = xx > 0.0 ? dot(X[j], rf) / xx : 0.0;
      std::vector<double> res = rf;
      for (size_t i = 0; i < res.size(); ++i)
        res[i] -= b * X[j][i];
      sric[j].add(pearson(ranks_of(res), ry));
    }
    const std::vector<double> res = resid_of(X, rf);
    const double tot = dot(rf, rf);
    if (tot > 0.0)
      r2.add(1.0 - dot(res, res) / tot);
    rric.add(pearson(ranks_of(res), ry));
    if (approx_row(fx, ey, w))
      orig.add(w);
    if (approx_row(res, ey, w))
      rest.add(w);
  }
  if (!fric.n) {
    out = s + "没有可用的行 (每行至少要 " + std::to_string(kMinCs) + " 个在池、标签有效且全部基准有值的格)\n";
    return false;
  }
  s += "抽 " + str(fric.n) + " 行, 每行取在池、标签有效且全部基准有值的格; 收益类基准是对数收益减同行在池均值 (跨日按昨收接); 相关与 rIC 都是逐行截面 Spearman 的均值\n";
  s += "因子 rIC (同口径) " + r3(fric.mean()) + "\n\n| 基准 | 与因子的截面秩相关 | 基准自身 rIC | 只剥它后的因子 rIC |\n|---|---|---|---|\n";
  for (size_t j = 0; j < m; ++j)
    s += "| " + bs[j].first + " | " + r3(corr[j].mean()) + " | " + r3(bric[j].mean()) + " | " + r3(sric[j].mean()) + " |\n";
  s += "\n全部基准一起剥 (每行把因子秩对全部基准秩做截面回归, 取残差): 平均 R² " + r3(r2.mean()) + " · 因子 rIC " + r3(fric.mean()) + " → 剩余部分 " + r3(rric.mean()) + "\n";
  s += "20 组按标签逐行近似分 (组超额 = 组内 (标签 − 本行在池均值) 等权均值, 建不了仓的格不剔), 与回测 / view_layers 的真持有口径略有出入, 只作两行之间对照\n\n";
  s += "|  | 行 | 有统计 | mono | 多空 bp | rIC | G1 bp | G10 bp | G20 bp | 20 组 |\n|---|---|---|---|---|---|---|---|---|---|\n";
  s += layers_row("原因子", orig) + layers_row("剩余部分", rest);
  out = std::move(s);
  return true;
}

// ---- 个股 ----
// 叠加的因子 (可选) 与特征式: 因子在 factors[0]
struct Over {
  const LookRoot *f = nullptr;
  std::vector<std::string> xs;
  std::vector<const LookFeat *> fs;
};
bool over_of(const Call &c, const Look &L, Over &o, std::string &out) {
  if (!L.factors.empty() && !(o.f = root_of(L, out)))
    return false;
  o.xs = c.strs("exprs");
  assert(L.feats.size() == o.xs.size());
  for (const LookFeat &f : L.feats)
    o.fs.push_back(&f);
  return true;
}
double feat_at(const LookFeat *f, size_t k) { return f->error.empty() ? f->v[k] : kNaN; }
std::string over_head(const Over &o) {
  std::string s;
  for (size_t i = 0; i < o.xs.size(); ++i)
    s += "E" + str(i + 1) + " = `" + o.xs[i] + "`" + (o.fs[i]->error.empty() ? "" : " (没算成: " + o.fs[i]->error + ")") + "\n";
  return s;
}

int day_of(const Look &L, const std::string &arg, std::string &out) {
  const int ymd = ymd_of(arg);
  const auto it = std::find(L.dates.begin(), L.dates.end(), ymd);
  if (it == L.dates.end()) {
    out = "日期 " + arg + " 不是评估期 (" + date_of(L.dates.front()) + " ~ " + date_of(L.dates.back()) + ") 里的交易日";
    return -1;
  }
  return static_cast<int>(it - L.dates.begin());
}

bool stock_day(const Call &c, const Look &L, const Context &ctx, std::string &out) {
  assert(L.A() == 1 && L.seg > 1);
  const int d = day_of(L, c.str("date"), out);
  if (d < 0)
    return false;
  const Px px(L);
  Over o;
  if (!over_of(c, L, o, out))
    return false;
  const int seg = L.seg, t0 = d * seg;
  const auto col = [&](auto &&f) {
    std::vector<double> v(static_cast<size_t>(seg));
    for (int k = 0; k < seg; ++k)
      v[k] = f(t0 + k);
    return v;
  };
  const std::vector<double> close = col([&](int t) { return px(Bar::Close, t, 0); }), high = col([&](int t) { return px(Bar::High, t, 0); }),
                            low = col([&](int t) { return px(Bar::Low, t, 0); }), vol = col([&](int t) { return px(Bar::Volume, t, 0); }),
                            amt = col([&](int t) { return px(Bar::Amount, t, 0); });
  const double pre = px.pre(t0, 0);
  double hi = -INFINITY, lo = INFINITY, vsum = 0.0, last = kNaN;
  int in = 0, first = -1;
  Mean amean;
  for (int k = 0; k < seg; ++k) {
    if (px.traded(t0 + k, 0)) {
      hi = std::fmax(hi, high[k]), lo = std::fmin(lo, low[k]);
      first = first < 0 ? k : first;
    }
    vsum += std::isfinite(vol[k]) ? vol[k] : 0.0, amean.add(amt[k]);
    last = std::isfinite(close[k]) ? close[k] : last;
    in += L.gate[static_cast<size_t>(t0 + k)];
  }
  std::string s = "## " + L.codes[0] + " · " + date_of(L.dates[static_cast<size_t>(d)]) + " · " + ctx.level + " (每天 " + std::to_string(seg) + " 行)\n";
  const std::string open = first < 0 ? "当日无成交" : "首个有成交行 (" + L.clock[static_cast<size_t>(first)] + ") 收 " + num(close[first]) + " (" + chg(close[first] / pre - 1.0) + ")";
  s += "昨收 " + num(pre) + " · " + open + " · 高 " + num(hi) + " (" + chg(hi / pre - 1.0) + ") · 低 " + num(lo) + " (" + chg(lo / pre - 1.0) + ") · 收 " + num(last) + " (" +
       chg(last / pre - 1.0) + ") · 涨停 " + num(px(Bar::LimUp, t0, 0)) + " / 跌停 " + num(px(Bar::LimDn, t0, 0)) + " · 成交额 " + wan(amean) + " 万 · 在池 " +
       std::to_string(in) + " / " + std::to_string(seg) + " 行\n";
  s += over_head(o) + "\n";
  s += "- 收盘价 " + spark(close) + "\n- 成交量 " + spark(vol) + "\n";
  if (o.f)
    s += "- 因子 `" + o.f->label + "` 分位 (高 = 做多端) " + spark(col([&](int t) { return static_cast<double>(o.f->rank(static_cast<size_t>(t))); }), rk) + "\n";
  for (size_t i = 0; i < o.fs.size(); ++i)
    s += "- E" + str(i + 1) + " " + spark(col([&](int t) { return feat_at(o.fs[i], static_cast<size_t>(t)); })) + "\n";

  s += "\n| 时段 | 涨跌 | 段内高 | 段内低 | 量占比 |" + std::string(o.f ? " 因子分位 | 多/空行 |" : "") + " 后续超额 bp |";
  for (size_t i = 0; i < o.xs.size(); ++i)
    s += " E" + str(i + 1) + " 段末 |";
  s += "\n|---|---|---|---|---|" + std::string(o.f ? "---|---|" : "") + "---|";
  for (size_t i = 0; i < o.xs.size(); ++i)
    s += "---|";
  s += "\n";
  for (const Slot &p : parts_of(L)) {
    double h = -INFINITY, l = INFINITY, v = 0.0;
    std::vector<double> rks, exs;
    int up = 0, dn = 0;
    for (int k = p.k0; k < p.k1; ++k) {
      const size_t t = static_cast<size_t>(t0 + k);
      if (px.traded(t0 + k, 0))
        h = std::fmax(h, high[k]), l = std::fmin(l, low[k]);
      v += std::isfinite(vol[k]) ? vol[k] : 0.0;
      exs.push_back(static_cast<double>(L.label[t]) - L.mkt[t]);
      if (o.f) {
        rks.push_back(o.f->rank(t));
        up += o.f->grp(t) == kG20, dn += o.f->grp(t) == kG1;
      }
    }
    const double from = p.k0 == 0 ? pre : close[static_cast<size_t>(p.k0 - 1)];
    s += "| " + p.name + " | " + chg(close[static_cast<size_t>(p.k1 - 1)] / from - 1.0) + " | " + chg(h / pre - 1.0) + " | " + chg(l / pre - 1.0) + " | " +
         pct(vsum > 0.0 ? v / vsum : kNaN) + " |" + (o.f ? " " + rk(mean_of(rks)) + " | " + std::to_string(up) + "/" + std::to_string(dn) + " |" : std::string()) + " " +
         bp(mean_of(exs)) + " |";
    for (const LookFeat *f : o.fs)
      s += " " + num(feat_at(f, static_cast<size_t>(t0 + p.k1 - 1))) + " |";
    s += "\n";
  }

  if (const std::string w = c.str("rows"); !w.empty()) {
    int k0 = 0, k1 = 0;
    std::string err;
    [[maybe_unused]] const bool ok = rows_in("rows", w, L.clock, kStockRows, k0, k1, err);
    assert(ok && "LookOf 已查过");
    const auto whole = [](double x) { return std::isfinite(x) ? fmt("%.0f", x) : std::string("—"); };
    s += "\n逐行 " + w + " (涨跌 = 对上一行收盘; 当日 = 对昨收; 额 万元):\n| 时刻 | 收 | 涨跌 | 当日 | 量 | 额 |" + std::string(o.f ? " 因子分位 | 组 |" : "") + " 后续超额 bp |";
    for (size_t i = 0; i < o.xs.size(); ++i)
      s += " E" + str(i + 1) + " |";
    s += "\n|---|---|---|---|---|---|" + std::string(o.f ? "---|---|" : "") + "---|";
    for (size_t i = 0; i < o.xs.size(); ++i)
      s += "---|";
    s += "\n";
    for (int k = k0; k < k1; ++k) {
      const size_t t = static_cast<size_t>(t0 + k), q = static_cast<size_t>(k);
      const double prev = k == 0 ? pre : close[q - 1];
      s += "| " + L.clock[q] + " | " + num(close[q]) + " | " + chg(close[q] / prev - 1.0) + " | " + chg(close[q] / pre - 1.0) + " | " + whole(vol[q]) + " | " +
           (std::isfinite(amt[q]) ? fmt("%.1f", amt[q] / 1e4) : std::string("—")) + " |";
      if (o.f)
        s += " " + rk(o.f->rank(t)) + " | " + (o.f->grp(t) < 0 ? std::string("—") : gname(o.f->grp(t))) + " |";
      s += " " + bp(static_cast<double>(L.label[t]) - L.mkt[t]) + " |";
      for (const LookFeat *f : o.fs)
        s += " " + num(feat_at(f, t)) + " |";
      s += "\n";
    }
  }
  out = std::move(s);
  return true;
}

bool stock_days(const Call &c, const Look &L, const Context &ctx, std::string &out) {
  assert(L.A() == 1);
  const int D = static_cast<int>(L.dates.size());
  const int d1 = c.str("to").empty() ? D - 1 : day_of(L, c.str("to"), out);
  if (d1 < 0)
    return false;
  const int d0 = c.str("from").empty() ? std::max(0, d1 - 19) : day_of(L, c.str("from"), out);
  if (d0 < 0)
    return false;
  if (d0 > d1 || d1 - d0 + 1 > kStockDays) {
    out = "from ~ to 须是先后有序且至多 " + std::to_string(kStockDays) + " 个交易日 (这里是 " + std::to_string(d1 - d0 + 1) + " 个)";
    return false;
  }
  const Px px(L);
  Over o;
  if (!over_of(c, L, o, out))
    return false;
  const int seg = L.seg, n = d1 - d0 + 1;
  std::vector<double> sc(n), sg(n), sf(n, kNaN);
  std::vector<std::vector<double>> se(o.fs.size(), std::vector<double>(static_cast<size_t>(n)));
  std::string rows;
  for (int d = d0; d <= d1; ++d) {
    const int t0 = d * seg, tl = t0 + seg - 1;
    double hi = -INFINITY, lo = INFINITY, last = kNaN;
    int in = 0, up = 0, dn = 0;
    Mean amean;
    std::vector<double> rks;
    for (int t = t0; t <= tl; ++t) {
      if (px.traded(t, 0))
        hi = std::fmax(hi, px(Bar::High, t, 0)), lo = std::fmin(lo, px(Bar::Low, t, 0));
      amean.add(px(Bar::Amount, t, 0));
      if (const double x = px(Bar::Close, t, 0); std::isfinite(x))
        last = x;
      in += L.gate[static_cast<size_t>(t)];
      if (o.f) {
        rks.push_back(o.f->rank(static_cast<size_t>(t)));
        up += o.f->grp(static_cast<size_t>(t)) == kG20, dn += o.f->grp(static_cast<size_t>(t)) == kG1;
      }
    }
    const double pre = px.pre(t0, 0), g = last / pre - 1.0;
    const size_t i = static_cast<size_t>(d - d0);
    sc[i] = last, sg[i] = g;
    rows += "| " + date_of(L.dates[static_cast<size_t>(d)]) + " | " + chg(g) + " | " + pct(hi > lo ? (hi - lo) / pre : kNaN) + " | " + wan(amean) + " | " +
            std::to_string(in) + " |";
    if (o.f) {
      sf[i] = mean_of(rks);
      rows += " " + rk(sf[i]) + " | " + rk(o.f->rank(static_cast<size_t>(tl))) + " | " + std::to_string(up) + "/" + std::to_string(dn) + " |";
    }
    rows += " " + bp(static_cast<double>(L.label[static_cast<size_t>(tl)]) - L.mkt[static_cast<size_t>(tl)]) + " |";
    for (size_t j = 0; j < o.fs.size(); ++j) {
      se[j][i] = feat_at(o.fs[j], static_cast<size_t>(tl));
      rows += " " + num(se[j][i]) + " |";
    }
    rows += "\n";
  }
  std::string s = "## " + L.codes[0] + " · " + date_of(L.dates[static_cast<size_t>(d0)]) + " ~ " + date_of(L.dates[static_cast<size_t>(d1)]) + " (" + std::to_string(n) +
                  " 日) · " + ctx.level + "\n" + over_head(o) + "\n";
  s += "- 收盘价 " + spark(sc) + "\n- 当日涨跌 " + spark(sg, chg) + "\n";
  if (o.f)
    s += "- 因子 `" + o.f->label + "` 日均分位 (高 = 做多端) " + spark(sf, rk) + "\n";
  for (size_t j = 0; j < o.fs.size(); ++j)
    s += "- E" + str(j + 1) + " 收盘值 " + spark(se[j]) + "\n";
  s += "\n| 日期 | 涨跌 | 振幅 | 成交额 万 | 在池行 |" + std::string(o.f ? " 因子日均分位 | 收盘分位 | 多/空行 |" : "") + " 收盘开仓超额 bp |";
  for (size_t j = 0; j < o.xs.size(); ++j)
    s += " E" + str(j + 1) + " 收盘 |";
  s += "\n|---|---|---|---|---|" + std::string(o.f ? "---|---|---|" : "") + "---|";
  for (size_t j = 0; j < o.xs.size(); ++j)
    s += "---|";
  out = s + "\n" + rows;
  return true;
}

// ---- view_snapshot ----
// 日内层: "HH:MM" = 行末时刻 → 日内行; 日层恒 0. 没有该行 → −1
int row_at(const std::vector<std::string> &clock, const std::string &hm) {
  if (clock.size() == 1)
    return 0;
  const auto it = std::find(clock.begin(), clock.end(), hm);
  return it == clock.end() ? -1 : static_cast<int>(it - clock.begin());
}

bool snapshot_view(const Call &c, const Look &L, const Context &ctx, std::string &out) {
  const LookRoot *r = root_of(L, out);
  if (!r)
    return false;
  const int d = day_of(L, c.str("date"), out);
  if (d < 0)
    return false;
  const int seg = L.seg, A = L.A(), k = row_at(L.clock, c.str("time")), t = d * seg + k, t0 = d * seg;
  assert(k >= 0 && "LookOf 已查过");
  const Px px(L);
  const std::vector<std::string> xs = c.strs("exprs");
  assert(xs.size() == L.feats.size());
  // 本行的一格: 分位 / 当日至此成交额 / 本行涨跌 (日层 = 当日涨跌) / 后续超额 / 开仓那行的盘面 / 真持有端 (+1 做多端 / −1 最低组 / 0)
  struct Cell {
    int a = 0;
    double rank = kNaN, amt = 0.0, step = kNaN, ex = kNaN;
    Mark m;
    int held = 0;
  };
  std::vector<int> held(static_cast<size_t>(A), 0);
  for (const LookCell &x : r->cells)
    if (x.t == t)
      held[static_cast<size_t>(x.a)] = x.top ? 1 : -1;
  std::vector<Cell> cs;
  for (int a = 0; a < A; ++a) {
    const size_t q = L.at(t, a);
    if (!L.gate[q] || !std::isfinite(r->rank(q)))
      continue;
    Cell x{a, r->rank(q)};
    for (int u = t0; u <= t; ++u)
      if (const double v = px(Bar::Amount, u, a); std::isfinite(v))
        x.amt += v;
    const double cl = px(Bar::Close, t, a), prev = k == 0 ? px.pre(t, a) : px(Bar::Close, t - 1, a);
    x.step = cl / prev - 1.0;
    x.ex = static_cast<double>(L.label[q]) - L.mkt[static_cast<size_t>(t)];
    x.m = mark_of(px, t, a);
    x.held = held[static_cast<size_t>(a)];
    cs.push_back(x);
  }
  const std::string when = date_of(L.dates[static_cast<size_t>(d)]) + (seg > 1 ? " " + L.clock[static_cast<size_t>(k)] : "");
  std::string s = "## 因子 `" + r->label + "` · " + when + " 那一行的截面 (" + ctx.level + "; 分位 / 组号已定向: G20 = 做多端)\n";
  if (cs.size() < static_cast<size_t>(kMinCs)) {
    out = s + "本行在池且因子有秩的只有 " + str(cs.size()) + " 只";
    return false;
  }
  std::sort(cs.begin(), cs.end(), [](const Cell &x, const Cell &y) { return x.rank > y.rank; });
  const LookRow &w = r->rows[static_cast<size_t>(t)];
  s += "在池且有秩 " + str(cs.size()) + " 只 · 本行" + (w.ok ? "有统计: rIC " + r3(w.ric) + " · G20 超额 " + bp(w.grp[kG20]) + "bp (真持有 " + std::to_string(w.cnt[kG20]) + " 只) · G1 超额 " + bp(w.grp[kG1]) + "bp (" + std::to_string(w.cnt[kG1]) + " 只)" : std::string("无统计 (建不了仓 / 标签无效)")) +
       " · 后续超额 = 本行开仓的持有期收益 − 在池均值\n";
  for (size_t i = 0; i < xs.size(); ++i)
    s += "E" + str(i + 1) + " = `" + xs[i] + "`" + (L.feats[i].error.empty() ? "" : " (没算成: " + L.feats[i].error + ")") + "\n";
  const auto ev = [&](size_t i, const Cell &x) { return L.feats[i].error.empty() ? static_cast<double>(L.feats[i].v[L.at(t, x.a)]) : kNaN; };

  // 与因子分位的截面 Spearman (全部在池有秩格)
  const auto corr = [&](auto &&f) {
    std::vector<double> u, v;
    for (const Cell &x : cs)
      if (const double y = f(x); std::isfinite(y))
        u.push_back(x.rank), v.push_back(y);
    return u.size() >= static_cast<size_t>(kMinCs) ? spearman(u, v) : kNaN;
  };
  s += "与因子分位的截面秩相关: 当日涨跌 " + r3(corr([](const Cell &x) { return x.m.chg; })) + (seg > 1 ? " · 本行涨跌 " + r3(corr([](const Cell &x) { return x.step; })) : std::string()) +
       " · 日内位置 " + r3(corr([](const Cell &x) { return x.m.pos; })) + " · 当日至此成交额 " + r3(corr([](const Cell &x) { return x.amt; })) + " · 后续超额 " +
       r3(corr([](const Cell &x) { return x.ex; }));
  for (size_t i = 0; i < xs.size(); ++i)
    s += " · E" + str(i + 1) + " " + r3(corr([&](const Cell &x) { return ev(i, x); }));
  s += "\n\n| 端 | 代码 | 分位 | 组 | 真持有 | 当日涨跌 |" + std::string(seg > 1 ? " 本行涨跌 | 日内位置 |" : "") + " 至此额 万 | 后续超额 bp | 盘面 |";
  for (size_t i = 0; i < xs.size(); ++i)
    s += " E" + str(i + 1) + " |";
  s += "\n|---|---|---|---|---|---|" + std::string(seg > 1 ? "---|---|" : "") + "---|---|---|";
  for (size_t i = 0; i < xs.size(); ++i)
    s += "---|";
  s += "\n";
  const auto line = [&](const std::string &end, const Cell &x) {
    s += "| " + end + " | " + L.codes[static_cast<size_t>(x.a)] + " | " + rk(x.rank) + " | " + gname(r->grp(L.at(t, x.a))) + " | " + (x.held > 0 ? "多" : x.held < 0 ? "空"
                                                                                                                                                                     : "") +
         " | " + chg(x.m.chg) +
         " |" + (seg > 1 ? " " + chg(x.step) + " | " + pos(x.m.pos) + " |" : std::string()) + " " + fmt("%.0f", x.amt / 1e4) + " | " + bp(x.ex) + " | " + tags_of(x.m.m) + " |";
    for (size_t i = 0; i < xs.size(); ++i)
      s += " " + num(ev(i, x)) + " |";
    s += "\n";
  };
  const size_t n = std::min(cs.size() / 2, static_cast<size_t>(c.num("n")));
  for (size_t i = 0; i < n; ++i)
    line("高", cs[i]);
  // 池中位: 各列的截面中位数
  {
    std::vector<double> chs, sts, pss, ams, exs;
    for (const Cell &x : cs)
      chs.push_back(x.m.chg), sts.push_back(x.step), pss.push_back(x.m.pos), ams.push_back(x.amt), exs.push_back(x.ex);
    s += "| 池中位 | | 50 | | | " + chg(median_of(chs)) + " |" + (seg > 1 ? " " + chg(median_of(sts)) + " | " + pos(median_of(pss)) + " |" : std::string()) + " " +
         fmt("%.0f", median_of(ams) / 1e4) + " | " + bp(median_of(exs)) + " | |";
    for (size_t i = 0; i < xs.size(); ++i) {
      std::vector<double> v;
      for (const Cell &x : cs)
        v.push_back(ev(i, x));
      s += " " + num(median_of(v)) + " |";
    }
    s += "\n";
  }
  for (size_t i = 0; i < n; ++i)
    line("低", cs[cs.size() - 1 - i]);
  out = std::move(s);
  return true;
}

// ---- view_book ----
constexpr int kAm0 = 9 * 3600 + 15 * 60, kAm1 = 11 * 3600 + 30 * 60, kPm0 = 13 * 3600, kPm1 = 15 * 3600; // 交易时段 (钟面秒)
constexpr int kOpenCall = 9 * 3600 + 25 * 60, kCloseCall = 14 * 3600 + 57 * 60;                          // 开盘竞价 [kAm0, kOpenCall) / 收盘竞价 [kCloseCall, kPm1)
constexpr size_t kProfile = 8;                                                                           // 剖面每侧几个价位
constexpr int kBarCells = 12;                                                                            // 剖面条形图的满格
constexpr int kSteps[] = {1, 2, 3, 5, 10, 15, 20, 30, 60, 120, 300, 600};                                // 自动 step 的候选
constexpr const char *kKindCn[] = {"新挂", "撤单", "被吃完", "扫单"};                                    // 下标 = BookKind

bool in_session(int c) { return (c >= kAm0 && c < kAm1) || (c >= kPm0 && c < kPm1); }
// "HH:MM" / "HH:MM:SS" → 钟面秒; 不成 → −1
int clock_of(const std::string &s) {
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
// 逐时表每行几秒: 给了照用, 0 = 候选里使行数 ≤ kBookRows 的最小者
int step_of(int want, int secs) {
  if (want > 0)
    return want;
  for (const int s : kSteps)
    if ((secs + s - 1) / s <= kBookRows)
      return s;
  return kSteps[std::size(kSteps) - 1];
}
std::string w4(double yuan) { // 元 → 万元
  const double v = yuan / 1e4;
  return v == 0.0 ? "0" : std::fabs(v) < 10.0 ? fmt("%.1f", v)
                                              : fmt("%.0f", v);
}
std::string px2(double p) { return p > 0.0 ? fmt("%.2f", p) : "—"; } // 无 (0 / NaN) → —

} // namespace

bool LookOf(const Call &c, const Lab &lab, LookSpec &out, std::string &err) {
  const Context &ctx = lab.ctx();
  out = {};
  const auto factor = [&](const std::string &arg) {
    LookFactor f;
    if (!lab.factor_of(arg, f, err)) {
      err = "factor: " + err;
      return false;
    }
    out.factors.push_back(std::move(f));
    return true;
  };
  // 各式各自校验: 不成的带着原因不算, 其余照算; 一个也不成 → false (只有 view_feature 当它没做成)
  const auto feats = [&](const std::vector<std::string> &xs) {
    bool any = false;
    for (size_t i = 0; i < xs.size(); ++i) {
      LookExpr x;
      if (lab.feature_of(xs[i], x.root, x.error))
        any = true;
      else
        err += (err.empty() ? "" : "; ") + ("exprs[" + str(i) + "]: " + x.error);
      out.feats.push_back(std::move(x));
    }
    return any;
  };
  const auto date = [&](const char *k) {
    if (const std::string v = c.str(k); !v.empty() && !ymd_of(v)) {
      err = std::string(k) + ": 日期写成 YYYY-MM-DD";
      return false;
    }
    return true;
  };
  switch (c.tool) {
  case Tool::ViewFeature:
    return feats(c.strs("exprs"));
  case Tool::ViewLayers:
    if (const std::string k = c.str("cond"); !k.empty()) {
      LookExpr x;
      if (!lab.feature_of(k, x.root, x.error)) {
        err = "cond: " + x.error;
        return false;
      }
      out.feats.push_back(std::move(x));
    }
    return factor(c.str("factor"));
  case Tool::ViewHolders:
    return factor(c.str("factor"));
  case Tool::ViewTrades:
    if (!date("from") || !date("to"))
      return false;
    if (const std::string w = c.str("tod"); !w.empty()) {
      if (ctx.seg == 1) {
        err = "tod: 日层每天只有一行, 没有日内时段可选";
        return false;
      }
      if (int k0 = 0, k1 = 0; !rows_in("tod", w, ctx.clock, 0, k0, k1, err))
        return false;
    }
    out.kline = true;
    return factor(c.str("factor"));
  case Tool::ViewResponse:
    out.kline = true;
    return factor(c.str("factor"));
  case Tool::ViewSnapshot:
    if (!date("date"))
      return false;
    if (ctx.seg > 1 && (c.str("time").empty() || row_at(ctx.clock, c.str("time")) < 0)) {
      err = "time: 写成本层某一行的行末时刻 HH:MM (本层 " + ctx.clock.front() + ", " + ctx.clock[1] + " … " + ctx.clock.back() + ")";
      return false;
    }
    out.kline = true;
    if (!factor(c.str("factor")))
      return false;
    feats(c.strs("exprs"));
    err.clear();
    return true;
  case Tool::ViewExplain:
    out.kline = true;
    if (!factor(c.str("factor")))
      return false;
    feats(c.strs("exprs"));
    err.clear();
    return true;
  case Tool::ViewStockDay:
  case Tool::ViewStockDays:
    if (c.tool == Tool::ViewStockDay && ctx.seg == 1) {
      err = "日层每天只有一行, 看个股用 view_stock_days";
      return false;
    }
    if (!lab.code_of(c.str("code"), out.code, err)) {
      err = "code: " + err;
      return false;
    }
    if (c.tool == Tool::ViewStockDay ? !date("date") : !date("from") || !date("to"))
      return false;
    if (int k0 = 0, k1 = 0; c.tool == Tool::ViewStockDay && !c.str("rows").empty() && !rows_in("rows", c.str("rows"), ctx.clock, kStockRows, k0, k1, err))
      return false;
    if (const std::string f = c.str("factor"); !f.empty() && !factor(f))
      return false;
    out.kline = true;
    feats(c.strs("exprs"));
    err.clear();
    return true;
  default:
    assert(false && "不是观察工具");
    return false;
  }
}

std::string RepText(const Look &L) {
  if (!L.error.empty())
    return "- 代表点没观察成: " + L.error + "\n";
  std::string why;
  const LookRoot *r = root_of(L, why);
  if (!r)
    return "- 代表点" + why + "\n";
  Layer all;
  for (const LookRow &w : r->rows)
    all.add(w);
  std::string s = "- 代表点的 20 组每期超额 G1 → G20: " + spark(all.grps(), bp) + " bp · mono " + r3(all.mono()) + " · 多空 " + bp(all.ls()) + "bp\n";
  if (L.seg > 1) {
    const std::vector<Slot> ps = parts_of(L);
    const std::vector<int> pr = part_of_row(ps, L.seg);
    std::vector<Layer> bs(ps.size());
    for (int t = 0; t < L.T(); ++t)
      bs[static_cast<size_t>(pr[t % L.seg])].add(r->rows[t]);
    s += "- 代表点分时段 mono:";
    for (size_t j = 0; j < ps.size(); ++j)
      s += (j ? " · " : " ") + ps[j].name + " " + r3(bs[j].mono());
    s += "\n";
  }
  return s + "- 观察工具的 factor 写本候选名即看代表点\n";
}

bool Render(const Call &c, const Look &L, const Context &ctx, std::string &out) {
  if (!L.error.empty()) {
    out = "没算成: " + L.error;
    return false;
  }
  assert(!L.dates.empty() && L.seg == ctx.seg);
  std::string body;
  bool ok = true;
  switch (c.tool) {
  case Tool::ViewFeature:
    body = feature_view(c, L);
    break;
  case Tool::ViewLayers:
    ok = layers_view(c, L, body);
    break;
  case Tool::ViewTrades:
    ok = trades_view(c, L, body);
    break;
  case Tool::ViewHolders:
    ok = holders_view(c, L, body);
    break;
  case Tool::ViewResponse:
    ok = response_view(c, L, body);
    break;
  case Tool::ViewExplain:
    ok = explain_view(c, L, body);
    break;
  case Tool::ViewStockDay:
    ok = stock_day(c, L, ctx, body);
    break;
  case Tool::ViewStockDays:
    ok = stock_days(c, L, ctx, body);
    break;
  case Tool::ViewSnapshot:
    ok = snapshot_view(c, L, ctx, body);
    break;
  default:
    assert(false && "不是观察工具");
  }
  out = ok ? head(L, ctx) + body : body;
  return ok;
}

bool BookOf(const Call &c, const Lab &lab, BookSpec &out, std::string &err) {
  out = {};
  if (!lab.code_of(c.str("code"), out.code, err)) {
    err = "code: " + err;
    return false;
  }
  if (out.date = ymd_of(c.str("date")); out.date == 0) {
    err = "date: 日期写成 YYYY-MM-DD";
    return false;
  }
  const std::string w = c.str("time");
  const size_t dash = w.find('-');
  const int from = dash == std::string::npos ? -1 : clock_of(w.substr(0, dash)), to = dash == std::string::npos ? -1 : clock_of(w.substr(dash + 1));
  if (from < 0 || to < 0) {
    err = "time: 写成 HH:MM-HH:MM 或 HH:MM:SS-HH:MM:SS (如 10:00-10:30)";
    return false;
  }
  if (from >= to || !in_session(from) || !in_session(to - 1)) {
    err = "time: " + w + " 须起 < 止, 两端在 09:15–11:30 / 13:00–15:00 内 (止是开区间, 可写 11:30 / 15:00)";
    return false;
  }
  const int n = TradeSec(to - 1) + 1 - TradeSec(from);
  if (n > kBookMins * 60) {
    err = "time: " + w + " 有 " + std::to_string((n + 59) / 60) + " 分钟交易时间, 至多 " + std::to_string(kBookMins) + " 分钟; 收窄窗口";
    return false;
  }
  if (const int st = c.num("step"); st > 0 && (n + st - 1) / st > kBookRows) {
    err = "step: 窗口 " + std::to_string(n) + " 秒按每行 " + std::to_string(st) + " 秒有 " + std::to_string((n + st - 1) / st) + " 行, 至多 " + std::to_string(kBookRows) +
          " 行; 加大 step 或给 0 自动";
    return false;
  }
  out.from = from, out.to = to, out.at = to - 1;
  if (const std::string a = c.str("at"); !a.empty()) {
    out.at = clock_of(a);
    if (out.at < from || out.at >= to || !in_session(out.at)) {
      err = "at: 写成窗口内交易时段里的 HH:MM:SS";
      return false;
    }
  }
  out.big = c.num("big") * 1e4;
  return true;
}

bool RenderBook(const Call &c, const Book &b, std::string &out) {
  const BookSpec &sp = b.spec;
  std::string s = "## " + sp.code + " · " + date_of(sp.date) + " · " + hms(sp.from) + "–" + hms(sp.to) + " 逐笔重放";
  if (!b.error.empty()) {
    out = s + "\n没重放成: " + b.error;
    return false;
  }
  const int n = static_cast<int>(b.secs.size()), st = step_of(c.num("step"), n), R = (n + st - 1) / st;
  assert(n > 0 && R <= kBookRows);
  s += " (额 万元; 价差 / 距盘口按 0.01 元一跳)\n";
  s += "当日: 逐笔 " + std::to_string(b.orders) + " 条 · " +
       (b.close > 0 ? "开 " + px2(b.open) + " 高 " + px2(b.high) + " 低 " + px2(b.low) + " 收 " + px2(b.close) + " · 成交额 " + w4(b.amount) : std::string("无成交")) + " · 大单门槛 " +
       w4(b.big) + (c.num("big") > 0 ? " (给定)" : " (当日新挂单额 99 分位)") + "\n";

  // 窗口汇总 + 逐秒序列 (成交价无成交沿用上一笔)
  double tk[2] = {}, mk[2] = {}, cx[2] = {}, spread = 0.0, cum = 0.0;
  int nsp = 0;
  float first = 0.f, carry = 0.f;
  std::vector<double> lastp(static_cast<size_t>(n), kNaN), net(static_cast<size_t>(n));
  for (int i = 0; i < n; ++i) {
    const BookSec &x = b.secs[static_cast<size_t>(i)];
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

  // 逐时表: 行 = [起, 起 + step) 秒; 流量为行内合计, 盘口 / 竞价为行末那秒末
  const bool call = sp.from < kOpenCall || sp.to > kCloseCall;
  s += "\n逐时表 (每行 " + std::to_string(st) + " 秒; 主买 / 主卖 = 主动成交额, 挂 / 撤 = 新挂 / 撤单额, 五档 = 前 5 价位挂单额" +
       (call ? std::string("; 竞价段: 参考价 / 匹配额 / 未匹配额 (+ 买剩 − 卖剩)") : std::string()) + ")\n| 起 | 末价 | 买一 | 卖一 | 价差 | 主买 | 主卖 | 挂买 | 挂卖 | 撤买 | 撤卖 | 买五档 | 卖五档 |" +
       (call ? " 参考价 | 匹配 | 未匹配 |" : "") + "\n|---|---|---|---|---|---|---|---|---|---|---|---|---|" + (call ? "---|---|---|" : "") + "\n";
  for (int r = 0; r < R; ++r) {
    const int i0 = r * st, i1 = std::min(n, i0 + st);
    double t2[2] = {}, m2[2] = {}, c2[2] = {};
    for (int i = i0; i < i1; ++i)
      for (int e = 0; e < 2; ++e) {
        const BookSec &x = b.secs[static_cast<size_t>(i)];
        t2[e] += x.taker[e], m2[e] += x.maker[e], c2[e] += x.cancel[e];
      }
    const BookSec &z = b.secs[static_cast<size_t>(i1 - 1)];
    s += "| " + hms(b.secs[static_cast<size_t>(i0)].clock) + " | " + px2(lastp[static_cast<size_t>(i1 - 1)]) + " | " + px2(z.bid) + " | " + px2(z.ask) + " | " +
         (z.bid > 0.f && z.ask > 0.f ? fmt("%.0f", std::round((z.ask - z.bid) * 100.0)) : std::string("—")) + " | " + w4(t2[0]) + " | " + w4(t2[1]) + " | " + w4(m2[0]) + " | " + w4(m2[1]) +
         " | " + w4(c2[0]) + " | " + w4(c2[1]) + " | " + w4(z.depth[0]) + " | " + w4(z.depth[1]) + " |" +
         (call ? " " + px2(z.ref) + " | " + w4(z.matched) + " | " + w4(z.imbalance) + " |" : std::string()) + "\n";
  }

  // 大单事件: 计数 (分侧) + 额最大的 n 起按时序
  int cnt[4][2] = {};
  double amt[4][2] = {}, lv[2] = {};
  std::vector<double> life[2];
  for (const BookEvent &e : b.events) {
    const size_t k = static_cast<size_t>(e.kind);
    ++cnt[k][e.side], amt[k][e.side] += e.amount;
    if (e.kind == BookKind::Sweep)
      lv[e.side] += e.levels;
    if (e.kind == BookKind::Cancel && e.life >= 0.f)
      life[e.side].push_back(e.life);
  }
  s += "\n大单事件 (窗口内共 " + str(b.events.size()) + " 起; 侧 = 委托所在侧, 扫单 = 主动方; 寿命 = 挂出到撤 / 吃完的交易秒):\n";
  for (size_t k = 0; k < std::size(kKindCn); ++k) {
    s += "- " + std::string(kKindCn[k]) + ": 买 " + std::to_string(cnt[k][0]) + " 起 " + w4(amt[k][0]) + " / 卖 " + std::to_string(cnt[k][1]) + " 起 " + w4(amt[k][1]);
    if (static_cast<BookKind>(k) == BookKind::Sweep && cnt[k][0] + cnt[k][1] > 0)
      s += " · 平均吃 买 " + (cnt[k][0] ? fmt("%.1f", lv[0] / cnt[k][0]) : std::string("—")) + " / 卖 " + (cnt[k][1] ? fmt("%.1f", lv[1] / cnt[k][1]) : std::string("—")) + " 价位";
    if (static_cast<BookKind>(k) == BookKind::Cancel && (!life[0].empty() || !life[1].empty()))
      s += " · 寿命中位 买 " + (life[0].empty() ? std::string("—") : fmt("%.1f", median_of(life[0]))) + " / 卖 " + (life[1].empty() ? std::string("—") : fmt("%.1f", median_of(life[1]))) + " 秒";
    s += "\n";
  }
  std::vector<size_t> ix(b.events.size());
  std::iota(ix.begin(), ix.end(), size_t{0});
  const size_t m = std::min(ix.size(), static_cast<size_t>(c.num("n")));
  std::partial_sort(ix.begin(), ix.begin() + static_cast<std::ptrdiff_t>(m), ix.end(), [&](size_t x, size_t y) { return b.events[x].amount > b.events[y].amount; });
  ix.resize(m);
  std::sort(ix.begin(), ix.end()); // 事件本按时序
  if (m > 0) {
    s += "\n额最大的 " + str(m) + " 起 (按时序; 距盘口 = 与上一秒末同侧最优价差几跳: 0 在盘口, + 在盘口后方, − 越过盘口):\n| 时刻 | 事件 | 侧 | 价 | 距盘口 | 额 | 寿命 秒 |\n|---|---|---|---|---|---|---|\n";
    const int t0 = TradeSec(sp.from);
    for (const size_t i : ix) {
      const BookEvent &e = b.events[i];
      const BookSec &z = b.secs[static_cast<size_t>(std::clamp(TradeSec(e.clock) - t0 - 1, 0, n - 1))];
      const float best = e.side == 0 ? z.bid : z.ask;
      const bool sweep = e.kind == BookKind::Sweep;
      const std::string dist = sweep        ? "吃 " + std::to_string(e.levels) + " 价位"
                               : best > 0.f ? fmt("%+.0f", std::round((e.side == 0 ? best - e.price : e.price - best) * 100.0))
                                            : std::string("—");
      s += "| " + hms(e.clock) + fmt(".%02.0f", e.cs) + " | " + kKindCn[static_cast<size_t>(e.kind)] + " | " + (e.side == 0 ? "买" : "卖") + " | " +
           (sweep ? px2(e.first) + "→" + px2(e.price) : px2(e.price)) + " | " + dist + " | " + w4(e.amount) + " | " + (e.life >= 0.f ? fmt("%.1f", e.life) : std::string("—")) + " |\n";
    }
  }

  // 全簿剖面: 盘口两侧各至多 kProfile 个价位 (卖高 → 卖一 → 买一 → 买低)
  std::vector<std::pair<float, float>> bids, asks; // (价, 额) 由盘口往外
  double tb = 0.0, ta = 0.0;
  for (const auto &[p, a] : b.book)
    if (a > 0.f)
      bids.emplace_back(p, a), tb += a;
    else
      asks.emplace_back(p, -a), ta -= a;
  std::reverse(bids.begin(), bids.end());
  bids.resize(std::min(bids.size(), kProfile)), asks.resize(std::min(asks.size(), kProfile));
  s += "\n全簿剖面 @ " + hms(sp.at) + " 秒末 (全簿 买 " + w4(tb) + " / 卖 " + w4(ta) + "; 列盘口两侧各至多 " + str(kProfile) + " 价位):\n";
  if (bids.empty() && asks.empty()) {
    out = s + "簿空\n";
    return true;
  }
  float mx = 0.f;
  for (const auto &[p, a] : bids)
    mx = std::max(mx, a);
  for (const auto &[p, a] : asks)
    mx = std::max(mx, a);
  s += "| 档 | 价 | 额 | |\n|---|---|---|---|\n";
  const auto level = [&](const char *side, size_t i, const std::pair<float, float> &x) {
    std::string bar;
    for (long k = std::lround(x.second / mx * kBarCells); k > 0; --k)
      bar += "█";
    s += "| " + std::string(side) + str(i + 1) + " | " + px2(x.first) + " | " + w4(x.second) + " | " + bar + " |\n";
  };
  for (size_t i = asks.size(); i-- > 0;)
    level("卖", i, asks[i]);
  for (size_t i = 0; i < bids.size(); ++i)
    level("买", i, bids[i]);
  out = std::move(s);
  return true;
}

} // namespace agent
