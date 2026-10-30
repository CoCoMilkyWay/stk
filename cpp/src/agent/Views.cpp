// Views — 见头文件
#include "agent/Views.hpp"
#include "agent/Config.hpp"
#include "agent/Text.hpp"

#include <algorithm>
#include <array>
#include <cassert>
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

// 火花线: 有限值线性映到 ▁..█, 无效 ·
std::string bars(const std::vector<double> &v) {
  static constexpr const char *kBar[8] = {"▁", "▂", "▃", "▄", "▅", "▆", "▇", "█"};
  double lo = INFINITY, hi = -INFINITY;
  for (const double x : v)
    if (std::isfinite(x))
      lo = std::min(lo, x), hi = std::max(hi, x);
  std::string s;
  for (const double x : v)
    s += !std::isfinite(x) ? "·" : kBar[hi > lo ? std::min(7, static_cast<int>((x - lo) / (hi - lo) * 8.0)) : 3];
  return s;
}
// 火花线 + [最小, 最大]
std::string spark(const std::vector<double> &v, std::string (*f)(double) = num) {
  double lo = INFINITY, hi = -INFINITY;
  for (const double x : v)
    if (std::isfinite(x))
      lo = std::min(lo, x), hi = std::max(hi, x);
  return "`" + bars(v) + "`" + (lo <= hi ? "  [" + f(lo) + ", " + f(hi) + "]" : "");
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

// 日内时段: 首行单列, 其余每段约 30 分钟 (8 段); 日层只一段
struct Part {
  int k0, k1; // 日内行 [k0, k1)
  std::string name;
};
std::vector<Part> parts_of(const Look &L) {
  if (L.seg == 1)
    return {{0, 1, "收盘"}};
  const int w = std::max(1, (L.seg - 1) / 8);
  std::vector<Part> p{{0, 1, L.clock[0]}};
  for (int k = 1; k < L.seg; k += w) {
    const int k1 = L.seg - (k + w) < w ? L.seg : k + w;
    p.push_back({k, k1, L.clock[static_cast<size_t>(k - 1)] + "–" + L.clock[static_cast<size_t>(k1 - 1)]});
    if (k1 == L.seg)
      break;
  }
  return p;
}
std::vector<int> part_of_row(const std::vector<Part> &ps, int seg) {
  std::vector<int> o(static_cast<size_t>(seg));
  for (size_t i = 0; i < ps.size(); ++i)
    for (int k = ps[i].k0; k < ps[i].k1; ++k)
      o[static_cast<size_t>(k)] = static_cast<int>(i);
  return o;
}

std::string head(const Look &L, const Context &ctx) {
  return "评估期 " + date_of(L.dates.front()) + " ~ " + date_of(L.dates.back()) + " (" + str(L.dates.size()) + " 天, 每天 " + std::to_string(L.seg) +
         " 行, 行末时刻 " + L.clock.front() + (L.seg > 1 ? " … " + L.clock.back() : std::string()) + ") · 持有期 " + ctx.hold + " · 超额 = 毛 − 本行在池均值\n\n";
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
  // 当日涨跌的基准: 分钟层 = 当日昨收 (除权); 日层 = 上一行收盘
  double pre(int t, int a) const {
    const double p = L.seg > 1 ? (*this)(Bar::PreClose, t, a) : t > 0 ? (*this)(Bar::Close, t - 1, a)
                                                                      : kNaN;
    return p > 0.0 ? p : kNaN;
  }
};

// ---- 交割单的标签 (开仓那行的盘面) ----
namespace tag {
enum : uint8_t { First,
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
constexpr const char *kTagNames[tag::N] = {"首行",
                                           "尾盘最后一段",
                                           "近涨停 (收 ≥ 涨停价 × 0.995)",
                                           "近跌停 (收 ≤ 跌停价 × 1.005)",
                                           "当日涨 > 5%",
                                           "当日跌 > 5%",
                                           "日内高位 (当日高低区间 ≥ 0.8)",
                                           "日内低位 (≤ 0.2)",
                                           "放量 (≥ 当日此前均量 3 倍)"};
struct Mark {
  uint32_t m = 0;
  double chg = kNaN, pos = kNaN; // 当日涨跌 / 收在当日至此高低区间的位置
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
  if (k == 0)
    r.m |= 1u << tag::First;
  if (k >= seg - std::max(1, (seg - 1) / 8))
    r.m |= 1u << tag::Last;
  if (c >= px(Bar::LimUp, t, a) * 0.995)
    r.m |= 1u << tag::NearUp;
  if (c <= px(Bar::LimDn, t, a) * 1.005)
    r.m |= 1u << tag::NearDn;
  double hi = -INFINITY, lo = INFINITY, vs = 0.0;
  int vn = 0;
  for (int u = t0; u <= t; ++u) {
    hi = std::fmax(hi, px(Bar::High, u, a)), lo = std::fmin(lo, px(Bar::Low, u, a));
    if (const double v = px(Bar::Volume, u, a); u < t && std::isfinite(v))
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
  static constexpr const char *kShort[tag::N] = {"首行", "尾盘", "近涨停", "近跌停", "大涨", "大跌", "高位", "低位", "放量"};
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
  Mean ric;
  void add(const LookRow &w) {
    ++rows;
    if (!w.ok)
      return;
    ++ok;
    for (int k = 0; k < kGroups; ++k)
      g[k] += w.grp[k], cnt[k] += w.cnt[k];
    ric.add(w.ric);
  }
  double grp(int k) const { return rows ? g[k] / rows : kNaN; }
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
  const std::vector<Part> ps = parts_of(L);
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
      for (const Part &p : ps)
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

bool layers_view(const Call &c, const Look &L, std::string &out) {
  const LookRoot *r = root_of(L, out);
  if (!r)
    return false;
  const int T = L.T(), seg = L.seg;
  Layer all;
  for (const LookRow &w : r->rows)
    all.add(w);
  std::string s = "## 因子 `" + c.str("factor") + "` (组号已定向: G20 = 做多端)\n";
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

  const std::string split = c.str("split");
  if (split == "none") {
    out = std::move(s);
    return true;
  }
  if (split == "tod" && seg == 1) {
    out = s + "\n(日层每天一行, 没有日内时段可切)\n";
    return true;
  }
  std::vector<std::pair<std::string, Layer>> bs;
  if (split == "tod") {
    const std::vector<Part> ps = parts_of(L);
    const std::vector<int> pr = part_of_row(ps, seg);
    for (const Part &p : ps)
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
  std::vector<LookCell> cs;
  for (const LookCell &x : r->cells)
    if (x.top == top)
      cs.push_back(x);
  const std::string side = top ? "做多端 G20" : "最低组 G1";
  std::string s = "## 因子 `" + c.str("factor") + "` · " + side + " · 合并 " + merge + "\n";
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
  s += "真持有 " + str(cs.size()) + " 格 (每行平均 " + fmt("%.1f", static_cast<double>(cs.size()) / T) + " 只) → " + str(M) + " 段, 段均 " +
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
    const std::vector<Part> ps = parts_of(L);
    const std::vector<int> pr = part_of_row(ps, seg);
    std::vector<Mean> by(ps.size());
    for (size_t i = 0; i < M; ++i)
      by[static_cast<size_t>(pr[ts[i].t0 % seg])].add(ex[i]);
    s += "\n| 开仓时段 | 段数 | 占比 | 段超额均值 bp | 胜率 |\n|---|---|---|---|---|\n";
    for (size_t j = 0; j < ps.size(); ++j)
      if (by[j].n)
        s += "| " + ps[j].name + " | " + str(by[j].n) + " | " + pct(static_cast<double>(by[j].n) / M) + " | " + bp(by[j].mean()) + " | " + pct(by[j].up()) + " |\n";
  }

  // 标签: 本端段 vs 池内格 (随机抽样, 固定种子)
  std::array<size_t, tag::N> base{};
  size_t nb = 0;
  {
    std::mt19937_64 rng(7);
    std::uniform_int_distribution<size_t> pick(0, static_cast<size_t>(T) * A - 1);
    for (size_t tries = 0; nb < kBase && tries < kBase * 20; ++tries) {
      const size_t k = pick(rng);
      if (!L.gate[k] || !std::isfinite(L.label[k]))
        continue;
      const Mark m = mark_of(px, static_cast<int>(k / A), static_cast<int>(k % A));
      ++nb;
      for (int j = 0; j < tag::N; ++j)
        base[static_cast<size_t>(j)] += m.m >> j & 1u;
    }
  }
  s += "\n| 开仓那行的盘面 | 本端段占比 | 池内格占比 | 提升 | 带此标签的段超额 bp | 不带的 bp |\n|---|---|---|---|---|---|\n";
  for (int j = 0; j < tag::N; ++j) {
    Mean with, without;
    for (size_t i = 0; i < M; ++i)
      (mk[i].m >> j & 1u ? with : without).add(ex[i]);
    const double fs = static_cast<double>(with.n) / M, fb = nb ? static_cast<double>(base[static_cast<size_t>(j)]) / nb : kNaN;
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
  s += "\n明细 (" + sort + "):\n| # | 代码 | 日期 | 时刻 | 行 | 开仓分位 | 段超额 bp | 当日涨跌 | 日内位置 | 盘面 |\n|---|---|---|---|---|---|---|---|---|---|\n";
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
  std::string s = "## 因子 `" + c.str("factor") + "` · " + side + " 的常客\n";
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
  double hi = -INFINITY, lo = INFINITY, vsum = 0.0, asum = 0.0, last = kNaN;
  int in = 0;
  for (int k = 0; k < seg; ++k) {
    hi = std::fmax(hi, high[k]), lo = std::fmin(lo, low[k]);
    vsum += std::isfinite(vol[k]) ? vol[k] : 0.0, asum += std::isfinite(amt[k]) ? amt[k] : 0.0;
    last = std::isfinite(close[k]) ? close[k] : last;
    in += L.gate[static_cast<size_t>(t0 + k)];
  }
  std::string s = "## " + L.codes[0] + " · " + date_of(L.dates[static_cast<size_t>(d)]) + " · " + ctx.level + " (每天 " + std::to_string(seg) + " 行)\n";
  s += "昨收 " + num(pre) + " · 首行收 " + num(close[0]) + " (" + chg(close[0] / pre - 1.0) + ") · 高 " + num(hi) + " (" + chg(hi / pre - 1.0) + ") · 低 " + num(lo) + " (" +
       chg(lo / pre - 1.0) + ") · 收 " + num(last) + " (" + chg(last / pre - 1.0) + ") · 涨停 " + num(px(Bar::LimUp, t0, 0)) + " / 跌停 " + num(px(Bar::LimDn, t0, 0)) +
       " · 成交额 " + fmt("%.0f", asum / 1e4) + " 万 · 在池 " + std::to_string(in) + " / " + std::to_string(seg) + " 行\n";
  s += over_head(o) + "\n";
  s += "- 收盘价 " + spark(close) + "\n- 成交量 " + spark(vol) + "\n";
  if (o.f)
    s += "- 因子分位 (高 = 做多端) " + spark(col([&](int t) { return static_cast<double>(o.f->rank(static_cast<size_t>(t))); }), rk) + "\n";
  for (size_t i = 0; i < o.fs.size(); ++i)
    s += "- E" + str(i + 1) + " " + spark(col([&](int t) { return feat_at(o.fs[i], static_cast<size_t>(t)); })) + "\n";

  s += "\n| 时段 | 涨跌 | 段内高 | 段内低 | 量占比 |" + std::string(o.f ? " 因子分位 | 多/空行 |" : "") + " 后续超额 bp |";
  for (size_t i = 0; i < o.xs.size(); ++i)
    s += " E" + str(i + 1) + " 段末 |";
  s += "\n|---|---|---|---|---|" + std::string(o.f ? "---|---|" : "") + "---|";
  for (size_t i = 0; i < o.xs.size(); ++i)
    s += "---|";
  s += "\n";
  for (const Part &p : parts_of(L)) {
    double h = -INFINITY, l = INFINITY, v = 0.0;
    std::vector<double> rks, exs;
    int up = 0, dn = 0;
    for (int k = p.k0; k < p.k1; ++k) {
      const size_t t = static_cast<size_t>(t0 + k);
      h = std::fmax(h, high[k]), l = std::fmin(l, low[k]), v += std::isfinite(vol[k]) ? vol[k] : 0.0;
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
    double hi = -INFINITY, lo = INFINITY, asum = 0.0, last = kNaN;
    int in = 0, up = 0, dn = 0;
    std::vector<double> rks;
    for (int t = t0; t <= tl; ++t) {
      hi = std::fmax(hi, px(Bar::High, t, 0)), lo = std::fmin(lo, px(Bar::Low, t, 0));
      if (const double a = px(Bar::Amount, t, 0); std::isfinite(a))
        asum += a;
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
    rows += "| " + date_of(L.dates[static_cast<size_t>(d)]) + " | " + chg(g) + " | " + pct(hi > lo ? (hi - lo) / pre : kNaN) + " | " + fmt("%.0f", asum / 1e4) + " | " +
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
    s += "- 因子日均分位 (高 = 做多端) " + spark(sf, rk) + "\n";
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
  const auto feats = [&](const std::vector<std::string> &xs) {
    for (size_t i = 0; i < xs.size(); ++i) {
      std::string r;
      if (!lab.feature_of(xs[i], r, err)) {
        err = "exprs[" + str(i) + "]: " + err;
        return false;
      }
      out.feats.push_back(std::move(r));
    }
    return true;
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
  case Tool::ViewHolders:
    return factor(c.str("factor"));
  case Tool::ViewTrades:
    out.kline = true;
    return factor(c.str("factor"));
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
    if (const std::string f = c.str("factor"); !f.empty() && !factor(f))
      return false;
    out.kline = true;
    return feats(c.strs("exprs"));
  default:
    assert(false && "不是观察工具");
    return false;
  }
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
  case Tool::ViewStockDay:
    ok = stock_day(c, L, ctx, body);
    break;
  case Tool::ViewStockDays:
    ok = stock_days(c, L, ctx, body);
    break;
  default:
    assert(false && "不是观察工具");
  }
  out = ok ? head(L, ctx) + body : body;
  return ok;
}

} // namespace agent
