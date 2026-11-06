// cta_bench — 只做多 CTA 测试台: 模型 × 参数网格 × 测试集 (篮子表的 set), 逐资产回测 → 等权组合 + 跨资产中位数
//   用法: cta_bench [--freq 1d|1m] [--sets idx,ind,concept] [--models ma,tsmom,…] [--cost 0.001] [--vol 0] [--buffer 0]
//                   [--stop close|intra] [--top 15] [--expo 0.2] [--roll 0] [--refit 20] [--trace 4]
//   先把 CTA 库 (cta/data, Db.hpp) 合成到 all.bar 特征库末日, 再按 freq 取面板; 模型参数 / 方差比周期的单位都是根 (1m = 分钟), band 的 n 以日计
//   终端: 各测试集先给方差比 VR(k) (各资产中位数: 哪些周期有趋势 / 均值回归), 再按组合夏普排行 (+ 基准 hold),
//         末尾各模型网格汇总 (最优 / 平原顶 / 网格均值); cta/data/bench_<freq>.tsv = 全部行
//   平原: 参数网格上的夏普曲面按邻域 (数值轴下标差 ≤ 1, 类别轴相同) 取均值, 平原顶 = 邻域均值最高的点 (不挑孤立尖峰)
//   滚动调参 (--roll W 日 > 0): 每 refit 日, 用前 W 日各资产 (样本) 的夏普均值拟一张曲面, 取平原顶参数用到下一个 refit 日; 前 W 日空仓
//   轨迹 (--trace N > 0, 需含 band): 各测试集 band 平原顶, 随机 N 只资产各一窗 (1d 250 日 / 1m 5 日) → cta/data/trace_<freq>.tsv (plot.py 画)
//   排行与最优只取平均仓位中位数 ≥ expo 的: 几乎不开仓的配置夏普没有可比性
//   列: SR / CAGR / MDD = 测试集等权组合 (逐根等权各资产净收益, 空仓部分计现金), SR1 / SR2 = 组合前 / 后半段夏普;
//       mSR = 各资产夏普中位数, mΔSR = 各资产 (夏普 − 自身买入持有夏普) 中位数, beat = 夏普胜过买入持有的资产占比;
//       Up / Dn / Edge / Expo / Turn / Tr/y / Win / Pay / Hold = 各资产中位数 (Metrics.hpp)
#include "Metrics.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdio>
#include <functional>
#include <map>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {

using P = std::vector<float>;

struct Axis {
  const char *name;
  std::vector<float> v;
  std::vector<const char *> label = {}; // 非空 = 类别轴 (取值 = 下标, 平原不跨类别平滑)
};

// ok / make 的第二参 = 每日根数
struct Kind {
  const char *name;
  std::vector<Axis> grid;
  bool (*ok)(const P &, double); // 组合约束 (nullptr = 全要)
  std::unique_ptr<Model> (*make)(const P &, double);
  bool trail; // 另叠 Trail 外挂 (k = kTrail) 各跑一份
};

constexpr float kTrail = 3;
constexpr size_t kMinDays = 500;                                                                                                  // 资产有效根数下限 = 这么多日的根
const std::vector<size_t> kVrLags[] = {{2, 3, 5, 10, 20, 40, 60, 120, 250}, {2, 5, 15, 30, 60, 120, 240, 480, 1200, 2400, 4800}}; // 1d / 1m

// clang-format off
const Kind kKinds[] = {
    {"hold", {}, nullptr, [](const P &, double) -> std::unique_ptr<Model> { return std::make_unique<Hold>(); }, false},
    {"ma", {{"fast", {5, 10, 20}}, {"slow", {20, 40, 60, 120, 250}}}, [](const P &p, double) { return 2 * p[0] <= p[1]; },
     [](const P &p, double) -> std::unique_ptr<Model> { return std::make_unique<Ma>(p[0], p[1]); }, true},
    {"tsmom", {{"L", {10, 20, 40, 60, 120, 250}}}, nullptr,
     [](const P &p, double) -> std::unique_ptr<Model> { return std::make_unique<TsMom>(p[0]); }, true},
    {"ens", {{"lo", {5, 10, 20}}}, nullptr,
     [](const P &p, double) -> std::unique_ptr<Model> { return std::make_unique<Ens>(p[0]); }, true},
    {"kama", {{"n", {10, 20}}, {"slow", {30, 60}}}, nullptr,
     [](const P &p, double) -> std::unique_ptr<Model> { return std::make_unique<Kama>(p[0], p[1]); }, true},
    {"donchian", {{"n", {20, 40, 60, 120}}, {"m", {10, 20, 40}}}, [](const P &p, double) { return p[1] < p[0]; },
     [](const P &p, double) -> std::unique_ptr<Model> { return std::make_unique<Donchian>(p[0], p[1]); }, false},
    {"chandelier", {{"n", {10, 20, 60}}, {"k", {2, 3, 4, 5}}}, nullptr,
     [](const P &p, double) -> std::unique_ptr<Model> { return std::make_unique<Chandelier>(p[0], p[1]); }, false},
    {"rebound", {{"d", {0.5, 1, 1.5}}, {"n", {5, 10, 20}}, {"k", {2, 3, 4}}}, nullptr,
     [](const P &p, double) -> std::unique_ptr<Model> { return std::make_unique<Rebound>(p[0], p[1], p[2]); }, false},
    {"revert", {{"L", {60, 120, 250}}, {"g", {0.5, 1, 1.5}}}, nullptr,
     [](const P &p, double) -> std::unique_ptr<Model> { return std::make_unique<Revert>(p[0], p[1]); }, false},
    {"gate", {{"L", {60, 120, 250}}, {"g", {-1, -0.5, 0}}, {"f", {5, 10, 20}}, {"h", {0, 0.25, 0.5}}}, nullptr,
     [](const P &p, double) -> std::unique_ptr<Model> { return std::make_unique<Gate>(p[0], p[1], p[2], p[3]); }, false},
    {"band", {{"c", {0, 1, 2}, {kCenters[0], kCenters[1], kCenters[2]}}, {"w", {0, 1, 2}, {kWidths[0], kWidths[1], kWidths[2]}},
              {"n", {0.5, 1, 2, 5, 10, 20, 60, 120}}, {"k", {0.25, 0.5, 1, 2}}, {"N", {1, 4}}, {"h", {0, 0.5}}},
     [](const P &p, double S) { return p[2] * S >= 5; },
     [](const P &p, double S) -> std::unique_ptr<Model> {
       return std::make_unique<Band>(static_cast<int>(p[0]), static_cast<int>(p[1]), std::lround(p[2] * S), p[3], static_cast<int>(p[4]), p[5]);
     }, false},
};
// clang-format on

struct Spec {
  size_t kind;
  P p;
  std::vector<size_t> at; // 各轴取值下标
  float trail = 0;
};

std::string name_of(const Spec &s) { return std::string(kKinds[s.kind].name) + (s.trail > 0 ? "+trail" : ""); }

std::string params_of(const Spec &s) {
  std::string o;
  char buf[32];
  for (size_t i = 0; i < s.p.size(); ++i) {
    const Axis &a = kKinds[s.kind].grid[i];
    if (a.label.empty())
      std::snprintf(buf, sizeof(buf), "%s%s=%g", i ? "," : "", a.name, s.p[i]);
    else
      std::snprintf(buf, sizeof(buf), "%s%s=%s", i ? "," : "", a.name, a.label[s.at[i]]);
    o += buf;
  }
  if (s.trail > 0) {
    std::snprintf(buf, sizeof(buf), "%sk=%g", o.empty() ? "" : ",", s.trail);
    o += buf;
  }
  return o.empty() ? "-" : o;
}

std::unique_ptr<Model> make(const Spec &s, double S) {
  std::unique_ptr<Model> m = kKinds[s.kind].make(s.p, S);
  return s.trail > 0 ? std::make_unique<Trail>(std::move(m), s.trail) : std::move(m);
}

// 同模型且网格相邻: trail / 类别轴相同, 数值轴下标差 ≤ 1 (含自身)
bool near(const Spec &a, const Spec &b) {
  if (a.kind != b.kind || a.trail != b.trail)
    return false;
  for (size_t i = 0; i < a.at.size(); ++i) {
    const size_t d = a.at[i] > b.at[i] ? a.at[i] - b.at[i] : b.at[i] - a.at[i];
    if (d > (kKinds[a.kind].grid[i].label.empty() ? 1u : 0u))
      return false;
  }
  return true;
}

// ids 内各点的邻居 (ids 内下标)
std::vector<std::vector<size_t>> near_of(const std::vector<Spec> &specs, const std::vector<size_t> &ids) {
  std::vector<std::vector<size_t>> out(ids.size());
  for (size_t i = 0; i < ids.size(); ++i)
    for (size_t j = 0; j < ids.size(); ++j)
      if (near(specs[ids[i]], specs[ids[j]]))
        out[i].push_back(j);
  return out;
}

// 曲面邻域均值 (跳过 NaN)
std::vector<double> smooth(const std::vector<std::vector<size_t>> &nb, const std::vector<double> &v) {
  std::vector<double> out(v.size(), kNaN);
  for (size_t i = 0; i < v.size(); ++i) {
    double s = 0, n = 0;
    for (const size_t j : nb[i])
      if (!std::isnan(v[j]))
        s += v[j], n += 1;
    if (n > 0)
      out[i] = s / n;
  }
  return out;
}

std::vector<Spec> specs_of(const std::vector<std::string> &models, double S) {
  std::vector<Spec> out;
  for (size_t k = 0; k < std::size(kKinds); ++k) {
    const Kind &K = kKinds[k];
    if (k > 0 && !models.empty() && std::find(models.begin(), models.end(), K.name) == models.end())
      continue;
    std::vector<size_t> at(K.grid.size(), 0); // 网格里程表
    for (bool more = true; more;) {
      P p;
      for (size_t i = 0; i < at.size(); ++i)
        p.push_back(K.grid[i].v[at[i]]);
      if (!K.ok || K.ok(p, S)) {
        out.push_back({k, p, at, 0});
        if (K.trail)
          out.push_back({k, p, at, kTrail});
      }
      more = false;
      for (size_t i = 0; i < at.size() && !more; ++i)
        if (++at[i] < K.grid[i].v.size())
          more = true;
        else
          at[i] = 0;
    }
  }
  return out;
}

struct Universe {
  std::string name;
  std::vector<Series> series = {};
  std::vector<double> hold_sr = {}; // [资产] 买入持有夏普
  std::vector<std::string> names = {};
  size_t lo = 0, hi = 0, mid = 0; // 张量时间段 [lo, hi], 前后半段分界
  size_t skipped = 0;             // 有效根数不足 kMinBars 的
  // 滚动调参的块 (refit 日一块, 自 lo 所在日起): 块数 + [资产][块] 根数前缀和
  size_t S = 1, R = 0, nb = 0;
  std::vector<std::vector<double>> cnt = {};
  size_t block_of(size_t t) const { return (t / S - lo / S) / R; }
};

struct Row {
  std::string set, model, params;
  Stat port; // 等权组合
  double sr1 = 0, sr2 = 0;
  double m_sr = 0, m_dsr = 0, beat = 0, m_mdd = 0;
  Stat med = {};               // 各资产中位数 (up dn edge expo turn trades win payoff hold)
  std::vector<float> blk = {}; // 滚动调参: [资产][块][Σr, Σr²] 前缀和 (空 = 未留)
};

using Factory = std::function<std::unique_ptr<Model>(size_t)>; // 资产下标 → 模型

Row evaluate(const Universe &u, const Factory &make, const Setup &setup, size_t T, bool keep) {
  std::vector<double> sum(T, 0), cnt(T, 0);
  std::vector<Stat> st;
  Row row;
  if (keep)
    row.blk.assign(u.series.size() * u.nb * 2, 0);
  for (size_t a = 0; a < u.series.size(); ++a) {
    const Series &se = u.series[a];
    const std::unique_ptr<Model> m = make(a);
    const Run run = simulate(se, *m, setup);
    st.push_back(stat_of(se, run, setup));
    for (size_t i = 0; i < se.t.size(); ++i)
      sum[se.t[i]] += run.r[i], cnt[se.t[i]] += 1;
    if (!keep)
      continue;
    std::vector<double> acc(u.nb * 2, 0);
    for (size_t i = 0; i < se.t.size(); ++i) {
      const size_t b = u.block_of(se.t[i]);
      acc[2 * b] += run.r[i], acc[2 * b + 1] += run.r[i] * run.r[i];
    }
    for (size_t k = 2; k < acc.size(); ++k)
      acc[k] += acc[k - 2];
    std::copy(acc.begin(), acc.end(), row.blk.begin() + static_cast<long>(a * u.nb * 2));
  }
  std::vector<double> r;
  for (size_t t = u.lo; t <= u.hi; ++t)
    r.push_back(cnt[t] > 0 ? sum[t] / cnt[t] : 0);
  row.set = u.name, row.port = returns_stat(r, setup.bpy);
  const size_t h = u.mid - u.lo;
  row.sr1 = returns_stat(std::span(r).first(h), setup.bpy).sharpe;
  row.sr2 = returns_stat(std::span(r).subspan(h), setup.bpy).sharpe;
  const auto med = [&](auto f) {
    std::vector<double> v;
    for (size_t k = 0; k < st.size(); ++k)
      v.push_back(f(k));
    return median(std::move(v));
  };
  double beat = 0;
  for (size_t k = 0; k < st.size(); ++k)
    beat += st[k].sharpe > u.hold_sr[k];
  row.beat = beat / static_cast<double>(st.size());
  row.m_sr = med([&](size_t k) { return st[k].sharpe; });
  row.m_dsr = med([&](size_t k) { return st[k].sharpe - u.hold_sr[k]; });
  row.m_mdd = med([&](size_t k) { return st[k].mdd; });
  row.med.up = med([&](size_t k) { return st[k].up; });
  row.med.dn = med([&](size_t k) { return st[k].dn; });
  row.med.edge = med([&](size_t k) { return st[k].edge; });
  row.med.expo = med([&](size_t k) { return st[k].expo; });
  row.med.turn = med([&](size_t k) { return st[k].turn; });
  row.med.trades = med([&](size_t k) { return st[k].trades; });
  row.med.win = med([&](size_t k) { return st[k].win; });
  row.med.payoff = med([&](size_t k) { return st[k].payoff; });
  row.med.hold = med([&](size_t k) { return st[k].hold; });
  return row;
}

// 滚动调参: 块 b ≥ W 起, 以块 [b − W, b) 内各资产 (样本) 夏普的均值作曲面, 取平原顶 → pick[b] (rows 内下标; −1 = 空仓)
//   窗内根数不足满窗一半的资产不入样本
std::vector<int> roll_pick(const Universe &u, const std::vector<const Row *> &rows, const std::vector<std::vector<size_t>> &nb, size_t W) {
  std::vector<int> pick(u.nb, -1);
  const double min_n = static_cast<double>(W * u.R * u.S) / 2;
  std::vector<double> sc(rows.size());
  for (size_t b = W; b < u.nb; ++b) {
    const auto win = [&](const auto &p, size_t k) { return p[k + 2 * (b - 1)] - (b > W ? p[k + 2 * (b - W - 1)] : 0); };
    for (size_t s = 0; s < rows.size(); ++s) {
      double sum = 0, m = 0;
      for (size_t a = 0; a < u.series.size(); ++a) {
        const std::vector<double> &c = u.cnt[a];
        const double n = c[b - 1] - (b > W ? c[b - W - 1] : 0);
        if (n < min_n)
          continue;
        const float *p = rows[s]->blk.data() + a * u.nb * 2;
        const double mean = win(p, 0) / n, var = win(p, 1) / n - mean * mean;
        sum += var > 0 ? mean / std::sqrt(var) : 0, m += 1;
      }
      sc[s] = m > 0 ? sum / m : kNaN;
    }
    const std::vector<double> sm = smooth(nb, sc);
    for (size_t s = 0; s < sm.size(); ++s)
      if (!std::isnan(sm[s]) && (pick[b] < 0 || sm[s] > sm[static_cast<size_t>(pick[b])]))
        pick[b] = static_cast<int>(s);
  }
  return pick;
}

// 滚动调参的执行: 候选配置逐根同跑 (状态常热), 取当根选中者的单; 切换的换手照常计成本
//   未选中者 (或刚切入者) 以自身上根目标仓位当持仓
class Switch : public Model {
  std::vector<std::unique_ptr<Model>> m_;
  std::vector<float> last_;
  std::vector<int> pick_; // [根] → m_ 下标 (−1 = 空仓)
  size_t i_ = 0;
  int prev_ = -1;

public:
  Switch(std::vector<std::unique_ptr<Model>> m, std::vector<int> pick) : m_(std::move(m)), last_(m_.size(), 0), pick_(std::move(pick)) {}
  Order step(const Bar &b, float held) override {
    const int k = pick_[i_++];
    Order o;
    for (size_t j = 0; j < m_.size(); ++j) {
      const bool on = static_cast<int>(j) == k;
      const Order x = m_[j]->step(b, on && k == prev_ ? held : last_[j]);
      last_[j] = x.w;
      if (on)
        o = x;
    }
    prev_ = k;
    return o;
  }
};

// 一个 (测试集, 模型) 的滚动调参
struct Roll {
  size_t set, kind;
  std::vector<size_t> ids = {}; // 候选 spec
  std::vector<int> pick = {};   // [块] → ids 下标 (−1 = 空仓)
  Row row = {};
};

// 轨迹: 逐根记下 band 中线 / 半宽 (非 Band = NaN)
class Probe : public Model {
  Model &m_;
  const Band *b_;

public:
  std::vector<double> mid, half;
  explicit Probe(Model &m) : m_(m), b_(dynamic_cast<const Band *>(&m)) {}
  Order step(const Bar &b, float held) override {
    const Order o = m_.step(b, held);
    mid.push_back(b_ ? b_->mid() : kNaN), half.push_back(b_ ? b_->half() : kNaN);
    return o;
  }
};

constexpr size_t kTraceDays[] = {250, 5}; // 轨迹窗: 1d / 1m

std::string date_of(int64_t t) {
  const int d = ymd_of(t);
  char buf[16];
  std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d", d / 10000, d / 100 % 100, d % 100);
  return buf;
}

void print_head() {
  std::printf("  %-16s %-36s %5s %6s %6s %5s %5s | %5s %5s %4s | %4s %4s %5s %4s %5s %5s %4s %4s %5s\n", "model", "params", "SR", "CAGR", "MDD",
              "SR1", "SR2", "mSR", "mΔSR", "beat", "Up", "Dn", "Edge", "Expo", "Turn", "Tr/y", "Win", "Pay", "Hold");
}

void print_row(const Row &r) {
  std::printf("  %-16s %-36s %5.2f %5.1f%% %5.1f%% %5.2f %5.2f | %5.2f %5.2f %3.0f%% | %4.2f %4.2f %+5.2f %4.2f %5.1f %5.1f %3.0f%% %4.2f %5.0f\n",
              r.model.c_str(), r.params.c_str(), r.port.sharpe, 100 * r.port.cagr, 100 * r.port.mdd, r.sr1, r.sr2, r.m_sr, r.m_dsr, 100 * r.beat,
              r.med.up, r.med.dn, r.med.edge, r.med.expo, r.med.turn, r.med.trades, 100 * r.med.win, r.med.payoff, r.med.hold);
}

std::vector<std::string> split(const std::string &s) {
  std::vector<std::string> out;
  for (size_t a = 0; a <= s.size();) {
    const size_t b = std::min(s.find(',', a), s.size());
    if (b > a)
      out.push_back(s.substr(a, b - a));
    a = b + 1;
  }
  return out;
}

void parallel(size_t n, const std::function<void(size_t)> &f) {
  std::atomic<size_t> next{0};
  std::vector<std::jthread> ws;
  for (unsigned i = 0; i < std::max(1u, std::thread::hardware_concurrency()); ++i)
    ws.emplace_back([&] {
      for (size_t k; (k = next++) < n;)
        f(k);
    });
}

} // namespace

int main(int argc, char **argv) {
  std::map<std::string, std::string> arg = {{"freq", "1d"}, {"sets", "idx,ind,concept"}, {"models", ""}, {"cost", "0.001"}, {"vol", "0"}, {"buffer", "0"}, {"stop", "close"}, {"top", "15"}, {"expo", "0.2"}, {"roll", "0"}, {"refit", "20"}, {"trace", "4"}};
  assert(argc % 2 == 1 && "参数须成对: --键 值");
  for (int i = 1; i + 1 < argc; i += 2) {
    const std::string k = argv[i];
    assert(k.starts_with("--") && arg.contains(k.substr(2)) && "未知参数");
    arg[k.substr(2)] = argv[i + 1];
  }
  assert((arg["stop"] == "close" || arg["stop"] == "intra") && "--stop close | intra");

  const std::string root = std::string(CTA_DIR) + "/..", dir = std::string(CTA_DIR) + "/data";
  const Lib lib = open_lib(root + "/output/features/all/bar");
  const Table table = make_table(lib, root + "/output/concept");
  sync(lib, table, dir);
  const Panel panel = open_panel(dir, table, arg["freq"]);
  Setup setup;
  setup.cost = std::stod(arg["cost"]), setup.vol = std::stod(arg["vol"]), setup.buffer = std::stod(arg["buffer"]);
  setup.stop = arg["stop"] == "close" ? Stop::Close : Stop::Intra;
  setup.bpy = panel.bars_per_year;
  const double S = static_cast<double>(panel.S);
  const std::vector<Spec> specs = specs_of(split(arg["models"]), S);
  const size_t top = std::stoul(arg["top"]);
  const double expo = std::stod(arg["expo"]);
  const size_t W = std::stoul(arg["roll"]), R = std::stoul(arg["refit"]);
  assert(R > 0 && W % R == 0 && "--roll 须为 --refit 的整数倍");

  std::vector<Universe> us;
  for (const std::string &name : split(arg["sets"])) {
    Universe u{name};
    u.lo = panel.T, u.hi = 0, u.S = panel.S, u.R = R;
    for (size_t b = 0; b < panel.B; ++b) {
      if (panel.assets[b].set != name)
        continue;
      Series se = series_of(panel, b);
      if (se.bar.size() < kMinDays * panel.S) {
        ++u.skipped;
        continue;
      }
      Hold h;
      u.hold_sr.push_back(stat_of(se, simulate(se, h, setup), setup).sharpe);
      u.lo = std::min<size_t>(u.lo, se.t.front()), u.hi = std::max<size_t>(u.hi, se.t.back());
      u.series.push_back(std::move(se)), u.names.push_back(panel.assets[b].name);
    }
    assert(!u.series.empty() && "测试集为空");
    u.mid = (u.lo + u.hi + 1) / 2;
    if (W > 0) {
      u.nb = u.block_of(u.hi) + 1;
      assert(u.nb > W / R && "样本短于滚动窗");
      for (const Series &se : u.series) {
        std::vector<double> c(u.nb, 0);
        for (const uint32_t t : se.t)
          c[u.block_of(t)] += 1;
        for (size_t b = 1; b < u.nb; ++b)
          c[b] += c[b - 1];
        u.cnt.push_back(std::move(c));
      }
    }
    us.push_back(std::move(u));
  }

  std::vector<std::pair<size_t, size_t>> tasks; // (测试集, spec); 下标 = 测试集 · |specs| + spec
  for (size_t i = 0; i < us.size(); ++i)
    for (size_t j = 0; j < specs.size(); ++j)
      tasks.emplace_back(i, j);
  std::vector<Row> rows(tasks.size());
  parallel(tasks.size(), [&](size_t k) {
    const Spec &sp = specs[tasks[k].second];
    rows[k] = evaluate(us[tasks[k].first], [&](size_t) { return make(sp, S); }, setup, panel.T, W > 0 && sp.kind > 0);
    rows[k].model = name_of(sp), rows[k].params = params_of(sp);
  });

  std::vector<Roll> rolls;
  if (W > 0) {
    for (size_t i = 0; i < us.size(); ++i)
      for (size_t k = 1; k < std::size(kKinds); ++k) {
        Roll ro{i, k};
        std::vector<const Row *> rs;
        for (size_t j = 0; j < specs.size(); ++j)
          if (specs[j].kind == k)
            ro.ids.push_back(j), rs.push_back(&rows[i * specs.size() + j]);
        if (ro.ids.size() < 2)
          continue;
        ro.pick = roll_pick(us[i], rs, near_of(specs, ro.ids), W / R);
        rolls.push_back(std::move(ro));
      }
    for (Row &r : rows)
      r.blk = {};
    parallel(rolls.size(), [&](size_t k) {
      Roll &ro = rolls[k];
      const Universe &u = us[ro.set];
      std::vector<int> pos(ro.ids.size(), -1);
      std::vector<size_t> uses;
      for (const int p : ro.pick)
        if (p >= 0 && pos[static_cast<size_t>(p)] < 0)
          pos[static_cast<size_t>(p)] = static_cast<int>(uses.size()), uses.push_back(static_cast<size_t>(p));
      ro.row = evaluate(
          u,
          [&](size_t a) -> std::unique_ptr<Model> {
            std::vector<std::unique_ptr<Model>> ms;
            for (const size_t x : uses)
              ms.push_back(make(specs[ro.ids[x]], S));
            std::vector<int> pk;
            for (const uint32_t t : u.series[a].t) {
              const int p = ro.pick[u.block_of(t)];
              pk.push_back(p < 0 ? -1 : pos[static_cast<size_t>(p)]);
            }
            return std::make_unique<Switch>(std::move(ms), std::move(pk));
          },
          setup, panel.T, false);
      ro.row.model = std::string(kKinds[ro.kind].name) + "@roll";
      ro.row.params = "W=" + std::to_string(W) + ",R=" + std::to_string(R);
    });
  }

  const std::string vol = setup.vol > 0 ? std::to_string(static_cast<int>(setup.vol * 100)) + "%" : "关";
  const std::string roll = W > 0 ? std::to_string(W) + " 日 / 每 " + std::to_string(R) + " 日" : "关";
  std::printf("CTA bench %s: %zu 配置 | 成本 %.1fbp | 目标波动 %s | 不交易带 %g | 止损 %s | 滚动调参 %s | 每年 %.0f 根 | 排行仓位 ≥ %g\n", arg["freq"].c_str(),
              specs.size(), setup.cost * 1e4, vol.c_str(), setup.buffer, arg["stop"].c_str(), roll.c_str(), setup.bpy, expo);
  std::vector<char> flat_of(rows.size(), 0);     // 平原顶行
  std::vector<std::pair<size_t, size_t>> traces; // (测试集, band 平原顶 spec)
  for (size_t i = 0; i < us.size(); ++i) {
    const Universe &u = us[i];
    std::printf("\n== %s: %zu 只 (不足 %zu 日略 %zu) | %s … %s ==\n  VR", u.name.c_str(), u.series.size(), kMinDays, u.skipped,
                date_of(panel.time[u.lo]).c_str(), date_of(panel.time[u.hi]).c_str());
    for (const size_t k : kVrLags[panel.S > 1]) {
      std::vector<double> v;
      for (const Series &se : u.series)
        v.push_back(variance_ratio(se, k));
      std::printf("  %zu:%.2f", k, median(std::move(v)));
    }
    std::printf("\n");

    const Row *rs = &rows[i * specs.size()]; // [spec]
    print_head();
    print_row(rs[0]); // hold
    std::vector<const Row *> by;
    for (size_t j = 1; j < specs.size(); ++j)
      if (rs[j].med.expo >= expo)
        by.push_back(&rs[j]);
    for (const Roll &ro : rolls)
      if (ro.set == i && ro.row.med.expo >= expo)
        by.push_back(&ro.row);
    std::stable_sort(by.begin(), by.end(), [](const Row *a, const Row *b) { return a->port.sharpe > b->port.sharpe; });
    for (size_t k = 0; k < std::min(top, by.size()); ++k)
      print_row(*by[k]);

    std::printf("\n  %-16s %3s  %-36s %6s  %-36s %6s %6s %6s %6s %6s %5s %5s %5s\n", "网格汇总", "n", "最优 params", "最优SR", "平原顶 params", "平原SR",
                "邻均SR", "均SR", "均mΔSR", "均beat", "均Up", "均Dn", "均Edge");
    std::vector<std::string> names;
    for (const Spec &sp : specs)
      if (std::find(names.begin(), names.end(), name_of(sp)) == names.end())
        names.push_back(name_of(sp));
    for (const std::string &m : names) {
      std::vector<size_t> ids;
      std::vector<double> sr;
      for (size_t j = 0; j < specs.size(); ++j)
        if (name_of(specs[j]) == m)
          ids.push_back(j), sr.push_back(rs[j].port.sharpe);
      const std::vector<double> sm = smooth(near_of(specs, ids), sr);
      const Row *best = nullptr;
      size_t flat = ids.size();
      double n = 0, ssr = 0, dsr = 0, beat = 0, up = 0, dn = 0, edge = 0;
      for (size_t x = 0; x < ids.size(); ++x) {
        const Row &r = rs[ids[x]];
        if (r.med.expo >= expo && (!best || r.port.sharpe > best->port.sharpe))
          best = &r;
        if (r.med.expo >= expo && (flat == ids.size() || sm[x] > sm[flat]))
          flat = x;
        n += 1, ssr += r.port.sharpe, dsr += r.m_dsr, beat += r.beat, up += r.med.up, dn += r.med.dn, edge += r.med.edge;
      }
      const bool f = flat < ids.size();
      if (f)
        flat_of[i * specs.size() + ids[flat]] = 1;
      if (f && m == "band")
        traces.emplace_back(i, ids[flat]);
      std::printf("  %-16s %3.0f  %-36s %6.2f  %-36s %6.2f %6.2f %6.2f %6.2f %5.0f%% %5.2f %5.2f %+5.2f\n", m.c_str(), n,
                  best ? best->params.c_str() : "-", best ? best->port.sharpe : kNaN, f ? rs[ids[flat]].params.c_str() : "-",
                  f ? rs[ids[flat]].port.sharpe : kNaN, f ? sm[flat] : kNaN, ssr / n, dsr / n, 100 * beat / n, up / n, dn / n, edge / n);
    }

    if (W == 0)
      continue;
    std::printf("\n  滚动调参 (窗 %zu 日, 每 %zu 日重拟; 前 %zu 日空仓)\n", W, R, W);
    print_head();
    for (const Roll &ro : rolls) {
      if (ro.set != i)
        continue;
      print_row(ro.row);
      std::map<int, size_t> freq;
      size_t m = 0;
      for (const int p : ro.pick)
        if (p >= 0)
          ++freq[p], ++m;
      const auto most = std::max_element(freq.begin(), freq.end(), [](const auto &a, const auto &b) { return a.second < b.second; });
      const auto label = [&](int p) { return p < 0 ? std::string("-") : name_of(specs[ro.ids[static_cast<size_t>(p)]]) + " " + params_of(specs[ro.ids[static_cast<size_t>(p)]]); };
      std::printf("    %zu 期选过 %zu 种 | 最常 %s (%.0f%%) | 当前 %s\n", m, freq.size(), most == freq.end() ? "-" : label(most->first).c_str(),
                  m > 0 && most != freq.end() ? 100.0 * static_cast<double>(most->second) / static_cast<double>(m) : 0.0, label(ro.pick.back()).c_str());
    }
  }

  const std::string out = dir + "/bench_" + arg["freq"] + ".tsv";
  std::FILE *f = std::fopen(out.c_str(), "w");
  assert(f && "bench.tsv: 写不了");
  std::fprintf(f, "set\tmodel\tparams\tplateau\tsr\tcagr\tvol\tmdd\tsr1\tsr2\tm_sr\tm_dsr\tbeat\tm_mdd\tup\tdn\tedge\texpo\tturn\ttrades\twin\tpayoff\thold\n");
  std::vector<std::pair<const Row *, int>> all;
  for (size_t k = 0; k < rows.size(); ++k)
    all.emplace_back(&rows[k], flat_of[k]);
  for (const Roll &ro : rolls)
    all.emplace_back(&ro.row, 0);
  for (const auto &[p, flat] : all) {
    const Row &r = *p;
    std::fprintf(f, "%s\t%s\t%s\t%d\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.2f\n",
                 r.set.c_str(), r.model.c_str(), r.params.c_str(), flat, r.port.sharpe, r.port.cagr, r.port.vol, r.port.mdd, r.sr1, r.sr2, r.m_sr, r.m_dsr,
                 r.beat, r.m_mdd, r.med.up, r.med.dn, r.med.edge, r.med.expo, r.med.turn, r.med.trades, r.med.win, r.med.payoff, r.med.hold);
  }
  std::fclose(f);
  std::printf("\n全部 %zu 行 → %s\n", all.size(), out.c_str());

  // 轨迹: 各测试集 band 平原顶, 随机 trace 只资产各取随机一窗 (起点不早于序列 1/5, 避开预热)
  const size_t ntr = std::stoul(arg["trace"]);
  const std::string tout = dir + "/trace_" + arg["freq"] + ".tsv";
  std::remove(tout.c_str());
  if (ntr == 0 || traces.empty())
    return 0;
  std::FILE *g = std::fopen(tout.c_str(), "w");
  assert(g && "trace.tsv: 写不了");
  std::fprintf(g, "set\tasset\tparams\ttime\to\th\tl\tc\tmid\thalf\tw\tr\n");
  std::mt19937 rng(std::random_device{}());
  const size_t L = kTraceDays[panel.S > 1] * panel.S;
  for (const auto &[i, j] : traces) {
    const Universe &u = us[i];
    std::vector<size_t> pool(u.series.size());
    std::iota(pool.begin(), pool.end(), 0);
    std::shuffle(pool.begin(), pool.end(), rng);
    pool.resize(std::min(ntr, pool.size()));
    for (const size_t a : pool) {
      const Series &se = u.series[a];
      const std::unique_ptr<Model> m = make(specs[j], S);
      Probe pr(*m);
      const Run run = simulate(se, pr, setup);
      const size_t n = se.bar.size();
      assert(n / 5 + L <= n && "序列短于轨迹窗");
      const size_t s0 = std::uniform_int_distribution<size_t>(n / 5, n - L)(rng);
      const std::string params = params_of(specs[j]);
      for (size_t k = s0; k < s0 + L; ++k) {
        const Bar &b = se.bar[k];
        std::fprintf(g, "%s\t%s\t%s\t%lld\t%g\t%g\t%g\t%g\t%g\t%g\t%g\t%g\n", u.name.c_str(), u.names[a].c_str(), params.c_str(),
                     static_cast<long long>(panel.time[se.t[k]]), b.o, b.h, b.l, b.c, pr.mid[k], pr.half[k], run.w[k], run.r[k]);
      }
    }
  }
  std::fclose(g);
  std::printf("轨迹 %zu 测试集 × %zu 只 → %s\n", traces.size(), ntr, tout.c_str());
}
