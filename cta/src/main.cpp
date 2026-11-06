// cta_bench — 只做多 CTA 测试台: 模型 × 参数网格 × 测试集 (篮子表的 set), 逐资产回测 → 等权组合 + 跨资产中位数
//   用法: cta_bench [--freq 1d|1m] [--sets idx,ind,concept] [--models ma,tsmom,…] [--cost 0.001] [--vol 0] [--buffer 0]
//                   [--stop close|intra] [--top 15] [--expo 0.2]
//   先把 CTA 库 (cta/data, Db.hpp) 合成到 all.bar 特征库末日, 再按 freq 取面板; 模型参数 / 方差比周期的单位都是根 (1m = 分钟)
//   终端: 各测试集先给方差比 VR(k) (各资产中位数: 哪些周期有趋势 / 均值回归), 再按组合夏普排行 (+ 基准 hold),
//         末尾各模型网格汇总 (最优 / 网格均值: 参数稳健看均值); cta/data/bench_<freq>.tsv = 全部行
//   排行与最优只取平均仓位中位数 ≥ expo 的: 几乎不开仓的配置夏普没有可比性
//   列: SR / CAGR / MDD = 测试集等权组合 (逐根等权各资产净收益, 空仓部分计现金), SR1 / SR2 = 组合前 / 后半段夏普;
//       mSR = 各资产夏普中位数, mΔSR = 各资产 (夏普 − 自身买入持有夏普) 中位数, beat = 夏普胜过买入持有的资产占比;
//       Up / Dn / Edge / Expo / Turn / Tr/y / Win / Pay / Hold = 各资产中位数 (Metrics.hpp)
#include "Metrics.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdio>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {

using P = std::vector<float>;

struct Kind {
  const char *name;
  std::vector<std::pair<const char *, std::vector<float>>> grid; // 参数名 → 取值
  bool (*ok)(const P &);                                         // 组合约束 (nullptr = 全要)
  std::unique_ptr<Model> (*make)(const P &);
  bool trail; // 另叠 Trail 外挂 (k = kTrail) 各跑一份
};

constexpr float kTrail = 3;
constexpr size_t kMinDays = 500;                                                                                                  // 资产有效根数下限 = 这么多日的根
const std::vector<size_t> kVrLags[] = {{2, 3, 5, 10, 20, 40, 60, 120, 250}, {2, 5, 15, 30, 60, 120, 240, 480, 1200, 2400, 4800}}; // 1d / 1m

// clang-format off
const Kind kKinds[] = {
    {"hold", {}, nullptr, [](const P &) -> std::unique_ptr<Model> { return std::make_unique<Hold>(); }, false},
    {"ma", {{"fast", {5, 10, 20}}, {"slow", {20, 40, 60, 120, 250}}}, [](const P &p) { return 2 * p[0] <= p[1]; },
     [](const P &p) -> std::unique_ptr<Model> { return std::make_unique<Ma>(p[0], p[1]); }, true},
    {"tsmom", {{"L", {10, 20, 40, 60, 120, 250}}}, nullptr,
     [](const P &p) -> std::unique_ptr<Model> { return std::make_unique<TsMom>(p[0]); }, true},
    {"ens", {{"lo", {5, 10, 20}}}, nullptr,
     [](const P &p) -> std::unique_ptr<Model> { return std::make_unique<Ens>(p[0]); }, true},
    {"kama", {{"n", {10, 20}}, {"slow", {30, 60}}}, nullptr,
     [](const P &p) -> std::unique_ptr<Model> { return std::make_unique<Kama>(p[0], p[1]); }, true},
    {"donchian", {{"n", {20, 40, 60, 120}}, {"m", {10, 20, 40}}}, [](const P &p) { return p[1] < p[0]; },
     [](const P &p) -> std::unique_ptr<Model> { return std::make_unique<Donchian>(p[0], p[1]); }, false},
    {"chandelier", {{"n", {10, 20, 60}}, {"k", {2, 3, 4, 5}}}, nullptr,
     [](const P &p) -> std::unique_ptr<Model> { return std::make_unique<Chandelier>(p[0], p[1]); }, false},
    {"rebound", {{"d", {0.5, 1, 1.5}}, {"n", {5, 10, 20}}, {"k", {2, 3, 4}}}, nullptr,
     [](const P &p) -> std::unique_ptr<Model> { return std::make_unique<Rebound>(p[0], p[1], p[2]); }, false},
    {"revert", {{"L", {60, 120, 250}}, {"g", {0.5, 1, 1.5}}}, nullptr,
     [](const P &p) -> std::unique_ptr<Model> { return std::make_unique<Revert>(p[0], p[1]); }, false},
    {"gate", {{"L", {60, 120, 250}}, {"g", {-1, -0.5, 0}}, {"f", {5, 10, 20}}, {"h", {0, 0.25, 0.5}}}, nullptr,
     [](const P &p) -> std::unique_ptr<Model> { return std::make_unique<Gate>(p[0], p[1], p[2], p[3]); }, false},
};
// clang-format on

struct Spec {
  size_t kind;
  P p;
  float trail = 0;
};

std::string name_of(const Spec &s) { return std::string(kKinds[s.kind].name) + (s.trail > 0 ? "+trail" : ""); }

std::string params_of(const Spec &s) {
  std::string o;
  char buf[32];
  for (size_t i = 0; i < s.p.size(); ++i) {
    std::snprintf(buf, sizeof(buf), "%s%s=%g", i ? "," : "", kKinds[s.kind].grid[i].first, s.p[i]);
    o += buf;
  }
  if (s.trail > 0) {
    std::snprintf(buf, sizeof(buf), "%sk=%g", o.empty() ? "" : ",", s.trail);
    o += buf;
  }
  return o.empty() ? "-" : o;
}

std::unique_ptr<Model> make(const Spec &s) {
  std::unique_ptr<Model> m = kKinds[s.kind].make(s.p);
  return s.trail > 0 ? std::make_unique<Trail>(std::move(m), s.trail) : std::move(m);
}

std::vector<Spec> specs_of(const std::vector<std::string> &models) {
  std::vector<Spec> out;
  for (size_t k = 0; k < std::size(kKinds); ++k) {
    const Kind &K = kKinds[k];
    if (k > 0 && !models.empty() && std::find(models.begin(), models.end(), K.name) == models.end())
      continue;
    std::vector<size_t> at(K.grid.size(), 0); // 网格里程表
    for (bool more = true; more;) {
      P p;
      for (size_t i = 0; i < at.size(); ++i)
        p.push_back(K.grid[i].second[at[i]]);
      if (!K.ok || K.ok(p)) {
        out.push_back({k, p, 0});
        if (K.trail)
          out.push_back({k, p, kTrail});
      }
      more = false;
      for (size_t i = 0; i < at.size() && !more; ++i)
        if (++at[i] < K.grid[i].second.size())
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
  size_t lo = 0, hi = 0, mid = 0;   // 张量时间段 [lo, hi], 前后半段分界
  size_t skipped = 0;               // 有效根数不足 kMinBars 的
};

struct Row {
  std::string set, model, params;
  Stat port; // 等权组合
  double sr1 = 0, sr2 = 0;
  double m_sr = 0, m_dsr = 0, beat = 0, m_mdd = 0;
  Stat med = {}; // 各资产中位数 (up dn edge expo turn trades win payoff hold)
};

Row evaluate(const Universe &u, const Spec &sp, const Setup &setup, size_t T) {
  std::vector<double> sum(T, 0), cnt(T, 0);
  std::vector<Stat> st;
  for (const Series &se : u.series) {
    const std::unique_ptr<Model> m = make(sp);
    const Run run = simulate(se, *m, setup);
    st.push_back(stat_of(se, run, setup));
    for (size_t i = 0; i < se.t.size(); ++i)
      sum[se.t[i]] += run.r[i], cnt[se.t[i]] += 1;
  }
  std::vector<double> r;
  for (size_t t = u.lo; t <= u.hi; ++t)
    r.push_back(cnt[t] > 0 ? sum[t] / cnt[t] : 0);
  Row row{u.name, name_of(sp), params_of(sp), returns_stat(r, setup.bpy)};
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

std::string date_of(int64_t t) {
  const int d = ymd_of(t);
  char buf[16];
  std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d", d / 10000, d / 100 % 100, d % 100);
  return buf;
}

void print_head() {
  std::printf("  %-16s %-22s %5s %6s %6s %5s %5s | %5s %5s %4s | %4s %4s %5s %4s %5s %5s %4s %4s %5s\n", "model", "params", "SR", "CAGR", "MDD",
              "SR1", "SR2", "mSR", "mΔSR", "beat", "Up", "Dn", "Edge", "Expo", "Turn", "Tr/y", "Win", "Pay", "Hold");
}

void print_row(const Row &r) {
  std::printf("  %-16s %-22s %5.2f %5.1f%% %5.1f%% %5.2f %5.2f | %5.2f %5.2f %3.0f%% | %4.2f %4.2f %+5.2f %4.2f %5.1f %5.1f %3.0f%% %4.2f %5.0f\n",
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

} // namespace

int main(int argc, char **argv) {
  std::map<std::string, std::string> arg = {{"freq", "1d"}, {"sets", "idx,ind,concept"}, {"models", ""}, {"cost", "0.001"}, {"vol", "0"}, {"buffer", "0"}, {"stop", "close"}, {"top", "15"}, {"expo", "0.2"}};
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
  const std::vector<Spec> specs = specs_of(split(arg["models"]));
  const size_t top = std::stoul(arg["top"]);
  const double expo = std::stod(arg["expo"]);

  std::vector<Universe> us;
  for (const std::string &name : split(arg["sets"])) {
    Universe u{name};
    u.lo = panel.T, u.hi = 0;
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
      u.series.push_back(std::move(se));
    }
    assert(!u.series.empty() && "测试集为空");
    u.mid = (u.lo + u.hi + 1) / 2;
    us.push_back(std::move(u));
  }

  std::vector<std::pair<size_t, size_t>> tasks; // (测试集, spec)
  for (size_t i = 0; i < us.size(); ++i)
    for (size_t j = 0; j < specs.size(); ++j)
      tasks.emplace_back(i, j);
  std::vector<Row> rows(tasks.size());
  {
    std::atomic<size_t> next{0};
    std::vector<std::jthread> ws;
    for (unsigned i = 0; i < std::max(1u, std::thread::hardware_concurrency()); ++i)
      ws.emplace_back([&] {
        for (size_t k; (k = next++) < tasks.size();)
          rows[k] = evaluate(us[tasks[k].first], specs[tasks[k].second], setup, panel.T);
      });
  }

  const std::string vol = setup.vol > 0 ? std::to_string(static_cast<int>(setup.vol * 100)) + "%" : "关";
  std::printf("CTA bench %s: %zu 配置 | 成本 %.1fbp | 目标波动 %s | 不交易带 %g | 止损 %s | 每年 %.0f 根 | 排行仓位 ≥ %g\n", arg["freq"].c_str(), specs.size(),
              setup.cost * 1e4, vol.c_str(), setup.buffer, arg["stop"].c_str(), setup.bpy, expo);
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

    std::vector<const Row *> rs;
    for (size_t k = 0; k < tasks.size(); ++k)
      if (tasks[k].first == i)
        rs.push_back(&rows[k]);
    print_head();
    print_row(*rs.front()); // hold
    std::vector<const Row *> by;
    for (const Row *r : rs)
      if (r != rs.front() && r->med.expo >= expo)
        by.push_back(r);
    std::stable_sort(by.begin(), by.end(), [](const Row *a, const Row *b) { return a->port.sharpe > b->port.sharpe; });
    for (size_t k = 0; k < std::min(top, by.size()); ++k)
      print_row(*by[k]);

    std::printf("\n  %-16s %3s  %-22s %6s %6s %6s %6s %5s %5s %5s\n", "网格汇总", "n", "最优 params", "最优SR", "均SR", "均mΔSR", "均beat", "均Up",
                "均Dn", "均Edge");
    std::vector<std::string> names;
    for (const Row *r : rs)
      if (std::find(names.begin(), names.end(), r->model) == names.end())
        names.push_back(r->model);
    for (const std::string &m : names) {
      const Row *best = nullptr;
      double n = 0, sr = 0, dsr = 0, beat = 0, up = 0, dn = 0, edge = 0;
      for (const Row *r : rs) {
        if (r->model != m)
          continue;
        if (r->med.expo >= expo && (!best || r->port.sharpe > best->port.sharpe))
          best = r;
        n += 1, sr += r->port.sharpe, dsr += r->m_dsr, beat += r->beat, up += r->med.up, dn += r->med.dn, edge += r->med.edge;
      }
      std::printf("  %-16s %3.0f  %-22s %6.2f %6.2f %6.2f %5.0f%% %5.2f %5.2f %+5.2f\n", m.c_str(), n, best ? best->params.c_str() : "-",
                  best ? best->port.sharpe : kNaN, sr / n, dsr / n, 100 * beat / n, up / n, dn / n, edge / n);
    }
  }

  const std::string out = dir + "/bench_" + arg["freq"] + ".tsv";
  std::FILE *f = std::fopen(out.c_str(), "w");
  assert(f && "bench.tsv: 写不了");
  std::fprintf(f, "set\tmodel\tparams\tsr\tcagr\tvol\tmdd\tsr1\tsr2\tm_sr\tm_dsr\tbeat\tm_mdd\tup\tdn\tedge\texpo\tturn\ttrades\twin\tpayoff\thold\n");
  for (const Row &r : rows)
    std::fprintf(f, "%s\t%s\t%s\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.2f\n",
                 r.set.c_str(), r.model.c_str(), r.params.c_str(), r.port.sharpe, r.port.cagr, r.port.vol, r.port.mdd, r.sr1, r.sr2, r.m_sr, r.m_dsr,
                 r.beat, r.m_mdd, r.med.up, r.med.dn, r.med.edge, r.med.expo, r.med.turn, r.med.trades, r.med.win, r.med.payoff, r.med.hold);
  std::fclose(f);
  std::printf("\n全部 %zu 行 → %s\n", rows.size(), out.c_str());
}
