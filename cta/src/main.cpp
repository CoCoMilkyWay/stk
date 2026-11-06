// cta_bench — 只做多 band 测试台: 部件网格 (Model.hpp kParts: 中线 × 带 × 分层, 各含模型参数) × 测试集 (篮子表的 set), 逐资产回测 → 等权组合 + 跨资产中位数
//   用法: cta_bench [--freq 1m|1d] [--sets idx,ind,concept] [--cost 0.001] [--vol 0] [--buffer 0]
//                   [--target sharpe|sortino] [--roll 250] [--refit 20] [--top 15] [--expo 0.2] [--assets 50]
//   每测试集固定种子随机抽 assets 只 (0 = 全部; 不足 kMinDays 的跳过不计)
//   先把 CTA 库 (cta/data, Db.hpp) 合成到 all.bar 特征库末日, 再按 freq 取面板; 窗口 / 方差比周期的单位都是根 (1m = 分钟), 部件的周期轴以日计
//   任务 = (测试集, 组): 组 = 中线 (模型 + 参数) 与带 (模型 + 周期) 相同的配置, 逐资产共用一份指标列 (Tape); 基准 hold 每测试集一份
//   优化目标 = 模型 (--target: sharpe / sortino) + 周期 (--roll W 日窗, ≥ 一年; --refit R 日重拟), 只做滚动, 不用未来信息:
//     每 R 日, 用前 W 日 (向上取整到 R 的倍数) 各资产 (样本) 的目标值均值拟一张曲面, 取平原顶配置用到下一个 R 日; 前 W 日空仓
//   全期网格 (按目标排行 / 平原顶 / 网格均值) 只是曲面的样子, 含未来信息, 不是可执行结果; band@roll 才是
//   终端: 各测试集先给方差比 VR(k) (各资产中位数: 哪些周期有趋势 / 均值回归), 再按全期目标排行 (+ 基准 hold, band@roll),
//         末尾网格汇总 + 滚动调参选中统计; cta/data/bench_<freq>.tsv = 全部行
//   平原: 曲面按邻域 (各组模型相同, 参数轴下标差 ≤ 1) 取均值, 平原顶 = 邻域均值最高的点 (不挑孤立尖峰)
//   排行与最优只取平均仓位中位数 ≥ expo 的: 几乎不开仓的配置没有可比性
//   列: SR / So / CAGR / MDD = 测试集等权组合 (逐根等权各资产净收益, 空仓部分计现金) 的夏普 / 索提诺 / ..., SR1 / SR2 = 组合前 / 后半段夏普;
//       mSR = 各资产夏普中位数, mΔSR = 各资产 (夏普 − 自身买入持有夏普) 中位数, beat = 夏普胜过买入持有的资产占比;
//       Up / Dn / Edge / Expo / Turn / Tr/y / Win / Pay / Hold = 各资产中位数 (Metrics.hpp)
#include "Metrics.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <functional>
#include <map>
#include <random>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {

constexpr unsigned kSeed = 1;                                                                                                     // 资产抽样: 固定种子, 各次运行同一批
constexpr size_t kMinRoll = 250;                                                                                                  // 滚动调参窗下限 (日) = 一年
constexpr size_t kMinDays = 500;                                                                                                  // 资产有效根数下限 = 这么多日的根
const std::vector<size_t> kVrLags[] = {{2, 3, 5, 10, 20, 40, 60, 120, 250}, {2, 5, 15, 30, 60, 120, 240, 480, 1200, 2400, 4800}}; // 1d / 1m

// 优化目标 (模型): 夏普 = 均值 / 标准差, 索提诺 = 均值 / 下行波动; 滚动块前缀和留 [Σr, Σq], q = r² / min(r, 0)²
enum Target { kSharpe,
              kSortino };

struct Spec {
  Cfg g;                  // 以根计
  std::vector<size_t> at; // 各组所选模型的参数轴取值下标, 按组序拼成一条
};

// 显示串: 各组 模型(轴=值, …) 以 " | " 隔开, 值按轴单位 (日)
std::string params_of(const Spec &s) {
  std::string o;
  char buf[32];
  for (size_t j = 0, x = 0; j < kNP; ++j) {
    const Kind &K = kParts[j].kinds[s.g.k[j]];
    o += j ? " | " : "", o += K.name;
    for (size_t i = 0; i < K.axes.size(); ++i, ++x) {
      std::snprintf(buf, sizeof(buf), "%s%s=%g", i ? "," : "(", K.axes[i].name, K.axes[i].v[s.at[x]]);
      o += buf;
    }
    if (!K.axes.empty())
      o += ')';
  }
  return o;
}

// 网格相邻: 各组模型相同, 参数轴下标差 ≤ 1 (含自身)
bool near(const Spec &a, const Spec &b) {
  if (a.g.k != b.g.k)
    return false;
  for (size_t i = 0; i < a.at.size(); ++i)
    if ((a.at[i] > b.at[i] ? a.at[i] - b.at[i] : b.at[i] - a.at[i]) > 1)
      return false;
  return true;
}

// 各配置的邻居 (specs 下标)
std::vector<std::vector<size_t>> near_of(const std::vector<Spec> &specs) {
  std::vector<std::vector<size_t>> out(specs.size());
  for (size_t i = 0; i < specs.size(); ++i)
    for (size_t j = 0; j < specs.size(); ++j)
      if (near(specs[i], specs[j]))
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

// 里程表进一格: at[i] ∈ [0, size(i)); 转满一圈 → false
template <class A, class F>
bool tick(A &at, F size) {
  for (size_t i = 0; i < at.size(); ++i) {
    if (++at[i] < size(i))
      return true;
    at[i] = 0;
  }
  return false;
}

// 全网格: 各组模型的组合 × 所选模型参数轴的组合
std::vector<Spec> specs_of(double S) {
  std::vector<Spec> out;
  std::array<size_t, kNP> k{};
  do {
    std::vector<const Axis *> ax;
    for (size_t j = 0; j < kNP; ++j)
      for (const Axis &a : kParts[j].kinds[k[j]].axes)
        ax.push_back(&a);
    std::vector<size_t> at(ax.size(), 0);
    do {
      Cfg g{k, {}};
      for (size_t j = 0, x = 0; j < kNP; ++j)
        for (size_t i = 0; i < kParts[j].kinds[k[j]].axes.size(); ++i, ++x)
          g.p[j].push_back(ax[x]->v[at[x]]);
      g = to_bars(std::move(g), S);
      if (admit(g))
        out.push_back({std::move(g), at});
    } while (tick(at, [&](size_t i) { return ax[i]->v.size(); }));
  } while (tick(k, [](size_t j) { return kParts[j].kinds.size(); }));
  return out;
}

// 按中线 (模型 + 参数) 与带 (模型 + 周期 = 带的首轴) 分组 → 各组的 specs 下标
std::vector<std::vector<size_t>> groups_of(const std::vector<Spec> &specs) {
  std::map<std::vector<size_t>, size_t> at;
  std::vector<std::vector<size_t>> out;
  for (size_t j = 0; j < specs.size(); ++j) {
    const Spec &s = specs[j];
    const size_t nc = kParts[kCenter].kinds[s.g.k[kCenter]].axes.size();
    std::vector<size_t> key = {s.g.k[kCenter], s.g.k[kBand]};
    key.insert(key.end(), s.at.begin(), s.at.begin() + static_cast<long>(nc + 1));
    const auto [it, fresh] = at.try_emplace(std::move(key), out.size());
    if (fresh)
      out.emplace_back();
    out[it->second].push_back(j);
  }
  return out;
}

struct Universe {
  std::string name;
  std::vector<Series> series = {};
  std::vector<double> hold_sr = {}; // [资产] 买入持有夏普
  size_t lo = 0, hi = 0, mid = 0;   // 张量时间段 [lo, hi], 前后半段分界
  std::vector<float> live = {};     // [t − lo] 当根有数据的资产数
  size_t total = 0;                 // 测试集资产数 (抽样前)
  size_t skipped = 0;               // 有效根数不足 kMinDays 日的
  // 滚动调参: 目标 + 块 (refit 日一块, 自 lo 所在日起): 块数 + [资产][块] 根数前缀和
  Target tg = kSharpe;
  size_t S = 1, R = 0, nb = 0;
  std::vector<std::vector<double>> cnt = {};
  size_t block_of(size_t t) const { return (t / S - lo / S) / R; }
};

struct Row {
  std::string set, model, params;
  Stat port, med;              // 等权组合 / 各资产中位数
  double sr1 = 0, sr2 = 0;     // 组合前 / 后半段夏普
  double m_dsr = 0, beat = 0;  // 各资产 (夏普 − 自身买入持有夏普) 中位数 / 夏普胜过买入持有的资产占比
  std::vector<float> blk = {}; // 滚动调参: [资产][块][Σr, Σq] 前缀和 (空 = 未留)
};

// 全期目标值 (只用于看曲面, 不用于滚动选参)
double obj(const Row &r, Target tg) { return tg == kSharpe ? r.port.sharpe : r.port.sortino; }

using Make = std::function<std::vector<Col>(size_t, Tape &)>; // (资产下标, 其列) → 各行的目标仓位列

// 逐资产: make 给 k 行的目标仓位列 → 回测; 每行汇成 等权组合 + 跨资产中位数; keep = 留滚动调参的块前缀和
std::vector<Row> evaluate(const Universe &u, size_t k, const Make &make, const Setup &setup, bool keep) {
  const size_t A = u.series.size(), T = u.live.size();
  std::vector<std::vector<float>> sum(k, std::vector<float>(T, 0));
  std::vector<std::vector<Stat>> st(k);
  std::vector<Row> rows(k);
  if (keep)
    for (Row &row : rows)
      row.blk.assign(A * u.nb * 2, 0);
  for (size_t a = 0; a < A; ++a) {
    const Series &se = u.series[a];
    Tape t(se);
    std::vector<Col> ws = make(a, t);
    assert(ws.size() == k && "make 给的列数");
    for (size_t j = 0; j < k; ++j) {
      const Run run = simulate(t, std::move(ws[j]), setup);
      st[j].push_back(stat_of(t, run, setup));
      for (size_t i = 0; i < t.n; ++i)
        sum[j][se.t[i] - u.lo] += static_cast<float>(run.r[i]);
      if (!keep)
        continue;
      std::vector<double> acc(u.nb * 2, 0);
      for (size_t i = 0; i < t.n; ++i) {
        const size_t b = u.block_of(se.t[i]);
        const double r = run.r[i], q = u.tg == kSharpe ? r : std::min(r, 0.0);
        acc[2 * b] += r, acc[2 * b + 1] += q * q;
      }
      for (size_t x = 2; x < acc.size(); ++x)
        acc[x] += acc[x - 2];
      std::copy(acc.begin(), acc.end(), rows[j].blk.begin() + static_cast<long>(a * u.nb * 2));
    }
  }
  for (size_t j = 0; j < k; ++j) {
    Row &row = rows[j];
    std::vector<double> r(T);
    for (size_t i = 0; i < T; ++i)
      r[i] = u.live[i] > 0 ? sum[j][i] / u.live[i] : 0;
    row.set = u.name, row.port = returns_stat(r, setup.bpy);
    const size_t h = u.mid - u.lo;
    row.sr1 = returns_stat(std::span(r).first(h), setup.bpy).sharpe;
    row.sr2 = returns_stat(std::span(r).subspan(h), setup.bpy).sharpe;
    for (const Metric &f : kStat) {
      std::vector<double> v;
      for (const Stat &s : st[j])
        v.push_back(s.*f.v);
      row.med.*f.v = median(std::move(v));
    }
    std::vector<double> d;
    for (size_t a = 0; a < A; ++a)
      d.push_back(st[j][a].sharpe - u.hold_sr[a]), row.beat += st[j][a].sharpe > u.hold_sr[a];
    row.m_dsr = median(std::move(d)), row.beat /= static_cast<double>(A);
  }
  return rows;
}

// 滚动调参: 块 b ≥ W 起, 以块 [b − W, b) 内各资产 (样本) 目标值的均值作曲面, 取平原顶 → pick[b] (rows 内下标; −1 = 空仓)
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
        const double mean = win(p, 0) / n, q = win(p, 1) / n, d = u.tg == kSharpe ? q - mean * mean : q;
        sum += d > 0 ? mean / std::sqrt(d) : 0, m += 1;
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

// 一个测试集的滚动调参 (候选 = 全网格); 执行: 选中过的候选各算全期仓位列 (状态常热), 逐根取当块选中者的; 切换的换手照常计成本
struct Roll {
  std::vector<int> pick = {}; // [块] → specs 下标 (−1 = 空仓)
  Row row = {};
};

std::string date_of(int64_t t) {
  const int d = ymd_of(t);
  char buf[16];
  std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d", d / 10000, d / 100 % 100, d % 100);
  return buf;
}

void print_head() {
  std::printf("  %-10s %-54s %5s %5s %6s %6s %5s %5s | %5s %5s %4s | %4s %4s %5s %4s %5s %5s %4s %4s %5s\n", "model", "params", "SR", "So", "CAGR", "MDD",
              "SR1", "SR2", "mSR", "mΔSR", "beat", "Up", "Dn", "Edge", "Expo", "Turn", "Tr/y", "Win", "Pay", "Hold");
}

void print_row(const Row &r) {
  std::printf("  %-10s %-54s %5.2f %5.2f %5.1f%% %5.1f%% %5.2f %5.2f | %5.2f %5.2f %3.0f%% | %4.2f %4.2f %+5.2f %4.2f %5.1f %5.1f %3.0f%% %4.2f %5.0f\n",
              r.model.c_str(), r.params.c_str(), r.port.sharpe, r.port.sortino, 100 * r.port.cagr, 100 * r.port.mdd, r.sr1, r.sr2, r.med.sharpe, r.m_dsr, 100 * r.beat,
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

// 多线程跑 n 个任务; stderr 每秒刷一行进度 (完成 / 总数, 已用, 预计剩余)
void parallel(const char *what, size_t n, const std::function<void(size_t)> &f) {
  std::atomic<size_t> next{0}, done{0};
  const auto t0 = std::chrono::steady_clock::now();
  const auto show = [&] {
    const double s = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    const size_t d = done;
    std::fprintf(stderr, "\r  %s %zu / %zu (%.0f%%) | 已用 %.0fs, 剩 ~%.0fs   ", what, d, n, n ? 100.0 * static_cast<double>(d) / static_cast<double>(n) : 100.0, s,
                 d ? s * static_cast<double>(n - d) / static_cast<double>(d) : 0.0);
  };
  {
    std::jthread mon([&](std::stop_token st) {
      for (int i = 0; !st.stop_requested(); ++i) {
        if (i % 10 == 0)
          show();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
    });
    std::vector<std::jthread> ws;
    for (unsigned i = 0; i < std::max(1u, std::thread::hardware_concurrency()); ++i)
      ws.emplace_back([&] {
        for (size_t k; (k = next++) < n;)
          f(k), ++done;
      });
    for (std::jthread &w : ws)
      w.join();
  }
  show();
  std::fputc('\n', stderr);
}

} // namespace

int main(int argc, char **argv) {
  std::map<std::string, std::string> arg = {{"freq", "1m"}, {"sets", "idx,ind,concept"}, {"cost", "0.001"}, {"vol", "0"}, {"buffer", "0"}, {"target", "sharpe"}, {"roll", "250"}, {"refit", "20"}, {"top", "15"}, {"expo", "0.2"}, {"assets", "50"}};
  assert(argc % 2 == 1 && "参数须成对: --键 值");
  for (int i = 1; i + 1 < argc; i += 2) {
    const std::string k = argv[i];
    assert(k.starts_with("--") && arg.contains(k.substr(2)) && "未知参数");
    arg[k.substr(2)] = argv[i + 1];
  }

  const std::string root = std::string(CTA_DIR) + "/..", dir = std::string(CTA_DIR) + "/data";
  const Lib lib = open_lib(root + "/output/features/all/bar");
  const Table table = make_table(lib, root + "/output/concept");
  sync(lib, table, dir);
  const Panel panel = open_panel(dir, table, arg["freq"]);
  Setup setup;
  setup.cost = std::stod(arg["cost"]), setup.vol = std::stod(arg["vol"]), setup.buffer = std::stod(arg["buffer"]);
  setup.bpy = panel.bars_per_year;
  const std::vector<Spec> specs = specs_of(static_cast<double>(panel.S));
  const std::vector<std::vector<size_t>> groups = groups_of(specs), nb = near_of(specs);
  const size_t top = std::stoul(arg["top"]);
  const double expo = std::stod(arg["expo"]);
  assert((arg["target"] == "sharpe" || arg["target"] == "sortino") && "--target: sharpe | sortino");
  const Target tg = arg["target"] == "sharpe" ? kSharpe : kSortino;
  const size_t W = std::stoul(arg["roll"]), R = std::stoul(arg["refit"]);
  assert(R > 0 && "--refit 须 > 0");
  const size_t Wb = (W + R - 1) / R; // 滚动窗块数 (向上取整)
  assert(W >= kMinRoll && "--roll 须 ≥ 一年");
  const size_t cap = std::stoul(arg["assets"]);

  std::vector<Universe> us;
  for (const std::string &name : split(arg["sets"])) {
    Universe u{name};
    u.lo = panel.T, u.hi = 0, u.tg = tg, u.S = panel.S, u.R = R;
    std::vector<size_t> pool;
    for (size_t b = 0; b < panel.B; ++b)
      if (panel.assets[b].set == name)
        pool.push_back(b);
    u.total = pool.size();
    std::shuffle(pool.begin(), pool.end(), std::mt19937(kSeed));
    for (const size_t b : pool) {
      if (cap > 0 && u.series.size() == cap)
        break;
      Series se = series_of(panel, b);
      if (se.c.size() < kMinDays * panel.S) {
        ++u.skipped;
        continue;
      }
      {
        Tape t(se);
        u.hold_sr.push_back(stat_of(t, simulate(t, hold(t), setup), setup).sharpe);
      }
      u.lo = std::min<size_t>(u.lo, se.t.front()), u.hi = std::max<size_t>(u.hi, se.t.back());
      u.series.push_back(std::move(se));
      std::fprintf(stderr, "\r  载入 %s %zu / %zu   ", name.c_str(), u.series.size(), cap > 0 ? std::min(cap, u.total) : u.total);
    }
    std::fputc('\n', stderr);
    assert(!u.series.empty() && "测试集为空");
    u.mid = (u.lo + u.hi + 1) / 2;
    u.live.assign(u.hi - u.lo + 1, 0);
    for (const Series &se : u.series)
      for (const uint32_t t : se.t)
        u.live[t - u.lo] += 1;
    u.nb = u.block_of(u.hi) + 1;
    assert(u.nb > Wb && "样本短于滚动窗");
    for (const Series &se : u.series) {
      std::vector<double> c(u.nb, 0);
      for (const uint32_t t : se.t)
        c[u.block_of(t)] += 1;
      for (size_t b = 1; b < u.nb; ++b)
        c[b] += c[b - 1];
      u.cnt.push_back(std::move(c));
    }
    us.push_back(std::move(u));
  }

  std::vector<std::pair<size_t, size_t>> tasks; // (测试集, 组)
  for (size_t i = 0; i < us.size(); ++i)
    for (size_t g = 0; g < groups.size(); ++g)
      tasks.emplace_back(i, g);
  std::vector<Row> rows(us.size() * specs.size()); // 下标 = 测试集 · |specs| + spec
  parallel("回测", tasks.size(), [&](size_t k) {
    const auto [i, g] = tasks[k];
    const std::vector<size_t> &ids = groups[g];
    std::vector<Row> rs = evaluate(
        us[i], ids.size(),
        [&](size_t, Tape &t) {
          std::vector<Col> ws;
          for (const size_t j : ids)
            ws.push_back(band(t, specs[j].g).w);
          return ws;
        },
        setup, true);
    for (size_t x = 0; x < ids.size(); ++x) {
      Row &r = rows[i * specs.size() + ids[x]] = std::move(rs[x]);
      r.model = "band", r.params = params_of(specs[ids[x]]);
    }
  });
  std::vector<Row> holds(us.size());
  parallel("基准", us.size(), [&](size_t i) {
    holds[i] = std::move(evaluate(us[i], 1, [](size_t, Tape &t) { return std::vector<Col>{hold(t)}; }, setup, false)[0]);
    holds[i].model = "hold", holds[i].params = "-";
  });

  std::vector<Roll> rolls(us.size()); // [测试集]
  parallel("拟曲面", us.size(), [&](size_t i) {
    std::vector<const Row *> rs;
    for (size_t j = 0; j < specs.size(); ++j)
      rs.push_back(&rows[i * specs.size() + j]);
    rolls[i].pick = roll_pick(us[i], rs, nb, Wb);
  });
  for (Row &r : rows)
    r.blk = {};
  const std::string roll_params = arg["target"] + ",W=" + std::to_string(Wb * R) + ",R=" + std::to_string(R);
  parallel("滚动调参", rolls.size(), [&](size_t i) {
    Roll &ro = rolls[i];
    const Universe &u = us[i];
    std::vector<char> used(specs.size(), 0);
    for (const int p : ro.pick)
      if (p >= 0)
        used[static_cast<size_t>(p)] = 1;
    ro.row = std::move(evaluate(
        u, 1,
        [&](size_t a, Tape &t) {
          std::vector<Col> ws(specs.size());
          for (size_t x = 0; x < ws.size(); ++x)
            if (used[x])
              ws[x] = band(t, specs[x].g).w;
          Col w(t.n, 0);
          for (size_t j = 0; j < t.n; ++j)
            if (const int p = ro.pick[u.block_of(u.series[a].t[j])]; p >= 0)
              w[j] = ws[static_cast<size_t>(p)][j];
          return std::vector<Col>{std::move(w)};
        },
        setup, false)[0]);
    ro.row.model = "band@roll";
    ro.row.params = roll_params;
  });

  const std::string vol = setup.vol > 0 ? std::to_string(static_cast<int>(setup.vol * 100)) + "%" : "关";
  std::printf("CTA bench %s: %zu 配置 | 成本 %.1fbp | 目标波动 %s | 不交易带 %g | 优化目标 %s, 窗 %zu 日 / 每 %zu 日 | 每年 %.0f 根 | 排行仓位 ≥ %g\n",
              arg["freq"].c_str(), specs.size(), setup.cost * 1e4, vol.c_str(), setup.buffer, arg["target"].c_str(), Wb * R, R, setup.bpy, expo);
  std::vector<char> flat_of(rows.size(), 0); // 平原顶行
  for (size_t i = 0; i < us.size(); ++i) {
    const Universe &u = us[i];
    std::printf("\n== %s: %zu / %zu 只 (不足 %zu 日略 %zu) | %s … %s ==\n  VR", u.name.c_str(), u.series.size(), u.total, kMinDays, u.skipped,
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
    print_row(holds[i]);
    std::vector<const Row *> by;
    for (size_t j = 0; j < specs.size(); ++j)
      if (rs[j].med.expo >= expo)
        by.push_back(&rs[j]);
    if (rolls[i].row.med.expo >= expo)
      by.push_back(&rolls[i].row);
    std::stable_sort(by.begin(), by.end(), [&](const Row *a, const Row *b) { return obj(*a, tg) > obj(*b, tg); });
    for (size_t k = 0; k < std::min(top, by.size()); ++k)
      print_row(*by[k]);

    std::printf("\n  %-10s %4s  %-54s %6s  %-54s %6s %6s %6s %6s %6s %5s %5s %5s\n", "网格汇总", "n", "最优 params", "最优", "平原顶 params", "平原",
                "邻均", "均", "均mΔSR", "均beat", "均Up", "均Dn", "均Edge");
    std::vector<double> sc;
    for (size_t j = 0; j < specs.size(); ++j)
      sc.push_back(obj(rs[j], tg));
    const std::vector<double> sm = smooth(nb, sc);
    const Row *best = nullptr;
    size_t flat = specs.size();
    double n = 0, ssc = 0, dsr = 0, beat = 0, up = 0, dn = 0, edge = 0;
    for (size_t j = 0; j < specs.size(); ++j) {
      const Row &r = rs[j];
      if (r.med.expo >= expo && (!best || sc[j] > obj(*best, tg)))
        best = &r;
      if (r.med.expo >= expo && (flat == specs.size() || sm[j] > sm[flat]))
        flat = j;
      n += 1, ssc += sc[j], dsr += r.m_dsr, beat += r.beat, up += r.med.up, dn += r.med.dn, edge += r.med.edge;
    }
    const bool f = flat < specs.size();
    if (f)
      flat_of[i * specs.size() + flat] = 1;
    std::printf("  %-10s %4.0f  %-54s %6.2f  %-54s %6.2f %6.2f %6.2f %6.2f %5.0f%% %5.2f %5.2f %+5.2f\n", arg["target"].c_str(), n,
                best ? best->params.c_str() : "-", best ? obj(*best, tg) : kNaN, f ? rs[flat].params.c_str() : "-",
                f ? sc[flat] : kNaN, f ? sm[flat] : kNaN, ssc / n, dsr / n, 100 * beat / n, up / n, dn / n, edge / n);

    std::printf("\n  滚动调参 (%s, 窗 %zu 日, 每 %zu 日重拟; 前 %zu 日空仓)\n", arg["target"].c_str(), Wb * R, R, Wb * R);
    print_head();
    const Roll &ro = rolls[i];
    print_row(ro.row);
    std::map<int, size_t> freq;
    size_t m = 0;
    for (const int p : ro.pick)
      if (p >= 0)
        ++freq[p], ++m;
    const auto most = std::max_element(freq.begin(), freq.end(), [](const auto &a, const auto &b) { return a.second < b.second; });
    const auto label = [&](int p) { return p < 0 ? std::string("-") : params_of(specs[static_cast<size_t>(p)]); };
    std::printf("    %zu 期选过 %zu 种 | 最常 %s (%.0f%%) | 当前 %s\n", m, freq.size(), most == freq.end() ? "-" : label(most->first).c_str(),
                m > 0 && most != freq.end() ? 100.0 * static_cast<double>(most->second) / static_cast<double>(m) : 0.0, label(ro.pick.back()).c_str());
  }

  const std::string out = dir + "/bench_" + arg["freq"] + ".tsv";
  std::FILE *f = std::fopen(out.c_str(), "w");
  assert(f && "bench.tsv: 写不了");
  std::fprintf(f, "set\tmodel\tparams\tplateau\tobj\tsr\tso\tcagr\tvol\tmdd\tsr1\tsr2\tm_dsr\tbeat");
  for (const Metric &x : kStat)
    std::fprintf(f, "\tm_%s", x.key);
  std::fputc('\n', f);
  std::vector<std::pair<const Row *, int>> all;
  for (size_t i = 0; i < us.size(); ++i) {
    all.emplace_back(&holds[i], 0);
    for (size_t k = i * specs.size(); k < (i + 1) * specs.size(); ++k)
      all.emplace_back(&rows[k], flat_of[k]);
    all.emplace_back(&rolls[i].row, 0);
  }
  for (const auto &[p, flat] : all) {
    const Row &r = *p;
    std::fprintf(f, "%s\t%s\t%s\t%d\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f", r.set.c_str(), r.model.c_str(), r.params.c_str(), flat,
                 obj(r, tg), r.port.sharpe, r.port.sortino, r.port.cagr, r.port.vol, r.port.mdd, r.sr1, r.sr2, r.m_dsr, r.beat);
    for (const Metric &x : kStat)
      std::fprintf(f, "\t%.4f", r.med.*x.v);
    std::fputc('\n', f);
  }
  std::fclose(f);
  std::printf("\n全部 %zu 行 → %s\n", all.size(), out.c_str());
}
