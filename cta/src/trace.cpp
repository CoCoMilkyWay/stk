// cta_trace — 分钟线交互轨迹 (后端): 浏览器里选资产 (idx / ind / concept) / 各组部件 (中线 / 带 / 分层) 的模型 + 优化目标, 参数由本资产滚动调参选出
//   用法: cta_trace [端口 8766]; 计算全在这里 (调参 + 模型 + 回测 + 指标 + 视窗聚合), 前端 cta/web 只画
//   滚动调参 (只用本资产, 不用未来信息): 所选模型的参数网格逐配置回测 → 每日以前 W 日各配置目标值拟岭回归曲面 (Tune.hpp),
//     最高点配置用到当日 (前 W 日 / 窗内根数不足满窗一半 = 空仓); 选中过的配置各算全期 (状态常热), 逐根取当日选中者的仓位与线, 切换计成本
//   接口: GET /api/meta → 全部资产 (名 / 码 / 测试集) + 部件表 (各组模型 + 其参数轴, 网格取值) + 每日根数
//         GET /api/run?a=资产&c=中线模型&w=带模型&m=分层模型&target=sharpe|sortino|band&roll=W&cost=单边费率&vol=目标年化波动&buffer=不交易带
//                     &lo=&hi=视窗 [lo, hi) (有效根下标, hi = 0 即到末根)&px=视窗最多几点
//           → 全期指标 (策略 / 持有) + 选参段 (起根 / 起日 / 各组参数, 轴单位; 空仓 = null)
//             + 视窗逐点 (每 step 根取末根: 收 / 上线 / 下线 / 吊灯线 / 仓位 / 全期净值 / 持有净值)
//   配置不变只换视窗时不重算; 视窗根数 ≤ px 时 step = 1 (逐根)
#include "Http.hpp"
#include "Metrics.hpp"
#include "Tune.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace {

// 当前资产 + 当前模型组合 / 优化目标的全期结果
struct State {
  size_t a = SIZE_MAX;
  Series se;
  std::string key;
  std::vector<Spec> specs;                 // 所选模型的参数网格
  std::vector<std::pair<size_t, int>> seg; // 选参段: (起根, specs 下标; −1 = 空仓), 起根升序
  Run rs;
  Stat st, sh;
  Col up, dn, stop;
  std::vector<double> nav;
};

// ---- JSON 拼接 ----

void num(std::string &o, double v) {
  if (!std::isfinite(v)) {
    o += "null";
    return;
  }
  char b[32];
  std::snprintf(b, sizeof(b), "%.7g", v);
  o += b;
}

void str(std::string &o, const std::string &s) {
  o += '"';
  for (const char ch : s) {
    if (ch == '"' || ch == '\\')
      o += '\\';
    o += ch;
  }
  o += '"';
}

// f 返回整数 (下标 / 时刻) 原样写, 浮点走 num
template <class F>
void arr(std::string &o, const char *key, size_t n, F f) {
  o += '"', o += key, o += "\":[";
  for (size_t i = 0; i < n; ++i) {
    if (i)
      o += ',';
    if constexpr (std::is_integral_v<decltype(f(i))>)
      o += std::to_string(f(i));
    else
      num(o, f(i));
  }
  o += ']';
}

void stat(std::string &o, const char *key, const Stat &s) {
  o += '"', o += key, o += "\":{";
  for (size_t i = 0; i < std::size(kStat); ++i) {
    if (i)
      o += ',';
    o += '"', o += kStat[i].key, o += "\":";
    num(o, s.*kStat[i].v);
  }
  o += '}';
}

std::string meta(const Panel &panel) {
  std::string o = "{\"bpd\":" + std::to_string(panel.S) + ",\"assets\":[";
  for (size_t b = 0; b < panel.B; ++b) {
    const Asset &a = panel.assets[b];
    o += b ? ",{" : "{";
    o += "\"name\":", str(o, a.name), o += ",\"code\":", str(o, a.code), o += ",\"set\":", str(o, a.set), o += '}';
  }
  o += "],\"parts\":[";
  for (size_t j = 0; j < kNP; ++j) {
    const Part &pt = kParts[j];
    o += j ? ",{" : "{";
    o += "\"name\":", str(o, pt.name), o += ",\"title\":", str(o, pt.title), o += ",\"kinds\":[";
    for (size_t k = 0; k < pt.kinds.size(); ++k) {
      const Kind &K = pt.kinds[k];
      o += k ? ",{" : "{";
      o += "\"name\":", str(o, K.name), o += ",\"axes\":[";
      for (size_t a = 0; a < K.axes.size(); ++a) {
        const Axis &x = K.axes[a];
        o += a ? ",{" : "{";
        o += "\"name\":", str(o, x.name), o += ",\"day\":", o += x.day ? "true," : "false,";
        arr(o, "values", x.v.size(), [&](size_t i) { return x.v[i]; });
        o += '}';
      }
      o += "]}";
    }
    o += "]}";
  }
  return o + "]}";
}

void compute(State &s, const Panel &panel, const Query &q) {
  std::string key;
  for (const char *k : {"a", "cost", "vol", "buffer", "target", "roll"})
    key += q.at(k), key += '|';
  for (const Part &pt : kParts)
    key += q.at(pt.name), key += '|';
  if (key == s.key)
    return;
  const size_t a = std::stoul(q.at("a"));
  assert(a < panel.B && "资产下标");
  if (a != s.a) {
    s.se = series_of(panel, a), s.a = a;
    assert(s.se.c.size() > 1 && "资产无数据");
  }
  std::array<size_t, kNP> kind{};
  for (size_t j = 0; j < kNP; ++j) {
    const Part &pt = kParts[j];
    const auto k = std::find_if(pt.kinds.begin(), pt.kinds.end(), [&](const Kind &K) { return q.at(pt.name) == K.name; });
    assert(k != pt.kinds.end() && "未知部件模型");
    kind[j] = static_cast<size_t>(k - pt.kinds.begin());
  }
  const Target tg = target_of(q.at("target"));
  const size_t W = std::stoul(q.at("roll"));
  assert(W >= kMinRoll && "roll 须 ≥ 一年");

  Setup u;
  u.cost = std::stod(q.at("cost")), u.vol = std::stod(q.at("vol")), u.buffer = std::stod(q.at("buffer"));
  u.bpy = panel.bars_per_year;
  const Series &se = s.se;
  const size_t n = se.c.size(), S = panel.S, d0 = se.t[0] / S;
  const auto day = [&](size_t i) { return se.t[i] / S - d0; }; // 自首根所在日起
  const size_t nd = day(n - 1) + 1;
  assert(nd > W && "样本短于滚动窗");
  std::vector<double> cnt(nd, 0); // 根数前缀和
  for (size_t i = 0; i < n; ++i)
    cnt[day(i)] += 1;
  for (size_t d = 1; d < nd; ++d)
    cnt[d] += cnt[d - 1];

  s.specs = specs_of(static_cast<double>(S), kind);
  assert(!s.specs.empty() && "所选模型无可用网格");
  const Ridge rg = ridge_of(s.specs);
  Tape t(se);
  std::vector<std::vector<double>> acc; // [spec] 日前缀和
  for (const Spec &x : s.specs) {
    const Sig g = band(t, x.g);
    acc.push_back(day_sums(terms(tg, t, g, simulate(t, g.w, u), u.cost), nd, day));
  }
  std::vector<int> pick(nd, -1); // [日] → specs 下标
  const double min_n = static_cast<double>(W * S) / 2;
  std::vector<double> y(s.specs.size());
  for (size_t d = W; d < nd; ++d) {
    const auto win = [&](const std::vector<double> &p, size_t k) { return p[k + 2 * (d - 1)] - (d > W ? p[k + 2 * (d - W - 1)] : 0); };
    const double m = cnt[d - 1] - (d > W ? cnt[d - W - 1] : 0);
    if (m < min_n)
      continue;
    for (size_t x = 0; x < y.size(); ++x)
      y[x] = score(tg, win(acc[x], 0), win(acc[x], 1), m);
    const std::vector<double> f = rg.fit(y);
    pick[d] = static_cast<int>(std::max_element(f.begin(), f.end()) - f.begin());
  }

  Col w(n, 0);
  s.up.assign(n, kNaN), s.dn.assign(n, kNaN), s.stop.assign(n, kNaN);
  std::vector<char> used(s.specs.size(), 0);
  for (const int p : pick)
    if (p >= 0)
      used[static_cast<size_t>(p)] = 1;
  for (size_t x = 0; x < s.specs.size(); ++x) {
    if (!used[x])
      continue;
    const Sig g = band(t, s.specs[x].g);
    for (size_t i = 0; i < n; ++i)
      if (pick[day(i)] == static_cast<int>(x))
        w[i] = g.w[i], s.up[i] = g.up[i], s.dn[i] = g.dn[i], s.stop[i] = g.stop[i];
  }
  s.seg.clear();
  for (size_t i = 0; i < n; ++i)
    if (const int p = pick[day(i)]; s.seg.empty() || s.seg.back().second != p)
      s.seg.emplace_back(i, p);
  s.rs = simulate(t, std::move(w), u);
  s.st = stat_of(t, s.rs, u), s.sh = stat_of(t, simulate(t, hold(t), u), u);
  s.nav.resize(s.rs.r.size());
  double nav = 1;
  for (size_t i = 0; i < s.nav.size(); ++i)
    s.nav[i] = nav *= 1 + s.rs.r[i];
  s.key = key;
}

// 视窗 [lo, hi) 按 step 根一点 (起点对齐到 step 的倍数), 每点取点内末根
std::string run(State &s, const Panel &panel, const Query &q) {
  compute(s, panel, q);
  const Series &se = s.se;
  const size_t n = se.c.size(), px = std::stoul(q.at("px"));
  assert(px > 0 && "px");
  size_t lo = std::min<size_t>(std::stoul(q.at("lo")), n - 1), hi = std::stoul(q.at("hi"));
  hi = hi == 0 || hi > n ? n : std::max(hi, lo + 1);
  const size_t step = (hi - lo + px - 1) / px;
  lo -= lo % step;
  const size_t nb = (hi - lo + step - 1) / step;
  const auto beg = [&](size_t k) { return lo + k * step; };
  const auto last = [&](size_t k) { return std::min(hi, lo + (k + 1) * step) - 1; };
  const auto time = [&](size_t i) { return panel.time[se.t[i]]; };
  const auto held = [&](size_t i) { return se.c[i] / se.c[0]; };

  std::string o = "{\"n\":" + std::to_string(n) + ",\"lo\":" + std::to_string(lo) + ",\"hi\":" + std::to_string(hi) + ",\"step\":" + std::to_string(step);
  o += ",\"first\":" + std::to_string(ymd_of(time(0))) + ",\"last\":" + std::to_string(ymd_of(time(n - 1))) + ',';
  stat(o, "stat", s.st), o += ',', stat(o, "hold", s.sh);
  o += ",\"seg\":[";
  for (size_t x = 0; x < s.seg.size(); ++x) {
    const auto [i, p] = s.seg[x];
    o += x ? ",{" : "{";
    o += "\"i\":" + std::to_string(i) + ",\"t\":" + std::to_string(ymd_of(time(i))) + ",\"p\":";
    if (p < 0) {
      o += "null}";
      continue;
    }
    const Spec &sp = s.specs[static_cast<size_t>(p)];
    for (size_t j = 0, at = 0; j < kNP; ++j) {
      const Kind &K = kParts[j].kinds[sp.g.k[j]];
      o += j ? ",[" : "[[";
      for (size_t e = 0; e < K.axes.size(); ++e, ++at)
        o += e ? "," : "", num(o, K.axes[e].v[sp.at[at]]);
      o += ']';
    }
    o += "]}";
  }
  o += "],\"v\":{";
  arr(o, "i", nb, beg), o += ',';
  arr(o, "t", nb, [&](size_t k) { return time(last(k)); }), o += ',';
  arr(o, "c", nb, [&](size_t k) { return se.c[last(k)]; }), o += ',';
  arr(o, "up", nb, [&](size_t k) { return s.up[last(k)]; }), o += ',';
  arr(o, "dn", nb, [&](size_t k) { return s.dn[last(k)]; }), o += ',';
  arr(o, "stop", nb, [&](size_t k) { return s.stop[last(k)]; }), o += ',';
  arr(o, "w", nb, [&](size_t k) { return s.rs.w[last(k)]; }), o += ',';
  arr(o, "nav", nb, [&](size_t k) { return s.nav[last(k)]; }), o += ',';
  arr(o, "hold", nb, [&](size_t k) { return held(last(k)); });
  o += ",\"w0\":", num(o, lo > 0 ? s.rs.w[lo - 1] : 0);
  return o + "}}";
}

Reply file(const std::string &path) {
  const std::string name = path == "/" ? "index.html" : path.substr(1);
  if (name.find('/') != std::string::npos || name.find("..") != std::string::npos)
    return {"text/plain", "无此文件", 404};
  std::ifstream f(std::string(CTA_DIR) + "/web/" + name, std::ios::binary);
  if (!f)
    return {"text/plain", "无此文件", 404};
  std::stringstream ss;
  ss << f.rdbuf();
  const std::string ext = name.substr(name.rfind('.') + 1);
  return {ext == "html" ? "text/html; charset=utf-8" : ext == "js" ? "text/javascript; charset=utf-8"
                                                                   : "text/css; charset=utf-8",
          ss.str()};
}

} // namespace

int main(int argc, char **argv) {
  const int port = argc > 1 ? std::stoi(argv[1]) : 8766;
  const std::string root = std::string(CTA_DIR) + "/..", dir = std::string(CTA_DIR) + "/data";
  const Lib lib = open_lib(root + "/output/features/all/bar");
  const Table table = make_table(lib, root + "/output/concept");
  sync(lib, table, dir);
  const Panel panel = open_panel(dir, table, "1m");

  const std::string meta_json = meta(panel);
  State st;
  serve(
      port,
      [&](const std::string &path, const Query &q) -> Reply {
        if (path == "/api/meta")
          return {"application/json; charset=utf-8", meta_json};
        if (path == "/api/run")
          return {"application/json; charset=utf-8", run(st, panel, q)};
        return file(path);
      },
      [&] {
        const std::string url = "http://127.0.0.1:" + std::to_string(port);
        std::printf("[*] 轨迹: %s  (Ctrl+C 退出)\n", url.c_str());
        std::fflush(stdout);
        [[maybe_unused]] const int rc = std::system(("xdg-open " + url + " >/dev/null 2>&1 &").c_str());
      });
}
