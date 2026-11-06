// cta_trace — 分钟线单配置交互轨迹 (后端): 浏览器里选资产 (idx / ind / concept) / band 参数, 每次现算一个配置的全期
//   用法: cta_trace [端口 8766]; 计算全在这里 (模型 + 回测 + 指标 + 视窗聚合), 前端 cta/web 只画
//   接口: GET /api/meta → 全部资产 (名 / 码 / 测试集) + 参数轴 (网格取值) + 每日根数
//         GET /api/run?a=资产&p=参数 (逗号分隔, 轴序)&cost=单边费率&vol=目标年化波动&buffer=不交易带
//                     &lo=&hi=视窗 [lo, hi) (有效根下标, hi = 0 即到末根)&px=视窗最多几点
//           → 全期指标 (策略 / 持有) + 视窗逐点 (每 step 根取末根: 收 / 上线 / 下线 / 吊灯线 / 仓位 / 全期净值 / 持有净值)
//   配置不变只换视窗时不重算; 视窗根数 ≤ px 时 step = 1 (逐根)
#include "Http.hpp"
#include "Metrics.hpp"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string>
#include <type_traits>
#include <vector>

namespace {

// 当前资产 + 当前配置的全期结果
struct State {
  size_t a = SIZE_MAX;
  Series se;
  std::string key;
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
  o += "],\"axes\":[";
  for (size_t a = 0; a < std::size(kAxes); ++a) {
    const Axis &x = kAxes[a];
    o += a ? ",{" : "{";
    o += "\"name\":", str(o, x.name), o += ',';
    arr(o, "values", x.v.size(), [&](size_t i) { return x.v[i]; });
    o += ",\"labels\":[";
    for (size_t i = 0; i < x.label.size(); ++i)
      o += i ? "," : "", str(o, x.label[i]);
    o += "]}";
  }
  return o + "]}";
}

std::vector<float> floats(const std::string &s) {
  std::vector<float> v;
  std::stringstream ss(s);
  for (std::string x; std::getline(ss, x, ',');)
    v.push_back(std::stof(x));
  return v;
}

void compute(State &s, const Panel &panel, const Query &q) {
  std::string key;
  for (const char *k : {"a", "p", "cost", "vol", "buffer"})
    key += q.at(k), key += '|';
  if (key == s.key)
    return;
  const size_t a = std::stoul(q.at("a"));
  assert(a < panel.B && "资产下标");
  if (a != s.a) {
    s.se = series_of(panel, a), s.a = a;
    assert(s.se.c.size() > 1 && "资产无数据");
  }
  const P p = floats(q.at("p"));
  assert(p.size() == std::size(kAxes) && "参数个数与参数轴不符");

  Setup u;
  u.cost = std::stod(q.at("cost")), u.vol = std::stod(q.at("vol")), u.buffer = std::stod(q.at("buffer"));
  u.bpy = panel.bars_per_year;
  Tape t(s.se);
  Sig g = rule(t, to_bars(p, static_cast<double>(panel.S)));
  s.rs = simulate(t, std::move(g.w), u);
  s.st = stat_of(t, s.rs, u), s.sh = stat_of(t, simulate(t, hold(t), u), u);
  s.up = std::move(g.up), s.dn = std::move(g.dn), s.stop = std::move(g.stop);
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
  o += ",\"v\":{";
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
