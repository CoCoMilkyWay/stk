// cta_trace — 分钟线单配置交互轨迹 (后端): 启动随机挑 kPick 只资产, 浏览器里选资产 / 模型 / 参数 / 窗口, 每次现算一个配置
//   用法: cta_trace [端口 8766]; 计算全在这里 (模型 + 回测 + 指标 + 下采样), 前端 cta/web 只画
//   接口: GET /api/meta → 资产 (名 / 测试集 / 首末日) + 模型表 (参数轴 + 网格取值) + 每日根数
//         GET /api/run?a=资产&kind=模型&p=参数 (逗号分隔, 轴序)&trail=吊灯 k (0 = 不叠)&cost=单边费率&stop=close|intra&vol=目标年化波动&buffer=不交易带
//                     &from=YYYYMMDD&days=窗口日数
//           → 全期指标 (策略 / 持有) + 全期日线概览 (日末收 / 策略净值 / 持有净值 / 日均仓位) + 窗口逐根 (K 线 / band 中线与半宽 / 生效止损 / 仓位 / 窗内净值)
#include "Http.hpp"
#include "Kinds.hpp"
#include "Metrics.hpp"

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <type_traits>
#include <vector>

namespace {

constexpr size_t kPick = 10, kMinDays = 500; // 挑几只 / 有效根数下限 = 这么多日的根

// 逐根记下 band 中线 / 半宽 (非 Band = NaN) 与模型给的止损价
class Probe : public Model {
  Model &m_;
  const Band *b_;

public:
  std::vector<double> mid, half, stop;
  Probe(Model &m, const Band *b) : m_(m), b_(b) {}
  Order step(const Bar &b, float held) override {
    const Order o = m_.step(b, held);
    mid.push_back(b_ ? b_->mid() : kNaN), half.push_back(b_ ? b_->half() : kNaN), stop.push_back(o.stop > 0 ? o.stop : kNaN);
    return o;
  }
};

struct Pick {
  const Asset *asset;
  Series se;
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

// f 返回整数 (日期 / 时刻) 原样写, 浮点走 num
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
  const std::pair<const char *, double> kv[] = {{"sr", s.sharpe}, {"cagr", s.cagr}, {"vol", s.vol}, {"mdd", s.mdd}, {"expo", s.expo}, {"turn", s.turn}, {"cost", s.cost}, {"up", s.up}, {"dn", s.dn}, {"edge", s.edge}, {"trades", s.trades}, {"win", s.win}, {"payoff", s.payoff}, {"hold", s.hold}};
  for (size_t i = 0; i < std::size(kv); ++i) {
    if (i)
      o += ',';
    o += '"', o += kv[i].first, o += "\":";
    num(o, kv[i].second);
  }
  o += '}';
}

std::string meta(const std::vector<Pick> &picks, const Panel &panel) {
  std::string o = "{\"bpd\":" + std::to_string(panel.S) + ",\"assets\":[";
  for (size_t i = 0; i < picks.size(); ++i) {
    const Pick &p = picks[i];
    o += i ? ",{" : "{";
    o += "\"name\":", str(o, p.asset->name), o += ",\"code\":", str(o, p.asset->code), o += ",\"set\":", str(o, p.asset->set);
    o += ",\"first\":" + std::to_string(ymd_of(panel.time[p.se.t.front()])) + ",\"last\":" + std::to_string(ymd_of(panel.time[p.se.t.back()])) + "}";
  }
  o += "],\"kinds\":[";
  for (size_t k = 0; k < std::size(kKinds); ++k) {
    const Kind &K = kKinds[k];
    o += k ? ",{" : "{";
    o += "\"name\":", str(o, K.name), o += ",\"axes\":[";
    for (size_t a = 0; a < K.grid.size(); ++a) {
      const Axis &x = K.grid[a];
      o += a ? ",{" : "{";
      o += "\"name\":", str(o, x.name), o += ',';
      arr(o, "values", x.v.size(), [&](size_t i) { return x.v[i]; });
      o += ",\"labels\":[";
      for (size_t i = 0; i < x.label.size(); ++i)
        o += i ? "," : "", str(o, x.label[i]);
      o += "]}";
    }
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

std::string run(const std::vector<Pick> &picks, const Panel &panel, const Query &q) {
  const size_t a = std::stoul(q.at("a"));
  assert(a < picks.size() && "资产下标");
  const Series &se = picks[a].se;
  const auto kind = std::find_if(std::begin(kKinds), std::end(kKinds), [&](const Kind &K) { return q.at("kind") == K.name; });
  assert(kind != std::end(kKinds) && "未知模型");
  const P p = floats(q.at("p"));
  assert(p.size() == kind->grid.size() && "参数个数与参数轴不符");
  const float trail = std::stof(q.at("trail"));

  Setup u;
  u.cost = std::stod(q.at("cost")), u.vol = std::stod(q.at("vol")), u.buffer = std::stod(q.at("buffer"));
  assert((q.at("stop") == "close" || q.at("stop") == "intra") && "stop: close | intra");
  u.stop = q.at("stop") == "close" ? Stop::Close : Stop::Intra;
  u.bpy = panel.bars_per_year;

  std::unique_ptr<Model> base = kind->make(p, static_cast<double>(panel.S));
  const Band *band = dynamic_cast<const Band *>(base.get());
  const std::unique_ptr<Model> m = trail > 0 ? std::make_unique<Trail>(std::move(base), trail) : std::move(base);
  Probe pr(*m, band);
  const Run rs = simulate(se, pr, u);
  Hold hold;
  const Run rh = simulate(se, hold, u);
  const size_t n = se.bar.size();
  const auto day = [&](size_t i) { return ymd_of(panel.time[se.t[i]]); };

  // 全期日线概览: 每日末根的收 / 净值, 日均仓位
  std::vector<int> od;
  std::vector<double> oc, onav, ohold, oexpo;
  double nav = 1, wsum = 0, wn = 0;
  for (size_t i = 0; i < n; ++i) {
    nav *= 1 + rs.r[i], wsum += rs.w[i], wn += 1;
    if (i + 1 == n || day(i + 1) != day(i)) {
      od.push_back(day(i)), oc.push_back(se.bar[i].c), onav.push_back(nav), ohold.push_back(se.bar[i].c / se.bar[0].c), oexpo.push_back(wsum / wn);
      wsum = wn = 0;
    }
  }

  // 窗口: from 起 (含) 的 days 个交易日
  const int from = std::stoi(q.at("from"));
  const size_t days = std::stoul(q.at("days"));
  size_t i0 = 0;
  while (i0 + 1 < n && day(i0) < from)
    ++i0;
  size_t i1 = i0;
  for (size_t nd = 0; i1 < n; ++i1)
    if ((i1 == i0 || day(i1) != day(i1 - 1)) && ++nd > days)
      break;
  const size_t L = i1 - i0;
  std::vector<double> wnav(L);
  for (size_t k = 0; k < L; ++k)
    wnav[k] = k ? wnav[k - 1] * (1 + rs.r[i0 + k]) : 1;

  std::string o = "{";
  stat(o, "stat", stat_of(se, rs, u)), o += ',', stat(o, "hold", stat_of(se, rh, u));
  o += ",\"over\":{";
  arr(o, "t", od.size(), [&](size_t i) { return od[i]; }), o += ',';
  arr(o, "c", oc.size(), [&](size_t i) { return oc[i]; }), o += ',';
  arr(o, "nav", onav.size(), [&](size_t i) { return onav[i]; }), o += ',';
  arr(o, "hold", ohold.size(), [&](size_t i) { return ohold[i]; }), o += ',';
  arr(o, "expo", oexpo.size(), [&](size_t i) { return oexpo[i]; });
  o += "},\"win\":{";
  arr(o, "t", L, [&](size_t k) { return panel.time[se.t[i0 + k]]; }), o += ',';
  arr(o, "o", L, [&](size_t k) { return se.bar[i0 + k].o; }), o += ',';
  arr(o, "h", L, [&](size_t k) { return se.bar[i0 + k].h; }), o += ',';
  arr(o, "l", L, [&](size_t k) { return se.bar[i0 + k].l; }), o += ',';
  arr(o, "c", L, [&](size_t k) { return se.bar[i0 + k].c; }), o += ',';
  arr(o, "mid", L, [&](size_t k) { return pr.mid[i0 + k]; }), o += ',';
  arr(o, "half", L, [&](size_t k) { return pr.half[i0 + k]; }), o += ',';
  // 本根生效的止损 = 上根收盘出单时给的 (上根持仓才有)
  arr(o, "stop", L, [&](size_t k) { const size_t i = i0 + k; return i > 0 && rs.w[i - 1] > 0 ? pr.stop[i - 1] : kNaN; }), o += ',';
  arr(o, "w", L, [&](size_t k) { return rs.w[i0 + k]; }), o += ',';
  arr(o, "nav", L, [&](size_t k) { return wnav[k]; });
  o += ",\"w0\":", num(o, i0 > 0 ? rs.w[i0 - 1] : 0);
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

  std::vector<size_t> pool(panel.B);
  for (size_t b = 0; b < panel.B; ++b)
    pool[b] = b;
  std::shuffle(pool.begin(), pool.end(), std::mt19937(std::random_device{}()));
  std::vector<Pick> picks;
  for (const size_t b : pool) {
    if (picks.size() == kPick)
      break;
    Series se = series_of(panel, b);
    if (se.bar.size() >= kMinDays * panel.S)
      picks.push_back({&panel.assets[b], std::move(se)});
    std::fprintf(stderr, "\r  载入 %zu / %zu   ", picks.size(), kPick);
  }
  std::fputc('\n', stderr);
  assert(!picks.empty() && "没有够长的资产");

  const std::string meta_json = meta(picks, panel);
  serve(
      port,
      [&](const std::string &path, const Query &q) -> Reply {
        if (path == "/api/meta")
          return {"application/json; charset=utf-8", meta_json};
        if (path == "/api/run")
          return {"application/json; charset=utf-8", run(picks, panel, q)};
        return file(path);
      },
      [&] {
        const std::string url = "http://127.0.0.1:" + std::to_string(port);
        std::printf("[*] 轨迹: %s  (Ctrl+C 退出)\n", url.c_str());
        std::fflush(stdout);
        [[maybe_unused]] const int rc = std::system(("xdg-open " + url + " >/dev/null 2>&1 &").c_str());
      });
}
