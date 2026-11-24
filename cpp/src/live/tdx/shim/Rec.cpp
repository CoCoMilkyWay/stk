// shim rec — 录制 (只读, 不投递任何输入): 每 kGap 取一次 spy 消息 (WM_COMMAND + 发给非标准控件的私有消息)、窗口树、绘制层,
//   与上一轮比对, 变了的逐条落 output/run/live/tdx/rec/<时刻>.jsonl ({t 距开录秒数, kind, ...}), 同时各一行打到 stdout.
//   录制期间独占 spy (每轮 dump 后 clear). 请求层另用 shim vt.
//   种类: start / tree (开录时全树, 只落盘) / msg (相邻同样的合并, n 为条数) /
//   new / del / chg (窗口出现、消失、属性变化) / gdi (可见窗口绘制文字) / stop.
//   t 是取到的那一轮的时刻, 同一轮内只有 msg 之间保序
#include "live/tdx/shim/Tool.hpp"

#include "disk/Disk.hpp"
#include "live/Base.hpp"
#include "live/tdx/Shim.hpp"
#include "live/tdx/Win.hpp"

#include <chrono>
#include <cmath>
#include <cstdio>
#include <map>
#include <thread>
#include <unordered_map>

namespace live::tdx::tool {

namespace {

using Clock = std::chrono::steady_clock;
using nlohmann::json;
using ojson = nlohmann::ordered_json;
using namespace std::chrono_literals;

constexpr auto kGap = 250ms; // 一轮的间隔
constexpr const char *kKeys[] = {"vis", "en", "title", "rect", "style"};

ojson oj(const json &j) { return ojson::parse(j.dump()); }

template <class J>
bool truthy(const J &j) {
  return !(j.is_null() || (j.is_boolean() && !j.template get<bool>()) || (j.is_number() && j.template get<double>() == 0) ||
           (j.is_string() && j.template get<std::string>().empty()));
}

std::string plain(const ojson &j) { return j.is_string() ? j.get<std::string>() : j.dump(); }

// ---------- 一条记录一行 ----------

std::string win(const ojson &n) {
  const ojson &t = n.at("title");
  return plain(n.at("hwnd")) + " " + plain(n.at("class")) + "#" + plain(n.at("id")) + "「" + (t.is_string() ? win::squash(t.get<std::string>()) : "") + "」";
}
std::string hidden(const ojson &vis) { return truthy(vis) ? "" : " (隐藏)"; }

std::string body(const std::string &kind, const ojson &r) {
  if (kind == "msg")
    return "msg " + plain(r.at("msg")) + " → " + win(r) + " wp=" + plain(r.at("wparam")) + " lp=" + plain(r.at("lparam")) +
           (r.at("n").get<long>() > 1 ? strf(" ×%ld", r.at("n").get<long>()) : "") + hidden(r.at("vis"));
  if (kind == "new" || kind == "del")
    return kind + " " + win(r.at("node")) + hidden(r.at("node").at("vis"));
  if (kind == "chg") {
    std::string s = "chg " + win(r.at("node")) + " ";
    bool first = true;
    for (const auto &e : r.at("diff").items()) {
      s += (first ? "" : "; ") + e.key() + ": " + e.value().at(0).dump() + " → " + e.value().at(1).dump();
      first = false;
    }
    return s;
  }
  if (kind == "gdi") {
    std::string s;
    for (const ojson &t : r.at("texts"))
      s += (s.empty() ? "" : " | ") + t.get<std::string>();
    return "gdi " + plain(r.at("hwnd")) + " " + plain(r.at("cls")) + "#" + plain(r.at("id")) + " | " + win::squash(s);
  }
  ojson rest = r;
  rest.erase("t");
  rest.erase("kind");
  return kind + " " + rest.dump();
}

// ---------- 落盘 ----------

struct Out {
  std::string stamp;
  Clock::time_point t0 = Clock::now();
  long n = 0;
  void put(const char *kind, const ojson &rest = ojson::object()) {
    ojson r = {{"t", std::round(std::chrono::duration<double>(Clock::now() - t0).count() * 1000) / 1000}, {"kind", kind}};
    for (const auto &e : rest.items())
      r[e.key()] = e.value();
    disk::append(disk::RunLiveTdxRecLog, {.stamp = stamp}, r.dump());
    ++n;
    if (std::string_view(kind) != "tree")
      std::printf("%8.2f %s\n", r["t"].get<double>(), body(kind, r).c_str());
  }
};

// 一轮的窗口树: 先序, hwnd → 记录
struct Snap {
  std::vector<json> nodes;
  std::unordered_map<std::string, size_t> at;
  const json *get(const std::string &h) const {
    const auto i = at.find(h);
    return i == at.end() ? nullptr : &nodes[i->second];
  }
};
Snap snap(std::vector<json> recs) {
  Snap s{std::move(recs), {}};
  for (size_t i = 0; i < s.nodes.size(); ++i)
    s.at.emplace(s.nodes[i].at("hwnd").get<std::string>(), i);
  return s;
}

ojson brief(const json &n) {
  ojson b = ojson::object();
  for (const char *k : {"hwnd", "depth", "id", "class", "title", "rect", "vis", "en"})
    b[k] = oj(n.at(k));
  return b;
}

std::vector<std::string> texts(const json &layer) {
  std::vector<std::string> out;
  for (const json &it : layer.at("items"))
    if (it.size() > 5 && !win::squash(it[5].get<std::string>()).empty())
      out.push_back(it[5].get<std::string>());
  return out;
}

void loop(Out &o, double secs) {
  o.put("start", {{"file", disk::path(disk::RunLiveTdxRecLog, {.stamp = o.stamp}).string()},
                  {"pid", oj(shim::cmd("ping").at(0).at("pid"))},
                  {"gap", std::chrono::duration<double>(kGap).count()}});
  shim::cmd("spy", "on", "cmd");
  shim::cmd("spy", "clear");
  Snap prev = snap(shim::cmd("tree"));
  ojson nodes = ojson::array();
  for (const json &n : prev.nodes)
    nodes.push_back(brief(n));
  o.put("tree", {{"nodes", std::move(nodes)}});
  std::map<std::string, std::vector<std::string>> gdi;
  const Clock::time_point end = o.t0 + std::chrono::duration_cast<Clock::duration>(std::chrono::duration<double>(secs));
  while (Clock::now() < end) {
    std::this_thread::sleep_for(kGap);
    const std::vector<json> ev = shim::cmd("spy", "dump");
    shim::cmd("spy", "clear");
    const Snap cur = snap(shim::cmd("tree"));
    for (size_t i = 0, j; i < ev.size(); i = j) {
      for (j = i + 1; j < ev.size() && ev[j] == ev[i]; ++j) {
      }
      const json &e = ev[i];
      const std::string h = e.at("hwnd").get<std::string>();
      const json *w = cur.get(h) ? cur.get(h) : prev.get(h);
      ojson r = ojson::object();
      for (const char *k : {"msg", "hwnd", "class", "wparam", "lparam"})
        r[k] = oj(e.at(k));
      r["n"] = j - i;
      for (const char *k : {"id", "title", "vis"})
        r[k] = w ? oj(w->at(k)) : ojson(nullptr);
      o.put("msg", r);
    }
    for (const json &n : cur.nodes) {
      const json *p = prev.get(n.at("hwnd").get<std::string>());
      if (!p) {
        o.put("new", {{"node", brief(n)}});
        continue;
      }
      ojson d = ojson::object();
      for (const char *k : kKeys)
        if (p->at(k) != n.at(k))
          d[k] = ojson::array({oj(p->at(k)), oj(n.at(k))});
      if (!d.empty() && (truthy(n.at("vis")) || truthy(p->at("vis"))))
        o.put("chg", {{"node", brief(n)}, {"diff", std::move(d)}});
    }
    for (const json &n : prev.nodes)
      if (!cur.get(n.at("hwnd").get<std::string>()))
        o.put("del", {{"node", brief(n)}});
    std::vector<std::pair<std::string, std::vector<std::string>>> now;
    for (const json &l : shim::cmd("gdidump")) {
      const std::string h = l.at("hwnd").get<std::string>();
      if (const json *n = cur.get(h); n && truthy(n->at("vis")))
        now.emplace_back(h, texts(l));
    }
    for (const auto &[h, tx] : now)
      if (const auto g = gdi.find(h); g == gdi.end() || g->second != tx) {
        const json &n = *cur.get(h);
        o.put("gdi", {{"hwnd", h}, {"cls", oj(n.at("class"))}, {"id", oj(n.at("id"))}, {"texts", tx}});
      }
    prev = cur;
    gdi = std::map<std::string, std::vector<std::string>>(now.begin(), now.end());
  }
}

} // namespace

int rec(double secs) {
  Out o{disk::stamp_now()};
  loop(o, secs);
  shim::cmd("spy", "off");
  o.put("stop");
  std::fprintf(stderr, "%ld 条 → %s\n", o.n, disk::path(disk::RunLiveTdxRecLog, {.stamp = o.stamp}).string().c_str());
  return 0;
}

} // namespace live::tdx::tool
