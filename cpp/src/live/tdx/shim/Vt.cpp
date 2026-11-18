// shim vt — 虚表追踪 (只记录, 不改参数与返回): 给目标类的虚表换桩 (shim 命令 vt), 录 secs 秒内经它的每次调用, 卸桩.
//   目标: <RTTI 类名 | 0x虚表址>[=槽,槽…]; 类名的嵌套类写 内@外 (如 CTcJob@CTcComm), 一类可有几张主虚表, 全钩.
//   = 后点名的槽另记返回 (换栈上返回址, 有异常越过被钩函数的风险, 故只给要看结果的槽); 其余只记进入.
//   每次调用一行 (按进入的先后): {t 距开录秒数, tid, this, cls, slot, args, in / out (参数所指头几字节, GBK 解码,
//   进入时 / 返回时, 不可打印的空), eax, from (返回址的模块+偏移)}; 记返回的槽 args 恰为自弹的参数个数, 其余列栈上 4 个;
//   不记返回的 eax / out 为空.
//   落 output/run/live/tdx/rec/<时刻>.jsonl ({kind: "vt", ...}), 同时各一行打到 stdout (同槽同调用处连续的读调用合成一行 ×N).
//   自弹字节 = 函数 ret N 的 N (thiscall / stdcall 的参数字节); 只经虚表的调用过桩, 直接调用不过.
#include "live/tdx/Image.hpp"
#include "live/tdx/shim/Tool.hpp"

#include "disk/Disk.hpp"
#include "live/tdx/Shim.hpp"

#include <chrono>
#include <cstdio>
#include <map>
#include <thread>

namespace live::tdx::tool {

namespace {

using Clock = std::chrono::steady_clock;
using nlohmann::json;
using ojson = nlohmann::ordered_json;
using namespace std::chrono_literals;

constexpr auto kGap = 200ms; // 取一次的间隔 (shim 的事件环 65536 条, 一轮查询约 1.5 万条)

uint32_t hex(const json &j) { return static_cast<uint32_t>(std::stoul(j.get<std::string>(), nullptr, 16)); }

// 参数所指头几字节 (十六进制) → 可打印的 GBK 文本; 含控制字符或解不出的回空
std::string text(const std::string &h) {
  std::string raw;
  for (size_t i = 0; i + 1 < h.size(); i += 2)
    raw += static_cast<char>(std::stoi(h.substr(i, 2), nullptr, 16));
  for (const char c : raw)
    if (static_cast<unsigned char>(c) < 0x20)
      return "";
  const std::string u = mem::gbk(raw);
  return u.find("\xEF\xBF\xBD") == std::string::npos ? u : "";
}

struct Call {
  json in;                 // 进入那条
  std::optional<json> out; // 返回那条
};

} // namespace

int vt(double secs, const std::vector<std::string> &targets) {
  need(!targets.empty(), "用法: shim vt <秒> <类名 | 0x虚表>[=槽,槽…]…");
  const Image img(mem::attach());
  std::map<uint32_t, std::string> cls, rets; // 虚表 → 类名 / 记返回的槽
  for (const std::string &arg : targets) {
    const size_t eq = arg.find('=');
    const std::string t = arg.substr(0, eq), ret = eq == std::string::npos ? "" : arg.substr(eq + 1);
    std::vector<mem::Addr> vs;
    if (t.starts_with("0x"))
      vs = {std::stoull(t, nullptr, 16)};
    else
      vs = img.vtables(t);
    need(!vs.empty(), "没找到类 " + t + " 的虚表");
    for (const mem::Addr v : vs) {
      cls[static_cast<uint32_t>(v)] = t.starts_with("0x") ? img.rtti(v).value_or(t) : t;
      rets[static_cast<uint32_t>(v)] = ret;
    }
  }
  for (const auto &[v, c] : cls) {
    const json r = shim::cmd("vt", "hook", strf("0x%08x", v), 256).at(0);
    if (!rets.at(v).empty())
      shim::cmd("vt", "ret", strf("0x%08x", v), rets.at(v));
    std::fprintf(stderr, "钩 %s 0x%08x (%s) %d 槽, 记返回: %s\n", c.c_str(), v, img.at(v).c_str(), r.at("hooked").get<int>(),
                 rets.at(v).empty() ? "无" : rets.at(v).c_str());
  }
  shim::cmd("vt", "clear");
  shim::cmd("vt", "on");
  std::vector<json> evs;
  const Clock::time_point t0 = Clock::now(), end = t0 + std::chrono::duration_cast<Clock::duration>(std::chrono::duration<double>(secs));
  const auto drain = [&] {
    for (json &e : shim::cmd_wait(30, "vt", "drain"))
      if (e.contains("id"))
        evs.push_back(std::move(e));
      else if (const int lost = e.at("lost").get<int>(); lost > 0)
        std::fprintf(stderr, "事件环满, 挤掉 %d 条\n", lost);
  };
  while (Clock::now() < end) {
    std::this_thread::sleep_for(kGap);
    drain();
  }
  shim::cmd("vt", "off");
  drain();
  shim::cmd("vt", "unhook");

  std::map<uint32_t, Call> calls; // id → 调用 (id 按进入递增)
  for (json &e : evs) {
    Call &c = calls[e.at("id").get<uint32_t>()];
    if (e.at("kind") == "call")
      c.in = std::move(e);
    else
      c.out = std::move(e);
  }
  const std::string stamp = disk::stamp_now();
  const uint32_t tick0 = calls.empty() || calls.begin()->second.in.is_null() ? 0 : calls.begin()->second.in.at("tick").get<uint32_t>();
  std::string prev, line;
  long rep = 0;
  const auto flush = [&] {
    if (!line.empty())
      std::printf("%s%s\n", line.c_str(), rep ? strf("  ×%ld", rep + 1).c_str() : "");
  };
  for (const auto &[id, c] : calls) {
    if (c.in.is_null())
      continue; // 开录前进入的调用, 只录到返回
    const json &in = c.in;
    const uint32_t v = hex(in.at("vt"));
    const long slot = in.at("slot").get<long>(), pop = c.out ? c.out->at("pop").get<long>() : 16;
    ojson r = {{"t", (in.at("tick").get<uint32_t>() - tick0) / 1000.0},
               {"kind", "vt"},
               {"tid", in.at("tid")},
               {"this", in.at("this")},
               {"cls", cls.contains(v) ? cls.at(v) : in.at("vt").get<std::string>()},
               {"slot", slot}};
    ojson args = ojson::array(), ins = ojson::array(), outs = ojson::array();
    std::string as;
    for (long i = 0; i < pop / 4 && i < 4; ++i) {
      const std::string a = in.at("a").at(i), si = text(in.at("s").at(i)), so = c.out ? text(c.out->at("s").at(i)) : "";
      args.push_back(a);
      ins.push_back(si);
      outs.push_back(so);
      as += (i ? ", " : "") + a + (si.empty() ? "" : "'" + si + "'") + (so.empty() || so == si ? "" : "→'" + so + "'");
    }
    r["args"] = std::move(args);
    r["in"] = std::move(ins);
    r["out"] = std::move(outs);
    r["eax"] = c.out ? c.out->at("eax") : json("");
    r["from"] = img.at(hex(in.at("ret")));
    disk::append(disk::RunLiveTdxRecLog, {.stamp = stamp}, r.dump());
    const std::string key = strf("%s %ld %s", in.at("this").get<std::string>().c_str(), slot, r["from"].get<std::string>().c_str());
    if (key == prev && pop > 0) {
      ++rep;
      continue;
    }
    flush();
    prev = key, rep = 0;
    line = strf("%8.3f t%-5u %s %s::s%ld(%s) = %s  ← %s", r["t"].get<double>(), in.at("tid").get<unsigned>(), in.at("this").get<std::string>().c_str(),
                r["cls"].get<std::string>().c_str(), slot, as.c_str(), c.out ? c.out->at("eax").get<std::string>().c_str() : "-",
                r["from"].get<std::string>().c_str());
  }
  flush();
  std::fprintf(stderr, "%zu 次调用 → %s\n", calls.size(), disk::path(disk::RunLiveTdxRecLog, {.stamp = stamp}).string().c_str());
  return 0;
}

} // namespace live::tdx::tool
