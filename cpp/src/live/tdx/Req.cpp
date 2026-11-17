#include "live/tdx/Req.hpp"

#include "live/tdx/Image.hpp"
#include "live/tdx/Schema.hpp"
#include "live/tdx/Shim.hpp"
#include "live/tdx/Trace.hpp"
#include "live/tdx/Until.hpp"

#include <algorithm>
#include <filesystem>
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

namespace live::tdx::req {

namespace {

using nlohmann::json;
using trace::Kw;

constexpr const char *kJob = "CTcJob@CTcComm";
// 槽: 减引用 (插件读完应答即调) / 设功能号 / 写字符串入参 / 写整数入参 / 网络线程收到应答 / 列数 / 发送 / 行数 /
//     应答可读 (插件开读先问) / 下一行 / 按列号取 / 按字段号取
enum Slot { kRelease = 1, kFunc = 6, kStr = 7, kInt = 9, kNet = 18, kCols = 22, kSend = 25, kRows = 39, kOk = 40, kNext = 41, kByCol = 43, kByField = 44 };
constexpr const char *kOnly = "1,6,7,9,18,22,25,39,40,41,43,44";
constexpr const char *kRet = "22,39,43,44"; // 无参 getter 与取字段 (返回时缓冲已填), 不会有异常越过
constexpr int kMinSlots = kByField + 1;
constexpr uint32_t kFieldMask = 0xFFFF; // 插件画表时字段号另带 0x120000
// done 时插件可能还没读应答 (查询键恢复比插件读早几到十几 ms): 等发出不久的请求都读完, 至多等这么久
constexpr double kWaitRead = 2.0, kRecent = 5.0;

struct State {
  std::mutex mu;
  int pid = 0;
  std::unique_ptr<Image> img;
  std::string vt;
  Schema schema;
};
State &state() {
  static State s;
  return s;
}

uint32_t hex(const json &j) { return static_cast<uint32_t>(std::stoul(j.get<std::string>(), nullptr, 16)); }

// 参数所指头几字节 (十六进制) → 文本 (GBK)
std::string text(const json &j) {
  const std::string h = j.get<std::string>();
  std::string raw;
  for (size_t i = 0; i + 1 < h.size(); i += 2)
    raw += static_cast<char>(std::stoi(h.substr(i, 2), nullptr, 16));
  return mem::gbk(raw);
}

// 一个请求: job 上自首个入参起, 到下一个请求的首个入参前
struct Req {
  std::string job, by; // job 地址 / 设功能号的模块 (插件)
  uint32_t t0 = 0;     // 首个入参的 tick
  std::optional<int> func;
  std::string page;
  Kw in = Kw::object();
  std::optional<uint32_t> sent, net, ans, end; // 发送 / 网络线程收到 / 插件开读 / 插件读完 的 tick
  long tid = 0;                           // 发送的线程
  Kw to = Kw::object();
  std::optional<long> cols, rows;
  long next = 0;
  std::string read;        // 按列号 / 按字段号
  Kw first = Kw::object(); // 首批读到的字段 (读过之后再移游标即首行完; AddinCredit 开读前会先多移两次)
  bool first_done = false;
};

Kw kw_of(const Req &r, const Schema &s, uint32_t now) {
  Kw k = Kw::object();
  if (r.func) {
    k["func"] = *r.func;
    if (s.titles.contains(*r.func))
      k["title"] = s.titles.at(*r.func);
  }
  if (!r.page.empty())
    k["page"] = r.page;
  k["by"] = r.by;
  k["job"] = r.job;
  k["in"] = r.in;
  if (r.sent) {
    k["to"] = r.to;
    k["ago"] = now - *r.sent;
  } else
    k["sent"] = false;
  if (r.net)
    k["net_ms"] = *r.net - *r.sent;
  if (r.ans)
    k["ans_ms"] = *r.ans - *r.sent;
  if (r.end)
    k["end_ms"] = *r.end - *r.sent;
  else if (r.sent)
    k["finished"] = false;
  if (r.cols)
    k["cols"] = *r.cols;
  if (r.rows)
    k["rows"] = *r.rows;
  if (r.next)
    k["next"] = r.next;
  if (!r.read.empty()) {
    k["read"] = r.read;
    k["first"] = r.first;
  }
  return k;
}

} // namespace

void arm(const mem::Heap &h) {
  State &s = state();
  std::lock_guard lk(s.mu);
  if (h.pid() != s.pid) {
    auto img = std::make_unique<Image>(h.pid());
    const std::vector<mem::Addr> vs = img->vtables(kJob);
    need(vs.size() == 1, strf("%s 的主虚表有 %zu 张", kJob, vs.size()));
    s.schema = schema(h);
    s.vt = strf("0x%08llx", static_cast<unsigned long long>(vs[0]));
    s.img = std::move(img);
    s.pid = h.pid();
  }
  if (shim::cmd("vt", "on").at(0).at("slots").get<int>() == 0) {
    const int n = shim::cmd("vt", "hook", s.vt, 256).at(0).at("hooked").get<int>();
    need(n >= kMinSlots, strf("%s 虚表 %s 只钩上 %d 槽", kJob, s.vt.c_str(), n));
    shim::cmd("vt", "clear");
    trace::ev("req_arm", {{"vt", s.vt}, {"slots", n}, {"dict", std::filesystem::path(s.schema.file).filename().string()}, {"fields", s.schema.fields.size()},
                          {"funcs", s.schema.funcs.size()}});
  }
  shim::cmd("vt", "only", s.vt, kOnly); // 别处 (shim vt) 可能改过记哪些槽: 每次接入都按本模块的设一遍
  shim::cmd("vt", "ret", s.vt, kRet);
}

namespace {

// 调用 (drain 的各条, 按进入先后) → 各请求
std::vector<Req> assemble(const std::vector<json> &evs, const State &s) {
  std::map<uint32_t, const json *> rets; // id → 返回那条
  for (const json &e : evs)
    if (e.at("kind") == "ret")
      rets[e.at("id").get<uint32_t>()] = &e;
  std::map<std::string, size_t> open; // job → reqs 里它当前那个请求
  std::vector<Req> reqs;
  for (const json &e : evs) {
    if (e.at("kind") != "call")
      continue;
    const int slot = e.at("slot").get<int>();
    const std::string job = e.at("this").get<std::string>();
    const uint32_t tick = e.at("tick").get<uint32_t>();
    const json &a = e.at("a"), &str = e.at("s");
    const Image::Mod *m = s.img->mod(hex(e.at("ret")));
    const std::string from = m ? m->name : "?";
    const auto rt = rets.find(e.at("id").get<uint32_t>());
    const json *ret = rt == rets.end() ? nullptr : rt->second;
    const auto it = open.find(job);
    Req *r = it == open.end() ? nullptr : &reqs[it->second];
    if (slot == kStr || slot == kInt) {
      if (!r || r->sent) {
        open[job] = reqs.size();
        reqs.push_back({.job = job, .t0 = tick});
        r = &reqs.back();
      }
      const int fid = static_cast<int>(hex(a.at(0)));
      std::string name = s.schema.name(fid);
      if (name == "?")
        name = strf("0x%x", fid);
      const std::string v =
          name.find("密码") != std::string::npos ? "***" : slot == kStr ? text(str.at(1)) : std::to_string(static_cast<int32_t>(hex(a.at(1))));
      if (fid == 0x16a)
        r->page = v;
      r->in[name] = v;
      continue;
    }
    if (!r)
      continue; // 本轮之前发起的请求, 其入参不在本轮
    if (slot == kFunc) {
      r->func = static_cast<int>(hex(a.at(0)) & kFieldMask);
      r->by = from;
    } else if (slot == kSend) {
      r->sent = tick;
      r->tid = e.at("tid").get<long>();
      r->to = {{"hwnd", a.at(0)}, {"lp", a.at(2)}};
    } else if (slot == kNet) {
      if (r->sent && !r->net && e.at("tid").get<long>() != r->tid)
        r->net = tick;
    } else if (from != "tc.dll" && r->net) { // 插件读应答 (tc.dll 自己也调这几个槽)
      if (slot == kRelease) {
        if (r->ans && !r->end)
          r->end = tick;
        continue;
      }
      if (!r->ans)
        r->ans = tick;
      if (slot == kCols && ret)
        r->cols = static_cast<long>(hex(ret->at("eax")));
      else if (slot == kRows && ret)
        r->rows = static_cast<long>(hex(ret->at("eax")));
      else if (slot == kNext) {
        ++r->next;
        r->first_done = r->first_done || !r->first.empty();
      } else if ((slot == kByCol || slot == kByField) && ret && !r->first_done) {
        r->read = slot == kByCol ? "按列号" : "按字段号";
        const uint32_t k = hex(a.at(0)) & kFieldMask;
        std::string name;
        if (slot == kByField) {
          if (hex(ret->at("eax")) == 0)
            continue; // 应答里没有该字段
          name = s.schema.name(static_cast<int>(k));
        } else {
          const auto f = r->func ? s.schema.funcs.find(*r->func) : s.schema.funcs.end();
          name = f != s.schema.funcs.end() && k < f->second.out.size() ? s.schema.name(f->second.out[k]) : strf("列 %u", k);
        }
        if (!r->first.contains(name))
          r->first[name] = text(ret->at("s").at(1));
      }
    }
  }
  return reqs;
}

} // namespace

void flush() {
  State &s = state();
  std::lock_guard lk(s.mu);
  if (!s.pid)
    return;
  std::vector<json> evs;
  uint32_t now = 0;
  const auto drain = [&] {
    std::vector<json> got = shim::cmd_wait(10, "vt", "drain");
    need(!got.empty() && got.back().contains("now"), "vt drain 缺末行 {now, lost}");
    now = got.back().at("now").get<uint32_t>();
    if (const int lost = got.back().at("lost").get<int>(); lost > 0)
      trace::ev("req_lost", {{"n", lost}});
    got.pop_back();
    evs.insert(evs.end(), std::make_move_iterator(got.begin()), std::make_move_iterator(got.end()));
  };
  drain();
  std::vector<Req> reqs = assemble(evs, s);
  const auto reading = [&] {
    return std::any_of(reqs.begin(), reqs.end(), [&](const Req &r) { return r.sent && !r.end && now - *r.sent < kRecent * 1000; });
  };
  poll(
      [&] {
        if (!reading())
          return true;
        drain();
        reqs = assemble(evs, s);
        return !reading();
      },
      kWaitRead, 0.02);
  for (const Req &r : reqs)
    trace::ev("req", kw_of(r, s.schema, now));
}

} // namespace live::tdx::req
