#include "live/tdx/Req.hpp"

#include "live/tdx/Image.hpp"
#include "live/tdx/Schema.hpp"
#include "live/tdx/Shim.hpp"
#include "live/Trace.hpp"
#include "live/tdx/Until.hpp"

#include <algorithm>
#include <filesystem>
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <vector>

namespace live::tdx::req {

namespace {

using nlohmann::json;
using trace::Kw;

constexpr const char *kJob = "CTcJob@CTcComm", *kUser = "CTcUser@CTcClient@CTcComm";
constexpr const char *kUserNew = "7"; // CTcUser 槽 7 (0, 0) 建 job: 插件调它的 this 即其所用的 CTcUser
// 槽: 减引用 (插件读完应答即调) / 设功能号 / 写字符串入参 / 写整数入参 / 网络线程收到应答 / 列数 / 发送 / 行数 /
//     应答可读 (插件开读先问) / 下一行 / 按列号取 / 按字段号取
enum Slot { kRelease = 1,
            kFunc = 6,
            kStr = 7,
            kInt = 9,
            kNet = 18,
            kCols = 22,
            kSend = 25,
            kRows = 39,
            kOk = 40,
            kNext = 41,
            kByCol = 43,
            kByField = 44 };
constexpr const char *kOnly = "1,6,7,9,18,22,25,39,40,41,43,44";
constexpr const char *kRet = "22,39,43,44"; // 无参 getter 与取字段 (返回时缓冲已填), 不会有异常越过
constexpr int kMinSlots = kByField + 1;
constexpr uint32_t kFieldMask = 0xFFFF;             // 插件画表时字段号另带 0x120000
constexpr int kPosField = 0x506, kPageRows = 0x508; // 定位字串 (分页的功能下一页从这里接) / 请求行数
constexpr const char *kPageMax = "1000";            // 分页时每页要的行数 (插件要 200; 服务器至多给七百余行)
// done 时插件可能还没读应答 (查询键恢复比插件读早几到十几 ms): 等发出不久的请求都读完, 至多等这么久
constexpr double kWaitRead = 2.0, kRecent = 5.0;

struct In {
  int fid;
  bool is_int;
  std::string v;
};
// 插件最近一次发某功能号时的样子 (ask 照搬)
struct Last {
  std::vector<In> ins; // 插件写的入参, 按序 (建 job 时 tc.dll 自填的不在内: 直发建 job 时它照样填)
  std::string lp;      // 回包 lParam
};

struct State {
  std::mutex mu;
  int pid = 0;
  std::unique_ptr<Image> img;
  std::string vt;
  Schema schema;
  std::string vt_user;      // CTcUser 的虚表
  std::map<int, Last> last; // 功能号 → 插件最近一次
  uint32_t user = 0;        // 插件所用的 CTcUser (job 回池后其 +0x18 即清, 故从建 job 的调用取)
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
  std::vector<In> ins; // 插件写的入参 (原样、按序)
  uint32_t t0 = 0;     // 首个入参的 tick
  std::optional<int> func;
  std::string page;
  Kw in = Kw::object();
  std::optional<uint32_t> sent, net, ans, end; // 发送 / 网络线程收到 / 插件开读 / 插件读完 的 tick
  long tid = 0;                                // 发送的线程
  Kw to = Kw::object();
  std::optional<long> cols, rows;
  long next = 0;
  std::string read;        // 按列号 / 按字段号
  Kw first = Kw::object(); // 首批读到的字段 (读过之后再移游标即首行完; AddinCredit 开读前会先多移两次)
  bool first_done = false;
  std::set<uint32_t> keys; // 首行已读的列号 / 字段号 (插件同一字段会读几遍, 只留首遍)
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

void arm(const mem::HeapP &h) {
  State &s = state();
  std::lock_guard lk(s.mu);
  if (h->pid() != s.pid) {
    auto img = std::make_unique<Image>(h);
    const std::vector<mem::Addr> vs = img->vtables(kJob), vu = img->vtables(kUser);
    need(vs.size() == 1 && vu.size() == 1, strf("%s / %s 的主虚表有 %zu / %zu 张", kJob, kUser, vs.size(), vu.size()));
    s.schema = schema(*h);
    s.vt = strf("0x%08llx", static_cast<unsigned long long>(vs[0]));
    s.vt_user = strf("0x%08llx", static_cast<unsigned long long>(vu[0]));
    s.img = std::move(img);
    s.pid = h->pid();
    s.last.clear(); // 换了进程: 插件的入参与 CTcUser 都要重新见过
    s.user = 0;
  }
  // 两张表都钩 (已钩的照旧, 其记录设置回缺省, 下面重设); 新钩上 (新注入的 shim / 被 shim vt 卸了) 即清掉其间零碎
  const bool fresh = shim::cmd("vt", "on").at(0).at("slots").get<int>() == 0;
  const int n = shim::cmd("vt", "hook", s.vt, 256).at(0).at("hooked").get<int>();
  need(n >= kMinSlots, strf("%s 虚表 %s 只钩上 %d 槽", kJob, s.vt.c_str(), n));
  const int nu = shim::cmd("vt", "hook", s.vt_user, 256).at(0).at("hooked").get<int>();
  need(nu > std::stoi(kUserNew), strf("%s 虚表 %s 只钩上 %d 槽", kUser, s.vt_user.c_str(), nu));
  if (fresh) {
    shim::cmd("vt", "clear");
    trace::ev("req_arm", {{"vt", s.vt}, {"slots", n}, {"dict", std::filesystem::path(s.schema.file).filename().string()}, {"fields", s.schema.fields.size()}, {"funcs", s.schema.funcs.size()}});
  }
  shim::cmd("vt", "only", s.vt, kOnly);
  shim::cmd("vt", "ret", s.vt, kRet);
  shim::cmd("vt", "only", s.vt_user, kUserNew);
}

namespace {

// 调用 (drain 的各条, 按进入先后) → 各请求; 插件建 job 所用的 CTcUser 记进 user
std::vector<Req> assemble(const std::vector<json> &evs, const State &s, uint32_t &user) {
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
    if (e.at("vt") == s.vt_user) {
      if (from.starts_with("Addin"))
        user = hex(e.at("this"));
      continue;
    }
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
      const std::string raw = slot == kStr ? text(str.at(1)) : std::to_string(static_cast<int32_t>(hex(a.at(1))));
      const std::string v = name.find("密码") != std::string::npos ? "***" : raw;
      if (fid == 0x16a)
        r->page = v;
      r->in[name] = v;
      if (from != "tc.dll")
        r->ins.push_back({fid, slot == kInt, raw});
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
        if (!r->keys.insert(k).second)
          continue;
        if (r->first.contains(name)) // 同名的列 (信用资产的三列「数值」)
          name += strf("#%u", k);
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
  uint32_t user = 0;
  std::vector<Req> reqs = assemble(evs, s, user);
  const auto reading = [&] {
    return std::any_of(reqs.begin(), reqs.end(), [&](const Req &r) { return r.sent && !r.end && now - *r.sent < kRecent * 1000; });
  };
  poll(
      [&] {
        if (!reading())
          return true;
        drain();
        reqs = assemble(evs, s, user);
        return !reading();
      },
      kWaitRead, 0.02);
  for (const Req &r : reqs) {
    trace::ev("req", kw_of(r, s.schema, now));
    if (r.sent && r.func && r.by.starts_with("Addin"))
      s.last[*r.func] = {r.ins, r.to.at("lp").get<std::string>()};
  }
  if (user)
    s.user = user;
}

bool seen(int func) {
  State &s = state();
  std::lock_guard lk(s.mu);
  return s.user && s.last.contains(func);
}

namespace {

// 直发一问
Answer ask_one(int func, const std::map<int, std::string> &over) {
  State &s = state();
  std::lock_guard lk(s.mu);
  need(s.pid != 0, "请求层没接上 (req::arm)");
  const auto l = s.last.find(func);
  need(l != s.last.end() && s.user, strf("还没见过插件发功能 %d, 入参无从照搬", func));
  std::string line = strf("req class=TdxW_MainFrame_Class 0x%08x %d %s", s.user, func, l->second.lp.c_str());
  std::set<int> have;
  for (const In &i : l->second.ins) {
    const std::string v = over.contains(i.fid) ? over.at(i.fid) : i.v;
    need(v.find(' ') == std::string::npos, strf("功能 %d 字段 %d 的值含空格: 「%s」", func, i.fid, v.c_str()));
    line += strf(" %c%d=", i.is_int ? 'i' : 's', i.fid) + v;
    have.insert(i.fid);
  }
  for (const auto &[fid, v] : over)
    need(have.contains(fid), strf("功能 %d 的插件入参里没有字段 %d (%s), 不能凭空加", func, fid, s.schema.name(fid).c_str()));
  // 各列按出参的字段号取、排成出参序 (服务器的列序未必同协议字典, 如担保品)
  const auto f = s.schema.funcs.find(func);
  need(f != s.schema.funcs.end(), strf("协议字典里没有功能 %d", func));
  line += " f=";
  for (size_t i = 0; i < f->second.out.size(); ++i)
    line += strf("%s%d", i ? "," : "", f->second.out[i]);
  const std::vector<json> r = shim::call(line, 30);
  need(r.size() == 2, strf("req 应答 %zu 条", r.size()));
  const json &a = r[1];
  need(a.at("ok").get<int>() == 1, strf("功能 %d 应答不可读 (ok = %d)", func, a.at("ok").get<int>()));
  Answer out{a.at("cols").get<int>(), {}, a.at("pos").get<std::string>()};
  for (const json &row : a.at("data"))
    out.rows.push_back(row.get<std::vector<std::string>>());
  need(static_cast<int>(out.rows.size()) == a.at("rows").get<int>(), "req 应答行数与 data 不符");
  trace::ev("ask", {{"func", func}, {"job", r[0].at("job")}, {"seq", r[0].at("seq")}, {"cols", out.cols}, {"have", a.at("have")}, {"rows", out.rows.size()}, {"refs", a.at("refs")}, {"pos", out.pos}});
  return out;
}

} // namespace

Answer ask(int func, std::map<int, std::string> over) {
  bool paged;
  {
    State &s = state();
    std::lock_guard lk(s.mu);
    const auto l = s.last.find(func);
    paged = l != s.last.end() && std::any_of(l->second.ins.begin(), l->second.ins.end(), [](const In &i) { return i.fid == kPosField; });
  }
  if (paged)
    over.try_emplace(kPageRows, kPageMax);
  Answer all = ask_one(func, over);
  for (Answer page = all; paged && !page.rows.empty();) {
    need(!page.pos.empty(), strf("功能 %d 分页的应答没有定位串", func));
    over[kPosField] = page.pos;
    page = ask_one(func, over);
    need(page.cols == all.cols || page.rows.empty(), strf("功能 %d 各页列数不一 (%d / %d)", func, all.cols, page.cols));
    all.rows.insert(all.rows.end(), std::make_move_iterator(page.rows.begin()), std::make_move_iterator(page.rows.end()));
    all.pos = page.pos;
  }
  return all;
}

} // namespace live::tdx::req
