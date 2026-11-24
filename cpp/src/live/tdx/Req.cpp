#include "live/tdx/Req.hpp"

#include "live/Base.hpp"
#include "live/Trace.hpp"
#include "live/tdx/Heap.hpp"
#include "live/tdx/Schema.hpp"
#include "live/tdx/Shim.hpp"

#include <algorithm>
#include <filesystem>
#include <iterator>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <vector>

namespace live::tdx::req {

namespace {

using nlohmann::json;

constexpr const char *kJob = "CTcJob@CTcComm", *kUser = "CTcUser@CTcClient@CTcComm";
constexpr const char *kUserOnly = "7";       // CTcUser 槽 7 (0, 0) 建 job: 插件调它的 this 即其所用的 CTcUser
constexpr const char *kJobOnly = "6,7,9,25"; // CTcJob: 设功能号 / 写串入参 / 写整入参 / 发送
enum Slot { kFunc = 6,
            kStr = 7,
            kInt = 9,
            kSend = 25 };
constexpr int kMinJobSlots = kSend + 1, kMinUserSlots = 8;
constexpr uint32_t kFieldMask = 0xFFFF;             // 插件画表时字段号另带 0x120000
constexpr int kPosField = 0x506, kPageRows = 0x508; // 定位字串 (分页的功能下一页从这里接) / 请求行数
constexpr const char *kPageMax = "1000";            // 分页时每页要的行数 (插件要 200; 服务器至多给七百余行)

struct Mod {
  uint32_t base = 0, size = 0;
  std::string name;
};

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
  std::vector<Mod> mods;
  std::string vt, vt_user; // CTcJob / CTcUser 的主虚表
  Schema schema;
  std::map<int, Last> last; // 功能号 → 插件最近一次
  uint32_t user = 0;        // 插件所用的 CTcUser (job 回池后其 +0x18 即清, 故从建 job 的调用取)

  std::string mod_of(uint32_t addr) const {
    for (const Mod &m : mods)
      if (m.base <= addr && addr < m.base + m.size)
        return m.name;
    return "?";
  }
};

State &state() {
  static State s;
  return s;
}

uint32_t hex_u32(const json &j) { return static_cast<uint32_t>(std::stoul(j.get<std::string>(), nullptr, 16)); }

// 参数所指头几字节 (十六进制) → 文本 (GBK)
std::string hex_gbk(const json &j) {
  const std::string h = j.get<std::string>();
  std::string raw;
  raw.reserve(h.size() / 2);
  for (size_t i = 0; i + 1 < h.size(); i += 2)
    raw += static_cast<char>(std::stoi(h.substr(i, 2), nullptr, 16));
  return mem::gbk(raw);
}

// 一个请求: job 上自首个入参起, 到下一个请求的首个入参前
struct Draft {
  std::vector<In> ins;
  std::optional<int> func;
  std::string by, lp;
  bool sent = false;
};

// 一问的 req 参数: <功能号> <lParam> 入参… f=出参…; 须持 s.mu
std::string req_args(const State &s, const Ask &q) {
  const auto l = s.last.find(q.func);
  need(l != s.last.end() && s.user, "还没见过插件发功能 %d, 入参无从照搬", q.func);
  std::string line = strf("%d %s", q.func, l->second.lp.c_str());
  std::set<int> have;
  for (const In &i : l->second.ins) {
    const std::string &v = q.over.contains(i.fid) ? q.over.at(i.fid) : i.v;
    need(v.find_first_of(" ;") == std::string::npos, "功能 %d 字段 %d 的值含空格或分号: 「%s」", q.func, i.fid, v.c_str());
    line += strf(" %c%d=", i.is_int ? 'i' : 's', i.fid) + v;
    have.insert(i.fid);
  }
  for (const auto &[fid, v] : q.over)
    need(have.contains(fid), "功能 %d 的插件入参里没有字段 %d (%s)", q.func, fid, s.schema.name(fid).c_str());
  const auto f = s.schema.funcs.find(q.func);
  need(f != s.schema.funcs.end(), "协议字典里没有功能 %d", q.func);
  // 各列按出参的字段号取、排成出参序 (服务器的列序未必同协议字典, 如担保品)
  line += " f=";
  for (size_t i = 0; i < f->second.out.size(); ++i)
    line += strf("%s%d", i ? "," : "", f->second.out[i]);
  return line;
}

// 直发一趟: 各问一条 req 一并发出 (同时在途), 应答序同问序
std::vector<Answer> ask_once(const std::vector<Ask> &qs) {
  State &s = state();
  std::lock_guard lk(s.mu);
  need(s.pid != 0, "请求层没接上 (req::arm)");
  std::string line = strf("req class=TdxW_MainFrame_Class 0x%08x", s.user);
  for (size_t i = 0; i < qs.size(); ++i)
    line += (i ? " ; " : " ") + req_args(s, qs[i]);

  const std::vector<json> r = shim::call(line, 30);
  need(r.size() == 2 * qs.size(), "req %zu 问应答 %zu 条", qs.size(), r.size());
  std::vector<Answer> out;
  out.reserve(qs.size());
  for (size_t i = 0; i < qs.size(); ++i) {
    const int func = qs[i].func;
    const json &a = r[qs.size() + i];
    need(a.at("ok").get<int>() == 1, "功能 %d 应答不可读 (ok = %d)", func, a.at("ok").get<int>());
    Answer x{a.at("cols").get<int>(), {}, a.at("pos").get<std::string>()};
    for (const json &row : a.at("data"))
      x.rows.push_back(row.get<std::vector<std::string>>());
    need(static_cast<int>(x.rows.size()) == a.at("rows").get<int>(), "功能 %d 应答行数与 data 不符", func);
    trace::ev("ask", {{"func", func}, {"cols", x.cols}, {"rows", x.rows.size()}, {"pos", x.pos}});
    out.push_back(std::move(x));
  }
  return out;
}

} // namespace

void arm(int pid) {
  State &s = state();
  std::lock_guard lk(s.mu);
  if (pid != s.pid) {
    std::vector<json> vs, vu;
    for (json &v : shim::cmd_wait(30, "vtable", kJob, kUser)) // 两类一趟扫 (各模块拷出来扫, 给足时间)
      (v.at("cls") == kJob ? vs : vu).push_back(std::move(v));
    need(vs.size() == 1 && vu.size() == 1, "%s / %s 的主虚表有 %zu / %zu 张", kJob, kUser, vs.size(), vu.size());
    std::vector<Mod> mods;
    for (const json &m : shim::cmd("mods"))
      mods.push_back({hex_u32(m.at("base")), m.at("size").get<uint32_t>(), m.at("name").get<std::string>()});
    s.schema = schema(pid);
    s.vt = vs[0].at("vt").get<std::string>();
    s.vt_user = vu[0].at("vt").get<std::string>();
    s.mods = std::move(mods);
    s.pid = pid;
    s.last.clear(); // 换了进程: 插件的入参与 CTcUser 都要重新见过
    s.user = 0;
  }
  // 两张表都钩 (已钩的照旧, 其记录设置回缺省, 下面重设); 新钩上 (新注入的 shim / 被 shim vt 卸了) 即清掉其间零碎
  const bool fresh = shim::cmd("vt", "on").at(0).at("slots").get<int>() == 0;
  const int n = shim::cmd("vt", "hook", s.vt, 256).at(0).at("hooked").get<int>();
  need(n >= kMinJobSlots, "%s 虚表 %s 只钩上 %d 槽", kJob, s.vt.c_str(), n);
  const int nu = shim::cmd("vt", "hook", s.vt_user, 256).at(0).at("hooked").get<int>();
  need(nu >= kMinUserSlots, "%s 虚表 %s 只钩上 %d 槽", kUser, s.vt_user.c_str(), nu);
  if (fresh) {
    shim::cmd("vt", "clear");
    trace::ev("req_arm", {{"vt", s.vt}, {"slots", n}, {"dict", std::filesystem::path(s.schema.file).filename().string()}, {"fields", s.schema.fields.size()}, {"funcs", s.schema.funcs.size()}});
  }
  shim::cmd("vt", "only", s.vt, kJobOnly);
  shim::cmd("vt", "only", s.vt_user, kUserOnly);
}

void flush() {
  State &s = state();
  std::lock_guard lk(s.mu);
  if (!s.pid)
    return;
  std::vector<json> evs = shim::cmd_wait(10, "vt", "drain");
  need(!evs.empty() && evs.back().contains("now"), "vt drain 缺末行 {now, lost}");
  if (const int lost = evs.back().at("lost").get<int>(); lost > 0)
    trace::ev("req_lost", {{"n", lost}});
  evs.pop_back();

  std::map<std::string, size_t> open; // job → drafts 里它当前那个请求
  std::vector<Draft> drafts;
  for (const json &e : evs) {
    if (e.at("kind") != "call")
      continue;
    const std::string from = s.mod_of(hex_u32(e.at("ret")));
    if (e.at("vt") == s.vt_user) {
      if (from.starts_with("Addin"))
        s.user = hex_u32(e.at("this"));
      continue;
    }
    const int slot = e.at("slot").get<int>();
    const std::string job = e.at("this").get<std::string>();
    const json &a = e.at("a"), &str = e.at("s");
    const auto it = open.find(job);
    Draft *r = it == open.end() ? nullptr : &drafts[it->second];
    if (slot == kStr || slot == kInt) {
      if (!r || r->sent) {
        open[job] = drafts.size();
        drafts.emplace_back();
        r = &drafts.back();
      }
      if (from != "tc.dll") {
        const int fid = static_cast<int>(hex_u32(a.at(0)));
        const std::string raw = slot == kStr ? hex_gbk(str.at(1)) : std::to_string(static_cast<int32_t>(hex_u32(a.at(1))));
        r->ins.push_back({fid, slot == kInt, raw});
      }
      continue;
    }
    if (!r)
      continue; // 本轮之前发起的请求, 其入参不在本轮
    if (slot == kFunc) {
      r->func = static_cast<int>(hex_u32(a.at(0)) & kFieldMask);
      r->by = from;
    } else if (slot == kSend) {
      r->sent = true;
      r->lp = a.at(2).get<std::string>();
    }
  }
  for (const Draft &r : drafts)
    if (r.sent && r.func && r.by.starts_with("Addin"))
      s.last[*r.func] = {r.ins, r.lp};
}

bool seen(int func) {
  State &s = state();
  std::lock_guard lk(s.mu);
  return s.user && s.last.contains(func);
}

std::vector<Answer> ask(std::vector<Ask> asks) {
  std::vector<bool> paged(asks.size());
  {
    State &s = state();
    std::lock_guard lk(s.mu);
    for (size_t i = 0; i < asks.size(); ++i) {
      const auto l = s.last.find(asks[i].func);
      paged[i] = l != s.last.end() && std::any_of(l->second.ins.begin(), l->second.ins.end(), [](const In &x) { return x.fid == kPosField; });
      if (paged[i])
        asks[i].over.try_emplace(kPageRows, kPageMax);
    }
  }
  std::vector<Answer> all = ask_once(asks);
  // 还没到底的分页各问: 以上页的定位串接着问, 一趟一并问一页
  std::vector<size_t> open;
  for (size_t i = 0; i < asks.size(); ++i)
    if (paged[i] && !all[i].rows.empty())
      open.push_back(i);
  while (!open.empty()) {
    std::vector<Ask> next;
    for (const size_t i : open) {
      need(!all[i].pos.empty(), "功能 %d 分页的应答没有定位串", asks[i].func);
      asks[i].over[kPosField] = all[i].pos;
      next.push_back(asks[i]);
    }
    std::vector<Answer> pages = ask_once(next);
    std::vector<size_t> still;
    for (size_t k = 0; k < open.size(); ++k) {
      const size_t i = open[k];
      Answer &page = pages[k];
      need(page.cols == all[i].cols || page.rows.empty(), "功能 %d 各页列数不一 (%d / %d)", asks[i].func, all[i].cols, page.cols);
      if (page.rows.empty())
        continue;
      all[i].rows.insert(all[i].rows.end(), std::make_move_iterator(page.rows.begin()), std::make_move_iterator(page.rows.end()));
      all[i].pos = page.pos;
      still.push_back(i);
    }
    open = std::move(still);
  }
  return all;
}

} // namespace live::tdx::req
