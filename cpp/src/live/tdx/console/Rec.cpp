// 录制 (只读, 不投递任何输入): 每 kRecGap 取一次 spy 消息 (WM_COMMAND + 发给非标准控件的私有消息)、交易请求、窗口树、绘制层,
//   与上一轮比对, 变了的逐条落 output/run/live/tdx/rec/<时刻>.jsonl ({t 距开录秒数, kind, ...}). 录制期间独占 spy (每轮 dump 后 clear).
//   种类: start / tree (开录时全树, 只落盘) / msg (相邻同样的合并, n 为条数) / req (客户端内部请求字段) /
//   new / del / chg (窗口出现、消失、属性变化) / gdi (可见窗口绘制文字) / mark (打点) / err / stop.
//   t 是取到的那一轮的时刻, 同一轮内只有 msg 之间保序. 页面留最近 kRecCap 条 (不含 tree), 各一行
#include "disk/Disk.hpp"
#include "live/tdx/Shim.hpp"
#include "live/tdx/console/Model.hpp"

#include <algorithm>
#include <cmath>
#include <thread>

namespace live::tdx::console {

namespace {

using Lock = std::lock_guard<std::mutex>;

constexpr const char *kRecKeys[] = {"vis", "en", "title", "rect", "style"};

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
  return plain(n.at("hwnd")) + " " + plain(n.at("class")) + "#" + plain(n.at("id")) + "「" + (t.is_string() ? ui::squash(t.get<std::string>()) : "") + "」";
}
std::string hidden(const ojson &vis) { return truthy(vis) ? "" : " (隐藏)"; }

// 请求记录按 op 各一行的格式; 表外回空
std::string req_line(const ojson &r) {
  const std::string op = r.contains("op") ? plain(r["op"]) : "";
  const auto num = [&] { return op + "[" + plain(r.at("id")) + "]=" + plain(r.at("value")); };
  if (op == "init")
    return "init=" + plain(r.at("value"));
  if (op == "func")
    return strf("func=%lld (0x%llx)", r.at("value").get<long long>(), r.at("value").get<unsigned long long>());
  if (op == "str")
    return "str[" + plain(r.at("id")) + "," + plain(r.at("aux")) + "]=" + r.at("value").dump();
  if (op == "int" || op == "float")
    return num();
  if (op == "send")
    return "SEND args=" + r.at("args").dump();
  return "";
}

std::string body(const std::string &kind, const ojson &r) {
  if (kind == "msg")
    return "msg " + plain(r.at("msg")) + " → " + win(r) + " wp=" + plain(r.at("wparam")) + " lp=" + plain(r.at("lparam")) +
           (r.at("n").get<long>() > 1 ? strf(" ×%ld", r.at("n").get<long>()) : "") + hidden(r.at("vis"));
  if (kind == "req")
    if (const std::string s = req_line(r); !s.empty())
      return "req req=" + plain(r.at("obj")) + " " + s;
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
    return "gdi " + plain(r.at("hwnd")) + " " + plain(r.at("cls")) + "#" + plain(r.at("id")) + " | " + cut(ui::squash(s), 300);
  }
  if (kind == "mark")
    return "===== " + plain(r.at("text")) + " =====";
  ojson rest = r;
  rest.erase("t");
  rest.erase("kind");
  return kind + " " + rest.dump();
}

// ---------- 落盘 ----------

void put(Core &c, const char *kind, const ojson &rest) { // 持 mu
  ojson r = {{"t", std::round(std::chrono::duration<double>(Clock::now() - c.rec_t0).count() * 1000) / 1000}, {"kind", kind}};
  for (const auto &e : rest.items())
    r[e.key()] = e.value();
  disk::append(disk::RunLiveTdxRecLog, {.stamp = c.rec_stamp}, r.dump());
  ++c.rec_n;
  if (std::string_view(kind) == "tree")
    return;
  c.rec_tail.push_back({kind, strf("%8.2f ", r["t"].get<double>()) + body(kind, r)});
  if (c.rec_tail.size() > kRecCap)
    c.rec_tail.pop_front();
  ++c.rec_ver;
}
void journal(Core &c, const char *kind, const ojson &rest = ojson::object()) {
  Lock lk(c.mu);
  put(c, kind, rest);
}

// ---------- 录制线程 ----------

struct Snap { // 一轮的窗口树: 先序, hwnd → 记录
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
    if (it.size() > 5 && !trim(it[5].get<std::string>()).empty())
      out.push_back(it[5].get<std::string>());
  return out;
}

void rec_loop(Core &c) {
  std::string file;
  {
    Lock lk(c.mu);
    file = c.rec_file;
  }
  journal(c, "start", {{"file", file}, {"pid", oj(shim::cmd("ping").at(0).at("pid"))}, {"gap", std::chrono::duration<double>(kRecGap).count()}});
  shim::cmd("spy", "on", "cmd");
  shim::cmd("spy", "clear");
  shim::cmd("req", "on");
  shim::cmd("req", "clear");
  Snap prev = snap(shim::cmd("tree"));
  ojson nodes = ojson::array();
  for (const json &n : prev.nodes)
    nodes.push_back(brief(n));
  journal(c, "tree", {{"nodes", std::move(nodes)}});
  std::map<std::string, std::vector<std::string>> gdi;
  while (true) {
    {
      std::unique_lock lk(c.mu);
      if (c.cv.wait_for(lk, kRecGap, [&] { return c.rec_halt; }))
        return;
    }
    const std::vector<json> ev = shim::cmd("spy", "dump");
    shim::cmd("spy", "clear");
    for (const json &e : shim::cmd("req", "drain"))
      journal(c, "req", oj(e));
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
      journal(c, "msg", r);
    }
    for (const json &n : cur.nodes) {
      const json *p = prev.get(n.at("hwnd").get<std::string>());
      if (!p) {
        journal(c, "new", {{"node", brief(n)}});
        continue;
      }
      ojson d = ojson::object();
      for (const char *k : kRecKeys)
        if (p->at(k) != n.at(k))
          d[k] = ojson::array({oj(p->at(k)), oj(n.at(k))});
      if (!d.empty() && (truthy(n.at("vis")) || truthy(p->at("vis"))))
        journal(c, "chg", {{"node", brief(n)}, {"diff", std::move(d)}});
    }
    for (const json &n : prev.nodes)
      if (!cur.get(n.at("hwnd").get<std::string>()))
        journal(c, "del", {{"node", brief(n)}});
    std::vector<std::pair<std::string, std::vector<std::string>>> now;
    for (const json &l : shim::cmd("gdidump")) {
      const std::string h = l.at("hwnd").get<std::string>();
      if (const json *n = cur.get(h); n && truthy(n->at("vis")))
        now.emplace_back(h, texts(l));
    }
    for (const auto &[h, tx] : now)
      if (const auto g = gdi.find(h); g == gdi.end() || g->second != tx) {
        const json &n = *cur.get(h);
        journal(c, "gdi", {{"hwnd", h}, {"cls", oj(n.at("class"))}, {"id", oj(n.at("id"))}, {"texts", tx}});
      }
    prev = cur;
    gdi = std::map<std::string, std::vector<std::string>>(now.begin(), now.end());
  }
}

// 出错记下即停录, 不重试
void rec_run(const std::shared_ptr<Core> &cp) {
  Core &c = *cp;
  try {
    rec_loop(c);
  } catch (const Fail &f) {
    journal(c, "err", {{"err", f.why}});
  }
  for (const char *a : {"spy", "req"})
    try {
      shim::cmd(a, "off");
    } catch (const Fail &) {
    }
  Lock lk(c.mu);
  put(c, "stop", ojson::object());
  c.rec_on = false;
  ++c.rec_ver;
}

void rec_toggle(const std::shared_ptr<Core> &cp) {
  Core &c = *cp;
  Lock lk(c.mu);
  if (c.rec_on) {
    c.rec_halt = true;
    c.cv.notify_all();
    return;
  }
  c.rec_on = true;
  c.rec_halt = false;
  c.rec_stamp = disk::stamp_now();
  c.rec_file = disk::path(disk::RunLiveTdxRecLog, {.stamp = c.rec_stamp}).string();
  c.rec_n = 0;
  c.rec_t0 = Clock::now();
  c.rec_tail.clear();
  c.rec_st.clear();
  ++c.rec_ver;
  std::thread([cp] { rec_run(cp); }).detach();
}

void rec_mark(Core &c, View &v) {
  const std::string text = trim(v.mark);
  Lock lk(c.mu);
  if (!c.rec_on) {
    c.rec_st = "未在录制";
    return;
  }
  put(c, "mark", {{"text", text.empty() ? "mark" : text}});
  v.mark.clear();
}

void req_set(Core &c, std::string s) {
  Lock lk(c.mu);
  c.req_st = std::move(s);
}

// 接入所选窗口的请求对象: 偏移空即由 shim 自动识别, 识别出多个候选时回各候选偏移
void req_attach(Core &c, const View &v) {
  if (!v.rec_on)
    return req_set(c, "✗ 请先开始录制");
  if (!v.sel || !v.sel->host.empty())
    return req_set(c, "✗ 请在窗口树中选择闪电下单顶层窗口");
  const std::string h = v.sel->hwnd, off = trim(v.req_off);
  act(c, [&c, h, off] {
    std::vector<json> r;
    try {
      r = off.empty() ? shim::cmd("req", "attach", h) : shim::cmd("req", "attach", h, off);
    } catch (const Fail &f) {
      return req_set(c, "✗ " + f.why);
    }
    if (std::any_of(r.begin(), r.end(), [](const json &x) { return x.contains("attached") && truthy(x["attached"]); }))
      return req_set(c, "✓ 已接入 " + h + "，请手动买入、卖出或撤单");
    std::string s;
    for (const json &x : r)
      if (x.contains("candidate") && truthy(x["candidate"]))
        s += (s.empty() ? "" : " / ") + plain(oj(x.at("offset")));
    req_set(c, "发现多个候选，请把对象偏移填为：" + s);
  });
}

bool shown_kind(const View &v, const std::string &k) {
  if (k == "msg")
    return v.show_msg;
  if (k == "req")
    return v.show_req;
  if (k == "new" || k == "del" || k == "chg")
    return v.show_win;
  if (k == "gdi")
    return v.show_gdi;
  return true;
}

} // namespace

void draw_rec(const std::shared_ptr<Core> &c, View &v) {
  ImGui::TextUnformatted("录制器（消息 / 内部请求 / 窗口树 / 绘制层 → jsonl）");
  if (ImGui::Button(v.rec_on ? "停止###rectoggle" : "开始###rectoggle"))
    rec_toggle(c);
  ImGui::SameLine();
  ImGui::TextDisabled("%s", v.rec_line.c_str());

  if (ImGui::Button("接入所选窗口"))
    req_attach(*c, v);
  ImGui::SameLine();
  ImGui::SetNextItemWidth(ImGui::GetFontSize() * 15);
  input_text("##reqoff", v.req_off, "对象偏移（自动识别失败时填写）");
  ImGui::SameLine();
  ImGui::TextDisabled("%s", v.req_st.c_str());

  ImGui::SetNextItemWidth(-ImGui::GetFontSize() * 4);
  if (input_text("##mark", v.mark, "打点说明（回车），如：买入挂单开始", ImGuiInputTextFlags_EnterReturnsTrue))
    rec_mark(*c, v);
  ImGui::SameLine();
  if (ImGui::Button("打点"))
    rec_mark(*c, v);

  ImGui::Checkbox("消息", &v.show_msg);
  ImGui::SameLine();
  ImGui::Checkbox("请求", &v.show_req);
  ImGui::SameLine();
  ImGui::Checkbox("窗口", &v.show_win);
  ImGui::SameLine();
  ImGui::Checkbox("绘制层", &v.show_gdi);
  ImGui::SameLine();
  ImGui::SetNextItemWidth(-ImGui::GetFontSize() * 4);
  input_text("##excl", v.excl, "排除含这些子串的行（逗号分隔），如 0x000007ea");

  std::vector<std::string> excl;
  for (size_t i = 0; i <= v.excl.size();) {
    const size_t j = std::min(v.excl.find(',', i), v.excl.size());
    if (std::string s = trim(std::string_view(v.excl).substr(i, j - i)); !s.empty())
      excl.push_back(std::move(s));
    i = j + 1;
  }
  std::vector<const std::string *> lines;
  for (const RecRow &r : v.rec_tail)
    if (shown_kind(v, r.kind) && std::none_of(excl.begin(), excl.end(), [&](const std::string &x) { return r.line.find(x) != std::string::npos; }))
      lines.push_back(&r.line);
  ImGui::SameLine();
  if (ImGui::Button("复制")) {
    std::string all;
    for (const std::string *l : lines)
      all += *l + "\n";
    ImGui::SetClipboardText(all.c_str());
  }

  ImGui::BeginChild("##reclog", ImVec2(0, 0), ImGuiChildFlags_Borders, ImGuiWindowFlags_HorizontalScrollbar);
  const bool at_end = ImGui::GetScrollY() >= ImGui::GetScrollMaxY() - 4;
  ImGuiListClipper clip;
  clip.Begin(static_cast<int>(lines.size()));
  while (clip.Step())
    for (int i = clip.DisplayStart; i < clip.DisplayEnd; ++i)
      ImGui::TextUnformatted(lines[static_cast<size_t>(i)]->c_str());
  if (at_end)
    ImGui::SetScrollHereY(1);
  ImGui::EndChild();
}

} // namespace live::tdx::console
