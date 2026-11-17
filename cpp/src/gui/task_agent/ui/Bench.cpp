// Bench — 见头文件
#include "gui/task_agent/ui/Bench.hpp"
#include "gui/task_agent/Mine.hpp"
#include "gui/task_agent/Text.hpp"
#include "gui/task_agent/tools/Kit.hpp"
#include "gui/task_agent/tools/Misc.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

#include "imgui.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <chrono>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace GUI::Agent {

using namespace ui;
using json = nlohmann::ordered_json;
using Clock = std::chrono::steady_clock;

struct Bench::Impl {
  Lab lab;
  Notes notes;
  std::mt19937_64 rng{std::random_device{}()};
  std::vector<catalog::PoolRow> pool_rows;
  // 快照: 一段存过的会话的前 k 轮 (空 = 空白)
  std::vector<Saved> saved;
  std::string snap_dir, snap_stamp;
  std::vector<Round> snap;
  int snap_k = 0;
  // 时间线 = 快照前 k 轮 + 工作台轮 (末轮); 假设板等照它折叠
  std::vector<Round> timeline{Round{.kind = Msg::Round}};
  State st;
  // 输入: 每个工具各一份
  Tool tool = Tool::ViewLayers;
  std::array<json, kToolCount> args;
  std::array<std::string, kToolCount> raw;
  bool json_mode = false;
  std::string error;
  std::string origin_pending; // 载入日志时那次调用当时的原文 (下一次 Run 带上作对照)
  std::vector<Seen> seen;
  // 工作台轮的调用
  std::vector<std::string> origin; // 与工作台轮的调用一一对应
  size_t pick = SIZE_MAX;
  std::optional<size_t> running;
  bool choosing = false;
  Gate gate;
  bool gpu = false;
  Clock::time_point t0 = Clock::now();
  LabLive live;

  double now() const { return std::chrono::duration<double>(Clock::now() - t0).count(); }
  int round() const { return static_cast<int>(timeline.size()); }
  std::vector<Act> &acts() { return timeline.back().acts; }

  void reset() {
    assert(!running);
    timeline.assign(snap.begin(), snap.begin() + snap_k);
    timeline.push_back(Round{.kind = Msg::Round});
    st = StateOf(timeline);
    origin.clear(), pick = SIZE_MAX;
  }

  Seat seat(size_t i) { return {lab, notes, gate, st, timeline, {round(), static_cast<int>(i) + 1}, catalog::label(lab.scope()), "", true}; }

  Feed feed() { return {lab.ctx(), lab.results(), st.board, timeline, {round(), static_cast<int>(acts().size()) + 1}, rng}; }

  void sample() {
    Feed f = feed();
    args[static_cast<size_t>(tool)] = SampleArgs(tool, f);
    raw[static_cast<size_t>(tool)] = args[static_cast<size_t>(tool)].dump(2);
    origin_pending.clear(), error.clear();
  }

  void end(size_t i) {
    Act &a = acts()[i];
    a.span.end(now());
    Apply(st, a, {round(), static_cast<int>(i) + 1});
    Clip(a.out);
    running.reset(), choosing = false;
  }

  void run(Call c) {
    acts().push_back({.call = std::move(c)});
    origin.push_back(std::move(origin_pending));
    origin_pending.clear();
    const size_t i = acts().size() - 1;
    Act &a = acts()[i];
    a.span.begin(now());
    pick = i;
    switch (def(a.call.tool).exec) {
    case Exec::Sync: {
      Seat s = seat(i);
      RunSync(a, s);
      end(i);
      break;
    }
    case Exec::Lab: {
      Seat s = seat(i);
      running = i;
      if (!SendLab(a, s, gpu))
        end(i);
      break;
    }
    case Exec::Human:
      a.extra.emplace<Asked>(QuestionsOf(a.call));
      running = i, choosing = true;
      break;
    }
  }

  void step(Act &a, LabStep s) {
    Trial *k = std::get_if<Trial>(&a.extra);
    if (!k)
      return;
    const double t = now();
    k->search.end(t), k->run.end(t);
    if (s == LabStep::Search)
      k->search.begin(t);
    else if (s == LabStep::Run)
      k->run.begin(t);
  }

  void poll() {
    while (std::optional<LabTick> t = lab.poll()) {
      if (!running || choosing)
        continue;
      const size_t i = *running;
      Act &a = acts()[i];
      step(a, t->step);
      if (t->step != LabStep::End)
        continue;
      if (std::holds_alternative<std::monostate>(t->out)) {
        if (Trial *k = std::get_if<Trial>(&a.extra); k && k->st.ok()) {
          lab.discard(k->st.file);
          k->st.file.clear();
          k->st.res = Result{.error = "取消了这次回测", .verdict = Verdict::Unstaged};
        }
        a.ok = false, a.out = "取消了这个调用, 没做完", a.res.reset();
      } else {
        SettleLab(a, std::move(t->out), gate);
      }
      end(i);
    }
    lab.live(live);
  }
};

Bench::Bench() : impl_(std::make_unique<Impl>()) {}
Bench::~Bench() = default;
void Bench::update() { impl_->poll(); }
bool Bench::busy() const { return impl_->lab.busy(); }

namespace {

bool long_field(const ToolDef &d, const ArgDef &a) {
  const std::string_view k = a.key;
  return d.cat == Cat::Hyp || d.cat == Cat::Note || k == "note" || k == "expr";
}

// 表单: 参数表逐项一个控件 (缺的按缺省补上); 返回 = 改了
bool form(const ToolDef &d, json &args) {
  bool changed = false;
  for (const ArgDef &a : d.args) {
    if (!args.contains(a.key))
      args[a.key] = a.def ? json::parse(a.def) : a.type == Arg::Int ? json(a.lo)
                                             : a.type == Arg::Str   ? json("")
                                                                    : json::array();
    json &v = args[a.key];
    ImGui::PushID(a.key);
    ImGui::TextColored(a.def ? ImGui::GetStyleColorVec4(ImGuiCol_TextDisabled) : kColHead, "%s %s", a.key, a.cn);
    tip_long(ArgText(a));
    ImGui::SameLine(ImGui::GetFontSize() * 9.0f);
    ImGui::SetNextItemWidth(-1);
    switch (a.type) {
    case Arg::Str: {
      std::string s = v.is_string() ? v.get<std::string>() : std::string();
      if (a.choices) {
        std::vector<std::string> opts;
        if (a.def && std::string_view(a.def) == "\"\"")
          opts.emplace_back();
        for (std::string_view c = a.choices;;) {
          const size_t bar = c.find('|');
          opts.emplace_back(c.substr(0, bar));
          if (bar == std::string_view::npos)
            break;
          c.remove_prefix(bar + 1);
        }
        if (ImGui::BeginCombo("##v", s.empty() ? "(空)" : s.c_str())) {
          for (const std::string &o : opts)
            if (ImGui::Selectable(o.empty() ? "(空)" : o.c_str(), o == s))
              v = o, changed = true;
          ImGui::EndCombo();
        }
      } else if (long_field(d, a) ? input_multiline("##v", s, ImVec2(-1, ImGui::GetTextLineHeightWithSpacing() * 2.6f)) : input_text("##v", s)) {
        v = s, changed = true;
      }
      break;
    }
    case Arg::Int: {
      int x = v.is_number_integer() ? v.get<int>() : a.lo;
      if (ImGui::InputInt("##v", &x))
        v = std::clamp(x, a.lo, a.hi), changed = true;
      break;
    }
    case Arg::Strs: {
      std::string s;
      if (v.is_array())
        for (const json &x : v)
          s += (s.empty() ? "" : "\n") + (x.is_string() ? x.get<std::string>() : x.dump());
      if (input_multiline("##v", s, ImVec2(-1, ImGui::GetTextLineHeightWithSpacing() * 2.6f))) {
        json arr = json::array();
        for (size_t i = 0; i <= s.size();) {
          const size_t e = std::min(s.find('\n', i), s.size());
          if (const std::string_view line = text::trim(std::string_view(s).substr(i, e - i)); !line.empty())
            arr.push_back(std::string(line));
          i = e + 1;
        }
        v = std::move(arr), changed = true;
      }
      tip("一行一项");
      break;
    }
    case Arg::Questions: {
      std::string s = v.dump(2);
      if (input_multiline("##v", s, ImVec2(-1, ImGui::GetTextLineHeightWithSpacing() * 8.0f)))
        if (const json p = json::parse(s, nullptr, false); !p.is_discarded())
          v = p, changed = true;
      tip("JSON 数组, 每题 {prompt, options, multiple, recommended}");
      break;
    }
    }
    ImGui::PopID();
  }
  return changed;
}

std::string call_line(Tool t, const json &args) { return Call{t, args}.text(80); }

} // namespace

void Bench::draw(SharedData &data, Scope &sc, const Gate &gate, bool gpu, bool blocked) {
  Impl &b = *impl_;
  const bool held = b.running.has_value() || b.lab.busy();
  // ---- 作用域 (换了就重绑, 快照与工作台的调用清空) ----
  if (!held && b.lab.scope() != sc && !catalog::holds(data, sc.level).empty()) {
    b.lab.bind(data, sc, false);
    if (!b.notes.loaded(false))
      b.notes.load(false);
    b.snap.clear(), b.snap_dir.clear(), b.snap_stamp.clear(), b.snap_k = 0;
    b.reset();
  }
  scope_row(data, sc, b.pool_rows, b.lab, held, "作用域:");
  const bool bound = b.lab.scope() == sc;
  if (!bound) {
    ImGui::TextDisabled("该作用域未就绪 (无可搜持有期), 或调用在跑");
    return;
  }
  const Context &ctx = b.lab.ctx();

  // ---- 快照 ----
  ImGui::TextColored(kColHead, "快照:");
  ImGui::SameLine();
  ImGui::SetNextItemWidth(320);
  ImGui::BeginDisabled(held);
  const std::string snap_label = b.snap_dir.empty() ? std::string("空白 (无假设)") : stamp_text(b.snap_stamp);
  if (ImGui::BeginCombo("##snap", snap_label.c_str())) {
    if (ImGui::IsWindowAppearing())
      b.saved = SavedSessions(sc);
    if (ImGui::Selectable("空白 (无假设)", b.snap_dir.empty())) {
      b.snap.clear(), b.snap_dir.clear(), b.snap_stamp.clear(), b.snap_k = 0;
      b.reset();
    }
    for (const Saved &x : b.saved) {
      const std::string label = stamp_text(x.stamp) + (x.test ? " · 自检" : "") + " · " + std::to_string(x.brief.at("rounds").get<int>()) + " 轮 · 假设 " +
                                std::to_string(x.brief.at("hyps").get<int>()) + "###" + x.dir;
      if (ImGui::Selectable(label.c_str(), x.dir == b.snap_dir)) {
        b.snap = TimelineOf(x.dir), b.snap_dir = x.dir, b.snap_stamp = x.stamp;
        b.snap_k = static_cast<int>(b.snap.size());
        b.reset();
      }
    }
    ImGui::EndCombo();
  }
  tip("会话类工具 (假设 / 判定 / 回测挂假设 / 提交 / 记录) 跑在快照上: 取这段会话前 k 轮的假设板, 工作台自己是第 k+1 轮; 只在内存, 不落盘");
  if (!b.snap.empty()) {
    ImGui::SameLine();
    ImGui::SetNextItemWidth(110);
    if (int k = b.snap_k; ImGui::InputInt("前 k 轮##snap_k", &k)) {
      b.snap_k = std::clamp(k, 0, static_cast<int>(b.snap.size()));
      b.reset();
    }
  }
  ImGui::SameLine();
  if (ImGui::SmallButton("清空工作台调用"))
    b.reset();
  ImGui::EndDisabled();
  tip("丢掉工作台轮的调用 (假设板回到快照; 已落的候选文件不动)");
  ImGui::SameLine();
  ImGui::TextDisabled("· 工作台 = 第 %d 轮 · 假设 %zu · 门槛 mono ≥ %.2f, rIC ≥ %.3f (Config 页)", b.round(), b.st.board.all().size(), gate.mono, gate.ric);
  ImGui::Separator();

  // ---- 左: 工具按类 ----
  ImGui::BeginChild("##bench_tools", ImVec2(ImGui::GetFontSize() * 15.0f, -1), ImGuiChildFlags_Borders);
  for (size_t k = 0; k < kNum<Cat>; ++k) {
    const Cat cat = static_cast<Cat>(k);
    if (!ImGui::TreeNodeEx(def(cat).cn, ImGuiTreeNodeFlags_DefaultOpen | ImGuiTreeNodeFlags_SpanAvailWidth))
      continue;
    for (const ToolDef &d : Tools()) {
      if (d.cat != cat)
        continue;
      if (ImGui::Selectable((std::string(d.name) + "##tool").c_str(), b.tool == d.id)) {
        b.tool = d.id, b.error.clear();
        if (b.args[static_cast<size_t>(d.id)].is_null())
          b.sample();
      }
      ImGui::SameLine(ImGui::GetFontSize() * 9.5f);
      ImGui::TextDisabled("%s", d.cn);
    }
    ImGui::TreePop();
  }
  ImGui::EndChild();
  ImGui::SameLine();

  // ---- 右: 输入 / 展示 / 原文 ----
  ImGui::BeginChild("##bench_page", ImVec2(0, -1));
  const ToolDef &d = def(b.tool);
  const size_t ti = static_cast<size_t>(b.tool);
  if (b.args[ti].is_null())
    b.sample();
  ImGui::TextColored(kColHead, "%s — %s", d.name, d.cn);
  ImGui::SameLine();
  ImGui::TextDisabled("(%s)", def(d.exec).cn);
  wrapped_dim(ToolText(d.desc));
  tip_long("返回: " + ToolText(d.out));

  ImGui::SeparatorText("输入");
  if (ImGui::Button("随机"))
    b.sample();
  tip("按本作用域取材 (开放的叶 / 候选名 / 评估期交易日 / 池内代码 / 假设板) 出一组参数");
  ImGui::SameLine();
  if (ImGui::Button("载入日志")) {
    b.seen = SeenCalls(sc, b.tool);
    ImGui::OpenPopup("##seen");
  }
  tip("本作用域存过的会话里 agent 真调过的这个工具: 载入参数, Run 后可对照当时交给 agent 的原文");
  if (ImGui::BeginPopup("##seen")) {
    if (b.seen.empty())
      ImGui::TextDisabled("本作用域的会话里没调过 %s", d.name);
    for (size_t i = 0; i < b.seen.size() && i < 200; ++i) {
      const Seen &x = b.seen[b.seen.size() - 1 - i];
      const std::string label = stamp_text(x.stamp) + (x.test ? " · 自检" : "") + " · " + RefText(x.at) + (x.ok ? "" : " · 没做成") + " · " + call_line(b.tool, x.args) +
                                "###seen" + std::to_string(i);
      if (ImGui::Selectable(label.c_str())) {
        b.args[ti] = x.args, b.raw[ti] = x.args.dump(2), b.origin_pending = x.out, b.error.clear();
        ImGui::CloseCurrentPopup();
      }
      tip_long(x.out);
    }
    ImGui::EndPopup();
  }
  ImGui::SameLine();
  if (ImGui::Checkbox("JSON", &b.json_mode))
    b.raw[ti] = b.args[ti].dump(2);
  ImGui::SameLine();
  const bool needs_scope = d.cat == Cat::Look || d.id == Tool::EvalFactor;
  const bool can_run = !held && !blocked && (!needs_scope || !ctx.days.empty());
  ImGui::BeginDisabled(!can_run);
  if (ImGui::Button("Run", ImVec2(70, 0))) {
    std::vector<std::string> errs;
    Call c;
    if (CallOf(b.tool, b.args[ti], "args", c, errs)) {
      b.error.clear(), b.gate = gate, b.gpu = gpu;
      b.run(std::move(c));
    } else {
      b.error = text::join(errs, "\n");
    }
  }
  ImGui::EndDisabled();
  tip(blocked ? "Mine 页的会话在走 (不并跑)" : needs_scope && ctx.days.empty() ? "本作用域评估期未就绪"
                                                                               : "按参数表校验后执行 (与会话同一条路径)");
  ImGui::SameLine();
  ImGui::BeginDisabled(!b.lab.busy());
  if (ImGui::Button("Cancel", ImVec2(70, 0)))
    b.lab.cancel();
  ImGui::EndDisabled();
  if (b.running && !b.choosing) {
    ImGui::SameLine();
    ImGui::ProgressBar(static_cast<float>(b.live.frac), ImVec2(140, 0));
  }
  if (!b.origin_pending.empty()) {
    ImGui::SameLine();
    ImGui::TextColored(color(TaskStatus::Kind::Warn), "载入自日志 (Run 后可对照当时的原文)");
  }
  ImGui::BeginChild("##bench_in", ImVec2(0, ImGui::GetTextLineHeightWithSpacing() * 10.0f), ImGuiChildFlags_Borders);
  if (b.json_mode) {
    if (input_multiline("##raw", b.raw[ti], ImVec2(-1, -1))) {
      const json p = json::parse(b.raw[ti], nullptr, false);
      if (!p.is_discarded() && p.is_object())
        b.args[ti] = p, b.error.clear();
      else
        b.error = "JSON 不合法 (须是参数对象)";
    }
  } else if (form(d, b.args[ti])) {
    b.raw[ti] = b.args[ti].dump(2), b.error.clear();
  }
  ImGui::EndChild();
  if (!b.error.empty())
    wrapped(color(TaskStatus::Kind::Error), b.error);

  // ---- 工作台轮的调用 ----
  std::vector<Act> &as = b.acts();
  ImGui::SeparatorText("调用");
  if (as.empty()) {
    ImGui::TextDisabled("还没有调用: 随机或载入一组参数, 按 Run");
    ImGui::EndChild();
    return;
  }
  if (b.pick >= as.size())
    b.pick = as.size() - 1;
  ImGui::SetNextItemWidth(-1);
  const auto label_of = [&](size_t i) {
    const Act &a = as[i];
    const std::string state = !a.span.ended() ? " · 执行中…" : (a.ok ? "" : " · 没做成") + std::string(" · ") + dur(a.span.len(a.span.t1));
    return RefText({b.round(), static_cast<int>(i) + 1}) + " " + a.call.text(80) + state;
  };
  if (ImGui::BeginCombo("##acts", label_of(b.pick).c_str())) {
    for (size_t i = as.size(); i-- > 0;)
      if (ImGui::Selectable((label_of(i) + "###act" + std::to_string(i)).c_str(), i == b.pick))
        b.pick = i;
    ImGui::EndCombo();
  }
  Act &a = as[b.pick];
  const Ref ref{b.round(), static_cast<int>(b.pick) + 1};
  const float w = ImGui::GetContentRegionAvail().x;
  ImGui::BeginChild("##bench_show", ImVec2(w * 0.58f, -1), ImGuiChildFlags_Borders);
  ImGui::TextColored(kColHead, "展示");
  if (b.choosing && b.running == b.pick) {
    if (asked_block(std::get<Asked>(a.extra), true)) {
      Put(a, AskOut(std::get<Asked>(a.extra)));
      b.end(b.pick);
    }
  } else if (!a.span.ended()) {
    ImGui::TextColored(color(TaskStatus::Kind::Busy), "执行中… %.0f%%", b.live.frac * 100.0);
  } else {
    if (!a.ok)
      wrapped(color(TaskStatus::Kind::Warn), a.res ? "没做成 (原文见右)" : a.out);
    if (a.res)
      a.res->draw();
  }
  ImGui::EndChild();
  ImGui::SameLine();
  ImGui::BeginChild("##bench_out", ImVec2(0, -1), ImGuiChildFlags_Borders);
  ImGui::TextColored(kColHead, "原文 (交给 agent 的)");
  if (a.span.ended()) {
    const std::string title = "## " + CallTitle(a, ref);
    ImGui::SameLine();
    ImGui::TextDisabled("%s", chars(a.out).c_str());
    copy_button("复制##out", title + "\n\n" + a.out);
    wrapped_dim(title);
    if (!a.gist.empty())
      wrapped(color(TaskStatus::Kind::Ready), "研究树里留的关键数: " + a.gist);
    box(a.out, false, 60.0f, false);
    if (b.pick < b.origin.size() && !b.origin[b.pick].empty())
      fold("origin", ImGuiTreeNodeFlags_None, "日志里当时交给 agent 的原文 · " + chars(b.origin[b.pick]), b.origin[b.pick], true, kBoxLines, false);
  }
  ImGui::EndChild();
  ImGui::EndChild();
}

} // namespace GUI::Agent
