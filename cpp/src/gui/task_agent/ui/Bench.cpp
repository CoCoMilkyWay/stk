// Bench — 见头文件
#include "gui/task_agent/ui/Bench.hpp"
#include "gui/task_agent/Mine.hpp"
#include "gui/task_agent/Prompt.hpp"
#include "gui/task_agent/Text.hpp"
#include "gui/task_agent/tools/Kit.hpp"
#include "gui/task_agent/tools/Misc.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

#include "imgui.h"
#include "nlohmann/json.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace GUI::Agent {

using namespace ui;
using json = nlohmann::ordered_json;
using Clock = std::chrono::steady_clock;

struct Bench::Impl : Rig {
  std::mt19937_64 rng{std::random_device{}()};
  // 输入: 每个工具各一份
  Tool tool = Tool::ViewLayers;
  Per<Tool, json> args;
  Per<Tool, std::string> raw;
  bool json_mode = false;
  std::string json_err;
  std::vector<Seen> seen;
  // 载入的日志调用: 快照 = 它所在会话在它之前的时间线 (前几轮 + 本轮它之前的调用; 没有 = 空白); 输入还是它的参数时, 原文栏对照它当时的原文
  struct Loaded {
    Tool tool;
    Seen at;
  };
  std::optional<Loaded> loaded;
  std::vector<Round> snap;
  // 工作台轮的调用: 前 base 个是快照里本轮已有的
  size_t base = 0;
  std::vector<std::string> origin; // 与工作台轮的调用一一对应 (日志里当时的原文; 空 = 不是载入的)
  std::optional<size_t> running;
  bool choosing = false;
  Gate gate;
  bool gpu = false;
  Clock::time_point t0 = Clock::now();
  LabLive live;

  Impl() { timeline.push_back(Round{.kind = Msg::Round}); }

  double now() const { return std::chrono::duration<double>(Clock::now() - t0).count(); }
  bool held() const { return running.has_value() || lab.busy(); }
  std::vector<Act> &acts() { return timeline.back().acts; }
  Seat bench_seat(size_t i) { return seat(i, gate, {}, true, gpu); }

  void reset() {
    assert(!held());
    flush();
    if (snap.empty())
      timeline.assign(1, Round{.kind = Msg::Round});
    else
      timeline = snap;
    restate();
    base = acts().size();
    origin.assign(base, {});
  }

  void clear() {
    snap.clear(), loaded.reset();
    reset();
  }

  void load(const Seen &x) {
    std::vector<Round> rs = TimelineOf(x.dir);
    assert(x.at.round >= 1 && static_cast<size_t>(x.at.round) <= rs.size() && x.at.call >= 1);
    rs.resize(static_cast<size_t>(x.at.round));
    rs.back().acts.resize(static_cast<size_t>(x.at.call - 1));
    snap = std::move(rs), loaded = Loaded{tool, x};
    reset();
    args[tool] = x.args, raw[tool] = x.args.dump(2), json_err.clear();
  }

  Feed feed() { return {lab.ctx(), lab.results(), st.board, timeline, {round(), static_cast<int>(acts().size()) + 1}, rng}; }

  void sample() {
    Feed f = feed();
    args[tool] = SampleArgs(tool, f);
    raw[tool] = args[tool].dump(2);
    json_err.clear();
  }

  void run(Call c, std::string org) {
    acts().push_back({.call = std::move(c)});
    origin.push_back(std::move(org));
    const size_t i = acts().size() - 1;
    Seat s = bench_seat(i);
    const Exec at = open_act(i, s, now());
    if (at != Exec::Sync)
      running = i, choosing = at == Exec::Human;
  }

  void abort() {
    if (!running) {
      lab.cancel();
      return;
    }
    const size_t i = *running;
    running.reset(), choosing = false;
    abort_act(i, now());
  }

  void poll() {
    while (std::optional<Lab::Tick> t = lab.poll()) {
      if (!running || choosing)
        continue;
      const size_t i = *running;
      if (t->step != LabStep::End) {
        step_act(i, t->step, now());
        continue;
      }
      if (t->out.cancelled) {
        abort_act(i, now());
      } else {
        Seat s = bench_seat(i);
        settle_act(i, std::move(t->out.out), s, now());
      }
      running.reset();
    }
    flush();
    lab.live(live);
  }
};

Bench::Bench() : impl_(std::make_unique<Impl>()) {}
Bench::~Bench() = default;
void Bench::update() { impl_->poll(); }
bool Bench::busy() const { return impl_->held(); }
std::optional<Tool> Bench::running() const {
  const Impl &b = *impl_;
  return b.running ? std::optional(b.timeline.back().acts[*b.running].call.tool) : std::nullopt;
}

namespace {

bool long_field(Tool t, const ArgDef &a) {
  const Cat cat = def(t).cat;
  const std::string_view k = a.key;
  return cat == Cat::Hyp || cat == Cat::Note || k == "note" || k == "expr";
}

// 表单: 参数表逐项一个控件 (缺的按 SeedJson 补上); 返回 = 改了
bool form(Tool t, json &args) {
  bool changed = false;
  for (const ArgDef &a : doc(t).args) {
    if (!args.contains(a.key))
      args[a.key] = SeedJson(a);
    json &v = args[a.key];
    ImGui::PushID(a.key);
    ImGui::TextColored(!a.required() ? ImGui::GetStyleColorVec4(ImGuiCol_TextDisabled) : kColHead, "%s %s", a.key, a.cn);
    tip_long(ArgText(a));
    ImGui::SameLine(ImGui::GetFontSize() * 9.0f);
    ImGui::SetNextItemWidth(-1);
    switch (a.type) {
    case Arg::Str: {
      std::string s = v.is_string() ? v.get<std::string>() : std::string();
      if (!a.choices.empty()) {
        if (ImGui::BeginCombo("##v", s.empty() ? "(空)" : s.c_str())) {
          if (a.empty_ok() && ImGui::Selectable("(空)", s.empty()))
            v = "", changed = true;
          for (const std::string_view o : a.choices) {
            const std::string opt(o);
            if (ImGui::Selectable(opt.c_str(), o == s))
              v = opt, changed = true;
          }
          ImGui::EndCombo();
        }
      } else if (long_field(t, a) ? input_multiline("##v", s, ImVec2(-1, ImGui::GetTextLineHeightWithSpacing() * 2.6f)) : input_text("##v", s)) {
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

// 文本框填满所在栏余下的高 (下面另留 keep 行)
float fill_lines(float keep) { return std::max(3.0f, ImGui::GetContentRegionAvail().y / ImGui::GetTextLineHeightWithSpacing() - keep); }

} // namespace

void Bench::draw(const SharedData &data, Tool tool, const Scope &sc, const Gate &gate, bool gpu, bool blocked) {
  Impl &b = *impl_;
  if (b.tool != tool)
    b.tool = tool, b.json_err.clear();
  const bool held = b.held();
  // ---- 作用域 = Mine 页的那份 (不在这里改; 变了就重绑, 快照与工作台的调用清空); 不能用的只给警告 ----
  const auto warn = [](const std::string &s) { wrapped(color(TaskStatus::Kind::Warn), s); };
  const std::string label = "作用域 (Mine 页) " + catalog::label(sc) + ": ";
  const std::vector<Choice> hs = catalog::holds(data, sc.level);
  if (hs.empty() || std::none_of(hs.begin(), hs.end(), [&](const Choice &h) { return h.key == sc.hold; })) {
    warn(label + (hs.empty() ? "该层无可搜持有期 (标签未建)" : "持有期在该层不可搜") + "; 到 Mine 页改");
    return;
  }
  if (!held && b.lab.scope() != sc) {
    b.lab.bind(data, sc, false);
    if (!b.notes.loaded(false))
      b.notes.load(false);
    b.clear();
  }
  if (b.lab.scope() != sc) {
    warn(label + "与工作台在跑的调用所用的不同, 等它做完或取消");
    return;
  }
  const Context &ctx = b.lab.ctx();
  if (ctx.days.empty())
    warn(label + "评估期未就绪 (开放的叶与标签库没有共同的落盘日): 观察与回测不能跑");
  if (ctx.leaves.empty())
    warn(label + "没有开放的特征叶 (Mine 页特征页一个没勾, 或本层无叶)");
  if (ctx.ops.empty())
    warn(label + "没有开放的算子 (Mine 页算子页一个没勾)");

  // ---- 工作台: 第几轮 / 快照 / 假设板 / 门槛 ----
  ImGui::TextColored(kColHead, "工作台:");
  ImGui::SameLine();
  const std::string at = b.loaded ? "快照 = " + stamp_text(b.loaded->at.stamp) + (b.loaded->at.test ? " 自检" : "") + " 会话 " + RefText(b.loaded->at.at) + " 之前" : "空白";
  ImGui::TextDisabled("第 %d 轮 · %s · 假设 %zu · 门槛 mono ≥ %.2f, rIC ≥ %.3f (Config 页)", b.round(), at.c_str(), b.st.board.all().size(), gate.mono, gate.ric);
  tip(("作用域 = Mine 页的 " + catalog::label(sc) + " · 评估期 " + ctx.window +
       "\n会话类工具 (假设 / 判定 / 回测挂假设 / 提交 / 记录) 跑在快照上: 输入栏载入日志里的一次调用, 就取那段会话在它之前的时间线 (前几轮 + 本轮它之前的调用) 折出假设板,\n"
       "工作台接着那一轮往下编号 (Run 的调用编号与日志里的相同, 可作 judge 的证据); 只在内存, 不落盘")
          .c_str());
  ImGui::SameLine();
  ImGui::BeginDisabled(held);
  if (ImGui::SmallButton("清空"))
    b.clear();
  ImGui::EndDisabled();
  tip("丢掉工作台的调用与快照, 回到空白的第 1 轮 (已落的候选文件不动)");

  const ToolMeta &m = def(b.tool);
  const ToolDoc &d = doc(b.tool);
  ImGui::TextColored(kColHead, "%s — %s", m.id, m.cn);
  ImGui::SameLine();
  ImGui::TextDisabled("(%s)", def(m.exec).cn);
  wrapped_dim(ToolText(d.desc));
  tip_long("返回: " + ToolText(d.out));

  // ---- 当前输入对应的调用: 校验 + 工作台里跑过同样参数的最近一次 (在问研究员的 ask_human 恒是它) ----
  json &in = b.args[b.tool];
  if (in.is_null())
    b.sample();
  std::vector<std::string> errs;
  Call cur;
  const bool valid = b.json_err.empty() && CallOf(b.tool, in, "args", cur, errs);
  std::optional<size_t> hit;
  if (b.choosing && b.acts()[*b.running].call.tool == b.tool)
    hit = b.running;
  else if (valid)
    for (size_t i = b.acts().size(); i-- > b.base && !hit;)
      if (const Call &c = b.acts()[i].call; c.tool == b.tool && c.args == cur.args)
        hit = i;
  const bool from_log = valid && b.loaded && b.loaded->tool == b.tool && b.loaded->at.args == cur.args;

  const float top = std::max(ImGui::GetContentRegionAvail().y * 0.4f, ImGui::GetTextLineHeightWithSpacing() * 12.0f);

  // ---- 输入 (左): 随机 / JSON / 载入日志 → Run · Cancel · 调用 → 表单 ----
  ImGui::BeginChild("##bench_in", ImVec2(ImGui::GetContentRegionAvail().x * 0.5f, top), ImGuiChildFlags_Borders);
  ImGui::TextColored(kColHead, "输入");
  ImGui::SameLine();
  if (ImGui::SmallButton("随机"))
    b.sample();
  tip("按本作用域取材出一组用得上的参数: 开放的叶 / 候选名 (没有就拿本层的叶套开放的归一根) / 本层的行与时段 / 当日有逐笔的票 (工作台里 view_trades 列过的段优先) / 假设板");
  ImGui::SameLine();
  if (ImGui::Checkbox("JSON", &b.json_mode))
    b.raw[b.tool] = in.dump(2), b.json_err.clear();
  ImGui::SameLine();
  ImGui::BeginDisabled(held);
  ImGui::SetNextItemWidth(-1);
  const std::string log_label = from_log ? "日志 " + stamp_text(b.loaded->at.stamp) + " · " + RefText(b.loaded->at.at) : std::string("载入日志…");
  std::optional<Seen> pick; // 选中的日志调用: 本帧画完再换快照 (换了时间线, 上面算的 hit 就作废)
  if (ImGui::BeginCombo("##seen", log_label.c_str(), ImGuiComboFlags_HeightLarge)) {
    if (ImGui::IsWindowAppearing())
      b.seen = SeenCalls(sc, b.tool);
    if (b.seen.empty())
      ImGui::TextDisabled("本作用域的会话里没调过 %s", m.id);
    for (size_t i = 0; i < b.seen.size() && i < 200; ++i) {
      const Seen &x = b.seen[b.seen.size() - 1 - i];
      const std::string label = stamp_text(x.stamp) + (x.test ? " · 自检" : "") + " · " + RefText(x.at) + (x.ok ? "" : " · 没做成") + " · " + call_line(b.tool, x.args) +
                                "###seen" + std::to_string(i);
      if (ImGui::Selectable(label.c_str()))
        pick = x;
      tip_long(x.out);
    }
    ImGui::EndCombo();
  }
  ImGui::EndDisabled();
  tip("本作用域存过的会话里 agent 真调过的这个工具: 载入参数, 工作台换成那段会话在这次调用之前的样子 (快照); 原文栏先给它当时交给 agent 的那段, Run 后两相对照");

  const bool needs_scope = m.cat == Cat::Look || b.tool == Tool::EvalFactor;
  const bool can_run = !held && !blocked && valid && (!needs_scope || !ctx.days.empty());
  ImGui::BeginDisabled(!can_run);
  if (ImGui::Button("Run", ImVec2(60, 0))) {
    b.gate = gate, b.gpu = gpu;
    b.run(cur, from_log ? b.loaded->at.out : std::string());
  }
  ImGui::EndDisabled();
  tip(blocked                           ? "Mine 页的会话在走 (不并跑)"
      : needs_scope && ctx.days.empty() ? "本作用域评估期未就绪"
      : !valid                          ? "参数不合 (见下)"
                                        : "按参数表校验后执行 (与会话同一条路径)");
  ImGui::SameLine();
  ImGui::BeginDisabled(!held);
  if (ImGui::Button("Cancel", ImVec2(60, 0)))
    b.abort();
  ImGui::EndDisabled();
  if (b.running && !b.choosing) {
    ImGui::SameLine();
    ImGui::ProgressBar(static_cast<float>(b.live.frac), ImVec2(90, 0));
  }
  std::vector<Act> &as = b.acts();
  const auto label_of = [&](size_t i) {
    const Act &a = as[i];
    const std::string state = !a.span.ended() ? " · 执行中…" : (a.out.ok ? "" : " · 没做成") + std::string(" · ") + dur(a.span.len(a.span.t1));
    return RefText({b.round(), static_cast<int>(i) + 1}) + " " + a.call.text(80) + state;
  };
  ImGui::SameLine();
  ImGui::SetNextItemWidth(-1);
  const std::string preview = !valid ? std::string("(参数不合)") : hit ? label_of(*hit)
                                                                       : "(未跑) " + cur.text(80);
  if (ImGui::BeginCombo("##acts", preview.c_str(), ImGuiComboFlags_HeightLarge)) {
    bool any = false;
    for (size_t i = as.size(); i-- > b.base;) {
      if (as[i].call.tool != b.tool)
        continue;
      any = true;
      if (ImGui::Selectable((label_of(i) + "###act" + std::to_string(i)).c_str(), hit == i))
        in = as[i].call.args, b.raw[b.tool] = in.dump(2), b.json_err.clear();
    }
    if (!any)
      ImGui::TextDisabled("工作台里还没跑过 %s", m.id);
    ImGui::EndCombo();
  }
  tip("调用 = 当前输入: 工作台里跑过同样参数的显示它的编号与结局, 原文与展示随之; 下拉选跑过的一次即载入它的参数");
  if (!b.json_err.empty() || !errs.empty())
    wrapped(color(TaskStatus::Kind::Error), b.json_err.empty() ? text::join(errs, "\n") : b.json_err);
  ImGui::BeginChild("##form", ImVec2(0, 0));
  if (b.json_mode) {
    if (input_multiline("##raw", b.raw[b.tool], ImVec2(-1, -1))) {
      const json p = json::parse(b.raw[b.tool], nullptr, false);
      if (!p.is_discarded() && p.is_object())
        in = p, b.json_err.clear();
      else
        b.json_err = "JSON 不合法 (须是参数对象)";
    }
  } else if (form(b.tool, in)) {
    b.raw[b.tool] = in.dump(2), b.json_err.clear();
  }
  ImGui::EndChild();
  ImGui::EndChild();

  // ---- 原文 (右): 交给 agent 的那段 (+ 日志里当时的) ----
  ImGui::SameLine();
  ImGui::BeginChild("##bench_out", ImVec2(0, top), ImGuiChildFlags_Borders);
  ImGui::TextColored(kColHead, "原文 (交给 agent 的)");
  if (hit && as[*hit].span.ended()) {
    const Act &a = as[*hit];
    const std::string title = "## " + CallTitle(a, {b.round(), static_cast<int>(*hit) + 1});
    const std::string &org = b.origin[*hit];
    ImGui::SameLine();
    ImGui::TextDisabled("%s", chars(a.out.text).c_str());
    copy_button("复制##out", title + "\n\n" + a.out.text);
    wrapped_dim(title);
    if (!a.out.gist.empty())
      wrapped(color(TaskStatus::Kind::Ready), "研究树里留的关键数: " + a.out.gist);
    box(a.out.text, false, fill_lines(org.empty() ? 0.5f : 2.0f), false);
    if (!org.empty())
      fold("origin", ImGuiTreeNodeFlags_None, "日志里当时交给 agent 的原文 · " + chars(org), org, true, kBoxLines, false);
  } else if (from_log) {
    const Seen &x = b.loaded->at;
    ImGui::SameLine();
    ImGui::TextDisabled("%s", chars(x.out).c_str());
    copy_button("复制##log", x.out);
    wrapped_dim("日志里 " + RefText(x.at) + " 当时交给 agent 的 (工作台还没跑; 按 Run 重跑对照)");
    box(x.out, true, fill_lines(0.5f), false);
  } else {
    ImGui::TextDisabled("%s", hit ? "执行中…" : "这组参数还没跑");
  }
  ImGui::EndChild();

  // ---- 展示 (底, 全宽): 该工具独有的界面 ----
  ImGui::BeginChild("##bench_show", ImVec2(0, -1), ImGuiChildFlags_Borders);
  ImGui::TextColored(kColHead, "展示");
  if (!hit) {
    ImGui::TextDisabled("%s", valid ? "这组参数还没跑: 按 Run" : "参数不合, 改好再 Run");
  } else if (Act &a = as[*hit]; b.choosing && b.running == hit) {
    if (edit_asked(AsAsk(a)->asked)) {
      b.answer_act(*hit, b.now());
      b.running.reset(), b.choosing = false;
    }
  } else if (!a.span.ended()) {
    ImGui::TextColored(color(TaskStatus::Kind::Busy), "执行中… %.0f%%", b.live.frac * 100.0);
  } else {
    if (!a.out.ok)
      wrapped(color(TaskStatus::Kind::Warn), a.out.res ? "没做成 (原文见上)" : a.out.text);
    if (a.out.res)
      a.out.res->draw();
  }
  ImGui::EndChild();
  if (pick)
    b.load(*pick);
}

} // namespace GUI::Agent
