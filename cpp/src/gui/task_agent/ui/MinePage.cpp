// Mine 页 — 见 gui/task_agent/TaskAgent.hpp
#include "gui/task_agent/Prompt.hpp"
#include "gui/task_agent/Text.hpp"
#include "gui/task_agent/tools/All.hpp"
#include "gui/task_agent/ui/Pages.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdio>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

namespace GUI::Agent::page {

using namespace ui;

namespace {

// 作用域 (含开放的叶 / 算子; Mine 与 Tools 页共用一份) 锁着: Mine 页有会话, 或 Tools 页有调用在跑
bool scope_locked(const Ui &s) { return s.mine->held() || bench_busy(s); }

// 思考在想什么: 末个独占一行的 **小标题** / # 标题; 没有则 (live) 末个非空行的开头
constexpr size_t kTopicChars = 48;
std::string topic(std::string_view s, bool live) {
  std::string_view tail;
  for (size_t end = s.size(); end > 0;) {
    const size_t nl = s.rfind('\n', end - 1);
    const size_t b = nl == std::string_view::npos ? 0 : nl + 1;
    const std::string_view line = text::trim(s.substr(b, end - b));
    end = b == 0 ? 0 : nl;
    if (line.empty())
      continue;
    if (line.size() > 4 && line.starts_with("**") && line.ends_with("**"))
      return text::head(text::trim(line.substr(2, line.size() - 4)), kTopicChars);
    if (const size_t h = line.find_first_not_of('#'); h > 0 && h <= 6 && h < line.size() && line[h] == ' ')
      return text::head(text::trim(line.substr(h)), kTopicChars);
    if (tail.empty())
      tail = line;
  }
  return live ? text::head(tail, kTopicChars) : std::string();
}

// 在收那一步此刻的样子 (每帧量): 等首包 → 思考 → 正文; quiet = 多久没来新增量
constexpr double kQuietS = 20.0; // 静默超过这么久标黄
struct Flow {
  enum Stage : uint8_t { Idle,
                         Wait,
                         Think,
                         Write } stage = Idle;
  double since = 0.0, quiet = 0.0, writing = 0.0; // 本条发出至今 / 距上个增量 / 正文已写
  std::string topic;
  bool remote = false; // 外部 agent 驱动: 只有等它交调用, 没有流
  bool stalled() const { return stage != Idle && !remote && quiet > kQuietS; }
};
// 阶段由末步的 Span 定, 静默由镜像 s.chat_live 的增量量 (外部 agent: 恒为等)
Flow flow_of(Ui &s, const Mine &m) {
  Flow f;
  if (m.timeline.empty() || !m.timeline.back().span.running())
    return f;
  const Step &rd = m.timeline.back();
  if (m.remote()) {
    f.stage = Flow::Wait, f.remote = true, f.since = rd.span.len(m.now());
    return f;
  }
  const llm::Reply &live = s.chat_live.reply;
  const double t = m.now();
  const size_t n = live.thinking.size() + live.text.size();
  if (s.live_t < rd.span.t0)
    s.live_n = 0, s.live_t = rd.span.t0;
  if (n != s.live_n)
    s.live_n = n, s.live_t = t;
  f.since = rd.span.len(t);
  f.quiet = t - s.live_t;
  f.writing = rd.write.len(t);
  f.stage = rd.write.began() ? Flow::Write : rd.think.began() ? Flow::Think
                                                              : Flow::Wait;
  if (f.stage == Flow::Think)
    f.topic = topic(live.thinking, true);
  return f;
}
// 阶段一句 (进行时) + 用时 + 静默
std::string doing(const Flow &f) {
  std::string h = f.remote                 ? "等外部 agent 交调用… " + dur(f.since)
                  : f.stage == Flow::Wait  ? "等待首包… " + dur(f.since)
                  : f.stage == Flow::Think ? "思考中… " + dur(f.since)
                                           : "输出中… " + dur(f.writing);
  if (f.stalled())
    h += " · 已 " + dur(f.quiet) + " 无新内容";
  return h;
}

// 在做的调用 (Act / Choose 中; 否则空)
const Act *acting(const Mine &m) { return m.phase() == Phase::Act || m.phase() == Phase::Choose ? m.acting() : nullptr; }

// ---- 会话行: New / Test / 载入 / 继续 / 交答案 / Cancel / 关闭 ----
void load_session(const SharedData &data, Ui &s, const Saved &x) {
  if (!catalog::ready(data, x.sc, false)) {
    s.message = "这段会话的作用域未就绪 (资产轴未扫描或标签库未建)";
    return;
  }
  s.mine->load(data, x.key);
}

// 载入菜单: 本作用域存过的会话, 新到旧; 一行 = 时刻 / 步数 / 停下原因 / 候选与入库 / 大脑, 悬浮看目录与要求
void saved_menu(const SharedData &data, Ui &s, const Scope &sc) {
  if (s.saved.empty()) {
    ImGui::TextDisabled("本作用域 (%s) 还没有存过的会话", catalog::label(sc).c_str());
    return;
  }
  for (const Saved &x : s.saved) {
    const nlohmann::ordered_json &b = x.brief;
    const std::string reason = b.at("reason").get<std::string>(), why = reason.empty() ? "没停稳就退出了" : reason;
    const std::string label = stamp_text(x.key.stamp) + (x.key.test ? " · 自检" : "") + " · " + std::to_string(b.at("steps").get<int>()) + " 步 · " + text::head(why, 24) + " · 候选 " +
                              std::to_string(b.at("cands").get<int>()) + " · 入库 " + std::to_string(b.at("adopted").size()) + " · " + b.at("brain").get<std::string>() + "###" + x.dir;
    if (ImGui::Selectable(label.c_str())) {
      load_session(data, s, x);
      ImGui::CloseCurrentPopup();
    }
    const std::string remark = b.at("remark").get<std::string>();
    tip_long(x.dir + "\n用时 " + dur(b.at("elapsed").get<double>()) + " · 有参数组 " + std::to_string(b.at("weights").get<int>()) +
             " · 停下原因: " + why + (remark.empty() ? "" : "\n本会话要求: " + remark));
  }
}

void session_row(const SharedData &data, Ui &s, const Scope &active_sc) {
  Mine &m = *s.mine;
  const Options &o = s.opt;
  const bool has_hold = !catalog::holds(data, active_sc.level).empty();
  const bool keyed = has_key(o.backend);
  const bool bound = m.desk.scope() == active_sc;
  const bool has_leaves = bound && !m.desk.ctx().leaves.empty(), has_ops = bound && !m.desk.ctx().ops.empty();
  const char *llm_not = bench_busy(s)  ? "Tools 页的调用在跑 (不并跑, 等它做完或取消)"
                        : !keyed       ? "所选后端的 key 未填 (output/config/agent/keys.json)"
                        : !model_ok(s) ? "模型未就绪 (Config 页: 嗅探中 / 嗅探失败 / 无可用模型)"
                                       : nullptr;

  ImGui::TextColored(kColHead, "2. Session:");
  ImGui::SameLine();
  const bool can_new = !m.live() && m.idle() && !llm_not && has_hold && has_leaves && has_ops;
  const char *why_not = llm_not                   ? llm_not
                        : has_hold && !has_leaves ? "没有开放的特征叶 (本层无叶, 或特征页一个没勾)"
                        : has_hold && !has_ops    ? "没有开放的算子 (算子页一个没勾)"
                                                  : nullptr;
  const auto begin_session = [&](bool test) {
    Mine::Setup st = setup_of(o);
    st.sc = active_sc;
    st.test = test;
    st.remark = std::string(text::trim(s.remark));
    if (std::string err = m.try_open(data, std::move(st), leg_of(o, use_gpu(s))); !err.empty())
      s.message = std::move(err);
  };
  ImGui::BeginDisabled(!can_new);
  if (ImGui::Button("New", ImVec2(60, 0)))
    begin_session(false);
  ImGui::EndDisabled();
  tip(why_not ? why_not
              : "按当前作用域与 Config 开新会话: 打包规则与工具表发给内置大模型, 它调工具看数据、回测、提交\n"
                "资产轴与标签就绪才开得成; 停着的会话已存盘, 换下也能再载入");
  ImGui::SameLine();
  ImGui::BeginDisabled(!can_new);
  if (ImGui::Button("Test", ImVec2(60, 0)))
    begin_session(true);
  ImGui::EndDisabled();
  tip(why_not ? why_not
              : "开一段自检会话: 同样的系统提示, 首条换成 test 模板, 让 agent 把每个工具都调一遍, 问题记 report_issue, 末了总评\n"
                "候选 / 日志 / 全局记录进 agent/test 目录, submit_factor 只查门槛不拷进正式库");
  ImGui::SameLine();
  ImGui::BeginDisabled(m.live() || !m.idle() || bench_busy(s));
  if (ImGui::Button("载入", ImVec2(60, 0))) {
    s.saved = SavedSessions(active_sc);
    ImGui::OpenPopup("##saved");
  }
  ImGui::EndDisabled();
  tip("载入本作用域 (池 / 层 / 持有期) 存过的一段会话: 停在它停下处, 可继续, 也可右键某一步回退");
  if (ImGui::BeginPopup("##saved")) {
    saved_menu(data, s, active_sc);
    ImGui::EndPopup();
  }
  ImGui::SameLine();
  const char *resume_not = bench_busy(s) ? "Tools 页的调用在跑 (不并跑, 等它做完或取消)"
                           : m.remote()  ? nullptr
                                         : llm_not;
  const bool can_resume = m.phase() == Phase::Pause && m.idle() && !resume_not;
  ImGui::BeginDisabled(!can_resume);
  if (ImGui::Button("继续", ImVec2(60, 0))) {
    const bool gpu = use_gpu(s);
    if (!catalog::ready(data, m.setup.sc, gpu)) {
      s.message = "资产轴未就绪 (先在 Database 页扫描) 或标签库未建";
    } else {
      s.message.clear();
      m.resume(data, leg_of(o, gpu));
      s.pinned = true;
    }
  }
  ImGui::EndDisabled();
  const std::string resume_tip =
      m.phase() != Phase::Pause ? std::string("会话停着时可用: 接着停下处走")
      : resume_not              ? std::string(resume_not)
      : m.remote()              ? "继续 = " + m.hint() + "\n恢复外部会话 (等外部 agent 调无参 run 接着做); 评估后端 / 预算按 Config 当前的, 本段预算从此刻起算"
                                : "继续 = " + m.hint() + "\n后端 / 模型 / 评估后端 / 预算按 Config 当前的; 本段预算从此刻起算";
  tip(resume_tip.c_str());
  ImGui::SameLine();
  ImGui::BeginDisabled(m.phase() != Phase::Choose);
  if (ImGui::Button("交答案", ImVec2(60, 0)))
    m.answer();
  ImGui::EndDisabled();
  tip("agent 用 ask_human 提问时可用: 把选好的答案与附言交给 agent, 接着执行余下的调用");
  ImGui::SameLine();
  ImGui::BeginDisabled(!m.live());
  if (ImGui::Button("Cancel", ImVec2(60, 0)))
    m.cancel();
  ImGui::EndDisabled();
  tip("立即停下: 在收的回复作废 (继续即重发本条); 在跑的调用作废 (落了文件的回测删掉候选; 在等你答的 ask_human 记为没答); 之后可继续 / 回退");
  ImGui::SameLine();
  ImGui::BeginDisabled(m.phase() != Phase::Pause || !m.idle());
  if (ImGui::Button("关闭", ImVec2(60, 0)))
    m.close();
  ImGui::EndDisabled();
  tip("放下这段停着的会话 (已存盘, 可再载入); 作用域与特征 / 算子的勾选随之解锁");

  if (m.held()) {
    const Mine::Setup &st = m.setup;
    ImGui::SameLine();
    ImGui::TextDisabled("%s%s · 近 %d 年 · %s · %s · %s", st.test ? "自检 · " : "", catalog::label(st.sc).c_str(), st.sc.years, m.brain().c_str(), m.leg.gpu ? "GPU" : "CPU",
                        st.ask ? "可提问" : "不提问");
    if (ImGui::IsItemHovered())
      ImGui::SetTooltip("%s", m.log.dir().c_str());
  }
  if (!s.message.empty()) {
    ImGui::SameLine();
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "| %s", s.message.c_str());
  }
}

// ---- 工具用量条: 按类拼接, 类内一工具一段 (色 = 工具色, 与时间线同), 段宽 = 次数占比; 类段够宽写类名与占比, 悬浮看各类各工具的次数 / 占比 / 失败数 (表格) ----
constexpr Per<Cat, ImVec4> kCatCols{{{{0.35f, 0.78f, 0.45f, 1.0f}, {0.95f, 0.62f, 0.25f, 1.0f}, {0.38f, 0.62f, 0.95f, 1.0f}, {0.72f, 0.52f, 0.92f, 1.0f}, {0.58f, 0.58f, 0.58f, 1.0f}}}};

// 工具色: 类色的色相左右摆、明度三档相间 (按工具在类内的序, 恒定)
ImVec4 tool_col(Tool t) {
  const Cat k = def(t).cat;
  int i = 0;
  for (const Tool u : kVals<Tool>) {
    if (u == t)
      break;
    i += def(u).cat == k;
  }
  const ImVec4 c = kCatCols[k];
  float h = 0.0f, s = 0.0f, v = 0.0f;
  ImGui::ColorConvertRGBtoHSV(c.x, c.y, c.z, h, s, v);
  const int step = (i + 1) / 2; // 0, 1, 1, 2, 2, …: 左右交替摆开
  h = std::fmod(h + 0.03f * static_cast<float>(step) * (i % 2 ? 1.0f : -1.0f) + 1.0f, 1.0f);
  v *= 1.0f - 0.15f * static_cast<float>(i % 3);
  ImVec4 o(0.0f, 0.0f, 0.0f, c.w);
  ImGui::ColorConvertHSVtoRGB(h, s, v, o.x, o.y, o.z);
  return o;
}

void tools_bar(const Tally &c) {
  const int all = c.all();
  const float w = ImGui::GetContentRegionAvail().x, h = ImGui::GetTextLineHeight();
  const ImVec2 p0 = ImGui::GetCursorScreenPos();
  ImGui::InvisibleButton("##tools_bar", ImVec2(w, h));
  const bool hovered = ImGui::IsItemHovered();
  ImDrawList *dl = ImGui::GetWindowDrawList();
  dl->AddRectFilled(p0, ImVec2(p0.x + w, p0.y + h), ImGui::GetColorU32(ImGuiCol_FrameBg));
  if (all == 0) {
    if (hovered)
      ImGui::SetTooltip("还没有调用");
    return;
  }
  Per<Cat, int> per{};
  for (const Cat k : kVals<Cat>)
    per[k] = c.n(k);
  const float mx = ImGui::GetIO().MousePos.x;
  const auto pct = [&](int n) { return 100.0 * n / all; };
  std::optional<Tool> hit;
  float x = p0.x;
  for (const Cat k : kVals<Cat>) {
    if (per[k] == 0)
      continue;
    const float k0 = x;
    for (const Tool t : kVals<Tool>) {
      if (def(t).cat != k || c.n(t) == 0)
        continue;
      const float x1 = x + w * static_cast<float>(c.n(t)) / static_cast<float>(all);
      dl->AddRectFilled(ImVec2(x, p0.y), ImVec2(x1, p0.y + h), ImGui::ColorConvertFloat4ToU32(tool_col(t)));
      if (hovered && mx >= x && mx < x1)
        hit = t;
      x = x1;
    }
    char label[32];
    std::snprintf(label, sizeof(label), "%s %.0f%%", def(k).cn, pct(per[k]));
    if (const float tw = ImGui::CalcTextSize(label).x; tw + 4.0f <= x - k0)
      dl->AddText(ImVec2((k0 + x - tw) * 0.5f, p0.y), IM_COL32(20, 20, 20, 255), label);
    if (k0 > p0.x)
      dl->AddLine(ImVec2(k0, p0.y), ImVec2(k0, p0.y + h), ImGui::GetColorU32(ImGuiCol_WindowBg), 2.0f);
  }
  if (!hovered)
    return;
  ImGui::BeginTooltip();
  constexpr ImGuiTableFlags kTipTable = ImGuiTableFlags_SizingFixedFit | ImGuiTableFlags_RowBg | ImGuiTableFlags_BordersInnerV | ImGuiTableFlags_PadOuterX;
  if (ImGui::BeginTable("##tools_tip", 5, kTipTable)) {
    for (const char *head : {"工具", "中文名", "次数", "占比", "失败"})
      ImGui::TableSetupColumn(head);
    ImGui::TableHeadersRow();
    const ImVec4 bad = color(TaskStatus::Kind::Error), dim = ImGui::GetStyleColorVec4(ImGuiCol_TextDisabled);
    const auto right = [](const ImVec4 &col, const char *s) {
      ImGui::SetCursorPosX(ImGui::GetCursorPosX() + ImGui::GetColumnWidth() - ImGui::CalcTextSize(s).x);
      ImGui::TextColored(col, "%s", s);
    };
    const auto row = [&](const ImVec4 &col, const char *name, const char *cn, int n, int fail) {
      char b[32];
      ImGui::TableNextRow();
      ImGui::TableNextColumn(), ImGui::TextColored(col, "%s", name);
      ImGui::TableNextColumn(), ImGui::TextColored(col, "%s", cn);
      ImGui::TableNextColumn(), std::snprintf(b, sizeof(b), "%d", n), right(col, b);
      ImGui::TableNextColumn(), std::snprintf(b, sizeof(b), "%.0f%%", pct(n)), right(col, b);
      ImGui::TableNextColumn(), std::snprintf(b, sizeof(b), "%d", fail), right(fail ? bad : dim, b);
    };
    row(kColHead, "全部", "", all, c.failed());
    for (const Cat k : kVals<Cat>) {
      if (per[k] == 0)
        continue;
      row(kCatCols[k], def(k).cn, "", per[k], c.failed(k));
      for (const Tool t : kVals<Tool>) {
        if (def(t).cat != k || c.n(t) == 0)
          continue;
        const ToolMeta &m = def(t);
        const std::string name = std::string(hit == t ? "> " : "  ") + m.id;
        row(tool_col(t), name.c_str(), m.cn, c.n(t), c.failed(t));
      }
    }
    ImGui::EndTable();
  }
  ImGui::EndTooltip();
}

// ---- 现在在干什么 (点了跳到当前一步) + 会话累计 (一行要点, 悬浮看全) ----
void now_row(Ui &s, const Mine &m, const Flow &f) {
  const Phase ph = m.phase();
  if (ph == Phase::None) {
    ImGui::TextDisabled("无会话: 选好作用域按 New, 或载入存过的");
    return;
  }
  const double t = m.now();
  const Tally all = m.tally();
  const llm::Reply &live = s.chat_live.reply;
  const LabLive &ll = s.lab_live;
  const Act *a = acting(m);
  std::string what;
  TaskStatus::Kind kind = TaskStatus::Kind::Busy;
  float frac = -1.0f; // 在做那步的进度 (−1 = 不画)
  if (f.stage != Flow::Idle) {
    what = "agent " + doing(f);
    if (!f.topic.empty())
      what += " · " + f.topic;
    if (!live.thinking.empty())
      what += " · 思考 " + chars(live.thinking);
    if (!live.text.empty())
      what += " · 正文 " + chars(live.text);
    if (f.stalled())
      kind = TaskStatus::Kind::Warn;
  } else {
    switch (ph) {
    case Phase::Choose:
      what = "等你选答案: 已按推荐预选, 改完按 交答案", kind = TaskStatus::Kind::Warn;
      break;
    case Phase::Act: {
      if (!a) {
        what = "处理回复";
        break;
      }
      what = std::string(def(a->call.tool).id) + " · " + def(a->call.tool).cn + " · " + text::head(a->call.text(), kTopicChars) + " · " + StageOf(*a, ll.frac) + " " + dur(a->span.len(t));
      if (def(a->call.tool).lab())
        frac = static_cast<float>(ll.frac);
      break;
    }
    case Phase::Pause:
      what = "停着: " + m.reason + " · 继续 = " + m.hint(), kind = TaskStatus::Kind::Ready;
      break;
    default:
      what = "处理回复";
      break;
    }
  }
  if (frac >= 0.0f) {
    ImGui::ProgressBar(frac, ImVec2(120, 0), "");
    ImGui::SameLine();
  }
  ImGui::TextColored(color(kind), "%s", what.c_str());
  if (!m.timeline.empty()) {
    if (ImGui::IsItemClicked())
      s.jump = true;
    if (ImGui::IsItemHovered()) {
      ImGui::SetMouseCursor(ImGuiMouseCursor_Hand);
      ImGui::SetTooltip("点击跳到当前一步");
    }
  }
  ImGui::TextDisabled("用时 %s · token %s · 调用 %d · 候选 %d · 入库 %d", dur(t).c_str(), kilo(all.usage.prompt + all.usage.completion).c_str(), all.all(),
                      all.staged(), all.adopted);
  tip_long("用时 " + dur(t) + " (LLM " + dur(all.llm_s) + " · 执行 " + dur(all.act_s) + ")\ntoken 输入 " + kilo(all.usage.prompt) + " (缓存 " + kilo(all.usage.cached) + ") / 输出 " +
           kilo(all.usage.completion) + "\n" + std::to_string(m.timeline.size()) + " 步 · 调用 " + std::to_string(all.all()) + " (观察 " + std::to_string(all.n(Cat::Look)) +
           " · 查研报 " + std::to_string(all.docs()) + " · 没做成 " + std::to_string(all.failed()) + ")\n候选 " + std::to_string(all.staged()) + " (有参数组 " +
           std::to_string(all.n(Verdict::Weights)) + ") · 提交 " + std::to_string(all.n(Tool::SubmitFactor)) + " · 入库 " + std::to_string(all.adopted) +
           (m.memo.empty() ? "" : "\n插话已排队, 随下一条发出"));
  tools_bar(all);
}

using Names = std::set<std::string>; // 入库的英文名

// 结局的颜色 (入库另标绿; agent 看不见的灰)
TaskStatus::Kind kind_of(const Names &in, const Result &r) {
  if (!r.visible)
    return TaskStatus::Kind::Muted;
  switch (r.verdict) {
  case Verdict::Unstaged:
  case Verdict::Broken:
    return TaskStatus::Kind::Error;
  case Verdict::Weights:
    return in.count(r.name) ? TaskStatus::Kind::Ready : TaskStatus::Kind::None;
  case Verdict::Null:
  case Verdict::Pending:
    return TaskStatus::Kind::Muted;
  }
  return TaskStatus::Kind::None;
}

// 结局两档 (同 Prompt 的 Brief / Detail): 一行 + 入库标记 / 多行 (去末换行)
std::string brief(const Names &in, const Result &r) { return Brief(r) + (in.count(r.name) ? " · 已入库" : ""); }
std::string detail(const Result &r) {
  std::string s = Detail(r);
  while (!s.empty() && s.back() == '\n')
    s.pop_back();
  return s;
}

// 一个候选文件的结局: 一行, 悬浮看全文
void result_line(const Names &in, const Result &r) {
  wrapped(color(kind_of(in, r)), brief(in, r));
  tip_long(detail(r));
}

// ---- 时间线 = 与 agent 的全部交互, 一件一条 ----
// 条目: 工具调用 (agent 的回复 → 执行 → 交回的 prompt) 与流程 (开场 / 插话 / 不合协议 / 待发 · 等回复 / 停下 / 继续 / 作废回复), 同一种格式:
// 标题一行 = 英文名 · 中文名 · 思考用时 (agent 拿到上一条到交来本调用) · 执行用时 (本调用) · 一行结论, 一律折叠; 展开 = 底色略异的区块, 右端切换 展示 (人读的) / prompt (与 agent 往来的原文)
constexpr ImGuiTreeNodeFlags kNode = ImGuiTreeNodeFlags_SpanAvailWidth | ImGuiTreeNodeFlags_DrawLinesFull;
constexpr ImGuiTreeNodeFlags kEntry = kNode | ImGuiTreeNodeFlags_AllowOverlap;
constexpr size_t kCallChars = 96; // 调用原文在一行结论里的字数
constexpr float kTint = 0.10f;    // 展开区块的底色 = 条目色的这么多不透明度

// 两段用时的色: 思考 = 大脑 (紫) / 执行 = 系统 (蓝)
constexpr ImVec4 kColThink(0.72f, 0.6f, 0.9f, 0.85f), kColRun(0.5f, 0.7f, 0.95f, 0.85f);

// 流程条目的色 (工具条目用工具色; 待发 / 等回复用状态色)
constexpr ImVec4 kColStart(0.95f, 0.55f, 0.75f, 1.0f), kColMemo(0.95f, 0.85f, 0.35f, 1.0f), kColRetry(0.95f, 0.35f, 0.35f, 1.0f), kColBeat(0.9f, 0.9f, 0.9f, 1.0f),
    kColDrop(0.85f, 0.45f, 0.45f, 1.0f);
constexpr std::pair<const char *, const char *> kLanes[] = {{"start", "开场"}, {"memo", "插话"}, {"retry", "不合协议"}, {"send", "待发"}, {"reply", "等回复"}};

// 标题的列宽 (英文名 / 中文名 / 两段用时 各取最宽的, 各条对齐)
struct Cols {
  float id = 0.0f, cn = 0.0f, took = 0.0f; // took = 一段用时的宽
};
Cols cols_of() {
  Cols c;
  const auto fit = [](float &w, const char *s) { w = std::max(w, ImGui::CalcTextSize(s).x); };
  for (const Tool t : kVals<Tool>)
    fit(c.id, def(t).id), fit(c.cn, def(t).cn);
  for (const Beat b : kVals<Beat>)
    fit(c.id, def(b).id), fit(c.cn, def(b).cn);
  for (const auto &[id, cn] : kLanes)
    fit(c.id, id), fit(c.cn, cn);
  fit(c.took, "00m00s");
  return c;
}

struct Head {
  ImVec4 col; // 条目色: 英文名 / 中文名 / 展开底色
  const char *id, *cn;
  std::string think, run, line; // 思考用时 / 执行用时 / 一行结论 (空 = 不写)
  ImVec4 tone;                  // 结论的色 (状态)
  std::string tip;              // 悬浮看的全文 (空 = 无)
};

std::string first_line(const std::string &s) { return std::string(text::trim(std::string_view(s).substr(0, s.find('\n')))); }

// 区块: 内容画在上层, 量出高后在下层铺底色 (内容里的表 / 图各用各的分层, 不相扰)
template <class F>
void tinted(const ImVec4 &col, F &&f) {
  const ImGuiStyle &st = ImGui::GetStyle();
  ImDrawList *dl = ImGui::GetWindowDrawList();
  ImDrawListSplitter split;
  split.Split(dl, 2);
  split.SetCurrentChannel(dl, 1);
  const ImVec2 p0 = ImGui::GetCursorScreenPos();
  const float x1 = p0.x + ImGui::GetContentRegionAvail().x;
  ImGui::SetCursorPosY(ImGui::GetCursorPosY() + st.FramePadding.y);
  ImGui::Indent(st.FramePadding.x);
  ImGui::BeginGroup();
  f();
  ImGui::EndGroup();
  ImGui::Unindent(st.FramePadding.x);
  const float y1 = ImGui::GetItemRectMax().y + st.FramePadding.y;
  split.SetCurrentChannel(dl, 0);
  dl->AddRectFilled(p0, ImVec2(x1, y1), ImGui::GetColorU32(ImVec4(col.x, col.y, col.z, kTint)), st.FrameRounding);
  split.Merge(dl);
  ImGui::Dummy(ImVec2(0.0f, st.FramePadding.y));
}

// 一条: 标题行 (树节点) + 展开的区块 (按模式画 human() / prompt()). k = 右键回退到第几步 (0 = 不可回退); now = 当前一步 (点状态行跳到它)
template <class Human, class Prompt>
void entry(Ui &s, const Mine &m, const Cols &c, const Head &h, int k, bool now, Human &&human, Prompt &&prompt) {
  const ImGuiStyle &st = ImGui::GetStyle();
  const float x0 = ImGui::GetCursorPosX(), right = x0 + ImGui::GetContentRegionAvail().x;
  const bool open = ImGui::TreeNodeEx("##e", kEntry);
  const ImGuiID id = ImGui::GetItemID();
  const bool editable = k > 0 && m.phase() == Phase::Pause && m.idle();
  if (ImGui::IsItemHovered() && !ImGui::IsPopupOpen("##rewind_menu")) {
    const std::string why = "右键: 从这一条重来 (丢这一步的回复与调用及其后各步, 继续即重发这条消息)";
    const std::string t = editable ? h.tip + (h.tip.empty() ? "" : "\n\n") + why : h.tip;
    if (!t.empty())
      tip_long(t);
  }
  if (editable && ImGui::BeginPopupContextItem("##rewind_menu")) {
    ImGui::TextDisabled("%s · 回退", StepTag(k).c_str());
    if (ImGui::MenuItem(("从 " + StepTag(k) + " 重来…").c_str()))
      s.rewind = {.k = k, .loss = m.loss(k), .open = true};
    ImGui::EndPopup();
  }
  if (s.jump && now)
    ImGui::SetScrollHereY(0.0f);
  const float sp = st.ItemInnerSpacing.x * 2.0f, x = x0 + ImGui::GetTreeNodeToLabelSpacing(), xl = x + c.id + c.cn + 2.0f * c.took + 4.0f * sp;
  ImGui::SameLine(x);
  ImGui::TextColored(h.col, "%s", h.id);
  ImGui::SameLine(x + c.id + sp);
  ImGui::TextColored(h.col, "%s", h.cn);
  ImGui::SameLine(x + c.id + c.cn + 2.0f * sp);
  ImGui::TextColored(kColThink, "%s", h.think.c_str());
  ImGui::SameLine(x + c.id + c.cn + c.took + 3.0f * sp);
  ImGui::TextColored(kColRun, "%s", h.run.c_str());
  constexpr const char *kModes[] = {"展示", "prompt"};
  const float modes = ImGui::CalcTextSize(kModes[0]).x + ImGui::CalcTextSize(kModes[1]).x + 4.0f * st.FramePadding.x + st.ItemSpacing.x;
  const float xr = open ? right - modes : right;
  ImGui::SameLine(xl);
  const ImVec2 p = ImGui::GetCursorScreenPos();
  ImGui::PushClipRect(p, ImVec2(p.x + std::max(0.0f, xr - xl - sp), p.y + ImGui::GetTextLineHeight()), true);
  ImGui::TextColored(h.tone, "%s", h.line.c_str());
  ImGui::PopClipRect();
  if (!open)
    return;
  const bool raw = s.raw.contains(id);
  ImGui::SameLine(xr);
  for (int i = 0; i < 2; ++i) {
    if (i)
      ImGui::SameLine();
    const bool on = raw == (i == 1);
    ImGui::PushStyleColor(ImGuiCol_Button, ImGui::GetStyleColorVec4(on ? ImGuiCol_ButtonActive : ImGuiCol_FrameBg));
    if (ImGui::SmallButton(kModes[i]) && !on) {
      if (raw)
        s.raw.erase(id);
      else
        s.raw.insert(id);
    }
    ImGui::PopStyleColor();
  }
  tinted(h.col, [&] { raw ? prompt() : human(); });
  ImGui::TreePop();
}

// prompt 模式的一段原文: 标签 · 字数 + 复制, 文本框
void raw_part(const char *id, const std::string &label, const std::string &body, bool tail = false) {
  ImGui::PushID(id);
  ImGui::TextDisabled("%s · %s", label.c_str(), chars(body).c_str());
  copy_button("复制", body);
  box(body.empty() ? std::string("(空)") : body, false, kBoxLines, tail);
  ImGui::PopID();
}

// agent 在第 i 步看到的完整上下文 (system 之外; 新开一段对话时整份给, 连着时只给本条): 照时间线现算, 展开时算一次 (时间线变了重算)
void context_block(Ui &s, const Mine &m, size_t i) {
  const bool open = ImGui::TreeNodeEx("context", ImGuiTreeNodeFlags_None, "agent 看到的完整上下文 (远处折成结论; system 之外)");
  if (!open) {
    s.ctx.erase(i);
    return;
  }
  const Step &sp = m.timeline[i];
  const auto sig = std::tuple(m.timeline.size(), sp.span.t0, sp.user.size(), sp.memo.size());
  Ui::Ctx &c = s.ctx[i];
  if (c.body.empty() || c.sig != sig)
    c = {sig, llm::Flat(TurnsOf(Steps(m.timeline).first(i + 1)))};
  copy_button("复制##context", c.body);
  box(c.body, false, kBoxLines, false);
  ImGui::TreePop();
}

// 第 i 步与 agent 的往来 (prompt 模式): 回复用时与 token · 它看到的上下文 · 思考 · 回复原文 · 交回的 prompt (下一条消息, 含插话与下一步)
void exchange(Ui &s, const Mine &m, size_t i) {
  const Step &sp = m.timeline[i];
  const llm::Reply &r = sp.reply;
  std::string line = "回复用时 " + dur(sp.span.len(m.now())) + (sp.think.began() ? " (思考 " + dur(sp.think.len(m.now())) + ")" : "");
  if (r.usage.prompt > 0)
    line += " · token 输入 " + kilo(r.usage.prompt) + " (缓存 " + kilo(r.usage.cached) + ") / 输出 " + kilo(r.usage.completion);
  ImGui::TextDisabled("%s", line.c_str());
  context_block(s, m, i);
  if (!r.thinking.empty())
    raw_part("think", "思考", r.thinking);
  raw_part("reply", "回复", r.text);
  if (i + 1 == m.timeline.size()) {
    wrapped_dim(sp.act ? "调用做完即把结果交回" : "重交要求随下一条交回");
    return;
  }
  const Step &nx = m.timeline[i + 1];
  raw_part("back", std::string(nx.span.began() ? "交回的 prompt · " : "待交回的 prompt · ") + def(KindOf(m.timeline, i + 1)).cn + (nx.memo.empty() ? "" : " · 附插话"), TextOf(nx));
}

// 调用用了多久 (后台分段的逐段写, 如 eval_factor 的 Search / Run)
std::string took(const Act &a, double t) {
  if (a.laps.empty())
    return dur(a.span.len(t));
  std::string s;
  for (const Lap &l : a.laps)
    s += (s.empty() ? "" : " · ") + std::string(def(l.step).name) + " " + dur(l.span.len(t));
  return s;
}

// 第 i 步的工具调用: 结论 = 结果的一行 (回测接结局色, 入库另标); 在问的 ask_human 展开即答题. 返回 = 按了交答案
bool call_entry(Ui &s, Mine &m, const Cols &c, const Names &adopted, size_t i) {
  Act &a = *m.timeline[i].act;
  const double t = m.now();
  const bool last = i + 1 == m.timeline.size(), edit = last && m.phase() == Phase::Choose;
  const TrialRes *k = As<Tool::EvalFactor>(a);
  const ToolMeta &meta = def(a.call.tool);
  const std::string full = a.call.text(), call = text::head(full, kCallChars);
  const std::string what = text::flat(!a.out.title.empty() ? a.out.title : a.out.res ? a.out.res->title()
                                                                                     : call);
  Head h{tool_col(a.call.tool), meta.id, meta.cn, dur(m.timeline[i].span.len(t)), a.span.began() ? dur(a.span.len(t)) : std::string(), what, color(TaskStatus::Kind::None),
         full + "\n\n思考 " + dur(m.timeline[i].span.len(t)) + " (agent 拿到上一条到交来本调用)" + (a.span.began() ? " · 执行 " + took(a, t) : std::string())};
  if (a.span.ended()) {
    if (!a.out.ok && a.out.title.empty())
      h.line = "没做成: " + first_line(a.out.text);
    if (k && adopted.count(k->st.res.name))
      h.line += " · 已入库";
    h.tone = color(k ? kind_of(adopted, k->st.res) : a.out.ok ? TaskStatus::Kind::None
                                                              : TaskStatus::Kind::Warn);
  } else if (edit) {
    h.line = "等你答 · " + what, h.tone = color(TaskStatus::Kind::Warn);
  } else if (a.span.running() && m.live()) {
    h.line = StageOf(a, s.lab_live.frac) + " · " + what, h.tone = color(TaskStatus::Kind::Busy);
  } else {
    h.run = m.live() ? "排队" : "未做", h.tone = color(TaskStatus::Kind::Muted);
  }
  bool start = false;
  entry(
      s, m, c, h, static_cast<int>(i) + 1, last,
      [&] {
        if (edit)
          start = edit_asked(As<Tool::AskHuman>(a)->asked);
        else if (a.out.res)
          a.out.res->draw();
        else if (a.span.ended())
          box(a.out.text, false, kBoxLines, false);
        else
          wrapped_dim(h.line);
      },
      [&] { exchange(s, m, i); });
  return start;
}

// 第 i 步的回复不合协议: 展示 = 不合之处; prompt = 原回复与交回的重交要求
void retry_entry(Ui &s, const Mine &m, const Cols &c, size_t i) {
  const Step &sp = m.timeline[i];
  const Head h{kColRetry, kLanes[2].first, kLanes[2].second, dur(sp.span.len(m.now())), "", first_line(sp.error), color(TaskStatus::Kind::Error), sp.error};
  entry(s, m, c, h, static_cast<int>(i) + 1, i + 1 == m.timeline.size(), [&] { wrapped(color(TaskStatus::Kind::Error), sp.error); }, [&] { exchange(s, m, i); });
}

// 末步还没交来回复: 待发 (停着, 按继续发出) / 等回复 (思考 / 正文随收随显)
void wait_entry(Ui &s, const Mine &m, const Cols &c, const Flow &f, size_t i) {
  const Step &sp = m.timeline[i];
  const llm::Reply &live = s.chat_live.reply;
  const std::string sent = TextOf(sp), kind = def(KindOf(m.timeline, i)).cn;
  if (!sp.span.began()) {
    const Head h{color(TaskStatus::Kind::Warn), kLanes[3].first, kLanes[3].second, "", "", kind + " · " + chars(sent) + (m.live() ? "" : " · 按 继续 发出"),
                 color(TaskStatus::Kind::Warn), ""};
    entry(s, m, c, h, static_cast<int>(i) + 1, true, [&] { wrapped_dim("这条消息还没发给 agent (" + kind + "); 原文见 prompt"); }, [&] { raw_part("send", "待发的 prompt", sent); });
    return;
  }
  const Head h{color(f.stalled() ? TaskStatus::Kind::Warn : TaskStatus::Kind::Busy), kLanes[4].first, kLanes[4].second, dur(f.since), "",
               doing(f) + (f.topic.empty() ? "" : " · " + f.topic), color(f.stalled() ? TaskStatus::Kind::Warn : TaskStatus::Kind::Busy), ""};
  const auto stream = [&] {
    if (!live.thinking.empty())
      raw_part("think", "思考", live.thinking, true);
    if (!live.text.empty())
      raw_part("text", "正文", live.text, true);
  };
  entry(
      s, m, c, h, static_cast<int>(i) + 1, true,
      [&] {
        if (live.thinking.empty() && live.text.empty())
          wrapped_dim(doing(f));
        stream();
      },
      [&] {
        context_block(s, m, i);
        raw_part("send", "发出的 prompt · " + kind + (sp.memo.empty() ? "" : " · 附插话"), sent);
        stream();
      });
}

// 开场: 系统提示 + 首条 (自检会话的首条; 本会话要求附在末尾)
void start_entry(Ui &s, const Mine &m, const Cols &c) {
  const Step &sp = m.timeline.front();
  const Mine::Setup &st = m.setup;
  assert(m.system && "开会话即有系统提示");
  const std::string scope = std::string(st.test ? "自检 · " : "") + catalog::label(st.sc) + " · 近 " + std::to_string(st.sc.years) + " 年 · " + m.brain();
  const Head h{kColStart, kLanes[0].first, kLanes[0].second, "", "", scope + (st.remark.empty() ? "" : " · 要求: " + text::flat(st.remark)), color(TaskStatus::Kind::None),
               st.remark};
  entry(
      s, m, c, h, 1, false,
      [&] {
        wrapped(scope + " · " + (st.ask ? "可提问" : "不提问"));
        wrapped_dim("系统提示 " + chars(*m.system) + " · 首条 " + chars(sp.user) + "; 原文见 prompt");
        if (!st.remark.empty())
          wrapped("本会话要求: " + st.remark);
      },
      [&] {
        raw_part("system", "系统提示 (system)", *m.system);
        raw_part("first", std::string(st.test ? "自检首条" : "首条") + " (user)", sp.user);
      });
}

// 研究员插话: 随第 i 条消息发出
void memo_entry(Ui &s, const Mine &m, const Cols &c, size_t i) {
  const std::string &memo = m.timeline[i].memo;
  const Head h{kColMemo, kLanes[1].first, kLanes[1].second, "", "", text::flat(memo), color(TaskStatus::Kind::None), memo};
  entry(s, m, c, h, static_cast<int>(i) + 1, false, [&] { wrapped(memo); }, [&] { raw_part("memo", "附在消息末尾", MemoText(memo)); });
}

// 停下汇总 (停着时末一笔停下的展示): 继续会做什么 + 计数 + 本会话有参数组的候选 (入库的在前, 再按 score (中位 rIC) 降序; 先列 kSummaryRows 个, 其余折起)
constexpr size_t kSummaryRows = 5;
void summary_block(const Mine &m) {
  const Tally c = m.tally();
  const Names in = AdoptedOf(m.timeline);
  std::vector<const Result *> hits;
  for (const Step &sp : m.timeline)
    if (const TrialRes *k = sp.act ? As<Tool::EvalFactor>(*sp.act) : nullptr; k && k->st.res.verdict == Verdict::Weights)
      hits.push_back(&k->st.res);
  std::stable_sort(hits.begin(), hits.end(), [&](const Result *a, const Result *b) {
    const bool ia = in.count(a->name) > 0, ib = in.count(b->name) > 0;
    return ia != ib ? ia : a->best()->score > b->best()->score;
  });
  wrapped(color(TaskStatus::Kind::Ready), m.reason + " · +" + dur(m.now()));
  wrapped_dim("按 继续 = " + m.hint() + "; 右键某一条的标题可回退到那一步; 关闭 = 放下这段 (已存盘, 可再载入)");
  ImGui::TextDisabled("%zu 步 · 调用 %d (观察 %d · 查研报 %d) · 候选 %d · 有参数组 %d · 提交 %d · 入库 %d", m.timeline.size(), c.all(), c.n(Cat::Look), c.docs(), c.staged(), c.n(Verdict::Weights),
                      c.n(Tool::SubmitFactor), c.adopted);
  const auto row = [&](size_t i) {
    const Result &r = *hits[i];
    ImGui::Bullet();
    ImGui::TextColored(kColHead, "%s  %s", r.name.c_str(), r.name_cn.c_str());
    ImGui::SameLine();
    result_line(in, r);
  };
  for (size_t i = 0; i < std::min(hits.size(), kSummaryRows); ++i)
    row(i);
  if (hits.size() > kSummaryRows && ImGui::TreeNodeEx("more", kNode, "其余 %zu 个", hits.size() - kSummaryRows)) {
    for (size_t i = kSummaryRows; i < hits.size(); ++i)
      row(i);
    ImGui::TreePop();
  }
}

// 流程的一笔 (停下 / 继续 / 作废的回复): 展示 = 一行 (停着时末一笔停下 = 停下汇总); prompt = 日志里的一节 (作废的回复原文在这)
void beat_entry(Ui &s, const Mine &m, const Cols &c, const LogMark &x, bool current) {
  const bool drop = x.kind == Beat::Drop;
  const Head h{drop ? kColDrop : kColBeat, def(x.kind).id, def(x.kind).cn, "", "", text::flat(x.line),
               color(drop ? TaskStatus::Kind::Error : current ? TaskStatus::Kind::Ready
                                                              : TaskStatus::Kind::None),
               x.line};
  entry(
      s, m, c, h, 0, false,
      [&] {
        if (current)
          summary_block(m);
        else
          wrapped(h.tone, x.line);
      },
      [&] {
        raw_part("md", "日志里的一节", x.md);
        if (current)
          raw_part("report", "停下报告", m.report);
      });
}

// 停着时末一笔 (按画的次序: 各步 发出后的笔 → 本步 → 回复 / 调用后的笔) 若是停下, 它的位置 (步, 笔); 否则无
std::optional<std::pair<size_t, size_t>> pause_at(const Mine &m) {
  if (m.phase() != Phase::Pause)
    return std::nullopt;
  for (size_t i = m.timeline.size(); i-- > 0;) {
    const std::vector<LogMark> &ms = m.timeline[i].marks;
    std::optional<size_t> at;
    for (size_t j = 0; j < ms.size(); ++j)
      if (ms[j].at != LogAt::Sent || !at || ms[*at].at == LogAt::Sent)
        at = j;
    if (at)
      return ms[*at].kind == Beat::Pause ? std::optional(std::pair(i, *at)) : std::nullopt;
  }
  return std::nullopt;
}

// 过程: 全会话的交互流, 一件一条. 钉在底部时跟新内容走; 放开后右下角给 "回到最新". bottom = 底下留给插话栏的高
void timeline_panel(Ui &s, Mine &m, const Flow &f, bool to_end, float bottom) {
  ImGui::BeginChild("##flow", ImVec2(0, -bottom), false);
  const float y = ImGui::GetScrollY();
  if (to_end || y >= ImGui::GetScrollMaxY() - 2.0f)
    s.pinned = true;
  else if (y < s.flow_y - 0.5f)
    s.pinned = false;
  if (ImGui::IsWindowHovered(ImGuiHoveredFlags_ChildWindows) && ImGui::IsMouseClicked(ImGuiMouseButton_Left))
    s.pinned = false; // 点开合 = 在读: 不把读的地方拖走
  bool start = false;
  if (m.phase() == Phase::None) {
    ImGui::TextDisabled("选好作用域按 New (或载入存过的): 系统把规则与工具表打包发给 agent, 它每条回复交一个工具调用 (看数据 / 回测 / 提交), 系统执行后把结果交回");
  } else {
    const Names adopted = AdoptedOf(m.timeline);
    const Cols c = cols_of();
    const std::optional<std::pair<size_t, size_t>> cur = pause_at(m);
    for (size_t i = 0; i < m.timeline.size(); ++i) {
      const Step &sp = m.timeline[i];
      const auto beats = [&](bool sent) {
        for (size_t j = 0; j < sp.marks.size(); ++j)
          if ((sp.marks[j].at == LogAt::Sent) == sent) {
            ImGui::PushID(static_cast<int>(j));
            beat_entry(s, m, c, sp.marks[j], cur == std::pair(i, j));
            ImGui::PopID();
          }
      };
      ImGui::PushID(static_cast<int>(i));
      if (i == 0) {
        ImGui::PushID("start");
        start_entry(s, m, c);
        ImGui::PopID();
      }
      if (!sp.memo.empty()) {
        ImGui::PushID("memo");
        memo_entry(s, m, c, i);
        ImGui::PopID();
      }
      ImGui::PushID("sent");
      beats(true);
      ImGui::PopID();
      ImGui::PushID("main");
      if (sp.act)
        start |= call_entry(s, m, c, adopted, i);
      else if (!sp.error.empty())
        retry_entry(s, m, c, i);
      else
        wait_entry(s, m, c, f, i);
      ImGui::PopID();
      ImGui::PushID("after");
      beats(false);
      ImGui::PopID();
      ImGui::PopID();
    }
  }
  if (start) {
    m.answer();
    s.pinned = true;
  }
  if (s.jump)
    s.jump = false, s.pinned = false;
  else if (s.pinned)
    ImGui::SetScrollHereY(1.0f);
  const bool behind = !s.pinned && ImGui::GetScrollY() < ImGui::GetScrollMaxY() - 2.0f;
  const ImVec2 corner(ImGui::GetWindowPos().x + ImGui::GetWindowWidth() - ImGui::GetStyle().ScrollbarSize, ImGui::GetWindowPos().y + ImGui::GetWindowHeight());
  s.flow_y = ImGui::GetScrollY();
  ImGui::EndChild();
  // 浮在时间线右下角 (单开子窗口: 叠在内容上也点得到); 点了下一帧钉回底部
  if (behind) {
    const ImGuiStyle &st = ImGui::GetStyle();
    const char *label = "回到最新";
    const ImVec2 sz(ImGui::CalcTextSize(label).x + 2.0f * st.FramePadding.x, ImGui::GetFrameHeight());
    ImGui::SetCursorScreenPos(ImVec2(corner.x - sz.x - st.WindowPadding.x, corner.y - sz.y - st.WindowPadding.y));
    ImGui::BeginChild("##latest", sz, ImGuiChildFlags_None, ImGuiWindowFlags_NoScrollbar | ImGuiWindowFlags_NoBackground);
    if (ImGui::Button(label))
      s.pinned = true;
    ImGui::EndChild();
  }
}

// 特征 / 算子 / 候选 三页的数据 = 候选台 Desk (会话中候选随评估实时更新). 无会话时随作用域重绑; 绑不上 (该层无可搜持有期 / 评估在收尾) = 空表
// 全局记录不随作用域, 随候选台的候选目录 (自检的 / 正式的) 读; 研究员在记录页标处理
struct View {
  const Context &ctx;
  const std::vector<Result> &results;
  Names adopted;
  Names staged; // 本会话落了文件的候选
  bool test;    // 候选与全局记录是自检会话的 (agent/test 目录)
  Notes &notes;
  const Desk &desk;
};

View view_of(const SharedData &data, Ui &s, const Scope &sc) {
  Mine &m = *s.mine;
  if (!m.held() && !m.lab.busy() && m.desk.scope() != sc && !catalog::holds(data, sc.level).empty())
    m.desk.bind(data, sc, false);
  if (!m.notes.loaded(m.desk.test()))
    m.notes.load(m.desk.test());
  static const Context kNoCtx;
  static const std::vector<Result> kNoResults;
  if (m.desk.scope() != sc)
    return {kNoCtx, kNoResults, {}, {}, m.desk.test(), m.notes, m.desk};
  const bool mine = m.held() && m.setup.sc == sc;
  return {m.desk.ctx(), m.desk.results(), mine ? AdoptedOf(m.timeline) : Names(), mine ? StagedOf(m.timeline) : Names(), m.desk.test(), m.notes, m.desk};
}

// 候选表: 列头点了排序 (默认 = 本持有期 score (中位 rIC) 降序); 本会话落的标 ●; 点一行在下面看详情与 Search 日志
void results_table(Ui &s, const View &v) {
  const std::vector<Result> &results = v.results;
  s.cand_filter.Draw("过滤##cand", 240);
  ImGui::SameLine();
  ImGui::TextDisabled("候选 %zu (%s; ● = 本会话落的; 列头点了排序, 点一行看详情)", results.size(), v.test ? "自检的 agent/test 目录" : "会话目录");
  const auto picked = std::find_if(results.begin(), results.end(), [&](const Result &r) { return r.file == s.pick; });
  const float detail_h = picked == results.end() ? 0.0f : ImGui::GetTextLineHeightWithSpacing() * 16.0f;
  constexpr ImGuiTableFlags kFlags = kTableFlags | ImGuiTableFlags_Sortable;
  if (!ImGui::BeginTable("##results", 10, kFlags, ImVec2(-1, -detail_h)))
    return;
  ImGui::TableSetupScrollFreeze(0, 1);
  constexpr ImGuiTableColumnFlags kDesc = ImGuiTableColumnFlags_PreferSortDescending;
  ImGui::TableSetupColumn("", ImGuiTableColumnFlags_NoSort);
  ImGui::TableSetupColumn("名");
  ImGui::TableSetupColumn("中文");
  ImGui::TableSetupColumn("结局");
  ImGui::TableSetupColumn("中位 rIC", kDesc);
  ImGui::TableSetupColumn("每期毛多空 bp", kDesc);
  ImGui::TableSetupColumn("每期净多空 bp", kDesc);
  ImGui::TableSetupColumn("年换手");
  ImGui::TableSetupColumn("思路");
  ImGui::TableSetupColumn("模板", ImGuiTableColumnFlags_WidthStretch | ImGuiTableColumnFlags_NoSort);
  ImGui::TableHeadersRow();
  // 统计列: 最佳权重 (中位 rIC + 每期毛 / 净多空 + 年换手)
  std::vector<size_t> ix;
  for (size_t i = 0; i < results.size(); ++i)
    if (const Result &r = results[i]; s.cand_filter.PassFilter((r.name + ' ' + r.name_cn + ' ' + r.tmpl + ' ' + r.note).c_str()))
      ix.push_back(i);
  if (const ImGuiTableSortSpecs *sp = ImGui::TableGetSortSpecs(); sp && sp->SpecsCount > 0) {
    const ImGuiTableColumnSortSpecs &c = sp->Specs[0];
    const auto num = [&](const Result &r) -> double {
      const Weight *w = r.best();
      switch (c.ColumnIndex) {
      case 4:
        return w ? w->score : -INFINITY;
      case 5:
        return w ? w->gross : -INFINITY;
      case 6:
        return w ? w->net : -INFINITY;
      case 7:
        return w ? w->turn : -INFINITY;
      default:
        return static_cast<double>(r.verdict);
      }
    };
    const auto str = [&](const Result &r) -> std::string {
      switch (c.ColumnIndex) {
      case 1:
        return r.name;
      case 2:
        return r.name_cn;
      default:
        return r.note;
      }
    };
    const bool text = c.ColumnIndex == 1 || c.ColumnIndex == 2 || c.ColumnIndex == 8;
    const bool up = c.SortDirection == ImGuiSortDirection_Ascending;
    std::stable_sort(ix.begin(), ix.end(), [&](size_t a, size_t b) {
      const Result &x = results[a], &y = results[b];
      return text ? (up ? str(x) < str(y) : str(y) < str(x)) : (up ? num(x) < num(y) : num(y) < num(x));
    });
  }
  for (const size_t i : ix) {
    const Result &r = results[i];
    ImGui::TableNextRow();
    ImGui::TableNextColumn();
    if (v.staged.contains(r.file))
      ImGui::TextColored(kColHead, "●");
    ImGui::TableNextColumn();
    if (ImGui::Selectable((r.name + "##" + r.file).c_str(), s.pick == r.file, ImGuiSelectableFlags_SpanAllColumns | ImGuiSelectableFlags_AllowOverlap))
      s.pick = s.pick == r.file ? std::string() : r.file;
    if (!r.note.empty())
      tip_long(r.note);
    ImGui::TableNextColumn();
    ImGui::TextUnformatted(r.name_cn.c_str());
    ImGui::TableNextColumn();
    ImGui::TextColored(color(kind_of(v.adopted, r)), "%s", v.adopted.count(r.name) ? "入库" : def(r.verdict).cn);
    tip_long(detail(r));
    const Weight *w = r.best();
    ImGui::TableNextColumn();
    if (w)
      ImGui::Text("%.4f", w->score);
    ImGui::TableNextColumn();
    if (w)
      ImGui::Text("%+.2f", w->gross * 1e4);
    ImGui::TableNextColumn();
    if (w)
      ImGui::Text("%+.2f", w->net * 1e4);
    ImGui::TableNextColumn();
    if (w)
      ImGui::Text("%.0f", w->turn);
    ImGui::TableNextColumn();
    if (!r.note.empty()) {
      ImGui::TextUnformatted(text::head(r.note, 16).c_str());
      tip_long(r.note);
    }
    ImGui::TableNextColumn();
    wrapped(r.tmpl);
  }
  ImGui::EndTable();
  // 选中的 (本帧可能刚换): 模板 / 思路 / 结局全文 + Search 日志 (换了选中才重读)
  const auto now_picked = std::find_if(results.begin(), results.end(), [&](const Result &r) { return r.file == s.pick; });
  if (picked == results.end() || now_picked == results.end())
    return;
  const Result &r = *now_picked;
  if (s.pick_log_of != s.pick)
    s.pick_log = v.desk.search_log(r.file), s.pick_log_of = s.pick;
  ImGui::BeginChild("##pick", ImVec2(-1, -1), ImGuiChildFlags_Borders);
  ImGui::TextColored(kColHead, "%s  %s", r.name.c_str(), r.name_cn.c_str());
  ImGui::SameLine();
  if (ImGui::SmallButton("复制模板"))
    ImGui::SetClipboardText(r.tmpl.c_str());
  ImGui::SameLine();
  if (ImGui::SmallButton("重读日志"))
    s.pick_log_of.clear();
  wrapped(ImVec4(0.9f, 0.8f, 0.55f, 1.0f), r.tmpl);
  if (!r.note.empty())
    wrapped_dim(r.note);
  wrapped(color(kind_of(v.adopted, r)), detail(r));
  fold("log", ImGuiTreeNodeFlags_None, "Search 日志 " + chars(s.pick_log), s.pick_log.empty() ? std::string("(没有)") : s.pick_log, true, kBoxLines, false);
  ImGui::EndChild();
}

// 一类全局记录, 新到旧; 研究员逐条标处理 (待办 / 已做 / 不做 / 搁置) 并可回一句, agent 在 list_notes 里看得到; 编号悬浮看出处
void notes_table(Ui &s, const View &v, NoteKind k) {
  const std::vector<Note> &all = v.notes.of(k);
  s.note_filter.Draw("过滤##note", 240);
  ImGui::SameLine();
  ImGui::Checkbox("只看待办", &s.only_open);
  ImGui::SameLine();
  ImGui::TextDisabled("%zu 条 · 待办 %d (%s, 新到旧); 标的处理与回复 agent 在 list_notes 里看得到", all.size(), v.notes.open(k), v.test ? "自检的 agent/test 目录" : "agent/main 目录");
  if (!begin_table("##notes", {"编号", "时间", "作用域", "附议", "处理", "内容"}))
    return;
  struct Change {
    std::string id;
    NoteState st;
    std::string reply;
  };
  std::optional<Change> change; // 表画完再改 (改写文件)
  for (auto it = all.rbegin(); it != all.rend(); ++it) {
    const Note &x = *it;
    const std::string title = x.title(), body = x.body();
    if ((s.only_open && x.state != NoteState::Open) || !s.note_filter.PassFilter((x.id + ' ' + x.scope + ' ' + title + ' ' + body + ' ' + x.reply).c_str()))
      continue;
    ImGui::PushID(x.id.c_str());
    ImGui::TableNextRow();
    ImGui::TableNextColumn();
    ImGui::TextUnformatted(x.id.c_str());
    if (ImGui::IsItemHovered())
      ImGui::SetTooltip("%s · %s", x.session.c_str(), StepTag(x.step).c_str());
    ImGui::TableNextColumn();
    ImGui::TextUnformatted(x.time.c_str());
    ImGui::TableNextColumn();
    ImGui::TextUnformatted(x.scope.c_str());
    ImGui::TableNextColumn();
    if (!x.ref.empty())
      ImGui::Text("附议 %s", x.ref.c_str());
    if (const int b = v.notes.backers(x); b > 0)
      ImGui::TextColored(color(TaskStatus::Kind::Ready), "%d 条附议", b);
    ImGui::TableNextColumn();
    ImGui::SetNextItemWidth(ImGui::GetFontSize() * 4.0f);
    if (ImGui::BeginCombo("##state", def(x.state).cn)) {
      for (const NoteState st : kVals<NoteState>)
        if (ImGui::Selectable(def(st).cn, x.state == st))
          change = Change{x.id, st, x.reply};
      ImGui::EndCombo();
    }
    ImGui::SameLine();
    if (ImGui::SmallButton("回复")) {
      s.reply = x.reply;
      ImGui::OpenPopup("##reply");
    }
    if (ImGui::BeginPopup("##reply")) {
      ImGui::TextDisabled("给 agent 的一句回复 (list_notes 里随处理一起给; 空 = 不回)");
      input_multiline("##reply_text", s.reply, ImVec2(ImGui::GetFontSize() * 30.0f, ImGui::GetTextLineHeightWithSpacing() * 4.0f));
      if (ImGui::Button("保存", ImVec2(80, 0))) {
        change = Change{x.id, x.state, std::string(text::trim(s.reply))};
        ImGui::CloseCurrentPopup();
      }
      ImGui::SameLine();
      if (ImGui::Button("取消", ImVec2(80, 0)))
        ImGui::CloseCurrentPopup();
      ImGui::EndPopup();
    }
    ImGui::TableNextColumn();
    wrapped(kColHead, title);
    wrapped_dim(body);
    if (x.state != NoteState::Open || !x.reply.empty())
      wrapped(color(x.state == NoteState::Done ? TaskStatus::Kind::Ready : TaskStatus::Kind::Warn), "研究员: " + std::string(def(x.state).cn) + (x.reply.empty() ? "" : " · " + x.reply));
    ImGui::PopID();
  }
  ImGui::EndTable();
  if (change)
    v.notes.set(k, change->id, change->st, std::move(change->reply));
}

// 插话栏 (过程页底): 草稿 → 排队, 随下一条发出的消息附上; 排着的可撤回. memo_height = 它占的高
float memo_height(const Mine &m) {
  return m.held() ? ImGui::GetTextLineHeightWithSpacing() * (m.memo.empty() ? 3.2f : 4.4f) + ImGui::GetStyle().ItemSpacing.y : 0.0f;
}
void memo_bar(Ui &s, Mine &m) {
  if (!m.held())
    return;
  if (!m.memo.empty()) {
    ImGui::TextColored(color(TaskStatus::Kind::Warn), "已排队的插话 · %s · 随下一条发出", chars(m.memo).c_str());
    tip_long(m.memo);
    ImGui::SameLine();
    if (ImGui::SmallButton("撤回")) {
      const std::string back = m.unqueue();
      s.memo = back + (s.memo.empty() ? "" : "\n\n" + s.memo);
    }
  }
  input_multiline("##memo", s.memo, ImVec2(-90, ImGui::GetTextLineHeightWithSpacing() * 2.6f));
  ImGui::SameLine();
  const std::string_view text = text::trim(s.memo);
  ImGui::BeginDisabled(text.empty());
  if (ImGui::Button("插话", ImVec2(80, 0))) {
    m.queue(std::string(text));
    s.memo.clear();
  }
  ImGui::EndDisabled();
  tip("给 agent 的意见: 排队, 随下一条发出的消息附上 (不打断在收的回复与在跑的调用; 停着时随继续发出)");
}

// 回退确认框: 写明会丢的步数 / 候选文件 / 全局记录
void rewind_modal(Ui &s, Mine &m) {
  Ui::Rewind &r = s.rewind;
  if (r.open)
    ImGui::OpenPopup("回退###rewind"), r.open = false;
  ImGui::SetNextWindowSize(ImVec2(ImGui::GetFontSize() * 48.0f, ImGui::GetFontSize() * 12.0f), ImGuiCond_Appearing);
  if (!ImGui::BeginPopupModal("回退###rewind"))
    return;
  if (m.phase() != Phase::Pause || r.k < 1 || r.k > m.step()) {
    ImGui::CloseCurrentPopup();
    ImGui::EndPopup();
    return;
  }
  const std::string k = std::to_string(r.k);
  const Mine::Loss &l = r.loss;
  ImGui::TextColored(kColHead, "%s", ("从 " + StepTag(r.k) + " 重来").c_str());
  wrapped_dim("状态回到第 " + k + " 条消息发出时, 按 继续 重发它, agent 重答; 它随附的插话退回排队 (可撤回改写), 要改方向就再插话");
  wrapped(color(TaskStatus::Kind::Warn), "将丢弃: " + StepTag(r.k) + " 起 " + std::to_string(l.steps) + " 步的回复与调用 · 候选文件 " + std::to_string(l.files) + " 个 (连同 Search 日志) · 本会话的全局记录 " +
                                             std::to_string(l.notes) + " 条; 已入正式库的不动; 不可撤销");
  ImGui::BeginDisabled(!m.idle());
  if (ImGui::Button("确定", ImVec2(100, 0))) {
    m.rewind(r.k);
    ImGui::CloseCurrentPopup();
    s.raw.clear(), s.pinned = true;
  }
  ImGui::EndDisabled();
  ImGui::SameLine();
  if (ImGui::Button("取消", ImVec2(100, 0)))
    ImGui::CloseCurrentPopup();
  ImGui::EndPopup();
}

// ---- 参考: 发给 agent 的特征表 / 算子表 / 工具表 ----
// 人勾开放 (特征 / 算子页同一份): all = 本表全部键, keys = 要改的; 勾满 = 回到全开 (此后新加的也开), 否则白名单
Open pick(Open o, const std::vector<std::string> &all, const std::vector<std::string> &keys, bool on) {
  if (o.all)
    o = {false, std::set<std::string>(all.begin(), all.end())};
  for (const std::string &k : keys) {
    if (on)
      o.on.insert(k);
    else
      o.on.erase(k);
  }
  if (std::all_of(all.begin(), all.end(), [&](const std::string &k) { return o.on.contains(k); }))
    o = {};
  return o;
}
// 全选 / 全清 (只动列出的行); 按了 → next
void pick_bar(const Open &o, const std::vector<std::string> &all, const std::vector<std::string> &shown, bool locked, std::optional<Open> &next) {
  const char *why = locked ? "有会话 (在走或停着) 时不可改; 关闭会话后改, 下次 New 生效" : "只动列出的行 (过滤后); 勾满 = 全开, 此后新加的也开; 否则只开勾了的";
  ImGui::BeginDisabled(locked);
  for (const bool on : {true, false}) {
    ImGui::SameLine();
    if (ImGui::SmallButton(on ? "全选" : "全清"))
      next = pick(o, all, shown, on);
    tip(why);
  }
  ImGui::EndDisabled();
}
// 一行的开放勾选框; 关着的行字变灰 (到 pick_end)
void pick_cell(const Open &o, const std::vector<std::string> &all, const std::string &key, bool locked, std::optional<Open> &next) {
  bool on = o.has(key);
  ImGui::TableNextColumn();
  ImGui::BeginDisabled(locked);
  if (ImGui::Checkbox(("##pick_" + key).c_str(), &on))
    next = pick(next.value_or(o), all, {key}, on);
  ImGui::EndDisabled();
  if (!o.has(key))
    ImGui::PushStyleColor(ImGuiCol_Text, ImGui::GetStyleColorVec4(ImGuiCol_TextDisabled));
}
void pick_end(const Open &o, const std::string &key) {
  if (!o.has(key))
    ImGui::PopStyleColor();
}

// 本层全部叶 (来源未开放的不列), 逐行勾开放给 agent 的
void leaves_panel(Ui &s, Scope &sc, const Context &c, bool locked) {
  const Open &open = sc.leaves_of(c.level);
  s.leaf_filter.Draw("过滤##leaf", 240);
  const auto pass = [&](const Leaf &l) {
    return sc.has(l.src) && s.leaf_filter.PassFilter((l.code + ' ' + l.name_cn + ' ' + l.desc + ' ' + l.family).c_str());
  };
  std::vector<std::string> all, shown;
  Per<Src, int> n{}, on{}; // [Src] 列出的叶 / 其中开放的
  for (const Leaf &l : c.all_leaves) {
    all.push_back(l.code);
    if (!pass(l))
      continue;
    shown.push_back(l.code);
    ++n[l.src];
    on[l.src] += open.has(l.code);
  }
  std::optional<Open> next;
  pick_bar(open, all, shown, locked, next);
  std::string counts;
  for (const Src src : kVals<Src>)
    if (n[src] > 0)
      counts += (counts.empty() ? "" : " · ") + std::string(def(src).id) + " " + std::to_string(on[src]) + "/" + std::to_string(n[src]);
  ImGui::SameLine();
  ImGui::TextDisabled("开放 / 列出: %s 个叶, 模板里写全名; 悬浮看公式", counts.c_str());
  if (begin_table("##leaves", {"开放", "叶", "中文名", "说明"})) {
    for (const Leaf &l : c.all_leaves) {
      if (!pass(l))
        continue;
      ImGui::TableNextRow();
      pick_cell(open, all, l.code, locked, next);
      ImGui::TableNextColumn();
      ImGui::TextUnformatted(l.code.c_str());
      if (ImGui::IsItemHovered())
        ImGui::SetTooltip("%s", l.formula.c_str());
      ImGui::TableNextColumn();
      ImGui::TextUnformatted(l.name_cn.c_str());
      ImGui::TableNextColumn();
      wrapped(l.desc);
      pick_end(open, l.code);
    }
    ImGui::EndTable();
  }
  if (!next)
    return;
  if (next->all)
    sc.leaves.erase(c.level);
  else
    sc.leaves[c.level] = std::move(*next);
}

// 全部算子, 逐行勾开放给 agent 的
void ops_panel(Ui &s, Scope &sc, const Context &c, bool locked) {
  Open &open = sc.ops;
  s.op_filter.Draw("过滤##op", 240);
  const auto pass = [&](const Op &o) { return s.op_filter.PassFilter((o.name + ' ' + o.name_cn + ' ' + o.note).c_str()); };
  std::vector<std::string> all, shown;
  int on = 0;
  for (const Op &o : c.all_ops) {
    all.push_back(o.name);
    if (!pass(o))
      continue;
    shown.push_back(o.name);
    on += open.has(o.name);
  }
  std::optional<Open> next;
  pick_bar(open, all, shown, locked, next);
  ImGui::SameLine();
  ImGui::TextDisabled("开放 / 列出: %d/%zu 个算子; 悬浮看签名与定义", on, shown.size());
  if (begin_table("##ops", {"开放", "算子", "中文名", "输出域", "说明"})) {
    for (const Op &o : c.all_ops) {
      if (!pass(o))
        continue;
      ImGui::TableNextRow();
      pick_cell(open, all, o.name, locked, next);
      ImGui::TableNextColumn();
      ImGui::TextUnformatted(o.name.c_str());
      if (ImGui::IsItemHovered())
        ImGui::SetTooltip("签名: %s\n定义: %s", o.sig.c_str(), o.tex.c_str());
      ImGui::TableNextColumn();
      ImGui::TextUnformatted(o.name_cn.c_str());
      ImGui::TableNextColumn();
      ImGui::TextUnformatted(o.out.c_str());
      ImGui::TableNextColumn();
      wrapped(o.note);
      pick_end(open, o.name);
    }
    ImGui::EndTable();
  }
  if (next)
    open = std::move(*next);
}

// 工具表 = 系统提示 "工具" 节的同一份 (kTools); ask_human 只在可提问的会话里发给 agent
void tools_panel(Ui &s) {
  s.tool_filter.Draw("过滤##tool", 240);
  ImGui::SameLine();
  ImGui::TextDisabled("%zu 个工具; 悬浮参数看类型与说明, 悬浮说明看返回", kNum<Tool>);
  if (!begin_table("##tools", {"工具", "中文名", "执行", "参数", "说明"}))
    return;
  for (const Tool t : kVals<Tool>) {
    const ToolMeta &m = def(t);
    const ToolDoc &d = doc(t);
    const std::string desc = ToolText(d.desc);
    if (!s.tool_filter.PassFilter((std::string(m.id) + ' ' + m.cn + ' ' + desc).c_str()))
      continue;
    ImGui::TableNextRow();
    ImGui::TableNextColumn();
    ImGui::TextUnformatted(m.id);
    ImGui::TableNextColumn();
    ImGui::TextUnformatted(m.cn);
    ImGui::TableNextColumn();
    ImGui::TextUnformatted(def(m.form).cn);
    ImGui::TableNextColumn();
    std::string keys, args;
    for (const ArgDef &a : d.args)
      keys += (keys.empty() ? "" : " ") + std::string(a.key), args += (args.empty() ? "" : "\n") + ArgText(a);
    ImGui::TextUnformatted(keys.empty() ? "—" : keys.c_str());
    if (!args.empty())
      tip_long(args);
    ImGui::TableNextColumn();
    wrapped(desc + (t == Tool::AskHuman ? " (只在可提问的会话里给)" : ""));
    tip_long("返回: " + ToolText(d.out));
  }
  ImGui::EndTable();
}

// 本会话要求: 过程页顶的折叠块 (默认展开, 开会话即折起; 折起时悬浮看全文). 有会话时只读 = 已随首条发出的那份; 其余时候编辑下次 New / Test 要发的
void remark_panel(Ui &s, const Mine &m) {
  const bool live = m.held();
  const std::string &text = live ? m.setup.remark : s.remark;
  if (s.fold_remark)
    ImGui::SetNextItemOpen(false, ImGuiCond_Always), s.fold_remark = false;
  const std::string label = "本会话要求 · " + (text.empty() ? std::string("无") : chars(text)) + "###remark";
  if (!ImGui::CollapsingHeader(label.c_str(), ImGuiTreeNodeFlags_DefaultOpen)) {
    if (!text.empty())
      tip_long(text);
    return;
  }
  if (live) {
    text.empty() ? wrapped_dim("(无)") : wrapped(text);
    return;
  }
  ImGui::TextDisabled("只对下一段会话: 附在首条消息 (自检会话的首条) 末尾发给 agent, 不改模板; 例: 着重测试某些算子 / 特征, 或交代这次的背景");
  input_multiline("##remark", s.remark, ImVec2(-1, ImGui::GetTextLineHeightWithSpacing() * 4));
}

} // namespace

// 会话换了 (不论哪个入口开的 / 载入的): 清上下文缓存与 prompt 模式, 本会话要求折起, 过程页从头钉底 (不覆写 ui.json 的默认作用域)
void follow(Ui &s) {
  const Mine &m = *s.mine;
  const std::string dir = m.held() ? m.log.dir() : std::string();
  if (dir == s.seen_dir)
    return;
  s.seen_dir = dir;
  s.ctx.clear(), s.raw.clear();
  if (dir.empty())
    return;
  s.message.clear();
  s.fold_remark = true, s.pinned = true;
}

// 过程 (本会话要求 + 全宽时间线 + 插话栏) / 候选表 / 特征表 / 算子表 (这三页不必开会话, 随作用域) / 工具表 (固定);
// 刚进 Choose 切回过程并滚到底 (问题在那); 回退确认框浮在页上. 大脑是大模型还是外部 agent, 这里都一样
void render_mine(SharedData &data, Ui &s) {
  ImGui::TextDisabled("外部 agent 的 MCP 接口 %s: open 同这里按 New 开新会话, 无参 run 接上已有外部会话", s.remote->listen().c_str());
  Mine &m = *s.mine;
  Scope shown = m.held() ? m.setup.sc : s.opt.sc;
  Scope &sc = m.held() ? shown : s.opt.sc;
  scope_row(data, sc, s.pool_rows, m.desk, scope_locked(s), "1. Scope:");
  session_row(data, s, sc);
  follow(s);
  m.chat.live(s.chat_live);
  m.lab.live(s.lab_live);
  const Flow f = flow_of(s, m);
  now_row(s, m, f);
  ImGui::Separator();
  const Phase ph = m.phase();
  const bool to_choose = ph == Phase::Choose && s.seen_phase != Phase::Choose;
  s.seen_phase = ph;
  if (!ImGui::BeginTabBar("##mine_tabs"))
    return;
  if (ImGui::BeginTabItem("过程###flow", nullptr, to_choose || s.jump ? ImGuiTabItemFlags_SetSelected : ImGuiTabItemFlags_None)) {
    remark_panel(s, m);
    timeline_panel(s, m, f, to_choose, memo_height(m));
    memo_bar(s, m);
    ImGui::EndTabItem();
  }
  const View v = view_of(data, s, sc);
  if (ImGui::BeginTabItem("候选###cands")) {
    if (ImGui::BeginTabBar("##cand_tabs")) {
      const std::string factors = "因子 (" + std::to_string(v.results.size()) + ")###factors";
      if (ImGui::BeginTabItem(factors.c_str())) {
        results_table(s, v);
        ImGui::EndTabItem();
      }
      for (const NoteKind k : kVals<NoteKind>) {
        const std::string label = std::string(def(k).cn) + " (" + std::to_string(v.notes.open(k)) + "/" + std::to_string(v.notes.of(k).size()) + ")###" + def(k).id;
        if (ImGui::BeginTabItem(label.c_str())) {
          notes_table(s, v, k);
          ImGui::EndTabItem();
        }
      }
      ImGui::EndTabBar();
    }
    ImGui::EndTabItem();
  }
  const Open &leaves_open = sc.leaves_of(v.ctx.level);
  int leaves_n = 0, leaves_on = 0, ops_on = 0;
  for (const Leaf &l : v.ctx.all_leaves)
    if (sc.has(l.src))
      ++leaves_n, leaves_on += leaves_open.has(l.code);
  for (const Op &o : v.ctx.all_ops)
    ops_on += sc.ops.has(o.name);
  const std::string leaves = "特征 (" + std::to_string(leaves_on) + "/" + std::to_string(leaves_n) + ")###leaves";
  const std::string ops = "算子 (" + std::to_string(ops_on) + "/" + std::to_string(v.ctx.all_ops.size()) + ")###ops";
  if (ImGui::BeginTabItem(leaves.c_str())) {
    leaves_panel(s, sc, v.ctx, scope_locked(s));
    ImGui::EndTabItem();
  }
  if (ImGui::BeginTabItem(ops.c_str())) {
    ops_panel(s, sc, v.ctx, scope_locked(s));
    ImGui::EndTabItem();
  }
  const std::string tools = "工具 (" + std::to_string(kNum<Tool>) + ")###tools";
  if (ImGui::BeginTabItem(tools.c_str())) {
    tools_panel(s);
    ImGui::EndTabItem();
  }
  ImGui::EndTabBar();
  rewind_modal(s, m);
}

} // namespace GUI::Agent::page
