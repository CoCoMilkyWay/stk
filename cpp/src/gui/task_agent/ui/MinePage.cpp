// Mine 页 — 见 gui/task_agent/TaskAgent.hpp
#include "gui/task_agent/Prompt.hpp"
#include "gui/task_agent/Text.hpp"
#include "gui/task_agent/tools/Misc.hpp"
#include "gui/task_agent/tools/Trial.hpp"
#include "gui/task_agent/ui/Pages.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

#include <algorithm>
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
  s.mine->load(data, x.dir);
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
    const std::string label = stamp_text(x.stamp) + (x.test ? " · 自检" : "") + " · " + std::to_string(b.at("steps").get<int>()) + " 步 · " + text::head(why, 24) + " · 候选 " +
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
  const bool bound = m.lab.scope() == active_sc;
  const bool has_leaves = bound && !m.lab.ctx().leaves.empty(), has_ops = bound && !m.lab.ctx().ops.empty();
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

// ---- 工具用量条: 按类拼接, 类内一工具一段 (同色相深浅相间), 段宽 = 次数占比; 类段够宽写类名与占比, 悬浮看各类各工具的次数 / 占比 / 失败数 (表格) ----
constexpr Per<Cat, ImVec4> kCatCols{{{{0.35f, 0.78f, 0.45f, 1.0f}, {0.95f, 0.62f, 0.25f, 1.0f}, {0.38f, 0.62f, 0.95f, 1.0f}, {0.72f, 0.52f, 0.92f, 1.0f}, {0.58f, 0.58f, 0.58f, 1.0f}}}};

ImU32 shade(ImVec4 c, int i) {
  const float f = 1.0f - 0.2f * static_cast<float>(i % 3);
  return ImGui::ColorConvertFloat4ToU32(ImVec4(c.x * f, c.y * f, c.z * f, c.w));
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
    int i = 0;
    for (const Tool t : kVals<Tool>) {
      if (def(t).cat != k || c.n(t) == 0)
        continue;
      const float x1 = x + w * static_cast<float>(c.n(t)) / static_cast<float>(all);
      dl->AddRectFilled(ImVec2(x, p0.y), ImVec2(x1, p0.y + h), shade(kCatCols[k], i++));
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
        const std::string name = std::string("  ") + m.id;
        row(hit == t ? kColHead : ImGui::GetStyleColorVec4(ImGuiCol_Text), name.c_str(), m.cn, c.n(t), c.failed(t));
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
    what = StepTag(m.step()) + " · agent " + doing(f);
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
      what = StepTag(m.step()) + " " + def(a->call.tool).cn + " · " + text::head(a->call.text(), kTopicChars) + " · " + StageOf(*a, ll.frac) + " " + dur(a->span.len(t));
      if (def(a->call.tool).exec == Exec::Lab)
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

// 时间线 = 调用流: 组 (每 kGroupSteps 步, 超过一组才分) → 一步一行, 主体是它的调用 (标题 = 编号 + 调用 + 状态 / 结局), 展开 = 结果, 末尾附大脑那一侧
// (发出的消息 / 上下文 / 思考 / 回复原文); 还没交来调用的步 (待发 / 在收 / 不合协议) 标题写它在干什么. 新一步来时只留最近一组展开; 人手动开合的保持
constexpr size_t kGroupSteps = 20;
constexpr ImGuiTreeNodeFlags kNode = ImGuiTreeNodeFlags_SpanAvailWidth | ImGuiTreeNodeFlags_DrawLinesFull;
constexpr size_t kCallChars = 96; // 调用一行在标题里的字数

void context_block(Ui &s, const Mine &m, size_t i);

// 过程页的一步那一行: open() 在标题项之前 (s.jump 时末步展开), head() 紧跟标题项 (停着时右键回退; s.jump 时滚到它), brain() 在展开体末尾
struct Row {
  Ui &s;
  Mine &m;
  size_t i;
  const Flow &f;
  const Names &adopted;
  bool last() const { return i + 1 == m.timeline.size(); }
  bool receiving() const { return last() && m.timeline[i].span.running() && f.stage != Flow::Idle; }
  void open() const {
    if (s.jump && last())
      ImGui::SetNextItemOpen(true, ImGuiCond_Always);
  }
  void head() const;
  void brain() const;
};

void Row::head() const {
  const int k = static_cast<int>(i) + 1;
  const bool editable = m.phase() == Phase::Pause && m.idle();
  if (editable && ImGui::IsItemHovered() && !ImGui::IsPopupOpen("##rewind_menu"))
    ImGui::SetTooltip("右键: 从这一步重来 (丢这一步的回复与调用及其后各步, 继续即重发这条消息)");
  if (editable && ImGui::BeginPopupContextItem("##rewind_menu")) {
    ImGui::TextDisabled("%s · 回退", StepTag(k).c_str());
    if (ImGui::MenuItem(("从 " + StepTag(k) + " 重来…").c_str()))
      s.rewind = {.k = k, .loss = m.loss(k), .open = true};
    ImGui::EndPopup();
  }
  if (s.jump && last())
    ImGui::SetScrollHereY(0.0f);
}

// 大脑那一侧: 发出的消息 (+ 插话) · 看到的上下文 · 思考 (在收的那一步思考时自动展开) · 在写的正文 · 回复一行 (+ 不合协议的原因) · 回复原文
void Row::brain() const {
  const Step &sp = m.timeline[i];
  const bool live = receiving();
  const llm::Reply &r = sp.span.ended() || !sp.span.began() ? sp.reply : s.chat_live.reply;
  const std::string sent = TextOf(sp);
  fold("send", ImGuiTreeNodeFlags_None, std::string(sp.span.began() ? "发出的消息 " : "待发的消息 ") + def(sp.kind).cn + " · " + chars(sent) + (sp.memo.empty() ? "" : " · 附插话"), sent, false,
       kBoxLines, false);
  context_block(s, m, i);
  if (live && f.stage == Flow::Wait)
    ImGui::TextColored(color(f.stalled() ? TaskStatus::Kind::Warn : TaskStatus::Kind::Busy), "%s", doing(f).c_str());
  if (!r.thinking.empty()) {
    const bool now = live && f.stage == Flow::Think;
    if (last()) {
      if (s.think_step != i)
        s.think_step = i, s.think_open = false, s.think_touched = false;
      if (!s.think_touched && s.think_open != now) {
        ImGui::SetNextItemOpen(now, ImGuiCond_Always);
        s.think_open = now;
      }
    }
    std::string label = now ? doing(f) : "思考了 " + dur(sp.think.len(m.now()));
    if (const std::string t = now ? f.topic : topic(r.thinking, false); !t.empty())
      label += " · " + t;
    if (fold("think", ImGuiTreeNodeFlags_None, label + " · " + chars(r.thinking), r.thinking, true, kThinkLines, true) && last())
      s.think_touched = true;
  }
  if (live && f.stage == Flow::Write)
    fold("text_live", ImGuiTreeNodeFlags_DefaultOpen, doing(f) + " · " + chars(r.text), r.text, false, kThinkLines, true);
  if (!sp.span.ended())
    return;
  std::string line = "回复 · " + dur(sp.span.len(m.now())) + (sp.think.began() ? " (思考 " + dur(sp.think.len(m.now())) + ")" : "");
  if (r.usage.prompt > 0)
    line += " · token 输入 " + kilo(r.usage.prompt) + " (缓存 " + kilo(r.usage.cached) + ") / 输出 " + kilo(r.usage.completion);
  ImGui::TextColored(color(sp.error.empty() ? TaskStatus::Kind::None : TaskStatus::Kind::Error), "%s", line.c_str());
  if (!sp.error.empty())
    wrapped(color(TaskStatus::Kind::Error), "不合协议: " + sp.error);
  fold("text", ImGuiTreeNodeFlags_None, "回复原文 " + chars(r.text), r.text, false, kBoxLines, false);
}

// 调用用了多久 (eval_factor 分 Search / Run)
std::string took(const Act &a, double t) {
  const TrialRes *k = AsTrial(a);
  if (!k || !k->search.began())
    return dur(a.span.len(t));
  return "Search " + dur(k->search.len(t)) + (k->run.began() ? " · Run " + dur(k->run.len(t)) : std::string());
}

// 一步的调用: 标题 = 编号 + 中文名 · 主语 (Res::title, 无则调用原文) + 状态 / 结局 (回测写候选结局, 入库另标), 悬浮看调用全文;
// 展开 = 展示 (在问的 ask_human = 答题) + 原文, 末尾附大脑那一侧. 回测定论那刻 (换节点 id, 开合重定) 未落 / BROKEN / 有参数组的自动展开; 返回 = 按了交答案
bool act_block(const Row &row, Act &a, bool edit) {
  const Mine &m = row.m;
  const double t = m.now();
  const TrialRes *k = AsTrial(a);
  const bool done = a.span.ended();
  std::string state;
  TaskStatus::Kind kind = TaskStatus::Kind::None;
  if (done && k)
    state = took(a, t) + " · " + brief(row.adopted, k->st.res), kind = kind_of(row.adopted, k->st.res);
  else if (done)
    state = took(a, t) + (a.out.ok ? "" : " · 没做成"), kind = a.out.ok ? TaskStatus::Kind::None : TaskStatus::Kind::Warn;
  else if (edit)
    state = "等你答", kind = TaskStatus::Kind::Warn;
  else if (a.span.running() && m.live())
    state = StageOf(a, row.s.lab_live.frac) + " · " + dur(a.span.len(t)), kind = TaskStatus::Kind::Busy;
  else
    state = m.live() ? "排队" : "未做", kind = TaskStatus::Kind::Muted;
  const std::string sub = a.out.res ? a.out.res->title() : std::string();
  const std::string label = StepTag(static_cast<int>(row.i) + 1) + " " + def(a.call.tool).cn + " · " + (sub.empty() ? text::head(a.call.text(), kCallChars) : sub) + " · " + state;
  if (done && k)
    ImGui::SetNextItemOpen(k->st.res.verdict == Verdict::Unstaged || k->st.res.verdict == Verdict::Broken || k->st.res.verdict == Verdict::Weights, ImGuiCond_Once);
  row.open();
  ImGui::PushStyleColor(ImGuiCol_Text, color(kind));
  const bool open = ImGui::TreeNodeEx(done && k ? "settled" : "act", kNode | (AsAsk(a) ? ImGuiTreeNodeFlags_DefaultOpen : 0), "%s", label.c_str());
  ImGui::PopStyleColor();
  if (!open && ImGui::IsItemHovered())
    tip_long(a.call.text());
  row.head();
  if (done)
    copy_button("复制##act", a.out.text);
  bool start = false;
  if (open) {
    if (edit) {
      start = edit_asked(AsAsk(a)->asked);
    } else if (a.out.res) {
      a.out.res->draw();
      if (done && !a.out.text.empty())
        fold("raw", ImGuiTreeNodeFlags_None, "原文 · " + chars(a.out.text), a.out.text, true, kBoxLines, false);
    } else {
      box(a.out.text, false, kBoxLines, false);
    }
    row.brain();
    ImGui::TreePop();
  }
  return start;
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

// 一步: 有调用即画调用 (act_block); 还没有的 (待发 / 在收 / 不合协议) 标题写它在干什么, 展开 = 大脑那一侧. 返回 = 按了交答案
bool step_block(const Row &row) {
  Step &sp = row.m.timeline[row.i];
  ImGui::PushID(static_cast<int>(row.i));
  bool start = false;
  if (sp.act) {
    start = act_block(row, *sp.act, row.last() && row.m.phase() == Phase::Choose);
  } else {
    const std::string tag = StepTag(static_cast<int>(row.i) + 1);
    std::string head;
    TaskStatus::Kind kind = TaskStatus::Kind::None;
    if (!sp.span.began())
      head = tag + " · 待发" + (row.m.live() ? "" : " (按 继续 发出)") + " · " + def(sp.kind).cn, kind = TaskStatus::Kind::Warn;
    else if (row.receiving())
      head = tag + " · " + doing(row.f) + (row.f.topic.empty() ? "" : " · " + row.f.topic), kind = row.f.stalled() ? TaskStatus::Kind::Warn : TaskStatus::Kind::Busy;
    else
      head = tag + " · 回复不合协议: " + text::head(std::string_view(sp.error).substr(0, sp.error.find('\n')), kCallChars), kind = TaskStatus::Kind::Error;
    row.open();
    ImGui::PushStyleColor(ImGuiCol_Text, color(kind));
    const bool open = ImGui::TreeNodeEx("step", kNode | (row.receiving() ? ImGuiTreeNodeFlags_DefaultOpen : 0), "%s", head.c_str());
    ImGui::PopStyleColor();
    row.head();
    if (open) {
      row.brain();
      ImGui::TreePop();
    }
  }
  ImGui::PopID();
  return start;
}

// 一组步的标题: 编号段 / 起始时刻 / 调用 / 观察 / 查研报 / 候选 / 有权重 / 入库
std::string group_head(const Mine &m, size_t g0, size_t g1) {
  const Tally c = TallyOf(Steps(m.timeline).subspan(g0, g1 - g0), m.now());
  return StepTag(static_cast<int>(g0) + 1) + "–" + std::to_string(g1) + " · +" + dur(m.timeline[g0].span.t0) + " · 调用 " + std::to_string(c.all()) + " · 观察 " +
         std::to_string(c.n(Cat::Look)) + " · 查研报 " + std::to_string(c.docs()) + " · 候选 " + std::to_string(c.staged()) + " · 有权重 " + std::to_string(c.n(Verdict::Weights)) +
         " · 入库 " + std::to_string(c.adopted);
}

// 停下汇总: 原因与继续会做什么 + 计数 + 本会话有参数组的候选 (入库的在前, 再按 score (中位 rIC) 降序; 先列 kSummaryRows 个, 其余折起) + 停下报告
constexpr size_t kSummaryRows = 5;
void summary_block(const Mine &m) {
  const Tally c = m.tally();
  const Names in = AdoptedOf(m.timeline);
  std::vector<const Result *> hits;
  for (const Step &sp : m.timeline)
    if (const TrialRes *k = sp.act ? AsTrial(*sp.act) : nullptr; k && k->st.res.verdict == Verdict::Weights)
      hits.push_back(&k->st.res);
  std::stable_sort(hits.begin(), hits.end(), [&](const Result *a, const Result *b) {
    const bool ia = in.count(a->name) > 0, ib = in.count(b->name) > 0;
    return ia != ib ? ia : a->best()->score > b->best()->score;
  });
  ImGui::SeparatorText(("停下 · +" + dur(m.now())).c_str());
  wrapped(color(TaskStatus::Kind::Ready), m.reason);
  wrapped_dim("按 继续 = " + m.hint() + "; 右键某一步的标题可回退到那一步; 关闭 = 放下这段 (已存盘, 可再载入)");
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
  fold("report", ImGuiTreeNodeFlags_None, "停下报告", m.report, false, kBoxLines, false);
}

// 过程: 全会话的调用流 (分组 → 逐步) + 停下汇总. 钉在底部时跟新内容走; 放开后右下角给 "回到最新".
// s.flow_steps = 上一帧画过的步数 (变了 = 新一步来, 只留末组展开); bottom = 底下留给插话栏的高
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
    const size_t n = m.timeline.size();
    const bool fresh = n != s.flow_steps, grouped = n > kGroupSteps;
    const Names adopted = AdoptedOf(m.timeline);
    s.flow_steps = n;
    for (size_t g0 = 0; g0 < n; g0 += kGroupSteps) {
      const size_t g1 = std::min(n, g0 + kGroupSteps);
      ImGui::PushID(static_cast<int>(g0));
      bool open = true;
      if (grouped) {
        if (s.jump && g1 == n)
          ImGui::SetNextItemOpen(true, ImGuiCond_Always);
        else if (fresh)
          ImGui::SetNextItemOpen(g1 == n, ImGuiCond_Always);
        const bool busy = g1 == n && (m.live() || !m.idle());
        ImGui::PushStyleColor(ImGuiCol_Text, color(busy ? TaskStatus::Kind::Busy : TaskStatus::Kind::None));
        open = ImGui::TreeNodeEx("group", kNode | ImGuiTreeNodeFlags_Framed, "%s", group_head(m, g0, g1).c_str());
        ImGui::PopStyleColor();
      }
      if (open) {
        for (size_t i = g0; i < g1; ++i)
          start |= step_block({s, m, i, f, adopted});
        if (grouped)
          ImGui::TreePop();
      }
      ImGui::PopID();
    }
  }
  if (start) {
    m.answer();
    s.pinned = true;
  }
  if (m.phase() == Phase::Pause)
    summary_block(m);
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

// 特征 / 算子 / 候选 三页的数据 = Lab (会话中候选随评估实时更新). 无会话时随作用域重绑; 绑不上 (该层无可搜持有期 / 评估在收尾) = 空表
// 全局记录不随作用域, 随 Lab 的候选目录 (自检的 / 正式的) 读; 研究员在记录页标处理
struct View {
  const Context &ctx;
  const std::vector<Result> &results;
  Names adopted;
  Names staged; // 本会话落了文件的候选
  bool test;    // 候选与全局记录是自检会话的 (agent/test 目录)
  Notes &notes;
  const Lab &lab;
};

View view_of(const SharedData &data, Ui &s, const Scope &sc) {
  Mine &m = *s.mine;
  if (!m.held() && !m.lab.busy() && m.lab.scope() != sc && !catalog::holds(data, sc.level).empty())
    m.lab.bind(data, sc, false);
  if (!m.notes.loaded(m.lab.test()))
    m.notes.load(m.lab.test());
  static const Context kNoCtx;
  static const std::vector<Result> kNoResults;
  if (m.lab.scope() != sc)
    return {kNoCtx, kNoResults, {}, {}, m.lab.test(), m.notes, m.lab};
  const bool mine = m.held() && m.setup.sc == sc;
  return {m.lab.ctx(), m.lab.results(), mine ? AdoptedOf(m.timeline) : Names(), mine ? StagedOf(m.timeline) : Names(), m.lab.test(), m.notes, m.lab};
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
    s.pick_log = v.lab.search_log(r.file), s.pick_log_of = s.pick;
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
    s.flow_steps = 0, s.pinned = true;
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
    ImGui::TextUnformatted(def(m.exec).cn);
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

// 会话换了 (不论哪个入口开的 / 载入的): 清上下文缓存, 本会话要求折起, 过程页从头钉底 (不覆写 ui.json 的默认作用域)
void follow(Ui &s) {
  const Mine &m = *s.mine;
  const std::string dir = m.held() ? m.log.dir() : std::string();
  if (dir == s.seen_dir)
    return;
  s.seen_dir = dir;
  s.ctx.clear();
  if (dir.empty())
    return;
  s.message.clear();
  s.fold_remark = true, s.flow_steps = 0, s.pinned = true;
}

// 过程 (本会话要求 + 全宽时间线 + 插话栏) / 候选表 / 特征表 / 算子表 (这三页不必开会话, 随作用域) / 工具表 (固定);
// 刚进 Choose 切回过程并滚到底 (问题在那); 回退确认框浮在页上. 大脑是大模型还是外部 agent, 这里都一样
void render_mine(SharedData &data, Ui &s) {
  ImGui::TextDisabled("外部 agent 的 MCP 接口 %s: open 同这里按 New 开新会话, 无参 run 接上已有外部会话", s.remote->listen().c_str());
  Mine &m = *s.mine;
  Scope shown = m.held() ? m.setup.sc : s.opt.sc;
  Scope &sc = m.held() ? shown : s.opt.sc;
  scope_row(data, sc, s.pool_rows, m.lab, scope_locked(s), "1. Scope:");
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
