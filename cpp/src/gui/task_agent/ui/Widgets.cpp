// Widgets — 见头文件
#include "gui/task_agent/ui/Widgets.hpp"
#include "gui/task_agent/Text.hpp"
#include "gui/util/Chart.hpp"
#include "gui/util/Reorder.hpp"

#include "implot.h"

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <functional>
#include <iterator>
#include <optional>
#include <string_view>
#include <unordered_map>

namespace GUI::Agent::ui {

// ---- std::string 输入框 (imgui 缓冲随串扩) ----
namespace {
int resize_cb(ImGuiInputTextCallbackData *d) {
  if (d->EventFlag == ImGuiInputTextFlags_CallbackResize) {
    auto *s = static_cast<std::string *>(d->UserData);
    s->resize(static_cast<size_t>(d->BufTextLen));
    d->Buf = s->data();
  }
  return 0;
}
} // namespace

bool input_text(const char *id, std::string &s, const char *hint) {
  return ImGui::InputTextWithHint(id, hint, s.data(), s.capacity() + 1, ImGuiInputTextFlags_CallbackResize, resize_cb, &s);
}
bool input_multiline(const char *id, std::string &s, ImVec2 size) {
  return ImGui::InputTextMultiline(id, s.data(), s.capacity() + 1, size, ImGuiInputTextFlags_CallbackResize | ImGuiInputTextFlags_WordWrap, resize_cb, &s);
}

void tip(const char *s) {
  if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled))
    ImGui::SetTooltip("%s", s);
}
// 悬浮看长文 (限宽换行)
void tip_long(const std::string &s) {
  if (!ImGui::IsItemHovered())
    return;
  ImGui::BeginTooltip();
  ImGui::PushTextWrapPos(ImGui::GetFontSize() * 50.0f);
  ImGui::TextUnformatted(s.c_str(), s.c_str() + s.size());
  ImGui::PopTextWrapPos();
  ImGui::EndTooltip();
}

void wrapped(const std::string &s) {
  ImGui::PushTextWrapPos(0.0f);
  ImGui::TextUnformatted(s.c_str(), s.c_str() + s.size());
  ImGui::PopTextWrapPos();
}
void wrapped(const ImVec4 &c, const std::string &s) {
  ImGui::PushStyleColor(ImGuiCol_Text, c);
  wrapped(s);
  ImGui::PopStyleColor();
}
void wrapped_dim(const std::string &s) { wrapped(ImGui::GetStyleColorVec4(ImGuiCol_TextDisabled), s); }
void copy_button(const char *id, const std::string &s) {
  ImGui::SameLine();
  if (ImGui::SmallButton(id))
    ImGui::SetClipboardText(s.c_str());
}

// 文本框的排版 (按框的 id 缓存): 按段 ('\n' 切) 记折行后的高, 每帧只画看得见的段 (长文 = 首条 / 结果 / 上下文 / 思考, 整篇每帧量不起).
// 键 (框内宽 / 字号 / 最大高) 不变且文本没变 → 原样; 只在末尾追加 (在收的思考 / 正文) → 从末段起续排; 否则整篇重排. 高过最大高就让出滚动条的宽排
namespace {
struct Layout {
  float w = 0.0f, font = 0.0f, cap = 0.0f; // 键: 框内宽 (不计滚动条) / 字号 / 文本的最大高
  size_t n = 0, hash = 0;                  // 排过的文本: 字节数 / 散列
  bool scroll = false;                     // 高过 cap: 按让出滚动条的宽 lw 排
  float lw = 0.0f;                         // 实排的宽
  std::vector<size_t> at;                  // 各段起点
  std::vector<float> y{0.0f};              // 各段顶, 末项 = 总高
  int used = 0;                            // 末次用到的帧 (久不用的丢)
};
constexpr int kLayoutFrames = 600;
std::unordered_map<ImGuiID, Layout> layouts;

// 从段起点 a 排到文末 (at / y 已截到 a 之前)
void lay(Layout &L, std::string_view s, size_t a) {
  for (;;) {
    const size_t e = std::min(s.find('\n', a), s.size());
    L.at.push_back(a);
    L.y.push_back(L.y.back() + ImGui::CalcTextSize(s.data() + a, s.data() + e, false, L.lw).y);
    if (e == s.size())
      return;
    a = e + 1;
  }
}
void relay(Layout &L, std::string_view s) {
  L.lw = L.scroll ? L.w - ImGui::GetStyle().ScrollbarSize : L.w;
  L.at.clear(), L.y.assign(1, 0.0f);
  lay(L, s, 0);
}
const Layout &layout_of(ImGuiID id, std::string_view s, float w, float cap) {
  const int frame = ImGui::GetFrameCount();
  if (frame % kLayoutFrames == 0)
    std::erase_if(layouts, [&](const auto &kv) { return frame - kv.second.used > kLayoutFrames; });
  Layout &L = layouts[id];
  L.used = frame;
  const float font = ImGui::GetFontSize();
  const std::hash<std::string_view> hash;
  if (!L.at.empty() && L.w == w && L.font == font && L.cap == cap && s.size() >= L.n && hash(s.substr(0, L.n)) == L.hash) {
    if (s.size() == L.n)
      return L;
    const size_t a = L.at.back();
    L.at.pop_back(), L.y.pop_back();
    lay(L, s, a);
  } else {
    L.w = w, L.font = font, L.cap = cap, L.scroll = false;
    relay(L, s);
  }
  if (!L.scroll && L.y.back() > cap) {
    L.scroll = true;
    relay(L, s);
  }
  L.n = s.size(), L.hash = hash(s);
  return L;
}
} // namespace

// 文本框: 全文, 高过 lines 行就在框内滚 (不把时间线顶走); tail = 停在底部时跟着新内容走 (初开即在底部)
void box(const std::string &body, bool dim, float lines, bool tail) {
  const ImGuiStyle &st = ImGui::GetStyle();
  const float pad = 2.0f * st.WindowPadding.y + 2.0f, cap = lines * ImGui::GetTextLineHeightWithSpacing();
  const Layout &L = layout_of(ImGui::GetID("##box"), body, ImGui::GetContentRegionAvail().x - 2.0f * st.WindowPadding.x, cap - pad);
  ImGui::BeginChild("##box", ImVec2(0, std::min(L.y.back() + pad, cap)), ImGuiChildFlags_Borders, L.scroll ? ImGuiWindowFlags_AlwaysVerticalScrollbar : ImGuiWindowFlags_None);
  const bool follow = tail && ImGui::GetScrollY() >= ImGui::GetScrollMaxY() - 2.0f;
  ImDrawList *dl = ImGui::GetWindowDrawList();
  const ImVec2 o = ImGui::GetCursorScreenPos();
  const float top = dl->GetClipRectMin().y - o.y, bot = dl->GetClipRectMax().y - o.y;
  const ImU32 col = ImGui::GetColorU32(dim ? ImGuiCol_TextDisabled : ImGuiCol_Text);
  for (size_t k = static_cast<size_t>(std::upper_bound(L.y.begin() + 1, L.y.end(), top) - L.y.begin()) - 1; k < L.at.size() && L.y[k] < bot; ++k) {
    const size_t e = k + 1 < L.at.size() ? L.at[k + 1] - 1 : body.size();
    dl->AddText(ImGui::GetFont(), ImGui::GetFontSize(), ImVec2(o.x, o.y + L.y[k]), col, body.data() + L.at[k], body.data() + e, L.lw);
  }
  ImGui::Dummy(ImVec2(L.lw, L.y.back()));
  if (follow)
    ImGui::SetScrollHereY(1.0f);
  ImGui::EndChild();
}
// 可折叠的文本块: 标题行 + 复制; 展开 = 文本框 (box). 返回 = 人点了开合
bool fold(const char *id, ImGuiTreeNodeFlags flags, const std::string &label, const std::string &body, bool dim, float lines, bool tail) {
  const bool open = ImGui::TreeNodeEx(id, flags, "%s", label.c_str());
  const bool toggled = ImGui::IsItemToggledOpen();
  copy_button((std::string("复制##") + id).c_str(), body);
  if (!open)
    return toggled;
  box(body, dim, lines, tail);
  ImGui::TreePop();
  return toggled;
}

// ---- 量 ----
std::string dur(double sec) {
  const int t = static_cast<int>(sec + 0.5);
  char b[32];
  if (t < 60)
    std::snprintf(b, sizeof(b), "%ds", t);
  else if (t < 3600)
    std::snprintf(b, sizeof(b), "%dm%02ds", t / 60, t % 60);
  else
    std::snprintf(b, sizeof(b), "%dh%02dm", t / 3600, t / 60 % 60);
  return b;
}
std::string kilo(double n) {
  char b[32];
  if (n < 1e3)
    std::snprintf(b, sizeof(b), "%.0f", n);
  else if (n < 1e6)
    std::snprintf(b, sizeof(b), "%.1fk", n / 1e3);
  else
    std::snprintf(b, sizeof(b), "%.2fM", n / 1e6);
  return b;
}
// 字数
std::string chars(const std::string &s) { return kilo(static_cast<double>(text::chars(s))) + " 字"; }
// 状态色; 进行中的随时间明暗呼吸 (一眼看出哪在动)
ImVec4 color(TaskStatus::Kind k) {
  if (k == TaskStatus::Kind::None)
    return ImGui::GetStyleColorVec4(ImGuiCol_Text);
  ImVec4 c = StatusColor(k);
  if (k == TaskStatus::Kind::Busy)
    c.w *= 0.7f + 0.3f * static_cast<float>(std::cos(ImGui::GetTime() * 4.0));
  return c;
}
// 时刻戳 YYYYMMDD-HHMMSS → YYYY-MM-DD HH:MM
std::string stamp_text(const std::string &st) {
  return st.size() == 15 ? st.substr(0, 4) + "-" + st.substr(4, 2) + "-" + st.substr(6, 2) + " " + st.substr(9, 2) + ":" + st.substr(11, 2) : st;
}

bool begin_table(const char *id, std::initializer_list<const char *> heads, float h) {
  if (!ImGui::BeginTable(id, static_cast<int>(heads.size()), kTableFlags, ImVec2(-1, h)))
    return false;
  ImGui::TableSetupScrollFreeze(0, 1);
  int i = 0;
  for (const char *x : heads)
    ImGui::TableSetupColumn(x, ++i == static_cast<int>(heads.size()) ? ImGuiTableColumnFlags_WidthStretch : ImGuiTableColumnFlags_None);
  ImGui::TableHeadersRow();
  return true;
}

// ---- 作用域行 ----
void scope_row(SharedData &data, Scope &o, std::vector<catalog::PoolRow> &rows, const Desk &desk, bool locked, const char *title) {
  ImGui::TextColored(kColHead, "%s", title);
  ImGui::SameLine();
  ImGui::BeginDisabled(locked);
  ImGui::SetNextItemWidth(160);
  if (ImGui::BeginCombo("##pool", catalog::pool_cn(o.pool).c_str())) {
    if (ImGui::IsWindowAppearing())
      rows = catalog::pool_rows(o.level);
    // 列: 各来源库状态 | 正式库因子数 | 中文名 | 池名 (可能很长, 放末列)
    const float gap = 2.0f * ImGui::GetStyle().ItemSpacing.x;
    float w_n = 0.0f, w_cn = 0.0f;
    for (const catalog::PoolRow &r : rows) {
      w_n = std::max(w_n, ImGui::CalcTextSize(("(" + std::to_string(r.factors) + ")").c_str()).x);
      w_cn = std::max(w_cn, ImGui::CalcTextSize(r.name_cn.c_str()).x);
    }
    const float x_src = ImGui::GetCursorPosX();
    const float x_n = x_src + ImGui::CalcTextSize("bar lob").x + gap, x_cn = x_n + w_n + gap, x_en = x_cn + w_cn + gap;
    Reorder rd("##pool_order");
    for (const catalog::PoolRow &r : rows) {
      const bool selected = r.name == o.pool;
      if (ImGui::Selectable(("##" + r.name).c_str(), selected))
        o.pool = r.name;
      rd.item(r.key, catalog::pool_movable(r.key));
      if (selected)
        ImGui::SetItemDefaultFocus();
      for (const Src src : kVals<Src>) {
        ImGui::SameLine(src == Src::Bar ? x_src : 0.0f);
        const char *name = def(src).id;
        if (r.built[src])
          ImGui::TextColored(StatusColor(TaskStatus::Kind::Ready), "%s", name);
        else
          ImGui::TextDisabled("%s", name);
        tip(r.built[src] ? "库已建" : "库未建");
      }
      ImGui::SameLine(x_n);
      ImGui::TextDisabled("(%zu)", r.factors);
      tip("正式因子库在本层的因子数");
      ImGui::SameLine(x_cn);
      ImGui::TextUnformatted(r.name_cn.c_str());
      ImGui::SameLine(x_en);
      ImGui::TextDisabled("%s", r.name.c_str());
    }
    if (const std::optional<Move> m = rd.end())
      catalog::move_pool(data, m->from, m->to);
    ImGui::EndCombo();
  }
  const std::vector<Choice> lvs = catalog::levels();
  for (const Choice &l : lvs) {
    ImGui::SameLine();
    if (ImGui::RadioButton((l.name + "##lv").c_str(), o.level == l.key))
      o.level = l.key;
  }
  const std::vector<Choice> hs = catalog::holds(data, o.level);
  ImGui::SameLine();
  ImGui::TextDisabled("|  Hold");
  if (hs.empty()) {
    ImGui::SameLine();
    ImGui::TextDisabled("(该层无可搜持有期: 标签未建)");
    ImGui::EndDisabled();
    return;
  }
  catalog::fit(data, o);
  for (const Choice &h : hs) {
    ImGui::SameLine();
    if (ImGui::RadioButton((h.name + "##hd").c_str(), o.hold == static_cast<HoldId>(h.key)))
      o.hold = static_cast<HoldId>(h.key);
  }
  ImGui::TextDisabled("   %s", catalog::hold_desc(o.hold, o.level).c_str());
  // 特征来源: bar 恒开放, lob 可选 (本层无 lob 叶或 lob 库未建则不可改) + 开放的合起来的评估期 (候选台绑到此作用域后才有)
  const Context *c = desk.scope() == o ? &desk.ctx() : nullptr;
  ImGui::TextDisabled("   Src  bar +");
  ImGui::SameLine();
  ImGui::BeginDisabled(c && !c->has_lob);
  ImGui::Checkbox("lob##src", &o.lob);
  ImGui::EndDisabled();
  tip(c && !c->has_lob ? "本层无 lob 叶, 或 lob 特征库未建" : "开放 lob (逐笔与盘口) 叶; bar (价量与基本面) 恒开放");
  ImGui::SameLine();
  ImGui::TextDisabled("|");
  ImGui::SameLine();
  ImGui::TextUnformatted("评估期 近");
  ImGui::SameLine();
  ImGui::SetNextItemWidth(90);
  if (int y = o.years; ImGui::InputInt("年##years", &y))
    o.years = std::max(y, 1);
  tip("评估期 = 标签库与开放的叶所在库落盘日交集的最近 N 年 (其前另装 10% 预热日只给回看, 不计统计): 回测 / 观察 / 已试候选与正式库的可见性都按这一段\n"
      "(别的年数算的统计对 agent 不可见); 会话内不变, 分块流式计算无整期内存封顶");
  ImGui::SameLine();
  ImGui::Text("%s", c ? c->window.c_str() : "…");
  ImGui::EndDisabled();
}

namespace {
bool render_asked(const Asked &view, Asked *edit) {
  if (edit)
    ImGui::TextColored(color(TaskStatus::Kind::Warn), "已按推荐预选, 只改不同意的 (可选 \"其他\" 自己写), 改完按 交答案");
  for (size_t i = 0; i < view.qs.size(); ++i) {
    const Question &q = view.qs[i];
    const Pick &pv = view.picks[i];
    Pick *pe = edit ? &edit->picks[i] : nullptr;
    ImGui::PushID(static_cast<int>(i));
    wrapped(std::to_string(i + 1) + ". " + q.prompt + (q.multiple ? " (可多选)" : ""));
    ImGui::Indent();
    const auto pick = [&](size_t j) {
      if (q.multiple) {
        pe->on[j] = !pe->on[j];
        return;
      }
      pe->on.assign(q.options.size(), false);
      pe->other = false;
      pe->on[j] = true;
    };
    const auto pick_other = [&] {
      if (!q.multiple)
        pe->on.assign(q.options.size(), false);
      pe->other = true;
    };
    for (size_t j = 0; j < q.options.size(); ++j) {
      ImGui::PushID(static_cast<int>(j));
      bool on = pv.on[j];
      ImGui::BeginDisabled(!pe);
      const bool hit = q.multiple ? ImGui::Checkbox("##o", &on) : ImGui::RadioButton("##o", on);
      ImGui::EndDisabled();
      ImGui::SameLine();
      if (q.recommended[j]) {
        ImGui::TextColored(color(TaskStatus::Kind::Ready), "推荐");
        ImGui::SameLine();
      }
      pv.on[j] || pe ? wrapped(q.options[j]) : wrapped_dim(q.options[j]);
      if (pe && (hit || ImGui::IsItemClicked()))
        pick(j);
      ImGui::PopID();
    }
    bool other = pv.other;
    ImGui::BeginDisabled(!pe);
    if (q.multiple ? ImGui::Checkbox("其他##other", &other) : ImGui::RadioButton("其他##other", other)) {
      if (q.multiple)
        pe->other = other;
      else
        pick_other();
    }
    ImGui::SameLine();
    ImGui::SetNextItemWidth(-1);
    std::string txt = pv.text;
    if (input_text("##other_text", pe ? pe->text : txt, "自己的意见") && pe && !pe->text.empty() && !pe->other)
      pick_other();
    ImGui::EndDisabled();
    ImGui::Unindent();
    ImGui::Spacing();
    ImGui::PopID();
  }
  if (edit) {
    ImGui::TextDisabled("附言 (可空)");
    input_multiline("##note", edit->note, ImVec2(-1, ImGui::GetTextLineHeightWithSpacing() * 4));
    return ImGui::Button("交答案", ImVec2(80, 0));
  }
  if (!view.note.empty())
    wrapped_dim("附言: " + view.note);
  return false;
}
} // namespace

bool edit_asked(Asked &a) { return render_asked(a, &a); }
void draw_asked(const Asked &a) { render_asked(a, nullptr); }

// ---- 工具展示 ----
void heading(const std::string &s) {
  ImGui::Spacing();
  ImGui::TextColored(kColHead, "%s", s.c_str());
}

bool grid(const char *id, const std::vector<std::string> &heads) {
  constexpr ImGuiTableFlags kGrid = ImGuiTableFlags_SizingFixedFit | ImGuiTableFlags_BordersInner | ImGuiTableFlags_BordersOuter | ImGuiTableFlags_RowBg | ImGuiTableFlags_NoHostExtendX;
  if (heads.empty() || !ImGui::BeginTable(id, static_cast<int>(heads.size()), kGrid))
    return false;
  for (const std::string &h : heads)
    ImGui::TableSetupColumn(h.c_str());
  ImGui::TableHeadersRow();
  return true;
}
void cell(const std::string &s) {
  ImGui::TableNextColumn();
  ImGui::TextUnformatted(s.c_str());
}
void cell_dim(const std::string &s) {
  ImGui::TableNextColumn();
  ImGui::TextDisabled("%s", s.c_str());
}
void cell_num(double v, const char *fmt, bool tint) {
  ImGui::TableNextColumn();
  if (!std::isfinite(v)) {
    ImGui::TextDisabled("—");
    return;
  }
  char b[48];
  std::snprintf(b, sizeof(b), fmt, v);
  if (tint)
    ImGui::TextColored(GUI::tint(v, 0.0), "%s", b);
  else
    ImGui::TextUnformatted(b);
}
void cell_bp(double r) { cell_num(r * 1e4, "%+.1f", true); }
void cell_pct(double x) { cell_num(x * 100.0, "%.0f%%"); }
void cell_tint(const std::string &s, double tint) {
  ImGui::TableNextColumn();
  if (s == "—")
    ImGui::TextDisabled("—");
  else if (std::isfinite(tint))
    ImGui::TextColored(GUI::tint(tint, 0.0), "%s", s.c_str());
  else
    ImGui::TextUnformatted(s.c_str());
}

void groups_plot(const char *id, const std::vector<double> &grp, float h, const char *unit) {
  if (grp.empty() || !ImPlot::BeginPlot(id, ImVec2(-1, h), ImPlotFlags_NoLegend | ImPlotFlags_NoMenus))
    return;
  static const char *kNames[] = {"G1", "", "", "", "G5", "", "", "", "", "G10", "", "", "", "", "G15", "", "", "", "", "G20"};
  std::vector<double> pos, neg;
  for (const double g : grp) {
    const double v = std::string_view(unit) == "bp" ? g * 1e4 : g;
    pos.push_back(v > 0 ? v : 0.0), neg.push_back(v < 0 ? v : 0.0);
  }
  ImPlot::SetupAxes(nullptr, unit, ImPlotAxisFlags_AutoFit, ImPlotAxisFlags_AutoFit);
  if (grp.size() == std::size(kNames)) {
    double ticks[std::size(kNames)];
    for (size_t i = 0; i < std::size(kNames); ++i)
      ticks[i] = static_cast<double>(i);
    ImPlot::SetupAxisTicks(ImAxis_X1, ticks, static_cast<int>(std::size(kNames)), kNames);
  }
  ImPlot::SetNextFillStyle(kUp);
  ImPlot::PlotBars("##pos", pos.data(), static_cast<int>(pos.size()), 0.7);
  ImPlot::SetNextFillStyle(kDn);
  ImPlot::PlotBars("##neg", neg.data(), static_cast<int>(neg.size()), 0.7);
  ImPlot::EndPlot();
}

void line_plot(const char *id, const std::vector<double> &ys, float h, const char *label, const std::vector<std::string> *ticks) {
  if (ys.size() < 2 || !ImPlot::BeginPlot(id, ImVec2(-1, h), ImPlotFlags_NoMenus))
    return;
  ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_AutoFit, ImPlotAxisFlags_AutoFit);
  if (ticks && ticks->size() == ys.size()) {
    const size_t every = std::max<size_t>(1, ys.size() / 8);
    std::vector<double> at;
    std::vector<const char *> names;
    for (size_t i = 0; i < ys.size(); i += every)
      at.push_back(static_cast<double>(i)), names.push_back((*ticks)[i].c_str());
    ImPlot::SetupAxisTicks(ImAxis_X1, at.data(), static_cast<int>(at.size()), names.data());
  }
  ImPlot::PlotLine(label, ys.data(), static_cast<int>(ys.size()));
  ImPlot::EndPlot();
}

void hist_plot(const char *id, const std::vector<double> &hist, double lo, double hi, float h) {
  if (hist.empty() || !(hi > lo) || !ImPlot::BeginPlot(id, ImVec2(-1, h), ImPlotFlags_NoLegend | ImPlotFlags_NoMenus))
    return;
  const double w = (hi - lo) / static_cast<double>(hist.size());
  std::vector<double> xs(hist.size());
  for (size_t i = 0; i < hist.size(); ++i)
    xs[i] = lo + (static_cast<double>(i) + 0.5) * w;
  ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_AutoFit, ImPlotAxisFlags_AutoFit);
  ImPlot::PlotBars("##hist", xs.data(), hist.data(), static_cast<int>(hist.size()), w * 0.9);
  ImPlot::EndPlot();
}

void bars_plot(const char *id, const std::vector<std::string> &names, const std::vector<double> &vals, float h) {
  if (vals.empty() || !ImPlot::BeginPlot(id, ImVec2(-1, h), ImPlotFlags_NoLegend | ImPlotFlags_NoMenus))
    return;
  std::vector<double> at(vals.size());
  std::vector<const char *> labels;
  for (size_t i = 0; i < vals.size(); ++i)
    at[i] = static_cast<double>(i), labels.push_back(names[i].c_str());
  ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_AutoFit, ImPlotAxisFlags_AutoFit | ImPlotAxisFlags_Invert);
  ImPlot::SetupAxisTicks(ImAxis_Y1, at.data(), static_cast<int>(at.size()), labels.data());
  ImPlot::PlotBars("##bars", vals.data(), static_cast<int>(vals.size()), 0.6, 0, ImPlotBarsFlags_Horizontal);
  ImPlot::EndPlot();
}

} // namespace GUI::Agent::ui
