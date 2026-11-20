#include "gui/task_live/ui/TabBoard.hpp"

#include "gui/util/Chart.hpp"
#include "gui/util/Tip.hpp"
#include "live/Clients.hpp"
#include "shared/SharedData.hpp"

#include "imgui.h"
#include "imgui_internal.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <cmath>
#include <cstdlib>
#include <map>
#include <set>
#include <span>
#include <vector>

namespace GUI::Live {

namespace {

using live::strf;

const ImVec4 kGreen = kDn, kRed = kUp, kHot = kStratCol, kAcc = kHead;
inline ImVec4 side_col(int s) { return s > 0 ? kRed : kGreen; } // 买红卖绿
inline ImVec4 grey() { return ImGui::GetStyleColorVec4(ImGuiCol_TextDisabled); }

constexpr ImGuiTableFlags kFlags = ImGuiTableFlags_SizingFixedFit | ImGuiTableFlags_RowBg | ImGuiTableFlags_BordersInnerV |
                                   ImGuiTableFlags_BordersOuterH | ImGuiTableFlags_NoHostExtendX;
constexpr size_t kOrderLines = 2; // 下单组的状态行: 最近几单

std::string slip_txt(const std::optional<double> &v) { return v ? strf("%.1fp", *v) : "-"; }
std::optional<ImVec4> slip_col(const std::optional<double> &v) {
  if (!v)
    return std::nullopt;
  return *v < kSlipOk ? kGreen : *v < kSlipHi ? kHot
                                              : kRed;
}

std::string wan_pnl(double v) { return wan(v, true); }
std::string size_txt(double n) { return strf("%.0fW", n / 1e4); } // 下单额 (整万)

void text_col(const ImVec4 &c, const std::string &s) { ImGui::TextColored(c, "%s", s.c_str()); }
// 数字格右对齐 (可几段异色连写), 不出省略号; 列宽按文字本身量, 取整到上一像素 (表把列宽截成整像素, 不取整则最宽的格差零点几像素)
// 返回是否悬停在其中一段上
using Seg = std::pair<ImVec4, std::string>;
bool right_segs(std::span<const Seg> segs) {
  float w = 0;
  for (const Seg &s : segs)
    w += ImGui::CalcTextSize(s.second.c_str()).x;
  ImGuiWindow *win = ImGui::GetCurrentWindow();
  const float x0 = win->DC.CursorPos.x, max0 = win->DC.CursorMaxPos.x;
  ImGui::SetCursorPosX(ImGui::GetCursorPosX() + std::max(0.0f, ImGui::GetContentRegionAvail().x - w));
  bool hov = false;
  for (size_t i = 0; i < segs.size(); ++i) {
    if (i)
      ImGui::SameLine(0, 0);
    ImGui::TextColored(segs[i].first, "%s", segs[i].second.c_str());
    hov |= ImGui::IsItemHovered();
  }
  win->DC.CursorMaxPos.x = std::max(max0, x0 + std::ceil(w));
  return hov;
}
void right(const ImVec4 &c, const std::string &s) { right_segs(std::array{Seg{c, s}}); }
void right(const std::string &s) { right(ImGui::GetStyleColorVec4(ImGuiCol_Text), s); }
void text_opt(const std::optional<double> &v, const char *fmt) {
  if (v)
    right(strf(fmt, *v));
}
void pnl_opt(const std::optional<double> &v, const std::string &s) {
  if (v)
    right(tint(*v, 0), s);
}
void masked() { ImGui::TextDisabled("%s", kMask); } // 隐私下遮住的格: 不出悬停提示
void masked_num() { right(grey(), kMask); }
std::string sec_txt(const live::Sec &s, bool priv) { return priv ? kMask : s.code + " " + s.name; }

// 仓位轻重的字色: 占比相对截面最重的一只, 越轻越灰淡
ImVec4 weight_col(const std::optional<double> &w, double top) {
  const float f = w && top > 0 ? static_cast<float>(std::clamp(*w / top, 0.0, 1.0)) : 0.f;
  ImVec4 lo = grey();
  lo.w *= 0.6f;
  return ImLerp(lo, ImGui::GetStyleColorVec4(ImGuiCol_Text), f);
}

// 占总市值的比例条; 悬停看金额与占比
void bar(const std::optional<double> &v, double mv, const ImVec4 &col) {
  if (!v || mv <= 0)
    return;
  const float f = static_cast<float>(std::clamp(*v / mv, 0.0, 1.0));
  ImGui::PushStyleColor(ImGuiCol_PlotHistogram, col);
  ImGui::ProgressBar(f, ImVec2(ImGui::GetFontSize() * 2.5f, ImGui::GetTextLineHeight()), "");
  ImGui::PopStyleColor();
  if (ImGui::IsItemHovered())
    tip("%s · %.0f%%", wan(*v).c_str(), f * 100);
}

// 卖出滑杆的值 (配置里的股数, 至多可卖)
int sell_n(LiveHub &hub, const live::rebal::Hold &h) {
  const std::map<std::string, int> &sells = hub.in().cfg.sells;
  const auto i = sells.find(h.code);
  return std::min(i == sells.end() ? 0 : i->second, h.avail);
}

// 一单的步骤条: 已过绿 / 当前橙 / 失败红 / 未到灰
void order_line(const OrderJob &j, bool priv) {
  ImGui::Text("%s", j.t.c_str());
  ImGui::SameLine();
  text_col(side_col(j.o.side), strf("%s%s", live::side_name(j.o.side), j.label.c_str()));
  ImGui::SameLine();
  ImGui::Text("%s %s ·", sec_txt(j.o.sec, priv).c_str(), j.detail.c_str());
  for (size_t i = 0; i < j.steps.size(); ++i) {
    const ImVec4 c = i < j.at || j.st == JobSt::Ok ? kGreen : i > j.at         ? grey()
                                                          : j.st == JobSt::Err ? kRed
                                                                               : kHot;
    ImGui::SameLine();
    text_col(c, j.steps[i] + (i + 1 < j.steps.size() ? " ›" : " ·"));
  }
  ImGui::SameLine();
  text_col(j.st == JobSt::Ok ? kGreen : j.st == JobSt::Err ? kRed
                                                           : kHot,
           strf("%s %s", job_name(j.st), j.got.msg.c_str()));
}

// 刷新进度两行: 各样读于几时 · 行情时刻 · 接入的客户端; 次行 = 时有时无的: 接入断、在做的各路 (刷新 / 事务, 各一段「路.步…」)、
// 上轮刷新失败, 都没有即「空闲」
void progress(LiveHub &hub) {
  const live::Refresh &g = hub.refresh();
  const auto &st = hub.state();
  std::string at;
  (st ? *st : live::State{}).each([&](const char *n, const std::string &t) { at += strf("%s%s %s", at.empty() ? "" : " · ", n, t.empty() ? "—" : t.c_str()); });
  ImGui::Text("%s · 读于 %s · 行情 %s · %s", hub.client().name, at.c_str(), hub.in().quote_at.empty() ? "—" : hub.in().quote_at.c_str(),
              st && !st->who.empty() ? st->who.c_str() : "未接入");
  bool any = false;
  const auto sep = [&] {
    if (any)
      ImGui::SameLine();
    any = true;
  };
  if (!g.down.empty()) {
    sep();
    text_col(kRed, "✗ 接入: " + g.down);
  }
  if (!g.step.empty()) {
    sep();
    text_col(kHot, "刷新." + g.step + "…");
  }
  if (const auto &t = hub.task(); t && t->st == JobSt::Run) {
    sep();
    text_col(kHot, t->name + (t->step.empty() ? "" : "." + t->step) + "…");
    if (!t->live.empty() && ImGui::IsItemHovered())
      tip("%s", t->live.c_str());
  }
  if (!g.err.empty()) {
    sep();
    text_col(kRed, "✗ 上轮刷新失败");
    if (ImGui::IsItemHovered())
      tip("%s", g.err.c_str());
  }
  if (!any)
    ImGui::TextDisabled("空闲");
}

void data_ctl(LiveHub &hub, BoardState &st) {
  live::Broker &b = hub.broker();
  if (ImGui::SmallButton("刷新"))
    b.request();
  if (ImGui::IsItemHovered())
    tip("客户端依次重查当日成交、持仓、挂单、信用资产与融资负债, 一轮查完整份更新");
  ImGui::SameLine();
  ImGui::TextUnformatted("定时");
  ImGui::SameLine();
  int every = st.every.value_or(static_cast<int>(hub.refresh().every));
  ImGui::SetNextItemWidth(ImGui::GetFontSize() * 3);
  if (ImGui::InputInt("s##every", &every, 0, 0))
    st.every = every;
  if (ImGui::IsItemDeactivatedAfterEdit())
    b.every(std::max(0, every));
  if (!ImGui::IsItemActive())
    st.every.reset();
  if (ImGui::IsItemHovered())
    tip("定时刷新周期 [s], 0 = 不定时; 回车或点别处生效. 本页可见时才刷");
  ImGui::SameLine(0, ImGui::GetFontSize());
  ImGui::BeginDisabled(hub.busy());
  if (ImGui::SmallButton("更新两融名单"))
    hub.pull_terms();
  ImGui::EndDisabled();
  if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled))
    tip("客户端重查融资标的与担保品两张全表 (慢, 期间独占客户端界面), 整份换掉缓存 output/live/terms.csv\n"
        "平时只用缓存; 读到信用账户后缓存缺、满 %d 天、或名单 / 持仓 / 负债里有缓存没有的代码即后台自动拉 (每天至多一次)\n"
        "个股的名称、融资标的、担保品另经本地库核 (Database 同步的两融明细 / 在市)",
        kTermsAge);
  ImGui::SameLine();
  if (const Inputs &in = hub.in(); in.terms)
    ImGui::Text("✓ %s · %zu 只", misc::dash(in.raw->pulled).substr(5).c_str(), in.terms->size());
  else
    text_col(kRed, "未拉过");
  if (ImGui::IsItemHovered())
    tip("两融条款缓存拉于哪天 · 只数");
  ImGui::SameLine(0, ImGui::GetFontSize());
  bool priv = hub.priv();
  if (ImGui::Checkbox("隐私", &priv))
    hub.set_priv(priv);
  if (ImGui::IsItemHovered())
    tip("遮住代码、名称与账户金额 (连同其悬停提示与事务日志), 比例、只数、下单与卖出各列照显");
}

void order_ctl(LiveHub &hub, BoardState &st) {
  int px = static_cast<int>(hub.in().cfg.px);
  ImGui::SetNextItemWidth(ImGui::GetFontSize() * 7);
  if (ImGui::Combo("委托价", &px, live::kPxModeNames, IM_ARRAYSIZE(live::kPxModeNames)))
    hub.edit_cfg([&](live::rebal::Cfg &c) { c.px = static_cast<live::PxMode>(px); });
  if (ImGui::IsItemHovered())
    tip("持仓行下单与调仓共用\n• 扫档: 对手盘累计恰好吃满该额那档\n• 对手价 / 对手价 ±1%%: 对手一档, 后者再朝成交方向偏 1%%\n• 本方价: 本方一档");
  ImGui::SameLine(0, ImGui::GetFontSize());
  ImGui::TextUnformatted("买入");
  ImGui::SameLine();
  if (ImGui::RadioButton("普通", !st.margin))
    st.margin = false;
  ImGui::SameLine();
  if (ImGui::RadioButton("融资", st.margin))
    st.margin = true;
  if (ImGui::IsItemHovered())
    tip("买入方式只管持仓行买入, 卖出一律担保品卖出");
}

// 带上限的百分比滑杆 (宽 w, 整条 0–100%): 滑块拖不过 top, 上限处一道竖线、其外铺暗; 文字 txt 居中.
// 拖动时 v = 手下的值 (截到 top), 返回是否在拖
bool cap_slider(const char *id, float &v, float top, float w, const std::string &txt, const ImVec4 &col) {
  const ImGuiStyle &s = ImGui::GetStyle();
  const ImVec2 a = ImGui::GetCursorScreenPos();
  const float h = ImGui::GetFrameHeight(), g = s.GrabMinSize, pad = 2, span = w - g - 2 * pad;
  ImGui::InvisibleButton(id, ImVec2(w, h));
  const bool act = ImGui::IsItemActive();
  if (act)
    v = std::clamp((ImGui::GetIO().MousePos.x - a.x - pad - g / 2) / span * 100, 0.f, top);
  ImDrawList *dl = ImGui::GetWindowDrawList();
  const ImVec2 b(a.x + w, a.y + h);
  dl->AddRectFilled(a, b, ImGui::GetColorU32(act ? ImGuiCol_FrameBgActive : ImGui::IsItemHovered() ? ImGuiCol_FrameBgHovered
                                                                                                   : ImGuiCol_FrameBg),
                    s.FrameRounding);
  if (top < 100) {
    const float x = a.x + pad + g / 2 + span * top / 100;
    dl->AddRectFilled(ImVec2(x, a.y), b, ImGui::GetColorU32(ImGuiCol_TextDisabled, 0.35f), s.FrameRounding, ImDrawFlags_RoundCornersRight);
    dl->AddLine(ImVec2(x, a.y), ImVec2(x, b.y), ImGui::GetColorU32(kHot));
  }
  const float gx = a.x + pad + span * v / 100;
  dl->AddRectFilled(ImVec2(gx, a.y + pad), ImVec2(gx + g, b.y - pad), ImGui::GetColorU32(act ? ImGuiCol_SliderGrabActive : ImGuiCol_SliderGrab), s.GrabRounding);
  const ImVec2 ts = ImGui::CalcTextSize(txt.c_str());
  dl->AddText(ImVec2(a.x + (w - ts.x) / 2, a.y + (h - ts.y) / 2), ImGui::GetColorU32(col), txt.c_str());
  return act;
}

// 融资加仓的卖出总 bar (占 Σ 非融资持仓额): 拖动 = 各持仓卖其非融资持仓的同一比例 r, 各只至多卖到其非融资可卖部分,
//   r 取到合计恰为 bar 值 (覆盖各只的卖出滑杆, 按能成单的档向下取, 不碰融资部分; 卖满非融资可卖的不管最小单元, 其余照守;
//   剩下不足一个最小单元且已过半的即卖满, 不留碎股); 拖不过上限 = Σ 非融资可卖额 / Σ 非融资持仓额
//   (今日买入的等不可卖, 上限 < 100% 即标色: 本次融资加仓做不满). 显示 = Σ 卖出额 (各只至多计到其非融资可卖部分) / Σ 非融资持仓额
void lever_sell(LiveHub &hub) {
  const auto &p = hub.plan().plan;
  double own = 0, cap = 0, got = 0;
  if (p)
    for (const live::rebal::Hold &h : p->holds) {
      own += h.own * h.px;
      cap += h.plain * h.px;
      got += std::min(sell_n(hub, h), h.plain) * h.px;
    }
  const float top = own > 0 ? static_cast<float>(cap / own * 100) : 0.f;
  float pct = own > 0 ? static_cast<float>(got / own * 100) : 0.f;
  const bool capped = std::lround(top) < 100;
  ImGui::BeginDisabled(cap <= 0 || hub.busy());
  const bool drag = cap_slider("##lever_sell", pct, top, ImGui::GetFontSize() * (capped ? 9 : 6),
                               capped ? strf("%.0f%% · 限 %.0f%%", pct, top) : strf("%.0f%%", pct), capped ? kHot : ImGui::GetStyleColorVec4(ImGuiCol_Text));
  if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled))
    tip("非融资部分卖出比例 (占非融资持仓): 拖动即按同一比例分到各持仓的卖出滑杆 (各只至多卖到其非融资可卖部分, 按能成单的档向下取, 不碰融资部分; 卖满非融资可卖的不管最小单元, 其余照守; 剩下不足一个最小单元且已过半的即卖满, 不留碎股), "
        "可再逐只调 (也可拖进融资部分)\n显示 = Σ 卖出额 (各只至多计到其非融资可卖部分) / Σ 非融资持仓额\n"
        "上限 %.0f%% = Σ 非融资可卖额 / Σ 非融资持仓额 (非融资可卖 = 可卖 − 负债数量)%s",
        top, capped ? "\n上限不足 100%: 部分非融资仓位今日不可卖 (如今日买入), 拖不上去, 本次融资加仓做不满" : "");
  ImGui::EndDisabled();
  ImGui::SameLine(0, ImGui::GetStyle().ItemInnerSpacing.x);
  ImGui::TextUnformatted("卖出");
  if (!drag)
    return;
  const auto sold = [&](double r) {
    double s = 0;
    for (const live::rebal::Hold &h : p->holds)
      s += std::min(r * h.own, static_cast<double>(h.plain)) * h.px;
    return s;
  };
  const double want = pct / 100 * own;
  double lo = 0, hi = 1;
  for (int i = 0; i < 40; ++i)
    (sold((lo + hi) / 2) < want ? lo : hi) = (lo + hi) / 2;
  const double unit = hub.in().cfg.unit;
  std::map<std::string, int> to = hub.in().cfg.sells;
  for (const live::rebal::Hold &h : p->holds) {
    const double x = std::min(hi * h.own, static_cast<double>(h.plain));
    const int full = live::rebal::snap(h, 0, h.plain, true);
    // 快卖完的 (剩下不足一个最小单元, 且已过半) 即卖满, 不留碎股
    const int m = (full - x) * h.px < unit && 2 * x >= full ? full : live::rebal::snap(h, unit, x, true, true);
    if (m > 0)
      to[h.code] = m;
    else
      to.erase(h.code);
  }
  if (to != hub.in().cfg.sells)
    hub.edit_cfg([&](live::rebal::Cfg &c) { c.sells = std::move(to); });
}

void rebal_ctl(LiveHub &hub, BoardState &st) {
  const live::rebal::Cfg &cfg = hub.in().cfg;
  if (ImGui::RadioButton("普通##rebal", !cfg.lever))
    hub.edit_cfg([](live::rebal::Cfg &c) { c.lever = false; });
  ImGui::SameLine();
  if (ImGui::RadioButton("融资加仓", cfg.lever))
    hub.edit_cfg([](live::rebal::Cfg &c) { c.lever = true; });
  if (ImGui::IsItemHovered())
    tip("A 股加杠杆须先卖出自有部分、再融资买入: 卖出非融资部分 (卖出总 bar) → 只融资买名单里的融资标的 (不出现金腿), 回款留作现金日后择机买");
  ImGui::SameLine(0, ImGui::GetFontSize());
  ImGui::SetNextItemWidth(ImGui::GetFontSize() * 10);
  if (ImGui::BeginCombo("名单", cfg.list.empty() ? "(无)" : cfg.list.c_str())) {
    for (const std::string &n : hub.lists())
      if (ImGui::Selectable(n.c_str(), n == cfg.list))
        hub.set_list(n);
    ImGui::EndCombo();
  }
  if (ImGui::IsItemHovered())
    tip("output/config/live/lists/<名>.csv, 名为字母数字下划线; 每行首列一个 6 位代码 (可有 code 表头), 行序即排名; 新在前\n选中即重读");
  ImGui::SameLine();
  ImGui::TextDisabled("%zu 只", hub.in().codes.size());
  if (!hub.in().list_err.empty()) {
    ImGui::SameLine();
    text_col(kRed, "✗ " + hub.in().list_err);
  }
  ImGui::SameLine(0, ImGui::GetFontSize());
  double unit = st.unit.value_or(cfg.unit);
  ImGui::SetNextItemWidth(ImGui::GetFontSize() * 5);
  if (ImGui::InputDouble("最小单元", &unit, 0, 0, "%.0f"))
    st.unit = unit;
  if (ImGui::IsItemDeactivatedAfterEdit())
    hub.edit_cfg([&](live::rebal::Cfg &c) { c.unit = std::max(0.0, unit); });
  if (!ImGui::IsItemActive())
    st.unit.reset();
  if (ImGui::IsItemHovered())
    tip("元, 不足此额的单不做, 新开仓至少买这么多; 回车或点别处生效");
  ImGui::SameLine(0, ImGui::GetFontSize());
  if (cfg.lever) {
    lever_sell(hub);
    ImGui::SameLine();
  }
  int pct = static_cast<int>(std::lround(cfg.pct));
  ImGui::SetNextItemWidth(ImGui::GetFontSize() * (cfg.lever ? 6 : 7));
  if (ImGui::SliderInt(cfg.lever ? "融资买入" : "行使", &pct, 0, 100, "%d%%"))
    hub.edit_cfg([&](live::rebal::Cfg &c) { c.pct = pct; });
  if (ImGui::IsItemHovered())
    tip("%s", cfg.lever ? "本次用掉卖后可用保证金、可融余量的比例 (融资购买力, 不用现金); 名单里的融资标的尽量等权补齐"
                        : "本次用掉现金、可用保证金、可融余量的比例");
  ImGui::SameLine(0, ImGui::GetFontSize());
  const PlanView &v = hub.plan();
  ImGui::BeginDisabled(!v.plan || !v.wait.empty() || v.plan->orders.empty() || hub.busy());
  if (ImGui::SmallButton("执行调仓"))
    st.run = true;
  ImGui::EndDisabled();
}

// 表头: '|' 处可折行, wrap 即折成两行, 否则去掉
std::string head(const char *h, bool wrap) {
  std::string s = h;
  if (const size_t i = s.find('|'); i != std::string::npos)
    s.replace(i, 1, wrap ? "\n" : "");
  return s;
}
float head_w(const char *h, bool wrap) { return ImGui::CalcTextSize(head(h, wrap).c_str(), nullptr, true).x; }

// 自适应表: 放不下先收格内边距 (至多收到 1px), 仍放不下则表头折行; 按上帧量度 (Fit) 定, 表宽随之贴合. 配 end_fit
//   量的是各列内容宽, 不是表宽: 放不下时表已被挤窄 (格里出 ...), 量表宽收不动
bool begin_fit(const char *id, std::span<const char *const> heads, ImGuiTableFlags flags, Fit &f) {
  const int n = static_cast<int>(heads.size());
  const ImVec2 pad0 = ImGui::GetStyle().CellPadding;
  float pad = pad0.x;
  if (f.body.size() == heads.size()) {
    const float avail = ImGui::GetContentRegionAvail().x;
    const auto need = [&](bool wrap) {
      float w = n + 1.0f; // 竖线与外沿
      for (int c = 0; c < n; ++c)
        w += std::max(f.body[c], f.extra[c] + head_w(heads[c], wrap));
      return w;
    };
    f.wrap = avail - need(false) < 2 * n;
    pad = std::clamp((avail - need(f.wrap)) / (2 * n), 1.0f, pad0.x);
  }
  ImGui::PushStyleVar(ImGuiStyleVar_CellPadding, ImVec2(pad, pad0.y));
  const bool began = ImGui::BeginTable(id, n, flags);
  ImGui::PopStyleVar();
  return began;
}
void end_fit(std::span<const char *const> heads, Fit &f) {
  ImGuiTable *t = ImGui::GetCurrentTable();
  ImGui::EndTable();
  f.body.resize(heads.size());
  f.extra.resize(heads.size());
  for (size_t c = 0; c < heads.size(); ++c) {
    const ImGuiTableColumn &col = t->Columns[static_cast<int>(c)];
    f.body[c] = std::max(col.ContentMaxXFrozen, col.ContentMaxXUnfrozen) - col.WorkMinX;
    f.extra[c] = std::max(0.0f, col.ContentMaxXHeadersIdeal - col.WorkMinX - head_w(heads[c], f.wrap));
  }
}
// 可排序表的表头格: 排序箭头的位只给正在排的列 (ImGui 给每个可排列都留, 且按留了箭头截列名)
//   TableHeader 只铺底、管点击与画箭头, 列名自画, 截断只让正在排的列的箭头
void sort_head(const char *label) {
  ImGuiTable *t = ImGui::GetCurrentTable();
  ImGuiTableColumn &col = t->Columns[t->CurrentColumn];
  const ImVec2 at = ImGui::GetCursorScreenPos();
  ImGui::TableHeader((std::string("##") + label).c_str());
  const float arrow = col.SortOrder == -1 ? 0 : col.ContentMaxXHeadersIdeal - at.x; // 箭头与序号
  const ImVec2 sz = ImGui::CalcTextSize(label);
  col.ContentMaxXHeadersIdeal = at.x + std::ceil(sz.x) + arrow;
  const float r = std::max(ImGui::TableGetCellBgRect(t, t->CurrentColumn).Max.x - arrow, at.x);
  ImGui::RenderTextEllipsis(ImGui::GetWindowDrawList(), at, ImVec2(r, at.y + sz.y), r, label, nullptr, &sz);
}
// 表头一行 (各列名按折行与否)
void fit_heads(std::span<const char *const> heads, const Fit &f) {
  for (const char *h : heads)
    ImGui::TableSetupColumn(head(h, f.wrap).c_str());
  ImGui::TableHeadersRow();
}

// 试算: 状态一行 (出错 / 试算时刻 · 等什么) + 购买力与行使一表; 未出即空格, 在等即上一份
void plan_sheet(LiveHub &hub, BoardState &st) {
  const PlanView &v = hub.plan();
  if (!v.err.empty())
    text_col(kRed, "✗ " + v.err);
  else if (!v.plan)
    ImGui::TextDisabled("%s", v.wait.empty() ? "(等试算)" : v.wait.c_str());
  else
    ImGui::TextDisabled("试算于 %s%s%s", v.plan->time.c_str(), v.wait.empty() ? "" : " · ", v.wait.c_str());
  constexpr int kN = 8;
  constexpr const char *kHeads[kN] = {"购买力", "本次|行使", "其中|融资", "成本", "入选", "每只|目标", "卖后|现金", "卖后|保证金"};
  if (!begin_fit("##plan", kHeads, kFlags, st.plan_fit))
    return;
  fit_heads(kHeads, st.plan_fit);
  ImGui::TableNextRow();
  if (v.plan) {
    const live::rebal::Plan &p = *v.plan;
    const auto w = [&](double x) { return hub.priv() ? std::string(kMask) : wan(x); };
    const std::array<std::string, kN> cells{w(p.power), w(p.use), w(p.financed), strf("%.0f", p.cost), strf("%d/%d", p.picked, p.buyable), w(p.V), w(p.C), w(p.X)};
    for (int c = 0; c < kN; ++c) {
      ImGui::TableSetColumnIndex(c);
      right(cells[c]);
      if (c == 3 && ImGui::IsItemHovered())
        tip("滑点 + 手续费 [元]");
      if (c == 4 && ImGui::IsItemHovered())
        tip("入选只数 / 名单里可买只数%s", hub.in().cfg.lever ? " (融资加仓只计融资标的)" : "");
    }
  } else
    for (int c = 0; c < kN; ++c) {
      ImGui::TableSetColumnIndex(c);
      right(grey(), "—");
    }
  end_fit(kHeads, st.plan_fit);
}

// 事务一行: 时刻 名称 · 状态 · 实时进度 结果
void task_line(LiveHub &hub) {
  const auto &t = hub.task();
  if (!t)
    return ImGui::TextDisabled("无事务");
  text_col(t->st == JobSt::Err ? kRed : t->st == JobSt::Run ? kHot
                                                            : ImGui::GetStyleColorVec4(ImGuiCol_Text),
           strf("%s %s · %s%s%s %s", t->t.c_str(), t->name.c_str(), job_name(t->st), t->live.empty() ? "" : " · ", t->live.c_str(), t->msg.c_str()));
}

// 账户对照: 现 / 调仓后 / 变动%, 调仓后与现不同的格标色; 变动按该列好坏着色 (好红坏绿)
void sheet(const live::rebal::Sheet *n, const live::rebal::Sheet *e, bool priv, Fit &fit) {
  constexpr int kN = 9;
  constexpr const char *kHeads[kN + 1] = {"", "总资产", "持仓|市值", "净资产", "负债", "现金", "可用|保证金", "可融|余量", "杠杆", "维持|担保"};
  constexpr int kGood[kN] = {1, 1, 1, -1, 1, 1, 1, -1, 1}; // 涨为好 1 / 跌为好 -1
  using Vals = std::array<std::optional<double>, kN>;
  const auto vals = [](const live::rebal::Sheet &s) { return Vals{s.assets, s.mv, s.net, s.debt, s.cash, s.X, s.L, s.lev, s.maint}; };
  const auto txt = [](int c, double v) { return c == 7 ? strf("%.2f", v) : c == 8 ? strf("%.0f%%", v * 100)
                                                                                  : wan(v); };
  if (!begin_fit("##sheet", kHeads, kFlags, fit))
    return;
  fit_heads(kHeads, fit);
  const Vals now = n ? vals(*n) : Vals{}, exp = e ? vals(*e) : Vals{};
  for (int r = 0; r < 2; ++r) {
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    ImGui::TextUnformatted(r ? "调仓后" : "现");
    for (int c = 0; c < kN; ++c) {
      ImGui::TableSetColumnIndex(c + 1);
      const std::optional<double> &v = (r ? exp : now)[c];
      if (!v)
        right(grey(), "—");
      else if (priv)
        masked_num();
      else if (r && now[c] && txt(c, *v) != txt(c, *now[c]))
        right(kHot, txt(c, *v));
      else
        right(txt(c, *v));
    }
  }
  ImGui::TableNextRow();
  ImGui::TableSetColumnIndex(0);
  ImGui::TextUnformatted("变动");
  for (int c = 0; c < kN; ++c) {
    ImGui::TableSetColumnIndex(c + 1);
    if (!now[c] || !exp[c] || *now[c] == 0) {
      right(grey(), "—");
      continue;
    }
    const double d = (*exp[c] - *now[c]) / std::abs(*now[c]) * 100;
    const std::string s = strf("%+.1f%%", d);
    if (s == "+0.0%" || s == "-0.0%")
      right(grey(), "0");
    else
      right(tint(d * kGood[c], 0), s);
  }
  end_fit(kHeads, fit);
}

// 控件分组: 一行一组, 左格组名, 右格控件行与定高的状态行
void groups(LiveHub &hub, BoardState &st) {
  if (!ImGui::BeginTable("##groups", 2, ImGuiTableFlags_SizingFixedFit | ImGuiTableFlags_BordersInnerH | ImGuiTableFlags_BordersOuterH))
    return;
  ImGui::TableSetupColumn("", ImGuiTableColumnFlags_WidthFixed);
  ImGui::TableSetupColumn("", ImGuiTableColumnFlags_WidthStretch);
  const auto row = [](const char *name) {
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    ImGui::TextDisabled("%s", name);
    ImGui::TableSetColumnIndex(1);
  };
  row("数据");
  data_ctl(hub, st);
  progress(hub);
  row("下单");
  order_ctl(hub, st);
  for (size_t i = 0; i < kOrderLines; ++i)
    if (i < hub.orders().size())
      order_line(hub.orders()[i], hub.priv());
    else if (i == 0)
      ImGui::TextDisabled("还没下过单");
    else
      ImGui::TextUnformatted("");
  row("调仓");
  rebal_ctl(hub, st);
  plan_sheet(hub, st);
  task_line(hub);
  row("账户");
  const PlanView &v = hub.plan();
  sheet(v.now ? &*v.now : nullptr, v.plan ? &v.plan->exp : nullptr, hub.priv(), st.acct_fit);
  ImGui::EndTable();
}

// 持仓表上方的提示行 (定高): 帧没出的原因、无持仓、持仓表里没有的代码的挂单
void info_line(LiveHub &hub, const std::set<std::string> &held) {
  const BoardView &v = hub.board();
  if (!v.err.empty())
    return text_col(kRed, "✗ " + v.err);
  if (!v.wait.empty())
    return ImGui::TextDisabled("%s%s", v.wait.c_str(), v.frame ? " · 表为上一份" : "");
  if (v.frame->rows.empty())
    return ImGui::TextDisabled("无持仓");
  const auto &orders = v.st->orders;
  std::string s;
  int n = 0;
  if (orders)
    for (const live::Pending &o : orders->list)
      if (!held.count(o.code)) {
        ++n;
        s += strf("%s%s %s %.0f股 @%.3f %s", s.empty() ? "" : "、", live::side_name(o.side), sec_txt(o, hub.priv()).c_str(), o.qty - o.filled, o.price,
                  o.status.c_str());
      }
  if (!n)
    return ImGui::TextUnformatted("");
  ImGui::Text("表外挂单 %d 笔: %s", n, s.c_str());
  if (ImGui::IsItemHovered())
    tip("%s", s.c_str());
}

// 状态列: 今日成交 ●、挂单 ○, 按方向着色
struct Flows {
  std::map<std::string, std::vector<const live::Fill *>> fills;
  std::map<std::string, std::vector<const live::Pending *>> orders;
};
Flows flows_of(const live::State &s) {
  Flows f;
  if (s.fills)
    for (const live::Fill &x : s.fills->rows)
      f.fills[x.code].push_back(&x);
  if (s.orders)
    for (const live::Pending &x : s.orders->list)
      f.orders[x.code].push_back(&x);
  return f;
}
void flow_cell(const Flows &f, const std::string &code, bool priv) {
  const auto fi = f.fills.find(code);
  const auto oi = f.orders.find(code);
  if (priv) {
    if (fi != f.fills.end() || oi != f.orders.end())
      masked();
    return;
  }
  std::string tips;
  bool any = false;
  const auto mark = [&](const char *m, auto &xs) {
    for (const int s : live::kSides)
      if (std::any_of(xs.begin(), xs.end(), [&](auto *x) { return x->side == s; })) {
        if (any)
          ImGui::SameLine(0, 0);
        text_col(side_col(s), m);
        any = true;
      }
  };
  if (fi != f.fills.end()) {
    mark("●", fi->second);
    tips += strf("成交 %zu 笔\n", fi->second.size());
    for (const live::Fill *x : fi->second)
      tips += strf("  %s %s %.0f股 @%.3f\n", live::side_name(x->side), x->time.c_str(), x->qty, x->price);
  }
  if (oi != f.orders.end()) {
    mark("○", oi->second);
    tips += strf("挂单 %zu 笔\n", oi->second.size());
    for (const live::Pending *x : oi->second)
      tips += strf("  %s %s %.0f%s股 @%.3f %s\n", live::side_name(x->side), x->time.c_str(), x->qty - x->filled,
                   x->filled ? strf("/%.0f", x->qty).c_str() : "", x->price, x->status.c_str());
  }
  if (any && ImGui::IsItemHovered())
    tip("%s", tips.c_str());
}

// 下单按钮: 一列滑点一个, 填充 = 该额的滑点色; 未持有的 (名单股) 卖不了; 点了直接下 (解锁与否由 can 管)
void order_cell(LiveHub &hub, BoardState &st, const live::board::Row &r, bool can) {
  if (r.closed)
    return ImGui::TextDisabled("今日清仓");
  assert(r.quote);
  // 悬停: 价格 (micro price) / 预期成交价 / 滑点 / 成本; 5 档吃不满的标 > (成交价买标 >、卖标 <, 实际更差)
  const auto btn_tip = [&](const live::Slip &x, int s) {
    if (!live::ref(*r.quote))
      return std::string("无行情");
    std::string t = strf("价格 %.3f (micro price)", r.quote->price);
    if (r.auction)
      return t + "\n竞价中";
    if (!x.px)
      return t + "\n对手盘空";
    const char *worse = x.full ? "" : ">";
    return t + strf("\n预期成交价 %s%.3f\n滑点 %s%.1fbp\n成本 %s%.1f 元", x.full ? "" : s > 0 ? ">"
                                                                                              : "<",
                    *x.px, worse, *x.pct, worse, *x.cost);
  };
  ImGui::BeginDisabled(!can);
  ImGui::PushStyleVar(ImGuiStyleVar_FramePadding, ImVec2(ImGui::GetFontSize() * 0.12f, ImGui::GetStyle().FramePadding.y));
  for (size_t i = 0; i < live::kSlipCols.size(); ++i) {
    const live::Col c = live::kSlipCols[i];
    if (i)
      ImGui::SameLine(0, i == live::kSlipCols.size() / 2 ? ImGui::GetFontSize() * 0.3f : 1);
    const live::Slip x = r.slips ? (*r.slips)[i] : live::Slip{};
    const std::optional<ImVec4> fill = slip_col(x.pct);
    ImGui::PushStyleColor(ImGuiCol_Text, side_col(c.s));
    if (fill)
      ImGui::PushStyleColor(ImGuiCol_Button, ImVec4(fill->x, fill->y, fill->z, 0.45f));
    ImGui::BeginDisabled(c.s < 0 && !r.market_value);
    const bool hit = ImGui::SmallButton(strf("%s##o%zu", size_txt(c.n).c_str(), i).c_str());
    ImGui::EndDisabled();
    ImGui::PopStyleColor(fill ? 2 : 1);
    if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled))
      tip("%s", btn_tip(x, c.s).c_str());
    if (hit)
      hub.submit(r.code, c.s, c.n, c.s > 0 && st.margin ? live::Credit::Margin : live::Credit::Plain);
  }
  ImGui::PopStyleVar();
  ImGui::EndDisabled();
}

void confirm_run(LiveHub &hub, BoardState &st) {
  if (st.run) {
    ImGui::OpenPopup("确认调仓");
    st.run = false;
  }
  if (!ImGui::BeginPopupModal("确认调仓", nullptr, ImGuiWindowFlags_AlwaysAutoResize))
    return;
  const PlanView &v = hub.plan();
  const live::rebal::Plan *p = v.plan && v.wait.empty() ? &*v.plan : nullptr;
  if (!p) {
    ImGui::TextDisabled("试算已失效");
  } else {
    int sell = 0, margin = 0, plain = 0;
    for (const live::rebal::Order &o : p->orders)
      (o.side < 0 ? sell : o.credit == live::Credit::Margin ? margin
                                                            : plain)++;
    ImGui::Text("按当前试算执行调仓: 卖 %d 单、融资买 %d 单、现金买 %d 单, 行使 %s, 成本约 %.0f 元?", sell, margin, plain,
                hub.priv() ? kMask : wan(p->use).c_str(), p->cost);
  }
  ImGui::BeginDisabled(!p || hub.busy());
  if (ImGui::Button("执行")) {
    hub.run();
    ImGui::CloseCurrentPopup();
  }
  ImGui::EndDisabled();
  ImGui::SameLine();
  if (ImGui::Button("取消"))
    ImGui::CloseCurrentPopup();
  ImGui::EndPopup();
}

enum Column {
  C_CODE,
  C_POS_PCT,
  C_CAP,
  C_NAME,
  C_FLOW,
  C_FIN,
  C_ORDER,
  C_DAY_PCT,
  C_DAY,
  C_PNL_PCT,
  C_PNL,
  C_MV,
  C_MARGIN,
  C_SELLABLE,
  C_SELL,
  C_AMT,
  C_COST,
  C_COLLAT,
  C_COUNT
};
constexpr const char *kHeads[C_COUNT] = {"代码", "仓位%", "总市值", "名称", "状态", "融", "买 / 卖", "当日%", "当日", "盈亏%",
                                         "盈亏", "市值", "融资", "可卖", "卖出", "金额", "成本", "折算"};

using Caps = std::unordered_map<std::string, double>; // 代码 → 总市值 [亿元]

// 代码 → 全局 id; 资产库没就绪或本地库没有 → 无 (id 表随 AssetInfo 的代码数变了即重建)
std::optional<uint32_t> id_of(const SharedData &data, BoardState &st, const std::string &code) {
  if (!data.task_state.database.asset_info_ready)
    return std::nullopt;
  const std::vector<std::string> &codes = data.asset_info.codes;
  if (st.ids.size() != codes.size()) {
    st.ids.clear();
    for (uint32_t i = 0; i < codes.size(); ++i)
      st.ids.emplace(codes[i], i);
  }
  const auto i = st.ids.find(code + live::kMktSuffix[static_cast<int>(live::market_of(code))]);
  return i == st.ids.end() ? std::nullopt : std::optional<uint32_t>(i->second);
}

// 本地库最新收盘的总市值; 没有即不记
void add_cap(Caps &caps, const SharedData &data, BoardState &st, const std::string &code) {
  const std::optional<uint32_t> id = id_of(data, st, code);
  if (!id || *id >= data.asset_info.stocks.size())
    return;
  if (const StockInfo *s = data.asset_info.find(*id); s && std::isfinite(s->mcap))
    caps.emplace(code, s->mcap);
}

ImGuiTableColumnFlags col_flags(int c) {
  switch (c) {
  case C_CODE:
  case C_NAME:
    return 0;
  case C_FLOW:
  case C_FIN:
  case C_ORDER:
  case C_SELL:
  case C_AMT:
  case C_COST:
    return ImGuiTableColumnFlags_NoSort;
  default:
    return ImGuiTableColumnFlags_PreferSortDescending;
  }
}

// 数值列的排序键; 融资 / 可卖按占总市值
std::optional<double> sort_key(const live::board::Row &r, int c, const Caps &caps) {
  const auto share = [&](const std::optional<double> &v) { return v && r.market_value.value_or(0) > 0 ? std::optional<double>(*v / *r.market_value) : std::nullopt; };
  switch (c) {
  case C_POS_PCT:
    return r.position_pct;
  case C_CAP: {
    const auto i = caps.find(r.code);
    return i == caps.end() ? std::nullopt : std::optional<double>(i->second);
  }
  case C_DAY_PCT:
    return r.day_pnl_pct;
  case C_DAY:
    return r.day_pnl;
  case C_PNL_PCT:
    return r.pnl_pct;
  case C_PNL:
    return r.pnl;
  case C_MV:
    return r.market_value;
  case C_MARGIN:
    return share(r.margin_value);
  case C_SELLABLE:
    return share(r.sellable_value);
  case C_COLLAT:
    return r.collateral;
  default:
    assert(false);
    return std::nullopt;
  }
}

// 表头点选排序 (三态, 不排 = 帧序); 今日清仓恒在后, 空值恒在有值后.
// imgui.ini 存的排序列是列下标, 列表改过后可能落在不可排的列上: 当不排
std::vector<const live::board::Row *> sorted(const live::board::Frame &f, const Caps &caps) {
  std::vector<const live::board::Row *> rows;
  for (const live::board::Row &r : f.rows)
    rows.push_back(&r);
  const ImGuiTableSortSpecs *ss = ImGui::TableGetSortSpecs();
  if (!ss || !ss->SpecsCount || col_flags(ss->Specs[0].ColumnIndex) & ImGuiTableColumnFlags_NoSort)
    return rows;
  const int c = ss->Specs[0].ColumnIndex;
  const bool desc = ss->Specs[0].SortDirection == ImGuiSortDirection_Descending;
  std::stable_sort(rows.begin(), rows.end(), [&](const live::board::Row *a, const live::board::Row *b) {
    if (a->closed != b->closed)
      return b->closed;
    if (c == C_CODE || c == C_NAME) {
      const std::string &x = c == C_CODE ? a->code : a->name, &y = c == C_CODE ? b->code : b->name;
      return desc ? y < x : x < y;
    }
    const std::optional<double> x = sort_key(*a, c, caps), y = sort_key(*b, c, caps);
    if (!x || !y)
      return x && !y;
    return desc ? *y < *x : *x < *y;
  });
  return rows;
}

void head_tip(int c, const live::board::Frame &f) {
  switch (c) {
  case C_NAME:
    return tip("名称: 青 = 在当前调仓名单里, 灰 = 今日清仓 / 名单里这次不入的\n名单里未持有的列在表末 (不随排序), 能出的列照持仓出 (可买的有行情: 买入按钮、当日涨跌); "
               "名单股悬停看排名、保证金比例与不补 / 不可买的原因");
  case C_CAP:
    return tip("总市值 [亿元]: 本地库最新收盘 (收盘价 × 总股本); 本地库没有即空");
  case C_FLOW:
    return tip("● 今日成交 · ○ 挂单, 买红卖绿; 悬停看逐笔");
  case C_FIN:
    return tip("R = 融资标的且融资状态正常 (两融条款缓存 ∧ 本地两融明细, 试算未出即空)");
  case C_ORDER: {
    std::string s;
    for (size_t i = 0; i < live::kSlipCols.size(); ++i)
      s += strf("%s%s%s %s", s.empty() ? "" : " · ", live::side_name(live::kSlipCols[i].s), size_txt(live::kSlipCols[i].n).c_str(), slip_txt(f.slips[i]).c_str());
    return tip("滑点 p = 万分之一, 正 = 成本; 按钮填充 绿 < %.0fp ≤ 黄 < %.0fp ≤ 红, 解锁后点了直接下单\n持仓按占比加权 %s", kSlipOk, kSlipHi, s.c_str());
  }
  case C_DAY:
  case C_DAY_PCT:
    return tip("当日盈亏: 按成交价自算 (毛, 不扣费); 与持仓的今买 / 今卖对不上即空; 名单股的当日%% = 现价 / 昨收 − 1\n"
               "合计行的当日%% = 当日盈亏 / 昨净资产 (现金 + 总市值 − 负债 − 当日盈亏, 信用账户未读即无)");
  case C_PNL:
  case C_PNL_PCT:
    return tip("持仓盈亏: 券商口径, 随现价平移");
  case C_POS_PCT:
  case C_MV:
    return tip("仓位占比 · 总市值; 字色按仓位相对最重的一只, 越轻越灰淡");
  case C_MARGIN:
    return tip("融资部分市值, 占总市值; 悬停看金额");
  case C_SELLABLE:
    return tip("可卖部分市值, 占总市值; 悬停看金额");
  case C_SELL:
    return tip("调仓先卖的拖卖出\n只停在能成单的档: 够起买股数、够最小单元, 整手或连同零股, 末档全卖 (融资加仓另有卖满非融资可卖一档, 这两档不管最小单元)\n"
               "融资加仓: 卖出总 bar 按同一比例分到各只的非融资可卖部分, 可再逐只调 (也可拖进融资部分), 总 bar 随之变");
  case C_AMT:
    return tip("调仓金额: 现金买, 融资买 / 卖 [万], 买红卖绿, 0 占位; 卖 = 拖到的股数 × 现价\n"
               "名单先融资标的、后其余, 各按排名入选, 等权补齐; 悬停看原因与成本拆分");
  case C_COST:
    return tip("该行各单 (卖与两腿买) 的滑点 + 手续费 [元]; 有单不成或不可买标 ✗ (悬停看原因), 拖了卖出而试算还没回算即 …");
  case C_COLLAT:
    return tip("担保折算率");
  default:
    return;
  }
}

// 卖出滑杆 (配置里的股数, 拖动即 snap 到能成单的); 悬停看股数
void sell_slider(LiveHub &hub, const live::rebal::Hold &h) {
  const live::rebal::Cfg &cfg = hub.in().cfg;
  int n = sell_n(hub, h);
  ImGui::BeginDisabled(live::rebal::snap(h, cfg.unit, h.avail, false, cfg.lever) == 0 || hub.busy());
  ImGui::SetNextItemWidth(ImGui::GetFontSize() * 4);
  if (ImGui::SliderInt("##sell", &n, 0, h.avail, ""))
    hub.edit_cfg([&, m = live::rebal::snap(h, cfg.unit, n, false, cfg.lever)](live::rebal::Cfg &c) {
      if (m > 0)
        c.sells[h.code] = m;
      else
        c.sells.erase(h.code);
    });
  ImGui::EndDisabled();
  if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled))
    tip("卖 %d 股 / 可卖 %d 股 · 非融资持仓 %d 股 · 非融资可卖 %d 股 (= 可卖 − 负债数量)", n, h.avail, h.own, h.plain);
}

// 一行的调仓: 金额 (现金买 / 融资买 / 卖)、成本、悬停说明; 持仓行带卖 (h, 拖到 n 股), 名单行带两腿买 (t)
struct Amt {
  double buy = 0, fin = 0, sell = 0;
};
struct RowPlan {
  Amt amt;
  double slip = 0, fee = 0;
  bool pending = false, failed = false; // 拖了卖出而试算还没回算 / 有单不成或不可买
  std::string tip;
};
RowPlan row_plan(const live::rebal::Hold *h, int n, const live::rebal::Target *t, bool priv) {
  RowPlan p;
  const auto line = [&](const std::string &s) { p.tip += (p.tip.empty() ? "" : "\n") + s; };
  const auto take = [&](const live::rebal::Order &o) { p.slip += o.slip, p.fee += o.fee; };
  std::vector<std::string> fails;
  if (t) {
    line(strf("名单第 %d%s", t->rank, t->ratio ? strf(" · 保证金比例 %.2f", *t->ratio).c_str() : ""));
    if (!t->why.empty())
      fails.push_back("不可买: " + t->why);
    else if (!t->skip.empty())
      line("不补: " + t->skip);
    for (const live::rebal::Leg &l : t->legs) {
      const bool fin = l.credit == live::Credit::Margin;
      if (l.order) {
        (fin ? p.amt.fin : p.amt.buy) += l.order->amount;
        take(*l.order);
      } else if (!l.skip.empty())
        fails.push_back(strf("%s %s 不成单: %s", fin ? "融买" : "买", priv ? kMask : wan(l.want).c_str(), l.skip.c_str()));
    }
  }
  if (h && n) {
    p.amt.sell = n * h->px;
    if (n != std::min(h->sell, h->avail))
      p.pending = true;
    else if (h->order)
      take(*h->order);
    else
      fails.push_back("卖不成单: " + h->skip);
  }
  if (p.slip || p.fee)
    line(strf("成本: 滑点 %.0f 元 · 费 %.0f 元", p.slip, p.fee));
  for (const std::string &s : fails)
    line(s);
  p.failed = !fails.empty();
  return p;
}
// 金额格: 现金买,融资买/卖 W, 买红卖绿, 0 占位不着色; 全 0 不出. 返回是否悬停
bool amt_cell(const Amt &a, bool priv) {
  if (!a.buy && !a.fin && !a.sell)
    return false;
  if (priv) {
    masked_num();
    return ImGui::IsItemHovered();
  }
  const auto num = [](double v, int side) { return v ? Seg{side_col(side), strf("%.1f", v / 1e4)} : Seg{grey(), "0"}; };
  return right_segs(std::array{num(a.buy, live::kBuy), Seg{grey(), ","}, num(a.fin, live::kBuy), Seg{grey(), "/"}, num(a.sell, live::kSell), Seg{grey(), "W"}});
}
// 金额与成本两格, 悬停出说明; 计入合计
void plan_cells(const RowPlan &p, bool priv, Amt &amt, double &cost) {
  ImGui::TableSetColumnIndex(C_AMT);
  bool hov = amt_cell(p.amt, priv);
  ImGui::TableSetColumnIndex(C_COST);
  if (p.pending)
    right(grey(), "…");
  else if (p.slip || p.fee)
    right(strf("%.0f", p.slip + p.fee));
  else if (p.failed)
    right(grey(), "✗");
  hov |= (p.pending || p.slip || p.fee || p.failed) && ImGui::IsItemHovered();
  if (hov && !p.tip.empty())
    tip("%s", p.tip.c_str());
  amt.buy += p.amt.buy, amt.fin += p.amt.fin, amt.sell += p.amt.sell;
  cost += p.slip + p.fee;
}

void table(const SharedData &data, LiveHub &hub, BoardState &st, const live::board::Frame &f, bool can) {
  const live::State &s = *hub.board().st;
  const Flows flows = flows_of(s);
  const std::set<std::string> listed(hub.in().codes.begin(), hub.in().codes.end());
  const auto &plan = hub.plan().plan;
  std::map<std::string, const live::rebal::Hold *> holds;
  std::map<std::string, const live::rebal::Target *> tgts;
  Caps caps;
  for (const live::board::Row &r : f.rows)
    add_cap(caps, data, st, r.code);
  if (plan) {
    for (const live::rebal::Hold &h : plan->holds)
      holds.emplace(h.code, &h);
    for (const live::rebal::Target &t : plan->targets) {
      tgts.emplace(t.code, &t);
      add_cap(caps, data, st, t.code);
    }
  }
  const auto lookup = [](const auto &m, const std::string &c) { const auto i = m.find(c); return i == m.end() ? nullptr : i->second; };
  if (!begin_fit("##pos", kHeads, kFlags | ImGuiTableFlags_Sortable | ImGuiTableFlags_SortTristate, st.pos_fit))
    return;
  for (int c = 0; c < C_COUNT; ++c)
    ImGui::TableSetupColumn(kHeads[c], col_flags(c));
  ImGui::TableNextRow(ImGuiTableRowFlags_Headers);
  for (int c = 0; c < C_COUNT; ++c) {
    ImGui::TableSetColumnIndex(c);
    if (c != C_ORDER) {
      sort_head(kHeads[c]);
      if (ImGui::IsItemHovered())
        head_tip(c, f);
      continue;
    }
    // 表头只铺底与悬停, 列名与解锁框另画在上面 (TableHeader 之后不能 SameLine)
    const ImVec2 at = ImGui::GetCursorScreenPos();
    ImGui::TableHeader("##order");
    if (ImGui::IsItemHovered())
      head_tip(c, f);
    ImGui::SetCursorScreenPos(at);
    ImGui::TextUnformatted(kHeads[c]);
    ImGui::SameLine();
    ImGui::PushStyleVar(ImGuiStyleVar_FramePadding, ImVec2(0, 0));
    ImGui::Checkbox("解锁", &st.armed);
    ImGui::PopStyleVar();
    ImGuiTableColumn &col = ImGui::GetCurrentTable()->Columns[c];
    col.ContentMaxXHeadersIdeal = std::max(col.ContentMaxXHeadersIdeal, ImGui::GetItemRectMax().x);
    if (ImGui::IsItemHovered())
      tip("勾上才下得了单");
  }
  const bool priv = hub.priv();
  double top = 0;
  for (const live::board::Row &r : f.rows)
    top = std::max(top, r.position_pct.value_or(0));
  int held = 0, fin = 0;
  double pos_pct = 0, margin = 0, sellable = 0, cost = 0;
  Amt amt;
  const auto code_cell = [&](const std::string &code) {
    ImGui::TableSetColumnIndex(C_CODE);
    if (ImGui::Selectable(priv ? kMask : code.c_str(), st.sel == code, ImGuiSelectableFlags_SpanAllColumns | ImGuiSelectableFlags_AllowOverlap))
      st.sel = code;
  };
  const auto name_cell = [&](const ImVec4 &c, const std::string &name, const RowPlan &rp, bool is_tgt) {
    ImGui::TableSetColumnIndex(C_NAME);
    if (priv)
      masked();
    else
      text_col(c, name);
    if (is_tgt && ImGui::IsItemHovered())
      tip("%s", rp.tip.c_str());
  };
  const auto cap_cell = [&](const std::string &code) {
    ImGui::TableSetColumnIndex(C_CAP);
    const auto i = caps.find(code);
    if (i == caps.end())
      return;
    if (priv)
      masked_num();
    else
      right(strf("%.0f", i->second));
  };
  const auto pnl_cell = [&](int c, const std::optional<double> &v, const std::string &s) {
    ImGui::TableSetColumnIndex(c);
    if (priv && v)
      masked_num();
    else
      pnl_opt(v, s);
  };
  const auto pct_txt = [](const std::optional<double> &v) { return v ? strf("%+.2f", *v) : ""; };
  for (const live::board::Row *p : sorted(f, caps)) {
    const live::board::Row &r = *p;
    ImGui::TableNextRow();
    ImGui::PushID(r.code.c_str());
    held += !r.closed;
    pos_pct += r.position_pct.value_or(0);
    margin += r.margin_value.value_or(0);
    sellable += r.sellable_value.value_or(0);
    const double mv = r.market_value.value_or(0);
    const live::rebal::Hold *h = lookup(holds, r.code);
    const live::rebal::Target *t = lookup(tgts, r.code);
    const RowPlan rp = row_plan(h, h ? sell_n(hub, *h) : 0, t, priv);

    code_cell(r.code);
    const ImVec4 wc = weight_col(r.position_pct, top);
    ImGui::TableSetColumnIndex(C_POS_PCT);
    if (r.position_pct)
      right(wc, strf("%.2f", *r.position_pct));
    cap_cell(r.code);
    name_cell(r.closed ? grey() : listed.count(r.code) ? kAcc
                                                       : ImGui::GetStyleColorVec4(ImGuiCol_Text),
              r.name, rp, t != nullptr);
    ImGui::TableSetColumnIndex(C_FLOW);
    flow_cell(flows, r.code, priv);
    ImGui::TableSetColumnIndex(C_FIN);
    if (h && h->financeable) {
      text_col(kHot, "R");
      ++fin;
    }
    ImGui::TableSetColumnIndex(C_ORDER);
    order_cell(hub, st, r, can);
    pnl_cell(C_DAY_PCT, r.day_pnl_pct, pct_txt(r.day_pnl_pct));
    pnl_cell(C_DAY, r.day_pnl, r.day_pnl ? wan_pnl(*r.day_pnl) : "");
    pnl_cell(C_PNL_PCT, r.pnl_pct, pct_txt(r.pnl_pct));
    pnl_cell(C_PNL, r.pnl, r.pnl ? wan_pnl(*r.pnl) : "");
    ImGui::TableSetColumnIndex(C_MV);
    if (r.market_value && priv)
      masked_num();
    else if (r.market_value)
      right(wc, wan(mv));
    ImGui::TableSetColumnIndex(C_MARGIN);
    bar(r.margin_value, mv, kHot);
    ImGui::TableSetColumnIndex(C_SELLABLE);
    bar(r.sellable_value, mv, kAcc);
    if (h) {
      ImGui::TableSetColumnIndex(C_SELL);
      sell_slider(hub, *h);
    }
    plan_cells(rp, priv, amt, cost);
    ImGui::TableSetColumnIndex(C_COLLAT);
    text_opt(r.collateral, "%.2f");
    ImGui::PopID();
  }
  // 名单里未持有的: 恒在表末, 按排名; 有行情的 (可买的) 照持仓出买入按钮与当日涨跌
  std::set<std::string> in_frame;
  for (const live::board::Row &r : f.rows)
    in_frame.insert(r.code);
  const std::shared_ptr<const live::Quotes> &quotes = hub.board().quotes;
  assert(quotes && "帧出了即有行情");
  if (plan)
    for (const live::rebal::Target &t : plan->targets) {
      if (in_frame.count(t.code))
        continue;
      ImGui::TableNextRow();
      ImGui::PushID(t.code.c_str());
      const RowPlan rp = row_plan(nullptr, 0, &t, priv);
      code_cell(t.code);
      cap_cell(t.code);
      name_cell(rp.amt.buy || rp.amt.fin ? kAcc : grey(), t.name, rp, true);
      ImGui::TableSetColumnIndex(C_FLOW);
      flow_cell(flows, t.code, priv);
      ImGui::TableSetColumnIndex(C_FIN);
      if (t.financeable && t.why.empty())
        text_col(kHot, "R");
      if (const auto q = quotes->find(t.code); t.why.empty() && q != quotes->end()) {
        const live::board::Row r = live::board::quoted(t.code, t.name, q->second);
        ImGui::TableSetColumnIndex(C_ORDER);
        order_cell(hub, st, r, can);
        pnl_cell(C_DAY_PCT, r.day_pnl_pct, pct_txt(r.day_pnl_pct));
      }
      plan_cells(rp, priv, amt, cost);
      ImGui::TableSetColumnIndex(C_COLLAT);
      text_opt(t.rate, "%.2f");
      ImGui::PopID();
    }
  ImGui::TableNextRow();
  ImGui::TableSetColumnIndex(C_CODE);
  ImGui::TextUnformatted("合计");
  ImGui::Text("%d 只", held);
  ImGui::TableSetColumnIndex(C_POS_PCT);
  right(strf("%.2f", pos_pct));
  ImGui::TableSetColumnIndex(C_FLOW);
  if (priv)
    masked();
  else {
    ImGui::Text("●%s", s.fills ? std::to_string(s.fills->rows.size()).c_str() : "-");
    ImGui::Text("○%s", s.orders ? std::to_string(s.orders->list.size()).c_str() : "-");
  }
  ImGui::TableSetColumnIndex(C_FIN);
  if (hub.plan().plan)
    text_col(kHot, std::to_string(fin));
  if (f.synced && s.acct) {
    ImGui::TableSetColumnIndex(C_DAY_PCT);
    const double ret = f.day_pnl / (s.acct->cash + f.market_value - s.acct->debt - f.day_pnl) * 100;
    right(tint(ret, 0), strf("%+.2f", ret));
  }
  ImGui::TableSetColumnIndex(C_DAY);
  if (f.synced)
    right(tint(f.day_pnl, 0), priv ? std::string(kMask) : wan_pnl(f.day_pnl));
  else if (priv)
    masked_num();
  else {
    text_col(kRed, "不全");
    if (ImGui::IsItemHovered())
      tip("成交与持仓今买 / 今卖对不上, 当日盈亏不全 (点刷新)");
  }
  ImGui::TableSetColumnIndex(C_PNL);
  if (priv)
    masked_num();
  else
    right(tint(f.pnl, 0), wan_pnl(f.pnl));
  ImGui::TableSetColumnIndex(C_MV);
  if (priv)
    masked_num();
  else
    right(wan(f.market_value));
  ImGui::TableSetColumnIndex(C_MARGIN);
  bar(margin, f.market_value, kHot);
  ImGui::TableSetColumnIndex(C_SELLABLE);
  bar(sellable, f.market_value, kAcc);
  ImGui::TableSetColumnIndex(C_AMT);
  amt_cell(amt, priv);
  if (cost) {
    ImGui::TableSetColumnIndex(C_COST);
    right(strf("%.0f", cost));
  }
  end_fit(kHeads, st.pos_fit);
}

// 事务日志: 落盘文件 + 留给页面的尾巴, 跟到底
void journal(LiveHub &hub, bool priv) {
  const auto &t = hub.task();
  if (!t)
    return;
  if (!ImGui::CollapsingHeader("事务日志"))
    return;
  ImGui::TextDisabled("%s", t->file.c_str());
  ImGui::BeginChild("##log", ImVec2(0, ImGui::GetTextLineHeightWithSpacing() * 12), ImGuiChildFlags_Borders);
  for (const LogLine &l : t->log)
    if (priv)
      ImGui::TextDisabled("%8.2f %s %s", l.t, l.kind.c_str(), kMask);
    else
      ImGui::Text("%8.2f %s %s", l.t, l.kind.c_str(), l.text.c_str());
  if (ImGui::GetScrollY() >= ImGui::GetScrollMaxY())
    ImGui::SetScrollHereY(1.0f);
  ImGui::EndChild();
}

// 右栏: 选中行 (持仓或名单股) 的行情图
void chart(SharedData &data, Quote::QuoteHub &quote, BoardState &st, bool priv) {
  if (!data.task_state.database.asset_info_ready)
    return ImGui::TextDisabled("行情图要本地资产库就绪 (见 Database)");
  quote.frame(data);
  const std::vector<std::string> &codes = data.asset_info.codes;
  const std::optional<uint32_t> id = st.sel.empty() ? std::nullopt : id_of(data, st, st.sel);
  Market::Want(quote, st.stock, id ? *id : Quote::Ref::kNone);
  std::string title;
  if (const uint32_t k = st.stock.shown; k != Quote::Ref::kNone) {
    assert(k < codes.size());
    const StockInfo *info = k < data.asset_info.stocks.size() ? data.asset_info.find(k) : nullptr;
    title = priv ? kMask : (info && !info->name.empty() ? info->name : std::string("-")) + "  " + codes[k];
  }
  Market::RenderStockPanel(quote, st.stock, title);
}

} // namespace

void RenderTabBoard(SharedData &data, LiveHub &hub, Quote::QuoteHub &quote, BoardState &st) {
  if (!hub.connected()) {
    ImGui::TextUnformatted("接券商客户端:");
    for (size_t i = 0; i < live::clients().size(); ++i) {
      const live::Client &c = live::clients()[i];
      if (ImGui::Button(strf("连接%s", c.name).c_str()))
        hub.connect(i);
      ImGui::SameLine();
      ImGui::TextWrapped("%s", c.about);
    }
    return;
  }
  const BoardView &v = hub.board();
  std::set<std::string> held;
  if (v.frame)
    for (const live::board::Row &r : v.frame->rows)
      held.insert(r.code);

  const float w = ImGui::GetContentRegionAvail().x;
  ImGui::BeginChild("##board_left", ImVec2(w * st.split, 0), ImGuiChildFlags_None, ImGuiWindowFlags_HorizontalScrollbar);
  ImGui::PushFont(nullptr, ImGui::GetStyle().FontSizeBase * kFontScale);
  const float em = ImGui::GetFontSize();
  ImGui::PushStyleVar(ImGuiStyleVar_ItemSpacing, ImVec2(em * 0.4f, em * 0.15f));
  ImGui::PushStyleVar(ImGuiStyleVar_FramePadding, ImVec2(em * 0.3f, em * 0.05f));
  ImGui::PushStyleVar(ImGuiStyleVar_CellPadding, ImVec2(em * 0.3f, em * 0.08f));
  groups(hub, st);
  info_line(hub, held);
  if (v.frame)
    table(data, hub, st, *v.frame, st.armed && !hub.busy());
  journal(hub, hub.priv());
  ImGui::PopStyleVar(3);
  ImGui::PopFont();
  ImGui::EndChild();
  ImGui::SameLine();
  ImGui::BeginChild("##board_right", ImVec2(0, 0), ImGuiChildFlags_Borders);
  chart(data, quote, st, hub.priv());
  ImGui::EndChild();
  confirm_run(hub, st);
}

} // namespace GUI::Live
