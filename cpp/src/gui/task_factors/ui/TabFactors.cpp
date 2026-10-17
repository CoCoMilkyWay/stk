// Tab Factors — 见头文件
#include "gui/task_factors/ui/TabFactors.hpp"
#include "factor/Check.hpp"           // default_d / set_k: 构建器选中算子时的参数默认值 (与 Operators 页同一张表)
#include "factor/GpuRun.hpp"          // available / device_name
#include "factor/Search/Contract.hpp" // 搜索窗 / 高原 / 终选常量: Optimize 提示
#include "factor/Search/Grid.hpp"     // kLayerMax: Optimize 提示
#include "gui/Tasks.hpp"              // StatusColor
#include "gui/util/Sparkline.hpp"     // ic~ / x~ 列迷你图

#include "imgui.h"
#include "imgui_internal.h" // TableSetColumnWidthAutoAll
#include "implot.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <cmath>
#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <iterator>
#include <mutex>
#include <numeric>
#include <optional>
#include <string>
#include <vector>

namespace GUI::Factors {

constexpr float kTipWrapEm = 48.0f;
void tip(const char *fmt, ...) {
  ImGui::BeginTooltip();
  ImGui::PushTextWrapPos(ImGui::GetCursorPosX() + ImGui::GetFontSize() * kTipWrapEm);
  va_list args;
  va_start(args, fmt);
  ImGui::TextV(fmt, args);
  va_end(args);
  ImGui::PopTextWrapPos();
  ImGui::EndTooltip();
}

namespace {

// ============================================================================
// 构建器 (从外到内): 每个槽一个下拉 (过滤框 + 算子段 + 特征段), 选了算子就在下面缩进画它的子槽 + 参数.
// 结构编辑局部化: 换算子 / 套一层 / 拆一层 都只动本节点, 子树原样保留 (中间插删一个算子不必重选下面的)
// ============================================================================

const char *kSlotNames[3] = {"x", "y", "z"};

bool contains_ci(std::string_view hay, std::string_view needle) {
  if (needle.empty())
    return true;
  const auto lower = [](char c) { return static_cast<char>((c >= 'A' && c <= 'Z') ? c + 32 : c); };
  for (size_t i = 0; i + needle.size() <= hay.size(); ++i) {
    bool ok = true;
    for (size_t j = 0; j < needle.size() && ok; ++j)
      ok = lower(hay[i + j]) == lower(needle[j]);
    if (ok)
      return true;
  }
  return false;
}

// 换本层算子, 槽内原有内容保留: 原是算子 → 子槽逐位对应 (多出的截掉, 不够的补空); 原是叶 → 进第一元. 参数取默认表.
// 子槽值域不在这里查 (下拉只按本槽 need 过滤), 交给每帧 parse 即时校验标红
void set_op(BuildNode &n, int op) {
  std::vector<BuildNode> args;
  if (n.op >= 0) {
    args = std::move(n.args);
  } else if (!n.feat.empty()) {
    BuildNode leaf;
    leaf.feat = std::move(n.feat);
    args.push_back(std::move(leaf));
  }
  args.resize(static_cast<size_t>(factor::expr::kOps[op].arity));
  n.op = op;
  n.feat.clear();
  n.args = std::move(args);
  const std::string name = factor::expr::kOps[op].name;
  n.p = factor::Param{};
  n.free = 0;
  n.p.d = factor::check::default_d(name);
  factor::check::set_k(name, n.p);
}

// 外面套一层: 本节点整体成为新算子的第一元 (新算子 arity ≥ 1, 列表已过滤)
void wrap_op(BuildNode &n, int op) {
  assert(factor::expr::kOps[op].arity >= 1);
  BuildNode inner = std::move(n);
  n = BuildNode{};
  set_op(n, op);
  n.args[0] = std::move(inner);
}

// 去掉本层: 第 a 元上提替换本节点 (空槽上提 = 清空)
void unwrap_op(BuildNode &n, size_t a) {
  assert(n.op >= 0 && a < n.args.size());
  BuildNode keep = std::move(n.args[a]);
  n = std::move(keep);
}

void set_feat(BuildNode &n, const std::string &code) {
  n.op = -1;
  n.feat = code;
  n.args.clear();
}

// 树 → 表达式源串 (与规范串同格式, 自由参数打裸键名); 有未选槽 → complete = false (源串里留 "?")
void to_source(const BuildNode &n, std::string &s, bool &complete) {
  if (n.op < 0) {
    if (n.feat.empty()) {
      s += '?';
      complete = false;
    } else {
      s += n.feat;
    }
    return;
  }
  const factor::expr::OpInfo &o = factor::expr::kOps[n.op];
  s += o.name;
  s += '(';
  bool first = true;
  const auto sep = [&] {
    if (!first)
      s += ", ";
    first = false;
  };
  for (const BuildNode &a : n.args) {
    sep();
    to_source(a, s, complete);
  }
  char buf[48];
  if (factor::expr::declares(o.params, "d")) {
    sep();
    std::snprintf(buf, sizeof(buf), "d=%d", n.p.d);
    s += (n.free & factor::expr::kFreeD) ? "d" : buf;
  }
  if (factor::expr::declares(o.params, "k")) {
    sep();
    std::snprintf(buf, sizeof(buf), "k=%g", static_cast<double>(n.p.k));
    s += (n.free & factor::expr::kFreeK) ? "k" : buf;
  }
  if (factor::expr::declares(o.params, "k2")) {
    sep();
    std::snprintf(buf, sizeof(buf), "k2=%g", static_cast<double>(n.p.k2));
    s += (n.free & factor::expr::kFreeK2) ? "k2" : buf;
  }
  s += ')';
}

// Expr 树 → 构建器 (表格行勾选载入模板); 自由参数处的占位值换成默认表值 (取消自由时有个像样的起点)
void from_expr(const factor::expr::Expr &e, int i, BuildNode &n) {
  const factor::expr::Node &x = e.nodes[static_cast<size_t>(i)];
  if (x.op < 0) {
    set_feat(n, x.feat);
    return;
  }
  set_op(n, x.op); // 参数 = 默认表
  if (!(x.free & factor::expr::kFreeD))
    n.p.d = x.p.d;
  if (!(x.free & factor::expr::kFreeK))
    n.p.k = x.p.k;
  if (!(x.free & factor::expr::kFreeK2))
    n.p.k2 = x.p.k2;
  n.free = x.free;
  for (size_t a = 0; a < n.args.size(); ++a)
    from_expr(e, x.args[a], n.args[a]);
}

// 候选列表 (槽下拉 / 套一层弹窗公用): 过滤框 + 算子段 (+ 槽模式: <清空> + 特征段).
// need = 本槽值域 (父算子该元的 in 域, 根 = REAL); 严格域 (组 id 的 INT) 只列 out ⊆ need 的算子 (与 parse 同规则).
// wrap 模式只列 arity ≥ 1 的算子 (要装得下当前节点), 无叶 / 无清空.
// 选中 → 返回 true: op ≥ 0 算子; op < 0 且 feat 非空 = 叶; 都空 = 清空
bool pick_list(const FeatureTable &ft, char *filter, size_t filter_cap, factor::Dom need, bool wrap, int cur_op, const std::string &cur_feat, int &op,
               std::string &feat) {
  if (ImGui::IsWindowAppearing()) {
    filter[0] = '\0';
    ImGui::SetKeyboardFocusHere();
  }
  ImGui::SetNextItemWidth(-1);
  ImGui::InputTextWithHint("##filter", wrap ? "过滤: 算子名 / 中文名" : "过滤: 算子名 / 中文名 / 特征 code", filter, filter_cap);
  const std::string_view f(filter);
  ImGui::Separator();
  bool picked = false;
  op = -1;
  feat.clear();
  if (!wrap && ImGui::Selectable("<清空>", false))
    picked = true;
  ImGui::TextDisabled("算子");
  for (int i = 0; i < factor::expr::kOpCount; ++i) {
    const factor::expr::OpInfo &o = factor::expr::kOps[i];
    if (wrap && o.arity == 0)
      continue;
    if (factor::dom_strict(need) && !factor::dom_sub(o.out, need))
      continue;
    if (!contains_ci(o.name, f) && !contains_ci(o.c_name, f))
      continue;
    char label[96];
    std::snprintf(label, sizeof(label), "%s  %s", o.name, o.c_name);
    if (ImGui::Selectable(label, cur_op == i)) {
      op = i;
      picked = true;
    }
    if (ImGui::IsItemHovered())
      tip("%s\n• %d 元\n• 参数 %s", o.note, o.arity, o.params[0] ? o.params : "无");
  }
  if (!wrap) {
    ImGui::TextDisabled("特征");
    for (const FeatCol &c : ft.cols) {
      if (!c.allowed || !contains_ci(c.code, f))
        continue;
      if (ImGui::Selectable(c.code.c_str(), cur_op < 0 && cur_feat == c.code)) {
        feat = c.code;
        picked = true;
      }
    }
  }
  return picked;
}

// 一个槽的下拉: 换本层 (内容保留, 见 set_op) / 选叶 / 清空
void slot_combo(BuildNode &n, const FeatureTable &ft, char *filter, size_t filter_cap, factor::Dom need) {
  const char *preview = n.op >= 0 ? factor::expr::kOps[n.op].name : (n.feat.empty() ? "<选择>" : n.feat.c_str());
  ImGui::SetNextItemWidth(220);
  if (!ImGui::BeginCombo("##slot", preview, ImGuiComboFlags_HeightLargest))
    return;
  int op;
  std::string feat;
  if (pick_list(ft, filter, filter_cap, need, false, n.op, n.feat, op, feat)) {
    if (op >= 0)
      set_op(n, op);
    else if (!feat.empty())
      set_feat(n, feat);
    else
      n = BuildNode{};
  }
  ImGui::EndCombo();
}

// 递归画一个节点: [槽名] [下拉] [参数...] [中文名] [↑] [✕]; 算子的子槽缩进一层.
// 三种结构操作各管一件事: 下拉 = 换本层 (子槽留着); ↑ = 外面加一层 (本节点进第一元); ✕ = 去掉本层 (选一元上提)
void render_node(BuildNode &n, const FeatureTable &ft, char *filter, size_t filter_cap, const char *slot_label, factor::Dom need) {
  ImGui::PushID(&n);
  ImGui::AlignTextToFramePadding();
  ImGui::TextDisabled("%s", slot_label);
  if (need != factor::Dom::REAL && ImGui::IsItemHovered())
    tip("该元值域 %s%s", factor::dom_name(need), factor::dom_strict(need) ? " (严格: 组 id)" : " (语义声明, 越界格算子自置无效)");
  ImGui::SameLine();
  slot_combo(n, ft, filter, filter_cap, need);
  if (n.op >= 0 || !n.feat.empty()) { // 非空槽才有结构操作
    ImGui::SameLine();
    if (ImGui::SmallButton("↑"))
      ImGui::OpenPopup("wrap");
    if (ImGui::IsItemHovered())
      tip("外面套一层\n"
          "• 本节点整体进新算子的第一元\n"
          "• 其余槽留空");
    if (ImGui::BeginPopup("wrap")) {
      int op;
      std::string feat;
      if (pick_list(ft, filter, filter_cap, need, true, -1, std::string(), op, feat))
        wrap_op(n, op);
      ImGui::EndPopup();
    }
  }
  if (n.op >= 0 && !n.args.empty()) {
    ImGui::SameLine();
    const factor::expr::OpInfo &o = factor::expr::kOps[n.op];
    if (ImGui::SmallButton("✕")) {
      if (n.args.size() == 1)
        unwrap_op(n, 0); // 一元: 直接上提
      else
        ImGui::OpenPopup("unwrap");
    }
    if (ImGui::IsItemHovered())
      tip("去掉本层 %s\n"
          "• 一元: 直接上提\n"
          "• 多元: 弹窗选哪一元留下, 其余丢弃",
          o.name);
    if (ImGui::BeginPopup("unwrap")) {
      ImGui::TextDisabled("留下哪一元");
      ImGui::Separator();
      for (size_t a = 0; a < n.args.size(); ++a) {
        std::string src;
        bool complete = true;
        to_source(n.args[a], src, complete);
        char label[320];
        std::snprintf(label, sizeof(label), "%s: %s", o.in[a] == factor::Dom::INT ? "组 id" : kSlotNames[a], src.c_str());
        if (ImGui::Selectable(label)) {
          unwrap_op(n, a);
          break; // n 已变, 不再迭代旧 args
        }
      }
      ImGui::EndPopup();
    }
  }
  if (n.op >= 0) {
    const factor::expr::OpInfo &o = factor::expr::kOps[n.op];
    // 每个参数: [☐ 搜] [输入]; 勾上 = 自由参数 (源串打裸键名, 输入禁用, 由 Optimize 搜)
    const auto free_box = [&](const char *id, uint8_t bit) {
      ImGui::SameLine();
      bool f = (n.free & bit) != 0;
      ImGui::PushStyleVar(ImGuiStyleVar_FramePadding, ImVec2(0.0f, ImGui::GetStyle().FramePadding.y));
      if (ImGui::Checkbox(id, &f))
        n.free = static_cast<uint8_t>(f ? (n.free | bit) : (n.free & ~bit));
      ImGui::PopStyleVar();
      if (ImGui::IsItemHovered())
        tip("自由参数开关\n"
            "• 勾上 = 自由参数\n"
            "    ◦ 表达式里写裸键名\n"
            "    ◦ 值由 Optimize 在搜索窗内每个持有期搜出, 写进 weights\n"
            "• 不勾 = 固定, 按右边的值");
      return f;
    };
    if (factor::expr::declares(o.params, "d")) {
      const bool f = free_box("##fd", factor::expr::kFreeD);
      ImGui::SameLine(0, 2);
      ImGui::SetNextItemWidth(70);
      ImGui::BeginDisabled(f);
      ImGui::InputInt("d", &n.p.d, 0);
      ImGui::EndDisabled();
      if (ImGui::IsItemHovered())
        tip("窗长 / 滞后 (分钟), 1..%d", factor::expr::kMaxD);
    }
    if (factor::expr::declares(o.params, "k")) {
      const bool f = free_box("##fk", factor::expr::kFreeK);
      ImGui::SameLine(0, 2);
      ImGui::SetNextItemWidth(70);
      ImGui::BeginDisabled(f);
      ImGui::InputFloat("k", &n.p.k, 0.f, 0.f, "%g");
      ImGui::EndDisabled();
    }
    if (factor::expr::declares(o.params, "k2")) {
      const bool f = free_box("##fk2", factor::expr::kFreeK2);
      ImGui::SameLine(0, 2);
      ImGui::SetNextItemWidth(70);
      ImGui::BeginDisabled(f);
      ImGui::InputFloat("k2", &n.p.k2, 0.f, 0.f, "%g");
      ImGui::EndDisabled();
    }
    ImGui::SameLine();
    ImGui::TextDisabled("%s", o.c_name);
    if (ImGui::IsItemHovered())
      tip("%s", o.note);
    ImGui::Indent(24.0f);
    for (size_t a = 0; a < n.args.size(); ++a) {
      const factor::Dom need = o.in[a];
      render_node(n.args[a], ft, filter, filter_cap, need == factor::Dom::INT ? "组 id" : kSlotNames[a], need);
    }
    ImGui::Unindent(24.0f);
  }
  ImGui::PopID();
}

// 该行选定持有期的汇总; 没有 → nullptr
const factor::stat::HoldStat *hold_of(const FactorRow &r, int hold) { return r.has_stat ? r.find_hold(hold) : nullptr; }

// 文件里的 stat 与当前作用域是否一致 (universe / 区间)
bool scope_matches(const FactorRow &r, const FactorsUIContext &ctx) {
  return r.scope.universe == ctx.universe && r.scope.start_date == ctx.start_date && r.scope.end_date == ctx.end_date;
}

// ============================================================================
// Stat 指标表: 主表 Stat 列 / 排序键 / 行悬停转置表 都从这一张表展开 (加指标 = 加一行)
//   group  分组, 按重要性排: 覆盖 (有效 / 可秩 / 触发 / 利用) → 形态 (单调 / 换手) → LS (能不能赚) → rIC (整体相关) → rAC (留仓程度) → 样本;
//          悬停表按组出标题行, 主表列同序; 组底色 (bg) 主表列 / 悬停表行共用; 覆盖组只在主表 (无底色, 格子不弹悬停, 不进悬停表)
//   col    非空 = 进主表 (列名), 空 = 只在悬停表
//   desc / range / best  说明 / 取值范围 / 最优 (悬停表三列, 主表列提示也由它们拼); 文案压短, 悬停表宽度由它们定
//   get    (HoldStat, 该行 FactorRow: 满档 cap = scope.n_full, 行级 val% / rank%) → double (整数计数也走这里, fmt 用 %.0f)
//   grade  固定区间着色 (主表 / 悬停表共用, 不随样本变): line(差端, 好端) 线性 (好端 < 差端 即越小越好); band(a, b) 中点最好 → 两端差;
//          区间外钳位到端色; 默认不着色 (诊断量 / 尺度随 h 变的)
//   lag    样本 = ok_rac 行 (n_rac; rAC 系 / turn), 否则 ok 行 (n); 对应行数 < 3 → 空
//   公式见 factor/Stat/Contract.hpp【持仓口径】【二级 HoldStat】(rIC / rAC 同一套矩; t 值按 n/h 折算重叠持有期; LS% / SR 是不触发记 0 的真实收益,
//   与 Inspect 累计线 / 图例同一份 ls_annual / ann_sharpe)
// ============================================================================
using HS = factor::stat::HoldStat;
using FR = FactorRow;
enum class Group : uint8_t { Cover,
                             Shape,
                             LS,
                             RIC,
                             RAC,
                             Sample,
                             kCount };
struct GroupInfo {
  const char *name, *desc;
  ImVec4 bg; // 组底色 (淡)
  bool tip;  // 进悬停表 + 主表带底色 + 格子弹悬停; false = 只在主表看板
};
constexpr GroupInfo kGroupInfo[static_cast<size_t>(Group::kCount)] = {
    {"覆盖", "有效 / 可秩 / 触发 / 利用", ImVec4(), false},
    {"形态", "分层单调 / 换手", ImVec4(0.70f, 0.45f, 0.95f, 0.10f), true},
    {"LS", "顶底多空超额 (G20 做多 + G1 做空)", ImVec4(0.35f, 0.85f, 0.45f, 0.10f), true},
    {"rIC", "因子秩 × 标签秩 截面相关 (rank IC)", ImVec4(0.35f, 0.65f, 1.00f, 0.10f), true},
    {"rAC", "因子秩 lag=h 截面自相关 (rank AC)", ImVec4(1.00f, 0.65f, 0.25f, 0.10f), true},
    {"样本", "有效 t 行数", ImVec4(0.60f, 0.60f, 0.60f, 0.08f), true},
};
ImU32 group_bg(Group g) { return ImGui::GetColorU32(kGroupInfo[static_cast<size_t>(g)].bg); }
bool group_tip(Group g) { return kGroupInfo[static_cast<size_t>(g)].tip; }
struct Grade {
  enum Kind : uint8_t { None,
                        Line,
                        Band } kind = None;
  double a = 0.0, b = 0.0; // Line: a 差端, b 好端; Band: 两端 (a < b)
};
constexpr Grade line(double bad, double good) { return {Grade::Line, bad, good}; }
constexpr Grade band(double a, double b) { return {Grade::Band, a, b}; }
struct Metric {
  Group group;
  const char *name; // 悬停表行名 (组内短名)
  const char *col;  // 主表列名; nullptr = 不进主表
  const char *desc, *range, *best;
  const char *fmt;
  double (*get)(const HS &, const FR &);
  Grade grade = {};
  bool lag = false;
};
constexpr Metric kMetrics[] = {
    {Group::Cover, "val%", "val%", "有效率 % = 池内格中根平面有效的占比", "[0, 100]", "越大", "%.1f",
     [](const HS &, const FR &r) { return static_cast<double>(r.valid_pct); }, line(90, 100)},
    {Group::Cover, "rank%", "rank%", "可秩率 % = 并列块 ≤ ⌈有效数/20⌉ 只的格 / 有效格", "[0, 100]", "越大", "%.1f",
     [](const HS &, const FR &r) { return static_cast<double>(r.rank_pct); }, line(60, 90)},
    {Group::Cover, "trig%", "trig%", "触发率 % = 至少一腿非空的行 / ok 行", "[0, 100]", "越大", "%.1f",
     [](const HS &h, const FR &) { return 100.0 * factor::stat::trigger_of(h); }, line(80, 90)},
    {Group::Cover, "util%", "util%", "利用率 % = 触发行平均每腿只数 / 满档 cap", "[0, ∞)", "≈ 100", "%.1f",
     [](const HS &h, const FR &r) { return 100.0 * factor::stat::util_of(h, r.scope.n_full); }, band(80, 120)},
    {Group::Shape, "mono", "mono", "组均值 对组号 Spearman", "[-1, 1]", "越大, 1 严格单调", "%+.3f",
     [](const HS &h, const FR &) { return static_cast<double>(h.mono); }, line(0.9, 1)},
    {Group::Shape, "turn", "turn", "年化换手 = (1 − rAC) × 每年期数", "[0, ∞)", "越小", "%.0f",
     [](const HS &h, const FR &) { return factor::stat::turnover_annual(h); }, line(1000, 100), true},
    {Group::LS, "LS%", "LS%", "年化真实收益 % (不触发记 0)", "(-∞, ∞)", "越大", "%+.1f",
     [](const HS &h, const FR &) { return 100.0 * factor::stat::ls_annual(h); }, line(10, 50)},
    {Group::LS, "SR", "SR", "年化真实 Sharpe (不触发记 0)", "(-∞, ∞)", "越大", "%+.2f",
     [](const HS &h, const FR &) { return factor::stat::sharpe_of(h); }, line(5, 10)},
    {Group::LS, "mean", nullptr, "ok 行每期多空 均值", "(-∞, ∞)", "越大", "%+.5f", [](const HS &h, const FR &) { return static_cast<double>(h.ls.mean); }},
    {Group::LS, "sd", nullptr, "ok 行每期多空 标准差", "[0, ∞)", "越小", "%.5f", [](const HS &h, const FR &) { return static_cast<double>(h.ls.sd); }},
    {Group::LS, "IR", nullptr, "ok 行每期多空 均值 / 标准差", "(-∞, ∞)", "越大", "%+.3f", [](const HS &h, const FR &) { return static_cast<double>(h.ls.ir); }},
    {Group::LS, "t", nullptr, "ok 行每期多空 t 值", "(-∞, ∞)", "越大, |t|>2 显著", "%+.2f", [](const HS &h, const FR &) { return static_cast<double>(h.ls.t); }},
    {Group::LS, "pos", nullptr, "ok 行每期多空 > 0 占比", "[0, 1]", "越大, 0.5 无信号", "%.2f",
     [](const HS &h, const FR &) { return static_cast<double>(h.ls.pos); }},
    {Group::LS, "β", nullptr, "ok 行每期多空 对市场 β", "(-∞, ∞)", "越近 0", "%+.3f", [](const HS &h, const FR &) { return static_cast<double>(h.beta); }},
    {Group::RIC, "mean", "rIC", "rIC 均值", "[-1, 1]", "越大", "%+.4f", [](const HS &h, const FR &) { return static_cast<double>(h.ric.mean); },
     line(0.03, 0.05)},
    {Group::RIC, "sd", nullptr, "rIC 标准差", "[0, 1]", "越小", "%.4f", [](const HS &h, const FR &) { return static_cast<double>(h.ric.sd); }},
    {Group::RIC, "IR", "rICIR", "rIC 均值 / 标准差", "(-∞, ∞)", "越大", "%+.3f", [](const HS &h, const FR &) { return static_cast<double>(h.ric.ir); },
     line(0, 0.3)},
    {Group::RIC, "t", nullptr, "rIC t 值", "(-∞, ∞)", "越大, |t|>2 显著", "%+.2f", [](const HS &h, const FR &) { return static_cast<double>(h.ric.t); }},
    {Group::RIC, "pos", "rIC>0", "rIC > 0 占比", "[0, 1]", "越大, 0.5 无信号", "%.2f", [](const HS &h, const FR &) { return static_cast<double>(h.ric.pos); },
     line(0.5, 0.7)},
    {Group::RAC, "mean", "rAC", "rAC 均值", "[-1, 1]", "越大", "%+.3f", [](const HS &h, const FR &) { return static_cast<double>(h.rac.mean); },
     line(0.8, 1), true},
    {Group::RAC, "sd", nullptr, "rAC 标准差", "[0, 1]", "越小", "%.4f", [](const HS &h, const FR &) { return static_cast<double>(h.rac.sd); }, {}, true},
    {Group::Sample, "n", nullptr, "标签 ok 行数", "[0, T]", "越大, <3 标签侧空", "%.0f", [](const HS &h, const FR &) { return static_cast<double>(h.n); }},
    {Group::Sample, "n_rac", nullptr, "rAC 可算行数", "[0, T]", "越大, <3 rAC / turn 空", "%.0f",
     [](const HS &h, const FR &) { return static_cast<double>(h.n_rac); }},
};
constexpr bool grades_ok() {
  for (const Metric &m : kMetrics)
    if ((m.grade.kind == Grade::Line && m.grade.a == m.grade.b) || (m.grade.kind == Grade::Band && !(m.grade.a < m.grade.b)) ||
        (m.col && m.grade.kind == Grade::None))
      return false;
  return true;
}
static_assert(grades_ok(), "着色区间: Line 两端不等, Band a < b, 主表列都须有区间");
constexpr int kNumMetrics = static_cast<int>(std::size(kMetrics));
constexpr int count_cols() {
  int n = 0;
  for (const Metric &m : kMetrics)
    n += m.col != nullptr;
  return n;
}
constexpr int kNumStatCols = count_cols();
// 主表第 i 个 Stat 列 ↔ kMetrics 下标
constexpr std::array<int, kNumStatCols> stat_col_metric() {
  std::array<int, kNumStatCols> a{};
  int i = 0;
  for (int m = 0; m < kNumMetrics; ++m)
    if (kMetrics[m].col)
      a[static_cast<size_t>(i++)] = m;
  return a;
}
constexpr std::array<int, kNumStatCols> kStatColMetric = stat_col_metric();

// 有值: 样本组照显; 其余看对应行数 ≥ 3 (lag 指标看 n_rac, 其余看 n)
bool has_value(const HS &h, const Metric &m) { return m.group == Group::Sample || (m.lag ? h.n_rac : h.n) >= 3; }

// 一格: 无值 → "-"; 否则按 fmt 打印, 可带色. r = h 所在行
void metric_cell(const HS *h, const Metric &m, const FR &r, const ImVec4 *color = nullptr) {
  if (!h || !has_value(*h, m))
    ImGui::TextDisabled("-");
  else if (color)
    ImGui::TextColored(*color, m.fmt, m.get(*h, r));
  else
    ImGui::Text(m.fmt, m.get(*h, r));
}
// 排序键: 无值的行排最前 (升序) / 最后 (降序)
double metric_key(const HS *h, const Metric &m, const FR &r) { return h ? m.get(*h, r) : -1e300; }

// ============================================================================
// 主表列模型: 固定列 (kFixedCols, 按 Col 枚举寻址) 与 Stat 列 (kMetrics 里 col 非空的, 同序) 穿插, 显示顺序 = kColLayout:
//   edit type name_en name_cn *h frame status time | val% rank% trig% util% mono turn LS% SR rIC rICIR rIC>0 rAC | ic~ x~ expr ops feats slots note
// ============================================================================
enum Col : int { Edit,
                 Type,
                 NameEn,
                 NameCn,
                 HoldSrc,
                 Frame,
                 Status,
                 Time,
                 IcPdf,
                 XPdf,
                 Expr,
                 Ops,
                 Feats,
                 Slots,
                 Note,
                 kNumFixed };
struct FixedCol {
  Col id; // 须 = 数组下标 (下方 static_assert)
  const char *name, *tip;
  bool sortable;
};
constexpr FixedCol kFixedCols[kNumFixed] = {
    {Edit, "edit",
     "勾选 → 编辑模式: 载入上方构建器, 可改 / 删\n"
     "• 同时只勾一个\n"
     "• 排序 = 文件名序 (默认)",
     true},
    {Type, "type",
     "因子类型 (文件 type 键)\n"
     "• alpha: 预测超额收益, 再分 CS / TS 口径 (见 frame 列)\n"
     "• beta: 风险暴露 (未实现, 留位)",
     true},
    {NameEn, "name_en",
     "英文名 = 文件名主干: <factor_dir>/<universe>/<name_en>.json\n"
     "• 仅 [A-Za-z0-9_], ≤63 字符\n"
     "• 点行高光 → Inspect 页单因子展示",
     true},
    {NameCn, "name_cn",
     "中文名 (文件 name_cn 键)\n"
     "• 纯汉字, 1..10 字\n"
     "• 缺 / 不合 → BROKEN",
     true},
    {HoldSrc, "*h",
     "该权重搜出来时的持有期 (它最优的 horizon)\n"
     "• 点行 → Inspect 自动切到这个 Hold\n"
     "• 父行 = 其代表权重的; 手填权重 = -",
     true},
    {Frame, "frame",
     "口径 (由根算子定)\n"
     "• 共同: 标签取超额 (减截面均值); 持仓口径: 一腿满档 cap 只, 不足按只数/cap 建仓, 超出均分, 空组 / 空腿记 0\n"
     "• CS = 截面\n"
     "    ◦ 根 CsRank / CsNormRank / CsZ\n"
     "    ◦ 每 t 截面 rank 分 20 组\n"
     "    ◦ 多空对冲\n"
     "• TS = 时序\n"
     "    ◦ 根 TsRankRoll 5 日\n"
     "    ◦ 自身分位直接分 20 组\n"
     "    ◦ 组可空 → long / flat",
     true},
    {Status, "status",
     "评估状态\n"
     "• BROKEN 红: 悬停行看原因; 文件不动, 手动处理\n"
     "    ◦ 文件 / type / name_cn / 表达式 不合\n"
     "    ◦ 根非归一\n"
     "    ◦ 权重不是模板的实例\n"
     "    ◦ 组 id 不合\n"
     "• 未搜索 灰: 模板含自由参数但还没 weights\n"
     "    ◦ 不评; 高光它点 Optimize\n"
     "• ok 绿: 有 stat 且作用域一致 (本轮算的或文件读的)\n"
     "• file≠scope 黄: 文件 stat 是别的 universe / 区间算的\n"
     "• no stat: 从未评估\n"
     "• dup 黄: 与另一行同 hold 同一规范串",
     false},
    {Time, "time",
     "算一遍 DAG 的耗时 (ms)\n"
     "• = 子树全部算子节点之和, 共享节点算给每个用它的因子\n"
     "• CPU 全核 wall / GPU 纯 kernel",
     true},
    {IcPdf, "ic~",
     "逐行 rank IC 分布 (选定持有期, ok 行; 与 Inspect 左下左半同一组样本)\n"
     "• KLL PDF, 峰值归一; x 域 = 数据范围并含 0\n"
     "• 绿 = rIC < 0 段, 红 = rIC > 0 段\n"
     "• 悬停放大; Run 时算, 落文件 stat",
     false},
    {XPdf, "x~",
     "根输入值分布 (归一前的因子值; 与 Inspect 左下右半同一份)\n"
     "• 池内有效格, 截尾 ≈ [P0.4, P99.6]; KLL PDF, 峰值归一; x 域含 0\n"
     "• 绿 = x < 0 段, 红 = x > 0 段; 与持有期无关\n"
     "• 悬停放大; Run 时算, 落文件 stat",
     false},
    {Expr, "expr",
     "规范串 = 解析后重新序列化\n"
     "• 格式: 算子 PascalCase, 参数 d / k / k2 显式\n"
     "• 显示哪个串\n"
     "    ◦ 多权重因子的父行 / 未搜索行: 模板 (自由参数打裸键名)\n"
     "    ◦ 子行 / 单权重行: 该权重的具体串\n"
     "    ◦ 解析不过的行: 文件原串",
     false},
    {Ops, "ops", "算子节点数", true},
    {Feats, "feats", "去重特征数 (输入平面数)", true},
    {Slots, "slots",
     "DAG 缓冲槽数 = 峰值同时存活的中间量\n"
     "• 内存 / 显存 = slots × T·A × 5B",
     true},
    {Note, "note", "文件 note 键: 人 / agent 写的一句话", false},
};
constexpr bool fixed_cols_ordered() {
  for (int i = 0; i < kNumFixed; ++i)
    if (kFixedCols[i].id != i)
      return false;
  return true;
}
static_assert(fixed_cols_ordered(), "kFixedCols 顺序须与 Col 枚举一致");
constexpr ImVec4 kEditColor(1.0f, 0.75f, 0.3f, 1.0f); // 编辑中的行 / 标题

// 显示顺序: Stat 列夹在 time 与 Expr 之间. 一列 = 固定列 (fixed ≥ 0) 或 Stat 列 (stat ≥ 0, 主表第 stat 个 Stat 列 → kMetrics[kStatColMetric[stat]])
struct ColRef {
  int fixed = -1, stat = -1;
};
constexpr int kStatInsertAfter = Time;
constexpr int kNumCols = kNumFixed + kNumStatCols;
constexpr std::array<ColRef, kNumCols> col_layout() {
  std::array<ColRef, kNumCols> a{};
  int c = 0;
  for (int f = 0; f <= kStatInsertAfter; ++f)
    a[static_cast<size_t>(c++)].fixed = f;
  for (int i = 0; i < kNumStatCols; ++i)
    a[static_cast<size_t>(c++)].stat = i;
  for (int f = kStatInsertAfter + 1; f < kNumFixed; ++f)
    a[static_cast<size_t>(c++)].fixed = f;
  return a;
}
constexpr std::array<ColRef, kNumCols> kColLayout = col_layout();
static_assert(kColLayout[0].fixed == Edit, "edit 列须在第 0 列 (行循环里单独画)");
// 表列号 ↔ 固定列 / 指标
constexpr int col_of(Col f) {
  for (int c = 0; c < kNumCols; ++c)
    if (kColLayout[static_cast<size_t>(c)].fixed == f)
      return c;
  return -1;
}
const Metric *col_metric(int c) {
  const int i = kColLayout[static_cast<size_t>(c)].stat;
  return i >= 0 ? &kMetrics[kStatColMetric[static_cast<size_t>(i)]] : nullptr;
}
const char *col_name(int c) {
  const ColRef &r = kColLayout[static_cast<size_t>(c)];
  return r.fixed >= 0 ? kFixedCols[r.fixed].name : col_metric(c)->col;
}
bool col_sortable(int c) {
  const ColRef &r = kColLayout[static_cast<size_t>(c)];
  return r.fixed >= 0 ? kFixedCols[r.fixed].sortable : true;
}
std::string grade_text(const Grade &g) {
  char buf[96];
  switch (g.kind) {
  case Grade::Line:
    std::snprintf(buf, sizeof(buf), "%g 红 → %g 绿, 外侧钳位", g.a, g.b);
    return buf;
  case Grade::Band:
    std::snprintf(buf, sizeof(buf), "%g / %g 红 → %g 绿, 外侧钳位", g.a, g.b, 0.5 * (g.a + g.b));
    return buf;
  case Grade::None:
    break;
  }
  assert(false);
  return {};
}
// 列提示: Stat 列由指标表拼 (只拼一次)
const std::string &col_tip(int c) {
  static const std::array<std::string, kNumCols> tips = [] {
    std::array<std::string, kNumCols> a;
    for (int i = 0; i < kNumCols; ++i) {
      const ColRef &r = kColLayout[static_cast<size_t>(i)];
      if (r.fixed >= 0) {
        a[static_cast<size_t>(i)] = kFixedCols[r.fixed].tip;
        continue;
      }
      const Metric &m = *col_metric(i);
      const GroupInfo &g = kGroupInfo[static_cast<size_t>(m.group)];
      a[static_cast<size_t>(i)] = std::string(g.name) + " · " + m.name + "\n• 说明  " + m.desc + "\n• 范围  " + m.range + "\n• 最优  " + m.best +
                                  "\n• 组    " + g.desc + "\n• 显示  选定持有期的值, 样本 < 3 为空\n• 字色  " + grade_text(m.grade) +
                                  (g.tip ? "\n• 底色  组\n• 悬停  Stat 列格子看全部持有期" : "");
    }
    return a;
  }();
  return tips[static_cast<size_t>(c)];
}

constexpr int kTipFixedCols = 4; // 悬停表前置列: 指标 / 说明 / 范围 / 最优

// 固定区间着色 (主表 / 悬停表共用一套): 值按 grade 映射到 [0, 1] (0 = 差端及以外, 1 = 好端及以外), 红 → 文字色 → 绿 三段插值; 无区间 → 不着色 (nullopt)
double grade_t(const Grade &g, double v) {
  switch (g.kind) {
  case Grade::Line:
    return std::clamp((v - g.a) / (g.b - g.a), 0.0, 1.0);
  case Grade::Band:
    return std::clamp(1.0 - std::fabs(2.0 * v - g.a - g.b) / (g.b - g.a), 0.0, 1.0);
  case Grade::None:
    break;
  }
  assert(false);
  return 0.0;
}
std::optional<ImVec4> grade_color(const Metric &m, const HS *h, const FR &r) {
  if (m.grade.kind == Grade::None || !h || !has_value(*h, m))
    return std::nullopt;
  const float t = static_cast<float>(grade_t(m.grade, m.get(*h, r)));
  const ImVec4 bad = StatusColor(TaskStatus::Kind::Error), mid = ImGui::GetStyleColorVec4(ImGuiCol_Text), good = StatusColor(TaskStatus::Kind::Ready);
  return t < 0.5f ? ImLerp(bad, mid, t * 2.f) : ImLerp(mid, good, (t - 0.5f) * 2.f);
}

// Stat 列悬停: 精简头部 + 转置表 (行 = kMetrics 按组, 列 = 持有期), 有区间的指标按固定区间着色
void stat_tooltip(const FactorRow &r, const FactorsUIContext &ctx) {
  ImGui::TextDisabled("%s%s | %s | %s %s..%s | 满档 %.1f | %.3f ms", r.stat_from_file ? "文件" : "本轮",
                      r.stat_from_file && !scope_matches(r, ctx) ? " (作用域≠当前)" : "", factor::stat::frame_name(r.frame), r.scope.universe.c_str(),
                      misc::dash(r.scope.start_date).c_str(), misc::dash(r.scope.end_date).c_str(), r.scope.n_full, r.eval_ms);
  ImGui::TextDisabled("标签: 毛价格收益; 着色 (固定区间, 见列头): ");
  ImGui::SameLine(0, 0);
  ImGui::TextColored(StatusColor(TaskStatus::Kind::Error), "红 = 差端");
  ImGui::SameLine(0, 0);
  ImGui::TextDisabled(" → ");
  ImGui::SameLine(0, 0);
  ImGui::TextColored(StatusColor(TaskStatus::Kind::Ready), "绿 = 好端");
  ImGui::SameLine(0, 0);
  ImGui::TextDisabled("; 原色 = 无区间");
  if (!ImGui::BeginTable("HoldStatTip", kTipFixedCols + r.n_hold, ImGuiTableFlags_SizingFixedFit | ImGuiTableFlags_BordersInnerV | ImGuiTableFlags_RowBg))
    return;
  ImGui::TableSetupColumn("指标");
  ImGui::TableSetupColumn("说明");
  ImGui::TableSetupColumn("范围");
  ImGui::TableSetupColumn("最优");
  std::string names[factor::stat::kMaxHold];
  for (int k = 0; k < r.n_hold; ++k) {
    names[k] = factor::stat::hold_name(r.hold[k].hold);
    ImGui::TableSetupColumn(names[k].c_str());
  }
  ImGui::TableHeadersRow();
  Group cur = Group::kCount;
  for (const Metric &m : kMetrics) {
    if (!group_tip(m.group))
      continue;
    if (m.group != cur) { // 组标题行
      cur = m.group;
      const GroupInfo &g = kGroupInfo[static_cast<size_t>(cur)];
      ImGui::TableNextRow();
      ImGui::TableSetBgColor(ImGuiTableBgTarget_RowBg1, group_bg(cur));
      ImGui::TableNextColumn();
      ImGui::TextDisabled("%s", g.name);
      ImGui::TableNextColumn();
      ImGui::TextDisabled("%s", g.desc);
    }
    ImGui::TableNextRow();
    ImGui::TableSetBgColor(ImGuiTableBgTarget_RowBg1, group_bg(cur));
    ImGui::TableNextColumn();
    ImGui::Text("  %s", m.name);
    ImGui::TableNextColumn();
    ImGui::TextUnformatted(m.desc);
    ImGui::TableNextColumn();
    ImGui::TextUnformatted(m.range);
    ImGui::TableNextColumn();
    ImGui::TextUnformatted(m.best);
    for (int k = 0; k < r.n_hold; ++k) {
      ImGui::TableNextColumn();
      const std::optional<ImVec4> c = grade_color(m, &r.hold[k], r);
      metric_cell(&r.hold[k], m, r, c ? &*c : nullptr);
    }
  }
  ImGui::EndTable();
}

// status 列: BROKEN / 未搜索 / … / running / no stat / ok / file / file≠scope (+ dup)
void status_cell(const FactorRow &r, const FactorsUIContext &ctx) {
  if (!r.error.empty()) {
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Error), "BROKEN");
    return;
  }
  if (r.widx < 0) { // 模板含自由参数而无 weights
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Muted), "未搜索");
    return;
  }
  switch (r.status) {
  case RowStatus::Pending:
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Muted), "…");
    return;
  case RowStatus::Running:
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Busy), "running");
    return;
  case RowStatus::Done:
    break;
  }
  if (!r.has_stat) {
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Muted), "no stat");
  } else if (!r.stat_from_file || scope_matches(r, ctx)) {
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Ready), "ok");
  } else {
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "file≠scope");
  }
  if (!r.dup_of.empty()) {
    ImGui::SameLine();
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "dup");
  }
}

// 一行在选定持有期的目标分 (RowScore 的压缩; 不显示, 只挑父行代表权重; hold = 0 即无)
struct OptCell {
  int hold = 0;
  double score = 0.0; // −∞ = LS / rIC ≤ 0
};

// 表的一行怎么画: 平铺 (单权重 / 文件级行) / 父 (多权重因子的代表权重, 表达式列显示模板) / 子 (展开后的一个权重, 名字列显示权重号)
enum class RowKind : uint8_t { Flat,
                               Parent,
                               Child };

// 分布小图一格 (ic~ / x~ 列): 点列两端补到含 0 (全表同口径), 以 0 为界红绿; 悬停 ImPlot 放大 (同 Features Dist 列)
void mini_pdf_cell(const MiniPdf *p, const char *id, const char *title) {
  if (!p || !p->has()) {
    ImGui::TextDisabled("-");
    return;
  }
  const ImVec2 size(kSparkWidth, ImGui::GetTextLineHeight());
  if (!ImGui::IsRectVisible(size)) {
    ImGui::Dummy(size);
    return;
  }
  static std::vector<float> xs, ys; // GUI 单线程, 帧内复用
  xs.clear();
  ys.clear();
  if (p->lo > 0.f) {
    xs.push_back(0.f);
    ys.push_back(0.f);
  }
  for (int i = 0; i < kMiniPdfPts; ++i) {
    xs.push_back(p->lo + (p->hi - p->lo) * static_cast<float>(i) / (kMiniPdfPts - 1));
    ys.push_back(p->y[i]);
  }
  if (p->hi < 0.f) {
    xs.push_back(0.f);
    ys.push_back(0.f);
  }
  const int n = static_cast<int>(xs.size());
  if (!sparkline(id, xs.data(), ys.data(), n, size, kSparkPosColor, &kSparkNegColor))
    return;
  ImGui::BeginTooltip();
  ImGui::TextUnformatted(title);
  if (ImPlot::BeginPlot("##mini_pdf", ImVec2(360, 200), ImPlotFlags_NoLegend)) {
    ImPlot::SetupAxes(nullptr, "pdf / 峰", 0, ImPlotAxisFlags_AutoFit);
    ImPlot::SetupAxisLimits(ImAxis_X1, xs.front(), xs.back(), ImPlotCond_Always);
    int k = 0;
    while (k < n && xs[static_cast<size_t>(k)] < 0.f)
      ++k;
    if (k > 0) {
      ImPlot::SetNextLineStyle(kSparkNegColor, 2.0f);
      ImPlot::PlotLine("##neg", xs.data(), ys.data(), std::min(k + 1, n));
    }
    if (k < n) {
      ImPlot::SetNextLineStyle(kSparkPosColor, 2.0f);
      ImPlot::PlotLine("##pos", xs.data() + k, ys.data() + k, n - k);
    }
    ImPlot::EndPlot();
  }
  ImGui::EndTooltip();
}

// 固定列一格 (edit 列是勾选框, 在表循环里单独画); hold = 看板选定持有期 (ic~ 列)
void fixed_cell(Col c, const FactorRow &r, const FactorsUIContext &ctx, bool editing, RowKind kind, int hold) {
  const bool ok = r.error.empty();
  const auto text = [&](const std::string &s, bool hi = false) {
    if (s.empty())
      ImGui::TextDisabled("-");
    else if (hi)
      ImGui::TextColored(kEditColor, "%s", s.c_str());
    else
      ImGui::TextUnformatted(s.c_str());
  };
  const auto num = [&](bool has, const char *fmt, auto v) {
    if (has)
      ImGui::Text(fmt, v);
    else
      ImGui::TextDisabled("-");
  };
  switch (c) {
  case Type:
    if (r.kind == FactorKind::Beta)
      ImGui::TextColored(StatusColor(TaskStatus::Kind::Muted), "%s", kind_name(r.kind));
    else
      ImGui::TextUnformatted(kind_name(r.kind));
    return;
  case NameEn:
    if (kind == RowKind::Child) { // 子行: 权重号 (持有期见 *h 列)
      ImGui::TextDisabled("  #%d", r.widx);
      return;
    }
    return text(r.name_en, editing);
  case NameCn:
    return kind == RowKind::Child ? ImGui::TextDisabled("") : text(r.name_cn, editing);
  case HoldSrc:
    return r.widx >= 0 && r.hold_src > 0 ? ImGui::TextUnformatted(factor::stat::hold_name(r.hold_src).c_str()) : ImGui::TextDisabled("-");
  case Frame:
    return ok ? ImGui::TextUnformatted(factor::stat::frame_name(r.frame)) : ImGui::TextDisabled("-");
  case Status:
    return status_cell(r, ctx);
  case Time:
    return num(r.has_stat, "%.1f", r.eval_ms);
  case IcPdf: {
    const std::string title = "rIC 分布 (" + factor::stat::hold_name(hold) + ", 峰值归一)";
    return mini_pdf_cell(r.has_stat ? r.find_ic_pdf(hold) : nullptr, "##ic_pdf", title.c_str());
  }
  case XPdf:
    return mini_pdf_cell(r.has_stat ? &r.x_pdf : nullptr, "##x_pdf", "根输入值分布 (截尾, 峰值归一)");
  case Expr: // 父 / 文件级行 = 模板 (自由参数裸键名); 子 / 平铺权重 = 具体串; 模板解析不过 → 文件原串 (红)
    if (kind != RowKind::Parent && !r.expr.empty())
      ImGui::TextUnformatted(r.expr.c_str());
    else if (!r.tmpl.empty())
      ImGui::TextUnformatted(r.tmpl.c_str());
    else if (!r.expr_raw.empty())
      ImGui::TextColored(StatusColor(TaskStatus::Kind::Error), "%s", r.expr_raw.c_str());
    else
      ImGui::TextDisabled("-");
    return;
  case Ops:
    return num(ok && !r.expr.empty(), "%d", r.n_ops);
  case Feats:
    return num(ok && !r.expr.empty(), "%d", r.n_feats);
  case Slots:
    return num(ok && !r.expr.empty(), "%d", r.n_slots);
  case Note:
    return text(r.note);
  case Edit:
  case kNumFixed:
    break;
  }
  assert(false);
}

// 表格行只保留 Stat 列 (mono … rAC, 覆盖组除外) 的期限对比 hover; 其它格子不弹冗余提示.
// 位置自己定: ImGui 的 tooltip 定位在四个方向都放不下时退化为贴鼠标右下 (大半出屏, 看着像没触发); 这里用上一帧的窗口尺寸把左上角夹回视口
void row_tooltip(const FactorRow &r, const FactorsUIContext &ctx) {
  static ImVec2 s_size;                          // 上一帧尺寸 (首帧 0 → 不夹, ImGui 的 AlwaysAutoResize 首帧本就隐藏)
  const int hc = ImGui::TableGetHoveredColumn(); // 须在 BeginTooltip 前取 (tooltip 窗口里没有当前表)
  const Metric *hm = hc >= 0 && hc < kNumCols ? col_metric(hc) : nullptr;
  const bool on_stat = hm && group_tip(hm->group);
  if (!r.has_stat || !on_stat)
    return;
  const ImGuiViewport *vp = ImGui::GetMainViewport();
  const ImVec2 mouse = ImGui::GetMousePos();
  ImVec2 pos(mouse.x + 16.f, mouse.y + 16.f);
  pos.x = std::max(vp->WorkPos.x, std::min(pos.x, vp->WorkPos.x + vp->WorkSize.x - s_size.x));
  pos.y = std::max(vp->WorkPos.y, std::min(pos.y, vp->WorkPos.y + vp->WorkSize.y - s_size.y));
  ImGui::SetNextWindowPos(pos);
  ImGui::BeginTooltip();
  stat_tooltip(r, ctx);
  s_size = ImGui::GetWindowSize();
  ImGui::EndTooltip();
}

} // namespace

int RenderTabFactors(FactorsService &svc, SearchService &ss, FactorsUIState &ui, const FactorsUIContext &ctx) {
  int action = 0;
  const Phase st = svc.phase.load(std::memory_order_acquire);
  const bool busy = svc.busy();
  const Phase sst = ss.phase.load(std::memory_order_acquire);
  const bool sbusy = ss.busy(); // Optimize 在跑: 它要写因子文件, Run 让路
  const FeatureTable &ft = svc.feats();
  const bool gpu_ok = factor::gpu::available();
  if (!gpu_ok)
    ui.backend = 0;
  else if (ui.backend < 0)
    ui.backend = 1; // 有 GPU 默认 GPU
  ui.hold_idx = std::clamp(ui.hold_idx, 0, std::max(0, static_cast<int>(ft.labels.size()) - 1));
  const int cur_hold = ft.labels.empty() ? 0 : ft.labels[static_cast<size_t>(ui.hold_idx)].hold;

  // ==========================================================================
  // 1. 作用域 + 后端 + Run / Cancel / Rescan + 状态
  // ==========================================================================
  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "1. Scope:");
  ImGui::SameLine();
  ImGui::Text("%s  %s .. %s", ctx.universe.c_str(), misc::dash(ctx.start_date).c_str(), misc::dash(ctx.end_date).c_str());
  if (ImGui::IsItemHovered())
    tip("因子目录 %s\n"
        "• 作用域 = Feature 页所选票池 (Lob 库) + config 日期区间\n"
        "• 一个时间只算一个作用域",
        ctx.factor_dir.c_str());
  ImGui::SameLine();
  ImGui::TextDisabled("|");
  ImGui::SameLine();
  ImGui::SetNextItemWidth(70);
  {
    const char *items[] = {"CPU", "GPU"};
    if (ImGui::BeginCombo("Backend", items[ui.backend])) {
      if (ImGui::Selectable("CPU", ui.backend == 0))
        ui.backend = 0;
      if (!gpu_ok)
        ImGui::BeginDisabled();
      if (ImGui::Selectable("GPU", ui.backend == 1))
        ui.backend = 1;
      if (!gpu_ok)
        ImGui::EndDisabled();
      ImGui::EndCombo();
    }
  }
  if (ImGui::IsItemHovered()) {
    const char *g = factor::gpu::device_name();
    tip("评估后端\n"
        "• 两端同走一张共享 DAG\n"
        "    ◦ 跨因子公共子式只算一次\n"
        "    ◦ 顺序走节点\n"
        "• CPU\n"
        "    ◦ 每节点切满所有核: CS / Point / Expand 按 t 切, Roll / Ema 按资产列切块\n"
        "    ◦ 与单线程逐位一致\n"
        "    ◦ Stat 全核\n"
        "• GPU: %s",
        g ? (std::string(g) + "\n    ◦ 输入上传一次\n    ◦ 中间量常驻显存\n    ◦ Stat 走常驻会话").c_str() : "off\n    ◦ 无 CUDA 设备或未编译: cmake -DFACTOR_CUDA=ON");
  }
  ImGui::SameLine();
  ImGui::SetNextItemWidth(70);
  if (ImGui::BeginCombo("Hold", ft.labels.empty() ? "-" : factor::stat::hold_name(cur_hold).c_str())) {
    for (size_t i = 0; i < ft.labels.size(); ++i)
      if (ImGui::Selectable(factor::stat::hold_name(ft.labels[i].hold).c_str(), static_cast<int>(i) == ui.hold_idx))
        ui.hold_idx = static_cast<int>(i);
    ImGui::EndCombo();
  }
  if (ImGui::IsItemHovered())
    tip("表格 Stat 列显示哪个持有期\n"
        "• 多权重因子的父行 = 该持有期下 score (每期真实 LS × rIC, 同 Optimize) 最高的权重\n"
        "• 不影响\n"
        "    ◦ Run: 一次算全部持有期\n"
        "    ◦ Optimize: 搜索窗 %s ~ T+%d 内每个持有期各搜一遍\n"
        "• 悬停 Stat 列格子看全部持有期",
        factor::stat::hold_name(factor::search::kHoldMinMinutes).c_str(), factor::search::kHoldMaxOpen);
  ImGui::SameLine();

  const bool can_run = !busy && !sbusy && ctx.axis_ready && !ft.labels.empty();
  if (!can_run)
    ImGui::BeginDisabled();
  if (ImGui::Button("Run", ImVec2(60, 0)))
    action = 1;
  if (!can_run)
    ImGui::EndDisabled();
  if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled))
    tip(ctx.axis_ready ? "评估全目录\n"
                         "• 流程: 扫描 → 读特征库 → 逐权重 eval → Stat\n"
                         "• 结果回写各因子文件 stats[i]\n"
                         "• 未搜索 (模板含自由参数而无 weights) 不评, 先 Optimize"
                       : "资产轴未就绪, 读不了特征库\n"
                         "• 先在 Database 页扫描");
  ImGui::SameLine();
  if (!busy)
    ImGui::BeginDisabled();
  if (ImGui::Button("Cancel", ImVec2(60, 0)))
    action = -1;
  if (!busy)
    ImGui::EndDisabled();
  ImGui::SameLine();
  if (busy)
    ImGui::BeginDisabled();
  if (ImGui::Button("Rescan", ImVec2(60, 0)))
    action = 2;
  if (busy)
    ImGui::EndDisabled();
  if (ImGui::IsItemHovered())
    tip("重扫目录: 只解析校验, 不算\n"
        "• 文件里的 stat 照常显示");

  ImGui::SameLine();
  ImGui::Text("Status:");
  ImGui::SameLine();
  const int done = static_cast<int>(svc.done.load(std::memory_order_relaxed)), total = static_cast<int>(svc.total.load(std::memory_order_relaxed)),
            broken = svc.broken();
  switch (st) {
  case Phase::Idle:
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Muted), "idle");
    break;
  case Phase::Scanning:
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Busy), "scanning");
    break;
  case Phase::Loading:
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Busy), "loading %d/%d days", done, total);
    break;
  case Phase::Running:
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Busy), "running %d/%d nodes", done, total);
    break;
  case Phase::Done:
    ImGui::TextColored(StatusColor(broken ? TaskStatus::Kind::Warn : TaskStatus::Kind::Ready), "done, %d BROKEN", broken);
    break;
  case Phase::Cancelled:
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "cancelled %d/%d", done, total);
    break;
  }

  // 短锁拷快照
  static std::vector<FactorRow> s_rows;
  std::string message;
  {
    std::lock_guard<std::mutex> lock(svc.mutex);
    s_rows = svc.rows;
    message = svc.message;
  }
  if (!message.empty()) {
    ImGui::SameLine();
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "| %s", message.c_str());
  }

  // ---- Optimize: 对高光行所在文件的模板搜全部自由参数, 搜索窗内每个持有期各搜 (不跨 h 比), 目标 = 该 h 的 毛 LS · 资金利用率 · rIC
  //      (Search/Contract.hpp【目标】【多权重】); 结果整体替换文件 weights, 随后 TaskFactors 自动 Run (看板从新 stat 算) ----
  {
    const FactorRow *view_row = nullptr;
    for (const FactorRow &r : s_rows)
      if (r.key == ui.view_file)
        view_row = &r;
    const bool tmpl_ok = view_row && view_row->error.empty() && !view_row->tmpl.empty() && view_row->tmpl != view_row->expr; // 模板有自由参数
    const bool can_opt = !busy && !sbusy && ctx.axis_ready && !ft.labels.empty() && tmpl_ok;
    ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "   Optimize:");
    ImGui::SameLine();
    if (!can_opt)
      ImGui::BeginDisabled();
    if (ImGui::Button("Optimize", ImVec2(70, 0))) {
      ui.opt_row = *view_row;
      action = 3;
    }
    if (!can_opt)
      ImGui::EndDisabled();
    if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled)) {
      if (!view_row)
        tip("先在表格里点一行高光 (与 Inspect 同一对象)");
      else if (!tmpl_ok)
        tip("%s 的模板没有自由参数\n"
            "• 做法: 勾选编辑 → 把要搜的参数勾成自由 (裸键名) → Save",
            view_row->file.c_str());
      else
        tip("搜 %s 模板里的自由参数 (裸键名 d / k / k2)\n"
            "• 范围: 搜索窗 %s ~ T+%d 内每个持有期各搜一遍\n"
            "• 目标 = 每期真实毛 LS × rIC\n"
            "    ◦ 只评排序能力: 不看成本, 不跨持有期比\n"
            "    ◦ 真实 LS: 一腿满档 cap 只 (池内平均有效票数 / 20), 不足按只数/cap 建仓, 超出均分, 不触发记 0\n"
            "    ◦ 触发率 / 利用率已在 LS 里, 稀疏触发的候选自然压低, 不设活跃度硬阈值\n"
            "    ◦ 自动定向: LS 与 rIC 同负 → 取反计 (权重带 sign = -1)\n"
            "• 搜索\n"
            "    ◦ 粗网格\n"
            "        ▪ 中心 = 有界槽中点 / 0\n"
            "        ▪ 步长按 ≤ %d 点的预算反推\n"
            "        ▪ 阈值类参数以其输入的 σ 为单位\n"
            "    ◦ 分支: 粗网格的每个局部峰各开一支 (不限支数)\n"
            "    ◦ 局部: 每支坐标模式搜索 (每轮每参数 ±步长), 有更优就移, 否则步长减半, 到取整分辨率停\n"
            "        ▪ 走到别支的点并入; 不升且低于高原线的剪掉\n"
            "• 终选\n"
            "    ◦ 高原集 = 各支最优里 score ≥ %.0f%%·max\n"
            "    ◦ 按两两截面相关 (抽 %d 行) 挑 ≤ %d 个持仓重合最少的 (不保证含 max)\n"
            "• 计算\n"
            "    ◦ 数据按天分块流式过, 块大小按内存 / 显存定\n"
            "    ◦ 窗内标签一次装全, 因子平面只算一遍\n"
            "    ◦ 候选合成共享 DAG, 公共前缀只算一次\n"
            "• 写回: 全部持有期的终选整体替换该文件 weights (旧的全丢), 并自动 Run 重算",
            view_row->file.c_str(), factor::stat::hold_name(factor::search::kHoldMinMinutes).c_str(), factor::search::kHoldMaxOpen,
            factor::search::grid::kLayerMax, 100.0 * (1.0 - factor::search::kPlateau), factor::search::kCorrRows,
            factor::search::kPicks);
    }
    ImGui::SameLine();
    if (!sbusy)
      ImGui::BeginDisabled();
    if (ImGui::Button("Cancel##opt", ImVec2(60, 0)))
      action = -3;
    if (!sbusy)
      ImGui::EndDisabled();
    ImGui::SameLine();
    ImGui::Text("状态:");
    ImGui::SameLine();
    static SearchResult s_sr; // 短锁拷快照 (每趟结束才变)
    {
      std::lock_guard<std::mutex> lock(ss.mutex);
      s_sr = ss.result;
    }
    const int sdone = static_cast<int>(ss.done.load(std::memory_order_relaxed)), stotal = static_cast<int>(ss.total.load(std::memory_order_relaxed)),
              nc = ss.chunks();
    const int n_cand = s_sr.holds.empty() ? 0 : static_cast<int>(s_sr.holds[0].cands.size()); // 每 h 列的是同一批候选
    const ImVec4 busy = StatusColor(TaskStatus::Kind::Busy);
    // 评估行: "<阶段>: N 候选 × C 块 (done/total)" (单块不写块数)
    const auto eval_line = [&](const std::string &phase) {
      const int cands = nc > 0 ? stotal / nc : 0;
      if (nc > 1)
        ImGui::TextColored(busy, "%s: %d 候选 × %d 块 (%d/%d)", phase.c_str(), cands, nc, sdone, stotal);
      else
        ImGui::TextColored(busy, "%s: %d 候选 (%d/%d)", phase.c_str(), cands, sdone, stotal);
    };
    switch (sst) {
    case Phase::Idle:
    case Phase::Scanning:
      ImGui::TextColored(StatusColor(TaskStatus::Kind::Muted), "空闲");
      break;
    case Phase::Loading: {
      const int ci = ss.chunk();
      if (ss.stage() == SearchStage::Scale && stotal > 0 && sdone >= stotal) {
        ImGui::TextColored(busy, "准备: 计算阈值尺度 σ");
      } else if (ci < static_cast<int>(s_sr.chunk_plan.size())) {
        const SearchChunk &c = s_sr.chunk_plan[static_cast<size_t>(ci)];
        const double got = stotal > 0 ? c.mb * sdone / stotal : 0.0;
        ImGui::TextColored(busy, "装载: %d/%d (每块 %d asset × %d days) (%.0f/%.0fMB)", ci + 1, nc, s_sr.n_assets, c.days, got, c.mb);
      } else {
        ImGui::TextColored(busy, "准备: 定分块");
      }
      break;
    }
    case Phase::Running:
      switch (ss.stage()) {
      case SearchStage::Scale:
      case SearchStage::Coarse:
        eval_line("粗网格");
        break;
      case SearchStage::Refine:
        eval_line("局部 第 " + std::to_string(ss.level()) + " 轮, 存活 " + std::to_string(ss.alive()) + " 支");
        break;
      case SearchStage::Corr:
        eval_line("截面相关");
        break;
      }
      break;
    case Phase::Done:
      if (s_sr.wrote)
        ImGui::TextColored(StatusColor(TaskStatus::Kind::Ready), "完成: 写入 %d 个权重 (%d 候选, %d 趟, %.0fs)", s_sr.n_weights, n_cand, s_sr.n_pass, s_sr.seconds);
      else
        ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "未写入: %s", s_sr.message.c_str());
      break;
    case Phase::Cancelled:
      ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "已取消");
      break;
    }
  }

  ImGui::Separator();

  // ==========================================================================
  // 2. 构建器: 根槽选算子 → 逐子槽往里选 (算子 / 特征), 参数手填 → 即时校验
  //    添加模式 (无选中行): Add 新文件 (名 / 规范串查重, 重了弹窗不加)
  //    编辑模式 (表格选中一行): Save 覆盖该文件 (弹窗确认) / 删除 (弹窗确认); 取消选定 → 回添加模式, 内容留作模板
  // ==========================================================================
  const FactorRow *edit_row = nullptr; // 勾选文件的首行 (模板 / 名字 / note 文件级, 任一行都一样); 重扫完没了 (外部删了) → 自动退出编辑模式 (忙时 rows 可能还是旧的, 不判)
  if (!ui.edit_file.empty()) {
    for (const FactorRow &r : s_rows)
      if (r.file == ui.edit_file && !edit_row)
        edit_row = &r;
    if (!edit_row && !busy)
      ui.edit_file.clear();
  }
  if (!ui.view_file.empty() && !busy) { // 高光行同理 (按 key: 权重没了也退)
    bool found = false;
    for (const FactorRow &r : s_rows)
      found = found || r.key == ui.view_file;
    if (!found)
      ui.view_file.clear();
  }
  if (edit_row)
    ImGui::TextColored(kEditColor, "2. Edit %s:", ui.edit_file.c_str());
  else
    ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "2. Add:");
  if (ImGui::IsItemHovered())
    tip("从外到内构建表达式\n"
        "• 根: 必须是归一算子\n"
        "    ◦ 截面口径: CsRank / CsNormRank / CsZ\n"
        "    ◦ 时序口径: TsRankRoll(d=1275, 5 日)\n"
        "• 子槽: 缩进里选算子或特征\n"
        "    ◦ 参数 d / k / k2 手填\n"
        "    ◦ 或勾前面的小框 = 自由参数 (表达式里写裸键名, 由 Optimize 搜)\n"
        "• 下拉\n"
        "    ◦ 可打字过滤: 算子名 / 中文名 / 特征 code\n"
        "    ◦ GROUP 域的组 id 槽只列整数列算子 + 特征\n"
        "• 改结构不动别处\n"
        "    ◦ 下拉换算子: 保留子槽 (叶自动进第一元)\n"
        "    ◦ 行末 ↑: 外面加一层\n"
        "    ◦ 行末 ✕: 去掉本层, 留一元\n"
        "• 模式\n"
        "    ◦ 编辑: 勾选表格首列\n"
        "        ▪ 模板载入到这里, Save 覆盖 / 删除\n"
        "        ▪ 改了模板, 该文件的 weights 全丢\n"
        "    ◦ 添加: 取消勾选, 内容留作模板");
  ImGui::SameLine();
  if (edit_row) {
    if (ImGui::SmallButton("取消勾选")) {
      ui.edit_file.clear();
      ui.add_msg.clear();
    }
    ImGui::SameLine();
  }
  if (ImGui::SmallButton("Clear")) {
    ui.build = BuildNode{};
    ui.name_buf[0] = '\0';
    ui.name_cn_buf[0] = '\0';
    ui.note_buf[0] = '\0';
    ui.add_msg.clear();
  }
  render_node(ui.build, ft, ui.filter_buf, sizeof(ui.filter_buf), "root", factor::Dom::REAL);

  std::string src;
  bool complete = true;
  to_source(ui.build, src, complete);
  ui.add_err.clear();
  std::string canon;
  factor::stat::Frame frame = factor::stat::Frame::CS;
  bool is_tmpl = false; // 有自由参数 (Add / Save 后是"未搜索", 要 Optimize)
  if (complete) {
    factor::expr::Expr e;
    if (factor::expr::parse(src, ft.lookup(), e, ui.add_err) && factor::expr::root_frame(e, frame, ui.add_err)) {
      canon = e.canon;
      is_tmpl = e.is_template();
    }
  }
  ImGui::AlignTextToFramePadding();
  ImGui::TextDisabled("expr:");
  ImGui::SameLine();
  if (ui.build.op < 0 && ui.build.feat.empty())
    ImGui::TextDisabled("(先选根槽)");
  else if (!complete)
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "%s   (有未选的槽)", src.c_str());
  else if (!ui.add_err.empty())
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Error), "%s   %s", src.c_str(), ui.add_err.c_str());
  else {
    ImGui::TextUnformatted(canon.c_str());
    ImGui::SameLine();
    ImGui::TextDisabled("[%s%s]", factor::stat::frame_name(frame), is_tmpl ? ", 模板" : "");
    if (ImGui::IsItemHovered())
      tip("%s%s", frame == factor::stat::Frame::CS ? "截面口径: 根是截面归一算子\n• 每 t 截面 rank → 20 组, 标签超额" : "时序口径: 根 TsRankRoll(5 日)\n• 自身分位直接分 20 组, 组可空, 标签超额",
          is_tmpl ? "\n• 模板: 含自由参数\n    ◦ 写入后是\"未搜索\"\n    ◦ 高光它点 Optimize 搜出 weights" : "");
  }

  // 两个名字: 非空且不合法 → 红字 (InputText 会改 buf, push/pop 必须用同一个判断结果)
  const auto name_input = [](const char *id, const char *hint, char *buf, size_t size, bool (*valid)(std::string_view), float width,
                             ImGuiInputTextFlags flags) {
    const bool red = buf[0] != '\0' && !valid(buf);
    if (red)
      ImGui::PushStyleColor(ImGuiCol_Text, StatusColor(TaskStatus::Kind::Error));
    ImGui::SetNextItemWidth(width);
    ImGui::InputTextWithHint(id, hint, buf, size, flags);
    if (red)
      ImGui::PopStyleColor();
  };
  name_input("##name", "name_en (必填, 文件名)", ui.name_buf, sizeof(ui.name_buf), ValidFactorName, 180, ImGuiInputTextFlags_CharsNoBlank);
  const bool name_ok = ValidFactorName(ui.name_buf);
  if (ImGui::IsItemHovered())
    tip("name_en = 文件名主干: <factor_dir>/<name_en>.json\n"
        "• 仅 [A-Za-z0-9_], ≤63 字符");
  ImGui::SameLine();
  name_input("##name_cn", "name_cn (必填)", ui.name_cn_buf, sizeof(ui.name_cn_buf), ValidFactorNameCn, 150, ImGuiInputTextFlags_None);
  const bool name_cn_ok = ValidFactorNameCn(ui.name_cn_buf);
  if (ImGui::IsItemHovered())
    tip("name_cn = 中文名, 落文件 name_cn 键\n"
        "• 纯汉字, 1..10 字");
  ImGui::SameLine();
  ImGui::SetNextItemWidth(std::max(200.0f, ImGui::GetContentRegionAvail().x - (edit_row ? 300.0f : 240.0f)));
  ImGui::InputTextWithHint("##note", "note (可选: 一句话说明, 落文件 note 键)", ui.note_buf, sizeof(ui.note_buf));
  ImGui::SameLine();
  const std::string new_file = std::string(ui.name_buf) + ".json";
  // 查重: 同名文件 / 同模板规范串的另一文件 (编辑模式下排除自己); 同一文件多行只报一次
  const auto conflict = [&](std::string &msg) {
    for (const FactorRow &r : s_rows) {
      if (edit_row && r.file == edit_row->file)
        continue;
      if (r.file == new_file) {
        msg = "已存在同名文件 " + r.file;
        return true;
      }
      if (!r.tmpl.empty() && r.tmpl == canon) {
        msg = "与 " + r.file + " 同一模板规范串:\n" + canon;
        return true;
      }
    }
    return false;
  };
  const bool can_write = !busy && complete && !canon.empty() && name_ok && name_cn_ok;
  if (!can_write)
    ImGui::BeginDisabled();
  if (edit_row) {
    if (ImGui::Button("Save", ImVec2(60, 0))) {
      std::string msg;
      if (conflict(msg)) {
        ui.popup = 3;
        ui.popup_msg = msg;
      } else {
        // 确认弹窗正文: 列出改动
        const bool expr_changed = edit_row->tmpl.empty() || edit_row->tmpl != canon;
        const bool name_changed = edit_row->file != new_file;
        const bool name_cn_changed = edit_row->name_cn != ui.name_cn_buf;
        const bool note_changed = edit_row->note != ui.note_buf;
        if (!expr_changed && !name_changed && !name_cn_changed && !note_changed) {
          ui.add_msg = "没有改动";
        } else {
          ui.popup_msg.clear();
          if (expr_changed)
            ui.popup_msg += "expr: " + (!edit_row->tmpl.empty() ? edit_row->tmpl : edit_row->expr_raw) + "\n   → " + canon +
                            (edit_row->widx >= 0 ? "\n   (文件里的 weights / stat 会被丢掉)" : "") + "\n";
          if (name_changed)
            ui.popup_msg += "name_en: " + edit_row->name_en + " → " + ui.name_buf + "\n";
          if (name_cn_changed)
            ui.popup_msg += "name_cn: " + edit_row->name_cn + " → " + ui.name_cn_buf + "\n";
          if (note_changed)
            ui.popup_msg += "note: " + edit_row->note + "\n   → " + ui.note_buf + "\n";
          ui.popup = 1;
        }
      }
    }
  } else if (ImGui::Button("Add", ImVec2(60, 0))) {
    std::string msg;
    if (conflict(msg)) {
      ui.popup = 3;
      ui.popup_msg = msg;
    } else {
      std::string err;
      const std::string file = AddFactorFile(ctx.factor_dir, ft, ui.name_buf, ui.name_cn_buf, canon, ui.note_buf, err);
      if (file.empty()) { // 查重已过仍失败 = 目录被外部改了 (agent 加文件), 重扫即可
        ui.popup = 3;
        ui.popup_msg = err + " (目录有外部改动, 已重扫)";
        action = 2;
      } else {
        ui.add_msg = "已加 " + file + "  " + canon;
        ui.build = BuildNode{};
        ui.name_buf[0] = '\0';
        ui.name_cn_buf[0] = '\0';
        ui.note_buf[0] = '\0';
        action = 2;
      }
    }
  }
  if (!can_write)
    ImGui::EndDisabled();
  if (edit_row) {
    ImGui::SameLine();
    if (busy)
      ImGui::BeginDisabled();
    if (ImGui::Button("删除", ImVec2(60, 0)))
      ui.popup = 2;
    if (busy)
      ImGui::EndDisabled();
  }
  if (!ui.add_msg.empty()) {
    ImGui::SameLine();
    ImGui::TextDisabled("%s", ui.add_msg.c_str());
  }

  ImGui::Separator();

  // ==========================================================================
  // 3. 表
  // ==========================================================================
  // 目标分 (不显示, 挑代表权重): 每行按选定持有期 (Hold) 从 stat 现算 (RowScore, 与 Optimize 同一份目标); 无 stat / 无该持有期 / n < 3 → hold = 0
  static std::vector<OptCell> s_opt;
  s_opt.assign(s_rows.size(), OptCell{});
  if (cur_hold > 0)
    for (size_t i = 0; i < s_rows.size(); ++i) {
      const factor::search::Score sc = RowScore(s_rows[i], cur_hold);
      if (!sc.ok)
        continue;
      OptCell &oc = s_opt[i];
      oc.hold = cur_hold;
      oc.score = sc.score;
    }
  // 按文件分组 (rows 同文件连续): 代表行 = 当前 h 下 score 最高的有效权重 (无分 → 首个有效 → 首行); 有权重的文件可展开 (文件级行 widx −1 不可)
  struct Group {
    std::vector<int> kids; // s_rows 下标, 文件内按 widx
    int rep = -1;
  };
  std::vector<Group> groups;
  for (int i = 0; i < static_cast<int>(s_rows.size()); ++i) {
    if (groups.empty() || s_rows[static_cast<size_t>(groups.back().kids.front())].file != s_rows[static_cast<size_t>(i)].file)
      groups.emplace_back();
    groups.back().kids.push_back(i);
  }
  for (Group &g : groups) {
    int first_ok = -1;
    for (int i : g.kids) {
      const FactorRow &r = s_rows[static_cast<size_t>(i)];
      if (!r.error.empty() || r.expr.empty())
        continue;
      if (first_ok < 0)
        first_ok = i;
      const OptCell &oc = s_opt[static_cast<size_t>(i)];
      if (oc.hold > 0 && std::isfinite(oc.score) && (g.rep < 0 || oc.score > s_opt[static_cast<size_t>(g.rep)].score))
        g.rep = i;
    }
    if (g.rep < 0)
      g.rep = first_ok >= 0 ? first_ok : g.kids.front();
  }
  int n_weights = 0;
  for (const FactorRow &r : s_rows)
    n_weights += r.widx >= 0;
  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "3. Factors:");
  ImGui::SameLine();
  ImGui::Text("%d 因子 / %d 权重 (%d BROKEN)", static_cast<int>(groups.size()), n_weights, broken);
  if (ImGui::IsItemHovered())
    tip("因子表: 一行一权重\n"
        "• 有权重的因子折成一行\n"
        "    ◦ 折叠行 = Hold 下 score 最高的权重\n"
        "    ◦ 名字前 ▸ 展开看全部权重, 子行标搜索时的持有期\n"
        "• 父行 / 未搜索行\n"
        "    ◦ 勾选: 编辑模板\n"
        "    ◦ 点行: 高光 → Optimize / Inspect\n"
        "• 子行\n"
        "    ◦ 点行: 高光 → Inspect\n"
        "    ◦ 悬停 Stat 列: 看该权重全部持有期\n"
        "• 排序: 按父行 (代表权重) 的值; 展开的子行在父行下按同一键排");
  ImGui::SameLine();
  ImGui::TextDisabled("Stat 列 = h %s (毛价格收益)", factor::stat::hold_name(cur_hold).c_str());

  {
    const uint64_t ep = svc.epoch.load(std::memory_order_acquire);
    const double now = ImGui::GetTime();
    if (ui.fit_epoch != ep && (now - ui.fit_last_time >= 0.5 || !busy)) {
      ui.fit_epoch = ep;
      ui.fit_last_time = now;
      ui.fit_frames = 2;
    }
  }

  ImGui::PushStyleVar(ImGuiStyleVar_CellPadding, ImVec2(4.0f, 2.0f));
  if (ImGui::BeginTable("FactorTable", kNumCols,
                        ImGuiTableFlags_SizingFixedFit | ImGuiTableFlags_Borders | ImGuiTableFlags_RowBg | ImGuiTableFlags_ScrollY |
                            ImGuiTableFlags_ScrollX | ImGuiTableFlags_Resizable | ImGuiTableFlags_Sortable | ImGuiTableFlags_SortTristate |
                            ImGuiTableFlags_NoSavedSettings,
                        ImVec2(0, 0))) {
    for (int c = 0; c < kNumCols; ++c)
      ImGui::TableSetupColumn(col_name(c), ImGuiTableColumnFlags_WidthFixed | (col_sortable(c) ? 0 : ImGuiTableColumnFlags_NoSort));
    ImGui::TableSetupScrollFreeze(0, 1);
    if (ui.fit_frames > 0) {
      ui.fit_frames--;
      ImGuiTable *table = ImGui::GetCurrentTable();
      assert(table);
      ImGui::TableSetColumnWidthAutoAll(table);
    }
    ImGui::TableNextRow(ImGuiTableRowFlags_Headers);
    for (int c = 0; c < kNumCols; ++c) {
      ImGui::TableSetColumnIndex(c);
      if (const Metric *m = col_metric(c); m && group_tip(m->group))
        ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, group_bg(m->group));
      ImGui::TableHeader(col_name(c));
      if (ImGui::IsItemHovered())
        tip("%s", col_tip(c).c_str());
    }

    if (ImGuiTableSortSpecs *specs = ImGui::TableGetSortSpecs(); specs && specs->SpecsDirty) {
      if (specs->SpecsCount > 0) {
        ui.sort_column = specs->Specs[0].ColumnIndex;
        ui.sort_ascending = specs->Specs[0].SortDirection == ImGuiSortDirection_Ascending;
      } else {
        ui.sort_column = -1;
      }
      specs->SpecsDirty = false;
    }
    // 显示序: 组按代表行排, 展开组的子行紧随其后按同一键排
    std::vector<int> gorder(groups.size());
    std::iota(gorder.begin(), gorder.end(), 0);
    const auto expandable = [&](const Group &g) { return s_rows[static_cast<size_t>(g.kids.front())].widx >= 0; };
    const auto is_expanded = [&](const Group &g) {
      return expandable(g) && std::find(ui.expanded.begin(), ui.expanded.end(), s_rows[static_cast<size_t>(g.kids.front())].file) != ui.expanded.end();
    };
    std::vector<std::vector<int>> korder(groups.size());
    for (size_t gi = 0; gi < groups.size(); ++gi)
      if (is_expanded(groups[gi]))
        korder[gi] = groups[gi].kids;
    if (ui.sort_column >= 0) {
      const int sc = ui.sort_column;
      // 文本列按串比; 其余按数值键 (无值 → -1 / -1e300, 升序排最前)
      const int sf = kColLayout[static_cast<size_t>(sc)].fixed; // 固定列号 (Stat 列 = -1)
      const auto text = [&](const FactorRow &r) -> const std::string * {
        if (sf == NameEn)
          return &r.name_en;
        if (sf == NameCn)
          return &r.name_cn;
        return nullptr;
      };
      const auto key = [&](const FactorRow &r) -> double {
        if (const Metric *m = col_metric(sc))
          return metric_key(hold_of(r, cur_hold), *m, r);
        switch (sf) {
        case Type:
          return static_cast<double>(r.kind);
        case HoldSrc:
          return r.widx >= 0 ? r.hold_src : -1.0;
        case Frame:
          return r.error.empty() ? static_cast<double>(r.frame) : -1.0;
        case Time:
          return r.has_stat ? r.eval_ms : -1.0;
        case Ops:
          return r.n_ops;
        case Feats:
          return r.n_feats;
        case Slots:
          return r.n_slots;
        default:
          return 0.0;
        }
      };
      const auto less = [&](int a, int b) {
        const FactorRow &ra = s_rows[static_cast<size_t>(a)], &rb = s_rows[static_cast<size_t>(b)];
        int cmp = 0;
        if (sf == Edit)
          cmp = a - b;
        else if (const std::string *ta = text(ra))
          cmp = ta->compare(*text(rb));
        else
          cmp = (key(ra) > key(rb)) - (key(ra) < key(rb));
        return ui.sort_ascending ? cmp < 0 : cmp > 0;
      };
      std::stable_sort(gorder.begin(), gorder.end(), [&](int a, int b) { return less(groups[static_cast<size_t>(a)].rep, groups[static_cast<size_t>(b)].rep); });
      for (std::vector<int> &ks : korder)
        std::stable_sort(ks.begin(), ks.end(), less);
    }
    struct Shown {
      int idx; // s_rows 下标
      RowKind kind;
      int n_kids; // 所在文件的权重数
    };
    std::vector<Shown> order;
    for (int gi : gorder) {
      const Group &g = groups[static_cast<size_t>(gi)];
      const int nk = static_cast<int>(g.kids.size());
      order.push_back({g.rep, expandable(g) ? RowKind::Parent : RowKind::Flat, nk});
      for (int k : korder[static_cast<size_t>(gi)])
        order.push_back({k, RowKind::Child, nk});
    }

    for (size_t oi = 0; oi < order.size(); ++oi) {
      const auto [idx, kind, n_kids] = order[oi];
      const FactorRow &r = s_rows[static_cast<size_t>(idx)];
      const HS *h = hold_of(r, cur_hold);
      const bool editing = kind != RowKind::Child && ui.edit_file == r.file;
      const bool viewing = ui.view_file == r.key;
      // 折叠的父行: 高光的权重藏在里面 → 父行也亮 (点它 = 换成高光代表权重)
      bool group_viewing = viewing;
      if (kind == RowKind::Parent && !viewing && !ui.view_file.empty())
        for (const FactorRow &k : s_rows)
          group_viewing = group_viewing || (k.file == r.file && k.key == ui.view_file);
      ImGui::TableNextRow();
      ImGui::TableSetColumnIndex(col_of(Edit));
      ImGui::PushID(static_cast<int>(oi));
      // 整行可点 (先提交, AllowOverlap 让后面的勾选框盖在上面): 点行高光 → Inspect 页的对象; 再点 → 取消
      const ImVec2 cell_pos = ImGui::GetCursorPos();
      if (ImGui::Selectable("##row", group_viewing, ImGuiSelectableFlags_SpanAllColumns | ImGuiSelectableFlags_AllowOverlap))
        ui.view_file = viewing ? std::string{} : r.key;
      if (ImGui::IsItemHovered())
        row_tooltip(r, ctx);
      if (kind != RowKind::Child) {
        // 勾选框 = 编辑模式 (同时只勾一个文件): 勾 → 模板载入构建器; 取消勾 → 回添加模式 (内容留作模板); 勾另一行 → 切换
        // 无 FramePadding → 方框 = 字高, 行高不变; 回到格起点画 (Selectable 已占满整格宽)
        ImGui::SetCursorPos(cell_pos);
        bool check = editing;
        ImGui::PushStyleVar(ImGuiStyleVar_FramePadding, ImVec2(0.0f, 0.0f));
        if (ImGui::Checkbox("##edit", &check)) {
          if (!check) {
            ui.edit_file.clear();
          } else {
            ui.edit_file = r.file;
            ui.build = BuildNode{};
            if (!r.tmpl.empty()) { // 模板规范串来自 scan 的 parse, 必可再解析 (根非归一的 BROKEN 行也有, 载入后套个根即可); 其他 BROKEN 行留空
              factor::expr::Expr e;
              std::string err;
              const bool ok = factor::expr::parse(r.tmpl, ft.lookup(), e, err);
              assert(ok);
              (void)ok;
              from_expr(e, 0, ui.build);
            }
            std::snprintf(ui.name_buf, sizeof(ui.name_buf), "%s", r.name_en.c_str());
            std::snprintf(ui.name_cn_buf, sizeof(ui.name_cn_buf), "%s", r.name_cn.c_str());
            std::snprintf(ui.note_buf, sizeof(ui.note_buf), "%s", r.note.c_str());
          }
          ui.add_msg.clear();
        }
        ImGui::PopStyleVar();
      }
      for (int c = 1; c < kNumCols; ++c) { // 0 = edit 列 (上面画过)
        ImGui::TableSetColumnIndex(c);
        const ColRef &cr = kColLayout[static_cast<size_t>(c)];
        if (cr.fixed >= 0) {
          if (cr.fixed == NameEn && kind == RowKind::Parent) { // 展开 / 折叠箭头 (多权重文件)
            const bool open = std::find(ui.expanded.begin(), ui.expanded.end(), r.file) != ui.expanded.end();
            ImGui::PushStyleVar(ImGuiStyleVar_FramePadding, ImVec2(0.0f, 0.0f));
            if (ImGui::ArrowButton("##exp", open ? ImGuiDir_Down : ImGuiDir_Right)) {
              if (open)
                ui.expanded.erase(std::find(ui.expanded.begin(), ui.expanded.end(), r.file));
              else
                ui.expanded.push_back(r.file);
            }
            ImGui::PopStyleVar();
            ImGui::SameLine(0, 2);
          }
          fixed_cell(static_cast<Col>(cr.fixed), r, ctx, editing, kind, cur_hold);
        } else {
          const Metric &m = *col_metric(c);
          if (group_tip(m.group))
            ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, group_bg(m.group));
          const std::optional<ImVec4> col = grade_color(m, h, r);
          metric_cell(h, m, r, col ? &*col : nullptr);
        }
      }
      ImGui::PopID();
    }
    ImGui::EndTable();
  }
  ImGui::PopStyleVar();

  // ==========================================================================
  // 弹窗: 1 Save 确认 / 2 删除确认 / 3 不能加. 成功 → 重扫
  // ==========================================================================
  if (ui.popup == 1)
    ImGui::OpenPopup("保存改动");
  else if (ui.popup == 2)
    ImGui::OpenPopup("删除因子");
  else if (ui.popup == 3)
    ImGui::OpenPopup("不能添加");
  ui.popup = 0;

  if (ImGui::BeginPopupModal("保存改动", nullptr, ImGuiWindowFlags_AlwaysAutoResize)) {
    ImGui::Text("覆盖 %s/%s:", ctx.factor_dir.c_str(), ui.edit_file.c_str());
    ImGui::TextUnformatted(ui.popup_msg.c_str());
    if (ImGui::Button("保存", ImVec2(80, 0))) {
      std::string err;
      const std::string file = UpdateFactorFile(ctx.factor_dir, ft, ui.edit_file, ui.name_buf, ui.name_cn_buf, canon, ui.note_buf, err);
      if (file.empty()) { // 查重已过仍失败 = 目录被外部改了, 重扫即可
        ui.add_msg = "失败: " + err;
      } else {
        ui.add_msg = "已保存 " + file;
        ui.edit_file = file; // 改名后继续编辑新文件
      }
      action = 2;
      ImGui::CloseCurrentPopup();
    }
    ImGui::SameLine();
    if (ImGui::Button("取消", ImVec2(80, 0)))
      ImGui::CloseCurrentPopup();
    ImGui::EndPopup();
  }

  if (ImGui::BeginPopupModal("删除因子", nullptr, ImGuiWindowFlags_AlwaysAutoResize)) {
    ImGui::Text("删除 %s/%s ?", ctx.factor_dir.c_str(), ui.edit_file.c_str());
    ImGui::TextDisabled("文件直接删, 不可恢复; 构建器内容保留 (可当模板再 Add)");
    if (ImGui::Button("删除", ImVec2(80, 0))) {
      DeleteFactorFile(ctx.factor_dir, ui.edit_file);
      ui.add_msg = "已删 " + ui.edit_file;
      ui.edit_file.clear();
      action = 2;
      ImGui::CloseCurrentPopup();
    }
    ImGui::SameLine();
    if (ImGui::Button("取消", ImVec2(80, 0)))
      ImGui::CloseCurrentPopup();
    ImGui::EndPopup();
  }

  if (ImGui::BeginPopupModal("不能添加", nullptr, ImGuiWindowFlags_AlwaysAutoResize)) {
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "%s", ui.popup_msg.c_str());
    if (ImGui::Button("OK", ImVec2(80, 0)))
      ImGui::CloseCurrentPopup();
    ImGui::EndPopup();
  }
  return action;
}

} // namespace GUI::Factors
