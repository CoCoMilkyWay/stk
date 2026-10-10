// Tab Factors — 见头文件
#include "gui/task_factors/ui/TabFactors.hpp"
#include "factor/Check.hpp"  // default_d / set_k: 构建器选中算子时的参数默认值 (与 Operators 页同一张表)
#include "factor/GpuRun.hpp" // available / device_name
#include "gui/Tasks.hpp"     // StatusColor

#include "imgui.h"
#include "imgui_internal.h" // TableSetColumnWidthAutoAll

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
// 构建器 (从外到内): 每个槽一个下拉 (过滤框 + 算子段 + 特征段), 选了算子就在下面缩进画它的子槽 + 参数
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

// 选中算子: 子槽按元数清空重建, 参数取默认表
void set_op(BuildNode &n, int op) {
  n.op = op;
  n.feat.clear();
  n.args.assign(static_cast<size_t>(factor::expr::kOps[op].arity), BuildNode{});
  const std::string name = factor::expr::kOps[op].name;
  n.p = factor::Param{};
  n.p.d = factor::check::default_d(name);
  factor::check::set_k(name, n.p);
}

void set_feat(BuildNode &n, const std::string &code) {
  n.op = -1;
  n.feat = code;
  n.args.clear();
}

// 树 → 表达式源串 (与规范串同格式); 有未选槽 → complete = false (源串里留 "?")
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
    s += buf;
  }
  if (factor::expr::declares(o.params, "k")) {
    sep();
    std::snprintf(buf, sizeof(buf), "k=%g", static_cast<double>(n.p.k));
    s += buf;
  }
  if (factor::expr::declares(o.params, "k2")) {
    sep();
    std::snprintf(buf, sizeof(buf), "k2=%g", static_cast<double>(n.p.k2));
    s += buf;
  }
  s += ')';
}

// Expr 树 → 构建器 (表格行点规范串载入)
void from_expr(const factor::expr::Expr &e, int i, BuildNode &n) {
  const factor::expr::Node &x = e.nodes[static_cast<size_t>(i)];
  if (x.op < 0) {
    set_feat(n, x.feat);
    return;
  }
  set_op(n, x.op);
  n.p = x.p;
  for (size_t a = 0; a < n.args.size(); ++a)
    from_expr(e, x.args[a], n.args[a]);
}

// 一个槽的下拉: need = 父算子该元的 in 域 (根 = REAL); 严格域 (组 id 的 INT) 只列 out ⊆ need 的算子 + 特征叶 (与 parse 同规则)
void slot_combo(BuildNode &n, const FeatureTable &ft, char *filter, size_t filter_cap, factor::Dom need) {
  const char *preview = n.op >= 0 ? factor::expr::kOps[n.op].name : (n.feat.empty() ? "<选择>" : n.feat.c_str());
  ImGui::SetNextItemWidth(220);
  if (!ImGui::BeginCombo("##slot", preview, ImGuiComboFlags_HeightLargest))
    return;
  if (ImGui::IsWindowAppearing()) {
    filter[0] = '\0';
    ImGui::SetKeyboardFocusHere();
  }
  ImGui::SetNextItemWidth(-1);
  ImGui::InputTextWithHint("##filter", "过滤: 算子名 / 中文名 / 特征 code", filter, filter_cap);
  const std::string_view f(filter);
  ImGui::Separator();
  if (ImGui::Selectable("<清空>", false))
    n = BuildNode{};
  ImGui::TextDisabled("算子");
  for (int i = 0; i < factor::expr::kOpCount; ++i) {
    const factor::expr::OpInfo &o = factor::expr::kOps[i];
    if (factor::dom_strict(need) && !factor::dom_sub(o.out, need))
      continue;
    if (!contains_ci(o.name, f) && !contains_ci(o.c_name, f))
      continue;
    char label[96];
    std::snprintf(label, sizeof(label), "%s  %s", o.name, o.c_name);
    if (ImGui::Selectable(label, n.op == i))
      set_op(n, i);
    if (ImGui::IsItemHovered())
      tip("%s\n• %d 元\n• 参数 %s", o.note, o.arity, o.params[0] ? o.params : "无");
  }
  ImGui::TextDisabled("特征");
  for (const FeatCol &c : ft.cols) {
    if (!c.allowed || !contains_ci(c.code, f))
      continue;
    if (ImGui::Selectable(c.code.c_str(), n.op < 0 && n.feat == c.code))
      set_feat(n, c.code);
  }
  ImGui::EndCombo();
}

// 递归画一个节点: [槽名] [下拉] [参数...]; 算子的子槽缩进一层
void render_node(BuildNode &n, const FeatureTable &ft, char *filter, size_t filter_cap, const char *slot_label, factor::Dom need) {
  ImGui::PushID(&n);
  ImGui::AlignTextToFramePadding();
  ImGui::TextDisabled("%s", slot_label);
  if (need != factor::Dom::REAL && ImGui::IsItemHovered())
    tip("该元值域 %s%s", factor::dom_name(need), factor::dom_strict(need) ? " (严格: 组 id)" : " (语义声明, 越界格算子自置无效)");
  ImGui::SameLine();
  slot_combo(n, ft, filter, filter_cap, need);
  if (n.op >= 0) {
    const factor::expr::OpInfo &o = factor::expr::kOps[n.op];
    if (factor::expr::declares(o.params, "d")) {
      ImGui::SameLine();
      ImGui::SetNextItemWidth(70);
      ImGui::InputInt("d", &n.p.d, 0);
      if (ImGui::IsItemHovered())
        tip("窗长 / 滞后 (分钟), 1..%d", factor::expr::kMaxD);
    }
    if (factor::expr::declares(o.params, "k")) {
      ImGui::SameLine();
      ImGui::SetNextItemWidth(70);
      ImGui::InputFloat("k", &n.p.k, 0.f, 0.f, "%g");
    }
    if (factor::expr::declares(o.params, "k2")) {
      ImGui::SameLine();
      ImGui::SetNextItemWidth(70);
      ImGui::InputFloat("k2", &n.p.k2, 0.f, 0.f, "%g");
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
//   group  分组, 按重要性排: 形态 (单调 / 换手) → LS (能不能赚) → IC (整体相关, 参考) → 样本; 悬停表按组出标题行, 主表列同序
//   col    非空 = 进主表 (列名), 空 = 只在悬停表
//   desc / range / best  说明 / 取值范围 / 最优 (悬停表三列, 主表列提示也由它们拼); 文案压短, 悬停表宽度由它们定
//   get    HoldStat → double (整数计数也走这里, fmt 用 %.0f)
//   cmp    跨持有期可比性 (渐变着色): None 不比; High 越大越好 (因子方向按根算子约定为正); Low 越小越好; AbsLow 越近 0 越好.
//          不比的: n / n_ac (由持有期机制决定), std (尺度随 h 变), skew / kurt (形态诊断). rAC 的 lag = h 随 h 变, 跨期比用 turn (年化换手)
//   公式见 factor/Stat/Contract.hpp【二级 HoldStat】(t 值按 n/h 折算重叠持有期; Sharpe 以持有期为一期年化)
// ============================================================================
using HS = factor::stat::HoldStat;
enum class Group : uint8_t { Shape,
                             LS,
                             IC,
                             Sample,
                             kCount };
struct GroupInfo {
  const char *name, *desc;
};
constexpr GroupInfo kGroupInfo[static_cast<size_t>(Group::kCount)] = {
    {"形态", "分层单调 / 换手"},
    {"LS", "顶组多 + 底组空 的超额"},
    {"IC", "截面秩相关, 只作参考"},
    {"样本", "有效 t 行数"},
};
enum class Cmp : uint8_t { None,
                           High,
                           Low,
                           AbsLow };
struct Metric {
  Group group;
  const char *name; // 悬停表行名 (组内短名)
  const char *col;  // 主表列名; nullptr = 不进主表
  const char *desc, *range, *best;
  const char *fmt;
  double (*get)(const HS &);
  Cmp cmp;
};
constexpr Metric kMetrics[] = {
    {Group::Shape, "mono", "mono", "组均值 对组号 Spearman", "[-1, 1]", "越大, 1 严格单调", "%+.3f", [](const HS &h) { return static_cast<double>(h.mono); },
     Cmp::High},
    {Group::Shape, "rAC", "rAC", "因子秩 lag=h 自相关", "[-1, 1]", "越大", "%+.3f", [](const HS &h) { return static_cast<double>(h.rank_ac); },
     Cmp::High},
    {Group::Shape, "turn", "turn", "年化换手 (倍/年)", "[0, ∞)", "越小", "%.0f", [](const HS &h) { return factor::stat::turnover_annual(h); }, Cmp::Low},
    {Group::LS, "mean", "LS", "多空超额 均值", "(-∞, ∞)", "越大", "%+.5f", [](const HS &h) { return static_cast<double>(h.ls_mean); }, Cmp::High},
    {Group::LS, "t", nullptr, "多空超额 t 值", "(-∞, ∞)", "越大, |t|>2 显著", "%+.2f", [](const HS &h) { return static_cast<double>(h.ls_t); }, Cmp::High},
    {Group::LS, "pos", nullptr, "多空超额 > 0 占比", "[0, 1]", "越大, 0.5 无信号", "%.2f", [](const HS &h) { return static_cast<double>(h.ls_pos); },
     Cmp::High},
    {Group::LS, "SR", "SR", "多空超额 年化 Sharpe", "(-∞, ∞)", "越大", "%+.2f", [](const HS &h) { return static_cast<double>(h.sharpe); }, Cmp::High},
    {Group::LS, "β", nullptr, "多空超额 对市场 β", "(-∞, ∞)", "越近 0", "%+.3f", [](const HS &h) { return static_cast<double>(h.beta); }, Cmp::AbsLow},
    {Group::IC, "mean", "IC", "rank IC 均值", "[-1, 1]", "越大", "%+.4f", [](const HS &h) { return static_cast<double>(h.ic_mean); }, Cmp::High},
    {Group::IC, "std", nullptr, "rank IC 标准差", "[0, 1]", "越小", "%.4f", [](const HS &h) { return static_cast<double>(h.ic_std); }, Cmp::None},
    {Group::IC, "IR", "ICIR", "rank IC 均值 / 标准差", "(-∞, ∞)", "越大", "%+.3f", [](const HS &h) { return static_cast<double>(h.icir); }, Cmp::High},
    {Group::IC, "t", nullptr, "rank IC t 值", "(-∞, ∞)", "越大, |t|>2 显著", "%+.2f", [](const HS &h) { return static_cast<double>(h.ic_t); }, Cmp::High},
    {Group::IC, "pos", nullptr, "rank IC > 0 占比", "[0, 1]", "越大, 0.5 无信号", "%.2f", [](const HS &h) { return static_cast<double>(h.ic_pos); },
     Cmp::High},
    {Group::IC, "skew", nullptr, "rank IC 偏度", "(-∞, ∞)", "≈ 0", "%+.2f", [](const HS &h) { return static_cast<double>(h.ic_skew); }, Cmp::None},
    {Group::IC, "kurt", nullptr, "rank IC 超额峰度", "[-2, ∞)", "≈ 0", "%+.2f", [](const HS &h) { return static_cast<double>(h.ic_kurt); }, Cmp::None},
    {Group::Sample, "n", nullptr, "标签 ok 行数", "[0, T]", "越大, <3 其余空", "%.0f", [](const HS &h) { return static_cast<double>(h.n); }, Cmp::None},
    {Group::Sample, "n_ac", nullptr, "rAC 可算行数", "[0, T]", "越大", "%.0f", [](const HS &h) { return static_cast<double>(h.n_ac); }, Cmp::None},
};
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

// 一格: 无值 → "-"; n < 3 时统计量为空 (样本组照显); 否则按 fmt 打印, 可带色
void metric_cell(const HS *h, const Metric &m, const ImVec4 *color = nullptr) {
  if (!h || (h->n < 3 && m.group != Group::Sample))
    ImGui::TextDisabled("-");
  else if (color)
    ImGui::TextColored(*color, m.fmt, m.get(*h));
  else
    ImGui::Text(m.fmt, m.get(*h));
}
// 排序键: 无值的行排最前 (升序) / 最后 (降序)
double metric_key(const HS *h, const Metric &m) { return h ? m.get(*h) : -1e300; }

// ============================================================================
// 主表列模型: 固定列 (kFixedCols, 按 Col 枚举寻址) 与 Stat 列 (kMetrics 里 col 非空的, 同序) 穿插, 显示顺序 = kColLayout:
//   edit type name_en name_cn frame status time valid | mono rAC turn LS SR IC ICIR | expr ops feats slots note
// ============================================================================
enum Col : int { Edit,
                 Type,
                 NameEn,
                 NameCn,
                 Frame,
                 Status,
                 Time,
                 Valid,
                 Expr,
                 Ops,
                 Feats,
                 Slots,
                 Note,
                 kNumFixed };
struct FixedCol {
  const char *name, *tip;
  bool sortable;
};
constexpr FixedCol kFixedCols[kNumFixed] = {
    {"edit",
     "勾选 → 编辑模式: 载入上方构建器, 可改 / 删\n"
     "• 同时只勾一个\n"
     "• 排序 = 文件名序 (默认)",
     true},
    {"type",
     "因子类型 (文件 type 键)\n"
     "• alpha: 预测超额收益, 再分 CS / TS 口径 (见 frame 列)\n"
     "• beta: 风险暴露 (未实现, 留位)",
     true},
    {"name_en",
     "英文名 = 文件名主干: <factor_dir>/<universe>/<name_en>.json\n"
     "• 仅 [A-Za-z0-9_], ≤63 字符\n"
     "• 点行高光 → Inspect 页单因子展示",
     true},
    {"name_cn",
     "中文名 (文件 name_cn 键)\n"
     "• 纯汉字, 1..10 字\n"
     "• 缺 / 不合 → BROKEN",
     true},
    {"frame",
     "口径, 由根算子定; 两口径标签都取超额 (减截面均值), 组均值沿 t 池化\n"
     "• CS = 截面: 根 CsRank / CsNormRank / CsZ; 每 t 截面 rank 分 20 组, 任一组空该行无效, 多空对冲\n"
     "• TS = 时序: 根 TsRankRoll 5 日; 自身分位直接分 20 组, 组可空 → long / flat",
     true},
    {"status",
     "评估状态\n"
     "• BROKEN 红: 文件 / type / name_cn / 表达式 / 根非归一 / params / 组 id 不合 (悬停看原因; 文件不动, 手动处理)\n"
     "• ok 绿: 本轮算的\n"
     "• file 灰: stat 来自文件, 作用域一致\n"
     "• file≠scope 黄: 文件 stat 是别的 universe / 区间算的\n"
     "• no stat: 从未评估\n"
     "• dup 黄: 与另一文件同一规范串",
     false},
    {"time",
     "算一遍 DAG 的耗时 (ms)\n"
     "• = 子树全部算子节点之和, 共享节点算给每个用它的因子\n"
     "• CPU 全核 wall / GPU 纯 kernel",
     true},
    {"expr",
     "规范串 = 解析后重新序列化\n"
     "• 算子 PascalCase, 参数 d / k / k2 显式, 含 params 覆盖后的当前值\n"
     "• 解析不过的行显示文件原串",
     false},
    {"ops", "算子节点数 (= params 数组长度)", true},
    {"feats", "去重特征数 (输入平面数)", true},
    {"slots",
     "DAG 缓冲槽数 = 峰值同时存活的中间量\n"
     "• 内存 / 显存 = slots × T·A × 5B",
     true},
    {"valid%", "根平面有效格占比", true},
    {"note", "文件 note 键: 人 / agent 写的一句话", false},
};
constexpr ImVec4 kEditColor(1.0f, 0.75f, 0.3f, 1.0f); // 编辑中的行 / 标题

// 显示顺序: Stat 列夹在 Valid 与 Expr 之间. 一列 = 固定列 (fixed ≥ 0) 或 Stat 列 (stat ≥ 0, 主表第 stat 个 Stat 列 → kMetrics[kStatColMetric[stat]])
struct ColRef {
  int fixed = -1, stat = -1;
};
constexpr int kStatInsertAfter = Valid;
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
                                  "\n• 组    " + g.desc + "\n• 显示  选定持有期的值, n < 3 为空; 着色 = 本列跨因子渐变 (红最差 → 绿最好); 悬停行看全部持有期";
    }
    return a;
  }();
  return tips[static_cast<size_t>(c)];
}

constexpr int kTipFixedCols = 4; // 悬停表前置列: 指标 / 说明 / 范围 / 最优

// 渐变着色 (悬停表跨持有期 / 主表列跨因子 共用一套): 可比指标按 cmp 方向统一成 "越大越好" 的分值, 在一组样本的 [lo, hi] 内线性映射到 [0, 1]
// (0 = 最差, 1 = 最好), 红 → 文字色 → 绿 三段插值; 不可比 / 样本 < 2 / 全等 → 不着色 (nullopt)
double metric_score(const Metric &m, const HS &h) {
  const double v = m.get(h);
  switch (m.cmp) {
  case Cmp::High:
    return v;
  case Cmp::Low:
    return -v;
  case Cmp::AbsLow:
    return -std::fabs(v);
  case Cmp::None:
    break;
  }
  assert(false);
  return 0.0;
}
struct ScoreRange { // 一组样本的分值范围, 逐个 add
  double lo = 1e300, hi = -1e300;
  int cnt = 0;
  void add(double s) { lo = std::min(lo, s), hi = std::max(hi, s), ++cnt; }
  bool usable() const { return cnt >= 2 && hi > lo; }
};
std::optional<ImVec4> grade_color(const Metric &m, const HS *h, const ScoreRange &rg) {
  if (m.cmp == Cmp::None || !h || h->n < 3 || !rg.usable())
    return std::nullopt;
  const float t = static_cast<float>((metric_score(m, *h) - rg.lo) / (rg.hi - rg.lo));
  const ImVec4 bad = StatusColor(TaskStatus::Kind::Error), mid = ImGui::GetStyleColorVec4(ImGuiCol_Text), good = StatusColor(TaskStatus::Kind::Ready);
  return t < 0.5f ? ImLerp(bad, mid, t * 2.f) : ImLerp(mid, good, (t - 0.5f) * 2.f);
}
// 跨持有期 (悬停表): 样本 = 该因子全部有效持有期
std::optional<ImVec4> hold_color(const FactorRow &r, const Metric &m, int k) {
  if (m.cmp == Cmp::None)
    return std::nullopt;
  ScoreRange rg;
  for (int j = 0; j < r.n_hold; ++j)
    if (r.hold[j].n >= 3)
      rg.add(metric_score(m, r.hold[j]));
  return grade_color(m, &r.hold[k], rg);
}

// 行悬停的 stat 部分 (调用方已 BeginTooltip): 头部对仗 bullet + 转置表 (行 = kMetrics 按组, 列 = 持有期), 可比指标跨持有期渐变着色
void stat_tooltip(const FactorRow &r, const FactorsUIContext &ctx) {
  // 键列对齐到 8 格 (CJK 一字 = 2 格, 等宽字体)
  ImGui::BulletText("来源    %s%s | 口径 %s | 标签 = 毛价格收益 (不含冲击 / 税佣; Inspect 页可选扣)", r.stat_from_file ? "文件" : "本轮",
                    r.stat_from_file && !scope_matches(r, ctx) ? " (作用域 ≠ 当前)" : "", factor::stat::frame_name(r.frame));
  ImGui::BulletText("作用域  %s  %s .. %s  (%d 天 × %d 资产, T = %d)", r.scope.universe.c_str(), r.scope.start_date.c_str(), r.scope.end_date.c_str(),
                    r.scope.days, r.scope.A, r.scope.T);
  ImGui::BulletText("算得    %s @ %s | valid %.2f%% | time %.3f ms (DAG 一遍, 与 hold 无关)", r.scope.backend.c_str(), r.scope.time.c_str(),
                    r.valid_pct, r.eval_ms);
  ImGui::BulletText("着色    可比指标跨持有期渐变 ");
  ImGui::SameLine(0, 0);
  ImGui::TextColored(StatusColor(TaskStatus::Kind::Error), "红 = 最差");
  ImGui::SameLine(0, 0);
  ImGui::TextDisabled(" → ");
  ImGui::SameLine(0, 0);
  ImGui::TextColored(StatusColor(TaskStatus::Kind::Ready), "绿 = 最好");
  ImGui::SameLine(0, 0);
  ImGui::TextDisabled("; 不着色 = 不跨期比");
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
    if (m.group != cur) { // 组标题行
      cur = m.group;
      const GroupInfo &g = kGroupInfo[static_cast<size_t>(cur)];
      ImGui::TableNextRow();
      ImGui::TableNextColumn();
      ImGui::TextDisabled("%s", g.name);
      ImGui::TableNextColumn();
      ImGui::TextDisabled("%s", g.desc);
    }
    ImGui::TableNextRow();
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
      const std::optional<ImVec4> c = hold_color(r, m, k);
      metric_cell(&r.hold[k], m, c ? &*c : nullptr);
    }
  }
  ImGui::EndTable();
}

// status 列: BROKEN / … / running / no stat / ok / file / file≠scope (+ dup)
void status_cell(const FactorRow &r, const FactorsUIContext &ctx) {
  if (!r.error.empty()) {
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Error), "BROKEN");
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
  } else if (!r.stat_from_file) {
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Ready), "ok");
  } else if (scope_matches(r, ctx)) {
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Muted), "file");
  } else {
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "file≠scope");
  }
  if (!r.dup_of.empty()) {
    ImGui::SameLine();
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "dup");
  }
}

// 固定列一格 (edit 列是勾选框, 在表循环里单独画)
void fixed_cell(Col c, const FactorRow &r, const FactorsUIContext &ctx, bool editing) {
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
    return text(r.name_en, editing);
  case NameCn:
    return text(r.name_cn, editing);
  case Frame:
    return ok ? ImGui::TextUnformatted(factor::stat::frame_name(r.frame)) : ImGui::TextDisabled("-");
  case Status:
    return status_cell(r, ctx);
  case Time:
    return num(r.has_stat, "%.1f", r.eval_ms);
  case Expr:
    if (!r.expr.empty())
      ImGui::TextUnformatted(r.expr.c_str());
    else if (!r.expr_raw.empty())
      ImGui::TextColored(StatusColor(TaskStatus::Kind::Error), "%s", r.expr_raw.c_str());
    else
      ImGui::TextDisabled("-");
    return;
  case Ops:
    return num(ok, "%d", r.n_ops);
  case Feats:
    return num(ok, "%d", r.n_feats);
  case Slots:
    return num(ok, "%d", r.n_slots);
  case Valid:
    return num(r.has_stat, "%.1f", r.valid_pct);
  case Note:
    return text(r.note);
  case Edit:
  case kNumFixed:
    break;
  }
  assert(false);
}

// 整行悬停 (任一格, 含勾选框): 文件 / 操作 / 原串 / BROKEN 原因 / dup / stat. 表格行只有这一个提示函数.
// 位置自己定: ImGui 的 tooltip 定位在四个方向都放不下时退化为贴鼠标右下 (大半出屏, 看着像没触发); 这里用上一帧的窗口尺寸把左上角夹回视口
void row_tooltip(const FactorRow &r, const FactorsUIContext &ctx, bool viewing, bool editing) {
  static ImVec2 s_size; // 上一帧尺寸 (首帧 0 → 不夹, ImGui 的 AlwaysAutoResize 首帧本就隐藏)
  const ImGuiViewport *vp = ImGui::GetMainViewport();
  const ImVec2 mouse = ImGui::GetMousePos();
  ImVec2 pos(mouse.x + 16.f, mouse.y + 16.f);
  pos.x = std::max(vp->WorkPos.x, std::min(pos.x, vp->WorkPos.x + vp->WorkSize.x - s_size.x));
  pos.y = std::max(vp->WorkPos.y, std::min(pos.y, vp->WorkPos.y + vp->WorkSize.y - s_size.y));
  ImGui::SetNextWindowPos(pos);
  ImGui::BeginTooltip();
  ImGui::PushTextWrapPos(ImGui::GetCursorPosX() + ImGui::GetFontSize() * kTipWrapEm);
  ImGui::BulletText("文件    %s/%s  (%s / %s)", ctx.factor_dir.c_str(), r.file.c_str(), r.name_en.c_str(), r.name_cn.empty() ? "无 name_cn" : r.name_cn.c_str());
  ImGui::BulletText("操作    点行 → %s; 勾选 → %s", viewing ? "取消高光" : "高光给 Inspect 页", editing ? "取消编辑 (构建器内容留作模板)" : "编辑 (载入上方构建器)");
  if (!r.expr_raw.empty() && r.expr_raw != r.expr)
    ImGui::BulletText("原串    %s", r.expr_raw.c_str());
  if (!r.dup_of.empty())
    ImGui::BulletText("dup     与 %s 同一规范串", r.dup_of.c_str());
  if (!r.error.empty()) {
    ImGui::Separator();
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Error), "BROKEN: %s", r.error.c_str());
    ImGui::TextDisabled("文件保留, 请手动修复或删除");
  }
  ImGui::PopTextWrapPos(); // 表格格子不能带折行位 (绝对 x, 右侧列会被折成零宽)
  if (r.has_stat) {
    ImGui::Separator();
    stat_tooltip(r, ctx);
  }
  s_size = ImGui::GetWindowSize();
  ImGui::EndTooltip();
}

} // namespace

int RenderTabFactors(FactorsService &svc, FactorsUIState &ui, const FactorsUIContext &ctx) {
  int action = 0;
  const FactorsStatus st = svc.status();
  const bool busy = st == FactorsStatus::Scanning || st == FactorsStatus::Loading || st == FactorsStatus::Running;
  const FeatureTable &ft = svc.feats();
  const bool gpu_ok = factor::gpu::available();
  if (!gpu_ok)
    ui.backend = 0;
  ui.hold_idx = std::clamp(ui.hold_idx, 0, std::max(0, static_cast<int>(ft.labels.size()) - 1));
  const int cur_hold = ft.labels.empty() ? 0 : ft.labels[static_cast<size_t>(ui.hold_idx)].hold;

  // ==========================================================================
  // 1. 作用域 + 后端 + Run / Cancel / Rescan + 状态
  // ==========================================================================
  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "1. Scope:");
  ImGui::SameLine();
  ImGui::Text("%s  %s .. %s", ctx.universe.c_str(), ctx.start_date.c_str(), ctx.end_date.c_str());
  if (ImGui::IsItemHovered())
    tip("因子目录 %s\n"
        "• 作用域 = config 的 universe + 日期区间, 与特征库同一推导\n"
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
    tip("两端同走一张共享 DAG: 跨因子公共子式只算一次, 顺序走节点\n"
        "• CPU: 每节点切满所有核 (CS / Point / Expand 按 t 切, Roll / Ema 按资产列切块), 与单线程逐位一致; Stat 全核\n"
        "• GPU: %s",
        g ? (std::string(g) + "; 输入上传一次, 中间量常驻显存, Stat 走常驻会话").c_str() : "off (无 CUDA 设备或未编译: cmake -DFACTOR_CUDA=ON)");
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
        "• 只影响显示, Run 一次算全部持有期\n"
        "• 悬停表格行看全部持有期");
  ImGui::SameLine();

  const bool can_run = !busy && ctx.axis_ready && !ft.labels.empty();
  if (!can_run)
    ImGui::BeginDisabled();
  if (ImGui::Button("Run", ImVec2(60, 0)))
    action = 1;
  if (!can_run)
    ImGui::EndDisabled();
  if (ImGui::IsItemHovered())
    tip(ctx.axis_ready ? "评估全目录: 扫描 → 读特征库 → 逐因子 eval → Stat\n"
                         "• 结果回写各因子文件的 params / stat 键"
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
  const int done = svc.done(), total = svc.total(), broken = svc.broken();
  switch (st) {
  case FactorsStatus::Idle:
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Muted), "idle");
    break;
  case FactorsStatus::Scanning:
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Busy), "scanning");
    break;
  case FactorsStatus::Loading:
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Busy), "loading %d/%d days", done, total);
    break;
  case FactorsStatus::Running:
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Busy), "running %d/%d nodes", done, total);
    break;
  case FactorsStatus::Done:
    ImGui::TextColored(StatusColor(broken ? TaskStatus::Kind::Warn : TaskStatus::Kind::Ready), "done, %d BROKEN", broken);
    break;
  case FactorsStatus::Cancelled:
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

  ImGui::Separator();

  // ==========================================================================
  // 2. 构建器: 根槽选算子 → 逐子槽往里选 (算子 / 特征), 参数手填 → 即时校验
  //    添加模式 (无选中行): Add 新文件 (名 / 规范串查重, 重了弹窗不加)
  //    编辑模式 (表格选中一行): Save 覆盖该文件 (弹窗确认) / 删除 (弹窗确认); 取消选定 → 回添加模式, 内容留作模板
  // ==========================================================================
  const FactorRow *edit_row = nullptr; // 勾选行; 重扫完还没了 (外部删了) → 自动退出编辑模式 (忙时 rows 可能还是旧的, 不判)
  if (!ui.edit_file.empty()) {
    for (const FactorRow &r : s_rows)
      if (r.file == ui.edit_file)
        edit_row = &r;
    if (!edit_row && !busy)
      ui.edit_file.clear();
  }
  if (!ui.view_file.empty() && !busy) { // 高光行同理
    bool found = false;
    for (const FactorRow &r : s_rows)
      found = found || r.file == ui.view_file;
    if (!found)
      ui.view_file.clear();
  }
  if (edit_row)
    ImGui::TextColored(kEditColor, "2. Edit %s:", ui.edit_file.c_str());
  else
    ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "2. Add:");
  if (ImGui::IsItemHovered())
    tip("从外到内构建表达式\n"
        "• 根: 必须是归一算子 —— CsRank / CsNormRank / CsZ (截面口径) 或 TsRankRoll(d=1275, 5 日) (时序口径)\n"
        "• 子槽: 缩进里选算子或特征; 参数 d / k / k2 手填\n"
        "• 下拉: 可打字过滤 (算子名 / 中文名 / 特征 code); GROUP 域的组 id 槽只列整数列算子 + 特征\n"
        "• 模式: 勾选表格首列 → 编辑 (载入到这里, Save 覆盖 / 删除); 取消勾选 → 添加, 内容留作模板");
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
  if (complete) {
    factor::expr::Expr e;
    if (factor::expr::parse(src, ft.lookup(), e, ui.add_err) && factor::expr::root_frame(e, frame, ui.add_err))
      canon = e.canon;
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
    ImGui::TextDisabled("[%s]", factor::stat::frame_name(frame));
    if (ImGui::IsItemHovered())
      tip("%s", frame == factor::stat::Frame::CS ? "截面口径: 根是截面归一算子\n• 每 t 截面 rank → 20 组, 标签超额"
                                                 : "时序口径: 根 TsRankRoll(5 日)\n• 自身分位直接分 20 组, 组可空, 标签超额");
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
  // 查重: 同名文件 / 同规范串的另一文件 (编辑模式下排除自己)
  const auto conflict = [&](std::string &msg) {
    for (const FactorRow &r : s_rows) {
      if (edit_row && r.file == edit_row->file)
        continue;
      if (r.file == new_file) {
        msg = "已存在同名文件 " + r.file;
        return true;
      }
      if (r.error.empty() && r.expr == canon) {
        msg = "与 " + r.file + " 同一规范串:\n" + canon;
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
        const bool expr_changed = edit_row->error.empty() ? edit_row->expr != canon : true;
        const bool name_changed = edit_row->file != new_file;
        const bool name_cn_changed = edit_row->name_cn != ui.name_cn_buf;
        const bool note_changed = edit_row->note != ui.note_buf;
        if (!expr_changed && !name_changed && !name_cn_changed && !note_changed) {
          ui.add_msg = "没有改动";
        } else {
          ui.popup_msg.clear();
          if (expr_changed)
            ui.popup_msg += "expr: " + (edit_row->error.empty() ? edit_row->expr : edit_row->expr_raw) + "\n   → " + canon +
                            (edit_row->has_stat ? "\n   (文件里的 stat 会被丢掉)" : "") + "\n";
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
  const int n = static_cast<int>(s_rows.size());
  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "3. Factors:");
  ImGui::SameLine();
  ImGui::Text("%d (%d BROKEN)", n, broken);
  ImGui::SameLine();
  ImGui::TextDisabled("Stat 列 = h %s (毛价格收益)", factor::stat::hold_name(cur_hold).c_str());

  {
    const uint64_t ep = svc.epoch();
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
    std::vector<int> order(static_cast<size_t>(n));
    std::iota(order.begin(), order.end(), 0);
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
          return metric_key(hold_of(r, cur_hold), *m);
        switch (sf) {
        case Type:
          return static_cast<double>(r.kind);
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
        case Valid:
          return r.has_stat ? r.valid_pct : -1.0;
        default:
          return 0.0;
        }
      };
      std::stable_sort(order.begin(), order.end(), [&](int a, int b) {
        const FactorRow &ra = s_rows[static_cast<size_t>(a)], &rb = s_rows[static_cast<size_t>(b)];
        int cmp = 0;
        if (sf == Edit)
          cmp = a - b;
        else if (const std::string *ta = text(ra))
          cmp = ta->compare(*text(rb));
        else
          cmp = (key(ra) > key(rb)) - (key(ra) < key(rb));
        return ui.sort_ascending ? cmp < 0 : cmp > 0;
      });
    }

    // 主表 Stat 列着色: 样本 = 全部因子在选定持有期的值 (跨因子横向比; 悬停表是同因子跨持有期比)
    ScoreRange col_rg[kNumStatCols];
    for (int i = 0; i < kNumStatCols; ++i) {
      const Metric &m = kMetrics[kStatColMetric[static_cast<size_t>(i)]];
      if (m.cmp == Cmp::None)
        continue;
      for (const FactorRow &r : s_rows)
        if (const HS *h = hold_of(r, cur_hold); h && h->n >= 3)
          col_rg[i].add(metric_score(m, *h));
    }

    for (int idx : order) {
      const FactorRow &r = s_rows[static_cast<size_t>(idx)];
      const HS *h = hold_of(r, cur_hold);
      const bool editing = edit_row == &r;
      const bool viewing = ui.view_file == r.file;
      ImGui::TableNextRow();
      ImGui::TableSetColumnIndex(col_of(Edit));
      ImGui::PushID(idx);
      // 整行可点 (先提交, AllowOverlap 让后面的勾选框盖在上面): 点行高光 → Inspect 页的对象; 再点 → 取消
      const ImVec2 cell_pos = ImGui::GetCursorPos();
      if (ImGui::Selectable("##row", viewing, ImGuiSelectableFlags_SpanAllColumns | ImGuiSelectableFlags_AllowOverlap))
        ui.view_file = viewing ? std::string{} : r.file;
      if (ImGui::IsItemHovered())
        row_tooltip(r, ctx, viewing, editing);
      // 勾选框 = 编辑模式 (同时只勾一个): 勾 → 载入构建器; 取消勾 → 回添加模式 (内容留作模板); 勾另一行 → 切换
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
          if (!r.expr.empty()) { // 规范串来自 scan 的 parse, 必可再解析 (根非归一的 BROKEN 行也有, 载入后套个根即可); 其他 BROKEN 行留空
            factor::expr::Expr e;
            std::string err;
            const bool ok = factor::expr::parse(r.expr, ft.lookup(), e, err);
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
      if (ImGui::IsItemHovered()) // 勾选框盖住了 Selectable 的 hover, 同一个提示函数
        row_tooltip(r, ctx, viewing, editing);
      ImGui::PopID();
      for (int c = 1; c < kNumCols; ++c) { // 0 = edit 列 (上面画过)
        ImGui::TableSetColumnIndex(c);
        const ColRef &cr = kColLayout[static_cast<size_t>(c)];
        if (cr.fixed >= 0) {
          fixed_cell(static_cast<Col>(cr.fixed), r, ctx, editing);
        } else {
          const Metric &m = *col_metric(c);
          const std::optional<ImVec4> col = grade_color(m, h, col_rg[cr.stat]);
          metric_cell(h, m, col ? &*col : nullptr);
        }
      }
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
