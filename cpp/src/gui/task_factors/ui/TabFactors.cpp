// Tab Factors — 见头文件
#include "gui/task_factors/ui/TabFactors.hpp"
#include "factor/Check.hpp"  // default_d / set_k: 构建器选中算子时的参数默认值 (与 Operators 页同一张表)
#include "factor/GpuRun.hpp" // available / device_name
#include "gui/Tasks.hpp"     // StatusColor

#include "imgui.h"
#include "imgui_internal.h" // TableSetColumnWidthAutoAll

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <numeric>
#include <string>
#include <vector>

namespace GUI::Factors {

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

// 一个槽的下拉: group_slot = GROUP 域算子的组 id 元 (只列整数列算子 + 特征叶)
void slot_combo(BuildNode &n, const FeatureTable &ft, char *filter, size_t filter_cap, bool group_slot) {
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
    if (group_slot) { // 组 id 元须是整数列
      factor::expr::Node probe;
      probe.op = i;
      if (factor::expr::kind_of(probe) != factor::expr::Kind::INT)
        continue;
    }
    if (!contains_ci(o.name, f) && !contains_ci(o.c_name, f))
      continue;
    char label[96];
    std::snprintf(label, sizeof(label), "%s  %s", o.name, o.c_name);
    if (ImGui::Selectable(label, n.op == i))
      set_op(n, i);
    if (ImGui::IsItemHovered())
      ImGui::SetTooltip("%d 元 | %s | %s", o.arity, o.params[0] ? o.params : "无参数", o.note);
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
void render_node(BuildNode &n, const FeatureTable &ft, char *filter, size_t filter_cap, const char *slot_label, bool group_slot) {
  ImGui::PushID(&n);
  ImGui::AlignTextToFramePadding();
  ImGui::TextDisabled("%s", slot_label);
  ImGui::SameLine();
  slot_combo(n, ft, filter, filter_cap, group_slot);
  if (n.op >= 0) {
    const factor::expr::OpInfo &o = factor::expr::kOps[n.op];
    if (factor::expr::declares(o.params, "d")) {
      ImGui::SameLine();
      ImGui::SetNextItemWidth(70);
      ImGui::InputInt("d", &n.p.d, 0);
      if (ImGui::IsItemHovered())
        ImGui::SetTooltip("窗长 / 滞后 (分钟), 1..%d", factor::expr::kMaxD);
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
      ImGui::SetTooltip("%s", o.note);
    ImGui::Indent(24.0f);
    for (size_t a = 0; a < n.args.size(); ++a) {
      const bool is_group = o.a == factor::A::GROUP && static_cast<int>(a) == factor::expr::group_arg(o);
      render_node(n.args[a], ft, filter, filter_cap, is_group ? "组 id" : kSlotNames[a], is_group);
    }
    ImGui::Unindent(24.0f);
  }
  ImGui::PopID();
}

// 该行选定 (金额档, 持有期) 的汇总; 没有 → nullptr
const factor::stat::HoldStat *hold_of(const FactorRow &r, int amt, int hold) { return r.has_stat ? r.find_hold(amt, hold) : nullptr; }

// 文件里的 stat 与当前作用域是否一致 (universe / 区间)
bool scope_matches(const FactorRow &r, const FactorsUIContext &ctx) {
  return r.scope.universe == ctx.universe && r.scope.start_date == ctx.start_date && r.scope.end_date == ctx.end_date;
}

// 排序键: 无值的行排最前 (升序) / 最后 (降序)
double stat_key(const factor::stat::HoldStat *h, float factor::stat::HoldStat::*f) { return h ? static_cast<double>(h->*f) : -1e300; }

void render_stat_cell(const factor::stat::HoldStat *h, float v, const char *fmt) {
  if (!h || h->n < 3)
    ImGui::TextDisabled("-");
  else
    ImGui::Text(fmt, v);
}

void status_cell(const FactorRow &r, const FactorsUIContext &ctx) {
  if (!r.error.empty()) {
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Error), "BROKEN");
    if (ImGui::IsItemHovered())
      ImGui::SetTooltip("%s\n\n文件保留, 请手动修复或删除", r.error.c_str());
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
  if (ImGui::IsItemHovered() && r.has_stat) {
    ImGui::BeginTooltip();
    ImGui::Text("stat 来自: %s | %s  %s..%s  %d 天 × %d 资产 (T=%d)  %s  %s", r.stat_from_file ? "文件" : "本轮", r.scope.universe.c_str(),
                r.scope.start_date.c_str(), r.scope.end_date.c_str(), r.scope.days, r.scope.A, r.scope.T, r.scope.backend.c_str(),
                r.scope.time.c_str());
    ImGui::Text("valid %.2f%%  time %.3f ms (DAG 算一遍, 与 amt / hold 无关)", r.valid_pct, r.eval_ms);
    for (int ai = 0; ai < r.n_amt; ++ai) {
      ImGui::Separator();
      ImGui::TextDisabled("amt = %dw", r.amt[ai]);
      for (int k = 0; k < r.n_hold; ++k) {
        const factor::stat::HoldStat &h = r.hold[ai][k];
        ImGui::Text("h=%-5s n=%d/%d  rIC %+.4f std %.4f IR %+.3f t %+.2f pos %.2f skew %+.2f kurt %+.2f | LS %+.5f t %+.2f SR %+.2f "
                    "β %+.3f | mono %+.3f | rAC %+.3f",
                    factor::stat::hold_name(h.hold).c_str(), h.n, h.n_ac, h.ic_mean, h.ic_std, h.icir, h.ic_t, h.ic_pos, h.ic_skew, h.ic_kurt, h.ls_mean, h.ls_t, h.sharpe,
                    h.beta, h.mono, h.rank_ac);
      }
    }
    ImGui::EndTooltip();
  }
  if (!r.dup_of.empty()) {
    ImGui::SameLine();
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "dup");
    if (ImGui::IsItemHovered())
      ImGui::SetTooltip("与 %s 同一规范串", r.dup_of.c_str());
  }
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
  ui.amt_idx = std::clamp(ui.amt_idx, 0, std::max(0, static_cast<int>(ft.amts.size()) - 1));
  ui.hold_idx = std::clamp(ui.hold_idx, 0, std::max(0, static_cast<int>(ft.labels.size()) - 1));
  const int cur_amt = ft.amts.empty() ? 0 : ft.amts[static_cast<size_t>(ui.amt_idx)];
  const int cur_hold = ft.labels.empty() ? 0 : ft.labels[static_cast<size_t>(ui.hold_idx)].hold;

  // ==========================================================================
  // 1. 作用域 + 后端 + Run / Cancel / Rescan + 状态
  // ==========================================================================
  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "1. Scope:");
  ImGui::SameLine();
  ImGui::Text("%s  %s .. %s", ctx.universe.c_str(), ctx.start_date.c_str(), ctx.end_date.c_str());
  if (ImGui::IsItemHovered())
    ImGui::SetTooltip("因子目录: %s\n作用域 = config 的 universe + 日期区间 (与特征库同一推导); 一个时间只算一个作用域", ctx.factor_dir.c_str());
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
    if (const char *g = factor::gpu::device_name())
      ImGui::SetTooltip("两端都走一张共享 DAG (跨因子公共子式只算一次), 顺序走节点\nCPU: 每个节点切满所有核 (CS / Point / Expand 按 t 切, "
                        "Roll / Ema 按资产列切块; 与单线程逐位一致), Stat 全核\nGPU (%s): 输入上传一次, 中间量常驻显存, Stat 走常驻会话",
                        g);
    else
      ImGui::SetTooltip("共享 DAG (跨因子公共子式只算一次), 顺序走节点\nCPU: 每个节点切满所有核 (CS / Point / Expand 按 t 切, "
                        "Roll / Ema 按资产列切块; 与单线程逐位一致), Stat 全核\nGPU: off (无 CUDA 设备或未编译: cmake -DFACTOR_CUDA=ON)");
  }
  ImGui::SameLine();
  ImGui::SetNextItemWidth(70);
  if (ImGui::BeginCombo("Amt", ft.amts.empty() ? "-" : (std::to_string(cur_amt) + "w").c_str())) {
    for (size_t i = 0; i < ft.amts.size(); ++i)
      if (ImGui::Selectable((std::to_string(ft.amts[i]) + "w").c_str(), static_cast<int>(i) == ui.amt_idx))
        ui.amt_idx = static_cast<int>(i);
    ImGui::EndCombo();
  }
  if (ImGui::IsItemHovered())
    ImGui::SetTooltip("表格 Stat 列显示哪个金额档 (万元; 只影响显示). Run 把全部 金额档 × 持有期 一起算 (lb_long/short_<h>m_<amt>w 列)");
  ImGui::SameLine();
  ImGui::SetNextItemWidth(70);
  if (ImGui::BeginCombo("Hold", ft.labels.empty() ? "-" : factor::stat::hold_name(cur_hold).c_str())) {
    for (size_t i = 0; i < ft.labels.size(); ++i)
      if (ImGui::Selectable(factor::stat::hold_name(ft.labels[i].hold).c_str(), static_cast<int>(i) == ui.hold_idx))
        ui.hold_idx = static_cast<int>(i);
    ImGui::EndCombo();
  }
  if (ImGui::IsItemHovered())
    ImGui::SetTooltip("表格 Stat 列显示哪个持有期 (只影响显示; 悬停 status 看全部持有期)");
  ImGui::SameLine();

  const bool can_run = !busy && ctx.axis_ready && !ft.labels.empty();
  if (!can_run)
    ImGui::BeginDisabled();
  if (ImGui::Button("Run", ImVec2(60, 0)))
    action = 1;
  if (!can_run)
    ImGui::EndDisabled();
  if (ImGui::IsItemHovered())
    ImGui::SetTooltip(ctx.axis_ready ? "扫描 + 读特征库 + 逐因子 eval + Stat, 结果回写各因子文件 (params / stat 键)"
                                     : "资产轴未就绪 (先在 Database 页扫描), 读不了特征库");
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
    ImGui::SetTooltip("重扫目录 (只解析校验, 不算); 文件里的 stat 照常显示");

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
  const FactorRow *edit_row = nullptr; // 选中行; 重扫完还没了 (外部删了) → 自动退出编辑模式 (忙时 rows 可能还是旧的, 不判)
  if (!ui.edit_file.empty()) {
    for (const FactorRow &r : s_rows)
      if (r.file == ui.edit_file)
        edit_row = &r;
    if (!edit_row && !busy)
      ui.edit_file.clear();
  }
  if (edit_row)
    ImGui::TextColored(ImVec4(1.0f, 0.75f, 0.3f, 1.0f), "2. Edit %s:", ui.edit_file.c_str());
  else
    ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "2. Add:");
  if (ImGui::IsItemHovered())
    ImGui::SetTooltip("从外到内: 先选根 (通常 CsRank / CsZ 等截面归一), 再在缩进的子槽里选算子或特征; 参数 (d / k / k2) 手填\n"
                      "下拉里可打字过滤 (算子名 / 中文名 / 特征 code); GROUP 域算子的组 id 槽只列整数列算子 + 特征\n"
                      "点表格一行 → 编辑模式 (该行高光, 载入到这里; Save 覆盖 / 删除); 取消选定 → 添加模式, 内容留作模板");
  ImGui::SameLine();
  if (edit_row) {
    if (ImGui::SmallButton("取消选定")) {
      ui.edit_file.clear();
      ui.add_msg.clear();
    }
    ImGui::SameLine();
  }
  if (ImGui::SmallButton("Clear")) {
    ui.build = BuildNode{};
    ui.name_buf[0] = '\0';
    ui.note_buf[0] = '\0';
    ui.add_msg.clear();
  }
  render_node(ui.build, ft, ui.filter_buf, sizeof(ui.filter_buf), "root", false);

  std::string src;
  bool complete = true;
  to_source(ui.build, src, complete);
  ui.add_err.clear();
  std::string canon;
  if (complete) {
    factor::expr::Expr e;
    if (factor::expr::parse(src, ft.lookup(), e, ui.add_err))
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
  else
    ImGui::TextUnformatted(canon.c_str());

  const bool name_red = ui.name_buf[0] != '\0' && !ValidFactorName(ui.name_buf); // InputText 会改 buf, push/pop 必须用同一个判断结果
  if (name_red)
    ImGui::PushStyleColor(ImGuiCol_Text, StatusColor(TaskStatus::Kind::Error));
  ImGui::SetNextItemWidth(180);
  ImGui::InputTextWithHint("##name", "name (必填, 文件名)", ui.name_buf, sizeof(ui.name_buf), ImGuiInputTextFlags_CharsNoBlank);
  if (name_red)
    ImGui::PopStyleColor();
  const bool name_ok = ValidFactorName(ui.name_buf);
  if (ImGui::IsItemHovered())
    ImGui::SetTooltip("因子名 = <factor_dir>/<name>.json; 仅 [A-Za-z0-9_], ≤63 字符");
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
  const bool can_write = !busy && complete && !canon.empty() && name_ok;
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
        const bool note_changed = edit_row->note != ui.note_buf;
        if (!expr_changed && !name_changed && !note_changed) {
          ui.add_msg = "没有改动";
        } else {
          ui.popup_msg.clear();
          if (expr_changed)
            ui.popup_msg += "expr: " + (edit_row->error.empty() ? edit_row->expr : edit_row->expr_raw) + "\n   → " + canon +
                            (edit_row->has_stat ? "\n   (文件里的 stat 会被丢掉)" : "") + "\n";
          if (name_changed)
            ui.popup_msg += "name: " + edit_row->file + " → " + new_file + "\n";
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
      const std::string file = AddFactorFile(ctx.factor_dir, ft, ui.name_buf, canon, ui.note_buf, err);
      if (file.empty()) { // 查重已过仍失败 = 目录被外部改了 (agent 加文件), 重扫即可
        ui.popup = 3;
        ui.popup_msg = err + " (目录有外部改动, 已重扫)";
        action = 2;
      } else {
        ui.add_msg = "已加 " + file + "  " + canon;
        ui.build = BuildNode{};
        ui.name_buf[0] = '\0';
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
  ImGui::TextDisabled("Stat 列 = h %s, amt %dw", factor::stat::hold_name(cur_hold).c_str(), cur_amt);

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
  constexpr int kNumCols = 19;
  if (ImGui::BeginTable("FactorTable", kNumCols,
                        ImGuiTableFlags_SizingFixedFit | ImGuiTableFlags_Borders | ImGuiTableFlags_RowBg | ImGuiTableFlags_ScrollY |
                            ImGuiTableFlags_ScrollX | ImGuiTableFlags_Resizable | ImGuiTableFlags_Sortable | ImGuiTableFlags_SortTristate |
                            ImGuiTableFlags_NoSavedSettings,
                        ImVec2(0, 0))) {
    const char *headers[kNumCols] = {"#", "file", "status", "time", "expr", "ops", "feats", "slots", "valid%", "IC",
                                     "ICIR", "IC_t", "LS", "SR", "β", "mono", "rAC", "n", "note"};
    const char *tooltips[kNumCols] = {
        "文件名序 (默认排序)",
        "<factor_dir>/<universe>/ 下的文件名 = <name>.json; 点行 → 编辑模式 (载入上方构建器, 可改 / 删)",
        "BROKEN 红 = 文件 / 表达式 / params / 组 id 数据不合 (悬停看原因; 文件不动, 人手动处理)\nok 绿 = 本轮算的; file 灰 = stat 来自文件且作用域一致; file≠scope 黄 = 文件 stat 是别的 universe / 区间算的\nno stat = 从未评估; dup 黄 = 与另一文件同一规范串",
        "该因子算一遍 DAG 的耗时 (ms) = 子树全部算子节点之和 (共享节点算给每个用它的因子): CPU 全核 wall / GPU 纯 kernel",
        "规范串 (解析后重新序列化: 算子 PascalCase, 参数 d/k/k2 显式, 含 params 覆盖后的当前值); BROKEN 行显示文件原串",
        "算子节点数 (= params 数组长度)",
        "去重特征数 (输入平面数)",
        "DAG 缓冲槽数 (峰值同时存活的中间量; 内存 / 显存 = slots × T·A × 5B)",
        "根平面有效格占比",
        "rank IC 均值 (Spearman, 每分钟截面, 沿 t 平均; 只计 ok 行)",
        "IC 均值 / IC 标准差",
        "IC t 值 = ICIR · √(n/h) (重叠持有期折算)",
        "多空净收益均值 (最高组做多 + 最低组做空, 实盘可成交口径)",
        "多空 Sharpe (按持有期为一期年化)",
        "多空对市场 (截面标签均值) 的 OLS 斜率",
        "20 组均值对组号的 Spearman (单调性)",
        "rank 自相关 (lag = h)",
        "有效行数 (ok 行; < 3 时 Stat 列空)",
        "文件 note 键 (人 / agent 写的一句话)",
    };
    for (int c = 0; c < kNumCols; ++c) {
      ImGuiTableColumnFlags fl = ImGuiTableColumnFlags_WidthFixed;
      if (c == 2 || c == 4 || c == 18)
        fl |= ImGuiTableColumnFlags_NoSort;
      ImGui::TableSetupColumn(headers[c], fl);
    }
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
      ImGui::TableHeader(headers[c]);
      if (ImGui::IsItemHovered())
        ImGui::SetTooltip("%s", tooltips[c]);
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
      using HS = factor::stat::HoldStat;
      auto cmp3 = [](double x, double y) { return x < y ? -1 : (x > y ? 1 : 0); };
      auto key = [&](const FactorRow &r) -> double {
        const HS *h = hold_of(r, cur_amt, cur_hold);
        switch (ui.sort_column) {
        case 3:
          return r.has_stat ? r.eval_ms : -1.0;
        case 5:
          return r.n_ops;
        case 6:
          return r.n_feats;
        case 7:
          return r.n_slots;
        case 8:
          return r.has_stat ? r.valid_pct : -1.0;
        case 9:
          return stat_key(h, &HS::ic_mean);
        case 10:
          return stat_key(h, &HS::icir);
        case 11:
          return stat_key(h, &HS::ic_t);
        case 12:
          return stat_key(h, &HS::ls_mean);
        case 13:
          return stat_key(h, &HS::sharpe);
        case 14:
          return stat_key(h, &HS::beta);
        case 15:
          return stat_key(h, &HS::mono);
        case 16:
          return stat_key(h, &HS::rank_ac);
        case 17:
          return h ? h->n : -1.0;
        default:
          return 0.0;
        }
      };
      std::stable_sort(order.begin(), order.end(), [&](int a, int b) {
        const FactorRow &ra = s_rows[static_cast<size_t>(a)], &rb = s_rows[static_cast<size_t>(b)];
        int cmp = 0;
        if (ui.sort_column == 0)
          cmp = a - b;
        else if (ui.sort_column == 1)
          cmp = std::strcmp(ra.file.c_str(), rb.file.c_str());
        else
          cmp = cmp3(key(ra), key(rb));
        return ui.sort_ascending ? cmp < 0 : cmp > 0;
      });
    }

    for (int idx : order) {
      const FactorRow &r = s_rows[static_cast<size_t>(idx)];
      const factor::stat::HoldStat *h = hold_of(r, cur_amt, cur_hold);
      const bool selected = edit_row == &r;
      ImGui::TableNextRow();
      if (selected)
        ImGui::TableSetBgColor(ImGuiTableBgTarget_RowBg0, ImGui::GetColorU32(ImVec4(1.0f, 0.75f, 0.3f, 0.35f)));
      ImGui::TableSetColumnIndex(0);
      ImGui::TextDisabled("%d", idx);
      ImGui::TableSetColumnIndex(1);
      ImGui::PushID(idx);
      // 整行可点: 选中 → 编辑模式并载入构建器; 再点选中行 → 取消选定 (内容留作模板)
      if (ImGui::Selectable(r.file.c_str(), selected, ImGuiSelectableFlags_SpanAllColumns | ImGuiSelectableFlags_AllowOverlap)) {
        if (selected) {
          ui.edit_file.clear();
        } else {
          ui.edit_file = r.file;
          ui.build = BuildNode{};
          if (r.error.empty()) { // 规范串来自 scan 的 parse, 必可再解析; BROKEN 行构建器留空 (只能改名 / 删)
            factor::expr::Expr e;
            std::string err;
            const bool ok = factor::expr::parse(r.expr, ft.lookup(), e, err);
            assert(ok);
            (void)ok;
            from_expr(e, 0, ui.build);
          }
          std::snprintf(ui.name_buf, sizeof(ui.name_buf), "%.*s", static_cast<int>(r.file.size() - 5), r.file.c_str()); // 去 .json
          std::snprintf(ui.note_buf, sizeof(ui.note_buf), "%s", r.note.c_str());
        }
        ui.add_msg.clear();
      }
      ImGui::PopID();
      if (ImGui::IsItemHovered())
        ImGui::SetTooltip("%s/%s\n(点击: %s)", ctx.factor_dir.c_str(), r.file.c_str(), selected ? "取消选定" : "进入编辑模式");
      ImGui::TableSetColumnIndex(2);
      status_cell(r, ctx);
      ImGui::TableSetColumnIndex(3);
      if (r.has_stat)
        ImGui::Text("%.1f", r.eval_ms);
      else
        ImGui::TextDisabled("-");
      ImGui::TableSetColumnIndex(4);
      if (r.expr.empty()) {
        if (r.expr_raw.empty())
          ImGui::TextDisabled("-");
        else
          ImGui::TextColored(StatusColor(TaskStatus::Kind::Error), "%s", r.expr_raw.c_str());
      } else {
        ImGui::TextUnformatted(r.expr.c_str());
        if (ImGui::IsItemHovered() && r.expr_raw != r.expr)
          ImGui::SetTooltip("文件原串: %s", r.expr_raw.c_str());
      }
      ImGui::TableSetColumnIndex(5);
      if (r.expr.empty())
        ImGui::TextDisabled("-");
      else
        ImGui::Text("%d", r.n_ops);
      ImGui::TableSetColumnIndex(6);
      if (r.expr.empty())
        ImGui::TextDisabled("-");
      else
        ImGui::Text("%d", r.n_feats);
      ImGui::TableSetColumnIndex(7);
      if (r.expr.empty())
        ImGui::TextDisabled("-");
      else
        ImGui::Text("%d", r.n_slots);
      ImGui::TableSetColumnIndex(8);
      if (r.has_stat)
        ImGui::Text("%.1f", r.valid_pct);
      else
        ImGui::TextDisabled("-");
      ImGui::TableSetColumnIndex(9);
      render_stat_cell(h, h ? h->ic_mean : 0.f, "%+.4f");
      ImGui::TableSetColumnIndex(10);
      render_stat_cell(h, h ? h->icir : 0.f, "%+.3f");
      ImGui::TableSetColumnIndex(11);
      render_stat_cell(h, h ? h->ic_t : 0.f, "%+.2f");
      ImGui::TableSetColumnIndex(12);
      render_stat_cell(h, h ? h->ls_mean : 0.f, "%+.5f");
      ImGui::TableSetColumnIndex(13);
      render_stat_cell(h, h ? h->sharpe : 0.f, "%+.2f");
      ImGui::TableSetColumnIndex(14);
      render_stat_cell(h, h ? h->beta : 0.f, "%+.3f");
      ImGui::TableSetColumnIndex(15);
      render_stat_cell(h, h ? h->mono : 0.f, "%+.3f");
      ImGui::TableSetColumnIndex(16);
      render_stat_cell(h, h ? h->rank_ac : 0.f, "%+.3f");
      ImGui::TableSetColumnIndex(17);
      if (h)
        ImGui::Text("%d", h->n);
      else
        ImGui::TextDisabled("-");
      ImGui::TableSetColumnIndex(18);
      if (r.note.empty())
        ImGui::TextDisabled("-");
      else
        ImGui::TextUnformatted(r.note.c_str());
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
      const std::string file = UpdateFactorFile(ctx.factor_dir, ft, ui.edit_file, ui.name_buf, canon, ui.note_buf, err);
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
