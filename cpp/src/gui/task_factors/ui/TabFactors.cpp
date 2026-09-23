// Tab Factors — 见头文件
#include "gui/task_factors/ui/TabFactors.hpp"
#include "factor/GpuRun.hpp" // available / device_name
#include "gui/Tasks.hpp"     // StatusColor

#include "imgui.h"
#include "imgui_internal.h" // TableSetColumnWidthAutoAll

#include <algorithm>
#include <cassert>
#include <cstring>
#include <mutex>
#include <numeric>
#include <string>
#include <vector>

namespace GUI::Factors {

namespace {

// 该行选定持有期的汇总; 没有 → nullptr
const factor::stat::HoldStat *hold_of(const FactorRow &r, int hold) {
  if (!r.has_stat)
    return nullptr;
  for (int i = 0; i < r.n_hold; ++i)
    if (r.hold[i].hold == hold)
      return &r.hold[i];
  return nullptr;
}

// 文件里的 stat 与当前作用域是否一致 (universe / 区间 / 金额档)
bool scope_matches(const FactorRow &r, const FactorsUIContext &ctx, int amt) {
  return r.scope.universe == ctx.universe && r.scope.start_date == ctx.start_date && r.scope.end_date == ctx.end_date && r.scope.amt == amt;
}

// 排序键: 无值的行排最前 (升序) / 最后 (降序)
double stat_key(const factor::stat::HoldStat *h, float factor::stat::HoldStat::*f) { return h ? static_cast<double>(h->*f) : -1e300; }

void render_stat_cell(const factor::stat::HoldStat *h, float v, const char *fmt) {
  if (!h || h->n < 3)
    ImGui::TextDisabled("-");
  else
    ImGui::Text(fmt, v);
}

void status_cell(const FactorRow &r, const FactorsUIContext &ctx, int amt) {
  if (!r.error.empty()) {
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Error), "BROKEN");
    if (ImGui::IsItemHovered())
      ImGui::SetTooltip("%s\n\n文件原样留着, 请手动修复或删除", r.error.c_str());
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
  } else if (scope_matches(r, ctx, amt)) {
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Muted), "file");
  } else {
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "file≠scope");
  }
  if (ImGui::IsItemHovered() && r.has_stat) {
    ImGui::BeginTooltip();
    ImGui::Text("stat 来自: %s | %s  %s..%s  %d 天 × %d 资产 (T=%d)  amt=%dw  %s  %s", r.stat_from_file ? "文件" : "本轮",
                r.scope.universe.c_str(), r.scope.start_date.c_str(), r.scope.end_date.c_str(), r.scope.days, r.scope.A, r.scope.T,
                r.scope.amt, r.scope.backend.c_str(), r.scope.time.c_str());
    ImGui::Text("valid %.2f%%  eval %.3f ms  stat %.3f ms", r.valid_pct, r.eval_ms, r.stat_ms);
    ImGui::Separator();
    for (int i = 0; i < r.n_hold; ++i) {
      const factor::stat::HoldStat &h = r.hold[i];
      ImGui::Text("h=%-3d n=%d/%d  rIC %+.4f std %.4f IR %+.3f t %+.2f pos %.2f skew %+.2f kurt %+.2f | LS %+.5f t %+.2f SR %+.2f "
                  "β %+.3f | mono %+.3f | rAC %+.3f",
                  h.hold, h.n, h.n_ac, h.ic_mean, h.ic_std, h.icir, h.ic_t, h.ic_pos, h.ic_skew, h.ic_kurt, h.ls_mean, h.ls_t, h.sharpe,
                  h.beta, h.mono, h.rank_ac);
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
      ImGui::SetTooltip("CPU: 因子间并行, 每因子单线程整张量批算 (factor::cpu)\nGPU (%s): 输入上传一次, 中间量常驻显存, Stat 走常驻会话", g);
    else
      ImGui::SetTooltip("CPU: 因子间并行, 每因子单线程整张量批算 (factor::cpu)\nGPU: off (无 CUDA 设备或未编译: cmake -DFACTOR_CUDA=ON)");
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
    ImGui::SetTooltip("标签的下单金额档 (万元): 评估用 lb_long/short_<h>m_<amt>w 列 (LabelReturn), 全部持有期一起算");
  ImGui::SameLine();
  ImGui::SetNextItemWidth(70);
  if (ImGui::BeginCombo("Hold", ft.labels.empty() ? "-" : (std::to_string(cur_hold) + "m").c_str())) {
    for (size_t i = 0; i < ft.labels.size(); ++i)
      if (ImGui::Selectable((std::to_string(ft.labels[i].hold) + "m").c_str(), static_cast<int>(i) == ui.hold_idx))
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
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Busy), "running %d/%d", done, total);
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
  // 2. 加因子: 表达式即时校验 → Add 落文件 → 重扫
  // ==========================================================================
  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "2. Add:");
  ImGui::SameLine();
  ImGui::SetNextItemWidth(std::max(300.0f, ImGui::GetContentRegionAvail().x - 140.0f));
  const bool enter = ImGui::InputText("##expr", ui.add_buf, sizeof(ui.add_buf), ImGuiInputTextFlags_EnterReturnsTrue);
  if (ImGui::IsItemHovered() && !ImGui::IsItemActive())
    ImGui::SetTooltip("前缀函数式: Op(arg, ..., d=, k=, k2=); 算子名见 Operators 页 e_name, 特征 = L1 字段 code\n"
                      "例: CsRank(TsMeanRoll(TsLog(amt_todz_5d), d=30))\n"
                      "即时校验: 算子存在 / 元数 / 参数齐且在值域 / 特征存在且可作输入 / GROUP 组 id 元是整数列");
  if (ui.add_checked != ui.add_buf) {
    ui.add_checked = ui.add_buf;
    ui.add_err.clear();
    if (ui.add_buf[0] != '\0') {
      factor::expr::Expr e;
      factor::expr::parse(ui.add_buf, ft.lookup(), e, ui.add_err);
    }
  }
  ImGui::SameLine();
  const bool can_add = !busy && ui.add_buf[0] != '\0' && ui.add_err.empty();
  if (!can_add)
    ImGui::BeginDisabled();
  if (ImGui::Button("Add", ImVec2(60, 0)) || (enter && can_add)) {
    std::string err;
    const std::string name = AddFactorFile(ctx.factor_dir, ft, ui.add_buf, err);
    if (name.empty()) {
      ui.add_msg = "失败: " + err;
    } else {
      ui.add_msg = "已加 " + name;
      ui.add_buf[0] = '\0';
      action = 2;
    }
  }
  if (!can_add)
    ImGui::EndDisabled();
  if (!ui.add_err.empty())
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Error), "%s", ui.add_err.c_str());
  else if (!ui.add_msg.empty())
    ImGui::TextDisabled("%s", ui.add_msg.c_str());

  ImGui::Separator();

  // ==========================================================================
  // 3. 表
  // ==========================================================================
  const int n = static_cast<int>(s_rows.size());
  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "3. Factors:");
  ImGui::SameLine();
  ImGui::Text("%d (%d BROKEN)", n, broken);
  ImGui::SameLine();
  ImGui::TextDisabled("Stat 列 = h %dm, amt %dw", cur_hold, cur_amt);

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
  constexpr int kNumCols = 20;
  if (ImGui::BeginTable("FactorTable", kNumCols,
                        ImGuiTableFlags_SizingFixedFit | ImGuiTableFlags_Borders | ImGuiTableFlags_RowBg | ImGuiTableFlags_ScrollY |
                            ImGuiTableFlags_ScrollX | ImGuiTableFlags_Resizable | ImGuiTableFlags_Sortable | ImGuiTableFlags_SortTristate |
                            ImGuiTableFlags_NoSavedSettings,
                        ImVec2(0, 0))) {
    const char *headers[kNumCols] = {"#", "file", "status", "expr", "ops", "feats", "slots", "valid%", "IC", "ICIR", "IC_t", "LS", "SR", "β",
                                     "mono", "rAC", "n", "eval ms", "stat ms", "note"};
    const char *tooltips[kNumCols] = {
        "文件名序 (默认排序)",
        "<factor_dir>/<universe>/ 下的文件名; Add 生成 f_<fnv1a(规范串) 8 hex>.json",
        "BROKEN 红 = 文件 / 表达式 / params / 组 id 数据不合 (悬停看原因; 文件不动, 人手动处理)\nok 绿 = 本轮算的; file 灰 = stat 来自文件且作用域一致; file≠scope 黄 = 文件 stat 是别的 universe / 区间 / 金额档算的\nno stat = 从未评估; dup 黄 = 与另一文件同一规范串",
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
        "因子 eval 耗时: CPU wall / GPU 纯 kernel 和 (ms)",
        "Stat eval 耗时 (ms; 不含标签预处理)",
        "文件 note 键 (人 / agent 写的一句话)",
    };
    for (int c = 0; c < kNumCols; ++c) {
      ImGuiTableColumnFlags fl = ImGuiTableColumnFlags_WidthFixed;
      if (c == 2 || c == 3 || c == 19)
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
        const HS *h = hold_of(r, cur_hold);
        switch (ui.sort_column) {
        case 4:
          return r.n_ops;
        case 5:
          return r.n_feats;
        case 6:
          return r.n_slots;
        case 7:
          return r.has_stat ? r.valid_pct : -1.0;
        case 8:
          return stat_key(h, &HS::ic_mean);
        case 9:
          return stat_key(h, &HS::icir);
        case 10:
          return stat_key(h, &HS::ic_t);
        case 11:
          return stat_key(h, &HS::ls_mean);
        case 12:
          return stat_key(h, &HS::sharpe);
        case 13:
          return stat_key(h, &HS::beta);
        case 14:
          return stat_key(h, &HS::mono);
        case 15:
          return stat_key(h, &HS::rank_ac);
        case 16:
          return h ? h->n : -1.0;
        case 17:
          return r.has_stat ? r.eval_ms : -1.0;
        case 18:
          return r.has_stat ? r.stat_ms : -1.0;
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
      const factor::stat::HoldStat *h = hold_of(r, cur_hold);
      ImGui::TableNextRow();
      ImGui::TableSetColumnIndex(0);
      ImGui::TextDisabled("%d", idx);
      ImGui::TableSetColumnIndex(1);
      ImGui::TextUnformatted(r.file.c_str());
      if (ImGui::IsItemHovered())
        ImGui::SetTooltip("%s/%s", ctx.factor_dir.c_str(), r.file.c_str());
      ImGui::TableSetColumnIndex(2);
      status_cell(r, ctx, cur_amt);
      ImGui::TableSetColumnIndex(3);
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
      ImGui::TableSetColumnIndex(4);
      if (r.expr.empty())
        ImGui::TextDisabled("-");
      else
        ImGui::Text("%d", r.n_ops);
      ImGui::TableSetColumnIndex(5);
      if (r.expr.empty())
        ImGui::TextDisabled("-");
      else
        ImGui::Text("%d", r.n_feats);
      ImGui::TableSetColumnIndex(6);
      if (r.expr.empty())
        ImGui::TextDisabled("-");
      else
        ImGui::Text("%d", r.n_slots);
      ImGui::TableSetColumnIndex(7);
      if (r.has_stat)
        ImGui::Text("%.1f", r.valid_pct);
      else
        ImGui::TextDisabled("-");
      ImGui::TableSetColumnIndex(8);
      render_stat_cell(h, h ? h->ic_mean : 0.f, "%+.4f");
      ImGui::TableSetColumnIndex(9);
      render_stat_cell(h, h ? h->icir : 0.f, "%+.3f");
      ImGui::TableSetColumnIndex(10);
      render_stat_cell(h, h ? h->ic_t : 0.f, "%+.2f");
      ImGui::TableSetColumnIndex(11);
      render_stat_cell(h, h ? h->ls_mean : 0.f, "%+.5f");
      ImGui::TableSetColumnIndex(12);
      render_stat_cell(h, h ? h->sharpe : 0.f, "%+.2f");
      ImGui::TableSetColumnIndex(13);
      render_stat_cell(h, h ? h->beta : 0.f, "%+.3f");
      ImGui::TableSetColumnIndex(14);
      render_stat_cell(h, h ? h->mono : 0.f, "%+.3f");
      ImGui::TableSetColumnIndex(15);
      render_stat_cell(h, h ? h->rank_ac : 0.f, "%+.3f");
      ImGui::TableSetColumnIndex(16);
      if (h)
        ImGui::Text("%d", h->n);
      else
        ImGui::TextDisabled("-");
      ImGui::TableSetColumnIndex(17);
      if (r.has_stat)
        ImGui::Text("%.3f", r.eval_ms);
      else
        ImGui::TextDisabled("-");
      ImGui::TableSetColumnIndex(18);
      if (r.has_stat)
        ImGui::Text("%.3f", r.stat_ms);
      else
        ImGui::TextDisabled("-");
      ImGui::TableSetColumnIndex(19);
      if (r.note.empty())
        ImGui::TextDisabled("-");
      else
        ImGui::TextUnformatted(r.note.c_str());
    }
    ImGui::EndTable();
  }
  ImGui::PopStyleVar();
  return action;
}

} // namespace GUI::Factors
