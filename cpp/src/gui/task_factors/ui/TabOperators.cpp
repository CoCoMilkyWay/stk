// Tab Operators — 见头文件
#include "gui/task_factors/ui/TabOperators.hpp"
#include "gui/Tasks.hpp"      // StatusColor: 全局色表, 行状态着色不自配颜色
#include "gui/util/Latex.hpp" // Formula 列 LaTeX 渲染 (与特征表共用缓存)

#include "imgui.h"
#include "imgui_internal.h" // TableSetColumnWidthAutoAll (强制列宽贴合)

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <numeric>
#include <vector>

namespace GUI::Factors {

namespace {

// 表内公式字号 (特征表悬停用 32, 表格行内要小一号)
constexpr float kFormulaTextSize = 18.0f;

// Args 列: "(输入; 参数=实际值)". 输入按元数 x / x, y / x, y, z; 0 元 = t_D. 参数按 OpTable 参数列逐个给值
void format_args(const OperatorRow &r, char *out, size_t cap) {
  static const char *kInputs[4] = {"t_D", "x", "x, y", "x, y, z"};
  assert(r.arity >= 0 && r.arity <= 3);
  int len = std::snprintf(out, cap, "(%s", kInputs[r.arity]);
  const char *p = r.params;
  bool first = true;
  while (*p) {
    const char *q = std::strchr(p, ',');
    const size_t tok_len = q ? static_cast<size_t>(q - p) : std::strlen(p);
    const char *sep = first ? "; " : ", ";
    if (tok_len == 1 && p[0] == 'd')
      len += std::snprintf(out + len, cap - len, "%sd=%d", sep, r.param.d);
    else if (tok_len == 1 && p[0] == 'k')
      len += std::snprintf(out + len, cap - len, "%sk=%g", sep, r.param.k);
    else if (tok_len == 2 && p[0] == 'k' && p[1] == '2')
      len += std::snprintf(out + len, cap - len, "%sk2=%g", sep, r.param.k2);
    else
      assert(false && "OpTable 参数列只允许 d / k / k2");
    first = false;
    p = q ? q + 1 : p + tok_len;
  }
  std::snprintf(out + len, cap - len, ")");
}

const char *win_name(factor::Win w) {
  switch (w) {
  case factor::Win::POINT:
    return "POINT";
  case factor::Win::EXPAND:
    return "EXPAND";
  case factor::Win::ROLL:
    return "ROLL";
  case factor::Win::EXPO:
    return "EXPO";
  }
  return "?";
}

const char *strat_name(factor::Strat s) {
  switch (s) {
  case factor::Strat::POINT:
    return "POINT";
  case factor::Strat::GATHER:
    return "GATHER";
  case factor::Strat::SCAN:
    return "SCAN";
  case factor::Strat::EXTREME:
    return "EXTREME";
  case factor::Strat::HIST:
    return "HIST";
  case factor::Strat::RECUR:
    return "RECUR";
  case factor::Strat::REDUCE:
    return "REDUCE";
  case factor::Strat::GROUP:
    return "GROUP";
  }
  return "?";
}

// 对拍列: 未跑 "…" / 跑中 "running" / ok + max|Δ| / FAIL + 掩码不符数 + 超差数
void render_diff_cell(const OperatorRow &r, const factor::check::Diff &d, bool absent) {
  if (r.status != RowStatus::Done) {
    ImGui::TextColored(StatusColor(r.status == RowStatus::Running ? TaskStatus::Kind::Busy : TaskStatus::Kind::Muted),
                       "%s", r.status == RowStatus::Running ? "running" : "…");
    return;
  }
  if (absent) {
    ImGui::TextDisabled("n/a");
    return;
  }
  if (d.ok())
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Ready), "ok  Δ=%.2g", d.worst);
  else
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Error), "FAIL mask %d val %d/%d Δ=%.2g", d.mask_bad,
                       d.val_bad, d.compared, d.worst);
}

void render_ms_cell(const OperatorRow &r, double ms) {
  if (r.status != RowStatus::Done)
    ImGui::TextDisabled("…");
  else if (ms < 0)
    ImGui::TextDisabled("n/a");
  else
    ImGui::Text("%.3f", ms);
}

// 排序键: 未跑的行排最后 (时间列 / 误差列都按 -1 处理)
double sort_ms(const OperatorRow &r, double ms) { return r.status == RowStatus::Done ? ms : -1.0; }
double sort_worst(const OperatorRow &r, const factor::check::Diff &d) {
  if (r.status != RowStatus::Done)
    return -1.0;
  return d.ok() ? d.worst : 1e300; // FAIL 恒排在 ok 之后 (降序时 FAIL 置顶)
}

} // namespace

int RenderTabOperators(OperatorsService &svc, OperatorsUIState &ui) {
  int action = 0;
  const OperatorsStatus st = svc.status();
  const bool running = st == OperatorsStatus::Running;

  // ==========================================================================
  // 1. 张量参数 + Run / Cancel + 进度
  // ==========================================================================
  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "1. Tensor:");
  ImGui::SameLine();
  ImGui::SetNextItemWidth(60);
  ImGui::InputInt("Days", &ui.req.days, 0);
  ImGui::SameLine();
  ImGui::SetNextItemWidth(70);
  ImGui::InputInt("Assets", &ui.req.A, 0);
  ImGui::SameLine();
  ImGui::SetNextItemWidth(60);
  ImGui::InputInt("d", &ui.req.d, 0);
  ImGui::SameLine();
  int seed = static_cast<int>(ui.req.seed);
  ImGui::SetNextItemWidth(60);
  ImGui::InputInt("Seed", &seed, 0);
  ui.req.days = std::max(ui.req.days, 1);
  ui.req.A = std::max(ui.req.A, 1);
  ui.req.d = std::max(ui.req.d, 1);
  ui.req.seed = static_cast<unsigned>(std::max(seed, 0));
  ImGui::SameLine();
  ImGui::TextDisabled("T=%d A=%d", ui.req.T(), ui.req.A);
  if (ImGui::IsItemHovered())
    ImGui::SetTooltip("T = Days × %d (段 = 交易日, 段界对齐是 EXPAND 窗的前提)\n"
                      "PLAIN 造数, 配方按算子 (正值型给对数正态, 分组型给整数 id), 与 op_check 同 seed 同张量",
                      factor::kSegLen);
  ImGui::SameLine();

  if (running)
    ImGui::BeginDisabled();
  if (ImGui::Button("Run", ImVec2(60, 0)))
    action = 1;
  if (running)
    ImGui::EndDisabled();
  ImGui::SameLine();
  if (!running)
    ImGui::BeginDisabled();
  if (ImGui::Button("Cancel", ImVec2(60, 0)))
    action = -1;
  if (!running)
    ImGui::EndDisabled();

  ImGui::SameLine();
  ImGui::Text("Status:");
  ImGui::SameLine();
  const int done = svc.done(), total = svc.total(), failed = svc.failed();
  switch (st) {
  case OperatorsStatus::Idle:
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Muted), "idle");
    break;
  case OperatorsStatus::Running:
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Busy), "running %d/%d", done, total);
    break;
  case OperatorsStatus::Done:
    ImGui::TextColored(StatusColor(failed ? TaskStatus::Kind::Error : TaskStatus::Kind::Ready), "done %d/%d, %d FAIL",
                       done, total, failed);
    break;
  case OperatorsStatus::Cancelled:
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "cancelled %d/%d", done, total);
    break;
  }
  ImGui::SameLine();
  ImGui::TextDisabled("| GPU: %s", svc.gpu_available() ? "on" : "off (未编译 CUDA 后端: cmake -DFACTOR_CUDA=ON)");

  ImGui::Separator();

  // ==========================================================================
  // 2. 表
  // ==========================================================================
  // 短锁拷快照 (~90 行小结构, worker 只在行末短锁写)
  static std::vector<OperatorRow> s_rows;
  OperatorsRequest cur;
  {
    std::lock_guard<std::mutex> lock(svc.mutex);
    s_rows = svc.rows;
    cur = svc.current;
  }
  const int n = static_cast<int>(s_rows.size());

  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "2. Operators:");
  ImGui::SameLine();
  ImGui::Text("%d (TS %d / CS %d)", n, (int)std::count_if(s_rows.begin(), s_rows.end(), [](const OperatorRow &r) { return !r.is_cs; }),
              (int)std::count_if(s_rows.begin(), s_rows.end(), [](const OperatorRow &r) { return r.is_cs; }));
  if (st != OperatorsStatus::Idle) {
    ImGui::SameLine();
    ImGui::TextDisabled("(last run: T=%d A=%d d=%d seed=%u)", cur.T(), cur.A, cur.d, cur.seed);
  }

  // 列宽贴合: 发布代变了 (限速 0.5s) → 连发 2 帧 AutoAll (全行每帧都提交, 测量完整, 不必像 TabFeature 那样等 clipper)
  {
    const uint64_t ep = svc.epoch();
    const double now = ImGui::GetTime();
    if (ui.fit_epoch != ep && (now - ui.fit_last_time >= 0.5 || !running)) {
      ui.fit_epoch = ep;
      ui.fit_last_time = now;
      ui.fit_frames = 2;
    }
  }

  ImGui::PushStyleVar(ImGuiStyleVar_CellPadding, ImVec2(4.0f, 2.0f));
  constexpr int kNumCols = 14;
  if (ImGui::BeginTable("OperatorTable", kNumCols,
                        ImGuiTableFlags_SizingFixedFit | ImGuiTableFlags_Borders | ImGuiTableFlags_RowBg |
                            ImGuiTableFlags_ScrollY | ImGuiTableFlags_ScrollX | ImGuiTableFlags_Resizable |
                            ImGuiTableFlags_Sortable | ImGuiTableFlags_SortTristate | ImGuiTableFlags_NoSavedSettings,
                        ImVec2(0, 0))) {
    ImGui::TableSetupColumn("Name", ImGuiTableColumnFlags_WidthFixed);                                   // 0
    ImGui::TableSetupColumn("Ar", ImGuiTableColumnFlags_WidthFixed);                                     // 1
    ImGui::TableSetupColumn("Win", ImGuiTableColumnFlags_WidthFixed);                                    // 2
    ImGui::TableSetupColumn("Axis", ImGuiTableColumnFlags_WidthFixed);                                   // 3
    ImGui::TableSetupColumn("Args", ImGuiTableColumnFlags_WidthFixed | ImGuiTableColumnFlags_NoSort);    // 4
    ImGui::TableSetupColumn("GPU", ImGuiTableColumnFlags_WidthFixed);                                    // 5
    ImGui::TableSetupColumn("Formula", ImGuiTableColumnFlags_WidthFixed | ImGuiTableColumnFlags_NoSort); // 6
    ImGui::TableSetupColumn("Note", ImGuiTableColumnFlags_WidthFixed | ImGuiTableColumnFlags_NoSort);    // 7
    ImGui::TableSetupColumn("Stream vs Naive", ImGuiTableColumnFlags_WidthFixed);                        // 8
    ImGui::TableSetupColumn("GPU vs Naive", ImGuiTableColumnFlags_WidthFixed);                           // 9
    ImGui::TableSetupColumn("Naive ms", ImGuiTableColumnFlags_WidthFixed);                               // 10
    ImGui::TableSetupColumn("Stream ms", ImGuiTableColumnFlags_WidthFixed);                              // 11
    ImGui::TableSetupColumn("GPU ms", ImGuiTableColumnFlags_WidthFixed);                                 // 12
    ImGui::TableSetupColumn("Stream/GPU", ImGuiTableColumnFlags_WidthFixed);                             // 13
    ImGui::TableSetupScrollFreeze(0, 1);

    if (ui.fit_frames > 0) {
      ui.fit_frames--;
      ImGuiTable *table = ImGui::GetCurrentTable();
      assert(table);
      ImGui::TableSetColumnWidthAutoAll(table);
    }

    ImGui::TableNextRow(ImGuiTableRowFlags_Headers);
    const char *headers[kNumCols] = {"Name", "Ar", "Win", "Axis", "Args", "GPU", "Formula", "Note",
                                     "Stream vs Naive", "GPU vs Naive", "Naive ms", "Stream ms", "GPU ms", "Stream/GPU"};
    const char *tooltips[kNumCols] = {
        "算子名: 轴_核_窗. Cum = 段内 expanding, Roll = 最近 d 期, Ema = 指数递推, 无后缀 = 逐点.\n"
        "默认排序: 元数 → 窗 → 轴 (TS 先) → OpTable 表序; 点表头按列排 (三态)",
        "元数: 输入序列数 0..3 (Args 里分号前的部分); d / k 是参数不算元",
        "窗形 (TS): POINT 逐点 / EXPAND 段内 expanding (段界 reset) / ROLL 最近 d 期 (跨段) / EXPO 指数递推 (全程)\n"
        "CS 无窗 (每个时刻取一整行截面)",
        "轴: TS 只接本资产的序列; CS 只接同一时刻的截面",
        "签名 (输入; 参数=本轮实际值):\n"
        "  输入  x, y, z = 按元数取的序列 (每格 值 + 有效位); t_D = 段内分钟位置 (TodMask 无序列输入)\n"
        "  参数  Param 字段 (factor/Contract.hpp):\n"
        "    d   窗长 / 滞后 (期 = 分钟), 来自页面 d; 参数列不含 d 的算子固定 1 (与 op_check 同)\n"
        "    k   阈值 / 指数 / 桶数 / EMA 系数 / 分位 / topk (含义见该行 Note), 来自 Check.hpp kKParams\n"
        "    k2  第二阈值 (仅 TodMask 上界)",
        "GPU 策略: POINT 逐格 / GATHER 移位 / SCAN 分段前缀和 / EXTREME van Herk / HIST 桶直方图 / RECUR 仿射 scan / REDUCE 沿资产归约 / GROUP 分组归约",
        "公式 (OpTable.hpp, LaTeX). 符号继承 features/FeaturesDefine.hpp (t 分钟, D 交易日, 1[·] 指示), 算子库补充:\n"
        "  x_t 本资产 t 分钟值; x_a 同一时刻资产 a 的值; t_D 段内位置\n"
        "  W_t 窗 (由 Win 列定): EXPAND {s: 同日, s ≤ t}; ROLL {s: t−d < s ≤ t}\n"
        "  n / N 窗内 / 截面有效样本数; μ_t σ_t 窗内均值 / 样本标准差 (ddof=1); m_k k 阶中心总体矩\n"
        "  pct(v; S) 并列均秩 pct rank ∈ [0,1]; Q_p 截面 p 分位; Φ⁻¹ 标准正态分位; G(a) 组 (整数 id 由 y / z 给)\n"
        "  所有 ∑ / 计数 / 极值只计有效样本; 序统计族三后端同用 256 桶近似",
        "备注: 退化条件 (输出无效) / 参数含义 / 近似说明. \"退化\"见 Contract.hpp: 全并列 (精确) / 相消 (相对 1e-6) / 除零",
        "流式 (实盘路径, 逐点 push) 对 naive (double, 按定义): 掩码逐位相等且 |Δ| ≤ 1e-5 + 1e-4·max(|a|,|b|)",
        "GPU (fp32 并行) 对 naive: |Δ| ≤ 1e-3 + 1e-3·max (CsNormRank / 三四阶矩单独放宽)",
        "naive 整段一次的 wall time (不含造数)",
        "stream 逐资产沿 t 推进的 wall time",
        "GPU wall time: 含 cudaMalloc + H2D/D2H 拷贝 (GpuRun 接口如此), 首次调用前已热身",
        "stream_ms / gpu_ms",
    };
    for (int c = 0; c < kNumCols; c++) {
      ImGui::TableSetColumnIndex(c);
      ImGui::TableHeader(headers[c]);
      if (ImGui::IsItemHovered())
        ImGui::SetTooltip("%s", tooltips[c]);
    }

    // 排序 (tristate: 升 → 降 → 默认序)
    if (ImGuiTableSortSpecs *specs = ImGui::TableGetSortSpecs(); specs && specs->SpecsDirty) {
      if (specs->SpecsCount > 0) {
        ui.sort_column = specs->Specs[0].ColumnIndex;
        ui.sort_ascending = specs->Specs[0].SortDirection == ImGuiSortDirection_Ascending;
      } else {
        ui.sort_column = -1;
      }
      specs->SpecsDirty = false;
    }
    // 默认序: 元数 → 窗 → 轴 (TS 先) → OpTable 表序 (下标); 用户选列时在此序上 stable_sort
    std::vector<int> order(n);
    std::iota(order.begin(), order.end(), 0);
    std::stable_sort(order.begin(), order.end(), [&](int a, int b) {
      const OperatorRow &ra = s_rows[a], &rb = s_rows[b];
      if (ra.arity != rb.arity)
        return ra.arity < rb.arity;
      if (ra.win != rb.win)
        return (int)ra.win < (int)rb.win;
      return (int)ra.is_cs < (int)rb.is_cs;
    });
    if (ui.sort_column >= 0) {
      auto cmp3 = [](double x, double y) { return x < y ? -1 : (x > y ? 1 : 0); };
      std::stable_sort(order.begin(), order.end(), [&](int a, int b) {
        const OperatorRow &ra = s_rows[a], &rb = s_rows[b];
        int cmp = 0;
        switch (ui.sort_column) {
        case 0:
          cmp = std::strcmp(ra.name, rb.name);
          break;
        case 1:
          cmp = ra.arity - rb.arity;
          break;
        case 2:
          cmp = (int)ra.win - (int)rb.win;
          break;
        case 3:
          cmp = (int)ra.is_cs - (int)rb.is_cs;
          break;
        case 5:
          cmp = (int)ra.strat - (int)rb.strat;
          break;
        case 8:
          cmp = cmp3(sort_worst(ra, ra.stream), sort_worst(rb, rb.stream));
          break;
        case 9:
          cmp = cmp3(ra.gpu_ms < 0 ? -1.0 : sort_worst(ra, ra.gpu), rb.gpu_ms < 0 ? -1.0 : sort_worst(rb, rb.gpu));
          break;
        case 10:
          cmp = cmp3(sort_ms(ra, ra.naive_ms), sort_ms(rb, rb.naive_ms));
          break;
        case 11:
          cmp = cmp3(sort_ms(ra, ra.stream_ms), sort_ms(rb, rb.stream_ms));
          break;
        case 12:
          cmp = cmp3(sort_ms(ra, ra.gpu_ms), sort_ms(rb, rb.gpu_ms));
          break;
        case 13: {
          auto sp = [](const OperatorRow &r) {
            return r.status == RowStatus::Done && r.gpu_ms > 0 ? r.stream_ms / r.gpu_ms : -1.0;
          };
          cmp = cmp3(sp(ra), sp(rb));
          break;
        }
        }
        return ui.sort_ascending ? cmp < 0 : cmp > 0;
      });
    }

    // 全行提交 (~90 行, 不需要 clipper; 贴合帧也因此测量完整)
    char args[96];
    for (int idx : order) {
      const OperatorRow &r = s_rows[idx];
      ImGui::TableNextRow();
      ImGui::TableSetColumnIndex(0);
      ImGui::TextColored(StatusColor(r.status == RowStatus::Done
                                         ? ((r.stream.ok() && (r.gpu_ms < 0 || r.gpu.ok())) ? TaskStatus::Kind::Ready
                                                                                            : TaskStatus::Kind::Error)
                                         : (r.status == RowStatus::Running ? TaskStatus::Kind::Busy : TaskStatus::Kind::Muted)),
                         "%s", r.name);
      ImGui::TableSetColumnIndex(1);
      ImGui::Text("%d", r.arity);
      ImGui::TableSetColumnIndex(2);
      if (r.is_cs)
        ImGui::TextDisabled("-");
      else
        ImGui::TextUnformatted(win_name(r.win));
      ImGui::TableSetColumnIndex(3);
      ImGui::TextUnformatted(r.is_cs ? "CS" : "TS");
      ImGui::TableSetColumnIndex(4);
      format_args(r, args, sizeof(args));
      ImGui::TextUnformatted(args);
      ImGui::TableSetColumnIndex(5);
      ImGui::TextUnformatted(strat_name(r.strat));
      ImGui::TableSetColumnIndex(6);
      if (tex::TeXRender *render = Latex::Get(r.formula, kFormulaTextSize))
        Latex::Draw(render);
      else
        ImGui::TextUnformatted(r.formula); // 解析失败回退原文
      ImGui::TableSetColumnIndex(7);
      if (r.note[0] == '\0')
        ImGui::TextDisabled("-");
      else
        ImGui::TextUnformatted(r.note);
      ImGui::TableSetColumnIndex(8);
      render_diff_cell(r, r.stream, false);
      ImGui::TableSetColumnIndex(9);
      render_diff_cell(r, r.gpu, r.gpu_ms < 0);
      ImGui::TableSetColumnIndex(10);
      render_ms_cell(r, r.naive_ms);
      ImGui::TableSetColumnIndex(11);
      render_ms_cell(r, r.stream_ms);
      ImGui::TableSetColumnIndex(12);
      render_ms_cell(r, r.gpu_ms);
      ImGui::TableSetColumnIndex(13);
      if (r.status == RowStatus::Done && r.gpu_ms > 0)
        ImGui::Text("%.2fx", r.stream_ms / r.gpu_ms);
      else
        ImGui::TextDisabled("%s", r.status == RowStatus::Done ? "n/a" : "…");
    }
    ImGui::EndTable();
  }
  ImGui::PopStyleVar();
  return action;
}

} // namespace GUI::Factors
