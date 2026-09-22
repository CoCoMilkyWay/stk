// Tab Operators — 见头文件
#include "gui/task_factors/ui/TabOperators.hpp"
#include "gui/Tasks.hpp"      // StatusColor: 全局色表, 行状态着色不自配颜色
#include "gui/util/Latex.hpp" // Formula 列 LaTeX 渲染 (与特征表共用缓存)

#include "imgui.h"
#include "imgui_internal.h" // TableSetColumnWidthAutoAll (强制列宽贴合)
#include "nlohmann/json.hpp"

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <numeric>
#include <vector>

namespace GUI::Factors {

namespace {

// 表内公式字号 (特征表悬停用 32, 表格行内要小一号)
constexpr float kFormulaTextSize = 18.0f;

// 输入签名按元数: 0 元 = t_D (TodMask), 1..3 元 = x / x, y / x, y, z
const char *inputs_of(const OperatorRow &r) {
  static const char *kInputs[4] = {"t_D", "x", "x, y", "x, y, z"};
  assert(r.arity >= 0 && r.arity <= 3);
  return kInputs[r.arity];
}

// 逐个走 OpTable 参数列 ("d,k" 等) 的字段, 取本轮实际值: fn(name, value). 只允许 d / k / k2
template <class Fn>
void for_each_param(const OperatorRow &r, Fn &&fn) {
  const char *p = r.params;
  while (*p) {
    const char *q = std::strchr(p, ',');
    const size_t tok_len = q ? static_cast<size_t>(q - p) : std::strlen(p);
    if (tok_len == 1 && p[0] == 'd')
      fn("d", static_cast<double>(r.param.d));
    else if (tok_len == 1 && p[0] == 'k')
      fn("k", static_cast<double>(r.param.k));
    else if (tok_len == 2 && p[0] == 'k' && p[1] == '2')
      fn("k2", static_cast<double>(r.param.k2));
    else
      assert(false && "OpTable 参数列只允许 d / k / k2");
    p = q ? q + 1 : p + tok_len;
  }
}

// Args 列: "(输入; 参数=实际值)"
void format_args(const OperatorRow &r, char *out, size_t cap) {
  int len = std::snprintf(out, cap, "(%s", inputs_of(r));
  bool first = true;
  for_each_param(r, [&](const char *name, double v) {
    len += std::snprintf(out + len, cap - len, "%s%s=%g", first ? "; " : ", ", name, v);
    first = false;
  });
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

// 时间列 (stream 是 golden, cpu / gpu 各自带对拍结论):
//   未跑 "…" / 跑中 "running" / 无后端 "n/a" / 没过 check → 红色 error + Δ (不显示时间) / 过了 → ms
//   slower = 该后端比上游慢 (要求 stream ≥ cpu ≥ gpu), 标红 ms
// chk = nullptr 表示该列无对拍 (stream 列)
void render_time_cell(const OperatorRow &r, double ms, const factor::check::Diff *chk, bool slower) {
  if (r.status != RowStatus::Done) {
    ImGui::TextColored(StatusColor(r.status == RowStatus::Running ? TaskStatus::Kind::Busy : TaskStatus::Kind::Muted),
                       "%s", r.status == RowStatus::Running ? "running" : "…");
    return;
  }
  if (ms < 0) {
    ImGui::TextDisabled("n/a");
    return;
  }
  if (chk && !chk->ok()) {
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Error), "FAIL mask %d val %d/%d Δ=%.2g", chk->mask_bad,
                       chk->val_bad, chk->compared, chk->worst);
    return;
  }
  if (slower)
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Error), "%.3f", ms);
  else
    ImGui::Text("%.3f", ms);
}

// 排序键: 未跑的行按 -1 (排最前 / 降序时最后), FAIL 恒排在 ok 之后
double sort_ms(const OperatorRow &r, double ms) { return r.status == RowStatus::Done ? ms : -1.0; }
double sort_ms_chk(const OperatorRow &r, double ms, const factor::check::Diff &d) {
  if (r.status != RowStatus::Done || ms < 0)
    return -1.0;
  return d.ok() ? ms : 1e300;
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
  constexpr int kNumCols = 11;
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
    ImGui::TableSetupColumn("Stream ms", ImGuiTableColumnFlags_WidthFixed);                              // 7
    ImGui::TableSetupColumn("CPU ms", ImGuiTableColumnFlags_WidthFixed);                                 // 8
    ImGui::TableSetupColumn("GPU ms", ImGuiTableColumnFlags_WidthFixed);                                 // 9
    ImGui::TableSetupColumn("Note", ImGuiTableColumnFlags_WidthFixed | ImGuiTableColumnFlags_NoSort);    // 10
    ImGui::TableSetupScrollFreeze(0, 1);

    if (ui.fit_frames > 0) {
      ui.fit_frames--;
      ImGuiTable *table = ImGui::GetCurrentTable();
      assert(table);
      ImGui::TableSetColumnWidthAutoAll(table);
    }

    ImGui::TableNextRow(ImGuiTableRowFlags_Headers);
    const char *headers[kNumCols] = {"Name", "Ar", "Win", "Axis", "Args", "GPU", "Formula",
                                     "Stream ms", "CPU ms", "GPU ms", "Note"};
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
        "  Σ_{W_t} / Π_{W_t} 窗内求和 / 乘积 (求和变量恒是 s 或 b); max min med cov var corr 的下标 = 取值域\n"
        "  pct(v; S) 并列均秩 pct rank ∈ [0,1]; Q_p 截面 p 分位; Φ⁻¹ 标准正态分位; G(a) 组 (整数 id 由 y / z 给)\n"
        "  所有 Σ / 计数 / 极值只计有效样本; 序统计族三后端同用 256 桶近似",
        "stream (实盘路径, 逐资产逐点 push) 的 wall time = golden: 正确性基准, 也是耗时上界",
        "cpu 整张量一次批算的 wall time (不含造数)\n"
        "  没过对拍 (掩码逐位相等且 |Δ| ≤ 1e-5 + 1e-4·max(|a|,|b|)) → 红色 FAIL + Δ, 不显示时间\n"
        "  过了但比 stream 慢 → ms 标红 (要求 stream ≥ cpu ≥ gpu)",
        "GPU wall time: 含 cudaMalloc + H2D/D2H 拷贝 (GpuRun 接口如此), 首次调用前已热身\n"
        "  没过对拍 (对 cpu: |Δ| ≤ 1e-3 + 1e-3·max, CsNormRank / 三四阶矩单独放宽) → 红色 FAIL + Δ, 不显示时间\n"
        "  过了但比 cpu 慢 → ms 标红",
        "备注: 退化条件 (输出无效) / 参数含义 / 近似说明. \"退化\"见 Contract.hpp: 全并列 (精确) / 相消 (相对 1e-6) / 除零",
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
        case 7:
          cmp = cmp3(sort_ms(ra, ra.stream_ms), sort_ms(rb, rb.stream_ms));
          break;
        case 8:
          cmp = cmp3(sort_ms_chk(ra, ra.cpu_ms, ra.stream), sort_ms_chk(rb, rb.cpu_ms, rb.stream));
          break;
        case 9:
          cmp = cmp3(sort_ms_chk(ra, ra.gpu_ms, ra.gpu), sort_ms_chk(rb, rb.gpu_ms, rb.gpu));
          break;
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
        Latex::Draw(render, ImGui::GetTextLineHeight());
      else
        ImGui::TextUnformatted(r.formula); // 解析失败回退原文
      ImGui::TableSetColumnIndex(7);
      render_time_cell(r, r.stream_ms, nullptr, false); // golden: 无对拍, 无快慢判据
      ImGui::TableSetColumnIndex(8);
      render_time_cell(r, r.cpu_ms, &r.stream, r.cpu_ms > r.stream_ms);
      ImGui::TableSetColumnIndex(9);
      render_time_cell(r, r.gpu_ms, &r.gpu, r.gpu_ms > r.cpu_ms);
      ImGui::TableSetColumnIndex(10);
      if (r.note[0] == '\0')
        ImGui::TextDisabled("-");
      else
        ImGui::TextUnformatted(r.note);
    }
    ImGui::EndTable();
  }
  ImGui::PopStyleVar();
  return action;
}

// ============================================================================
// 算子表落地 JSON (对仗 TabFeature::SaveFeatureTableJson: 手写外层, 一行一算子, ordered_json 保键序 = 列序)
// ============================================================================

namespace {

// 4 位有效数字 (人读; 耗时 / 误差全精度只添噪)
double sig4(double v) {
  char buf[32];
  snprintf(buf, sizeof(buf), "%.4g", v);
  return std::strtod(buf, nullptr);
}

const char *status_name(OperatorsStatus st) {
  switch (st) {
  case OperatorsStatus::Idle:
    return "idle";
  case OperatorsStatus::Running:
    return "running";
  case OperatorsStatus::Done:
    return "done";
  case OperatorsStatus::Cancelled:
    return "cancelled";
  }
  return "?";
}

const char *row_status_name(RowStatus st) {
  switch (st) {
  case RowStatus::Pending:
    return "pending";
  case RowStatus::Running:
    return "running";
  case RowStatus::Done:
    return "done";
  }
  return "?";
}

nlohmann::ordered_json diff_json(const factor::check::Diff &d) {
  return {{"ok", d.ok()}, {"worst", sig4(d.worst)}, {"mask_bad", d.mask_bad}, {"val_bad", d.val_bad}, {"compared", d.compared}};
}

} // namespace

void SaveOperatorTableJson(const std::string &factor_dir, OperatorsService &svc) {
  using json = nlohmann::json;

  const std::filesystem::path path = std::filesystem::path(factor_dir) / "operators.json";
  std::filesystem::create_directories(path.parent_path());
  const std::filesystem::path tmp = path.string() + ".tmp";
  {
    std::ofstream file(tmp);
    assert(file.is_open() && "operators.json: 临时文件打不开");

    std::lock_guard<std::mutex> lock(svc.mutex);
    const OperatorsRequest &cur = svc.current;
    file << "{\n";
    file << " \"tensor\": {\"days\": " << cur.days << ", \"assets\": " << cur.A << ", \"T\": " << cur.T()
         << ", \"d\": " << cur.d << ", \"seed\": " << cur.seed << "},\n";
    file << " \"gpu\": " << (svc.gpu_available() ? "true" : "false") << ",\n";
    file << " \"status\": " << json(status_name(svc.status())).dump() << ", \"done\": " << svc.done()
         << ", \"total\": " << svc.total() << ", \"failed\": " << svc.failed() << ",\n";
    file << " \"tol\": {\"stream\": \"|Δ| ≤ 1e-5 + 1e-4·max(|a|,|b|), mask 逐位相等\","
            " \"gpu\": \"|Δ| ≤ 1e-3 + 1e-3·max (CsNormRank / 三四阶矩单独放宽)\"},\n";

    file << " \"rows\": [\n";
    const size_t n = svc.rows.size();
    for (size_t i = 0; i < n; ++i) {
      const OperatorRow &r = svc.rows[i];
      // 键序 = 表格列序
      nlohmann::ordered_json j;
      j["idx"] = i;
      j["name"] = r.name;
      j["arity"] = r.arity;
      j["axis"] = r.is_cs ? "CS" : "TS";
      if (!r.is_cs)
        j["win"] = win_name(r.win);
      j["inputs"] = inputs_of(r);
      nlohmann::ordered_json params = nlohmann::ordered_json::object();
      for_each_param(r, [&](const char *name, double v) { params[name] = v; });
      j["params"] = params;
      j["gpu_strat"] = strat_name(r.strat);
      j["formula"] = r.formula;
      j["note"] = r.note;
      j["status"] = row_status_name(r.status);
      // 动态列只落跑完的行 (表格显示 "…" 的格子不落键)
      if (r.status == RowStatus::Done) {
        j["stream_vs_cpu"] = diff_json(r.stream);
        if (r.gpu_ms >= 0)
          j["gpu_vs_cpu"] = diff_json(r.gpu);
        j["cpu_ms"] = sig4(r.cpu_ms);
        j["stream_ms"] = sig4(r.stream_ms);
        if (r.gpu_ms >= 0) {
          j["gpu_ms"] = sig4(r.gpu_ms);
          if (r.gpu_ms > 0)
            j["stream_over_gpu"] = sig4(r.stream_ms / r.gpu_ms);
        }
      }
      file << "  " << j.dump() << (i + 1 < n ? ",\n" : "\n");
    }
    file << " ]\n";
    file << "}\n";
    assert(file.good() && "operators.json: 写入失败");
  }
  std::filesystem::rename(tmp, path); // 原子替换: 崩在中途不留半截文件 (同 features.json)
}

} // namespace GUI::Factors
