// Tab Operators — 见头文件
#include "gui/task_factors/ui/TabOperators.hpp"
#include "factor/GpuRun.hpp"                      // device_name: GPU 型号显示
#include "gui/Tasks.hpp"                          // StatusColor: 全局色表, 行状态着色不自配颜色
#include "gui/task_factors/services/StatJson.hpp" // sig4 / json_* (与因子文件同一格式)
#include "gui/util/Latex.hpp"                     // Operands / Operator 列 LaTeX 渲染 (与特征表共用缓存)

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
#include <string>
#include <vector>

namespace GUI::Factors {

namespace {

// 表内公式字号 (特征表悬停用 32, 表格行内要小一号)
constexpr float kFormulaTextSize = 18.0f;

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

// operand 列: OpTable 模板 (LaTeX) 里的占位符 ⟨d⟩ ⟨k⟩ ⟨k2⟩ → 本轮实际参数值.
// 参数列声明的字段必须都有占位符, 且替换后不得残留 (表与模板不齐 = OpTable 的 bug)
std::string subst_operand(const OperatorRow &r) {
  std::string s = r.operand;
  for_each_param(r, [&](const char *name, double v) {
    char tok[16], val[32];
    std::snprintf(tok, sizeof(tok), "⟨%s⟩", name);
    std::snprintf(val, sizeof(val), "%g", v);
    size_t pos = s.find(tok);
    assert(pos != std::string::npos && "OpTable operand 列缺该参数的占位符");
    for (; pos != std::string::npos; pos = s.find(tok, pos))
      s.replace(pos, std::strlen(tok), val);
  });
  assert(s.find("⟨") == std::string::npos && "operand 占位符未被参数列覆盖");
  return s;
}

const char *T_name(factor::T t) {
  switch (t) {
  case factor::T::POINT:
    return "POINT";
  case factor::T::EXPAND:
    return "EXPAND";
  case factor::T::ROLL:
    return "ROLL";
  case factor::T::EXPO:
    return "EXPO";
  }
  return "?";
}

const char *A_name(factor::A a) {
  switch (a) {
  case factor::A::SELF:
    return "SELF";
  case factor::A::ALL:
    return "ALL";
  case factor::A::GROUP:
    return "GROUP";
  }
  return "?";
}

const char *kern_name(factor::Kern k) {
  switch (k) {
  case factor::Kern::MAP:
    return "MAP";
  case factor::Kern::SHIFT:
    return "SHIFT";
  case factor::Kern::MOMENT:
    return "MOMENT";
  case factor::Kern::EXTREME:
    return "EXTREME";
  case factor::Kern::ORDER:
    return "ORDER";
  case factor::Kern::RECUR:
    return "RECUR";
  }
  return "?";
}

// 时间列 (cpu / gpu 各自带对拍结论 cpu_chk / gpu_chk; stream 列是 golden, 无对拍):
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

// 三维分类列的落盘文本: 不在分类里的行 (Stat / Search) 空着 (out 列所有行都有: 表格内按 dom_tex 渲染, JSON 落 dom_name)
const char *cls_T(const OperatorRow &r) { return r.classified ? T_name(r.T) : ""; }
const char *cls_A(const OperatorRow &r) { return r.classified ? A_name(r.A) : ""; }
const char *cls_kern(const OperatorRow &r) { return r.classified ? kern_name(r.kern) : ""; }

// 悬停 e_name: 静态详述 (doc) + 跑完后跑手的附带文本 (detail); 两者都空 → 不弹
void row_tooltip(const OperatorRow &r) {
  const bool has_detail = r.status == RowStatus::Done && !r.detail.empty();
  if (r.doc[0] == '\0' && !has_detail)
    return;
  ImGui::BeginTooltip();
  if (r.doc[0] != '\0')
    ImGui::TextUnformatted(r.doc);
  if (has_detail) {
    if (r.doc[0] != '\0')
      ImGui::Separator();
    ImGui::TextUnformatted(r.detail.c_str());
  }
  ImGui::EndTooltip();
}

} // namespace

int RenderTabOperators(OperatorsService &svc, OperatorsUIState &ui) {
  int action = 0;
  const Phase st = svc.phase.load(std::memory_order_acquire);
  const bool running = svc.busy();

  // ==========================================================================
  // 1. 张量 [times × assets] + Run / Cancel + 进度
  // ==========================================================================
  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "1. Tensor:");
  ImGui::SameLine();
  ImGui::SetNextItemWidth(80);
  ImGui::InputInt("Times", &ui.req.times, 0);
  if (ImGui::IsItemHovered())
    ImGui::SetTooltip("时间轴长度 (无单位)\n"
                      "• 自动取整到 %d 的倍数\n"
                      "• 段界对齐是 EXPAND 的前提",
                      factor::kSegMax);
  ImGui::SameLine();
  ImGui::Text("×");
  ImGui::SameLine();
  ImGui::SetNextItemWidth(70);
  ImGui::InputInt("Assets", &ui.req.A, 0);
  if (ImGui::IsItemHovered())
    ImGui::SetTooltip("资产轴宽度\n"
                      "• GPU 后端一线程一资产, 设计点 5000\n"
                      "• 太小喂不满卡, 吞吐不公允");
  ui.req.times = std::max(ui.req.times, 2 * factor::kSegMax); // fill 造数要 ≥ 2 段
  ui.req.times -= ui.req.times % factor::kSegMax;
  ui.req.A = std::max(ui.req.A, 2);
  ImGui::SameLine();
  {
    double bytes = double(ui.req.times) * ui.req.A * (sizeof(float) + sizeof(uint8_t)); // factor::Plane: v + m
    const char *units[] = {"B", "KiB", "MiB", "GiB"};
    int u = 0;
    while (u < 3 && bytes >= 1024.0)
      bytes /= 1024.0, ++u;
    ImGui::TextDisabled("= %.1f %s", bytes, units[u]);
  }
  if (ImGui::IsItemHovered())
    ImGui::SetTooltip("PLAIN 造数\n"
                      "• 配方按算子\n"
                      "    ◦ 正值型给对数正态\n"
                      "    ◦ 分组型给整数 id\n"
                      "• seed 取时间戳 (不做复现)\n"
                      "• d / k 每算子自带默认值 (见 operand 列; Check.hpp kDParams / kKParams)");
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
  const int done = static_cast<int>(svc.done.load(std::memory_order_relaxed)), total = static_cast<int>(svc.total.load(std::memory_order_relaxed)),
            failed = svc.failed();
  switch (st) {
  case Phase::Idle:
  case Phase::Scanning:
  case Phase::Loading:
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Muted), "idle");
    break;
  case Phase::Running:
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Busy), "running %d/%d", done, total);
    break;
  case Phase::Done:
    ImGui::TextColored(StatusColor(failed ? TaskStatus::Kind::Error : TaskStatus::Kind::Ready), "done %d/%d, %d FAIL",
                       done, total, failed);
    break;
  case Phase::Cancelled:
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "cancelled %d/%d", done, total);
    break;
  }
  ImGui::SameLine();
  if (const char *gname = factor::gpu::device_name())
    ImGui::TextDisabled("| GPU: %s", gname);
  else
    ImGui::TextDisabled("| GPU: off (无 CUDA 设备或未编译: cmake -DGPU_BACKEND=CUDA)");

  ImGui::Separator();

  // ==========================================================================
  // 2. 表
  // ==========================================================================
  // 短锁拷快照 (~90 行小结构, worker 只在行末短锁写)
  static std::vector<OperatorRow> s_rows;
  OperatorsRequest cur;
  bool from_json = false;
  {
    std::lock_guard<std::mutex> lock(svc.mutex);
    s_rows = svc.rows;
    cur = svc.current;
    from_json = svc.from_json;
  }
  const int n = static_cast<int>(s_rows.size());
  const auto count_A = [&](factor::A a) {
    return (int)std::count_if(s_rows.begin(), s_rows.end(), [a](const OperatorRow &r) { return r.classified && r.A == a; });
  };

  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "2. Operators:");
  ImGui::SameLine();
  const int n_eval = (int)std::count_if(s_rows.begin(), s_rows.end(), [](const OperatorRow &r) { return !r.classified; });
  ImGui::Text("%d (SELF %d / ALL %d / GROUP %d + 评估 %d)", n, count_A(factor::A::SELF), count_A(factor::A::ALL), count_A(factor::A::GROUP),
              n_eval);
  if (st != Phase::Idle) {
    ImGui::SameLine();
    ImGui::TextDisabled(from_json ? "(from operators.json: %d × %d)" : "(last run: %d × %d)", cur.times, cur.A);
    if (from_json && ImGui::IsItemHovered())
      ImGui::SetTooltip("表内容来自上次跑完落盘的 operators.json\n"
                        "• 本进程没算; 静态列已逐行校验过\n"
                        "• 按 Run 才重算, 跑完覆盖该文件");
  }

  // 列宽贴合: 发布代变了 (限速 0.5s) → 连发 2 帧 AutoAll (全行每帧都提交, 测量完整, 不必像 TabFeature 那样等 clipper)
  {
    const uint64_t ep = svc.epoch.load(std::memory_order_acquire);
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
    ImGui::TableSetupColumn("#", ImGuiTableColumnFlags_WidthFixed);                                       // 0
    ImGui::TableSetupColumn("e_name", ImGuiTableColumnFlags_WidthFixed);                                  // 1
    ImGui::TableSetupColumn("c_name", ImGuiTableColumnFlags_WidthFixed | ImGuiTableColumnFlags_NoSort);   // 2
    ImGui::TableSetupColumn("Ar", ImGuiTableColumnFlags_WidthFixed);                                      // 3
    ImGui::TableSetupColumn("T", ImGuiTableColumnFlags_WidthFixed);                                       // 4
    ImGui::TableSetupColumn("A", ImGuiTableColumnFlags_WidthFixed);                                       // 5
    ImGui::TableSetupColumn("Kernel", ImGuiTableColumnFlags_WidthFixed);                                  // 6
    ImGui::TableSetupColumn("operand", ImGuiTableColumnFlags_WidthFixed | ImGuiTableColumnFlags_NoSort);  // 7
    ImGui::TableSetupColumn("out", ImGuiTableColumnFlags_WidthFixed);                                     // 8
    ImGui::TableSetupColumn("operator", ImGuiTableColumnFlags_WidthFixed | ImGuiTableColumnFlags_NoSort); // 9
    ImGui::TableSetupColumn("Stream ms", ImGuiTableColumnFlags_WidthFixed);                               // 10
    ImGui::TableSetupColumn("CPU ms", ImGuiTableColumnFlags_WidthFixed);                                  // 11
    ImGui::TableSetupColumn("GPU ms", ImGuiTableColumnFlags_WidthFixed);                                  // 12
    ImGui::TableSetupColumn("Note", ImGuiTableColumnFlags_WidthFixed | ImGuiTableColumnFlags_NoSort);     // 13
    ImGui::TableSetupScrollFreeze(0, 1);

    if (ui.fit_frames > 0) {
      ui.fit_frames--;
      ImGuiTable *table = ImGui::GetCurrentTable();
      assert(table);
      ImGui::TableSetColumnWidthAutoAll(table);
    }

    ImGui::TableNextRow(ImGuiTableRowFlags_Headers);
    const char *headers[kNumCols] = {"#", "e_name", "c_name", "Ar", "T", "A", "Kernel", "operand", "out", "operator",
                                     "Stream ms", "CPU ms", "GPU ms", "Note"};
    const char *tooltips[kNumCols] = {
        // #
        "全局 idx (= operators.json 的 idx, code / UI 共用)\n"
        "• 顺序: 评估算子 (Stat, Search) 在前, OpTable 算子在后\n"
        "• OpTable 内: 元数 → A 域 (Ts 先 Cs 后) → T 窗 (Ts) / ALL→GROUP (Cs) → 核类 → 名字\n"
        "• 默认按此列排; 点表头三态切换 (升 / 降 / 默认)",
        // e_name
        "英文名 = 域_核_窗\n"
        "• 域: Ts (SELF) / Cs (ALL, GROUP)\n"
        "• 窗: Cum 段内 expanding / Roll 最近 d 期 / Ema 指数递推 / 无后缀 逐点",
        // c_name
        "中文名, 与 e_name 逐段对应\n"
        "• 域: 时 (Ts) / 截 (Cs·ALL) / 组 (Cs·GROUP)\n"
        "• 窗: 累 (Cum) / 滚 (Roll) / 指 (Ema) / 逐点无窗字\n"
        "• 核: 直译",
        // Ar
        "元数 = 输入序列数 (0..3)\n"
        "• 即 operand 里分号前的 x, y, z\n"
        "• d / k 是参数, 不算元",
        // T
        "T 窗 = 时间支撑\n"
        "• POINT   当前点\n"
        "• EXPAND  段内 expanding (段界 reset)\n"
        "• ROLL    最近 d 期 (跨段)\n"
        "• EXPO    指数加权全历史\n"
        "• 截面算子恒 POINT",
        // A
        "A 域 = 资产支撑\n"
        "• SELF   只看本资产\n"
        "• ALL    同一时刻全截面\n"
        "• GROUP  同一时刻组内 (组 id 由 y / z 给)",
        // Kernel
        "核类 = 统计核的代数类\n"
        "• MAP      逐元素\n"
        "• SHIFT    下标平移\n"
        "• MOMENT   可和分解 (矩族)\n"
        "• EXTREME  极值 / arg 族\n"
        "• ORDER    序统计\n"
        "• RECUR    递推\n"
        "• 复合算子标最重一级\n"
        "• 详见 factor/Contract.hpp【分类】",
        // operand
        "签名: 自变量在前, 参数在后 (x, y, z → d → k → k2)\n"
        "• x y z  输入序列 (值 + 有效位), 值域 = in 列\n"
        "• t_D    段内分钟位置 (仅 TodMask)\n"
        "• d      窗长 / 滞后 (分钟)\n"
        "    ◦ 仅 T = ROLL; 值取 Check.hpp kDParams\n"
        "• k      阈值 / 桶数 / EMA 系数 / 分位\n"
        "    ◦ 仅 k域 ≠ NONE; 值取 kKParams\n"
        "• k2     第二阈值\n"
        "    ◦ 仅 k域 = TOD\n"
        "• = 号后是本轮实际值\n"
        "• in 为严格域 (INT) 时 parser 要求上游 out ⊆ in; 其余 in 只是语义声明",
        // out
        "因变量值域 (enum Dom, 与 in 同一套; 上游 out = 下游 in)\n"
        "• REAL ℝ / NONNEG ℝ≥0 / POS ℝ>0\n"
        "• UNIT [0,1] / SIGNED [−1,1]\n"
        "• BIN {0,1} / SIGN3 {−1,0,1} / INT {0..1023}\n"
        "• BCAST ℝ 截面广播\n"
        "• 规则\n"
        "    ◦ 因子根不收离散 (BIN / SIGN3 / INT) 与 BCAST\n"
        "    ◦ 组 id 元只收 BIN / INT\n"
        "    ◦ 透传与取大取小一律写 REAL",
        // operator
        "算子定义 (OpTable.hpp, LaTeX); 符号沿用 features/FeaturesDefine.hpp\n"
        "• x_t / x_a  本资产 t 分钟值 / 同时刻资产 a 的值; t_D 段内位置\n"
        "• W_t        窗: EXPAND {s: 同日, s ≤ t}; ROLL {s: t−d < s ≤ t}\n"
        "• n / N      窗内 / 截面有效样本数\n"
        "• μ_t σ_t    窗内均值 / 样本标准差 (ddof=1); m_k k 阶中心矩\n"
        "• Σ Π        窗内求和 / 乘积 (变量 s 或 b); max min cov var corr 下标 = 取值域\n"
        "• pct(v; S)  并列均秩 pct rank ∈ [0,1]; Q_p(S) p 分位; Φ⁻¹ 正态分位; G(a) 组\n"
        "• 只计有效样本\n"
        "• 序统计族三后端同用 256 桶近似",
        // Stream ms
        "stream (实盘路径, 逐资产逐点 push) 的 wall time\n"
        "• OpTable 算子的 golden: 正确性基准 + 耗时上界\n"
        "• 评估算子无此后端 → n/a",
        // CPU ms
        "cpu 整张量批算的 wall time (不含造数)\n"
        "• 对拍 cpu_chk\n"
        "    ◦ OpTable 对 stream: 掩码相等, |Δ| ≤ 1e-5 + 1e-4·max\n"
        "    ◦ Search 对 Stat CPU (+ 分块 ≡ 整块)\n"
        "    ◦ Stat 自身即 golden\n"
        "• 没过 → 红 FAIL + Δ, 不显示时间\n"
        "• 比 stream 慢 → ms 标红 (要求 stream ≥ cpu ≥ gpu)",
        // GPU ms
        "GPU 纯 kernel 耗时 (cudaEvent, 已热身; 不含 cudaMalloc / H2D / D2H)\n"
        "• 对拍 gpu_chk: gpu 对 cpu\n"
        "    ◦ OpTable: |Δ| ≤ 1e-3 + 1e-3·max (CsNormRank / 高阶矩放宽)\n"
        "    ◦ 评估算子: |Δ| ≤ 1e-5 + 1e-4·max\n"
        "• 没过 → 红 FAIL + Δ, 不显示时间\n"
        "• 比 cpu 慢 → ms 标红",
        // Note
        "用途与选型一句话 (给 agent 检索)\n"
        "• 写: 量什么; 怎么用 / 配什么\n"
        "• 不含实现 / 近似 / 退化细节\n"
        "• 退化与数值契约 → factor/Contract.hpp; 成本与误差 → operator.md",
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
    // 默认序 = 全局 idx (行序: Stat 首行, 后接 OpTable 表序, 不另排); 用户选列时在此序上 stable_sort
    std::vector<int> order(n);
    std::iota(order.begin(), order.end(), 0);
    if (ui.sort_column >= 0) {
      auto cmp3 = [](double x, double y) { return x < y ? -1 : (x > y ? 1 : 0); };
      std::stable_sort(order.begin(), order.end(), [&](int a, int b) {
        const OperatorRow &ra = s_rows[a], &rb = s_rows[b];
        int cmp = 0;
        switch (ui.sort_column) {
        case 0:
          cmp = a - b;
          break;
        case 1:
          cmp = std::strcmp(ra.e_name, rb.e_name);
          break;
        case 3:
          cmp = ra.arity - rb.arity;
          break;
        case 4:
          cmp = (int)ra.T - (int)rb.T;
          break;
        case 5:
          cmp = (int)ra.A - (int)rb.A;
          break;
        case 6:
          cmp = (int)ra.kern - (int)rb.kern;
          break;
        case 8:
          cmp = (int)ra.out - (int)rb.out;
          break;
        case 10:
          cmp = cmp3(sort_ms(ra, ra.stream_ms), sort_ms(rb, rb.stream_ms));
          break;
        case 11:
          cmp = cmp3(sort_ms_chk(ra, ra.cpu_ms, ra.cpu_chk), sort_ms_chk(rb, rb.cpu_ms, rb.cpu_chk));
          break;
        case 12:
          cmp = cmp3(sort_ms_chk(ra, ra.gpu_ms, ra.gpu_chk), sort_ms_chk(rb, rb.gpu_ms, rb.gpu_chk));
          break;
        }
        return ui.sort_ascending ? cmp < 0 : cmp > 0;
      });
    }

    // operand 列: 模板占位符 → 本轮实际参数值. 只建一次 (默认参数是编译期常量, 不随轮变);
    // string 常驻不再动, c_str 指针稳定, 供 Latex::Get 按指针缓存
    static std::vector<std::string> s_operand;
    if (s_operand.empty()) {
      s_operand.reserve(s_rows.size());
      for (const OperatorRow &r : s_rows)
        s_operand.push_back(subst_operand(r));
    }
    assert(s_operand.size() == s_rows.size());

    // 全行提交 (~90 行, 不需要 clipper; 贴合帧也因此测量完整)
    for (int idx : order) {
      const OperatorRow &r = s_rows[idx];
      ImGui::TableNextRow();
      ImGui::TableSetColumnIndex(0);
      ImGui::TextDisabled("%d", idx); // 全局 idx = operators.json 的 idx (评估算子在前, 之后 = OpTable 表序)
      ImGui::TableSetColumnIndex(1);
      ImGui::TextColored(StatusColor(r.status == RowStatus::Done
                                         ? ((r.cpu_chk.ok() && (r.gpu_ms < 0 || r.gpu_chk.ok())) ? TaskStatus::Kind::Ready
                                                                                                 : TaskStatus::Kind::Error)
                                         : (r.status == RowStatus::Running ? TaskStatus::Kind::Busy : TaskStatus::Kind::Muted)),
                         "%s", r.e_name);
      if (ImGui::IsItemHovered())
        row_tooltip(r);
      ImGui::TableSetColumnIndex(2);
      ImGui::TextUnformatted(r.c_name);
      ImGui::TableSetColumnIndex(3);
      ImGui::Text("%d", r.arity);
      ImGui::TableSetColumnIndex(4);
      ImGui::TextUnformatted(cls_T(r));
      ImGui::TableSetColumnIndex(5);
      ImGui::TextUnformatted(cls_A(r));
      ImGui::TableSetColumnIndex(6);
      ImGui::TextUnformatted(cls_kern(r));
      ImGui::TableSetColumnIndex(7);
      if (tex::TeXRender *render = Latex::Get(s_operand[idx].c_str(), kFormulaTextSize))
        Latex::Draw(render, ImGui::GetTextLineHeight());
      else
        ImGui::TextUnformatted(s_operand[idx].c_str()); // 解析失败回退原文
      ImGui::TableSetColumnIndex(8);
      if (tex::TeXRender *render = Latex::Get(factor::dom_tex(r.out), kFormulaTextSize))
        Latex::Draw(render, ImGui::GetTextLineHeight());
      else
        ImGui::TextUnformatted(factor::dom_name(r.out)); // 解析失败回退枚举名
      if (ImGui::IsItemHovered())
        ImGui::SetTooltip("%s", factor::dom_name(r.out));
      ImGui::TableSetColumnIndex(9);
      if (tex::TeXRender *render = Latex::Get(r.op, kFormulaTextSize))
        Latex::Draw(render, ImGui::GetTextLineHeight());
      else
        ImGui::TextUnformatted(r.op); // 解析失败回退原文
      ImGui::TableSetColumnIndex(10);
      render_time_cell(r, r.stream_ms, nullptr, false); // golden: 无对拍, 无快慢判据 (评估算子无 stream → n/a)
      ImGui::TableSetColumnIndex(11);
      render_time_cell(r, r.cpu_ms, &r.cpu_chk, r.stream_ms >= 0 && r.cpu_ms > r.stream_ms);
      ImGui::TableSetColumnIndex(12);
      render_time_cell(r, r.gpu_ms, &r.gpu_chk, r.gpu_ms > r.cpu_ms);
      ImGui::TableSetColumnIndex(13);
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
    const char *gname = factor::gpu::device_name();
    file << "{\n";
    file << " \"tensor\": {\"times\": " << cur.times << ", \"assets\": " << cur.A << "},\n";
    file << " \"gpu\": " << (gname ? json(gname).dump() : "false") << ",\n";
    file << " \"status\": " << json(PhaseName(svc.phase.load())).dump() << ", \"done\": " << svc.done.load()
         << ", \"total\": " << svc.total.load() << ", \"failed\": " << svc.failed() << ",\n";
    file << " \"tol\": {\"cpu_chk\": \"cpu 对 golden (OpTable 行 = stream; Search = Stat CPU + 分块 ≡ 整块; Stat 无): |Δ| ≤ 1e-5 + 1e-4·max(|a|,|b|), mask / 计数逐位相等\","
            " \"gpu_chk\": \"gpu 对 cpu: OpTable 行 |Δ| ≤ 1e-3 + 1e-3·max (CsNormRank / 三四阶矩单独放宽); 评估算子 |Δ| ≤ 1e-5 + 1e-4·max\"},\n";

    file << " \"rows\": [\n";
    const size_t n = svc.rows.size();
    for (size_t i = 0; i < n; ++i) {
      const OperatorRow &r = svc.rows[i];
      // 键序 = 表格列序 (operand 落原始模板, 占位符 ⟨d⟩⟨k⟩⟨k2⟩; 实际值在 params 键)
      nlohmann::ordered_json j;
      j["idx"] = i;
      j["e_name"] = r.e_name;
      j["c_name"] = r.c_name;
      j["arity"] = r.arity;
      j["T"] = cls_T(r);
      j["A"] = cls_A(r);
      j["kernel"] = cls_kern(r);
      j["operand"] = r.operand;
      nlohmann::ordered_json params = nlohmann::ordered_json::object();
      for_each_param(r, [&](const char *name, double v) { params[name] = v; });
      j["params"] = params;
      j["out"] = factor::dom_name(r.out);
      j["operator"] = r.op;
      j["note"] = r.note;
      j["status"] = row_status_name(r.status);
      // 动态列只落跑完的行 (表格显示 "…" 的格子不落键); 没有的后端 (ms < 0) 不落其耗时 / 对拍键; detail 非空才落
      if (r.status == RowStatus::Done) {
        const bool has_stream = r.stream_ms >= 0, has_gpu = r.gpu_ms >= 0;
        j["cpu_chk"] = diff_json(r.cpu_chk);
        if (has_gpu)
          j["gpu_chk"] = diff_json(r.gpu_chk);
        j["cpu_ms"] = sig4(r.cpu_ms);
        if (has_stream)
          j["stream_ms"] = sig4(r.stream_ms);
        if (has_gpu) {
          j["gpu_ms"] = sig4(r.gpu_ms);
          if (has_stream && r.gpu_ms > 0)
            j["stream_over_gpu"] = sig4(r.stream_ms / r.gpu_ms);
        }
        if (!r.detail.empty())
          j["detail"] = r.detail;
      }
      file << "  " << j.dump() << (i + 1 < n ? ",\n" : "\n");
    }
    file << " ]\n";
    file << "}\n";
    assert(file.good() && "operators.json: 写入失败");
  }
  std::filesystem::rename(tmp, path); // 原子替换: 崩在中途不留半截文件 (同 features.json)
}

// ============================================================================
// 算子表载入 JSON (Save 的逆): 只吃整轮跑完 + 静态列对得上当前 OpTable 的快照, 别的一概判废
// ============================================================================

namespace {

// 文件是上次运行留下的 (可能来自旧代码 / 半截崩的), 故 parse 关异常, 取值全走 find + 类型判定 (StatJson.hpp json_*):
// 任一环节不合就整份判废 (调用方删了重算), 不做部分接受

bool load_diff(const nlohmann::json &j, factor::check::Diff &out) {
  if (!j.is_object())
    return false;
  factor::check::Diff d;
  if (!json_int(j, "mask_bad", d.mask_bad) || !json_int(j, "val_bad", d.val_bad) || !json_int(j, "compared", d.compared) ||
      !json_num(j, "worst", d.worst))
    return false;
  d.worst_at = -1; // 落盘不记下标 (只 op_check 的 CLI 用), ok() 由 mask_bad / val_bad 现推
  out = d;
  return true;
}

// 一行: 静态列逐个对当前 OpTable, 对上了才把动态列灌进 dst (dst 进来时是 rows[i] 的拷贝).
// 参数比实际值不只比键名: kDParams / kKParams 改过 → 旧耗时作废. operand / operator 比 (语义列, 改了
// 即算子含义变); c_name / note 不比 (纯文字, 改错别字不该作废整表)
bool load_row(const nlohmann::json &j, OperatorRow &dst) {
  if (!j.is_object())
    return false;
  if (!json_str_is(j, "e_name", dst.e_name) || !json_str_is(j, "T", cls_T(dst)) || !json_str_is(j, "A", cls_A(dst)) ||
      !json_str_is(j, "kernel", cls_kern(dst)) || !json_str_is(j, "operand", dst.operand.c_str()) || !json_str_is(j, "out", factor::dom_name(dst.out)) ||
      !json_str_is(j, "operator", dst.op))
    return false;
  int arity = -1;
  if (!json_int(j, "arity", arity) || arity != dst.arity)
    return false;
  const auto pit = j.find("params");
  if (pit == j.end() || !pit->is_object())
    return false;
  size_t np = 0;
  bool param_ok = true;
  for_each_param(dst, [&](const char *name, double v) { // dst.param = 构造期填的当轮默认
    double got = 0;
    param_ok = param_ok && json_num(*pit, name, got) && got == v;
    np++;
  });
  if (!param_ok || pit->size() != np)
    return false;
  // 动态列: 只认跑完的行 (取消/半截留下的 pending 行 → 整份判废)
  if (!json_str_is(j, "status", "done"))
    return false;
  if (!json_num(j, "cpu_ms", dst.cpu_ms))
    return false;
  const auto cit = j.find("cpu_chk"); // 每行必有 (Stat 落的是空 Diff)
  if (cit == j.end() || !load_diff(*cit, dst.cpu_chk))
    return false;
  // stream 后端是静态的: 分类内的行必有, 评估算子行必无
  if (dst.classified) {
    if (!json_num(j, "stream_ms", dst.stream_ms) || dst.stream_ms < 0)
      return false;
  } else {
    if (j.find("stream_ms") != j.end())
      return false;
    dst.stream_ms = -1;
  }
  const auto git = j.find("gpu_chk"); // 落盘那轮没 GPU 后端 → 无 gpu_* 键, 载入后 GPU 列显示 n/a
  if (git == j.end()) {
    if (j.find("gpu_ms") != j.end())
      return false;
    dst.gpu_chk = {}, dst.gpu_ms = -1;
  } else if (!load_diff(*git, dst.gpu_chk) || !json_num(j, "gpu_ms", dst.gpu_ms) || dst.gpu_ms < 0) {
    return false;
  }
  const auto dit = j.find("detail"); // 跑手的附带文本, 可无
  if (dit == j.end()) {
    dst.detail.clear();
  } else if (!dit->is_string()) {
    return false;
  } else {
    dst.detail = dit->get<std::string>();
  }
  dst.status = RowStatus::Done;
  return true;
}

} // namespace

bool LoadOperatorTableJson(const std::string &factor_dir, OperatorsService &svc, OperatorsUIState &ui) {
  assert(svc.phase.load() == Phase::Idle && "载入只在进页第一帧 (一轮都没跑过) 做");
  const std::filesystem::path path = std::filesystem::path(factor_dir) / "operators.json";
  std::ifstream file(path);
  if (!file.is_open())
    return false; // 没落过盘: 无文件可删, 调用方直接起算一轮

  OperatorsRequest req;
  std::vector<OperatorRow> snap;
  int failed = 0;
  const bool ok = [&] {
    const nlohmann::json j = nlohmann::json::parse(file, nullptr, /*allow_exceptions=*/false);
    if (j.is_discarded() || !j.is_object())
      return false;
    if (!json_str_is(j, "status", "done")) // cancelled / running 的半截快照不吃
      return false;
    int total = 0, done = 0;
    if (!json_int(j, "total", total) || !json_int(j, "done", done) || total != static_cast<int>(svc.rows.size()) || done != total)
      return false;
    const auto tit = j.find("tensor");
    if (tit == j.end() || !tit->is_object() || !json_int(*tit, "times", req.times) || !json_int(*tit, "assets", req.A))
      return false;
    if (req.times < 2 * factor::kSegMax || req.times % factor::kSegMax != 0 || req.A < 2) // Request 的前置条件
      return false;
    const auto rit = j.find("rows");
    if (rit == j.end() || !rit->is_array() || rit->size() != svc.rows.size())
      return false;
    snap = svc.rows; // 静态列 + 构造期默认参数原样带过去, load_row 只改动态列
    for (size_t i = 0; i < snap.size(); ++i)
      if (!load_row((*rit)[i], snap[i]))
        return false;
    for (const OperatorRow &r : snap) // 表头的 failed 不信, 按行现算
      if (!r.cpu_chk.ok() || (r.gpu_ms >= 0 && !r.gpu_chk.ok()))
        failed++;
    return true;
  }();
  if (!ok) {
    file.close();
    std::filesystem::remove(path); // 不完整 / 与当前 OpTable 不一致: 删掉, 调用方重算后落新的
    return false;
  }
  svc.AdoptSnapshot(req, std::move(snap), failed);
  ui.req.times = req.times, ui.req.A = req.A; // 页面参数跟上快照 (Run 默认按同一形状重跑)
  return true;
}

} // namespace GUI::Factors
