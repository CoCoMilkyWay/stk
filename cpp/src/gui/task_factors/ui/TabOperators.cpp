// Tab Operators — 见头文件
#include "gui/task_factors/ui/TabOperators.hpp"
#include "factor/GpuRun.hpp"  // device_name: GPU 型号显示
#include "gui/Tasks.hpp"      // StatusColor: 全局色表, 行状态着色不自配颜色
#include "gui/util/Latex.hpp" // Operands / Operator 列 LaTeX 渲染 (与特征表共用缓存)

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
  // 1. 张量 [times × assets] + Run / Cancel + 进度
  // ==========================================================================
  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "1. Tensor:");
  ImGui::SameLine();
  ImGui::SetNextItemWidth(80);
  ImGui::InputInt("Times", &ui.req.times, 0);
  if (ImGui::IsItemHovered())
    ImGui::SetTooltip("时间轴长度 (无单位), 自动取整到 %d 的倍数 (段界对齐是 EXPAND 的前提)",
                      factor::kSegLen);
  ImGui::SameLine();
  ImGui::Text("×");
  ImGui::SameLine();
  ImGui::SetNextItemWidth(70);
  ImGui::InputInt("Assets", &ui.req.A, 0);
  if (ImGui::IsItemHovered())
    ImGui::SetTooltip("资产轴宽度. GPU 后端一线程一资产, 设计点 5000: 太小喂不满卡, 吞吐不公允");
  ui.req.times = std::max(ui.req.times, factor::kSegLen);
  ui.req.times -= ui.req.times % factor::kSegLen;
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
    ImGui::SetTooltip("PLAIN 造数, 配方按算子 (正值型给对数正态, 分组型给整数 id); seed 取时间戳 (不做复现)\n"
                      "d / k 每算子自带默认值 (见 Args 列; Check.hpp kDParams / kKParams)");
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
  if (const char *gname = factor::gpu::device_name())
    ImGui::TextDisabled("| GPU: %s", gname);
  else
    ImGui::TextDisabled("| GPU: off (无 CUDA 设备或未编译: cmake -DFACTOR_CUDA=ON)");

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

  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "2. Operators:");
  ImGui::SameLine();
  ImGui::Text("%d (SELF %d / ALL %d / GROUP %d)", n,
              (int)std::count_if(s_rows.begin(), s_rows.end(), [](const OperatorRow &r) { return r.A == factor::A::SELF; }),
              (int)std::count_if(s_rows.begin(), s_rows.end(), [](const OperatorRow &r) { return r.A == factor::A::ALL; }),
              (int)std::count_if(s_rows.begin(), s_rows.end(), [](const OperatorRow &r) { return r.A == factor::A::GROUP; }));
  if (st != OperatorsStatus::Idle) {
    ImGui::SameLine();
    ImGui::TextDisabled(from_json ? "(from operators.json: %d × %d)" : "(last run: %d × %d)", cur.times, cur.A);
    if (from_json && ImGui::IsItemHovered())
      ImGui::SetTooltip("表内容是上次跑完落盘的 operators.json (静态列已逐行校验过), 本进程没算;\n"
                        "按 Run 才重算, 跑完覆盖该文件");
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
  constexpr int kNumCols = 13;
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
    ImGui::TableSetupColumn("operator", ImGuiTableColumnFlags_WidthFixed | ImGuiTableColumnFlags_NoSort); // 8
    ImGui::TableSetupColumn("Stream ms", ImGuiTableColumnFlags_WidthFixed);                               // 9
    ImGui::TableSetupColumn("CPU ms", ImGuiTableColumnFlags_WidthFixed);                                  // 10
    ImGui::TableSetupColumn("GPU ms", ImGuiTableColumnFlags_WidthFixed);                                  // 11
    ImGui::TableSetupColumn("Note", ImGuiTableColumnFlags_WidthFixed | ImGuiTableColumnFlags_NoSort);     // 12
    ImGui::TableSetupScrollFreeze(0, 1);

    if (ui.fit_frames > 0) {
      ui.fit_frames--;
      ImGuiTable *table = ImGui::GetCurrentTable();
      assert(table);
      ImGui::TableSetColumnWidthAutoAll(table);
    }

    ImGui::TableNextRow(ImGuiTableRowFlags_Headers);
    const char *headers[kNumCols] = {"#", "e_name", "c_name", "Ar", "T", "A", "Kernel", "operand", "operator",
                                     "Stream ms", "CPU ms", "GPU ms", "Note"};
    const char *tooltips[kNumCols] = {
        "全局 idx = OpTable 表序 = operators.json 的 idx (code 与 UI 统一按它):\n"
        "元数 → A 域 (Ts 前 Cs 后) → TS 按 T 窗 / CS 按 ALL→GROUP → 核类 → 名字字母序\n"
        "默认排序即此列; 点表头按列排 (三态, 第三态回默认)",
        "英文名: 域_核_窗 (Ts = SELF, Cs = ALL/GROUP). Cum = 段内 expanding, Roll = 最近 d 期, Ema = 指数递推, 无后缀 = 逐点",
        "中文名, 与 e_name 逐段对应 (固定规则, 无冗余字):\n"
        "域 时 (Ts) / 截 (Cs·ALL) / 组 (Cs·GROUP) + 窗 累 (Cum) / 滚 (Roll) / 指 (Ema), 逐点无窗字 + 核",
        "元数: 输入序列数 0..3 (operand 里分号前的部分); d / k 是参数不算元",
        "T 窗 (时间支撑): POINT 当前点 / EXPAND 段内 expanding (段界 reset) / ROLL 最近 d 期 (跨段) / EXPO 指数加权全历史\n"
        "截面算子恒 POINT (只看当前时刻)",
        "A 域 (资产支撑): SELF 只看本资产 / ALL 同一时刻全截面 / GROUP 同一时刻组内 (整数组 id 由 y 或 z 给)",
        "核类 (统计核的代数类): MAP 逐元素 / SHIFT 下标平移 / MOMENT 可和分解 (矩族) / EXTREME 极值及 arg 族 /\n"
        "ORDER 序统计 / RECUR 递推; 复合算子标主导 (最重) 一级. 三维分类见 factor/Contract.hpp【分类】",
        "签名与值域 (OpTable.hpp, LaTeX): 自变量在前, 参数在后, 顺序恒 x, y, z → d → k → k2, 每项都带值域\n"
        "  自变量  x, y, z = 按元数取的序列 (每格 值 + 有效位); t_D = 段内分钟位置 (TodMask 无序列输入)\n"
        "  参数    = 号后是本轮实际值 (OpTable 里是占位符 ⟨d⟩ ⟨k⟩ ⟨k2⟩, 渲染前替换), 每算子自带默认值:\n"
        "    d   窗长 / 滞后 (期 = 分钟), 来自 Check.hpp kDParams; 参数列不含 d 的算子固定 1 (与 op_check 同)\n"
        "    k   阈值 / 桶数 / EMA 系数 / 分位, 来自 Check.hpp kKParams\n"
        "    k2  第二阈值 (仅 TodMask 上界)",
        "算子定义 (OpTable.hpp, LaTeX). 符号继承 features/FeaturesDefine.hpp (t 分钟, D 交易日, 1[·] 指示), 算子库补充:\n"
        "  x_t 本资产 t 分钟值; x_a 同一时刻资产 a 的值; t_D 段内位置\n"
        "  W_t 窗 (由 T 列定): EXPAND {s: 同日, s ≤ t}; ROLL {s: t−d < s ≤ t}\n"
        "  n / N 窗内 / 截面有效样本数; μ_t σ_t 窗内均值 / 样本标准差 (ddof=1); m_k k 阶中心总体矩\n"
        "  Σ_{W_t} / Π_{W_t} 窗内求和 / 乘积 (求和变量恒是 s 或 b); max min cov var corr 的下标 = 取值域\n"
        "  pct(v; S) 并列均秩 pct rank ∈ [0,1]; Q_p(S) 样本集 S 的 p 分位; Φ⁻¹ 标准正态分位; G(a) 组 (整数 id 由 y / z 给)\n"
        "  所有 Σ / 计数 / 极值只计有效样本; 序统计族三后端同用 256 桶近似",
        "stream (实盘路径, 逐资产逐点 push) 的 wall time = golden: 正确性基准, 也是耗时上界",
        "cpu 整张量一次批算的 wall time (不含造数)\n"
        "  没过对拍 (掩码逐位相等且 |Δ| ≤ 1e-5 + 1e-4·max(|a|,|b|)) → 红色 FAIL + Δ, 不显示时间\n"
        "  过了但比 stream 慢 → ms 标红 (要求 stream ≥ cpu ≥ gpu)",
        "GPU 纯 kernel 耗时 (cudaEvent): 不含 cudaMalloc / H2D / D2H (搬运是对拍接口的成本, 不是算子的), 首次调用前已热身\n"
        "  没过对拍 (对 cpu: |Δ| ≤ 1e-3 + 1e-3·max, CsNormRank / 三四阶矩单独放宽) → 红色 FAIL + Δ, 不显示时间\n"
        "  过了但比 cpu 慢 → ms 标红",
        "用途与选型 (给 agent 检索的一句话, 统一 \"量什么; 怎么用 / 配什么\"): 不含实现 / 近似 / 退化细节\n"
        "退化与数值契约见 factor/Contract.hpp, 成本与误差见 operator.md",
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
    // 默认序 = 全局 idx (行序即 OpTable 表序, 不另排); 用户选列时在此序上 stable_sort
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
        case 9:
          cmp = cmp3(sort_ms(ra, ra.stream_ms), sort_ms(rb, rb.stream_ms));
          break;
        case 10:
          cmp = cmp3(sort_ms_chk(ra, ra.cpu_ms, ra.stream), sort_ms_chk(rb, rb.cpu_ms, rb.stream));
          break;
        case 11:
          cmp = cmp3(sort_ms_chk(ra, ra.gpu_ms, ra.gpu), sort_ms_chk(rb, rb.gpu_ms, rb.gpu));
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
      ImGui::TextDisabled("%d", idx); // 全局 idx = 行在 OpTable 的表序 = operators.json 的 idx
      ImGui::TableSetColumnIndex(1);
      ImGui::TextColored(StatusColor(r.status == RowStatus::Done
                                         ? ((r.stream.ok() && (r.gpu_ms < 0 || r.gpu.ok())) ? TaskStatus::Kind::Ready
                                                                                            : TaskStatus::Kind::Error)
                                         : (r.status == RowStatus::Running ? TaskStatus::Kind::Busy : TaskStatus::Kind::Muted)),
                         "%s", r.e_name);
      ImGui::TableSetColumnIndex(2);
      ImGui::TextUnformatted(r.c_name);
      ImGui::TableSetColumnIndex(3);
      ImGui::Text("%d", r.arity);
      ImGui::TableSetColumnIndex(4);
      ImGui::TextUnformatted(T_name(r.T));
      ImGui::TableSetColumnIndex(5);
      ImGui::TextUnformatted(A_name(r.A));
      ImGui::TableSetColumnIndex(6);
      ImGui::TextUnformatted(kern_name(r.kern));
      ImGui::TableSetColumnIndex(7);
      if (tex::TeXRender *render = Latex::Get(s_operand[idx].c_str(), kFormulaTextSize))
        Latex::Draw(render, ImGui::GetTextLineHeight());
      else
        ImGui::TextUnformatted(s_operand[idx].c_str()); // 解析失败回退原文
      ImGui::TableSetColumnIndex(8);
      if (tex::TeXRender *render = Latex::Get(r.op, kFormulaTextSize))
        Latex::Draw(render, ImGui::GetTextLineHeight());
      else
        ImGui::TextUnformatted(r.op); // 解析失败回退原文
      ImGui::TableSetColumnIndex(9);
      render_time_cell(r, r.stream_ms, nullptr, false); // golden: 无对拍, 无快慢判据
      ImGui::TableSetColumnIndex(10);
      render_time_cell(r, r.cpu_ms, &r.stream, r.cpu_ms > r.stream_ms);
      ImGui::TableSetColumnIndex(11);
      render_time_cell(r, r.gpu_ms, &r.gpu, r.gpu_ms > r.cpu_ms);
      ImGui::TableSetColumnIndex(12);
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
    const char *gname = factor::gpu::device_name();
    file << "{\n";
    file << " \"tensor\": {\"times\": " << cur.times << ", \"assets\": " << cur.A << "},\n";
    file << " \"gpu\": " << (gname ? json(gname).dump() : "false") << ",\n";
    file << " \"status\": " << json(status_name(svc.status())).dump() << ", \"done\": " << svc.done()
         << ", \"total\": " << svc.total() << ", \"failed\": " << svc.failed() << ",\n";
    file << " \"tol\": {\"stream\": \"|Δ| ≤ 1e-5 + 1e-4·max(|a|,|b|), mask 逐位相等\","
            " \"gpu\": \"|Δ| ≤ 1e-3 + 1e-3·max (CsNormRank / 三四阶矩单独放宽)\"},\n";

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
      j["T"] = T_name(r.T);
      j["A"] = A_name(r.A);
      j["kernel"] = kern_name(r.kern);
      j["operand"] = r.operand;
      nlohmann::ordered_json params = nlohmann::ordered_json::object();
      for_each_param(r, [&](const char *name, double v) { params[name] = v; });
      j["params"] = params;
      j["operator"] = r.op;
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

// ============================================================================
// 算子表载入 JSON (Save 的逆): 只吃整轮跑完 + 静态列对得上当前 OpTable 的快照, 别的一概判废
// ============================================================================

namespace {

// 文件是上次运行留下的 (可能来自旧代码 / 半截崩的), 故 parse 关异常, 取值全走 find + 类型判定:
// 任一环节不合就整份判废 (调用方删了重算), 不做部分接受
bool get_num(const nlohmann::json &j, const char *key, double &out) {
  const auto it = j.find(key);
  if (it == j.end() || !it->is_number())
    return false;
  out = it->get<double>();
  return true;
}

bool get_int(const nlohmann::json &j, const char *key, int &out) {
  const auto it = j.find(key);
  if (it == j.end() || !it->is_number_integer())
    return false;
  out = it->get<int>();
  return true;
}

bool str_is(const nlohmann::json &j, const char *key, const char *want) {
  const auto it = j.find(key);
  return it != j.end() && it->is_string() && it->get_ref<const std::string &>() == want;
}

bool load_diff(const nlohmann::json &j, factor::check::Diff &out) {
  if (!j.is_object())
    return false;
  factor::check::Diff d;
  if (!get_int(j, "mask_bad", d.mask_bad) || !get_int(j, "val_bad", d.val_bad) || !get_int(j, "compared", d.compared) ||
      !get_num(j, "worst", d.worst))
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
  if (!str_is(j, "e_name", dst.e_name) || !str_is(j, "T", T_name(dst.T)) || !str_is(j, "A", A_name(dst.A)) ||
      !str_is(j, "kernel", kern_name(dst.kern)) || !str_is(j, "operand", dst.operand) || !str_is(j, "operator", dst.op))
    return false;
  int arity = -1;
  if (!get_int(j, "arity", arity) || arity != dst.arity)
    return false;
  const auto pit = j.find("params");
  if (pit == j.end() || !pit->is_object())
    return false;
  size_t np = 0;
  bool param_ok = true;
  for_each_param(dst, [&](const char *name, double v) { // dst.param = 构造期填的当轮默认
    double got = 0;
    param_ok = param_ok && get_num(*pit, name, got) && got == v;
    np++;
  });
  if (!param_ok || pit->size() != np)
    return false;
  // 动态列: 只认跑完的行 (取消/半截留下的 pending 行 → 整份判废)
  if (!str_is(j, "status", "done"))
    return false;
  const auto sit = j.find("stream_vs_cpu");
  if (sit == j.end() || !load_diff(*sit, dst.stream) || !get_num(j, "cpu_ms", dst.cpu_ms) ||
      !get_num(j, "stream_ms", dst.stream_ms))
    return false;
  const auto git = j.find("gpu_vs_cpu"); // 落盘那轮没 GPU 后端 → 无 gpu_* 键, 载入后 GPU 列显示 n/a
  if (git == j.end()) {
    dst.gpu = {}, dst.gpu_ms = -1;
  } else if (!load_diff(*git, dst.gpu) || !get_num(j, "gpu_ms", dst.gpu_ms) || dst.gpu_ms < 0) {
    return false;
  }
  dst.status = RowStatus::Done;
  return true;
}

} // namespace

bool LoadOperatorTableJson(const std::string &factor_dir, OperatorsService &svc, OperatorsUIState &ui) {
  assert(svc.status() == OperatorsStatus::Idle && "载入只在进页第一帧 (worker 未起) 做");
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
    if (!str_is(j, "status", "done")) // cancelled / running 的半截快照不吃
      return false;
    int total = 0, done = 0;
    if (!get_int(j, "total", total) || !get_int(j, "done", done) || total != svc.total() || done != total)
      return false;
    const auto tit = j.find("tensor");
    if (tit == j.end() || !tit->is_object() || !get_int(*tit, "times", req.times) || !get_int(*tit, "assets", req.A))
      return false;
    if (req.times < factor::kSegLen || req.times % factor::kSegLen != 0 || req.A < 2) // Request 的前置条件
      return false;
    const auto rit = j.find("rows");
    if (rit == j.end() || !rit->is_array() || rit->size() != static_cast<size_t>(svc.total()))
      return false;
    snap = svc.rows; // 静态列 + 构造期默认参数原样带过去, load_row 只改动态列
    for (size_t i = 0; i < snap.size(); ++i)
      if (!load_row((*rit)[i], snap[i]))
        return false;
    for (const OperatorRow &r : snap) // 表头的 failed 不信, 按行现算
      if (!r.stream.ok() || (r.gpu_ms >= 0 && !r.gpu.ok()))
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
