// Tab Operators — 见头文件
#include "gui/task_factors/ui/TabOperators.hpp"
#include "factor/GpuRun.hpp"                // device_name: GPU 型号显示
#include "factor/Stat/Check.hpp"            // kHolds: Stat 行载入时校验持有期列表
#include "gui/Tasks.hpp"                    // StatusColor: 全局色表, 行状态着色不自配颜色
#include "gui/task_factors/ui/StatJson.hpp" // sig4 / hold_json / load_hold / json_* (与因子文件同一格式)
#include "gui/util/Latex.hpp"               // Operands / Operator 列 LaTeX 渲染 (与特征表共用缓存)

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

// 三维分类列 + 输出域列的落盘文本: 不在分类里的行 (Stat) 空着 (out 列表格内按 dom_tex 渲染, JSON 落 dom_name)
const char *cls_T(const OperatorRow &r) { return r.classified ? T_name(r.T) : ""; }
const char *cls_A(const OperatorRow &r) { return r.classified ? A_name(r.A) : ""; }
const char *cls_kern(const OperatorRow &r) { return r.classified ? kern_name(r.kern) : ""; }
const char *cls_out(const OperatorRow &r) { return r.classified ? factor::dom_name(r.out) : ""; }

// Stat 行悬停 e_name: 表列放不下的附带信息 (prep 耗时 + 每持有期 cpu 侧二级汇总)
void stat_tooltip(const OperatorRow &r, const StatExtra &x) {
  ImGui::BeginTooltip();
  ImGui::TextUnformatted("因子评估算子 (factor/Stat): 因子 x + 每持有期一组 fp16 标签 → 每 (h, t) 的 IC / 20 组和 / 多空 / rank-AC, 再沿 t 汇总 (组沿 t 池化).\n"
                         "标签统一超额 (减截面均值). 两口径: CS 每 t 截面 rank x, 任一组空行无效; TS x 已是自身 5 日分位, 直接量化, 组可空.\n"
                         "无流式后端, cpu ↔ gpu 两方对拍 (无 golden; 表格 GPU 列的 Diff = 两口径 × (一级 rows + 二级 stat) 合并).\n"
                         "表列耗时 = 两口径 eval 之和 (每因子一次: rank/量化 + 逐持有期一遍 A 轴); prep = 标签 rank 预处理 (常驻期一次), 见下.\n"
                         "造数: long = 0.3·z + 噪声 (IC ≈ 0.3; TS 的 x = Φ(z)). ic_t / ls_t 按 n/h 折算 (相邻 h 行标签重叠); Sharpe 以持有期为一期年化;\n"
                         "rank-AC 的 lag = h; 段末 h+3 行 (持有到收盘) 不计标签统计.");
  if (r.status == RowStatus::Done) {
    ImGui::Separator();
    if (x.gpu_prep_ms >= 0)
      ImGui::Text("prep: cpu %.3f ms, gpu %.3f ms", x.cpu_prep_ms, x.gpu_prep_ms);
    else
      ImGui::Text("prep: cpu %.3f ms", x.cpu_prep_ms);
    for (int fi = 0; fi < 2; ++fi) {
      ImGui::Separator();
      ImGui::TextDisabled("%s", factor::stat::frame_name(static_cast<factor::stat::Frame>(fi)));
      for (int i = 0; i < x.n_hold; ++i) {
        const factor::stat::HoldStat &h = x.hold[fi][i];
        ImGui::Text("h=%-3d n=%d/%d  rIC %+.4f std %.4f IR %+.3f t %+.2f pos %.2f skew %+.2f kurt %+.2f | LS %+.5f t %+.2f pos %.2f SR %+.2f "
                    "β %+.3f | mono %+.3f | rAC %+.3f",
                    h.hold, h.n, h.n_ac, h.ic_mean, h.ic_std, h.icir, h.ic_t, h.ic_pos, h.ic_skew, h.ic_kurt, h.ls_mean, h.ls_t, h.ls_pos,
                    h.sharpe, h.beta, h.mono, h.rank_ac);
      }
    }
  }
  ImGui::EndTooltip();
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
  StatExtra s_stat;
  OperatorsRequest cur;
  bool from_json = false;
  {
    std::lock_guard<std::mutex> lock(svc.mutex);
    s_rows = svc.rows;
    s_stat = svc.stat;
    cur = svc.current;
    from_json = svc.from_json;
  }
  const int n = static_cast<int>(s_rows.size());
  const auto count_A = [&](factor::A a) {
    return (int)std::count_if(s_rows.begin(), s_rows.end(), [a](const OperatorRow &r) { return r.classified && r.A == a; });
  };

  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "2. Operators:");
  ImGui::SameLine();
  ImGui::Text("%d (SELF %d / ALL %d / GROUP %d + Stat)", n, count_A(factor::A::SELF), count_A(factor::A::ALL), count_A(factor::A::GROUP));
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
        "全局 idx = operators.json 的 idx (code 与 UI 统一按它): 0 = Stat 评估算子 (每轮先跑), 之后 = OpTable 表序:\n"
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
        "签名 (由 OpTable 的 in / T / k域 列生成, Expr.hpp operand_tex): 自变量在前, 参数在后, 顺序恒 x, y, z → d → k → k2\n"
        "  自变量  x, y, z = 按元数取的序列 (每格 值 + 有效位), 值域 = in 列 (enum Dom, 与 out 列同一套);\n"
        "          t_D = 段内分钟位置 (TodMask 无序列输入)\n"
        "  参数    d ⇔ T = ROLL; k ⇔ k域 ≠ NONE; k2 ⇔ k域 = TOD. = 号后是本轮实际值 (占位符 ⟨d⟩ ⟨k⟩ ⟨k2⟩ 渲染前替换):\n"
        "    d   窗长 / 滞后 (期 = 分钟), 来自 Check.hpp kDParams; 无 d 的算子固定 1 (与 op_check 同)\n"
        "    k   阈值 / 桶数 / EMA 系数 / 分位, 来自 Check.hpp kKParams; k2 第二阈值 (仅 TodMask 上界)\n"
        "  in 为严格域 (INT = 组 id) 时 parser 要求子算子 out ⊆ in, 特征叶按数据查; 其余 in 是语义声明 (越界格算子自置无效)",
        "因变量值域 (OpTable out 列, enum Dom; 与 in 列同一套, 前一算子的 out 就是后一算子的 in):\n"
        "  ℝ REAL / ℝ≥0 NONNEG (含计数) / ℝ>0 POS / [0,1] UNIT / [−1,1] SIGNED / {0,1} BIN / {−1,0,1} SIGN3 / {0..1023} INT / ℝ(bcast) 截面广播\n"
        "  因子根 (归一算子) 的输入不许离散 (BIN / SIGN3 / INT), 且沿资产轴要有变异 (BCAST 抹平); 组 id 元只收 ⊆ INT 的 (BIN / INT)\n"
        "  只按算子自身声明: 透传 (Mask / Where / Delay) 与取大取小写 REAL, 不随输入推导",
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
          cmp = cmp3(sort_ms_chk(ra, ra.cpu_ms, ra.stream), sort_ms_chk(rb, rb.cpu_ms, rb.stream));
          break;
        case 12:
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
      ImGui::TextDisabled("%d", idx); // 全局 idx = operators.json 的 idx (0 = Stat, 之后 = OpTable 表序)
      ImGui::TableSetColumnIndex(1);
      ImGui::TextColored(StatusColor(r.status == RowStatus::Done
                                         ? ((r.stream.ok() && (r.gpu_ms < 0 || r.gpu.ok())) ? TaskStatus::Kind::Ready
                                                                                            : TaskStatus::Kind::Error)
                                         : (r.status == RowStatus::Running ? TaskStatus::Kind::Busy : TaskStatus::Kind::Muted)),
                         "%s", r.e_name);
      if (!r.classified && ImGui::IsItemHovered())
        stat_tooltip(r, s_stat);
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
      if (r.classified) {
        if (tex::TeXRender *render = Latex::Get(factor::dom_tex(r.out), kFormulaTextSize))
          Latex::Draw(render, ImGui::GetTextLineHeight());
        else
          ImGui::TextUnformatted(factor::dom_name(r.out)); // 解析失败回退枚举名
        if (ImGui::IsItemHovered())
          ImGui::SetTooltip("%s", factor::dom_name(r.out));
      }
      ImGui::TableSetColumnIndex(9);
      if (tex::TeXRender *render = Latex::Get(r.op, kFormulaTextSize))
        Latex::Draw(render, ImGui::GetTextLineHeight());
      else
        ImGui::TextUnformatted(r.op); // 解析失败回退原文
      ImGui::TableSetColumnIndex(10);
      render_time_cell(r, r.stream_ms, nullptr, false); // golden: 无对拍, 无快慢判据 (Stat 无 stream → n/a)
      ImGui::TableSetColumnIndex(11);
      render_time_cell(r, r.cpu_ms, r.stream_ms >= 0 ? &r.stream : nullptr, r.stream_ms >= 0 && r.cpu_ms > r.stream_ms);
      ImGui::TableSetColumnIndex(12);
      render_time_cell(r, r.gpu_ms, &r.gpu, r.gpu_ms > r.cpu_ms);
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

// Stat 行表列放不下的附带信息: prep 耗时 + 两口径每持有期的 cpu 侧二级汇总 (耗时 / 对拍在 rows 里与其他行同列).
// 键 holds_CS / holds_TS (= "holds_" + frame_name)
nlohmann::ordered_json stat_json(const StatExtra &s) {
  nlohmann::ordered_json j;
  j["cpu_prep_ms"] = sig4(s.cpu_prep_ms);
  if (s.gpu_prep_ms >= 0)
    j["gpu_prep_ms"] = sig4(s.gpu_prep_ms);
  for (int fi = 0; fi < 2; ++fi) {
    nlohmann::ordered_json holds = nlohmann::ordered_json::array();
    for (int i = 0; i < s.n_hold; ++i)
      holds.push_back(hold_json(s.hold[fi][i]));
    j[std::string("holds_") + factor::stat::frame_name(static_cast<factor::stat::Frame>(fi))] = holds;
  }
  return j;
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
            " \"gpu\": \"|Δ| ≤ 1e-3 + 1e-3·max (CsNormRank / 三四阶矩单独放宽)\","
            " \"stat\": \"cpu ↔ gpu |Δ| ≤ 1e-5 + 1e-4·max, ok / n 逐位相等\"},\n";
    if (svc.rows[svc.stat_index()].status == RowStatus::Done) // Stat 行的附带信息, 只落跑完的
      file << " \"stat\": " << stat_json(svc.stat).dump() << ",\n";

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
      j["out"] = cls_out(r);
      j["operator"] = r.op;
      j["note"] = r.note;
      j["status"] = row_status_name(r.status);
      // 动态列只落跑完的行 (表格显示 "…" 的格子不落键); 无 stream 后端的行 (Stat) 不落 stream_* 键
      if (r.status == RowStatus::Done) {
        const bool has_stream = r.stream_ms >= 0;
        if (has_stream)
          j["stream_vs_cpu"] = diff_json(r.stream);
        if (r.gpu_ms >= 0)
          j["gpu_vs_cpu"] = diff_json(r.gpu);
        j["cpu_ms"] = sig4(r.cpu_ms);
        if (has_stream)
          j["stream_ms"] = sig4(r.stream_ms);
        if (r.gpu_ms >= 0) {
          j["gpu_ms"] = sig4(r.gpu_ms);
          if (has_stream && r.gpu_ms > 0)
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
      !json_str_is(j, "kernel", cls_kern(dst)) || !json_str_is(j, "operand", dst.operand.c_str()) || !json_str_is(j, "out", cls_out(dst)) ||
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
  const auto sit = j.find("stream_vs_cpu"); // 有 stream 后端的行 (分类内) 必有; Stat 行无 → stream 列 n/a
  if (dst.classified) {
    if (sit == j.end() || !load_diff(*sit, dst.stream) || !json_num(j, "stream_ms", dst.stream_ms))
      return false;
  } else {
    if (sit != j.end() || j.find("stream_ms") != j.end())
      return false;
    dst.stream = {}, dst.stream_ms = -1;
  }
  const auto git = j.find("gpu_vs_cpu"); // 落盘那轮没 GPU 后端 → 无 gpu_* 键, 载入后 GPU 列显示 n/a
  if (git == j.end()) {
    dst.gpu = {}, dst.gpu_ms = -1;
  } else if (!load_diff(*git, dst.gpu) || !json_num(j, "gpu_ms", dst.gpu_ms) || dst.gpu_ms < 0) {
    return false;
  }
  dst.status = RowStatus::Done;
  return true;
}

// Stat 行附带信息 (stat_json 的逆); 持有期列表须与当前 Stat/Check.hpp kHolds 一致 (改了 → 旧耗时作废).
// has_gpu = 该行 rows 里有 gpu_ms (两处必须一致)
bool load_stat(const nlohmann::json &j, bool has_gpu, StatExtra &dst) {
  if (!j.is_object())
    return false;
  StatExtra s;
  if (!json_num(j, "cpu_prep_ms", s.cpu_prep_ms))
    return false;
  if (has_gpu) {
    if (!json_num(j, "gpu_prep_ms", s.gpu_prep_ms) || s.gpu_prep_ms < 0)
      return false;
  } else {
    if (j.find("gpu_prep_ms") != j.end())
      return false;
    s.gpu_prep_ms = -1;
  }
  s.n_hold = factor::stat::check::kNumHolds;
  for (int fi = 0; fi < 2; ++fi) {
    const auto hit = j.find(std::string("holds_") + factor::stat::frame_name(static_cast<factor::stat::Frame>(fi)));
    if (hit == j.end() || !hit->is_array() || hit->size() != static_cast<size_t>(factor::stat::check::kNumHolds))
      return false;
    for (int i = 0; i < s.n_hold; ++i) {
      if (!load_hold((*hit)[static_cast<size_t>(i)], s.hold[fi][i]) || s.hold[fi][i].hold != factor::stat::check::kHolds[i])
        return false;
    }
  }
  dst = s;
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
  StatExtra extra;
  int failed = 0;
  const bool ok = [&] {
    const nlohmann::json j = nlohmann::json::parse(file, nullptr, /*allow_exceptions=*/false);
    if (j.is_discarded() || !j.is_object())
      return false;
    if (!json_str_is(j, "status", "done")) // cancelled / running 的半截快照不吃
      return false;
    int total = 0, done = 0;
    if (!json_int(j, "total", total) || !json_int(j, "done", done) || total != svc.total() || done != total)
      return false;
    const auto tit = j.find("tensor");
    if (tit == j.end() || !tit->is_object() || !json_int(*tit, "times", req.times) || !json_int(*tit, "assets", req.A))
      return false;
    if (req.times < factor::kSegLen || req.times % factor::kSegLen != 0 || req.A < 2) // Request 的前置条件
      return false;
    const auto rit = j.find("rows");
    if (rit == j.end() || !rit->is_array() || rit->size() != svc.rows.size())
      return false;
    snap = svc.rows; // 静态列 + 构造期默认参数原样带过去, load_row 只改动态列
    for (size_t i = 0; i < snap.size(); ++i)
      if (!load_row((*rit)[i], snap[i]))
        return false;
    const auto sit = j.find("stat"); // Stat 行的附带信息 (行都 done 了它必在, 缺 / 半截即整份判废)
    if (sit == j.end() || !load_stat(*sit, snap[svc.stat_index()].gpu_ms >= 0, extra))
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
  svc.AdoptSnapshot(req, std::move(snap), extra, failed);
  ui.req.times = req.times, ui.req.A = req.A; // 页面参数跟上快照 (Run 默认按同一形状重跑)
  return true;
}

} // namespace GUI::Factors
