// TabTimeSeries - Time Series Analysis Tab Implementation
// ============================================================================
//
// 时序分析流程 (SARIMA + GARCH 框架):
//   目标: 如果存在稳定可预测的成分,剥离它,减少与其他特征的虚假相关性
//
// 分析步骤:
//   Step 0: 平稳性检验 - ADF/KPSS 确认序列可建模
//   Step 1: 频域分析   - 检测周期性成分,确认频谱宽度
//   Step 2: ARMA建模   - ACF/PACF 确定模型阶数
//   Step 3: 残差分析   - 验证模型充分性,诊断残差性质
//   Step 4: 时间衰减   - 评估截面结构的时间稳定性
//
// ============================================================================

#include "gui/task_features/ui/TabTimeSeries.hpp"
#include "gui/task_features/services/TimeSeriesService.hpp"
#include "shared/Asset.hpp"
#include "shared/SharedData.hpp"
#include "shared/TimeSeries.hpp"

#include "imgui.h"
#include "implot.h"

#include <cstdio>

namespace GUI::Features {

// ============================================================================
// Helpers
// ============================================================================

static const char *StatusText(TimeSeries::Compute::Status s) {
  switch (s) {
  case TimeSeries::Compute::Status::Idle:
    return "Idle";
  case TimeSeries::Compute::Status::Building:
    return "Building...";
  case TimeSeries::Compute::Status::Done:
    return "Done";
  case TimeSeries::Compute::Status::Error:
    return "Error";
  case TimeSeries::Compute::Status::Cancelled:
    return "Cancelled";
  }
  return "?";
}

// Pass/Fail/Warn indicator with color
static void RenderStatus(bool pass, bool warn = false) {
  if (pass) {
    ImGui::TextColored(ImVec4(0.2f, 0.8f, 0.2f, 1.0f), "[PASS]");
  } else if (warn) {
    ImGui::TextColored(ImVec4(0.9f, 0.7f, 0.1f, 1.0f), "[WARN]");
  } else {
    ImGui::TextColored(ImVec4(0.8f, 0.2f, 0.2f, 1.0f), "[FAIL]");
  }
}

// ============================================================================
// Control Panel
// ============================================================================

static void RenderControlPanel([[maybe_unused]] TimeSeriesService *service, SharedData &data) {
  auto &ts = data.timeseries;

  // Help button with tooltip
  ImGui::TextDisabled("(?)");
  if (ImGui::IsItemHovered()) {
    ImGui::BeginTooltip();
    ImGui::PushTextWrapPos(450.0f);
    ImGui::TextUnformatted("在极低信噪比环境下, 对于输入特征, 尝试剥离稳定, 显著, 可预测(建模)的经典时序成分(SARIMA + GARCH), 降低特征之间的相关性和共线性, 提高后续因子质量");
    ImGui::TextUnformatted("先剥离均值(SARIMA), 再剥离方差(GARCH)");
    ImGui::PopTextWrapPos();
    ImGui::EndTooltip();
  }

  ImGui::SameLine();

  // Status display (auto-compute on step change)
  size_t done = ts.compute.done.load();
  size_t total = ts.compute.total.load();
  if (ts.compute.is_busy()) {
    ImGui::TextColored(ImVec4(1.0f, 0.7f, 0.3f, 1.0f), "Computing... (%zu/%zu)", done, total);
  } else {
    ImGui::Text("Status: %s (%zu/%zu)", StatusText(ts.compute.status), done, total);
  }
}

// ============================================================================
// Step 0: 平稳性检验 - Tooltip
// ============================================================================

static void RenderStep0Tooltip() {
  ImGui::BeginTooltip();
  ImGui::PushTextWrapPos(450.0f);

  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "[平稳性检验]");
  ImGui::Separator();

  ImGui::TextUnformatted("核心问题: 序列的分布是否随时间变化？");
  ImGui::Spacing();

  ImGui::TextUnformatted("检验方法:");
  ImGui::TextUnformatted("├─ ADF (Augmented Dickey-Fuller) 单位根检验");
  ImGui::TextUnformatted("│   H0: 存在单位根 (非平稳)");
  ImGui::TextColored(ImVec4(0.5f, 1.0f, 0.5f, 1.0f),
                     "│   判断: p < 0.05 → 拒绝H0 → 序列平稳 ✓");
  ImGui::TextUnformatted("│   注意: ADF对趋势敏感,需先去趋势(detrend)");
  ImGui::TextUnformatted("│");
  ImGui::TextUnformatted("└─ KPSS (Kwiatkowski-Phillips-Schmidt-Shin) 平稳性检验");
  ImGui::TextUnformatted("    H0: 序列平稳");
  ImGui::TextColored(ImVec4(0.5f, 1.0f, 0.5f, 1.0f),
                     "    判断: p > 0.05 → 接受H0 → 序列平稳 ✓");
  ImGui::TextUnformatted("    注意: KPSS对季节性敏感,需先去季节(deseason)");
  ImGui::Spacing();

  ImGui::TextColored(ImVec4(0.7f, 0.9f, 0.7f, 1.0f), "输入平稳性=>梯度稳定性");
  ImGui::TextUnformatted("对于梯度算法: 若输入过程是平稳(时间不变)且遍历(样本量够), 且损失函数相对于参数可微(损失函数选择合理, 梯度局部线性近似)且可积(输入不重尾, 梯度不爆炸), 则对任意参数, 梯度过程是平稳且遍历的随机过程 (Birkhoff ergodic theorem) (梯度噪声可能会延长时间平均梯度收敛到期望的时间)");
  ImGui::Spacing();
  ImGui::TextUnformatted("对于非梯度算法(树模型, 聚类, 非深度概率模型(先验或后验))和数据挖掘(启发,演化算法), 输入'可被平稳化'(模型内部能够平稳化输入)仍然是模型参数收敛的必要条件 (可被平稳化难以被证明, 但是: 输入已经平稳 => 可被平稳化) ");
  ImGui::PopTextWrapPos();
  ImGui::EndTooltip();
}

// ============================================================================
// Step 1: 频域分析 - Tooltip
// ============================================================================

static void RenderStep1Tooltip() {
  ImGui::BeginTooltip();
  ImGui::PushTextWrapPos(450.0f);

  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "[频域分析]");
  ImGui::Separator();

  ImGui::TextUnformatted("核心问题: 信号的频率结构是否影响模型表现, 是否能进行后面的分析？");
  ImGui::Spacing();

  ImGui::TextUnformatted("检验指标:");
  ImGui::TextUnformatted("├─ Nyquist采样频率检查");
  ImGui::TextUnformatted("│   问题: 信号主频是否远低于采样频率的一半？");
  ImGui::TextColored(ImVec4(1.0f, 0.6f, 0.6f, 1.0f),
                     "│   风险: 低频成分 → 可能是趋势误判, 影响模型表现");
  ImGui::TextUnformatted("│   处理: 考虑重采样或更长周期聚合");
  ImGui::TextUnformatted("│");
  ImGui::TextUnformatted("├─ 频谱宽度/连续性");
  ImGui::TextUnformatted("│   目标: 频谱较宽、相对连续、无明显尖峰");
  ImGui::TextUnformatted("│   原理: 满足最大熵原则 → 信息分布均匀 → 可预测性低(信息含量高)");
  ImGui::TextColored(ImVec4(1.0f, 0.6f, 0.6f, 1.0f),
                     "│   风险: 明显波峰 → 存在周期性成分 → 可能是季节误判, 影响模型表现");
  ImGui::TextUnformatted("│");
  ImGui::TextUnformatted("└─ Q因子 (Quality Factor)");
  ImGui::TextUnformatted("    公式: Q = f₀ / Δf  (峰值频率 / 峰宽)");
  ImGui::TextUnformatted("    判断:");
  ImGui::TextColored(ImVec4(0.5f, 1.0f, 0.5f, 1.0f),
                     "      Q < 3  → 宽频谱,可开始ARMA建模 ✓");
  ImGui::Spacing();

  ImGui::TextColored(ImVec4(0.7f, 0.9f, 0.7f, 1.0f), "频谱形态解读:");
  ImGui::TextUnformatted("  - 平坦频谱 → 白噪声特征,低可预测性");
  ImGui::TextUnformatted("  - 1/f 衰减 → 长记忆过程,考虑ARFIMA");
  ImGui::TextUnformatted("  - 尖峰 → 周期性成分,需识别并剥离");

  ImGui::PopTextWrapPos();
  ImGui::EndTooltip();
}

// ============================================================================
// Step 2: ARMA建模分析 - Tooltip
// ============================================================================

static void RenderStep2Tooltip() {
  ImGui::BeginTooltip();
  ImGui::PushTextWrapPos(450.0f);

  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "[ARMA建模分析]");
  ImGui::Separator();

  ImGui::TextUnformatted("核心问题: 序列是否存在可建模的自相关结构？");
  ImGui::Spacing();

  ImGui::TextUnformatted("分析工具:");
  ImGui::TextUnformatted("├─ ACF (自相关函数) → 确定 MA(q) 阶数");
  ImGui::TextUnformatted("│   观察: ACF 在滞后 q 处截断(骤降至置信区间内)");
  ImGui::TextUnformatted("│   解释: 过去 q 期的随机冲击影响当前值");
  ImGui::TextUnformatted("│   示例: ACF 在 lag=2 截断 → MA(2) 候选");
  ImGui::TextUnformatted("│");
  ImGui::TextUnformatted("└─ PACF (偏自相关函数) → 确定 AR(p) 阶数");
  ImGui::TextUnformatted("    观察: PACF 在滞后 p 处截断");
  ImGui::TextUnformatted("    解释: 控制中间变量后,滞后 p 期与当前值的直接相关");
  ImGui::TextUnformatted("    示例: PACF 在 lag=1 截断 → AR(1) 候选");
  ImGui::Spacing();

  ImGui::TextColored(ImVec4(0.7f, 0.9f, 0.7f, 1.0f), "模型识别指南:");
  ImGui::TextUnformatted("  ACF 截断 + PACF 拖尾 → MA(q) 过程");
  ImGui::TextUnformatted("  ACF 拖尾 + PACF 截断 → AR(p) 过程");
  ImGui::TextUnformatted("  ACF 拖尾 + PACF 拖尾 → ARMA(p,q) 混合过程");
  ImGui::TextUnformatted("  两者都在 lag=0 截断  → 白噪声,无需建模");
  ImGui::Spacing();

  ImGui::TextColored(ImVec4(0.8f, 0.8f, 0.5f, 1.0f), "原理:");
  ImGui::TextUnformatted(
      "在:\n"
      "   - 离散时间, \n"
      "   - 因果(无未来信息), \n"
      "   - 弱平稳(二阶(均值)平稳)、\n"
      "   - 输入(创新项)i.i.d, \n"
      "   - 且过程具有有理谱密度(有限个极点/零点/参数)的条件下, \n"
      "   - 若时间重排仅通过LTI(滤波器效果)引入, \n"
      "则此过程必然等价于一个ARMA(p, q) 过程\n"
      "(AR: IIR卷积(频响极点) MA: FIR卷积(频响零点))");
  ImGui::PopTextWrapPos();
  ImGui::EndTooltip();
}

// ============================================================================
// Step 3: 残差分析 - Tooltip
// ============================================================================

static void RenderStep3Tooltip() {
  ImGui::BeginTooltip();
  ImGui::PushTextWrapPos(500.0f);

  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "[残差分析]");
  ImGui::Separator();

  ImGui::TextUnformatted("核心问题: 模型残差是否满足白噪声假设？");
  ImGui::Spacing();

  ImGui::TextUnformatted("检验项目:");
  ImGui::TextUnformatted("├─ Ljung-Box Q 检验 (自相关性)");
  ImGui::TextUnformatted("│   H0: 残差无自相关");
  ImGui::TextColored(ImVec4(0.5f, 1.0f, 0.5f, 1.0f),
                     "│   判断: p > 0.05 → 残差无显著自相关 ✓");
  ImGui::TextColored(ImVec4(1.0f, 0.6f, 0.6f, 1.0f),
                     "│   失败: 均值模型不完整,需增加 AR/MA 阶数");
  ImGui::TextUnformatted("│");
  ImGui::TextUnformatted("├─ ARCH LM 检验 (条件异方差)");
  ImGui::TextUnformatted("│   H0: 残差无 ARCH 效应(波动聚集)");
  ImGui::TextColored(ImVec4(0.5f, 1.0f, 0.5f, 1.0f),
                     "│   判断: p > 0.05 → 无异方差 ✓");
  ImGui::TextUnformatted("│   失败 + 需要波动率建模 → 考虑 GARCH");
  ImGui::TextUnformatted("│   失败 + 不需要 → 可忽略,但记录");
  ImGui::TextUnformatted("│   辅助: 观察残差平方的 ACF/PACF");
  ImGui::TextUnformatted("│");
  ImGui::TextUnformatted("├─ Jarque-Bera 检验 (正态性)");
  ImGui::TextUnformatted("│   H0: 残差服从正态分布");
  ImGui::TextColored(ImVec4(0.5f, 1.0f, 0.5f, 1.0f),
                     "│   判断: p > 0.05 → 近似正态 ✓");
  ImGui::TextUnformatted("│   失败:");
  ImGui::TextColored(ImVec4(1.0f, 0.8f, 0.4f, 1.0f),
                     "│     轻微 (p > 0.01) → 警告,模型仍可用 ⚠");
  ImGui::TextColored(ImVec4(1.0f, 0.4f, 0.4f, 1.0f),
                     "│     严重 (p < 0.01) → 考虑 t分布/偏态分布 ✗");
  ImGui::TextUnformatted("│   辅助: Q-Q 图直观判断尾部行为");
  ImGui::TextUnformatted("│");
  ImGui::TextUnformatted("└─ CUSUM / CUSUMQ 检验 (时间稳定性)");
  ImGui::TextUnformatted("    目的: 检验模型参数是否随时间稳定");
  ImGui::TextColored(ImVec4(0.5f, 1.0f, 0.5f, 1.0f),
                     "    判断: 累积和在置信带内 → 参数稳定 ✓");
  ImGui::TextColored(ImVec4(1.0f, 0.6f, 0.6f, 1.0f),
                     "    失败: 存在结构性断点 → 需要分段建模");
  ImGui::Spacing();

  ImGui::TextColored(ImVec4(0.8f, 0.8f, 0.5f, 1.0f), "残差分析流程图:");
  ImGui::TextUnformatted("  残差 → Ljung-Box ✓ → ARCH LM → 正态性 → CUSUM");
  ImGui::TextUnformatted("           ↓ ✗");
  ImGui::TextUnformatted("        增加ARMA阶数");

  ImGui::PopTextWrapPos();
  ImGui::EndTooltip();
}

// ============================================================================
// Step 4: 时间衰减分析 - Tooltip
// ============================================================================

static void RenderStep4Tooltip() {
  ImGui::BeginTooltip();
  ImGui::PushTextWrapPos(500.0f);

  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "[时间衰减分析]");
  ImGui::Separator();

  ImGui::TextUnformatted("核心问题: 特征的截面结构是否随时间保持稳定？");
  ImGui::Spacing();

  ImGui::TextUnformatted("分析维度:");
  ImGui::TextUnformatted("├─ 截面异质时序一致性 (Heterogeneity Consistency)");
  ImGui::TextUnformatted("│   目标: 资产间的分散程度是否稳定");
  ImGui::TextUnformatted("│");
  ImGui::TextUnformatted("│   ├─ Gini(t): 基尼系数随时间的演化");
  ImGui::TextUnformatted("│   │   解释: 衡量截面分布的不平等程度");
  ImGui::TextColored(ImVec4(0.5f, 1.0f, 0.5f, 1.0f),
                     "│   │   稳定: Gini(t) 波动小 → 异质性结构稳定 ✓");
  ImGui::TextUnformatted("│   │");
  ImGui::TextUnformatted("│   ├─ HHI(t): 赫芬达尔指数随时间的演化");
  ImGui::TextUnformatted("│   │   解释: 衡量截面集中度");
  ImGui::TextColored(ImVec4(0.5f, 1.0f, 0.5f, 1.0f),
                     "│   │   稳定: HHI(t) 波动小 → 无资产主导切换 ✓");
  ImGui::TextUnformatted("│   │");
  ImGui::TextUnformatted("│   └─ GRS(t): 协方差矩阵稳定性 (Gibbons-Ross-Shanken)");
  ImGui::TextUnformatted("│       解释: 资产间相关结构是否稳定");
  ImGui::TextUnformatted("│       应用: 因子组合权重的稳定性前提");
  ImGui::TextUnformatted("│");
  ImGui::TextUnformatted("└─ 截面排序时序一致性 (Scale Robustness)");
  ImGui::TextUnformatted("    目标: 不同标准化方法下,资产排序是否一致");
  ImGui::TextUnformatted("");
  ImGui::TextUnformatted("    测试: raw ↔ zscore ↔ minmax ↔ robust 标准化");
  ImGui::TextUnformatted("    指标: RankCorr(t) - 秩相关系数随时间的稳定性");
  ImGui::TextUnformatted("");
  ImGui::TextColored(ImVec4(0.5f, 1.0f, 0.5f, 1.0f),
                     "    稳定: 不同标准化下排序高度一致 → 因子信号robust ✓");
  ImGui::TextColored(ImVec4(1.0f, 0.8f, 0.4f, 1.0f),
                     "    不稳定: 排序对标准化敏感 → 因子信号fragile ⚠");
  ImGui::Spacing();

  ImGui::TextColored(ImVec4(0.7f, 0.9f, 0.7f, 1.0f), "为什么重要:");
  ImGui::TextUnformatted("  - Gini/HHI 不稳定 → 因子暴露不稳定 → 需要动态权重");
  ImGui::TextUnformatted("  - RankCorr 不稳定 → 因子选股不稳定 → 信号噪声大");
  ImGui::TextUnformatted("  - 两者都稳定 → 可放心用于多因子组合");

  ImGui::PopTextWrapPos();
  ImGui::EndTooltip();
}

// ============================================================================
// Step Panel Item Renderer
// ============================================================================

static bool RenderStepItem(int step_idx, const char *title, int selected_step,
                           void (*tooltip_func)()) {
  bool is_selected = (step_idx == selected_step);
  bool clicked = false;

  // Highlight selected step
  if (is_selected) {
    ImGui::PushStyleColor(ImGuiCol_ChildBg, ImVec4(0.15f, 0.35f, 0.45f, 1.0f));
  }

  char child_id[32];
  snprintf(child_id, sizeof(child_id), "Step%d", step_idx);
  ImGui::BeginChild(child_id, ImVec2(0, 0), true,
                    ImGuiWindowFlags_NoScrollbar);

  // Step header: "Step X: Title (?)"
  char header[64];
  snprintf(header, sizeof(header), "Step %d: %s", step_idx, title);

  if (ImGui::Selectable(header, is_selected, ImGuiSelectableFlags_AllowOverlap)) {
    clicked = true;
  }

  ImGui::SameLine();
  ImGui::TextDisabled("(?)");
  if (ImGui::IsItemHovered()) {
    tooltip_func();
  }

  ImGui::EndChild();

  if (is_selected) {
    ImGui::PopStyleColor();
  }

  // Border for selected
  if (is_selected) {
    ImDrawList *draw = ImGui::GetWindowDrawList();
    ImVec2 p_min = ImGui::GetItemRectMin();
    ImVec2 p_max = ImGui::GetItemRectMax();
    draw->AddRect(p_min, p_max, IM_COL32(0, 255, 255, 255), 0.0f, 0, 2.0f);
  }

  return clicked;
}

// ============================================================================
// Left Panel: Step List with Results
// ============================================================================

static void RenderStepPanel(SharedData &data, TimeSeriesUIState &ui) {
  auto &ts = data.timeseries;
  bool can_switch = !ts.compute.is_busy();

  // Step 0: 平稳性检验
  ImGui::BeginChild("Step0Child", ImVec2(0, 80), true);
  {
    ImGui::Text("Step 0: 平稳性检验");
    ImGui::SameLine();
    ImGui::TextDisabled("(?)");
    if (ImGui::IsItemHovered())
      RenderStep0Tooltip();

    if (ImGui::IsWindowHovered() && ImGui::IsMouseClicked(0) && can_switch) {
      ui.selected_step = 0;
    }

    if (ts.step0_stationarity.valid) {
      ImGui::Text("  ADF:  p=%.3f", ts.step0_stationarity.adf_pvalue);
      ImGui::SameLine();
      RenderStatus(ts.step0_stationarity.adf_pass);

      ImGui::Text("  KPSS: p=%.3f", ts.step0_stationarity.kpss_pvalue);
      ImGui::SameLine();
      RenderStatus(ts.step0_stationarity.kpss_pass);
    } else {
      ImGui::TextDisabled("  (no data)");
    }
  }
  if (ui.selected_step == 0) {
    ImDrawList *draw = ImGui::GetWindowDrawList();
    ImVec2 p_min = ImGui::GetWindowPos();
    ImVec2 p_max = ImVec2(p_min.x + ImGui::GetWindowWidth(),
                          p_min.y + ImGui::GetWindowHeight());
    draw->AddRect(p_min, p_max, IM_COL32(0, 255, 255, 255), 0.0f, 0, 2.0f);
  }
  ImGui::EndChild();

  // Step 1: 频域分析
  ImGui::BeginChild("Step1Child", ImVec2(0, 95), true);
  {
    ImGui::Text("Step 1: 频域分析");
    ImGui::SameLine();
    ImGui::TextDisabled("(?)");
    if (ImGui::IsItemHovered())
      RenderStep1Tooltip();

    if (ImGui::IsWindowHovered() && ImGui::IsMouseClicked(0) && can_switch) {
      ui.selected_step = 1;
    }

    if (ts.step1_frequency.valid) {
      ImGui::Text("  Q-factor: %.2f", ts.step1_frequency.q_factor);
      ImGui::SameLine();
      RenderStatus(ts.step1_frequency.q_factor_pass,
                   ts.step1_frequency.q_factor >= 3.0f &&
                       ts.step1_frequency.q_factor < 10.0f);

      ImGui::Text("  低频成分: %s", ts.step1_frequency.has_low_freq ? "有" : "无");
      ImGui::SameLine();
      RenderStatus(!ts.step1_frequency.has_low_freq);

      ImGui::Text("  波峰检测: %s", ts.step1_frequency.has_peaks ? "有" : "无");
      ImGui::SameLine();
      RenderStatus(!ts.step1_frequency.has_peaks);
    } else {
      ImGui::TextDisabled("  (no data)");
    }
  }
  if (ui.selected_step == 1) {
    ImDrawList *draw = ImGui::GetWindowDrawList();
    ImVec2 p_min = ImGui::GetWindowPos();
    ImVec2 p_max = ImVec2(p_min.x + ImGui::GetWindowWidth(),
                          p_min.y + ImGui::GetWindowHeight());
    draw->AddRect(p_min, p_max, IM_COL32(0, 255, 255, 255), 0.0f, 0, 2.0f);
  }
  ImGui::EndChild();

  // Step 2: ARMA建模
  ImGui::BeginChild("Step2Child", ImVec2(0, 80), true);
  {
    ImGui::Text("Step 2: ARMA建模");
    ImGui::SameLine();
    ImGui::TextDisabled("(?)");
    if (ImGui::IsItemHovered())
      RenderStep2Tooltip();

    if (ImGui::IsWindowHovered() && ImGui::IsMouseClicked(0) && can_switch) {
      ui.selected_step = 2;
    }

    if (ts.step2_arma.valid) {
      ImGui::Text("  ACF截断:  q=%d %s", ts.step2_arma.acf_cutoff_lag,
                  ts.step2_arma.acf_is_cutoff ? "(截断)" : "(拖尾)");
      ImGui::Text("  PACF截断: p=%d %s", ts.step2_arma.pacf_cutoff_lag,
                  ts.step2_arma.pacf_is_cutoff ? "(截断)" : "(拖尾)");
      if (ts.step2_arma.is_white_noise) {
        ImGui::TextColored(ImVec4(0.5f, 0.8f, 0.5f, 1.0f), "  → 白噪声,无需建模");
      } else {
        ImGui::Text("  → ARMA(%d,%d) 候选", ts.step2_arma.suggested_p,
                    ts.step2_arma.suggested_q);
      }
    } else {
      ImGui::TextDisabled("  (no data)");
    }
  }
  if (ui.selected_step == 2) {
    ImDrawList *draw = ImGui::GetWindowDrawList();
    ImVec2 p_min = ImGui::GetWindowPos();
    ImVec2 p_max = ImVec2(p_min.x + ImGui::GetWindowWidth(),
                          p_min.y + ImGui::GetWindowHeight());
    draw->AddRect(p_min, p_max, IM_COL32(0, 255, 255, 255), 0.0f, 0, 2.0f);
  }
  ImGui::EndChild();

  // Step 3: 残差分析
  ImGui::BeginChild("Step3Child", ImVec2(0, 110), true);
  {
    ImGui::Text("Step 3: 残差分析");
    ImGui::SameLine();
    ImGui::TextDisabled("(?)");
    if (ImGui::IsItemHovered())
      RenderStep3Tooltip();

    if (ImGui::IsWindowHovered() && ImGui::IsMouseClicked(0) && can_switch) {
      ui.selected_step = 3;
    }

    if (ts.step3_residual.valid) {
      ImGui::Text("  Ljung-Box: p=%.3f", ts.step3_residual.ljung_box_pvalue);
      ImGui::SameLine();
      RenderStatus(ts.step3_residual.ljung_box_pass);

      ImGui::Text("  ARCH LM:   p=%.3f", ts.step3_residual.arch_lm_pvalue);
      ImGui::SameLine();
      RenderStatus(ts.step3_residual.arch_lm_pass);

      ImGui::Text("  J-B:       p=%.3f", ts.step3_residual.jarque_bera_pvalue);
      ImGui::SameLine();
      RenderStatus(ts.step3_residual.jarque_bera_pass,
                   ts.step3_residual.jarque_bera_warn);

      ImGui::Text("  CUSUM:     %s", ts.step3_residual.cusum_pass ? "stable" : "unstable");
      ImGui::SameLine();
      RenderStatus(ts.step3_residual.cusum_pass);
    } else {
      ImGui::TextDisabled("  (no data)");
    }
  }
  if (ui.selected_step == 3) {
    ImDrawList *draw = ImGui::GetWindowDrawList();
    ImVec2 p_min = ImGui::GetWindowPos();
    ImVec2 p_max = ImVec2(p_min.x + ImGui::GetWindowWidth(),
                          p_min.y + ImGui::GetWindowHeight());
    draw->AddRect(p_min, p_max, IM_COL32(0, 255, 255, 255), 0.0f, 0, 2.0f);
  }
  ImGui::EndChild();

  // Step 4: 时间衰减
  ImGui::BeginChild("Step4Child", ImVec2(0, 80), true);
  {
    ImGui::Text("Step 4: 时间衰减");
    ImGui::SameLine();
    ImGui::TextDisabled("(?)");
    if (ImGui::IsItemHovered())
      RenderStep4Tooltip();

    if (ImGui::IsWindowHovered() && ImGui::IsMouseClicked(0) && can_switch) {
      ui.selected_step = 4;
    }

    if (ts.step4_temporal_decay.valid) {
      ImGui::Text("  Gini一致性:   %.2f", ts.step4_temporal_decay.gini_stability);
      ImGui::Text("  秩相关稳定性: %.2f", ts.step4_temporal_decay.rank_corr_stability);
    } else {
      ImGui::TextDisabled("  (no data)");
    }
  }
  if (ui.selected_step == 4) {
    ImDrawList *draw = ImGui::GetWindowDrawList();
    ImVec2 p_min = ImGui::GetWindowPos();
    ImVec2 p_max = ImVec2(p_min.x + ImGui::GetWindowWidth(),
                          p_min.y + ImGui::GetWindowHeight());
    draw->AddRect(p_min, p_max, IM_COL32(0, 255, 255, 255), 0.0f, 0, 2.0f);
  }
  ImGui::EndChild();
}

// ============================================================================
// Right Panel: Visualization (Step-dependent)
// ============================================================================

// Heatmap color based on deviation from 0.05 threshold
// ADF: want p < 0.05, deviation = max(0, p - 0.05)
// KPSS: want p > 0.05, deviation = max(0, 0.05 - p)
// Map max deviation [0, 0.05] to green->yellow->red
static ImU32 GetStationarityColor(const TimeSeries::StationarityCell &cell) {
  if (!cell.valid)
    return IM_COL32(60, 60, 60, 255);

  float adf_dev = std::max(0.0f, cell.adf_pvalue - 0.05f);
  float kpss_dev = std::max(0.0f, 0.05f - cell.kpss_pvalue);
  float t = std::min(1.0f, std::max(adf_dev, kpss_dev) / 0.05f);

  uint8_t r = static_cast<uint8_t>(40 + t * 160);
  uint8_t g = static_cast<uint8_t>(180 - t * 120);
  return IM_COL32(r, g, 40, 255);
}

static void RenderStep0Plot(const TimeSeries &ts, const Asset &asset,
                            bool /*need_autofit*/) {
  const auto &cache = ts.stationarity_cache;
  if (cache.empty()) {
    ImGui::TextDisabled("No data - run compute first");
    return;
  }

  // Dimensions
  const size_t n_months = cache.size();
  const size_t n_assets = cache[0].n_assets;

  if (n_assets == 0 || n_months == 0) {
    ImGui::TextDisabled("No data available");
    return;
  }

  // Summary statistics (before heatmap) - always show to keep layout stable
  if (ts.step0_stationarity.valid) {
    ImGui::Text("Median ADF p=%.3f %s  |  Median KPSS p=%.3f %s",
                ts.step0_stationarity.adf_pvalue,
                ts.step0_stationarity.adf_pass ? "[PASS]" : "[FAIL]",
                ts.step0_stationarity.kpss_pvalue,
                ts.step0_stationarity.kpss_pass ? "[PASS]" : "[FAIL]");
  } else {
    ImGui::TextDisabled("Median ADF p=---   |  Median KPSS p=---");
  }

  ImGui::Text("热力图: 绿=通过, 红色=偏离阈值0.05以上");
  ImGui::Separator();

  // Calculate cell size
  ImVec2 avail = ImGui::GetContentRegionAvail();
  float cell_w = std::max(4.0f, (avail.x - 80.0f) / static_cast<float>(n_months));
  float cell_h = std::max(2.0f, (avail.y - 40.0f) / static_cast<float>(n_assets));

  // Clamp cell size for readability
  cell_w = std::min(cell_w, 20.0f);
  cell_h = std::min(cell_h, 8.0f);

  ImVec2 canvas_pos = ImGui::GetCursorScreenPos();
  ImVec2 canvas_size(cell_w * n_months + 60.0f, cell_h * n_assets + 30.0f);

  ImDrawList *draw_list = ImGui::GetWindowDrawList();

  // Draw month labels on top
  for (size_t m = 0; m < n_months; ++m) {
    if (m % 3 == 0) {  // Label every 3rd month
      const std::string &month = cache[m].month;
      std::string label = month.substr(2, 2) + "/" + month.substr(4, 2);
      float x = canvas_pos.x + 50.0f + m * cell_w;
      draw_list->AddText(ImVec2(x, canvas_pos.y), IM_COL32(200, 200, 200, 255),
                         label.c_str());
    }
  }

  // Track hovered cell for tooltip
  int hovered_month = -1;
  int hovered_asset = -1;

  // Draw heatmap cells
  float start_y = canvas_pos.y + 20.0f;
  for (size_t a = 0; a < n_assets; ++a) {
    float y = start_y + a * cell_h;

    for (size_t m = 0; m < n_months; ++m) {
      float x = canvas_pos.x + 50.0f + m * cell_w;

      const auto &cell = cache[m].by_asset[a];
      ImU32 color = GetStationarityColor(cell);

      ImVec2 p_min(x, y);
      ImVec2 p_max(x + cell_w - 1.0f, y + cell_h - 1.0f);

      draw_list->AddRectFilled(p_min, p_max, color);

      // Check hover
      ImVec2 mouse = ImGui::GetMousePos();
      if (mouse.x >= p_min.x && mouse.x < p_max.x &&
          mouse.y >= p_min.y && mouse.y < p_max.y) {
        hovered_month = static_cast<int>(m);
        hovered_asset = static_cast<int>(a);
        // Highlight border
        draw_list->AddRect(p_min, p_max, IM_COL32(255, 255, 255, 255), 0.0f, 0, 2.0f);
      }
    }
  }

  // Reserve space for the canvas
  ImGui::Dummy(canvas_size);

  // Tooltip for hovered cell
  if (hovered_month >= 0 && hovered_asset >= 0) {
    const auto &cell = cache[hovered_month].by_asset[hovered_asset];
    const std::string &month = cache[hovered_month].month;

    ImGui::BeginTooltip();

    // Asset name
    if (static_cast<size_t>(hovered_asset) < asset.items.size()) {
      const auto &item = asset.items[hovered_asset];
      ImGui::Text("Asset: %s %s", item.asset_code.c_str(), item.asset_name.c_str());
    } else {
      ImGui::Text("Asset: #%d", hovered_asset);
    }

    // Month
    ImGui::Text("Month: %s/%s", month.substr(0, 4).c_str(),
                month.substr(4, 2).c_str());

    ImGui::Separator();

    if (cell.valid) {
      // ADF result
      if (cell.adf_pass) {
        ImGui::TextColored(ImVec4(0.2f, 0.8f, 0.2f, 1.0f), "ADF: p=%.3f [PASS]",
                           cell.adf_pvalue);
      } else {
        ImGui::TextColored(ImVec4(0.8f, 0.2f, 0.2f, 1.0f), "ADF: p=%.3f [FAIL]",
                           cell.adf_pvalue);
      }

      // KPSS result
      if (cell.kpss_pass) {
        ImGui::TextColored(ImVec4(0.2f, 0.8f, 0.2f, 1.0f), "KPSS: p=%.3f [PASS]",
                           cell.kpss_pvalue);
      } else {
        ImGui::TextColored(ImVec4(0.8f, 0.2f, 0.2f, 1.0f), "KPSS: p=%.3f [FAIL]",
                           cell.kpss_pvalue);
      }

      ImGui::Text("Samples: %zu", cell.n_samples);
    } else {
      ImGui::TextDisabled("Invalid (insufficient samples)");
    }

    ImGui::EndTooltip();
  }
}

static void RenderStep1Plot(const TimeSeries &ts, bool need_autofit) {
  ImGui::Text("频谱分析 (Power Spectrum)");
  ImGui::Separator();

  if (!ts.step1_frequency.valid || ts.step1_frequency.frequencies.empty()) {
    ImGui::TextDisabled("No data - run compute first");
    return;
  }

  if (need_autofit) {
    ImPlot::SetNextAxesToFit();
  }

  if (ImPlot::BeginPlot("##SpectrumPlot", ImVec2(-1, -1))) {
    ImPlot::SetupAxes("Frequency", "Power (log)", ImPlotAxisFlags_None,
                      ImPlotAxisFlags_None);

    const auto &freq = ts.step1_frequency.frequencies;
    const auto &power = ts.step1_frequency.power_spectrum;

    if (!freq.empty() && !power.empty()) {
      ImPlot::PlotLine("Spectrum", freq.data(), power.data(),
                       static_cast<int>(freq.size()));

      float nyq = ts.step1_frequency.nyquist_freq;
      if (nyq > 0) {
        ImPlot::PlotInfLines("Nyquist", &nyq, 1);
      }
    }

    char q_label[64];
    snprintf(q_label, sizeof(q_label), "Q=%.2f", ts.step1_frequency.q_factor);
    ImPlot::PlotText(q_label, ts.step1_frequency.peak_frequency,
                     ts.step1_frequency.power_spectrum.empty()
                         ? 0.0f
                         : ts.step1_frequency.power_spectrum[0]);

    ImPlot::EndPlot();
  }
}

static void RenderStep2Plot(const TimeSeries &ts, bool need_autofit) {
  ImGui::Text("自相关分析 (ACF / PACF)");
  ImGui::Separator();

  if (!ts.step2_arma.valid || ts.step2_arma.acf_values.empty()) {
    ImGui::TextDisabled("No data - run compute first");
    return;
  }

  float height = (ImGui::GetContentRegionAvail().y - ImGui::GetTextLineHeightWithSpacing()) * 0.5f;

  if (need_autofit) {
    ImPlot::SetNextAxesToFit();
  }
  if (ImPlot::BeginPlot("##ACFPlot", ImVec2(-1, height))) {
    ImPlot::SetupAxes("Lag", "ACF");

    const auto &acf = ts.step2_arma.acf_values;
    int n = static_cast<int>(acf.size());

    float cb = ts.step2_arma.confidence_bound;
    float cb_neg = -cb;
    ImPlot::PlotInfLines("##cb_pos", &cb, 1, ImPlotInfLinesFlags_Horizontal);
    ImPlot::PlotInfLines("##cb_neg", &cb_neg, 1, ImPlotInfLinesFlags_Horizontal);

    std::vector<float> lags(n);
    for (int i = 0; i < n; ++i)
      lags[i] = static_cast<float>(i);
    ImPlot::PlotBars("ACF", lags.data(), acf.data(), n, 0.4);

    ImPlot::EndPlot();
  }

  if (need_autofit) {
    ImPlot::SetNextAxesToFit();
  }
  if (ImPlot::BeginPlot("##PACFPlot", ImVec2(-1, height))) {
    ImPlot::SetupAxes("Lag", "PACF");

    const auto &pacf = ts.step2_arma.pacf_values;
    int n = static_cast<int>(pacf.size());

    float cb = ts.step2_arma.confidence_bound;
    float cb_neg = -cb;
    ImPlot::PlotInfLines("##cb_pos", &cb, 1, ImPlotInfLinesFlags_Horizontal);
    ImPlot::PlotInfLines("##cb_neg", &cb_neg, 1, ImPlotInfLinesFlags_Horizontal);

    std::vector<float> lags(n);
    for (int i = 0; i < n; ++i)
      lags[i] = static_cast<float>(i);
    ImPlot::PlotBars("PACF", lags.data(), pacf.data(), n, 0.4);

    ImPlot::EndPlot();
  }
}

static void RenderStep3Plot(const TimeSeries &ts, bool need_autofit) {
  ImGui::Text("残差诊断 (Q-Q / 残差时序 / CUSUM)");
  ImGui::Separator();

  if (!ts.step3_residual.valid || ts.step3_residual.residuals.empty()) {
    ImGui::TextDisabled("No data - run compute first");
    return;
  }

  float height = (ImGui::GetContentRegionAvail().y - ImGui::GetTextLineHeightWithSpacing() * 2) / 3.0f;

  if (need_autofit) {
    ImPlot::SetNextAxesToFit();
  }
  if (ImPlot::BeginPlot("##QQPlot", ImVec2(-1, height))) {
    ImPlot::SetupAxes("Theoretical Quantile", "Empirical Quantile");

    const auto &theo = ts.step3_residual.qq_theoretical;
    const auto &emp = ts.step3_residual.qq_empirical;

    if (!theo.empty() && !emp.empty()) {
      ImPlot::PlotScatter("Q-Q", theo.data(), emp.data(),
                          static_cast<int>(theo.size()));
      float diag[] = {theo.front(), theo.back()};
      ImPlot::PlotLine("##ref", diag, diag, 2);
    }

    ImPlot::EndPlot();
  }

  if (need_autofit) {
    ImPlot::SetNextAxesToFit();
  }
  if (ImPlot::BeginPlot("##ResidualPlot", ImVec2(-1, height))) {
    ImPlot::SetupAxes("Time", "Residual");

    const auto &resid = ts.step3_residual.residuals;
    ImPlot::PlotLine("Residual", resid.data(), static_cast<int>(resid.size()));

    ImPlot::EndPlot();
  }

  if (need_autofit) {
    ImPlot::SetNextAxesToFit();
  }
  if (ImPlot::BeginPlot("##CUSUMPlot", ImVec2(-1, height))) {
    ImPlot::SetupAxes("Time", "CUSUM");

    const auto &cusum = ts.step3_residual.cusum_values;
    const auto &upper = ts.step3_residual.cusum_upper;
    const auto &lower = ts.step3_residual.cusum_lower;

    if (!cusum.empty()) {
      ImPlot::PlotLine("CUSUM", cusum.data(), static_cast<int>(cusum.size()));
    }
    if (!upper.empty()) {
      ImPlot::SetNextLineStyle(ImVec4(1, 0.5f, 0.5f, 0.5f));
      ImPlot::PlotLine("Upper", upper.data(), static_cast<int>(upper.size()));
    }
    if (!lower.empty()) {
      ImPlot::SetNextLineStyle(ImVec4(1, 0.5f, 0.5f, 0.5f));
      ImPlot::PlotLine("Lower", lower.data(), static_cast<int>(lower.size()));
    }

    ImPlot::EndPlot();
  }
}

static void RenderStep4Plot(const TimeSeries &ts, bool need_autofit) {
  ImGui::Text("时间衰减分析 (Gini / HHI / RankCorr)");
  ImGui::Separator();

  if (!ts.step4_temporal_decay.valid ||
      ts.step4_temporal_decay.time_points.empty()) {
    ImGui::TextDisabled("No data - run compute first");
    return;
  }

  if (need_autofit) {
    ImPlot::SetNextAxesToFit();
  }

  if (ImPlot::BeginPlot("##TemporalDecayPlot", ImVec2(-1, -1))) {
    ImPlot::SetupAxes("Time", "Value");
    ImPlot::SetupLegend(ImPlotLocation_NorthEast);

    const auto &t = ts.step4_temporal_decay.time_points;
    const auto &gini = ts.step4_temporal_decay.gini_series;
    const auto &hhi = ts.step4_temporal_decay.hhi_series;
    const auto &rank = ts.step4_temporal_decay.rank_corr_series;

    int n = static_cast<int>(t.size());

    if (!gini.empty() && gini.size() == t.size()) {
      ImPlot::PlotLine("Gini(t)", t.data(), gini.data(), n);
    }
    if (!hhi.empty() && hhi.size() == t.size()) {
      ImPlot::PlotLine("HHI(t)", t.data(), hhi.data(), n);
    }
    if (!rank.empty() && rank.size() == t.size()) {
      ImPlot::PlotLine("RankCorr(t)", t.data(), rank.data(), n);
    }

    ImPlot::EndPlot();
  }
}

static void RenderVisualizationPanel(SharedData &data, TimeSeriesUIState &ui) {
  const auto &ts = data.timeseries;

  switch (ui.selected_step) {
  case 0:
    RenderStep0Plot(ts, data.asset, ui.need_autofit);
    break;
  case 1:
    RenderStep1Plot(ts, ui.need_autofit);
    break;
  case 2:
    RenderStep2Plot(ts, ui.need_autofit);
    break;
  case 3:
    RenderStep3Plot(ts, ui.need_autofit);
    break;
  case 4:
    RenderStep4Plot(ts, ui.need_autofit);
    break;
  default:
    ImGui::TextDisabled("Invalid step");
    break;
  }
}

// ============================================================================
// Main Render
// ============================================================================

void RenderTabTimeSeries(TimeSeriesService *service, SharedData &data,
                         TimeSeriesUIState &ui) {
  // Auto-start coroutine
  if (!service->is_running()) {
    service->StartCompute(data.coromgr, data);
  }

  auto &ts = data.timeseries;

  // Trigger autofit when compute just finished
  static auto last_status = ts.compute.status;
  if (last_status != TimeSeries::Compute::Status::Done &&
      ts.compute.status == TimeSeries::Compute::Status::Done) {
    ui.need_autofit = true;
  }
  last_status = ts.compute.status;

  // Control panel
  float ctrl_height =
      ImGui::GetFrameHeightWithSpacing() + ImGui::GetStyle().WindowPadding.y * 2;
  ImGui::BeginChild("ControlBar", ImVec2(0, ctrl_height), true);
  RenderControlPanel(service, data);
  ImGui::EndChild();

  // Main content: Left (Steps) + Right (Visualization)
  float content_height = ImGui::GetContentRegionAvail().y;
  ImGui::Columns(2, "MainCols", true);
  ImGui::SetColumnWidth(0, 350);

  // Left: Step list
  ImGui::BeginChild("StepPanel", ImVec2(0, content_height), false);
  RenderStepPanel(data, ui);
  ImGui::EndChild();

  ImGui::NextColumn();

  // Right: Visualization
  ImGui::BeginChild("VizPanel", ImVec2(0, content_height), true);
  RenderVisualizationPanel(data, ui);
  ImGui::EndChild();

  ImGui::Columns(1);

  // Clear autofit flag
  ui.need_autofit = false;
}

void StopTabTimeSeries(TimeSeriesService *service, SharedData &data) {
  data.timeseries.cancel();
  if (service && service->is_running()) {
    service->StopCompute(data.coromgr, data);
  }
}

} // namespace GUI::Features
