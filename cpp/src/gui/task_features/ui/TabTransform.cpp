// Tab Transform Implementation
#include "gui/task_features/ui/TabTransform.hpp"
#include "graphic/graphic_basic.h"
#include "gui/task_features/services/TransformService.hpp"
#include "imgui.h"
#include "implot.h"
#include "latex.h"
#include "package/utfcpp/utf8.hpp"
#include "platform/imgui/graphic_imgui.h"
#include "render.h"
#include "shared/Asset.hpp"
#include "shared/SharedData.hpp"
#include <algorithm>
#include <cassert>
#include <cstdio>
#include <unordered_map>


namespace GUI::Features {

// ============================================================================
// LaTeX Formula Rendering (shared with TabFeature)
// ============================================================================

static std::wstring utf8ToWide(std::string_view s) {
  auto u16 = utf8::utf8to16(s);
  return {u16.begin(), u16.end()};
}

static std::unordered_map<const char *, tex::TeXRender *> s_formula_cache;

static tex::TeXRender *getOrCreateFormulaRender(const char *formula, float text_size = 24.0f) {
  auto it = s_formula_cache.find(formula);
  if (it != s_formula_cache.end()) {
    return it->second;
  }

  static bool s_latex_initialized = false;
  if (!s_latex_initialized) {
    tex::LaTeX::init("res");
    s_latex_initialized = true;
  }

  std::wstring wlatex = utf8ToWide(formula);
  tex::TeXRender *render = tex::LaTeX::parse(wlatex, 0, text_size, 5.0f, tex::green);

  s_formula_cache[formula] = render;
  return render;
}

static void renderLatexFormula(tex::TeXRender *render) {
  assert(render);

  tex::Font_imgui::rebuildFontAtlasIfNeeded();

  ImDrawList *draw_list = ImGui::GetWindowDrawList();
  ImVec2 cursor_pos = ImGui::GetCursorScreenPos();

  tex::Graphics2D_imgui g2(draw_list);
  g2.translate(cursor_pos.x, cursor_pos.y);
  render->draw(g2, 0, 0);

  ImGui::Dummy(ImVec2((float)render->getWidth(), (float)render->getHeight()));
}

// ============================================================================
// Stationarity Comparison Table (Tooltip)
// ============================================================================

static void RenderStationarityTooltip() {
  ImGui::BeginTooltip();
  ImGui::PushTextWrapPos(800.0f);

  ImGui::TextColored(ImVec4(1.0f, 0.9f, 0.4f, 1.0f), "平稳化方法对比");
  ImGui::Spacing();

  // Define formulas
  static const char *formula_ma = "x_t - \\text{MA}_W(x_t)";
  static const char *formula_diff_int = "(1-L)^d x_t, \\; d \\in \\mathbb{Z}^+";
  static const char *formula_diff_frac = "(1-L)^d x_t, \\; d \\in \\mathbb{R}";

  if (ImGui::BeginTable("StationarityTable", 4,
                        ImGuiTableFlags_Borders | ImGuiTableFlags_SizingStretchProp | ImGuiTableFlags_RowBg)) {
    ImGui::TableSetupColumn("属性", ImGuiTableColumnFlags_WidthFixed, 160.0f);
    ImGui::TableSetupColumn("移动平均去趋势", ImGuiTableColumnFlags_WidthStretch);
    ImGui::TableSetupColumn("整数阶差分", ImGuiTableColumnFlags_WidthStretch);
    ImGui::TableSetupColumn("分数阶差分", ImGuiTableColumnFlags_WidthStretch);
    ImGui::TableHeadersRow();

    // Row: 典型形式
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    ImGui::TextUnformatted("典型形式");
    ImGui::TableSetColumnIndex(1);
    {
      tex::TeXRender *r = getOrCreateFormulaRender(formula_ma);
      if (r)
        renderLatexFormula(r);
    }
    ImGui::TableSetColumnIndex(2);
    {
      tex::TeXRender *r = getOrCreateFormulaRender(formula_diff_int);
      if (r)
        renderLatexFormula(r);
    }
    ImGui::TableSetColumnIndex(3);
    {
      tex::TeXRender *r = getOrCreateFormulaRender(formula_diff_frac);
      if (r)
        renderLatexFormula(r);
    }

    // Row: 是否线性算子
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    ImGui::TextUnformatted("是否线性算子");
    ImGui::TableSetColumnIndex(1);
    ImGui::TextColored(ImVec4(1.0f, 0.8f, 0.2f, 1.0f), "[?] 依赖 x_{t-} 定义");
    ImGui::TableSetColumnIndex(2);
    ImGui::TextColored(ImVec4(0.3f, 0.9f, 0.3f, 1.0f), "Y");
    ImGui::TableSetColumnIndex(3);
    ImGui::TextColored(ImVec4(0.3f, 0.9f, 0.3f, 1.0f), "Y");

    // Row: 是否消除单位根
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    ImGui::TextUnformatted("是否消除单位根");
    ImGui::TableSetColumnIndex(1);
    ImGui::TextColored(ImVec4(0.9f, 0.3f, 0.3f, 1.0f), "N 不保证");
    ImGui::TableSetColumnIndex(2);
    ImGui::TextColored(ImVec4(0.3f, 0.9f, 0.3f, 1.0f), "Y");
    ImGui::TableSetColumnIndex(3);
    ImGui::TextColored(ImVec4(0.3f, 0.9f, 0.3f, 1.0f), "Y");

    // Row: 平稳性保证
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    ImGui::TextUnformatted("平稳性保证");
    ImGui::TableSetColumnIndex(1);
    ImGui::TextColored(ImVec4(0.9f, 0.3f, 0.3f, 1.0f), "N 经验性");
    ImGui::TableSetColumnIndex(2);
    ImGui::TextColored(ImVec4(0.3f, 0.9f, 0.3f, 1.0f), "Y 理论保证");
    ImGui::TableSetColumnIndex(3);
    ImGui::TextColored(ImVec4(0.3f, 0.9f, 0.3f, 1.0f), "Y 理论保证");

    // Row: ADF / KPSS 典型表现
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    ImGui::TextUnformatted("ADF / KPSS 典型表现");
    ImGui::TableSetColumnIndex(1);
    ImGui::Text("ADF +  KPSS -");
    ImGui::TableSetColumnIndex(2);
    ImGui::Text("ADF ++  KPSS +");
    ImGui::TableSetColumnIndex(3);
    ImGui::Text("ADF +  KPSS + (合适d)");

    // Row: 残留低频结构
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    ImGui::TextUnformatted("残留低频结构");
    ImGui::TableSetColumnIndex(1);
    ImGui::TextColored(ImVec4(0.9f, 0.3f, 0.3f, 1.0f), "多");
    ImGui::TableSetColumnIndex(2);
    ImGui::TextColored(ImVec4(0.3f, 0.9f, 0.3f, 1.0f), "极少");
    ImGui::TableSetColumnIndex(3);
    ImGui::TextColored(ImVec4(0.5f, 0.8f, 1.0f, 1.0f), "可控");

    // Row: 长期记忆保留
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    ImGui::TextUnformatted("长期记忆保留");
    ImGui::TableSetColumnIndex(1);
    ImGui::TextColored(ImVec4(1.0f, 0.8f, 0.2f, 1.0f), "不稳定");
    ImGui::TableSetColumnIndex(2);
    ImGui::TextColored(ImVec4(0.9f, 0.3f, 0.3f, 1.0f), "N");
    ImGui::TableSetColumnIndex(3);
    ImGui::TextColored(ImVec4(0.3f, 0.9f, 0.3f, 1.0f), "Y");

    // Row: 过度平稳风险
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    ImGui::TextUnformatted("过度平稳风险");
    ImGui::TableSetColumnIndex(1);
    ImGui::TextColored(ImVec4(1.0f, 0.8f, 0.2f, 1.0f), "中");
    ImGui::TableSetColumnIndex(2);
    ImGui::TextColored(ImVec4(0.9f, 0.3f, 0.3f, 1.0f), "高");
    ImGui::TableSetColumnIndex(3);
    ImGui::TextColored(ImVec4(0.3f, 0.9f, 0.3f, 1.0f), "低 (最小d原则)");

    // Row: 引入伪均值回归
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    ImGui::TextUnformatted("引入伪均值回归");
    ImGui::TableSetColumnIndex(1);
    ImGui::TextColored(ImVec4(0.9f, 0.3f, 0.3f, 1.0f), "高");
    ImGui::TableSetColumnIndex(2);
    ImGui::TextColored(ImVec4(0.3f, 0.9f, 0.3f, 1.0f), "低");
    ImGui::TableSetColumnIndex(3);
    ImGui::TextColored(ImVec4(0.3f, 0.9f, 0.3f, 1.0f), "低");

    // Row: 对参数/窗口敏感性
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    ImGui::TextUnformatted("对参数/窗口敏感性");
    ImGui::TableSetColumnIndex(1);
    ImGui::TextColored(ImVec4(0.9f, 0.3f, 0.3f, 1.0f), "高 (依赖W)");
    ImGui::TableSetColumnIndex(2);
    ImGui::TextColored(ImVec4(0.3f, 0.9f, 0.3f, 1.0f), "低");
    ImGui::TableSetColumnIndex(3);
    ImGui::TextColored(ImVec4(0.3f, 0.9f, 0.3f, 1.0f), "低");

    // Row: 可逆性
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    ImGui::TextUnformatted("可逆性");
    ImGui::TableSetColumnIndex(1);
    ImGui::TextColored(ImVec4(0.9f, 0.3f, 0.3f, 1.0f), "N");
    ImGui::TableSetColumnIndex(2);
    ImGui::TextColored(ImVec4(0.3f, 0.9f, 0.3f, 1.0f), "Y");
    ImGui::TableSetColumnIndex(3);
    ImGui::TextColored(ImVec4(0.3f, 0.9f, 0.3f, 1.0f), "Y");

    // Row: ACF 典型形态
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    ImGui::TextUnformatted("ACF 典型形态");
    ImGui::TableSetColumnIndex(1);
    ImGui::TextUnformatted("慢衰减");
    ImGui::TableSetColumnIndex(2);
    ImGui::TextUnformatted("快速衰减/振荡");
    ImGui::TableSetColumnIndex(3);
    ImGui::TextUnformatted("慢→快");

    // Row: 本质作用
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    ImGui::TextUnformatted("本质作用");
    ImGui::TableSetColumnIndex(1);
    ImGui::TextColored(ImVec4(0.7f, 0.7f, 0.7f, 1.0f), "局部去趋势");
    ImGui::TableSetColumnIndex(2);
    ImGui::TextColored(ImVec4(0.5f, 0.8f, 1.0f, 1.0f), "强力去单位根");
    ImGui::TableSetColumnIndex(3);
    ImGui::TextColored(ImVec4(0.5f, 0.8f, 1.0f, 1.0f), "温和去单位根");

    // Row: 是否严格 I(0)
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    ImGui::TextUnformatted("是否严格 I(0)");
    ImGui::TableSetColumnIndex(1);
    ImGui::TextColored(ImVec4(0.9f, 0.3f, 0.3f, 1.0f), "N");
    ImGui::TableSetColumnIndex(2);
    ImGui::TextColored(ImVec4(0.3f, 0.9f, 0.3f, 1.0f), "Y");
    ImGui::TableSetColumnIndex(3);
    ImGui::TextColored(ImVec4(0.5f, 0.8f, 1.0f, 1.0f), "~Y");

    ImGui::EndTable();
  }

  ImGui::PopTextWrapPos();
  ImGui::EndTooltip();
}

// ============================================================================
// Helpers
// ============================================================================

static const char *StatusText(Transform::Compute::Status s) {
  switch (s) {
  case Transform::Compute::Status::Idle:
    return "Idle";
  case Transform::Compute::Status::Loading:
    return "Loading...";
  case Transform::Compute::Status::Computing:
    return "Computing...";
  case Transform::Compute::Status::Done:
    return "Done";
  case Transform::Compute::Status::Error:
    return "Error";
  case Transform::Compute::Status::Cancelled:
    return "Cancelled";
  }
  return "?";
}

static ImVec4 StatusColor(Transform::Compute::Status s) {
  switch (s) {
  case Transform::Compute::Status::Idle:
  case Transform::Compute::Status::Cancelled:
    return ImVec4(0.5f, 0.5f, 0.5f, 1.0f);
  case Transform::Compute::Status::Loading:
    return ImVec4(0.3f, 0.7f, 1.0f, 1.0f);
  case Transform::Compute::Status::Computing:
    return ImVec4(1.0f, 0.7f, 0.3f, 1.0f);
  case Transform::Compute::Status::Done:
    return ImVec4(0.4f, 0.9f, 0.5f, 1.0f);
  case Transform::Compute::Status::Error:
    return ImVec4(1.0f, 0.3f, 0.3f, 1.0f);
  }
  return ImVec4(1.0f, 1.0f, 1.0f, 1.0f);
}

static const char *StationaryMethodName(Transform::StationaryMethod m) {
  switch (m) {
  case Transform::StationaryMethod::NONE:
    return "无";
  case Transform::StationaryMethod::MA_DETREND:
    return "MA去趋势";
  case Transform::StationaryMethod::INT_DIFF:
    return "整数差分";
  case Transform::StationaryMethod::FRAC_DIFF:
    return "分数差分";
  }
  return "?";
}

static const char *NormMethodName(NormMethod m) {
  switch (m) {
  case NormMethod::NONE:
    return "NONE";
  case NormMethod::ZSCORE:
    return "ZSCORE";
  case NormMethod::ROBUST_ZSCORE:
    return "ROBUST";
  case NormMethod::IQR_ZSCORE:
    return "IQR";
  case NormMethod::RANK:
    return "RANK";
  case NormMethod::RANK_ZSCORE:
    return "RANK_Z";
  case NormMethod::CLIP:
    return "CLIP";
  case NormMethod::WINSOR:
    return "WINSOR";
  case NormMethod::LOG:
    return "LOG";
  case NormMethod::POWER:
    return "POWER";
  case NormMethod::ASINH:
    return "ASINH";
  case NormMethod::TANH:
    return "TANH";
  case NormMethod::SINCOS:
    return "SINCOS";
  case NormMethod::LOG_ZSCORE:
    return "LOG_Z";
  case NormMethod::POWER_ZSCORE:
    return "POW_Z";
  case NormMethod::ASINH_ZSCORE:
    return "ASH_Z";
  case NormMethod::CLIP_ZSCORE:
    return "CLP_Z";
  case NormMethod::WINSOR_ZSCORE:
    return "WIN_Z";
  case NormMethod::CLIP_LOG_ZSCORE:
    return "CLG_Z";
  }
  return "?";
}

// ADF颜色: p < 0.05 绿, p > 0.1 红
static ImU32 GetADFColor(float pval) {
  if (pval < 0.05f)
    return IM_COL32(60, 200, 60, 255);
  if (pval < 0.10f)
    return IM_COL32(200, 200, 60, 255);
  return IM_COL32(200, 60, 60, 255);
}

// KPSS颜色: p > 0.05 绿, p < 0.01 红
static ImU32 GetKPSSColor(float pval) {
  if (pval > 0.05f)
    return IM_COL32(60, 200, 60, 255);
  if (pval > 0.01f)
    return IM_COL32(200, 200, 60, 255);
  return IM_COL32(200, 60, 60, 255);
}

// ============================================================================
// Control Panel
// ============================================================================

static void RenderControlPanel(TransformService *service, SharedData &data,
                               TransformUIState &ui) {
  auto &tf = data.transform;

  // Status
  ImGui::Text("Status: ");
  ImGui::SameLine(0, 0);
  ImGui::TextColored(StatusColor(tf.compute.status), "%s",
                     StatusText(tf.compute.status));

  if (tf.compute.is_busy()) {
    ImGui::SameLine();
    ImGui::Text("(%.0f%%)", tf.compute.progress());
  }

  ImGui::SameLine(150);

  // Level显示
  int level = data.feature.selection.selected_level;
  const char *level_names[] = {"L0 (秒)", "L1 (分钟)", "L2 (小时)"};
  if (level >= 0 && level < 3) {
    ImGui::Text("级别: %s", level_names[level]);
  } else {
    ImGui::TextDisabled("级别: -");
  }

  ImGui::SameLine(300);

  // 计算按钮
  bool can_compute = !tf.compute.is_busy() &&
                     data.feature.selection.primary_feature_idx >= 0;
  ImGui::BeginDisabled(!can_compute);
  if (ImGui::Button("Compute")) {
    service->RequestCompute();
  }
  ImGui::EndDisabled();

  ImGui::SameLine();
  if (ImGui::Button("Cancel")) {
    tf.cancel();
  }

  // 时间拖动条
  if (!tf.results.empty() && tf.results[0].valid) {
    size_t max_t = tf.results[0].raw.size();
    if (max_t > 0) {
      ImGui::SameLine(500);
      ImGui::SetNextItemWidth(200);
      int t = tf.time_slider;
      if (ImGui::SliderInt("##TimeSlider", &t, 0, static_cast<int>(max_t - 1),
                           "t=%d")) {
        tf.time_slider = t;
        tf.update_cross_section(static_cast<size_t>(t));
      }
    }
  }
}

// ============================================================================
// Stationarity Config Panel (Left)
// ============================================================================

static bool RenderStationaryConfig(Transform::Config &config) {
  bool changed = false;

  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "平稳化配置");
  ImGui::SameLine();
  ImGui::TextDisabled("(?)");
  if (ImGui::IsItemHovered()) {
    RenderStationarityTooltip();
  }
  ImGui::Separator();

  // 方法选择
  int method = static_cast<int>(config.stationary_method);
  if (ImGui::RadioButton("无##st", &method, 0)) {
    config.stationary_method = Transform::StationaryMethod::NONE;
    changed = true;
  }
  if (ImGui::RadioButton("MA去趋势##st", &method, 1)) {
    config.stationary_method = Transform::StationaryMethod::MA_DETREND;
    changed = true;
  }
  if (config.stationary_method == Transform::StationaryMethod::MA_DETREND) {
    ImGui::Indent();
    ImGui::SetNextItemWidth(150);
    if (ImGui::SliderInt("窗口##ma", &config.ma_window, 10, 500)) {
      changed = true;
    }
    ImGui::Unindent();
  }

  if (ImGui::RadioButton("整数差分##st", &method, 2)) {
    config.stationary_method = Transform::StationaryMethod::INT_DIFF;
    changed = true;
  }
  if (config.stationary_method == Transform::StationaryMethod::INT_DIFF) {
    ImGui::Indent();
    ImGui::SetNextItemWidth(150);
    if (ImGui::SliderInt("阶数##diff", &config.diff_order, 1, 3)) {
      changed = true;
    }
    ImGui::Unindent();
  }

  if (ImGui::RadioButton("分数差分##st", &method, 3)) {
    config.stationary_method = Transform::StationaryMethod::FRAC_DIFF;
    changed = true;
  }
  if (config.stationary_method == Transform::StationaryMethod::FRAC_DIFF) {
    ImGui::Indent();
    ImGui::SetNextItemWidth(150);
    if (ImGui::SliderFloat("d##frac", &config.frac_d, 0.0f, 1.0f, "%.2f")) {
      changed = true;
    }
    ImGui::SetNextItemWidth(150);
    if (ImGui::SliderInt("窗口##frac", &config.frac_window, 10, 500)) {
      changed = true;
    }
    ImGui::Unindent();
  }

  return changed;
}

// ============================================================================
// Normalization Config Panel (Right)
// ============================================================================

static bool RenderNormConfig(Transform::Config &config) {
  bool changed = false;

  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "归一化配置");
  ImGui::Separator();

  // 方法选择 (分组显示)
  int method = static_cast<int>(config.norm_method);

  // 第一行: 基本
  if (ImGui::RadioButton("NONE", method == 0)) {
    config.norm_method = NormMethod::NONE;
    changed = true;
  }
  ImGui::SameLine();
  if (ImGui::RadioButton("ZSCORE", method == 1)) {
    config.norm_method = NormMethod::ZSCORE;
    changed = true;
  }
  ImGui::SameLine();
  if (ImGui::RadioButton("ROBUST", method == 2)) {
    config.norm_method = NormMethod::ROBUST_ZSCORE;
    changed = true;
  }
  ImGui::SameLine();
  if (ImGui::RadioButton("IQR", method == 3)) {
    config.norm_method = NormMethod::IQR_ZSCORE;
    changed = true;
  }

  // 第二行: 排序
  if (ImGui::RadioButton("RANK", method == 4)) {
    config.norm_method = NormMethod::RANK;
    changed = true;
  }
  ImGui::SameLine();
  if (ImGui::RadioButton("RANK_Z", method == 5)) {
    config.norm_method = NormMethod::RANK_ZSCORE;
    changed = true;
  }

  // 第三行: 边界
  if (ImGui::RadioButton("CLIP", method == 6)) {
    config.norm_method = NormMethod::CLIP;
    changed = true;
  }
  ImGui::SameLine();
  if (ImGui::RadioButton("WINSOR", method == 7)) {
    config.norm_method = NormMethod::WINSOR;
    changed = true;
  }

  // 第四行: 非线性
  if (ImGui::RadioButton("LOG", method == 8)) {
    config.norm_method = NormMethod::LOG;
    changed = true;
  }
  ImGui::SameLine();
  if (ImGui::RadioButton("POWER", method == 9)) {
    config.norm_method = NormMethod::POWER;
    changed = true;
  }
  ImGui::SameLine();
  if (ImGui::RadioButton("ASINH", method == 10)) {
    config.norm_method = NormMethod::ASINH;
    changed = true;
  }
  ImGui::SameLine();
  if (ImGui::RadioButton("TANH", method == 11)) {
    config.norm_method = NormMethod::TANH;
    changed = true;
  }

  // 第五行: 复合
  if (ImGui::RadioButton("LOG_Z", method == 20)) {
    config.norm_method = NormMethod::LOG_ZSCORE;
    changed = true;
  }
  ImGui::SameLine();
  if (ImGui::RadioButton("CLP_Z", method == 23)) {
    config.norm_method = NormMethod::CLIP_ZSCORE;
    changed = true;
  }
  ImGui::SameLine();
  if (ImGui::RadioButton("WIN_Z", method == 24)) {
    config.norm_method = NormMethod::WINSOR_ZSCORE;
    changed = true;
  }

  ImGui::Spacing();

  // 参数
  bool needs_clip = config.norm_method == NormMethod::CLIP ||
                    config.norm_method == NormMethod::CLIP_ZSCORE ||
                    config.norm_method == NormMethod::CLIP_LOG_ZSCORE;
  bool needs_winsor = config.norm_method == NormMethod::WINSOR ||
                      config.norm_method == NormMethod::WINSOR_ZSCORE;
  bool needs_power = config.norm_method == NormMethod::POWER ||
                     config.norm_method == NormMethod::POWER_ZSCORE;

  if (needs_clip) {
    ImGui::SetNextItemWidth(150);
    if (ImGui::SliderFloat("k (clip)##norm", &config.clip_k, 1.0f, 10.0f,
                           "%.1f")) {
      changed = true;
    }
  }

  if (needs_winsor) {
    ImGui::SetNextItemWidth(150);
    if (ImGui::SliderFloat("pct (winsor)##norm", &config.winsor_pct, 0.01f,
                           0.25f, "%.2f")) {
      changed = true;
    }
  }

  if (needs_power) {
    ImGui::SetNextItemWidth(150);
    if (ImGui::SliderFloat("alpha (power)##norm", &config.power_alpha, 0.1f,
                           2.0f, "%.2f")) {
      changed = true;
    }
  }

  return changed;
}

// ============================================================================
// ADF/KPSS Heatmap
// ============================================================================

static void RenderStationarityHeatmap(const Transform &tf, const Asset &asset) {
  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "平稳性检验");
  ImGui::SameLine();
  ImGui::TextDisabled("(ADF: p<0.05=绿, KPSS: p>0.05=绿)");
  ImGui::Separator();

  if (tf.results.empty() || !tf.results[0].valid) {
    ImGui::TextDisabled("无数据");
    return;
  }

  const size_t n = tf.results.size();
  ImVec2 avail = ImGui::GetContentRegionAvail();
  float cell_w = std::min(20.0f, avail.x / (2 * n + 2));
  float cell_h = 20.0f;

  ImDrawList *draw = ImGui::GetWindowDrawList();
  ImVec2 pos = ImGui::GetCursorScreenPos();

  // 标题
  draw->AddText(ImVec2(pos.x + 50, pos.y), IM_COL32(200, 200, 200, 255), "ADF");
  draw->AddText(ImVec2(pos.x + 50 + n * cell_w + 20, pos.y),
                IM_COL32(200, 200, 200, 255), "KPSS");

  float y = pos.y + 20;

  // 热力图条
  for (size_t i = 0; i < n; ++i) {
    const auto &r = tf.results[i];
    if (!r.valid)
      continue;

    float x_adf = pos.x + 50 + i * cell_w;
    float x_kpss = pos.x + 50 + n * cell_w + 20 + i * cell_w;

    draw->AddRectFilled(ImVec2(x_adf, y), ImVec2(x_adf + cell_w - 1, y + cell_h),
                        GetADFColor(r.adf_pval));
    draw->AddRectFilled(ImVec2(x_kpss, y),
                        ImVec2(x_kpss + cell_w - 1, y + cell_h),
                        GetKPSSColor(r.kpss_pval));
  }

  // Tooltip
  ImVec2 mouse = ImGui::GetMousePos();
  if (mouse.y >= y && mouse.y < y + cell_h) {
    for (size_t i = 0; i < n; ++i) {
      float x_adf = pos.x + 50 + i * cell_w;
      float x_kpss = pos.x + 50 + n * cell_w + 20 + i * cell_w;

      bool in_adf = mouse.x >= x_adf && mouse.x < x_adf + cell_w;
      bool in_kpss = mouse.x >= x_kpss && mouse.x < x_kpss + cell_w;

      if ((in_adf || in_kpss) && tf.results[i].valid) {
        const auto &r = tf.results[i];
        ImGui::BeginTooltip();
        if (i < asset.items.size()) {
          ImGui::Text("%s %s", asset.items[i].asset_code.c_str(),
                      asset.items[i].asset_name.c_str());
        } else {
          ImGui::Text("Asset #%zu", i);
        }
        ImGui::Separator();
        if (in_adf) {
          ImGui::Text("ADF: stat=%.3f, p=%.3f %s", r.adf_stat, r.adf_pval,
                      r.adf_pass ? "[PASS]" : "[FAIL]");
        } else {
          ImGui::Text("KPSS: stat=%.3f, p=%.3f %s", r.kpss_stat, r.kpss_pval,
                      r.kpss_pass ? "[PASS]" : "[FAIL]");
        }
        ImGui::EndTooltip();
        break;
      }
    }
  }

  ImGui::Dummy(ImVec2(avail.x, cell_h + 25));
}

// ============================================================================
// Feature Plots (Raw vs Processed)
// ============================================================================

static void RenderFeaturePlots(const Transform &tf, bool need_autofit) {
  if (tf.results.empty())
    return;

  // 找第一个有效的资产
  const Transform::AssetResult *first_valid = nullptr;
  for (const auto &r : tf.results) {
    if (r.valid) {
      first_valid = &r;
      break;
    }
  }
  if (!first_valid)
    return;

  float height = 150;

  // 左: 原始特征
  ImGui::BeginChild("RawPlot", ImVec2(ImGui::GetContentRegionAvail().x * 0.5f, height), true);
  ImGui::Text("原始特征");

  if (need_autofit)
    ImPlot::SetNextAxesToFit();

  if (ImPlot::BeginPlot("##Raw", ImVec2(-1, -1), ImPlotFlags_NoLegend)) {
    ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_NoLabel,
                      ImPlotAxisFlags_NoLabel);

    for (const auto &r : tf.results) {
      if (!r.valid || r.raw.empty())
        continue;
      ImPlot::SetNextLineStyle(IMPLOT_AUTO_COL, 0.5f);
      ImPlot::PlotLine("##r", r.raw.data(), static_cast<int>(r.raw.size()));
    }
    ImPlot::EndPlot();
  }
  ImGui::EndChild();

  ImGui::SameLine();

  // 右: 处理后特征
  ImGui::BeginChild("ProcPlot", ImVec2(0, height), true);
  ImGui::Text("处理后特征");

  if (need_autofit)
    ImPlot::SetNextAxesToFit();

  if (ImPlot::BeginPlot("##Proc", ImVec2(-1, -1), ImPlotFlags_NoLegend)) {
    ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_NoLabel,
                      ImPlotAxisFlags_NoLabel);

    for (const auto &r : tf.results) {
      if (!r.valid || r.normalized.empty())
        continue;
      ImPlot::SetNextLineStyle(IMPLOT_AUTO_COL, 0.5f);
      ImPlot::PlotLine("##n", r.normalized.data(),
                       static_cast<int>(r.normalized.size()));
    }
    ImPlot::EndPlot();
  }
  ImGui::EndChild();
}

// ============================================================================
// Cross-section PDF & FFT
// ============================================================================

static void RenderBottomPlots(const Transform &tf, bool need_autofit) {
  float height = 180;

  // 左: 横截面PDF
  ImGui::BeginChild("PDFPlot", ImVec2(ImGui::GetContentRegionAvail().x * 0.5f, height), true);
  ImGui::Text("横截面分布 (t=%d)", tf.time_slider);

  if (tf.cross_section.valid && !tf.cross_section.hist_x.empty()) {
    if (need_autofit)
      ImPlot::SetNextAxesToFit();

    if (ImPlot::BeginPlot("##PDF", ImVec2(-1, -1), ImPlotFlags_NoLegend)) {
      ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_NoLabel,
                        ImPlotAxisFlags_NoLabel);

      ImPlot::PlotBars("##hist", tf.cross_section.hist_x.data(),
                       tf.cross_section.hist_y.data(),
                       static_cast<int>(tf.cross_section.hist_x.size()), 0.8);

      ImPlot::EndPlot();
    }
  } else {
    ImGui::TextDisabled("无数据");
  }
  ImGui::EndChild();

  ImGui::SameLine();

  // 右: FFT功率谱
  ImGui::BeginChild("FFTPlot", ImVec2(0, height), true);
  ImGui::Text("FFT功率谱 (平均)");

  if (!tf.avg_fft_freq.empty()) {
    if (need_autofit)
      ImPlot::SetNextAxesToFit();

    if (ImPlot::BeginPlot("##FFT", ImVec2(-1, -1), ImPlotFlags_NoLegend)) {
      ImPlot::SetupAxes("Frequency", "Power", ImPlotAxisFlags_NoLabel,
                        ImPlotAxisFlags_NoLabel);

      ImPlot::PlotLine("##fft", tf.avg_fft_freq.data(), tf.avg_fft_power.data(),
                       static_cast<int>(tf.avg_fft_freq.size()));

      ImPlot::EndPlot();
    }
  } else {
    ImGui::TextDisabled("无数据");
  }
  ImGui::EndChild();
}

// ============================================================================
// Main Render
// ============================================================================

void RenderTabTransform(TransformService *service, SharedData &data,
                        TransformUIState &ui) {
  // 配置ImPlot输入映射 (框选缩放)
  static bool input_configured = false;
  if (!input_configured) {
    ImPlot::MapInputReverse();
    input_configured = true;
  }

  // Auto-start coroutine
  if (!service->is_running()) {
    service->StartCompute(data.coromgr, data);
  }

  auto &tf = data.transform;

  // Trigger autofit when compute finishes
  static auto last_status = tf.compute.status;
  if (last_status != Transform::Compute::Status::Done &&
      tf.compute.status == Transform::Compute::Status::Done) {
    ui.need_autofit = true;
  }
  last_status = tf.compute.status;

  // 控制栏
  float ctrl_height = ImGui::GetFrameHeightWithSpacing() +
                      ImGui::GetStyle().WindowPadding.y * 2;
  ImGui::BeginChild("ControlBar", ImVec2(0, ctrl_height), true);
  RenderControlPanel(service, data, ui);
  ImGui::EndChild();

  // 配置区: 左=平稳化, 右=归一化
  float config_height = 180;
  ImGui::BeginChild("ConfigRow", ImVec2(0, config_height), false);

  float half_width = ImGui::GetContentRegionAvail().x * 0.5f - 5;

  ImGui::BeginChild("StationaryPanel", ImVec2(half_width, 0), true);
  bool st_changed = RenderStationaryConfig(tf.config);
  ImGui::EndChild();

  ImGui::SameLine();

  ImGui::BeginChild("NormPanel", ImVec2(0, 0), true);
  bool norm_changed = RenderNormConfig(tf.config);
  ImGui::EndChild();

  ImGui::EndChild();

  // 参数变化时触发计算
  if (st_changed || norm_changed) {
    ui.params_changed = true;
    service->RequestCompute();
  }

  // 热力图
  float heatmap_height = 60;
  ImGui::BeginChild("HeatmapPanel", ImVec2(0, heatmap_height), true);
  RenderStationarityHeatmap(tf, data.asset);
  ImGui::EndChild();

  // 特征对比图
  RenderFeaturePlots(tf, ui.need_autofit);

  // 底部: PDF + FFT
  RenderBottomPlots(tf, ui.need_autofit);

  // 清除autofit
  ui.need_autofit = false;
}

void StopTabTransform(TransformService *service, SharedData &data) {
  // 只停止协程，不清理数据
  if (data.transform.compute.is_busy()) {
    data.transform.cancel();
  }
  if (service && service->is_running()) {
    service->StopCompute(data.coromgr, data);
  }
}

} // namespace GUI::Features
