// Tab Transform Implementation
#include "gui/task_features/ui/TabTransform.hpp"
#include "graphic/graphic_basic.h"

#include "imgui.h"
#include "latex.h"
#include "package/utfcpp/utf8.hpp"
#include "platform/imgui/graphic_imgui.h"
#include "render.h"

#include <cassert>
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
// Stationarity Comparison Table
// ============================================================================

static void RenderStationarityTooltip() {
  ImGui::BeginTooltip();
  ImGui::PushTextWrapPos(800.0f);

  ImGui::TextColored(ImVec4(1.0f, 0.9f, 0.4f, 1.0f), "平稳化方法对比");
  ImGui::Spacing();

  // Define formulas
  static const char *formula_ma = "x_t - \\text{MA}_W(x_t)";
  static const char *formula_demean = "x_t - \\mu_t";
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
      if (r) renderLatexFormula(r);
    }
    ImGui::TableSetColumnIndex(2);
    {
      tex::TeXRender *r = getOrCreateFormulaRender(formula_diff_int);
      if (r) renderLatexFormula(r);
    }
    ImGui::TableSetColumnIndex(3);
    {
      tex::TeXRender *r = getOrCreateFormulaRender(formula_diff_frac);
      if (r) renderLatexFormula(r);
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
    ImGui::TextColored(ImVec4(0.9f, 0.3f, 0.3f, 1.0f), "❌");
    ImGui::TableSetColumnIndex(3);
    ImGui::TextColored(ImVec4(0.3f, 0.9f, 0.3f, 1.0f), "✅");

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
    ImGui::TextColored(ImVec4(0.9f, 0.3f, 0.3f, 1.0f), "❌");
    ImGui::TableSetColumnIndex(2);
    ImGui::TextColored(ImVec4(0.3f, 0.9f, 0.3f, 1.0f), "✅");
    ImGui::TableSetColumnIndex(3);
    ImGui::TextColored(ImVec4(0.3f, 0.9f, 0.3f, 1.0f), "✅");

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
    ImGui::TextColored(ImVec4(0.9f, 0.3f, 0.3f, 1.0f), "❌");
    ImGui::TableSetColumnIndex(2);
    ImGui::TextColored(ImVec4(0.3f, 0.9f, 0.3f, 1.0f), "✅");
    ImGui::TableSetColumnIndex(3);
    ImGui::TextColored(ImVec4(0.5f, 0.8f, 1.0f, 1.0f), "近似 ✅");

    ImGui::EndTable();
  }

  ImGui::PopTextWrapPos();
  ImGui::EndTooltip();
}

// ============================================================================
// Main Render Function
// ============================================================================

void RenderTabTransform(TransformUIState & /*ui_state*/) {
  ImGui::TextWrapped("Feature Transformation - 特征变换与预处理");
  ImGui::Spacing();
  ImGui::Separator();
  ImGui::Spacing();

  // ==========================================================================
  // Section 1: 平稳化 (Stationarity)
  // ==========================================================================
  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "1. 平稳化 (Stationarity)");
  ImGui::SameLine();
  ImGui::TextDisabled("(?)");
  if (ImGui::IsItemHovered()) {
    RenderStationarityTooltip();
  }
  ImGui::Spacing();

  ImGui::TextWrapped("将非平稳时间序列转化为平稳序列，消除趋势、季节性和单位根。");
  ImGui::Spacing();

  ImGui::BulletText("移动平均去趋势: 简单但不保证消除单位根");
  ImGui::BulletText("整数阶差分: 强力消除单位根，但可能过度平稳");
  ImGui::BulletText("分数阶差分: 保留长期记忆的温和去单位根方法");

  ImGui::Spacing();
  ImGui::Separator();
  ImGui::Spacing();

  // ==========================================================================
  // Section 2: 归一化 (Normalization)
  // ==========================================================================
  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "2. 归一化 (Normalization)");
  ImGui::Spacing();

  ImGui::TextWrapped("将特征值映射到标准化范围，便于模型训练和特征比较。");
  ImGui::Spacing();

  ImGui::BulletText("Z-Score: (x - mean) / std");
  ImGui::BulletText("Robust Z-Score: (x - median) / MAD");
  ImGui::BulletText("Min-Max: (x - min) / (max - min)");
  ImGui::BulletText("Rank: 分位数排名");
  ImGui::BulletText("Log/Power: 对数/幂变换处理偏态分布");

  ImGui::Spacing();
}

} // namespace GUI::Features
