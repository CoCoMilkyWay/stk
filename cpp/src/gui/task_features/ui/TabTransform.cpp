// Tab Transform Implementation
#include "gui/task_features/ui/TabTransform.hpp"
#include "features/FeaturesDefine.hpp"
#include "graphic/graphic_basic.h"
#include "gui/task_features/services/TransformService.hpp"
#include "imgui.h"
#include "implot.h"
#include "latex.h"
#include "misc/profiler.hpp"
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
// Color Gradient for Asset Differentiation (使用ImPlot内置colormap)
// ============================================================================

static ImVec4 GetAssetColor(size_t idx, size_t total) {
  if (total <= 1)
    return ImPlot::GetColormapColor(0);
  float t = static_cast<float>(idx) / static_cast<float>(total - 1);
  return ImPlot::SampleColormap(t, ImPlotColormap_Spectral);
}

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
// Row 1: Status + Level + Feature + Stationary + Normalization
// ============================================================================

static void Render_StatusInfo(SharedData &data) {
  TraceN("UI:StatusInfo");
  auto &tf = data.transform;
  int level = data.feature.selection.selected_level;

  // Level
  static const char *level_names[] = {"L0", "L1", "L2"};
  if (level >= 0 && level < 3) {
    ImGui::Text("%s", level_names[level]);
  } else {
    ImGui::TextDisabled("--");
  }

  // 主特征名称
  ImGui::SameLine(0, 15);
  int feat_idx = data.feature.selection.primary_feature_idx;
  if (feat_idx >= 0) {
    const auto &meta = level == 0   ? data.feature.metadata.features_l0
                       : level == 1 ? data.feature.metadata.features_l1
                                    : data.feature.metadata.features_l2;
    if (feat_idx < (int)meta.size()) {
      ImGui::Text("%s", meta[feat_idx].code);
    }
  } else {
    ImGui::TextDisabled("无特征");
  }
  ImGui::SameLine(0, 15);

  // Status (左边，长度可变)
  ImGui::TextColored(StatusColor(tf.compute.status), "%s", StatusText(tf.compute.status));
  if (tf.compute.is_busy()) {
    ImGui::SameLine(0, 0);
    ImGui::Text(" %.0f%%", tf.compute.progress());
  }
}

// ============================================================================
// Row 3: Asset Selector + Time Window Slider
// ============================================================================

// 格式化 asset slider 标签: index.exchange.name(sample_count)
static const char *FormatAssetLabel(const Asset &asset, const Transform &tf, int sel, char *buf, size_t buf_size) {
  if (sel < 0 || sel >= (int)asset.items.size()) {
    snprintf(buf, buf_size, "---");
    return buf;
  }

  const auto &item = asset.items[sel];
  size_t sample_count = 0;
  if (sel < (int)tf.cache.sparse.size()) {
    sample_count = tf.cache.sparse[sel].size();
  }

  snprintf(buf, buf_size, "%d.%s.%s(%zu)",
           sel,
           item.exchange.c_str(),
           item.asset_name.c_str(),
           sample_count);
  return buf;
}

static bool Render_AssetAndWindow(TransformService *service, SharedData &data, TransformUIState &ui) {
  TraceN("UI:AssetAndWindow");
  auto &tf = data.transform;
  bool changed = false;
  const auto &items = data.asset.items;
  const int n_assets = static_cast<int>(items.size());

  // All checkbox
  bool is_all = (tf.display.selected_asset < 0);
  if (ImGui::Checkbox("All", &is_all)) {
    if (is_all) {
      if (tf.display.selected_asset != -1)
        changed = true;
      tf.display.selected_asset = -1;
    } else {
      // 取消 All，选择第一个 asset
      if (tf.display.selected_asset < 0 && n_assets > 0) {
        tf.display.selected_asset = 0;
        changed = true;
      }
    }
  }

  // Asset slider (禁用状态: All 模式或无 asset)
  ImGui::SameLine(0, 10);
  ImGui::BeginDisabled(is_all || n_assets == 0);
  {
    ImGui::SetNextItemWidth(300);
    int sel = tf.display.selected_asset;
    if (sel < 0)
      sel = 0;

    char label_buf[128];
    FormatAssetLabel(data.asset, tf, sel, label_buf, sizeof(label_buf));

    if (ImGui::SliderInt("##AssetSlider", &sel, 0, std::max(0, n_assets - 1), label_buf)) {
      if (tf.display.selected_asset != sel) {
        tf.display.selected_asset = sel;
        changed = true;
      }
    }
  }
  ImGui::EndDisabled();

  // Time slider (显示 YY/MM/DD)
  ImGui::SameLine(0, 20);
  ImGui::SetNextItemWidth(300);
  if (!tf.blocks.empty()) {
    int block_idx = tf.selected_block;

    const auto &block = tf.blocks[block_idx];

    if (ImGui::SliderInt("##TimeSlider", &block_idx, 0,
                         static_cast<int>(tf.blocks.size() - 1), block.label.c_str())) {
      if (tf.selected_block != block_idx) {
        tf.selected_block = block_idx;
        changed = true;
        service->RequestCompute();
        // 重置 autofit 跟踪，使得新计算完成后会触发 autofit
        ui.last_autofit_generation = 0;
      }
    }
  } else {
    // 保持占位
    ImGui::BeginDisabled();
    int dummy = 0;
    ImGui::SliderInt("##TimeSlider", &dummy, 0, 0, "---");
    ImGui::EndDisabled();
  }

  return changed;
}

// ============================================================================
// Stationarity Config Panel (Left)
// ============================================================================

// 通用参数 slider 渲染 (完全自动化)
static bool RenderOperatorParams(math::Operator &op, const char *suffix) {
  bool changed = false;
  for (size_t i = 0; i < op.param_count; ++i) {
    auto &m = op.meta[i];
    ImGui::SameLine();
    ImGui::SetNextItemWidth(100);
    char label[32];
    snprintf(label, sizeof(label), "%s##%s%zu", m.name, suffix, i);
    if (ImGui::SliderFloat(label, &op[i], m.min_val, m.max_val, "%.2f")) {
      changed = true;
    }
  }
  return changed;
}

static bool RenderStationaryConfig(Transform::Params &config) {
  TraceN("UI:StationaryConfig");
  bool changed = false;

  ImGui::Text("平稳化");
  ImGui::SameLine();
  ImGui::TextDisabled("(?)");
  if (ImGui::IsItemHovered()) {
    RenderStationarityTooltip();
  }

  ImGui::SameLine();
  ImGui::SetNextItemWidth(150);

  // 使用 Transform::g_stationary 表
  auto &cur = Transform::GetStationaryDef(config.stationary_method);
  if (ImGui::BeginCombo("##st_method", cur.name)) {
    for (size_t i = 0; i < Transform::g_stationary_count; ++i) {
      auto &e = Transform::g_stationary[i];
      if (ImGui::Selectable(e.def->name, config.stationary_method == e.method)) {
        if (config.stationary_method != e.method) {
          config.stationary_method = e.method;
          config.reset_stationary();
          changed = true;
        }
      }
    }
    ImGui::EndCombo();
  }

  // 自动渲染参数
  if (RenderOperatorParams(config.stationary, "st"))
    changed = true;

  return changed;
}

// ============================================================================
// Normalization Config (TS/CS 对仗)
// ============================================================================

// 使用 math::normalize::g_methods 表，无需手动定义

// 统一渲染参数 slider
static bool RenderTSNormConfig(Transform::Params &config) {
  TraceN("UI:TSNormConfig");
  bool changed = false;

  ImGui::Text("TS归一化");
  ImGui::SameLine();
  ImGui::SetNextItemWidth(120);

  auto &cur = math::normalize::GetMethod(config.ts_norm);
  if (ImGui::BeginCombo("##ts_norm", cur.name)) {
    for (size_t i = 0; i < math::normalize::g_method_count; ++i) {
      auto &d = math::normalize::g_methods[i];
      if (ImGui::Selectable(d.name, config.ts_norm == d.method)) {
        if (config.ts_norm != d.method) {
          config.ts_norm = d.method;
          config.reset_ts();
          changed = true;
        }
      }
    }
    ImGui::EndCombo();
  }

  // 自动渲染参数
  if (RenderOperatorParams(config.ts, "ts"))
    changed = true;

  return changed;
}

static bool RenderCSNormConfig(Transform::Params &config) {
  TraceN("UI:CSNormConfig");
  bool changed = false;

  ImGui::Text("CS归一化");
  ImGui::SameLine();
  ImGui::SetNextItemWidth(120);

  auto &cur = math::normalize::GetMethod(config.cs_norm);
  if (ImGui::BeginCombo("##cs_norm", cur.name)) {
    for (size_t i = 0; i < math::normalize::g_method_count; ++i) {
      auto &d = math::normalize::g_methods[i];
      if (ImGui::Selectable(d.name, config.cs_norm == d.method)) {
        if (config.cs_norm != d.method) {
          config.cs_norm = d.method;
          config.reset_cs();
          changed = true;
        }
      }
    }
    ImGui::EndCombo();
  }

  // 自动渲染参数
  if (RenderOperatorParams(config.cs, "cs"))
    changed = true;

  return changed;
}

// ============================================================================
// Bandpass Config
// ============================================================================

static const char *BandpassTypeName(Transform::BandpassType t) {
  switch (t) {
  case Transform::BandpassType::NONE: return "无";
  case Transform::BandpassType::FIR: return "FIR";
  case Transform::BandpassType::IIR: return "IIR";
  }
  return "?";
}

static const char *FIRWindowName(int w) {
  switch (w) {
  case 0: return "Hann";
  case 1: return "Hamming";
  case 2: return "Blackman";
  }
  return "?";
}

static const char *IIRTypeName(int t) {
  switch (t) {
  case 0: return "Butterworth";
  case 1: return "Chebyshev I";
  case 2: return "Chebyshev II";
  }
  return "?";
}

// 非标 bin index → 周期(秒)
static float BinToPeriod(float bin_idx) {
  if (bin_idx < 58.0f) {
    return bin_idx + 2.0f;
  } else if (bin_idx < 117.0f) {
    return (bin_idx - 58.0f + 1.0f) * 60.0f;
  } else if (bin_idx < 127.0f) {
    return (bin_idx - 117.0f + 1.0f) * 3600.0f;
  } else {
    return 1e9f;
  }
}

// bin → 归一化频率 (与 TransformService 一致)
static float BinToFreq(float bin_idx, int level) {
  float period = BinToPeriod(bin_idx);
  float sample_rate = (level == 0) ? 1.0f : (level == 1) ? (1.0f / 60.0f) : (1.0f / 3600.0f);
  float nyquist = sample_rate / 2.0f;
  return std::clamp((1.0f / period) / nyquist, 0.001f, 0.999f);
}

// 检查两个 bin 转换后的频率是否满足 f_lo < f_hi
static bool FreqValid(double lo_bin, double hi_bin, int level) {
  float f_lo = BinToFreq(static_cast<float>(hi_bin), level);
  float f_hi = BinToFreq(static_cast<float>(lo_bin), level);
  return f_lo < f_hi;
}

// 非标bin index → 周期描述 (使用双 buffer 避免连续调用覆盖)
static const char *BinToLabel(float bin_idx, int buf_idx = 0) {
  static char buf[2][32];
  size_t idx = static_cast<size_t>(bin_idx);
  char *b = buf[buf_idx & 1];
  if (idx < 58) {
    std::snprintf(b, 32, "%zus", idx + 2);
  } else if (idx < 117) {
    std::snprintf(b, 32, "%zum", idx - 58 + 1);
  } else if (idx < 127) {
    std::snprintf(b, 32, "%zuh", idx - 117 + 1);
  } else {
    std::snprintf(b, 32, "DC");
  }
  return b;
}

static bool RenderBandpassConfig(Transform::Params &config, TransformUIState &ui) {
  TraceN("UI:BandpassConfig");
  bool changed = false;

  ImGui::Text("带通");
  ImGui::SameLine();
  ImGui::SetNextItemWidth(80);

  // 类型选择
  if (ImGui::BeginCombo("##bp_type", BandpassTypeName(config.bandpass_type))) {
    for (int i = 0; i < 3; ++i) {
      auto t = static_cast<Transform::BandpassType>(i);
      if (ImGui::Selectable(BandpassTypeName(t), config.bandpass_type == t)) {
        if (config.bandpass_type != t) {
          config.bandpass_type = t;
          config.reset_bandpass();
          changed = true;
        }
      }
    }
    ImGui::EndCombo();
  }

  // 子类型 (FIR: 窗类型, IIR: 滤波器类型)
  if (config.bandpass_type != Transform::BandpassType::NONE) {
    ImGui::SameLine();
    ImGui::SetNextItemWidth(100);

    if (config.bandpass_type == Transform::BandpassType::FIR) {
      if (ImGui::BeginCombo("##bp_subtype", FIRWindowName(config.bandpass_subtype))) {
        for (int i = 0; i < 3; ++i) {
          if (ImGui::Selectable(FIRWindowName(i), config.bandpass_subtype == i)) {
            if (config.bandpass_subtype != i) {
              config.bandpass_subtype = i;
              changed = true;
            }
          }
        }
        ImGui::EndCombo();
      }
    } else {
      if (ImGui::BeginCombo("##bp_subtype", IIRTypeName(config.bandpass_subtype))) {
        for (int i = 0; i < 3; ++i) {
          if (ImGui::Selectable(IIRTypeName(i), config.bandpass_subtype == i)) {
            if (config.bandpass_subtype != i) {
              config.bandpass_subtype = i;
              changed = true;
            }
          }
        }
        ImGui::EndCombo();
      }
    }

    // 阶数
    ImGui::SameLine();
    ImGui::SetNextItemWidth(80);
    int order_min = (config.bandpass_type == Transform::BandpassType::FIR) ? 8 : 1;
    int order_max = (config.bandpass_type == Transform::BandpassType::FIR) ? 512 : 8;
    if (ImGui::SliderInt("阶数##bp", &config.bandpass_order, order_min, order_max)) {
      changed = true;
    }

    // 频带显示 (只读, 实际值从光标同步)
    ImGui::SameLine();
    ImGui::TextDisabled("[%s - %s]", BinToLabel(ui.bandpass_lo, 0), BinToLabel(ui.bandpass_hi, 1));
  } else {
    // 未启用带通时也显示光标范围
    ImGui::SameLine();
    ImGui::TextDisabled("[%s - %s]", BinToLabel(ui.bandpass_lo, 0), BinToLabel(ui.bandpass_hi, 1));
  }

  return changed;
}

// ============================================================================
// ADF/KPSS Heatmap (单行紧凑版)
// ============================================================================

static void RenderStationarityHeatmap(const Transform &tf, const Asset &asset) {
  TraceN("UI:StationarityHeatmap");
  // 直接画，不显示"无数据" - 计算线程会很快更新
  if (tf.results.empty()) {
    ImGui::Dummy(ImVec2(0, 18.0f)); // 保持行高稳定
    return;
  }

  const size_t n = tf.results.size();
  ImVec2 avail = ImGui::GetContentRegionAvail();

  // 计算单元格大小，确保 ADF + KPSS 能放在一行
  float label_w = 40.0f;
  float gap = 10.0f;
  float usable = avail.x - label_w * 2 - gap * 3;
  float cell_w = std::min(12.0f, usable / (2 * n));
  float cell_h = 14.0f;

  ImDrawList *draw = ImGui::GetWindowDrawList();
  ImVec2 pos = ImGui::GetCursorScreenPos();
  float y = pos.y + 2;

  // ADF 标签 + 热力条
  draw->AddText(ImVec2(pos.x, y), IM_COL32(180, 180, 180, 255), "ADF");
  float x_adf_start = pos.x + label_w;
  for (size_t i = 0; i < n; ++i) {
    if (!tf.results[i].valid)
      continue;
    float x = x_adf_start + i * cell_w;
    draw->AddRectFilled(ImVec2(x, y), ImVec2(x + cell_w - 1, y + cell_h),
                        GetADFColor(tf.results[i].adf_pval));
  }

  // KPSS 标签 + 热力条
  float x_kpss_label = x_adf_start + n * cell_w + gap;
  draw->AddText(ImVec2(x_kpss_label, y), IM_COL32(180, 180, 180, 255), "KPSS");
  float x_kpss_start = x_kpss_label + label_w;
  for (size_t i = 0; i < n; ++i) {
    if (!tf.results[i].valid)
      continue;
    float x = x_kpss_start + i * cell_w;
    draw->AddRectFilled(ImVec2(x, y), ImVec2(x + cell_w - 1, y + cell_h),
                        GetKPSSColor(tf.results[i].kpss_pval));
  }

  // Tooltip
  ImVec2 mouse = ImGui::GetMousePos();
  if (mouse.y >= y && mouse.y < y + cell_h) {
    for (size_t i = 0; i < n; ++i) {
      if (!tf.results[i].valid)
        continue;
      float xa = x_adf_start + i * cell_w;
      float xk = x_kpss_start + i * cell_w;
      bool in_adf = mouse.x >= xa && mouse.x < xa + cell_w;
      bool in_kpss = mouse.x >= xk && mouse.x < xk + cell_w;

      if (in_adf || in_kpss) {
        const auto &r = tf.results[i];
        ImGui::BeginTooltip();
        if (i < asset.items.size()) {
          ImGui::Text("%s", asset.items[i].asset_code.c_str());
        }
        if (in_adf) {
          ImGui::Text("ADF: %.3f (p=%.3f) %s", r.adf_stat, r.adf_pval,
                      r.adf_pass ? "PASS" : "FAIL");
        } else {
          ImGui::Text("KPSS: %.3f (p=%.3f) %s", r.kpss_stat, r.kpss_pval,
                      r.kpss_pass ? "PASS" : "FAIL");
        }
        ImGui::EndTooltip();
        break;
      }
    }
  }

  ImGui::Dummy(ImVec2(avail.x, cell_h + 4));
}

// ============================================================================
// Time Formatting for Anchor
// ============================================================================

// 格式化索引为时间字符串
// L0: 单日，idx → HH:MM:SS
// L1: 多日，idx → D{day} HH:MM
// L2: 多日，idx → D{day} {hour}H
static void FormatAnchorTime(char *buf, size_t buf_size, size_t idx, int level) {
  if (level == 0) {
    // L0: 单日，直接用 L0_to_Clock
    ClockTime ct = L0_to_Clock(idx);
    std::snprintf(buf, buf_size, "%02d:%02d:%02d", ct.hour, ct.minute, ct.second);
  } else if (level == 1) {
    // L1: 多日，每天 240 分钟
    constexpr size_t MINS_PER_DAY = 240;
    size_t day_idx = idx / MINS_PER_DAY;
    size_t min_idx = idx % MINS_PER_DAY;
    ClockTime ct = L1_to_Clock(min_idx);
    std::snprintf(buf, buf_size, "D%zu %02d:%02d", day_idx, ct.hour, ct.minute);
  } else {
    // L2: 多日，每天 4-5 小时
    constexpr size_t HOURS_PER_DAY = 4;
    size_t day_idx = idx / HOURS_PER_DAY;
    size_t hour_idx = idx % HOURS_PER_DAY;
    uint8_t hour = L2_to_Clock(hour_idx);
    std::snprintf(buf, buf_size, "D%zu %02dH", day_idx, hour);
  }
}

// ============================================================================
// Render Decision Helper: 判断是否应该渲染某个 asset 的数据
// ============================================================================

// 判断是否应该渲染 asset 结果
// 简单逻辑：有数据且 valid 就渲染
static bool ShouldRenderAssetResult(const Transform::AssetResult &r, bool has_data) {
  return r.valid && has_data;
}

// ============================================================================
// Feature Plots (Raw vs Processed)
// ============================================================================

// 更新 min/max 值的辅助函数
static void UpdateMinMax(std::vector<float> &min_vals, std::vector<float> &max_vals, size_t idx, float val) {
  if (idx >= min_vals.size() || !std::isfinite(val))
    return;
  // 初始化为 0，第一次遇到有效值时直接设置，后续更新 min/max
  if (min_vals[idx] == 0.0f && max_vals[idx] == 0.0f) {
    min_vals[idx] = val;
    max_vals[idx] = val;
  } else {
    min_vals[idx] = std::min(min_vals[idx], val);
    max_vals[idx] = std::max(max_vals[idx], val);
  }
}

// 初始化 min/max 数组 (zero allocate: 只在大小变化时 resize)
static void InitMinMaxArrays(std::vector<float> &min_vals, std::vector<float> &max_vals, size_t n_samples) {
  if (min_vals.size() != n_samples) {
    min_vals.resize(n_samples);
    max_vals.resize(n_samples);
  }
  std::fill(min_vals.begin(), min_vals.end(), 0.0f);
  std::fill(max_vals.begin(), max_vals.end(), 0.0f);
}

static void RenderFeaturePlots(const Transform &tf, TransformUIState &ui, bool need_autofit, int level, float height) {
  TraceN("UI:FeaturePlots");
  const size_t n_assets = tf.results.size();
  const int sel = tf.display.selected_asset; // -1 = ALL
  const bool show_all = (sel < 0);
  const bool has_data = !tf.results.empty();
  const size_t n_samples = tf.cache.n_samples;
  const uint64_t cur_gen = tf.compute.generation.load();

  // Clamp anchor_x
  if (n_samples > 0 && ui.anchor_x >= static_cast<double>(n_samples)) {
    ui.anchor_x = static_cast<double>(n_samples - 1);
  }

  // 更新 anchor 缓存 (只在变化时重新计算)
  size_t anchor_idx = static_cast<size_t>(ui.anchor_x);
  auto &cache = ui.anchor_cache;
  if (anchor_idx < n_samples &&
      (cache.idx != anchor_idx || cache.generation != cur_gen || cache.selected_asset != sel)) {
    cache.idx = anchor_idx;
    cache.generation = cur_gen;
    cache.selected_asset = sel;
    cache.raw_y = 0.0;
    cache.norm_y = 0.0;
    cache.valid = false;

    // 查找 raw_y
    for (size_t i = 0; i < tf.cache.raw.size(); ++i) {
      if (!show_all && (int)i != sel)
        continue;
      if (i >= tf.results.size() || !tf.results[i].valid)
        continue;
      const auto &raw = tf.cache.raw[i];
      if (anchor_idx < raw.size() && std::isfinite(raw[anchor_idx])) {
        cache.raw_y = raw[anchor_idx];
        break;
      }
    }

    // 查找 norm_y
    for (size_t i = 0; i < tf.results.size(); ++i) {
      if (!show_all && (int)i != sel)
        continue;
      const auto &r = tf.results[i];
      if (!r.valid || anchor_idx >= r.cs_normed.size())
        continue;
      if (std::isfinite(r.cs_normed[anchor_idx])) {
        cache.norm_y = r.cs_normed[anchor_idx];
        break;
      }
    }

    // 格式化时间字符串
    FormatAnchorTime(cache.time_str, sizeof(cache.time_str), anchor_idx, level);
    cache.valid = true;
  }

  // 左: 原始特征 (从 cache 获取)
  ImGui::BeginChild("RawPlot", ImVec2(ImGui::GetContentRegionAvail().x * 0.5f, height), true);
  ImGui::Text("原始特征");

  if (need_autofit && has_data)
    ImPlot::SetNextAxesToFit();

  if (ImPlot::BeginPlot("##Raw", ImVec2(-1, -1), ImPlotFlags_NoLegend)) {
    ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_NoLabel, ImPlotAxisFlags_NoLabel);

    if (show_all && n_samples > 0) {
      // ALL 模式: 使用 fill in between
      static std::vector<float> min_vals, max_vals;
      InitMinMaxArrays(min_vals, max_vals, n_samples);

      // 遍历每个 asset 的 sparse data
      for (size_t i = 0; i < tf.cache.sparse.size(); ++i) {
        if (i >= tf.results.size() || !ShouldRenderAssetResult(tf.results[i], !tf.cache.sparse[i].empty())) {
          continue;
        }
        const auto &sp = tf.cache.sparse[i];
        for (size_t j = 0; j < sp.size(); ++j) {
          UpdateMinMax(min_vals, max_vals, sp.index[j], sp.value[j]);
        }
      }

      // 准备 x 轴数据，所有点都有值（初始化为 0）
      static std::vector<float> x_data, y_min, y_max;
      x_data.resize(n_samples);
      y_min.resize(n_samples);
      y_max.resize(n_samples);
      for (size_t i = 0; i < n_samples; ++i) {
        x_data[i] = static_cast<float>(i);
        y_min[i] = min_vals[i];
        y_max[i] = max_vals[i];
      }

      if (n_samples > 0) {
        ImVec4 col = ImVec4(1.0f, 1.0f, 0.0f, 1.0f); // 黄色，更明显
        ImPlot::PushStyleColor(ImPlotCol_Fill, col);
        ImPlot::PushStyleVar(ImPlotStyleVar_FillAlpha, 0.6f);
        ImPlot::PlotShaded("##raw_range", x_data.data(), y_min.data(), y_max.data(), static_cast<int>(n_samples));
        ImPlot::PopStyleVar();
        ImPlot::PopStyleColor();
      }
    } else {
      // 单 asset 模式: 画单条线
      for (size_t i = 0; i < tf.cache.raw.size(); ++i) {
        if ((int)i != sel)
          continue;
        const auto &raw = tf.cache.raw[i];
        if (raw.empty())
          continue;
        if (i >= tf.results.size() || !ShouldRenderAssetResult(tf.results[i], !raw.empty())) {
          continue;
        }
        ImVec4 col = GetAssetColor(i, n_assets);
        ImPlot::SetNextLineStyle(col, 0.8f);
        ImPlot::PlotLine("##r", raw.data(), static_cast<int>(raw.size()));
      }
    }

    // 光标 (DragLineX)
    if (n_samples > 0) {
      bool drag_changed = ImPlot::DragLineX(0, &ui.anchor_x, ImVec4(1, 0.5f, 0, 1), 2.0f);
      bool drag_active = ImGui::IsItemActive();

      // Snap on release
      if (drag_changed && !drag_active) {
        ui.anchor_x = std::clamp(std::round(ui.anchor_x), 0.0, static_cast<double>(n_samples - 1));
      }

      // Double-click to set anchor
      if (ImPlot::IsPlotHovered() && ImGui::IsMouseDoubleClicked(0)) {
        ui.anchor_x = std::clamp(std::round(ImPlot::GetPlotMousePos().x), 0.0, static_cast<double>(n_samples - 1));
      }

      // Annotation: 使用缓存
      if (cache.valid) {
        ImPlot::Annotation(ui.anchor_x, cache.raw_y, ImVec4(1, 0.5f, 0, 1), ImVec2(5, -15), false, "%s", cache.time_str);
      }
    }

    ImPlot::EndPlot();
  }
  ImGui::EndChild();

  ImGui::SameLine();

  // 右: 处理后特征 (带通启用时显示带通后的数据)
  ImGui::BeginChild("ProcPlot", ImVec2(0, height), true);
  bool use_bandpass = (tf.params.bandpass_type != Transform::BandpassType::NONE);
  if (use_bandpass) {
    ImGui::Text("处理后特征 (带通)");
  } else {
    ImGui::Text("处理后特征");
  }

  if (need_autofit && has_data)
    ImPlot::SetNextAxesToFit();

  if (ImPlot::BeginPlot("##Proc", ImVec2(-1, -1), ImPlotFlags_NoLegend)) {
    ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_NoLabel, ImPlotAxisFlags_NoLabel);

    if (show_all && n_samples > 0) {
      // ALL 模式: 使用 fill in between
      static std::vector<float> min_vals, max_vals;
      InitMinMaxArrays(min_vals, max_vals, n_samples);

      // 遍历每个 asset 的数据 (带通启用时用 bandpass，否则用 cs_normed)
      for (size_t i = 0; i < tf.results.size(); ++i) {
        const auto &r = tf.results[i];
        const auto &vec = use_bandpass ? r.bandpass : r.cs_normed;
        if (!ShouldRenderAssetResult(r, !vec.empty())) {
          continue;
        }
        for (size_t idx = 0; idx < std::min(n_samples, vec.size()); ++idx) {
          UpdateMinMax(min_vals, max_vals, idx, vec[idx]);
        }
      }

      // 准备 x 轴数据，所有点都有值（初始化为 0）
      static std::vector<float> x_data, y_min, y_max;
      x_data.resize(n_samples);
      y_min.resize(n_samples);
      y_max.resize(n_samples);
      for (size_t i = 0; i < n_samples; ++i) {
        x_data[i] = static_cast<float>(i);
        y_min[i] = min_vals[i];
        y_max[i] = max_vals[i];
      }

      if (n_samples > 0) {
        ImVec4 col = use_bandpass ? ImVec4(0.3f, 0.9f, 0.5f, 1.0f) : ImVec4(1.0f, 1.0f, 0.0f, 1.0f);
        ImPlot::PushStyleColor(ImPlotCol_Fill, col);
        ImPlot::PushStyleVar(ImPlotStyleVar_FillAlpha, 0.6f);
        ImPlot::PlotShaded("##norm_range", x_data.data(), y_min.data(), y_max.data(), static_cast<int>(n_samples));
        ImPlot::PopStyleVar();
        ImPlot::PopStyleColor();
      }
    } else {
      // 单 asset 模式: 画单条线
      for (size_t i = 0; i < tf.results.size(); ++i) {
        if ((int)i != sel)
          continue;
        const auto &r = tf.results[i];
        const auto &vec = use_bandpass ? r.bandpass : r.cs_normed;
        if (vec.empty())
          continue;
        if (!ShouldRenderAssetResult(r, !vec.empty())) {
          continue;
        }
        ImVec4 col = use_bandpass ? ImVec4(0.3f, 0.9f, 0.5f, 1.0f) : GetAssetColor(i, n_assets);
        ImPlot::SetNextLineStyle(col, 0.8f);
        ImPlot::PlotLine("##n", vec.data(), static_cast<int>(vec.size()));
      }
    }

    // 光标 (同步)
    if (n_samples > 0) {
      bool drag_changed = ImPlot::DragLineX(1, &ui.anchor_x, ImVec4(1, 0.5f, 0, 1), 2.0f);
      bool drag_active = ImGui::IsItemActive();

      if (drag_changed && !drag_active) {
        ui.anchor_x = std::clamp(std::round(ui.anchor_x), 0.0, static_cast<double>(n_samples - 1));
      }

      if (ImPlot::IsPlotHovered() && ImGui::IsMouseDoubleClicked(0)) {
        ui.anchor_x = std::clamp(std::round(ImPlot::GetPlotMousePos().x), 0.0, static_cast<double>(n_samples - 1));
      }

      // Annotation: 使用缓存
      if (cache.valid) {
        ImPlot::Annotation(ui.anchor_x, cache.norm_y, ImVec4(1, 0.5f, 0, 1), ImVec2(5, -15), false, "%s", cache.time_str);
      }
    }

    ImPlot::EndPlot();
  }
  ImGui::EndChild();
}

// ============================================================================
// Asset PDF & FFT (直接从 AssetResult 读取，零分配)
// ============================================================================

static void RenderBottomPlots(const Transform &tf, const SharedData &data, TransformUIState &ui, bool need_autofit, int level, float height) {
  TraceN("UI:BottomPlots");

  const size_t n_assets = tf.results.size();
  const int sel = tf.display.selected_asset; // -1 = ALL
  const bool show_all = (sel < 0);

  size_t n_valid = 0;
  for (const auto &r : tf.results) {
    if (r.valid)
      ++n_valid;
  }

  // 左: 每个 asset 的 PDF 叠加 (直接从 KLLcache 读取)
  ImGui::BeginChild("PDFPlot", ImVec2(ImGui::GetContentRegionAvail().x * 0.5f, height), true);
  ImGui::Text("资产分布 (n=%zu)", tf.results.size());

  if (ImPlot::BeginPlot("##PDF", ImVec2(-1, -1), ImPlotFlags_NoLegend)) {
    // 先收集所有要渲染的数据
    struct PDFData {
      size_t idx;
      KLLcache::LinePtr pdf;
    };
    static std::vector<PDFData> to_render;
    to_render.clear();
    
    for (size_t i = 0; i < tf.results.size(); ++i) {
      if (!show_all && (int)i != sel)
        continue;
      const auto &r = tf.results[i];
      if (r.KLL.empty() || !ShouldRenderAssetResult(r, !r.KLL.empty()))
        continue;
      auto pdf = r.KLL.exportPDF();
      if (pdf.n > 0) {
        to_render.push_back({i, pdf});
      }
    }
    
    // 只在 need_autofit 时计算一次整体 range (merge 所有 KLL 后计算)
    float x_extent = 0.0f;
    if (need_autofit && !to_render.empty()) {
      // Merge 所有 asset 的 KLL 到一个临时 KLL
      static KLLcache merged_kll(512, 1024);
      merged_kll.clear();
      
      for (const auto &d : to_render) {
        const auto &r = tf.results[d.idx];
        if (!r.KLL.empty()) {
          merged_kll.mergeWith(r.KLL);
        }
      }
      
      if (!merged_kll.empty()) {
        // 直接从 ICDF 获取 2.5% 和 97.5% 分位数 (tail cut)
        auto icdf = merged_kll.exportICDF();
        if (icdf.n >= 2) {
          // ICDF: u ∈ [0,1] 均匀分布，直接计算索引
          size_t i025 = static_cast<size_t>(0.025f * (icdf.n - 1));
          size_t i975 = static_cast<size_t>(0.975f * (icdf.n - 1));
          
          float x_low = icdf.y[i025];
          float x_high = icdf.y[i975];
          
          // 取绝对值最大值，用于对称居中
          x_extent = std::max(std::abs(x_low), std::abs(x_high));
        }
      }
    }

    // 设置轴: 使用收集好的 range
    ImPlotAxisFlags y_flags = ImPlotAxisFlags_NoLabel | (need_autofit ? ImPlotAxisFlags_AutoFit : 0);
    ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_NoLabel, y_flags);
    if (need_autofit && x_extent > 0.0f) {
      float margin = x_extent * 0.05f;
      ImPlot::SetupAxisLimits(ImAxis_X1, -(x_extent + margin), x_extent + margin, ImGuiCond_Always);
    }

    // 渲染收集好的数据
    for (const auto &d : to_render) {
      ImVec4 col = GetAssetColor(d.idx, n_assets);
      ImPlot::SetNextLineStyle(col, 0.8f);
      ImPlot::PlotLine("##KLL", d.pdf.x, d.pdf.y, static_cast<int>(d.pdf.n));
    }

    ImPlot::EndPlot();
  }
  ImGui::EndChild();

  ImGui::SameLine();

  // 右: PSD 功率谱 (非标周期轴, 128 bins)
  ImGui::BeginChild("PSDPlot", ImVec2(0, height), true);

  // 标题 + 频段比例 (四色bar)
  const auto &psd = tf.psd;
  ImGui::Text("PSD");
  if (psd.valid) {
    ImGui::SameLine();

    // 固定长度bar
    constexpr float BAR_W = 200.0f;
    constexpr float BAR_H = 14.0f;

    ImVec2 bar_pos = ImGui::GetCursorScreenPos();
    ImDrawList *draw = ImGui::GetWindowDrawList();

    // 四色: 秒(蓝) 分(绿) 时(橙) DC(灰)
    ImU32 col_sec = IM_COL32(80, 140, 200, 255);
    ImU32 col_min = IM_COL32(120, 180, 80, 255);
    ImU32 col_hour = IM_COL32(220, 140, 60, 255);
    ImU32 col_dc = IM_COL32(140, 140, 140, 255);

    float x = bar_pos.x;
    float y = bar_pos.y;
    float w_sec = BAR_W * psd.ratio_sec;
    float w_min = BAR_W * psd.ratio_min;
    float w_hour = BAR_W * psd.ratio_hour;
    float w_dc = BAR_W * psd.ratio_dc;

    draw->AddRectFilled(ImVec2(x, y), ImVec2(x + w_sec, y + BAR_H), col_sec);
    x += w_sec;
    draw->AddRectFilled(ImVec2(x, y), ImVec2(x + w_min, y + BAR_H), col_min);
    x += w_min;
    draw->AddRectFilled(ImVec2(x, y), ImVec2(x + w_hour, y + BAR_H), col_hour);
    x += w_hour;
    draw->AddRectFilled(ImVec2(x, y), ImVec2(x + w_dc, y + BAR_H), col_dc);

    // 占位 + tooltip
    ImGui::Dummy(ImVec2(BAR_W, BAR_H));
    if (ImGui::IsItemHovered()) {
      ImGui::BeginTooltip();
      ImGui::TextColored(ImVec4(0.3f, 0.55f, 0.8f, 1.0f), "秒级: %.1f%%", psd.ratio_sec * 100.0f);
      ImGui::TextColored(ImVec4(0.47f, 0.7f, 0.3f, 1.0f), "分钟级: %.1f%%", psd.ratio_min * 100.0f);
      ImGui::TextColored(ImVec4(0.86f, 0.55f, 0.24f, 1.0f), "小时级: %.1f%%", psd.ratio_hour * 100.0f);
      ImGui::TextColored(ImVec4(0.55f, 0.55f, 0.55f, 1.0f), "DC: %.1f%%", psd.ratio_dc * 100.0f);
      ImGui::EndTooltip();
    }
  }

  // 准备刻度指针
  static std::vector<const char *> tick_ptrs;
  if (tick_ptrs.size() != psd.tick_labels.size()) {
    tick_ptrs.resize(psd.tick_labels.size());
    for (size_t i = 0; i < psd.tick_labels.size(); ++i) {
      tick_ptrs[i] = psd.tick_labels[i].c_str();
    }
  }

  constexpr size_t N_BINS = 128;
  static std::vector<float> psd_log;
  psd_log.resize(N_BINS);

  if (ImPlot::BeginPlot("##PSD", ImVec2(-1, -1), ImPlotFlags_NoLegend)) {
    ImPlot::SetupAxes("Period", "Power");
    ImPlot::SetupAxisLimits(ImAxis_X1, 0, static_cast<double>(N_BINS), ImGuiCond_Once);
    ImPlot::SetupAxisLimits(ImAxis_Y1, -1.0, 3.0, ImGuiCond_Always);

    if (!psd.tick_positions.empty()) {
      ImPlot::SetupAxisTicks(ImAxis_X1, psd.tick_positions.data(),
                             static_cast<int>(psd.tick_positions.size()),
                             tick_ptrs.data());
    }

    // 三色背景 (秒/分/时)
    ImPlotRect limits = ImPlot::GetPlotLimits();
    ImPlot::PushPlotClipRect();
    ImDrawList *draw = ImPlot::GetPlotDrawList();

    ImU32 col_sec = IM_COL32(50, 100, 150, 40);
    ImU32 col_min = IM_COL32(100, 130, 50, 40);
    ImU32 col_hour = IM_COL32(150, 80, 50, 40);

    float y_top = static_cast<float>(limits.Y.Max);
    float y_bot = static_cast<float>(limits.Y.Min);

    // 秒级背景 (0-58)
    {
      ImVec2 p0 = ImPlot::PlotToPixels(0, y_top);
      ImVec2 p1 = ImPlot::PlotToPixels(58, y_bot);
      draw->AddRectFilled(p0, p1, col_sec);
    }
    // 分钟级背景 (58-117)
    {
      ImVec2 p0 = ImPlot::PlotToPixels(58, y_top);
      ImVec2 p1 = ImPlot::PlotToPixels(117, y_bot);
      draw->AddRectFilled(p0, p1, col_min);
    }
    // 小时级背景 (117-128)
    {
      ImVec2 p0 = ImPlot::PlotToPixels(117, y_top);
      ImVec2 p1 = ImPlot::PlotToPixels(128, y_bot);
      draw->AddRectFilled(p0, p1, col_hour);
    }

    ImPlot::PopPlotClipRect();

    // 画 PSD
    if (psd.valid) {
      if (show_all) {
        // ALL 模式: 只画平均 PSD (已经是先能量平均再 dB)
        ImPlot::SetNextLineStyle(ImVec4(1.0f, 0.8f, 0.2f, 1.0f), 2.0f);
        ImPlot::PlotLine("Avg", psd.plot_x.data(), psd.avg_psd_db.data(), static_cast<int>(N_BINS));
      } else if (sel >= 0 && sel < (int)psd.asset_psd.size() && tf.results[sel].valid) {
        // 单 asset 模式: 只画该 asset
        const auto &src = psd.asset_psd[sel];
        for (size_t k = 0; k < N_BINS; ++k) {
          float v = src[k];
          psd_log[k] = (v > 1e-20f) ? std::log10(v) : -20.0f;
        }
        ImPlot::SetNextLineStyle(ImVec4(1.0f, 0.8f, 0.2f, 1.0f), 2.0f);
        ImPlot::PlotLine("Asset", psd.plot_x.data(), psd_log.data(), static_cast<int>(N_BINS));
      }
    }

    // 带通光标 (始终显示)
    // 绘制选中区域半透明填充
    ImPlot::PushPlotClipRect();
    ImDrawList *draw_bp = ImPlot::GetPlotDrawList();
    {
      ImVec2 p0 = ImPlot::PlotToPixels(ui.bandpass_lo, limits.Y.Max);
      ImVec2 p1 = ImPlot::PlotToPixels(ui.bandpass_hi, limits.Y.Min);
      // 启用带通时绿色，否则灰色
      ImU32 fill_col = (tf.params.bandpass_type != Transform::BandpassType::NONE)
                           ? IM_COL32(100, 200, 100, 60)
                           : IM_COL32(150, 150, 150, 40);
      draw_bp->AddRectFilled(p0, p1, fill_col);
    }
    ImPlot::PopPlotClipRect();

    // 保存旧值用于回退
    constexpr double MIN_GAP = 5.0;
    double old_lo = ui.bandpass_lo;
    double old_hi = ui.bandpass_hi;

    // 低频光标 (绿色)
    ImPlot::DragLineX(100, &ui.bandpass_lo, ImVec4(0.3f, 0.9f, 0.3f, 1.0f), 2.0f);
    if (ImGui::IsItemActive() || ImGui::IsItemHovered()) {
      ImPlot::Annotation(ui.bandpass_lo, limits.Y.Max, ImVec4(0.3f, 0.9f, 0.3f, 1.0f),
                         ImVec2(5, -10), false, "Lo: %s", BinToLabel(static_cast<float>(ui.bandpass_lo), 0));
    }
    // lo 约束：clamp，先保证 MIN_GAP，再确保 FreqValid
    ui.bandpass_lo = std::clamp(ui.bandpass_lo, 0.0, 127.0 - MIN_GAP);
    // 推 hi 保持 MIN_GAP
    if (ui.bandpass_hi - ui.bandpass_lo < MIN_GAP) {
      ui.bandpass_hi = ui.bandpass_lo + MIN_GAP;
    }
    // 继续推 hi 直到 FreqValid
    while (ui.bandpass_hi <= 127.0 && !FreqValid(ui.bandpass_lo, ui.bandpass_hi, level)) {
      ui.bandpass_hi += 1.0;
    }
    // hi 超边界则回退 lo
    if (ui.bandpass_hi > 127.0) {
      ui.bandpass_hi = 127.0;
      ui.bandpass_lo = old_lo;
    }

    // 高频光标 (红色)
    ImPlot::DragLineX(101, &ui.bandpass_hi, ImVec4(0.9f, 0.3f, 0.3f, 1.0f), 2.0f);
    if (ImGui::IsItemActive() || ImGui::IsItemHovered()) {
      ImPlot::Annotation(ui.bandpass_hi, limits.Y.Max, ImVec4(0.9f, 0.3f, 0.3f, 1.0f),
                         ImVec2(5, -10), false, "Hi: %s", BinToLabel(static_cast<float>(ui.bandpass_hi), 0));
    }
    // hi 约束：clamp，先保证 MIN_GAP，再确保 FreqValid
    ui.bandpass_hi = std::clamp(ui.bandpass_hi, MIN_GAP, 127.0);
    // 推 lo 保持 MIN_GAP
    if (ui.bandpass_hi - ui.bandpass_lo < MIN_GAP) {
      ui.bandpass_lo = ui.bandpass_hi - MIN_GAP;
    }
    // 继续推 lo 直到 FreqValid
    while (ui.bandpass_lo >= 0.0 && !FreqValid(ui.bandpass_lo, ui.bandpass_hi, level)) {
      ui.bandpass_lo -= 1.0;
    }
    // lo 低于边界则回退 hi
    if (ui.bandpass_lo < 0.0) {
      ui.bandpass_lo = 0.0;
      ui.bandpass_hi = old_hi;
    }

    ImPlot::EndPlot();
  }
  ImGui::EndChild();
}

// ============================================================================
// Main Render
// ============================================================================

void RenderTabTransform(TransformService *service, SharedData &data, TransformUIState &ui) {
  TraceN("UI:RenderTabTransform");
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

  // Autozoom 逻辑: 计算完成后触发
  {
    TraceN("UI:AutozoomCheck");
    int cur_asset = tf.display.selected_asset;
    uint64_t cur_gen = tf.compute.generation.load();
    bool should_autofit = false;

    if (cur_asset < 0) {
      // ALL mode: 只在计算完成时触发
      if (tf.compute.status == Transform::Compute::Status::Done &&
          (ui.last_autofit_asset != -1 || ui.last_autofit_generation != cur_gen)) {
        should_autofit = true;
      }
    } else {
      // 单 asset mode: 当选中的 asset 有效数据时触发
      if (cur_asset >= 0 && cur_asset < (int)tf.results.size() &&
          tf.results[cur_asset].valid &&
          (ui.last_autofit_asset != cur_asset || ui.last_autofit_generation != cur_gen)) {
        should_autofit = true;
      }
    }

    if (should_autofit) {
      ui.need_autofit = true;
      ui.last_autofit_asset = cur_asset;
      ui.last_autofit_generation = cur_gen;
    }
  }

  // 第一行: 状态 + 级别 + 特征
  Render_StatusInfo(data);

  // 第二行: 平稳化
  bool st_changed = RenderStationaryConfig(tf.params);

  // 第三行: 归一化 (TS/CS 各自独立参数)
  bool ts_changed = RenderTSNormConfig(tf.params);
  bool cs_changed = RenderCSNormConfig(tf.params);
  bool norm_changed = ts_changed || cs_changed;

  // 第四行: 带通滤波
  bool bp_changed = RenderBandpassConfig(tf.params, ui);

  // ADF/KPSS热力图
  RenderStationarityHeatmap(tf, data.asset);

  // 第三行: Asset选择 + 时间窗口滑块
  bool sel_changed = Render_AssetAndWindow(service, data, ui);

  // 参数变化触发重计算 (autozoom 由上面的逻辑自动处理)
  if (st_changed || norm_changed || bp_changed) {
    ui.params_changed = true;
    service->RequestCompute();
    // 重置 autofit 跟踪，使得新计算完成后会触发 autofit
    ui.last_autofit_generation = 0;
  }
  (void)sel_changed; // asset选择变化不再直接触发autozoom

  // 更新渲染缓存
  {
    uint64_t cur_gen = tf.compute.generation.load();
    if (tf.compute.status == Transform::Compute::Status::Done &&
        cur_gen != ui.last_rendered_generation) {
      ui.last_rendered_generation = cur_gen;
    }
  }

  // 获取当前 level
  int level = data.feature.selection.selected_level;

  // 计算剩余可用空间，平均分配给两行plot（每行各占50%高度）
  float avail_h = ImGui::GetContentRegionAvail().y;
  float plot_height = std::max(100.0f, avail_h * 0.5f); // 每行plot占剩余空间的50%

  // 特征对比图
  RenderFeaturePlots(tf, ui, ui.need_autofit, level, plot_height);

  // 底部: PDF + PSD
  RenderBottomPlots(tf, data, ui, ui.need_autofit, level, plot_height);

  // 同步光标值到 params (用于计算)
  if (tf.params.bandpass_type != Transform::BandpassType::NONE) {
    float lo = static_cast<float>(ui.bandpass_lo);
    float hi = static_cast<float>(ui.bandpass_hi);
    if (std::abs(tf.params.bandpass_lo_bin - lo) >= 0.5f ||
        std::abs(tf.params.bandpass_hi_bin - hi) >= 0.5f) {
      tf.params.bandpass_lo_bin = lo;
      tf.params.bandpass_hi_bin = hi;
      service->RequestCompute();
    }
  }

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
