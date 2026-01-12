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

static bool Render_AssetAndWindow(TransformService *service, SharedData &data) {
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

    // 格式化为 YY/MM/DD
    char time_label[32];
    const auto &block = tf.blocks[block_idx];
    if (block.date.size() >= 8) {
      // YYYYMMDD -> YY/MM/DD
      snprintf(time_label, sizeof(time_label), "%s/%s/%s",
               block.date.substr(2, 2).c_str(),
               block.date.substr(4, 2).c_str(),
               block.date.substr(6, 2).c_str());
    } else {
      snprintf(time_label, sizeof(time_label), "%s", block.display.c_str());
    }

    if (ImGui::SliderInt("##TimeSlider", &block_idx, 0,
                         static_cast<int>(tf.blocks.size() - 1), time_label)) {
      if (tf.selected_block != block_idx) {
        tf.selected_block = block_idx;
        changed = true;
        service->RequestCompute();
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
  static const char *st_items[] = {"无", "MA去趋势", "整数差分", "分数差分"};
  int method = static_cast<int>(config.stationary_method);
  if (ImGui::Combo("##st_method", &method, st_items, IM_ARRAYSIZE(st_items))) {
    config.stationary_method = static_cast<Transform::StationaryMethod>(method);
    changed = true;
  }

  // 按需显示参数
  switch (config.stationary_method) {
  case Transform::StationaryMethod::MA_DETREND:
    ImGui::SameLine();
    ImGui::SetNextItemWidth(300);
    if (ImGui::SliderInt("窗口##ma", &config.ma_window, 10, 500)) {
      changed = true;
    }
    break;
  case Transform::StationaryMethod::INT_DIFF:
    ImGui::SameLine();
    ImGui::SetNextItemWidth(200);
    if (ImGui::SliderInt("阶数##diff", &config.diff_order, 1, 3)) {
      changed = true;
    }
    break;
  case Transform::StationaryMethod::FRAC_DIFF:
    ImGui::SameLine();
    ImGui::SetNextItemWidth(200);
    if (ImGui::SliderFloat("d##frac", &config.frac_d, 0.0f, 1.0f, "%.2f")) {
      changed = true;
    }
    ImGui::SameLine();
    ImGui::SetNextItemWidth(300);
    if (ImGui::SliderInt("窗口##frac", &config.frac_window, 10, 500)) {
      changed = true;
    }
    break;
  default:
    break;
  }

  return changed;
}

// ============================================================================
// Normalization Config Panel (Right)
// ============================================================================

static bool RenderNormConfig(Transform::Params &config) {
  TraceN("UI:NormConfig");
  bool changed = false;

  ImGui::Text("归一化");
  ImGui::SameLine();
  ImGui::SetNextItemWidth(150);

  // Combo需要连续索引，建立映射
  static const struct {
    NormMethod method;
    const char *name;
  } norm_items[] = {
      {NormMethod::NONE, "NONE"},
      {NormMethod::ZSCORE, "ZSCORE"},
      {NormMethod::ROBUST_ZSCORE, "ROBUST"},
      {NormMethod::IQR_ZSCORE, "IQR"},
      {NormMethod::RANK, "RANK"},
      {NormMethod::RANK_ZSCORE, "RANK_Z"},
      {NormMethod::CLIP, "CLIP"},
      {NormMethod::WINSOR, "WINSOR"},
      {NormMethod::LOG, "LOG"},
      {NormMethod::POWER, "POWER"},
      {NormMethod::ASINH, "ASINH"},
      {NormMethod::TANH, "TANH"},
      {NormMethod::LOG_ZSCORE, "LOG_Z"},
      {NormMethod::CLIP_ZSCORE, "CLP_Z"},
      {NormMethod::WINSOR_ZSCORE, "WIN_Z"},
      {NormMethod::CLIP_LOG_ZSCORE, "CLG_Z"},
  };
  constexpr int n_items = IM_ARRAYSIZE(norm_items);

  // 查找当前索引
  int cur_idx = 0;
  for (int i = 0; i < n_items; ++i) {
    if (norm_items[i].method == config.norm_method) {
      cur_idx = i;
      break;
    }
  }

  if (ImGui::BeginCombo("##norm_method", norm_items[cur_idx].name)) {
    for (int i = 0; i < n_items; ++i) {
      bool selected = (i == cur_idx);
      if (ImGui::Selectable(norm_items[i].name, selected)) {
        config.norm_method = norm_items[i].method;
        changed = true;
      }
      if (selected)
        ImGui::SetItemDefaultFocus();
    }
    ImGui::EndCombo();
  }

  // 按需显示参数
  bool needs_clip = config.norm_method == NormMethod::CLIP ||
                    config.norm_method == NormMethod::CLIP_ZSCORE ||
                    config.norm_method == NormMethod::CLIP_LOG_ZSCORE;
  bool needs_winsor = config.norm_method == NormMethod::WINSOR ||
                      config.norm_method == NormMethod::WINSOR_ZSCORE;
  bool needs_power = config.norm_method == NormMethod::POWER ||
                     config.norm_method == NormMethod::POWER_ZSCORE;

  if (needs_clip) {
    ImGui::SameLine();
    ImGui::SetNextItemWidth(250);
    if (ImGui::SliderFloat("k##clip", &config.clip_k, 1.0f, 10.0f, "%.1f")) {
      changed = true;
    }
  }

  if (needs_winsor) {
    ImGui::SameLine();
    ImGui::SetNextItemWidth(250);
    if (ImGui::SliderFloat("pct##win", &config.winsor_pct, 0.01f, 0.25f, "%.2f")) {
      changed = true;
    }
  }

  if (needs_power) {
    ImGui::SameLine();
    ImGui::SetNextItemWidth(250);
    if (ImGui::SliderFloat("α##pow", &config.power_alpha, 0.1f, 2.0f, "%.2f")) {
      changed = true;
    }
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

// 判断是否应该渲染 asset 结果（避免计算过程中的空白）
static bool ShouldRenderAssetResult(const Transform::AssetResult &r, size_t cur_gen,
                                    size_t last_rendered_gen,
                                    Transform::Compute::Status status,
                                    bool has_data) {
  if (r.valid) {
    return true; // 新数据已准备好
  }
  if (!has_data) {
    return false; // 没有数据可显示
  }
  // 新计算进行中，继续显示旧数据避免空白
  if (cur_gen > last_rendered_gen && status == Transform::Compute::Status::Computing) {
    return true;
  }
  // 旧数据，继续显示
  if (cur_gen == last_rendered_gen) {
    return true;
  }
  return false;
}

// ============================================================================
// Feature Plots (Raw vs Processed)
// ============================================================================

static void RenderFeaturePlots(const Transform &tf, TransformUIState &ui, bool need_autofit, int level) {
  TraceN("UI:FeaturePlots");
  const size_t n_assets = tf.results.size();
  const int sel = tf.display.selected_asset; // -1 = ALL
  const bool show_all = (sel < 0);
  const bool has_data = !tf.results.empty();
  const size_t n_samples = tf.cache.n_samples;
  const size_t cur_gen = tf.compute.generation.load();

  // 动态计算高度: 剩余高度的45%给特征图, 45%给底部图, 10%留白
  float avail_h = ImGui::GetContentRegionAvail().y;
  float height = std::max(100.0f, avail_h * 0.45f);

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
      if (!r.valid || anchor_idx >= r.normalized.size())
        continue;
      if (std::isfinite(r.normalized[anchor_idx])) {
        cache.norm_y = r.normalized[anchor_idx];
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
    ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_NoLabel,
                      ImPlotAxisFlags_NoLabel);

    for (size_t i = 0; i < tf.cache.raw.size(); ++i) {
      if (!show_all && (int)i != sel)
        continue;
      const auto &raw = tf.cache.raw[i];
      if (raw.empty())
        continue;
      // 判断是否应该渲染（避免计算过程中的空白）
      if (i >= tf.results.size() || !ShouldRenderAssetResult(tf.results[i], cur_gen, ui.last_rendered_generation, tf.compute.status, !raw.empty())) {
        continue;
      }
      ImVec4 col = GetAssetColor(i, n_assets);
      ImPlot::SetNextLineStyle(col, 0.8f);
      ImPlot::PlotLine("##r", raw.data(), static_cast<int>(raw.size()));
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

  // 右: 处理后特征
  ImGui::BeginChild("ProcPlot", ImVec2(0, height), true);
  ImGui::Text("处理后特征");

  if (need_autofit && has_data)
    ImPlot::SetNextAxesToFit();

  if (ImPlot::BeginPlot("##Proc", ImVec2(-1, -1), ImPlotFlags_NoLegend)) {
    ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_NoLabel,
                      ImPlotAxisFlags_NoLabel);

    for (size_t i = 0; i < tf.results.size(); ++i) {
      if (!show_all && (int)i != sel)
        continue;
      const auto &r = tf.results[i];
      if (r.normalized.empty())
        continue;
      // 判断是否应该渲染（避免计算过程中的空白）
      if (!ShouldRenderAssetResult(r, cur_gen, ui.last_rendered_generation, tf.compute.status, !r.normalized.empty())) {
        continue;
      }
      ImVec4 col = GetAssetColor(i, n_assets);
      ImPlot::SetNextLineStyle(col, 0.8f);
      ImPlot::PlotLine("##n", r.normalized.data(),
                       static_cast<int>(r.normalized.size()));
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

static void RenderBottomPlots(const Transform &tf, TransformUIState &ui, bool need_autofit, int level) {
  TraceN("UI:BottomPlots");
  float height = std::max(120.0f, ImGui::GetContentRegionAvail().y - 5.0f);

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

  if (need_autofit && n_valid > 0)
    ImPlot::SetNextAxesToFit();

  if (ImPlot::BeginPlot("##PDF", ImVec2(-1, -1), ImPlotFlags_NoLegend)) {
    ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_NoLabel,
                      ImPlotAxisFlags_NoLabel);

    // 直接从 AssetResult.KLL 读取 (exportPDF 返回内部指针，零 copy)
    const size_t cur_gen = tf.compute.generation.load();
    for (size_t i = 0; i < tf.results.size(); ++i) {
      if (!show_all && (int)i != sel)
        continue;
      const auto &r = tf.results[i];
      // 判断是否应该渲染（避免计算过程中的空白）
      auto KLL = r.KLL.exportPDF();
      if (!ShouldRenderAssetResult(r, cur_gen, ui.last_rendered_generation, tf.compute.status, KLL.n > 0)) {
        continue;
      }
      if (KLL.n > 0) {
        ImVec4 col = GetAssetColor(i, n_assets);
        ImPlot::SetNextLineStyle(col, 0.8f);
        ImPlot::PlotLine("##KLL", KLL.x, KLL.y, static_cast<int>(KLL.n));
      }
    }

    ImPlot::EndPlot();
  }
  ImGui::EndChild();

  ImGui::SameLine();

  // 右: 每个 asset 的 FFT 功率谱叠加 (背景根据周期区分秒/分钟/小时)
  ImGui::BeginChild("FFTPlot", ImVec2(0, height), true);
  ImGui::Text("FFT功率谱");

  if (need_autofit && n_valid > 0)
    ImPlot::SetNextAxesToFit();

  if (ImPlot::BeginPlot("##FFT", ImVec2(-1, -1), ImPlotFlags_NoLegend)) {
    ImPlot::SetupAxes("Frequency", "Power", ImPlotAxisFlags_NoLabel,
                      ImPlotAxisFlags_NoLabel);

    // 绘制背景色带 (根据周期区分)
    const float dt[] = {1.0f, 60.0f, 3600.0f};
    float sample_dt = dt[std::clamp(level, 0, 2)];

    ImPlotRect limits = ImPlot::GetPlotLimits();
    ImPlot::PushPlotClipRect();
    ImDrawList *draw = ImPlot::GetPlotDrawList();

    ImU32 col_sec = IM_COL32(50, 100, 150, 60);
    ImU32 col_min = IM_COL32(100, 130, 50, 60);
    ImU32 col_hour = IM_COL32(150, 80, 50, 60);

    float freq_sec_lo = sample_dt / 60.0f;
    float freq_sec_hi = sample_dt / 2.0f;
    float freq_min_lo = sample_dt / 3600.0f;
    float freq_min_hi = freq_sec_lo;
    float freq_hour_hi = freq_min_lo;

    float y_top = static_cast<float>(limits.Y.Max);
    float y_bot = static_cast<float>(limits.Y.Min);

    // 秒级背景
    if (freq_sec_lo < 0.5f && freq_sec_lo < limits.X.Max) {
      float x0 = std::max(static_cast<float>(limits.X.Min), freq_sec_lo);
      float x1 = std::min(static_cast<float>(limits.X.Max), std::min(freq_sec_hi, 0.5f));
      if (x0 < x1) {
        ImVec2 p0 = ImPlot::PlotToPixels(x0, y_top);
        ImVec2 p1 = ImPlot::PlotToPixels(x1, y_bot);
        draw->AddRectFilled(p0, p1, col_sec);
      }
    }

    // 分钟级背景
    if (freq_min_lo < freq_min_hi && freq_min_lo < limits.X.Max) {
      float x0 = std::max(static_cast<float>(limits.X.Min), freq_min_lo);
      float x1 = std::min(static_cast<float>(limits.X.Max), freq_min_hi);
      if (x0 < x1) {
        ImVec2 p0 = ImPlot::PlotToPixels(x0, y_top);
        ImVec2 p1 = ImPlot::PlotToPixels(x1, y_bot);
        draw->AddRectFilled(p0, p1, col_min);
      }
    }

    // 小时级背景
    if (freq_hour_hi > 0.0f && limits.X.Min < freq_hour_hi) {
      float x0 = std::max(static_cast<float>(limits.X.Min), 0.001f);
      float x1 = std::min(static_cast<float>(limits.X.Max), freq_hour_hi);
      if (x0 < x1) {
        ImVec2 p0 = ImPlot::PlotToPixels(x0, y_top);
        ImVec2 p1 = ImPlot::PlotToPixels(x1, y_bot);
        draw->AddRectFilled(p0, p1, col_hour);
      }
    }

    ImPlot::PopPlotClipRect();

    // 绘制每个 asset 的 FFT (直接从动态数组读取)
    const size_t cur_gen_fft = tf.compute.generation.load();
    for (size_t i = 0; i < tf.results.size(); ++i) {
      if (!show_all && (int)i != sel)
        continue;
      const auto &r = tf.results[i];
      if (r.fft_freq.empty())
        continue;
      // 判断是否应该渲染（避免计算过程中的空白）
      if (!ShouldRenderAssetResult(r, cur_gen_fft, ui.last_rendered_generation, tf.compute.status, !r.fft_freq.empty())) {
        continue;
      }
      ImVec4 col = GetAssetColor(i, n_assets);
      ImPlot::SetNextLineStyle(col, 0.8f);
      ImPlot::PlotLine("##fft", r.fft_freq.data(), r.fft_power.data(),
                       static_cast<int>(r.fft_freq.size()));
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

  // 首次进入Tab且有有效特征选择时，自动触发计算
  static bool first_enter = true;
  if (first_enter && data.feature.selection.primary_feature_idx >= 0) {
    first_enter = false;
    service->RequestCompute();
    // 重置 autofit 跟踪，使得计算完成后会触发 autofit
    ui.last_autofit_generation = 0;
    ui.last_autofit_asset = -2;
  }

  // Autozoom 逻辑: 只在预期 assets 更新完成后触发
  // ALL mode: 当计算完成 (status == Done) 且 generation 变化时触发
  // 单 asset mode: 当选中的 asset 的 result.valid 变化时触发
  {
    TraceN("UI:AutozoomCheck");
    int cur_asset = tf.display.selected_asset;
    size_t cur_gen = tf.compute.generation.load();
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

  // 第三行: 归一化
  bool norm_changed = RenderNormConfig(tf.params);

  // 第二行: ADF/KPSS热力图
  RenderStationarityHeatmap(tf, data.asset);

  // 第三行: Asset选择 + 时间窗口滑块
  bool sel_changed = Render_AssetAndWindow(service, data);

  // 参数变化触发重计算 (autozoom 由上面的逻辑自动处理)
  if (st_changed || norm_changed) {
    ui.params_changed = true;
    service->RequestCompute();
    // 重置 autofit 跟踪，使得新计算完成后会触发 autofit
    ui.last_autofit_generation = 0;
  }
  (void)sel_changed; // asset选择变化不再直接触发autozoom

  // 更新渲染缓存: 只有当计算完成时才更新，避免计算过程中的空白
  {
    size_t cur_gen = tf.compute.generation.load();
    if (tf.compute.status == Transform::Compute::Status::Done) {
      if (cur_gen != ui.last_rendered_generation) {
        ui.last_rendered_generation = cur_gen;
      }
    }
  }

  // 获取当前 level
  int level = data.feature.selection.selected_level;

  // 特征对比图
  RenderFeaturePlots(tf, ui, ui.need_autofit, level);

  // 底部: PDF + FFT
  RenderBottomPlots(tf, ui, ui.need_autofit, level);

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
