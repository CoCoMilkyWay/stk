// Tab Feature Implementation
#include "gui/task_features/ui/TabFeature.hpp"
#include "features/FeatureCategoriesGenerated.hpp"
#include "graphic/graphic_basic.h"
#include "shared/Feature.hpp"
#include "shared/SharedData.hpp"

#include "imgui.h"
#include "latex.h"
#include "platform/imgui/graphic_imgui.h"
#include "render.h"
#include "utfcpp/utf8.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <cctype>
#include <cmath>
#include <cstring>
#include <functional>
#include <string_view>
#include <unordered_map>

namespace GUI::Features {

// ============================================================================
// LaTeX Formula Rendering Cache
// ============================================================================

static std::wstring utf8ToWide(std::string_view s) {
  auto u16 = utf8::utf8to16(s);
  return {u16.begin(), u16.end()};
}

// Cache for parsed LaTeX formulas (keyed by formula string pointer for efficiency)
static std::unordered_map<const char *, tex::TeXRender *> s_formula_cache;

static tex::TeXRender *getOrCreateFormulaRender(const char *formula) {
  auto it = s_formula_cache.find(formula);
  if (it != s_formula_cache.end()) {
    return it->second;
  }

  // Ensure LaTeX engine is initialized
  static bool s_latex_initialized = false;
  if (!s_latex_initialized) {
    tex::LaTeX::init("res");
    s_latex_initialized = true;
  }

  // Parse LaTeX formula
  std::wstring wlatex = utf8ToWide(formula);
  constexpr float kFormulaTextSize = 32.0f;
  tex::TeXRender *render = tex::LaTeX::parse(wlatex, 0, kFormulaTextSize, 5.0f, tex::green);

  s_formula_cache[formula] = render; // May be nullptr if parse failed
  return render;
}

// Render LaTeX formula at current cursor position
static void renderLatexFormula(tex::TeXRender *render) {
  assert(render);

  // Font atlas may have been invalidated by new glyphs during parse
  tex::Font_imgui::rebuildFontAtlasIfNeeded();

  ImDrawList *draw_list = ImGui::GetWindowDrawList();
  ImVec2 cursor_pos = ImGui::GetCursorScreenPos();

  tex::Graphics2D_imgui g2(draw_list);
  g2.translate(cursor_pos.x, cursor_pos.y);
  render->draw(g2, 0, 0);

  ImGui::Dummy(ImVec2((float)render->getWidth(), (float)render->getHeight()));
}

// ============================================================================
// Helper Functions
// ============================================================================

// Get background color for feature category
static ImU32 get_category_color(std::string_view cat) {
  constexpr float alpha = 0.15f; // 背景透明度
  uint32_t h = 2166136261u;
  for (char c : cat)
    h = (h ^ static_cast<unsigned char>(c)) * 16777619u;
  float r, g, b;
  ImGui::ColorConvertHSVtoRGB(static_cast<float>(h % 360u) / 360.0f, 0.45f, 0.95f, r, g, b);
  return ImGui::GetColorU32(ImVec4(r, g, b, alpha));
}

// Get current level features based on selection
static const std::vector<FeatureMetadata> &get_current_level_features(const Feature &feature) {
  return feature.metadata.features[feature.selection.selected_level];
}

// Filter features based on current filter settings
static std::vector<int> get_filtered_indices(const Feature::Selection &sel, const std::vector<FeatureMetadata> &features) {
  std::vector<int> result;
  for (int i = 0; i < (int)features.size(); ++i) {
    bool pass = true;

    // Filter by data_type
    if (!sel.filter_data_type.empty() && sel.filter_data_type.find(features[i].data_type) == sel.filter_data_type.end())
      pass = false;

    // Filter by cat_l1
    if (!sel.filter_cat_l1.empty() && sel.filter_cat_l1.find(features[i].cat_l1) == sel.filter_cat_l1.end())
      pass = false;

    // Filter by cat_l2
    if (!sel.filter_cat_l2.empty() && sel.filter_cat_l2.find(features[i].cat_l2) == sel.filter_cat_l2.end())
      pass = false;

    // Filter by norm_method
    if (!sel.filter_norm_method.empty() && sel.filter_norm_method.find(features[i].norm_method) == sel.filter_norm_method.end())
      pass = false;

    if (pass)
      result.push_back(i);
  }
  return result;
}

// ============================================================================
// Within-group ordering: 拓扑依赖序 + 贪心名字相似度聚类.
// 参考: qmt/cpp/src/feature/report.cpp (by_kind_then_topo + greedy_nearest_neighbor).
// 仅在单个 cat_l1 组内生效, 只考虑组内依赖 (跨组依赖被忽略, cat_l1 分组优先).
// ============================================================================

// 相似度设计原则: 数字只当序号不当身份 —— 去掉数字后算 token / bigram 相似度,
// 数字差异只影响自然序 (natural_compare). 这样 cost_buy_1/5/10 相似度全等
// (由自然序排 1,5,10), 而不会被 "_1"/"10" 的 bigram 巧合干扰.

// 去掉全部数字字符, 再去掉首尾 '_'.
static std::string strip_digits(const std::string &s) {
  std::string out;
  out.reserve(s.size());
  for (char c : s)
    if (!std::isdigit((unsigned char)c))
      out.push_back(c);
  std::size_t b = out.find_first_not_of('_');
  std::size_t e = out.find_last_not_of('_');
  return b == std::string::npos ? std::string() : out.substr(b, e - b + 1);
}

// 按 '_' 切分为 token 集合 (排序去重). 完整 token 匹配是聚类主信号:
// mcap / mcap_cs 共享 "mcap", cost_buy_* 共享 "cost","buy".
static std::vector<std::string> token_set(const std::string &s) {
  std::vector<std::string> out;
  std::size_t start = 0;
  while (start <= s.size()) {
    std::size_t sep = s.find('_', start);
    if (sep == std::string::npos)
      sep = s.size();
    if (sep > start)
      out.emplace_back(s.substr(start, sep - start));
    start = sep + 1;
  }
  std::sort(out.begin(), out.end());
  out.erase(std::unique(out.begin(), out.end()), out.end());
  return out;
}

// bigram 集合 (排序去重), 次信号: 部分子串重叠 (mcap↔fmcap 靠此靠近).
static std::vector<std::string> bigram_set(const std::string &s) {
  std::vector<std::string> out;
  for (std::size_t i = 0; i + 1 < s.size(); ++i)
    out.push_back(s.substr(i, 2));
  std::sort(out.begin(), out.end());
  out.erase(std::unique(out.begin(), out.end()), out.end());
  return out;
}

// Jaccard 相似度 (两侧已排序去重).
static double jaccard(const std::vector<std::string> &a, const std::vector<std::string> &b) {
  if (a.empty() || b.empty())
    return 0.0;
  std::size_t i = 0, j = 0, inter = 0;
  while (i < a.size() && j < b.size()) {
    if (a[i] < b[j])
      ++i;
    else if (a[i] > b[j])
      ++j;
    else {
      ++inter;
      ++i;
      ++j;
    }
  }
  return static_cast<double>(inter) / static_cast<double>(a.size() + b.size() - inter);
}

// 自然序比较: 字母段按字典序, 数字段按数值. "cost_buy_1" < "cost_buy_5" < "cost_buy_10".
// 用作聚类的最终兜底 (name_sim 打平时按此升序), 保证确定性 + 符合直觉.
static int natural_compare(const std::string &a, const std::string &b) {
  std::size_t i = 0, j = 0, na = a.size(), nb = b.size();
  while (i < na && j < nb) {
    bool da = std::isdigit((unsigned char)a[i]);
    bool db = std::isdigit((unsigned char)b[j]);
    if (da != db)
      return da ? -1 : 1; // 数字段 < 字母段
    if (!da) {
      while (i < na && j < nb && !std::isdigit((unsigned char)a[i]) && !std::isdigit((unsigned char)b[j])) {
        if (a[i] != b[j])
          return (unsigned char)a[i] < (unsigned char)b[j] ? -1 : 1;
        ++i;
        ++j;
      }
    } else {
      std::size_t sa = i, sb = j;
      while (i < na && std::isdigit((unsigned char)a[i]))
        ++i;
      while (j < nb && std::isdigit((unsigned char)b[j]))
        ++j;
      while (sa < i && a[sa] == '0')
        ++sa;
      while (sb < j && b[sb] == '0')
        ++sb;
      std::size_t la = i - sa, lb = j - sb;
      if (la != lb)
        return la < lb ? -1 : 1;
      int c = std::strncmp(a.c_str() + sa, b.c_str() + sb, la);
      if (c != 0)
        return c < 0 ? -1 : 1;
    }
  }
  if (i < na)
    return 1;
  if (j < nb)
    return -1;
  return 0;
}

// 拆分 deps_list[i] ("code1;code2;...") 为单个 code.
static std::vector<std::string> split_deps(const std::string &s) {
  std::vector<std::string> out;
  std::size_t start = 0;
  while (start <= s.size()) {
    std::size_t sep = s.find(';', start);
    if (sep == std::string::npos)
      sep = s.size();
    if (sep > start)
      out.emplace_back(s.substr(start, sep - start));
    start = sep + 1;
  }
  return out;
}

// 单个 cat_l1 组内排序 (四个正交组件, Python 原型在真实特征表上验证过):
//   1. 相似度: 2×token Jaccard + 1×bigram Jaccard (均去数字) + 2×依赖/出度
//      (单依赖对如 mcap→mcap_cs 强配对; 17 路节点级扇入摊薄成噪声级)
//   2. 平均链接凝聚聚类, 平局取自然序最小对 (确定性)
//   3. 聚类树线性化: 依赖定向 (被依赖侧在前), 无依赖按自然序
//   4. 粘性 Kahn 拓扑修复: 优先取上一节点的提议后继 (保簇邻接),
//      否则取提议序最靠前的就绪节点; 无就绪节点 = 依赖成环 (assert)
static std::vector<int> topo_cluster_group(const std::vector<int> &group,
                                           const std::vector<FeatureMetadata> &features,
                                           const std::vector<std::string> &deps_list) {
  const int n = (int)group.size();
  if (n <= 1)
    return group;
  const auto code_of = [&](int local) -> std::string { return features[group[local]].code; };

  // code -> 组内本地下标 (仅本组出现的 code)
  std::unordered_map<std::string, int> code_to_local;
  for (int i = 0; i < n; ++i)
    code_to_local.emplace(code_of(i), i);

  // 组内依赖邻接 (跨组依赖忽略: cat_l1 分组优先级更高)
  std::vector<std::vector<int>> deps_local(n);
  for (int i = 0; i < n; ++i) {
    int idx = group[i];
    if (idx >= (int)deps_list.size())
      continue;
    for (const auto &code : split_deps(deps_list[idx])) {
      auto it = code_to_local.find(code);
      if (it != code_to_local.end())
        deps_local[i].push_back(it->second);
    }
  }

  // ---- 1. 相似度矩阵 ----
  std::vector<std::vector<std::string>> toks(n), bigs(n);
  for (int i = 0; i < n; ++i) {
    std::string a = strip_digits(code_of(i));
    toks[i] = token_set(a);
    bigs[i] = bigram_set(a);
  }
  std::vector<double> S((std::size_t)n * n, 0.0);
  for (int i = 0; i < n; ++i)
    for (int j = i + 1; j < n; ++j) {
      double s = 2.0 * jaccard(toks[i], toks[j]) + 1.0 * jaccard(bigs[i], bigs[j]);
      S[(std::size_t)i * n + j] = S[(std::size_t)j * n + i] = s;
    }
  for (int i = 0; i < n; ++i) {
    if (deps_local[i].empty())
      continue;
    double b = 2.0 / (double)deps_local[i].size(); // 出度归一化
    for (int d : deps_local[i]) {
      S[(std::size_t)i * n + d] += b;
      S[(std::size_t)d * n + i] += b;
    }
  }

  // ---- 2. 平均链接凝聚聚类 (簇 id: 0..n-1 叶, n..2n-2 内部节点) ----
  const int total = 2 * n - 1;
  std::vector<double> sum((std::size_t)total * total, 0.0); // 簇间相似度和
  for (int i = 0; i < n; ++i)
    for (int j = 0; j < n; ++j)
      sum[(std::size_t)i * total + j] = S[(std::size_t)i * n + j];
  std::vector<int> sz(total, 0), repv(total, -1); // rep = 自然序最小成员
  std::vector<std::array<int, 2>> kids(total, {-1, -1});
  std::vector<char> active(total, 0);
  for (int i = 0; i < n; ++i) {
    sz[i] = 1;
    repv[i] = i;
    active[i] = 1;
  }
  for (int nid = n; nid < total; ++nid) {
    int ba = -1, bb = -1;
    double best_avg = -1e18;
    for (int a = 0; a < nid; ++a) {
      if (!active[a])
        continue;
      for (int b = a + 1; b < nid; ++b) {
        if (!active[b])
          continue;
        double avg = sum[(std::size_t)a * total + b] / ((double)sz[a] * sz[b]);
        bool better = avg > best_avg + 1e-12;
        if (!better && std::fabs(avg - best_avg) <= 1e-12) {
          // 平局: (min rep, max rep) 自然序最小的对胜出
          const std::string ra = code_of(repv[a]), rb = code_of(repv[b]);
          const std::string &lo1 = natural_compare(ra, rb) <= 0 ? ra : rb;
          const std::string &hi1 = natural_compare(ra, rb) <= 0 ? rb : ra;
          const std::string ca = code_of(repv[ba]), cb = code_of(repv[bb]);
          const std::string &lo2 = natural_compare(ca, cb) <= 0 ? ca : cb;
          const std::string &hi2 = natural_compare(ca, cb) <= 0 ? cb : ca;
          int c = natural_compare(lo1, lo2);
          better = c < 0 || (c == 0 && natural_compare(hi1, hi2) < 0);
        }
        if (better) {
          best_avg = avg;
          ba = a;
          bb = b;
        }
      }
    }
    assert(ba >= 0 && bb >= 0);
    kids[nid] = {ba, bb};
    sz[nid] = sz[ba] + sz[bb];
    repv[nid] = natural_compare(code_of(repv[ba]), code_of(repv[bb])) <= 0 ? repv[ba] : repv[bb];
    for (int c = 0; c < nid; ++c) {
      if (!active[c])
        continue;
      double v = sum[(std::size_t)ba * total + c] + sum[(std::size_t)bb * total + c];
      sum[(std::size_t)nid * total + c] = sum[(std::size_t)c * total + nid] = v;
    }
    active[ba] = active[bb] = 0;
    active[nid] = 1;
  }
  const int root = total - 1;

  // ---- 3. 树线性化: 每个内部节点决定左右子树先后 ----
  //   有依赖跨子树 → 被依赖侧在前; 双向 (只能靠修复) / 无依赖 → 自然序小的在前.
  std::vector<std::vector<int>> members(total);
  std::function<void(int)> collect = [&](int cid) {
    if (kids[cid][0] < 0) {
      members[cid] = {cid};
      return;
    }
    collect(kids[cid][0]);
    collect(kids[cid][1]);
    members[cid] = members[kids[cid][0]];
    members[cid].insert(members[cid].end(), members[kids[cid][1]].begin(), members[kids[cid][1]].end());
  };
  collect(root);
  std::vector<int> prop;
  prop.reserve(n);
  std::vector<char> mark(n, 0);
  std::function<void(int)> lin = [&](int cid) {
    if (kids[cid][0] < 0) {
      prop.push_back(cid);
      return;
    }
    int l = kids[cid][0], r = kids[cid][1];
    // mark: 1 = l 成员, 2 = r 成员
    for (int x : members[l])
      mark[x] = 1;
    for (int x : members[r])
      mark[x] = 2;
    bool l_first = false, r_first = false;
    for (int x : members[r])
      for (int d : deps_local[x])
        if (mark[d] == 1)
          l_first = true; // r 依赖 l → l 在前
    for (int x : members[l])
      for (int d : deps_local[x])
        if (mark[d] == 2)
          r_first = true; // l 依赖 r → r 在前
    for (int x : members[cid])
      mark[x] = 0;
    bool swap_lr;
    if (l_first != r_first)
      swap_lr = r_first;
    else
      swap_lr = natural_compare(code_of(repv[r]), code_of(repv[l])) < 0;
    if (swap_lr)
      std::swap(l, r);
    lin(l);
    lin(r);
  };
  lin(root);

  // ---- 4. 粘性 Kahn 拓扑修复 ----
  //   优先取上一发出节点的提议后继 (就绪即取, 保簇邻接); 否则取提议序最靠前的
  //   就绪节点 (被推迟的节点在依赖满足后尽早归位).
  std::vector<int> pos(n);
  for (int k = 0; k < n; ++k)
    pos[prop[k]] = k;
  std::vector<char> emitted(n, 0);
  std::vector<int> out_local;
  out_local.reserve(n);
  auto is_ready = [&](int x) {
    for (int d : deps_local[x])
      if (!emitted[d])
        return false;
    return true;
  };
  while ((int)out_local.size() < n) {
    int pick = -1;
    if (!out_local.empty()) {
      int k = pos[out_local.back()] + 1;
      if (k < n && !emitted[prop[k]] && is_ready(prop[k]))
        pick = prop[k];
    }
    if (pick < 0) {
      for (int x : prop) {
        if (!emitted[x] && is_ready(x)) {
          pick = x;
          break;
        }
      }
    }
    assert(pick >= 0 && "组内依赖成环");
    out_local.push_back(pick);
    emitted[pick] = 1;
  }

  // 本地下标 → 原始 features 下标
  std::vector<int> out;
  out.reserve(n);
  for (int local : out_local)
    out.push_back(group[local]);
  return out;
}

// ============================================================================
// UI Components
// ============================================================================

// Render multi-select dropdown for filters
template <typename EnumType, size_t N>
static void render_filter_dropdown(const char *label, bool &show_dropdown, std::set<EnumType> &selected_values, const std::array<EnumType, N> &all_values) {
  ImGui::Text("%s:", label);
  ImGui::SameLine();

  char button_label[128];
  if (selected_values.empty()) {
    snprintf(button_label, sizeof(button_label), "All###%s", label);
  } else {
    snprintf(button_label, sizeof(button_label), "%d###%s", (int)selected_values.size(), label);
  }

  if (ImGui::Button(button_label, ImVec2(80, 0))) {
    show_dropdown = !show_dropdown;
  }

  if (show_dropdown) {
    ImGui::SetNextWindowPos(ImVec2(ImGui::GetItemRectMin().x, ImGui::GetItemRectMax().y));
    ImGui::Begin(label, &show_dropdown, ImGuiWindowFlags_NoMove | ImGuiWindowFlags_AlwaysAutoResize);

    for (EnumType value : all_values) {
      auto s = to_string(value);
      char display[128];
      snprintf(display, sizeof(display), "%s (%s)", s.en, s.cn);
      bool is_selected = selected_values.find(value) != selected_values.end();
      if (ImGui::Checkbox(display, &is_selected)) {
        if (is_selected)
          selected_values.insert(value);
        else
          selected_values.erase(value);
      }
    }
    ImGui::End();
  }
}

template <size_t N>
static void render_filter_dropdown(const char *label, bool &show_dropdown, std::set<std::string_view> &selected_values, const std::array<const char *, N> &all_values) {
  ImGui::Text("%s:", label);
  ImGui::SameLine();

  char button_label[128];
  if (selected_values.empty()) {
    snprintf(button_label, sizeof(button_label), "All###%s", label);
  } else {
    snprintf(button_label, sizeof(button_label), "%d###%s", (int)selected_values.size(), label);
  }

  if (ImGui::Button(button_label, ImVec2(80, 0))) {
    show_dropdown = !show_dropdown;
  }

  if (show_dropdown) {
    ImGui::SetNextWindowPos(ImVec2(ImGui::GetItemRectMin().x, ImGui::GetItemRectMax().y));
    ImGui::Begin(label, &show_dropdown, ImGuiWindowFlags_NoMove | ImGuiWindowFlags_AlwaysAutoResize);

    for (const char *value : all_values) {
      std::string_view key(value);
      bool is_selected = selected_values.find(key) != selected_values.end();
      if (ImGui::Checkbox(value, &is_selected)) {
        if (is_selected)
          selected_values.insert(key);
        else
          selected_values.erase(key);
      }
    }
    ImGui::End();
  }
}

// ============================================================================
// Main Render Function
// ============================================================================

void RenderTabFeature(SharedData &data, FeatureUIState &ui_state) {
  Feature &feature = data.feature;
  Feature::Selection &sel = feature.selection;

  // ==========================================================================
  // Section 1: Level Selection
  // ==========================================================================
  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "1. Level:");
  ImGui::SameLine();

  bool level_changed = false;
  int prev_level = sel.selected_level;

  ImGui::RadioButton("L0", &sel.selected_level, 0);
  ImGui::SameLine();
  ImGui::RadioButton("L1", &sel.selected_level, 1);

  level_changed = (sel.selected_level != prev_level);

  // Clear selection when level changes
  if (level_changed) {
    sel.primary_feature_idx = -1;
    sel.secondary_features.clear();
  }

  ImGui::Separator();

  // ==========================================================================
  // Section 2: Filters
  // ==========================================================================
  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "2. Filters:");
  ImGui::SameLine();

  // All filters in one line
  render_filter_dropdown("DataType", ui_state.show_filter_data_type, sel.filter_data_type, FeatureDataType_ALL);
  ImGui::SameLine();
  render_filter_dropdown("Cat L1", ui_state.show_filter_cat_l1, sel.filter_cat_l1, FeatureCategoryL1_ALL);
  ImGui::SameLine();
  render_filter_dropdown("Cat L2", ui_state.show_filter_cat_l2, sel.filter_cat_l2, FeatureCategoryL2_ALL);
  ImGui::SameLine();
  render_filter_dropdown("Norm", ui_state.show_filter_norm_method, sel.filter_norm_method, NormMethod_ALL);
  ImGui::SameLine();

  // Reset filters button
  if (ImGui::Button("Reset", ImVec2(60, 0))) {
    sel.filter_data_type.clear();
    sel.filter_cat_l1.clear();
    sel.filter_cat_l2.clear();
    sel.filter_norm_method.clear();
    ui_state.sort_column = -1; // Reset table sorting
  }

  ImGui::Separator();

  // ==========================================================================
  // Section 3: Feature Table
  // ==========================================================================
  const auto &features = get_current_level_features(feature);
  const auto &deps_list = feature.metadata.deps[sel.selected_level];
  auto filtered_indices = get_filtered_indices(sel, features);

  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "3. Features:");
  ImGui::SameLine();
  ImGui::Text("Showing %d / %d", (int)filtered_indices.size(), (int)features.size());

  // Feature table - 占满剩余高度 (留一行给下方按钮)
  ImGui::PushStyleVar(ImGuiStyleVar_CellPadding, ImVec2(4.0f, 2.0f)); // Tighter padding
  const float table_height = std::max(ImGui::GetContentRegionAvail().y - ImGui::GetFrameHeightWithSpacing(), ImGui::GetFrameHeight());

  if (ImGui::BeginTable("FeatureTable", 11,
                        ImGuiTableFlags_SizingFixedFit | ImGuiTableFlags_Borders | ImGuiTableFlags_RowBg |
                            ImGuiTableFlags_ScrollY | ImGuiTableFlags_ScrollX | ImGuiTableFlags_Resizable |
                            ImGuiTableFlags_Sortable | ImGuiTableFlags_SortTristate,
                        ImVec2(0, table_height))) {

    // Table headers - fixed fit (auto shrink to content)
    ImGui::TableSetupColumn("Primary", ImGuiTableColumnFlags_WidthFixed | ImGuiTableColumnFlags_NoSort);
    ImGui::TableSetupColumn("Multi", ImGuiTableColumnFlags_WidthFixed | ImGuiTableColumnFlags_NoSort);
    ImGui::TableSetupColumn("Code", ImGuiTableColumnFlags_WidthFixed);
    ImGui::TableSetupColumn("W", ImGuiTableColumnFlags_WidthFixed);
    ImGui::TableSetupColumn("Valid", ImGuiTableColumnFlags_WidthFixed);
    ImGui::TableSetupColumn("Name CN", ImGuiTableColumnFlags_WidthFixed);
    ImGui::TableSetupColumn("DataType", ImGuiTableColumnFlags_WidthFixed);
    ImGui::TableSetupColumn("Cat L1", ImGuiTableColumnFlags_WidthFixed);
    ImGui::TableSetupColumn("Cat L2", ImGuiTableColumnFlags_WidthFixed);
    ImGui::TableSetupColumn("Norm", ImGuiTableColumnFlags_WidthFixed);
    ImGui::TableSetupColumn("Deps", ImGuiTableColumnFlags_WidthFixed);
    ImGui::TableSetupScrollFreeze(0, 1); // Freeze header row

    // Custom header row with tooltips
    ImGui::TableNextRow(ImGuiTableRowFlags_Headers);
    const char *headers[] = {"Primary", "Multi", "Code", "W", "Valid", "Name CN", "DataType", "Cat L1", "Cat L2", "Norm", "Deps"};
    const char *tooltips[] = {
        "主特征: 用于分析的主要特征",
        "多选: 选择多个特征进行对比",
        "代码: 特征的唯一标识符",
        "宽度: 特征的维度数量",
        "有效粒度: ALL=全部, DATA=数据, DEPTH=深度(仅L0)",
        "中文名称: 特征的描述性名称",
        "数据类型: TS=时序, CS=截面, LB=标签, SH=共享, META=元数据",
        "一级分类: 特征的类别 (同色同组相邻)",
        "二级分类: 特征的量纲",
        "标准化方法: 特征的归一化处理方式",
        "直接依赖: 该特征计算所依赖的其他特征 code (分号分隔)",
    };

    for (int column = 0; column < 11; column++) {
      ImGui::TableSetColumnIndex(column);
      ImGui::TableHeader(headers[column]);
      if (ImGui::IsItemHovered()) {
        ImGui::SetTooltip("%s", tooltips[column]);
      }
    }

    // Handle sorting (tristate: ascending -> descending -> none)
    ImGuiTableSortSpecs *sort_specs = ImGui::TableGetSortSpecs();
    if (sort_specs && sort_specs->SpecsDirty) {
      if (sort_specs->SpecsCount > 0) {
        const ImGuiTableColumnSortSpecs &spec = sort_specs->Specs[0];
        ui_state.sort_column = spec.ColumnIndex;
        ui_state.sort_ascending = (spec.SortDirection == ImGuiSortDirection_Ascending);
      } else {
        ui_state.sort_column = -1; // No sorting (third click)
      }
      sort_specs->SpecsDirty = false;
    }

    // 排序: 主键恒为 cat_l1 (升序, 同组相邻). 组内次序:
    //   - 用户未选列 (sort_column == -1): 依赖拓扑 + 名字聚类 (topo_cluster_group)
    //   - 用户选了列: 按该列升/降序 (保留原交互)
    {
      // 1. 按 cat_l1 升序稳定分组 (同组相邻, 组内原序暂保留)
      std::stable_sort(filtered_indices.begin(), filtered_indices.end(),
                       [&](int a, int b) {
                         return std::strcmp(features[a].cat_l1, features[b].cat_l1) < 0;
                       });

      // 聚类排序较重, 缓存: level / 过滤集不变 → 直接用上次结果
      const bool use_cluster_cache =
          ui_state.sort_column == -1 &&
          ui_state.cluster_cache_level == sel.selected_level &&
          ui_state.cluster_cache_key == filtered_indices;
      if (use_cluster_cache) {
        filtered_indices = ui_state.cluster_cache_val;
      } else {
        if (ui_state.sort_column == -1)
          ui_state.cluster_cache_key = filtered_indices; // 先存 key (下面就地重排)

        // 2. 逐 cat_l1 组应用组内排序
        auto it = filtered_indices.begin();
        while (it != filtered_indices.end()) {
          auto g_end = it;
          while (g_end != filtered_indices.end() &&
                 std::strcmp(features[*g_end].cat_l1, features[*it].cat_l1) == 0)
            ++g_end;

          if (ui_state.sort_column == -1) {
            // 默认: 依赖拓扑 + 名字聚类
            std::vector<int> group(it, g_end);
            group = topo_cluster_group(group, features, deps_list);
            std::copy(group.begin(), group.end(), it);
          } else {
            // 用户选列: 按该列排序
            std::sort(it, g_end, [&](int a, int b) {
              const FeatureMetadata &fa = features[a];
              const FeatureMetadata &fb = features[b];
              int cmp = 0;
              switch (ui_state.sort_column) {
              case 2:
                cmp = strcmp(fa.code, fb.code);
                break;
              case 3:
                cmp = fa.width - fb.width;
                break;
              case 4:
                cmp = (int)fa.valid_type - (int)fb.valid_type;
                break;
              case 5:
                cmp = strcmp(fa.name_cn, fb.name_cn);
                break;
              case 6:
                cmp = (int)fa.data_type - (int)fb.data_type;
                break;
              case 7:
                cmp = std::strcmp(fa.cat_l1, fb.cat_l1);
                break;
              case 8:
                cmp = std::strcmp(fa.cat_l2, fb.cat_l2);
                break;
              case 9:
                cmp = (int)fa.norm_method - (int)fb.norm_method;
                break;
              case 10:
                cmp = deps_list[a].compare(deps_list[b]);
                break;
              }
              return ui_state.sort_ascending ? cmp < 0 : cmp > 0;
            });
          }
          it = g_end;
        }

        if (ui_state.sort_column == -1) {
          ui_state.cluster_cache_level = sel.selected_level;
          ui_state.cluster_cache_val = filtered_indices;
        }
      } // !use_cluster_cache
    }

    // Table rows
    for (int idx : filtered_indices) {
      const FeatureMetadata &f = features[idx];

      // Check if this row is selected
      bool is_primary = (sel.primary_feature_idx == idx);
      bool is_secondary = (sel.secondary_features.find(idx) != sel.secondary_features.end());
      bool is_selected = is_primary || is_secondary;

      ImGui::TableNextRow();

      // Category background color
      ImGui::TableSetBgColor(ImGuiTableBgTarget_RowBg0, get_category_color(f.cat_l1));

      // Highlight selected rows (overlay on top of category color)
      if (is_selected) {
        ImGui::TableSetBgColor(ImGuiTableBgTarget_RowBg1, ImGui::GetColorU32(ImVec4(0.2f, 0.4f, 0.6f, 0.3f)));
      }

      // Column: Primary (RadioButton)
      ImGui::TableNextColumn();
      char radio_label[32];
      snprintf(radio_label, sizeof(radio_label), "##primary_%d", idx);
      if (ImGui::RadioButton(radio_label, sel.primary_feature_idx == idx)) {
        sel.primary_feature_idx = idx;
        // Remove from secondary if present
        sel.secondary_features.erase(idx);
      }

      // Column: Multi (Checkbox)
      ImGui::TableNextColumn();
      char check_label[32];
      snprintf(check_label, sizeof(check_label), "##multi_%d", idx);
      bool is_multi_checked = is_secondary;
      if (ImGui::Checkbox(check_label, &is_multi_checked)) {
        if (is_multi_checked) {
          // Add to secondary only if not primary
          if (sel.primary_feature_idx != idx) {
            sel.secondary_features.insert(idx);
          }
        } else {
          sel.secondary_features.erase(idx);
        }
      }

      // Column: Code
      ImGui::TableNextColumn();
      ImGui::TextUnformatted(f.code);

      // Column: Width
      ImGui::TableNextColumn();
      ImGui::Text("%d", f.width);

      // Column: ValidType
      ImGui::TableNextColumn();
      ImGui::TextUnformatted(to_string(f.valid_type).en);
      if (ImGui::IsItemHovered())
        ImGui::SetTooltip("%s", to_string(f.valid_type).cn);

      // Column: Name CN (with tooltip showing LaTeX formula)
      ImGui::TableNextColumn();
      ImGui::TextUnformatted(f.name_cn);
      if (ImGui::IsItemHovered()) {
        ImGui::BeginTooltip();
        ImGui::PushTextWrapPos(ImGui::GetFontSize() * 35.0f);
        ImGui::TextColored(ImVec4(1.0f, 1.0f, 0.0f, 1.0f), "%s", f.name_en);
        ImGui::Separator();
        ImGui::Text("Formula:");

        // Render LaTeX formula
        tex::TeXRender *render = getOrCreateFormulaRender(f.formula);
        if (render) {
          renderLatexFormula(render);
        } else {
          ImGui::TextWrapped("%s", f.formula); // Fallback to plain text
        }

        ImGui::Spacing();
        ImGui::Text("Description:");
        ImGui::TextWrapped("%s", f.description);
        ImGui::PopTextWrapPos();
        ImGui::EndTooltip();
      }

      // Column: DataType
      ImGui::TableNextColumn();
      ImGui::TextUnformatted(to_string(f.data_type).en);

      // Column: Cat L1
      ImGui::TableNextColumn();
      ImGui::TextUnformatted(f.cat_l1);

      // Column: Cat L2
      ImGui::TableNextColumn();
      ImGui::TextUnformatted(f.cat_l2);

      // Column: Norm Method
      ImGui::TableNextColumn();
      ImGui::TextUnformatted(to_string(f.norm_method).en);
      if (ImGui::IsItemHovered())
        ImGui::SetTooltip("%s", to_string(f.norm_method).cn);

      // Column: Deps (直接依赖的其他特征 code, 分号分隔)
      ImGui::TableNextColumn();
      if (idx < (int)deps_list.size() && !deps_list[idx].empty()) {
        ImGui::TextUnformatted(deps_list[idx].c_str());
        if (ImGui::IsItemHovered()) {
          ImGui::SetTooltip("%s", deps_list[idx].c_str());
        }
      } else {
        ImGui::TextDisabled("—");
      }
    }

    ImGui::EndTable();
  }

  ImGui::PopStyleVar(); // CellPadding

  // Select all filtered button
  if (ImGui::Button("Select All Multi", ImVec2(120, 0))) {
    for (int idx : filtered_indices) {
      if (sel.primary_feature_idx != idx) {
        sel.secondary_features.insert(idx);
      }
    }
  }
  ImGui::SameLine();
  if (ImGui::Button("Clear All", ImVec2(80, 0))) {
    sel.primary_feature_idx = -1;
    sel.secondary_features.clear();
  }
}

} // namespace GUI::Features
