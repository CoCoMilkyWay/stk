// Tab Feature Implementation
#include "gui/task_features/ui/TabFeature.hpp"
#include "features/FeatureCategoriesGenerated.hpp"
#include "graphic/graphic_basic.h"
#include "gui/task_features/ui/Common.hpp" // 账目着色 / 状态文本
#include "misc/format.hpp"                 // misc::fmt_width
#include "shared/Feature.hpp"
#include "shared/FeaturePreview.hpp"
#include "shared/SharedData.hpp"

#include "imgui.h"
#include "implot.h"
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

// 归一化单元: Method 名 (None → "-"), 悬停给中文
static void render_norm_cell(EnumStr method, bool none) {
  if (none) {
    ImGui::TextDisabled("-");
    return;
  }
  ImGui::TextUnformatted(method.en);
  if (ImGui::IsItemHovered())
    ImGui::SetTooltip("%s", method.cn);
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

    // Filter by TS / CS norm method (Tf 不参与过滤)
    if (!sel.filter_ts_method.empty() && sel.filter_ts_method.find(features[i].ts_method) == sel.filter_ts_method.end())
      pass = false;
    if (!sel.filter_cs_method.empty() && sel.filter_cs_method.find(features[i].cs_method) == sel.filter_cs_method.end())
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

// 单个 cat_l1 组内排序 (四个正交组件, Python 原型在真实特征表上验证过).
// 依赖只是约束 (先后满足即可), 不参与聚类 —— 聚类纯看名称 match
// (*_ttm12 互相抱团, 而不是各自贴依赖源):
//   1. 相似度: 2×token Jaccard + 1×bigram Jaccard (均去数字:
//      数字是序号不是身份, cost_buy_1/5/10 相似度全等, 由自然序排);
//      无共享 token 时 fallback 字符级前/后缀 (绑住 pb/pe/ps/pcf 缩写族)
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
  std::vector<std::string> stripped(n);
  std::vector<std::vector<std::string>> toks(n), bigs(n);
  for (int i = 0; i < n; ++i) {
    stripped[i] = strip_digits(code_of(i));
    toks[i] = token_set(stripped[i]);
    bigs[i] = bigram_set(stripped[i]);
  }
  std::vector<double> S((std::size_t)n * n, 0.0);
  for (int i = 0; i < n; ++i)
    for (int j = i + 1; j < n; ++j) {
      double tok = jaccard(toks[i], toks[j]);
      double s = 2.0 * tok + 1.0 * jaccard(bigs[i], bigs[j]);
      if (tok == 0.0) {
        // fallback: 无共享 token 的缩写名 (pb/pe/ps/pcf), 用字符级
        // 共同前/后缀近似; 有 token 信号时不加 (避免前缀把派生拉回
        // 主体, 压过 ttm/cs 等变换族的 token 抱团)
        const std::string &a = stripped[i], &b = stripped[j];
        std::size_t mn = std::min(a.size(), b.size()), mx = std::max(a.size(), b.size());
        std::size_t pre = 0, suf = 0;
        while (pre < mn && a[pre] == b[pre])
          ++pre;
        while (suf < mn && a[a.size() - 1 - suf] == b[b.size() - 1 - suf])
          ++suf;
        if (mx > 0)
          s += 0.5 * (double)(pre + suf) / (double)mx;
      }
      S[(std::size_t)i * n + j] = S[(std::size_t)j * n + i] = s;
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
// Preview 迷你图 (Dist / PSD 两列): cell 内 polyline, hover 弹 ImPlot 放大图
// ============================================================================

static constexpr float kSparkWidth = 96.0f; // 迷你图列宽 (px)
static const ImVec4 kSparkDistColor{0.4f, 0.8f, 1.0f, 1.0f};
static const ImVec4 kSparkPsdColor{1.0f, 0.8f, 0.2f, 1.0f};

// PSD 迷你图 x 轴: log10 周期升序 (k 降序), 与 TabTransform 的 PSD 周期轴同向
struct PsdSparkAxis {
  std::array<float, kPvPsdPts> log_period{};
  PsdSparkAxis() {
    for (size_t j = 0; j < kPvPsdPts; ++j)
      log_period[j] = std::log10(analysis::DayPSD::period_of(kPvPsdPts - j));
  }
};
static const PsdSparkAxis s_psd_axis;

// 迷你折线: 当前光标处画 size 大小的 polyline (x/y 各自归一化), 返回是否 hover
static bool sparkline(const char *id, const float *xs, const float *ys, int n, ImVec2 size, const ImVec4 &color) {
  assert(n >= 2);
  const ImVec2 p0 = ImGui::GetCursorScreenPos();
  ImGui::InvisibleButton(id, size);
  const bool hovered = ImGui::IsItemHovered();

  float ymin = ys[0], ymax = ys[0];
  for (int i = 1; i < n; ++i) {
    ymin = std::min(ymin, ys[i]);
    ymax = std::max(ymax, ys[i]);
  }
  const float xr = xs[n - 1] - xs[0];
  const float inv_x = xr > 0.0f ? 1.0f / xr : 0.0f;
  const float yr = ymax - ymin;
  const float inv_y = yr > 0.0f ? 1.0f / yr : 0.0f;

  static std::vector<ImVec2> pts; // GUI 单线程, 帧内复用
  pts.resize(n);
  for (int i = 0; i < n; ++i) {
    const float u = (xs[i] - xs[0]) * inv_x;
    const float v = yr > 0.0f ? (ys[i] - ymin) * inv_y : 0.5f;
    pts[i] = ImVec2(p0.x + u * size.x, p0.y + (1.0f - v) * (size.y - 2.0f) + 1.0f);
  }
  ImGui::GetWindowDrawList()->AddPolyline(pts.data(), n, ImGui::GetColorU32(color), 0, 1.0f);
  return hovered;
}

// Dist 列 cell: PDF 迷你图 + hover 放大
static void render_preview_dist(const FeaturePreview::Cell &cell, const char *code) {
  if (cell.n_pts < 2) {
    ImGui::TextDisabled("—");
    return;
  }
  const ImVec2 size(kSparkWidth, ImGui::GetTextLineHeight());
  if (!sparkline("##spark_dist", cell.x.data(), cell.y.data(), (int)cell.n_pts, size, kSparkDistColor))
    return;
  ImGui::BeginTooltip();
  ImGui::Text("%s  平均分布 (n = %llu)", code, (unsigned long long)cell.n);
  if (ImPlot::BeginPlot("##pv_pdf", ImVec2(360, 200), ImPlotFlags_NoLegend)) {
    ImPlot::SetupAxes(nullptr, "pdf", ImPlotAxisFlags_AutoFit, ImPlotAxisFlags_AutoFit);
    ImPlot::SetNextLineStyle(kSparkDistColor, 2.0f);
    ImPlot::PlotLine("##pdf", cell.x.data(), cell.y.data(), (int)cell.n_pts);
    ImPlot::EndPlot();
  }
  ImGui::EndTooltip();
}

// PSD 列 cell: log10 谱迷你图 (周期升序) + hover 放大 (log 周期轴)
static void render_preview_psd(const FeaturePreview::Cell &cell, const char *code) {
  if (cell.psd_n == 0) {
    ImGui::TextDisabled("—");
    return;
  }
  static std::vector<float> py; // 重排到周期升序 (k 降序)
  py.resize(kPvPsdPts);
  for (size_t j = 0; j < kPvPsdPts; ++j)
    py[j] = cell.psd[kPvPsdPts - j - 1];

  const ImVec2 size(kSparkWidth, ImGui::GetTextLineHeight());
  if (!sparkline("##spark_psd", s_psd_axis.log_period.data(), py.data(), (int)kPvPsdPts, size, kSparkPsdColor))
    return;
  ImGui::BeginTooltip();
  ImGui::Text("%s  单日 PSD 均值 (%u 资产·天)", code, cell.psd_n);
  if (ImPlot::BeginPlot("##pv_psd", ImVec2(360, 200), ImPlotFlags_NoLegend)) {
    ImPlot::SetupAxes("周期 (min)", "log10 P", ImPlotAxisFlags_AutoFit, ImPlotAxisFlags_AutoFit);
    ImPlot::SetupAxisScale(ImAxis_X1, ImPlotScale_Log10);
    static std::vector<float> px;
    px.resize(kPvPsdPts);
    for (size_t j = 0; j < kPvPsdPts; ++j)
      px[j] = analysis::DayPSD::period_of(kPvPsdPts - j);
    ImPlot::SetNextLineStyle(kSparkPsdColor, 2.0f);
    ImPlot::PlotLine("##psd", px.data(), py.data(), (int)kPvPsdPts);
    ImPlot::EndPlot();
  }
  ImGui::EndTooltip();
}

// ============================================================================
// 账目两列 (与 Distribution 完整性条同口径同着色, 抽样格子逐轮累积):
//   Stat:  "nan,zero,-inf,+inf%"  四个占比, 每个恰 3 字符 (nan/inf 相对全部格子, zero 相对有效值)
//   Range: "min -1sd +1sd max"     四个数, 每个恰 4 字符 (sd 来自 sketch 矩)
// ============================================================================

static void render_stat_cell(const analysis::Integrity &it) {
  if (it.n_total == 0) {
    ImGui::TextDisabled("—");
    return;
  }
  const float n_total = static_cast<float>(it.n_total);
  const float pct[4] = {it.nan_pct(), it.zero_pct(),
                        100.0f * static_cast<float>(it.n_neg_inf) / n_total,
                        100.0f * static_cast<float>(it.n_pos_inf) / n_total};
  char s[8];
  for (int k = 0; k < 4; ++k) {
    if (k > 0) {
      ImGui::SameLine(0, 0);
      ImGui::TextUnformatted(",");
      ImGui::SameLine(0, 0);
    }
    misc::fmt_width(s, sizeof(s), pct[k], 3, /*fixed_zero_ok=*/true); // 百分比有界 [0,100]: 小量显示 "0.0", 不走 SI
    ImGui::TextColored(k == 1 ? GetZeroPctColor(pct[k]) : GetNanInfPctColor(pct[k]), "%s", s);
  }
  ImGui::SameLine(0, 0);
  ImGui::TextUnformatted("%");
  if (ImGui::IsItemHovered())
    ImGui::SetTooltip("nan %zu, zero %zu, -inf %zu, +inf %zu  /  格子 %zu, 有效 %zu",
                      it.n_nan, it.n_zero, it.n_neg_inf, it.n_pos_inf, it.n_total, it.n_valid);
}

static void render_range_cell(const FeaturePreview::Cell &cell) {
  const analysis::Integrity &it = cell.integrity;
  if (it.n_valid == 0) {
    ImGui::TextDisabled("—");
    return;
  }
  const float sd = cell.sd();
  char s[4][8];
  misc::fmt_width(s[0], sizeof(s[0]), it.val_min, 4);
  misc::fmt_width(s[1], sizeof(s[1]), cell.mean - sd, 4);
  misc::fmt_width(s[2], sizeof(s[2]), cell.mean + sd, 4);
  misc::fmt_width(s[3], sizeof(s[3]), it.val_max, 4);
  ImGui::TextColored(GetMinMaxColor(std::max(std::fabs(it.val_min), std::fabs(it.val_max))),
                     "%s %s %s %s", s[0], s[1], s[2], s[3]);
  if (ImGui::IsItemHovered())
    ImGui::SetTooltip("min %.4g  -1sd %.4g  +1sd %.4g  max %.4g\nmean %.4g  sd %.4g  n %llu",
                      it.val_min, cell.mean - sd, cell.mean + sd, it.val_max,
                      cell.mean, sd, static_cast<unsigned long long>(cell.n));
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
    sel.selected_features.clear();
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
  render_filter_dropdown("TS Norm", ui_state.show_filter_ts_method, sel.filter_ts_method, ts_MethodId_ALL);
  ImGui::SameLine();
  render_filter_dropdown("CS Norm", ui_state.show_filter_cs_method, sel.filter_cs_method, cs_MethodId_ALL);
  ImGui::SameLine();

  // Reset filters button
  if (ImGui::Button("Reset", ImVec2(60, 0))) {
    sel.filter_data_type.clear();
    sel.filter_cat_l1.clear();
    sel.filter_cat_l2.clear();
    sel.filter_ts_method.clear();
    sel.filter_cs_method.clear();
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

  // Preview 轮训进度 (Dist/PSD 两列逐轮收敛; 免锁读原子)
  {
    const auto pv_status = data.preview.status.load(std::memory_order_relaxed);
    if (pv_status == analysis::Status::Building) {
      const int done = (int)data.preview.done.load(std::memory_order_relaxed);
      const int total = (int)data.preview.total.load(std::memory_order_relaxed);
      ImGui::SameLine();
      ImGui::TextDisabled("(preview %d/%d)", done, total);
    }
  }

  // Feature table - 占满剩余高度 (留一行给下方按钮)
  ImGui::PushStyleVar(ImGuiStyleVar_CellPadding, ImVec2(4.0f, 2.0f)); // Tighter padding
  const float table_height = std::max(ImGui::GetContentRegionAvail().y - ImGui::GetFrameHeightWithSpacing(), ImGui::GetFrameHeight());

  // 表列序 (sort_column / 排序 switch 皆按此下标)
  constexpr int kNumCols = 15;
  if (ImGui::BeginTable("FeatureTable", kNumCols,
                        ImGuiTableFlags_SizingFixedFit | ImGuiTableFlags_Borders | ImGuiTableFlags_RowBg |
                            ImGuiTableFlags_ScrollY | ImGuiTableFlags_ScrollX | ImGuiTableFlags_Resizable |
                            ImGuiTableFlags_Sortable | ImGuiTableFlags_SortTristate |
                            ImGuiTableFlags_NoSavedSettings,
                        ImVec2(0, table_height))) {

    // Table headers - fixed fit (auto shrink to content)
    ImGui::TableSetupColumn("Multi", ImGuiTableColumnFlags_WidthFixed | ImGuiTableColumnFlags_NoSort); // 0
    ImGui::TableSetupColumn("Code", ImGuiTableColumnFlags_WidthFixed);                                 // 1
    ImGui::TableSetupColumn("W", ImGuiTableColumnFlags_WidthFixed);                                    // 2
    ImGui::TableSetupColumn("Valid", ImGuiTableColumnFlags_WidthFixed);                                // 3
    ImGui::TableSetupColumn("Name CN", ImGuiTableColumnFlags_WidthFixed);                              // 4
    ImGui::TableSetupColumn("DataType", ImGuiTableColumnFlags_WidthFixed);                             // 5
    ImGui::TableSetupColumn("Cat L1", ImGuiTableColumnFlags_WidthFixed);                               // 6
    ImGui::TableSetupColumn("Cat L2", ImGuiTableColumnFlags_WidthFixed);                               // 7
    ImGui::TableSetupColumn("TS Norm", ImGuiTableColumnFlags_WidthFixed);                              // 8
    ImGui::TableSetupColumn("CS Norm", ImGuiTableColumnFlags_WidthFixed);                              // 9
    ImGui::TableSetupColumn("Stat", ImGuiTableColumnFlags_WidthFixed);                                 // 10
    ImGui::TableSetupColumn("Range", ImGuiTableColumnFlags_WidthFixed);                                // 11
    ImGui::TableSetupColumn("Dist", ImGuiTableColumnFlags_WidthFixed | ImGuiTableColumnFlags_NoSort);  // 12
    ImGui::TableSetupColumn("PSD", ImGuiTableColumnFlags_WidthFixed | ImGuiTableColumnFlags_NoSort);   // 13
    ImGui::TableSetupColumn("Deps", ImGuiTableColumnFlags_WidthFixed);                                 // 14
    ImGui::TableSetupScrollFreeze(0, 1);                                                               // Freeze header row

    // Custom header row with tooltips
    ImGui::TableNextRow(ImGuiTableRowFlags_Headers);
    const char *headers[kNumCols] = {"Multi", "Code", "W", "Valid", "Name CN", "DataType", "Cat L1", "Cat L2", "TS Norm", "CS Norm",
                                     "Stat", "Range", "Dist", "PSD", "Deps"};
    const char *tooltips[kNumCols] = {
        "多选: 选择多个特征进行对比 (首个作为主特征)",
        "代码: 特征的唯一标识符",
        "宽度: 特征的维度数量",
        "有效粒度: ALL=全部, DATA=数据, DEPTH=深度(仅L0)",
        "中文名称: 特征的描述性名称",
        "数据类型: TS=时序, CS=截面, LB=标签, SH=共享, META=元数据",
        "一级分类: 特征的类别 (同色同组相邻)",
        "二级分类: 特征的量纲",
        "时序归一化: SRC 列 OP(..., Tf, Method) 推出",
        "截面归一化: SRC 列 CS(..., Tf, Method) 推出",
        "账目: nan,zero,-inf,+inf 占比%",
        "值域: min -1sd +1sd max",
        "平均分布: 抽样 (日 × 资产) 的 PDF",
        "平均频谱: 单日 PSD 的算术平均 (log10 功率, x = 周期)",
        "直接依赖: 该特征计算所依赖的其他特征 code",
    };

    for (int column = 0; column < kNumCols; column++) {
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

    // 持 preview 锁覆盖 排序 + 行循环 (账目列可排序, 比较器要读 cells; worker 只在块末短锁发布, 不会长等)
    std::lock_guard<std::mutex> preview_lock(data.preview.mutex);
    const bool preview_level = (sel.selected_level == (int)analysis::kLevel);
    static const FeaturePreview::Cell s_empty_cell{};
    // 槽位 = metadata 下标; 非预览层 / 未就绪 → 空 cell
    auto cell_of = [&](int i) -> const FeaturePreview::Cell & {
      return (preview_level && i < (int)data.preview.cells.size()) ? data.preview.cells[i] : s_empty_cell;
    };

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
            auto cmp3 = [](auto x, auto y) { return x < y ? -1 : (x > y ? 1 : 0); };
            std::sort(it, g_end, [&](int a, int b) {
              const FeatureMetadata &fa = features[a];
              const FeatureMetadata &fb = features[b];
              const FeaturePreview::Cell &ca = cell_of(a);
              const FeaturePreview::Cell &cb = cell_of(b);
              int cmp = 0;
              switch (ui_state.sort_column) {
              case 1:
                cmp = strcmp(fa.code, fb.code);
                break;
              case 2:
                cmp = fa.width - fb.width;
                break;
              case 3:
                cmp = (int)fa.valid_type - (int)fb.valid_type;
                break;
              case 4:
                cmp = strcmp(fa.name_cn, fb.name_cn);
                break;
              case 5:
                cmp = (int)fa.data_type - (int)fb.data_type;
                break;
              case 6:
                cmp = std::strcmp(fa.cat_l1, fb.cat_l1);
                break;
              case 7:
                cmp = std::strcmp(fa.cat_l2, fb.cat_l2);
                break;
              case 8: // TS Norm
                cmp = (int)fa.ts_method - (int)fb.ts_method;
                break;
              case 9: // CS Norm
                cmp = (int)fa.cs_method - (int)fb.cs_method;
                break;
              case 10: // Stat: nan%
                cmp = cmp3(ca.integrity.nan_pct(), cb.integrity.nan_pct());
                break;
              case 11: // Range: sd (var 单调等价)
                cmp = cmp3(ca.var, cb.var);
                break;
              case 14:
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

    // Table rows (preview 锁已在上方持有)
    for (int idx : filtered_indices) {
      const FeatureMetadata &f = features[idx];

      // Check if this row is selected
      bool is_selected = (sel.selected_features.find(idx) != sel.selected_features.end());

      ImGui::TableNextRow();

      // Category background color
      ImGui::TableSetBgColor(ImGuiTableBgTarget_RowBg0, get_category_color(f.cat_l1));

      // Highlight selected rows (overlay on top of category color)
      if (is_selected) {
        ImGui::TableSetBgColor(ImGuiTableBgTarget_RowBg1, ImGui::GetColorU32(ImVec4(0.2f, 0.4f, 0.6f, 0.3f)));
      }

      // Column: Multi (Checkbox)
      ImGui::TableNextColumn();
      char check_label[32];
      snprintf(check_label, sizeof(check_label), "##multi_%d", idx);
      bool is_multi_checked = is_selected;
      if (ImGui::Checkbox(check_label, &is_multi_checked)) {
        if (is_multi_checked)
          sel.selected_features.insert(idx);
        else
          sel.selected_features.erase(idx);
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

      // Columns: TS Norm / CS Norm (SRC 推出的 Method; Tf 不显示; None 显 "-")
      ImGui::TableNextColumn();
      render_norm_cell(to_string(f.ts_method), f.ts_method == ts::MethodId::None);
      ImGui::TableNextColumn();
      render_norm_cell(to_string(f.cs_method), f.cs_method == cs::MethodId::None);

      // Columns: Stat / Range 账目 + Dist / PSD 迷你图 (预览, 仅 L1; 槽位 = metadata 下标)
      const FeaturePreview::Cell &cell = cell_of(idx);
      ImGui::PushID(idx);
      ImGui::TableNextColumn();
      render_stat_cell(cell.integrity);
      ImGui::TableNextColumn();
      render_range_cell(cell);
      ImGui::TableNextColumn();
      render_preview_dist(cell, f.code);
      ImGui::TableNextColumn();
      render_preview_psd(cell, f.code);
      ImGui::PopID();

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

  if (ImGui::Button("Clear All", ImVec2(80, 0))) {
    sel.selected_features.clear();
  }
}

} // namespace GUI::Features
