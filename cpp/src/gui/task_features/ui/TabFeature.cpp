// Tab Feature Implementation
#include "gui/task_features/ui/TabFeature.hpp"
#include "gui/task_features/ui/Common.hpp" // 账目着色 / 状态文本
#include "gui/util/Latex.hpp"              // 公式渲染 (Name CN 悬停)
#include "gui/util/Sparkline.hpp"          // 迷你图 (Dist / PSD 列)
#include "misc/format.hpp"                 // misc::fmt_width
#include "shared/Feature.hpp"
#include "shared/FeatureLib.hpp"
#include "shared/Preview.hpp"
#include "shared/SharedData.hpp"

#include "imgui.h"
#include "imgui_internal.h" // TableSetColumnWidthAutoAll (强制列宽贴合)
#include "implot.h"
#include "nlohmann/json.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <cctype>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <limits>
#include <string_view>
#include <unordered_map>
#include <utility>

namespace GUI::Features {

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
  return feature.meta().features[feature.selection.selected_level];
}

// ============================================================================
// Cat2 自动探测 (值域启发, 与 Range 列同口径: 预览抽样 min/max):
//   字段表 cat_l2 = AUTO (dummy) 的行按值域归类; 写了别的 token = 人类强行赋值,
//   覆盖探测并绿字显示. 无预览 (非预览档 / 未就绪 / 无有效样本) → "?".
//   price:     逐日笼内 (preview 主扫描随批判定, 见 Preview::Cell 笼账目):
//              每个抽样日每资产的有效值都落在该资产当日 [lim_dn, lim_up] 内
//   flag:      有效值全整数 (±0, 1, 2, …; 主扫描随格判, Cell::int_only) 且 |min|,|max| < 1024
//              (f16 格距 < 1 才分得出小数, 再往上什么都是整数, 判定失效); 先于 rank (0/1 的 CS 列不是 rank)
//   rank:      CS 且值域 ⊆ [0, 1]
//   ratio_*:   全绿 (|min|,|max| ≤ 100, 同 GetMinMaxColor): ≥0 → ratio_pos, ≤0 → ratio_neg, 跨零 → ratio
//   raw:       其余
// ============================================================================

static constexpr const char *kCat2Dummy = "AUTO";
static constexpr const char *kCat2Unknown = "?";
static constexpr const char *kCat2Classes[7] = {"price", "flag", "rank", "ratio_pos", "ratio_neg", "ratio", "raw"};

static bool cat2_overridden(const FeatureMetadata &f) {
  return std::strcmp(f.cat_l2, kCat2Dummy) != 0;
}

static const char *detect_cat2(const FeatureMetadata &f, const Preview::Cell &cell) {
  const analysis::Integrity &it = cell.integrity;
  if (it.n_valid == 0)
    return kCat2Unknown;
  if (cell.cage_n > 0 && cell.cage_miss == 0)
    return kCat2Classes[0]; // price (逐日笼内, 无一笼外)
  const float abs_max = std::max(std::fabs(it.val_min), std::fabs(it.val_max));
  if (cell.int_only && abs_max < 1024.0f)
    return kCat2Classes[1]; // flag (全整数, 且量级在 f16 能分出小数的范围内)
  if (f.data_type == FeatureDataType::CS && it.val_min >= 0.0f && it.val_max <= 1.0f)
    return kCat2Classes[2]; // rank
  if (abs_max <= 100.0f) {  // 全绿
    if (it.val_min >= 0.0f)
      return kCat2Classes[3]; // ratio_pos
    if (it.val_max <= 0.0f)
      return kCat2Classes[4]; // ratio_neg
    return kCat2Classes[5];   // ratio
  }
  return kCat2Classes[6]; // raw
}

// (需持 preview 锁) 该 (档, 层) 每行有效 Cat2 快照; out[i] 指向静态串或字段表串
static void effective_cat2_snapshot(const Feature &feature, const Preview &pv, TierId tier,
                                    size_t level, std::vector<const char *> &out) {
  const auto &features = feature.meta(tier).features[level];
  out.resize(features.size());

  static const Preview::Cell s_empty{};
  for (size_t i = 0; i < features.size(); ++i) {
    const FeatureMetadata &f = features[i];
    const Preview::Cell *cell = pv.cell(tier, level, i);
    out[i] = cat2_overridden(f) ? f.cat_l2 : detect_cat2(f, cell ? *cell : s_empty);
  }
}

// 公开口 (见 TabFeature.hpp): 自加短锁, OrderFlow 两图 legend 标注 / flag 轴缩放用
void EffectiveCat2Snapshot(SharedData &data, TierId tier, size_t level, Cat2Snapshot &out) {
  std::lock_guard<std::mutex> preview_lock(data.preview.mutex);
  const Preview &pv = data.preview;
  effective_cat2_snapshot(data.feature, pv, tier, level, out.cat2);
  const size_t n = out.cat2.size();
  out.val_min.resize(n);
  out.val_max.resize(n);
  static const Preview::Cell s_empty{};
  for (size_t i = 0; i < n; ++i) {
    const Preview::Cell *cell = pv.cell(tier, level, i);
    const analysis::Integrity &it = (cell ? *cell : s_empty).integrity;
    out.val_min[i] = it.val_min;
    out.val_max[i] = it.val_max;
  }
}

// Filter features based on current filter settings (eff_cat2 = 探测/覆盖后的有效 Cat2)
static std::vector<int> get_filtered_indices(const Feature::Selection &sel, const std::vector<FeatureMetadata> &features,
                                             const std::vector<const char *> &eff_cat2) {
  std::vector<int> result;
  for (int i = 0; i < (int)features.size(); ++i) {
    bool pass = true;

    // Filter by data_type
    if (!sel.filter_data_type.empty() && sel.filter_data_type.find(features[i].data_type) == sel.filter_data_type.end())
      pass = false;

    // Filter by cat_l1
    if (!sel.filter_cat_l1.empty() && sel.filter_cat_l1.find(features[i].cat_l1) == sel.filter_cat_l1.end())
      pass = false;

    // Filter by cat_l2 (有效值)
    if (!sel.filter_cat_l2.empty() && sel.filter_cat_l2.find(eff_cat2[i]) == sel.filter_cat_l2.end())
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

// 公开口 (见 TabFeature.hpp): 自加短锁, Corr 热图取与表格同一行集
void FilteredFeatureIndices(SharedData &data, std::vector<int> &out) {
  const Feature &feature = data.feature;
  const size_t level = static_cast<size_t>(feature.selection.selected_level);
  static std::vector<const char *> s_cat2; // GUI 单线程, 帧内复用
  {
    std::lock_guard<std::mutex> preview_lock(data.preview.mutex);
    effective_cat2_snapshot(feature, data.preview, feature.selection.tier, level, s_cat2);
  }
  out = get_filtered_indices(feature.selection, feature.meta().features[level], s_cat2);
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

static const ImVec4 kSparkPsdColor{1.0f, 0.8f, 0.2f, 1.0f};

// PSD 迷你图 x 轴 (每层一份, 与 Preview 同一 psd_axis): log10 桶中心周期升序 (k 降序), 与 TabTransform 的 PSD 周期轴同向;
// 点 = 桶 (Cell::psd[j]). 域 = 首末桶中心 (Nyquist 侧 ~ 整段一个周期), 同层全表同轴绝对可比; 周期单位 = 网格行 (level_unit)
struct PsdSparkAxis {
  uint32_t n = 0;
  std::array<float, kPvPsdBins> log_period{}, period{};
  std::array<double, kPvPsdBins> lnw{}; // 桶的 log 频率宽度 Σ_{k∈桶} 1/k (面积权重)
  float dom[2] = {};
  explicit PsdSparkAxis(size_t level) {
    const PsdAxis ax = psd_axis(math::spectral::next_pow2(seg_rows(level)));
    n = ax.n;
    for (size_t j = 0; j < n; ++j) {
      period[j] = ax.period[j];
      log_period[j] = std::log10(ax.period[j]);
      for (size_t k = ax.k0[j]; k <= ax.k1[j]; ++k)
        lnw[j] += 1.0 / static_cast<double>(k);
    }
    dom[0] = log_period[0];
    dom[1] = log_period[n - 1];
  }
};
static const PsdSparkAxis &psd_spark_axis(size_t level) {
  static const auto s = []<size_t... L>(std::index_sequence<L...>) {
    return std::array<PsdSparkAxis, LEVEL_COUNT>{PsdSparkAxis(L)...};
  }(std::make_index_sequence<LEVEL_COUNT>{});
  return s[level];
}
// 白噪声参照: 桶内每 bin 占比 × N/2 = 1 (Parseval 归一后全层恒定), 高于线 = 超白噪声份额
static constexpr float kPsdWhiteRef = 0.0f;
// PSD y 固定域 (log10 相对白噪声密度): 下界 = 白噪声之下半个数量级, 上界 = 白噪声之上 1.5 个数量级
static constexpr float kPsdYLo = kPsdWhiteRef - 0.5f, kPsdYHi = kPsdWhiteRef + 1.5f;
static constexpr ImU32 kPsdCutColor = IM_COL32(80, 200, 255, 255); // 谱重心竖刀 (亮青, 与黄谱线 / 灰白噪声参照区分)

// Dist 列 cell: PDF 迷你图 + hover 放大.
// x 域 = 分位截尾 [P0.5, P99.5] (winsorize, 分位天然在数据范围内), 再强行含住 0 →
// 全表同口径可比; 以 0 为界 x > 0 红 / x < 0 绿
static void render_preview_dist(const Preview::Cell &cell, const char *code) {
  if (cell.n_pts < 2) {
    ImGui::TextDisabled("—");
    return;
  }
  const ImVec2 size(kSparkWidth, ImGui::GetTextLineHeight());
  if (!ImGui::IsRectVisible(size)) { // 视野外行 (列宽贴合帧全行提交): 占位即可, 不算点列
    ImGui::Dummy(size);
    return;
  }
  const float *cx = cell.x.data(), *cy = cell.y.data();
  const int m = (int)cell.n_pts;

  // 分位截尾 [P0.5, P99.5]: PDF 是等距网格密度, 就地积分 CDF 找分位下标 (dx 约掉).
  // 分位点即网格点 → 裁剪就是下标截取, 无需插值; total = 0 (退化 PDF) 自然不裁
  constexpr float kTailQ = 0.005f;
  double total = 0.0;
  for (int i = 0; i < m; ++i)
    total += cy[i];
  int i_lo = 0, i_hi = m - 1;
  double cum = 0.0;
  for (; i_lo < m - 1; ++i_lo) {
    cum += cy[i_lo];
    if (cum >= kTailQ * total)
      break;
  }
  cum = 0.0;
  for (; i_hi > i_lo; --i_hi) {
    cum += cy[i_hi];
    if (cum >= kTailQ * total)
      break;
  }

  // 点列 = 截取段, 两端强行含住 0 (全表同口径可比); sparkline 按数据范围归一化 → 整格铺满.
  // 补点高度: 0 在数据范围外 → 真空区, 贴底; 0 在范围内 (PDF x 是格中点, min == 0 时首点
  // 已在 0 右侧半格) → 端点质量的峰就压在 0 上, 平伸到 0 保持峰高, 否则 0 → 峰一根竖直跳变
  static std::vector<float> xs, ys; // GUI 单线程, 帧内复用
  xs.clear();
  ys.clear();
  if (cx[i_lo] > 0.0f) {
    xs.push_back(0.0f);
    ys.push_back(cell.integrity.val_min <= 0.0f ? cy[i_lo] : 0.0f);
  }
  xs.insert(xs.end(), cx + i_lo, cx + i_hi + 1);
  ys.insert(ys.end(), cy + i_lo, cy + i_hi + 1);
  if (cx[i_hi] < 0.0f) {
    xs.push_back(0.0f);
    ys.push_back(cell.integrity.val_max >= 0.0f ? cy[i_hi] : 0.0f);
  }
  if (xs.front() == xs.back()) { // 恒 0 特征: 域退化成单点, 铺条对称底线
    xs.assign({-1.0f, 1.0f});
    ys.assign({0.0f, 0.0f});
  }
  const int n = (int)xs.size();
  assert(n >= 2);

  if (!sparkline("##spark_dist", xs.data(), ys.data(), n, size, kSparkPosColor, &kSparkNegColor))
    return;
  ImGui::BeginTooltip();
  ImGui::Text("%s  平均分布 (n = %llu)", code, (unsigned long long)cell.n);
  if (ImPlot::BeginPlot("##pv_pdf", ImVec2(360, 200), ImPlotFlags_NoLegend)) {
    // 点列已裁到域内, y 直接 AutoFit 即可 (无域外长尾干扰)
    ImPlot::SetupAxes(nullptr, "pdf", 0, ImPlotAxisFlags_AutoFit);
    ImPlot::SetupAxisLimits(ImAxis_X1, xs.front(), xs.back(), ImPlotCond_Always); // 与迷你图同域
    // 跨 0 的那段两色各画一次 (重叠一段, 视觉无碍), 与迷你图着色一致
    int k = 0;
    while (k < n && xs[k] < 0.0f)
      ++k;
    if (k > 0) {
      ImPlot::SetNextLineStyle(kSparkNegColor, 2.0f);
      ImPlot::PlotLine("##pdf_neg", xs.data(), ys.data(), std::min(k + 1, n));
    }
    if (k < n) {
      ImPlot::SetNextLineStyle(kSparkPosColor, 2.0f);
      ImPlot::PlotLine("##pdf_pos", xs.data() + k, ys.data() + k, n - k);
    }
    ImPlot::EndPlot();
  }
  ImGui::EndTooltip();
}

// PSD 能量密度染色: 色域锚在白噪声参照 kPsdWhiteRef:
// 低于白噪声 1 个数量级 → 纯绿, 高于 2 个数量级 → 纯红, 中间渐进 (黄 = kSparkPsdColor)
static ImU32 psd_share_color(float v) {
  const float lo = kPsdWhiteRef - 1.0f, hi = kPsdWhiteRef + 2.0f;
  const float t = std::clamp((v - lo) / (hi - lo), 0.0f, 1.0f);
  ImVec4 c;
  if (t < 0.5f) { // 绿 (0.2, 0.8, 0.2) → 黄 (1, 0.8, 0.2)
    c = ImVec4(0.2f + 1.6f * t, 0.8f, 0.2f, 1.0f);
  } else { // 黄 → 红 (1, 0.2, 0.2)
    c = ImVec4(1.0f, 0.8f - 1.2f * (t - 0.5f), 0.2f, 1.0f);
  }
  return ImGui::GetColorU32(c);
}

// PSD 迷你图逐行缓存: 派生量 (谱重心 + 逐 bin 染色) 只在 preview 发布代
// 变化时重算 (epoch 跨构建单调不归零, 见 shared/Progress.hpp), 帧间零计算直接画.
struct PsdSparkCache {
  uint64_t epoch = UINT64_MAX;        // 计算时的 preview 发布代 (UINT64_MAX = 未算)
  size_t level = SIZE_MAX;            // 计算时的层 (同下标跨层复用缓存槽)
  float xm = 0.0f;                    // 谱重心 log10 周期 (log 轴下曲线面积中位, 见 rebuild_psd_spark)
  float med_period = 0.0f;            // 谱重心周期 (网格行)
  float y_dom[2] = {};                // 迷你图 y 固定域 (全表同尺, 见 kPsdYLo; hover 放大图 autofit)
  std::array<ImU32, kPvPsdBins> cs{}; // 逐段渐变色 (桶密度染色)
};
static std::vector<PsdSparkCache> s_psd_cache; // [metadata 下标] (非预览行早退不建)

static void rebuild_psd_spark(PsdSparkCache &pc, const Preview::Cell &cell, uint64_t epoch, size_t level) {
  pc.epoch = epoch;
  pc.level = level;
  const PsdSparkAxis &ax = psd_spark_axis(level);
  const size_t n = ax.n;
  // 谱重心 = 曲线在 log 周期轴下的面积中位: 权重 = 桶密度 × 桶的 log 频率宽度 Σ1/k (∫ P d ln f).
  // 不是能量中位 (∫ P df): 能量按频率均匀分 bin, 白噪声一半 bin 落在 Nyquist 一个倍频程内,
  // 在 log 轴上没有可读性. 面积中位: 白噪声 → 轴中央 (L_1min 精确期望 32 min: Σ1/k 从 k=128 累到一半在 k=8,
  // 抽样残差 ±几 min), 谱越红越右, 越蓝越左
  std::array<double, kPvPsdBins> pw{}; // 桶面积
  double total = 0.0;
  bool flat = true; // 全地板 = 抽样段全部恒值
  for (size_t j = 0; j < n; ++j) {
    pw[j] = std::pow(10.0, static_cast<double>(cell.psd[j])) * ax.lnw[j];
    total += pw[j];
    flat &= cell.psd[j] <= kPvPsdFloor;
  }

  // 面积沿周期轴累积过半的位置 (左右各半); 全地板无重心 → NaN 不画刀
  pc.xm = pc.med_period = std::numeric_limits<float>::quiet_NaN();
  if (!flat) {
    double cum = 0.0;
    size_t jm = 0;
    float frac = 0.0f; // bin 内插值 [0, 1)
    for (; jm < n; ++jm) {
      if (cum + pw[jm] >= 0.5 * total) {
        frac = static_cast<float>((0.5 * total - cum) / pw[jm]);
        break;
      }
      cum += pw[jm];
    }
    const auto &lp = ax.log_period;
    pc.xm = jm + 1 < n ? lp[jm] + frac * (lp[jm + 1] - lp[jm]) : lp[n - 1];
    pc.med_period = std::pow(10.0f, pc.xm); // 网格行
  }

  for (size_t j = 0; j < n; ++j)
    pc.cs[j] = psd_share_color(cell.psd[j]);

  // y 固定域 [kPsdYLo, kPsdYHi], 全表同尺: 按数据范围铺满会把均值的
  // 抽样残差 (±1/√n) 铺满格高, 看成"宽幅震荡". 峰超过上界的特征 (能量集中单 bin) 只放宽上界不裁
  pc.y_dom[0] = kPsdYLo;
  pc.y_dom[1] = kPsdYHi;
  for (size_t j = 0; j < n; ++j)
    pc.y_dom[1] = std::max(pc.y_dom[1], cell.psd[j] + 0.1f);
}

// PSD 列 cell: log10 谱迷你图 (周期升序, 逐段密度染色) + hover 放大 (log 周期轴)
static void render_preview_psd(const Preview::Cell &cell, const char *code, size_t idx, uint64_t epoch, size_t level) {
  if (cell.psd_n == 0) {
    ImGui::TextDisabled("—");
    return;
  }
  const ImVec2 size(kSparkWidth, ImGui::GetTextLineHeight());
  if (!ImGui::IsRectVisible(size)) { // 视野外行 (列宽贴合帧全行提交): 占位即可, 不算不画
    ImGui::Dummy(size);
    return;
  }
  if (idx >= s_psd_cache.size())
    s_psd_cache.resize(idx + 1);
  PsdSparkCache &pc = s_psd_cache[idx];
  if (pc.epoch != epoch || pc.level != level)
    rebuild_psd_spark(pc, cell, epoch, level);
  const PsdSparkAxis &ax = psd_spark_axis(level);
  const char *unit = level_unit(level);

  const ImVec2 p0 = ImGui::GetCursorScreenPos();
  const bool hovered = sparkline("##spark_psd", ax.log_period.data(), cell.psd.data(), (int)ax.n, size,
                                 kSparkPsdColor, nullptr, pc.cs.data(), ax.dom, pc.y_dom);
  if (pc.xm == pc.xm) {
    // 谱重心竖刀 (与 sparkline 同一 x 归一化: 该层固定域)
    const float xpix = p0.x + (pc.xm - ax.dom[0]) / (ax.dom[1] - ax.dom[0]) * size.x;
    ImGui::GetWindowDrawList()->AddLine(ImVec2(xpix, p0.y), ImVec2(xpix, p0.y + size.y), kPsdCutColor, 2.0f);
  }
  if (!hovered)
    return;
  ImGui::BeginTooltip();
  if (pc.xm == pc.xm)
    ImGui::Text("%s  n = %u  重心 %.0f %s", code, cell.psd_n, pc.med_period, unit);
  else
    ImGui::Text("%s  n = %u  段内恒值", code, cell.psd_n);
  if (ImPlot::BeginPlot("##pv_psd", ImVec2(360, 200), ImPlotFlags_NoLegend)) {
    // y autofit: 迷你图固定尺是为了跨行比, 放大图看的是单行细节
    char xlabel[32];
    std::snprintf(xlabel, sizeof(xlabel), "周期 (%s)", unit);
    ImPlot::SetupAxes(xlabel, "log10 密度 / 白噪声", 0, ImPlotAxisFlags_AutoFit);
    ImPlot::SetupAxisScale(ImAxis_X1, ImPlotScale_Log10);
    ImPlot::SetupAxisLimits(ImAxis_X1, ax.period[0], ax.period[ax.n - 1], ImPlotCond_Always); // 与迷你图同域
    ImPlot::SetNextLineStyle(ImVec4(0.6f, 0.6f, 0.6f, 0.8f), 1.0f);
    ImPlot::PlotInfLines("##white_ref", &kPsdWhiteRef, 1, ImPlotInfLinesFlags_Horizontal);
    // 谱重心竖线 (与迷你图同一把刀)
    if (pc.xm == pc.xm) {
      ImPlot::SetNextLineStyle(ImGui::ColorConvertU32ToFloat4(kPsdCutColor), 2.5f);
      ImPlot::PlotInfLines("##med_ref", &pc.med_period, 1, 0);
    }
    ImPlot::SetNextLineStyle(kSparkPsdColor, 2.0f);
    ImPlot::PlotLine("##psd", ax.period.data(), cell.psd.data(), (int)ax.n);
    ImPlot::EndPlot();
  }
  ImGui::EndTooltip();
}

// ============================================================================
// 账目两列 (与 Distribution 完整性条同口径同着色, 抽样格子逐轮累积):
//   Stat:  "invalid,nan,zero,-inf,+inf%"  五个占比, 每个恰 3 字符 (invalid/nan/inf 相对全部格子, zero 相对有效值);
//          invalid = ts_valid 门控不过 (特征应稠密, 排最前)
//   Range: "min -1sd +1sd max"     四个数, 每个恰 4 字符 (sd 来自 sketch 矩)
// ============================================================================

static void render_stat_cell(const analysis::Integrity &it) {
  if (it.n_total == 0) {
    ImGui::TextDisabled("—");
    return;
  }
  const float n_total = static_cast<float>(it.n_total);
  const float pct[5] = {it.invalid_pct(), it.nan_pct(), it.zero_pct(),
                        100.0f * static_cast<float>(it.n_neg_inf) / n_total,
                        100.0f * static_cast<float>(it.n_pos_inf) / n_total};
  char s[8];
  for (int k = 0; k < 5; ++k) {
    if (k > 0) {
      ImGui::SameLine(0, 0);
      ImGui::TextUnformatted(",");
      ImGui::SameLine(0, 0);
    }
    misc::fmt_width(s, sizeof(s), pct[k], 3, /*fixed_zero_ok=*/true); // 百分比有界 [0,100]: 小量显示 "0.0", 不走 SI
    ImGui::TextColored(k == 2 ? GetZeroPctColor(pct[k]) : GetNanInfPctColor(pct[k]), "%s", s);
  }
  ImGui::SameLine(0, 0);
  ImGui::TextUnformatted("%");
  if (ImGui::IsItemHovered())
    ImGui::SetTooltip("invalid %zu, nan %zu, zero %zu, -inf %zu, +inf %zu  /  格子 %zu, 有效 %zu",
                      it.n_invalid(), it.n_nan, it.n_zero, it.n_neg_inf, it.n_pos_inf, it.n_total, it.n_valid);
}

static void render_range_cell(const Preview::Cell &cell) {
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

static void render_filter_dropdown(const char *label, bool &show_dropdown, std::set<std::string_view> &selected_values, std::span<const char *const> all_values) {
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
  // Section 1: Library + Level (库 = all (Bar 档) / 各票池 (Lob 档); 分析页读所选库, 因子页与本处共用每档当前库)
  // ==========================================================================
  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "1. Lib:");
  ImGui::SameLine();
  const std::string cur_lib = sel.lib();
  ImGui::SetNextItemWidth(140);
  if (ImGui::BeginCombo("##lib", cur_lib.c_str())) {
    for (const LibDef &d : feature_libs()) {
      const bool selected = cur_lib == d.name;
      if (ImGui::Selectable(d.name, selected) && !selected) {
        if (d.tier != sel.tier) {
          sel.clear_filters(); // 换档 = 换字段表: 过滤作废; 选中是 ColRef (带库), 切库保留
          sel.tier = d.tier;
        }
        sel.libs[d.tier] = d.name;
      }
      if (selected)
        ImGui::SetItemDefaultFocus();
    }
    ImGui::EndCombo();
  }
  if (!has_level(sel.tier, static_cast<size_t>(sel.selected_level)))
    sel.selected_level = L_1min; // 档缺该层 (Bar 无 L_sec, Lob 无分钟箱层 / L_1day); 两档都有 L_1min

  ImGui::SameLine();
  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "Level:");
  ImGui::SameLine();

  // 换层只换视图: 选中是 ColRef (带层), 跨层保留. 标签带该层特征数
  for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl) {
    if (lvl > 0)
      ImGui::SameLine();
    char label[48];
    std::snprintf(label, sizeof(label), "%s (%zu)###lvl%zu", level_name(lvl), feature.meta().features[lvl].size(), lvl);
    ImGui::BeginDisabled(!has_level(sel.tier, lvl));
    ImGui::RadioButton(label, &sel.selected_level, static_cast<int>(lvl));
    ImGui::EndDisabled();
  }

  ImGui::Separator();

  // ==========================================================================
  // Section 2: Filters
  // ==========================================================================
  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "2. Filters:");
  ImGui::SameLine();

  // All filters in one line
  render_filter_dropdown("Type", ui_state.show_filter_data_type, sel.filter_data_type, FeatureDataType_ALL);
  ImGui::SameLine();
  render_filter_dropdown("Cat1", ui_state.show_filter_cat_l1, sel.filter_cat_l1, feature_meta::categories_l1(sel.tier));
  ImGui::SameLine();
  // Cat2 过滤项 = 探测类 + 字段表人工 token (两档并集; dummy AUTO 不列, 与探测类重名去重)
  static const std::vector<const char *> s_cat2_items = [] {
    std::vector<const char *> v(std::begin(kCat2Classes), std::end(kCat2Classes));
    for (size_t tier = 0; tier < TIER_COUNT; ++tier)
      for (const char *t : feature_meta::categories_l2(static_cast<TierId>(tier))) {
        if (std::strcmp(t, kCat2Dummy) == 0)
          continue;
        bool dup = false;
        for (const char *u : v)
          dup |= std::strcmp(u, t) == 0;
        if (!dup)
          v.push_back(t);
      }
    return v;
  }();
  render_filter_dropdown("Cat2", ui_state.show_filter_cat_l2, sel.filter_cat_l2,
                         std::span<const char *const>(s_cat2_items.data(), s_cat2_items.size()));
  ImGui::SameLine();
  render_filter_dropdown("TS Norm", ui_state.show_filter_ts_method, sel.filter_ts_method, ts_MethodId_ALL);
  ImGui::SameLine();
  render_filter_dropdown("CS Norm", ui_state.show_filter_cs_method, sel.filter_cs_method, cs_MethodId_ALL);
  ImGui::SameLine();

  // Reset filters button
  if (ImGui::Button("Reset", ImVec2(60, 0))) {
    sel.clear_filters();
    ui_state.sort_column = -1; // Reset table sorting
  }

  ImGui::Separator();

  // ==========================================================================
  // Section 3: Feature Table
  // ==========================================================================
  const auto &features = get_current_level_features(feature);
  const auto &deps_list = feature.meta().deps[sel.selected_level];

  // Cat2 有效值快照 (探测 or 人工覆盖; 过滤/排序/显示共用). 短锁读 preview cells
  static std::vector<const char *> s_eff_cat2;
  {
    std::lock_guard<std::mutex> preview_lock(data.preview.mutex);
    effective_cat2_snapshot(feature, data.preview, sel.tier, (size_t)sel.selected_level, s_eff_cat2);
  }
  auto filtered_indices = get_filtered_indices(sel, features, s_eff_cat2);

  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "3. Features:");
  ImGui::SameLine();
  ImGui::Text("Showing %d / %d", (int)filtered_indices.size(), (int)features.size());

  // Preview 轮训进度 (Dist/PSD 两列逐轮收敛; 免锁读原子)
  {
    if (data.preview.busy()) {
      const int done = (int)data.preview.done.load(std::memory_order_relaxed);
      const int total = (int)data.preview.total.load(std::memory_order_relaxed);
      ImGui::SameLine();
      ImGui::TextDisabled("(preview %d/%d)", done, total);
    }
  }

  // 内容变了 (层 / 过滤行集 / preview 发布代) → 请求列宽重新贴合.
  // 连发 3 帧: TableSetColumnWidthAutoAll 先解除列裁剪, 下一帧才量到内容.
  // 贴合帧全行提交 (跳过视野裁剪), 构建期发布很密 (每块一个 epoch) → epoch 触发限速;
  // epoch 未消化的话每帧重查, 限速窗口过了必然补上最后一次贴合
  const uint64_t pv_epoch = data.preview.epoch.load(std::memory_order_relaxed);
  {
    uint64_t rows_hash = filtered_indices.size();
    for (int i : filtered_indices)
      rows_hash = rows_hash * 1099511628211ull + (uint64_t)(uint32_t)i;
    const bool struct_changed = ui_state.fit_level != sel.selected_level || ui_state.fit_rows_hash != rows_hash;
    const bool epoch_changed = ui_state.fit_preview_epoch != pv_epoch;
    const double now = ImGui::GetTime();
    if (struct_changed || (epoch_changed && now - ui_state.fit_last_time >= 0.5)) {
      ui_state.fit_level = sel.selected_level;
      ui_state.fit_rows_hash = rows_hash;
      ui_state.fit_preview_epoch = pv_epoch;
      ui_state.fit_last_time = now;
      ui_state.fit_frames = 5; // 递减后 4→1 共 4 帧全行提交: 喂测量 / 请求 ×2 / 落定
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
    ImGui::TableSetupColumn("Multi", ImGuiTableColumnFlags_WidthFixed | ImGuiTableColumnFlags_NoSort);         // 0
    ImGui::TableSetupColumn("Code", ImGuiTableColumnFlags_WidthFixed);                                         // 1
    ImGui::TableSetupColumn("Valid", ImGuiTableColumnFlags_WidthFixed);                                        // 2
    ImGui::TableSetupColumn("Name CN", ImGuiTableColumnFlags_WidthFixed);                                      // 3
    ImGui::TableSetupColumn("Type", ImGuiTableColumnFlags_WidthFixed);                                         // 4
    ImGui::TableSetupColumn("Cat1", ImGuiTableColumnFlags_WidthFixed);                                         // 5
    ImGui::TableSetupColumn("Cat2", ImGuiTableColumnFlags_WidthFixed);                                         // 6
    ImGui::TableSetupColumn("Stat", ImGuiTableColumnFlags_WidthFixed);                                         // 7
    ImGui::TableSetupColumn("Range", ImGuiTableColumnFlags_WidthFixed);                                        // 8
    ImGui::TableSetupColumn("Dist", ImGuiTableColumnFlags_WidthFixed | ImGuiTableColumnFlags_NoSort);          // 9
    ImGui::TableSetupColumn("PSD(intraday)", ImGuiTableColumnFlags_WidthFixed | ImGuiTableColumnFlags_NoSort); // 10
    ImGui::TableSetupColumn("File", ImGuiTableColumnFlags_WidthFixed);                                         // 11
    ImGui::TableSetupColumn("TS Norm", ImGuiTableColumnFlags_WidthFixed);                                      // 12
    ImGui::TableSetupColumn("CS Norm", ImGuiTableColumnFlags_WidthFixed);                                      // 13
    ImGui::TableSetupColumn("Deps", ImGuiTableColumnFlags_WidthFixed);                                         // 14
    ImGui::TableSetupScrollFreeze(0, 1);                                                                       // Freeze header row

    // 贴合窗 (kFitFrames 帧) 全程全行提交, 但只在中段两帧真正请求贴合:
    // 请求用的是上一帧的测量 —— 若上一帧是 clipper 帧 (只量到视野内行),
    // 列宽会先缩窄再弹回 (肉眼抽搐). 首帧先把全行喂进测量, 尾帧让最后
    // 一次请求在全量测量下落定, 之后才交还 clipper
    if (ui_state.fit_frames > 0) {
      ui_state.fit_frames--;
      if (ui_state.fit_frames == 2 || ui_state.fit_frames == 3) {
        ImGuiTable *table = ImGui::GetCurrentTable();
        assert(table);
        ImGui::TableSetColumnWidthAutoAll(table); // 全列贴合内容 (含视野外列)
      }
    }

    // Custom header row with tooltips
    ImGui::TableNextRow(ImGuiTableRowFlags_Headers);
    const char *headers[kNumCols] = {"Multi", "Code", "Valid", "Name CN", "Type", "Cat1", "Cat2",
                                     "Stat", "Range", "Dist", "PSD (intraday)", "File", "TS Norm", "CS Norm", "Deps"};
    const char *tooltips[kNumCols] = {
        "多选: 选择多个特征进行对比\n"
        "• 首个作为主特征",
        "代码: 特征的唯一标识符",
        "有效粒度\n"
        "• ALL = 全部\n"
        "• DATA = 数据\n"
        "• DEPTH = 深度 (仅 L_sec)",
        "中文名称: 特征的描述性名称",
        "数据类型\n"
        "• TS = 时序\n"
        "• CS = 截面\n"
        "• LB = 标签\n"
        "• SH = 共享\n"
        "• META = 元数据",
        "一级分类: 特征的类别\n"
        "• 同色同组相邻",
        "二级分类: 值域自动探测\n"
        "• 取值: price / flag / rank / ratio_pos / ratio_neg / ratio / raw\n"
        "• 绿字 = 字段表人工赋值覆盖",
        "账目: invalid / nan / zero / -inf / +inf 占比%\n"
        "• invalid = ts_valid 门控不过\n"
        "• 特征应稠密",
        "值域: min -1sd +1sd max",
        "平均分布: 抽样 (日 × 资产) 的 PDF",
        "日内频谱: 单日 PSD 每 bin 能量占比均值\n"
        "• x = 周期 2~256 min\n"
        "• 灰线 = 白噪声\n"
        "• 青刀 = 谱重心\n"
        "• 日频特征 = 地板线",
        "节点文件: 定义该列 FIELDS_ 行的文件\n"
        "• 悬停看完整路径",
        "时序归一化: SRC 列 TS(..., Tf, Method) 推出",
        "截面归一化: SRC 列 CS(..., Tf, Method) 推出",
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
    const size_t view_level = static_cast<size_t>(sel.selected_level);
    static const Preview::Cell s_empty_cell{};
    // 槽位 = metadata 下标; 非预览档 / 未就绪 → 空 cell
    auto cell_of = [&](int i) -> const Preview::Cell & {
      const Preview::Cell *c = data.preview.cell(sel.tier, view_level, static_cast<size_t>(i));
      return c ? *c : s_empty_cell;
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
              const Preview::Cell &ca = cell_of(a);
              const Preview::Cell &cb = cell_of(b);
              int cmp = 0;
              switch (ui_state.sort_column) {
              case 1:
                cmp = strcmp(fa.code, fb.code);
                break;
              case 2:
                cmp = (int)fa.valid_type - (int)fb.valid_type;
                break;
              case 3:
                cmp = strcmp(fa.name_cn, fb.name_cn);
                break;
              case 4:
                cmp = (int)fa.data_type - (int)fb.data_type;
                break;
              case 5:
                cmp = std::strcmp(fa.cat_l1, fb.cat_l1);
                break;
              case 6:
                cmp = std::strcmp(s_eff_cat2[a], s_eff_cat2[b]);
                break;
              case 7: // Stat: invalid% (次键 nan%)
                cmp = cmp3(ca.integrity.invalid_pct(), cb.integrity.invalid_pct());
                if (cmp == 0)
                  cmp = cmp3(ca.integrity.nan_pct(), cb.integrity.nan_pct());
                break;
              case 8: // Range: sd (var 单调等价)
                cmp = cmp3(ca.var, cb.var);
                break;
              case 11:
                cmp = std::strcmp(fa.file, fb.file);
                break;
              case 12: // TS Norm
                cmp = (int)fa.ts_method - (int)fb.ts_method;
                break;
              case 13: // CS Norm
                cmp = (int)fa.cs_method - (int)fb.cs_method;
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

    // Table rows (preview 锁已在上方持有): 行体 lambda, 下面按视野裁剪分两条路提交
    const LibId view_lib = sel.cur_lib();
    auto render_row = [&](int idx) {
      const FeatureMetadata &f = features[idx];

      // Check if this row is selected
      const ColRef ref{view_lib, static_cast<uint8_t>(view_level), static_cast<uint16_t>(idx)};
      bool is_selected = sel.picked.contains(ref);

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
          sel.picked.insert(ref);
        else
          sel.picked.erase(ref);
      }

      // Column: Code
      ImGui::TableNextColumn();
      ImGui::TextUnformatted(f.code);

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

        // Render LaTeX formula (gui/util/Latex: 与 Operators 表共用缓存)
        tex::TeXRender *render = Latex::Get(f.formula, 32.0f);
        if (render) {
          Latex::Draw(render);
        } else {
          ImGui::TextWrapped("%s", f.formula); // Fallback to plain text
        }

        ImGui::Spacing();
        ImGui::Text("Description:");
        ImGui::TextWrapped("%s", f.description);
        ImGui::PopTextWrapPos();
        ImGui::EndTooltip();
      }

      // Column: Type
      ImGui::TableNextColumn();
      ImGui::TextUnformatted(to_string(f.data_type).en);

      // Column: Cat1
      ImGui::TableNextColumn();
      ImGui::TextUnformatted(f.cat_l1);

      // Column: Cat2 (值域自动探测; 绿字 = 字段表人工赋值覆盖)
      ImGui::TableNextColumn();
      if (cat2_overridden(f))
        ImGui::TextColored(ImVec4(0.2f, 0.8f, 0.4f, 1.0f), "%s", f.cat_l2);
      else if (s_eff_cat2[idx] == kCat2Unknown)
        ImGui::TextDisabled("?");
      else
        ImGui::TextUnformatted(s_eff_cat2[idx]);

      // Columns: Stat / Range 账目 + Dist / PSD 迷你图 (预览; 槽位 = metadata 下标)
      const Preview::Cell &cell = cell_of(idx);
      ImGui::PushID(idx);
      ImGui::TableNextColumn();
      render_stat_cell(cell.integrity);
      ImGui::TableNextColumn();
      render_range_cell(cell);
      ImGui::TableNextColumn();
      render_preview_dist(cell, f.code);
      ImGui::TableNextColumn();
      render_preview_psd(cell, f.code, (size_t)idx, pv_epoch, view_level);
      ImGui::PopID();

      // Column: File (节点文件名; 悬停 = include/ 下完整路径)
      ImGui::TableNextColumn();
      const char *slash = std::strrchr(f.file, '/');
      ImGui::TextUnformatted(slash ? slash + 1 : f.file);
      if (ImGui::IsItemHovered())
        ImGui::SetTooltip("%s", f.file);

      // Columns: TS Norm / CS Norm (SRC 推出的 Method; Tf 不显示; None 显 "-")
      ImGui::TableNextColumn();
      render_norm_cell(to_string(f.ts_method), f.ts_method == ts::MethodId::None);
      ImGui::TableNextColumn();
      render_norm_cell(to_string(f.cs_method), f.cs_method == cs::MethodId::None);

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
    };

    if (ui_state.fit_frames > 0) {
      // 贴合帧: 全行提交 (视野外行的列宽也要量到; 迷你图两列在渲染器内占位早退, 便宜)
      for (int idx : filtered_indices)
        render_row(idx);
    } else {
      // 等高行 → clipper 只提交视野内行 (几百行表格的主滚动开销就在这)
      ImGuiListClipper clipper;
      clipper.Begin((int)filtered_indices.size());
      while (clipper.Step())
        for (int r = clipper.DisplayStart; r < clipper.DisplayEnd; ++r)
          render_row(filtered_indices[r]);
    }

    ImGui::EndTable();
  }

  ImGui::PopStyleVar(); // CellPadding

  if (ImGui::Button("Clear All", ImVec2(80, 0))) {
    sel.picked.clear();
  }
  if (!sel.picked.empty()) {
    ImGui::SameLine();
    ImGui::TextDisabled("Picked %d", (int)sel.picked.size());
    if (ImGui::IsItemHovered()) {
      ImGui::BeginTooltip();
      for (const ColRef &c : sel.picked)
        ImGui::Text("%s %s %s", lib_def(c.lib).name, level_name(c.level), feature.meta(c).code);
      ImGui::EndTooltip();
    }
  }
}

// ============================================================================
// 特征表落地 JSON (给人看: 表格可见列 + Stat / Range, 一行一特征; Dist / PSD 曲线不落)
// ============================================================================

// 4 位有效数字 (人读; 全精度 float 一串尾数只添噪)
static double sig4(float v) {
  char buf[32];
  snprintf(buf, sizeof(buf), "%.4g", (double)v);
  return std::strtod(buf, nullptr);
}

void SaveFeatureTableJson(SharedData &data) {
  using json = nlohmann::json;
  const Feature::Metadata &meta = data.feature.meta();
  const std::filesystem::path dir = data.config.FeatureLibDir(data.feature.selection.lib());

  const std::filesystem::path path = dir / "features.json";
  std::filesystem::create_directories(path.parent_path());
  const std::filesystem::path tmp = path.string() + ".tmp";
  {
    std::ofstream file(tmp);
    assert(file.is_open() && "features.json: 临时文件打不开");
    file << "{\n";
    file << " \"tier\": " << json(tier_name(meta.tier)).dump() << ",\n";
    file << " \"lib\": " << json(dir.filename().string()).dump() << ",\n";
    file << " \"start_date\": " << json(misc::dash(data.config.start_date)).dump() << ",\n";
    file << " \"end_date\": " << json(misc::dash(data.config.end_date)).dump() << ",\n";
    file << " \"preview\": {\"rounds\": " << data.preview.done.load(std::memory_order_relaxed)
         << ", \"assets_per_round\": " << kPvAssetsPerRound << "},\n";

    std::lock_guard<std::mutex> preview_lock(data.preview.mutex);
    std::vector<const char *> eff_cat2;
    bool first_level = true;
    for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl) {
      if (!has_level(meta.tier, lvl))
        continue;
      const auto &features = meta.features[lvl];
      const auto &deps_list = meta.deps[lvl];
      assert(deps_list.size() == features.size());
      effective_cat2_snapshot(data.feature, data.preview, meta.tier, lvl, eff_cat2); // 与表格同口径 (探测 or 人工覆盖)

      file << (first_level ? "" : ",\n") << " \"" << level_name(lvl) << "\": [\n";
      first_level = false;
      for (size_t i = 0; i < features.size(); ++i) {
        const FeatureMetadata &f = features[i];
        // 键序 = 表格列序 (nlohmann ordered_json 保插入序)
        nlohmann::ordered_json r;
        r["col"] = i;
        r["code"] = f.code;
        r["valid"] = to_string(f.valid_type).en;
        r["name_cn"] = f.name_cn;
        r["type"] = to_string(f.data_type).en;
        r["cat_l1"] = f.cat_l1;
        r["cat_l2"] = eff_cat2[i];

        // Stat / Range 与表格同口径 (百分比 / min -1sd +1sd max); 表格显示 "—" 的格子不落键
        if (const Preview::Cell *pc = data.preview.cell(meta.tier, lvl, i)) {
          const Preview::Cell &cell = *pc;
          const analysis::Integrity &it = cell.integrity;
          if (it.n_total > 0) {
            const float n_total = static_cast<float>(it.n_total);
            r["stat"] = {{"invalid%", sig4(it.invalid_pct())},
                         {"nan%", sig4(it.nan_pct())},
                         {"zero%", sig4(it.zero_pct())},
                         {"-inf%", sig4(100.0f * static_cast<float>(it.n_neg_inf) / n_total)},
                         {"+inf%", sig4(100.0f * static_cast<float>(it.n_pos_inf) / n_total)},
                         {"n", it.n_total}};
          }
          if (it.n_valid > 0) {
            const float sd = cell.sd();
            r["range"] = {{"min", sig4(it.val_min)},
                          {"-1sd", sig4(cell.mean - sd)},
                          {"+1sd", sig4(cell.mean + sd)},
                          {"max", sig4(it.val_max)},
                          {"mean", sig4(cell.mean)},
                          {"sd", sig4(sd)}};
          }
        }
        r["file"] = f.file;
        r["ts_norm"] = to_string(f.ts_method).en;
        r["cs_norm"] = to_string(f.cs_method).en;
        r["deps"] = split_deps(deps_list[i]);

        file << "  " << r.dump() << (i + 1 < features.size() ? ",\n" : "\n");
      }
      file << " ]";
    }
    file << "\n}\n";
    assert(file.good() && "features.json: 写入失败");
  }
  std::filesystem::rename(tmp, path); // 原子替换: 崩在中途不留半截文件 (同 AssetAxis::save)
}

} // namespace GUI::Features
