// Tab Inspect — 见头文件
#include "gui/task_factors/ui/TabInspect.hpp"
#include "features/TimeIndex.hpp" // L_1min_to_Clock (K 线横轴刻度)
#include "gui/Tasks.hpp"          // StatusColor
#include "shared/Asset.hpp"       // AssetItem (资产代码 / 名称)

#include "imgui.h"
#include "implot.h"
#include "implot_internal.h" // BeginItem / FitPoint (自绘蜡烛)

#include <algorithm>
#include <array>
#include <cassert>
#include <cmath>
#include <cstdio>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

namespace GUI::Factors {

namespace {

constexpr int K = factor::stat::kGroups;
using factor::stat::kFwd;
using factor::stat::kLegs;
using factor::stat::kLong;
using factor::stat::kShort;
using factor::stat::leg_period;
// 正向腿 l 的组号 (G20 / G1)
constexpr int leg_group(int l) { return factor::stat::leg_group(kFwd, l); }
// 组 k 是正向两腿之一 (LS 图例项 hover 时高亮)
constexpr bool is_leg(int k) { return k == leg_group(kLong) || k == leg_group(kShort); }

// 竖轴刻度统一定宽 (字体是等宽的 Maple Mono, 7 字符 = 定像素): 切因子 / 持有期 / 光标时数字位数变也不改绘图区左边界, 图不震
constexpr const char *kYTickFmt = "%7.2f";

// 紧凑图例: 几张图的 legend box 等大 (统一 padding / spacing), 字体沿用默认; RAII, 构造时 push, 析构时 pop, 包住 BeginPlot/EndPlot 即生效
struct CompactLegend {
  CompactLegend() {
    ImPlot::PushStyleVar(ImPlotStyleVar_LegendPadding, ImVec2(4.f, 2.f));
    ImPlot::PushStyleVar(ImPlotStyleVar_LegendInnerPadding, ImVec2(2.f, 2.f));
    ImPlot::PushStyleVar(ImPlotStyleVar_LegendSpacing, ImVec2(2.f, 0.f));
  }
  ~CompactLegend() { ImPlot::PopStyleVar(3); }
};

// 可手动缩放的视野: 目标 [lo, hi] (数据 / 图例决定) 变了才强设一次, 否则留着用户的缩放 / 平移. 每 (图, 轴) 记上次目标; BeginPlot 后 Setup 阶段调
void follow_limits(ImAxis ax, double lo, double hi) {
  static std::unordered_map<ImGuiID, ImPlotRange> last;
  assert(GImPlot->CurrentPlot);
  ImPlotRange &r = last[ImHashData(&ax, sizeof(ax), GImPlot->CurrentPlot->ID)];
  const bool changed = r.Min != lo || r.Max != hi;
  r = ImPlotRange(lo, hi);
  ImPlot::SetupAxisLimits(ax, lo, hi, changed ? ImPlotCond_Always : ImPlotCond_Once);
}

// 0 钉在正中的可缩放视野: 目标半宽 half 变了才强设 [−half, half]; 否则取用户缩放 / 平移 / 双击 fit 后的 max(|lo|, |hi|) 重新对称到 0.
// 经 SetupAxisLinks (ImPlot 在 EndPlot 回写交互后的范围), 不用 ImPlotCond_Always (会锁死交互). BeginPlot 后 Setup 阶段调
void follow_centered(ImAxis ax, double half) {
  struct Lim {
    double target, lo, hi;
  };
  static std::unordered_map<ImGuiID, Lim> last; // 节点式容器: 元素地址跨 rehash 不变, 可作 link 指针
  assert(GImPlot->CurrentPlot && half > 0.0);
  Lim &l = last[ImHashData(&ax, sizeof(ax), GImPlot->CurrentPlot->ID)];
  const double h = l.target != half ? half : std::max(std::abs(l.lo), std::abs(l.hi));
  l = Lim{half, -h, h};
  ImPlot::SetupAxisLinks(ax, &l.lo, &l.hi);
}

// AutoFit 的可缩放替身: 数据键 key 或图例显隐 (上帧各 item 的 Show; 隐藏项不参与 fit) 变了才让下一张图 (title) 对 axes 各 fit 一次; BeginPlot 前调
void fit_on_change(const char *title, ImGuiID key, std::initializer_list<ImAxis> axes) {
  static std::unordered_map<ImGuiID, ImGuiID> last;
  if (ImPlotPlot *p = ImPlot::GetPlot(title)) // 首帧还没这张图 → 只看数据键
    for (int i = 0; i < p->Items.GetItemCount(); ++i) {
      const ImPlotItem &it = *p->Items.GetItemByIndex(i);
      key = ImHashData(&it.Show, sizeof(it.Show), ImHashData(&it.ID, sizeof(it.ID), key));
    }
  const auto [it, fresh] = last.try_emplace(ImGui::GetID(title), key);
  if (!fresh && it->second == key)
    return;
  it->second = key;
  for (const ImAxis ax : axes)
    ImPlot::SetNextAxisToFit(ax);
}

// 横轴刻度: 交易日下标 → YYMMDD (user_data = InspectDerived::dates)
int date_formatter(double v, char *buf, int size, void *ud) {
  const auto &dates = *static_cast<const std::vector<misc::Ymd> *>(ud);
  if (dates.empty())
    return std::snprintf(buf, size, "%g", v);
  const int i = std::clamp(static_cast<int>(std::floor(v)), 0, static_cast<int>(dates.size()) - 1);
  return std::snprintf(buf, size, "%06d", dates[static_cast<size_t>(i)] % 1000000);
}

// 分层色: 两端两色 (G1 深蓝 / G20 深红), 中心中性灰, 色度 w = 线性底坡 + 两侧反向 tanh 台阶: w = kColRamp·|s| + (1 − kColRamp)·step,
// step = ½(1 + tanh(kColGain·(|s| − c))) 归一到 [0, 1], s ∈ [−1, 1] 为到中心的有符号距离, 台阶中心 c 落在从边数第 kColEdge + 1 组;
// 边上 kColEdge 组 (G1 G2 / G19 G20) 近全色, 台阶往里由底坡接着渐淡到中心灰 (中段仍分得出组序, 不全灰).
// 单调不翻色 (Jet 在中段来回翻色; 纯线性渐变两端不够跳; 纯台阶中段全灰)
constexpr ImVec4 kColBottom(0.00f, 0.20f, 0.70f, 1.f), kColTop(0.85f, 0.15f, 0.10f, 1.f), kColLS(1.f, 1.f, 1.f, 1.f);
constexpr ImVec4 kColNeutral(0.50f, 0.50f, 0.52f, 1.f);                   // 中段
constexpr ImVec4 kColCursor(1.f, 0.5f, 0.f, 1.f);                         // 光标 (左上图交易日 / 右上 K 线 / 资产轴), 对仗 OrderFlow 的锚
constexpr double kCursorPad = 0.02;                                       // 左上 / 右上图默认 x 视野右侧留白 (占全段): 默认末日光标不贴边
constexpr ImVec4 kColIndex(0.55f, 0.47f, 0.15f, 0.40f);                   // 右上 K 线叠加的 池等权指数线 (视野首根重锚; 背景, 深淡)
constexpr ImVec4 kColRef(0.15f, 0.45f, 0.48f, 0.40f);                     // 右上 K 线叠加的滚动重锚指数线 (背景, 深淡)
constexpr ImVec4 kColIcHigh(0.30f, 0.60f, 1.00f, 0.85f);                  // 右上 K 线叠加的累计 rIC (右轴): 新高色
constexpr ImVec4 kColIcDd(1.00f, 0.25f, 0.25f, 0.85f);                    // 同上: 全段最大回撤处的色 (中间按回撤渐变)
constexpr float kTailLayerWeight[kTails] = {1.0f, 0.75f, 0.5f};           // 尾部分层线宽 (组线 1.2): 越极端越细
constexpr float kTailLsWeight[kTails] = {2.0f, 1.5f, 1.0f};               // 尾部 LS 线宽 (LS 2.5): 同上
constexpr float kTailFillAlpha[kTails] = {0.08f, 0.15f, 0.24f};           // 相邻尾部分层线间色带 alpha: 越极端越重, 但不盖线
constexpr float kHiScale = 2.f;                                           // 图例 hover 高亮倍数 (同 ImPlot 线宽 ITEM_HIGHLIGHT_LINE_SCALE; 线以外的件自己乘)
constexpr float kColEdge = 2.f;                                           // 两端各留几组显色
constexpr float kColGain = 8.f;                                           // tanh 增益: 越大台阶越陡 (8 → 台阶上相邻组 ~0.84 / ~0.5 / ~0.16)
constexpr float kColRamp = 0.3f;                                          // 线性底坡占比: 中段色度 ≈ kColRamp·|s|, 中心灰起渐变
ImVec4 lerp_color(float t) {                                              // t ∈ [0, 1]: 0 = 深蓝 (G1 / 最负), 0.5 = 灰, 1 = 深红 (G20 / 最正)
  constexpr float mid = 1.f - kColEdge * 2.f / static_cast<float>(K - 1); // 台阶中心 = 从边数第 kColEdge + 1 组的 |s|
  const auto step = [](float a) { return 0.5f * (1.f + std::tanh(kColGain * (a - mid))); };
  const float s = (t - 0.5f) * 2.f, a = std::abs(s);
  const float w = kColRamp * a + (1.f - kColRamp) * (step(a) - step(0.f)) / (step(1.f) - step(0.f));
  const ImVec4 &c = s < 0.f ? kColBottom : kColTop;
  return ImVec4(kColNeutral.x + (c.x - kColNeutral.x) * w, kColNeutral.y + (c.y - kColNeutral.y) * w, kColNeutral.z + (c.z - kColNeutral.z) * w, 1.f);
}
ImVec4 group_color(int k) { return lerp_color(static_cast<float>(k) / static_cast<float>(K - 1)); }

// 单调三次 Hermite (Fritsch–Carlson) 把节点 (x 严格升序) 稠密采样成折线: 圆滑且不过冲 (期限结构点少, 直连折线生硬)
void pchip_dense(const std::vector<double> &x, const std::vector<double> &y, std::vector<double> &xs, std::vector<double> &ys) {
  const int n = static_cast<int>(x.size());
  assert(y.size() == x.size());
  xs.clear();
  ys.clear();
  if (n < 2) {
    xs = x;
    ys = y;
    return;
  }
  std::vector<double> h(static_cast<size_t>(n) - 1), del(static_cast<size_t>(n) - 1), m(static_cast<size_t>(n), 0.0);
  for (int i = 0; i + 1 < n; ++i) {
    h[static_cast<size_t>(i)] = x[static_cast<size_t>(i) + 1] - x[static_cast<size_t>(i)];
    assert(h[static_cast<size_t>(i)] > 0.0);
    del[static_cast<size_t>(i)] = (y[static_cast<size_t>(i) + 1] - y[static_cast<size_t>(i)]) / h[static_cast<size_t>(i)];
  }
  if (n == 2) {
    m[0] = m[1] = del[0];
  } else {
    for (int i = 1; i + 1 < n; ++i) {
      const double d0 = del[static_cast<size_t>(i) - 1], d1 = del[static_cast<size_t>(i)];
      if (d0 * d1 <= 0.0)
        continue; // 极值点斜率 0
      const double w1 = 2.0 * h[static_cast<size_t>(i)] + h[static_cast<size_t>(i) - 1], w2 = h[static_cast<size_t>(i)] + 2.0 * h[static_cast<size_t>(i) - 1];
      m[static_cast<size_t>(i)] = (w1 + w2) / (w1 / d0 + w2 / d1);
    }
    auto endpoint = [](double h0, double h1, double d0, double d1) { // 三点外推 + 保形钳位
      const double s = ((2.0 * h0 + h1) * d0 - h0 * d1) / (h0 + h1);
      if (s * d0 <= 0.0)
        return 0.0;
      if (d0 * d1 <= 0.0 && std::abs(s) > 3.0 * std::abs(d0))
        return 3.0 * d0;
      return s;
    };
    m[0] = endpoint(h[0], h[1], del[0], del[1]);
    m[static_cast<size_t>(n) - 1] = endpoint(h[static_cast<size_t>(n) - 2], h[static_cast<size_t>(n) - 3], del[static_cast<size_t>(n) - 2], del[static_cast<size_t>(n) - 3]);
  }
  constexpr int kPerSeg = 16;
  xs.reserve(static_cast<size_t>(n - 1) * kPerSeg + 1);
  ys.reserve(xs.capacity());
  for (int i = 0; i + 1 < n; ++i) {
    const size_t u = static_cast<size_t>(i);
    for (int j = 0; j < kPerSeg; ++j) {
      const double t = static_cast<double>(j) / kPerSeg, t2 = t * t, t3 = t2 * t;
      const double h00 = 2 * t3 - 3 * t2 + 1, h10 = t3 - 2 * t2 + t, h01 = -2 * t3 + 3 * t2, h11 = t3 - t2;
      xs.push_back(x[u] + t * h[u]);
      ys.push_back(h00 * y[u] + h10 * h[u] * m[u] + h01 * y[u + 1] + h11 * h[u] * m[u + 1]);
    }
  }
  xs.push_back(x.back());
  ys.push_back(y.back());
}

// 选定 hold → 日级序列 + IC 分布 / 时序. 分层 / LS 线 = InspectResult::nav (净值引擎 fold_book) 选定口径的日收益 r_d,
// 第 c 日画 Σ_{d<c} ln(1 + r_d) × kDaysPerYear / days × 100 % (对数累计, 全部线同一系数: 线间差 / 斜率比不变, 匀速 = 直线; 末点 = 全段对数年化, 图例);
// LS Sharpe = 日收益 mean / sd × √kDaysPerYear; rIC 分布 = InspectResult::ic_agg (IcPdfP 成品, Run 行小图是它的迷你版).
void derive(const InspectResult &res, int sel, bool absolute, InspectDerived &d) {
  d = InspectDerived{};
  if (res.key.empty() || !res.error.empty() || res.pl.ric.empty())
    return;
  assert(sel >= 0 && sel < res.hd.n);
  const int T = res.scope.T, days = res.scope.days, S = factor_level(res.lv).seg;
  assert(T == days * S && days >= 1);
  const float *ric = res.ric_at(sel);
  assert(res.nav.n == res.hd.n * kNavLines && res.nav.P == days);
  d.hs = res.hold[static_cast<size_t>(sel)];
  d.adist = res.alpha_dist;
  d.days = days;
  d.dates = res.dates;
  assert(res.pool_index.n == res.hd.n && res.pool_index.P == T);
  d.pool_index.resize(static_cast<size_t>(T));
  compound(res.pool_index.at(sel), static_cast<size_t>(T), [&](size_t t, double l) { d.pool_index[t] = static_cast<float>(l); });
  d.x_day.resize(static_cast<size_t>(days) + 1);
  for (int i = 0; i <= days; ++i)
    d.x_day[static_cast<size_t>(i)] = static_cast<float>(i);
  for (std::vector<float> &c : d.lay_ann)
    c.assign(static_cast<size_t>(days) + 1, 0.f);
  for (std::vector<float> &c : d.ls_ann)
    c.assign(static_cast<size_t>(days) + 1, 0.f);
  std::vector<float> &ics = d.ic_series;
  ics.reserve(static_cast<size_t>(T));
  d.ic_cum.resize(static_cast<size_t>(T));
  d.ic_dd.resize(static_cast<size_t>(T));
  double ic_sum = 0.0, ic_peak = 0.0;
  for (int t = 0; t < T; ++t) {
    if (!std::isnan(ric[t])) {
      ics.push_back(ric[t]);
      ic_sum += ric[t];
    }
    ic_peak = std::max(ic_peak, ic_sum);
    d.ic_cum[static_cast<size_t>(t)] = static_cast<float>(ic_sum);
    d.ic_dd[static_cast<size_t>(t)] = static_cast<float>(ic_peak - ic_sum);
    d.ic_dd_max = std::max(d.ic_dd_max, d.ic_dd[static_cast<size_t>(t)]);
  }
  const double k_ann = 100.0 * factor::stat::kDaysPerYear / days;
  const auto ann_of = [&](const float *v, std::vector<float> &ann) {
    double s = 0.0;
    for (int day = 0; day < days; ++day) {
      assert(v[day] > -1.f && "日收益 ≤ −100%: 对数累计无定义");
      s += std::log1p(static_cast<double>(v[day]));
      ann[static_cast<size_t>(day) + 1] = static_cast<float>(s * k_ann);
    }
  };
  for (int i = 0; i < kLayerLines; ++i)
    ann_of(res.nav_at(sel, (absolute ? 0 : kNavLayers) + i), d.lay_ann[i]);
  for (int i = 0; i < kLsLines; ++i) {
    const float *v = res.nav_at(sel, 2 * kNavLayers + (absolute ? 0 : kNavLs) + i);
    ann_of(v, d.ls_ann[i]);
    d.ls_turn[i] = static_cast<float>(factor::stat::turn_annual(d.hs, res.ls_turn_of(i, sel)));
    factor::stat::MomAcc m;
    for (int day = 0; day < days; ++day)
      m.add(v[day]);
    d.ls_sharpe[i] = static_cast<float>(factor::stat::ann_sharpe(m.finish(1), m.n, days, factor::stat::kDaysPerYear));
  }
  // 各簇 / 各 LS 线的 min / max (含起点 0): y 范围按显示的簇每帧合成 (layer_range)
  const auto span = [](const std::vector<float> &v, float &a, float &b) {
    for (const float x : v)
      a = std::min(a, x), b = std::max(b, x);
  };
  for (int k = 0; k < K; ++k)
    span(d.lay_ann[k], d.lay_lo[0], d.lay_hi[0]);
  for (int j = 0; j < kTails; ++j)
    for (int l = 0; l < kLegs; ++l)
      span(d.lay_ann[layer_tail(l, j)], d.lay_lo[1 + j], d.lay_hi[1 + j]);
  for (int i = 0; i < kLsLines; ++i)
    span(d.ls_ann[i], d.ls_lo[i], d.ls_hi[i]);
  d.ic_pdf = res.ic_agg[sel];
  d.valid = true;
}

// 空图占位 (统一样式)
void empty_plot(const char *title, const ImVec2 &size, const char *hint) {
  if (ImPlot::BeginPlot(title, size, ImPlotFlags_NoLegend | ImPlotFlags_NoMenus)) {
    ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_NoTickLabels | ImPlotAxisFlags_NoGridLines, ImPlotAxisFlags_NoTickLabels | ImPlotAxisFlags_NoGridLines);
    ImPlot::SetupAxesLimits(0, 1, 0, 1, ImPlotCond_Always);
    ImPlot::PlotText(hint, 0.5, 0.5);
    ImPlot::EndPlot();
  }
}

// 左上 y 范围: 显示中的分层簇 (簇 0 = 20 组恒显 + 开着的尾部级) 与显示中的 LS 线 (顶底 + 开着的尾部级) 同一口径, 合并 min / max 加边.
// 每帧按图例状态合成 (开关图例即重贴), 只合并 derive 缓存的簇极值, O(kLsLines). show[i] = LS 线 i 显示; i ≥ 1 时同一图例项也管簇 i (尾部级 i − 1)
struct LayerRange {
  float y_lo, y_hi;
};
LayerRange layer_range(const InspectDerived &d, const bool *show) {
  float lo = d.lay_lo[0], hi = d.lay_hi[0];
  for (int i = 0; i < kLsLines; ++i)
    if (show[i]) {
      lo = std::min(lo, d.ls_lo[i]), hi = std::max(hi, d.ls_hi[i]);
      if (i)
        lo = std::min(lo, d.lay_lo[i]), hi = std::max(hi, d.lay_hi[i]);
    }
  const float margin = (hi > lo ? hi - lo : 1.f) * 0.03f;
  return {lo - margin, hi + margin};
}

// 左上右 1/4: 光标日截面 = 各分层线在光标日的值 (对数累计 × kDaysPerYear / days, 与左图同一量) 的横柱, 在共享 y 范围里均匀排 (G1 下 … G20 上); x 范围 = 显示中的柱 (随图例 / 光标) 与 0 的
// min / max 加边, 目标变才强设 (follow_limits, 之间可手动缩放);
// 柱色按组号 (与左图同一 group_color, 两边对得上; 柱长非单调即分层破绽), 不标组号. 开着的尾部级 (show[1 + j], 同 layer_range) 两腿各加一柱:
// 底腿排 G1 之下, 顶腿排 G20 之上, 越极端越靠外; 色 / 细度同左图尾部分层线 (饱和顶 / 底色, kTailLayerWeight). 须在 plot_layers 的子图内调 (y 由 LinkRows 跟左图)
// hov[i] = 左图图例项 i hover: 该项的柱 (i = 0 → G1 / G20; i ≥ 1 → 尾部级 i − 1 两腿) 描白边高亮
void plot_section(const InspectDerived &d, const LayerRange &yr, const bool *show, const bool *hov, int cursor_day) {
  if (!ImPlot::BeginPlot("##section", ImVec2(), ImPlotFlags_NoMenus | ImPlotFlags_NoLegend))
    return;
  ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_NoTickLabels | ImPlotAxisFlags_NoTickMarks | ImPlotAxisFlags_NoGridLines,
                    ImPlotAxisFlags_NoTickLabels | ImPlotAxisFlags_NoTickMarks);
  struct Slot {
    int line; // lay_ann 下标
    ImVec4 col;
    float w; // 柱宽系数
    bool hl;
    char tag[24];
  };
  Slot s[kLayerLines];
  int ns = 0;
  const auto tail = [&](int l, int j) {
    Slot &t = s[ns++];
    t = Slot{layer_tail(l, j), l == kLong ? kColTop : kColBottom, kTailLayerWeight[j], hov[1 + j], {}};
    std::snprintf(t.tag, sizeof(t.tag), "G%d 尾 %g%%", leg_group(l) + 1, static_cast<double>(kTailOfGroup[j]) * 100.0);
  };
  for (int j = kTails - 1; j >= 0; --j)
    if (show[1 + j])
      tail(kShort, j);
  for (int k = 0; k < K; ++k) {
    Slot &t = s[ns++];
    t = Slot{k, group_color(k), 1.f, hov[0] && is_leg(k), {}};
    std::snprintf(t.tag, sizeof(t.tag), "G%d", k + 1);
  }
  for (int j = 0; j < kTails; ++j)
    if (show[1 + j])
      tail(kLong, j);
  float v[kLayerLines], v_lo = 0.f, v_hi = 0.f; // 含 0 (柱从 0 起)
  for (int i = 0; i < ns; ++i) {
    v[i] = d.lay_ann[s[i].line][static_cast<size_t>(cursor_day)];
    v_lo = std::min(v_lo, v[i]), v_hi = std::max(v_hi, v[i]);
  }
  const float margin = (v_hi > v_lo ? v_hi - v_lo : 1.f) * 0.03f;
  follow_limits(ImAxis_X1, v_lo - margin, v_hi + margin);
  const double zero = 0.0;
  ImPlot::SetNextLineStyle(ImVec4(0.6f, 0.6f, 0.6f, 0.8f), 1.0f);
  ImPlot::PlotInfLines("##zero", &zero, 1);
  const double slot = (yr.y_hi - yr.y_lo) / ns; // y 范围均分 ns 槽
  for (int i = 0; i < ns; ++i) {                // 逐线一柱: PlotBars 一次只能一色
    const double x = v[i], y = yr.y_lo + (i + 0.5) * slot;
    ImPlot::SetNextFillStyle(s[i].col);
    if (s[i].hl)
      ImPlot::SetNextLineStyle(kColLS, kHiScale);
    char id[16];
    std::snprintf(id, sizeof(id), "##b%d", s[i].line);
    ImPlot::PlotBars(id, &x, &y, 1, slot * 0.8 * s[i].w, ImPlotBarsFlags_Horizontal);
  }
  if (ImPlot::IsPlotHovered()) {
    const int i = std::clamp(static_cast<int>(std::floor((ImPlot::GetPlotMousePos().y - yr.y_lo) / slot)), 0, ns - 1);
    ImGui::SetTooltip("%s  %+.3f  (对数累计 %%, 按全段折年, 到光标日)", s[i].tag, v[i]);
  }
  ImPlot::EndPlot();
}

// 左上: 1×2 子图 (LinkRows 共 y 轴, 横向 PlotPadding 置 0 → 两图无缝相接), 刻度单位 % (derive 已 ×100, 标在 y 轴 label)
//   左 3/4 时序: 对数累计 × kDaysPerYear / days (20 组 + 尾部 2 × kTails 条 + 相邻尾部线间色带) + LS 系 (顶底 + kTails 条尾部), 同一 y 轴. y 范围 = layer_range(图例状态):
//   进 BeginPlot 先按 ID 查上帧 item 的 Show (Setup 阶段种子未推, 按 Items.ID 自己哈希; 首帧还没 item → 尾部默认隐藏, LS 显), 再 SetupAxisLimits, 开关图例下一帧即重贴;
//   尾部线越极端越细 (kTailLayerWeight / kTailLsWeight), 色带越极端越重 (kTailFillAlpha), 一级一个图例项管该级分层 + 色带 + LS + 右图截面柱, 默认隐藏
//   光标: DragLineX 锚在交易日 (cursor_day ∈ [0, days], 整数吸附; 双击落点; 默认末日), 不做 hover 列表; 右 1/4 = plot_section(光标日)
void plot_layers(const InspectDerived &d, bool absolute, int &cursor_day, const ImVec2 &size) {
  if (cursor_day < 0 || cursor_day > d.days)
    cursor_day = d.days;
  char title[128];
  std::snprintf(title, sizeof(title), "分层对数累计 %s###ts", absolute ? "(绝对: lv)" : "(超额: lv − mkt)");
  const int n = d.days + 1;
  float col_ratios[2] = {3.f, 1.f};
  LayerRange yr{};
  bool show[kLsLines] = {}, hov[kLsLines] = {};
  ImPlot::PushStyleVar(ImPlotStyleVar_PlotPadding, ImVec2(0.f, ImPlot::GetStyle().PlotPadding.y));
  if (ImPlot::BeginSubplots("##layers", 1, 2, size, ImPlotSubplotFlags_NoTitle | ImPlotSubplotFlags_NoMenus | ImPlotSubplotFlags_NoResize | ImPlotSubplotFlags_LinkRows,
                            nullptr, col_ratios)) {
    ImVec4 legend_bg = ImPlot::GetStyleColorVec4(ImPlotCol_LegendBg); // 图例压在线上: 底色更透
    legend_bg.w *= 0.4f;
    ImPlot::PushStyleColor(ImPlotCol_LegendBg, legend_bg);
    if (ImPlot::BeginPlot(title, ImVec2(), ImPlotFlags_NoMenus)) {
      CompactLegend cl;
      { // 图例状态 → y 范围. item 按 "###id" 查 (与下面 Plot 的 label 同 ID); 不存在 = 首帧, 取默认显隐.
        // ImPlot 在 SetupFinish 才 PushOverrideID(Items.ID) 作 item 哈希种子, 这里还在 Setup 阶段, GetItem(label) 会用错种子 → 直接按同一种子哈希
        // hover = 上帧 EndPlot 记的 LegendHovered (ImPlot 自己 BeginItem 加粗同名线也读它); ImPlot 只管同名 item 的线, 组线 / 色带 / 右图截面柱按 hov 自己高亮
        ImPlotItemGroup &items = *GImPlot->CurrentItems;
        const auto look = [&](const char *id, bool dflt, bool &sh, bool &hv) {
          const ImPlotItem *it = items.GetItem(ImHashStr(id, 0, items.ID));
          sh = it ? it->Show : dflt;
          hv = it && it->Show && it->LegendHovered;
        };
        look("###ls", true, show[0], hov[0]);
        for (int j = 0; j < kTails; ++j) {
          char id[16];
          std::snprintf(id, sizeof(id), "###tls%d", j);
          look(id, false, show[1 + j], hov[1 + j]);
        }
        yr = layer_range(d, show);
      }
      ImPlot::SetupAxes(nullptr, absolute ? "对数年化(%)" : "超额对数年化(%)", ImPlotAxisFlags_None, ImPlotAxisFlags_None);
      ImPlot::SetupAxisFormat(ImAxis_Y1, kYTickFmt);
      ImPlot::SetupAxisFormat(ImAxis_X1, date_formatter, const_cast<std::vector<misc::Ymd> *>(&d.dates));
      follow_limits(ImAxis_X1, 0, std::max(1, d.days) * (1.0 + kCursorPad));
      follow_limits(ImAxis_Y1, yr.y_lo, yr.y_hi);
      ImPlot::SetupLegend(ImPlotLocation_SouthWest, ImPlotLegendFlags_None);
      ImPlot::PushStyleVar(ImPlotStyleVar_LineWeight, 1.2f);
      for (int k = 0; k < K; ++k) { // 顶底组线 = 顶底 LS 的分层: 随 LS 图例项高亮
        const bool hl = hov[0] && is_leg(k);
        ImPlot::SetNextLineStyle(group_color(k), hl ? 1.2f * kHiScale : IMPLOT_AUTO);
        char id[16];
        std::snprintf(id, sizeof(id), "##g%d", k);
        ImPlot::PlotLine(id, d.x_day.data(), d.lay_ann[k].data(), n);
      }
      ImPlot::PopStyleVar();
      // 图例 (对仗 Portfolio, 等宽字体逐列定宽): "LS <tag:4>  对数年化=<+8.2f>%  夏普=<+6.2f>  年换手=<7.1f>倍", 值 = InspectDerived::ls_ann 末点 / ls_sharpe / ls_turn;
      // tag = 占组比例 (顶底 = 100%, 整组)
      constexpr const char *kLsFmt = "LS %4s  对数年化=%+8.2f%%  夏普=%+6.2f  年换手=%7.1f倍###%s";
      char ll[128];
      std::snprintf(ll, sizeof(ll), kLsFmt, "100%", d.ls_ann[0].back(), d.ls_sharpe[0], d.ls_turn[0], "ls");
      ImPlot::SetNextLineStyle(kColLS, 2.5f);
      ImPlot::PlotLine(ll, d.x_day.data(), d.ls_ann[0].data(), n);
      const ImVec4 leg_col[kLegs] = {kColTop, kColBottom};
      for (int j = 0; j < kTails; ++j) { // 尾部: 一级一个图例项 (默认隐藏) 同时管该级的色带 + 两腿分层线 + LS (同名 = 同一 item, 一起显隐; 图例色 = 最后画的 LS 白)
        char tag[8], id[8];
        std::snprintf(tag, sizeof(tag), "%g%%", static_cast<double>(kTailOfGroup[j]) * 100.0);
        std::snprintf(id, sizeof(id), "tls%d", j);
        std::snprintf(ll, sizeof(ll), kLsFmt, tag, d.ls_ann[1 + j].back(), d.ls_sharpe[1 + j], d.ls_turn[1 + j], id);
        ImPlot::HideNextItem(true, ImPlotCond_Once);
        for (int l = 0; l < kLegs; ++l) { // 色带: 本级线与上一级线 (j = 0 时是该腿的组线) 之间, 越极端越重
          const float *inner = (j ? d.lay_ann[layer_tail(l, j - 1)] : d.lay_ann[leg_group(l)]).data();
          ImPlot::SetNextFillStyle(leg_col[l], kTailFillAlpha[j] * (hov[1 + j] ? kHiScale : 1.f));
          ImPlot::PlotShaded(ll, d.x_day.data(), inner, d.lay_ann[layer_tail(l, j)].data(), n);
        }
        for (int l = 0; l < kLegs; ++l) { // 分层: 饱和顶 / 底色, 越极端越细
          ImPlot::SetNextLineStyle(leg_col[l], kTailLayerWeight[j]);
          ImPlot::PlotLine(ll, d.x_day.data(), d.lay_ann[layer_tail(l, j)].data(), n);
        }
        ImPlot::SetNextLineStyle(kColLS, kTailLsWeight[j]); // LS: 同色, 越极端越细
        ImPlot::PlotLine(ll, d.x_day.data(), d.ls_ann[1 + j].data(), n);
      }
      { // 光标 (对仗 OrderFlow 的锚): 拖动跟手并吸附整日, 双击落点; 标注光标日期 + LS 线在该日的值
        double cx = cursor_day;
        if (ImPlot::DragLineX(0, &cx, kColCursor, 2.f))
          cursor_day = std::clamp(static_cast<int>(std::lround(cx)), 0, d.days);
        else if (ImPlot::IsPlotHovered() && ImGui::IsMouseDoubleClicked(0))
          cursor_day = std::clamp(static_cast<int>(std::lround(ImPlot::GetPlotMousePos().x)), 0, d.days);
        const float ls_c = d.ls_ann[0][static_cast<size_t>(cursor_day)];
        ImPlot::Annotation(static_cast<double>(cursor_day), ls_c, kColCursor, ImVec2(5, -15), true, "%s  LS %+.2f",
                           cursor_day ? misc::dense(d.dates[static_cast<size_t>(cursor_day) - 1]).c_str() : "起点", ls_c);
      }
      ImPlot::EndPlot();
    }
    ImPlot::PopStyleColor();
    plot_section(d, yr, show, hov, cursor_day);
    ImPlot::EndSubplots();
  }
  ImPlot::PopStyleVar();
}

// ---------------------------------------------------------------------------------------------------------------------------------
// 右上: 资产轴 + 选中资产的分钟 K 线 (因子具体怎么交易的)
// ---------------------------------------------------------------------------------------------------------------------------------

// r16 → R ∈ [0,1] (与组号同向: 1 = G20 侧); 无效 → −1
float r_of(uint16_t r16) {
  if (r16 == factor::stat::kRankNone)
    return -1.f;
  return static_cast<float>(r16) / static_cast<float>(factor::stat::kRankMax);
}
// R → 组色; 池外 (pooled = false, 秩来自 rx_all) 半透明淡化; 无值 → 暗灰 (比中段灰暗, 分得开)
ImU32 r_color(float r, bool pooled = true) {
  if (r < 0.f)
    return IM_COL32(70, 70, 70, 255);
  ImVec4 c = lerp_color(r);
  c.w = pooled ? 1.f : 0.45f;
  return ImGui::ColorConvertFloat4ToU32(c);
}
int r_group(float r) { return factor::stat::grp_of(static_cast<unsigned>(std::lround(static_cast<double>(r) * factor::stat::kRankMax))); }

// "代码-市场 名称"; 全局 id 不在资产轴内 → "#id"
void asset_label(const FactorsUIContext &ctx, uint32_t gid, char *buf, size_t n) {
  if (ctx.assets && gid < ctx.assets->size()) {
    const AssetItem &it = (*ctx.assets)[gid];
    std::snprintf(buf, n, "%s-%s %s", it.asset_code.c_str(), it.exchange.c_str(), it.asset_name.c_str());
  } else {
    std::snprintf(buf, n, "#%u", gid);
  }
}

// 资产轴派生: 光标日 (day = 交易日下标) 各资产的日内均 R (有效刻平均) → 降序 (左 = G20 侧; 并列按池轴序); 当日无一有效刻的资产不列
void derive_axis(const InspectResult &res, int day, std::vector<int> &order, std::vector<uint32_t> &gid, std::vector<float> &r) {
  const int A = res.scope.A, S = factor_level(res.lv).seg;
  assert(day >= 0 && day < res.scope.days && res.pl.rx.size() == static_cast<size_t>(res.scope.T) * A);
  std::vector<float> mean(static_cast<size_t>(A), 0.f);
  std::vector<int> cnt(static_cast<size_t>(A), 0);
  for (int t = day * S; t < (day + 1) * S; ++t) {
    const uint16_t *row = res.pl.rx.data() + static_cast<size_t>(t) * A;
    for (int a = 0; a < A; ++a) {
      const float v = r_of(row[a]);
      mean[static_cast<size_t>(a)] += v < 0.f ? 0.f : v;
      cnt[static_cast<size_t>(a)] += v >= 0.f;
    }
  }
  order.clear();
  for (int a = 0; a < A; ++a)
    if (cnt[static_cast<size_t>(a)]) {
      mean[static_cast<size_t>(a)] /= static_cast<float>(cnt[static_cast<size_t>(a)]);
      order.push_back(a);
    }
  std::stable_sort(order.begin(), order.end(), [&](int x, int y) { return mean[static_cast<size_t>(x)] > mean[static_cast<size_t>(y)]; });
  gid.resize(order.size());
  r.resize(order.size());
  for (size_t i = 0; i < order.size(); ++i) {
    gid[i] = res.read_scope.axis.global(static_cast<size_t>(order[i]));
    r[i] = mean[static_cast<size_t>(order[i])];
  }
}

// 选中资产 (池轴 sub) 的 R(t) [T]: col = 池内秩 (rx), col_all = 不看池的秩 (rx_all; 池外段靠它显示)
void derive_col(const InspectResult &res, int sub, std::vector<float> &col, std::vector<float> &col_all) {
  const int A = res.scope.A, T = res.scope.T;
  assert(sub >= 0 && sub < A && res.pl.rx.size() == static_cast<size_t>(T) * A && res.pl.rx_all.size() == res.pl.rx.size());
  col.resize(static_cast<size_t>(T));
  col_all.resize(static_cast<size_t>(T));
  for (int t = 0; t < T; ++t) {
    const size_t i = static_cast<size_t>(t) * A + static_cast<size_t>(sub);
    col[static_cast<size_t>(t)] = r_of(res.pl.rx[i]);
    col_all[static_cast<size_t>(t)] = r_of(res.pl.rx_all[i]);
  }
}

// 默认资产的兜底: 光标日无池内资产时, 取全区间池内有效刻最多的那个 (-1 = 整个结果无一有效格)
int fallback_asset(const InspectResult &res) {
  const int A = res.scope.A, T = res.scope.T;
  std::vector<int> cnt(static_cast<size_t>(A), 0);
  for (int t = 0; t < T; ++t) {
    const uint16_t *row = res.pl.rx.data() + static_cast<size_t>(t) * A;
    for (int a = 0; a < A; ++a)
      cnt[static_cast<size_t>(a)] += row[a] != factor::stat::kRankNone;
  }
  const auto it = std::max_element(cnt.begin(), cnt.end());
  return *it > 0 ? static_cast<int>(it - cnt.begin()) : -1;
}

// 资产轴 (右上的头行条, 单行高): 一资产一格 x ∈ [i, i+1), 格色 = R 组色 (不足 1px 补到 1px); 定死全部资产, 不缩放 / 平移 / 框选.
// 光标 = 选中格中线 (pos < 0 = 选中资产当日无值, 不画); 按下即选中所指格, 按住拖动连续换 (ui.ax_scrub, 出条也跟随); 悬停报该格资产.
// 返回 pos 是否变 (调用方按 ax_gid[pos] 换资产)
bool plot_asset_axis(InspectUIState &ui, const FactorsUIContext &ctx, int &pos, const ImVec2 &size) {
  const int n = static_cast<int>(ui.ax_order.size());
  bool changed = false;
  ImPlot::PushStyleVar(ImPlotStyleVar_PlotPadding, ImVec2(0.f, 0.f)); // SetupFinish 才读, 包到 EndPlot
  if (!ImPlot::BeginPlot("##assets", size, ImPlotFlags_NoLegend | ImPlotFlags_NoMenus | ImPlotFlags_NoMouseText | ImPlotFlags_NoBoxSelect)) {
    ImPlot::PopStyleVar();
    return false;
  }
  ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_NoDecorations | ImPlotAxisFlags_Lock, ImPlotAxisFlags_NoDecorations | ImPlotAxisFlags_Lock);
  ImPlot::SetupAxisLimits(ImAxis_X1, 0, std::max(1, n), ImPlotCond_Always);
  ImPlot::SetupAxisLimits(ImAxis_Y1, 0, 1, ImPlotCond_Always);
  ImPlot::SetupFinish();
  {
    ImPlot::PushPlotClipRect();
    ImDrawList *dl = ImPlot::GetPlotDrawList();
    const ImPlotRect lim = ImPlot::GetPlotLimits();
    const int i0 = std::clamp(static_cast<int>(std::floor(lim.X.Min)), 0, n), i1 = std::clamp(static_cast<int>(std::ceil(lim.X.Max)), 0, n);
    const float y0 = ImPlot::PlotToPixels(0.0, 1.0).y, y1 = ImPlot::PlotToPixels(0.0, 0.0).y;
    for (int i = i0; i < i1; ++i) {
      const float xa = ImPlot::PlotToPixels(static_cast<double>(i), 0.0).x, xb = ImPlot::PlotToPixels(static_cast<double>(i) + 1.0, 0.0).x;
      dl->AddRectFilled(ImVec2(xa, y0), ImVec2(std::max(xb, xa + 1.f), y1), r_color(ui.ax_r[static_cast<size_t>(i)]));
    }
    ImPlot::PopPlotClipRect();
  }
  if (pos >= 0 && pos < n) {
    double cx = pos + 0.5;
    ImPlot::DragLineX(0, &cx, kColCursor, 2.f, ImPlotDragToolFlags_NoFit | ImPlotDragToolFlags_NoInputs);
  }
  if (n > 0 && ImPlot::IsPlotHovered()) {
    if (ImGui::IsMouseClicked(0))
      ui.ax_scrub = true;
    const int i = std::clamp(static_cast<int>(std::floor(ImPlot::GetPlotMousePos().x)), 0, n - 1);
    char name[96];
    asset_label(ctx, ui.ax_gid[static_cast<size_t>(i)], name, sizeof(name));
    ImGui::SetTooltip("#%d/%d  %s  R %.3f (G%d)\n按下选中, 按住拖动连续换", i + 1, n, name, ui.ax_r[static_cast<size_t>(i)],
                      r_group(ui.ax_r[static_cast<size_t>(i)]) + 1);
  }
  if (!ImGui::IsMouseDown(0))
    ui.ax_scrub = false;
  if (n > 0 && ui.ax_scrub) { // 按住期间跟随鼠标 (出条也跟, 夹到两端)
    const int i = std::clamp(static_cast<int>(std::floor(ImPlot::GetPlotMousePos().x)), 0, n - 1);
    if (i != pos) {
      changed = true;
      pos = i;
    }
  }
  ImPlot::EndPlot();
  ImPlot::PopStyleVar();
  return changed;
}

// K 线横轴: 全局行 (日下标 × seg + 行) → "YYMMDD HH:MM" (行末分钟)
struct KlineAxis {
  const std::vector<misc::Ymd> *dates = nullptr;
  int seg = 0;
};
int minute_formatter(double v, char *buf, int size, void *ud) {
  const KlineAxis &ax = *static_cast<const KlineAxis *>(ud);
  const std::vector<misc::Ymd> &dates = *ax.dates;
  if (dates.empty())
    return std::snprintf(buf, size, "%g", v);
  const int S = ax.seg, days = static_cast<int>(dates.size());
  const long long g = std::clamp(static_cast<long long>(std::floor(v)), 0LL, static_cast<long long>(days) * S - 1);
  const int d = static_cast<int>(g / S), m = factor::geo_of(S).minute(static_cast<int>(g % S));
  const ClockTime ct = L_1min_to_Clock(static_cast<size_t>(m));
  return std::snprintf(buf, size, "%06d %02d:%02d", dates[static_cast<size_t>(d)] % 1000000, ct.hour, ct.minute);
}

// 滚动重锚指数线: line[t] = EMA_L(P)_{t−L} · exp(EMA_S(log(I_t / EMA_L(I)_{t−L}))). P = 选中资产 close 前值填充到逐行格 (无成交行持平), I = 池等权指数
// 等权指数 [T]. 基准取 L 分钟前的 EMA_L: 当前价不进基准 (近期偏移不被吸收), 基准本身平滑 (过去的高频波动不干扰当前); 指数偏移再 EMA_S 压指数自身
// 的分钟噪声 (个股侧 EMA_L(P) 已平滑, 不另压). 线 = "个股若跟着指数近期走应在的价", 自发贴住蜡烛, 蜡烛 − 线 = 个股相对指数的近期偏移.
// 跨日连续算 (EMA 不按日重置); pandas ewm(span, adjust=False) 口径: α = 2/(span+1), 首个样本起算. 只用已发布前缀 [0, pub_pts), 个股在
// [首根有效蜡烛, pub_days·S) 有值; 线定义域 [t0, t1) = [首根 + L, pub_days·S) (基准要 L 分钟前有值). 全量重算 (T ≤ 几万, 微秒级)
// L = 1 交易日 = seg 行
constexpr int kRefSmooth = 10; // S (行)
void derive_ref(const KlineStream &ks, int seg_rows, size_t pub_days, size_t pub_pts, const std::vector<float> &pool_idx, int &t0, int &t1, std::vector<float> &line) {
  const size_t T = pool_idx.size(), seg = static_cast<size_t>(seg_rows), L = seg;
  assert(pub_days * seg <= T); // 只按 pub word 读前缀, 不碰 vector 元数据 (worker 在 push_back)
  line.assign(T, 0.f);
  t0 = t1 = 0;
  if (pub_pts == 0)
    return;
  const size_t tf = static_cast<size_t>(ks.x[0]), te = pub_days * seg;
  assert(tf < te);
  if (tf + L >= te)
    return; // 已发布前缀还不够 L 分钟
  t0 = static_cast<int>(tf + L), t1 = static_cast<int>(te);
  const double aL = 2.0 / (static_cast<double>(L) + 1), aS = 2.0 / (kRefSmooth + 1);
  std::vector<double> ema_i(te), ema_p(te);
  ema_i[0] = static_cast<double>(pool_idx[0]);
  for (size_t t = 1; t < te; ++t) {
    assert(pool_idx[t] > 0.f);
    ema_i[t] = aL * pool_idx[t] + (1.0 - aL) * ema_i[t - 1];
  }
  size_t k = 0;
  double p = ks.close[0];
  for (size_t t = tf; t < te; ++t) { // 个股 close 前值填充 + EMA_L
    while (k < pub_pts && static_cast<size_t>(ks.x[k]) <= t)
      p = ks.close[k++];
    ema_p[t] = t == tf ? p : aL * p + (1.0 - aL) * ema_p[t - 1];
  }
  double sd = 0.0;
  for (size_t t = L; t < te; ++t) { // 指数偏移从 t = L 起平滑 (不依赖个股), 线从 t0 起
    const double dev = std::log(static_cast<double>(pool_idx[t]) / ema_i[t - L]);
    sd = t == L ? dev : aS * dev + (1.0 - aS) * sd;
    if (t >= tf + L)
      line[t] = static_cast<float>(ema_p[t - L] * std::exp(sd));
  }
}

// K 线 (右上的下图): 选中资产沿全部交易日的分钟 OHLC, 画 KlineService 已发布前缀 (gen 配对; 换代在途照画空图, 轴不撤). 每根色 = 该刻 R(t)
// 组色 (ui.col_r; 灰 = 无值), 阴线实心 / 阳线空心, 自绘 (ImPlot 一个 item 只能一色).
// 视野: x 域 = 日数 × seg (全段历史, 不按个股上市区间), 换代 (换资产 / 换结果) 即复位到全段, 双击 fit 同样贴全段; y 在换代 / 前缀增长 / 图例开关 / 换 Hold
// (池指数换线) 时按**当前 x 窗口内**的已发布蜡烛 + 显示中的指数线重定 (上帧视野 ui.kline_x_lo/hi; 窗口内还没流到就下帧再试), 之后不打扰.
// 光标 = 与左上图共享的 cursor_day (锚在交易日边界 x = day × seg): 同色 DragLineX 吸附整日, 双击落点, 拖哪边另一边同步; 光标日 [day−1, day]
// 染淡色竖带. 叠加 池等权指数 pool_idx [T] (x = t, 与蜡烛同格): 每帧按视野内个股首根蜡烛重锚; 再叠加滚动重锚指数线 (derive_ref, 键变才重算).
// 两条指数线不参与双击 fit (NoFit), 只在显示时进上面的 y 贴合. 累计 rIC ic_cum [T] 走右轴 Y2 (x = t), 右轴每帧贴合上帧 x 窗口内这段.
// 悬停报最近一根的时刻 / OHLC / R / 两条指数线 / 个股相对滚动重锚线的偏移 / 累计 rIC
void plot_kline(InspectUIState &ui, const KlineStream &ks, const InspectDerived &d, int &cursor_day, const char *title, const ImVec2 &size) {
  const std::vector<misc::Ymd> &dates = d.dates;
  const std::vector<float> &pool_idx = d.pool_index, &ic_cum = d.ic_cum;
  uint32_t pub_gen;
  size_t pub_days = 0, pub_pts = 0;
  KlineStream::unpack(ks.pub.load(std::memory_order_acquire), pub_gen, pub_days, pub_pts);
  const bool ready = pub_gen == (ui.kline_gen & 0xFFFF);
  if (!ready)
    pub_days = pub_pts = 0; // 旧代计数不可用 (数组正在重建)
  const int S = ui.kline_seg, days = ui.kline_days;
  assert(static_cast<size_t>(days) == dates.size() && "K 线代与派生日期表不同源");
  const KlineAxis ax{&dates, S};
  assert(pool_idx.size() == static_cast<size_t>(days) * S && ic_cum.size() == pool_idx.size());
  const double x_dom = std::max(1, days) * static_cast<double>(S) * (1.0 + kCursorPad);
  if (!ImPlot::BeginPlot(title, size, ImPlotFlags_NoMenus))
    return;
  CompactLegend cl;
  ImPlot::SetupLegend(ImPlotLocation_NorthWest, ImPlotLegendFlags_None);
  ImPlot::SetupAxes(nullptr, "价格", ImPlotAxisFlags_None, ImPlotAxisFlags_None);
  ImPlot::SetupAxisFormat(ImAxis_X1, minute_formatter, const_cast<KlineAxis *>(&ax));
  ImPlot::SetupAxisFormat(ImAxis_Y1, kYTickFmt);
  if (ui.kline_gen_applied != ui.kline_gen) { // 换代 (换资产 / 换结果) → x 复位到全段历史, y 随新代前缀按全段重贴
    ui.kline_gen_applied = ui.kline_gen;
    ui.kline_x_lo = 0.0, ui.kline_x_hi = x_dom;
    ImPlot::SetupAxisLimits(ImAxis_X1, 0, x_dom, ImPlotCond_Always);
  } else {
    ImPlot::SetupAxisLimits(ImAxis_X1, 0, x_dom, ImPlotCond_Once);
  }
  ImPlot::SetupAxis(ImAxis_Y2, "累计 rIC", ImPlotAxisFlags_AuxDefault);
  ImPlot::SetupAxisFormat(ImAxis_Y2, kYTickFmt);
  {
    const int T = static_cast<int>(ic_cum.size());
    const int ta = std::clamp(static_cast<int>(std::floor(ui.kline_x_lo)), 0, T - 1), tb = std::clamp(static_cast<int>(std::ceil(ui.kline_x_hi)) + 1, ta + 1, T);
    const auto [lo, hi] = std::minmax_element(ic_cum.begin() + ta, ic_cum.begin() + tb);
    const double m = (*hi > *lo ? *hi - *lo : 1.0) * 0.05;
    ImPlot::SetupAxisLimits(ImAxis_Y2, *lo - m, *hi + m, ImPlotCond_Always);
  }
  const double *xs = ks.x.data();
  const int n = static_cast<int>(pub_pts);
  constexpr const char *kIdxName = "池等权指数", *kRefName = "指数 滚动重锚 (1d)", *kIcName = "累计 rIC";
  uint8_t shown = 0; // 上帧图例显隐 (bit0 池等权指数 / bit1 滚动重锚); Setup 阶段按 Items.ID 种子自己哈希 (同 plot_layers), 首帧取默认
  {
    ImPlotItemGroup &items = *GImPlot->CurrentItems;
    const auto on = [&](const char *id, bool dflt) {
      const ImPlotItem *it = items.GetItem(ImHashStr(id, 0, items.ID));
      return it ? it->Show : dflt;
    };
    shown = static_cast<uint8_t>((on(kIdxName, false) ? 1 : 0) | (on(kRefName, true) ? 2 : 0));
  }
  if (n > 0 && (ui.kline_last_pub_days != pub_days || ui.kline_shown != shown || ui.kline_sel != ui.derived_sel)) { // y 贴合当前 x 窗口内的已发布蜡烛 + 显示中的指数线
    const int w0 = static_cast<int>(std::lower_bound(xs, xs + n, ui.kline_x_lo) - xs), w1 = static_cast<int>(std::upper_bound(xs, xs + n, ui.kline_x_hi) - xs);
    if (w1 > w0) {
      double lo = ks.low[static_cast<size_t>(w0)], hi = ks.high[static_cast<size_t>(w0)];
      for (int i = w0 + 1; i < w1; ++i)
        lo = std::min(lo, ks.low[static_cast<size_t>(i)]), hi = std::max(hi, ks.high[static_cast<size_t>(i)]);
      const int T = static_cast<int>(pool_idx.size());
      const int ta = std::clamp(static_cast<int>(std::floor(ui.kline_x_lo)), 0, T), tb = std::clamp(static_cast<int>(std::ceil(ui.kline_x_hi)) + 1, ta, T);
      if (shown & 1) { // 与下面画线同一重锚: 窗口内首根蜡烛
        const double k = ks.close[static_cast<size_t>(w0)] / static_cast<double>(pool_idx[static_cast<size_t>(xs[w0])]);
        for (int t = ta; t < tb; ++t)
          lo = std::min(lo, pool_idx[static_cast<size_t>(t)] * k), hi = std::max(hi, pool_idx[static_cast<size_t>(t)] * k);
      }
      if ((shown & 2) && ready && ui.ref_gen == ui.kline_gen) // 上帧算好的线 (前缀增长时稍旧, 下次增长再贴)
        for (int t = std::max(ta, ui.ref_t0); t < std::min(tb, ui.ref_t1); ++t)
          lo = std::min(lo, static_cast<double>(ui.ref_line[static_cast<size_t>(t)])), hi = std::max(hi, static_cast<double>(ui.ref_line[static_cast<size_t>(t)]));
      const double m = (hi > lo ? hi - lo : 1.0) * 0.05;
      ImPlot::SetupAxisLimits(ImAxis_Y1, lo - m, hi + m, ImPlotCond_Always);
      ui.kline_last_pub_days = pub_days;
      ui.kline_shown = shown;
      if (!(shown & 2) || ui.ref_sel == ui.derived_sel) // 换 Hold 的那帧滚动重锚线还是旧的 (本帧稍后重算) → 下帧再贴一次
        ui.kline_sel = ui.derived_sel;
    }
  }
  ImPlot::SetupFinish();
  ImDrawList *dl = ImPlot::GetPlotDrawList();
  {
    const ImPlotRect lim = ImPlot::GetPlotLimits();
    ui.kline_x_lo = lim.X.Min, ui.kline_x_hi = lim.X.Max; // 本帧视野 → 下次 y 贴合用
    if (cursor_day >= 1 && cursor_day <= days) {          // 光标日竖带 (先画, 不遮蜡烛)
      ImPlot::PushPlotClipRect();
      const ImVec2 p0 = ImPlot::PlotToPixels(static_cast<double>(cursor_day - 1) * S, lim.Y.Max), p1 = ImPlot::PlotToPixels(static_cast<double>(cursor_day) * S, lim.Y.Min);
      dl->AddRectFilled(p0, p1, IM_COL32(255, 128, 0, 28));
      ImPlot::PopPlotClipRect();
    }
  }
  // 图例三项每帧都提交 (无数据 = 空线): 换代在途 / 视野内无蜡烛时图例不缺项, 拖资产条连续换资产时不闪
  double uk = 0.0; // 指数重锚系数 (价格 / 指数点); 0 = 视野内无蜡烛, 画空线
  {
    struct Rebased {
      const float *v;
      long long t0;
      double k;
    };
    const ImPlotRect lim = ImPlot::GetPlotLimits();
    const long long t0 = std::clamp(static_cast<long long>(std::floor(lim.X.Min)), 0LL, static_cast<long long>(pool_idx.size()));
    long long t1 = t0;
    const int ia = static_cast<int>(std::lower_bound(xs, xs + n, lim.X.Min) - xs);
    if (ia < n && xs[ia] <= lim.X.Max) {
      uk = ks.close[static_cast<size_t>(ia)] / static_cast<double>(pool_idx[static_cast<size_t>(xs[ia])]);
      t1 = std::clamp(static_cast<long long>(std::ceil(lim.X.Max)) + 1, t0, static_cast<long long>(pool_idx.size()));
    }
    Rebased rb{pool_idx.data(), t0, uk};
    const auto rebased = [](int i, void *ud) {
      const Rebased &r = *static_cast<const Rebased *>(ud);
      const long long t = r.t0 + i;
      return ImPlotPoint(static_cast<double>(t), r.v[t] * r.k);
    };
    ImPlot::SetNextLineStyle(kColIndex, 1.5f);
    ImPlot::HideNextItem(true, ImPlotCond_Once); // 默认隐藏 (带全区间历史), 点图例显; 重锚系数 uk 照算 (悬停仍报)
    ImPlot::PlotLineG(kIdxName, rebased, &rb, static_cast<int>(t1 - t0), ImPlotItemFlags_NoFit);
  }
  // 滚动重锚指数线: 键 (代 / 已发布点数 / 派生 epoch / Hold) 变才重算; 换代在途 (!ready) 不算, 旧线也不画 (pool_idx 已换)
  if (ready && (ui.ref_gen != ui.kline_gen || ui.ref_pts != pub_pts || ui.ref_epoch != ui.derived_epoch || ui.ref_sel != ui.derived_sel)) {
    ui.ref_gen = ui.kline_gen, ui.ref_pts = pub_pts, ui.ref_epoch = ui.derived_epoch, ui.ref_sel = ui.derived_sel;
    derive_ref(ks, S, pub_days, pub_pts, pool_idx, ui.ref_t0, ui.ref_t1, ui.ref_line);
  }
  const bool ref_ok = ready && ui.ref_gen == ui.kline_gen && ui.ref_t1 > ui.ref_t0;
  {
    int ta = 0, tb = 0; // 无线 → 空线
    if (ref_ok) {
      const ImPlotRect lim = ImPlot::GetPlotLimits();
      ta = std::clamp(static_cast<int>(std::floor(lim.X.Min)), ui.ref_t0, ui.ref_t1);
      tb = std::clamp(static_cast<int>(std::ceil(lim.X.Max)) + 1, ta, ui.ref_t1);
    }
    ImPlot::SetNextLineStyle(kColRef, 1.5f);
    ImPlot::PlotLine(kRefName, ui.ref_line.data() + ta, tb - ta, 1.0, static_cast<double>(ta), ImPlotItemFlags_NoFit);
  }
  ImPlot::SetAxes(ImAxis_X1, ImAxis_Y2);
  ImPlot::SetNextLineStyle(kColIcHigh, 1.5f);
  if (ImPlot::BeginItem(kIcName, ImPlotItemFlags_NoFit, ImPlotCol_Line)) { // 逐段着色自绘: 新高蓝 → 回撤越深越红 (√ 归一, 连续渐变); 视野内每像素至多 2 段
    const ImPlotRect lim = ImPlot::GetPlotLimits();
    const int T = static_cast<int>(ic_cum.size());
    const int ta = std::clamp(static_cast<int>(std::floor(lim.X.Min)), 0, T - 1), tb = std::clamp(static_cast<int>(std::ceil(lim.X.Max)) + 1, ta + 1, T);
    const int step = std::max(1, static_cast<int>(static_cast<float>(tb - ta) / std::max(1.f, 2.f * ImPlot::GetPlotSize().x)));
    const float wt = ImPlot::GetItemData().LineWeight;
    ImDrawList *idl = ImPlot::GetPlotDrawList();
    ImVec2 p0 = ImPlot::PlotToPixels(ta, ic_cum[static_cast<size_t>(ta)]);
    for (int t = ta; t + 1 < tb;) {
      t = std::min(t + step, tb - 1);
      const ImVec2 p1 = ImPlot::PlotToPixels(t, ic_cum[static_cast<size_t>(t)]);
      const float w = d.ic_dd_max > 0.f ? std::sqrt(d.ic_dd[static_cast<size_t>(t)] / d.ic_dd_max) : 0.f;
      const ImVec4 c(kColIcHigh.x + (kColIcDd.x - kColIcHigh.x) * w, kColIcHigh.y + (kColIcDd.y - kColIcHigh.y) * w,
                     kColIcHigh.z + (kColIcDd.z - kColIcHigh.z) * w, kColIcHigh.w);
      idl->AddLine(p0, p1, ImGui::ColorConvertFloat4ToU32(c), wt);
      p0 = p1;
    }
    ImPlot::EndItem();
  }
  ImPlot::SetAxes(ImAxis_X1, ImAxis_Y1);
  if (n > 0 && ImPlot::BeginItem("##ohlc", ImPlotItemFlags_NoLegend)) { // 指数线之后画, 蜡烛压在指数线上
    if (ImPlot::FitThisFrame()) {                                       // x 贴全段历史 (不是个股上市区间), y 贴全部蜡烛
      ImPlot::FitPointX(0.0);
      ImPlot::FitPointX(x_dom);
      for (int i = 0; i < n; ++i) {
        ImPlot::FitPoint(ImPlotPoint(xs[i], ks.low[static_cast<size_t>(i)]));
        ImPlot::FitPoint(ImPlotPoint(xs[i], ks.high[static_cast<size_t>(i)]));
      }
    }
    const ImPlotRect lim = ImPlot::GetPlotLimits();
    const int i0 = static_cast<int>(std::lower_bound(xs, xs + n, lim.X.Min - 1.0) - xs), i1 = static_cast<int>(std::upper_bound(xs, xs + n, lim.X.Max + 1.0) - xs);
    for (int i = i0; i < i1; ++i) {
      const size_t u = static_cast<size_t>(i), t = static_cast<size_t>(xs[i]);
      const double o = ks.open[u], h = ks.high[u], l = ks.low[u], c = ks.close[u];
      const float rp = t < ui.col_r.size() ? ui.col_r[t] : -1.f; // 池内秩优先, 池外段用不看池的秩 (淡)
      const ImU32 col = rp >= 0.f ? r_color(rp) : r_color(t < ui.col_r_all.size() ? ui.col_r_all[t] : -1.f, false);
      dl->AddLine(ImPlot::PlotToPixels(xs[i], l), ImPlot::PlotToPixels(xs[i], h), col);
      ImVec2 a = ImPlot::PlotToPixels(xs[i] - 0.5, o), b = ImPlot::PlotToPixels(xs[i] + 0.5, c);
      if (std::abs(b.y - a.y) < 1.f) { // 身体至少 1px
        const float mid = (a.y + b.y) * 0.5f;
        a.y = mid - 0.5f, b.y = mid + 0.5f;
      }
      if (c >= o)
        dl->AddRect(a, b, col); // 阳线空心
      else
        dl->AddRectFilled(a, b, col); // 阴线实心
    }
    ImPlot::EndItem();
  }
  { // 共享光标 (与左上图同一个 cursor_day, 同色同交互): 锚在日边界, 拖动吸附整日, 双击落点; x 轴标光标日期
    double cx = static_cast<double>(cursor_day) * S;
    if (ImPlot::DragLineX(0, &cx, kColCursor, 2.f, ImPlotDragToolFlags_NoFit))
      cursor_day = std::clamp(static_cast<int>(std::lround(cx / S)), 0, days);
    else if (ImPlot::IsPlotHovered() && ImGui::IsMouseDoubleClicked(0))
      cursor_day = std::clamp(static_cast<int>(std::lround(ImPlot::GetPlotMousePos().x / S)), 0, days);
    ImPlot::TagX(static_cast<double>(cursor_day) * S, kColCursor, "%s", cursor_day ? misc::dense(dates[static_cast<size_t>(cursor_day) - 1]).c_str() : "起点");
  }
  if (ImPlot::IsPlotHovered()) {
    const ImPlotPoint mp = ImPlot::GetPlotMousePos();
    if (n > 0) { // 最近一根
      int i = static_cast<int>(std::lower_bound(xs, xs + n, mp.x) - xs);
      if (i > 0 && (i == n || xs[i] - mp.x > mp.x - xs[i - 1]))
        --i;
      const size_t u = static_cast<size_t>(i), t = static_cast<size_t>(xs[i]);
      const float rp = t < ui.col_r.size() ? ui.col_r[t] : -1.f, ra = t < ui.col_r_all.size() ? ui.col_r_all[t] : -1.f;
      char when[32], ux[224] = "";
      minute_formatter(xs[i], when, sizeof(when), const_cast<KlineAxis *>(&ax));
      int ul = 0;
      if (uk > 0.0)
        ul += std::snprintf(ux + ul, sizeof(ux) - static_cast<size_t>(ul), "\n池等权指数 %.2f (按视野首根重锚)", pool_idx[t] * uk);
      if (ref_ok && static_cast<int>(t) >= ui.ref_t0 && static_cast<int>(t) < ui.ref_t1) {
        const double ref = ui.ref_line[t];
        ul += std::snprintf(ux + ul, sizeof(ux) - static_cast<size_t>(ul), "\n青线 %.2f  相对偏移 %+.2f%%", ref, (ks.close[u] / ref - 1.0) * 100.0);
      }
      std::snprintf(ux + ul, sizeof(ux) - static_cast<size_t>(ul), "\n累计 rIC %.2f  回撤 %.2f", ic_cum[t], d.ic_dd[t]);
      if (rp >= 0.f)
        ImGui::SetTooltip("%s  O %.2f H %.2f L %.2f C %.2f\nR %.3f (G%d)%s", when, ks.open[u], ks.high[u], ks.low[u], ks.close[u], rp, r_group(rp) + 1, ux);
      else if (ra >= 0.f)
        ImGui::SetTooltip("%s  O %.2f H %.2f L %.2f C %.2f\n池外: R %.3f (G%d)%s", when, ks.open[u], ks.high[u], ks.low[u], ks.close[u], ra, r_group(ra) + 1,
                          ux); // 出 CS 池 / 当日无成交: 不看池的秩
      else
        ImGui::SetTooltip("%s  O %.2f H %.2f L %.2f C %.2f\nR 无值%s", when, ks.open[u], ks.high[u], ks.low[u], ks.close[u], ux);
    }
  }
  if (!ready || static_cast<int>(pub_days) < days) { // 流式进度 (图内右上角; 左上角是图例)
    char buf[48];
    std::snprintf(buf, sizeof(buf), "K 线 %zu/%d 天", ready ? pub_days : size_t{0}, days);
    const ImVec2 p = ImPlot::GetPlotPos();
    const float w = ImPlot::GetPlotSize().x - ImGui::CalcTextSize(buf).x - 6.f;
    dl->AddText(ImVec2(p.x + w, p.y + 4.f), ImGui::ColorConvertFloat4ToU32(StatusColor(TaskStatus::Kind::Busy)), buf);
  }
  ImPlot::EndPlot();
}

// PDF 的光滑类高斯重拟合 (惩罚对数密度): 等距格上 min_z Σ w_i (ln f_i − z_i)² + λ Σ (Δ³z)², w_i = f_i (密度加权; f = 0 的格不进数据项),
// g = exp(z), 按 f 的面积归一. Δ³ 的零空间 = 二次多项式 → λ → ∞ 即高斯 (对数二次); 有限 λ 保留偏度 / 峰度, 只抹锯齿.
// λ 自适应分布尺度: 等效平滑宽 ≈ (λ / f)^(1/6) 格, 峰处取 kFitWidthSd · sd → λ = f_pk · (kFitWidthSd · sd / dx)^6 (与单位 / 格距无关).
// 法方程 diag(w) + λ DᵀD 是半带宽 3 的对称正定带状阵; double (大 λ 时 float 条件数不够) 带状 Cholesky, O(n)
constexpr double kFitWidthSd = 0.35;
void smooth_fit(const analysis::AggPdf &p, std::vector<float> &g) {
  constexpr int P = 3;
  constexpr double c[P + 1] = {-1.0, 3.0, -3.0, 1.0};
  const int n = static_cast<int>(p.n_pts);
  const float *f = p.y.data();
  assert(n > P);
  const double dx = static_cast<double>(p.x[1]) - p.x[0], f_pk = *std::max_element(f, f + n), sd = p.sd();
  assert(dx > 0.0 && f_pk > 0.0 && sd > 0.0);
  const double lambda = f_pk * std::pow(kFitWidthSd * sd / dx, 6.0);
  static std::vector<std::array<double, P + 1>> L; // L[i][k] = A(i, i − k) → 原地分解为 L(i, i − k)
  static std::vector<double> z;
  L.assign(static_cast<size_t>(n), {});
  z.assign(static_cast<size_t>(n), 0.0);
  double area = 0.0;
  for (int i = 0; i < n; ++i) {
    const double w = f[i];
    L[static_cast<size_t>(i)][0] = w;
    z[static_cast<size_t>(i)] = w > 0.0 ? w * std::log(w) : 0.0;
    area += w;
  }
  for (int r = 0; r + P < n; ++r) // D 第 r 行 = c 落在列 r..r+P: (DᵀD)(r+a, r+e) += c_a c_e
    for (int a = 0; a <= P; ++a)
      for (int e = 0; e <= a; ++e)
        L[static_cast<size_t>(r + a)][static_cast<size_t>(a - e)] += lambda * c[a] * c[e];
  const auto at = [&](int i, int j) -> double & { return L[static_cast<size_t>(i)][static_cast<size_t>(i - j)]; }; // L(i, j), 0 ≤ i − j ≤ P
  for (int i = 0; i < n; ++i)
    for (int j = std::max(0, i - P); j <= i; ++j) {
      double s = at(i, j);
      for (int m = std::max(0, i - P); m < j; ++m)
        s -= at(i, m) * at(j, m);
      if (j < i) {
        at(i, j) = s / at(j, j);
      } else {
        assert(s > 0.0);
        at(i, i) = std::sqrt(s);
      }
    }
  for (int i = 0; i < n; ++i) { // L y = b
    double s = z[static_cast<size_t>(i)];
    for (int m = std::max(0, i - P); m < i; ++m)
      s -= at(i, m) * z[static_cast<size_t>(m)];
    z[static_cast<size_t>(i)] = s / at(i, i);
  }
  for (int i = n - 1; i >= 0; --i) { // Lᵀ z = y
    double s = z[static_cast<size_t>(i)];
    for (int m = i + 1; m <= std::min(n - 1, i + P); ++m)
      s -= at(m, i) * z[static_cast<size_t>(m)];
    z[static_cast<size_t>(i)] = s / at(i, i);
  }
  double sum = 0.0;
  for (double &v : z)
    sum += (v = std::exp(v));
  assert(sum > 0.0);
  g.resize(static_cast<size_t>(n));
  for (int i = 0; i < n; ++i)
    g[static_cast<size_t>(i)] = static_cast<float>(z[static_cast<size_t>(i)] * area / sum);
}

// 左下两半 (rIC | alpha) 同一张分布图, 元素与图例序对仗: PDF (KLL, 阴影 + 线) / PDF 光滑拟合 (smooth_fit) / mean / 时序 (背景) / 分组 (仅 alpha).
//   时序: x = 时间铺满当前 x 轴, y = 值 [−max|s|, max|s|] → 当前 y 视野 (0 钉在纵向正中), 淡灰压在最底, NoFit (不动坐标自适应)
//   分组: 组边界竖切 PDF, 每段组色填充 (决策边界); 图例一项, 一键开关
//   图例序 = ImPlot 本帧首次登记序, 与画序解耦: 先 PlotDummy 按图例序登记, 再按画序 (时序 → 填充 → 线) 真画
//   x: 0 恒钉在横向正中 (follow_centered); 目标半宽 = x_half, 给 0 则贴 PDF 两端 max |x|
struct DistView {
  const char *title, *x_label, *series_label;
  const analysis::AggPdf &pdf;
  const std::vector<float> &series; // 时序 (行序)
  double mean;
  double x_half = 0.0;
  const AlphaDist *bands = nullptr; // 非空 = 分组 (边界 / 组占比 / 组均)
};
void plot_dist(const DistView &v, const ImVec2 &size) {
  const ImGuiID key = ImHashData(v.pdf.y.data(), v.pdf.y.size() * sizeof(float), ImHashStr(v.title)); // 标题带矩 + PDF 本身
  fit_on_change(v.title, key, {ImAxis_Y1});
  if (!ImPlot::BeginPlot(v.title, size, ImPlotFlags_NoMenus))
    return;
  CompactLegend cl;
  const analysis::AggPdf &p = v.pdf;
  const int np = static_cast<int>(p.n_pts);
  ImPlot::SetupAxes(v.x_label, "density", ImPlotAxisFlags_None, ImPlotAxisFlags_None);
  ImPlot::SetupAxisFormat(ImAxis_Y1, kYTickFmt);
  {
    double half = v.x_half;
    if (half <= 0.0 && np > 0)
      half = std::max(std::abs(static_cast<double>(p.x[0])), std::abs(static_cast<double>(p.x[static_cast<size_t>(np) - 1])));
    follow_centered(ImAxis_X1, half > 0.0 ? half : 1.0); // 无 PDF / 退化在 0 点 → 单位半宽占位
  }
  ImPlot::SetupLegend(ImPlotLocation_NorthWest, ImPlotLegendFlags_None);
  const char *const kPdf = "PDF", *const kFit = "PDF 光滑拟合", *const kMean = "mean", *const kBands = "分组 G1~G20";
  const ImVec4 col_pdf(0.4f, 0.7f, 1.0f, 1.0f), col_fit(1.0f, 0.4f, 0.7f, 0.9f), col_mean(1.0f, 0.75f, 0.3f, 1.0f), col_ser(0.6f, 0.6f, 0.6f, 0.3f);
  const auto legend = [](const char *id, const ImVec4 &c) {
    ImPlot::SetNextLineStyle(c);
    ImPlot::PlotDummy(id);
  };
  legend(kPdf, col_pdf);
  legend(kFit, col_fit);
  legend(kMean, col_mean);
  legend(v.series_label, col_ser);
  if (v.bands)
    legend(kBands, group_color(K - 1));
  if (np > 3) {
    const float *px = p.x.data(), *py = p.y.data();
    const int ns = static_cast<int>(v.series.size());
    const auto [s_lo, s_hi] = std::minmax_element(v.series.begin(), v.series.end());
    const float amp = ns >= 2 ? std::max(std::abs(*s_lo), std::abs(*s_hi)) : 0.f;
    if (amp > 0.f) {
      const ImPlotRect lim = ImPlot::GetPlotLimits();
      const double y_mid = 0.5 * (lim.Y.Min + lim.Y.Max), y_half = 0.5 * lim.Y.Size();
      static std::vector<float> tx, ty;
      tx.resize(static_cast<size_t>(ns)), ty.resize(static_cast<size_t>(ns));
      for (int i = 0; i < ns; ++i) {
        tx[static_cast<size_t>(i)] = static_cast<float>(lim.X.Min + lim.X.Size() * i / (ns - 1));
        ty[static_cast<size_t>(i)] = static_cast<float>(y_mid + y_half * v.series[static_cast<size_t>(i)] / amp);
      }
      ImPlot::SetNextLineStyle(col_ser, 1.0f);
      ImPlot::PlotLine(v.series_label, tx.data(), ty.data(), ns, ImPlotItemFlags_NoFit);
    }
    ImPlot::SetNextFillStyle(col_pdf, 0.35f);
    ImPlot::PlotShaded(kPdf, px, py, np, 0.0);
    if (v.bands) {
      const auto y_at = [&](float x) { // PDF 线性插值
        const int j = std::clamp(static_cast<int>(std::lower_bound(px, px + np, x) - px), 1, np - 1);
        const float t = std::clamp((x - px[j - 1]) / (px[j] - px[j - 1]), 0.f, 1.f);
        return py[j - 1] + t * (py[j] - py[j - 1]);
      };
      static std::vector<float> sx, sy;
      for (int k = 0; k < K; ++k) {
        const float lo = std::max(px[0], k ? v.bands->bound[k - 1] : px[0]), hi = std::min(px[np - 1], k + 1 < K ? v.bands->bound[k] : px[np - 1]);
        if (!(hi > lo))
          continue;
        sx.assign(1, lo), sy.assign(1, y_at(lo));
        for (int i = 0; i < np; ++i)
          if (px[i] > lo && px[i] < hi)
            sx.push_back(px[i]), sy.push_back(py[i]);
        sx.push_back(hi), sy.push_back(y_at(hi));
        ImPlot::SetNextFillStyle(group_color(k), 0.6f);
        ImPlot::PlotShaded(kBands, sx.data(), sy.data(), static_cast<int>(sx.size()), 0.0);
      }
    }
    ImPlot::SetNextLineStyle(col_pdf, 2.0f);
    ImPlot::PlotLine(kPdf, px, py, np);
    static std::vector<float> fit;
    smooth_fit(p, fit);
    ImPlot::SetNextLineStyle(col_fit, 1.5f);
    ImPlot::PlotLine(kFit, px, fit.data(), np);
  }
  const double zero = 0.0;
  ImPlot::SetNextLineStyle(ImVec4(0.6f, 0.6f, 0.6f, 0.8f), 1.0f);
  ImPlot::PlotInfLines("##zero", &zero, 1);
  ImPlot::SetNextLineStyle(col_mean, 1.5f);
  ImPlot::PlotInfLines(kMean, &v.mean, 1);
  if (np > 3 && ImPlot::IsPlotHovered()) {
    const float x = static_cast<float>(ImPlot::GetPlotMousePos().x);
    const int i = std::clamp(static_cast<int>(std::lower_bound(p.x.data(), p.x.data() + np, x) - p.x.data()), 0, np - 1);
    if (!v.bands) {
      ImGui::SetTooltip("%s %+.4g  density %.3f", v.x_label, x, p.y[static_cast<size_t>(i)]);
    } else {
      const AlphaDist &b = *v.bands;
      const int k = static_cast<int>(std::upper_bound(b.bound, b.bound + K - 1, x) - b.bound);
      ImGui::SetTooltip("%s %+.4g  density %.3f\nG%d  [%+.4g, %+.4g]  占全体 %.1f%%  组均 %+.4g", v.x_label, x, p.y[static_cast<size_t>(i)], k + 1,
                        k ? b.bound[k - 1] : b.lo, k + 1 < K ? b.bound[k] : b.hi, 100.f * b.share[k], b.gmean[k]);
    }
  }
  ImPlot::EndPlot();
}

// 左下左半: rIC 分布 — 逐行 rank IC 当随机变量 (标题的矩与顶部 stat 行同一组样本); x 默认 ±0.5, 0 居中: 换持有期 / 因子时分布位置可直接比
void plot_ic(const InspectDerived &d, const ImVec2 &size) {
  char title[160];
  std::snprintf(title, sizeof(title), "rIC 分布 (逐行 rank IC)  sd %.4f  skew %+.2f  kurt %+.2f###ic", d.hs.ric.sd, d.hs.ric.skew, d.hs.ric.kurt);
  plot_dist({title, "IC", "rIC(t)", d.ic_pdf, d.ic_series, d.hs.ric.mean, 0.5, nullptr}, size);
}

// 左下右半: alpha 分布 — 池内有效格的 alpha (标题的矩 = 全体, 不截尾); PDF 已截尾 → x 半宽贴 PDF 两端 max |alpha| 即按主体缩放 (0 居中); 时序 = 逐行截面均值; 组边界竖切分组
void plot_alpha(const InspectDerived &d, const ImVec2 &size) {
  const AlphaDist &a = d.adist;
  char title[160];
  std::snprintf(title, sizeof(title), "alpha 分布 (PDF 截 0.4%%~99.6%%)  mean %+.4g  sd %.4g  skew %+.2f  kurt %+.2f###alpha", a.mean, a.sd, a.skew,
                a.kurt);
  plot_dist({title, "alpha", "alpha(t)", a.pdf, a.series, a.mean, 0.0, &a}, size);
}

// 右下: 期限结构 = 全部持有期的 L (G20 做多超额) / S (G1 做空超额) / LS (= ½ (L + S)) 均值, PCHIP 圆滑折线 + 节点圆点.
// 三组 × (毛 + 全部金额段) 一次画全 (金额段越大线越细越淡), 成本只在这张图出现; L / S 默认隐藏, 点图例切 (图例项 = 组 × 金额段, 左上, 两列对齐).
// 另一条池指数线 (池等权做多的绝对年化, 与因子无关): 看池本身在各持有期的漂移, 与超额线同轴对照.
// 口径 = Stat 契约【持仓口径】的每期真实收益 (正向; 腿 = leg_period = HoldStat::leg; ls_period = Σ ls / 全部行, 不触发行记 0).
// 净线 = 毛二级 − 该腿该持有期均摊换手成本 (Stat 契约【换手成本】CostAcc::cost(腿), 含 fixed_impact; 两腿都按本侧超额画, 成本都使线下移;
// LS = net_ls_period): 纯派生, 现算. 均摊成本随 h 变 (交接与否 / 每期换手量随 h 变), 所以每 (金额段, h) 一份 CostAcc.
//   毛 L / S / LS 的样本是 J ∧ ok 行, 成本的样本是腿成员 ∧ 标签有效格 (与组计数同口径)
// hs[n_hold] 毛二级; costs[n_cost] 该层金额段 (至少一段; Bar 层 = 固定段). n < 3 的持有期不作节点.
// pool_ann[n_hold] = 池等权指数各持有期的算术年化 (InspectResult::pool_ann, 绝对口径; 与因子无关, 随池一代); nullptr = 不画 (Factors 表 stat 无此项)
// 悬停: 就近命中一个可见数据点 (组 × 金额段 × 持有期, 像素距 ≤ kPick) → 高亮圈 + 该点的账 (bp/期): 毛 → 开仓冲击 / 关仓 / 换手 → 均摊 → 净 → × 期数 = 画的年化
void plot_term(const factor::stat::HoldStat *hs, int n_hold, const CostTier *costs, int n_cost, const float *pool_ann, double fixed_impact, int sel_hold,
               const char *source, const ImVec2 &size) {
  assert(n_hold >= 1 && n_cost >= 1 && fixed_impact >= 0.0);
  char title[128];
  std::snprintf(title, sizeof(title), "期限结构(%s)###term", source);
  {
    ImGuiID key = ImHashData(hs, static_cast<size_t>(n_hold) * sizeof(*hs), ImHashStr(source)); // 毛二级 + 池指数 + 金额段数 + 冲击口径
    if (pool_ann)
      key = ImHashData(pool_ann, static_cast<size_t>(n_hold) * sizeof(*pool_ann), key);
    key = ImHashData(&n_cost, sizeof(n_cost), key);
    fit_on_change(title, ImHashData(&fixed_impact, sizeof(fixed_impact), key), {ImAxis_Y1});
  }
  if (!ImPlot::BeginPlot(title, size, ImPlotFlags_NoMenus))
    return;
  CompactLegend cl;
  const int n_tier = n_cost + 1; // q = 0 毛, q ≥ 1 = costs[q − 1]
  std::vector<double> pos(static_cast<size_t>(n_hold));
  std::vector<std::string> names(static_cast<size_t>(n_hold)), tier_names(static_cast<size_t>(n_tier));
  std::vector<const char *> labels(static_cast<size_t>(n_hold));
  for (int i = 0; i < n_hold; ++i) {
    pos[static_cast<size_t>(i)] = i;
    names[static_cast<size_t>(i)] = factor::stat::hold_name(hs[i].hold);
    labels[static_cast<size_t>(i)] = names[static_cast<size_t>(i)].c_str();
  }
  tier_names[0] = "无冲击";
  for (int q = 1; q < n_tier; ++q)
    tier_names[static_cast<size_t>(q)] = costs[q - 1].amt == kAmtFixed ? std::string("固定冲击") : amt_name(costs[q - 1].amt);
  // 线 c (c < kLegs = 腿 c, c = kLegs = LS) 在金额段 q 下每期均值 (未年化): 毛原值; 净 = 毛 − 该腿该持有期均摊换手成本 (LS 走契约 net_ls_period)
  const auto value = [&](int c, int q, int i) {
    const factor::stat::HoldStat &h = hs[i];
    if (c == kLegs)
      return q == 0 ? factor::stat::ls_period(h, kFwd) : factor::stat::net_ls_period(h, costs[q - 1].acc[i], fixed_impact);
    const double gross = leg_period(h, kFwd, c);
    return q == 0 ? gross : gross - costs[q - 1].acc[i].cost(c, fixed_impact, h.n_all);
  };
  ImPlot::SetupAxes("hold", "年化均值超额 (%)", ImPlotAxisFlags_None, ImPlotAxisFlags_None);
  ImPlot::SetupAxisFormat(ImAxis_Y1, kYTickFmt);
  ImPlot::SetupAxisLimitsConstraints(ImAxis_Y1, -100.0, 200.0); // 年化 < −100% (高金额段净线会砸穿) / > 200% 无意义: 轴夹在 [−100, 200], 线出界即裁
  ImPlot::SetupAxisTicks(ImAxis_X1, pos.data(), n_hold, labels.data());
  follow_limits(ImAxis_X1, -0.6, n_hold - 0.4);
  ImPlot::SetupLegend(ImPlotLocation_NorthWest, ImPlotLegendFlags_None);
  constexpr int kIdx = kLegs + 1; // 命中码: 池指数线
  const char *items[kLegs + 1] = {"L", "S", "LS"};
  const ImVec4 cols[kLegs + 1] = {kColTop, kColBottom, kColLS};
  // 就近命中: 可见项的数据点里离鼠标最近且 ≤ kPick 像素的那个 (组 c / 金额段 q / 持有期 i)
  constexpr float kPick = 10.f;
  const bool hovered = ImPlot::IsPlotHovered();
  const ImVec2 mouse = ImGui::GetMousePos();
  int pc = -1, pq = -1, pi = -1;
  float pd2 = kPick * kPick;
  ImVec2 ppx;
  ImVec4 pcol;
  std::vector<double> xk, yk, xs, ys;
  for (int c = 0; c <= kLegs; ++c)
    for (int q = 0; q < n_tier; ++q) {
      xk.clear();
      yk.clear();
      for (int i = 0; i < n_hold; ++i) {
        if (hs[i].n < 3)
          continue;
        const double ann = 100.0 * factor::stat::span_per_year(hs[i]); // 每期均值 × 每年满仓轮数 → 满仓年化 %, 跨持有期可比
        xk.push_back(i);
        yk.push_back(value(c, q, i) * ann);
      }
      pchip_dense(xk, yk, xs, ys);
      const float fade = n_tier > 1 ? 1.f - 0.6f * static_cast<float>(q) / static_cast<float>(n_tier - 1) : 1.f; // 金额段越大越淡
      const ImVec4 col(cols[c].x, cols[c].y, cols[c].z, fade);
      const float wt = (c == kLegs ? 2.5f : 1.5f) * (0.5f + 0.5f * fade);
      char name[64];
      std::snprintf(name, sizeof(name), "%-2s %s", items[c], tier_names[static_cast<size_t>(q)].c_str()); // 等宽字体: 组名定宽, 金额段列对齐
      if (c < kLegs)
        ImPlot::HideNextItem(true, ImPlotCond_Once); // 默认只显 LS (全部金额段), 图例可点开
      ImPlot::SetNextLineStyle(col, wt);
      ImPlot::PlotLine(name, xs.data(), ys.data(), static_cast<int>(xs.size()));
      ImPlot::SetNextMarkerStyle(ImPlotMarker_Circle, 3.f, col, IMPLOT_AUTO, col);
      ImPlot::PlotScatter(name, xk.data(), yk.data(), static_cast<int>(xk.size())); // 同名 → 同一图例项, 一起显隐
      const ImPlotItem *item = ImPlot::GetItem(name);                               // 刚 Plot 过, 必在池里
      assert(item);
      if (!hovered || !item->Show)
        continue;
      for (size_t k = 0; k < xk.size(); ++k) {
        const ImVec2 px = ImPlot::PlotToPixels(xk[k], yk[k]);
        const float dx = px.x - mouse.x, dy = px.y - mouse.y, d2 = dx * dx + dy * dy;
        if (d2 < pd2)
          pd2 = d2, pc = c, pq = q, pi = static_cast<int>(xk[k]), ppx = px, pcol = ImVec4(col.x, col.y, col.z, 1.f);
      }
    }
  constexpr const char *kIdxName = "池指数 (绝对)";
  if (pool_ann) { // 池等权指数: 与因子无关的绝对基准, 全部持有期作节点
    xk.clear();
    yk.clear();
    for (int i = 0; i < n_hold; ++i) {
      xk.push_back(i);
      yk.push_back(100.0 * pool_ann[i]);
    }
    pchip_dense(xk, yk, xs, ys);
    const ImVec4 icol(kColIndex.x, kColIndex.y, kColIndex.z, 1.f); // 与 K 线叠加的指数同色相, 这里是前景线: 不透明
    ImPlot::SetNextLineStyle(icol, 1.5f);
    ImPlot::PlotLine(kIdxName, xs.data(), ys.data(), static_cast<int>(xs.size()));
    ImPlot::SetNextMarkerStyle(ImPlotMarker_Square, 3.f, icol, IMPLOT_AUTO, icol);
    ImPlot::PlotScatter(kIdxName, xk.data(), yk.data(), static_cast<int>(xk.size()));
    const ImPlotItem *item = ImPlot::GetItem(kIdxName);
    assert(item);
    if (hovered && item->Show)
      for (size_t k = 0; k < xk.size(); ++k) {
        const ImVec2 px = ImPlot::PlotToPixels(xk[k], yk[k]);
        const float dx = px.x - mouse.x, dy = px.y - mouse.y, d2 = dx * dx + dy * dy;
        if (d2 < pd2)
          pd2 = d2, pc = kIdx, pq = 0, pi = static_cast<int>(xk[k]), ppx = px, pcol = icol;
      }
  }
  if (sel_hold >= 0 && sel_hold < n_hold)
    ImPlot::TagX(static_cast<double>(sel_hold), ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "sel");
  if (pc >= 0) {
    ImPlot::PushPlotClipRect();
    ImPlot::GetPlotDrawList()->AddCircle(ppx, 6.f, ImGui::ColorConvertFloat4ToU32(pcol), 0, 2.f);
    ImPlot::PopPlotClipRect();
  }
  if (pc == kIdx) {
    ImGui::BeginTooltip();
    ImGui::Text("%s  h=%s", kIdxName, labels[static_cast<size_t>(pi)]);
    ImGui::Separator();
    ImGui::Text("年化 %+.2f %%", 100.0 * pool_ann[pi]);
    ImGui::TextDisabled("池内全部成分等权做多 (可买才建仓), 每批 1/h̄, 不扣成本; 全期逐行收益和 / 天数 × %d", factor::stat::kDaysPerYear);
    ImGui::EndTooltip();
  } else if (pc >= 0) {
    // 账 (bp/期): L = G20 做多 / S = G1 做空 (都按本侧超额, 净 = 毛 − 成本) / LS = 两腿平均 (契约 ls_period, 成本 Σ cost(腿)/2); 净 × 期数 = 画的 y
    constexpr double bp = 1e4;
    const factor::stat::HoldStat &h = hs[pi];
    const bool ls = pc == kLegs;
    const double gross = ls ? factor::stat::ls_period(h, kFwd) : leg_period(h, kFwd, pc);
    const double ppy = factor::stat::span_per_year(h);
    const char *tier = tier_names[static_cast<size_t>(pq)].c_str();
    // 一行一项, 三列定位 (SameLine 绝对 x, 不靠 printf 补空格 —— 中文字节宽 ≠ 字形宽): 名目 | 数值 | 注
    constexpr float kXVal = 90.f, kXNote = 230.f;
    const auto row = [&](const char *name, const char *value, const char *note) {
      ImGui::TextUnformatted(name);
      ImGui::SameLine(kXVal);
      ImGui::TextUnformatted(value);
      ImGui::SameLine(kXNote);
      ImGui::TextDisabled("%s", note);
    };
    char v[64], note[192];
    ImGui::BeginTooltip();
    ImGui::Text("%s %s  h=%s", items[pc], tier, labels[static_cast<size_t>(pi)]);
    ImGui::Separator();
    std::snprintf(v, sizeof(v), "%+.2f bp/期", gross * bp);
    if (ls)
      std::snprintf(note, sizeof(note), "G20 多 %+.2f / G1 空 %+.2f 的平均", leg_period(h, kFwd, kLong) * bp, leg_period(h, kFwd, kShort) * bp);
    else
      std::snprintf(note, sizeof(note), "%s", pc == kLong ? "G20 做多超额" : "G1 做空超额");
    row("毛", v, note);
    double amort = 0.0;
    if (pq > 0) {
      const factor::stat::CostAcc &ca = costs[pq - 1].acc[pi];
      const auto avg = [&](auto f) { return ls ? 0.5 * (f(kLong) + f(kShort)) : f(pc); }; // LS = 两腿平均, 否则该腿
      const auto entry = [&](int l) { return ca.entry(l, fixed_impact); };
      const auto turn = [&](int l) { return factor::stat::turn_annual(h, ca, l); };
      const auto cost = [&](int l) { return ca.cost(l, fixed_impact, h.n_all); };
      amort = avg(cost);
      ImGui::Spacing();
      // 开仓冲击 = 增仓量加权建仓冲击; 年换手 = 单边 Σ Δw⁺ / 全部行 × 每年满仓轮数 (同一时刻交接的只算差额);
      // 均摊成本 = (Σ 增仓 × 建仓冲击 + Σ 平仓量 × 固定冲击) / 全部行; LS 两腿各算再平均
      std::snprintf(v, sizeof(v), "%.2f bp", avg(entry) * bp);
      if (ls)
        std::snprintf(note, sizeof(note), "buy %.2f / sell %.2f", entry(kLong) * bp, entry(kShort) * bp);
      else
        std::snprintf(note, sizeof(note), "%s", pc == kLong ? "buy" : "sell");
      row("开仓冲击", v, note);
      std::snprintf(v, sizeof(v), "%.2f bp", fixed_impact * bp);
      row("平仓冲击", v, "config fixed_impact");
      std::snprintf(v, sizeof(v), "%.1f 倍/年", avg(turn));
      if (ls)
        std::snprintf(note, sizeof(note), "L %.1f / S %.1f", turn(kLong), turn(kShort));
      else
        note[0] = '\0';
      row("年换手", v, note);
      std::snprintf(v, sizeof(v), "%.2f bp/期", amort * bp);
      if (ls)
        std::snprintf(note, sizeof(note), "L %.2f / S %.2f", cost(kLong) * bp, cost(kShort) * bp);
      else
        note[0] = '\0';
      row("均摊成本", v, note);
    }
    const double net = gross - amort;
    ImGui::Spacing();
    if (pq > 0) {
      std::snprintf(v, sizeof(v), "%+.2f bp/期", net * bp);
      row("净", v, "= 毛 − 成本");
    }
    std::snprintf(v, sizeof(v), "%+.2f %%", net * ppy * 100.0);
    std::snprintf(note, sizeof(note), "= %s × %.0f 期/年", pq > 0 ? "净" : "毛", ppy);
    row("年化", v, note);
    ImGui::EndTooltip();
  }
  ImPlot::EndPlot();
}

// 有效权重行 = 可 Inspect (有具体串且解析通过; 文件级"未搜索"行不算)
bool inspectable(const FactorRow &r) { return r.error.empty() && !r.expr.empty(); }

// 高光行 (fui.view_file = FactorRow::key) 短锁拷出 (rows 由 worker 重扫时整体替换); 顺带给出相邻有效权重行的 key (箭头循环切权重用). 找不到 → false
bool view_row(FactorsService &fsvc, const std::string &key, FactorRow &row, std::string *prev_key = nullptr, std::string *next_key = nullptr) {
  std::lock_guard<std::mutex> lock(fsvc.mutex);
  const auto &rows = fsvc.rows;
  const int n = static_cast<int>(rows.size());
  int cur = -1;
  for (int i = 0; i < n; ++i)
    if (rows[static_cast<size_t>(i)].key == key) {
      cur = i;
      break;
    }
  if (cur < 0)
    return false;
  row = rows[static_cast<size_t>(cur)];
  const auto neighbor = [&](int dir, std::string &out) { // 从相邻行起按 dir 绕一圈找第一个有效权重行; 没有 → 空
    out.clear();
    for (int step = 1; step < n; ++step) {
      const FactorRow &r = rows[static_cast<size_t>(((cur + dir * step) % n + n) % n)];
      if (inspectable(r)) {
        out = r.key;
        return;
      }
    }
  };
  if (prev_key)
    neighbor(-1, *prev_key);
  if (next_key)
    neighbor(+1, *next_key);
  return true;
}

// 该行在当前作用域下的请求 key (与 MakeInspectRequest 产出的一致)
std::string probe_key(const FactorRow &row, const FactorsUIContext &ctx) { return inspect_key(ctx.lv, ctx.pool, row.key, row.expr); }

} // namespace

bool InspectAutoRequest(FactorsService &fsvc, InspectService &isvc, FactorsUIState &fui, InspectUIState &ui, const FactorsUIContext &ctx) {
  // 默认高光: 因子表当前 Hold 下 rIC 最高的有效权重 (都无该 Hold 的 stat → 表序第一个); 本作用域的表还没扫出 → 下帧再试
  if (!fui.view_defaulted && fui.view_file.empty()) {
    const auto &labels = ctx.ft->y.labels;
    const int hold = labels.empty() ? 0 : labels[static_cast<size_t>(std::clamp(fui.hold_idx, 0, static_cast<int>(labels.size()) - 1))].key;
    std::lock_guard<std::mutex> lock(fsvc.mutex);
    if (fsvc.current.scope.factor_dir == ctx.factor_dir) {
      const FactorRow *first = nullptr, *best = nullptr;
      for (const FactorRow &r : fsvc.rows) {
        if (!inspectable(r))
          continue;
        if (!first)
          first = &r;
        const factor::stat::HoldStat *h = r.has_stat ? r.find_hold(hold) : nullptr;
        if (h && h->n >= 3 && (!best || h->ric.mean > best->find_hold(hold)->ric.mean))
          best = &r;
      }
      if (const FactorRow *pick = best ? best : first) {
        fui.view_file = pick->key;
        fui.view_defaulted = true;
      }
    }
  }
  if (fui.view_file.empty() || !ctx.axis_ready || ctx.ft->y.labels.empty())
    return false;
  if (isvc.busy())
    return false;
  FactorRow row;
  if (!view_row(fsvc, fui.view_file, row) || !inspectable(row))
    return false;
  const std::string key = probe_key(row, ctx);
  if (ui.last_req_key == key) // 取消过 / 已发过的 key 不反复起 (Compute 手动)
    return false;
  {
    std::lock_guard<std::mutex> lock(isvc.mutex);
    if (isvc.result.key == key)
      return false;
  }
  ui.last_req_key = key;
  ui.req_row = row;
  ui.req_reload = false;
  return true;
}

int RenderTabInspect(FactorsService &fsvc, InspectService &isvc, KlineService &ksvc, FactorsUIState &fui, InspectUIState &ui, const FactorsUIContext &ctx) {
  int action = 0;
  // 左键 = 框选缩放 (K 线 / 分层图手动 zoom), 右键 = 平移 (对仗 OrderFlow); 全局, 但本页与 OrderFlow 同向, 不冲突
  ImPlot::MapInputReverse();
  // 顶部 2:1 两栏: 左 = 头信息 + 控件 + 作用域, 右 = 选定持有期的 stat 面板
  if (!ImGui::BeginTable("##inspect_top", 2, ImGuiTableFlags_SizingStretchProp | ImGuiTableFlags_BordersInnerV))
    return 0;
  ImGui::TableSetupColumn("##head", ImGuiTableColumnFlags_WidthStretch, 2.f);
  ImGui::TableSetupColumn("##stat", ImGuiTableColumnFlags_WidthStretch, 1.f);
  ImGui::TableNextRow();
  ImGui::TableNextColumn();
  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "Factor:");
  ImGui::SameLine();
  if (fui.view_file.empty()) {
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Muted), "(无) 在 Factors 页点一行高光");
    ImGui::EndTable();
    return 0;
  }
  // sel = 选中行 (view_file = 权重行 key; 箭头 / 自动起算 / Compute 的对象); row = 显示行 = 现结果的权重 (切换期间整页继续画旧的, 新结果到了一次性换),
  // 无结果 (或结果权重已不在表里) 时 = sel
  FactorRow sel;
  std::string prev_key, next_key;
  if (!view_row(fsvc, fui.view_file, sel, &prev_key, &next_key)) {
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "%s (重扫中 / 已不在)", fui.view_file.c_str());
    ImGui::EndTable();
    return 0;
  }
  FactorRow row = sel;
  {
    std::string res_key;
    {
      std::lock_guard<std::mutex> lock(isvc.mutex);
      if (isvc.result.error.empty())
        res_key = isvc.result.row;
    }
    FactorRow r;
    if (!res_key.empty() && res_key != sel.key && view_row(fsvc, res_key, r))
      row = r;
  }
  const bool switching = row.key != sel.key;
  if (prev_key.empty())
    ImGui::BeginDisabled();
  if (ImGui::ArrowButton("##prev_factor", ImGuiDir_Left)) // 不切页换权重: 按表序循环到相邻有效权重行 (自动起算随之)
    fui.view_file = prev_key;
  if (prev_key.empty())
    ImGui::EndDisabled();
  ImGui::SameLine(0.f, 2.f);
  if (next_key.empty())
    ImGui::BeginDisabled();
  if (ImGui::ArrowButton("##next_factor", ImGuiDir_Right))
    fui.view_file = next_key;
  if (next_key.empty())
    ImGui::EndDisabled();
  ImGui::SameLine();
  ImGui::Text("%s  %s  #%d", row.name_cn.c_str(), row.name_en.c_str(), row.widx);
  if (ImGui::IsItemHovered())
    tip("%s  (搜自 h %s)\n◂ ▸ 切到表里相邻的权重", row.file.c_str(), row.hold_src > 0 ? factor::stat::hold_name(row.hold_src).c_str() : "- (手填)");
  if (switching) {
    ImGui::SameLine();
    if (sel.error.empty())
      ImGui::TextColored(StatusColor(TaskStatus::Kind::Busy), "→ %s  %s (计算中…)", sel.name_cn.c_str(), sel.name_en.c_str());
    else
      ImGui::TextColored(StatusColor(TaskStatus::Kind::Error), "→ %s BROKEN: %s", sel.name_en.c_str(), sel.error.c_str());
  }
  ImGui::SameLine();
  ImGui::TextDisabled("|");
  ImGui::SameLine();
  if (!row.error.empty()) {
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Error), "BROKEN: %s", row.error.c_str());
    ImGui::EndTable();
    return 0;
  }
  ImGui::Text("[%s] %s", factor::stat::frame_name(row.frame), row.expr.c_str());
  if (!row.note.empty())
    ImGui::TextDisabled("%s", row.note.c_str());

  // ---- 控件行 (各切换控件之间留宽间隔, 不然 "▸ 分层 ◂" 连成一片) ----
  constexpr float kCtlGap = 24.f;
  const FeatureTable &ft = *ctx.ft;
  const Phase st = isvc.phase.load(std::memory_order_acquire);
  const bool busy = isvc.busy();
  const int n_hold = static_cast<int>(ft.y.labels.size());
  if (ui.hold_for != sel.key) { // 换了权重 → Hold 切到它搜出来的持有期 (手填权重无, 不动)
    ui.hold_for = sel.key;
    for (int i = 0; i < n_hold; ++i)
      if (ft.y.labels[static_cast<size_t>(i)].key == sel.hold_src)
        ui.hold_idx = i;
  }
  ui.hold_idx = std::clamp(ui.hold_idx, 0, std::max(0, n_hold - 1));
  const int cur_hold = n_hold ? ft.y.labels[static_cast<size_t>(ui.hold_idx)].key : 0;
  ImGui::AlignTextToFramePadding();
  ImGui::TextDisabled("级别");
  ImGui::SameLine(0.f, 4.f);
  ImGui::TextUnformatted(factor_level(ft.lv).name);
  ImGui::SameLine(0.f, kCtlGap);
  ImGui::BeginGroup(); // Hold 周期名 + 开 / 清仓 (时点 / 价分色; 占位 = 各持有期最宽一条, 后面的控件不随 Hold 跳)
  hold_cycler(ui.hold_idx, ft);
  if (n_hold) {
    float w_max = 0.f;
    for (int i = 0; i < n_hold; ++i)
      w_max = std::max(w_max, hold_points_line(hold_points(ft.y.labels[static_cast<size_t>(i)].key, ft.lv), false));
    ImGui::SameLine(0.f, kCtlGap * 0.5f);
    const float x0 = ImGui::GetCursorPosX();
    hold_points_line(hold_points(cur_hold, ft.lv));
    ImGui::SameLine(0.f, 0.f);
    ImGui::Dummy(ImVec2(std::max(0.f, x0 + w_max - ImGui::GetCursorPosX()), 0.f));
  }
  ImGui::EndGroup();
  if (ImGui::IsItemHovered())
    tip("各图看哪个持有期 (只影响显示)\n"
        "%s",
        kHoldRulesTip);
  ImGui::SameLine(0.f, kCtlGap);
  {
    static const char *const kModes[2] = {"超额", "绝对"};
    int abs_idx = ui.absolute ? 1 : 0;
    if (cycler("分层", abs_idx, kModes, 2))
      ui.absolute = abs_idx == 1;
  }
  // 标签一律毛价格收益, 成本只进期限结构; 超额 = 组均 (lv − mkt), 绝对 = 组均 lv; LS = top(lv) + bottom(sv) 市场中性, 两口径同一条.
  // 尾部 = G20 / G1 内更极端的 50% / 20% / 5% (= 整截面 2.5% / 1% / 0.25%): 每级一条分层线 + 一条 LS, 右侧截面同步加柱, 越极端越细
  if (ImGui::IsItemHovered())
    tip("分层图的口径: 日收益对数累计 × 一年 / 全段天数 (全部线同一系数, 末点 = 对数年化)\n"
        "• 超额: 减去市场, 看形状\n"
        "• 绝对: 含市场\n"
        "• 点图例 \"LS 50%%\" 等可看更极端的尾部");
  ImGui::SameLine(0.f, kCtlGap);
  const bool can_run = !busy && ctx.axis_ready && n_hold > 0 && sel.error.empty();
  if (!can_run)
    ImGui::BeginDisabled();
  if (ImGui::Button("Compute", ImVec2(80, 0))) { // 算的是选中行
    action = 1;
    ui.req_row = sel;
    ui.req_reload = true;
  }
  if (!can_run)
    ImGui::EndDisabled();
  // 重读特征库 → eval (CPU) → Stat; 留毛口径一级 Row[H][T] + 二级 + 各金额段组均冲击 (期限结构净线). 换因子自动起算只补缺的特征
  if (ImGui::IsItemHovered())
    tip(ctx.axis_ready ? "弃缓存重算 (换因子会自动算, 一般不用点)" : "资产轴未就绪: 先在 Database 页扫描");
  ImGui::SameLine();
  if (!busy)
    ImGui::BeginDisabled();
  if (ImGui::Button("Cancel", ImVec2(60, 0)))
    action = -1;
  if (!busy)
    ImGui::EndDisabled();
  ImGui::SameLine();
  ImGui::Text("Status:");
  ImGui::SameLine();
  const int done = static_cast<int>(isvc.done.load(std::memory_order_relaxed)), total = static_cast<int>(isvc.total.load(std::memory_order_relaxed));
  switch (st) {
  case Phase::Idle:
  case Phase::Scanning:
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Muted), "idle");
    break;
  case Phase::Loading:
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Busy), "loading %d/%d days", done, total);
    break;
  case Phase::Running:
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Busy), "running %d/%d nodes", done, total);
    break;
  case Phase::Done:
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Ready), "done");
    break;
  case Phase::Cancelled:
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "cancelled %d/%d", done, total);
    break;
  }

  // ---- 结果快照 (持锁派生切片 (hold), 不拷平面) ----
  const std::string key = probe_key(sel, ctx);
  const int sel_hold = ui.hold_idx;
  std::string message, res_error, res_key, res_row;
  EvalStat res_ev;                 // 作用域 / 有效率 / 毛二级 [n_hold] / 各金额段成本 (顶部 stat 面板 + 期限结构净线现算)
  std::vector<float> res_pool_ann; // [n_hold] 池指数算术年化 (期限结构池指数线)
  bool res_has = false;
  int k_sub = -1;                   // 右上选中资产 (结果 A 轴下标); -1 = 无
  std::optional<KlineRequest> kreq; // 本帧要发的 K 线请求 (锁内填, 出锁发)
  {
    std::lock_guard<std::mutex> lock(isvc.mutex);
    message = isvc.message;
    const InspectResult &res = isvc.result;
    res_key = res.key;
    res_error = res.error;
    if (!res.key.empty() && res.error.empty()) {
      assert(res.n_hold == n_hold && "Inspect 与 FeatureTable 的标签组不一致");
      res_has = true;
      res_row = res.row;
      res_ev = static_cast<const EvalStat &>(res);
      res_pool_ann.assign(res.pool_ann, res.pool_ann + res.n_hold);
    }
    const uint64_t ep = isvc.result_gen;
    if (ep != ui.derived_epoch || sel_hold != ui.derived_sel || ui.absolute != ui.derived_abs) {
      ui.derived_epoch = ep;
      ui.derived_sel = sel_hold;
      ui.derived_abs = ui.absolute;
      if (res_has && sel_hold < res.hd.n)
        derive(res, sel_hold, ui.absolute, ui.der);
      else
        ui.der = InspectDerived{};
    }
    // 右上: 资产轴 (光标日序) / 选中资产 R(t) 列 / K 线请求期望态 —— 都要读 res.pl.rx / read_scope, 在锁内派生, 出锁只留派生
    if (res_has && ui.der.valid) {
      if (ui.cursor_day < 0 || ui.cursor_day > ui.der.days)
        ui.cursor_day = ui.der.days;
      const int day = std::max(ui.cursor_day, 1) - 1; // 起点 (0) 也按首日排
      if (ep != ui.ax_epoch || day != ui.ax_day) {
        ui.ax_epoch = ep, ui.ax_day = day;
        derive_axis(res, day, ui.ax_order, ui.ax_gid, ui.ax_r);
      }
      const PoolAxis &axis = res.read_scope.axis;
      size_t sub = axis.sub_of(ui.asset_gid); // 未选 / 不在本池轴内 → 光标日 R 最高者 (光标日无池内资产 → 全区间有效刻最多者), 结果一到就有 K 线
      if (sub >= axis.size()) {
        const int pick = !ui.ax_order.empty() ? ui.ax_order.front() : fallback_asset(res);
        if (pick >= 0) {
          sub = static_cast<size_t>(pick);
          ui.asset_gid = axis.global(sub);
        }
      }
      if (sub < axis.size()) {
        k_sub = static_cast<int>(sub);
        if (ep != ui.col_epoch || k_sub != ui.col_sub) {
          ui.col_epoch = ep, ui.col_sub = k_sub;
          derive_col(res, k_sub, ui.col_r, ui.col_r_all);
        }
        if (ui.kline_key != res.key || ui.kline_sub != k_sub) { // 换结果 / 换资产 → 新代从头流
          ++ui.kline_gen;
          ui.kline_key = res.key, ui.kline_sub = k_sub, ui.kline_days = res.scope.days, ui.kline_seg = factor_level(res.lv).seg, ui.kline_last_pub_days = ~0ull;
          kreq.emplace();
          kreq->gen = ui.kline_gen, kreq->scope = res.read_scope, kreq->dates = res.dates, kreq->sub = k_sub;
          assert(ft.lv == res.lv && "字段表层与结果层不符");
          kreq->level = factor_level(res.lv).lvl;
          for (int i = 0; i < 4; ++i)
            kreq->col[i] = ft.b.ohlc[i].col;
          kreq->col[4] = ft.y.ts.col;
        }
      }
    }
  }
  if (kreq)
    ksvc.Request(std::move(*kreq));
  if (!message.empty()) {
    ImGui::SameLine();
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "| %s", message.c_str());
  }
  if (action == 1) // 手动 Compute (自动起算在 InspectAutoRequest, 两页都每帧调)
    ui.last_req_key = key;

  // ---- 作用域行 (左栏末) + 选定持有期的 stat 面板 (右栏) ----
  // match = 结果就是当前请求 (选中行 + 作用域); 否则仍整页画旧结果 (服务不清旧 result), 新结果发布一次性换; 头部显示行 row 已随结果
  const bool match = res_has && res_key == key;
  const bool shown = res_has;
  assert(!shown || res_row == row.key || row.key == sel.key); // 显示行 = 结果权重 (结果权重已不在表里时退回选中行)
  const StatScope &sc = res_ev.scope;
  if (!res_error.empty()) {
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Error), "算不了: %s", res_error.c_str());
  } else if (shown) {
    ImGui::TextDisabled("%s  %s..%s  %d 天 × %d 资产  满仓 %.1f%s", sc.pool.c_str(), misc::dash(sc.start_date).c_str(), misc::dash(sc.end_date).c_str(),
                        sc.days, sc.A, sc.n_full, match ? "" : "  (旧结果, 新请求计算中…)");
    if (ImGui::IsItemHovered())
      tip("有效率 %.1f%%  可秩率 %.1f%%\n耗时 %.0f ms (%s, %s)", res_ev.valid_pct, res_ev.rank_pct, res_ev.eval_ms, sc.backend.c_str(), sc.time.c_str());
  } else {
    ImGui::TextDisabled("%s", busy ? "计算中…" : (ctx.axis_ready ? "尚未计算 (自动起算 / Compute)" : "资产轴未就绪, 先在 Database 页扫描"));
  }
  ImGui::TableNextColumn();
  if (shown) {
    assert(sel_hold < res_ev.n_hold);
    hold_stat_panel(res_ev, res_ev.hold[sel_hold]);
  }
  ImGui::EndTable();
  ImGui::Separator();

  // ---- 2×2 图 ----
  const ImVec2 avail = ImGui::GetContentRegionAvail();
  const float sp = ImGui::GetStyle().ItemSpacing.x;
  const ImVec2 cell(std::max(100.f, (avail.x - sp) * 0.5f), std::max(100.f, (avail.y - ImGui::GetStyle().ItemSpacing.y) * 0.5f));
  const char *wait_hint = busy ? "计算中…" : "Compute 后可见";

  if (shown && ui.der.valid)
    plot_layers(ui.der, ui.absolute, ui.cursor_day, cell);
  else
    empty_plot("分层对数累计###layers", cell, wait_hint);
  ImGui::SameLine();
  // 右上: 头行 (◂ ▸ 步进 + 资产轴条) + K 线
  if (shown && ui.der.valid && k_sub >= 0) {
    ImGui::BeginGroup();
    const int n = static_cast<int>(ui.ax_order.size());
    int pos = -1; // 选中资产在光标日序里的位置 (-1 = 当日无值)
    for (int i = 0; i < n; ++i)
      if (ui.ax_order[static_cast<size_t>(i)] == k_sub) {
        pos = i;
        break;
      }
    char name[96];
    asset_label(ctx, ui.asset_gid, name, sizeof(name));
    bool moved = false;
    const float row_x0 = ImGui::GetCursorPosX();
    const bool can_prev = pos > 0, can_next = pos >= 0 && pos + 1 < n;
    if (!can_prev)
      ImGui::BeginDisabled();
    if (ImGui::ArrowButton("##prev_asset", ImGuiDir_Left)) // 沿光标日序步进 (#1 = G20 侧)
      --pos, moved = true;
    if (!can_prev)
      ImGui::EndDisabled();
    ImGui::SameLine(0.f, 2.f);
    if (!can_next)
      ImGui::BeginDisabled();
    if (ImGui::ArrowButton("##next_asset", ImGuiDir_Right))
      ++pos, moved = true;
    if (!can_next)
      ImGui::EndDisabled();
    ImGui::SameLine();
    const ImGuiStyle &st = ImGui::GetStyle();
    const float strip_w = std::max(50.f, cell.x - (ImGui::GetCursorPosX() - row_x0) - ImGui::CalcTextSize("(?)").x - st.ItemSpacing.x);
    if (plot_asset_axis(ui, ctx, pos, ImVec2(strip_w, ImGui::GetFrameHeight())))
      moved = true;
    ImGui::SameLine();
    ImGui::AlignTextToFramePadding();
    ImGui::TextDisabled("(?)");
    // R(t) = r16(x) / kRankMax (CS 截面秩 / TS 自身分位), 与组号同向; 池 = 截面门控 g, 出池段秩不入评估 (K 线用不看池的秩淡色显示);
    // 选中沿资产粘住: 拖光标只换序, K 线不重载, 换池轴才重选.
    // 青线 line_t = EMA_L(个股)_{t−L} · exp(EMA_S(log(I_t / EMA_L(I)_{t−L}))), L = 1 交易日行数, S = kRefSmooth 行:
    //   基准 = L 行前的 EMA_L (当前价不进基准), 指数偏移再 EMA_S 压分钟噪声; 个股 close 无成交分钟前值填充, 跨日连续算; 线从首根蜡烛 + L 起有值
    // 黄线每帧按视野内首根蜡烛重锚, 带着全区间历史, 长视野下与个股渐行渐远
    if (ImGui::IsItemHovered())
      tip("资产条 = 光标日池内全部资产按因子值排序 (左 = 因子值最高, G20 侧), 格色 = 组色\n"
          "• 换资产: ◂ ▸ / 在条上按下选中, 按住拖动连续换\n"
          "K 线图例\n"
          "• 蜡烛色 = 该刻因子组色 (G1 蓝 → G20 橙; 淡 = 池外, 暗灰 = 无值)\n"
          "• 青线 = 个股跟着指数近期走势应在的价; 蜡烛 − 青线 = 相对指数的偏移\n"
          "• 黄线 = 池等权指数 (默认隐藏)\n"
          "• 累计 rIC (右轴) = 当前 Hold 的逐行 rIC 累计; 斜率 = 该段因子有效性; 蓝 = 新高, 回撤越深越红");
    const float sp_y = st.ItemSpacing.y;
    if (moved && pos >= 0 && pos < n)
      ui.asset_gid = ui.ax_gid[static_cast<size_t>(pos)];
    char title[160];
    std::snprintf(title, sizeof(title), "分钟K %s###kline", name);
    const float kh = std::max(60.f, cell.y - ImGui::GetFrameHeight() - sp_y);
    plot_kline(ui, ksvc.stream(), ui.der, ui.cursor_day, title, ImVec2(cell.x, kh));
    ImGui::EndGroup();
  } else {
    empty_plot("资产 K 线###kline", cell, wait_hint);
  }

  if (shown && ui.der.valid) { // 左下一格分两半: rIC 分布 | alpha 分布
    const ImVec2 half(std::max(50.f, (cell.x - sp) * 0.5f), cell.y);
    plot_ic(ui.der, half);
    ImGui::SameLine();
    plot_alpha(ui.der, half);
  } else
    empty_plot("IC 分布###ic", cell, wait_hint);
  ImGui::SameLine();
  // 右下 期限结构: Inspect 结果优先, 否则 Factors 表的 stat (row.hold + row.costs; 两者都是毛 + 全部金额段, 同一份 CostTier)
  if (shown) {
    plot_term(res_ev.hold, n_hold, res_ev.costs.data(), static_cast<int>(res_ev.costs.size()), res_pool_ann.data(), ctx.fixed_impact, ui.hold_idx,
              "Inspect", cell);
  } else if (row.has_stat && row.n_hold == n_hold) {
    plot_term(row.hold, n_hold, row.costs.data(), static_cast<int>(row.costs.size()), nullptr, ctx.fixed_impact, ui.hold_idx,
              row.stat_from_file ? "文件 stat" : "Factors Run", cell);
  } else {
    empty_plot("期限结构###term", cell, "无 stat (Factors 页 Run 或此处 Compute)");
  }
  return action;
}

} // namespace GUI::Factors
