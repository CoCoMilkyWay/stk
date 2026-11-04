#include "gui/task_database/ui/QuotesPanel.hpp"

#include "imgui.h"
#include "implot.h"
#include "implot_internal.h"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdio>
#include <initializer_list>
#include <limits>
#include <mutex>
#include <string>
#include <vector>

namespace GUI::Database {

namespace {

constexpr ImVec4 kUp(0.945f, 0.298f, 0.298f, 1.f), kDn(0.416f, 0.600f, 0.333f, 1.f), kFlat(0.80f, 0.80f, 0.80f, 1.f);
constexpr ImVec4 kRule(0.55f, 0.55f, 0.55f, 0.45f);
constexpr int kMaN[] = {5, 10, 20};
constexpr const char *kMaName[] = {"MA5", "MA10", "MA20"};
constexpr ImVec4 kMaCol[] = {ImVec4(0.843f, 0.729f, 0.490f, 1.f), ImVec4(0.306f, 0.788f, 0.690f, 1.f),
                             ImVec4(0.773f, 0.525f, 0.753f, 1.f)};
constexpr int kIntraDays[] = {1, 2, 3, 4};
constexpr const char *kIntraName[] = {"日内", "2日", "3日", "4日"};
constexpr int kDailySpan[] = {5, 22, 250, 2500}; // 交易日
constexpr const char *kDailyName[] = {"周", "月", "年", "10年"};
static_assert(kDailySpan[3] <= static_cast<int>(kDailyDays));
constexpr const char *kHours[] = {"09:30", "10:30", "11:30/13:00", "14:00", "15:00"};
constexpr double kBody = 0.35; // 蜡烛 / 量柱半宽 [根]
constexpr int kSub = 4;        // 日内高低线相邻两根间的插值段数
constexpr double kMinSpan = 5; // 视野至少几根 (框选 / 滚轮)
constexpr double kWheel = 0.8; // 滚轮一格视野宽 × kWheel (向前 = 放大)
constexpr float kDragPx = 4.f; // 框选生效的最小拖动 [px]
constexpr double kNaN = std::numeric_limits<double>::quiet_NaN();
constexpr ImVec4 kRefTag(0.55f, 0.55f, 0.55f, 1.f);
constexpr ImVec4 kCross(0.85f, 0.85f, 0.85f, 1.f), kCrossLine(0.85f, 0.85f, 0.85f, 0.5f); // 十字光标: 标签底 / 线

ImVec4 tint(double v, double ref) { return v > ref ? kUp : v < ref ? kDn
                                                                   : kFlat; }

// 量 [股] → 手
int fmt_lots(char *buf, int size, double vol) {
  const double h = vol / 100.0;
  return h >= 1e8   ? std::snprintf(buf, size, "%.2f亿手", h / 1e8)
         : h >= 1e4 ? std::snprintf(buf, size, "%.1f万手", h / 1e4)
                    : std::snprintf(buf, size, "%.0f手", h);
}
// 额 [元]
int fmt_amount(char *buf, int size, double a) {
  return a >= 1e8   ? std::snprintf(buf, size, "%.2f亿", a / 1e8)
         : a >= 1e4 ? std::snprintf(buf, size, "%.0f万", a / 1e4)
                    : std::snprintf(buf, size, "%.0f", a);
}
int lots_tick(double v, char *buf, int size, void *) { return v <= 0 ? std::snprintf(buf, size, "0") : fmt_lots(buf, size, v); }
int amount_tick(double v, char *buf, int size, void *) { return v <= 0 ? std::snprintf(buf, size, "0") : fmt_amount(buf, size, v); }
// 右轴: 涨跌幅; 无基准 → 空刻度 (轴照留, 图框不变)
int pct_tick(double v, char *buf, int size, void *ud) {
  return *static_cast<const double *>(ud) > 0 ? std::snprintf(buf, size, "%.1f%%", v) : std::snprintf(buf, size, " ");
}

// 刻度区定尺: 尺寸 = 样板文字的标签框 (同 Tag: 文字 + 两侧 AnnotationPadding), 左 = 价 / 量 / 额, 右 = 涨跌幅, 底 = 时刻.
//   ImPlot 的刻度区每帧以上帧 LateSize 为底再随刻度 / 标签撑大 (Tag 经 OverrideSizeLate 计入);
//   底先垫到最大标签框 ⇒ 现价 / 十字标签随出随没、换标的、缩放都不改图框. 超尺的刻度照实撑开
void pin(ImAxis ax, std::initializer_list<const char *> samples) {
  const ImVec2 pad = ImPlot::GetStyle().AnnotationPadding;
  ImPlotTicker &t = ImPlot::GetCurrentPlot()->Axes[ax].Ticker;
  for (const char *s : samples) {
    const ImVec2 text = ImGui::CalcTextSize(s);
    t.OverrideSizeLate(ImVec2(text.x + 2 * pad.x, text.y + 2 * pad.y));
  }
}
// 当前图各轴定尺 (BeginPlot 之后任意处调; 落到下一帧); x = 底轴有刻度
void pin_axes(bool x) {
  pin(ImAxis_Y1, {"0000.00", "0000.0万手", "00.00亿手", "0000万", "00.00亿"});
  pin(ImAxis_Y2, {"-000.0%", "+00.00%"});
  if (x)
    pin(ImAxis_X1, {"00-00 00:00", "0000-00-00"});
}

// 一张图的根: 日内 x = 第 k 日 · kSlots + 槽 (无成交的槽留空); 日线 x = 下标
struct Series {
  bool intraday = false;
  bool money = false; // 量柱 = 成交额 (篮子: 量不合成)
  std::vector<Candle> bars;
  std::vector<double> x;
  std::vector<misc::Ymd> days; // 日内各日
  double ref = 0;              // 涨跌幅轴基准 (末日前收); 0 = 无
  void push(const Candle &b, double at) {
    bars.push_back(b);
    x.push_back(at);
  }
  double vol(const Candle &b) const { return money ? b.amount : b.volume; }
};

int day_tick(double v, char *buf, int size, void *ud) {
  const Series &s = *static_cast<const Series *>(ud);
  const long i = std::lround(v);
  if (i < 0 || i >= static_cast<long>(s.bars.size()))
    return std::snprintf(buf, size, " ");
  const misc::Ymd d = s.bars[static_cast<size_t>(i)].date;
  return std::snprintf(buf, size, "%02d-%02d", d / 100 % 100, d % 100);
}

int nearest(const Series &s, double at) {
  const int n = static_cast<int>(s.x.size());
  int i = static_cast<int>(std::lower_bound(s.x.begin(), s.x.end(), at) - s.x.begin());
  if (i > 0 && (i == n || s.x[i] - at > at - s.x[i - 1]))
    --i;
  return i;
}

// 一根的时刻: 日内 "MM-DD HH:MM", 日线 "YYYY-MM-DD"
void fmt_when(char *buf, int size, const Series &s, const Candle &b) {
  if (s.intraday) {
    const int m = slot_label(b.slot);
    std::snprintf(buf, size, "%02d-%02d %02d:%02d", b.date / 100 % 100, b.date % 100, m / 60, m % 60);
  } else {
    std::snprintf(buf, size, "%s", misc::dash(b.date).c_str());
  }
}

void tooltip(const Series &s, int i) {
  const Candle &b = s.bars[static_cast<size_t>(i)];
  char when[32], vol[32], amt[32], chg[48] = "";
  fmt_when(when, sizeof(when), s, b);
  if (b.ref > 0)
    std::snprintf(chg, sizeof(chg), "\n涨跌 %+.2f  %+.2f%%", b.close - b.ref, (b.close / b.ref - 1.0) * 100.0);
  fmt_amount(amt, sizeof(amt), b.amount);
  if (s.money) {
    ImGui::SetTooltip("%s\n开 %.2f  高 %.2f\n低 %.2f  收 %.2f%s\n额 %s", when, b.open, b.high, b.low, b.close, chg, amt);
    return;
  }
  fmt_lots(vol, sizeof(vol), b.volume);
  ImGui::SetTooltip("%s\n开 %.2f  高 %.2f\n低 %.2f  收 %.2f%s\n量 %s  额 %s", when, b.open, b.high, b.low, b.close, chg, vol, amt);
}

bool range_bar(const char *const names[], int n, int &cur) {
  bool changed = false;
  for (int i = 0; i < n; ++i) {
    if (i)
      ImGui::SameLine(0, 2);
    const bool on = cur == i;
    if (on)
      ImGui::PushStyleColor(ImGuiCol_Button, ImGui::GetStyleColorVec4(ImGuiCol_ButtonActive));
    if (ImGui::SmallButton(names[i]) && !on) {
      cur = i;
      changed = true;
    }
    if (on)
      ImGui::PopStyleColor();
  }
  return changed;
}

// 单调三次 Hermite (PCHIP 斜率): 过每个点, 段内不越出两端值 (不过冲, 平段仍平); 相邻两点间插 kSub 段.
//   out 依次 = 各段 kSub 个起点 + 末点, 共 (n − 1) · kSub + 1 个
void pchip(const std::vector<double> &x, const std::vector<double> &y, std::vector<double> &out) {
  const size_t n = x.size();
  assert(n > 0 && y.size() == n);
  out.clear();
  std::vector<double> d(n - 1), m(n);
  for (size_t i = 0; i + 1 < n; ++i)
    d[i] = (y[i + 1] - y[i]) / (x[i + 1] - x[i]);
  for (size_t i = 1; i + 1 < n; ++i) {
    const double h0 = x[i] - x[i - 1], h1 = x[i + 1] - x[i], w0 = 2 * h1 + h0, w1 = h1 + 2 * h0;
    m[i] = d[i - 1] * d[i] > 0 ? (w0 + w1) / (w0 / d[i - 1] + w1 / d[i]) : 0.0;
  }
  if (n > 1)
    m[0] = d[0], m[n - 1] = d[n - 2];
  for (size_t i = 0; i + 1 < n; ++i) {
    const double h = x[i + 1] - x[i];
    for (int k = 0; k < kSub; ++k) {
      const double t = static_cast<double>(k) / kSub, t2 = t * t, t3 = t2 * t;
      out.push_back((2 * t3 - 3 * t2 + 1) * y[i] + (t3 - 2 * t2 + t) * h * m[i] + (3 * t2 - 2 * t3) * y[i + 1] + (t3 - t2) * h * m[i + 1]);
    }
  }
  out.push_back(y[n - 1]);
}

// 日内 [a, b) 根: 高 / 低两条插值线围出的带 (上下沿同色描边), 按 带中线 vs 前收 染涨跌色 (无前收 → 平色); 逐日成段, 跨日不连
void band(const Series &s, int a, int b) {
  ImDrawList *dl = ImPlot::GetPlotDrawList();
  const auto color = [&](double mid) {
    const ImVec4 c = s.ref > 0 ? tint(mid, s.ref) : kFlat;
    return ImGui::ColorConvertFloat4ToU32(ImVec4(c.x, c.y, c.z, 0.85f));
  };
  std::vector<double> x, h, l, xs, hs, ls;
  std::vector<ImVec2> ph, pl;
  for (int i = a; i < b;) {
    const int day = static_cast<int>(s.x[i]) / kSlots;
    x.clear(), h.clear(), l.clear();
    for (; i < b && static_cast<int>(s.x[i]) / kSlots == day; ++i) {
      const Candle &k = s.bars[static_cast<size_t>(i)];
      x.push_back(s.x[i]), h.push_back(k.high), l.push_back(k.low);
    }
    pchip(x, x, xs); // x 自身过 pchip = 线性等分
    pchip(x, h, hs);
    pchip(x, l, ls);
    ph.resize(xs.size()), pl.resize(xs.size());
    for (size_t j = 0; j < xs.size(); ++j)
      ph[j] = ImPlot::PlotToPixels(xs[j], hs[j]), pl[j] = ImPlot::PlotToPixels(xs[j], ls[j]);
    if (xs.size() == 1) { // 孤根: 一道竖线
      dl->AddLine(ph[0], pl[0], color((hs[0] + ls[0]) * 0.5));
      continue;
    }
    const ImDrawListFlags flags = dl->Flags;
    dl->Flags &= ~ImDrawListFlags_AntiAliasedFill; // 相邻四边形的抗锯齿边会叠出竖纹
    for (size_t j = 0; j + 1 < xs.size(); ++j)
      dl->AddQuadFilled(ph[j], ph[j + 1], pl[j + 1], pl[j], color((hs[j] + hs[j + 1] + ls[j] + ls[j + 1]) * 0.25));
    dl->Flags = flags;
    for (size_t j = 0; j + 1 < xs.size(); ++j) { // 上下沿描同色边: 带窄于一像素处不断
      const ImU32 c = color((hs[j] + hs[j + 1] + ls[j] + ls[j + 1]) * 0.25);
      dl->AddLine(ph[j], ph[j + 1], c);
      dl->AddLine(pl[j], pl[j + 1], c);
    }
  }
}

// 价 (日内高低带 / 日线蜡烛 + 前收 / 现价线, 日线另叠均线; 右轴涨跌幅) 叠 量, x 联动. key 变 / 双击 → 视野复位到档窗口 [win_lo, win_hi];
//   左键拖动 = 框选 x 段放大; wheel → 滚轮只缩放 x, 右端锚在 win_hi (最新一根); y 恒随视野内的根贴合.
//   视野的改动在下一帧生效 (本帧已设给 ImPlot).
// 档位按钮叠在价图左上角 (在图之后提交, 抢得到悬停). 画完光标停在按钮处, 调用方显式定位下一块
void plot_chart(const char *id, Series &s, double win_lo, double win_hi, uint64_t key, QuotesView &v, const ImVec2 &size,
                const char *const ranges[], int nr, int &cur, bool wheel) {
  const int n = static_cast<int>(s.bars.size());
  const bool reset = v.key != key;
  if (reset) {
    v.key = key;
    v.lo = win_lo, v.hi = win_hi, v.drag = kNaN;
  }
  const int w0 = static_cast<int>(std::lower_bound(s.x.begin(), s.x.end(), v.lo) - s.x.begin());
  const int w1 = static_cast<int>(std::upper_bound(s.x.begin(), s.x.end(), v.hi) - s.x.begin());
  double lo = std::numeric_limits<double>::max(), hi = -lo, vmax = 0;
  for (int i = w0; i < w1; ++i) {
    const Candle &b = s.bars[static_cast<size_t>(i)];
    lo = std::min<double>(lo, b.low), hi = std::max<double>(hi, b.high), vmax = std::max(vmax, s.vol(b));
  }
  if (w0 >= w1)
    lo = hi = s.ref > 0 ? s.ref : 1.0;
  const double pad = (hi > lo ? hi - lo : hi * 0.01) * 0.06, ylo = lo - pad, yhi = hi + pad;
  const auto pct = [&](double p) { return (p / s.ref - 1.0) * 100.0; };

  std::vector<double> tick_x;
  std::vector<std::string> tick_s;
  std::vector<const char *> tick_p;
  std::vector<double> seps;
  if (s.intraday) {
    const int nd = static_cast<int>(s.days.size());
    if (nd <= 1) {
      for (int h = 0; h < 5; ++h)
        tick_x.push_back(h * 60 - 0.5), tick_s.emplace_back(kHours[h]);
    } else {
      for (int k = 0; k < nd; ++k) {
        char buf[8];
        std::snprintf(buf, sizeof(buf), "%02d-%02d", s.days[k] / 100 % 100, s.days[k] % 100);
        tick_x.push_back((k + 0.5) * kSlots - 0.5), tick_s.emplace_back(buf);
        if (k)
          seps.push_back(k * kSlots - 0.5);
      }
    }
    for (const std::string &t : tick_s)
      tick_p.push_back(t.c_str());
  }
  const auto setup_x = [&](bool labels) {
    ImPlot::SetupAxis(ImAxis_X1, nullptr, labels ? ImPlotAxisFlags_None : ImPlotAxisFlags_NoTickLabels);
    ImPlot::SetupAxisLimits(ImAxis_X1, v.lo, v.hi, ImPlotCond_Always);
    if (s.intraday)
      ImPlot::SetupAxisTicks(ImAxis_X1, tick_x.data(), static_cast<int>(tick_x.size()), tick_p.data());
    else
      ImPlot::SetupAxisFormat(ImAxis_X1, day_tick, &s);
  };
  const auto visible = [&](const ImPlotRect &lim, int &a, int &b) {
    a = static_cast<int>(std::lower_bound(s.x.begin(), s.x.end(), lim.X.Min - 1.0) - s.x.begin());
    b = static_cast<int>(std::upper_bound(s.x.begin(), s.x.end(), lim.X.Max + 1.0) - s.x.begin());
  };
  // 每格: 收悬停 / 鼠标 x (价图坐标), 占住滚轮 (不滚外层窗口); 框选中两格同画选区.
  //   十字光标 (非框选时): 竖线吸附到最近一根, 鼠标在两格任一 (上帧) 即两格同画; 返回 = 本格画横线 (鼠标在本格)
  bool hov = false;
  double mx = 0;
  const auto input = [&](bool price) {
    const double clamped = std::clamp(ImPlot::GetPlotMousePos().x, v.lo, v.hi); // 框选端点不出图区
    if (price)
      mx = clamped;
    const bool here = ImPlot::IsPlotHovered();
    if (here) {
      hov = true;
      ImGui::SetKeyOwner(ImGuiKey_MouseWheelY, ImPlot::GetCurrentPlot()->ID);
    }
    if (std::isnan(v.drag)) {
      if ((here || v.cross) && n > 0) {
        const double x = s.x[static_cast<size_t>(nearest(s, ImPlot::GetPlotMousePos().x))];
        ImPlot::SetNextLineStyle(kCrossLine, 1.f);
        ImPlot::PlotInfLines("##cx", &x, 1);
      }
      return here;
    }
    const ImVec2 pos = ImPlot::GetPlotPos(), sz = ImPlot::GetPlotSize();
    const float a = ImPlot::PlotToPixels(v.drag, 0.0).x, b = ImPlot::PlotToPixels(clamped, 0.0).x;
    const ImVec2 p0(std::min(a, b), pos.y), p1(std::max(a, b), pos.y + sz.y);
    ImDrawList *dl = ImPlot::GetPlotDrawList();
    dl->AddRectFilled(p0, p1, IM_COL32(86, 156, 214, 40));
    dl->AddRect(p0, p1, IM_COL32(86, 156, 214, 160));
    return false;
  };
  const auto cross_y = [](double y) {
    ImPlot::SetNextLineStyle(kCrossLine, 1.f);
    ImPlot::PlotInfLines("##cy", &y, 1, ImPlotInfLinesFlags_Horizontal);
  };

  ImPlot::PushStyleVar(ImPlotStyleVar_PlotPadding, ImVec2(2, 2)); // 贴边: 图区让给蜡烛
  ImPlot::PushStyleVar(ImPlotStyleVar_LabelPadding, ImVec2(2, 1));
  ImPlot::PushStyleVar(ImPlotStyleVar_PlotBorderSize, 0.f);
  if (!ImPlot::BeginSubplots(id, 2, 1, size, ImPlotSubplotFlags_NoTitle | ImPlotSubplotFlags_NoMenus | ImPlotSubplotFlags_LinkCols, v.rows)) {
    ImPlot::PopStyleVar(3);
    return;
  }

  ImVec2 corner(-1, -1); // 价图区左上角
  if (ImPlot::BeginPlot("##px", ImVec2(), ImPlotFlags_NoMenus | ImPlotFlags_NoBoxSelect | (s.intraday ? ImPlotFlags_NoLegend : 0))) {
    ImPlot::SetupLegend(ImPlotLocation_NorthEast, ImPlotLegendFlags_Horizontal);
    setup_x(false);
    ImPlot::SetupAxisLimits(ImAxis_Y1, ylo, yhi, ImPlotCond_Always);
    ImPlot::SetupAxisFormat(ImAxis_Y1, "%.2f");
    ImPlot::SetupAxis(ImAxis_Y2, nullptr, ImPlotAxisFlags_AuxDefault);
    ImPlot::SetupAxisLimits(ImAxis_Y2, s.ref > 0 ? pct(ylo) : 0.0, s.ref > 0 ? pct(yhi) : 1.0, ImPlotCond_Always);
    ImPlot::SetupAxisFormat(ImAxis_Y2, pct_tick, &s.ref);
    pin_axes(false);
    ImPlot::SetupFinish();
    corner = ImPlot::GetPlotPos();
    const ImPlotRect lim = ImPlot::GetPlotLimits();
    // 水平线的两端标签: 左 = 价, 右 = 对前收的涨跌幅 (右轴同一基准)
    const auto tags = [&](double y, const ImVec4 &c) {
      ImPlot::TagY(y, c, "%.2f", y);
      if (!(s.ref > 0))
        return;
      ImPlot::SetAxes(ImAxis_X1, ImAxis_Y2);
      ImPlot::TagY(pct(y), c, "%+.2f%%", pct(y));
      ImPlot::SetAxes(ImAxis_X1, ImAxis_Y1);
    };

    if (!seps.empty()) {
      ImPlot::SetNextLineStyle(kRule, 1.f);
      ImPlot::PlotInfLines("##sep", seps.data(), static_cast<int>(seps.size()));
    }
    if (s.intraday && s.ref > 0) {
      ImPlot::SetNextLineStyle(kRule, 1.f);
      ImPlot::PlotInfLines("##ref", &s.ref, 1, ImPlotInfLinesFlags_Horizontal);
      tags(s.ref, kRefTag);
    }
    std::vector<double> ma(static_cast<size_t>(n));
    for (int k = 0; k < 3 && !s.intraday; ++k) {
      const int N = kMaN[k];
      double sum = 0;
      for (int i = 0; i < n; ++i) {
        sum += s.bars[static_cast<size_t>(i)].close;
        if (i >= N)
          sum -= s.bars[static_cast<size_t>(i - N)].close;
        ma[static_cast<size_t>(i)] = sum / N;
      }
      const int m = std::max(0, n - N + 1);
      ImPlot::SetNextLineStyle(kMaCol[k], 1.f);
      ImPlot::PlotLine(kMaName[k], s.x.data() + (n - m), ma.data() + (n - m), m, ImPlotItemFlags_NoFit);
    }
    if (n > 0 && ImPlot::BeginItem("##k", ImPlotItemFlags_NoLegend)) { // 日内 = 高低带; 日线 = 蜡烛 (阳线空心 / 阴线实心)
      if (ImPlot::FitThisFrame())
        for (int i = 0; i < n; ++i) {
          ImPlot::FitPoint(ImPlotPoint(s.x[i], s.bars[static_cast<size_t>(i)].low));
          ImPlot::FitPoint(ImPlotPoint(s.x[i], s.bars[static_cast<size_t>(i)].high));
        }
      ImDrawList *dl = ImPlot::GetPlotDrawList();
      int a, b;
      visible(lim, a, b);
      if (s.intraday)
        band(s, std::max(0, a - 2), std::min(n, b + 2)); // 多带两根: 视野内各点的斜率取到真邻点
      else
        for (int i = a; i < b; ++i) {
          const Candle &k = s.bars[static_cast<size_t>(i)];
          const ImU32 col = ImGui::ColorConvertFloat4ToU32(k.close >= k.open ? kUp : kDn);
          dl->AddLine(ImPlot::PlotToPixels(s.x[i], k.low), ImPlot::PlotToPixels(s.x[i], k.high), col);
          ImVec2 p0 = ImPlot::PlotToPixels(s.x[i] - kBody, k.open), p1 = ImPlot::PlotToPixels(s.x[i] + kBody, k.close);
          if (std::abs(p1.y - p0.y) < 1.f) {
            const float mid = (p0.y + p1.y) * 0.5f;
            p0.y = mid - 0.5f, p1.y = mid + 0.5f;
          }
          if (k.close >= k.open)
            dl->AddRect(ImVec2(std::min(p0.x, p1.x), std::min(p0.y, p1.y)), ImVec2(std::max(p0.x, p1.x), std::max(p0.y, p1.y)), col);
          else
            dl->AddRectFilled(ImVec2(std::min(p0.x, p1.x), std::min(p0.y, p1.y)), ImVec2(std::max(p0.x, p1.x), std::max(p0.y, p1.y)), col);
        }
      ImPlot::EndItem();
    }
    if (n > 0) {
      const double last = s.bars.back().close;
      const ImVec4 c = tint(last, s.ref > 0 ? s.ref : last);
      ImPlot::SetNextLineStyle(ImVec4(c.x, c.y, c.z, 0.55f), 1.f);
      ImPlot::PlotInfLines("##last", &last, 1, ImPlotInfLinesFlags_Horizontal);
      tags(last, c);
    }
    if (input(true)) { // 十字横线: 左标价 / 右标涨跌幅
      const double y = ImPlot::GetPlotMousePos().y;
      cross_y(y);
      tags(y, kCross);
    }
    ImPlot::EndPlot();
  }

  if (ImPlot::BeginPlot("##vol", ImVec2(), ImPlotFlags_NoMenus | ImPlotFlags_NoBoxSelect | ImPlotFlags_NoLegend)) {
    setup_x(true);
    ImPlot::SetupAxisLimits(ImAxis_Y1, 0, vmax > 0 ? vmax * 1.1 : 1.0, ImPlotCond_Always);
    ImPlot::SetupAxisFormat(ImAxis_Y1, s.money ? amount_tick : lots_tick, nullptr);
    pin_axes(true);
    ImPlot::SetupFinish();
    if (!seps.empty()) {
      ImPlot::SetNextLineStyle(kRule, 1.f);
      ImPlot::PlotInfLines("##sep", seps.data(), static_cast<int>(seps.size()));
    }
    if (n > 0 && ImPlot::BeginItem("##v", ImPlotItemFlags_NoLegend)) {
      ImDrawList *dl = ImPlot::GetPlotDrawList();
      int a, b;
      visible(ImPlot::GetPlotLimits(), a, b);
      for (int i = a; i < b; ++i) {
        const Candle &k = s.bars[static_cast<size_t>(i)];
        const ImVec4 c = k.close >= k.open ? kUp : kDn;
        const ImVec2 p0 = ImPlot::PlotToPixels(s.x[i] - kBody, s.vol(k)), p1 = ImPlot::PlotToPixels(s.x[i] + kBody, 0.0);
        dl->AddRectFilled(ImVec2(std::min(p0.x, p1.x), p0.y), ImVec2(std::max(p0.x, p1.x) + 1.f, p1.y),
                          ImGui::ColorConvertFloat4ToU32(ImVec4(c.x, c.y, c.z, 0.75f)));
      }
      ImPlot::EndItem();
    }
    if (n > 0 && ImPlot::IsPlotHovered()) // 悬停提示只在量图: 价图留给框选 / 十字读价
      tooltip(s, nearest(s, ImPlot::GetPlotMousePos().x));
    if (input(false)) { // 十字横线: 左标量 / 额
      const double y = ImPlot::GetPlotMousePos().y;
      cross_y(y);
      ImPlot::TagY(y, kCross, false);
    }
    if (std::isnan(v.drag) && (hov || v.cross) && n > 0) { // 十字竖线落在底轴的时刻
      const size_t i = static_cast<size_t>(nearest(s, ImPlot::GetPlotMousePos().x));
      char when[32];
      fmt_when(when, sizeof(when), s, s.bars[i]);
      ImPlot::TagX(s.x[i], kCross, "%s", when);
    }
    ImPlot::EndPlot();
  }
  ImPlot::EndSubplots();
  ImPlot::PopStyleVar(3);
  v.cross = hov;

  const ImGuiIO &io = ImGui::GetIO();
  if (hov && ImGui::IsMouseDoubleClicked(ImGuiMouseButton_Left))
    v.lo = win_lo, v.hi = win_hi, v.drag = kNaN;
  else if (hov && ImGui::IsMouseClicked(ImGuiMouseButton_Left))
    v.drag = mx, v.drag_px = io.MousePos.x;
  if (!std::isnan(v.drag)) {
    ImGui::SetMouseCursor(ImGuiMouseCursor_ResizeEW); // 盖掉 ImPlot 的 "不可拖动" 光标
    if (ImGui::IsMouseReleased(ImGuiMouseButton_Left)) {
      if (std::abs(io.MousePos.x - v.drag_px) > kDragPx && std::abs(mx - v.drag) >= kMinSpan)
        v.lo = std::min(v.drag, mx), v.hi = std::max(v.drag, mx);
      v.drag = kNaN;
    }
  }
  if (wheel && hov && io.MouseWheel != 0) {
    const double w = std::clamp((v.hi - v.lo) * std::pow(kWheel, io.MouseWheel), kMinSpan, std::max(win_hi - win_lo, n + 1.0));
    v.lo = win_hi - w, v.hi = win_hi;
  }
  if (corner.x >= 0) {
    ImGui::SetCursorScreenPos(ImVec2(corner.x + 4, corner.y + 4));
    range_bar(ranges, nr, cur);
  }
}

// 一档金额 [元] → "XX万" / "X.XX亿"
int fmt_wan(char *buf, int size, double a) {
  const double w = a / 1e4;
  return w >= 1e4 ? std::snprintf(buf, size, "%.2f亿", w / 1e4) : w >= 100 ? std::snprintf(buf, size, "%.0f万", w)
                                                                           : std::snprintf(buf, size, "%.1f万", w);
}

// 五档 (卖5 → 卖1 / 买1 → 买5): 价 / 涨幅按昨收着色, 额 = 价 × 量, 底条 = 该档额 / 十档最大; 下接摘要. date = 行情日
void render_book(const sina::Quote &q, misc::Ymd date) {
  const double ref = q.last_close;
  if (q.sym.empty()) {
    ImGui::TextDisabled("等待快照…");
    return;
  }
  double wmax = 0;
  for (int k = 0; k < sina::kLevels; ++k)
    wmax = std::max({wmax, q.bid[k] * q.bid_vol[k], q.ask[k] * q.ask_vol[k]});
  constexpr const char *kAsk[] = {"卖1", "卖2", "卖3", "卖4", "卖5"}, *kBid[] = {"买1", "买2", "买3", "买4", "买5"};
  ImGui::PushStyleVar(ImGuiStyleVar_CellPadding, ImVec2(3, 0));
  if (ImGui::BeginTable("##book", 4, ImGuiTableFlags_SizingFixedFit | ImGuiTableFlags_NoSavedSettings)) {
    ImGui::TableSetupColumn("档");
    ImGui::TableSetupColumn("价");
    ImGui::TableSetupColumn("额", ImGuiTableColumnFlags_WidthStretch);
    ImGui::TableSetupColumn("涨幅");
    const auto row = [&](const char *name, float px, double vol, bool last) {
      ImGui::TableNextRow();
      ImGui::TableSetColumnIndex(0);
      ImGui::TextDisabled("%s", name);
      if (!(px > 0)) {
        ImGui::TableSetColumnIndex(1);
        ImGui::TextDisabled("-");
        return;
      }
      const ImVec4 c = tint(px, ref);
      ImGui::TableSetColumnIndex(1);
      ImGui::TextColored(c, "%.2f", px);
      ImGui::TableSetColumnIndex(2);
      if (wmax > 0) {
        const ImVec2 p = ImGui::GetCursorScreenPos();
        const float w = ImGui::GetContentRegionAvail().x * static_cast<float>(px * vol / wmax);
        ImGui::GetWindowDrawList()->AddRectFilled(p, ImVec2(p.x + w, p.y + ImGui::GetTextLineHeight()), IM_COL32(86, 156, 214, 56));
      }
      char amt[24];
      fmt_wan(amt, sizeof(amt), px * vol);
      ImGui::TextUnformatted(amt);
      ImGui::TableSetColumnIndex(3);
      if (ref > 0)
        ImGui::TextColored(c, "%+.2f%%", (px / ref - 1.0) * 100.0);
      if (last) // 卖 / 买 分界
        ImGui::GetWindowDrawList()->AddLine(ImVec2(ImGui::GetWindowPos().x, ImGui::GetItemRectMax().y + 1.f),
                                            ImVec2(ImGui::GetWindowPos().x + ImGui::GetWindowWidth(), ImGui::GetItemRectMax().y + 1.f),
                                            ImGui::GetColorU32(kRule));
    };
    for (int k = sina::kLevels - 1; k >= 0; --k)
      row(kAsk[k], q.ask[k], q.ask_vol[k], k == 0);
    for (int k = 0; k < sina::kLevels; ++k)
      row(kBid[k], q.bid[k], q.bid_vol[k], false);
    ImGui::EndTable();
  }
  ImGui::Spacing();
  if (ImGui::BeginTable("##kv", 4, ImGuiTableFlags_SizingStretchProp | ImGuiTableFlags_NoSavedSettings)) {
    ImGui::TableSetupColumn("k0", ImGuiTableColumnFlags_WidthFixed);
    ImGui::TableSetupColumn("v0", ImGuiTableColumnFlags_WidthStretch);
    ImGui::TableSetupColumn("k1", ImGuiTableColumnFlags_WidthFixed);
    ImGui::TableSetupColumn("v1", ImGuiTableColumnFlags_WidthStretch);
    char vol[32], amt[32];
    fmt_lots(vol, sizeof(vol), q.volume);
    fmt_amount(amt, sizeof(amt), q.amount);
    const auto px = [&](const char *k, float v) {
      ImGui::TableNextColumn();
      ImGui::TextDisabled("%s", k);
      ImGui::TableNextColumn();
      if (v > 0)
        ImGui::TextColored(tint(v, ref), "%.2f", v);
      else
        ImGui::TextDisabled("-");
    };
    const auto txt = [&](const char *k, const char *v) {
      ImGui::TableNextColumn();
      ImGui::TextDisabled("%s", k);
      ImGui::TableNextColumn();
      ImGui::TextUnformatted(v);
    };
    px("今开", q.open);
    px("最高", q.high);
    px("最低", q.low);
    ImGui::TableNextColumn();
    ImGui::TextDisabled("昨收");
    ImGui::TableNextColumn();
    ImGui::Text("%.2f", q.last_close);
    txt("量", vol);
    txt("额", amt);
    char amp[16] = "-";
    if (ref > 0 && q.high > 0)
      std::snprintf(amp, sizeof(amp), "%.2f%%", (q.high - q.low) / ref * 100.0);
    txt("振幅", amp);
    char when[16];
    std::snprintf(when, sizeof(when), "%02d:%02d:%02d", q.sec / 3600, q.sec / 60 % 60, q.sec % 60);
    txt(q.date == date ? "时刻" : misc::dash(q.date).c_str(), when);
    ImGui::EndTable();
  }
  ImGui::PopStyleVar();
}

// 最新一根: 当日接上取当日, 否则日线末根; 日线空 → nullptr
const Candle *last(const Kline &kl, const Day &today) {
  return joins(kl, today) ? &today.candle : kl.daily.empty() ? nullptr
                                                             : &kl.daily.back();
}

// 两张图的根 = 历史 + 当日 (joins 才接); 日内取末 days 日, 分钟根的前收 = 同日日根的; 两图涨跌基准同为末日前收
void build(const Kline &kl, const Day &today, int days, bool money, Series &intra, Series &daily) {
  const bool on = joins(kl, today);
  intra.intraday = true;
  intra.money = daily.money = money;
  for (const Candle &c : kl.daily)
    daily.push(c, static_cast<double>(daily.bars.size()));
  if (on)
    daily.push(today.candle, static_cast<double>(daily.bars.size()));
  std::vector<const Bars *> src;
  for (const Bars &b : kl.minutes)
    src.push_back(&b);
  if (on && today.bars.has.any())
    src.push_back(&today.bars);
  const size_t nd = std::min(static_cast<size_t>(days), src.size());
  for (size_t k = 0; k < nd; ++k) {
    const Bars &b = *src[src.size() - nd + k];
    const auto it = std::lower_bound(daily.bars.begin(), daily.bars.end(), b.date,
                                     [](const Candle &c, misc::Ymd d) { return c.date < d; });
    const float ref = it != daily.bars.end() && it->date == b.date ? it->ref : 0;
    intra.days.push_back(b.date);
    intra.ref = ref;
    for (int s = 0; s < kSlots; ++s)
      if (b.has[static_cast<size_t>(s)])
        intra.push({b.cell[static_cast<size_t>(s)], b.date, s, ref}, static_cast<double>(k) * kSlots + s);
  }
  if (!daily.bars.empty())
    daily.ref = daily.bars.back().ref;
}

// 当日没接上 (历史落后) → 提示写进 buf, true
bool gap_note(const Kline &kl, const Day &today, char *buf, size_t size) {
  if (!today.date || joins(kl, today) || today.date <= kl.last)
    return false;
  std::snprintf(buf, size, "历史止于 %02d-%02d, 未接当日 %02d-%02d", kl.last / 100 % 100, kl.last % 100, today.date / 100 % 100,
                today.date % 100);
  return true;
}

// 标题行 (标题 · 最新价 / 涨跌) + 注行: 恒两行, 图区起点不随内容动 (注行末可再 SameLine 一个 SmallButton, 同高)
void header(const char *title, const Kline &kl, const Day &today, const char *note) {
  ImGui::TextUnformatted(title);
  if (const Candle *k = last(kl, today); k && k->ref > 0) {
    ImGui::SameLine();
    ImGui::TextColored(tint(k->close, k->ref), "%.2f  %+.2f  %+.2f%%", k->close, k->close - k->ref, (k->close / k->ref - 1.0) * 100.0);
  }
  ImGui::TextDisabled("%s", note);
}

// 两图: id = 标的的复位键高位, 档 / 根数拼进低位
void plot_intraday(const char *name, Series &s, uint64_t id, ChartState &st, const ImVec2 &size) {
  const int nd = static_cast<int>(s.days.size());
  const uint64_t key = id | static_cast<uint64_t>(st.intraday) << 16 | static_cast<uint64_t>(nd);
  plot_chart(name, s, -0.5, std::max(1, nd) * kSlots - 0.5, key, st.iv, size, kIntraName, 4, st.intraday, false);
}
// 日线档窗口右端 = 最新一根; 不足一档的历史靠右, 左侧留空
void plot_daily(const char *name, Series &s, uint64_t id, ChartState &st, const ImVec2 &size) {
  const int n = static_cast<int>(s.bars.size()), span = kDailySpan[st.daily];
  const uint64_t key = id | static_cast<uint64_t>(st.daily) << 24 | static_cast<uint64_t>(n);
  plot_chart(name, s, n - span - 0.5, n - 0.5, key, st.dv, size, kDailyName, 4, st.daily, true);
}

// 行情页的图区: 上 日内 (book 非空 → 右挂 1/3 宽五档, 上下 6 : 4; 否则通栏, 上下对半) / 下 日线.
//   显式定位 (档位按钮叠在图上, 画完光标不在流式位置)
void charts(const char *name, Series &intra, Series &daily, uint64_t id, ChartState &st, const sina::Quote *book, misc::Ymd date) {
  ImGui::PushID(name);
  const float gap = 4.f;
  const ImVec2 p0 = ImGui::GetCursorScreenPos(), avail = ImGui::GetContentRegionAvail();
  const float bw = book ? std::floor((avail.x - gap) / 3.f) : 0.f, iw = book ? avail.x - bw - gap : avail.x;
  const float top_h = std::floor((avail.y - gap) * (book ? 0.6f : 0.5f));
  plot_intraday("##ichart", intra, id, st, ImVec2(iw, top_h));
  if (book) {
    ImGui::SetCursorScreenPos(ImVec2(p0.x + iw + gap, p0.y));
    ImGui::PushStyleVar(ImGuiStyleVar_WindowPadding, ImVec2(0, 0));
    ImGui::BeginChild("##book", ImVec2(bw, top_h), false);
    render_book(*book, date);
    ImGui::EndChild();
    ImGui::PopStyleVar();
  }
  ImGui::SetCursorScreenPos(ImVec2(p0.x, p0.y + top_h + gap));
  plot_daily("##dchart", daily, id, st, ImVec2(avail.x, avail.y - top_h - gap));
  ImGui::PopID();
}

// 收历史发布: 末次请求的发布到齐 → 整体换上 (true), 当日待重拷; 到齐前照留在画的
template <class T>
bool pull(const HistoryPub<T> &x, Feed<T> &f) {
  std::lock_guard<std::mutex> lock(x.mutex);
  if (x.gen != f.gen || x.data == f.data)
    return false;
  f.data = x.data, f.shown = f.gen, f.ver = ~0ull;
  return true;
}

} // namespace

void SyncStock(QuoteService &live, StockHistory &hist, const AssetItem *asset, const StockInfo *info, StockState &st) {
  const std::string code = asset ? asset->asset_code + "." + asset->exchange : std::string();
  if (code != st.want.code) {
    st.want = {code, (info && !info->name.empty() ? info->name : std::string("-")) + "  " + code, asset ? live.find(code) : ~0u};
    ++st.feed.gen;
    if (asset)
      hist.Request({st.feed.gen, code});
    live.select(st.want.i);
  }
  if (pull(hist.published(), st.feed))
    st.shown = st.want;
  if (st.shown.code.empty())
    return;
  const uint64_t ver = live.status().stock_ver;
  if (ver == st.feed.ver)
    return;
  st.feed.ver = ver;
  if (st.shown.i != ~0u)
    live.today(st.shown.i, st.today, st.quote);
  else
    st.today.date = 0, st.quote = {};
}

void SyncBaskets(const QuoteService &live, BasketHistory &hist, const Config &cfg, BasketState &st) {
  if (st.want) {
    st.want = false;
    FeatureLib lib = load_lib(cfg, kAllBar);
    st.missing = !lib.ready();
    if (!st.missing)
      hist.Request({++st.feed.gen, std::move(lib)});
  }
  pull(hist.published(), st.feed);
  if (!st.feed.data || st.feed.data->list.empty())
    return;
  const uint64_t ver = live.status().basket_ver;
  if (ver == st.feed.ver)
    return;
  st.feed.ver = ver;
  st.today.resize(st.feed.data->list.size());
  live.today(st.today);
}

void RenderStockPanel(const QuoteService &live, StockState &st) {
  const QuoteStatus qs = live.status();
  char note[160];
  if (qs.date == 0)
    std::snprintf(note, sizeof(note), "新浪: 等待首轮快照…");
  else if (qs.fill)
    std::snprintf(note, sizeof(note), "新浪 %02d:%02d:%02d · 分钟 K 已补 %zu / 待补 %zu", qs.sec / 3600, qs.sec / 60 % 60, qs.sec % 60,
                  qs.fetched, qs.pending);
  else
    std::snprintf(note, sizeof(note), "新浪 %02d:%02d:%02d · 当日分钟由 bar1m 给", qs.sec / 3600, qs.sec / 60 % 60, qs.sec % 60);
  if (st.shown.code.empty()) {
    ImGui::TextDisabled("%s", note);
    ImGui::TextWrapped(st.want.code.empty() ? "点表格中的一行查看行情." : "读取历史…");
    return;
  }
  const Kline &kl = *st.feed.data;
  Series intra, daily;
  build(kl, st.today, kIntraDays[st.chart.intraday], false, intra, daily);

  if (st.want.code != st.shown.code)
    std::snprintf(note, sizeof(note), "读取 %s 历史…", st.want.code.c_str());
  else if (st.shown.i == ~0u)
    std::snprintf(note, sizeof(note), "不在新浪全市场");
  else
    gap_note(kl, st.today, note, sizeof(note));
  header(st.shown.title.c_str(), kl, st.today, note);
  charts("stock", intra, daily, static_cast<uint64_t>(st.feed.shown) << 32, st.chart, &st.quote, qs.date);
}

void RenderBasketPanel(const QuoteService &live, BasketState &st, int page) {
  const int sel = st.sel[page];
  if (!st.feed.data || st.feed.data->list.empty() || sel < 0) {
    ImGui::TextDisabled("点左栏一行查看走势.");
    return;
  }
  const QuoteStatus qs = live.status();
  const size_t b = static_cast<size_t>(sel);
  const Baskets &d = *st.feed.data;
  const Basket &bk = d.list[b];
  assert(st.today.size() == d.list.size());
  const Day &today = st.today[b];
  Series intra, daily;
  build(bk.kline, today, kIntraDays[st.chart.intraday], true, intra, daily);

  char note[160];
  if (joins(bk.kline, today))
    std::snprintf(note, sizeof(note), "当日由新浪快照合成 %02d:%02d:%02d · 成员 / 权重取 %02d-%02d 收盘 (%zu 只)", qs.sec / 3600,
                  qs.sec / 60 % 60, qs.sec % 60, bk.kline.last / 100 % 100, bk.kline.last % 100, bk.size);
  else if (!gap_note(bk.kline, today, note, sizeof(note)))
    std::snprintf(note, sizeof(note), "all.bar 截至 %02d-%02d", bk.kline.last / 100 % 100, bk.kline.last % 100);
  header(bk.name.c_str(), bk.kline, today, note);
  ImGui::SameLine();
  if (ImGui::SmallButton("重载"))
    st.want = true;
  charts("basket", intra, daily, static_cast<uint64_t>(st.feed.shown) << 48 | static_cast<uint64_t>(b) << 32, st.chart, nullptr, qs.date);
}

bool RenderBasketList(const BasketHistory &hist, BasketState &st, int page) {
  if (st.missing || (st.feed.data && st.feed.data->list.empty())) {
    ImGui::TextWrapped(st.missing ? "指数 / 行业由 all.bar 特征库合成: 先在 Features → Compute 算好 all 池的 Bar 库."
                                  : "all.bar 特征库落盘不足 2 日.");
    if (ImGui::SmallButton("重载"))
      st.want = true;
    return false;
  }
  if (!st.feed.data) {
    const BasketKline &x = hist.published();
    ImGui::TextDisabled("读取 all.bar 特征库… %zu / %zu 日", x.done.load(std::memory_order_relaxed), x.total.load(std::memory_order_relaxed));
    return false;
  }
  const Baskets &d = *st.feed.data;
  struct Row {
    size_t b;
    Candle k; // 最新一根; 日线空 → ref 0, 行只显示名称
  };
  std::vector<Row> rows;
  for (size_t b = 0; b < d.list.size(); ++b)
    if (d.list[b].industry == (page == 1)) {
      const Candle *k = last(d.list[b].kline, st.today[b]);
      rows.push_back({b, k ? *k : Candle{}});
    }

  if (!ImGui::BeginTable("##baskets", 5,
                         ImGuiTableFlags_RowBg | ImGuiTableFlags_BordersInnerV | ImGuiTableFlags_ScrollY | ImGuiTableFlags_Sortable |
                             ImGuiTableFlags_SizingFixedFit | ImGuiTableFlags_NoSavedSettings))
    return false;
  ImGui::TableSetupScrollFreeze(0, 1);
  ImGui::TableSetupColumn("名称", ImGuiTableColumnFlags_DefaultSort | ImGuiTableColumnFlags_PreferSortAscending);
  ImGui::TableSetupColumn("点位", ImGuiTableColumnFlags_PreferSortDescending);
  ImGui::TableSetupColumn("涨幅", ImGuiTableColumnFlags_PreferSortDescending);
  ImGui::TableSetupColumn("成交额", ImGuiTableColumnFlags_PreferSortDescending);
  ImGui::TableSetupColumn("成分", ImGuiTableColumnFlags_PreferSortDescending);
  ImGui::TableHeadersRow();
  if (const ImGuiTableSortSpecs *ss = ImGui::TableGetSortSpecs(); ss && ss->SpecsCount > 0) { // 名称 = 定义序
    const int c = ss->Specs[0].ColumnIndex;
    const bool asc = ss->Specs[0].SortDirection == ImGuiSortDirection_Ascending;
    const auto key = [&](const Row &r) -> double {
      switch (c) {
      case 1:
        return r.k.close;
      case 2:
        return r.k.ref > 0 ? r.k.close / r.k.ref : 0;
      case 3:
        return r.k.amount;
      case 4:
        return static_cast<double>(d.list[r.b].size);
      default:
        return static_cast<double>(r.b);
      }
    };
    std::stable_sort(rows.begin(), rows.end(), [&](const Row &a, const Row &b) { return asc ? key(a) < key(b) : key(a) > key(b); });
  }

  bool picked = false;
  for (const Row &r : rows) {
    const Basket &bk = d.list[r.b];
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    ImGui::PushID(static_cast<int>(r.b));
    if (ImGui::Selectable(bk.name.c_str(), st.sel[page] == static_cast<int>(r.b), ImGuiSelectableFlags_SpanAllColumns)) {
      st.sel[page] = static_cast<int>(r.b);
      picked = true;
    }
    ImGui::PopID();
    if (!(r.k.ref > 0))
      continue;
    const ImVec4 c = tint(r.k.close, r.k.ref);
    ImGui::TableSetColumnIndex(1);
    ImGui::TextColored(c, "%.2f", r.k.close);
    ImGui::TableSetColumnIndex(2);
    ImGui::TextColored(c, "%+.2f%%", (r.k.close / r.k.ref - 1.0) * 100.0);
    ImGui::TableSetColumnIndex(3);
    char amt[32];
    fmt_amount(amt, sizeof(amt), r.k.amount);
    ImGui::TextUnformatted(amt);
    ImGui::TableSetColumnIndex(4);
    ImGui::Text("%zu", bk.size);
  }
  ImGui::EndTable();
  return picked;
}

} // namespace GUI::Database
