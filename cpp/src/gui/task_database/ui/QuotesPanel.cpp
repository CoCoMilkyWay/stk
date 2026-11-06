#include "gui/task_database/ui/QuotesPanel.hpp"
#include "gui/util/Chart.hpp" // kUp / kDn / tint / button_bar / EndTag
#include "shared/AssetAxis.hpp"

#include "imgui.h"
#include "implot.h"
#include "implot_internal.h"

#include <algorithm>
#include <cassert>
#include <cfloat>
#include <cmath>
#include <cstdio>
#include <initializer_list>
#include <limits>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace GUI::Database {

namespace {

constexpr ImVec4 kRule(0.55f, 0.55f, 0.55f, 0.45f);
constexpr int kMaN[] = {5, 10, 20};
constexpr const char *kMaName[] = {"MA5", "MA10", "MA20"};
constexpr ImVec4 kMaCol[] = {ImVec4(0.843f, 0.729f, 0.490f, 1.f), ImVec4(0.306f, 0.788f, 0.690f, 1.f),
                             ImVec4(0.773f, 0.525f, 0.753f, 1.f)};
constexpr int kIntras = 5;
constexpr int kIntraDays[kIntras] = {1, 2, 3, 4, 5};
constexpr const char *kIntraName[kIntras] = {"日内", "2日", "3日", "4日", "5日"};
static_assert(kIntraDays[kIntras - 1] <= static_cast<int>(kMinuteDays));
constexpr const char *kFlowUnitName[] = {"占比", "金额"};
constexpr int kDailySpan[] = {5, 22, 250, 2500}; // 交易日
constexpr const char *kDailyName[] = {"周", "月", "年", "10年"};
static_assert(kDailySpan[3] <= static_cast<int>(kDailyDays));
constexpr const char *kHours[] = {"09:30", "10:30", "11:30/13:00", "14:00", "15:00"};
constexpr double kBody = 0.35; // 蜡烛 / 量柱半宽 [槽]
constexpr float kMinPx = 3.f;  // 日内蜡烛每根最窄 [px] (窄了并根)
constexpr double kMinSpan = 5; // 视野至少几根 (框选 / 滚轮)
constexpr double kWheel = 0.8; // 滚轮一格视野宽 × kWheel (向前 = 放大)
constexpr float kDragPx = 4.f; // 框选生效的最小拖动 [px]
constexpr double kNaN = std::numeric_limits<double>::quiet_NaN();
constexpr ImVec4 kRefTag(0.55f, 0.55f, 0.55f, 1.f);
constexpr ImVec4 kGridTag(0.62f, 0.62f, 0.62f, 0.8f);                                     // 叠加图横网格的内标小字
constexpr float kTagScale = 0.8f, kGridPx = 28.f;                                         // 叠加图内标小字字号 (× 字体) / 横网格约每格像素
constexpr ImVec4 kCross(0.85f, 0.85f, 0.85f, 1.f), kCrossLine(0.85f, 0.85f, 0.85f, 0.5f); // 十字光标: 标签底 / 线

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

// 一张图的根: 日内 x = 第 k 日 · kSlots + 槽 (无成交的槽留空; 并根 = 组中点, 每根 g 槽); 日线 x = 下标
struct Series {
  bool intraday = false;
  bool money = false; // 量柱 = 成交额 (篮子: 量不合成)
  std::vector<Candle> bars;
  std::vector<double> x;
  std::vector<misc::Ymd> days; // 日内各日
  double ref = 0;              // 涨跌幅轴基准 (末日前收); 0 = 无
  int g = 1;
  void push(const Candle &b, double at) {
    bars.push_back(b);
    x.push_back(at);
  }
  double vol(const Candle &b) const { return money ? b.amount : b.volume; }
  // 第 i 根实体起点 = 上一根收 (根根首尾相接); 首根 = 前收 (无 → 开)
  double from(size_t i) const { return i ? bars[i - 1].close : bars[0].ref > 0 ? bars[0].ref
                                                                               : bars[0].open; }
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

// 第 i 根的时刻: 日内 "MM-DD HH:MM" (槽 = 组末槽除 kSlots 的余), 日线 "YYYY-MM-DD"
void fmt_when(char *buf, int size, const Series &s, size_t i) {
  const misc::Ymd d = s.bars[i].date;
  if (s.intraday) {
    const int m = slot_label(static_cast<int>(s.x[i] + (s.g - 1) * 0.5) % kSlots);
    std::snprintf(buf, size, "%02d-%02d %02d:%02d", d / 100 % 100, d % 100, m / 60, m % 60);
  } else {
    std::snprintf(buf, size, "%s", misc::dash(d).c_str());
  }
}

void tooltip(const Series &s, int i) {
  const Candle &b = s.bars[static_cast<size_t>(i)];
  char when[32], vol[32], amt[32], chg[48] = "";
  fmt_when(when, sizeof(when), s, static_cast<size_t>(i));
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

// 一根实心蜡烛 (当前轴坐标): 实体 from → close, 涨红跌绿 (close vs from), 体至少 1 px; 影线 low → high, 半宽 half
void candle(ImDrawList *dl, double x, double half, double from, double high, double low, double close) {
  const ImU32 col = ImGui::ColorConvertFloat4ToU32(close >= from ? kUp : kDn);
  const float cx = std::round(ImPlot::PlotToPixels(x, 0.0).x) + 0.5f;
  const float x0 = ImPlot::PlotToPixels(x - half, 0.0).x, x1 = ImPlot::PlotToPixels(x + half, 0.0).x;
  const float yo = ImPlot::PlotToPixels(0.0, from).y, yc = ImPlot::PlotToPixels(0.0, close).y;
  dl->AddLine(ImVec2(cx, ImPlot::PlotToPixels(0.0, high).y), ImVec2(cx, ImPlot::PlotToPixels(0.0, low).y), col);
  const float y0 = std::min(yo, yc), y1 = std::max(std::max(yo, yc), y0 + 1.f);
  dl->AddRectFilled(ImVec2(std::min(x0, cx - 0.5f), y0), ImVec2(std::max(x1, cx + 0.5f), y1), col);
}

// 日内蜡烛并根 (同 py/app/tdx): g = span 槽在 px 宽里每根不窄于 kMinPx 的最小 kSlots 因子;
//   同日每 g 槽并一根 (开取先, 高 / 低取极, 收取后, 量 / 额相加), x = 组中点. 返回 g
int merge(Series &s, double span, float px) {
  int g = 1;
  while (g < kSlots && (kSlots % g || px * g < kMinPx * span))
    ++g;
  if (g == 1)
    return g;
  size_t n = 0;
  long key = -1;
  for (size_t i = 0; i < s.bars.size(); ++i) {
    const long k = static_cast<long>(s.x[i]) / g;
    if (n && k == key) {
      Candle &b = s.bars[n - 1];
      const Candle &c = s.bars[i];
      b.high = std::max(b.high, c.high);
      b.low = std::min(b.low, c.low);
      b.close = c.close;
      b.volume += c.volume;
      b.amount += c.amount;
      continue;
    }
    key = k;
    s.bars[n] = s.bars[i];
    s.x[n] = static_cast<double>(k * g) + (g - 1) * 0.5;
    ++n;
  }
  s.bars.resize(n);
  s.x.resize(n);
  return g;
}

// 价 (蜡烛 + 前收 / 现价线, 日线另叠均线; 右轴涨跌幅) 叠 量, x 联动. 日内按视野宽并根 (merge). key 变 / 双击 → 视野复位到档窗口 [win_lo, win_hi];
//   左键拖动 = 框选 x 段放大; wheel → 滚轮只缩放 x, 右端锚在 win_hi (最新一根); y 恒随视野内的根贴合.
//   视野的改动在下一帧生效 (本帧已设给 ImPlot).
// 档位按钮叠在价图左上角 (在图之后提交, 抢得到悬停). 画完光标停在按钮处, 调用方显式定位下一块
void plot_chart(const char *id, Series &s, double win_lo, double win_hi, uint64_t key, QuotesView &v, const ImVec2 &size,
                const char *const ranges[], int nr, int &cur, bool wheel) {
  const bool reset = v.key != key;
  if (reset) {
    v.key = key;
    v.lo = win_lo, v.hi = win_hi, v.drag = kNaN;
  }
  if (s.intraday)
    s.g = merge(s, v.hi - v.lo, size.x);
  const int n = static_cast<int>(s.bars.size());
  const int w0 = static_cast<int>(std::lower_bound(s.x.begin(), s.x.end(), v.lo) - s.x.begin());
  const int w1 = static_cast<int>(std::upper_bound(s.x.begin(), s.x.end(), v.hi) - s.x.begin());
  double lo = std::numeric_limits<double>::max(), hi = -lo, vmax = 0;
  for (int i = w0; i < w1; ++i) {
    const Candle &b = s.bars[static_cast<size_t>(i)];
    const double f = s.from(static_cast<size_t>(i));
    lo = std::min({lo, static_cast<double>(b.low), f}), hi = std::max({hi, static_cast<double>(b.high), f}), vmax = std::max(vmax, s.vol(b));
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
    a = static_cast<int>(std::lower_bound(s.x.begin(), s.x.end(), lim.X.Min - s.g) - s.x.begin());
    b = static_cast<int>(std::upper_bound(s.x.begin(), s.x.end(), lim.X.Max + s.g) - s.x.begin());
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
    if (n > 0 && ImPlot::BeginItem("##k", ImPlotItemFlags_NoLegend)) {
      if (ImPlot::FitThisFrame())
        for (int i = 0; i < n; ++i) {
          ImPlot::FitPoint(ImPlotPoint(s.x[i], s.bars[static_cast<size_t>(i)].low));
          ImPlot::FitPoint(ImPlotPoint(s.x[i], s.bars[static_cast<size_t>(i)].high));
        }
      ImDrawList *dl = ImPlot::GetPlotDrawList();
      int a, b;
      visible(lim, a, b);
      for (int i = a; i < b; ++i) {
        const Candle &k = s.bars[static_cast<size_t>(i)];
        candle(dl, s.x[i], kBody * s.g, s.from(static_cast<size_t>(i)), k.high, k.low, k.close);
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
        const ImVec4 c = k.close >= s.from(static_cast<size_t>(i)) ? kUp : kDn;
        const ImVec2 p0 = ImPlot::PlotToPixels(s.x[i] - kBody * s.g, s.vol(k)), p1 = ImPlot::PlotToPixels(s.x[i] + kBody * s.g, 0.0);
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
      fmt_when(when, sizeof(when), s, i);
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
    button_bar(ranges, nr, cur);
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

// d 日的日根前收: 当日 (joins) / 历史; 无 → 0
float ref_of(const Kline &kl, const Day &today, misc::Ymd d) {
  if (joins(kl, today) && today.candle.date == d)
    return today.candle.ref;
  const auto it = std::lower_bound(kl.daily.begin(), kl.daily.end(), d, [](const Candle &c, misc::Ymd x) { return c.date < x; });
  return it != kl.daily.end() && it->date == d ? it->ref : 0;
}

// 日线的根 = 历史 + 当日 (joins 才接); 涨跌基准 = 末根前收
void daily(const Kline &kl, const Day &today, bool money, Series &s) {
  s.money = money;
  for (const Candle &c : kl.daily)
    s.push(c, static_cast<double>(s.bars.size()));
  if (joins(kl, today))
    s.push(today.candle, static_cast<double>(s.bars.size()));
  if (!s.bars.empty())
    s.ref = s.bars.back().ref;
}

// 日内日轴 = 行情日及其前 n − 1 个交易日 (升序); 行情日未知 (首轮快照前) → 空
std::vector<misc::Ymd> day_axis(std::span<const misc::Ymd> trading, misc::Ymd date, int n) {
  if (!date)
    return {};
  const auto end = std::lower_bound(trading.begin(), trading.end(), date);
  std::vector<misc::Ymd> days(end - std::min<std::ptrdiff_t>(n - 1, end - trading.begin()), end);
  days.push_back(date);
  return days;
}

// d 日的分钟根: 当日 (joins 且有根) / 历史; 无 → nullptr
const Bars *bars_on(const Kline &kl, const Day &today, misc::Ymd d) {
  if (joins(kl, today) && today.bars.has.any() && today.bars.date == d)
    return &today.bars;
  const auto it = std::lower_bound(kl.minutes.begin(), kl.minutes.end(), d, [](const Bars &m, misc::Ymd x) { return m.date < x; });
  return it != kl.minutes.end() && it->date == d ? &*it : nullptr;
}

// 资金流计入槽 j: 有根, 且首根 (开盘集合竞价并入) / 末根 (收盘集合竞价) 只在 ends 时计
bool flow_slot(const Bars &b, size_t j, bool ends) { return b.has[j] && (ends || (j != 0 && j != static_cast<size_t>(kSlots) - 1)); }

// 资金流占比的分母: 历史日线末 kFlowRefDays 根 (新浪当日不算) 的日均成交额 [元]; 盘前已定, 开盘起即稳. 无 → 0
constexpr size_t kFlowRefDays = 5;
double flow_ref(const Kline &kl) {
  const size_t n = std::min(kl.daily.size(), kFlowRefDays);
  double s = 0;
  for (size_t i = kl.daily.size() - n; i < kl.daily.size(); ++i)
    s += kl.daily[i].amount;
  return n ? s / static_cast<double>(n) : 0.0;
}

// 日内的根 = 日轴各日的分钟 (历史 + 当日, joins 才接; 某日无源 → 留空), x = 第 k 日 · kSlots + 槽;
//   分钟根的前收 = 同日日根的; 涨跌基准 = 末个有根日的前收
void intraday(const Kline &kl, const Day &today, std::span<const misc::Ymd> days, bool money, Series &s) {
  s.intraday = true;
  s.money = money;
  s.days.assign(days.begin(), days.end());
  for (size_t k = 0; k < days.size(); ++k) {
    const Bars *b = bars_on(kl, today, days[k]);
    if (!b)
      continue;
    const float ref = ref_of(kl, today, b->date);
    s.ref = ref;
    for (int j = 0; j < kSlots; ++j)
      if (b->has[static_cast<size_t>(j)])
        s.push({b->cell[static_cast<size_t>(j)], b->date, ref}, static_cast<double>(k) * kSlots + j);
  }
}

// 当日没接上 (历史落后) → 提示写进 buf, true
bool gap_note(const Kline &kl, const Day &today, char *buf, size_t size) {
  const misc::Ymd d = today.candle.date;
  if (!d || joins(kl, today) || d <= kl.last)
    return false;
  std::snprintf(buf, size, "历史止于 %02d-%02d, 未接当日 %02d-%02d", kl.last / 100 % 100, kl.last % 100, d / 100 % 100, d % 100);
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
  plot_chart(name, s, -0.5, std::max(1, nd) * kSlots - 0.5, key, st.iv, size, kIntraName, kIntras, st.intraday, false);
}
// 日线档窗口右端 = 最新一根; 不足一档的历史靠右, 左侧留空
void plot_daily(const char *name, Series &s, uint64_t id, ChartState &st, const ImVec2 &size) {
  const int n = static_cast<int>(s.bars.size()), span = kDailySpan[st.daily];
  const uint64_t key = id | static_cast<uint64_t>(st.daily) << 24 | static_cast<uint64_t>(n);
  plot_chart(name, s, n - span - 0.5, n - 0.5, key, st.dv, size, kDailyName, 4, st.daily, true);
}

// 个股行情的图区: 上 日内 (右挂 1/3 宽五档; 上下 6 : 4) / 下 日线.
//   显式定位 (档位按钮叠在图上, 画完光标不在流式位置)
void charts(const char *name, Series &intra, Series &daily, uint64_t id, ChartState &st, const sina::Quote &book, misc::Ymd date) {
  ImGui::PushID(name);
  const float gap = 4.f;
  const ImVec2 p0 = ImGui::GetCursorScreenPos(), avail = ImGui::GetContentRegionAvail();
  const float bw = std::floor((avail.x - gap) / 3.f), iw = avail.x - bw - gap;
  const float top_h = std::floor((avail.y - gap) * 0.6f);
  plot_intraday("##ichart", intra, id, st, ImVec2(iw, top_h));
  ImGui::SetCursorScreenPos(ImVec2(p0.x + iw + gap, p0.y));
  ImGui::PushStyleVar(ImGuiStyleVar_WindowPadding, ImVec2(0, 0));
  ImGui::BeginChild("##book", ImVec2(bw, top_h), false);
  render_book(book, date);
  ImGui::EndChild();
  ImGui::PopStyleVar();
  ImGui::SetCursorScreenPos(ImVec2(p0.x, p0.y + top_h + gap));
  plot_daily("##dchart", daily, id, st, ImVec2(avail.x, avail.y - top_h - gap));
  ImGui::PopID();
}

// 篮子日内叠加的一条: x = 日轴第 k 日 · kSlots + 槽 (并根 = 组中点), y = 收对本条在日轴内首日前收 (base) 的涨跌幅 [%] (各条同一零点, 跨日接续)
//   g = 0: 收盘线; g ≥ 1: 蜡烛 (每根并 g 槽; 实体 = 上一根收 → 本根收, 首根起于首日前收 = 0, 根根首尾相接; open = 实体起点,
//     high / low 同 y 口径并包住实体), 另带现价十字: chg = 当日涨跌幅 [%] (定色), now = 末个有根的槽 x
struct Overlay {
  std::string name;
  ImVec4 col;
  std::vector<double> x, y;
  int g = 0;
  std::vector<double> open, high, low;
  double chg = 0;
  int now = 0;
};

// 一条的蜡烛
void candles(const Overlay &l) {
  if (!ImPlot::BeginItem("##k", ImPlotItemFlags_NoLegend))
    return;
  ImDrawList *dl = ImPlot::GetPlotDrawList();
  for (size_t i = 0; i < l.x.size(); ++i)
    candle(dl, l.x[i], kBody * l.g, l.open[i], l.high[i], l.low[i], l.y[i]);
  ImPlot::EndItem();
}

// 叠加图: x 视野 = 档窗口 (不缩放), y 贴合全部线 / 蜡烛 (含零线); 刻度区全空 (图框不变), 不画图例:
//   x 只留网格; y 网格约每 kGridPx 一格 (步长 1 / 2 / 2.5 / 5 × 10^e, 0 恰在格上), 每格 y 值 (带单位 unit) 以小字内标在右壁 (格线上方, 顶格放不下 → 下方);
//   线名 (线色小字) 贴各线末点右侧, 不越过刻度字列, 按 y 排开不重叠.
//   蜡烛条另画现价十字: 过末根的横 / 竖虚线 (当日涨跌色), 右壁标末根涨跌幅 (同 y 口径), 底壁标时刻 (色底黑字).
//   十字光标: 竖线吸附到分钟 (蜡烛 = 吸附到根), 横线随鼠标; 提示 = 时刻 + 光标处 y + 该处各条读数 (高 → 低). 画完返回图区左上角
ImVec2 plot_overlay(const std::vector<Overlay> &ls, const std::vector<misc::Ymd> &days, const ImVec2 &size, const char *unit) {
  const int nd = std::max(1, static_cast<int>(days.size()));
  const Overlay *one = nullptr; // 蜡烛条
  double lo = 0, hi = 0;
  for (const Overlay &l : ls) {
    if (l.g)
      one = &l;
    for (const double y : l.y)
      lo = std::min(lo, y), hi = std::max(hi, y);
    for (const double y : l.low)
      lo = std::min(lo, y);
    for (const double y : l.high)
      hi = std::max(hi, y);
  }
  const double pad = std::max(hi - lo, 0.2) * 0.04, ylo = lo - pad, yhi = hi + pad;
  const double raw = (yhi - ylo) / std::max(2.0, std::floor(static_cast<double>(size.y / kGridPx))), e = std::floor(std::log10(raw)), m = raw / std::pow(10.0, e);
  const double nice = m <= 1 ? 1 : m <= 2 ? 2
                               : m <= 2.5 ? 2.5
                               : m <= 5   ? 5
                                          : 10,
               step = nice * std::pow(10.0, e);
  const int prec = std::max(0, static_cast<int>(-std::floor(std::log10(step) + 1e-9)) + (nice == 2.5 ? 1 : 0));
  std::vector<double> yt;
  for (double k = std::ceil(ylo / step); k * step <= yhi; ++k)
    yt.push_back(k * step);
  std::vector<double> grid, seps;
  for (int k = 0; k < nd; ++k) {
    for (int h = 0; h < 5; ++h)
      grid.push_back(k * kSlots + h * 60 - 0.5);
    if (k)
      seps.push_back(k * kSlots - 0.5);
  }
  ImVec2 corner(-1, -1);
  ImPlot::PushStyleVar(ImPlotStyleVar_PlotPadding, ImVec2(1, 1));
  ImPlot::PushStyleVar(ImPlotStyleVar_LabelPadding, ImVec2(0, 0));
  ImPlot::PushStyleVar(ImPlotStyleVar_PlotBorderSize, 0.f);
  if (ImPlot::BeginPlot("##overlay", size, ImPlotFlags_NoMenus | ImPlotFlags_NoBoxSelect | ImPlotFlags_NoLegend | ImPlotFlags_NoMouseText)) {
    ImPlot::SetupAxis(ImAxis_X1, nullptr, ImPlotAxisFlags_NoTickLabels | ImPlotAxisFlags_NoTickMarks);
    ImPlot::SetupAxisLimits(ImAxis_X1, -0.5, nd * kSlots - 0.5, ImPlotCond_Always);
    ImPlot::SetupAxisTicks(ImAxis_X1, grid.data(), static_cast<int>(grid.size()));
    ImPlot::SetupAxis(ImAxis_Y1, nullptr, ImPlotAxisFlags_NoTickLabels | ImPlotAxisFlags_NoTickMarks);
    ImPlot::SetupAxisLimits(ImAxis_Y1, ylo, yhi, ImPlotCond_Always);
    ImPlot::SetupAxisTicks(ImAxis_Y1, yt.data(), static_cast<int>(yt.size()));
    ImPlot::SetupFinish();
    corner = ImPlot::GetPlotPos();
    if (!seps.empty()) {
      ImPlot::SetNextLineStyle(kRule, 1.f);
      ImPlot::PlotInfLines("##sep", seps.data(), static_cast<int>(seps.size()));
    }
    const double zero = 0;
    ImPlot::SetNextLineStyle(kRule, 1.f);
    ImPlot::PlotInfLines("##ref", &zero, 1, ImPlotInfLinesFlags_Horizontal);
    for (size_t i = 0; i < ls.size(); ++i) {
      ImGui::PushID(static_cast<int>(i));
      if (ls[i].g) {
        candles(ls[i]);
      } else {
        ImPlot::SetNextLineStyle(ls[i].col, 1.5f);
        ImPlot::PlotLine("##l", ls[i].x.data(), ls[i].y.data(), static_cast<int>(ls[i].x.size()));
      }
      ImGui::PopID();
    }
    { // 内标小字 (压在线上方): 横网格涨跌幅贴右内壁; 现价十字; 线名贴线尾, 右缘不过刻度字列
      ImDrawList *dl = ImPlot::GetPlotDrawList();
      ImFont *font = ImGui::GetFont();
      const ImVec2 pos = ImPlot::GetPlotPos(), psz = ImPlot::GetPlotSize();
      const float fs = ImGui::GetFontSize() * kTagScale, top = pos.y, right = pos.x + psz.x - 2.f;
      const ImU32 col = ImGui::ColorConvertFloat4ToU32(kGridTag);
      float tw = 0;
      for (const double v : yt) {
        char buf[16];
        if (v == 0)
          std::snprintf(buf, sizeof(buf), "0%s", unit);
        else
          std::snprintf(buf, sizeof(buf), "%+.*f%s", prec, v, unit);
        const ImVec2 sz = font->CalcTextSizeA(fs, FLT_MAX, 0.f, buf);
        const float y = ImPlot::PlotToPixels(0.0, v).y;
        dl->AddText(font, fs, ImVec2(right - sz.x, y - sz.y >= top ? y - sz.y : y), col, buf);
        tw = std::max(tw, sz.x);
      }
      if (one) {
        char y[24], x[24];
        std::snprintf(y, sizeof(y), "%+.2f%%", one->y.back());
        const int k = one->now / kSlots, mn = slot_label(one->now % kSlots);
        if (nd > 1 && static_cast<size_t>(k) < days.size())
          std::snprintf(x, sizeof(x), "%02d-%02d %02d:%02d", days[static_cast<size_t>(k)] / 100 % 100, days[static_cast<size_t>(k)] % 100, mn / 60, mn % 60);
        else
          std::snprintf(x, sizeof(x), "%02d:%02d", mn / 60, mn % 60);
        const NowCross c{ImPlot::PlotToPixels(one->x.back(), one->y.back()), ImGui::ColorConvertFloat4ToU32(tint(one->chg, 0)), y, x};
        draw_now_cross(c, kTagScale, right);
        tw = std::max(tw, now_cross_width(c, kTagScale));
      }
      std::vector<EndTag> tags;
      for (const Overlay &l : ls)
        if (!l.x.empty())
          tags.push_back({ImPlot::PlotToPixels(l.x.back(), l.y.back()), ImGui::ColorConvertFloat4ToU32(l.col), l.name});
      draw_end_tags(tags, kTagScale, right - tw - 6.f);
    }
    if (ImPlot::IsPlotHovered()) {
      const ImPlotPoint m = ImPlot::GetPlotMousePos();
      const int g = one ? one->g : 1, grp = static_cast<int>(std::clamp(std::round(m.x), 0.0, nd * kSlots - 1.0)) / g;
      const double x = static_cast<double>(grp * g) + (g - 1) * 0.5; // 同 merge 的组中点
      ImPlot::SetNextLineStyle(kCrossLine, 1.f);
      ImPlot::PlotInfLines("##cx", &x, 1);
      ImPlot::SetNextLineStyle(kCrossLine, 1.f);
      ImPlot::PlotInfLines("##cy", &m.y, 1, ImPlotInfLinesFlags_Horizontal);
      const int end = grp * g + g - 1, k = end / kSlots, mn = slot_label(end % kSlots);
      std::vector<std::pair<size_t, const Overlay *>> at;
      for (const Overlay &l : ls) {
        const auto it = std::lower_bound(l.x.begin(), l.x.end(), x);
        if (it != l.x.end() && *it == x)
          at.emplace_back(static_cast<size_t>(it - l.x.begin()), &l);
      }
      std::sort(at.begin(), at.end(), [](const auto &a, const auto &b) { return a.second->y[a.first] > b.second->y[b.first]; });
      char per[16] = "";
      if (g > 1)
        std::snprintf(per, sizeof(per), " (%d分)", g);
      ImGui::BeginTooltip();
      if (static_cast<size_t>(k) < days.size())
        ImGui::Text("%02d-%02d %02d:%02d%s", days[static_cast<size_t>(k)] / 100 % 100, days[static_cast<size_t>(k)] % 100, mn / 60, mn % 60, per);
      ImGui::TextDisabled("光标 %+.2f%s", m.y, unit);
      for (const auto &[j, l] : at) {
        const double y = l->y[j];
        ImGui::TextColored(l->col, "%s", l->name.c_str());
        ImGui::SameLine();
        ImGui::TextColored(tint(y, 0), "%+.2f%s", y, unit);
        if (l->g)
          ImGui::Text("前 %+.2f%%  高 %+.2f%%\n低 %+.2f%%  收 %+.2f%%", l->open[j], l->high[j], l->low[j], y);
      }
      ImGui::EndTooltip();
    }
    ImPlot::EndPlot();
  }
  ImPlot::PopStyleVar(3);
  return corner;
}

// 收历史发布: 末次请求的发布到齐 → 整体换上, 当日待重拷; 到齐前照留在画的. 返回 = 发布代新于末次请求 (被别的 Feed 覆盖)
bool pull(const HistoryPub &x, Feed &f) {
  std::lock_guard<std::mutex> lock(x.mutex);
  if (x.gen == f.gen && x.data != f.data)
    f.data = x.data, f.shown = f.gen, f.ver = ~0ull;
  return x.gen > f.gen;
}

// 篮子图的前置: 源未就绪 / 读取中 → 画提示, false
bool basket_ready(const Market &mk, Feed &f) {
  if (f.data && f.data->lack) {
    ImGui::TextWrapped("%s", f.data->lack);
    if (ImGui::SmallButton("重载"))
      f.reload = true;
    return false;
  }
  if (!f.data) {
    const HistoryPub &x = mk.history(kBasket);
    ImGui::TextDisabled("读取 all.bar 特征库… %zu / %zu 日", x.done.load(std::memory_order_relaxed), x.total.load(std::memory_order_relaxed));
    return false;
  }
  assert(f.today && f.today->size() == f.data->list.size());
  return true;
}

// 篮子图的来源注 (按首条在画的篮子): 当日合成 / all.bar 截至 + 补日; 当日没接上 → gap_note, 返回 true
bool basket_note(const Tape &d, const Kline &kl, const Day &today, const QuoteStatus &qs, char *note, size_t size) {
  int n = 0;
  bool gap = false;
  if (joins(kl, today))
    n = std::snprintf(note, size, "当日由新浪快照合成 %02d:%02d:%02d · 成员取 all.bar %02d-%02d 收盘行", qs.sec / 3600, qs.sec / 60 % 60, qs.sec % 60,
                      d.basis / 100 % 100, d.basis % 100);
  else if (!(gap = gap_note(kl, today, note, size)))
    n = std::snprintf(note, size, "all.bar 截至 %02d-%02d", d.basis / 100 % 100, d.basis % 100);
  if (n > 0 && d.basis < kl.last)
    std::snprintf(note + n, size - static_cast<size_t>(n), " · 其后至 %02d-%02d 由 bar1d / bar1m 补", kl.last / 100 % 100, kl.last % 100);
  return gap;
}

// 叠加一条的源: 篮子 = 篮子历史 [basket_line] + 当日; 个股 = 叠加个股的历史 + 当日 (未到 → kl 空)
struct Source {
  const Kline *kl = nullptr;
  const Day *today = nullptr;
};
Source source_of(const BasketState &st, const OverlayPick &p) {
  if (p.side == kBasket) {
    const Tape &d = *st.feed.data;
    assert(p.key < d.list.size() && d.keys[p.key] == p.key);
    return {d.list[p.key].get(), &(*st.feed.today)[p.key]};
  }
  const Feed &f = st.stocks;
  if (!f.data || !f.today)
    return {};
  const auto it = std::find(f.data->keys.begin(), f.data->keys.end(), p.key);
  if (it == f.data->keys.end())
    return {};
  const size_t k = static_cast<size_t>(it - f.data->keys.begin());
  assert(f.today->size() == f.data->keys.size());
  return {f.data->list[k].get(), &(*f.today)[k]};
}

// 叠加图左上角 (corner; < 0 = 图没起来): bars() 画档位条, 再 "重载" (悬停看 note), 当日分钟未补齐时行尾小字待补只数;
//   hint 非空 → 下一行提示
template <class Tools>
void overlay_tools(const ImVec2 &corner, Feed &f, const char *note, const char *hint, const QuoteStatus &qs, const Tools &bars) {
  if (corner.x < 0)
    return;
  ImGui::SetCursorScreenPos(ImVec2(corner.x + 4, corner.y + 4));
  bars();
  ImGui::SameLine(0, 8);
  if (ImGui::SmallButton("重载"))
    f.reload = true;
  if (ImGui::IsItemHovered() && note[0])
    ImGui::SetTooltip("%s", note);
  if (qs.gaps) {
    ImGui::SameLine(0, 8);
    ImGui::TextDisabled("补取 %zu", qs.gaps);
    if (ImGui::IsItemHovered())
      ImGui::SetTooltip("当日分钟补取中: %zu 只待补 (已取 %zu)\n未补齐的分钟暂按前收持平, 曲线随补取更新", qs.gaps, qs.fetched);
  }
  if (hint) {
    ImGui::SetCursorScreenPos(ImVec2(corner.x + 4, corner.y + 4 + ImGui::GetFrameHeightWithSpacing()));
    ImGui::TextDisabled("%s", hint);
  }
}

} // namespace

void Sync(Market &mk, Side s, std::span<const uint32_t> want, Feed &f) {
  const bool lost = pull(mk.history(s), f) && !f.want.empty(); // 空键集被覆盖不必重发
  if (lost || f.reload || !std::ranges::equal(want, f.want)) {
    f.reload = false;
    f.want.assign(want.begin(), want.end());
    f.gen = ++mk.gen[s];
    mk.request(s, {f.gen, f.want});
    if (s == kStock)
      mk.quote->select(f.want.empty() ? ~0u : f.want.front());
  }
  if (!f.data || f.data->list.empty())
    return;
  const uint64_t ver = mk.quote->status().ver[s];
  if (ver == f.ver)
    return;
  f.ver = ver;
  f.today = mk.quote->today(s, f.data->keys);
}

void RenderStockPanel(const Market &mk, StockState &st, const std::string &title) {
  const QuoteStatus qs = mk.quote->status();
  char note[160];
  if (qs.date == 0)
    std::snprintf(note, sizeof(note), "新浪: 等待首轮快照…");
  else if (qs.fill)
    std::snprintf(note, sizeof(note), "新浪 %02d:%02d:%02d · 分钟 K 已补 %zu / 待补 %zu", qs.sec / 3600, qs.sec / 60 % 60, qs.sec % 60,
                  qs.fetched, qs.pending);
  else
    std::snprintf(note, sizeof(note), "新浪 %02d:%02d:%02d · 当日分钟由 bar1m 给", qs.sec / 3600, qs.sec / 60 % 60, qs.sec % 60);
  const Feed &f = st.feed;
  if (!f.data || f.data->keys.empty()) {
    ImGui::TextDisabled("%s", note);
    ImGui::TextWrapped(f.want.empty() ? "点表格中的一行查看行情." : "读取历史…");
    return;
  }
  assert(f.today && f.today->size() == 1);
  const uint32_t id = f.data->keys.front();
  const Kline &kl = *f.data->list.front();
  const Day &today = f.today->front();
  Series is, ds;
  intraday(kl, today, day_axis(mk.quote->trading(), qs.date, kIntraDays[st.chart.intraday]), false, is);
  daily(kl, today, false, ds);

  if (!f.want.empty() && f.want.front() != id)
    std::snprintf(note, sizeof(note), "读取 %s 历史…", asset_axis().code(f.want.front()).c_str());
  else if (mk.quote->axis().sub_of(id) == mk.quote->axis().size())
    std::snprintf(note, sizeof(note), "不在新浪全市场");
  else
    gap_note(kl, today, note, sizeof(note));
  header(title.c_str(), kl, today, note);
  charts("stock", is, ds, static_cast<uint64_t>(f.shown) << 32, st.chart, mk.quote->book(id), qs.date);
}

void RenderBasketIntraday(const Market &mk, BasketState &st, const std::vector<OverlayPick> &picks) {
  Feed &f = st.feed;
  if (!basket_ready(mk, f))
    return;
  const QuoteStatus qs = mk.quote->status();
  const Tape &d = *f.data;
  // 日轴 = 行情日及其前 n − 1 个交易日; 各条按日落位, 某条缺的日留空 (当日未接上 → 当日空)
  const std::vector<misc::Ymd> days = day_axis(mk.quote->trading(), qs.date, kIntraDays[st.intraday]);
  const int nd = std::max(1, static_cast<int>(days.size()));
  const ImVec2 avail = ImGui::GetContentRegionAvail();
  std::vector<Overlay> ls;
  char note[160] = "";
  bool gap = false;
  for (const OverlayPick &p : picks) {
    const Source src = source_of(st, p);
    if (!src.kl)
      continue;
    const Kline &kl = *src.kl;
    const Day &today = *src.today;
    if (!note[0] && p.side == kBasket)
      gap = basket_note(d, kl, today, qs, note, sizeof(note));
    Series s;
    intraday(kl, today, days, p.side == kBasket, s);
    if (s.bars.empty() || !(s.bars.front().ref > 0))
      continue;
    const double base = s.bars.front().ref;
    const auto pct = [&](double v) { return (v / base - 1.0) * 100.0; };
    Overlay &l = ls.emplace_back();
    l.name = p.name, l.col = ImGui::ColorConvertU32ToFloat4(p.col);
    if (picks.size() == 1) { // 单条: 蜡烛 + 现价十字
      const Candle &e = s.bars.back();
      l.chg = e.ref > 0 ? (e.close / e.ref - 1.0) * 100.0 : 0.0;
      l.now = static_cast<int>(s.x.back());
      l.g = merge(s, nd * kSlots, avail.x);
      double prev = 0;
      for (const Candle &c : s.bars) {
        const double y = pct(c.close);
        l.open.push_back(prev), l.high.push_back(std::max({pct(c.high), prev, y})), l.low.push_back(std::min({pct(c.low), prev, y}));
        prev = y;
      }
    }
    l.x = s.x;
    for (const Candle &c : s.bars)
      l.y.push_back(pct(c.close));
  }
  ImGui::PushID("basket");
  const ImVec2 corner = plot_overlay(ls, days, avail, "%");
  overlay_tools(corner, f, note, picks.empty() ? "在右表选指数 / 行业 / 概念行或其成分股" : gap ? note
                                                                                                : nullptr,
                qs, [&] { button_bar(kIntraName, kIntras, st.intraday); });
  ImGui::PopID();
}

void RenderBasketFlow(const Market &mk, BasketState &st, const std::vector<OverlayPick> &picks) {
  Feed &f = st.feed;
  if (!basket_ready(mk, f))
    return;
  const QuoteStatus qs = mk.quote->status();
  const Tape &d = *f.data;
  const std::vector<misc::Ymd> days = FlowDays(mk, st);
  const bool share = st.flow_unit == 0;
  std::vector<Overlay> ls;
  char note[160] = "";
  bool gap = false;
  for (const OverlayPick &p : picks) {
    const Source src = source_of(st, p);
    if (!src.kl)
      continue;
    const Kline &kl = *src.kl;
    const Day &today = *src.today;
    if (!note[0] && p.side == kBasket)
      gap = basket_note(d, kl, today, qs, note, sizeof(note));
    const double unit = share ? flow_ref(kl) / 100.0 : 1e8; // y = 累计净流入 / unit
    if (!(unit > 0))
      continue;
    Overlay l;
    l.name = p.name, l.col = ImGui::ColorConvertU32ToFloat4(p.col);
    double net = 0;
    for (size_t k = 0; k < days.size(); ++k)
      if (const Bars *b = bars_on(kl, today, days[k]))
        for (size_t j = 0; j < static_cast<size_t>(kSlots); ++j)
          if (flow_slot(*b, j, st.flow_ends)) {
            net += b->net[j];
            l.x.push_back(static_cast<double>(k * kSlots + j));
            l.y.push_back(net / unit);
          }
    if (!l.x.empty())
      ls.push_back(std::move(l));
  }
  ImGui::PushID("flow");
  const ImVec2 corner = plot_overlay(ls, days, ImGui::GetContentRegionAvail(), share ? "%" : "亿");
  overlay_tools(corner, f, note, picks.empty() ? "在右表选指数 / 行业 / 概念行或其成分股" : gap ? note
                                                                                                : nullptr,
                qs, [&] {
                  button_bar(kIntraName, kIntras, st.flow);
                  ImGui::SameLine(0, 8);
                  button_bar(kFlowUnitName, 2, st.flow_unit);
                  ImGui::SameLine(0, 8);
                  if (st.flow_ends)
                    ImGui::PushStyleColor(ImGuiCol_Button, ImGui::GetStyleColorVec4(ImGuiCol_ButtonActive));
                  const bool flip = ImGui::SmallButton("开收盘");
                  if (st.flow_ends)
                    ImGui::PopStyleColor();
                  if (flip)
                    st.flow_ends = !st.flow_ends;
                  if (ImGui::IsItemHovered())
                    ImGui::SetTooltip("计入开盘 / 收盘的资金流 (默认不计; 图与右表同一开关)\n"
                                      "• 开盘 = 首根 09:31 (集合竞价并入, 与 09:30 那分钟分不开)\n"
                                      "• 收盘 = 末根 15:00 (收盘集合竞价)\n"
                                      "• 两根成交额大、方向只看对前一价的涨跌, 易把全日累计带偏 (关 = 曲线自开盘贴 0 轴起步)\n"
                                      "• 占比 = 累计净流入 / 近 5 日日均成交额 (不含当日, 盘前已定: 开盘即稳, 跨篮子可比)");
                });
  ImGui::PopID();
}

std::vector<misc::Ymd> FlowDays(const Market &mk, const BasketState &st) {
  return day_axis(mk.quote->trading(), mk.quote->status().date, kIntraDays[st.flow]);
}

Flow FlowOf(const Bars &b, bool ends) {
  Flow o;
  for (size_t j = 0; j < static_cast<size_t>(kSlots); ++j)
    if (flow_slot(b, j, ends))
      o.net += b.net[j], o.amount += b.cell[j].amount;
  return o;
}

std::vector<Flow> FlowOf(const BasketState &st, size_t k, std::span<const misc::Ymd> days) {
  const Feed &f = st.feed;
  if (!f.data || k >= f.data->list.size())
    return {};
  assert(f.today && f.today->size() == f.data->list.size());
  std::vector<Flow> o(days.size());
  for (size_t i = 0; i < days.size(); ++i)
    if (const Bars *b = bars_on(*f.data->list[k], (*f.today)[k], days[i]))
      o[i] = FlowOf(*b, st.flow_ends);
  return o;
}

Recent RecentOf(std::span<const Mark> m) {
  Recent r;
  const size_t n = m.size();
  if (n >= 1 && m[0].ref > 0)
    r.chg = m[0].close / m[0].ref - 1.0;
  if (n >= 5) {
    double c = 0;
    for (size_t i = 0; i < 5; ++i)
      c += m[i].close;
    r.ma5 = m[0].close / (c / 5) - 1.0;
  }
  return r;
}

Recent RecentOf(const Feed &f, size_t k) {
  if (!f.data || k >= f.data->list.size())
    return {};
  assert(f.today && f.today->size() == f.data->list.size());
  const Kline &kl = *f.data->list[k];
  const Day &today = (*f.today)[k];
  Mark m[kMarks];
  size_t n = 0;
  const auto put = [&](const Candle &c) { m[n++] = {c.close, c.ref}; };
  if (joins(kl, today))
    put(today.candle);
  for (auto it = kl.daily.rbegin(); it != kl.daily.rend() && n < kMarks; ++it)
    put(*it);
  return RecentOf({m, n});
}

Recent RecentOf(const StockInfo &info, const Candle &today, misc::Ymd prev) {
  Mark m[kMarks];
  size_t n = 0;
  if (today.date != 0 && prev == info.update_date)
    m[n++] = {today.close, today.ref};
  for (const StockInfo::DayClose &c : info.closes)
    if (c.date != 0 && n < kMarks)
      m[n++] = {c.close, c.ref};
  return RecentOf({m, n});
}

} // namespace GUI::Database
