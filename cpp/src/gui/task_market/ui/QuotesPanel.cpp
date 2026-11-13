#include "gui/task_market/ui/QuotesPanel.hpp"
#include "fundamental/AssetAxis.hpp"
#include "gui/Tasks.hpp"      // StatusColor (补分钟进度条 = 进行中色)
#include "gui/util/Chart.hpp" // kUp / kDn / tint / toggle_button / button_bar / EndTag
#include "gui/util/HoverSync.hpp"
#include "shared/Basket.hpp" // 0 轴净流入柱的默认标的 (全A.等)

#include "imgui.h"
#include "implot.h"
#include "implot_internal.h"

#include <algorithm>
#include <cassert>
#include <cfloat>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <initializer_list>
#include <limits>
#include <string>
#include <utility>
#include <vector>

namespace GUI::Market {

namespace {

constexpr ImVec4 kRule(0.55f, 0.55f, 0.55f, 0.45f);
constexpr int kMaN[] = {5, 10, 20};
constexpr const char *kMaName[] = {"MA5", "MA10", "MA20"};
constexpr ImVec4 kMaCol[] = {ImVec4(0.843f, 0.729f, 0.490f, 1.f), ImVec4(0.306f, 0.788f, 0.690f, 1.f),
                             ImVec4(0.773f, 0.525f, 0.753f, 1.f)};
constexpr int kIntras = 5;
constexpr int kIntraDays[kIntras] = {1, 2, 3, 4, 5};
constexpr const char *kIntraName[kIntras] = {"日内", "2日", "3日", "4日", "5日"};
constexpr const char *kFlowUnitName[] = {"占比", "金额"};
constexpr int kKSpan[] = {5, 22, 250, 2500}; // K 线档 [交易日]
constexpr const char *kKName[] = {"周", "月", "年", "10年"};
constexpr const char *kHours[] = {"09:30", "10:30", "11:30/13:00", "14:00", "15:00"};
constexpr double kBody = 0.35; // 蜡烛 / 额柱半宽 [槽]
constexpr float kMinPx = 3.f;  // 蜡烛每根最窄 [px] (窄了并根)
constexpr double kMinSpan = 5; // 视野至少几槽 (框选 / 滚轮)
constexpr double kWheel = 0.8; // 滚轮一格视野宽 × kWheel (向前 = 放大)
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
  pin(ImAxis_Y1, {"0000.00", "0000万", "0000.00亿"});
  pin(ImAxis_Y2, {"-000.0%", "+00.00%"});
  if (x)
    pin(ImAxis_X1, {"00-00 00:00", "0000-00-00"});
}

// 一张图的根: x = 第 k 日 · kSlots + 槽 (日内 = 日轴各日, K 线 = 全史各日; 无成交的槽留空; 并根 = 组中点, 每根 g 槽, g ≥ kSlots = 几日一根)
struct PlotBars {
  bool intraday = false;
  std::vector<Candle> bars;
  std::vector<double> x;
  std::vector<double> cut;     // 缺口槽的 x (升序; Bars::hole): 不画根, 线在此断开, 并根时含缺口的组整组不画
  std::vector<misc::Ymd> days; // [k]
  double ref = 0;              // 涨跌幅轴基准 (末日前收); 0 = 无
  int g = 1;
  void push(const Candle &b, double at) {
    bars.push_back(b);
    x.push_back(at);
  }
  // 第 i 根实体起点 = 上一根收 (根根首尾相接); 首根 = 前收 (无 → 开)
  double from(size_t i) const { return i ? bars[i - 1].close : bars[0].ref > 0 ? bars[0].ref
                                                                               : bars[0].open; }
};

// K 线底轴: 分钟根 "MM-DD HH:MM", 日根 "MM-DD", 月以上一根 "YYYY-MM"
int k_tick(double v, char *buf, int size, void *ud) {
  const PlotBars &s = *static_cast<const PlotBars *>(ud);
  const long k = static_cast<long>(std::floor((v + 0.5) / kSlots));
  if (k < 0 || k >= static_cast<long>(s.days.size()))
    return std::snprintf(buf, size, " ");
  const misc::Ymd d = s.days[static_cast<size_t>(k)];
  if (s.g < kSlots) {
    const int m = slot_label(std::clamp(static_cast<int>(std::lround(v) - k * kSlots), 0, kSlots - 1));
    return std::snprintf(buf, size, "%02d-%02d %02d:%02d", d / 100 % 100, d % 100, m / 60, m % 60);
  }
  if (s.g >= 20 * kSlots)
    return std::snprintf(buf, size, "%04d-%02d", d / 10000, d / 100 % 100);
  return std::snprintf(buf, size, "%02d-%02d", d / 100 % 100, d % 100);
}

int nearest(const PlotBars &s, double at) {
  const int n = static_cast<int>(s.x.size());
  int i = static_cast<int>(std::lower_bound(s.x.begin(), s.x.end(), at) - s.x.begin());
  if (i > 0 && (i == n || s.x[i] - at > at - s.x[i - 1]))
    --i;
  return i;
}

// 第 i 根的时刻: 分钟根 "MM-DD HH:MM" (槽 = 组末槽除 kSlots 的余), 日根 "YYYY-MM-DD" (几日一根 = 组首日)
void fmt_when(char *buf, int size, const PlotBars &s, size_t i) {
  const misc::Ymd d = s.bars[i].date;
  if (s.g < kSlots) {
    const int m = slot_label(static_cast<int>(s.x[i] + (s.g - 1) * 0.5) % kSlots);
    std::snprintf(buf, size, "%02d-%02d %02d:%02d", d / 100 % 100, d % 100, m / 60, m % 60);
  } else {
    std::snprintf(buf, size, "%s", misc::dash(d).c_str());
  }
}

void tooltip(const PlotBars &s, int i) {
  const Candle &b = s.bars[static_cast<size_t>(i)];
  char when[32], amt[32], chg[48] = "";
  fmt_when(when, sizeof(when), s, static_cast<size_t>(i));
  if (b.ref > 0)
    std::snprintf(chg, sizeof(chg), "\n涨跌 %+.2f  %+.2f%%", b.close - b.ref, (b.close / b.ref - 1.0) * 100.0);
  fmt_amount(amt, sizeof(amt), b.amount);
  ImGui::SetTooltip("%s\n高 %.2f  低 %.2f\n收 %.2f%s\n额 %s", when, b.high, b.low, b.close, chg, amt);
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

// 并根的槽数: span 槽在 px 宽里每根不窄于 kMinPx 的最小 kSlots 因子; days → 不够再取 kSlots 的整数倍 (几日一根)
int lod(double span, float px, bool days) {
  int g = 1;
  while (g < kSlots && (kSlots % g || px * g < kMinPx * span))
    ++g;
  while (days && px * g < kMinPx * span)
    g += kSlots;
  return g;
}

// 每 g 槽并一根 (开取先, 高 / 低取极, 收取后, 额相加; 日 / 前收取组首根), x = 组中点; 含缺口的组整组不画
void merge(PlotBars &s, int g) {
  s.g = g;
  if (g == 1)
    return;
  std::vector<long> holed; // 含缺口的组 (升序)
  for (const double c : s.cut)
    holed.push_back(static_cast<long>(c) / g);
  size_t n = 0;
  long key = -1;
  for (size_t i = 0; i < s.bars.size(); ++i) {
    const long k = static_cast<long>(s.x[i]) / g;
    if (std::ranges::binary_search(holed, k))
      continue;
    if (n && k == key) {
      Candle &b = s.bars[n - 1];
      const Candle &c = s.bars[i];
      b.high = std::max(b.high, c.high);
      b.low = std::min(b.low, c.low);
      b.close = c.close;
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
}

// 复位键变 → 视野回档窗口 [lo, hi]
void reset(QuotesView &v, uint64_t key, double lo, double hi) {
  if (v.key == key)
    return;
  v.key = key;
  v.lo = lo, v.hi = hi, v.zoom.cancel();
}

// 价 (蜡烛 + 前收 / 现价线, K 线另叠均线; 右轴涨跌幅) 叠 额, x 联动; s = 视野 v 的根 (已并好). 双击 → 视野复位到档窗口 [win_lo, win_hi];
//   左键拖动 = 框选 x 段放大; wheel > 0 → 滚轮只缩放 x (至多 wheel 宽), 右端锚在 win_hi (最新一根); y 恒随视野内的根贴合.
//   视野的改动在下一帧生效 (本帧已设给 ImPlot).
// 档位按钮叠在价图左上角 (在图之后提交, 抢得到悬停). 画完光标停在按钮处, 调用方显式定位下一块
void plot_chart(const char *id, const PlotBars &s, double win_lo, double win_hi, QuotesView &v, const ImVec2 &size,
                const char *const ranges[], int nr, int &cur, double wheel) {
  const int n = static_cast<int>(s.bars.size());
  const int w0 = static_cast<int>(std::lower_bound(s.x.begin(), s.x.end(), v.lo) - s.x.begin());
  const int w1 = static_cast<int>(std::upper_bound(s.x.begin(), s.x.end(), v.hi) - s.x.begin());
  double lo = std::numeric_limits<double>::max(), hi = -lo, vmax = 0;
  for (int i = w0; i < w1; ++i) {
    const Candle &b = s.bars[static_cast<size_t>(i)];
    const double f = s.from(static_cast<size_t>(i));
    lo = std::min({lo, static_cast<double>(b.low), f}), hi = std::max({hi, static_cast<double>(b.high), f}), vmax = std::max(vmax, b.amount);
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
      ImPlot::SetupAxisFormat(ImAxis_X1, k_tick, const_cast<PlotBars *>(&s));
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
    if (v.zoom.on()) {
      v.zoom.draw(clamped);
      return false;
    }
    if ((here || v.cross) && n > 0) {
      const double x = s.x[static_cast<size_t>(nearest(s, ImPlot::GetPlotMousePos().x))];
      ImPlot::SetNextLineStyle(kCrossLine, 1.f);
      ImPlot::PlotInfLines("##cx", &x, 1);
    }
    return here;
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
    ImPlot::SetupAxisFormat(ImAxis_Y2, pct_tick, const_cast<double *>(&s.ref));
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
    ImPlot::SetupAxisFormat(ImAxis_Y1, amount_tick, nullptr);
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
        const ImVec2 p0 = ImPlot::PlotToPixels(s.x[i] - kBody * s.g, k.amount), p1 = ImPlot::PlotToPixels(s.x[i] + kBody * s.g, 0.0);
        dl->AddRectFilled(ImVec2(std::min(p0.x, p1.x), p0.y), ImVec2(std::max(p0.x, p1.x) + 1.f, p1.y),
                          ImGui::ColorConvertFloat4ToU32(ImVec4(c.x, c.y, c.z, 0.75f)));
      }
      ImPlot::EndItem();
    }
    if (n > 0 && ImPlot::IsPlotHovered()) // 悬停提示只在额图: 价图留给框选 / 十字读价
      tooltip(s, nearest(s, ImPlot::GetPlotMousePos().x));
    if (input(false)) { // 十字横线: 左标额
      const double y = ImPlot::GetPlotMousePos().y;
      cross_y(y);
      ImPlot::TagY(y, kCross, false);
    }
    if (!v.zoom.on() && (hov || v.cross) && n > 0) { // 十字竖线落在底轴的时刻
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
    v.lo = win_lo, v.hi = win_hi, v.zoom.cancel();
  else if (hov && ImGui::IsMouseClicked(ImGuiMouseButton_Left))
    v.zoom.begin(mx);
  if (double lo, hi; v.zoom.end(mx, lo, hi) && hi - lo >= kMinSpan)
    v.lo = lo, v.hi = hi;
  if (wheel > 0 && hov && io.MouseWheel != 0) {
    const double w = std::clamp((v.hi - v.lo) * std::pow(kWheel, io.MouseWheel), kMinSpan, std::max(win_hi - win_lo, wheel));
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
void render_book(const sina::Quote *p, misc::Ymd date) {
  if (!p || p->sym.empty()) {
    ImGui::TextDisabled("等待快照…");
    return;
  }
  const sina::Quote &q = *p;
  const double ref = q.last_close;
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

// K 线的日根 = 源的日序列
std::vector<Candle> k_days(const Source &src) {
  std::vector<Candle> o(src.size());
  for (size_t i = 0; i < o.size(); ++i)
    o[i] = src.candle(i);
  return o;
}

// K 线视野 [lo, hi] 的根 (px = 图宽; days = k_days, 分钟根读 src 同下标): g = lod (可几日一根); 涨跌基准 = 末根前收.
//   g < kSlots → 各日的分钟根 (该日无分钟 → 空), 否则日根; 视野左侧多取 kMaN 末项根 (均线起得来), 再按 g 并根
void k_bars(std::span<const Candle> days, const Source &src, double lo, double hi, float px, PlotBars &s) {
  s = {};
  const int g = lod(hi - lo, px, true);
  const long nd = static_cast<long>(days.size());
  long k0 = std::clamp(static_cast<long>(std::floor((lo - static_cast<double>(kMaN[2]) * g) / kSlots)), 0L, nd);
  const long k1 = std::clamp(static_cast<long>(std::floor(hi / kSlots)) + 1, k0, nd);
  if (g > kSlots)
    k0 -= k0 % (g / kSlots);
  for (const Candle &c : days)
    s.days.push_back(c.date);
  if (nd)
    s.ref = days.back().ref;
  Bars b;
  for (long k = k0; k < k1; ++k) {
    const Candle &d = days[static_cast<size_t>(k)];
    const double x0 = static_cast<double>(k) * kSlots;
    if (g >= kSlots) {
      s.push(d, x0 + (kSlots - 1) * 0.5);
      continue;
    }
    if (!src.bars(static_cast<size_t>(k), b))
      continue;
    for (int j = 0; j < kSlots; ++j)
      if (b.has[static_cast<size_t>(j)])
        s.push({b.cell[static_cast<size_t>(j)], d.date, d.ref}, x0 + j);
      else if (b.hole[static_cast<size_t>(j)])
        s.cut.push_back(x0 + j);
  }
  merge(s, g);
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

// 资金流计入槽 j: 有根, 且首根 (开盘集合竞价并入) / 末根 (收盘集合竞价) 只在 ends 时计
bool flow_slot(const Bars &b, size_t j, bool ends) { return b.has[j] && (ends || (j != 0 && j != static_cast<size_t>(kSlots) - 1)); }

// 资金流占比的分母: 日线末 kFlowRefDays 根 (新浪当日不算) 的日均成交额 [元]; 盘前已定, 开盘起即稳. 无日线 → 0
constexpr size_t kFlowRefDays = 5;
static_assert(kFlowRefDays <= kTailDays);
double flow_ref(const Source &src) {
  const std::span<const Candle> d = src.daily.last(std::min(src.daily.size(), kFlowRefDays));
  double s = 0;
  for (const Candle &c : d)
    s += c.amount;
  return d.empty() ? 0.0 : s / static_cast<double>(d.size());
}

// 日内的根 = 日轴各日的分钟 (源的日序列; 某日无源 → 留空; 缺口记进 cut), x = 第 k 日 · kSlots + 槽;
//   价前复权到末个有根日 (末日真价; Source::adj); 分钟根的前收 = 同日日根的 (同乘); 涨跌基准 = 末个有根日的前收
void intraday(const Source &src, std::span<const misc::Ymd> days, PlotBars &s) {
  s.intraday = true;
  s.days.assign(days.begin(), days.end());
  const size_t nd = days.size();
  std::vector<size_t> at(nd); // [k] 日序列下标
  std::vector<Bars> bs(nd);
  std::vector<bool> has(nd);
  double end = 0; // 末个有根日的复权倍数
  for (size_t k = 0; k < nd; ++k) {
    at[k] = src.find(days[k]), has[k] = src.bars(at[k], bs[k]);
    if (has[k])
      end = src.adj(at[k]);
  }
  for (size_t k = 0; k < nd; ++k) {
    if (!has[k])
      continue;
    const Bars &b = bs[k];
    const double a = src.adj(at[k]) / end;
    const float ref = static_cast<float>(src.candle(at[k]).ref * a);
    s.ref = ref;
    for (int j = 0; j < kSlots; ++j)
      if (b.has[static_cast<size_t>(j)])
        s.push({scaled(b.cell[static_cast<size_t>(j)], a), days[k], ref}, static_cast<double>(k) * kSlots + j);
      else if (b.hole[static_cast<size_t>(j)])
        s.cut.push_back(static_cast<double>(k) * kSlots + j);
  }
}

// 来源注 (个股 / 篮子同一行): all.bar 库末日 · 基本面段末日 · 新浪归档末日 · 新浪快照时刻 (y = 当日; nullptr = 未到);
//   行情日在日脊之后却接不上 (历史落后) → 注明, 返回 true
bool coverage(const PriceView &v, const Live *y, char *buf, size_t size) {
  assert(size >= 160);
  const misc::Ymd a = v.lib_last(), f = v.fund_last(), b = v.last(), d = y ? y->date : 0;
  const auto at = [&] { return static_cast<size_t>(std::strlen(buf)); };
  std::snprintf(buf, size, "all.bar 至 %02d-%02d", a / 100 % 100, a % 100);
  if (f > a)
    std::snprintf(buf + at(), size - at(), " · bar1d / bar1m 补至 %02d-%02d", f / 100 % 100, f % 100);
  if (b > f)
    std::snprintf(buf + at(), size - at(), " · 新浪归档补至 %02d-%02d", b / 100 % 100, b % 100);
  if (d == 0) {
    std::snprintf(buf + at(), size - at(), " · 新浪: 等待首轮快照…");
    return false;
  }
  if (d > b && !v.joins(y->prev)) {
    std::snprintf(buf + at(), size - at(), " · 历史落后, 未接当日 %02d-%02d", d / 100 % 100, d % 100);
    return true;
  }
  if (d > b)
    std::snprintf(buf + at(), size - at(), " · 新浪 %02d:%02d:%02d", y->sec / 3600, y->sec / 60 % 60, y->sec % 60);
  return false;
}

// 标题行 (标题 · 最新价 / 涨跌) + 注行: 恒两行, 图区起点不随内容动 (注行末可再 SameLine 一个 SmallButton, 同高)
void header(const char *title, const Source &src, const char *note) {
  ImGui::TextUnformatted(title);
  if (const Candle *k = src.last(); k && k->ref > 0) {
    ImGui::SameLine();
    ImGui::TextColored(tint(k->close, k->ref), "%.2f  %+.2f  %+.2f%%", k->close, k->close - k->ref, (k->close / k->ref - 1.0) * 100.0);
  }
  ImGui::TextDisabled("%s", note);
}

} // namespace

// 个股图的根: 键 = 源 (个股历史 / 当日) + 标的 + 日内档
struct StockSeries {
  std::shared_ptr<const PriceView> view; // 持着 tape 与 src 的全史
  const Tape *tape = nullptr;
  std::shared_ptr<const Live> live; // 持着 src 的当日
  uint32_t id = Ref::kNone;
  int span = 0;
  Source src;
  PlotBars intra;           // 日内 (未并根)
  std::vector<Candle> days; // K 线的日根 [k] (= 源的日序列)
  PlotBars k;               // K 线视野的根 (视野 / 图宽变才重建)
  double klo = 0, khi = 0;
  float kpx = 0;
};

namespace {

// 两图: id = 标的的复位键高位, 档 / 日数拼进低位
void plot_intraday(const char *name, const PlotBars &intra, uint64_t id, ChartState &st, const ImVec2 &size) {
  const int nd = static_cast<int>(intra.days.size());
  const double hi = std::max(1, nd) * kSlots - 0.5;
  reset(st.iv, id | static_cast<uint64_t>(st.intraday) << 16 | static_cast<uint64_t>(nd), -0.5, hi);
  PlotBars s = intra;
  merge(s, lod(st.iv.hi - st.iv.lo, size.x, false));
  plot_chart(name, s, -0.5, hi, st.iv, size, kIntraName, kIntras, st.intraday, 0);
}
// K 线档窗口右端 = 最新一日; 不足一档的历史靠右, 左侧留空
void plot_k(const char *name, StockSeries &c, uint64_t id, ChartState &st, const ImVec2 &size) {
  const double end = static_cast<double>(c.days.size()) * kSlots, lo = end - kKSpan[st.kline] * kSlots - 0.5, hi = end - 0.5;
  reset(st.kv, id | static_cast<uint64_t>(st.kline) << 24 | static_cast<uint64_t>(c.days.size()), lo, hi);
  if (c.klo != st.kv.lo || c.khi != st.kv.hi || c.kpx != size.x) {
    c.klo = st.kv.lo, c.khi = st.kv.hi, c.kpx = size.x;
    k_bars(c.days, c.src, c.klo, c.khi, c.kpx, c.k);
  }
  plot_chart(name, c.k, lo, hi, st.kv, size, kKName, 4, st.kline, end + 1.0);
}

// 个股行情的图区: 上 日内 (右挂 1/3 宽五档; 上下 6 : 4) / 下 K 线.
//   显式定位 (档位按钮叠在图上, 画完光标不在流式位置)
void charts(const char *name, StockSeries &c, uint64_t id, ChartState &st, const sina::Quote *book, misc::Ymd date) {
  ImGui::PushID(name);
  const float gap = 4.f;
  const ImVec2 p0 = ImGui::GetCursorScreenPos(), avail = ImGui::GetContentRegionAvail();
  const float bw = std::floor((avail.x - gap) / 3.f), iw = avail.x - bw - gap;
  const float top_h = std::floor((avail.y - gap) * 0.6f);
  plot_intraday("##ichart", c.intra, id, st, ImVec2(iw, top_h));
  ImGui::SetCursorScreenPos(ImVec2(p0.x + iw + gap, p0.y));
  ImGui::PushStyleVar(ImGuiStyleVar_WindowPadding, ImVec2(0, 0));
  ImGui::BeginChild("##book", ImVec2(bw, top_h), false);
  render_book(book, date);
  ImGui::EndChild();
  ImGui::PopStyleVar();
  ImGui::SetCursorScreenPos(ImVec2(p0.x, p0.y + top_h + gap));
  plot_k("##kchart", c, id, st, ImVec2(avail.x, avail.y - top_h - gap));
  ImGui::PopID();
}

// 篮子日内叠加的一条 (线): x = 日轴第 k 日 · kSlots + 槽, y = 收对本条在日轴内首日前收 (base) 的涨跌幅 [%] (各条同一零点, 跨日接续);
//   缺口处一点 y = NaN, 线断开. cross = 带现价十字 (只一条时): chg = 当日涨跌幅 [%] (定色), now = 末个有根的槽 x
struct Overlay {
  size_t pick = 0; // picks 下标
  std::string name;
  ImVec4 col;
  std::vector<double> x, y;
  bool cross = false;
  double chg = 0;
  int now = 0;
};

// 叠加图 0 轴上的净流入柱: 标的在日轴各日的每分钟净流入 (Bars::net, 分钟涨跌法) [元], x 同叠加图;
//   历史日 = 历史分钟根, 当日 = 快照合成的根 (此刻所在根随快照动, 走完即与分钟 K 同值).
//   柱高按像素, 不进 y 贴合: top (首 / 末根集合竞价之外的最大幅度) ↔ 图高 × kNetH, 首末根截在 kNetCap 倍; 半透明, 压在线下
constexpr float kNetH = 0.2f, kNetCap = 1.5f, kNetAlpha = 0.3f;
struct NetBars {
  std::string name;
  std::vector<double> x, v;
  double top = 0;
};
NetBars net_bars(const Source &src, std::string name, std::span<const misc::Ymd> days) {
  NetBars n{std::move(name), {}, {}, 0};
  Bars b;
  for (size_t k = 0; k < days.size(); ++k)
    if (src.bars(src.find(days[k]), b))
      for (size_t j = 0; j < static_cast<size_t>(kSlots); ++j)
        if (b.has[j]) {
          n.x.push_back(static_cast<double>(k * kSlots + j)), n.v.push_back(b.net[j]);
          if (j != 0 && j != static_cast<size_t>(kSlots) - 1)
            n.top = std::max(n.top, std::abs(b.net[j]));
        }
  return n;
}
// 金额带符号: "+1.23亿" / "-456万"
void fmt_signed(char *buf, int size, double a) {
  buf[0] = a < 0 ? '-' : '+';
  fmt_amount(buf + 1, size - 1, std::abs(a));
}

// 叠加图: x 视野 = 档窗口 (不缩放), y 贴合全部线 (含零线; center = 0 轴居中: 上下取两侧较大者对称); 刻度区全空 (图框不变), 不画图例:
//   x 只留网格; y 网格约每 kGridPx 一格 (步长 1 / 2 / 2.5 / 5 × 10^e, 0 恰在格上), 每格 y 值 (带单位 unit) 以小字内标在右壁 (格线上方, 顶格放不下 → 下方);
//   线名 (线色小字) 贴各线末点右侧, 不越过刻度字列, 按 y 排开不重叠.
//   带 cross 的线另画现价十字: 过末点的横 / 竖虚线 (当日涨跌色), 右壁标末点涨跌幅 (同 y 口径), 底壁标时刻 (色底黑字).
//   十字光标: 竖线吸附到分钟, 横线随鼠标; 提示 = 时刻 + 光标处 y + 该处各条读数 (高 → 低).
//   悬停联动 (HoverSync): hot = 白粗线画在最上的条 (pick; −1 = 无), near = 图上悬停的条 (pick; −1 = 无).
//   net = 0 轴上的净流入柱 (nullptr = 不画): 左下角小字 "净流入 · 名" + 末根读数, 提示另列光标处的净流入. 画完返回图区左上角
ImVec2 plot_overlay(const std::vector<Overlay> &ls, const std::vector<misc::Ymd> &days, const ImVec2 &size, const char *unit, bool center, int hot, int &near,
                    const NetBars *net = nullptr) {
  near = -1;
  const int nd = std::max(1, static_cast<int>(days.size()));
  const Overlay *one = nullptr; // 带现价十字的线
  double lo = 0, hi = 0;
  for (const Overlay &l : ls) {
    if (l.cross)
      one = &l;
    for (const double y : l.y)
      lo = std::min(lo, y), hi = std::max(hi, y);
  }
  if (center)
    hi = std::max(hi, -lo), lo = -hi;
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
    if (net && net->top > 0 && ImPlot::BeginItem("##net", ImPlotItemFlags_NoLegend)) {
      ImDrawList *dl = ImPlot::GetPlotDrawList();
      const float h = ImPlot::GetPlotSize().y * kNetH, y0 = ImPlot::PlotToPixels(0.0, 0.0).y;
      for (size_t i = 0; i < net->x.size(); ++i) {
        const float d = std::clamp(static_cast<float>(net->v[i] / net->top) * h, -h * kNetCap, h * kNetCap);
        const float x0 = ImPlot::PlotToPixels(net->x[i] - kBody, 0.0).x, x1 = std::max(ImPlot::PlotToPixels(net->x[i] + kBody, 0.0).x, x0 + 1.f);
        const ImVec4 c = net->v[i] >= 0 ? kUp : kDn;
        dl->AddRectFilled(ImVec2(x0, std::min(y0, y0 - d)), ImVec2(x1, std::max(y0, y0 - d)), ImGui::ColorConvertFloat4ToU32(ImVec4(c.x, c.y, c.z, kNetAlpha)));
      }
      ImPlot::EndItem();
    }
    for (size_t i = 0; i < ls.size(); ++i) {
      ImGui::PushID(static_cast<int>(i));
      ImPlot::SetNextLineStyle(ls[i].col, 1.5f);
      ImPlot::PlotLine("##l", ls[i].x.data(), ls[i].y.data(), static_cast<int>(ls[i].x.size()));
      ImGui::PopID();
    }
    const int k = near_line(ls.size(), [&](size_t i, double x) {
      const Overlay &l = ls[i];
      return line_at(l.x.size(), x, [&](size_t j) { return ImPlotPoint(l.x[j], l.y[j]); });
    });
    if (k >= 0)
      near = static_cast<int>(ls[static_cast<size_t>(k)].pick);
    for (const Overlay &l : ls)
      if (static_cast<int>(l.pick) == hot) {
        ImPlot::SetNextLineStyle(kHotCol, kHotW);
        ImPlot::PlotLine("##hot", l.x.data(), l.y.data(), static_cast<int>(l.x.size()));
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
      if (net) {
        const std::string head = "净流入 · " + net->name;
        const ImVec2 hs = font->CalcTextSizeA(fs, FLT_MAX, 0.f, head.c_str());
        const ImVec2 at(pos.x + 4.f, pos.y + psz.y - hs.y - 2.f);
        dl->AddText(font, fs, at, col, head.c_str());
        if (!net->v.empty()) {
          char now[24];
          fmt_signed(now, sizeof(now), net->v.back());
          dl->AddText(font, fs, ImVec2(at.x + hs.x + 6.f, at.y), ImGui::ColorConvertFloat4ToU32(tint(net->v.back(), 0)), now);
        }
      }
    }
    if (ImPlot::IsPlotHovered()) {
      const ImPlotPoint m = ImPlot::GetPlotMousePos();
      const int slot = static_cast<int>(std::clamp(std::round(m.x), 0.0, nd * kSlots - 1.0));
      const double x = static_cast<double>(slot);
      ImPlot::SetNextLineStyle(kCrossLine, 1.f);
      ImPlot::PlotInfLines("##cx", &x, 1);
      ImPlot::SetNextLineStyle(kCrossLine, 1.f);
      ImPlot::PlotInfLines("##cy", &m.y, 1, ImPlotInfLinesFlags_Horizontal);
      const int k = slot / kSlots, mn = slot_label(slot % kSlots);
      std::vector<std::pair<size_t, const Overlay *>> at;
      for (const Overlay &l : ls) {
        const auto it = std::lower_bound(l.x.begin(), l.x.end(), x);
        if (it != l.x.end() && *it == x && std::isfinite(l.y[static_cast<size_t>(it - l.x.begin())]))
          at.emplace_back(static_cast<size_t>(it - l.x.begin()), &l);
      }
      std::sort(at.begin(), at.end(), [](const auto &a, const auto &b) { return a.second->y[a.first] > b.second->y[b.first]; });
      ImGui::BeginTooltip();
      if (static_cast<size_t>(k) < days.size())
        ImGui::Text("%02d-%02d %02d:%02d", days[static_cast<size_t>(k)] / 100 % 100, days[static_cast<size_t>(k)] % 100, mn / 60, mn % 60);
      ImGui::TextDisabled("光标 %+.2f%s", m.y, unit);
      for (const auto &[j, l] : at) {
        const double y = l->y[j];
        ImGui::TextColored(l->col, "%s", l->name.c_str());
        ImGui::SameLine();
        ImGui::TextColored(tint(y, 0), "%+.2f%s", y, unit);
      }
      if (net) { // 光标所在分钟的净流入
        const auto it = std::lower_bound(net->x.begin(), net->x.end(), x);
        if (it != net->x.end() && *it == x) {
          const double v = net->v[static_cast<size_t>(it - net->x.begin())];
          char amt[24];
          fmt_signed(amt, sizeof(amt), v);
          ImGui::TextColored(tint(v, 0), "净流入 · %s %s", net->name.c_str(), amt);
        }
      }
      ImGui::EndTooltip();
    }
    ImPlot::EndPlot();
  }
  ImPlot::PopStyleVar(3);
  return corner;
}

// 篮子图的前置: 价格库用不了 / 未发布 → 画提示, false
bool basket_ready(const QuoteHub &mk) {
  if (!mk.view) {
    ImGui::TextDisabled("备价格库 (all.bar 特征库 + 补段)… %zu / %zu", mk.progress().done.load(std::memory_order_relaxed), mk.progress().total.load(std::memory_order_relaxed));
    return false;
  }
  if (!mk.view->lack.empty()) {
    ImGui::TextWrapped("%s (建好即自动接上)", mk.view->lack.c_str());
    return false;
  }
  return true;
}

// 当日分钟的完整度, 接在上一项行尾 (完整且无事不画): 有缺口 → 警示色进度条 "缺口 N 只 · 补分钟 已取 / 共";
// 无缺口在取 → 进行中色进度条 "新浪补分钟 已取 / 共"; 只剩等定稿的 → 灰字只数; 悬停看说明
void fill_chip(const QuoteStatus &qs) {
  if (qs.pending == 0 && qs.gaps == 0)
    return;
  ImGui::SameLine(0, 8);
  if (qs.pending > 0 || qs.holes > 0) {
    const size_t n = qs.fetched + qs.pending;
    char s[96];
    if (qs.holes > 0)
      std::snprintf(s, sizeof(s), "缺口 %zu 只 · 补分钟 %zu / %zu", qs.holes, qs.fetched, n);
    else
      std::snprintf(s, sizeof(s), "新浪补分钟 %zu / %zu", qs.fetched, n);
    ImGui::PushStyleColor(ImGuiCol_PlotHistogram, StatusColor(qs.holes > 0 ? TaskStatus::Kind::Warn : TaskStatus::Kind::Busy));
    ImGui::ProgressBar(n > 0 ? static_cast<float>(qs.fetched) / static_cast<float>(n) : 0.f, ImVec2(ImGui::CalcTextSize(s).x + 16, ImGui::GetTextLineHeight()), s);
    ImGui::PopStyleColor();
  } else {
    ImGui::TextDisabled("待定稿 %zu 只", qs.gaps);
  }
  if (ImGui::IsItemHovered())
    ImGui::SetTooltip("当日分钟补取: 在取 %zu 只 / 已取 %zu; 待补 %zu 只, 其中有缺口 %zu 只\n"
                      "• 缺口 = 快照没盯住的分钟 (启动 / 断网 / 停机重启), 有无成交未知: 个股与含它的篮子该分钟不画 (线断开), 立即补取, 补到即画\n"
                      "• 待定稿 = 已有图, 等新浪那根分钟 K 不再变 (约 %d 秒) 后再取一次补成精确",
                      qs.pending, qs.fetched, qs.gaps, qs.holes, QuoteService::kSettle);
}

// 叠加图左上角 (corner; < 0 = 图没起来): bars() 画档位条, 再来源 (悬停看 note), 行尾新浪补分钟进度 (fill_chip);
//   hint 非空 → 下一行提示
template <class Tools>
void overlay_tools(const ImVec2 &corner, const char *note, const char *hint, const QuoteStatus &qs, const Tools &bars) {
  if (corner.x < 0)
    return;
  ImGui::SetCursorScreenPos(ImVec2(corner.x + 4, corner.y + 4));
  bars();
  ImGui::SameLine(0, 8);
  ImGui::TextDisabled("(?)");
  if (ImGui::IsItemHovered())
    ImGui::SetTooltip("%s", note);
  fill_chip(qs);
  if (hint) {
    ImGui::SetCursorScreenPos(ImVec2(corner.x + 4, corner.y + 4 + ImGui::GetFrameHeightWithSpacing()));
    ImGui::TextDisabled("%s", hint);
  }
}

// ---- 叠加图两种 (OverlayKind): 同一条路 RenderOverlay, 差异只在 kOverlayDefs ----
// 一张叠加图的公共输入: 日轴 (各条按日落位, 某条缺的日留空; 当日未接上 → 当日空) / 只一条
struct OverlayCtx {
  const OverlayState &st;
  std::span<const misc::Ymd> days;
  bool one;
};

// 涨跌幅: y = 收对本条在日轴内首日前收的涨跌幅 [%] (收盘线, 缺口处插 NaN 断开); 只一条另带现价十字
bool pct_line(const OverlayCtx &cx, const OverlayPick &p, const Source &src, Overlay &l) {
  PlotBars s;
  intraday(src, cx.days, s);
  if (s.bars.empty() || !(s.bars.front().ref > 0))
    return false;
  const double base = s.bars.front().ref;
  const auto pct = [&](double v) { return (v / base - 1.0) * 100.0; };
  if (cx.one) {
    const Candle &e = s.bars.back();
    l.cross = true;
    l.chg = e.ref > 0 ? (e.close / e.ref - 1.0) * 100.0 : 0.0;
    l.now = static_cast<int>(s.x.back());
  }
  for (size_t i = 0, c = 0; i < s.bars.size(); ++i) {
    const bool cut = c < s.cut.size() && s.cut[c] < s.x[i];
    if (cut && i > 0)
      l.x.push_back(s.cut[c]), l.y.push_back(std::numeric_limits<double>::quiet_NaN());
    while (c < s.cut.size() && s.cut[c] < s.x[i])
      ++c;
    l.x.push_back(s.x[i]), l.y.push_back(pct(s.bars[i].close));
  }
  return true;
}
// 累计净流入: 自日轴首日首根起逐槽累计, 跨日接续; y = 累计 / 单位 (占比 = 近 5 日日均成交额 %, 金额 = 亿元); 止于首个缺口 (其后的累计不可知)
bool flow_line(const OverlayCtx &cx, const OverlayPick &, const Source &src, Overlay &l) {
  const double unit = cx.st.flow_unit == 0 ? flow_ref(src) / 100.0 : 1e8;
  if (!(unit > 0))
    return false;
  double net = 0;
  Bars b;
  for (size_t k = 0; k < cx.days.size(); ++k)
    if (src.bars(src.find(cx.days[k]), b))
      for (size_t j = 0; j < static_cast<size_t>(kSlots); ++j) {
        if (b.hole[j])
          return !l.x.empty();
        if (flow_slot(b, j, cx.st.flow_ends)) {
          net += b.net[j];
          l.x.push_back(static_cast<double>(k * kSlots + j));
          l.y.push_back(net / unit);
        }
      }
  return !l.x.empty();
}
// 资金流的口径条 (接在档位条后)
void flow_tools(OverlayState &st) {
  ImGui::SameLine(0, 8);
  button_bar(kFlowUnitName, 2, st.flow_unit);
  ImGui::SameLine(0, 8);
  if (toggle_button("开收盘", st.flow_ends))
    st.flow_ends = !st.flow_ends;
  if (ImGui::IsItemHovered())
    ImGui::SetTooltip("计入开盘 / 收盘的资金流 (默认不计; 图与右表同一开关)\n"
                      "• 开盘 = 首根 09:31 (集合竞价并入, 与 09:30 那分钟分不开)\n"
                      "• 收盘 = 末根 15:00 (收盘集合竞价)\n"
                      "• 两根成交额大、方向只看对前一价的涨跌, 易把全日累计带偏 (关 = 曲线自开盘贴 0 轴起步)\n"
                      "• 占比 = 累计净流入 / 近 5 日日均成交额 (不含当日, 盘前已定: 开盘即稳, 跨篮子可比)");
}

struct OverlayDef {
  const char *id;
  bool net;                                                                         // 0 轴叠净流入柱
  bool center;                                                                      // y 以 0 轴居中
  const char *(*unit)(const OverlayState &);                                        // y 单位
  bool (*line)(const OverlayCtx &, const OverlayPick &, const Source &, Overlay &); // 一条 (false = 不画)
  void (*tools)(OverlayState &);                                                    // 档位条之后的工具 (nullptr = 无)
};
constexpr OverlayDef kOverlayDefs[kOverlays] = {
    {"intraday", true, true, [](const OverlayState &) { return "%"; }, pct_line, nullptr},
    {"flow", false, false, [](const OverlayState &s) { return s.flow_unit == 0 ? "%" : "亿"; }, flow_line, flow_tools},
};

} // namespace

// 叠加图的线: 键 = 源 (价格库 / 当日) + 选中 + 档
struct OverlayCache {
  std::shared_ptr<const PriceView> view;
  std::shared_ptr<const Live> live;
  std::vector<OverlayPick> picks;
  int span = 0, unit = 0;
  bool ends = false;
  std::vector<misc::Ymd> days;
  std::vector<Overlay> ls;
  NetBars net;
  char note[256] = "";
  bool gap = false;
};

void Want(QuoteHub &mk, StockState &st, uint32_t id) {
  if (id != st.want) {
    st.want = id;
    mk.select(id);
  }
  if (id == Ref::kNone) {
    st.shown = Ref::kNone;
    return;
  }
  const Ref r{kStock, id};
  mk.want(r, true);
  if (mk.view && mk.view->covers(r, true))
    st.shown = id;
}

void RenderStockPanel(const QuoteHub &mk, StockState &st, const std::string &title) {
  const QuoteStatus &qs = mk.status;
  if (st.shown == Ref::kNone) {
    if (qs.date == 0)
      ImGui::TextDisabled("新浪: 等待首轮快照…");
    else
      ImGui::TextDisabled("新浪 %02d:%02d:%02d", qs.sec / 3600, qs.sec / 60 % 60, qs.sec % 60);
    fill_chip(qs);
    if (st.want == Ref::kNone)
      ImGui::TextWrapped("点表格中的一行查看行情.");
    else if (!mk.view)
      ImGui::TextWrapped("读取历史… 备价格库 (all.bar 特征库 + 补段) %zu / %zu", mk.progress().done.load(std::memory_order_relaxed),
                         mk.progress().total.load(std::memory_order_relaxed));
    else
      ImGui::TextWrapped("读取历史…");
    return;
  }
  const uint32_t id = st.shown;
  const Joint j = mk.joint(kStock);
  const Source src = j.of(id);
  if (!st.series)
    st.series = std::make_shared<StockSeries>();
  StockSeries &c = *st.series;
  if (c.tape != j.tape() || c.live != j.live || c.id != id || c.span != st.chart.intraday) {
    c = {.view = j.view, .tape = j.tape(), .live = j.live, .id = id, .span = st.chart.intraday, .src = src, .days = k_days(src)};
    intraday(src, day_axis(mk.calendar().days, j.live ? j.live->date : 0, kIntraDays[st.chart.intraday]), c.intra);
  }

  char note[256];
  if (st.want != id)
    std::snprintf(note, sizeof(note), "读取 %s 历史…", asset_axis().code(st.want).c_str());
  else if (!j.tape())
    std::snprintf(note, sizeof(note), "只画当日 · %s", mk.view->lack.c_str());
  else {
    coverage(*j.view, j.live.get(), note, sizeof(note));
    if (!src.se)
      std::snprintf(note + std::strlen(note), sizeof(note) - std::strlen(note), " · 本只无历史, 只画当日");
  }
  if (!mk.listed(id))
    std::snprintf(note + std::strlen(note), sizeof(note) - std::strlen(note), " · 不在新浪全市场");
  header(title.c_str(), src, note);
  fill_chip(qs);
  charts("stock", c, static_cast<uint64_t>(id) << 32, st.chart, j.live ? j.live->quote(id) : nullptr, j.live ? j.live->date : 0);
}

int RenderOverlay(OverlayKind kind, QuoteHub &mk, OverlayState &st, const std::vector<OverlayPick> &picks, int hot) {
  const OverlayDef &def = kOverlayDefs[kind];
  const size_t all_eq = basket_table().at_pool(kBasketBase);
  for (const OverlayPick &p : picks)
    mk.want(p.ref, true);
  if (def.net && picks.size() != 1)
    mk.want({kBasket, static_cast<uint32_t>(all_eq)}, true);
  if (!basket_ready(mk))
    return -1;
  if (!st.cache[kind])
    st.cache[kind] = std::make_shared<OverlayCache>();
  OverlayCache &c = *st.cache[kind];
  if (c.view != mk.view || c.live != mk.live || c.picks != picks || c.span != st.span[kind] || c.unit != st.flow_unit || c.ends != st.flow_ends) {
    c.view = mk.view, c.live = mk.live, c.picks = picks, c.span = st.span[kind], c.unit = st.flow_unit, c.ends = st.flow_ends;
    c.days = OverlayDays(kind, mk, st);
    c.gap = coverage(*mk.view, mk.live.get(), c.note, sizeof(c.note));
    const OverlayCtx cx{st, c.days, picks.size() == 1};
    c.ls.clear();
    for (size_t i = 0; i < picks.size(); ++i) {
      const OverlayPick &p = picks[i];
      const Source src = mk.of(p.ref);
      if (!src.any())
        continue;
      Overlay l{.pick = i, .name = p.name, .col = ImGui::ColorConvertU32ToFloat4(p.col)};
      if (def.line(cx, p, src, l))
        c.ls.push_back(std::move(l));
    }
    c.net = {};
    if (def.net) { // 0 轴净流入柱的标的: 只一条 = 它, 否则全A.等
      const Ref nk = cx.one ? picks.front().ref : Ref{kBasket, static_cast<uint32_t>(all_eq)};
      c.net = net_bars(mk.of(nk), cx.one ? picks.front().name : basket_table().name(all_eq), c.days);
    }
  }
  ImGui::PushID(def.id);
  int near = -1;
  const ImVec2 corner = plot_overlay(c.ls, c.days, ImGui::GetContentRegionAvail(), def.unit(st), def.center, hot, near, def.net ? &c.net : nullptr);
  overlay_tools(corner, c.note, picks.empty() ? "在右表选指数 / 行业 / 概念行或其成分股" : c.gap ? c.note
                                                                                                 : nullptr,
                mk.status, [&] {
                  button_bar(kIntraName, kIntras, st.span[kind]);
                  if (def.tools)
                    def.tools(st);
                });
  ImGui::PopID();
  return near;
}

std::vector<misc::Ymd> OverlayDays(OverlayKind kind, const QuoteHub &mk, const OverlayState &st) {
  return day_axis(mk.calendar().days, mk.live ? mk.live->date : 0, kIntraDays[st.span[kind]]);
}

Recent RecentOf(std::span<const Mark> m) {
  Recent r;
  const size_t n = m.size();
  if (n >= 1 && m[0].ref > 0)
    r.chg = m[0].close / m[0].ref - 1.0;
  if (n >= kMarks && std::all_of(m.begin(), m.begin() + kMarks - 1, [](const Mark &x) { return x.ref > 0; })) {
    double c = m[0].close, f = 1; // f = 第 i 日收折到最新一日口径的前复权比 (前收链)
    for (size_t i = 1; i < kMarks; ++i) {
      f *= m[i - 1].ref / m[i].close;
      c += m[i].close * f;
    }
    r.ma5 = m[0].close / (c / kMarks) - 1.0;
  }
  return r;
}

Recent RecentOf(const Source &src) {
  Mark m[kMarks];
  size_t n = 0;
  for (size_t i = src.size(); i-- > 0 && n < kMarks;)
    m[n++] = {src.candle(i).close, src.candle(i).ref};
  return RecentOf({m, n});
}

} // namespace GUI::Market