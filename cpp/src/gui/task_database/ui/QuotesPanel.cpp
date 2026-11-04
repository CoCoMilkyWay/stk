#include "gui/task_database/ui/QuotesPanel.hpp"

#include "imgui.h"
#include "implot.h"
#include "implot_internal.h"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdio>
#include <limits>
#include <mutex>
#include <string>
#include <tuple>
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
constexpr int kDailySpan[] = {5, 22, 250}; // 交易日
constexpr const char *kDailyName[] = {"周", "月", "年"};
constexpr const char *kHours[] = {"09:30", "10:30", "11:30/13:00", "14:00", "15:00"};
constexpr double kBody = 0.35; // 蜡烛 / 量柱半宽 [根]

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

// 一张图的根: 日内 x = 第 k 日 · kSlots + 槽 (无成交的槽留空); 日线 x = 下标
struct Series {
  bool intraday = false;
  bool money = false; // 量柱 = 成交额 (篮子: 量不合成)
  std::vector<Candle> bars;
  std::vector<double> x;
  std::vector<misc::Ymd> days; // 日内各日
  double ref = 0;              // 涨跌幅轴基准; 0 = 无
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

void tooltip(const Series &s, int i) {
  const Candle &b = s.bars[static_cast<size_t>(i)];
  char when[32], vol[32], amt[32], chg[48] = "";
  if (s.intraday) {
    const int m = slot_label(b.slot);
    std::snprintf(when, sizeof(when), "%02d-%02d %02d:%02d", b.date / 100 % 100, b.date % 100, m / 60, m % 60);
  } else {
    std::snprintf(when, sizeof(when), "%s", misc::dash(b.date).c_str());
  }
  const double prev = s.intraday ? (b.date == s.days.back() ? s.ref : 0) : i > 0 ? s.bars[static_cast<size_t>(i) - 1].close
                                                                                 : 0;
  if (prev > 0)
    std::snprintf(chg, sizeof(chg), "\n涨跌 %+.2f  %+.2f%%", b.close - prev, (b.close / prev - 1.0) * 100.0);
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

// 接在上一项后 (间距 16) 放得下 w 宽 → SameLine; 否则另起一行
void flow(float w) {
  const float right = ImGui::GetCursorScreenPos().x + ImGui::GetContentRegionAvail().x; // 光标此时在下一行行首
  if (ImGui::GetItemRectMax().x + 16 + w <= right)
    ImGui::SameLine(0, 16);
}

// 价 (蜡烛 + 基准 / 现价线, 日线另叠均线; 右轴涨跌幅) 叠 量, x 联动. key 变 → 视野复位到 [win_lo, win_hi]; y 随上帧视野内的根贴合.
// 档位按钮叠在价图左上角 (在图之后提交, 抢得到悬停). 画完光标停在按钮处, 调用方显式定位下一块
void plot_chart(const char *id, Series &s, double win_lo, double win_hi, uint64_t key, QuotesView &v, const ImVec2 &size,
                const char *const ranges[], int nr, int &cur) {
  const int n = static_cast<int>(s.bars.size());
  const bool reset = v.key != key;
  if (reset) {
    v.key = key;
    v.lo = win_lo, v.hi = win_hi;
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
    ImPlot::SetupAxisLimits(ImAxis_X1, v.lo, v.hi, reset ? ImPlotCond_Always : ImPlotCond_Once);
    if (s.intraday)
      ImPlot::SetupAxisTicks(ImAxis_X1, tick_x.data(), static_cast<int>(tick_x.size()), tick_p.data());
    else
      ImPlot::SetupAxisFormat(ImAxis_X1, day_tick, &s);
  };
  const auto visible = [&](const ImPlotRect &lim, int &a, int &b) {
    a = static_cast<int>(std::lower_bound(s.x.begin(), s.x.end(), lim.X.Min - 1.0) - s.x.begin());
    b = static_cast<int>(std::upper_bound(s.x.begin(), s.x.end(), lim.X.Max + 1.0) - s.x.begin());
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
    if (s.ref > 0) {
      ImPlot::SetupAxis(ImAxis_Y2, nullptr, ImPlotAxisFlags_AuxDefault);
      ImPlot::SetupAxisLimits(ImAxis_Y2, pct(ylo), pct(yhi), ImPlotCond_Always);
      ImPlot::SetupAxisFormat(ImAxis_Y2, "%.1f%%");
    }
    ImPlot::SetupFinish();
    corner = ImPlot::GetPlotPos();
    const ImPlotRect lim = ImPlot::GetPlotLimits();
    v.lo = lim.X.Min, v.hi = lim.X.Max;

    if (!seps.empty()) {
      ImPlot::SetNextLineStyle(kRule, 1.f);
      ImPlot::PlotInfLines("##sep", seps.data(), static_cast<int>(seps.size()));
    }
    if (s.intraday && s.ref > 0) {
      ImPlot::SetNextLineStyle(kRule, 1.f);
      ImPlot::PlotInfLines("##ref", &s.ref, 1, ImPlotInfLinesFlags_Horizontal);
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
    if (n > 0 && ImPlot::BeginItem("##k", ImPlotItemFlags_NoLegend)) { // 阳线空心 / 阴线实心
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
      ImPlot::TagY(last, c, "%.2f", last);
    }
    if (n > 0 && ImPlot::IsPlotHovered())
      tooltip(s, nearest(s, ImPlot::GetPlotMousePos().x));
    ImPlot::EndPlot();
  }

  if (ImPlot::BeginPlot("##vol", ImVec2(), ImPlotFlags_NoMenus | ImPlotFlags_NoBoxSelect | ImPlotFlags_NoLegend)) {
    setup_x(true);
    ImPlot::SetupAxisLimits(ImAxis_Y1, 0, vmax > 0 ? vmax * 1.1 : 1.0, ImPlotCond_Always);
    ImPlot::SetupAxisFormat(ImAxis_Y1, s.money ? amount_tick : lots_tick, nullptr);
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
    if (n > 0 && ImPlot::IsPlotHovered())
      tooltip(s, nearest(s, ImPlot::GetPlotMousePos().x));
    ImPlot::EndPlot();
  }
  ImPlot::EndSubplots();
  ImPlot::PopStyleVar(3);
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

// 最新一根与其前收: 当日接上取当日, 否则日线末根 (前收 = 其前一根); 日线空 → false
bool last(const Kline &kl, const Day &today, Candle &k, double &prev) {
  if (joins(kl, today)) {
    k = today.candle, prev = today.ref;
    return true;
  }
  if (kl.daily.empty())
    return false;
  k = kl.daily.back();
  prev = kl.daily.size() > 1 ? kl.daily[kl.daily.size() - 2].close : 0;
  return true;
}

// 两张图的根 = 历史 + 当日 (joins 才接); 日内取末 days 日, 涨跌基准 = 末日的前收
void build(const Kline &kl, const Day &today, int days, bool money, Series &intra, Series &daily) {
  const bool on = joins(kl, today);
  intra.intraday = true;
  intra.money = daily.money = money;
  std::vector<const Bars *> src;
  for (const Bars &b : kl.minutes)
    src.push_back(&b);
  if (on && today.bars.has.any())
    src.push_back(&today.bars);
  const size_t nd = std::min(static_cast<size_t>(days), src.size());
  for (size_t k = 0; k < nd; ++k) {
    const Bars &b = *src[src.size() - nd + k];
    intra.days.push_back(b.date);
    for (int s = 0; s < kSlots; ++s)
      if (b.has[static_cast<size_t>(s)])
        intra.push({b.cell[static_cast<size_t>(s)], b.date, s}, static_cast<double>(k) * kSlots + s);
  }
  for (const Candle &c : kl.daily)
    daily.push(c, static_cast<double>(daily.bars.size()));
  if (on)
    daily.push(today.candle, static_cast<double>(daily.bars.size()));
  if (!intra.days.empty()) {
    const misc::Ymd d = intra.days.back();
    if (on && d == today.date)
      intra.ref = today.ref;
    else
      for (auto it = daily.bars.rbegin(); it != daily.bars.rend(); ++it)
        if (it->date < d) {
          intra.ref = it->close;
          break;
        }
  }
  if (!daily.bars.empty())
    daily.ref = daily.bars.back().close;
}

// 当日没接上 (历史落后) → 提示写进 buf, true
bool gap_note(const Kline &kl, const Day &today, char *buf, size_t size) {
  if (!today.date || joins(kl, today) || today.date <= kl.last)
    return false;
  std::snprintf(buf, size, "历史止于 %02d-%02d, 未接当日 %02d-%02d", kl.last / 100 % 100, kl.last % 100, today.date / 100 % 100,
                today.date % 100);
  return true;
}

// 标题 · 最新价 / 涨跌 · 注 (放不下另起一行)
void header(const char *title, const Kline &kl, const Day &today, const char *note) {
  ImGui::TextUnformatted(title);
  Candle k;
  double prev = 0;
  if (last(kl, today, k, prev) && prev > 0) {
    ImGui::SameLine();
    ImGui::TextColored(tint(k.close, prev), "%.2f  %+.2f  %+.2f%%", k.close, k.close - prev, (k.close / prev - 1.0) * 100.0);
  }
  flow(ImGui::CalcTextSize(note).x);
  ImGui::TextDisabled("%s", note);
}

// 两图: id = 标的的复位键高位, 档 / 根数拼进低位
void plot_intraday(const char *name, Series &s, uint64_t id, ChartState &st, const ImVec2 &size) {
  const int nd = static_cast<int>(s.days.size());
  const uint64_t key = id | static_cast<uint64_t>(st.intraday) << 16 | static_cast<uint64_t>(nd);
  plot_chart(name, s, -0.5, std::max(1, nd) * kSlots - 0.5, key, st.iv, size, kIntraName, 4, st.intraday);
}
void plot_daily(const char *name, Series &s, uint64_t id, ChartState &st, const ImVec2 &size) {
  const int n = static_cast<int>(s.bars.size()), span = kDailySpan[st.daily];
  const uint64_t key = id | static_cast<uint64_t>(st.daily) << 24 | static_cast<uint64_t>(n);
  plot_chart(name, s, std::max(n, span) - span - 0.5, std::max(n, span) - 0.5, key, st.dv, size, kDailyName, 3, st.daily);
}

const Kline kNoKline;

} // namespace

void SyncStock(QuoteService &live, StockHistory &hist, const AssetItem *asset, StockState &st) {
  const std::string code = asset ? asset->asset_code + "." + asset->exchange : std::string();
  if (code != st.code) {
    st.code = code;
    st.i = asset ? live.find(code) : ~0u;
    st.data.reset();
    ++st.gen;
    if (asset)
      hist.Request({st.gen, code});
    live.select(st.i);
  }
  {
    const StockKline &x = hist.published();
    std::lock_guard<std::mutex> lock(x.mutex);
    if (x.gen == st.gen && x.data != st.data)
      st.data = x.data;
  }
  if (st.i != ~0u)
    live.today(st.i, st.today, st.quote);
  else
    st.today.date = 0, st.quote = {};
}

void SyncBaskets(QuoteService &live, BasketHistory &hist, const Config &cfg, BasketState &st) {
  if (st.want) {
    st.want = false;
    FeatureLib lib = load_lib(cfg, kAllBar);
    st.missing = !lib.ready();
    if (!st.missing)
      hist.Request({++st.gen, std::move(lib)});
  }
  {
    const BasketKline &x = hist.published();
    std::lock_guard<std::mutex> lock(x.mutex);
    if (x.gen == st.gen && x.data != st.data)
      st.data = x.data;
  }
  if (!st.data)
    return;
  const Baskets &d = *st.data;
  const QuoteStatus qs = live.status();
  const auto key = std::make_tuple(qs.date, qs.sec, st.data.get());
  if (key == st.today_key)
    return;
  st.today_key = key;
  std::vector<double> level(d.list.size());
  for (size_t b = 0; b < d.list.size(); ++b)
    level[b] = d.list[b].kline.daily.empty() ? 0 : d.list[b].kline.daily.back().close;
  st.today.resize(d.list.size());
  if (!d.list.empty())
    live.today(d.live, level, st.today);
}

void RenderStockPanel(const QuoteService &live, const StockInfo *info, StockState &st) {
  const QuoteStatus qs = live.status();
  char note[160];
  if (qs.date == 0)
    std::snprintf(note, sizeof(note), "新浪: 等待首轮快照…");
  else if (qs.fill)
    std::snprintf(note, sizeof(note), "新浪 %02d:%02d:%02d · 分钟 K 已补 %zu / 待补 %zu", qs.sec / 3600, qs.sec / 60 % 60, qs.sec % 60,
                  qs.fetched, qs.pending);
  else
    std::snprintf(note, sizeof(note), "新浪 %02d:%02d:%02d · 当日分钟由 bar1m 给", qs.sec / 3600, qs.sec / 60 % 60, qs.sec % 60);
  if (st.code.empty()) {
    ImGui::TextDisabled("%s", note);
    ImGui::TextWrapped("点表格中的一行查看行情.");
    return;
  }
  const Kline &kl = st.data ? *st.data : kNoKline;
  Series intra, daily;
  build(kl, st.today, kIntraDays[st.chart.intraday], false, intra, daily);

  if (st.i == ~0u)
    std::snprintf(note, sizeof(note), "不在新浪全市场");
  else if (!st.data)
    std::snprintf(note, sizeof(note), "读取历史…");
  else
    gap_note(kl, st.today, note, sizeof(note));
  char title[96];
  std::snprintf(title, sizeof(title), "%s  %s", info && !info->name.empty() ? info->name.c_str() : "-", st.code.c_str());
  header(title, kl, st.today, note);

  // 上: 日内 : 盘口 = 2 : 1; 下: 日线. 显式定位 (档位按钮叠在图上, 画完光标不在流式位置)
  const float gap = 4.f;
  const ImVec2 p0 = ImGui::GetCursorScreenPos(), avail = ImGui::GetContentRegionAvail();
  const float bw = std::floor((avail.x - gap) / 3.f), iw = avail.x - bw - gap;
  const float top_h = std::floor((avail.y - gap) * 0.6f);
  const uint64_t id = static_cast<uint64_t>(st.gen) << 32;
  plot_intraday("##ichart", intra, id, st.chart, ImVec2(iw, top_h));
  ImGui::SetCursorScreenPos(ImVec2(p0.x + iw + gap, p0.y));
  ImGui::PushStyleVar(ImGuiStyleVar_WindowPadding, ImVec2(0, 0));
  ImGui::BeginChild("##book", ImVec2(bw, top_h), false);
  render_book(st.quote, qs.date);
  ImGui::EndChild();
  ImGui::PopStyleVar();
  ImGui::SetCursorScreenPos(ImVec2(p0.x, p0.y + top_h + gap));
  plot_daily("##dchart", daily, id, st.chart, ImVec2(avail.x, avail.y - top_h - gap));
}

void RenderBasketPanel(BasketState &st, int page) {
  const int sel = st.sel[page];
  if (!st.data || st.data->list.empty() || sel < 0) {
    ImGui::TextDisabled("点左栏一行查看走势.");
    return;
  }
  const size_t b = static_cast<size_t>(sel);
  const Baskets &d = *st.data;
  const Basket &bk = d.list[b];
  assert(st.today.size() == d.list.size());
  const Day &today = st.today[b];
  Series intra, daily;
  build(bk.kline, today, kIntraDays[st.chart.intraday], true, intra, daily);

  char note[160];
  if (joins(bk.kline, today)) {
    const int sec = std::get<1>(st.today_key);
    std::snprintf(note, sizeof(note), "当日由新浪快照合成 %02d:%02d:%02d · 成员 / 权重取 %02d-%02d 收盘 (%zu 只)", sec / 3600, sec / 60 % 60,
                  sec % 60, bk.kline.last / 100 % 100, bk.kline.last % 100, bk.size);
  } else if (!gap_note(bk.kline, today, note, sizeof(note))) {
    std::snprintf(note, sizeof(note), "all.bar 截至 %02d-%02d", bk.kline.last / 100 % 100, bk.kline.last % 100);
  }
  header(bk.name.c_str(), bk.kline, today, note);
  ImGui::SameLine();
  if (ImGui::SmallButton("重载"))
    st.want = true;

  // 上: 日内; 下: 日线 (同个股页, 无盘口)
  const float gap = 4.f;
  const ImVec2 p0 = ImGui::GetCursorScreenPos(), avail = ImGui::GetContentRegionAvail();
  const float top_h = std::floor((avail.y - gap) * 0.5f);
  const uint64_t id = static_cast<uint64_t>(st.gen) << 48 | static_cast<uint64_t>(b) << 32;
  plot_intraday("##bichart", intra, id, st.chart, ImVec2(avail.x, top_h));
  ImGui::SetCursorScreenPos(ImVec2(p0.x, p0.y + top_h + gap));
  plot_daily("##bdchart", daily, id, st.chart, ImVec2(avail.x, avail.y - top_h - gap));
}

bool RenderBasketList(const BasketHistory &hist, BasketState &st, int page) {
  if (st.missing || (st.data && st.data->list.empty())) {
    ImGui::TextWrapped(st.missing ? "指数 / 行业由 all.bar 特征库合成: 先在 Features → Compute 算好 all 池的 Bar 库."
                                  : "all.bar 特征库落盘不足 3 日.");
    if (ImGui::SmallButton("重载"))
      st.want = true;
    return false;
  }
  if (!st.data) {
    const BasketKline &x = hist.published();
    ImGui::TextDisabled("读取 all.bar 特征库… %zu / %zu 日", x.done.load(std::memory_order_relaxed), x.total.load(std::memory_order_relaxed));
    return false;
  }
  const Baskets &d = *st.data;
  struct Row {
    size_t b;
    Candle k;
    double prev = 0;
  };
  std::vector<Row> rows;
  for (size_t b = 0; b < d.list.size(); ++b)
    if (d.list[b].industry == (page == 1)) {
      Row &r = rows.emplace_back(Row{b, {}});
      last(d.list[b].kline, st.today[b], r.k, r.prev); // 日线空 → prev 留 0, 行只显示名称
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
        return r.prev > 0 ? r.k.close / r.prev : 0;
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
    if (!(r.prev > 0))
      continue;
    const ImVec4 c = tint(r.k.close, r.prev);
    ImGui::TableSetColumnIndex(1);
    ImGui::TextColored(c, "%.2f", r.k.close);
    ImGui::TableSetColumnIndex(2);
    ImGui::TextColored(c, "%+.2f%%", (r.k.close / r.prev - 1.0) * 100.0);
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
