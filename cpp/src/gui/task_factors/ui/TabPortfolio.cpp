// Factors→Portfolio — 见头文件
#include "gui/task_factors/ui/TabPortfolio.hpp"

#include "gui/Tasks.hpp"                      // StatusColor
#include "gui/task_factors/ui/TabFactors.hpp" // tip / hold_desc
#include "gui/util/Chart.hpp"                 // tint / button_bar / EndTag / NowCross
#include "imgui.h"
#include "implot.h"
#include "implot_internal.h"    // GetItem (主图例显隐 / 池线色), TableFindByID (右表内容宽)
#include "misc/date.hpp"        // dash (现价十字时刻)
#include "shared/AssetAxis.hpp" // asset_axis (成分行代码)
#include "shared/Basket.hpp"    // basket_line / cut_of_line

#include <algorithm>
#include <array>
#include <cassert>
#include <cfloat>
#include <cmath>
#include <cstdarg>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <functional>
#include <initializer_list>
#include <limits>
#include <ranges>
#include <set>
#include <type_traits>
namespace GUI::Factors {

namespace {

constexpr double kNaN = std::numeric_limits<double>::quiet_NaN();
const ImVec4 kHead(0.4f, 0.8f, 1.0f, 1.0f);

void load_current(PortfolioUIState &ui) {
  ui.entries = ui.name.empty() ? std::vector<PortfolioEntry>{} : LoadPortfolio(ui.dir, ui.name);
  ui.items.clear();
  ++ui.items_gen;
  ui.sel = -1;
  ++ui.content_gen;
}
// 重列目录, 选 want (不在 → 第一个 / 无) 并载入
void relist(PortfolioUIState &ui, const std::string &want) {
  ui.names = ListPortfolios(ui.dir);
  ui.name = std::find(ui.names.begin(), ui.names.end(), want) != ui.names.end() ? want : ui.names.empty() ? std::string{}
                                                                                                          : ui.names.front();
  load_current(ui);
}
void save_current(PortfolioUIState &ui) {
  assert(!ui.name.empty());
  SavePortfolio(ui.dir, ui.name, ui.entries);
  ui.items.clear();
  ++ui.items_gen;
  ++ui.content_gen;
}

std::string entry_label(const PortfolioEntry &e) {
  return "(" + e.pool + ", " + factor::stat::hold_name(e.hold) + "): " + std::filesystem::path(e.file).stem().string();
}

// 横轴刻度 (主图 / 区间条): x = 日下标 · kDayW + 分钟 → YY/MM/DD, 只到日 (user_data = 日并集轴)
int day_formatter(double v, char *buf, int size, void *ud) {
  const auto &dates = *static_cast<const std::vector<misc::Ymd> *>(ud);
  if (dates.empty() || v < 0)
    return std::snprintf(buf, size, " ");
  const misc::Ymd y = dates[std::min(static_cast<size_t>(v) / kDayW, dates.size() - 1)];
  return std::snprintf(buf, size, "%02d/%02d/%02d", y / 10000 % 100, y / 100 % 100, y % 100);
}

// 页表 [PortfolioPage]: 页签名 / 页签悬停 / 右表类别条的组数 (前 groups 个 NavGroup) / 行选中方式 / 右表 ID
struct PageDef {
  const char *name, *tip;
  int groups, pick;
  const char *table;
};
constexpr PageDef kPageDefs[kPages] = {
    {"NAV",
     "NAV = 资金曲线 (Net Asset Value, 净值)\n"
     "• 橙线 = 本组合 G20 多头: 各因子独立回测 (分批 1/h̄, 全日平均满仓; 能成交才成交) 的逐行增量等权聚合\n"
     "• 池指数线 = 各池等权指数 (all 池拆 全A.等 / 全A.加); 行业线 = 申万一级 / 概念线 = 东财题材概念 (全A 内切, 总市值加权)\n"
     "• 单因子线 = 各自的 G20 多头净值\n"
     "• 每条成对: 可成交 (篮子 = 标签指数, 收盘封板不建仓 / 平仓顺延; 因子 / 策略 = 扣换手冲击) + 理想 (篮子 = 后复权收盘价指数; 因子 / 策略 = 无冲击; 同色淡线)\n"
     "• 默认不画 (空图); 右侧类别条选一组 (指数 / 行业 / 概念 / 因子 / 策略), 点行选中 = 画该行两条, 主图例默认只显理想, 可成交点图例显; 显隐变了自动重新 fit\n"
     "• 日内 (右侧开关, 默认开): 主图上方画选中的指数 / 行业 / 概念的日内行情 (多行 = 同主图线色叠加; 一行 = 蜡烛 + 现价十字)\n"
     "• 只显示一条线: 视野内末点画现价十字 (同日内栏; 右壁标区间涨跌, 底壁标日期)\n"
     "• 底部区间条选日段: 各线在区间起点归一 = 1, 年化按区间复利",
     kNavGroups, kPickMany, "##pf_nav_tbl"},
    {"VAL",
     "VAL = 估值 (Valuation)\n"
     "• 右侧单选一个池 / 申万一级行业 / 概念: 主图 = 它的指数 + 估值 Band (全A.加 / 行业 / 概念 = 总市值加权, 全A.等 / 其余池 = 等权)\n"
     "• EP 盈利收益率 (1/PE) / BP 账面市值比 (1/PB) / SP 营收市值比 (1/PS) / CP 现金流收益率 (1/PCF) / DY 股息率\n"
     "  (收益率口径, 与指数同权重: 加权 = 整体法, 等权 = 个股均值; 高 = 便宜, 亏损照计为负, 过 0 连续)\n"
     "• 主图左上图例矩阵: 行 = 口, 列 = 当前收益率 / 倍数 / N 年分位 / Band / 等值线; 点色块开关, 点行头 / 列头开关整行 / 整列\n"
     "• Band = 把过去 N 年的估值分位换算成价格线: 指数落在哪档 = 当前估值在历史里的位置 (绿 = 便宜, 红 = 贵)\n"
     "• 等值线 = 绝对估值: 倍数 (PE / PB / PS / PCF / P/D = 1 / 收益率) 的价格线, 档 = 该篮子全史倍数区间 (两头各剔 2% 极端) 20 档等分;\n"
     "  默认关, 悬停一条 = 高亮 + 看该档倍数 / 当日倍数 / 指数到该线还差多少\n"
     "• 线尾刻度 = 指数末点 +10% / -5% / -10% / -20% / -30% 的点位: 对照 Band 线看还要涨跌多少到哪档; 末点另画现价十字 (同 NAV)\n"
     "• 区间 / log 与 NAV 页共用",
     kBasketGroups, kPickOne, "##pf_val_tbl"},
    {"FLOW",
     "FLOW = 资金流 (Money Flow): 指数 / 行业 / 概念的日内净流入, 当日随新浪快照实时动\n"
     "• 净流入 = Σ 成分 Σ 分钟 ± 该分钟额 (分钟涨跌法): 收 > 上一分钟收 (首分钟对昨收) 记 +, < 记 −, 平随上一分钟; 成分之和不加权, 分不出大单\n"
     "• 左图 = 右表选中行 (与 NAV 页同一份选中) 自日轴首日开盘起逐分钟累计的净流入, 跨日接续; 档位 日内 / 2日 … 5日 叠在图左上角\n"
     "• y 口径 (图左上角切): 占比 = 累计净流入 / 近 5 日日均成交额 (默认; 分母盘前已定, 开盘即稳, 跨篮子可比) / 金额 = 累计净流入 (亿元)\n"
     "• 开收盘 (图左上角开关, 默认关): 计入首根 09:31 (开盘集合竞价并入) / 末根 15:00 (收盘集合竞价); 关 = 两根的额与净流入都不计 (图与右表同一开关)\n"
     "• 右表: 日轴各日的净流入占当日成交额 + 档内合计; 过滤条与 NAV / VAL 页同一份\n"
     "• 历史日 = all.bar 库 L_1min (其后至基本面末日 = bar1m) 合成, 当日 = 新浪快照合成; 不看区间条",
     kBasketGroups, kPickMany, "##pf_flow_tbl"},
};

// NAV 组表 [NavGroup]: 右表类别条名 (前 kBasketGroups 个 = VAL / FLOW 类别条) / 线宽 (策略加粗)
constexpr const char *kNavGroupName[kNavGroups] = {"指数", "行业", "概念", "因子", "策略"};
constexpr float kNavWidth[kNavGroups] = {1.0f, 1.0f, 1.0f, 1.0f, 2.0f};
const ImVec4 kStratCol(1.0f, 0.55f, 0.15f, 1.0f);
// 成对线表 [口径][PairLine] (口径 = pair_kind: 0 = 篮子, 1 = 因子 / 策略): 主图例口径列 / 线尾小字后缀; 线 ID 后缀 [PairLine]
constexpr const char *kPairName[2][kPairLines] = {{"可成交", "理想"}, {"有冲击", "无冲击"}};
constexpr const char *kPairTag[2][kPairLines] = {{" 可成交", " 理想"}, {"", " 无冲击"}};
constexpr const char *kPairId[kPairLines] = {"", "_ideal"};
int pair_kind(int g) { return g >= kNavFactor; }
std::string line_id(const PortfolioRow &r, int l) { return r.id + kPairId[l]; }
// VAL 估值口表 [PoolVal]: 名 / 倍数名 (1 / 收益率, 等值线) / 百分比小数位 (五口同为收益率, 显示 %)
struct ValMeta {
  const char *name, *inv;
  int prec;
};
constexpr ValMeta kVal[kPoolVals] = {{"EP", "PE", 2}, {"BP", "PB", 1}, {"SP", "PS", 1}, {"CP", "PCF", 2}, {"DY", "P/D", 2}};
// VAL 线表 (价格线色 = 该篮子行的 nav_color): band 填色 / 线 / 线尾刻度 / 右表格底色的 alpha
constexpr float kBandFillA = 0.12f, kBandLineA = 0.35f, kBandTagA = 0.9f, kValCellA = 0.45f;
// VAL 线尾刻度: 价格线末点 × (1 + r) 的点位 (对照 band 线看离各档还差多少); 色 = band_col(u) (涨绿, 跌越多越红)
struct ValMark {
  double r, u;
};
constexpr ValMark kValMarks[] = {{0.10, 0.0}, {-0.05, 0.6}, {-0.10, 0.7}, {-0.20, 0.85}, {-0.30, 1.0}};
// band 色: u ∈ [0, 1] (0 = 最便宜 绿 → 1 = 最贵 红) 按色相渐变
ImVec4 band_col(double u, float a) {
  ImVec4 c(0.f, 0.f, 0.f, a);
  ImGui::ColorConvertHSVtoRGB(0.33f * static_cast<float>(1.0 - u), 0.8f, 0.9f, c.x, c.y, c.z);
  return c;
}
// 估值值的显示 (收益率 %, 小数位按 kVal), 不可算 → "-"
void fmt_val(int b, double x, char *buf, size_t n) {
  if (std::isfinite(x))
    std::snprintf(buf, n, "%.*f%%", kVal[b].prec, 100.0 * x);
  else
    std::snprintf(buf, n, "-");
}

// VAL 绝对估值等值线: 篮子全史倍数 m (= 1 / 收益率) 区间 kContours 等分, y = unit · m (unit = P · v, 见 PortfolioBand); 很淡的灰, 悬停 kContourHit px 内最近一条高亮
constexpr float kContourHit = 6.0f; // 悬停命中半宽 (px)
const ImVec4 kContourCol(0.6f, 0.6f, 0.6f, 0.22f), kContourHotCol(1.0f, 1.0f, 1.0f, 0.9f);
// 一条等值线 (零拷贝: 按下标回调取点; u 的 NaN 处断开)
struct ContourLine {
  const double *x, *u;
  double m;
};
ImPlotPoint contour_pt(int i, void *p) {
  const ContourLine &l = *static_cast<const ContourLine *>(p);
  return {l.x[i], l.u[i] * l.m};
}

// 表格数值格 (NumCell): 填格时量文本宽, 画时右对齐到该列最宽
NumCell num_cell(const ImVec4 &col, const char *fmt, ...) {
  NumCell c;
  va_list ap;
  va_start(ap, fmt);
  std::vsnprintf(c.s, sizeof(c.s), fmt, ap);
  va_end(ap);
  c.col = col, c.w = ImGui::CalcTextSize(c.s).x;
  return c;
}
NumCell num_none() { return num_cell(ImGui::GetStyleColorVec4(ImGuiCol_TextDisabled), "-"); }
void draw_cell(const NumCell &c, float col_w) {
  ImGui::SetCursorPosX(ImGui::GetCursorPosX() + col_w - c.w);
  ImGui::TextColored(c.col, "%s", c.s);
}

// 右补空格到 px 宽 (等宽字体, 中文占两格: 按实测像素补, 列对齐)
std::string pad_to(const char *s, float px) {
  const float w = ImGui::CalcTextSize(s).x, sp = ImGui::CalcTextSize(" ").x;
  return s + std::string(static_cast<size_t>(std::lround(std::max(0.f, px - w) / sp)), ' ');
}
// NAV 主图例多列排版: 第 c 列起点 = 前 c 列各自全行最宽之和 + 列间隔; 按已拼前缀的实测宽补空格 (取整误差不跨列累积); 空格子不补
template <size_t N>
std::vector<std::string> align_cols(const std::vector<std::array<std::string, N>> &rows) {
  float w[N] = {};
  for (const std::array<std::string, N> &r : rows)
    for (size_t c = 0; c < N; ++c)
      w[c] = std::max(w[c], ImGui::CalcTextSize(r[c].c_str()).x);
  const float gap = ImGui::CalcTextSize("  ").x;
  std::vector<std::string> out;
  out.reserve(rows.size());
  for (const std::array<std::string, N> &r : rows) {
    std::string s;
    float x = 0.f;
    for (size_t c = 0; c < N; ++c) {
      if (c > 0)
        x += w[c - 1] + gap;
      if (!r[c].empty())
        s = pad_to(s.c_str(), x) + r[c];
    }
    out.push_back(std::move(s));
  }
  return out;
}

// OKLCH (感知均匀: 同 L / C 的色环上两色的感知距离 = OKLab 弦长, 小角度 ∝ 色相差) → sRGB; 分量截到 [0, 1] 只吸收舍入
ImVec4 oklch(float L, float C, float hue) {
  const float h = 2.0f * IM_PI * hue, a = C * std::cos(h), b = C * std::sin(h);
  const float l = std::pow(L + 0.3963377774f * a + 0.2158037573f * b, 3.0f), m = std::pow(L - 0.1055613458f * a - 0.0638541728f * b, 3.0f),
              s = std::pow(L - 0.0894841775f * a - 1.2914855480f * b, 3.0f);
  const auto gamma = [](float x) {
    x = std::clamp(x, 0.0f, 1.0f);
    return x <= 0.0031308f ? 12.92f * x : 1.055f * std::pow(x, 1.0f / 2.4f) - 0.055f;
  };
  return {gamma(4.0767416621f * l - 3.3077115913f * m + 0.2309699292f * s), gamma(-1.2684380046f * l + 2.6097574011f * m - 0.3413193965f * s),
          gamma(-0.0041960863f * l - 0.7034186147f * m + 1.7076147010f * s), 1.0f};
}
// NAV 组 g 行 r 线 l 的色 (主图 / 右表 / 日内同一份): 策略 = kStratCol, 其余 = 行色相 (PortfolioRow::hue) 的定亮定艳色, 未定色 = 同亮度灰; 理想列 = 同色淡
constexpr float kIdealAlpha = 0.5f;
constexpr float kRowL = 0.75f, kRowC = 0.125f; // 行色的 OKLCH 亮度 / 艳度: 该亮度下整环都在 sRGB 色域内的艳度上限附近 (环不被截变形)
ImVec4 nav_color(const PortfolioDerived &d, int g, size_t r, int l) {
  const float hue = d.nav[g][r].hue;
  ImVec4 c = g == kNavStrat ? kStratCol : std::isnan(hue) ? oklch(kRowL, 0.0f, 0.0f)
                                                          : oklch(kRowL, kRowC, hue);
  c.w = l == kIdeal ? kIdealAlpha : 1.0f;
  return c;
}
// 成分股的色 (叠加线 / 点中的成分行名同一份): 色相按全局 id 黄金角散开, 亮度 / 艳度同行色
ImVec4 stock_color(uint32_t id) {
  const double h = static_cast<double>(id) * 0.6180339887498949;
  return oklch(kRowL, kRowC, static_cast<float>(h - std::floor(h)));
}
// 成分股的名: 简称 (无档案 / 无简称 → 代码)
std::string stock_name(const StockInfos &infos, uint32_t id) {
  if (id < infos.size() && infos[id].known && !infos[id].name.empty())
    return infos[id].name;
  return asset_axis().code(id);
}

// 页 page 的右表行选中 (按页表的选中方式; NAV / FLOW 同一份)
bool picked(const PortfolioUIState &ui, int page, const PortfolioRow &r) { return ui.pick.rows[kPageDefs[page].pick].contains(r.id); }
// 页 page 点行: 选中 ↔ 取消 (单选 = 换掉原选中); 多选里刚选中 → 其线在主图例里回默认显隐 (理想列显, 可成交列隐)
void pick(PortfolioUIState &ui, int page, const PortfolioRow &r) {
  const int m = kPageDefs[page].pick;
  std::set<std::string> &s = ui.pick.rows[m];
  if (s.erase(r.id))
    return;
  if (m == kPickOne)
    s.clear();
  s.insert(r.id);
  if (m == kPickMany)
    for (int l = 0; l < kPairLines; ++l)
      ui.reveal.insert(line_id(r, l));
}
// 点成分行: 叠加 ↔ 取消 (满 StockHistory::kKeep 只不再加)
void toggle_stock(PortfolioUIState &ui, uint32_t id) {
  if (!ui.pick.stocks.erase(id) && ui.pick.stocks.size() < Database::StockHistory::kKeep)
    ui.pick.stocks.insert(id);
}

// 净值线的近况 (Database::RecentOf 同一核): 日末视图自末个活跃日往前 (起点 = 首日前一日的日末), 前收 = 前一日末
Database::Recent recent_of(const PortfolioLine &c) {
  Database::Mark m[Database::kMarks]; // 新 → 旧
  size_t n = 0;
  for (int k = c.e.empty() ? c.f - 1 : c.l + 1; k >= c.f && n < std::size(m); --k)
    m[n++] = {c.end(k), k > c.f ? c.end(k - 1) : kNaN};
  return Database::RecentOf({m, n});
}

// n 点线 (pt(i) = 第 i 点, x 升序) 视野内末点; 无 (或不可算) → false
template <class Pt>
bool end_point(const ImPlotRect &lim, size_t n, const Pt &pt, ImPlotPoint &p) {
  const auto r = std::views::iota(size_t{0}, n);
  const size_t e = static_cast<size_t>(std::ranges::partition_point(r, [&](size_t i) { return pt(i).x <= lim.X.Max; }) - r.begin());
  if (e == 0)
    return false;
  p = pt(e - 1);
  return p.x >= lim.X.Min && std::isfinite(p.y);
}
template <class Pt>
void push_tag(std::vector<EndTag> &tags, const ImPlotRect &lim, size_t n, const Pt &pt, ImU32 col, std::string text) {
  ImPlotPoint p;
  if (end_point(lim, n, pt, p))
    tags.push_back({ImPlot::PlotToPixels(p), col, std::move(text)});
}

// ---- 切片画线 (零拷贝: ImPlot 按下标回调取点) ----
ImPlotPoint slice_pt(const PortfolioSlice &s, size_t i) { return {s.x(i), s.y(i)}; }
ImPlotPoint slice_getter(int i, void *s) { return slice_pt(*static_cast<const PortfolioSlice *>(s), static_cast<size_t>(i)); }
void plot_slice(const char *label, const PortfolioSlice &s) {
  ImPlot::PlotLineG(label, slice_getter, const_cast<PortfolioSlice *>(&s), static_cast<int>(s.size()));
}
void push_tag(std::vector<EndTag> &tags, const ImPlotRect &lim, const PortfolioSlice &s, ImU32 col, std::string text) {
  push_tag(tags, lim, s.size(), [&](size_t i) { return slice_pt(s, i); }, col, std::move(text));
}
constexpr float kTagScale = 0.95f; // 线尾小字 / 刻度字号 (× 字体)
// 线尾小字右缘 (图区右壁内 2 px)
float tags_right() { return ImPlot::GetPlotPos().x + ImPlot::GetPlotSize().x - 2.0f; }
constexpr float kMarkTick = 6.0f; // 刻度的短横长 (px)
// 刻度占的像素宽 (短横 + 字; 无 → 0)
float marks_width(const std::vector<EndTag> &marks) { return marks.empty() ? 0.0f : end_tags_width(marks, kTagScale) + kMarkTick; }
// 刻度: 真 y 处短横贴点右侧 (同线尾小字, 出界则左移进 right 内), 字左对齐接在短横后, 按像素 y 排开不重叠
void draw_marks(std::vector<EndTag> &marks, float right) {
  if (marks.empty())
    return;
  std::sort(marks.begin(), marks.end(), [](const EndTag &a, const EndTag &b) { return a.at.y < b.at.y; });
  ImFont *font = ImGui::GetFont();
  const float fs = ImGui::GetFontSize() * kTagScale;
  ImDrawList *dl = ImPlot::GetPlotDrawList();
  ImPlot::PushPlotClipRect();
  float y = -FLT_MAX;
  for (const EndTag &m : marks) {
    const ImVec2 sz = font->CalcTextSizeA(fs, FLT_MAX, 0.0f, m.text.c_str());
    const float x0 = std::min(m.at.x + kTagGap, right - marks_width(marks) + kTagGap), x1 = x0 + kMarkTick; // 短横 [x0, x1); 字左缘 = x1 + kTagGap
    dl->AddLine(ImVec2(x0, m.at.y), ImVec2(x1, m.at.y), m.col);
    y = std::max(m.at.y, y + fs);
    dl->AddText(font, fs, ImVec2(x1 + kTagGap, y - 0.5f * sz.y), m.col, m.text.c_str());
  }
  ImPlot::PopPlotClipRect();
}

// 主图一帧的公共件 (NAV / VAL 同一份): 小字筛选视野 + 线尾小字 + 线尾刻度 (VAL) + 显示的线 (只一条 → 现价十字) + 显隐签名 (下帧进 fit 键)
struct PlotFrame {
  ImPlotRect lim;
  std::vector<EndTag> tags;
  std::vector<EndTag> marks;
  std::vector<const PortfolioSlice *> lines;
  std::string shown;
  // 图例项 label 的显隐记进 shown; 显示 → 该项 (Color = 实际线色), 否则 nullptr
  const ImPlotItem *item(const char *label) {
    const ImPlotItem *it = ImPlot::GetItem(label);
    assert(it);
    shown += it->Show ? '1' : '0';
    return it->Show ? it : nullptr;
  }
};
// 现价十字 (同日内栏): 只显示一条线时, 其视野内末点; 读数 = 区间涨跌 (切片起点 = 1), 时刻 = 该点所在日. 无 → false
bool now_cross(const PlotFrame &pf, const std::vector<misc::Ymd> &dates, NowCross &c) {
  if (pf.lines.size() != 1)
    return false;
  const PortfolioSlice &s = *pf.lines.front();
  ImPlotPoint p;
  if (!end_point(pf.lim, s.size(), [&](size_t i) { return slice_pt(s, i); }, p))
    return false;
  char y[24];
  std::snprintf(y, sizeof(y), "%+.2f%%", 100.0 * (p.y - 1.0));
  const misc::Ymd day = dates[std::min(static_cast<size_t>(p.x) / kDayW, dates.size() - 1)];
  c = {ImPlot::PlotToPixels(p), ImGui::ColorConvertFloat4ToU32(tint(p.y, 1.0)), y, misc::dash(day)};
  return true;
}

// ---- 头: 组合 / 成本 / Recompute / Cancel / 状态 / Clear (选择与图回默认) + 失效 / 失败条目原因. 返回 1 = Recompute, -1 = Cancel, 0 = 无 ----
int head(PortfolioService &svc, PortfolioUIState &ui, const PortfolioDerived &d, const PortfolioResult &res, const std::string &message,
         double fixed_impact) {
  int action = 0;
  size_t n_bad = 0;
  for (const PortfolioItem &it : ui.items)
    n_bad += !it.missing.empty();
  ImGui::Text("%s  %zu 因子 (算成 %d", ui.name.c_str(), ui.entries.size(), d.n_ready);
  ImGui::SameLine(0, 0);
  if (d.n_error > 0) {
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Error), ", 失败 %d", d.n_error);
    ImGui::SameLine(0, 0);
  }
  if (n_bad > 0) {
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Error), ", 失效 %zu", n_bad);
    ImGui::SameLine(0, 0);
  }
  ImGui::Text(")");
  ImGui::SameLine();
  ImGui::TextDisabled("|");
  ImGui::SameLine();
  ImGui::TextDisabled("有冲击");
  if (ImGui::IsItemHovered())
    tip("换手成本 = 各因子自己的最小冲击段, 不可选\n"
        "• 每行开 1/h̄ 净值一批 (h̄ = 平均持有行数, 全日平均满仓), 换手按 Δw: 上一批平仓与本批建仓同一价格点时只交接差额 (留仓不成交);\n"
        "  新进成交不了不建仓、续持者封板照持也加不了仓, 平仓成交不了的顺延\n"
        "• 用了 lob.* 叶: 增仓 × 最小金额段冲击 (封顶 max_impact) + 平仓量 × 固定冲击 %.1f bp\n"
        "• 否则固定段: 增仓量 / 平仓量都按固定冲击 %.1f bp\n"
        "• 组合只算 G20 多头; 无冲击线不扣",
        fixed_impact * 1e4, fixed_impact * 1e4);
  ImGui::SameLine();
  ImGui::Checkbox("log", &ui.log_y);
  if (ImGui::IsItemHovered())
    tip("y 轴 (净值) 切对数刻度");
  ImGui::SameLine();
  const bool busy = svc.busy();
  ImGui::BeginDisabled(busy);
  if (ImGui::Button("Recompute", ImVec2(90, 0)))
    action = 1;
  ImGui::EndDisabled();
  if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled))
    tip("弃缓存重算全部因子 + 池指数 (特征库重算过之后用)\n"
        "• 平时不用: 组合加 / 删因子自动只算新的那个");
  ImGui::SameLine();
  ImGui::BeginDisabled(!busy);
  if (ImGui::Button("Cancel", ImVec2(60, 0)))
    action = -1;
  ImGui::EndDisabled();
  ImGui::SameLine();
  {
    const Phase ph = svc.phase.load(std::memory_order_acquire);
    const int jd = svc.jobs_done.load(std::memory_order_relaxed), jt = svc.jobs_total.load(std::memory_order_relaxed);
    const int done = static_cast<int>(svc.done.load(std::memory_order_relaxed)), total = static_cast<int>(svc.total.load(std::memory_order_relaxed));
    const ImVec4 bc = StatusColor(TaskStatus::Kind::Busy);
    switch (ph) {
    case Phase::Idle:
    case Phase::Scanning:
      ImGui::TextColored(StatusColor(TaskStatus::Kind::Muted), "idle");
      break;
    case Phase::Loading:
      ImGui::TextColored(bc, "[%d/%d] loading %d/%d days", jd + 1, jt, done, total);
      break;
    case Phase::Running:
      ImGui::TextColored(bc, "[%d/%d] running %d/%d", jd + 1, jt, done, total);
      break;
    case Phase::Done:
      ImGui::TextColored(StatusColor(TaskStatus::Kind::Ready), "done");
      break;
    case Phase::Cancelled:
      ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "cancelled %d/%d", jd, jt);
      break;
    }
  }
  if (!message.empty()) {
    ImGui::SameLine();
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "| %s", message.c_str());
  }
  ImGui::SameLine();
  if (ImGui::Button("Clear", ImVec2(60, 0))) {
    ui.pick = PortfolioPick{.of = ui.name};
    ui.range = PortfolioRange{.d0 = 0, .d1 = static_cast<int>(d.dates.size())};
    ui.log_y = false;
  }
  if (ImGui::IsItemHovered())
    tip("清掉全部选择, 图回默认 (空): NAV / FLOW 选中行 / VAL 选中篮子 / 展开的行 / 叠加的成分股 / 区间 (回全段) / log 关");
  for (const PortfolioItem &it : ui.items) {
    const auto f = res.factors.find(it.e.key());
    const std::string &why = !it.missing.empty() ? it.missing : f != res.factors.end() ? f->second->error
                                                                                       : it.missing;
    if (!why.empty())
      ImGui::TextColored(StatusColor(TaskStatus::Kind::Error), "  %s  %s", entry_label(it.e).c_str(), why.c_str());
  }
  return action;
}

// ---- 页签: NAV = 资金曲线 / VAL = 池估值 (同一布局; 区间 / log 共用) / FLOW = 篮子日内资金流. 返回 PortfolioPage ----
int pages() {
  int page = kPageNav;
  if (ImGui::BeginTabBar("##pf_page")) {
    for (int p = 0; p < kPages; ++p) {
      const bool open = ImGui::BeginTabItem(kPageDefs[p].name);
      if (ImGui::IsItemHovered())
        tip("%s", kPageDefs[p].tip);
      if (open) {
        page = p;
        ImGui::EndTabItem();
      }
    }
    ImGui::EndTabBar();
  }
  return page;
}

// ---- NAV 主图: 画右表选中行的两条线, 按组序 (策略在最上层). 主图例 = cn | 口径 | 区间年化 | 区间夏普 | 年换手 (align_cols; 悬停一项看 hover), ###线 ID 定 ID (数值变了不换 item);
//      点图例单独隐 / 显一条 (行刚选中 → ui.reveal 强制回默认显隐一次: 理想列显, 可成交列隐) ----
void plot_nav(PortfolioUIState &ui, const PortfolioDerived &d, const PortfolioView &v, PlotFrame &pf) {
  struct Draw {
    int g, l;
    const PortfolioRow *row;
    const PortfolioSlice *s;
    ImVec4 col;
  };
  std::vector<Draw> ds; // 画序, 只含选中行的
  for (int g = 0; g < kNavGroups; ++g)
    for (size_t r = 0; r < d.nav[g].size(); ++r)
      if (picked(ui, kPageNav, d.nav[g][r]))
        for (int l = 0; l < kPairLines; ++l)
          ds.push_back({g, l, &d.nav[g][r], &v.nav[g][r][l], nav_color(d, g, r, l)});
  std::vector<std::array<std::string, 5>> rows;
  for (const Draw &x : ds) {
    char ann[48] = "", shp[48] = "", turn[48] = "";
    if (std::isfinite(x.s->ann))
      std::snprintf(ann, sizeof(ann), "(年化=%+5.1f%%)", x.s->ann);
    if (std::isfinite(x.s->sharpe))
      std::snprintf(shp, sizeof(shp), "(夏普=%+5.2f)", x.s->sharpe);
    if (std::isfinite(x.row->turn))
      std::snprintf(turn, sizeof(turn), "(年换手=%5.1f倍)", x.row->turn);
    rows.push_back({x.row->cn, kPairName[pair_kind(x.g)][x.l], ann, shp, turn});
  }
  const std::vector<std::string> texts = align_cols(rows);
  for (size_t i = 0; i < ds.size(); ++i) {
    const Draw &x = ds[i];
    const std::string id = line_id(*x.row, x.l), lb = texts[i] + "###" + id;
    if (ui.reveal.erase(id))
      ImPlot::HideNextItem(x.l == kTrade, ImPlotCond_Always);
    ImPlot::SetNextLineStyle(x.col, kNavWidth[x.g]);
    plot_slice(lb.c_str(), *x.s);
    if (ImPlot::IsLegendEntryHovered(lb.c_str()))
      tip("%s", x.row->hover.c_str());
    if (pf.item(lb.c_str())) {
      push_tag(pf.tags, pf.lim, *x.s, ImGui::ColorConvertFloat4ToU32(x.col), x.row->tag + kPairTag[pair_kind(x.g)][x.l]);
      pf.lines.push_back(x.s);
    }
  }
}

// ---- VAL 主图例 = 自绘矩阵 (对仗 Inspect 期限结构图例), 绘图区左上: 行 = 口 (EP / BP / SP / CP / DY), 列 = 当前收益率 | 当前倍数 | N 年估值分位 | Band | 等值线;
//      开关列的格 = 色块 (实心 = 显示, 空框 = 隐), 点格切该项, 点行头 / 列头切整行 / 整列 (有显示的 → 全隐, 否则全显); 悬停格 / 头 → 对应线高亮 (下帧) ----
enum ValLgCol : int { kLgBand,
                      kLgContour };
constexpr int kLgToggles = 2;
constexpr float kHiScale = 2.0f; // 图例悬停高亮: 线宽倍数 (等值线改用 kContourHotA)
constexpr float kContourHotA = 0.6f;
// 上帧图例矩阵的悬停 (b = −1 整列, c = −1 整行) + 框 (悬停等值线避开); 本图唯一实例
struct ValLegendHl {
  bool on = false;
  int b = -1, c = -1;
  ImRect box;
  bool at(int b0, int c0) const { return on && (b < 0 || b == b0) && (c < 0 || c == c0); }
};
// 开关列 c 口 b 的 item label (Band / 等值线). 须有可显示的名: ImPlot 对无名 item 每帧强制 Show = true, 开关不住; 图本身 NoLegend, 名不显示
std::string val_item(int c, int b) {
  return std::string(kVal[b].name) + (c == kLgBand ? " Band###pf_band" : " 等值线###pf_contour") + std::to_string(b);
}

void val_legend(const PortfolioView &v, int years, ValLegendHl &hl) {
  constexpr int kTexts = 3, kCols = kTexts + kLgToggles;
  const ImPlotStyle &ps = ImPlot::GetStyle();
  const ImVec2 inner = ps.LegendInnerPadding;
  const float th = ImGui::GetTextLineHeight(), sw = th * 0.7f, gx = 2.f * ImGui::GetStyle().ItemSpacing.x;
  char yrs[24];
  std::snprintf(yrs, sizeof(yrs), "%d年分位", years);
  const char *head[kCols] = {"当前", "倍数", yrs, "Band", "等值线"};
  std::array<std::array<std::string, kTexts>, kPoolVals> txt;
  for (int b = 0; b < kPoolVals; ++b) {
    const PortfolioBand &bd = v.band[b];
    char a[24] = "-", m[24] = "-", p[16] = "-";
    if (bd.ok) {
      fmt_val(b, bd.now, a, sizeof(a));
      if (bd.now > 0.0)
        std::snprintf(m, sizeof(m), "%s %.2f", kVal[b].inv, 1.0 / bd.now);
      std::snprintf(p, sizeof(p), "%3.0f%%", bd.pct);
    }
    txt[static_cast<size_t>(b)] = {a, m, p};
  }
  float w_head = 0.f, w[kCols], xc[kCols];
  for (const ValMeta &vm : kVal)
    w_head = std::max(w_head, ImGui::CalcTextSize(vm.name).x);
  const ImVec2 p0 = ImPlot::GetPlotPos(), o(p0.x + ps.LegendPadding.x, p0.y + ps.LegendPadding.y);
  float x = o.x + inner.x + w_head;
  for (int c = 0; c < kCols; ++c) {
    w[c] = std::max(ImGui::CalcTextSize(head[c]).x, c < kTexts ? 0.f : sw);
    if (c < kTexts)
      for (const auto &r : txt)
        w[c] = std::max(w[c], ImGui::CalcTextSize(r[static_cast<size_t>(c)].c_str()).x);
    xc[c] = x + gx;
    x = xc[c] + w[c];
  }
  const ImRect box(o, ImVec2(x + inner.x, o.y + 2.f * inner.y + static_cast<float>(kPoolVals + 1) * th));
  const auto y_row = [&](int b) { return box.Min.y + inner.y + static_cast<float>(b + 1) * th; }; // b = −1 → 列头行
  const auto button = [&](const ImRect &r, const char *id, bool &hov) {
    bool held = false;
    const ImGuiID gid = ImGui::GetID(id);
    ImGui::KeepAliveID(gid);
    return ImGui::ButtonBehavior(r, gid, &hov, &held, ImGuiButtonFlags_AllowOverlap | ImGuiButtonFlags_PressedOnClick);
  };
  const auto item_of = [&](int c, int b) {
    ImPlotItem *it = ImPlot::GetItem(val_item(c, b).c_str()); // 本帧刚 Plot 过, 必在池里
    assert(it);
    return it;
  };
  // 整行 / 整列: 集合里有显示的 → 全隐, 否则全显 (b0 / c0 = −1 = 不限)
  const auto any_shown = [&](int b0, int c0) {
    for (int b = 0; b < kPoolVals; ++b)
      for (int c = 0; c < kLgToggles; ++c)
        if ((b0 < 0 || b == b0) && (c0 < 0 || c == c0) && item_of(c, b)->Show)
          return true;
    return false;
  };
  const auto toggle_set = [&](int b0, int c0) {
    const bool any = any_shown(b0, c0);
    for (int b = 0; b < kPoolVals; ++b)
      for (int c = 0; c < kLgToggles; ++c)
        if ((b0 < 0 || b == b0) && (c0 < 0 || c == c0))
          item_of(c, b)->Show = !any;
  };
  bool bg_hov = false;
  button(box, "##pf_val_legend", bg_hov);
  ImDrawList &dl = *ImPlot::GetPlotDrawList();
  ImPlot::PushPlotClipRect();
  dl.AddRectFilled(box.Min, box.Max, ImPlot::GetStyleColorU32(ImPlotCol_LegendBg));
  dl.AddRect(box.Min, box.Max, ImPlot::GetStyleColorU32(ImPlotCol_LegendBorder));
  const ImU32 c_txt = ImGui::GetColorU32(ImGuiCol_Text), c_dis = ImGui::GetColorU32(ImGuiCol_TextDisabled),
              c_hov = ImGui::GetColorU32(ImGuiCol_PlotLinesHovered);
  ValLegendHl nx;
  nx.box = box;
  char id[32];
  for (int c = 0; c < kCols; ++c) { // 列头 (开关列可点)
    const ImRect r(ImVec2(xc[c], y_row(-1)), ImVec2(xc[c] + w[c], y_row(-1) + th));
    const float tw = ImGui::CalcTextSize(head[c]).x;
    const ImVec2 at(c < kTexts ? r.Max.x - tw : r.Min.x + (w[c] - tw) * 0.5f, r.Min.y);
    if (c < kTexts) {
      dl.AddText(at, c_dis, head[c]);
      continue;
    }
    std::snprintf(id, sizeof(id), "##pf_lg_c%d", c);
    bool hov = false;
    if (button(r, id, hov))
      toggle_set(-1, c - kTexts);
    if (hov)
      nx.on = true, nx.b = -1, nx.c = c - kTexts;
    dl.AddText(at, hov ? c_hov : any_shown(-1, c - kTexts) ? c_txt
                                                           : c_dis,
               head[c]);
  }
  for (int b = 0; b < kPoolVals; ++b) {
    const float yb = y_row(b);
    { // 行头
      const ImRect r(ImVec2(box.Min.x + inner.x, yb), ImVec2(box.Min.x + inner.x + w_head, yb + th));
      std::snprintf(id, sizeof(id), "##pf_lg_r%d", b);
      bool hov = false;
      if (button(r, id, hov))
        toggle_set(b, -1);
      if (hov)
        nx.on = true, nx.b = b, nx.c = -1;
      dl.AddText(r.Min, hov ? c_hov : any_shown(b, -1) ? c_txt
                                                       : c_dis,
                 kVal[b].name);
    }
    const PortfolioBand &bd = v.band[b];
    for (int c = 0; c < kTexts; ++c) { // 数值列右对齐; 分位按便宜绿 → 贵红着色
      const std::string &t = txt[static_cast<size_t>(b)][static_cast<size_t>(c)];
      const ImU32 col = !bd.ok ? c_dis : c == 2 ? ImGui::ColorConvertFloat4ToU32(band_col(bd.pct / 100.0, 1.0f))
                                                : c_txt;
      dl.AddText(ImVec2(xc[c] + w[c] - ImGui::CalcTextSize(t.c_str()).x, yb), col, t.c_str());
    }
    for (int c = 0; c < kLgToggles; ++c) {
      const float xq = xc[kTexts + c], wq = w[kTexts + c];
      const ImRect r(ImVec2(xq, yb), ImVec2(xq + wq, yb + th));
      const ImVec2 s0(xq + (wq - sw) * 0.5f, yb + (th - sw) * 0.5f), s1(s0.x + sw, s0.y + sw);
      std::snprintf(id, sizeof(id), "##pf_lg_%d_%d", b, c);
      bool hov = false;
      ImPlotItem *it = item_of(c, b);
      if (button(r, id, hov))
        it->Show = !it->Show;
      if (hov) {
        nx.on = true, nx.b = b, nx.c = c;
        const std::vector<double> &lv = bd.levels;
        if (c == kLgBand)
          tip("%s Band: 过去 %d 年估值分位换算的价格线 (点 = 开关)", kVal[b].name, years);
        else if (!lv.empty())
          tip("%s 等值线: %s %.2f ~ %.2f 倍 %d 等分 (该篮子全史倍数区间, 两头各剔 %.0f%%)\n点 = 开关; 悬停主图里的线看该档", kVal[b].name, kVal[b].inv, lv.front(),
              lv.back(), kContours - 1, 100.0 * kContourTrim);
        else
          tip("%s 等值线: 全史不可算 (无收益率 > 0 的日子)", kVal[b].name);
      }
      const float a = hov ? 0.75f : 1.f;
      if (!it->Show)
        dl.AddRect(s0, s1, hov ? c_txt : c_dis);
      else if (c == kLgBand) // 色块 = band 渐变 (便宜绿 → 贵红)
        dl.AddRectFilledMultiColor(s0, s1, ImGui::ColorConvertFloat4ToU32(band_col(0.0, a)), ImGui::ColorConvertFloat4ToU32(band_col(1.0, a)),
                                   ImGui::ColorConvertFloat4ToU32(band_col(1.0, a)), ImGui::ColorConvertFloat4ToU32(band_col(0.0, a)));
      else
        dl.AddRectFilled(s0, s1, ImGui::ColorConvertFloat4ToU32(ImVec4(kContourCol.x, kContourCol.y, kContourCol.z, 0.8f * a)));
    }
  }
  ImPlot::PopPlotClipRect();
  hl = nx;
}

// VAL 悬停等值线: 鼠标 x 处 (相邻两点插值) 各开着的口里离鼠标最近的一档, kContourHit px 内 → 高亮 (最上层) + 提示数据; 鼠标在图例矩阵上不算
void hover_contour(const PortfolioDerived &d, const PortfolioView &v, const PortfolioSlice &sl, const bool *on, const ImRect &legend) {
  const double *bx = v.band_x.data();
  const size_t n = v.band_x.size();
  if (!ImPlot::IsPlotHovered() || legend.Contains(ImGui::GetMousePos()))
    return;
  const ImPlotPoint mp = ImPlot::GetPlotMousePos();
  const size_t j = static_cast<size_t>(std::upper_bound(bx, bx + n, mp.x) - bx);
  if (j == 0 || j >= n)
    return;
  const double t = (mp.x - bx[j - 1]) / (bx[j] - bx[j - 1]);
  const auto lerp = [&](double a, double c) { return a + (c - a) * t; };
  const float my = ImGui::GetMousePos().y;
  int hb = -1;
  double m = 0.0, hu = 0.0;
  float best = kContourHit;
  for (int b = 0; b < kPoolVals; ++b) {
    const std::vector<double> &u = v.band[b].unit;
    if (!on[b] || !std::isfinite(u[j - 1]) || !std::isfinite(u[j]))
      continue;
    const double uu = lerp(u[j - 1], u[j]);
    for (const double lv : v.band[b].levels)
      if (const float dy = std::abs(ImPlot::PlotToPixels(mp.x, uu * lv).y - my); dy < best)
        best = dy, hb = b, m = lv, hu = uu;
  }
  if (hb < 0)
    return;
  const double px = lerp(sl.y(j - 1), sl.y(j)), now = px / hu;
  ContourLine l{bx, v.band[hb].unit.data(), m};
  ImPlot::SetNextLineStyle(kContourHotCol, 1.5f);
  ImPlot::PlotLineG("##pf_contour_hot", contour_pt, &l, static_cast<int>(n), ImPlotItemFlags_NoFit | ImPlotItemFlags_NoLegend);
  const misc::Ymd day = d.dates[std::min(static_cast<size_t>(bx[t < 0.5 ? j - 1 : j]) / kDayW, d.dates.size() - 1)];
  const ValMeta &vm = kVal[hb];
  char y_line[24], y_now[24];
  fmt_val(hb, 1.0 / m, y_line, sizeof(y_line));
  fmt_val(hb, 1.0 / now, y_now, sizeof(y_now));
  tip("%s 等值线: %s = %.2f 倍 (%s %s)\n"
      "• %d/%02d/%02d: 线上点位 %.3f, 指数 %.3f (%s %.2f 倍, %s %s)\n"
      "• 指数到该线 %+.1f%%",
      vm.name, vm.inv, m, vm.name, y_line, day / 10000, day / 100 % 100, day % 100, hu * m, px, vm.inv, now, vm.name, y_now, 100.0 * (m / now - 1.0));
}

// ---- VAL 主图: 选中篮子各口滚动 band (填色 + kBandQ 条线共一个 item, 一键开关; 首次默认只开 BP)
//      + 各口绝对估值等值线 (同口各档共一个 item, 首次默认关; 不进 fit) + 价格线 (常显) + 悬停等值线高亮 + 图例矩阵 (最上层) ----
void plot_val(const PortfolioUIState &ui, const PortfolioDerived &d, const PortfolioView &v, PortfolioAt vs, PlotFrame &pf) {
  static ValLegendHl s_hl;
  const PortfolioRow &s = d.nav[vs.g][vs.r];
  const PortfolioSlice &sl = v.nav[vs.g][vs.r][kTrade];
  const double *bx = v.band_x.data();
  const int bn = static_cast<int>(v.band_x.size());
  for (int b = 0; b < kPoolVals; ++b) {
    const PortfolioBand &bd = v.band[b];
    const std::string lbs = val_item(kLgBand, b);
    const char *lb = lbs.c_str();
    const float hw = s_hl.at(b, kLgBand) ? kHiScale : 1.0f;
    if (b != kValBp)
      ImPlot::HideNextItem(true, ImPlotCond_Once);
    // 色块 c ∈ [0, kBandQ]: 0 = 最便宜档以下 (到图底), kBandQ = 最贵档以上 (到图顶; 两头不进 fit), 其余 = 相邻两档之间
    for (const auto &[i0, i1] : bd.segs) {
      const int n = i1 - i0;
      const double *x = bx + i0;
      for (int c = 0; c <= kBandQ; ++c) {
        ImPlot::SetNextFillStyle(band_col(static_cast<double>(c) / kBandQ, 1.0f), kBandFillA);
        if (c == 0)
          ImPlot::PlotShaded(lb, x, bd.y[0].data() + i0, n, -HUGE_VAL, ImPlotItemFlags_NoFit);
        else if (c == kBandQ)
          ImPlot::PlotShaded(lb, x, bd.y[kBandQ - 1].data() + i0, n, HUGE_VAL, ImPlotItemFlags_NoFit);
        else
          ImPlot::PlotShaded(lb, x, bd.y[c - 1].data() + i0, bd.y[c].data() + i0, n);
      }
    }
    for (int j = 0; j < kBandQ; ++j) {
      ImPlot::SetNextLineStyle(band_col((j + 0.5) / kBandQ, kBandLineA), (j == kBandQ / 2 ? 1.5f : 1.0f) * hw);
      ImPlot::PlotLine(lb, bx, bd.y[j].data(), bn, ImPlotLineFlags_SkipNaN);
    }
    pf.item(lb);
  }
  // 等值线: 每档一条 (同一 item; 无档也登记). 开关不记进 pf.shown (不进 fit, 不必重新 fit)
  bool on[kPoolVals];
  for (int b = 0; b < kPoolVals; ++b) {
    const std::vector<double> &lv = v.band[b].levels;
    const std::string lbs = val_item(kLgContour, b);
    const char *lb = lbs.c_str();
    const ImVec4 col(kContourCol.x, kContourCol.y, kContourCol.z, s_hl.at(b, kLgContour) ? kContourHotA : kContourCol.w);
    ImPlot::HideNextItem(true, ImPlotCond_Once);
    if (lv.empty())
      ImPlot::PlotLineG(lb, contour_pt, nullptr, 0, ImPlotItemFlags_NoFit);
    for (const double m : lv) {
      ContourLine l{bx, v.band[b].unit.data(), m};
      ImPlot::SetNextLineStyle(col, 1.0f);
      ImPlot::PlotLineG(lb, contour_pt, &l, bn, ImPlotItemFlags_NoFit);
    }
    const ImPlotItem *it = ImPlot::GetItem(lb);
    assert(it);
    on[b] = it->Show;
  }
  ImPlot::SetNextLineStyle(nav_color(d, vs.g, vs.r, kTrade), 2.0f);
  plot_slice("##pf_val_px", sl);
  if (const ImPlotItem *it = pf.item("##pf_val_px")) {
    push_tag(pf.tags, pf.lim, sl, it->Color, s.tag);
    pf.lines.push_back(&sl);
    ImPlotPoint p;
    if (end_point(pf.lim, sl.size(), [&](size_t i) { return slice_pt(sl, i); }, p))
      for (const ValMark &m : kValMarks) {
        char t[16];
        std::snprintf(t, sizeof(t), "%+.0f%%", 100.0 * m.r);
        pf.marks.push_back({ImPlot::PlotToPixels(p.x, p.y * (1.0 + m.r)), ImGui::ColorConvertFloat4ToU32(band_col(m.u, kBandTagA)), t});
      }
  }
  hover_contour(d, v, sl, on, s_hl.box);
  val_legend(v, ui.roll.key.years, s_hl);
}

// ---- 区间条: 全段组合 (有冲击) / VAL 选中篮子价格 (可成交指数); 选段 [d0, d1) 两边吸附到月初 (每月首个交易日; 右边 = 下月月初 / 末尾), 拖动中即吸附即生效 ----
//   左右边 = 竖把手 (拖改起止, 双击拉到头); 段内拖 = 平移 (月数不变); 悬停 / 拖动时把手旁标起止日
void range_bar(PortfolioUIState &ui, bool nav, PortfolioAt vs, float pw, float bar_h) {
  const PortfolioDerived &d = ui.der.val;
  PortfolioRange &rg = ui.range;
  const int D = static_cast<int>(d.dates.size());
  if (!ImPlot::BeginPlot("##pf_range", ImVec2(pw, bar_h),
                         ImPlotFlags_NoTitle | ImPlotFlags_NoLegend | ImPlotFlags_NoMenus | ImPlotFlags_NoBoxSelect | ImPlotFlags_NoMouseText))
    return;
  ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_None, ImPlotAxisFlags_NoDecorations | ImPlotAxisFlags_AutoFit);
  if (D > 0)
    ImPlot::SetupAxisLimits(ImAxis_X1, 0.0, x_of(d.dates.size(), kOpen), ImPlotCond_Always);
  ImPlot::SetupAxisFormat(ImAxis_X1, day_formatter, &ui.der.val.dates);
  if (ui.log_y)
    ImPlot::SetupAxisScale(ImAxis_Y1, ImPlotScale_Log10);
  static const PortfolioLine kNone;
  const PortfolioLine &rc = nav ? d.nav[kNavStrat][0].line[kTrade] : vs.g >= 0 ? d.nav[vs.g][vs.r].line[kTrade]
                                                                               : kNone;
  ImPlot::SetNextLineStyle(nav || vs.g < 0 ? kStratCol : nav_color(d, vs.g, vs.r, kTrade), 1.0f);
  ImPlot::PlotLine("##pf_range_line", rc.x.data(), rc.y.data(), static_cast<int>(rc.x.size()));
  if (D > 0) {
    std::vector<int> ms; // 吸附点 = 月初下标, 末尾补 D
    for (int i = 0; i < D; ++i)
      if (i == 0 || d.dates[i] / 100 != d.dates[i - 1] / 100)
        ms.push_back(i);
    ms.push_back(D);
    const int K = static_cast<int>(ms.size()) - 1;
    const auto near_k = [&](double day, int lo, int hi) { // ms[lo..hi] 里离 day 最近的那个的下标
      int best = lo;
      for (int k = lo + 1; k <= hi; ++k)
        if (std::abs(ms[k] - day) < std::abs(ms[best] - day))
          best = k;
      return best;
    };
    const ImPlotRect lim = ImPlot::GetPlotLimits();
    const auto px_of = [&](int day) { return ImPlot::PlotToPixels(x_of(static_cast<size_t>(day), kOpen), lim.Y.Min).x; };
    const ImVec2 pp = ImPlot::GetPlotPos(), ps = ImPlot::GetPlotSize(), mp = ImGui::GetMousePos();
    const double mday = ImPlot::GetPlotMousePos().x / static_cast<double>(kDayW);
    constexpr float kGrab = 7.0f; // 把手抓取半宽 (px)
    int over = 0;                 // 鼠标下: 1 左把手 / 2 右把手 / 3 段内
    if (mp.x >= pp.x && mp.x <= pp.x + ps.x && mp.y >= pp.y && mp.y <= pp.y + ps.y &&
        ImGui::IsWindowHovered(ImGuiHoveredFlags_AllowWhenBlockedByActiveItem)) {
      const float x0 = px_of(rg.d0), x1 = px_of(rg.d1);
      over = std::abs(mp.x - x0) <= kGrab ? 1 : std::abs(mp.x - x1) <= kGrab ? 2
                                            : (mp.x > x0 && mp.x < x1)       ? 3
                                                                             : 0;
    }
    int k0 = near_k(rg.d0, 0, K), k1 = near_k(rg.d1, 0, K);
    if (k1 <= k0) // 保 0 ≤ k0 < k1 ≤ K (下面按边拖的取值范围要它)
      k1 = std::min(k0 + 1, K), k0 = k1 - 1;
    if (rg.drag == 0 && over != 0 && ImGui::IsMouseClicked(0))
      rg.drag = over, rg.grab_day = mday, rg.grab_k0 = k0, rg.grab_k1 = k1;
    if (rg.drag != 0 && !ImGui::IsMouseDown(0))
      rg.drag = 0;
    if (rg.drag == 1)
      k0 = near_k(mday, 0, k1 - 1);
    else if (rg.drag == 2)
      k1 = near_k(mday, k0 + 1, K);
    else if (rg.drag == 3) {
      const int w = rg.grab_k1 - rg.grab_k0;
      k0 = near_k(ms[rg.grab_k0] + (mday - rg.grab_day), 0, K - w), k1 = k0 + w;
    }
    if (over == 1 && ImGui::IsMouseDoubleClicked(0))
      k0 = 0;
    if (over == 2 && ImGui::IsMouseDoubleClicked(0))
      k1 = K;
    if (k1 <= k0)
      k1 = std::min(k0 + 1, K), k0 = k1 - 1;
    rg.d0 = ms[k0], rg.d1 = ms[k1];
    const int act = rg.drag != 0 ? rg.drag : over;
    if (act == 1 || act == 2)
      ImGui::SetMouseCursor(ImGuiMouseCursor_ResizeEW);
    else if (act == 3)
      ImGui::SetMouseCursor(ImGuiMouseCursor_Hand);

    // 画: 段外压暗 + 段内淡色 + 两边竖把手 (带握纹, 悬停 / 拖动高亮)
    const float x0 = px_of(rg.d0), x1 = px_of(rg.d1), yb = pp.y + ps.y;
    ImDrawList &dl = *ImPlot::GetPlotDrawList();
    ImPlot::PushPlotClipRect();
    dl.AddRectFilled(pp, ImVec2(x0, yb), IM_COL32(0, 0, 0, 120));
    dl.AddRectFilled(ImVec2(x1, pp.y), ImVec2(pp.x + ps.x, yb), IM_COL32(0, 0, 0, 120));
    dl.AddRectFilled(ImVec2(x0, pp.y), ImVec2(x1, yb), IM_COL32(102, 204, 255, 28));
    const auto handle = [&](float x, bool hot) {
      const ImU32 c = hot ? IM_COL32(190, 240, 255, 255) : IM_COL32(102, 204, 255, 255);
      dl.AddLine(ImVec2(x, pp.y), ImVec2(x, yb), c, hot ? 3.0f : 2.0f);
      constexpr float hw = 5.0f;
      const float cx = std::clamp(x, pp.x + hw, pp.x + ps.x - hw), cy = pp.y + ps.y * 0.5f, hh = ps.y * 0.28f;
      dl.AddRectFilled(ImVec2(cx - hw, cy - hh), ImVec2(cx + hw, cy + hh), c, 3.0f);
      for (const float gx : {cx - 1.5f, cx + 1.5f})
        dl.AddLine(ImVec2(gx, cy - hh * 0.5f), ImVec2(gx, cy + hh * 0.5f), IM_COL32(20, 50, 70, 255), 1.0f);
    };
    handle(x0, act == 1 || act == 3);
    handle(x1, act == 2 || act == 3);
    if (act != 0) { // 起止日贴把手外侧 (放不下改内侧)
      char s0[16], s1[16];
      const misc::Ymd a = d.dates[rg.d0], b = d.dates[rg.d1 - 1];
      std::snprintf(s0, sizeof(s0), "%d/%02d/%02d", a / 10000, a / 100 % 100, a % 100);
      std::snprintf(s1, sizeof(s1), "%d/%02d/%02d", b / 10000, b / 100 % 100, b % 100);
      const ImVec2 t0 = ImGui::CalcTextSize(s0), t1 = ImGui::CalcTextSize(s1);
      float lx = x0 - 9.0f - t0.x, rx = x1 + 9.0f;
      if (lx < pp.x)
        lx = x0 + 9.0f;
      if (rx + t1.x > pp.x + ps.x)
        rx = x1 - 9.0f - t1.x;
      const auto label = [&](float tx, const char *s, const ImVec2 &ts) {
        dl.AddRectFilled(ImVec2(tx - 3.0f, pp.y + 2.0f), ImVec2(tx + ts.x + 3.0f, pp.y + 4.0f + ts.y), IM_COL32(10, 20, 30, 220), 3.0f);
        dl.AddText(ImVec2(tx, pp.y + 3.0f), IM_COL32(255, 255, 255, 255), s);
      };
      label(lx, s0, t0);
      label(rx, s1, t1);
    }
    ImPlot::PopPlotClipRect();
    if (act != 0)
      tip("区间 %d/%02d/%02d ~ %d/%02d/%02d (%d 日)\n"
          "• 拖左右把手改起止, 拖段内平移 (月数不变); 吸附到月初\n"
          "• 双击把手 = 拉到头",
          d.dates[rg.d0] / 10000, d.dates[rg.d0] / 100 % 100, d.dates[rg.d0] % 100, d.dates[rg.d1 - 1] / 10000, d.dates[rg.d1 - 1] / 100 % 100,
          d.dates[rg.d1 - 1] % 100, rg.d1 - rg.d0);
  }
  ImPlot::EndPlot();
}

// 右图例表头排序 (三张右表同一份; 三态: 升 → 降 → 原序): order (表行下标) 按当前排序列重排;
// 首列 (名) 按 name(a, b) 三路比较, 其余按 num(i, 列), 不可算的恒在末
template <class Name, class Num>
void sort_rows(std::vector<size_t> &order, const Name &name, const Num &num) {
  const ImGuiTableSortSpecs *ss = ImGui::TableGetSortSpecs();
  if (!ss || ss->SpecsCount == 0)
    return;
  const int c = ss->Specs[0].ColumnIndex;
  const bool desc = ss->Specs[0].SortDirection == ImGuiSortDirection_Descending;
  std::stable_sort(order.begin(), order.end(), [&](size_t a, size_t b) {
    if (c == 0) {
      const int r = name(a, b);
      return desc ? r > 0 : r < 0;
    }
    const double x = num(a, c), y = num(b, c);
    if (!std::isfinite(x) || !std::isfinite(y))
      return std::isfinite(x) && !std::isfinite(y);
    return desc ? x > y : x < y;
  });
}
// 名格悬停 (首列; 行 Selectable 跨全列, 只认本格) → 提示
void name_tip(const std::string &s) {
  if (ImGui::IsItemHovered() && ImGui::TableGetHoveredColumn() == 0)
    tip("%s", s.c_str());
}

// ---- 右表 (NAV / VAL / FLOW 同一骨架 table): 列 = 名 + 各段 (Seg) 的列 (SheetCol), 格 = 文本 + 排序键 + 底色名次 (SheetCell);
//      格缓存 (sheet_of: 来源变才整组重填) → 列出 (listing: 篮子组按过滤条, 选中 / 展开行恒列) → 量列出行各列最宽 → 表头排序 (组内) → 画 (sheet) ----
// 一列: 表头 / 悬停说明 (各列同一格式: "列名 = 全称" + 算法 / 数据 / 显示 / 适用 四行)
struct SheetCol {
  std::string name, tip;
};
// 值 y 的格: 字色 col, 按 fmt 显示 a; 不可算 → "-"; 排序键 = y
template <class... A>
SheetCell sheet_cell(double y, const ImVec4 &col, const char *fmt, A... a) {
  return {std::isfinite(y) ? num_cell(col, fmt, a...) : num_none(), y};
}
// 涨跌 / 流入: 百分比两位, 按正负着色
SheetCell signed_cell(double y) { return sheet_cell(y, tint(y, 0.0), "%+.2f%%", 100.0 * y); }
// 带名次的值 (NAV 数值列 / VAL 估值分位): 按 fmt 显示 q.x, 底色 = q.rank
SheetCell ranked_cell(const Ranked &q, const char *fmt) {
  SheetCell c = sheet_cell(q.x, ImGui::GetStyleColorVec4(ImGuiCol_Text), fmt, q.x);
  c.rank = q.rank;
  return c;
}
// 一张右表: 组 g 的行, 列 cols (cols[0] = 名); order = 列出的组内行 (按表头排序重排后画); ps = 该组的格 (sheet_of);
// on(r) = 选中 (高亮), 点行 → click(r); 悬停表行 r (组内行 / 成分行) 的数值列 k → hover(r, k).
// 宽由外面定: 名列伸满余宽, 内容更宽 → 横滚 (名列钉住). 篮子组的行名前 ▸ 展开 / 收起 (ui.pick.open, 至多一行): 展开行钉在表头下,
// 其下先列成分行 (缩进; 同列排序), 再列其余组内行; 点成分行 = 叠加 / 取消该股 (ui.pick.stocks); 只画可见的行
template <class On, class Click, class Hover>
void sheet(const char *id, const std::vector<SheetCol> &cols, PortfolioUIState &ui, const PortfolioDerived &d, int g, std::vector<size_t> order,
           const PortfolioSheet &ps, const On &on, const Click &click, const Hover &hover) {
  const std::vector<PortfolioRow> &rs = d.nav[g];
  const size_t nc = cols.size() - 1, R = rs.size(); // 名之后的列 / 组内行
  assert(ps.nc == nc && ps.rows() == R && ps.cells.size() == (R + ps.kids.size()) * nc);
  const bool tree = g < kBasketGroups;
  const auto pin_at = std::ranges::find_if(order, [&](size_t r) { return tree && rs[r].id == ui.pick.open; });
  const size_t pin = pin_at == order.end() ? SIZE_MAX : *pin_at; // 钉住的展开行
  if (pin != SIZE_MAX)
    order.erase(pin_at);
  // 横滚宽 = 上帧各列内容宽之和 (EndTable 按内容更新; 列不可拖 → 纯内容宽), 不足表宽 → 表宽 (名列伸满); 首帧 0 = 按内容
  const ImGuiTable *last = ImGui::TableFindByID(ImGui::GetID(id));
  const float inner = last ? std::max(last->ColumnsAutoFitWidth, ImGui::GetContentRegionAvail().x - ImGui::GetStyle().ScrollbarSize) : 0.f;
  if (!ImGui::BeginTable(id, static_cast<int>(cols.size()),
                         ImGuiTableFlags_ScrollX | ImGuiTableFlags_ScrollY | ImGuiTableFlags_SizingFixedFit | ImGuiTableFlags_BordersInnerV |
                             ImGuiTableFlags_Sortable | ImGuiTableFlags_SortTristate,
                         ImVec2(0, 0), inner))
    return;
  ImGui::TableSetupScrollFreeze(1, pin == SIZE_MAX ? 1 : 2);
  ImGui::TableSetupColumn(cols[0].name.c_str(), ImGuiTableColumnFlags_WidthStretch);
  std::vector<float> cw(nc); // 列出行的最宽 (表头起)
  for (size_t k = 0; k < nc; ++k) {
    ImGui::TableSetupColumn(cols[1 + k].name.c_str());
    cw[k] = ImGui::CalcTextSize(cols[1 + k].name.c_str()).x;
  }
  ImGui::TableNextRow(ImGuiTableRowFlags_Headers); // 同 TableHeadersRow, 另加悬停说明
  for (int c = 0; c < static_cast<int>(cols.size()); ++c) {
    if (!ImGui::TableSetColumnIndex(c))
      continue;
    ImGui::PushID(c);
    ImGui::TableHeader(cols[static_cast<size_t>(c)].name.c_str());
    ImGui::PopID();
    if (ImGui::IsItemHovered())
      tip("%s", cols[static_cast<size_t>(c)].tip.c_str());
  }
  const auto at = [&](size_t r) { return ps.cells.data() + r * nc; };
  const auto num = [&](size_t r, int c) { return at(r)[c - 1].key; };
  sort_rows(
      order, [&](size_t a, size_t b) { // 名列: 按色相 (相似的挨着; 未定色 = 按 cn 字典序)
        const float ha = rs[a].hue, hb = rs[b].hue;
        if (!std::isnan(ha) && !std::isnan(hb))
          return (ha > hb) - (ha < hb);
        return rs[a].cn.compare(rs[b].cn);
      },
      num);
  std::vector<size_t> lines; // 钉行之下的画序: 钉行的成分行 R + j, 再其余组内行 r < R
  if (pin != SIZE_MAX) {
    for (uint32_t j = ps.kid_at[pin]; j < ps.kid_at[pin + 1]; ++j)
      lines.push_back(R + j);
    sort_rows(lines, [&](size_t a, size_t b) { return ps.kids[a - R].name.compare(ps.kids[b - R].name); }, num);
    for (size_t k = 0; k < nc; ++k)
      cw[k] = std::max(cw[k], at(pin)[k].text.w);
  }
  lines.insert(lines.end(), order.begin(), order.end());
  for (const size_t r : lines)
    for (size_t k = 0; k < nc; ++k)
      cw[k] = std::max(cw[k], at(r)[k].text.w);
  constexpr ImGuiTreeNodeFlags kNode = ImGuiTreeNodeFlags_SpanAllColumns | ImGuiTreeNodeFlags_NoTreePushOnOpen;
  const auto row = [&](size_t r) {
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    int hov = -1; // 行跨全列: 悬停的数值列
    if (r >= R) {
      const PortfolioKid &kd = ps.kids[r - R];
      const bool sel = ui.pick.stocks.contains(kd.id);
      ImGui::Indent();
      if (sel)
        ImGui::PushStyleColor(ImGuiCol_Text, stock_color(kd.id));
      ImGui::TreeNodeEx((kd.name + "##k" + std::to_string(r)).c_str(), kNode | ImGuiTreeNodeFlags_Leaf | (sel ? ImGuiTreeNodeFlags_Selected : 0));
      if (sel)
        ImGui::PopStyleColor();
      ImGui::Unindent();
      if (ImGui::IsItemClicked())
        toggle_stock(ui, kd.id);
      hov = ImGui::IsItemHovered() ? ImGui::TableGetHoveredColumn() - 1 : -1;
      name_tip(kd.hover);
    } else {
      const PortfolioRow &h = rs[r];
      const std::string label = h.cn + "##" + h.id;
      ImGui::PushStyleColor(ImGuiCol_Text, nav_color(d, g, r, kTrade));
      if (tree) {
        const bool open = r == pin;
        ImGui::SetNextItemOpen(open, ImGuiCond_Always);
        ImGui::TreeNodeEx(label.c_str(), kNode | ImGuiTreeNodeFlags_OpenOnArrow | (on(r) ? ImGuiTreeNodeFlags_Selected : 0));
        if (ImGui::IsItemToggledOpen()) {
          ui.pick.open = open ? std::string{} : h.id;
          ImGui::SetScrollY(0.f);
        } else if (ImGui::IsItemClicked()) {
          click(r);
        }
      } else if (ImGui::Selectable(label.c_str(), on(r), ImGuiSelectableFlags_SpanAllColumns)) {
        click(r);
      }
      ImGui::PopStyleColor();
      hov = ImGui::IsItemHovered() ? ImGui::TableGetHoveredColumn() - 1 : -1;
      name_tip(h.hover);
    }
    for (size_t k = 0; k < nc; ++k) {
      ImGui::TableSetColumnIndex(static_cast<int>(1 + k));
      const SheetCell &c = at(r)[k];
      if (std::isfinite(c.rank))
        ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::ColorConvertFloat4ToU32(band_col(c.rank, kValCellA)));
      draw_cell(c.text, cw[k]);
    }
    if (hov >= 0)
      hover(r, hov);
  };
  if (pin != SIZE_MAX) // 冻结行须在 clipper 之前交
    row(pin);
  ImGuiListClipper clip;
  clip.Begin(static_cast<int>(lines.size()));
  while (clip.Step())
    for (int i = clip.DisplayStart; i < clip.DisplayEnd; ++i)
      row(lines[static_cast<size_t>(i)]);
  ImGui::EndTable();
}

// ---- 当日四列 (TodayCol; 三张右表同一份, 不随区间): 只数 / 总市值 / 换手不着色, 流入按正负着色; 换手 / 流入格悬停看分子分母金额 ----
const SheetCol kTodayCols[kTodays] = {
    {"只数", "只数 = 成分只数\n"
             "• 算法: 成员表的成分计数 (.等 / .加 同一份)\n"
             "• 数据: 成员表 = 历史末日收盘定 (= 当日 bind 给新浪行情的那份)\n"
             "• 显示: 整数, 不着色\n"
             "• 适用: 指数 / 行业 / 概念; 成分行 -; 不随区间"},
    {"总市值", "总市值 = 成分总市值之和\n"
               "• 算法: Σ 成分 末日收盘 × 总股本; 当日接上再 × 加权线当日涨跌 (今收 / 前收); 成分行 = 本只 (基本面末日总市值, 当日接上再 × 今收 / 前收)\n"
               "• 数据: 成员表同只数; 总股本取末日适用\n"
               "• 显示: 亿 / 万亿, 不着色\n"
               "• 适用: 指数 / 行业 / 概念 + 成分行; 不随区间"},
    {"换手", "换手 = 当日换手率\n"
             "• 算法: 成交额 / 总市值; 成交额 = Σ 成分 当日成交额 (成分行 = 本只), 总市值同上列 (总股本口径, 非流通)\n"
             "• 数据: 新浪行情日 (盘后 / 重启照样全天); 无新浪行情日 / 当日无成交 → -\n"
             "• 显示: 百分比两位, 不着色; 悬停格看成交额 / 总市值\n"
             "• 适用: 指数 / 行业 / 概念 + 成分行; 不随区间"},
    {"流入", "流入 = 当日资金流入占成交额\n"
             "• 算法: 净流入 / 成交额; 净流入 = Σ 成分 Σ 分钟 ± 该分钟额 (分钟涨跌法; 成分行 = 本只): 收 > 上一分钟收 (首分钟对昨收) 记 +, < 记 −, 平随上一分钟\n"
             "• 数据: 新浪行情日分钟根 (快照合成 + 分钟 K 补), 含开收盘集合竞价; 成分之和不加权, 分不出大单\n"
             "• 显示: 百分比两位, 按正负着色; 悬停格看净流入 / 成交额\n"
             "• 适用: 指数 / 行业 / 概念 + 成分行; 不随区间"},
};
// 金额 y [亿元] 的显示: ≥ 1 万亿按万亿, 其余按亿 (越大小数越少); 带符号 sign
std::string yi_text(double y, bool sign = false) {
  const double a = std::abs(y);
  char s[32];
  const char *pm = !sign ? "" : y < 0 ? "-"
                                      : "+";
  if (a >= 1e4)
    std::snprintf(s, sizeof(s), a >= 1e6 ? "%s%.0f万亿" : "%s%.1f万亿", pm, a / 1e4);
  else
    std::snprintf(s, sizeof(s), a >= 10 ? "%s%.0f亿" : a >= 1 ? "%s%.1f亿"
                                                              : "%s%.2f亿",
                  pm, a);
  return s;
}
// 篮子行的当日 (SheetToday): 只数 / 总市值 = Tape::members / weight 的同切片等权线 / 加权线 (总市值当日接上 × 加权线当日涨跌);
// 成交额 / 流入 = 新浪当日合成的 Day (不看接续); 换手 = 成交额 / 总市值
SheetToday basket_today(const Database::Feed &f, const PortfolioRow &h) {
  SheetToday o;
  if (h.basket < 0 || !f.data || f.data->members.empty())
    return o;
  const size_t c = cut_of_line(static_cast<size_t>(h.basket)), eq = basket_line(c, kWeightEq), cap = basket_line(c, kWeightCap);
  assert(f.data->keys[eq] == eq && f.data->keys[cap] == cap && f.today && f.today->size() == f.data->list.size());
  const std::vector<Database::Day> &today = *f.today;
  double w = f.data->weight[cap];
  if (const Database::Day &t = today[cap]; Database::joins(*f.data->list[cap], t))
    w *= t.candle.close / t.candle.ref;
  o.v[kTodayMembers] = f.data->members[eq];
  o.v[kTodayCap] = w > 0 ? w : kNaN;
  if (const Database::Day &t = today[static_cast<size_t>(h.basket)]; t.candle.date != 0 && t.candle.amount > 0)
    o.amount = t.candle.amount / 1e8, o.v[kTodayFlow] = t.flow / t.candle.amount;
  o.v[kTodayTurn] = o.amount / o.v[kTodayCap];
  return o;
}
// 成分行的源: 基本面档案 (无 → nullptr) + 新浪当日 (未取 / 当日无成交 → nullptr)
struct KidSrc {
  const StockInfo *info = nullptr;
  const Database::Day *day = nullptr;
};
KidSrc kid_src(const PortfolioConsToday &ct, const StockInfos &infos, uint32_t id) {
  KidSrc x;
  if (id < infos.size() && infos[id].known)
    x.info = &infos[id];
  const auto it = std::ranges::lower_bound(ct.ids, id);
  if (ct.day && it != ct.ids.end() && *it == id)
    if (const Database::Day &t = (*ct.day)[static_cast<size_t>(it - ct.ids.begin())]; t.candle.date != 0)
      x.day = &t;
  return x;
}
// 成分行的当日 (SheetToday): 总市值 = 档案末日收盘总市值 (新浪当日接上档案末日再 × 当日涨跌); 成交额 / 流入 = 新浪当日 (本只分钟涨跌法);
// 换手 = 成交额 / 总市值; 无只数
SheetToday stock_today(const KidSrc &x) {
  SheetToday o;
  double cap = x.info ? x.info->mcap : kNaN;
  if (x.info && x.day && x.day->prev == x.info->update_date && x.day->candle.ref > 0)
    cap *= x.day->candle.close / x.day->candle.ref;
  o.v[kTodayCap] = cap > 0 ? cap : kNaN;
  if (x.day && x.day->candle.amount > 0)
    o.amount = x.day->candle.amount / 1e8, o.v[kTodayFlow] = x.day->flow / x.day->candle.amount;
  o.v[kTodayTurn] = o.amount / o.v[kTodayCap];
  return o;
}
// 成分行的近况 (Database::RecentOf: Assets 个股同一份); 无档案 → 不可算
Database::Recent kid_recent(const KidSrc &x) {
  if (!x.info)
    return {};
  return x.day ? Database::RecentOf(*x.info, x.day->candle, x.day->prev) : Database::RecentOf(*x.info, Database::Candle{}, 0);
}
// 成分行第 b 口的收益率 (基本面最新快照 = Assets 估值列的倒数; DY = DY1): 不可算 → 非有限
double stock_yield(const StockInfo &s, int b) {
  constexpr float StockInfo::*kMult[kPoolVals] = {&StockInfo::pe_ttm, &StockInfo::pb_mrq, &StockInfo::ps_ttm, &StockInfo::pcf_ttm, nullptr};
  return b == kValDy ? s.dy1y / 100.0 : 1.0 / (s.*kMult[b]);
}
// 成分行: 名 + 悬停 (代码 / 权占比 = 本只权 / 篮子权和 / 行业 / 估值)
PortfolioKid kid_of(const StockInfos &infos, const Database::Constituent &c, double sum_w) {
  PortfolioKid k{c.id, stock_name(infos, c.id), {}};
  const auto num = [](const char *label, double v, const char *fmt) {
    char s[48];
    if (std::isfinite(v))
      std::snprintf(s, sizeof(s), fmt, label, v);
    else
      std::snprintf(s, sizeof(s), "%s -", label);
    return std::string(s);
  };
  k.hover = k.name + " " + asset_axis().code(c.id) + "\n• " + num("权占比", 100.0 * c.w / sum_w, "%s %.2f%%");
  if (c.id < infos.size() && infos[c.id].known) {
    const StockInfo &s = infos[c.id];
    k.hover += "\n• 行业 " + (s.ind_name.empty() ? std::string("-") : s.ind_name) + (s.st ? " · ST" : "");
    k.hover += "\n• " + num("总市值", s.mcap, "%s %.0f亿") + "  " + num("PE", s.pe_ttm, "%s %.1f") + "  " + num("PB", s.pb_mrq, "%s %.2f") + "  " +
               num("PS", s.ps_ttm, "%s %.2f") + "  " + num("PCF", s.pcf_ttm, "%s %.1f") + "  " + num("DY1", s.dy1y, "%s %.2f%%");
  }
  k.hover += "\n• 点 = 叠加 / 取消到日内栏 / FLOW 图";
  return k;
}
// 四列 → out[0, kTodays)
void today_cells(const SheetToday &t, SheetCell *out) {
  const ImVec4 &txt = ImGui::GetStyleColorVec4(ImGuiCol_Text);
  const double *v = t.v;
  out[kTodayMembers] = sheet_cell(v[kTodayMembers], txt, "%.0f", v[kTodayMembers]);
  out[kTodayCap] = sheet_cell(v[kTodayCap], txt, "%s", yi_text(v[kTodayCap]).c_str());
  out[kTodayTurn] = sheet_cell(v[kTodayTurn], txt, "%.2f%%", 100.0 * v[kTodayTurn]);
  out[kTodayFlow] = signed_cell(v[kTodayFlow]);
}
// 悬停四列第 k 列 (k 不在 [0, kTodays) = 别的列) → 换手 / 流入的分子分母
void today_tip(const SheetToday &t, int k) {
  if (k == kTodayTurn && std::isfinite(t.v[kTodayTurn]))
    tip("换手 %.2f%% = 成交额 %s / 总市值 %s", 100.0 * t.v[kTodayTurn], yi_text(t.amount).c_str(), yi_text(t.v[kTodayCap]).c_str());
  else if (k == kTodayFlow && std::isfinite(t.v[kTodayFlow]))
    tip("流入 %+.2f%% = 净流入 %s / 成交额 %s", 100.0 * t.v[kTodayFlow], yi_text(t.v[kTodayFlow] * t.amount, true).c_str(), yi_text(t.amount).c_str());
}
// 篮子行的当日过 f (见 BasketFilter)
bool passes(const BasketFilter &f, const SheetToday &t) {
  const auto in = [](double x, double lo, double hi) { return (lo <= 0 && hi <= 0) || (std::isfinite(x) && x >= lo && (hi <= 0 || x <= hi)); };
  return in(t.v[kTodayMembers], f.n_lo, f.n_hi) && in(t.v[kTodayCap], f.cap_lo, f.cap_hi) &&
         (f.flow <= 0 || (std::isfinite(t.v[kTodayFlow]) && 100.0 * std::abs(t.v[kTodayFlow]) >= f.flow));
}
// 过滤条一项的界 [lo, hi] (0 = 该端不限)
template <class T>
struct Bound {
  T lo, hi;
};
// 界的摘要: 不限 / ≥lo / ≤hi / lo~hi (值按 fmt)
template <class T, class Fmt>
std::string bound_text(T lo, T hi, const Fmt &fmt) {
  if (lo > 0 && hi > 0)
    return fmt(lo) + "~" + fmt(hi);
  if (lo > 0)
    return "≥" + fmt(lo);
  if (hi > 0)
    return "≤" + fmt(hi);
  return "不限";
}
// 过滤条一项: 按钮 = 名 + 摘要 (设了 → 按下色), 点 = 弹出 预设 (点即设) + 精确输入 (in_fmt; 0 = 不限); hi = nullptr → 只有下限
template <class T, class Fmt>
void bound_chip(const char *id, const char *name, T &lo, T *hi, std::initializer_list<Bound<T>> presets, const char *in_fmt, const Fmt &fmt) {
  const T h = hi ? *hi : T{};
  const bool on = lo > 0 || h > 0;
  if (on)
    ImGui::PushStyleColor(ImGuiCol_Button, ImGui::GetStyleColorVec4(ImGuiCol_ButtonActive));
  if (ImGui::SmallButton((std::string(name) + " " + bound_text(lo, h, fmt) + "###" + id).c_str()))
    ImGui::OpenPopup(id);
  if (on)
    ImGui::PopStyleColor();
  if (!ImGui::BeginPopup(id))
    return;
  for (const Bound<T> &p : presets)
    if (ImGui::Selectable(bound_text(p.lo, p.hi, fmt).c_str(), p.lo == lo && p.hi == h)) {
      lo = p.lo;
      if (hi)
        *hi = p.hi;
    }
  ImGui::Separator();
  constexpr ImGuiDataType kType = std::is_integral_v<T> ? ImGuiDataType_S32 : ImGuiDataType_Float;
  const float w = ImGui::CalcTextSize("0000000").x + 2.0f * ImGui::GetStyle().FramePadding.x;
  ImGui::TextDisabled("≥");
  ImGui::SameLine(0, 2);
  ImGui::SetNextItemWidth(w);
  ImGui::InputScalar("##lo", kType, &lo, nullptr, nullptr, in_fmt);
  lo = std::max(lo, T{});
  if (hi) {
    ImGui::SameLine();
    ImGui::TextDisabled("≤");
    ImGui::SameLine(0, 2);
    ImGui::SetNextItemWidth(w);
    ImGui::InputScalar("##hi", kType, hi, nullptr, nullptr, in_fmt);
    *hi = std::max(*hi, T{});
  }
  ImGui::SameLine();
  ImGui::TextDisabled("(0 = 不限)");
  ImGui::EndPopup();
}
// 篮子组右表的过滤条 (三页同一份): [只数 …] [市值 …] [|流入| …] 列出 / 总数; 改动下帧生效
void filter_bar(BasketFilter &f, size_t listed, size_t total) {
  const auto pct = [](float v) {
    char s[16];
    std::snprintf(s, sizeof(s), "%g%%", v);
    return std::string(s);
  };
  bound_chip<int>("pf_f_n", "只数", f.n_lo, &f.n_hi, {{0, 0}, {0, 50}, {0, 200}, {0, 500}, {50, 0}, {200, 0}}, "%d",
                  [](int v) { return std::to_string(v); });
  ImGui::SameLine(0, 2);
  bound_chip<float>("pf_f_cap", "市值", f.cap_lo, &f.cap_hi, {{0, 0}, {500, 0}, {2000, 0}, {10000, 0}, {0, 2000}, {0, 10000}}, "%.0f",
                    [](float v) { return yi_text(v); });
  ImGui::SameLine(0, 2);
  bound_chip<float>("pf_f_flow", "|流入|", f.flow, nullptr, {{0, 0}, {1, 0}, {2, 0}, {5, 0}, {10, 0}}, "%.1f", pct);
  ImGui::SameLine();
  ImGui::TextDisabled("%zu/%zu", listed, total);
  if (ImGui::IsItemHovered())
    tip("过滤本组的行 (指数 / 行业 / 概念各一份, NAV / VAL / FLOW 三页共用): 只数 / 总市值 [亿元] 在区间内, |资金流入| [%%] ≥ 下限\n"
        "• 点一项 = 弹出: 预设 (点即设) / 精确输入 (0 = 该端不限); 设了的项按下色\n"
        "• 设了的条件缺值 (行情未到 / 无新浪行情日) 即不列; 已选中 / 展开的行恒列\n"
        "• 本处 = 列出 / 总数");
}
// 右表的一段列 (三页由段拼成, 同一段三页同一份): 列 + 组内行 / 成分行的格 + 悬停格
struct Seg {
  std::vector<SheetCol> cols;
  std::function<void(size_t, PortfolioSheet &, SheetCell *)> row;                 // 组内行 r → 段内格 out[0, cols)
  std::function<void(const KidSrc &, size_t, PortfolioSheet &, SheetCell *)> kid; // 成分行 (x = 该股的档案 / 新浪当日, 表行下标) → 段内格
  std::function<void(const PortfolioSheet &, size_t, int)> tip;                   // 悬停表行 (组内行 / 成分行) 的段内列 k; 空 = 无
};
size_t cols_in(const std::vector<Seg> &segs) {
  size_t n = 0;
  for (const Seg &s : segs)
    n += s.cols.size();
  return n;
}
// 名列 + 各段的列
std::vector<SheetCol> cols_of(const SheetCol &name, const std::vector<Seg> &segs) {
  std::vector<SheetCol> o{name};
  for (const Seg &s : segs)
    o.insert(o.end(), s.cols.begin(), s.cols.end());
  return o;
}
// 页 page 组 g 的格 (ui.sheet[page]): 键 (days / ends 只 FLOW 给) 变 → 该组全部行重填: 当日 + 各段依次填组内行的格;
// 展开的篮子行的成分 (Tape::cons) 接在组内行之后为成分行, 各段依次填成分行的格
const PortfolioSheet &sheet_of(PortfolioUIState &ui, const StockInfos &infos, int page, int g, const std::vector<PortfolioRow> &rs,
                               const std::vector<Seg> &segs, std::vector<misc::Ymd> days, bool ends) {
  const Database::Feed &f = ui.basket.feed;
  const PortfolioSheetKey k{g, ui.view.gen, f.data, f.today, ui.pick.open, ui.cons.day, infos.size(), std::move(days), ends, ImGui::GetFontSize(),
                            ImGui::GetColorU32(ImGuiCol_Text)};
  ui.sheet[page].update(k, [&](PortfolioSheet &ps) {
    const size_t R = rs.size(), nc = cols_in(segs);
    ps.nc = nc;
    ps.today.clear();
    ps.kids.clear();
    ps.kid_at.assign(1, 0);
    for (const PortfolioRow &h : rs) {
      ps.today.push_back(basket_today(f, h));
      if (h.basket >= 0 && h.id == ui.pick.open && f.data && !f.data->lack)
        for (const Database::Constituent &c : f.data->cons[static_cast<size_t>(h.basket)])
          ps.kids.push_back(kid_of(infos, c, f.data->weight[static_cast<size_t>(h.basket)]));
      ps.kid_at.push_back(static_cast<uint32_t>(ps.kids.size()));
    }
    std::vector<KidSrc> xs;
    for (const PortfolioKid &c : ps.kids) {
      xs.push_back(kid_src(ui.cons, infos, c.id));
      ps.today.push_back(stock_today(xs.back()));
    }
    const size_t n = R + ps.kids.size();
    ps.flow.assign(n, {});
    ps.cells.assign(n * nc, {});
    for (size_t r = 0; r < R; ++r) {
      SheetCell *out = ps.cells.data() + r * nc;
      for (const Seg &s : segs)
        s.row(r, ps, out), out += s.cols.size();
    }
    for (size_t j = 0; j < xs.size(); ++j) {
      SheetCell *out = ps.cells.data() + (R + j) * nc;
      for (const Seg &s : segs)
        s.kid(xs[j], R + j, ps, out), out += s.cols.size();
    }
  });
  return ui.sheet[page].val;
}
// 展开行的成分在新浪当日的根 (ui.cons): 成分集 / 行情日 · 快照时刻变了才重取
void sync_cons(PortfolioUIState &ui, const PortfolioDerived &d, const Database::QuoteService &q) {
  const Database::Feed &f = ui.basket.feed;
  std::vector<uint32_t> ids;
  if (const PortfolioAt o = d.at(ui.pick.open); o.g >= 0 && f.data && !f.data->lack) {
    const PortfolioRow &h = d.nav[o.g][o.r];
    assert(o.g < kBasketGroups && h.basket >= 0);
    for (const Database::Constituent &c : f.data->cons[static_cast<size_t>(h.basket)])
      ids.push_back(c.id);
  }
  std::ranges::sort(ids);
  const Database::QuoteStatus st = q.status();
  const int64_t at = static_cast<int64_t>(st.date) * 86400 + st.sec;
  if (ids == ui.cons.ids && at == ui.cons.at && (ui.cons.day || ids.empty()))
    return;
  ui.cons.day = ids.empty() ? nullptr : q.today(Database::kStock, ids);
  ui.cons.ids = std::move(ids);
  ui.cons.at = at;
}
// 组 g 右表列出的行: 篮子组 = 过过滤条的 ∪ 选中的 (on) ∪ 展开的 (表上画过滤条), 其余组全列
template <class On>
std::vector<size_t> listing(PortfolioUIState &ui, const PortfolioSheet &ps, const std::vector<PortfolioRow> &rs, int g, const On &on) {
  std::vector<size_t> o;
  const size_t n = ps.rows();
  for (size_t r = 0; r < n; ++r)
    if (g >= kBasketGroups || on(r) || rs[r].id == ui.pick.open || passes(ui.filter[g], ps.today[r]))
      o.push_back(r);
  if (g < kBasketGroups)
    filter_bar(ui.filter[g], o.size(), n);
  return o;
}
// 页 page 的右表 (三页同一条; 组 = 类别条选中的): 格 (sheet_of) → 列出 (listing) → 画 (sheet: 点行 = 按页的选中方式选 / 取消,
// 悬停数值格 → 所在段的 tip)
void table(PortfolioUIState &ui, const StockInfos &infos, const PortfolioDerived &d, int page, const SheetCol &name, const std::vector<Seg> &segs,
           std::vector<misc::Ymd> days = {}, bool ends = false) {
  const int g = ui.cat[page];
  const std::vector<PortfolioRow> &rs = d.nav[g];
  const auto on = [&](size_t r) { return picked(ui, page, rs[r]); };
  const PortfolioSheet &ps = sheet_of(ui, infos, page, g, rs, segs, std::move(days), ends);
  const std::vector<size_t> order = listing(ui, ps, rs, g, on);
  ImGui::PushID(static_cast<int>(ps.nc)); // 列数可变 (FLOW 随档位)
  sheet(
      kPageDefs[page].table, cols_of(name, segs), ui, d, g, order, ps, on, [&](size_t r) { pick(ui, page, rs[r]); },
      [&](size_t r, int k) {
        for (const Seg &s : segs) {
          const int n = static_cast<int>(s.cols.size());
          if (k < n) {
            if (s.tip)
              s.tip(ps, r, k);
            return;
          }
          k -= n;
        }
      });
  ImGui::PopID();
}
// 段: 当日四列 (三页同一份)
Seg today_seg() {
  return {{std::begin(kTodayCols), std::end(kTodayCols)},
          [](size_t r, PortfolioSheet &p, SheetCell *out) { today_cells(p.today[r], out); },
          [](const KidSrc &, size_t row, PortfolioSheet &p, SheetCell *out) { today_cells(p.today[row], out); },
          [](const PortfolioSheet &p, size_t row, int k) { today_tip(p.today[row], k); }};
}

// ---- NAV 右图例: 类别条 (指数 / 行业 / 概念 / 因子 / 策略, 一次看一组) + 日内开关; 表 = 该组一行一对线, 点行 = 选中 / 取消 (选中 = 主图画该行全部线, 单条在主图例里再点掉;
//      选中跨组保留; 选中的篮子行另叠进日内栏). 列 = 中文名 (线色; 悬停看 hover) | 近况 (当日涨幅 | Δ5日线) | 当日四列
//      | 可成交 | 理想 (区间年化) | 夏普 (可成交列) | 年换手; 近况按正负着色, 末段底色 = 同组同列截面名次 (好绿 → 差红); 表头点排序 (组内) ----
// 近况段 (不随区间; Database::Recent)
enum RecentCol : int { kRecentChg,
                       kRecentMa5 };
constexpr int kRecents = 2;
const SheetCol kRecentCols[kRecents] = {
    {"涨幅", "涨幅 = 当日涨跌幅\n"
             "• 算法: 最新一日收盘 / 前收 − 1\n"
             "• 数据: 篮子 = Assets 篮子 (当日接上 = 新浪快照合成, 否则 all.bar 末日); 成分行 = Assets 个股同一份 (新浪当日接上 bar1d); 因子 / 策略 = 净值末点\n"
             "• 显示: 百分比两位, 按正负着色\n"
             "• 适用: 全部组 + 成分行; 不随区间"},
    {"Δ5日线", "Δ5日线 = 对 5 日均线的偏离\n"
               "• 算法: 最新一日收盘 / MA5 (含当日) − 1\n"
               "• 数据: 篮子 = Assets 篮子 (当日接上 = 新浪快照合成, 否则 all.bar 末日); 成分行 = Assets 个股同一份; 因子 / 策略 = 净值末 5 点\n"
               "• 显示: 百分比两位, 按正负着色\n"
               "• 适用: 全部组 + 成分行; 不随区间"},
};
// 右段 [NavNum]
constexpr const char *kNavNumFmt[kNavNums] = {"%+.1f%%", "%+.1f%%", "%+.2f", "%.1f倍"};
const SheetCol kNavNumCols[kNavNums] = {
    {"可成交", "可成交 = 可成交线区间年化\n"
               "• 算法: 区间起点归一, 按区间复利年化\n"
               "• 数据: 篮子 = 标签指数 (收盘封板不建仓 / 平仓顺延); 因子 / 策略 = 有冲击净值\n"
               "• 显示: 百分比一位, 底色 = 组内名次 (高绿 → 低红)\n"
               "• 适用: 全部组; 成分行 -; 随区间"},
    {"理想", "理想 = 理想线区间年化\n"
             "• 算法: 区间起点归一, 按区间复利年化\n"
             "• 数据: 篮子 = 价格指数 (成员 / 权重取前一日收盘, 不看可成交); 因子 / 策略 = 无冲击净值\n"
             "• 显示: 百分比一位, 底色 = 组内名次 (高绿 → 低红)\n"
             "• 适用: 全部组; 成分行 -; 随区间"},
    {"夏普", "夏普 = 可成交线区间夏普\n"
             "• 算法: 区间内日收益 mean / sd × √每年交易日\n"
             "• 数据: 篮子 = 标签指数; 因子 / 策略 = 有冲击净值\n"
             "• 显示: 两位小数, 底色 = 组内名次 (高绿 → 低红)\n"
             "• 适用: 全部组; 成分行 -; 随区间"},
    {"年换手", "年换手 = 单边年换手\n"
               "• 算法: 自首次建仓起的日均增仓量 × 每年交易日 (倍/年)\n"
               "• 数据: 篮子 = 成分增仓 Σ (w − 漂移后上一成分日 w)⁺ (再平衡 + 成分进出); 因子 = G20 多头增仓量; 策略 = 各因子按当日份额加权 (不计因子间对冲)\n"
               "• 显示: 一位小数 + 倍, 底色 = 组内名次 (低绿 → 高红)\n"
               "• 适用: 全部组; 成分行 -; 不随区间"},
};
const SheetCol kNavName = {"名", "名 = 行名\n"
                                 "• 算法: 篮子 = 切片名 + .等 (等权) / .加 (总市值加权); 因子 / 策略 = 中文名\n"
                                 "• 数据: 切片 = 全A / 池 / 申万一级行业 / 东财题材概念\n"
                                 "• 显示: 字色 = 主图线色 (四组一起按 max(成员重合, 剔全A.加的残差相关) 定色相: 色差 ∝ 不相似; 没算完全灰); 点本列表头 = 按颜色排 (相似的挨着; 没算完按名); 超长截断补 …, 悬停看全名\n"
                                 "• 成分行: 篮子名前 ▸ 展开 (至多一行), 该行钉在表头下, 其下缩进列出成分 (简称; 默认权降序, 点表头在本篮子内排, 名列按简称);\n"
                                 "  点 = 叠加 / 取消到日内栏 (字色 = 叠加线色)\n"
                                 "• 适用: 全部组; 成分行只指数 / 行业 / 概念"};
void recent_cells(const Database::Recent &x, SheetCell *out) { out[kRecentChg] = signed_cell(x.chg), out[kRecentMa5] = signed_cell(x.ma5); }
// 段: 近况 (篮子行 = 篮子 Feed; 因子 / 策略 = 可成交线日末; 成分行 = Assets 个股)
Seg recent_seg(const Database::Feed &f, const std::vector<PortfolioRow> &rs) {
  return {{std::begin(kRecentCols), std::end(kRecentCols)},
          [&f, &rs](size_t r, PortfolioSheet &, SheetCell *out) {
            const PortfolioRow &h = rs[r];
            recent_cells(h.basket >= 0 ? Database::RecentOf(f, static_cast<size_t>(h.basket)) : recent_of(h.line[kTrade]), out);
          },
          [](const KidSrc &x, size_t, PortfolioSheet &, SheetCell *out) { recent_cells(kid_recent(x), out); },
          {}};
}
// 段: NAV 数值列 (组 g 的 PortfolioView::num; 成分行 -)
Seg num_seg(const PortfolioView &v, int g) {
  return {{std::begin(kNavNumCols), std::end(kNavNumCols)},
          [&v, g](size_t r, PortfolioSheet &, SheetCell *out) {
            for (int k = 0; k < kNavNums; ++k)
              out[k] = ranked_cell(v.num[g][r][k], kNavNumFmt[k]);
          },
          [](const KidSrc &, size_t, PortfolioSheet &, SheetCell *out) {
            for (int k = 0; k < kNavNums; ++k)
              out[k] = {num_none()};
          },
          {}};
}
void legend_nav(PortfolioUIState &ui, const PortfolioDerived &d, const PortfolioView &v, const StockInfos &infos) {
  button_bar(kNavGroupName, kPageDefs[kPageNav].groups, ui.cat[kPageNav]);
  ImGui::SameLine();
  ImGui::TextDisabled("|");
  ImGui::SameLine();
  if (ui.intraday)
    ImGui::PushStyleColor(ImGuiCol_Button, ImGui::GetStyleColorVec4(ImGuiCol_ButtonActive));
  const bool flip = ImGui::SmallButton("日内");
  if (ui.intraday)
    ImGui::PopStyleColor();
  if (flip)
    ui.intraday = !ui.intraday;
  if (ImGui::IsItemHovered())
    tip("日内栏开关: 主图上方 (日内 : 主图 = 1 : 2, 上下分) 叠加选中的指数 / 行业 / 概念行的日内收盘线 (同主图线色); 因子 / 策略无日内\n"
        "• y = 对首日前收的涨跌幅, 横网格的涨跌幅小字标在右内壁; 悬停看十字光标处时刻 / 涨跌幅与该分钟各条读数\n"
        "• 只选一行: 改画分钟蜡烛 (窄了自动并根) + 现价十字 (虚线, 标涨跌幅 / 时刻)\n"
        "• 篮子同 Assets: all.bar 库合成历史, 当日由新浪快照合成; 档位 日内 / 2日 … 5日 叠在图左上角");
  ImGui::SameLine();
  ImGui::TextColored(kHead, "(点行 = 画)");
  if (ImGui::IsItemHovered())
    tip("点行 = 选中 / 取消, 选中 = 主图画该行两条线, 主图例默认只显理想列, 可成交列点图例显 (显隐变了自动重新 fit); 默认不选; 选中换组保留\n"
        "• 指数 = 各池等权指数 (all 池拆 全A.等 / 全A.加); 行业 = 申万一级 (全A 内切, 总市值加权); 概念 = 东财题材概念 (全A 内切, 总市值加权):\n"
        "    ◦ 可成交 = 标签指数 (收盘封板不建仓 / 平仓顺延, 与因子回测同口径)\n"
        "    ◦ 理想 = 价格指数 (Assets 篮子同一份: 成员 / 权重取前一日收盘, 点位按前收合成, 不看可成交, 同中证 / 万得口径); 两者之差 = 可成交拖累\n"
        "• 因子 = 单因子各自的 G20 多头净值: 可成交列 = 有冲击 (它自己的冲击段: Lob 最小金额段 / Bar 固定冲击), 理想列 = 无冲击\n"
        "• 策略 = 本组合 G20 多头: 可成交列 = 有冲击, 理想列 = 无冲击\n"
        "• 各列怎么算: 悬停表头 (算法 / 数据 / 显示 / 适用)\n"
        "• 指数 / 行业 / 概念表上过滤条 (与 VAL / FLOW 页同一份): 按只数 / 总市值 / |资金流入| 筛本组的行, 已画 / 展开的行恒列\n"
        "• 指数 / 行业 / 概念行名前 ▸ = 展开成分股 (至多一行, 三页同一份): 该行钉在表头下, 成分接在其下 (列同篮子行, 不能算的 -);\n"
        "  点成分行 = 叠加到日内栏 / FLOW 图 (至多 64 只)\n"
        "• 列宽按内容最紧, 超出栏宽横滚 (名列不动); 名超长截断补 …, 悬停中文名看全名; 点表头排序 (组内): 升序 → 降序 → 原序; 名列按颜色排 (相似的挨着)");
  const int g = ui.cat[kPageNav];
  table(ui, infos, d, kPageNav, kNavName, {recent_seg(ui.basket.feed, d.nav[g]), today_seg(), num_seg(v, g)});
}

// ---- VAL 右图例: 滚动窗长 + 类别条 (指数 / 行业 / 概念, 一次看一组) + 篮子单选表格; 列 = 中文名 (悬停看名) | 当日四列 | 各口;
//      各口格 = 区间末各口的估值分位, 底色 = 同组同口内的截面名次 (便宜绿 → 贵红); 表头点排序 (组内) ----
// 各口 [PoolVal]: 同一模板, 只换分子 / 负值口径
const std::array<SheetCol, kPoolVals> kValCols = [] {
  constexpr const char *num[kPoolVals] = {"归母净利TTM", "归母权益MRQ", "营业总收入TTM", "经营现金流TTM", "近4季报告期税前分红"};
  constexpr const char *neg[kPoolVals] = {"亏损计负", "负权益计负", "无收入计 0", "负现金流计负", "不分红计 0"};
  std::array<SheetCol, kPoolVals> o;
  for (int b = 0; b < kPoolVals; ++b) {
    char s[1024];
    std::snprintf(s, sizeof(s),
                  "%s = %s 倒数 (%s / 总市值) 的估值分位\n"
                  "• 算法: 区间末日 %s 在过去 N 年 (上方滑条) 窗内的加权分位; 收益率越低越贵 (高 = 贵, ≤ 0 排最贵端)\n"
                  "• 数据: 分子 = %s (%s); .加 = Σ分子 / Σ总市值, .等 = 成分 %s 的算术平均; 成分 / 财务取当日适用 (无前视)\n"
                  "• 显示: 分位百分比, 底色 = 组内同口名次 (便宜绿 → 贵红); 绝对值看主图 Band 图例\n"
                  "• 成分行: 无分位, 显示该股当前 %s (%s; 基本面最新快照, 同 Assets 估值列), 不铺底色, 不随区间; 不可算 → -\n"
                  "• 适用: 指数 / 行业 / 概念; 随区间末日, 估值史不满 1 年 → -",
                  kVal[b].name, kVal[b].inv, num[b], kVal[b].name, num[b], neg[b], kVal[b].name, kVal[b].name,
                  b == kValDy ? "= DY1 近 1 年分红 / 总市值" : (std::string("= 1 / ") + kVal[b].inv).c_str());
    o[static_cast<size_t>(b)] = {kVal[b].name, s};
  }
  return o;
}();
const SheetCol kValName = {"名", "名 = 篮子名\n"
                                 "• 算法: 切片名 + .等 (等权) / .加 (总市值加权)\n"
                                 "• 数据: 切片 = 全A / 池 / 申万一级行业 / 东财题材概念\n"
                                 "• 显示: 超长截断补 …, 悬停看全名; 选中行高亮 (主图画它)\n"
                                 "• 成分行: 篮子名前 ▸ 展开, 缩进列出成分 (同 NAV 页); 点 = 叠加 / 取消到 NAV 日内栏 / FLOW 图\n"
                                 "• 适用: 指数 / 行业 / 概念"};
// 段: 各口估值分位 (组 g 的 PortfolioView::pct; 成分行 = 该股当前收益率, 不铺底色)
Seg val_seg(const PortfolioView &v, int g) {
  return {{kValCols.begin(), kValCols.end()},
          [&v, g](size_t r, PortfolioSheet &, SheetCell *out) {
            for (int b = 0; b < kPoolVals; ++b)
              out[b] = ranked_cell(v.pct[g][r][b], "%.0f%%");
          },
          [](const KidSrc &x, size_t, PortfolioSheet &, SheetCell *out) {
            const ImVec4 &txt = ImGui::GetStyleColorVec4(ImGuiCol_Text);
            for (int b = 0; b < kPoolVals; ++b) {
              const double y = x.info ? stock_yield(*x.info, b) : kNaN;
              out[b] = sheet_cell(y, txt, "%.*f%%", kVal[b].prec, 100.0 * y);
            }
          },
          {}};
}
void legend_val(PortfolioUIState &ui, const PortfolioDerived &d, const PortfolioView &v, const StockInfos &infos) {
  ImGui::SetNextItemWidth(-FLT_MIN);
  ImGui::SliderInt("##pf_band_years", &ui.band_years, 1, kBandYearsMax, "Band 滚动窗 %d 年", ImGuiSliderFlags_AlwaysClamp);
  if (ImGui::IsItemHovered())
    tip("Band 分位的滚动窗长 N: 每日取过去 N 年 (含当日) 的收益率加权分位, 拖动即时重算\n"
        "• 权重 = 1 - exp(-10 u), u = 1 - 距今 / 窗跨度: 近 ~3/4 窗约等权, 窗尾段快速降到 0 (老点出窗时分位不跳)\n"
        "• 池估值史不足 N 年时先扩张窗 (权重按实际史长缩放); 不满 1 年不画");
  button_bar(kNavGroupName, kPageDefs[kPageVal].groups, ui.cat[kPageVal]);
  ImGui::SameLine();
  ImGui::TextColored(kHead, "(单选)");
  if (ImGui::IsItemHovered())
    tip("类别条选一组 (指数 = 各池, 行业 = 申万一级, 概念 = 东财题材概念), 点行选一个: 主图画它的价格 + EP / BP / SP / CP / DY Band; 选中换组保留\n"
        "• 价格: 全A.加 / 行业 / 概念 = 总市值加权指数 (全A 内切); 全A.等 / 其余池 = 等权指数 (同 NAV 池线)\n"
        "• 行业成员 = 全A 成分按当日适用 (盘前已知) 的 SW2021 一级行业 (ind_l1) 切; 行业未知的只进全A\n"
        "• 概念成员 = 全A 成分 ∩ 最近一份东财概念快照的成分 (全史同一份)\n"
        "• 估值与价格同权重: 个股收益率 = 分子 / 总市值; 加权 = 整体法 Σ分子 / Σ总市值, 等权 = 成分个股收益率的算术平均 (= 等权组合每元的分子)\n"
        "  分子: EP = 归母净利TTM, BP = 归母权益MRQ, SP = 营业总收入TTM, CP = 经营现金流TTM, DY = 近4季报告期税前分红 (亏损 / 负权益 / 负现金流照计为负)\n"
        "• 成分 = 当日持仓 (同池指数: 上一交易日收盘建仓的池); 市值 = 收盘价 × 当日适用总股本, 财务取当日适用 (无前视)\n"
        "• 估值分位 = 当日收益率在过去 N 年 (含当日; 上方滑条) 窗内的加权分位 (窗尾降权, 见滑条提示), 收益率越低越贵 (高 = 贵; ≤ 0 排在最贵端), 逐日滚动; 估值史不满 1 年不画\n"
        "• Band = 价格 × 当日收益率 / q, q = 窗内 5/10/25/50/75/90/95%% 贵度分位的收益率 (与倍数口径的 Band 同一条线);\n"
        "  当日 ≤ 0 或窗内收益率 > 0 的日子不足 80%% 整段不画, 某档 q ≤ 0 该档线断开 (价格无解); 分位照常. 最便宜档以下绿 → 最贵档以上红\n"
        "• 各列怎么算: 悬停表头 (算法 / 数据 / 显示 / 适用)\n"
        "• 表上过滤条 (与 NAV / FLOW 页同一份): 按只数 / 总市值 / |资金流入| 筛本组的行, 选中 / 展开行恒列\n"
        "• 行名前 ▸ = 展开成分股 (至多一行, 三页同一份; 该行钉在表头下): 各口显示该股当前收益率 (非分位); 点成分行 = 叠加到 NAV 日内栏 / FLOW 图\n"
        "• 列宽按内容最紧, 超出栏宽横滚 (名列不动); 名超长截断补 …, 悬停中文名看全名; 点表头排序 (组内): 升序 → 降序 → 原序; 名列按颜色排 (相似的挨着)");
  table(ui, infos, d, kPageVal, kValName, {today_seg(), val_seg(v, ui.cat[kPageVal])});
}

// ---- FLOW 右图例: 类别条 (指数 / 行业 / 概念) + 过滤条; 表 = 该组一行一篮子, 点行 = 选中 / 取消 (与 NAV 同一份选中, 左图画选中的).
//      列 = 中文名 (线色) | 当日四列 | 日轴各日 (MM-DD) 净流入占当日成交额 | 档内合计净流入 (亿) | 合计占比; 按正负着色, 悬停格看净流入 / 成交额; 表头点排序 (组内) ----
// 列: 名 / 一日 (表头 = 该日 MM-DD, 拼在说明前) / 合计两列
const SheetCol kFlowNameCol = {"名", "名 = 篮子名\n"
                                     "• 算法: 切片名 + .等 (等权) / .加 (总市值加权); 资金流不分权重, 同切片两行同成分即同值\n"
                                     "• 数据: 切片 = 全A / 池 / 申万一级行业 / 东财题材概念\n"
                                     "• 显示: 字色 = 线色 (同 NAV); 超长截断补 …, 悬停看全名\n"
                                     "• 成分行: 篮子名前 ▸ 展开, 缩进列出成分 (同 NAV 页); 点 = 叠加 / 取消到左图 (字色 = 叠加线色)\n"
                                     "• 适用: 指数 / 行业 / 概念"};
const std::string kFlowDayTip = " = 该日资金流入占成交额\n"
                                "• 算法: 净流入 / 成交额; 净流入 = Σ 成分 Σ 分钟 ± 该分钟额 (分钟涨跌法; 成分行 = 本只): 收 > 上一分钟收 (首分钟对昨收) 记 +, < 记 −, 平随上一分钟\n"
                                "• 数据: 当日 = 新浪快照合成 (实时); 历史日 = all.bar 库 L_1min (其后至基本面末日 = bar1m) 合成; 成分之和不加权;\n"
                                "  首根 09:31 (开盘集合竞价) / 末根 15:00 (收盘集合竞价) 按图左上角 开收盘 开关计入 (默认不计)\n"
                                "• 显示: 百分比两位, 按正负着色; 悬停格看净流入 / 成交额; 该日无分钟根 → -\n"
                                "• 适用: 指数 / 行业 / 概念; 成分行只当日 (历史日 -, 看左图叠加); 随档位 (日内 / 2日 … 5日)";
const SheetCol kFlowSumCols[2] = {
    {"净流入", "净流入 = 档内合计净流入\n"
               "• 算法: 日轴各日净流入之和 (各日同上列)\n"
               "• 数据: 同各日列\n"
               "• 显示: 亿 / 万亿带符号, 按正负着色; 悬停格看成交额\n"
               "• 适用: 指数 / 行业 / 概念; 成分行只档位 = 日内; 随档位"},
    {"占比", "占比 = 档内合计净流入占合计成交额\n"
             "• 算法: Σ 各日净流入 / Σ 各日成交额 (左图占比的分母是近 5 日日均成交额, 两者不同)\n"
             "• 数据: 同各日列\n"
             "• 显示: 百分比两位, 按正负着色; 悬停格看净流入 / 成交额\n"
             "• 适用: 指数 / 行业 / 概念; 成分行只档位 = 日内; 随档位"},
};
// 一段的净流入占成交额 (无根 → NaN)
double flow_pct(const Database::Flow &x) { return x.amount > 0 ? x.net / x.amount : kNaN; }
// 段: 日轴各日净流入占比 + 档内合计两列 (篮子行 = Assets 篮子同一份; 成分行只当日); 各行流水记进 p.flow[行] (末项 = 合计; 悬停看金额)
Seg flow_seg(const Database::BasketState &bs, const std::vector<PortfolioRow> &rs, const std::vector<misc::Ymd> &days) {
  const size_t nd = days.size();
  Seg s;
  for (const misc::Ymd day : days) {
    char t[16];
    std::snprintf(t, sizeof(t), "%02d-%02d", day / 100 % 100, day % 100);
    s.cols.push_back({t, t + kFlowDayTip});
  }
  s.cols.insert(s.cols.end(), std::begin(kFlowSumCols), std::end(kFlowSumCols));
  // 行 row 的各日资金流 x (日轴同序; whole = 各日齐全才有合计) → p.flow[row] + 格
  const auto fill = [nd](std::vector<Database::Flow> x, bool whole, size_t row, PortfolioSheet &p, SheetCell *out) {
    x.resize(nd);
    Database::Flow sum;
    if (whole)
      for (const Database::Flow &y : x)
        sum.net += y.net, sum.amount += y.amount;
    x.push_back(sum);
    for (size_t k = 0; k < nd; ++k)
      out[k] = signed_cell(flow_pct(x[k]));
    const double net = sum.amount > 0 ? sum.net / 1e8 : kNaN;
    out[nd] = sheet_cell(net, tint(net, 0.0), "%s", yi_text(net, true).c_str());
    out[nd + 1] = signed_cell(flow_pct(sum));
    p.flow[row] = std::move(x);
  };
  s.row = [&bs, &rs, &days, fill](size_t r, PortfolioSheet &p, SheetCell *out) {
    assert(rs[r].basket >= 0);
    fill(Database::FlowOf(bs, static_cast<size_t>(rs[r].basket), days), true, r, p, out);
  };
  s.kid = [&bs, &days, fill, nd](const KidSrc &x, size_t row, PortfolioSheet &p, SheetCell *out) {
    std::vector<Database::Flow> fl(nd);
    bool whole = true;
    for (size_t k = 0; k < nd; ++k)
      if (x.day && x.day->candle.date == days[k])
        fl[k] = Database::FlowOf(x.day->bars, bs.flow_ends);
      else
        whole = false;
    fill(std::move(fl), whole, row, p, out);
  };
  s.tip = [nd](const PortfolioSheet &p, size_t row, int k) {
    const Database::Flow &x = p.flow[row][std::min(static_cast<size_t>(k), nd)]; // 合计两列同一段
    if (x.amount > 0)
      tip("净流入 %s / 成交额 %s = %+.2f%%", yi_text(x.net / 1e8, true).c_str(), yi_text(x.amount / 1e8).c_str(), 100.0 * x.net / x.amount);
  };
  return s;
}
void legend_flow(PortfolioUIState &ui, const PortfolioDerived &d, const std::vector<misc::Ymd> &days, const StockInfos &infos) {
  button_bar(kNavGroupName, kPageDefs[kPageFlow].groups, ui.cat[kPageFlow]);
  ImGui::SameLine();
  ImGui::TextColored(kHead, "(点行 = 画)");
  if (ImGui::IsItemHovered())
    tip("点行 = 选中 / 取消, 选中 = 左图画该篮子的累计净流入 (与 NAV 页同一份选中: 这里选的 NAV 主图 / 日内栏也画); 选中换组保留\n"
        "• 指数 = 各池 (all 池拆 全A.等 / 全A.加); 行业 = 申万一级; 概念 = 东财题材概念 (全A 内切)\n"
        "• 各列怎么算: 悬停表头 (算法 / 数据 / 显示 / 适用)\n"
        "• 表上过滤条 (与 NAV / VAL 页同一份): 按只数 / 总市值 / |当日资金流入| 筛本组的行, 已画 / 展开的行恒列\n"
        "• 行名前 ▸ = 展开成分股 (至多一行, 三页同一份; 该行钉在表头下): 资金流只当日; 点成分行 = 叠加到左图 (个股历史 + 新浪当日, 跨日接续)\n"
        "• 列宽按内容最紧, 超出栏宽横滚 (名列不动); 名超长截断补 …, 悬停中文名看全名; 点表头排序 (组内): 升序 → 降序 → 原序; 名列按颜色排 (相似的挨着)");
  table(ui, infos, d, kPageFlow, kFlowNameCol, {today_seg(), flow_seg(ui.basket, d.nav[ui.cat[kPageFlow]], days)}, days, ui.basket.flow_ends);
}

} // namespace

void SyncPortfolioDir(PortfolioUIState &ui, const std::string &dir) {
  if (ui.dir == dir)
    return;
  ui.dir = dir;
  relist(ui, ui.name);
}

void RenderPortfolioBoxes(PortfolioUIState &ui, const FactorRow *view_row, const std::string &pool, LevelId lv, int hold) {
  const ImGuiStyle &st = ImGui::GetStyle();
  const float h = 3.0f * ImGui::GetFrameHeightWithSpacing() - st.ItemSpacing.y + 2.0f * st.WindowPadding.y;
  constexpr float kW1 = 250.0f, kW2 = 380.0f;

  // ---- 小框 1: 选组合 / New / Rename / Del ----
  ImGui::BeginChild("##pf_box", ImVec2(kW1, h), ImGuiChildFlags_Borders, ImGuiWindowFlags_NoScrollbar);
  ImGui::TextColored(kHead, "Portfolio");
  ImGui::SameLine();
  ImGui::SetNextItemWidth(-FLT_MIN);
  if (ImGui::BeginCombo("##pf_sel", ui.name.empty() ? "-" : ui.name.c_str())) {
    for (const std::string &n : ui.names)
      if (ImGui::Selectable(n.c_str(), n == ui.name) && n != ui.name) {
        ui.name = n;
        load_current(ui);
        ui.msg.clear();
        ui.confirm_delete = false;
      }
    ImGui::EndCombo();
  }
  if (ImGui::IsItemHovered()) // 组合内容 = <dir>/<name>.json, 改了即存; 选哪个只在内存
    tip("当前组合 (改动即存盘; Portfolio 页画它的曲线)");
  const std::string nm = ui.name_buf;
  const bool nm_ok = ValidFactorName(nm) && std::find(ui.names.begin(), ui.names.end(), nm) == ui.names.end();
  ImGui::SetNextItemWidth(96);
  ImGui::InputTextWithHint("##pf_name", "name", ui.name_buf, sizeof(ui.name_buf));
  ImGui::SameLine();
  ImGui::BeginDisabled(!nm_ok);
  if (ImGui::SmallButton("New")) {
    SavePortfolio(ui.dir, nm, {});
    relist(ui, nm);
    ui.msg = "新建 " + nm;
    ui.name_buf[0] = '\0';
  }
  ImGui::EndDisabled();
  if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled))
    tip("用左边的名字新建空组合 (仅 [A-Za-z0-9_], 不与已有重名)");
  ImGui::SameLine();
  ImGui::BeginDisabled(!nm_ok || ui.name.empty());
  if (ImGui::SmallButton("Rename")) {
    const std::string old = ui.name;
    const bool ok = RenamePortfolio(ui.dir, old, nm);
    assert(ok && "重名已在按钮处排除");
    (void)ok;
    relist(ui, nm);
    ui.msg = old + " → " + nm;
    ui.name_buf[0] = '\0';
  }
  ImGui::EndDisabled();
  if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled))
    tip("当前组合改成左边的名字");
  ImGui::SameLine();
  ImGui::BeginDisabled(ui.name.empty());
  if (ImGui::SmallButton("Del"))
    ui.confirm_delete = true;
  ImGui::EndDisabled();
  if (ui.confirm_delete) {
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "删除 %s?", ui.name.c_str());
    ImGui::SameLine();
    if (ImGui::SmallButton("Yes")) {
      DeletePortfolio(ui.dir, ui.name);
      ui.msg = "已删除 " + ui.name;
      relist(ui, "");
      ui.confirm_delete = false;
    }
    ImGui::SameLine();
    if (ImGui::SmallButton("No"))
      ui.confirm_delete = false;
  } else {
    ImGui::TextDisabled("%s", ui.msg.c_str());
  }
  ImGui::EndChild();

  // ---- 小框 2: 当前组合的条目 ----
  ImGui::SameLine();
  ImGui::BeginChild("##pf_items", ImVec2(kW2, h), ImGuiChildFlags_Borders, ImGuiWindowFlags_NoScrollbar);
  ImGui::TextColored(kHead, "Factors (%zu)", ui.entries.size());
  ImGui::SameLine();
  const bool row_ok = view_row && view_row->error.empty() && !view_row->expr.empty();
  ImGui::BeginDisabled(ui.name.empty() || !row_ok || hold <= 0);
  if (ImGui::SmallButton("Add")) {
    const PortfolioEntry e{pool, lv, view_row->file, view_row->expr, hold, view_row->sign};
    const auto dup = std::find_if(ui.entries.begin(), ui.entries.end(), [&](const PortfolioEntry &x) { return x.key() == e.key(); });
    if (dup != ui.entries.end()) {
      ui.msg = entry_label(e) + " 已在组合里";
    } else {
      ui.entries.push_back(e);
      save_current(ui);
      ui.sel = static_cast<int>(ui.entries.size()) - 1;
      ui.msg = "加入 " + entry_label(e);
    }
  }
  ImGui::EndDisabled();
  if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled)) {
    if (ui.name.empty())
      tip("先在左框新建 / 选一个组合");
    else if (!row_ok)
      tip("先在表格里点一行");
    else
      tip("把 %s 加进组合 (持有期 = 当前 Hold %s)", view_row->key.c_str(), factor::stat::hold_name(hold).c_str());
  }
  ImGui::SameLine();
  const bool sel_ok = ui.sel >= 0 && ui.sel < static_cast<int>(ui.entries.size());
  ImGui::BeginDisabled(!sel_ok);
  if (ImGui::SmallButton("Del##pf_item")) {
    ui.msg = "移除 " + entry_label(ui.entries[static_cast<size_t>(ui.sel)]);
    ui.entries.erase(ui.entries.begin() + ui.sel);
    save_current(ui);
    ui.sel = -1;
  }
  ImGui::EndDisabled();
  ImGui::BeginChild("##pf_list", ImVec2(0, 0));
  const bool synced = ui.items.size() == ui.entries.size();
  for (size_t i = 0; i < ui.entries.size(); ++i) {
    const PortfolioEntry &e = ui.entries[i];
    const PortfolioItem *it = synced && ui.items[i].e.key() == e.key() ? &ui.items[i] : nullptr;
    const bool bad = it && !it->missing.empty();
    if (bad)
      ImGui::PushStyleColor(ImGuiCol_Text, StatusColor(TaskStatus::Kind::Error));
    const std::string label = entry_label(e) + "##pfe" + std::to_string(i);
    if (ImGui::Selectable(label.c_str(), ui.sel == static_cast<int>(i)))
      ui.sel = ui.sel == static_cast<int>(i) ? -1 : static_cast<int>(i);
    if (bad)
      ImGui::PopStyleColor();
    if (ImGui::IsItemHovered())
      tip("%s / %s  %s  %s\n"
          "• %s\n"
          "• 规范串 %s%s%s%s",
          e.pool.c_str(), factor_level(e.lv).name, e.file.c_str(), it ? it->name_cn.c_str() : "", hold_desc(e.hold, e.lv).c_str(), e.expr.c_str(),
          e.sign < 0 ? " · 取反" : "", bad ? "\n• 失效: " : "", bad ? it->missing.c_str() : "");
  }
  ImGui::EndChild();
  ImGui::EndChild();
}

int RenderTabPortfolio(PortfolioService &svc, PortfolioUIState &ui, double fixed_impact, const Database::Market *mk, const StockInfos &infos) {
  PortfolioResult res;
  uint64_t gen = 0;
  std::string message;
  {
    std::lock_guard<std::mutex> lock(svc.mutex);
    res = svc.result; // shared_ptr 拷贝, 不拷序列
    gen = svc.result_gen;
    message = svc.message;
  }
  PortfolioDerived &d = ui.der.val;

  // ---- 三级缓存: 派生 → 滚动分位 / 末点分位 (键 = 篮子代) → 视图 (键 = 派生 gen + 二级两份 gen): 上级重算 → 下级自动重算 ----
  ui.der.update(PortfolioDerKey{gen, ui.items_gen, ui.basket.feed.shown, fixed_impact}, [&](PortfolioDerived &dv) {
    const std::vector<misc::Ymd> old = dv.dates;
    MakePortfolioDerived(ui.items, ui.name, res, ui.basket.feed.data, fixed_impact, dv);
    if (dv.dates != old) // 日轴变 → 区间回全段
      ui.range = PortfolioRange{.d0 = 0, .d1 = static_cast<int>(dv.dates.size())};
  });
  if (ui.pick.of != ui.name) // 换组合 → 选中清掉
    ui.pick = PortfolioPick{.of = ui.name};
  if (mk)
    sync_cons(ui, d, *mk->quote);
  const std::set<std::string> &one = ui.pick.rows[kPickOne];
  const PortfolioAt vs = one.empty() ? PortfolioAt{} : d.at(*one.begin()); // VAL 选中篮子行
  ui.roll.update(PortfolioRollKey{d.basket_gen, ui.band_years, vs},
                 [&](std::array<PortfolioRoll, kPoolVals> &r) { MakePortfolioRoll(d, vs, ui.band_years, r); });
  ui.pct.update(PortfolioPctKey{d.basket_gen, ui.band_years, ui.range.d1}, [&](PortfolioPct &p) { MakePortfolioPct(d, ui.range.d1, ui.band_years, p); });
  ui.view.update(PortfolioViewKey{ui.der.gen, ui.roll.gen, ui.pct.gen, ui.range.d0, ui.range.d1},
                 [&](PortfolioView &vw) { MakePortfolioView(d, ui.range.d0, ui.range.d1, vs, ui.roll.val, ui.pct.val, vw); });
  const PortfolioView &v = ui.view.val;

  ImGui::TextColored(kHead, "Portfolio:");
  ImGui::SameLine();
  if (ui.name.empty()) {
    ImGui::TextDisabled("无 (在 Factors 页 1. 区右侧新建 / 选组合)");
    return 0;
  }
  const int action = head(svc, ui, d, res, message, fixed_impact);
  const int page = pages();
  const bool nav = page == kPageNav;

  // ---- 图: 左 = 日内 (NAV 开日内, 切 1/3 高) / 主图 (plot_nav ↔ plot_val) / 区间条 自上而下, FLOW = 只一张资金流图; 最右 = 图例 (legend_nav ↔ legend_val ↔ legend_flow) ----
  constexpr float kBarH = 90.0f, kLegendCells = 100.0f, kLegendMaxFrac = 0.5f; // 右图例定宽 (三页同宽: 字符格数, 表格超宽横滚) / 至多占整宽的比例
  const ImGuiStyle &st = ImGui::GetStyle();
  const ImVec2 av = ImGui::GetContentRegionAvail();
  const float lw = std::min(kLegendCells * ImGui::CalcTextSize("0").x + 2.0f * (st.WindowPadding.x + st.ChildBorderSize), av.x * kLegendMaxFrac);
  const float left = av.x - lw - st.ItemSpacing.x;
  const float ph = av.y - kBarH - st.ItemSpacing.y;
  const float ih = nav && ui.intraday ? std::floor((ph - st.ItemSpacing.y) / 3.0f) : 0.0f;
  // 选中篮子行 + 点中的成分股的行情叠加图 (日内栏 / FLOW 同一份选中, 篮子线色 = 主图的), 贴边占满 size
  const auto basket_child = [&](const char *id, const ImVec2 &size, bool flow) {
    ImGui::PushStyleVar(ImGuiStyleVar_WindowPadding, ImVec2(0, 0));
    ImGui::BeginChild(id, size);
    ImGui::PopStyleVar();
    if (mk) {
      std::vector<Database::OverlayPick> picks;
      for (int g = 0; g < kBasketGroups; ++g)
        for (size_t r = 0; r < d.nav[g].size(); ++r)
          if (const PortfolioRow &h = d.nav[g][r]; picked(ui, page, h))
            picks.push_back({Database::kBasket, static_cast<uint32_t>(h.basket), ImGui::ColorConvertFloat4ToU32(nav_color(d, g, r, kTrade)),
                             basket_name(static_cast<size_t>(h.basket))});
      for (const uint32_t id : ui.pick.stocks)
        picks.push_back({Database::kStock, id, ImGui::ColorConvertFloat4ToU32(stock_color(id)), stock_name(infos, id)});
      if (flow)
        Database::RenderBasketFlow(*mk, ui.basket, picks);
      else
        Database::RenderBasketIntraday(*mk, ui.basket, picks);
    } else {
      ImGui::TextDisabled("行情等基本面就绪…");
    }
    ImGui::EndChild();
  };
  if (page == kPageFlow) {
    basket_child("##pf_flow", ImVec2(left, av.y), true);
    ImGui::SameLine();
    ImGui::BeginChild("##pf_flegend", ImVec2(lw, av.y), ImGuiChildFlags_Borders);
    legend_flow(ui, d, mk ? Database::FlowDays(*mk, ui.basket) : std::vector<misc::Ymd>{}, infos);
    ImGui::EndChild();
    return action;
  }
  ImGui::BeginGroup();
  if (ih > 0.0f)
    basket_child("##pf_intra", ImVec2(left, ih), false);
  ImPlot::PushStyleVar(ImPlotStyleVar_PlotPadding, ImVec2(2, 2)); // 贴边: 图区让给线
  ImPlot::PushStyleVar(ImPlotStyleVar_LabelPadding, ImVec2(2, 1));
  PortfolioFitKey fk{ui.view.gen, nav, ui.log_y, {}, ui.shown_key};
  if (nav)
    for (const std::vector<PortfolioRow> &rs : d.nav)
      for (const PortfolioRow &r : rs)
        fk.rows += picked(ui, kPageNav, r) ? '1' : '0';
  if (fk != ui.fit_key) // 只作用于紧接的 BeginPlot: 须在日内栏之后
    ImPlot::SetNextAxesToFit();
  if (ImPlot::BeginPlot(nav ? "##portfolio" : "##pf_val", ImVec2(left, ih > 0.0f ? ph - ih - st.ItemSpacing.y : ph), nav ? ImPlotFlags_None : ImPlotFlags_NoLegend)) {
    ui.fit_key = fk;
    // 线性: y 下限钉在 0 (净值 / 点位恒正), fit / 缩放只动上限; log: 正常
    ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_None, ui.log_y ? ImPlotAxisFlags_None : ImPlotAxisFlags_LockMin);
    ImPlot::SetupAxisFormat(ImAxis_X1, day_formatter, &d.dates);
    if (ui.log_y)
      ImPlot::SetupAxisScale(ImAxis_Y1, ImPlotScale_Log10);
    else
      ImPlot::GetCurrentPlot()->Axes[ImAxis_Y1].SetMin(0.0, /*force=*/true);
    ImPlot::SetupLegend(ImPlotLocation_NorthWest);
    // fit 帧的视野还是上帧的 (区间刚变时可能整段在外): 小字不按视野筛, 全收来量留白宽度
    const ImPlotRect cur = ImPlot::GetPlotLimits(); // 先锁 setup, FitThisFrame 才定
    PlotFrame pf{ImPlot::FitThisFrame() ? ImPlotRect(-DBL_MAX, DBL_MAX, -DBL_MAX, DBL_MAX) : cur, {}, {}, {}, {}};
    if (nav)
      plot_nav(ui, d, v, pf);
    else if (vs.g >= 0)
      plot_val(ui, d, v, vs, pf);
    ui.shown_key = std::move(pf.shown);
    NowCross cross{};
    const bool crossed = now_cross(pf, d.dates, cross);
    const float cw = crossed ? now_cross_width(cross, kTagScale) + kTagGap : 0.0f; // 十字右壁标签占的宽 (小字 / 刻度让开)
    // 本帧 fit (自动 / 双击): x 右端再扩一段, 让区间末点右侧留出线尾小字 / 刻度 (同贴末点, 取宽者) + 十字标签的宽度
    if (ImPlot::FitThisFrame() && !pf.tags.empty() && ui.range.d1 > ui.range.d0) {
      const float tw = std::max(end_tags_width(pf.tags, kTagScale), marks_width(pf.marks)) + cw, W = ImPlot::GetPlotSize().x;
      const double x0 = x_of(static_cast<size_t>(ui.range.d0), kOpen), x1 = x_of(static_cast<size_t>(ui.range.d1), kOpen);
      if (W > 2.0f * tw)
        ImPlot::FitPointX(x1 + (x1 - x0) * tw / (W - tw));
    }
    draw_end_tags(pf.tags, kTagScale, tags_right() - cw);
    draw_marks(pf.marks, tags_right() - cw);
    if (crossed)
      draw_now_cross(cross, kTagScale, tags_right());
    ImPlot::EndPlot();
  }
  ImPlot::PopStyleVar(2);
  range_bar(ui, nav, vs, left, kBarH);
  ImGui::EndGroup();

  ImGui::SameLine();
  ImGui::BeginChild("##pf_flegend", ImVec2(lw, av.y), ImGuiChildFlags_Borders);
  if (nav)
    legend_nav(ui, d, v, infos);
  else
    legend_val(ui, d, v, infos);
  ImGui::EndChild();
  return action;
}

} // namespace GUI::Factors
