// Factors→Portfolio — 见头文件
#include "gui/task_factors/ui/TabPortfolio.hpp"

#include "features/TimeIndex.hpp"             // TRADE_MINUTES_PER_DAY (横轴每日跨度)
#include "gui/Tasks.hpp"                      // StatusColor
#include "gui/task_factors/ui/TabFactors.hpp" // tip / hold_desc
#include "gui/util/Chart.hpp"                 // tint / button_bar / EndTag
#include "misc/date.hpp"                      // to_days (滚动窗权重按日历天)
#include "shared/Basket.hpp"                  // basket_group / basket_name / cut_code
#include "shared/FeatureLib.hpp"              // pool_id

#include "imgui.h"
#include "implot.h"
#include "implot_internal.h" // GetItem (主图例显隐 / 池线色)

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
#include <limits>
#include <numeric>
#include <ranges>
#include <set>
namespace GUI::Factors {

namespace {

// 横轴格 x = 日下标 · kDayW + 日内分钟 (同 OrderFlow 图 2); 横轴编码只在这里
constexpr size_t kDayW = TRADE_MINUTES_PER_DAY;
constexpr size_t kOpen = 0, kClose = kDayW - 1; // 日内分钟: 开盘 / 收盘
constexpr size_t x_at(size_t di, size_t minute) { return di * kDayW + minute; }
double x_of(size_t di, size_t minute) { return static_cast<double>(x_at(di, minute)); }
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

// NAV 组表 [NavGroup]: 右表类别条名 (前 kBasketGroups 个 = VAL 类别条) / 线宽 (策略加粗)
constexpr const char *kNavGroupName[kNavGroups] = {"指数", "行业", "概念", "因子", "策略"};
constexpr float kNavWidth[kNavGroups] = {1.0f, 1.0f, 1.0f, 1.0f, 2.0f};
const ImVec4 kStratCol(1.0f, 0.55f, 0.15f, 1.0f);
// 成对线表 [口径][PairLine] (口径 = pair_kind: 0 = 篮子, 1 = 因子 / 策略): 主图例口径列 / 线尾小字后缀; 线 ID 后缀 [PairLine]
constexpr const char *kPairName[2][kPairLines] = {{"可成交", "理想"}, {"有冲击", "无冲击"}};
constexpr const char *kPairTag[2][kPairLines] = {{" 可成交", " 理想"}, {"", " 无冲击"}};
constexpr const char *kPairId[kPairLines] = {"", "_ideal"};
int pair_kind(int g) { return g >= kNavFactor; }
std::string line_id(const PortfolioRow &r, int l) { return r.id + kPairId[l]; }
// 篮子行 id = pf_ + 切片代码 + 权重后缀 [Weight]
constexpr const char *kWeightId[kWeights] = {"", "_cap"};
std::string basket_id(size_t line) { return "pf_" + cut_code(cut_of_line(line)) + kWeightId[weight_of(line)]; }
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
constexpr int kContours = 20;         // 一口的档数
constexpr double kContourTrim = 0.02; // 全史区间两头各剔的分位 (近 0 收益率的日子倍数极大, 不让它撑开区间)
constexpr float kContourHit = 6.0f;   // 悬停命中半宽 (px)
const ImVec4 kContourCol(0.6f, 0.6f, 0.6f, 0.22f), kContourHotCol(1.0f, 1.0f, 1.0f, 0.9f);
// 收益率序列 v[0, n) 中 v > 0 的点的倍数 1 / v, 取 [kContourTrim, 1 − kContourTrim] 分位为区间, 首尾含在内 kContours 等分 (升序); 不足两个不同倍数 → 空
std::vector<double> levels_of(const float *v, size_t n) {
  std::vector<double> m;
  for (size_t i = 0; i < n; ++i)
    if (v[i] > 0.f)
      m.push_back(1.0 / v[i]);
  if (m.empty())
    return {};
  const auto at = [&](double q) {
    const auto it = m.begin() + static_cast<std::ptrdiff_t>(q * static_cast<double>(m.size() - 1));
    std::nth_element(m.begin(), it, m.end());
    return *it;
  };
  const double lo = at(kContourTrim), hi = at(1.0 - kContourTrim);
  if (!(hi > lo))
    return {};
  std::vector<double> lv(kContours);
  for (int k = 0; k < kContours; ++k)
    lv[static_cast<size_t>(k)] = lo + (hi - lo) * k / (kContours - 1);
  return lv;
}
// 一条等值线 (零拷贝: 按下标回调取点; u 的 NaN 处断开)
struct ContourLine {
  const double *x, *u;
  double m;
};
ImPlotPoint contour_pt(int i, void *p) {
  const ContourLine &l = *static_cast<const ContourLine *>(p);
  return {l.x[i], l.u[i] * l.m};
}

// 行名主体的显示宽上限 (格: ASCII 1, 其余 2): 超出 → 截到 kNameCells − 2 格补 "…" (表 / 图例 / 线尾同用; 全名进悬停)
constexpr int kNameCells = 14;
std::string clip_name(const std::string &s) {
  int w = 0;
  size_t cut = 0;
  for (size_t i = 0; i < s.size();) {
    const unsigned char c = static_cast<unsigned char>(s[i]);
    i += c < 0x80 ? 1 : c < 0xE0 ? 2
                    : c < 0xF0   ? 3
                                 : 4;
    w += c < 0x80 ? 1 : 2;
    if (w <= kNameCells - 2)
      cut = i;
  }
  return w <= kNameCells ? s : s.substr(0, cut) + "…";
}
// 表格数值格 (格式串不补宽): 先排全表量出各列本帧最宽 (含表头), 再右对齐到该宽 ⇒ 列宽恒 = 本帧内容, 可缩
//   (不能对齐到列宽本身: 列宽是上帧的, 对齐后每格都顶满, 列只涨不缩)
struct NumCell {
  char s[24] = "-";
  ImVec4 col;
  float w = 0.f;
};
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
// 右图例的宽 (NAV / VAL 同一份) = 表格列不截断所需 (end_table) 与表上工具行 (末项在 last_x) 之大者
float legend_width(float last_x, float table_w) { return std::max(last_x - ImGui::GetWindowPos().x - ImGui::GetStyle().WindowPadding.x, table_w); }

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
// NAV 组 g 行 r 线 l 的色 (主图 / 右表 / 日内同一份): 策略 = kStratCol, 其余 = 行色相 (见 paint) 的定亮定艳色, 未定色 = 同亮度灰; 理想列 = 同色淡
constexpr float kIdealAlpha = 0.5f;
constexpr float kRowL = 0.75f, kRowC = 0.125f; // 行色的 OKLCH 亮度 / 艳度: 该亮度下整环都在 sRGB 色域内的艳度上限附近 (环不被截变形)
ImVec4 nav_color(const PortfolioDerived &d, int g, size_t r, int l) {
  const float hue = d.nav[g][r].hue;
  ImVec4 c = g == kNavStrat ? kStratCol : std::isnan(hue) ? oklch(kRowL, 0.0f, 0.0f)
                                                          : oklch(kRowL, kRowC, hue);
  c.w = l == kIdeal ? kIdealAlpha : 1.0f;
  return c;
}

// ---- 行配色 (策略外四组一起, 见 PortfolioDerived::nav): 相似度 s = max(成员重合, 残差相关), 平方距离 2 (1 − s) 上 Ward 树定叶序;
//      叶序相邻两行的色相差 ∝ 它们在树上分开处两侧的平均距离 (1 − s), 累加后铺满色环的前 kArc (首尾不相接) ----
constexpr size_t kSimDays = 60; // 残差相关取日并集轴末几日的日收益
constexpr size_t kSimMin = 20;  // 两行共同有收益的日数下限 (不足 → 相关记 0)
constexpr float kArc = 0.85f;   // 色相铺开的弧长 (整环 = 1)

// 线 c 在日并集轴 [lo, lo + n) 各日的日收益 (日末值逐日比 − 1): 无点的日沿用前一日末值 (lo 前的末点起), 不可算 = NaN
std::vector<double> rets_of(const PortfolioCurve &c, size_t lo, size_t n) {
  const auto it = std::lower_bound(c.x.begin(), c.x.end(), x_of(lo, kOpen));
  std::vector<double> e(n + 1, kNaN); // [0] = lo 前末点, [1 + k] = lo + k 日末
  if (it != c.x.begin())
    e[0] = c.y[static_cast<size_t>(it - c.x.begin()) - 1];
  for (auto i = it; i != c.x.end(); ++i) {
    const size_t k = static_cast<size_t>(*i) / kDayW - lo;
    assert(k < n);
    e[1 + k] = c.y[static_cast<size_t>(i - c.x.begin())];
  }
  for (size_t k = 1; k <= n; ++k)
    if (std::isnan(e[k]))
      e[k] = e[k - 1];
  std::vector<double> r(n);
  for (size_t k = 0; k < n; ++k)
    r[k] = e[k] > 0 && e[k + 1] > 0 ? e[k + 1] / e[k] - 1.0 : kNaN;
  return r;
}
// r 对 m 在共同日上回归 (带截距) 的残差 (共同日外 NaN); 共同日不足 kSimMin → 全 NaN
void residual(std::vector<double> &r, const std::vector<double> &m) {
  double n = 0, sr = 0, sm = 0, srm = 0, smm = 0;
  for (size_t k = 0; k < r.size(); ++k)
    if (std::isfinite(r[k]) && std::isfinite(m[k]))
      ++n, sr += r[k], sm += m[k], srm += r[k] * m[k], smm += m[k] * m[k];
  const double vm = n >= kSimMin ? smm - sm * sm / n : 0.0;
  if (!(vm > 0)) {
    std::fill(r.begin(), r.end(), kNaN);
    return;
  }
  const double b = (srm - sr * sm / n) / vm, a = (sr - b * sm) / n;
  for (size_t k = 0; k < r.size(); ++k)
    r[k] = std::isfinite(r[k]) && std::isfinite(m[k]) ? r[k] - a - b * m[k] : kNaN;
}
// 两条残差在共同日上的相关 (残差已零均值); 共同日不足 kSimMin / 无波动 → 0
double corr(const std::vector<double> &a, const std::vector<double> &b) {
  size_t n = 0;
  double ab = 0, aa = 0, bb = 0;
  for (size_t k = 0; k < a.size(); ++k)
    if (std::isfinite(a[k]) && std::isfinite(b[k]))
      ++n, ab += a[k] * b[k], aa += a[k] * a[k], bb += b[k] * b[k];
  return n >= kSimMin && aa > 0 && bb > 0 ? ab / std::sqrt(aa * bb) : 0.0;
}

// Ward 连接 (最近邻链, O(n²)): 平方距离 dist [n × n] 就地改写 → n − 1 次合并 (两簇各取一成员 a / b, 合并高 h), 按 h 升序.
// 合并代价 = 簇内平方和的增量, 偏好大小相近的簇 (不会一簇吞掉大半)
struct Merge {
  size_t a, b;
  double h;
};
std::vector<Merge> ward_linkage(std::vector<double> &dist, size_t n) {
  std::vector<size_t> sz(n, 1), chain;
  std::vector<uint8_t> live(n, 1);
  std::vector<Merge> ms;
  while (ms.size() + 1 < n) {
    if (chain.empty())
      chain.push_back(static_cast<size_t>(std::find(live.begin(), live.end(), 1) - live.begin()));
    const size_t a = chain.back(), p = chain.size() > 1 ? chain[chain.size() - 2] : n;
    size_t b = p; // 同距优先链上前一个 (否则可能来回不收)
    for (size_t c = 0; c < n; ++c)
      if (live[c] && c != a && (b == n || dist[a * n + c] < dist[a * n + b]))
        b = c;
    if (b != p) {
      chain.push_back(b);
      continue;
    }
    chain.resize(chain.size() - 2);
    ms.push_back({a, b, dist[a * n + b]});
    const double wa = static_cast<double>(sz[a]), wb = static_cast<double>(sz[b]), dab = dist[a * n + b];
    for (size_t c = 0; c < n; ++c)
      if (live[c] && c != a && c != b) {
        const double wc = static_cast<double>(sz[c]);
        dist[b * n + c] = dist[c * n + b] = ((wa + wc) * dist[a * n + c] + (wb + wc) * dist[b * n + c] - wc * dab) / (wa + wb + wc);
      }
    sz[b] += sz[a], live[a] = 0;
  }
  std::stable_sort(ms.begin(), ms.end(), [](const Merge &x, const Merge &y) { return x.h < y.h; });
  return ms;
}

// 策略外四组各行定色相 (见上); 因子没到齐 / 价格指数未就绪 / 日轴不足两日 → 全不定色 (灰, 免得边算边跳)
void paint(PortfolioDerived &d) {
  std::vector<PortfolioRow *> rs;
  size_t mkt = SIZE_MAX; // 全A.加 (残差的基准)
  for (int g = 0; g < kNavStrat; ++g)
    for (PortfolioRow &r : d.nav[g]) {
      if (r.basket == static_cast<int>(basket_line(0, kWeightCap)))
        mkt = rs.size();
      rs.push_back(&r);
      r.hue = std::numeric_limits<float>::quiet_NaN();
    }
  const size_t n = rs.size(), D = d.dates.size();
  assert(mkt < n);
  if (d.n_wait > 0 || !d.prices || D < 2)
    return;

  const size_t T = std::min(kSimDays, D - 1), lo = D - T;
  const std::vector<double> m = rets_of(rs[mkt]->line[kIdeal], lo, T);
  std::vector<std::vector<double>> res(n); // [行] 残差日收益
  for (size_t i = 0; i < n; ++i) {
    res[i] = rets_of(rs[i]->line[kIdeal], lo, T);
    residual(res[i], m);
  }
  std::fill(res[mkt].begin(), res[mkt].end(), kNaN); // 自身对自身回归无残差
  const size_t K = d.prices->keys.size();
  assert(d.prices->overlap.size() == K * K);
  std::vector<double> far(n * n, 0.0); // [行 × 行] 1 − s
  for (size_t i = 0; i < n; ++i)
    for (size_t j = i + 1; j < n; ++j) {
      double s = corr(res[i], res[j]);
      if (rs[i]->basket >= 0 && rs[j]->basket >= 0)
        s = std::max(s, static_cast<double>(d.prices->overlap[static_cast<size_t>(rs[i]->basket) * K + static_cast<size_t>(rs[j]->basket)]));
      far[i * n + j] = far[j * n + i] = 1.0 - s;
    }

  // Ward 树: 结点 < n = 叶, n + k = 第 k 次合并; 子先于父 ⇒ 按号自底向上数叶数, 自顶向下定各结点叶段起点 (叶序 = 先左子后右子)
  std::vector<double> sq(n * n);
  std::transform(far.begin(), far.end(), sq.begin(), [](double x) { return 2.0 * x; });
  const std::vector<Merge> ms = ward_linkage(sq, n);
  std::vector<size_t> up(n), node(n);
  std::iota(up.begin(), up.end(), 0), std::iota(node.begin(), node.end(), 0);
  const auto find = [&](size_t x) {
    while (up[x] != x)
      x = up[x] = up[up[x]];
    return x;
  };
  std::vector<std::array<size_t, 2>> kids; // [结点 − n] 两子
  for (const Merge &mg : ms) {
    const size_t a = find(mg.a), b = find(mg.b);
    kids.push_back({node[a], node[b]});
    up[a] = b, node[b] = n + kids.size() - 1;
  }
  const size_t N = 2 * n - 1;
  std::vector<size_t> cnt(N, 1), at(N, 0); // [结点] 叶数 / 叶段起点
  for (size_t x = n; x < N; ++x)
    cnt[x] = cnt[kids[x - n][0]] + cnt[kids[x - n][1]];
  for (size_t x = N; x-- > n;)
    at[kids[x - n][0]] = at[x], at[kids[x - n][1]] = at[x] + cnt[kids[x - n][0]];
  std::vector<size_t> leaf(n); // [叶序] 行
  for (size_t i = 0; i < n; ++i)
    leaf[at[i]] = i;
  // 叶序第 k / k + 1 行之间的间隔 = 在 k + 1 处分开左右两子的那个结点的两侧平均 1 − s (各对行只在其最近公共祖先处算一次, 共 O(n²))
  std::vector<double> t(n, 0.0); // [叶序] 累加位置
  for (size_t x = n; x < N; ++x) {
    const size_t l0 = at[x], l1 = l0 + cnt[kids[x - n][0]], l2 = l0 + cnt[x];
    double sum = 0;
    for (size_t p = l0; p < l1; ++p)
      for (size_t q = l1; q < l2; ++q)
        sum += far[leaf[p] * n + leaf[q]];
    t[l1] = sum / static_cast<double>((l1 - l0) * (l2 - l1));
  }
  for (size_t k = 1; k < n; ++k)
    t[k] += t[k - 1];
  const double span = t[n - 1];
  for (size_t i = 0; i < n; ++i)
    rs[i]->hue = span > 0 ? kArc * static_cast<float>(t[at[i]] / span) : 0.0f;
}
// NAV 右表行选中 (缺 = 默认 PortfolioRow::on)
bool shown(const PortfolioUIState &ui, const PortfolioRow &r) {
  const auto it = ui.show.find(r.id);
  return it != ui.show.end() ? it->second : r.on;
}

// src (升序) 各日在 axis (升序) 上的 lower_bound 下标 (双指针)
std::vector<size_t> days_on(const std::vector<misc::Ymd> &axis, const std::vector<misc::Ymd> &src) {
  std::vector<size_t> di(src.size());
  size_t j = 0;
  for (size_t i = 0; i < src.size(); ++i) {
    while (j < axis.size() && axis[j] < src[i])
      ++j;
    di[i] = j;
  }
  return di;
}

// ---- 增量流 (组合 / 单因子同一份) ----
// x 严格升序; d[PairLine] = 该时刻实现的净值增量 (占本金; 有冲击 / 无冲击)
struct Inc {
  std::vector<double> x, d[kPairLines];
  // 追加 at 处的增量 r[kPairLines] (at 不降; 同 x 并入末点)
  void add(double at, const double *r) {
    assert(x.empty() || at >= x.back());
    if (x.empty() || at != x.back()) {
      x.push_back(at);
      for (std::vector<double> &v : d)
        v.push_back(0.0);
    }
    for (int l = 0; l < kPairLines; ++l)
      d[l].back() += r[l];
  }
};
// 两条增量流按 x 归并, 同 x 相加
Inc merge(const Inc &a, const Inc &b) {
  Inc o;
  size_t i = 0, j = 0;
  while (i < a.x.size() || j < b.x.size()) {
    const bool ta = j == b.x.size() || (i < a.x.size() && a.x[i] <= b.x[j]);
    const Inc &s = ta ? a : b;
    const size_t k = ta ? i++ : j++;
    double r[kPairLines];
    for (int l = 0; l < kPairLines; ++l)
      r[l] = s.d[l][k];
    o.add(s.x[k], r);
  }
  return o;
}
// 多条增量流之和: 两两归并 (O(P log n), P = 总点数)
Inc sum_of(std::vector<Inc> v) {
  assert(!v.empty());
  while (v.size() > 1) {
    std::vector<Inc> h;
    for (size_t i = 0; i + 1 < v.size(); i += 2)
      h.push_back(merge(v[i], v[i + 1]));
    if (v.size() % 2)
      h.push_back(std::move(v.back()));
    v = std::move(h);
  }
  return std::move(v.front());
}
// 增量流的线 l → 净值线: 起点 (x0, 1) + 每个 x 一点, 逐点复利. 起点取首日开盘前 kStartDx 格: 首行可在开盘点就有增量 (L_1min 行 0 末分钟 = 开盘)
constexpr double kStartDx = 0.5;
PortfolioCurve curve_of(const Inc &s, int l, double x0) {
  assert(s.x.empty() || s.x.front() > x0);
  PortfolioCurve c;
  c.x.reserve(s.x.size() + 1), c.y.reserve(s.x.size() + 1);
  c.x.push_back(x0), c.y.push_back(1.0);
  compound(s.d[l].data(), s.x.size(), [&](size_t k, double y) { c.x.push_back(s.x[k]), c.y.push_back(y); });
  return c;
}
// 净值线的近况 (Database::RecentOf 同一核): 日 = 每日末个点 (起点算首日前一日的日末), 前收 = 前一日的
Database::Recent recent_of(const PortfolioCurve &c) {
  double e[Database::kMarks]; // 新 → 旧
  size_t n = 0;
  double day = -1.0;
  for (size_t i = c.x.size(); i-- > 0 && n < std::size(e);)
    if (const double di = std::floor(c.x[i] / kDayW); n == 0 || di != day)
      day = di, e[n++] = c.y[i];
  Database::Mark m[std::size(e)];
  for (size_t k = 0; k < n; ++k)
    m[k] = {e[k], k + 1 < n ? e[k + 1] : kNaN};
  return Database::RecentOf({m, n});
}

// 因子 f 的增量流 (di = f 各日在并集轴上的下标): FactorCurve 逐行增量 (fold_book 已实现口径, 与池指数对齐) × wt(实现日并集轴下标), 记在行末分钟
// (日层行末 = 收盘)
template <class Wt>
Inc inc_of(const FactorCurve &f, const std::vector<size_t> &di, double fixed_impact, const Wt &wt) {
  const factor::Geo g = factor_level(f.lv).geo();
  const int S = g.S;
  assert(!f.dates.empty() && f.nav.n == kCurveLines && static_cast<size_t>(f.nav.P) == f.dates.size() * static_cast<size_t>(S));
  const float *pnl = f.nav_at(kCurvePnl), *imp = f.nav_at(kCurveImp), *fix = f.nav_at(kCurveFix);
  const auto row_x = [&](size_t dl, int ts) -> size_t { return x_at(di[dl], static_cast<size_t>(g.minute(ts))); };
  Inc o;
  for (size_t dl = 0; dl < di.size(); ++dl) {
    const double k = wt(di[dl]);
    for (int ts = 0; ts < S; ++ts) {
      const size_t t = dl * S + static_cast<size_t>(ts);
      double r[kPairLines];
      r[kTrade] = (pnl[t] - imp[t] - fix[t] * fixed_impact) * k;
      r[kIdeal] = pnl[t] * k;
      o.add(static_cast<double>(row_x(dl, ts)), r);
    }
  }
  return o;
}

// ---- 篮子投影 (可成交线 = BasketCurve, 理想线 = 价格指数; 同一投影): 源 = 日点位线, 只取日并集轴范围内 = 范围首日开盘一点 (其前一日点位; 源首日无)
//      + 轴上每日收盘一点; 不在轴上的日只差点位, 收益自然并进下一个轴上的日 ----
// 日点位线: dates[i] 收盘点位 lv[i]
struct Levels {
  std::vector<misc::Ymd> dates;
  std::vector<double> lv;
};
// 可成交指数篮子线: 日收益复利 (起点 1)
Levels levels(const BasketCurve &bc, size_t line) {
  Levels s{bc.dates, std::vector<double>(bc.dates.size())};
  compound(bc.index_of(line), s.dates.size(), [&](size_t i, double l) { s.lv[i] = l; });
  return s;
}
// 价格指数篮子线: 日线收盘
Levels levels(const Database::Kline &kl) {
  Levels s;
  for (const Database::Candle &e : kl.daily)
    s.dates.push_back(e.date), s.lv.push_back(e.close);
  return s;
}
// → c (须空); 返回 [c.x] → s.dates 下标 (严格升序)
std::vector<uint32_t> project(const std::vector<misc::Ymd> &axis, const Levels &s, PortfolioCurve &c) {
  assert(c.x.empty() && !axis.empty());
  std::vector<uint32_t> at;
  const std::vector<size_t> di = days_on(axis, s.dates);
  const auto push = [&](double x, size_t i) { c.x.push_back(x), c.y.push_back(s.lv[i]), at.push_back(static_cast<uint32_t>(i)); };
  for (size_t i = 0; i < s.dates.size(); ++i) {
    if (s.dates[i] < axis.front() || s.dates[i] > axis.back())
      continue;
    if (at.empty() && i > 0)
      push(x_of(di[i], kOpen), i - 1);
    if (axis[di[i]] == s.dates[i])
      push(x_of(di[i], kClose), i);
  }
  return at;
}

// 组合 + 单因子 (见头文件): 每因子两条增量流 — 自身 (权 1) → 单因子线; 份额 (权 1 / 当日因子数) → Σ → 组合线. 单因子线 ≡ 只含它的组合.
// o = 上一份派生: 同一条 FactorCurve 且日轴 / fixed_impact 不变 → 单因子线沿用
void derive_factors(const std::vector<PortfolioItem> &items, const std::string &name, const PortfolioResult &res, double fixed_impact,
                    PortfolioDerived &o, PortfolioDerived &d) {
  std::vector<const PortfolioItem *> its;
  std::vector<std::shared_ptr<const FactorCurve>> &fc = d.src;
  for (const PortfolioItem &it : items) {
    if (!it.missing.empty())
      continue;
    const auto f = res.factors.find(it.e.key());
    if (f == res.factors.end()) {
      ++d.n_wait;
      continue;
    }
    if (!f->second->error.empty()) {
      ++d.n_error;
      continue;
    }
    ++d.n_ready;
    its.push_back(&it);
    fc.push_back(f->second);
  }
  for (const std::shared_ptr<const FactorCurve> &f : fc)
    d.dates.insert(d.dates.end(), f->dates.begin(), f->dates.end());
  std::sort(d.dates.begin(), d.dates.end());
  d.dates.erase(std::unique(d.dates.begin(), d.dates.end()), d.dates.end());
  d.impact = fixed_impact;
  std::map<const FactorCurve *, size_t> keep; // 可沿用的: 源 → o 的因子行
  if (d.dates == o.dates && fixed_impact == o.impact)
    for (size_t q = 0; q < o.src.size(); ++q)
      keep[o.src[q].get()] = q;
  std::vector<std::vector<size_t>> dix(fc.size()); // [因子][该因子日] → 并集轴下标
  for (size_t q = 0; q < fc.size(); ++q)
    dix[q] = days_on(d.dates, fc[q]->dates);
  std::vector<int> n_act(d.dates.size(), 0); // [并集轴] 当日有数据的因子数
  for (const std::vector<size_t> &di : dix)
    for (const size_t i : di)
      ++n_act[i];

  std::vector<Inc> shares; // [因子] 份额流
  double strat_buy = 0.0;  // Σ 份额 · 增仓量 (组合年换手)
  for (size_t q = 0; q < fc.size(); ++q) {
    const FactorCurve &f = *fc[q];
    const std::vector<size_t> &di = dix[q];
    const size_t S = static_cast<size_t>(factor_level(f.lv).geo().S);
    const float *buy = f.nav_at(kCurveBuy);
    double own_buy = 0.0;
    for (size_t dl = 0; dl < di.size(); ++dl)
      for (size_t ts = 0; ts < S; ++ts) {
        own_buy += buy[dl * S + ts];
        strat_buy += buy[dl * S + ts] / n_act[di[dl]];
      }
    const PortfolioItem &it = *its[q];
    const auto kp = keep.find(&f);
    PortfolioRow r;
    r.id = it.e.key();
    r.en = std::filesystem::path(it.e.file).stem().string();
    r.cn = clip_name(it.name_cn);
    r.tag = it.name_en + " " + factor::stat::hold_name(f.hold);
    r.hover = r.en + "  " + it.name_cn + "\n• " + it.e.pool + " " + factor_level(it.e.lv).name + "\n• " + hold_desc(it.e.hold, it.e.lv) +
              "\n• 冲击 " + amt_name(f.amt) + "\n• 规范串 " + it.e.expr + (it.e.sign < 0 ? " · 取反" : "");
    r.turn = own_buy / static_cast<double>(f.dates.size()) * factor::stat::kDaysPerYear;
    if (kp != keep.end()) {
      for (int l = 0; l < kPairLines; ++l)
        r.line[l] = std::move(o.nav[kNavFactor][kp->second].line[l]);
    } else {
      const Inc own = inc_of(f, di, fixed_impact, [](size_t) { return 1.0; });
      for (int l = 0; l < kPairLines; ++l)
        r.line[l] = curve_of(own, l, x_of(di.front(), kOpen) - kStartDx);
    }
    d.nav[kNavFactor].push_back(std::move(r));
    shares.push_back(inc_of(f, di, fixed_impact, [&](size_t i) { return 1.0 / n_act[i]; }));
  }
  const Inc sum = shares.empty() ? Inc{} : sum_of(std::move(shares));
  PortfolioRow s;
  s.id = "pf_strat";
  s.en = name, s.cn = "G20 多头";
  s.tag = s.hover = name;
  if (!d.dates.empty()) {
    s.turn = strat_buy / static_cast<double>(d.dates.size()) * factor::stat::kDaysPerYear;
    for (int l = 0; l < kPairLines; ++l)
      s.line[l] = curve_of(sum, l, x_of(0, kOpen) - kStartDx);
  }
  d.nav[kNavStrat].push_back(std::move(s));
}

// 篮子行 (见头文件): 指数 / 行业 / 概念三组 = basket_group 三组, 一行两条 = BasketCurve 可成交指数 / 价格指数 (prices) 的同一篮子线, 同一 project.
// o = 上一份派生: 同一份 BasketCurve、同一份价格指数且日轴不变 → 整份沿用
void derive_baskets(const PortfolioResult &res, const std::shared_ptr<const Database::Tape> &prices, PortfolioDerived &o, PortfolioDerived &d) {
  d.baskets = res.baskets && res.baskets->error.empty() ? res.baskets : nullptr;
  d.prices = prices && !prices->list.empty() ? prices : nullptr;
  if (o.basket_gen > 0 && d.baskets == o.baskets && d.prices == o.prices && d.dates == o.dates) {
    for (int g = 0; g < kBasketGroups; ++g)
      d.nav[g] = std::move(o.nav[g]);
    d.basket_gen = o.basket_gen;
    return;
  }
  d.basket_gen = o.basket_gen + 1;
  for (int g = 0; g < kBasketGroups; ++g)
    for (const size_t line : basket_group(static_cast<BasketGroup>(g))) {
      PortfolioRow r;
      r.id = basket_id(line);
      r.en = cut_code(cut_of_line(line));
      r.hover = r.en + "  " + basket_name(line);
      r.cn = r.tag = clip_name(cut_name(cut_of_line(line))) + weight_suffix(weight_of(line));
      r.on = line == basket_line(0, kWeightEq);
      r.basket = static_cast<int>(line);
      if (d.baskets && !d.dates.empty()) {
        r.turn = d.baskets->turn[line];
        r.at = project(d.dates, levels(*d.baskets, line), r.line[kTrade]);
      }
      if (d.prices && !d.dates.empty()) {
        assert(d.prices->keys[line] == line);
        project(d.dates, levels(d.prices->list[line]), r.line[kIdeal]);
      }
      d.nav[g].push_back(std::move(r));
    }
}

void derive(const std::vector<PortfolioItem> &items, const std::string &name, const PortfolioResult &res,
            const std::shared_ptr<const Database::Tape> &prices, double fixed_impact, PortfolioDerived &d) {
  PortfolioDerived o = std::move(d);
  d = PortfolioDerived{};
  derive_factors(items, name, res, fixed_impact, o, d);
  derive_baskets(res, prices, o, d);
  paint(d);
}

// 曲线 c 在 [x0, x1) 的切片 (见 PortfolioSlice); 整条在区间外 → 空. 年化 = 末点净值按首末点所跨交易日数复利折年 (%);
// 夏普 = 同一段交易日逐日收益 (日末点净值 / 上日末点 − 1, 首日对首点) 的 mean / sd (ddof = 1) × √每年交易日
PortfolioSlice slice_of(const PortfolioCurve &c, double x0, double x1) {
  PortfolioSlice s;
  s.c = &c, s.x0 = x0;
  const size_t i0 = static_cast<size_t>(std::lower_bound(c.x.begin(), c.x.end(), x0) - c.x.begin());
  const size_t i1 = static_cast<size_t>(std::lower_bound(c.x.begin(), c.x.end(), x1) - c.x.begin());
  if (i0 == c.x.size() || i1 == 0)
    return s;
  s.j0 = i0 > 0 ? i0 - 1 : 0, s.j1 = i1;
  s.base = c.y[s.j0];
  if (s.size() < 2)
    return s;
  const size_t e = s.size() - 1;
  const double days = std::floor(s.x(e) / kDayW) - std::floor(s.x(0) / kDayW) + 1.0;
  s.ann = static_cast<float>(100.0 * factor::stat::ann_geo(s.y(e), days));
  double n = 0.0, s1 = 0.0, s2 = 0.0, prev = s.y(0), last = s.y(0);
  const auto close_day = [&] {
    const double r = last / prev - 1.0;
    n += 1.0, s1 += r, s2 += r * r, prev = last;
  };
  double day = std::floor(s.x(0) / kDayW);
  for (size_t i = 1; i <= e; ++i) {
    for (const double di = std::floor(s.x(i) / kDayW); day < di; ++day)
      close_day();
    last = s.y(i);
  }
  close_day();
  assert(n == days);
  const double var = (s2 - s1 * s1 / n) / (n - 1.0);
  if (n >= 2.0 && var > 0.0)
    s.sharpe = static_cast<float>(s1 / n / std::sqrt(var) * std::sqrt(static_cast<double>(factor::stat::kDaysPerYear)));
  return s;
}
// ---- 估值口径 (roll_of / last_pct 同一份) ----
// 估值 (EP / BP / SP / CP / DY 收益率) 可算: 有限 (≤ 0 照排, 在最贵端)
bool val_ok(double r) { return std::isfinite(r); }
// 收益率 ↔ 贵度 (取负; 自逆, 贵度升 = 贵): 分位 / 排序一律在贵度上
template <class F>
F dear_of(F x) {
  return -x;
}
// 自然年平移: 滚动窗 = (years_before(日, N), 日]; 史满 1 年 = 首个可算日 ≤ years_before(日, 1)
constexpr misc::Ymd years_before(misc::Ymd day, int years) { return day - years * 10000; }
// 估值分位 = 窗内贵度 ≤ 当日的权重和 le / 窗内可算点权重和 w (%, 高 = 贵)
float pct_of(double le, double w) { return static_cast<float>(100.0 * le / w); }
// 窗内权重 (指数饱和): u = 1 − a / T, a = 距当日的日历天数, T = 当日 − max(N 年前, 首个可算日) (史不足 N 年 → 按实际史长缩放);
// 窗内前 ~3/4 ≈ 等权, 窗尾段快速降到 0 (点出窗时权重已 ≈ 0, 分位不跳)
constexpr double kRollK = 10.0;
double roll_w(int a, int T) { return 1.0 - std::exp(-kRollK * (1.0 - static_cast<double>(a) / T)); }
int days_of(misc::Ymd d) { return static_cast<int>(misc::to_days(d).time_since_epoch().count()); }
// 当日 day 的窗跨度 T (日历天; first = 首个可算日)
int span_of(misc::Ymd day, misc::Ymd first, int years) { return days_of(day) - days_of(std::max(years_before(day, years), first)); }

// 篮子行 s 的口 b 的滚动分位 → o [s.at]: 窗内可算点按贵度保持有序 (逐日二分插入 / 出窗删除);
// 出点日扫两遍窗: 权重和 → 加权分位 = 累计权重首个 ≥ q · 总权重的点 + 当日加权估值分位. 权重按距今天数查表 (T 变才重建)
void roll_of(const BasketCurve &bc, const PortfolioRow &s, int b, int years, PortfolioRoll &o) {
  constexpr float kNaN = std::numeric_limits<float>::quiet_NaN();
  const std::vector<misc::Ymd> &hd = bc.dates;
  const size_t n = hd.size(), np = s.at.size();
  const float *v = bc.val_of(static_cast<size_t>(s.basket), b);
  std::vector<int> dd(n); // 日历天
  for (size_t i = 0; i < n; ++i)
    dd[i] = days_of(hd[i]);
  const auto by_dear = [&](size_t x, size_t y) { return dear_of(v[x]) < dear_of(v[y]); };
  std::vector<size_t> win; // 窗内可算点 (hd 下标), 贵度升序
  std::vector<double> wt;  // wt[a] = roll_w(a, T)
  int wt_T = -1;
  for (std::vector<float> &q : o.q)
    q.assign(np, kNaN);
  o.pct.assign(np, kNaN);
  o.band.assign(np, 0);
  misc::Ymd first = 0; // 首个可算日
  size_t lo = 0, k = 0;
  int wp = 0; // 窗内收益率 > 0 的个数
  for (size_t t = 0; t < n && k < np; ++t) {
    if (val_ok(v[t])) {
      win.insert(std::upper_bound(win.begin(), win.end(), t, by_dear), t), wp += v[t] > 0.f;
      if (first == 0)
        first = hd[t];
    }
    for (; hd[lo] <= years_before(hd[t], years); ++lo)
      if (val_ok(v[lo])) {
        const auto [e0, e1] = std::equal_range(win.begin(), win.end(), lo, by_dear);
        const auto e = std::find(e0, e1, lo);
        assert(e != e1);
        win.erase(e), wp -= v[lo] > 0.f;
      }
    if (s.at[k] != t)
      continue;
    const size_t i = k++;
    if (win.empty() || first == 0 || first > years_before(hd[t], 1))
      continue;
    const int T = span_of(hd[t], first, years);
    assert(T > 0);
    if (T != wt_T) {
      wt.resize(static_cast<size_t>(T) + 1);
      for (int a = 0; a <= T; ++a)
        wt[static_cast<size_t>(a)] = roll_w(a, T);
      wt_T = T;
    }
    const auto w_of = [&](size_t u) {
      assert(dd[t] - dd[u] <= T);
      return wt[static_cast<size_t>(dd[t] - dd[u])];
    };
    double W = 0.0;
    for (const size_t u : win)
      W += w_of(u);
    double cum = 0.0, le = 0.0;
    int j = 0;
    for (const size_t u : win) {
      const double x = w_of(u);
      cum += x;
      for (; j < kBandQ && cum >= kBandQs[j] * W; ++j)
        o.q[j][i] = v[u];
      if (val_ok(v[t]) && dear_of(v[u]) <= dear_of(v[t]))
        le += x;
    }
    assert(j == kBandQ);
    o.band[i] = wp == static_cast<int>(win.size());
    if (val_ok(v[t]))
      o.pct[i] = pct_of(le, W);
  }
  assert(k == np && "at 须升序且落在 hd 内");
}

// 篮子行 s 的口 b 在切片 sl 内末个可算点的估值分位 (NaN = 无): 与 roll_of 的 pct 同一定义, 只一点 → 直接数窗, O(窗)
double last_pct(const BasketCurve &bc, const PortfolioRow &s, const PortfolioSlice &sl, int b, int years) {
  const std::vector<misc::Ymd> &hd = bc.dates;
  const float *v = bc.val_of(static_cast<size_t>(s.basket), b);
  size_t f = 0; // 首个可算日 (无 → hd.size(), 下面 val_ok 先短路)
  while (f < hd.size() && !val_ok(v[f]))
    ++f;
  for (size_t j = sl.j1; j > sl.j0; --j) {
    const size_t t = s.at[j - 1];
    if (!val_ok(v[t]) || hd[f] > years_before(hd[t], 1))
      continue;
    const float m = dear_of(v[t]);
    const int T = span_of(hd[t], hd[f], years), d = days_of(hd[t]);
    double w = 0.0, le = 0.0;
    for (size_t u = static_cast<size_t>(std::upper_bound(hd.begin(), hd.begin() + t, years_before(hd[t], years)) - hd.begin()); u <= t; ++u)
      if (val_ok(v[u])) {
        const double x = roll_w(d - days_of(hd[u]), T);
        w += x, le += dear_of(v[u]) <= m ? x : 0.0;
      }
    return pct_of(le, w);
  }
  return std::numeric_limits<double>::quiet_NaN();
}

// 截面名次 (VAL 格 / NAV 数值列同一份): x(i) (i < n) 在有限值里的名次 ∈ [0, 1] (0 = 最小, 并列取中; 只一个 = 0.5) → put(i, r); 不可算的不排
template <class X, class Put>
void ranks(size_t n, const X &x, const Put &put) {
  for (size_t i = 0; i < n; ++i) {
    const double p = x(i);
    if (!std::isfinite(p))
      continue;
    int m = 0, lt = 0, eq = 0;
    for (size_t j = 0; j < n; ++j)
      if (const double q = x(j); std::isfinite(q))
        ++m, lt += q < p, eq += q == p;
    put(i, m > 1 ? (lt + 0.5 * (eq - 1)) / (m - 1) : 0.5);
  }
}
// 切片的区间年化 (%); 不足两点 → NaN
double ann_of(const PortfolioSlice &s) { return s.size() >= 2 ? static_cast<double>(s.ann) : std::numeric_limits<double>::quiet_NaN(); }
// NAV 右表组 g 行 r 的数值列 k (NavNum; NaN = 不可算)
double nav_num(const PortfolioDerived &d, const PortfolioView &v, int g, size_t r, int k) {
  switch (k) {
  case kNumTrade:
  case kNumIdeal:
    return ann_of(v.nav[g][r][k]);
  case kNumSharpe:
    return v.nav[g][r][kTrade].sharpe;
  default:
    return d.nav[g][r].turn;
  }
}

// 区间视图 (见 PortfolioView). vs = VAL 选中篮子行, roll = 它的滚动分位, years = 窗长
void make_view(const PortfolioDerived &d, int d0, int d1, PortfolioAt vs, int years, const std::array<PortfolioRoll, kPoolVals> &roll, PortfolioView &v) {
  const double x0 = x_of(static_cast<size_t>(d0), kOpen), x1 = x_of(static_cast<size_t>(d1), kOpen);
  // ---- 各线同一份切片; NAV 右表: 各数值列在同组内的截面名次 (年换手取负: 低 = 好) ----
  std::array<double, kNavNums> no_rank;
  no_rank.fill(kNaN);
  for (int g = 0; g < kNavGroups; ++g) {
    const size_t n = d.nav[g].size();
    v.nav[g].clear();
    for (const PortfolioRow &r : d.nav[g])
      v.nav[g].push_back({slice_of(r.line[kTrade], x0, x1), slice_of(r.line[kIdeal], x0, x1)});
    v.nav_rank[g].assign(n, no_rank);
    for (int k = 0; k < kNavNums; ++k) {
      const double s = k == kNumTurn ? -1.0 : 1.0;
      ranks(n, [&](size_t i) { return s * nav_num(d, v, g, i, k); }, [&](size_t i, double r) { v.nav_rank[g][i][k] = r; });
    }
  }

  // ---- VAL 表格: 各篮子行各口切片内末个可算点的估值分位 + 同组同口内的截面名次 ----
  for (int g = 0; g < kBasketGroups; ++g) {
    const size_t n = d.nav[g].size();
    v.cell[g].assign(n, {});
    if (d.baskets)
      for (size_t i = 0; i < n; ++i)
        for (int b = 0; b < kPoolVals; ++b)
          v.cell[g][i][b].pct = last_pct(*d.baskets, d.nav[g][i], v.nav[g][i][kTrade], b, years);
    for (int b = 0; b < kPoolVals; ++b)
      ranks(n, [&](size_t i) { return v.cell[g][i][b].pct; }, [&](size_t i, double r) { v.cell[g][i][b].rank = r; });
  }

  // ---- VAL 选中篮子各口 band: 与价格线同一切片, y = P · v / q (v = 当日收益率; v ≤ 0 / q ≤ 0 → 价格无解, 断开; 窗内有任一无解点 → 不画);
  //      等值线单位点位 unit = P · v (只看当日, 不看滚动分位; v ≤ 0 → 断开), 档 = 篮子全史倍数区间等分 (levels_of) ----
  v.band_x.clear();
  for (PortfolioBand &bd : v.band)
    bd = {};
  if (vs.g < 0 || !d.baskets)
    return;
  const BasketCurve &bc = *d.baskets;
  const PortfolioRow &s = d.nav[vs.g][vs.r];
  const PortfolioSlice &sl = v.nav[vs.g][vs.r][kTrade];
  const size_t n = sl.size();
  for (size_t i = 0; i < n; ++i)
    v.band_x.push_back(sl.x(i));
  for (int b = 0; b < kPoolVals; ++b) {
    const PortfolioRoll &r = roll[b];
    assert(r.pct.size() == s.at.size());
    PortfolioBand &bd = v.band[b];
    const float *hv = bc.val_of(static_cast<size_t>(s.basket), b);
    for (std::vector<double> &y : bd.y)
      y.assign(n, kNaN);
    bd.unit.assign(n, kNaN);
    bd.levels = levels_of(hv, bc.dates.size());
    size_t last = SIZE_MAX; // 末个有分位的点 (源下标)
    for (size_t i = 0; i < n; ++i) {
      const size_t j = sl.j0 + i;
      const double x = hv[s.at[j]];
      if (x > 0.0)
        bd.unit[i] = sl.y(i) * x;
      if (!std::isfinite(r.pct[j]))
        continue;
      last = j;
      if (!(x > 0.0) || !r.band[j])
        continue;
      for (int q = 0; q < kBandQ; ++q)
        if (r.q[q][j] > 0.f)
          bd.y[q][i] = sl.y(i) * x / r.q[q][j];
      if (!(r.q[kBandQ - 1][j] > 0.f)) // q 降序: 最贵档 > 0 ⇔ 各档全可画 (填色只画全有限段)
        continue;
      const int k = static_cast<int>(i);
      if (bd.segs.empty() || bd.segs.back().second != k)
        bd.segs.push_back({k, k + 1});
      else
        ++bd.segs.back().second;
    }
    if (last == SIZE_MAX)
      continue;
    bd.ok = true;
    bd.now = hv[s.at[last]], bd.pct = r.pct[last];
  }
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
// 刻度: 真 y 处短横贴点右侧 (同线尾小字, 出界则左移进图内), 字左对齐接在短横后, 按像素 y 排开不重叠
void draw_marks(std::vector<EndTag> &marks) {
  if (marks.empty())
    return;
  std::sort(marks.begin(), marks.end(), [](const EndTag &a, const EndTag &b) { return a.at.y < b.at.y; });
  ImFont *font = ImGui::GetFont();
  const float fs = ImGui::GetFontSize() * kTagScale;
  const float right = ImPlot::GetPlotPos().x + ImPlot::GetPlotSize().x;
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

// 主图一帧的公共件 (NAV / VAL 同一份): 小字筛选视野 + 线尾小字 + 线尾刻度 (VAL) + 显隐签名 (下帧进 fit 键)
struct PlotFrame {
  ImPlotRect lim;
  std::vector<EndTag> tags;
  std::vector<EndTag> marks;
  std::string shown;
  // 图例项 label 的显隐记进 shown; 显示 → 该项 (Color = 实际线色), 否则 nullptr
  const ImPlotItem *item(const char *label) {
    const ImPlotItem *it = ImPlot::GetItem(label);
    assert(it);
    shown += it->Show ? '1' : '0';
    return it->Show ? it : nullptr;
  }
};

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
    ui.vis_key.clear(); // NAV 行选中 + 主图例显隐回默认 (见 RenderTabPortfolio)
    ui.val_id.clear();
    ui.range = PortfolioRange{.d0 = 0, .d1 = static_cast<int>(d.dates.size())};
    ui.log_y = false;
  }
  if (ImGui::IsItemHovered())
    tip("清掉全部选择, 图回默认: NAV 行选中 (回只选全A.等) + 主图例显隐 / VAL 选中篮子 / 区间 (回全段) / log 关");
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
    const bool nav_open = ImGui::BeginTabItem("NAV");
    if (ImGui::IsItemHovered())
      tip("NAV = 资金曲线 (Net Asset Value, 净值)\n"
          "• 橙线 = 本组合 G20 多头: 各因子独立回测 (分批 1/h̄, 全日平均满仓; 能成交才成交) 的逐行增量等权聚合\n"
          "• 池指数线 = 各池等权指数 (all 池拆 全A.等 / 全A.加); 行业线 = 申万一级 / 概念线 = 东财题材概念 (全A 内切, 总市值加权)\n"
          "• 单因子线 = 各自的 G20 多头净值\n"
          "• 每条成对: 可成交 (篮子 = 标签指数, 收盘封板不建仓 / 平仓顺延; 因子 / 策略 = 扣换手冲击) + 理想 (篮子 = 后复权收盘价指数; 因子 / 策略 = 无冲击; 同色淡线)\n"
          "• 默认只画全A.等; 右侧类别条选一组 (指数 / 行业 / 概念 / 因子 / 策略), 点行选中 = 画该行两条, 主图例默认只显理想, 可成交点图例显; 显隐变了自动重新 fit\n"
          "• 日内 (右侧开关, 默认开): 主图上方画选中的指数 / 行业 / 概念的日内行情 (多行 = 同主图线色叠加; 一行 = 蜡烛 + 现价十字)\n"
          "• 底部区间条选日段: 各线在区间起点归一 = 1, 年化按区间复利");
    if (nav_open)
      ImGui::EndTabItem();
    const bool val_open = ImGui::BeginTabItem("VAL");
    if (ImGui::IsItemHovered())
      tip("VAL = 估值 (Valuation)\n"
          "• 右侧单选一个池 / 申万一级行业 / 概念: 主图 = 它的指数 + 估值 Band (全A.加 / 行业 / 概念 = 总市值加权, 全A.等 / 其余池 = 等权)\n"
          "• EP 盈利收益率 (1/PE) / BP 账面市值比 (1/PB) / SP 营收市值比 (1/PS) / CP 现金流收益率 (1/PCF) / DY 股息率\n"
          "  (收益率口径, 与指数同权重: 加权 = 整体法, 等权 = 个股均值; 高 = 便宜, 亏损照计为负, 过 0 连续)\n"
          "• 主图左上图例矩阵: 行 = 口, 列 = 当前收益率 / 倍数 / N 年分位 / Band / 等值线; 点色块开关, 点行头 / 列头开关整行 / 整列\n"
          "• Band = 把过去 N 年的估值分位换算成价格线: 指数落在哪档 = 当前估值在历史里的位置 (绿 = 便宜, 红 = 贵)\n"
          "• 等值线 = 绝对估值: 倍数 (PE / PB / PS / PCF / P/D = 1 / 收益率) 的价格线, 档 = 该篮子全史倍数区间 (两头各剔 2%% 极端) 20 档等分;\n"
          "  默认关, 悬停一条 = 高亮 + 看该档倍数 / 当日倍数 / 指数到该线还差多少\n"
          "• 线尾刻度 = 指数末点 +10%% / -5%% / -10%% / -20%% / -30%% 的点位: 对照 Band 线看还要涨跌多少到哪档\n"
          "• 区间 / log 与 NAV 页共用");
    if (val_open) {
      page = kPageVal;
      ImGui::EndTabItem();
    }
    const bool flow_open = ImGui::BeginTabItem("FLOW");
    if (ImGui::IsItemHovered())
      tip("FLOW = 资金流 (Money Flow): 指数 / 行业 / 概念的日内净流入, 当日随新浪快照实时动\n"
          "• 净流入 = Σ 成分 Σ 分钟 ± 该分钟额 (分钟涨跌法): 收 > 上一分钟收 (首分钟对昨收) 记 +, < 记 −, 平随上一分钟; 成分之和不加权, 分不出大单\n"
          "• 左图 = 右表选中行 (与 NAV 页同一份选中) 自日轴首日开盘起逐分钟累计的净流入, 跨日接续; 档位 日内 / 2日 … 5日 叠在图左上角\n"
          "• y 口径 (图左上角切): 占比 = 累计净流入 / 近 5 日日均成交额 (默认; 分母盘前已定, 开盘即稳, 跨篮子可比) / 金额 = 累计净流入 (亿元)\n"
          "• 开收盘 (图左上角开关, 默认关): 计入首根 09:31 (开盘集合竞价并入) / 末根 15:00 (收盘集合竞价); 关 = 两根的额与净流入都不计 (图与右表同一开关)\n"
          "• 右表: 日轴各日的净流入占当日成交额 + 档内合计; 过滤条与 NAV / VAL 页同一份\n"
          "• 历史日 = all.bar 库 L_1min (其后至基本面末日 = bar1m) 合成, 当日 = 新浪快照合成; 不看区间条");
    if (flow_open) {
      page = kPageFlow;
      ImGui::EndTabItem();
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
      if (shown(ui, d.nav[g][r]))
        for (int l = 0; l < kPairLines; ++l)
          ds.push_back({g, l, &d.nav[g][r], &v.nav[g][r][l], nav_color(d, g, r, l)});
  std::vector<std::array<std::string, 5>> rows;
  for (const Draw &x : ds) {
    char ann[48], shp[48] = "", turn[48] = "";
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
    if (pf.item(lb.c_str()))
      push_tag(pf.tags, pf.lim, *x.s, ImGui::ColorConvertFloat4ToU32(x.col), x.row->tag + kPairTag[pair_kind(x.g)][x.l]);
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
  static const PortfolioCurve kNone;
  const PortfolioCurve &rc = nav ? d.nav[kNavStrat][0].line[kTrade] : vs.g >= 0 ? d.nav[vs.g][vs.r].line[kTrade]
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

// 右图例表格收尾 (NAV / VAL 同一份): 返回列不截断所需的宽 = ColumnsAutoFitWidth (EndTable 按本帧内容更新; 列不可拖 → 纯内容宽) + 竖滚动条
float end_table() {
  const ImGuiTable *t = ImGui::GetCurrentTable();
  assert(t);
  ImGui::EndTable();
  return t->ColumnsAutoFitWidth + ImGui::GetStyle().ScrollbarSize;
}

// 右图例表头排序 (NAV / VAL 同一份; 三态: 升 → 降 → 原序): order (rs 的组内下标) 按当前排序列重排;
// 首列 (中文名) 按色相 (相似的挨着; 未定色 = 按 cn 字典序), 其余按 num(i, 列), 不可算的恒在末
template <class Num>
void sort_rows(std::vector<size_t> &order, const std::vector<PortfolioRow> &rs, const Num &num) {
  const ImGuiTableSortSpecs *ss = ImGui::TableGetSortSpecs();
  if (!ss || ss->SpecsCount == 0)
    return;
  const int c = ss->Specs[0].ColumnIndex;
  const bool desc = ss->Specs[0].SortDirection == ImGuiSortDirection_Descending;
  std::stable_sort(order.begin(), order.end(), [&](size_t a, size_t b) {
    if (c == 0) {
      const float ha = rs[a].hue, hb = rs[b].hue;
      if (!std::isnan(ha) && !std::isnan(hb))
        return desc ? ha > hb : ha < hb;
      const int r = rs[a].cn.compare(rs[b].cn);
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
// 表头行 (同 TableHeadersRow) + 悬停第 c 列表头 → tips[c]. 各列说明同一格式: "列名 = 全称" + 算法 / 数据 / 显示 / 适用 四行
void headers_row(const std::string *tips, int n) {
  assert(n == ImGui::TableGetColumnCount());
  ImGui::TableNextRow(ImGuiTableRowFlags_Headers);
  for (int c = 0; c < n; ++c) {
    if (!ImGui::TableSetColumnIndex(c))
      continue;
    ImGui::PushID(c);
    ImGui::TableHeader(ImGui::TableGetColumnName(c));
    ImGui::PopID();
    if (ImGui::IsItemHovered())
      tip("%s", tips[c].c_str());
  }
}

// ---- NAV 右图例: 类别条 (指数 / 行业 / 概念 / 因子 / 策略, 一次看一组) + 日内开关; 表 = 该组一行一对线, 点行 = 选中 / 取消 (选中 = 主图画该行全部线, 单条在主图例里再点掉;
//      选中跨组保留; 选中的篮子行另叠进日内栏). 列 = 中文名 (线色; 悬停看 hover) | 左段 (当日涨幅 | Δ5日线 | 只数 | 总市值 | 换手 | 资金流入)
//      | 可成交 | 理想 (区间年化) | 夏普 (可成交列) | 年换手; 当日涨幅 / Δ5日线 / 资金流入按正负着色, 只数 / 总市值 / 换手不着色, 其余数值列底色 = 同组同列截面名次
//      (好绿 → 差红); 换手 / 资金流入格悬停看分子分母金额; 表头点排序 (组内) ----
// 篮子当日四列 (三页右表同一份, 不随区间; 值 = today_vals(basket_today)): 只数 / 总市值 / 换手不着色, 流入按正负着色; 换手 / 流入格悬停看分子分母金额
enum TodayCol : int { kTodayMembers,
                      kTodayCap,
                      kTodayTurn,
                      kTodayFlow };
constexpr int kTodays = 4;
constexpr const char *kTodayName[kTodays] = {"只数", "总市值", "换手", "流入"};
const std::string kTodayTip[kTodays] = {
    "只数 = 成分只数\n"
    "• 算法: 成员表的成分计数 (.等 / .加 同一份)\n"
    "• 数据: 成员表 = 历史末日收盘定 (= 当日 bind 给新浪行情的那份)\n"
    "• 显示: 整数, 不着色\n"
    "• 适用: 指数 / 行业 / 概念; 不随区间",
    "总市值 = 成分总市值之和\n"
    "• 算法: Σ 成分 末日收盘 × 总股本; 当日接上再 × 加权线当日涨跌 (今收 / 前收)\n"
    "• 数据: 成员表同只数; 总股本取末日适用\n"
    "• 显示: 亿 / 万亿, 不着色\n"
    "• 适用: 指数 / 行业 / 概念; 不随区间",
    "换手 = 当日换手率\n"
    "• 算法: 成交额 / 总市值; 成交额 = Σ 成分 当日成交额, 总市值同上列 (总股本口径, 非流通)\n"
    "• 数据: 新浪行情日 (盘后 / 重启照样全天); 无新浪行情日 → -\n"
    "• 显示: 百分比两位, 不着色; 悬停格看成交额 / 总市值\n"
    "• 适用: 指数 / 行业 / 概念; 不随区间",
    "流入 = 当日资金流入占成交额\n"
    "• 算法: 净流入 / 成交额; 净流入 = Σ 成分 Σ 分钟 ± 该分钟额 (分钟涨跌法): 收 > 上一分钟收 (首分钟对昨收) 记 +, < 记 −, 平随上一分钟\n"
    "• 数据: 新浪行情日分钟根 (快照合成 + 分钟 K 补), 含开收盘集合竞价; 成分之和不加权, 分不出大单\n"
    "• 显示: 百分比两位, 按正负着色; 悬停格看净流入 / 成交额\n"
    "• 适用: 指数 / 行业 / 概念; 不随区间",
};
// NAV 左段列 (不随区间): 近况 (Database::Recent) + 篮子当日四列
enum NavLeft : int { kLeftChg,
                     kLeftMa5 };
constexpr int kRecents = 2, kLefts = kRecents + kTodays;
constexpr const char *kRecentName[kRecents] = {"涨幅", "Δ5日线"};
constexpr const char *kNavNumName[kNavNums] = {"可成交", "理想", "夏普", "年换手"};     // [NavNum]
constexpr const char *kNavNumFmt[kNavNums] = {"%+.1f%%", "%+.1f%%", "%+.2f", "%.1f倍"}; // [NavNum]
constexpr int kNavCols = kLefts + kNavNums;                                             // 名之后的数值列 (左段 + NavNum)
// [列] 表头悬停说明 (名 + kRecentName + kTodayName + kNavNumName)
const std::string kNavTip[1 + kNavCols] = {
    "名 = 行名\n"
    "• 算法: 篮子 = 切片名 + .等 (等权) / .加 (总市值加权); 因子 / 策略 = 中文名\n"
    "• 数据: 切片 = 全A / 池 / 申万一级行业 / 东财题材概念\n"
    "• 显示: 字色 = 主图线色 (四组一起按 max(成员重合, 剔全A.加的残差相关) 定色相: 色差 ∝ 不相似; 没算完全灰); 点本列表头 = 按颜色排 (相似的挨着; 没算完按名); 超长截断补 …, 悬停看全名\n"
    "• 适用: 全部组",
    "涨幅 = 当日涨跌幅\n"
    "• 算法: 最新一日收盘 / 前收 − 1\n"
    "• 数据: 篮子 = Assets 篮子 (当日接上 = 新浪快照合成, 否则 all.bar 末日); 因子 / 策略 = 净值末点\n"
    "• 显示: 百分比两位, 按正负着色\n"
    "• 适用: 全部组; 不随区间",
    "Δ5日线 = 对 5 日均线的偏离\n"
    "• 算法: 最新一日收盘 / MA5 (含当日) − 1\n"
    "• 数据: 篮子 = Assets 篮子 (当日接上 = 新浪快照合成, 否则 all.bar 末日); 因子 / 策略 = 净值末 5 点\n"
    "• 显示: 百分比两位, 按正负着色\n"
    "• 适用: 全部组; 不随区间",
    kTodayTip[kTodayMembers],
    kTodayTip[kTodayCap],
    kTodayTip[kTodayTurn],
    kTodayTip[kTodayFlow],
    "可成交 = 可成交线区间年化\n"
    "• 算法: 区间起点归一, 按区间复利年化\n"
    "• 数据: 篮子 = 标签指数 (收盘封板不建仓 / 平仓顺延); 因子 / 策略 = 有冲击净值\n"
    "• 显示: 百分比一位, 底色 = 组内名次 (高绿 → 低红)\n"
    "• 适用: 全部组; 随区间",
    "理想 = 理想线区间年化\n"
    "• 算法: 区间起点归一, 按区间复利年化\n"
    "• 数据: 篮子 = 价格指数 (成员 / 权重取前一日收盘, 不看可成交); 因子 / 策略 = 无冲击净值\n"
    "• 显示: 百分比一位, 底色 = 组内名次 (高绿 → 低红)\n"
    "• 适用: 全部组; 随区间",
    "夏普 = 可成交线区间夏普\n"
    "• 算法: 区间内日收益 mean / sd × √每年交易日\n"
    "• 数据: 篮子 = 标签指数; 因子 / 策略 = 有冲击净值\n"
    "• 显示: 两位小数, 底色 = 组内名次 (高绿 → 低红)\n"
    "• 适用: 全部组; 随区间",
    "年换手 = 单边年换手\n"
    "• 算法: 全评估期日均单边换手 × 每年交易日 (倍/年)\n"
    "• 数据: 指数 = 池整体 ½ Σ|w − 漂移后上日 w| (再平衡 + 成分进出); 因子 = G20 多头买入额; 策略 = 各因子按当日份额加权 (不计因子间对冲)\n"
    "• 显示: 一位小数 + 倍, 底色 = 组内名次 (低绿 → 高红)\n"
    "• 适用: 指数 / 因子 / 策略; 不随区间",
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
// 篮子行 h 的当日 {只数, 总市值 [亿元], 成交额 [亿元], 资金流入 (净流入 / 成交额)}: 只数 / 总市值 = Tape::members / weight 的同切片等权线 / 加权线
// (总市值当日接上 × 加权线当日涨跌); 成交额 / 资金流入 = 新浪当日合成的 Day (不看接续); 非篮子 / 未到 → NaN
std::array<double, 4> basket_today(const Database::Feed &f, const PortfolioRow &h) {
  std::array<double, 4> o = {kNaN, kNaN, kNaN, kNaN};
  if (h.basket < 0 || !f.data || f.data->members.empty())
    return o;
  const size_t c = cut_of_line(static_cast<size_t>(h.basket)), eq = basket_line(c, kWeightEq), cap = basket_line(c, kWeightCap);
  assert(f.data->keys[eq] == eq && f.data->keys[cap] == cap && f.today.size() == f.data->list.size());
  double v = f.data->weight[cap];
  if (const Database::Day &t = f.today[cap]; Database::joins(f.data->list[cap], t))
    v *= t.candle.close / t.candle.ref;
  o[0] = f.data->members[eq];
  o[1] = v > 0 ? v : kNaN;
  if (const Database::Day &t = f.today[static_cast<size_t>(h.basket)]; t.candle.date != 0 && t.candle.amount > 0)
    o[2] = t.candle.amount / 1e8, o[3] = t.flow / t.candle.amount;
  return o;
}
// 篮子当日 bt → 四列值 [TodayCol] (换手 = 成交额 / 总市值)
std::array<double, kTodays> today_vals(const std::array<double, 4> &bt) { return {bt[0], bt[1], bt[2] / bt[1], bt[3]}; }
// 四列第 k 列值 y 的格
NumCell today_cell(int k, double y) {
  const ImVec4 &txt = ImGui::GetStyleColorVec4(ImGuiCol_Text);
  if (!std::isfinite(y))
    return num_none();
  switch (k) {
  case kTodayMembers:
    return num_cell(txt, "%.0f", y);
  case kTodayCap:
    return num_cell(txt, "%s", yi_text(y).c_str());
  case kTodayTurn:
    return num_cell(txt, "%.2f%%", 100.0 * y);
  default:
    return num_cell(tint(y, 0.0), "%+.2f%%", 100.0 * y);
  }
}
// 悬停四列第 k 列 (k 不在 [0, kTodays) = 别的列) → 换手 / 流入的分子分母
void today_tip(int k, const std::array<double, 4> &bt) {
  if (const double t = bt[2] / bt[1]; k == kTodayTurn && std::isfinite(t))
    tip("换手 %.2f%% = 成交额 %s / 总市值 %s", 100.0 * t, yi_text(bt[2]).c_str(), yi_text(bt[1]).c_str());
  else if (k == kTodayFlow && std::isfinite(bt[3]))
    tip("流入 %+.2f%% = 净流入 %s / 成交额 %s", 100.0 * bt[3], yi_text(bt[3] * bt[2], true).c_str(), yi_text(bt[2]).c_str());
}
// 篮子行的当日 bt (basket_today) 过 f (见 BasketFilter)
bool passes(const BasketFilter &f, const std::array<double, 4> &bt) {
  const auto in = [](double x, double lo, double hi) { return (lo <= 0 && hi <= 0) || (std::isfinite(x) && x >= lo && (hi <= 0 || x <= hi)); };
  return in(bt[0], f.n_lo, f.n_hi) && in(bt[1], f.cap_lo, f.cap_hi) && (f.flow <= 0 || (std::isfinite(bt[3]) && 100.0 * std::abs(bt[3]) >= f.flow));
}
// 篮子组右表的过滤条 (三页同一份): 只数 [≥ lo][≤ hi] 市值 [≥ lo][≤ hi] |流入| [≥] 列出 / 总数; 改动下帧生效. 返回末项右缘 x
float filter_bar(BasketFilter &f, size_t listed, size_t total) {
  constexpr int kMaxN = 100000;
  constexpr float kMaxCap = 1e7f; // 亿元
  const float fp = 2.0f * ImGui::GetStyle().FramePadding.x;
  const auto width = [&](const char *widest) { ImGui::SetNextItemWidth(ImGui::CalcTextSize(widest).x + fp); };
  constexpr ImGuiSliderFlags kClamp = ImGuiSliderFlags_AlwaysClamp;
  ImGui::TextDisabled("只数");
  ImGui::SameLine(0, 2);
  width("≥00000");
  ImGui::DragInt("##pf_f_n_lo", &f.n_lo, 5.0f, 0, kMaxN, "≥%d", kClamp);
  ImGui::SameLine(0, 2);
  width("≤00000");
  ImGui::DragInt("##pf_f_n_hi", &f.n_hi, 5.0f, 0, kMaxN, f.n_hi > 0 ? "≤%d" : "≤不限", kClamp);
  ImGui::SameLine();
  ImGui::TextDisabled("市值");
  ImGui::SameLine(0, 2);
  width("≥000000亿");
  ImGui::DragFloat("##pf_f_cap_lo", &f.cap_lo, 10.0f, 0.f, kMaxCap, "≥%.0f亿", kClamp);
  ImGui::SameLine(0, 2);
  width("≤000000亿");
  ImGui::DragFloat("##pf_f_cap_hi", &f.cap_hi, 10.0f, 0.f, kMaxCap, f.cap_hi > 0 ? "≤%.0f亿" : "≤不限", kClamp);
  ImGui::SameLine();
  ImGui::TextDisabled("|流入|");
  ImGui::SameLine(0, 2);
  width("≥00.0%");
  ImGui::DragFloat("##pf_f_flow", &f.flow, 0.1f, 0.f, 100.f, "≥%.1f%%", kClamp);
  ImGui::SameLine();
  ImGui::TextDisabled("%zu/%zu", listed, total);
  const float x = ImGui::GetItemRectMax().x;
  if (ImGui::IsItemHovered())
    tip("过滤本组的行 (指数 / 行业 / 概念各一份, NAV / VAL / FLOW 三页共用): 只数 / 总市值在区间内 (上限 0 = 不限), |资金流入| ≥ 下限 (0 = 不限)\n"
        "• 设了的条件缺值 (行情未到 / 无新浪行情日) 即不列; 已选中的行恒列\n"
        "• 拖动改值, 双击 / Ctrl+点击直接输入; 右侧 = 列出 / 总数");
  return x;
}
void legend_nav(PortfolioUIState &ui, const PortfolioDerived &d, const PortfolioView &v) {
  button_bar(kNavGroupName, kNavGroups, ui.cat[kPageNav]);
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
  const float bar_x = ImGui::GetItemRectMax().x;
  if (ImGui::IsItemHovered())
    tip("点行 = 选中 / 取消, 选中 = 主图画该行两条线, 主图例默认只显理想列, 可成交列点图例显 (显隐变了自动重新 fit); 默认只选全A.等; 选中换组保留\n"
        "• 指数 = 各池等权指数 (all 池拆 全A.等 / 全A.加); 行业 = 申万一级 (全A 内切, 总市值加权); 概念 = 东财题材概念 (全A 内切, 总市值加权):\n"
        "    ◦ 可成交 = 标签指数 (收盘封板不建仓 / 平仓顺延, 与因子回测同口径)\n"
        "    ◦ 理想 = 价格指数 (Assets 篮子同一份: 成员 / 权重取前一日收盘, 点位按前收合成, 不看可成交, 同中证 / 万得口径); 两者之差 = 可成交拖累\n"
        "• 因子 = 单因子各自的 G20 多头净值: 可成交列 = 有冲击 (它自己的冲击段: Lob 最小金额段 / Bar 固定冲击), 理想列 = 无冲击\n"
        "• 策略 = 本组合 G20 多头: 可成交列 = 有冲击, 理想列 = 无冲击\n"
        "• 各列怎么算: 悬停表头 (算法 / 数据 / 显示 / 适用)\n"
        "• 指数 / 行业 / 概念表上过滤条 (与 VAL / FLOW 页同一份): 按只数 / 总市值 / |资金流入| 筛本组的行, 已画的行恒列\n"
        "• 列宽按内容最紧; 名超长截断补 …, 悬停中文名看全名; 点表头排序 (组内): 升序 → 降序 → 原序; 名列按颜色排 (相似的挨着)");
  // 一行 r = 一对线; 切片 / 名次同下标. 篮子组按过滤条定列出的行 (选中行恒列)
  const int g = ui.cat[kPageNav];
  const std::vector<PortfolioRow> &rs = d.nav[g];
  const size_t n = rs.size();
  std::vector<std::array<double, 4>> bt(n);
  std::vector<size_t> order;
  for (size_t r = 0; r < n; ++r) {
    bt[r] = basket_today(ui.basket.feed, rs[r]);
    if (g >= kBasketGroups || shown(ui, rs[r]) || passes(ui.filter[g], bt[r]))
      order.push_back(r);
  }
  const float top_x = g < kBasketGroups ? std::max(bar_x, filter_bar(ui.filter[g], order.size(), n)) : bar_x;
  if (!ImGui::BeginTable("##pf_nav_tbl", 1 + kLefts + kNavNums,
                         ImGuiTableFlags_ScrollY | ImGuiTableFlags_SizingFixedFit | ImGuiTableFlags_BordersInnerV | ImGuiTableFlags_Sortable |
                             ImGuiTableFlags_SortTristate))
    return;
  ImGui::TableSetupScrollFreeze(0, 1);
  ImGui::TableSetupColumn("名", ImGuiTableColumnFlags_WidthStretch);
  float cw[kNavCols]; // [列 − 1] 本帧最宽 (表头起)
  for (int k = 0; k < kNavCols; ++k) {
    const char *name = k < kRecents ? kRecentName[k] : k < kLefts ? kTodayName[k - kRecents]
                                                                  : kNavNumName[k - kLefts];
    ImGui::TableSetupColumn(name);
    cw[k] = ImGui::CalcTextSize(name).x;
  }
  headers_row(kNavTip, 1 + kNavCols);
  std::vector<std::array<double, kLefts>> lc(n); // [行][NavLeft] (只填列出的行)
  std::vector<std::array<NumCell, kNavCols>> tc(n);
  const ImVec4 &txt = ImGui::GetStyleColorVec4(ImGuiCol_Text);
  for (const size_t r : order) {
    const PortfolioRow &h = rs[r];
    const Database::Recent x = h.basket >= 0 ? Database::RecentOf(ui.basket.feed, static_cast<size_t>(h.basket)) : recent_of(h.line[kTrade]);
    const std::array<double, kTodays> tv = today_vals(bt[r]);
    lc[r] = {x.chg, x.ma5};
    std::copy(tv.begin(), tv.end(), lc[r].begin() + kRecents);
    for (int k = 0; k < kNavCols; ++k) {
      const double y = k < kLefts ? lc[r][static_cast<size_t>(k)] : nav_num(d, v, g, r, k - kLefts);
      NumCell &c = tc[r][static_cast<size_t>(k)];
      if (k >= kRecents && k < kLefts)
        c = today_cell(k - kRecents, y);
      else if (!std::isfinite(y))
        c = num_none();
      else if (k >= kLefts)
        c = num_cell(txt, kNavNumFmt[k - kLefts], y);
      else
        c = num_cell(tint(y, 0.0), "%+.2f%%", 100.0 * y);
      cw[k] = std::max(cw[k], c.w);
    }
  }
  sort_rows(order, rs, [&](size_t r, int c) { return c <= kLefts ? lc[r][static_cast<size_t>(c - 1)] : nav_num(d, v, g, r, c - 1 - kLefts); });
  for (const size_t r : order) {
    const PortfolioRow &h = rs[r];
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    const bool on = shown(ui, h);
    ImGui::PushStyleColor(ImGuiCol_Text, nav_color(d, g, r, kTrade));
    if (ImGui::Selectable((h.cn + "##pfl_" + h.id).c_str(), on, ImGuiSelectableFlags_SpanAllColumns)) {
      ui.show[h.id] = !on;
      if (!on)
        for (int l = 0; l < kPairLines; ++l)
          ui.reveal.insert(line_id(h, l));
    }
    ImGui::PopStyleColor();
    const int hov = ImGui::IsItemHovered() ? ImGui::TableGetHoveredColumn() - 1 : -1; // 行 Selectable 跨全列: 悬停的数值列
    name_tip(h.hover);
    for (int k = 0; k < kNavCols; ++k) {
      ImGui::TableSetColumnIndex(1 + k);
      if (k >= kLefts)
        if (const double rk = v.nav_rank[g][r][k - kLefts]; std::isfinite(rk))
          ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::ColorConvertFloat4ToU32(band_col(1.0 - rk, kValCellA)));
      draw_cell(tc[r][static_cast<size_t>(k)], cw[k]);
    }
    today_tip(hov - kRecents, bt[r]);
  }
  ui.legend_w[kPageNav] = legend_width(top_x, end_table());
}

// ---- VAL 右图例: 滚动窗长 + 类别条 (指数 / 行业 / 概念, 一次看一组) + 篮子单选表格; 列 = 中文名 (悬停看名) | 当日四列 | 各口; 各口格 = 区间末各口的估值分位, 底色 = 同组同口内的截面名次 (便宜绿 → 贵红); 表头点排序 (组内) ----
// [列] 表头悬停说明 (名 + kTodayName + kVal); 各口同一模板, 只换分子 / 负值口径
const std::array<std::string, 1 + kTodays + kPoolVals> kValTip = [] {
  constexpr const char *num[kPoolVals] = {"归母净利TTM", "归母权益MRQ", "营业总收入TTM", "经营现金流TTM", "近4季报告期税前分红"}; // [PoolVal]
  constexpr const char *neg[kPoolVals] = {"亏损计负", "负权益计负", "无收入计 0", "负现金流计负", "不分红计 0"};                  // [PoolVal]
  std::array<std::string, 1 + kTodays + kPoolVals> o;
  std::copy(std::begin(kTodayTip), std::end(kTodayTip), o.begin() + 1);
  o[0] = "名 = 篮子名\n"
         "• 算法: 切片名 + .等 (等权) / .加 (总市值加权)\n"
         "• 数据: 切片 = 全A / 池 / 申万一级行业 / 东财题材概念\n"
         "• 显示: 超长截断补 …, 悬停看全名; 选中行高亮 (主图画它)\n"
         "• 适用: 指数 / 行业 / 概念";
  for (int b = 0; b < kPoolVals; ++b) {
    char s[1024];
    std::snprintf(s, sizeof(s),
                  "%s = %s 倒数 (%s / 总市值) 的估值分位\n"
                  "• 算法: 区间末日 %s 在过去 N 年 (上方滑条) 窗内的加权分位; 收益率越低越贵 (高 = 贵, ≤ 0 排最贵端)\n"
                  "• 数据: 分子 = %s (%s); .加 = Σ分子 / Σ总市值, .等 = 成分 %s 的算术平均; 成分 / 财务取当日适用 (无前视)\n"
                  "• 显示: 分位百分比, 底色 = 组内同口名次 (便宜绿 → 贵红); 绝对值看主图 Band 图例\n"
                  "• 适用: 指数 / 行业 / 概念; 随区间末日, 估值史不满 1 年 → -",
                  kVal[b].name, kVal[b].inv, num[b], kVal[b].name, num[b], neg[b], kVal[b].name);
    o[static_cast<size_t>(1 + kTodays + b)] = s;
  }
  return o;
}();
void legend_val(PortfolioUIState &ui, const PortfolioDerived &d, const PortfolioView &v, PortfolioAt vs) {
  ImGui::SetNextItemWidth(-FLT_MIN);
  ImGui::SliderInt("##pf_band_years", &ui.band_years, 1, kBandYearsMax, "Band 滚动窗 %d 年", ImGuiSliderFlags_AlwaysClamp);
  if (ImGui::IsItemHovered())
    tip("Band 分位的滚动窗长 N: 每日取过去 N 年 (含当日) 的收益率加权分位, 拖动即时重算\n"
        "• 权重 = 1 - exp(-10 u), u = 1 - 距今 / 窗跨度: 近 ~3/4 窗约等权, 窗尾段快速降到 0 (老点出窗时分位不跳)\n"
        "• 池估值史不足 N 年时先扩张窗 (权重按实际史长缩放); 不满 1 年不画");
  button_bar(kNavGroupName, kBasketGroups, ui.cat[kPageVal]);
  ImGui::SameLine();
  ImGui::TextColored(kHead, "(单选)");
  const float bar_x = ImGui::GetItemRectMax().x;
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
        "• 表上过滤条 (与 NAV / FLOW 页同一份): 按只数 / 总市值 / |资金流入| 筛本组的行, 选中行恒列\n"
        "• 列宽按内容最紧; 名超长截断补 …, 悬停中文名看全名; 点表头排序 (组内): 升序 → 降序 → 原序; 名列按颜色排 (相似的挨着)");
  const int g = ui.cat[kPageVal];
  const std::vector<PortfolioRow> &rs = d.nav[g];
  const size_t n = rs.size();
  std::vector<std::array<double, 4>> bt(n);
  std::vector<size_t> order;
  for (size_t r = 0; r < n; ++r) {
    bt[r] = basket_today(ui.basket.feed, rs[r]);
    if (vs == PortfolioAt{g, r} || passes(ui.filter[g], bt[r]))
      order.push_back(r);
  }
  const float top_x = std::max(bar_x, filter_bar(ui.filter[g], order.size(), n));
  constexpr int kCols = kTodays + kPoolVals; // 名之后: 当日四列 + 各口
  if (!ImGui::BeginTable("##pf_val_tbl", 1 + kCols,
                         ImGuiTableFlags_ScrollY | ImGuiTableFlags_SizingFixedFit | ImGuiTableFlags_BordersInnerV | ImGuiTableFlags_Sortable |
                             ImGuiTableFlags_SortTristate))
    return;
  ImGui::TableSetupScrollFreeze(0, 1);
  ImGui::TableSetupColumn("名", ImGuiTableColumnFlags_WidthStretch);
  float cw[kCols]; // [列 − 1] 本帧最宽 (表头起)
  for (int k = 0; k < kCols; ++k) {
    const char *name = k < kTodays ? kTodayName[k] : kVal[k - kTodays].name;
    ImGui::TableSetupColumn(name);
    cw[k] = ImGui::CalcTextSize(name).x;
  }
  headers_row(kValTip.data(), 1 + kCols);
  std::vector<std::array<double, kTodays>> tv(n); // 只填列出的行
  std::vector<std::array<NumCell, kCols>> tc(n);
  for (const size_t r : order) {
    tv[r] = today_vals(bt[r]);
    for (int k = 0; k < kCols; ++k) {
      NumCell &c = tc[r][static_cast<size_t>(k)];
      if (k < kTodays) {
        c = today_cell(k, tv[r][static_cast<size_t>(k)]);
      } else {
        const double p = v.cell[g][r][k - kTodays].pct;
        c = std::isfinite(p) ? num_cell(ImGui::GetStyleColorVec4(ImGuiCol_Text), "%.0f%%", p) : num_none();
      }
      cw[k] = std::max(cw[k], c.w);
    }
  }
  sort_rows(order, rs, [&](size_t r, int c) { return c <= kTodays ? tv[r][static_cast<size_t>(c - 1)] : v.cell[g][r][c - 1 - kTodays].pct; });
  for (const size_t r : order) {
    const PortfolioRow &s = rs[r];
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    ImGui::PushStyleColor(ImGuiCol_Text, nav_color(d, g, r, kTrade));
    if (ImGui::Selectable((s.cn + "##pfv_" + s.id).c_str(), vs == PortfolioAt{g, r}, ImGuiSelectableFlags_SpanAllColumns))
      ui.val_id = s.id;
    ImGui::PopStyleColor();
    const int hov = ImGui::IsItemHovered() ? ImGui::TableGetHoveredColumn() - 1 : -1; // 行 Selectable 跨全列: 悬停的数值列
    name_tip(s.hover);
    for (int k = 0; k < kCols; ++k) {
      ImGui::TableSetColumnIndex(1 + k);
      if (k >= kTodays)
        if (const double rk = v.cell[g][r][k - kTodays].rank; std::isfinite(rk))
          ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::ColorConvertFloat4ToU32(band_col(rk, kValCellA)));
      draw_cell(tc[r][static_cast<size_t>(k)], cw[k]);
    }
    today_tip(hov, bt[r]);
  }
  ui.legend_w[kPageVal] = legend_width(top_x, end_table());
}

// ---- FLOW 右图例: 类别条 (指数 / 行业 / 概念) + 过滤条; 表 = 该组一行一篮子, 点行 = 选中 / 取消 (与 NAV 同一份选中, 左图画选中的).
//      列 = 中文名 (线色) | 当日四列 | 日轴各日 (MM-DD) 净流入占当日成交额 | 档内合计净流入 (亿) | 合计占比; 按正负着色, 悬停格看净流入 / 成交额; 表头点排序 (组内) ----
// 表头悬停说明: 名 / 一日 (表头 = 该日 MM-DD, 拼在说明前) / 合计两列 (当日四列 = kTodayTip)
const std::string kFlowNameTip = "名 = 篮子名\n"
                                 "• 算法: 切片名 + .等 (等权) / .加 (总市值加权); 资金流不分权重, 同切片两行同成分即同值\n"
                                 "• 数据: 切片 = 全A / 池 / 申万一级行业 / 东财题材概念\n"
                                 "• 显示: 字色 = 线色 (同 NAV); 超长截断补 …, 悬停看全名\n"
                                 "• 适用: 指数 / 行业 / 概念";
const std::string kFlowDayTip = " = 该日资金流入占成交额\n"
                                "• 算法: 净流入 / 成交额; 净流入 = Σ 成分 Σ 分钟 ± 该分钟额 (分钟涨跌法): 收 > 上一分钟收 (首分钟对昨收) 记 +, < 记 −, 平随上一分钟\n"
                                "• 数据: 当日 = 新浪快照合成 (实时); 历史日 = all.bar 库 L_1min (其后至基本面末日 = bar1m) 合成; 成分之和不加权;\n"
                                "  首根 09:31 (开盘集合竞价) / 末根 15:00 (收盘集合竞价) 按图左上角 开收盘 开关计入 (默认不计)\n"
                                "• 显示: 百分比两位, 按正负着色; 悬停格看净流入 / 成交额; 该日无分钟根 → -\n"
                                "• 适用: 指数 / 行业 / 概念; 随档位 (日内 / 2日 … 5日)";
const std::string kFlowSumTip[2] = {
    "净流入 = 档内合计净流入\n"
    "• 算法: 日轴各日净流入之和 (各日同上列)\n"
    "• 数据: 同各日列\n"
    "• 显示: 亿 / 万亿带符号, 按正负着色; 悬停格看成交额\n"
    "• 适用: 指数 / 行业 / 概念; 随档位",
    "占比 = 档内合计净流入占合计成交额\n"
    "• 算法: Σ 各日净流入 / Σ 各日成交额 (左图占比的分母是近 5 日日均成交额, 两者不同)\n"
    "• 数据: 同各日列\n"
    "• 显示: 百分比两位, 按正负着色; 悬停格看净流入 / 成交额\n"
    "• 适用: 指数 / 行业 / 概念; 随档位",
};
void legend_flow(PortfolioUIState &ui, const PortfolioDerived &d, const std::vector<misc::Ymd> &days) {
  button_bar(kNavGroupName, kBasketGroups, ui.cat[kPageFlow]);
  ImGui::SameLine();
  ImGui::TextColored(kHead, "(点行 = 画)");
  const float bar_x = ImGui::GetItemRectMax().x;
  if (ImGui::IsItemHovered())
    tip("点行 = 选中 / 取消, 选中 = 左图画该篮子的累计净流入 (与 NAV 页同一份选中: 这里选的 NAV 主图 / 日内栏也画); 选中换组保留\n"
        "• 指数 = 各池 (all 池拆 全A.等 / 全A.加); 行业 = 申万一级; 概念 = 东财题材概念 (全A 内切)\n"
        "• 各列怎么算: 悬停表头 (算法 / 数据 / 显示 / 适用)\n"
        "• 表上过滤条 (与 NAV / VAL 页同一份): 按只数 / 总市值 / |当日资金流入| 筛本组的行, 已画的行恒列\n"
        "• 列宽按内容最紧; 名超长截断补 …, 悬停中文名看全名; 点表头排序 (组内): 升序 → 降序 → 原序; 名列按颜色排 (相似的挨着)");
  const int g = ui.cat[kPageFlow];
  const std::vector<PortfolioRow> &rs = d.nav[g];
  const size_t n = rs.size(), nd = days.size(), nf = nd + 2, nc = kTodays + nf; // 名之后: 当日四列 + 各日 + 合计两列
  std::vector<std::array<double, 4>> bt(n);
  std::vector<size_t> order;
  for (size_t r = 0; r < n; ++r) {
    bt[r] = basket_today(ui.basket.feed, rs[r]);
    if (shown(ui, rs[r]) || passes(ui.filter[g], bt[r]))
      order.push_back(r);
  }
  const float top_x = std::max(bar_x, filter_bar(ui.filter[g], order.size(), n));
  ImGui::PushID(static_cast<int>(nd)); // 列数随档位
  if (!ImGui::BeginTable("##pf_flow_tbl", static_cast<int>(1 + nc),
                         ImGuiTableFlags_ScrollY | ImGuiTableFlags_SizingFixedFit | ImGuiTableFlags_BordersInnerV | ImGuiTableFlags_Sortable |
                             ImGuiTableFlags_SortTristate)) {
    ImGui::PopID();
    return;
  }
  ImGui::TableSetupScrollFreeze(0, 1);
  ImGui::TableSetupColumn("名", ImGuiTableColumnFlags_WidthStretch);
  std::vector<std::string> names(nc), tips(1 + nc);
  tips[0] = kFlowNameTip;
  for (int k = 0; k < kTodays; ++k)
    names[static_cast<size_t>(k)] = kTodayName[k], tips[static_cast<size_t>(1 + k)] = kTodayTip[k];
  for (size_t k = 0; k < nd; ++k) {
    char s[16];
    std::snprintf(s, sizeof(s), "%02d-%02d", days[k] / 100 % 100, days[k] % 100);
    names[kTodays + k] = s, tips[1 + kTodays + k] = s + kFlowDayTip;
  }
  names[kTodays + nd] = "净流入", names[kTodays + nd + 1] = "占比";
  tips[1 + kTodays + nd] = kFlowSumTip[0], tips[2 + kTodays + nd] = kFlowSumTip[1];
  std::vector<float> cw(nc); // 本帧最宽 (表头起)
  for (size_t k = 0; k < nc; ++k) {
    ImGui::TableSetupColumn(names[k].c_str());
    cw[k] = ImGui::CalcTextSize(names[k].c_str()).x;
  }
  headers_row(tips.data(), static_cast<int>(1 + nc));
  // [行][各日 + 合计两列]: 净流入 / 成交额 (只填列出的行); 格值 = 占比 (合计净流入列 = 金额 [亿元])
  std::vector<std::vector<Database::Flow>> fl(n);
  std::vector<std::array<double, kTodays>> tv(n);
  std::vector<std::vector<NumCell>> tc(n);
  const auto value = [&](size_t r, size_t k) { // k = 名之后的列
    if (k < kTodays)
      return tv[r][k];
    const Database::Flow &x = fl[r][k - kTodays];
    return k - kTodays == nd ? (x.amount > 0 ? x.net / 1e8 : kNaN) : x.amount > 0 ? x.net / x.amount
                                                                                  : kNaN;
  };
  for (const size_t r : order) {
    tv[r] = today_vals(bt[r]);
    std::vector<Database::Flow> &x = fl[r];
    assert(rs[r].basket >= 0);
    x = Database::FlowOf(ui.basket, static_cast<size_t>(rs[r].basket), days);
    x.resize(nd);
    Database::Flow sum;
    for (const Database::Flow &y : x)
      sum.net += y.net, sum.amount += y.amount;
    x.push_back(sum), x.push_back(sum);
    tc[r].resize(nc);
    for (size_t k = 0; k < nc; ++k) {
      const double y = value(r, k);
      NumCell &c = tc[r][k];
      c = k < kTodays         ? today_cell(static_cast<int>(k), y)
          : !std::isfinite(y) ? num_none()
          : k - kTodays == nd ? num_cell(tint(y, 0.0), "%s", yi_text(y, true).c_str())
                              : num_cell(tint(y, 0.0), "%+.2f%%", 100.0 * y);
      cw[k] = std::max(cw[k], c.w);
    }
  }
  sort_rows(order, rs, [&](size_t r, int c) { return value(r, static_cast<size_t>(c - 1)); });
  for (const size_t r : order) {
    const PortfolioRow &h = rs[r];
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    const bool on = shown(ui, h);
    ImGui::PushStyleColor(ImGuiCol_Text, nav_color(d, g, r, kTrade));
    if (ImGui::Selectable((h.cn + "##pff_" + h.id).c_str(), on, ImGuiSelectableFlags_SpanAllColumns)) {
      ui.show[h.id] = !on;
      if (!on)
        for (int l = 0; l < kPairLines; ++l)
          ui.reveal.insert(line_id(h, l));
    }
    ImGui::PopStyleColor();
    const int hov = ImGui::IsItemHovered() ? ImGui::TableGetHoveredColumn() - 1 : -1; // 行 Selectable 跨全列: 悬停的数值列
    name_tip(h.hover);
    for (size_t k = 0; k < nc; ++k) {
      ImGui::TableSetColumnIndex(static_cast<int>(1 + k));
      draw_cell(tc[r][k], cw[k]);
    }
    today_tip(hov, bt[r]);
    if (const int f = hov - kTodays; f >= 0 && fl[r][static_cast<size_t>(f)].amount > 0) {
      const Database::Flow &x = fl[r][static_cast<size_t>(f)];
      tip("净流入 %s / 成交额 %s = %+.2f%%", yi_text(x.net / 1e8, true).c_str(), yi_text(x.amount / 1e8).c_str(), 100.0 * x.net / x.amount);
    }
  }
  ui.legend_w[kPageFlow] = legend_width(top_x, end_table());
  ImGui::PopID();
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

int RenderTabPortfolio(PortfolioService &svc, PortfolioUIState &ui, double fixed_impact, const Database::Market *mk) {
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

  // ---- 三级缓存: 派生 → 滚动分位 (键 = 篮子代) → 视图 (键 = 派生 gen + 滚动分位 gen): 上级重算 → 下级自动重算 ----
  ui.der.update(PortfolioDerKey{gen, ui.items_gen, ui.basket.feed.shown, fixed_impact}, [&](PortfolioDerived &dv) {
    const std::vector<misc::Ymd> old = dv.dates;
    derive(ui.items, ui.name, res, ui.basket.feed.data, fixed_impact, dv);
    if (dv.dates != old) // 日轴变 → 区间回全段
      ui.range = PortfolioRange{.d0 = 0, .d1 = static_cast<int>(dv.dates.size())};
  });
  // VAL 选中篮子行: 换组合 → 回默认 (组合首个因子所在池的等权行; 无因子 → 暂用首行, 不记)
  if (ui.val_for != ui.name)
    ui.val_for = ui.name, ui.val_id.clear();
  const auto val_at = [&](const std::string &id) {
    for (int g = 0; g < kBasketGroups; ++g)
      for (size_t r = 0; r < d.nav[g].size(); ++r)
        if (d.nav[g][r].id == id)
          return PortfolioAt{g, r};
    return PortfolioAt{};
  };
  PortfolioAt vs = val_at(ui.val_id);
  if (vs.g < 0) {
    const auto first = std::find_if(ui.items.begin(), ui.items.end(), [](const PortfolioItem &it) { return it.missing.empty(); });
    vs = first != ui.items.end() ? val_at(basket_id(basket_line(cut_of(pool_id(first->e.pool)), kWeightEq))) : PortfolioAt{};
    if (vs.g >= 0)
      ui.val_id = d.nav[vs.g][vs.r].id;
    else if (!d.nav[kNavPool].empty())
      vs = {kNavPool, 0};
  }
  ui.roll.update(PortfolioRollKey{d.basket_gen, ui.band_years, vs}, [&](std::array<PortfolioRoll, kPoolVals> &r) {
    for (int b = 0; b < kPoolVals; ++b)
      if (vs.g >= 0 && d.baskets)
        roll_of(*d.baskets, d.nav[vs.g][vs.r], b, ui.band_years, r[b]);
      else
        r[b] = {};
  });
  ui.view.update(PortfolioViewKey{ui.der.gen, ui.roll.gen, ui.range.d0, ui.range.d1},
                 [&](PortfolioView &vw) { make_view(d, ui.range.d0, ui.range.d1, vs, ui.roll.key.years, ui.roll.val, vw); });
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
  if (page != kPageVal && ui.name != ui.vis_key) { // NAV 行选中 (FLOW 同一份) 回默认 (换组合), 其线在主图例里回默认显隐
    ui.vis_key = ui.name;
    for (const std::vector<PortfolioRow> &rs : d.nav)
      for (const PortfolioRow &r : rs) {
        ui.show.erase(r.id);
        for (int l = 0; l < kPairLines; ++l)
          ui.reveal.insert(line_id(r, l));
      }
  }
  constexpr float kBarH = 90.0f, kLegendMaxFrac = 0.5f; // 右图例最多占整宽的比例 (再宽表格自己截)
  const ImGuiStyle &st = ImGui::GetStyle();
  const ImVec2 av = ImGui::GetContentRegionAvail();
  const float lw = std::min(ui.legend_w[page] + 2.0f * (st.WindowPadding.x + st.ChildBorderSize), av.x * kLegendMaxFrac);
  const float left = av.x - lw - st.ItemSpacing.x;
  const float ph = av.y - kBarH - st.ItemSpacing.y;
  const float ih = nav && ui.intraday ? std::floor((ph - st.ItemSpacing.y) / 3.0f) : 0.0f;
  // 选中篮子行的行情叠加图 (日内栏 / FLOW 同一份选中, 线色 = 主图的), 贴边占满 size
  const auto basket_child = [&](const char *id, const ImVec2 &size, bool flow) {
    ImGui::PushStyleVar(ImGuiStyleVar_WindowPadding, ImVec2(0, 0));
    ImGui::BeginChild(id, size);
    ImGui::PopStyleVar();
    if (mk) {
      std::vector<Database::BasketPick> picks;
      for (int g = 0; g < kBasketGroups; ++g)
        for (size_t r = 0; r < d.nav[g].size(); ++r)
          if (const PortfolioRow &h = d.nav[g][r]; shown(ui, h))
            picks.push_back({static_cast<size_t>(h.basket), ImGui::ColorConvertFloat4ToU32(nav_color(d, g, r, kTrade))});
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
    legend_flow(ui, d, mk ? Database::FlowDays(*mk, ui.basket) : std::vector<misc::Ymd>{});
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
        fk.rows += shown(ui, r) ? '1' : '0';
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
    PlotFrame pf{ImPlot::FitThisFrame() ? ImPlotRect(-DBL_MAX, DBL_MAX, -DBL_MAX, DBL_MAX) : cur, {}, {}, {}};
    if (nav)
      plot_nav(ui, d, v, pf);
    else if (vs.g >= 0)
      plot_val(ui, d, v, vs, pf);
    ui.shown_key = std::move(pf.shown);
    // 本帧 fit (自动 / 双击): x 右端再扩一段, 让区间末点右侧留出线尾小字 / 刻度 (同贴末点, 取宽者) 的宽度
    if (ImPlot::FitThisFrame() && !pf.tags.empty() && ui.range.d1 > ui.range.d0) {
      const float tw = std::max(end_tags_width(pf.tags, kTagScale), marks_width(pf.marks)), W = ImPlot::GetPlotSize().x;
      const double x0 = x_of(static_cast<size_t>(ui.range.d0), kOpen), x1 = x_of(static_cast<size_t>(ui.range.d1), kOpen);
      if (W > 2.0f * tw)
        ImPlot::FitPointX(x1 + (x1 - x0) * tw / (W - tw));
    }
    draw_end_tags(pf.tags, kTagScale, tags_right());
    draw_marks(pf.marks);
    ImPlot::EndPlot();
  }
  ImPlot::PopStyleVar(2);
  range_bar(ui, nav, vs, left, kBarH);
  ImGui::EndGroup();

  ImGui::SameLine();
  ImGui::BeginChild("##pf_flegend", ImVec2(lw, av.y), ImGuiChildFlags_Borders);
  if (nav)
    legend_nav(ui, d, v);
  else
    legend_val(ui, d, v, vs);
  ImGui::EndChild();
  return action;
}

} // namespace GUI::Factors
