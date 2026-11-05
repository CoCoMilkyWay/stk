// Factors→Portfolio 页的派生层 (GUI 线程, 只算不画): PortfolioService 的结果 + 行情侧价格指数 → 三级缓存, TabPortfolio 只读末级画
//   派生 PortfolioDerived (结果 / 内容 / fixed_impact 变) → 滚动分位 PortfolioRoll (VAL 选中篮子 / 窗长变) → 区间视图 PortfolioView (区间变)
//   三级各一个入口 MakePortfolio{Derived, Roll, View}; 各级的键带上级的 gen (PortfolioStage): 上级重算 → 下级自动重算
#pragma once

#include "features/TimeIndex.hpp"               // TRADE_MINUTES_PER_DAY (横轴每日跨度)
#include "gui/task_database/services/Kline.hpp" // Tape (价格指数)
#include "gui/task_factors/services/PortfolioService.hpp"
#include "shared/Basket.hpp" // kGroupPool / kBasketGroups

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace GUI::Factors {

// 横轴格 x = 日下标 · kDayW + 日内分钟 (同 OrderFlow 图 2); 横轴编码只在这里
inline constexpr size_t kDayW = TRADE_MINUTES_PER_DAY;
inline constexpr size_t kOpen = 0, kClose = kDayW - 1; // 日内分钟: 开盘 / 收盘
constexpr size_t x_at(size_t di, size_t minute) { return di * kDayW + minute; }
constexpr double x_of(size_t di, size_t minute) { return static_cast<double>(x_at(di, minute)); }

// 一条线 (x 严格升序; x = 日下标 · 255 + 分钟), 全段原值 (净值 / 点位); 归一只在切片
struct PortfolioCurve {
  std::vector<double> x, y;
};

// 成对线的两条 (一行两条, 同色, 理想列淡): 篮子 (指数 / 行业 / 概念) = 可成交 (T+1C 标签指数) / 理想 (价格指数); 因子 / 策略 = 有冲击 / 无冲击.
// 行选中 → 主图例里理想列显、可成交列隐 (点图例再显)
enum PairLine : int { kTrade,   // 可成交 / 有冲击
                      kIdeal }; // 理想 / 无冲击
inline constexpr int kPairLines = 2;

// 一行 = 一对线, 全段 (NAV 右表一行; VAL / FLOW 表一行 = NAV 篮子行同一份). 主图例列 = cn | 口径 | 区间年化 | 区间夏普 | 年换手 (补空格对齐);
// NAV 右表 = cn | 近况两列 + 篮子当日四列 | 两条年化 | 可成交列夏普 | 年换手; en 只在悬停时显示; 排版 / 近况 / 年化 / 夏普在渲染时拼
//   篮子行: 两条 = BasketCurve 可成交指数 / 价格指数 (Assets 篮子日线) 同一投影到日并集轴 = 前收一点 (范围首日开盘; 源首日无) + 轴上每日收盘一点, 原值
struct PortfolioRow {
  std::string id;                                         // 选中键 (PortfolioPick); 线的 ImGui ID = id + 线后缀
  std::string en, cn;                                     // 名 / 中文名
  std::string tag;                                        // 线尾小字 (口径后缀另拼)
  std::string hover;                                      // 悬停 (en 起头 + 详情)
  double turn = std::numeric_limits<double>::quiet_NaN(); // 年换手 (全评估期单边, 倍/年; NaN = 无)
  float hue = std::numeric_limits<float>::quiet_NaN();    // 色相 ∈ [0, 1) (指数 / 行业 / 概念 / 因子四组一起按相似度定, 见 PortfolioDerived::nav); NaN = 未定 (灰); 策略不取
  int basket = -1;                                        // 篮子行: 篮子线 basket_line (BasketCurve / 篮子 Feed 同键; 近况 / 日内 / VAL 取它); 其余 −1
  std::vector<uint32_t> at;                               // 篮子行: [line[kTrade].x] → BasketCurve::dates 下标 (严格升序; 滚动窗看日并集轴以外的全史)
  PortfolioCurve line[kPairLines];
};

// NAV 五组 (右表类别条 / 主图画序同序, 策略在最上层); 前三组 = 篮子目录三组 (BasketGroup 同序)
enum NavGroup : int { kNavPool,
                      kNavInd,
                      kNavConcept,
                      kNavFactor,
                      kNavStrat };
inline constexpr int kNavGroups = 5;
static_assert(int{kNavPool} == int{kGroupPool} && int{kNavInd} == int{kGroupInd} && int{kNavConcept} == int{kGroupConcept} &&
              int{kNavFactor} == kBasketGroups);
// NAV 右表一行的数值列 (前两列下标 = PairLine)
enum NavNum : int { kNumTrade,  // 可成交列区间年化
                    kNumIdeal,  // 理想列区间年化
                    kNumSharpe, // 可成交列区间夏普
                    kNumTurn }; // 年换手
inline constexpr int kNavNums = 4;
static_assert(int{kNumTrade} == int{kTrade} && int{kNumIdeal} == int{kIdeal});
inline constexpr int kNavGood[kNavNums] = {1, 1, 1, -1}; // [NavNum] 好的方向: 年化 / 夏普高好, 年换手低好

// 一级: 派生 (结果 / 内容 / fixed_impact 变才重算): 全段. 重算时沿用上一份里没变的:
//   单因子线 = 同一条 FactorCurve 且日轴 / fixed_impact 不变; 篮子行 = 同一份 BasketCurve、同一份价格指数且日轴不变 (basket_gen 不变).
//   组合线恒重算 (份额随当日因子数)
struct PortfolioDerived {
  std::vector<misc::Ymd> dates;                        // 日并集轴 (横轴 day 下标)
  std::vector<PortfolioRow> nav[kNavGroups];           // [NavGroup][行]:
                                                       //   指数 / 行业 / 概念 = [basket_group 序] (源没算成也列, 线空); en = cut_code, cn = tag = 切片名 (超长截断) + .等 / .加, hover = en + 全名;
                                                       //   因子 = [items 序, 只含算成的] 该因子单独的 G20 多头 (与组合同式); id = PortfolioEntry::key,
                                                       //     en | cn = 因子文件主干 | 中文名 (超长截断), hover = en + 全名 + 池 层 持有期 冲击段 规范串
                                                       //   策略 = 一行, 组合 G20 多头: 首日开盘前 1 + 每个有增量的 x; en | cn = 组合名 | G20 多头
                                                       //   色: 策略外四组的行一起按相似度 s = max(成员重合, 残差相关) 建 Ward 树, 叶序相邻两行色相差 ∝ 树上分开处两侧平均 1 − s
                                                       //     (OKLCH 定亮定艳色环); 因子没到齐 / 价格指数未就绪 → 全灰. 右表名列排序 = 色相序 (相似的挨着)
                                                       //     成员重合 = Tape::overlap (只篮子之间), 残差相关 = 理想线末段日收益对全A.加回归的残差相关
  int n_ready = 0, n_error = 0, n_wait = 0;            // 已算成 / 算失败 / 未到的因子数
  std::vector<std::shared_ptr<const FactorCurve>> src; // [因子行] 单因子线的源 (nav[kNavFactor] 同下标)
  double impact = 0.0;                                 // 单因子线按的 fixed_impact
  std::shared_ptr<const BasketCurve> baskets;          // 可成交线 / 估值的源 (没算成 = 空)
  std::shared_ptr<const Database::Tape> prices;        // 理想线的源 (篮子历史 [basket_line]; 未到 / 源未就绪 = 空, 理想线空)
  uint64_t basket_gen = 0;                             // 篮子行重建次数 (滚动分位键)
};
// 一行的位置 der.nav[g][r] (g < 0 = 无)
struct PortfolioAt {
  int g = -1;
  size_t r = 0;
  bool operator==(const PortfolioAt &) const = default;
};

// 二级: VAL 选中篮子的滚动分位 (只随篮子 / 窗长). 每口收益率 v (EP / BP / SP / CP / DY) 按贵度 −v (升 = 贵) 逐日在过去 N 自然年
// (含当日; N = band_years) 窗内取 kBandQs 分位; 池估值史 (首个可算日起) 不满 1 年 → 不画 (不足 N 年先扩张窗)
inline constexpr int kBandYearsDef = 5, kBandYearsMax = 15;
inline constexpr int kBandQ = 7;
inline constexpr double kBandQs[kBandQ] = {0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95};
// 绝对估值等值线的档: 篮子全史倍数 m (= 1 / 收益率) 区间两头各剔 kContourTrim 分位 (近 0 收益率的日子倍数极大, 不让它撑开区间), kContours 等分
inline constexpr int kContours = 20;
inline constexpr double kContourTrim = 0.02;
// 选中篮子一口 [该行 at] (窗内按距今天数加权, 见 PortfolioView.cpp roll_w)
struct PortfolioRoll {
  std::vector<float> q[kBandQ]; // 窗内贵度 kBandQs 加权分位处的收益率 (便宜 → 贵 = 降序; NaN = 史不满 1 年)
  std::vector<float> pct;       // 当日贵度在窗内的加权分位 (%, 高 = 贵; NaN = 同上 / 当日不可算)
  std::vector<uint8_t> band;    // 当日 band 可画 (窗内可算点收益率全 > 0, 价格全有解; 分位照算不受限)
  std::vector<double> levels;   // 等值线倍数 (升序, kContours 档); 空 = 全史不可算
};

// 三级: 区间视图 (派生 / 滚动分位 / 区间变才重算), 与 PortfolioDerived 逐组对仗
// 曲线 c 在 [x0, x1) 的切片 (不拷数据): 源点 [j0, j1) = 区间前末点 (无则自身首点) + 区间内各点, 首点 x 钉到 x0, y / base (base = c.y[j0]) → 起点 1
struct PortfolioSlice {
  const PortfolioCurve *c = nullptr;
  size_t j0 = 0, j1 = 0; // 空 = j0 == j1
  double x0 = 0.0, base = 1.0;
  float ann = 0.f;                                        // 区间复利年化 (%; 不足两点 = 0)
  float sharpe = std::numeric_limits<float>::quiet_NaN(); // 区间日频夏普年化 (不足两日 / 日收益无波动 = NaN)
  size_t size() const { return j1 - j0; }
  double x(size_t i) const { return std::max(c->x[j0 + i], x0); }
  double y(size_t i) const { return c->y[j0 + i] / base; }
};
// 右表一格的值 + 它在同组同列内的截面名次 (NAV 数值列 / VAL 估值分位同一份)
struct Ranked {
  double x = std::numeric_limits<double>::quiet_NaN();    // NaN = 不可算
  double rank = std::numeric_limits<double>::quiet_NaN(); // ∈ [0, 1]: 0 = 好 (NAV 见 kNavGood; VAL = 便宜), 并列取中, 组内只一个 = 0.5; NaN = 不可算
};
// VAL 页选中篮子的一口 band (PoolVal): 滚动分位的价格线, x = PortfolioView::band_x
struct PortfolioBand {
  bool ok = false;                       // 区间内有分位点 (否则其余空)
  std::vector<double> y[kBandQ];         // P · v / q_j (v = 当日收益率; v ≤ 0 / q_j ≤ 0 / 无分位 = NaN), 升序 = 便宜 → 贵
  std::vector<std::pair<int, int>> segs; // y 全有限的连续段 [i0, i1) (填色按段画: PlotShaded 不跳 NaN)
  std::vector<double> unit;              // P · v (v ≤ 0 / 不可算 = NaN; 不看分位): 倍数 m (= 1 / v) 的绝对估值等值线 y = unit · m
  std::vector<double> levels;            // 等值线倍数 (= PortfolioRoll::levels)
  double now = 0.0, pct = 0.0;           // 区间末个有分位点的收益率 (EP / BP / SP / CP / DY) / 估值分位 (%, 高 = 贵)
};
struct PortfolioView {
  std::vector<std::array<PortfolioSlice, kPairLines>> nav[kNavGroups]; // [NavGroup][行][PairLine]
  std::vector<std::array<Ranked, kNavNums>> num[kNavGroups];           // [NavGroup][行][NavNum] NAV 右表数值列
  std::vector<std::array<Ranked, kPoolVals>> pct[kBasketGroups];       // [BasketGroup][行][PoolVal] VAL 表格: 区间末个可算点的估值分位 (%, 高 = 贵)
  std::vector<double> band_x;                                          // 选中篮子切片的 x (各口 band 共用; 未选 → 空)
  PortfolioBand band[kPoolVals];                                       // [PoolVal] 选中篮子的
};

// 一级派生缓存: 键带上级的 gen → 上级重算, 本级自动失效
template <class Key, class Val>
struct PortfolioStage {
  Key key{};
  uint64_t gen = 0; // 本级重算次数 (0 = 未算)
  Val val{};
  // 首次 / 键变 → f(val) 重算, gen +1
  template <class F>
  void update(const Key &k, F &&f) {
    if (gen > 0 && k == key)
      return;
    key = k;
    f(val);
    ++gen;
  }
};
struct PortfolioDerKey {
  uint64_t gen = 0, items = 0; // PortfolioService::result_gen / PortfolioUIState::items_gen
  uint32_t prices = 0;         // 价格指数在画的发布代 (BasketState feed.shown)
  double impact = 0.0;         // fixed_impact
  bool operator==(const PortfolioDerKey &) const = default;
};
struct PortfolioRollKey {
  uint64_t baskets = 0; // PortfolioDerived::basket_gen
  int years = 0;        // 窗长
  PortfolioAt vs;       // VAL 选中篮子行
  bool operator==(const PortfolioRollKey &) const = default;
};
struct PortfolioViewKey {
  uint64_t der = 0, roll = 0; // 派生 gen / 滚动分位 gen
  int d0 = 0, d1 = 0;         // 区间
  bool operator==(const PortfolioViewKey &) const = default;
};

// 一级: items / name = 当前组合, prices = 篮子历史在画的发布 (BasketState feed.data); d = 上一份 (沿用没变的) → 本份
void MakePortfolioDerived(const std::vector<PortfolioItem> &items, const std::string &name, const PortfolioResult &res,
                          const std::shared_ptr<const Database::Tape> &prices, double fixed_impact, PortfolioDerived &d);
// 二级: VAL 选中篮子行 vs 各口 [PoolVal], 窗长 years; 未选 / 篮子没算成 → 空
void MakePortfolioRoll(const PortfolioDerived &d, PortfolioAt vs, int years, std::array<PortfolioRoll, kPoolVals> &r);
// 三级: 区间 [d0, d1) (日并集轴下标); vs / years / roll = 二级的
void MakePortfolioView(const PortfolioDerived &d, int d0, int d1, PortfolioAt vs, int years, const std::array<PortfolioRoll, kPoolVals> &roll,
                       PortfolioView &v);

} // namespace GUI::Factors
