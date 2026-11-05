// Factors→Portfolio 页的派生层 (GUI 线程, 只算不画): PortfolioService 的账 + 行情侧价格指数 → 三级缓存, TabPortfolio 只读末级画
//   派生 PortfolioDerived (结果 / 内容 / fixed_impact / 成分组变) → 二级两份: 滚动分位 PortfolioRolls (VAL 标的 / 窗长变)
//   + 末点分位 PortfolioPct (窗长 / 区间末 / 估值行变; 不随区间起点) → 区间视图 PortfolioView (区间变)
//   各级一个入口 MakePortfolio{Derived, Roll, Pct, View}; 各级的键带上级的 gen (PortfolioStage): 上级重算 → 下级自动重算
//   行只一种 PortfolioRow (篮子 / 因子 / 策略 / 成分股): 两条线 + 可选的行情键 (近况 / 当日 / 资金流 / 日内叠加) + 可选的估值腿 (估值分位 / band)
//   线的来路只两种 (PortfolioLineSrc): 腿集合成 (腿 = 账的一线) / 价格指数 → 点位 → 投到日并集轴 → PortfolioLine; 按线源逐线沿用
#pragma once

#include "features/TimeIndex.hpp"               // TRADE_MINUTES_PER_DAY (横轴每日跨度)
#include "gui/task_database/services/Kline.hpp" // Tape / Side (价格指数 / 行情键)
#include "gui/task_factors/services/PortfolioService.hpp"
#include "shared/AssetInfo.hpp" // StockInfos (成分行名 / 悬停)
#include "shared/Basket.hpp"    // kGroupPool / kBasketGroups

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace GUI::Factors {

// 横轴格 x = 日下标 · kDayW + 日内分钟 (同 OrderFlow 图 2); 横轴编码只在这里
inline constexpr size_t kDayW = TRADE_MINUTES_PER_DAY;
inline constexpr size_t kOpen = 0, kClose = kDayW - 1; // 日内分钟: 开盘 / 收盘
inline constexpr double kStartDx = 0.5;                // 线的起点 (前收) 在首日开盘前几格: 首行可在开盘点就有增量 (L_1min 行 0 末分钟 = 开盘)
constexpr size_t x_at(size_t di, size_t minute) { return di * kDayW + minute; }
constexpr double x_of(size_t di, double minute) { return static_cast<double>(di * kDayW) + minute; }

// 一条线 (全段原值: 净值 / 点位; 归一只在切片): 画图点 + 日并集轴上的日末视图 (年化 / 夏普 / 近况 / 配色同读它) + 年换手
//   点: x 严格升序, 首点 = 首个活跃日开盘前 kStartDx 格的起点 (前收 = 前一日末), 其后各点在所在日的行末分钟
//   日末视图: 活跃日 [f, l] (起点所在日 ~ 末点所在日); end(k) = 第 k − 1 日末点位 (k = f 为前收; 活跃日内无点的日持平, l 之后持平),
//   s1 / s2 = 活跃日的日收益 / 平方的前缀和 (下标同 e)
struct PortfolioLine {
  std::vector<double> x, y;
  int f = 0, l = -1;                                      // 无点 = e 空; 只有起点 = l = f − 1
  std::vector<double> e, s1, s2;                          // [l − f + 2]: 下标 k − f
  double turn = std::numeric_limits<double>::quiet_NaN(); // 年换手 (腿集增仓量自首次建仓起、在有腿的日上的日均 × 每年交易日, 倍/年; 价格指数 / 无仓位 = NaN)
  double end(int k) const {
    if (e.empty() || k < f)
      return std::numeric_limits<double>::quiet_NaN();
    return e[static_cast<size_t>(std::min(k, l + 1) - f)];
  }
  // 点 j 的点位所属日 (日并集轴下标): 起点 = 前一日 f − 1 (可为 −1 = 轴外)
  int day(size_t j) const { return j == 0 ? f - 1 : static_cast<int>(x[j]) / static_cast<int>(kDayW); }
};

// 成对线的两条 (一行两条, 同色, 理想列淡): 有行情键的行 (篮子 / 成分股) = 可成交 / 理想; 因子 / 策略 = 有冲击 / 无冲击.
// 行选中 → 主图例里理想列显、可成交列隐 (点图例再显)
enum PairLine : int { kTrade,   // 可成交 / 有冲击
                      kIdeal }; // 理想 / 无冲击
inline constexpr int kPairLines = 2;

// 腿 = 账的一线 (逐行增量 + 增仓量 + 估值); b = nullptr = 无
struct PortfolioLeg {
  const PortfolioBook *b = nullptr;
  size_t line = 0;
  bool operator==(const PortfolioLeg &) const = default;
};
// 线源 (线的身份): 腿集合成 legs (cost = 扣成本: 毛 − 冲击 − 固定份数 · impact; 腿账都无成本列 → 恒不扣) / 价格指数 price (legs 空); 都空 = 空线.
// 源对象由 PortfolioDerived 持有, 地址活过下一次派生 ⇒ 按地址比即按对象比; 日轴不变时同线源的线沿用上一份
struct PortfolioLineSrc {
  std::vector<PortfolioLeg> legs;
  bool cost = false;
  double impact = 0.0; // fixed_impact (cost 才有)
  const Database::Kline *price = nullptr;
  bool operator==(const PortfolioLineSrc &) const = default;
};
// 行情键 (Database::Side 路的键): 篮子 = basket_line / 个股 = 全局 id; 因子 / 策略 = 无
struct PortfolioQuote {
  static constexpr uint32_t kNone = std::numeric_limits<uint32_t>::max();
  Database::Side side = Database::kBasket;
  uint32_t key = kNone;
  bool ok() const { return key != kNone; }
  bool is(Database::Side s) const { return ok() && side == s; }
  bool operator==(const PortfolioQuote &) const = default;
};

// 一行 = 一对线, 全段 (右表一行). 主图例列 = cn | 口径 | 区间年化 | 区间夏普 | 年换手 (补空格对齐);
// NAV 右表 = cn | 近况两列 + 当日四列 | 两条年化 | 可成交列夏普 | 年换手; en 只在悬停时显示; 排版 / 近况 / 年化 / 夏普在渲染时拼
struct PortfolioRow {
  std::string id;                                        // 选中键 (PortfolioPick); 线的 ImGui ID = id + 线后缀
  std::string en, cn;                                    // 名 / 中文名 (超长截断)
  std::string tag;                                       // 线尾小字 (主图口径后缀另拼; 日内栏 / FLOW 图线名同此)
  std::string hover;                                     // 悬停 (en 起头 + 详情)
  PortfolioLineSrc src[kPairLines];                      // [PairLine] 线源
  std::shared_ptr<const PortfolioLine> line[kPairLines]; // [PairLine] 线 (非空; 两线源相同 → 同一条)
  float hue = std::numeric_limits<float>::quiet_NaN();   // 色相 ∈ [0, 1) (见 PortfolioDerived::rows); NaN = 未定 (灰); 策略不取
  PortfolioQuote q;                                      // 行情键
  PortfolioLeg val;                                      // 估值腿 (篮子账 / 个股账的一线)
};

// 行组 (右表类别条 = 前 kGroupKid 组; 成分行只挂在展开的篮子行下): 前 kBasketGroups 组 = 篮子目录三组 (BasketGroup), 其后 因子 / 策略 / 成分
enum RowGroup : int { kGroupFactor = kBasketGroups,
                      kGroupStrat,
                      kGroupKid };
inline constexpr int kRowGroups = kGroupKid + 1;
inline constexpr int kPlotOrder[kRowGroups] = {kGroupPool, kGroupInd, kGroupConcept, kGroupKid, kGroupFactor, kGroupStrat}; // 主图画序 (策略在最上层)
// NAV 右表一行的数值列 (前两列下标 = PairLine)
enum NavNum : int { kNumTrade,  // 可成交列区间年化
                    kNumIdeal,  // 理想列区间年化
                    kNumSharpe, // 可成交列区间夏普
                    kNumTurn }; // 年换手
inline constexpr int kNavNums = 4;
static_assert(int{kNumTrade} == int{kTrade} && int{kNumIdeal} == int{kIdeal});
inline constexpr int kNavGood[kNavNums] = {1, 1, 1, -1}; // [NavNum] 好的方向: 年化 / 夏普高好, 年换手低好

// 一行的位置 der.rows[g][r] (g < 0 = 无; VAL 标的同此)
struct PortfolioAt {
  int g = -1;
  size_t r = 0;
  bool operator==(const PortfolioAt &) const = default;
};
// 成分行的选中键 = 前缀 + 全局 id
inline std::string stock_key(uint32_t id) { return "stk_" + std::to_string(id); }

// 一级: 派生 (结果 / 内容 / fixed_impact / 成分组变才重算): 全段. 各线按线源沿用上一份 (PortfolioLineSrc)
struct PortfolioDerived {
  std::vector<misc::Ymd> dates;                            // 日并集轴 (横轴 day 下标)
  std::vector<PortfolioRow> rows[kRowGroups];              // [RowGroup][行]:
                                                           //   组             | 行情键    | 可成交 / 有冲击线           | 理想 / 无冲击线        | 估值腿
                                                           //   指数/行业/概念 | 篮子线    | 篮子账该线合成 (不扣成本)   | 价格指数 (篮子历史该线) | 篮子账该线
                                                           //   因子           | -         | 该因子账扣成本合成          | 同腿不扣               | -
                                                           //   策略           | -         | 全部因子账扣成本合成        | 同腿不扣               | -
                                                           //   成分           | 个股 id   | 个股账该只合成 (后复权收盘, 无成本列 ⇒ 两条同一条)    | 个股账该只
                                                           //   指数 / 行业 / 概念 = [basket_group 序] (源没算成也列, 线空); en = cut_code, cn = tag = 切片名 (超长截断) + .等 / .加, hover = en + 全名;
                                                           //   因子 = [items 序, 只含算成的] 该因子单独的 G20 多头; id = PortfolioEntry::key,
                                                           //     en | cn = 因子文件主干 | 中文名 (超长截断), hover = en + 全名 + 池 层 持有期 冲击段 规范串
                                                           //   策略 = 一行, 组合 G20 多头; en | cn = 组合名 | G20 多头
                                                           //   成分 = 展开的篮子行的成分 (Tape::cons, 权降序; 前 n_cons 行, 右表挂在展开行下) + 其余选中的个股 (收起后照画, 升序);
                                                           //     id = stock_key, en | cn = 代码 | 简称, hover = 代码 + 权占比 + 行业 / 估值
                                                           //   色: 指数 / 行业 / 概念 / 因子四组的行一起按相似度 s = max(成员重合, 残差相关) 建 Ward 树, 叶序相邻两行色相差 ∝ 树上分开处两侧平均 1 − s
                                                           //     (OKLCH 定亮定艳色环); 因子没到齐 / 价格指数未就绪 → 全灰. 右表名列排序 = 色相序 (相似的挨着)
                                                           //     成员重合 = Tape::overlap (只篮子之间), 残差相关 = 理想线末段日收益对全A.加回归的残差相关; 成分 = 按全局 id 黄金角散开
  std::map<std::string, PortfolioAt> index;                // 行 id → 位置 (选中解析)
  int n_ready = 0, n_error = 0, n_wait = 0;                // 已算成 / 算失败 / 未到的因子数
  size_t n_cons = 0;                                       // 成分组前 n_cons 行 = 展开行的成分
  std::vector<std::shared_ptr<const PortfolioBook>> books; // [因子行] 因子账 (因子 / 策略行的腿)
  std::shared_ptr<const PortfolioBook> baskets;            // 篮子账 (篮子行的腿 / 估值腿; 没算成 = 空)
  std::shared_ptr<const PortfolioBook> stocks;             // 个股账 (成分行的腿 / 估值腿; 与篮子账同有同无, 同日轴)
  std::shared_ptr<const Database::Tape> prices;            // 篮子理想线 / 成分表的源 (篮子历史 [basket_line]; 未到 / 源未就绪 = 空)
  std::vector<int32_t> val_at;                             // [日并集轴] 两本账的行 (估值的源下标; −1 = 账无该日 / 没算成)
  uint64_t val_gen = 0;                                    // 估值行 (位置 + 估值腿) / 日轴变的次数 (滚动分位 / 末点分位键)
  PortfolioAt at(const std::string &id) const {
    const auto it = index.find(id);
    return it == index.end() ? PortfolioAt{} : it->second;
  }
  const PortfolioRow &row(const PortfolioAt &a) const { return rows[a.g][a.r]; }
};

// 二级: VAL 标的的滚动分位 (只随标的 / 窗长). 每口收益率 v (EP / BP / SP / CP / DY) 按贵度 −v (升 = 贵) 逐日在过去 N 自然年
// (含当日; N = band_years) 窗内取 kBandQs 分位; 估值史 (首个可算日起) 不满 1 年 → 不画 (不足 N 年先扩张窗)
inline constexpr int kBandYearsDef = 5, kBandYearsMax = 15;
inline constexpr int kBandQ = 7;
inline constexpr double kBandQs[kBandQ] = {0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95};
// 绝对估值等值线的档: 标的全史倍数 m (= 1 / 收益率) 区间两头各剔 kContourTrim 分位 (近 0 收益率的日子倍数极大, 不让它撑开区间), kContours 等分
inline constexpr int kContours = 20;
inline constexpr double kContourTrim = 0.02;
// 标的一口 [日并集轴] (窗内按距今天数加权, 见 PortfolioView.cpp roll_w). 标的全日 ↔ 末点分位 PortfolioPct 各行一日, 分位同一个核 pct_at
struct PortfolioRoll {
  std::vector<float> q[kBandQ]; // 窗内贵度 kBandQs 加权分位处的收益率 (便宜 → 贵 = 降序; NaN = 史不满 1 年 / 账无该日)
  std::vector<float> pct;       // 当日贵度在窗内的加权分位 (%, 高 = 贵; NaN = 同上 / 当日不可算)
  std::vector<uint8_t> band;    // 当日 band 可画 (窗内可算点收益率全 > 0, 价格全有解; 分位照算不受限)
  std::vector<double> levels;   // 等值线倍数 (升序, kContours 档); 空 = 全史不可算
};
using PortfolioRolls = std::array<PortfolioRoll, kPoolVals>; // [PoolVal]
// 二级 (另一份): 各估值行各口截至区间末的末个可算日 (视图按区间起点取舍: 落在区间内 (含起点前收日) 才用)
struct PortfolioLastPct {
  int k = -1;                                            // 该日 [日并集轴]; −1 = 无
  double pct = std::numeric_limits<double>::quiet_NaN(); // 估值分位 (%, 高 = 贵; 同 PortfolioRoll::pct 的定义)
};
using PortfolioLastPcts = std::array<PortfolioLastPct, kPoolVals>; // [PoolVal]
struct PortfolioPct {
  std::vector<PortfolioLastPcts> rows[kRowGroups]; // [RowGroup][行]; 无估值腿的行全无
};

// 三级: 区间视图 (派生 / 滚动分位 / 区间变才重算), 与 PortfolioDerived 逐组对仗
// 线 c 在日段 [d0, d1) 的切片 (不拷数据): 源点 [j0, j1) = 区间前末点 (无则自身首点) + 区间内各点, 首点 x 钉到 x0, y / base (base = 区间起点前收) → 起点 1
struct PortfolioSlice {
  const PortfolioLine *c = nullptr;
  size_t j0 = 0, j1 = 0; // 空 = j0 == j1
  double x0 = 0.0, base = 1.0;
  float ann = std::numeric_limits<float>::quiet_NaN();    // 区间复利年化 (%; 不足一日 = NaN)
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
// VAL 标的的一口 band (PoolVal): 标的可成交线的切片上, x = PortfolioView::band_x
struct PortfolioBand {
  bool ok = false;                       // 区间内有分位点 (否则其余空)
  std::vector<double> y[kBandQ];         // P · v / q_j (v = 当日收益率; v ≤ 0 / q_j ≤ 0 / 无分位 = NaN), 升序 = 便宜 → 贵
  std::vector<std::pair<int, int>> segs; // y 全有限的连续段 [i0, i1) (填色按段画: PlotShaded 不跳 NaN)
  std::vector<double> unit;              // P · v (v ≤ 0 / 不可算 = NaN; 不看分位): 倍数 m (= 1 / v) 的绝对估值等值线 y = unit · m
  std::vector<double> levels;            // 等值线倍数 (= PortfolioRoll::levels)
  double now = 0.0, pct = 0.0;           // 区间末个有分位点的收益率 (EP / BP / SP / CP / DY) / 估值分位 (%, 高 = 贵)
};
struct PortfolioView {
  std::vector<std::array<PortfolioSlice, kPairLines>> slice[kRowGroups]; // [RowGroup][行][PairLine]
  std::vector<std::array<Ranked, kNavNums>> num[kRowGroups];             // [RowGroup][行][NavNum] NAV 右表数值列
  std::vector<std::array<Ranked, kPoolVals>> pct[kRowGroups];            // [RowGroup][行][PoolVal] VAL 表格: 区间末个可算日的估值分位 (%, 高 = 贵)
  std::vector<double> band_x;                                            // VAL 标的可成交线切片的 x (各口 band 共用; 未选 → 空)
  PortfolioBand band[kPoolVals];                                         // [PoolVal] VAL 标的的
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
  uint64_t gen = 0, items = 0;  // PortfolioService::result_gen / PortfolioUIState::items_gen
  uint32_t prices = 0;          // 价格指数在画的发布代 (BasketState feed.shown)
  double impact = 0.0;          // fixed_impact
  size_t infos = 0;             // 基本面档案数 (成分行名 / 悬停; 首次载入)
  std::string open;             // 展开的篮子行
  std::vector<uint32_t> stocks; // 选中的个股 (升序)
  bool operator==(const PortfolioDerKey &) const = default;
};
struct PortfolioRollKey {
  uint64_t val = 0; // PortfolioDerived::val_gen
  int years = 0;    // 窗长
  PortfolioAt pin;  // VAL 标的
  bool operator==(const PortfolioRollKey &) const = default;
};
struct PortfolioPctKey {
  uint64_t val = 0; // PortfolioDerived::val_gen
  int years = 0;    // 窗长
  int d1 = 0;       // 区间末
  bool operator==(const PortfolioPctKey &) const = default;
};
struct PortfolioViewKey {
  uint64_t der = 0, roll = 0, pct = 0; // 派生 gen / 滚动分位 gen / 末点分位 gen
  int d0 = 0, d1 = 0;                  // 区间
  bool operator==(const PortfolioViewKey &) const = default;
};

// 一级: items / name = 当前组合, prices = 篮子历史在画的发布 (BasketState feed.data); open / stocks = 展开的篮子行 / 选中的个股 (成分组);
// d = 上一份 (按线源沿用) → 本份
void MakePortfolioDerived(const std::vector<PortfolioItem> &items, const std::string &name, const PortfolioResult &res,
                          const std::shared_ptr<const Database::Tape> &prices, double fixed_impact, const std::string &open,
                          const std::vector<uint32_t> &stocks, const StockInfos &infos, PortfolioDerived &d);
// 二级: VAL 标的 pin 的估值腿各口 [PoolVal], 窗长 years; 无标的 / 无估值腿 → 空
void MakePortfolioRoll(const PortfolioDerived &d, const PortfolioAt &pin, int years, PortfolioRolls &r);
// 二级: 区间末 d1 (日并集轴下标) 之前各估值行各口的末个可算日, 窗长 years
void MakePortfolioPct(const PortfolioDerived &d, int d1, int years, PortfolioPct &p);
// 三级: 区间 [d0, d1) (日并集轴下标); pin / roll / pct = 二级的
void MakePortfolioView(const PortfolioDerived &d, int d0, int d1, const PortfolioAt &pin, const PortfolioRolls &roll, const PortfolioPct &pct,
                       PortfolioView &v);

} // namespace GUI::Factors
