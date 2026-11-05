// Factors→Portfolio: 因子组合页 + Factors 页 1. 区右侧的两个组合小框.
//   小框 1: 选组合 (<portfolio_dir>/<name>.json) / New / Rename / Del;  小框 2: 当前组合的条目 "(池, 持有期): 名", Add = 加入 Factors 表里高光的
//   权重 (持有期 = Factors 页 Hold), Del = 删选中条目; 失效条目 (因子文件里找不到该规范串等) 红. 组合选择在内存, 内容即文件.
//   Portfolio 页 (对仗 OrderFlow 图 2 的横轴: x = 日下标 · 255 + 行末分钟, 刻度只到日 YY/MM/DD; 不画个股):
//   NAV 右表 = 顶部类别条选一组看 (指数 / 行业 / 因子 / 策略, 四组同形: 一行一对线, 见 NavGroup), 点行 = 选中 (同 VAL 行高亮; 跨组保留) = 主图画该行两条线, 主图例默认只显理想列;
//     默认只选全A等权. 列 = 中文名 (线色; 悬停看名) | 当日涨幅 | Δ5日线 | Δ5日量 | 可成交年化 | 理想年化 | 夏普 (可成交列) | 年换手;
//     近况三列 (Database::Recent; 指数 / 行业 = Assets 篮子的日线 + 新浪当日, 因子 / 策略 = 净值日末点, 无量) 按涨跌着色, 不随区间;
//     其余数值列底色 = 同组同列截面名次 (好绿 → 差红: 年化 / 夏普高好, 年换手低好; 同 VAL 配色); 表头点排序 (组内, 升 → 降 → 原序; VAL 表同)
//   NAV 日内栏 (类别条旁 "日内" 开关, 默认开): 主图上方占左栏 1/3 高 (日内 : 主图 = 1 : 2), 选中的指数 / 行业行同主图线色叠加日内收盘线
//     (Database::RenderBasketIntraday: Assets 篮子同一份, 当日由新浪快照合成; 对首日前收的涨跌幅, 横网格 (含 0 轴) 涨跌幅小字内标在右壁,
//      线名中文小字贴线尾, 读数看十字光标)
//   主图 / 日内栏贴边 (窄内边距, 不标轴名), 图区让给线
//   主图例只列画出的线 (同列, 补空格对齐; 悬停一条看名), 点图例单独隐 / 显一条
//     组合策略线 (有冲击 / 无冲击两条) = 各因子独立回测 (PortfolioService FactorCurve,
//     各自的层, 净值引擎 fold_book: 每行开 1/h̄ 一批 (全日平均满仓), 收益在平仓行实现; 可成交 / 顺延在标签里) 的逐行增量等权聚合: 每个交易日在当日有数据的 n 个因子间等分 (按实现日),
//     因子增量 = 毛收益 − 建仓冲击 − 固定冲击份数 · fixed_impact (占净值), 记在行末 (收益 = 平仓行, 成本 = 建仓行;
//       已实现口径, 与池指数对齐); 净值 = 同一 x 的增量和逐点复利连乘. 单因子线同式 (权 1) ⇒ 单因子线 ≡ 只含它的组合.
//       只算 G20 多头 (无冲击 = 不扣). 成本按 Δw (Stat 契约【换手成本】), 冲击段 = 各因子自己的主段, 不可选:
//       用了 lob.* 叶 = 增仓 × 最小金额段建仓冲击 + 平仓量 × Config::fixed_impact; 否则固定段, 增仓量 / 平仓量都按 fixed_impact;
//       年换手 = 全评估期单边 (倍/年: Σ 增仓量 / 交易日数 × 每年交易日; 组合 = 各因子增仓量按当日份额加权, 不计因子间对冲)
//     指数线 / 行业线 = 篮子目录两组 (shared/Basket.hpp basket_group: 各池整体, all 池两条 = 全A加权 (总市值) / 全A等权, 其余池等权 / SW2021 一级, 总市值加权):
//       只画因子日并集轴范围内, 当日收盘一点; 每条成对 = 可成交 (T+1C 标签: 封板不建仓 / 平仓顺延) + 理想 (价格指数, 不看可成交: Assets 篮子同一份), 两者之差 = 可成交拖累;
//       年换手 = 全评估期单边 BasketCurve::turn[篮子线] (倍/年, 只池整体有; 指数线本身不扣成本)
//     单因子线: 该因子单独的 G20 多头净值, 成对 = 有冲击 (它自己的冲击段) + 无冲击, 年换手 = 它自己的
//     y = 净值 (线性下限钉 0; 可切 log, log 下正常缩放); 底部区间条选日段 [d0, d1) (两边竖把手 / 段内平移, 拖动中吸附到月初即生效, 把手旁标起止日), 各线在区间起点归一 (起点 = 1), 年化按区间复利折算;
//     夏普 = 区间内日收益 (日末净值, 无点的日收益 0) mean / sd × √每年交易日;
//     派生重算 (新结果到) / 主图例显隐 / 右表选中 / log / 区间 任一变 → 重新 fit; 每条可见线在视野内的末点右侧标小字名
//   页: NAV = 上面的资金曲线; VAL = 池估值 (同一布局, 区间 / log 共用; 行 = NAV 指数 / 行业两组的同一份): 右侧类别条 (指数 / 行业) 选一组, 表格单选一个池或申万一级行业 (全A 内按上一行 ind_l1 切;
//     默认 = 组合首个因子所在池; 列 = 中文名 (悬停看名) | 区间末 EP / BP / SP / CP / DY 的 N 年估值分位, 底色 = 该分位在同组 (池 / 行业) 同口内的截面名次, 便宜绿 → 贵红),
//     主图 = 该篮子指数 P (全A加权 / 行业 = 总市值加权, 全A等权 / 其余池 = 等权; 区间起点 = 1, 与 NAV 同一切片) + 同权重的滚动 EP / BP / SP / CP / DY Band
//     (默认只开 BP): 价格 band y_t = P_t · v_t / q_j(t), v = 收益率 (高 = 便宜), q_j(t) = 过去 N 年窗内 v 按贵度 (−v) 的 kBandQs 分位
//     (N = 右图例滑条, 即时重算; 正值窗内 ≡ 倍数口径 P · m_q / m); band 间填色 (最便宜档以下绿 → 最贵档以上红, 低 alpha 渐变), 指数线在上;
//     不可算 / v ≤ 0 / 窗内有 v ≤ 0 的点断开, q_j ≤ 0 的档单独断开;
//     绝对估值等值线 (默认关; 很淡的灰, 不进 fit): 倍数 m = 1 / v (PE / PB / PS / PCF / P/D), y_t = P_t · v_t · m,
//     档 = 该篮子全史倍数区间 (两头剔极端分位) 20 档等分, 不随区间 / 视野变; v ≤ 0 断开; 悬停最近一条高亮 + 提示该档倍数 / 当日倍数 / 指数到该线的涨跌;
//     主图例 = 左上自绘矩阵 (对仗 Inspect 期限结构): 行 = 口, 列 = 当前收益率 | 当前倍数 | N 年分位 | Band | 等值线, 点色块 / 行头 / 列头开关, 悬停高亮
#pragma once

#include "gui/task_database/services/Market.hpp"
#include "gui/task_database/ui/QuotesPanel.hpp"      // BasketState / Recent (NAV 日内栏 + 近况列)
#include "gui/task_factors/services/FactorsFile.hpp" // FactorRow
#include "gui/task_factors/services/PortfolioService.hpp"
#include "shared/Basket.hpp" // kGroupPool / kBasketGroups

#include <algorithm>
#include <array>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace GUI::Factors {

// 一条线 (x 严格升序; x = 日下标 · 255 + 分钟), 全段原值 (净值 / 点位); 归一只在切片
struct PortfolioCurve {
  std::vector<double> x, y;
};

// 成对线的两条 (一行两条, 同色, 理想列淡): 指数 / 行业 = 可成交 (T+1C 标签指数) / 理想 (价格指数); 因子 / 策略 = 有冲击 / 无冲击.
// 行选中 → 主图例里理想列显、可成交列隐 (点图例再显)
enum PairLine : int { kTrade,   // 可成交 / 有冲击
                      kIdeal }; // 理想 / 无冲击
inline constexpr int kPairLines = 2;

// 一行 = 一对线, 全段 (NAV 右表一行; VAL 表一行 = NAV 指数 / 行业行同一份). 主图例列 = cn | 口径 | 区间年化 | 区间夏普 | 年换手 (补空格对齐);
// NAV 右表 = cn | 近况三列 | 两条年化 | 可成交列夏普 | 年换手; en 只在悬停时显示; 排版 / 近况 / 年化 / 夏普在渲染时拼
//   篮子行: 两条 = BasketCurve 可成交指数 / 价格指数 (Assets 篮子日线) 同一投影到日并集轴 = 前收一点 (范围首日开盘; 源首日无) + 轴上每日收盘一点, 原值
struct PortfolioRow {
  std::string id;                                         // 选中键 / VAL 单选键; 线的 ImGui ID = id + 线后缀
  std::string en, cn;                                     // 名 / 中文名
  std::string tag;                                        // 线尾小字 (口径后缀另拼)
  std::string hover;                                      // 悬停 (en 起头 + 详情)
  double turn = std::numeric_limits<double>::quiet_NaN(); // 年换手 (全评估期单边, 倍/年; NaN = 无)
  bool on = false;                                        // 默认选中
  int basket = -1;                                        // 指数 / 行业行: 篮子线 basket_line (BasketCurve / 篮子 Feed 同键; 近况 / 日内 / VAL 取它); 其余 −1
  std::vector<uint32_t> at;                               // 篮子行: [line[kTrade].x] → BasketCurve::dates 下标 (严格升序; 滚动窗看日并集轴以外的全史)
  PortfolioCurve line[kPairLines];
};

// NAV 四组 (右表类别条 / 主图画序同序, 策略在最上层); 前两组 = 篮子目录两组 (BasketGroup 同序)
enum NavGroup : int { kNavPool,
                      kNavInd,
                      kNavFactor,
                      kNavStrat };
inline constexpr int kNavGroups = 4;
static_assert(int{kNavPool} == int{kGroupPool} && int{kNavInd} == int{kGroupInd});
// NAV 右表一行的数值列 (前两列下标 = PairLine)
enum NavNum : int { kNumTrade,  // 可成交列区间年化
                    kNumIdeal,  // 理想列区间年化
                    kNumSharpe, // 可成交列区间夏普
                    kNumTurn }; // 年换手
inline constexpr int kNavNums = 4;
static_assert(int{kNumTrade} == int{kTrade} && int{kNumIdeal} == int{kIdeal});

// Portfolio 页派生 (UI 线程, 结果 / 内容 / fixed_impact 变才重算): 全段. 重算时沿用上一份里没变的:
//   单因子线 = 同一条 FactorCurve 且日轴 / fixed_impact 不变; 篮子行 = 同一份 BasketCurve、同一份价格指数且日轴不变 (basket_gen 不变).
//   组合线恒重算 (份额随当日因子数)
struct PortfolioDerived {
  std::vector<misc::Ymd> dates;                        // 日并集轴 (横轴 day 下标)
  std::vector<PortfolioRow> nav[kNavGroups];           // [NavGroup][行]; on = 只全A等权:
                                                       //   指数 / 行业 = [basket_group 序] (源没算成也列, 线空); en | cn = cut_code | basket_name, tag = cn
                                                       //   因子 = [items 序, 只含算成的] 该因子单独的 G20 多头 (与组合同式); id = PortfolioEntry::key,
                                                       //     en | cn = 因子文件主干 | 中文名, hover = en + 池 层 持有期 冲击段 规范串
                                                       //   策略 = 一行, 组合 G20 多头: 首日开盘前 1 + 每个有增量的 x; en | cn = 组合名 | G20 多头
  int n_ready = 0, n_error = 0;                        // 已算成 / 算失败的因子数
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

// VAL 滚动 band: 每口收益率 v (EP / BP / SP / CP / DY) 按贵度 −v (升 = 贵) 逐日在过去 N 自然年 (含当日; N = band_years) 窗内取 kBandQs 分位;
// 池估值史 (首个可算日起) 不满 1 年 → 不画 (不足 N 年先扩张窗)
inline constexpr int kBandYearsDef = 5, kBandYearsMax = 15;
inline constexpr int kBandQ = 7;
inline constexpr double kBandQs[kBandQ] = {0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95};
// 选中篮子一口的滚动分位 [该行 at] (窗内按距今天数加权, 见 TabPortfolio.cpp roll_w)
struct PortfolioRoll {
  std::vector<float> q[kBandQ]; // 窗内贵度 kBandQs 加权分位处的收益率 (便宜 → 贵 = 降序; NaN = 史不满 1 年)
  std::vector<float> pct;       // 当日贵度在窗内的加权分位 (%, 高 = 贵; NaN = 同上 / 当日不可算)
  std::vector<uint8_t> band;    // 当日 band 可画 (窗内可算点收益率全 > 0, 价格全有解; 分位照算不受限)
};
// VAL 页选中篮子的一口 band (PoolVal): 滚动分位的价格线, x = PortfolioView::band_x
struct PortfolioBand {
  bool ok = false;                       // 区间内有分位点 (否则其余空)
  std::vector<double> y[kBandQ];         // P · v / q_j (v = 当日收益率; v ≤ 0 / q_j ≤ 0 / 无分位 = NaN), 升序 = 便宜 → 贵
  std::vector<std::pair<int, int>> segs; // y 全有限的连续段 [i0, i1) (填色按段画: PlotShaded 不跳 NaN)
  std::vector<double> unit;              // P · v (v ≤ 0 / 不可算 = NaN; 不看分位): 倍数 m (= 1 / v) 的绝对估值等值线 y = unit · m
  std::vector<double> levels;            // 等值线倍数 (升序): 篮子全史倍数区间 (两头剔极端分位) 等分; 空 = 全史不可算
  double now = 0.0, pct = 0.0;           // 区间末个有分位点的收益率 (EP / BP / SP / CP / DY) / 估值分位 (%, 高 = 贵)
};

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
// VAL 表格一格: 篮子一口在切片内末个可算点的估值分位 + 它在同组 (池 / 行业) 同口内的截面名次
struct PortfolioCell {
  double pct = std::numeric_limits<double>::quiet_NaN();  // %, 高 = 贵; NaN = 无
  double rank = std::numeric_limits<double>::quiet_NaN(); // ∈ [0, 1] (0 = 最便宜; 组内只一个 = 0.5)
};

// 区间视图 (派生 / 滚动分位 / 区间变才重算): 与 PortfolioDerived 逐组对仗
struct PortfolioView {
  std::vector<std::array<PortfolioSlice, kPairLines>> nav[kNavGroups];   // [NavGroup][行][PairLine]
  std::vector<std::array<double, kNavNums>> nav_rank[kNavGroups];        // [NavGroup][行][NavNum] 同组同列截面名次 ∈ [0, 1] (1 = 最好; NaN = 不可算)
  std::vector<std::array<PortfolioCell, kPoolVals>> cell[kBasketGroups]; // [BasketGroup][行][PoolVal] VAL 表格
  std::vector<double> band_x;                                            // 选中篮子切片的 x (各口 band 共用; 未选 → 空)
  PortfolioBand band[kPoolVals];                                         // [PoolVal] 选中篮子的
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
// 三级: 派生 → 滚动分位 (只随篮子) → 视图 (随派生 + 滚动分位)
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

// 区间条: [d0, d1) (der.dates 下标; 两边在月初 / 末尾; 日轴变 → 全段) + 拖动态
struct PortfolioRange {
  int d0 = 0, d1 = 0;
  int drag = 0;                 // 拖动中: 0 无 / 1 左把手 / 2 右把手 / 3 段内平移
  double grab_day = 0.0;        // 按下时鼠标所在日 (平移按与它的差算)
  int grab_k0 = 0, grab_k1 = 0; // 按下时两边的月初序号 (平移保持 k1 − k0)
};

struct PortfolioUIState {
  // ---- 组合 (小框) ----
  std::string dir;                     // names 对应的目录 (config.portfolio_dir 变 → 重列)
  std::vector<std::string> names;      // 目录下的组合 (主干名)
  std::string name;                    // 当前组合; 空 = 无
  std::vector<PortfolioEntry> entries; // 当前组合内容 (= 文件)
  std::vector<PortfolioItem> items;    // 解析结果 (TaskFactors 在内容变 / 因子重扫后重解析; 与 entries 同序, 可能落后一帧)
  uint64_t content_gen = 0;            // 换组合 / 改内容 +1
  uint64_t items_gen = 0;              // items 每改一次 +1 (解析 / 建请求时标失效)
  int sel = -1;                        // 小框 2 选中条目
  char name_buf[64] = "";              // New / Rename 的名字
  std::string msg;                     // 上次操作结果
  bool confirm_delete = false;
  // ---- Portfolio 页 ----
  PortfolioStage<PortfolioDerKey, PortfolioDerived> der;
  PortfolioStage<PortfolioRollKey, std::array<PortfolioRoll, kPoolVals>> roll; // [PoolVal] 选中篮子的
  PortfolioStage<PortfolioViewKey, PortfolioView> view;
  PortfolioRange range;
  std::string val_id;             // VAL 选中篮子行 id (空 = 默认: 组合首个因子所在池的等权行, 无因子暂用首行)
  std::string val_for;            // val_id 对应的组合 (换组合 → 回默认)
  int band_years = kBandYearsDef; // VAL 滚动窗 N (年; 右图例滑条)
  bool log_y = false;
  std::map<std::string, bool> show; // NAV 右表选中行: PortfolioRow::id → 画 (缺 = PortfolioRow::on)
  std::set<std::string> reveal;     // 待在主图例里回默认显隐的线 ID (行刚选中 / 回默认; 画到时消费; 理想列显, 可成交列隐)
  std::string vis_key;              // NAV 行选中已按默认设过的组合 (换组合 → 清掉选中回默认)
  std::string shown_key;            // 上帧各线显隐 + log (变了 → 下帧重新 fit)
  bool refit = false;
  float legend_w[2] = {400.0f, 380.0f}; // [NAV, VAL] 右图例表格不截断所需的宽 (上帧 EndTable 量的; 本帧右图例按它定宽)
  int cat[2] = {kNavPool, kGroupPool};  // [NAV, VAL] 右表类别条选中组 (NAV = NavGroup; VAL = BasketGroup)
  bool intraday = true;                 // NAV 日内栏
  Database::BasketState basket;         // 篮子历史 + 当日 (日内栏 / 指数 / 行业行近况)
};

// GUI 线程每帧 (两页都调): 目录变 → 重列, 当前组合不在了 → 选第一个 (载入内容, content_gen +1)
void SyncPortfolioDir(PortfolioUIState &ui, const std::string &dir);
// Factors 页 1. 区右侧两框. view_row = Factors 表高光行 (nullptr = 无); (pool, lv, hold) = Factors 页当前池 / 层 / Hold
void RenderPortfolioBoxes(PortfolioUIState &ui, const FactorRow *view_row, const std::string &pool, LevelId lv, int hold);
// mk = 行情 (已起, ui.basket 本帧已 Sync; nullptr = AssetInfo 未建, 日内栏 / 指数行业近况空着). 返回值: 1 = Recompute (弃缓存重算), -1 = Cancel, 0 = 无
int RenderTabPortfolio(PortfolioService &svc, PortfolioUIState &ui, double fixed_impact, const Database::Market *mk);

} // namespace GUI::Factors
