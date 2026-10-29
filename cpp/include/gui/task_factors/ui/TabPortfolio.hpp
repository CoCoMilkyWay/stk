// Factors→Portfolio: 因子组合页 + Factors 页 1. 区右侧的两个组合小框.
//   小框 1: 选组合 (<portfolio_dir>/<name>.json) / New / Rename / Del;  小框 2: 当前组合的条目 "(池, 持有期): 名", Add = 加入 Factors 表里高光的
//   权重 (持有期 = Factors 页 Hold), Del = 删选中条目; 失效条目 (因子文件里找不到该规范串等) 红. 组合选择在内存, 内容即文件.
//   Portfolio 页 (对仗 OrderFlow 图 2 的横轴: x = 日下标 · 255 + 行末分钟, 刻度只到日 YY/MM/DD; 不画个股):
//   NAV 右表 = 分组 (因子 / 指数 / 策略) 一行一个, 点行 = 选中 (同 VAL 行高亮) = 主图画该行全部线 (指数 = 可成交 + 理想, 因子 / 策略 = 有冲击 + 无冲击);
//     列 = 名 (线色) | 中文名 | 可成交年化 | 理想年化 | 尾 (三组都是年换手), 年化底色 = 同组同列截面名次 (高绿 → 低红, 同 VAL 配色); 表头点排序 (组内, 升 → 降 → 原序; VAL 表同)
//   主图例只列画出的线 (同列, 补空格对齐), 点图例单独隐 / 显一条
//     组合策略线 (有冲击 / 无冲击两条) = 各因子独立回测 (PortfolioService FactorCurve,
//     各自的层, 净值引擎 fold_book: 每行开 1/h̄ 一批 (全日平均满仓), 收益在平仓行实现; 可成交 / 顺延在标签里) 的逐行增量等权聚合: 每个交易日在当日有数据的 n 个因子间等分 (按实现日),
//     因子增量 = 毛收益 − 建仓冲击 − 固定冲击份数 · fixed_impact (占净值), 记在行末 (收益 = 平仓行, 成本 = 建仓行;
//       已实现口径, 与池指数对齐); 净值 = 同一 x 的增量和逐点复利连乘. 单因子线同式 (权 1) ⇒ 单因子线 ≡ 只含它的组合.
//       只算 G20 多头 (无冲击 = 不扣, 细线; 策略行默认选中). 成本按 Δw (Stat 契约【换手成本】), 冲击段 = 各因子自己的最小段, 不可选:
//       Lob 层 = 增仓 × 最小金额段建仓冲击 + 平仓量 × Config::fixed_impact; Bar 层 = 固定段, 增仓量 / 平仓量都按 fixed_impact;
//       尾 = 年换手 (全评估期单边, 倍/年: Σ 增仓量 / 交易日数 × 每年交易日; 组合 = 各因子增仓量按当日份额加权, 不计因子间对冲)
//     池指数线 (日层篮子 0, 每池一条等权; all 池两条 = 全A加权 (总市值) / 全A等权): 只画因子日并集轴范围内, 当日收盘一点; 默认只选全A等权;
//       每条成对 = 可成交 (T+1C 标签: 封板不建仓 / 平仓顺延) + 理想 (后复权收盘价收益, 不看可成交; 同色淡线), 两者之差 = 可成交拖累;
//       行选中时主图例默认只显理想, 可成交点图例显;
//       尾 = 年换手 (全评估期单边 PoolCurve::turn[该线权重], 倍/年; 指数线本身不扣成本)
//     单因子线 (默认全关): 该因子单独的 G20 多头净值, 成对 = 有冲击 (它自己的冲击段) + 无冲击 (同色淡线), 尾 = 它自己的年换手
//     y = 净值 (线性下限钉 0; 可切 log, log 下正常缩放); 底部区间条选日段 [d0, d1) (两边竖把手 / 段内平移, 拖动中吸附到月初即生效, 把手旁标起止日), 各线在区间起点归一 (起点 = 1), 年化按区间复利折算;
//     主图例显隐 / 右表选中 / log / 区间 任一变 → 重新 fit; 每条可见线在视野内的末点右侧标小字名
//   页: NAV = 上面的资金曲线; VAL = 池估值 (同一布局, 区间 / log 共用): 右侧表格单选一个池或申万一级行业 (全A 内按上一行 ind_l1 切;
//     默认 = 组合首个因子所在池; 格 = 区间末 EP / BP / SP / CP / DY 的 N 年估值分位, 底色 = 该分位在同组 (池 / 行业) 同口内的截面名次, 便宜绿 → 贵红),
//     主图 = 该篮子指数 P (全A加权 / 行业 = 总市值加权, 全A等权 / 其余池 = 等权; 区间起点 = 1, 与 NAV 同一切片) + 同权重的滚动 EP / BP / SP / CP / DY Band
//     (主图例开关, 默认只开 EP): 价格 band y_t = P_t · v_t / q_j(t), v = 收益率 (高 = 便宜), q_j(t) = 过去 N 年窗内 v 按贵度 (−v) 的 kBandQs 分位
//     (N = 右图例滑条, 即时重算; 正值窗内 ≡ 倍数口径 P · m_q / m); band 间填色 (最便宜档以下绿 → 最贵档以上红, 低 alpha 渐变), 指数线在上;
//     不可算 / v ≤ 0 / 窗内有 v ≤ 0 的点断开, q_j ≤ 0 的档单独断开
#pragma once

#include "gui/task_factors/services/PortfolioService.hpp"

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

// 一条图例线, 全段. 主图例列 = en | cn | kind | 区间年化 | note (补空格对齐); NAV 右表一行 = 行首线 (+ 成对线) 的 en | cn | 两口年化 | note; 排版 / 年化在渲染时拼
struct PortfolioLine {
  std::string id;     // ImGui ID (###id) / 选中键 (行首线) / 单选键
  std::string en, cn; // 名 / 中文名
  std::string kind;   // 口径 (策略: 有冲击 / 无冲击; 池线: 可成交 / 理想; 单因子: 冲击段)
  std::string note;   // 尾 (池线: 年换手; 单因子: 池 层 持有期)
  std::string tag;    // 线尾小字
  bool on = false;    // 默认选中 (只看行首线)
  bool hide = false;  // 行选中时在主图例里默认隐 (池可成交线; 点图例显)
  PortfolioCurve c;
};
// 单因子线 = 线 + 它的条目 (右表悬停: 池 / 层 / 持有期 / 冲击段 / 规范串)
struct PortfolioFactor : PortfolioLine {
  PortfolioItem item;
  std::string amt; // 冲击段 (amt_name)
};
// 篮子线 (NAV 池线 / VAL 篮子同形) = 线 + 它的源: PoolCurve 篮子 k 权重 w 的指数投影到日并集轴
//   = 前收一点 (范围首日开盘; 源首日无) + 轴上每日收盘一点, 原值
struct PortfolioBasket : PortfolioLine {
  std::shared_ptr<const PoolCurve> pc; // 全史 (滚动窗要看日并集轴以外的历史); 空 = 池没算成 (c 空)
  size_t k = 0;                        // 篮子号 (0 = 池整体, ≥ 1 = 行业)
  Weight w = kWeightEq;
  bool px = false;          // 源 = 收盘价指数 (PoolCurve::index_px, 不看可成交); 否则可成交标签指数 (index)
  std::vector<uint32_t> at; // [c.x] → pc->dates 下标 (严格升序)
};

// Portfolio 页派生 (UI 线程, 结果 / 内容 / fixed_impact 变才重算): 全段
struct PortfolioDerived {
  std::vector<misc::Ymd> dates;         // 日并集轴 (横轴 day 下标)
  PortfolioLine strat[kImpacts];        // 组合策略 (G20 多头, 右表同一行, 行首 = 有冲击): 首日开盘前 1 + 每个有增量的 x; en | cn | kind = 组合名 | G20 多头 | 冲击口径, note = 年换手
  std::vector<PortfolioFactor> factors; // [items 序, 只含算成的] 成对 (右表同一行, 同池线): 该因子单独的 G20 多头有冲击 (偶下标, 行首) + 无冲击 (奇下标), 与组合同式;
                                        //   id = PortfolioEntry::key + kImpactId, en | cn | kind | note = 因子文件主干 | 中文名 | 冲击口径 | 年换手
  std::vector<PortfolioBasket> pools;   // [池线序] 篮子 0, 成对 (右表同一行): 可成交标签线 (偶下标, 行首) + 同池同权的理想收盘价线 (px, 奇下标);
                                        //   en | cn | kind = 池名 | 中文名 | 可成交 / 理想, note = 年换手, on = 全A等权 (仅可成交线), hide = 可成交线
  std::vector<PortfolioBasket> vals;    // [feature_pools 序, 只含算成的池; all 池两条 = 全A加权 / 全A等权, 其余池等权] 后接 [SW2021 一级 1..31] (全A 内切, all 池算成才有, 总市值加权);
                                        //   价格 / 估值同权重; id = 单选键 (池名 + kWeightId / "sw<ID>"), en | cn = 池名 | 中文名 (all 池 = kWeightName) / sw<ID> | 行业名,
                                        //   tag = 池名 (all 池 = kWeightName) / 行业名
  size_t n_val_pools = 0;               // vals 前 n 个是池
  int n_ready = 0, n_error = 0;         // 已算成 / 算失败的因子数
};

// VAL 滚动 band: 每口收益率 v (EP / BP / SP / CP / DY) 按贵度 −v (升 = 贵) 逐日在过去 N 自然年 (含当日; N = band_years) 窗内取 kBandQs 分位;
// 池估值史 (首个可算日起) 不满 1 年 → 不画 (不足 N 年先扩张窗)
inline constexpr int kBandYearsDef = 5, kBandYearsMax = 15;
inline constexpr int kBandQ = 7;
inline constexpr double kBandQs[kBandQ] = {0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95};
// 选中篮子一口的滚动分位 [der.vals[vs].c.x] (窗内按距今天数加权, 见 TabPortfolio.cpp roll_w)
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
  double now = 0.0, pct = 0.0;           // 区间末个有分位点的收益率 (EP / BP / SP / CP / DY) / 估值分位 (%, 高 = 贵)
  double q[kBandQ] = {};                 // 同点各档的收益率
};

// 曲线 c 在 [x0, x1) 的切片 (不拷数据): 源点 [j0, j1) = 区间前末点 (无则自身首点) + 区间内各点, 首点 x 钉到 x0, y / base (base = c.y[j0]) → 起点 1
struct PortfolioSlice {
  const PortfolioCurve *c = nullptr;
  size_t j0 = 0, j1 = 0; // 空 = j0 == j1
  double x0 = 0.0, base = 1.0;
  float ann = 0.f; // 区间复利年化 (%; 不足两点 = 0)
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
  PortfolioSlice strat[kImpacts];                         // [Impact]
  std::vector<PortfolioSlice> factors, pools, vals;       // [der.factors] / [der.pools] / [der.vals]
  std::vector<double> factor_rank, pool_rank;             // [der.factors] / [der.pools] 区间年化在同组同口 (可成交 / 理想各自) 内的截面名次 ∈ [0, 1] (0 = 最低; NaN = 不可算)
  std::vector<std::array<PortfolioCell, kPoolVals>> cell; // [der.vals][PoolVal]
  std::vector<double> band_x;                             // 选中篮子切片的 x (各口 band 共用; 未选 → 空)
  PortfolioBand band[kPoolVals];                          // [PoolVal] 选中篮子的
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
// 三级: 派生 → 滚动分位 → 视图
struct PortfolioDerKey {
  uint64_t gen = 0, items = 0; // PortfolioService::result_gen / PortfolioUIState::items_gen
  double impact = 0.0;         // fixed_impact
  bool operator==(const PortfolioDerKey &) const = default;
};
struct PortfolioRollKey {
  uint64_t der = 0;      // 派生 gen
  int years = 0, vs = 0; // 窗长 / VAL 选中篮子 (der.vals 下标, −1 = 无)
  bool operator==(const PortfolioRollKey &) const = default;
};
struct PortfolioViewKey {
  uint64_t roll = 0;  // 滚动分位 gen
  int d0 = 0, d1 = 0; // 区间
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
  std::string val_pool;           // VAL 选中篮子 id (空 = 默认: 组合首个因子所在池, 未算出前暂用首个池)
  std::string val_for;            // val_pool 对应的组合 (换组合 → 回默认)
  int band_years = kBandYearsDef; // VAL 滚动窗 N (年; 右图例滑条)
  bool log_y = false;
  std::map<std::string, bool> show; // NAV 右表选中行: 行首线 PortfolioLine::id → 画 (缺 = PortfolioLine::on)
  std::set<std::string> reveal;     // 待在主图例里回默认显隐的线 id (行刚选中 / 回默认; 画到时消费; PortfolioLine::hide 隐, 其余显)
  std::string vis_key;              // 策略 / 池行选中已按默认设过的组合 (换组合 → 清掉它们的选中回默认)
  std::string shown_key;            // 上帧各线显隐 + log (变了 → 下帧重新 fit)
  bool refit = false;
  float legend_w[2] = {400.0f, 380.0f}; // [NAV, VAL] 右图例表格不截断所需的宽 (上帧 EndTable 量的; 本帧右图例按它定宽)
};

// GUI 线程每帧 (两页都调): 目录变 → 重列, 当前组合不在了 → 选第一个 (载入内容, content_gen +1)
void SyncPortfolioDir(PortfolioUIState &ui, const std::string &dir);
// Factors 页 1. 区右侧两框. view_row = Factors 表高光行 (nullptr = 无); (pool, lv, hold) = Factors 页当前池 / 层 / Hold
void RenderPortfolioBoxes(PortfolioUIState &ui, const FactorRow *view_row, const std::string &pool, LevelId lv, int hold);
// 返回值: 1 = Recompute (弃缓存重算), -1 = Cancel, 0 = 无
int RenderTabPortfolio(PortfolioService &svc, PortfolioUIState &ui, double fixed_impact);

} // namespace GUI::Factors
