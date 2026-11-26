// Table — 因子层公共类型 (各服务与装载层共用):
//   因子层 FactorLevel (FactorLevelId / PerFactorLevel) + 持有期开关仓描述 HoldPoints ↔ 网格层 / 因子子目录 / K 线列;
//   字段表视图 FeatureTable (X 叶 ↔ Y 门控 / 标签 / 成本 + K 线 / 复权 / 估值七叶; 每进程代按层建一次 table_of, 全仓共享);
//   作用域 FactorScope (+ 年数上限 year_cap) + 请求 Request<Spec> / FactorRef; 评估结果 EvalStat + AlphaDist.
#pragma once

#include "disk/Disk.hpp"
#include "factor/Expr.hpp"
#include "factor/Stat/Contract.hpp"
#include "factor/Stat/Fold.hpp"            // HoldStat
#include "factor/Stat/Roll.hpp"            // Roll (因子表 score)
#include "factor/Stat/Turn.hpp"            // CostAcc
#include "features/Storage/FeatureLib.hpp" // FeatureLib (请求的库清单)
#include "math/distribution/PdfSnap.hpp"   // AggPdf
#include "orders/L2DataType.hpp"           // L2::ValidType
#include "shared/Feature.hpp"              // Feature
#include "shared/ReadRequest.hpp"          // reader::Request / read_plan

#include <array>
#include <cassert>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

struct SharedData;
struct Config;

namespace GUI::Engine {

// 表的一行 (算子 / 因子) 在本轮里的进度
enum class RowState : uint8_t { Pending,
                                Running,
                                Done };

// ---- 因子层 (唯一真相 FACTOR_LEVELS: 加层只加一行) ↔ 网格层 / 因子子目录 / K 线列 ----
//   因子层 = 网格 (行 / d 的单位) = 所用特征的最细层; 分类 = 池 × 层 (任何池都能用任何层), 因子文件在 output/factors/<池>/<层>/.
//   标签库 (门控 ts / cs_valid + 标签 + K 线, 恒取网格层本层列, 同行) = 因子池的 Bar 库 (label_lib); 截面 = 该库 cs_valid
//   (all: 当日活跃; 子池: 当日成员 = all Bar 库选股表在 D 之前调仓行选出的).
//   特征叶写全名 档.层.列 (kLevelToken): bar.* / lob.* = 因子池的 Bar / Lob 库; 叶可来自任意不细于网格的层 (level_asof 投影).
//   成本: 固定段恒有; 作用域开盘口成本 (Config::lob_cost) 时另有盘口段 (Lob 库 L_1min 的建仓冲击列, 分钟各层按行末分钟取; 日层无), 评估域并入 Lob 库
//   评估期 = 标签库 + 所用库 落盘日的交集的最近 N 年 (FactorScope::years; 不看 config 区间)
//   层间的差别全在表里 (网格层 / 名 / K 线列名式); 层几何 Geo 由网格层定 (行数 = level_valid_rows, 行末分钟与 level_end_sec 对账),
//   服务 / 装载 / UI 不按层分支
// 档 → 库 (因子池 pool 下): 各档列在 pool 的该档库 (lib_id). 叶 / 门控 / 标签 / 成本 / K 线 / 日频原料同一条
inline LibId label_lib(PoolId pool) { return lib_id(pool, TierId::Bar); } // 标签库 = 因子池的 Bar 库
// 因子层下标 (kFactorLevels, 细 → 粗; 与 PoolId / TierId 对仗)
using FactorLevelId = uint8_t;
struct FactorLevel {
  FactorLevelId id = 0;
  size_t lvl = 0;                                                   // 网格层 (FeatureLevel)
  const char *name = "";                                            // 子目录名 / UI 名
  const char *kline = "";                                           // 本层 K 线列名式 (printf, %s = open / high / low / close)
  int seg = 0;                                                      // 每日行数 (= level_valid_rows(lvl) = Geo::S)
  constexpr factor::Geo geo() const { return factor::geo_of(seg); } // 层几何 (factor/Contract.hpp【层几何】)
};
#define FACTOR_LEVELS(X)          \
  X(L_1min, "min", "%s")          \
  X(L_5min, "5min", "%s_5min")    \
  X(L_15min, "15min", "%s_15min") \
  X(L_30min, "30min", "%s_30min") \
  X(L_60min, "60min", "%s_60min") \
  X(L_1day, "day", "%s_day")
#define FACTOR_LEVEL_ROW(lvl, name, kline) FactorLevel{0, lvl, name, kline, static_cast<int>(level_valid_rows(lvl))},
inline constexpr auto kFactorLevels = [] {
  std::array a{FACTOR_LEVELS(FACTOR_LEVEL_ROW)};
  for (size_t i = 0; i < a.size(); ++i)
    a[i].id = static_cast<FactorLevelId>(i);
  return a;
}();
#undef FACTOR_LEVEL_ROW
inline constexpr size_t kFactorLevelCount = kFactorLevels.size();
inline constexpr const FactorLevel &factor_level(FactorLevelId lv) {
  assert(lv < kFactorLevelCount);
  return kFactorLevels[lv];
}
// 网格层 lvl 的因子层 (不在表里 → assert)
inline constexpr FactorLevelId factor_level_of(size_t lvl) {
  for (const FactorLevel &l : kFactorLevels)
    if (l.lvl == lvl)
      return l.id;
  assert(false && "网格层不是因子层");
  return 0;
}
FactorLevelId factor_level_of(std::string_view name); // name (FactorLevel::name) 不在表里 → assert
// 按因子层下标的数组 (与 PerTier / PerLib 对仗)
template <class X>
struct PerFactorLevel {
  std::array<X, kFactorLevelCount> v{};
  constexpr X &operator[](FactorLevelId l) { return v[l]; }
  constexpr const X &operator[](FactorLevelId l) const { return v[l]; }
};
// 叶名里的层段 (档.层.列 的中段) = kLevelToken (FeatureLevels.hpp, ALL_LEVELS 表), 下标 = FeatureLevel
// 持有期在因子层 lv 上的开 / 清仓 = define/Label.hpp【协议表 1】的一行: 各一个 时点 @ 价 (UI 凡显示开关仓处同一份; 由层表 + Geo 推, 不按层分支)
struct HoldPoint {
  std::string at, px;
};
struct HoldPoints {
  HoldPoint open, close;
};
HoldPoints hold_points(HoldId hold, FactorLevelId lv);
std::string hold_desc(HoldId hold, FactorLevelId lv);  // "开仓: C(层) @ 价, 清仓: 点 @ 价"
std::vector<std::string> row_clocks(FactorLevelId lv); // [seg] 各行的行末时刻 HH:MM (日层 = 15:00)
// 因子目录: 一组因子文件 <名>.json (file 节点, 键定到目录) + 各自的 Search 日志 <名>.log (log 节点, 同键, 在 run/ 下同构位置).
//   正式 = output/factors/<池>/<层>/; Agent 候选 = 会话作用域的 factors/ (gui/task_agent/Bridge.cpp)
struct FactorDir {
  disk::Node file = disk::FactorFile, log = disk::RunSearchLog;
  disk::Key key;
  disk::Key of(std::string_view name) const; // 文件名 (含 .json) → 该文件的键; 不合布局 → assert
  std::filesystem::path path(std::string_view name) const { return disk::path(file, of(name)); }
  std::filesystem::path log_path(std::string_view name) const { return disk::path(log, of(name)); }
  std::string dir() const;                // 目录 (显示用)
  std::vector<std::string> files() const; // 盘上的因子文件名, 名序
  bool operator==(const FactorDir &) const = default;
};
FactorDir factor_dir_of(const std::string &pool, FactorLevelId lv);
// 请求的库清单 (GUI 线程读): 因子池两档库 (Bar = 标签库 / bar.* 叶 / 日频原料; Lob = lob.* 叶 / 盘口段成本); 余库空. 未建库的清单 ready() == false
PerLib<FeatureLib> factor_libs(const std::string &pool);

// ---- 字段表视图 (GUI 线程从 Feature 建一次, 每层一份全部页共享, worker 只读; 与池无关) ----
// 一列 = (档, 层, 列下标), 库随因子池定 (lib_id(池, 档)): 叶 / 门控 / 标签 / 成本 / K 线 / 日频原料同一个类型, 进读计划同一个 ref
inline constexpr uint32_t kNoCol = UINT32_MAX; // 无此列 (固定段的成本列)
struct Col {
  TierId tier = TierId::Bar;
  size_t level = 0;
  uint32_t col = kNoCol; // 该 (档, 层) 列下标
  bool has() const { return col != kNoCol; }
  ColRef ref(PoolId pool) const {
    assert(has());
    return {lib_id(pool, tier), static_cast<uint8_t>(level), static_cast<uint16_t>(col)};
  }
};
struct FeatCol : Col {
  std::string code; // 叶名 档.层.列
  L2::ValidType vt = L2::ValidType::ALL;
  bool allowed = false; // 可作因子输入 (TS / CS 特征); 标签 / ts_valid / cs_valid 不许 (前视)
  bool target = false;  // N 元回归的目标叶 (网格层本层的做多标签列, y.labels 收的持有期; 不可写进表达式, 由 h 推出)
};
// X ↔ Y 两侧装载平面 (XCols / YCols ↔ Load LeafId / YSpec → XPlanes / YPlanes, load_x / load_y → Store::pin):
//   X = 因子输入叶 = Lob 档 (只 min 层有) 及 Bar 档不细于网格的各层全部列 (含日层估值七叶 val[PoolVal])
//   Y = 门控 / 标签 = Bar 档网格层本层列; 成本盘口段 = Lob 档 L_1min 列 (level_asof 投影到网格行)
//   b = K 线 / 复权列定位 (K 线流与 Inspect 导出按列直读); 池成员 / 持仓权 / 行业 / 市值 / 收益 取选股表原料带 pool::Strip
// 叶的身份 = leaves 下标 (字段表编译期定 → 同层进程内不变; 叶名只在 Dag / 解析边界, 装载 / 缓存 / 引擎全按 LeafId)
using LeafId = uint32_t;
struct XCols {
  std::vector<FeatCol> leaves;                                          // 全部叶 (可选与不可选)
  std::map<std::string, LeafId, std::less<>> index;                     // 叶名 → LeafId
  const FeatCol *find(std::string_view code) const;                     // 不在 → nullptr
  std::vector<LeafId> ids(const std::vector<std::string> &codes) const; // 须都在
};
// 池估值七口 (收益率口径 = 分子 / 市值, 按 Weight 平均: 等权 = 个股均值, 加权 = 整体法 Σ分子 / Σ市值; Ledger → Market VAL / MARGIN 页):
//   估值五口 [kValEp, kValRz): EP = 净利, BP = 权益, SP = 营收, CP = 经营现金流, DY = 分红 (高 = 便宜, 分子过 0 连续; 个股不可算 → 不计入)
//   两融两口 [kValRz, kPoolVals): RZ = 融资余额, RQ = 融券余额 (非两融标的计 0 照入平均; 成员全无余额 → NaN)
enum PoolVal : int { kValEp,
                     kValBp,
                     kValSp,
                     kValCp,
                     kValDy,
                     kValRz,
                     kValRq };
inline constexpr int kPoolVals = 7, kMarginVals = kPoolVals - kValRz;
inline constexpr bool margin_val(int m) { return m >= kValRz; }
// 各口的日频输入叶 (Ledger pool_val_leaves 的下标 = PoolVal, bar.1d.<列>): 归母净利 TTM / 归母普通股权益 MRQ / 营业总收入 TTM / 经营现金流 TTM / 股息率 TTM
// / 融资余额 / 融券余额
inline constexpr const char *kPoolValCols[kPoolVals] = {"pl_npp_ttm", "bs_eq", "pl_rev_ttm", "cf_ocf_ttm", "dy_ttm", "rz_bal", "rq_bal"};
// 一个持有期的标签列: lb_<h> 做多收益 / lb_nom_<h> 名义收益 (只偏移型)
struct LabCols {
  Col lv, nv;
};
struct YCols {
  Col ts;                      // ts_valid (当日有成交)
  Col cs;                      // cs_valid (标签 + 截面门控平面)
  Col entry;                   // lb_entry_ok 建仓可成交码 (各持有期共用; Stat 仓位感知 (factor/Stat/Pos.hpp))
  HoldSet holds = 0;           // 本层合法 (hold_ok) 的持有期 = 有标签列的
  Holds list;                  // holds 的有序视图 (UI 按下标切持有期)
  LabCols lab[kHoldCount];     // [HoldId]
  AmtSet amts = 0;             // 固定段 (恒在) + 盘口段 (非日层; 作用域开盘口成本才装, 见 y_spec)
  Col cost[kAmtCount][kSides]; // [AmtId][买 / 卖] Lob 档 L_1min 冲击列 lb_cost_<buy|sell>_<a>w; 固定段无列
  bool has(HoldId h) const { return (holds & hold_bit(h)) != 0; }
};
struct BCols {
  Col ohlc[4];    // open high low close (K 线, 不复权; 列名 FactorLevel::kline)
  Col adj_factor; // 复权因子 (盘后列, 网格行 as-of = 当日因子): 后复权收盘 = 本行 close × 本列
};
struct FeatureTable {
  FactorLevelId lv = 0;             // 因子层
  int seg = 0;                      // = factor_level(lv).seg (张量形 / 解析的每日行数)
  const Feature *feature = nullptr; // 元数据 (读计划 reader::read_plan; SharedData 常驻)
  XCols x;
  YCols y;
  BCols b;
  factor::expr::FeatureLookup lookup() const; // 给 Expr::parse 注入 (比网格细的层 → FINER; 回归目标 = label_leaf)
  // 持有期 hold 的回归目标叶名 (y.lab 的做多列); 本层无该持有期 → 空串
  std::string label_leaf(HoldId hold) const;
  // 因子层 = 最细叶层: 无叶落在网格层 → false + err (该放更粗的层目录). parse 过的模板调 (目标叶不计)
  bool check_grid(const factor::expr::Expr &e, std::string &err) const;
  // 已校验过的规范串 (scan / 构建器出的模板 / 权重) 再解析; 不成 → assert
  factor::expr::Expr reparse(std::string_view canon) const;
  // 模板的自由 h 落为 hold 再解析 (Search / 权重比对按所评持有期; 无自由 h 即 reparse)
  factor::expr::Expr reparse_at(std::string_view canon, HoldId hold) const;
};
// LB 列按协议表 (define/Label.hpp) 拼名定位: Bar 档 lb_<token> / lb_nom_<token> 标签 + lb_entry_ok; Lob 档 lb_cost_<buy|sell>_<a>w 建仓冲击 (盘口段)
FeatureTable BuildFeatureTable(const Feature &feature, FactorLevelId lv);
// 按层缓存的字段表 (每进程代一份, Alpha / Market / Agent 线程安全共享): 首次用到该层时建
std::shared_ptr<const FeatureTable> table_of(const Feature &feature, FactorLevelId lv);

// 本地时间 "YYYY-MM-DD HH:MM" (stat.time / search.time 落盘用)
std::string now_string();

// ---- 请求 (GUI 线程解析 config, worker 不碰) = 作用域 + 服务自己的 Spec (与 reader::Request<Spec> 对仗) ----
// 作用域: 各服务同一份; 评估期 / A 轴按因子在 worker 解析 (Scope domain_of)
inline constexpr int kEvalYears = 1; // 评估期默认只留最近几年 (Alpha 页顶 / Agent Mine 页各自可改)
struct FactorScope {
  FactorLevelId lv = 0;                   // 因子层 (= ft->lv)
  std::string pool;                       // 因子池
  FactorDir dir;                          // factor_dir_of(pool, lv)
  std::shared_ptr<const FeatureTable> ft; // 该层字段表 (每层建一次, 各请求共享; worker 只读)
  PerLib<FeatureLib> libs;                // factor_libs
  int years = 0;                          // 评估期只留最近几年 (0 = 全期: Market 组合)
  double max_impact = 0.0;                // Config::max_impact (买入冲击封顶, 成本装载时生效)
  bool lob_cost = false;                  // Config::lob_cost (盘口段成本; 评估域并入 Lob 库)
  bool gpu = false;
};
// 层 / 池 / 目录 / 字段表 / 年数 / 封顶 / 成本口径 / 后端恒填; 资产轴未就绪 (数据库未扫) / 标签库未建 → false (libs 不可用)
bool MakeFactorScope(const SharedData &data, std::shared_ptr<const FeatureTable> ft, const std::string &pool, int years, bool gpu, FactorScope &sc);
// 评估年数上限 (池 × 因子层; 本次运行内只降不升, 线程安全): 整期一块的作业装不下时记下装得下的最多年数, UI 年数随之封顶 (0 = 未触顶)
void cap_years(const std::string &pool, FactorLevelId lv, int years);
int year_cap(const std::string &pool, FactorLevelId lv);
template <class Spec>
struct Request {
  FactorScope scope;
  Spec spec;
  bool reload = false; // 弃作用域缓存整体重读 (特征库重算过 / 手动 Recompute; 每轮本就重读的服务不看)
};
// 一个因子 (Inspect 请求 / Portfolio 条目同一份): 具体规范串 (已校验, worker 用 scope.ft->reparse 再解析) + 口径 + 所评方向 (权重行的 sign)
struct FactorRef {
  std::string expr;
  factor::stat::Frame frame = factor::stat::Frame::CS;
  factor::stat::Dir dir = factor::stat::kFwd;
};

// ---- 评估结果 (Run 行 / Inspect 结果同一份) ----
struct StatScope {
  std::string pool, backend, time;
  misc::Ymd start_date = 0, end_date = 0; // 评估期首末 (所用库落盘日交集的最近 years 年)
  int years = 0;                          // = FactorScope::years (0 = 全期)
  int days = 0, T = 0, A = 0;
  double max_impact = 0.0; // 买入冲击封顶 (Config::max_impact, 成本装载时生效)
  double n_full = 0.0;     // 满仓一组只数 = 池内平均有效资产数 / kGroups (Stat 契约【持仓口径】的 cap, util 分母; 池宽决定, 与因子无关)
};
// 一个金额段的换手成本 = Stat/Turn.hpp 的 CostAcc (整组层级), 每持有期一份 (Run / Inspect 同一份代码: Stat/Cpu.hpp turn_rows):
// 两端按 Δw 扣: 盘口段 = 两侧量付真实买 / 卖冲击; 固定段 = 两侧量都付固定冲击; 税佣两侧另按 Fee (同一价点交接只算差额); 均摊成本 cost(端, Fee) 随 h 变.
// 两侧冲击按 Config::max_impact 封顶后进 (装载时; 盘口缺 = 封顶).
// Run 每因子每金额段都攒 (落 stat.costs), 年换手取段 0 (各段同一份 Δw), Inspect 期限结构净线从它算
struct CostTier {
  AmtId amt = kAmtFixed;                      // 金额段 (kAmtFixed = 固定段)
  factor::stat::CostAcc acc[kHoldCount] = {}; // [hd.n], 与 hold 同序
};
// 因子表迷你 PDF: Inspect 同款 KLL PDF (AggPdf) 线性插值到 [lo, hi] 上等距 kMiniPdfPts 点, y 按峰值归一到 [0, 1]; lo == hi → 无 (不画)
inline constexpr int kMiniPdfPts = 32;
struct MiniPdf {
  float lo = 0.f, hi = 0.f;
  float y[kMiniPdfPts] = {};
  bool has() const { return hi > lo; }
};
// 尾部分层 (Stat 契约【尾部】, Row::tail): 两端组内更极端的一截, 占该组 tail_frac(j)
using factor::stat::kTails;
// 分层净值线表 (Probe NavP 出, Inspect 左上 / agent eval_factor 的分层图): 分层 = 20 组 + 两端各 kTails 级尾部, LS = 整组 + 各级尾部;
// 全量 = 绝对分层 / 超额分层 / 绝对 LS / 超额 LS 四段 (kNavLines), 只要超额 = 超额分层 / 超额 LS 两段 (kNavExcess)
inline constexpr int kNavLayers = factor::stat::kGroups + factor::stat::kEnds * kTails;
inline constexpr int kNavLs = 1 + kTails;
inline constexpr int kNavLines = 2 * (kNavLayers + kNavLs);
inline constexpr int kNavExcess = kNavLayers + kNavLs;
inline constexpr int layer_tail(int e, int j) { return factor::stat::kGroups + e * kTails + j; }
// alpha 分布 (Inspect 左下右半图 / Run 落因子表小图, 同一份 Probe alpha_dist): 样本 = 池内有效格 (rx 有效 ∧ alpha 有效), 组号 = grp_of(rx)
// (与 Stat 同一组); 按所评方向取值 (反向 = −alpha, 组倒序, 与 oriented 同读法). 与持有期无关
struct AlphaDist {
  AggPdf pdf;                                         // 截尾 [lo, hi] 内样本的 PDF (255 点全落在主体上, 重尾不压扁); 只画图, 矩不用它
  uint64_t n = 0;                                     // 全体样本数 (不截尾, 下同)
  float mean = 0.f, sd = 0.f, skew = 0.f, kurt = 0.f; // 全体矩
  float lo = 0.f, hi = 0.f;                           // 截尾范围 = 全体 ICDF 首末内点 (≈ 0.4% / 99.6% 分位)
  float bound[factor::stat::kGroups - 1] = {};        // G_k | G_{k+1} 边界 = 逐行 (G_k 最大值 + G_{k+1} 最小值) / 2 的行平均 (TS 口径组间交叠, 仅示意)
  float share[factor::stat::kGroups] = {};            // 组占全体比例
  float gmean[factor::stat::kGroups] = {};            // 组均值
  std::vector<float> series;                          // 逐行截面均值 (有样本的行, 时序)
};
// 一个持有期一个自然年的分层统计 (Probe YearP 逐日攒、按年合; 已定向, 毛收益): 与 HoldStat 同口径, 只取判稳定性的几项
struct YearStat {
  int year = 0, n = 0, wins = 0;         // 年 / 年内有统计 (ok) 的行数 / 年内收尾的滚动窗数
  float mono = 0.f, ls = 0.f, ric = 0.f; // 分层单调性 / 每期毛多空均值 / rIC 均值
  float ric_med = 0.f;                   // 年内收尾的滚动窗 rIC 的中位 (wins = 0 → 0)
  float turn = 0.f;                      // 年换手 (Probe TurnP; 倍, 两端单边均, 段 0): 年内 Σ Δw⁺ / 年内统计行 × span_per_year, 不足一年的年同样年化
};
// 一个根的评估结果 (Run 行 / Inspect 结果同一份; Probe fill_eval 填全部: scope / eval_ms 由调用方给, 其余取探针;
// StatJson scope_json + eval_json ⇄ load_eval)
struct EvalStat {
  StatScope scope;                       // stat 的作用域 (Load stat_scope)
  double eval_ms = 0;                    // 子树算子节点 wall 之和 (与 hold 无关: 因子平面只算一遍, Stat 按持有期复用它)
  float valid_pct = 0.f, rank_pct = 0.f; // 池内有效率 / 可秩率 (Probe CoverP)
  int n_hold = 0;
  factor::stat::HoldStat hold[kHoldCount]; // [hold], 毛收益口径
  std::vector<CostTier> costs;             // 金额段升序 (首个 = 固定段), 每金额段 [n_hold] CostAcc (年换手 / Inspect 净线从它算)
  MiniPdf ic_pdf[kHoldCount];              // [hold] 逐行 rIC (ok 行) 分布 (Probe IcPdfP; 因子表小图)
  std::vector<YearStat> years[kHoldCount]; // [hold] 分年 (年升序, 只列有统计行 ≥ 3 的年)
  factor::stat::Roll roll[kHoldCount];     // [hold] 滚动窗统计 (Probe YearP; 中位 rIC = 因子表 score, Stat/Roll.hpp)
  MiniPdf alpha_pdf;                       // alpha 分布 (截尾; Probe AlphaP; 与持有期无关)
  const factor::stat::HoldStat *find_hold(HoldId hold_m) const {
    for (int j = 0; j < n_hold; ++j)
      if (hold[j].hold == hold_m)
        return &hold[j];
    return nullptr;
  }
  const factor::stat::Roll &roll_of(const factor::stat::HoldStat &h) const { // h = 本行 hold[] 之一
    assert(&h >= hold && &h < hold + n_hold);
    return roll[&h - hold];
  }
  const MiniPdf *find_ic_pdf(HoldId hold_m) const {
    const factor::stat::HoldStat *h = find_hold(hold_m);
    return h ? &ic_pdf[h - hold] : nullptr;
  }
  const std::vector<YearStat> *find_years(HoldId hold_m) const {
    const factor::stat::HoldStat *h = find_hold(hold_m);
    return h ? &years[h - hold] : nullptr;
  }
  const factor::stat::CostAcc &turn_of(const factor::stat::HoldStat &h) const { // h = 本行 hold[] 之一; 换手量各段同一份, 取段 0
    assert(&h >= hold && &h < hold + n_hold && !costs.empty());
    return costs.front().acc[&h - hold];
  }
};

} // namespace GUI::Engine