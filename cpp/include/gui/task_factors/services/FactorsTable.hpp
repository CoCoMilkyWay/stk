// FactorsTable — 因子层公共类型 (GUI 线程建, worker 只读; 四服务与装载层共用):
//   因子层 FactorLevel ↔ 网格层 / 标签档 / 因子子目录; 字段表视图 FeatureTable (X 叶 ↔ Y 门控 / 标签 / 成本 / OHLC / 估值列);
//   作用域 FactorScope + 请求 Request<Spec> / FactorRef; 评估结果 EvalStat (StatScope / HoldStat / CostTier / MiniPdf) + AlphaDist.
#pragma once

#include "codec/L2_DataType.hpp" // L2::ValidType
#include "factor/Expr.hpp"
#include "factor/Stat/Contract.hpp"
#include "shared/Analysis.hpp"   // AggPdf
#include "shared/Feature.hpp"    // Feature
#include "shared/FeatureLib.hpp" // FeatureLib (请求的库清单)

#include <cassert>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

struct SharedData;

namespace GUI::Factors {

// ---- 因子层 (factor/Contract.hpp【层几何】: 段长 seg 唯一确定层) ↔ 网格层 / 标签档 / 因子子目录 ----
//   因子层 = 网格 (行 / d 的单位) = 所用特征的最细层; 分类 = 池 × 层, 因子文件在 <config.factor_dir>/<池>/<name>/.
//   标签库 (门控 ts / cs_valid + 标签 + 成本 + OHLC, 恒取网格层本层列, 同行): tier = Lob → 该 Lob 池 (盘口特征 + 冲击成本);
//   tier = Bar → all 池 (无冲击成本), 因子池是 Lob 池时截面再与上池成员 (pool::Panel 网格层 as-of).
//   特征叶写全名 档.层.列 (kLevelToken): lob.* = 因子池的库, bar.* = all 池; 叶可来自任意不细于网格的层 (level_asof 投影).
//   评估期 = 标签库 + 叶所在库 落盘日的交集 (不看 config 区间)
// 档 → 库 (因子池 pool 下): Lob 档列在 pool 库, Bar 档列在 all 库. 叶 / 门控 / 标签 / 成本 / OHLC 同一条
inline constexpr PoolId lib_of(TierId tier, PoolId pool) { return tier == TierId::Lob ? pool : kAllPool; }
struct FactorLevel {
  int seg = 0;                                                           // 每日行数 (Geo::S)
  size_t lvl = 0;                                                        // 网格层 (FeatureLevel)
  TierId tier = TierId::Lob;                                             // 标签档
  const char *name = "";                                                 // 子目录名 / UI 名
  constexpr PoolId lab(PoolId pool) const { return lib_of(tier, pool); } // 标签库
};
inline constexpr FactorLevel kFactorLevels[] = {
    {255, L_1min, TierId::Lob, "min"},
    {49, L_5min, TierId::Bar, "5min"},
    {17, L_15min, TierId::Bar, "15min"},
    {9, L_30min, TierId::Bar, "30min"},
    {5, L_60min, TierId::Bar, "60min"},
    {1, L_1day, TierId::Bar, "day"},
};
const FactorLevel &factor_level(int seg);               // seg 不在表里 → assert
const FactorLevel &factor_level(std::string_view name); // name (FactorLevel::name) 不在表里 → assert
// 池 (档 pool_tier) 能用的因子层: Bar 标签层任何池都能用 (Lob 池 = all 标签 + 池成员门控); Lob 标签层只给 Lob 池
inline constexpr bool level_for_pool(const FactorLevel &l, TierId pool_tier) { return l.tier == TierId::Bar || l.tier == pool_tier; }
// 叶名里的层段 (档.层.列 的中段), 下标 = FeatureLevel
inline constexpr const char *kLevelToken[LEVEL_COUNT] = {"1s", "1m", "5m", "15m", "30m", "60m", "1d"};
// 该层因子目录 = <config.factor_dir>/<pool>/<name> (level_for_pool)
std::string factor_dir_of(const SharedData &data, const std::string &pool, int seg);
// 请求的库清单 (GUI 线程读): 标签库 + all 池 (bar.* 叶 / 池成员); 余池空. 未建库的清单 ready() == false
PerPool<FeatureLib> factor_libs(const SharedData &data, const std::string &pool, int seg);
// 因子目录下的因子文件 (*.json 普通文件, 不进子目录), 路径序; 目录不存在 = 空
std::vector<std::filesystem::path> list_factor_files(const std::string &dir);

// ---- 字段表视图 (GUI 线程从 Feature 建一次, 每层一份四服务共享, worker 只读; 与池无关) ----
// 一列 = (档, 层, 列下标), 库随因子池定 (lib_of): 叶 / 门控 / 标签 / 成本 / OHLC 同一个类型, 进读计划同一个 ref
struct Col {
  TierId tier = TierId::Bar;
  size_t level = 0;
  uint32_t col = 0; // 该 (档, 层) 列下标
  ColRef ref(PoolId pool) const { return {lib_of(tier, pool), static_cast<uint8_t>(level), static_cast<uint16_t>(col)}; }
};
struct FeatCol : Col {
  std::string code; // 叶名 档.层.列
  L2::ValidType vt = L2::ValidType::ALL;
  bool allowed = false; // 可作因子输入 (TS / CS 特征); 标签 / ts_valid / cs_valid 不许 (前视)
};
// 两腿列 (标签 / 成本同形): 标签 key = 持有期键 (Stat/Contract.hpp), 腿 = lb_long_<name> / lb_short_<name>;
// 成本 key = 下单金额段 (万), 腿 = lb_cost_buy_<amt>w / lb_cost_sell_<amt>w
struct LegCol {
  int key = 0;
  Col leg[factor::stat::kLegs];
};
const LegCol *find_leg(const std::vector<LegCol> &cols, int key); // key 不在 → nullptr
const LegCol &leg_of(const std::vector<LegCol> &cols, int key);   // key 不在 → assert
// X ↔ Y ↔ B 三侧 (FactorsLoad: 叶 LeafId / YSpec → XPlanes / YPlanes / BPlanes, load_x / load_y / load_b):
//   X = 因子输入叶 = 标签档及 Bar 档 (Lob 标签层才有 lob.*) 不细于网格的各层全部列, 随因子变
//   Y = 门控 / 标签 / 成本 = 标签档网格层本层列, 只随作用域变
//   B = 篮子 (OHLC close / 复权 / 股本 / 估值 / 行业) = 日层本层列, 只 Portfolio 池线
// 叶的身份 = leaves 下标 (字段表编译期定 → 同层进程内不变; 叶名只在 Dag / 解析边界, 装载 / 缓存 / 引擎全按 LeafId)
using LeafId = uint32_t;
struct XCols {
  std::vector<FeatCol> leaves;                                          // 全部叶 (可选与不可选)
  std::map<std::string, LeafId, std::less<>> index;                     // 叶名 → LeafId
  const FeatCol *find(std::string_view code) const;                     // 不在 → nullptr
  std::vector<LeafId> ids(const std::vector<std::string> &codes) const; // 须都在
};
// 池估值五口 (收益率口径 = 分子 / 市值, 按 Weight 平均: 等权 = 个股均值, 加权 = 整体法 Σ分子 / Σ市值; FactorsLoad load_b → PoolCurve → Portfolio VAL 页):
// EP = 净利, BP = 权益, SP = 营收, CP = 经营现金流, DY = 分红 (高 = 便宜, 分子过 0 连续)
enum PoolVal : int { kValEp,
                     kValBp,
                     kValSp,
                     kValCp,
                     kValDy };
inline constexpr int kPoolVals = 5;
// 各口的日层输入列 (YCols::val 下标 = PoolVal): 归母净利 TTM / 归母权益 MRQ / 营业总收入 TTM / 经营现金流 TTM / 股息率 TTM (DY 的分子 load_b 还原)
inline constexpr const char *kPoolValCols[kPoolVals] = {"pl_npp_ttm", "bs_eq", "pl_rev_ttm", "cf_ocf_ttm", "dy_ttm"};
// 篮子指数 / 估值两权重 (FactorsLoad load_b → PoolCurve → Portfolio NAV 池线 / VAL 价格线 + Band)
enum Weight : int { kWeightEq,    // 等权
                    kWeightCap }; // 总市值加权
inline constexpr int kWeights = 2;
struct YCols {
  Col ts;                     // ts_valid (当日有成交)
  Col cs;                     // cs_valid (标签 + 截面门控平面)
  Col ohlc[4];                // open high low close (K 线, 不复权): lob_<x> / <x>_<n>min / <x>_day
  Col adj_factor;             // 复权因子 (只日层设; 行 D = D+1 日的因子): 日层池篮子换手漂移的后复权收盘 = close_day(D) × 行 D−1 的本列
  Col total_shares;           // 总股本 (只日层设): 池估值的市值 + 池加权指数的权重 = close_day × 本列
  Col val[kPoolVals];         // [PoolVal] 估值输入 (kPoolValCols; 只日层设): 池估值 EP / BP / SP / CP / DY (整体法)
  Col ind_l1;                 // SW2021 一级行业 ID (只日层设; 池估值页的行业切分)
  std::vector<LegCol> labels; // 按 key 升序; long / short 都齐且本层合法 (hold_ok) 才收
  std::vector<LegCol> costs;  // 按 key 升序 (Run / Inspect 每金额段攒 CostAcc; Search 不看成本; Bar 标签无)
};
struct FeatureTable {
  int seg = 0;                      // 因子层 (factor_level(seg))
  const Feature *feature = nullptr; // 元数据 (读计划 analysis::read_plan; SharedData 常驻)
  XCols x;
  YCols y;
  factor::expr::FeatureLookup lookup() const; // 给 Expr::parse 注入 (比网格细的层 → FINER)
  // 因子层 = 最细叶层: 无叶落在网格层 → false + err (该放更粗的层目录). parse 过的模板调
  bool check_grid(const factor::expr::Expr &e, std::string &err) const;
  // 已校验过的规范串 (scan / 构建器出的模板 / 权重 / 取反串) 再解析; 不成 → assert
  factor::expr::Expr reparse(std::string_view canon) const;
};
// LB 列按 code 识别 (LabelLob 生成的字段行): lb_<long|short>_<name> 标签 (name = <n>m / close / t<N>o / t<N>c → 持有期键, Stat/Contract.hpp);
// lb_cost_<buy|sell>_<amt>w 建仓冲击成本
FeatureTable BuildFeatureTable(const Feature &feature, int seg);

// 本地时间 "YYYY-MM-DD HH:MM" (stat.time / search.time 落盘用)
std::string now_string();

// ---- 请求 (GUI 线程解析 config, worker 不碰) = 作用域 + 服务自己的 Spec (与 analysis::Request<Spec> 对仗) ----
// 作用域: 四服务同一份; 评估期 / A 轴按因子在 worker 解析 (FactorsLoad eval_scope)
struct FactorScope {
  int seg = 0;                            // 因子层 (= ft->seg)
  std::string pool;                       // 因子池
  std::string factor_dir;                 // factor_dir_of(pool, seg)
  std::shared_ptr<const FeatureTable> ft; // 该层字段表 (每层建一次, 各请求共享; worker 只读)
  PerPool<FeatureLib> libs;               // factor_libs
  double max_impact = 0.0;                // Config::max_impact (买入冲击封顶, 成本装载时生效)
  bool gpu = false;
};
// 层 / 池 / 目录 / 字段表 / 封顶 / 后端恒填; 资产轴未就绪 (数据库未扫) / 标签库未建 → false (libs 不可用)
bool MakeFactorScope(const SharedData &data, std::shared_ptr<const FeatureTable> ft, const std::string &pool, bool gpu, FactorScope &sc);
template <class Spec>
struct Request {
  FactorScope scope;
  Spec spec;
  bool reload = false; // 弃作用域缓存整体重读 (特征库重算过 / 手动 Compute; 每轮本就重读的服务不看)
};
// 一个因子 (Inspect 请求 / Portfolio 条目同一份): 具体规范串 (已校验, worker 用 scope.ft->reparse 再解析) + 口径
struct FactorRef {
  std::string expr;
  factor::stat::Frame frame = factor::stat::Frame::CS;
};

// ---- 评估结果 (Run 行 / Inspect 结果同一份) ----
struct StatScope {
  std::string pool, backend, time;
  misc::Ymd start_date = 0, end_date = 0; // 评估期首末 (所用库落盘日交集)
  int days = 0, T = 0, A = 0;
  double max_impact = 0.0; // 买入冲击封顶 (Config::max_impact, 成本装载时生效)
  double n_full = 0.0;     // 满仓一腿只数 = 池内平均有效资产数 / kGroups (Stat 契约【持仓口径】的 cap, util 分母; 池宽决定, 与因子无关)
};
// 一个金额段的换手成本 = Stat 契约【换手成本】的 CostAcc, 每持有期一份 (Run / Inspect 同一份代码: Stat/Cpu.hpp cost_rows):
// 两腿按 Δw 扣: 增仓 Δw⁺ 付建仓冲击, 平仓量付 fixed_impact (同一价点交接只算差额); 均摊成本 cost(腿) 随 h 变.
// 买入冲击按 Config::max_impact 封顶后进 (装载时; 标签有效格恒有限, 建仓作废格特征层 NaN).
// Run 每因子每金额段都攒 (落 stat.costs), Inspect 期限结构净线从它算
struct CostTier {
  int amt = 0;                                            // 万 (= FeatureTable::costs[c].key)
  factor::stat::CostAcc acc[factor::stat::kMaxHold] = {}; // [hd.n], 与 hold 同序
};
// 因子表迷你 PDF: Inspect 同款 KLL PDF (AggPdf) 线性插值到 [lo, hi] 上等距 kMiniPdfPts 点, y 按峰值归一到 [0, 1]; lo == hi → 无 (不画)
inline constexpr int kMiniPdfPts = 32;
struct MiniPdf {
  float lo = 0.f, hi = 0.f;
  float y[kMiniPdfPts] = {};
  bool has() const { return hi > lo; }
};
// alpha 分布 (Inspect 左下右半图 / Run 落因子表小图, 同一份 FactorsProbe alpha_dist): 样本 = 池内有效格 (rx 有效 ∧ alpha 有效), 组号 = grp_of(rx)
// (与 Stat 同一组). 与持有期无关
struct AlphaDist {
  analysis::AggPdf pdf;                               // 截尾 [lo, hi] 内样本的 PDF (255 点全落在主体上, 重尾不压扁); 只画图, 矩不用它
  uint64_t n = 0;                                     // 全体样本数 (不截尾, 下同)
  float mean = 0.f, sd = 0.f, skew = 0.f, kurt = 0.f; // 全体矩
  float lo = 0.f, hi = 0.f;                           // 截尾范围 = 全体 ICDF 首末内点 (≈ 0.4% / 99.6% 分位)
  float bound[factor::stat::kGroups - 1] = {};        // G_k | G_{k+1} 边界 = 逐行 (G_k 最大值 + G_{k+1} 最小值) / 2 的行平均 (TS 口径组间交叠, 仅示意)
  float share[factor::stat::kGroups] = {};            // 组占全体比例
  float gmean[factor::stat::kGroups] = {};            // 组均值
  std::vector<float> series;                          // 逐行截面均值 (有样本的行, 时序)
};
// 一个根的评估结果 (Run 行 / Inspect 结果同一份; FactorsProbe fill_eval 填全部: scope / eval_ms 由调用方给, 其余取探针;
// StatJson scope_json + eval_json ⇄ load_eval)
struct EvalStat {
  StatScope scope;                       // stat 的作用域 (FactorsLoad stat_scope)
  double eval_ms = 0;                    // 子树算子节点 wall 之和 (与 hold 无关: 因子平面只算一遍, Stat 按持有期复用它)
  float valid_pct = 0.f, rank_pct = 0.f; // 池内有效率 / 可秩率 (FactorsProbe ValidP / RankP)
  int n_hold = 0;
  factor::stat::HoldStat hold[factor::stat::kMaxHold]; // [hold], 毛收益口径
  factor::stat::CostAcc turn[factor::stat::kMaxHold];  // [hold] 两腿换手量 (只量, imp 恒 0; 与金额段无关, 无段的层也有): 年换手 turn_annual
  std::vector<CostTier> costs;                         // [amt] = FeatureTable::y.costs 序, 每金额段 [n_hold] CostAcc (Inspect 净线从它算)
  MiniPdf ic_pdf[factor::stat::kMaxHold];              // [hold] 逐行 rIC (ok 行) 分布 (FactorsProbe IcPdfP; 因子表小图)
  MiniPdf alpha_pdf;                                   // alpha 分布 (截尾; FactorsProbe AlphaP; 与持有期无关)
  const factor::stat::HoldStat *find_hold(int hold_m) const {
    for (int j = 0; j < n_hold; ++j)
      if (hold[j].hold == hold_m)
        return &hold[j];
    return nullptr;
  }
  const MiniPdf *find_ic_pdf(int hold_m) const {
    const factor::stat::HoldStat *h = find_hold(hold_m);
    return h ? &ic_pdf[h - hold] : nullptr;
  }
  const factor::stat::CostAcc &turn_of(const factor::stat::HoldStat &h) const { // h = 本行 hold[] 之一
    assert(&h >= hold && &h < hold + n_hold);
    return turn[&h - hold];
  }
  const CostTier *find_cost(int amt) const {
    for (const CostTier &c : costs)
      if (c.amt == amt)
        return &c;
    return nullptr;
  }
};

} // namespace GUI::Factors
