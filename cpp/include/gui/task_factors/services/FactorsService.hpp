// FactorsService — Factors→Factors 表的单 worker: 扫 <factor_dir>/<pool>/*.json 因子文件 → 解析 / 校验模板与权重表达式
// (factor/Expr.hpp) → (Run 时) 读特征库把因子输入 + 标签装成 [T = days·seg][A] 平面 (seg = 因子层, 见 FactorLevel) → 每权重 Expr → Dag →
// CPU / GPU evaluator 算根平面 → Stat 评估 → 发布一行 + 回写该权重的 stat (stats[i]; expr / weights 不动).
//
// 因子文件 (一因子一文件, 独立落盘, 格式破碎也**绝不删**: 表里标 BROKEN + 原因, 人手动处理):
//   { "type":   "alpha",                                      // 必有: alpha (选股 / 择时, 走 Stat) | beta (风险暴露, 未实现, 留位)
//     "name_cn": "三十分钟动量",                                // 必有: 中文名, 纯汉字 1..10 字 (英文名 = 文件名主干 name_en)
//     "expr":   "CsRank(TsMeanRoll(TsLog(amt), d))",         // 必有: **模板**, 裸键名 d / k / k2 = 待搜参数 (Expr.hpp 自由参数), 数字 = 固定
//     "note":   "…",                                          // 可选, 人 / agent 写的一句话
//     "weights": [                                            // 可选: 参数搜索 (SearchService) 写的全部权重, 每项一个具体表达式, 一行一个
//       { "hold": 30,                                         //   搜索时的持有期 (0 = 手填)
//         "sign": -1,                                         //   可选, ±1 (缺 = +1): −1 = 取反才是好因子, 评估根下插 TsNeg (Expr negated_canon)
//         "expr": "CsRank(TsMeanRoll(TsLog(amt), d=35))" }, … ], //   模板的一个实例 (结构同模板, 固定参数相同, 自由参数落值)
//     —— 以下本服务写 (评估派生, 换掉 / 清空 weights 时一起删) ——
//     "run": { scope…, "frame", "max_impact", "n_full" },     // 全部 stats 共用的口径; 回写时变了 → 别的权重 stat 清成 null
//     "hold_cols": [...], "acc_cols": [...],                  // holds / acc 数组的列名 (StatJson), 与代码不符 → 全部 stats 忽略
//     "stats": [                                              // 与 weights 同长同序 (没评的位置 null), 一行一个, 数值表密排
//       { "time", "valid_pct", "rank_pct", "eval_ms",
//         "holds": [[HoldStat]…],                             //   全部持有期 (毛收益口径), 载入时显示
//         "costs": [{amt, cover, acc: [[CostAcc]…]}…],          //   每金额段 × 每持有期换手成本累加器 (Inspect 净线从它算)
//         "ic_pdf": [{lo, hi, y}…], "alpha_pdf": {lo, hi, y} }, … ] } //   可选 (缺 = 因子表小图空): 每持有期逐行 rIC 分布 / alpha 分布 (MiniPdf)
//   有效 = type 合法 且 name_cn 合法 且 expr 是字串且 parse 过 (算子 / 元数 / 参数值域 / 特征存在且可作输入)
//   且根是归一算子 (Expr.hpp root_frame → 口径 CS / TS, 决定 Stat 怎么算; Stat/Contract.hpp【口径 Frame】).
//   表里一行 = 一个权重 (FactorRow::widx); 模板含自由参数而无 weights → 一行 "未搜索" (不评); 模板本身具体而无 weights → 视作唯一权重 (hold 0).
//   同一具体规范串多行 (跨文件 / 同文件) → 后者标 dup (黄), 仍算 (同文件的在共享 DAG 里是同一个根, Stat 也只算一次).
//
// 线程模型 (gui/util/Service.hpp, 随对象): GUI 线程 Request 覆盖挂起请求 + 取消在跑; UI 持 mutex 读 rows;
// 进度走原子. evaluate = false 只扫描 (进页 / Add / Rescan), = true 扫描 + 装载 + 评估.
//
// 装载: 逐因子文件 (各文件评估期 / A 轴不同, 见 FactorLevel) 把该文件用到的特征 (去重, 跨库跨层 level_asof 投影) + 标签库 ts_valid / cs_valid
// + 全部持有期标签列读进宿主 (逐天并行, 门控 fmeta::valid ∧ isfinite → 值 + 掩码); 标签存 fp16 位 (与落盘同格式, Stat 直接吃).
// 装载前按宿主内存预算断言放得下 (FactorsLoad assert_fits; 放不下 = 该因子层太细 / 评估期太长).
// 两张门控列的分工 (features/MetaFlag.hpp): 特征输入按 ts_valid 门控 (TS 节点要跨进出池的历史); 标签按 cs_valid 门控 (只被 Stat 吃,
// rank(y) 得在池内排); cs_valid 的有效位另成一张截面池掩码平面 g, 原样递给 CS 节点的算子与 Stat (factor/Contract.hpp【截面池 g】:
// 池内统计 / 池外就近取值全在算子内部, 评估器不做二次门控).
// 评估 (Run 的并行方案, 与 search 的解耦): 一个文件的全部权重合成一张共享 DAG (factor::build_forest, 公共子式只算一次), 按拓扑序
// 顺序走节点; CPU 每个节点内部切满所有核 (factor::cpu::run_node + 常驻 ForkJoin, 结果与单线程逐位一致), 根算完立刻全核 Stat, 再按槽计划
// 释放 (内存 = 峰值活槽 × T·A × 5B, 确定); GPU 同一张 DAG, 输入上传一次, 中间量常驻显存 (DevPool), Stat 走常驻会话.
// Stat 的持有期维 hd.n = n_hold ≤ kMaxHold, x 的 rank 只算一次. 标签是毛价格收益 (不含冲击 / 税佣), HoldStat 是毛口径;
// 换手成本 (lb_cost_* 每金额段, 买入按 Config::max_impact 封顶) 用同一份秩再攒一遍 CostAcc (FactorsLoad cost_tiers, 两后端同一份宿主代码:
// CPU 用刚算的 rx, GPU 把 r16(x) 拷回), 落 stat.costs; 净线由 Inspect plot_term 在二级上现算, 因子表 score (排序 / 选代表权重) 由 RowScore 从该 h 的 HoldStat 算,
// Config::fixed_impact 只进公式不进文件. 因子表的全部数据都来自 stat —— Search 只内部用同一份目标挑参数, 不往文件写结论.
// time 列 (eval_ms) = 该因子子树全部节点 wall 之和 (共享节点算给每个用它的因子 = 单独算它要多久).
// 出错策略: 因子文件 / 表达式不合法是正常输入 → 行 error; 特征库 / 契约不一致 → assert.
#pragma once

#include "codec/L2_DataType.hpp" // L2::ValidType
#include "factor/Expr.hpp"
#include "factor/Search/Contract.hpp" // Score / Objective (因子表 score / util 与 Search 同一份目标)
#include "factor/Stat/Contract.hpp"
#include "gui/task_factors/services/OperatorsService.hpp" // RowStatus
#include "gui/util/Service.hpp"
#include "misc/exec.hpp"
#include "shared/Analysis.hpp"   // ReadScope
#include "shared/Feature.hpp"    // Feature
#include "shared/FeatureLib.hpp" // FeatureLib (请求的库清单)

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

struct SharedData;

namespace GUI::Factors {

struct ScopeCache; // FactorsLoad.hpp

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
const FactorLevel &factor_level(int seg); // seg 不在表里 → assert
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

// ---- 字段表视图 (GUI 线程从 Feature 建一次, 每层一份三服务共享, worker 只读; 与池无关) ----
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
const LegCol &leg_of(const std::vector<LegCol> &cols, int key); // key 不在 → assert
std::vector<int> keys_of(const std::vector<LegCol> &cols);      // 全部 key (同序)
// X ↔ Y 两侧 (FactorsLoad 同一对: XSpec / YSpec, XPlanes / YPlanes, load_x / load_y):
//   X = 因子输入叶 = 标签档及 Bar 档 (Lob 标签层才有 lob.*) 不细于网格的各层全部列, 随因子变
//   Y = 门控 / 标签 / 成本 / OHLC = 标签档网格层本层列, 只随作用域变
struct XCols {
  std::vector<FeatCol> leaves; // 全部叶 (可选与不可选)
  const FeatCol *find(std::string_view code) const;
};
// 池估值五口 (整体法收益率口径, 分母同为 Σ市值; FactorsLoad pool_baskets → PoolCurve → Portfolio VAL 页): EP = Σ净利, BP = Σ权益, SP = Σ营收,
// CP = Σ经营现金流, DY = Σ分红 (各 / Σ市值; 高 = 便宜, 分子过 0 连续)
enum PoolVal : int { kValEp,
                     kValBp,
                     kValSp,
                     kValCp,
                     kValDy };
inline constexpr int kPoolVals = 5;
// 篮子指数两权重 (FactorsLoad pool_baskets → PoolCurve → Portfolio NAV 池线 / VAL 价格线)
enum Weight : int { kWeightEq,    // 等权
                    kWeightCap }; // 流通市值加权
inline constexpr int kWeights = 2;
struct YCols {
  Col ts;           // ts_valid (当日有成交)
  Col cs;           // cs_valid (标签 + 截面门控平面)
  Col ohlc[4];      // open high low close (K 线, 不复权): lob_<x> / <x>_<n>min / <x>_day
  Col close_adj;    // 后复权收盘价 (池等权指数): lob_close_adj / close_adj_<n>min / close_adj_day
  Col float_shares; // A 股流通股本 (只日层设; 池流通市值加权指数的权重 = close_day × 本列)
  Col total_shares; // 总股本 / 归母净利 TTM (pl_npp_ttm) / 归母权益 MRQ (bs_eq) / 营业总收入 TTM (pl_rev_ttm) / 经营现金流 TTM (cf_ocf_ttm) /
                    // 股息率 TTM (dy_ttm): 只日层设, 池估值 EP / BP / SP / CP / DY (整体法)
  Col np_ttm, eq_mrq, rev_ttm, ocf_ttm, dy_ttm;
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
};
// LB 列按 code 识别 (LabelLob 生成的字段行): lb_<long|short>_<name> 标签 (name = <n>m / close / t<N> → 持有期键, Stat/Contract.hpp);
// lb_cost_<buy|sell>_<amt>w 建仓冲击成本
FeatureTable BuildFeatureTable(const Feature &feature, int seg);

// 本地时间 "YYYY-MM-DD HH:MM" (stat.time / search.time 落盘用)
std::string now_string();

// ---- 请求 (GUI 线程解析 config, worker 不碰) = 作用域 + 服务自己的 Spec (与 analysis::Request<Spec> 对仗) ----
// 作用域: 三服务 (Run / Inspect / Search) 同一份; 评估期 / A 轴按因子在 worker 解析 (FactorsLoad eval_scope)
struct FactorScope {
  int seg = 0;              // 因子层
  std::string pool;         // 因子池
  std::string factor_dir;   // factor_dir_of(pool, seg)
  PerPool<FeatureLib> libs; // factor_libs
  double max_impact = 0.0;  // Config::max_impact (买入冲击封顶, 成本装载时生效)
  bool gpu = false;
};
// 层 / 池 / 目录 / 封顶 / 后端恒填; 资产轴未就绪 (数据库未扫) / 标签库未建 → false (libs 不可用)
bool MakeFactorScope(const SharedData &data, const std::string &pool, int seg, bool gpu, FactorScope &sc);
template <class Spec>
struct Request {
  FactorScope scope;
  Spec spec;
};
struct RunSpec {
  bool evaluate = false; // false = 只扫描解析 (不读库)
  std::string only_file; // 非空 = 只评这个因子文件的权重 (Search 写完后), 其余行保留文件里的 stat
};
using FactorsRequest = Request<RunSpec>;
// 只扫描恒成; evaluate 要作用域可用 (MakeFactorScope)
bool MakeFactorsRequest(const SharedData &data, const std::string &pool, int seg, bool evaluate, bool gpu, FactorsRequest &req);

// ---- 行 ----
// 因子类型 (文件 type 键): alpha 走 Stat 评估 (口径由根算子定); beta 只留位, 扫描到即标 error
enum class FactorKind : uint8_t { Alpha,
                                  Beta };
inline constexpr const char *kind_name(FactorKind k) { return k == FactorKind::Alpha ? "alpha" : "beta"; }
struct StatScope {
  std::string pool, backend, time;
  misc::Ymd start_date = 0, end_date = 0; // 评估期首末 (所用库落盘日交集)
  int days = 0, T = 0, A = 0;
  double max_impact = 0.0; // 买入冲击封顶 (Config::max_impact, 成本装载时生效)
  double n_full = 0.0;     // 满仓一腿只数 = 池内平均有效资产数 / kGroups (Stat 契约【持仓口径】的 cap, util 分母; 池宽决定, 与因子无关)
};
// 一个金额段的换手成本 = Stat 契约【换手成本】的 CostAcc, 每持有期一份 (Run / Inspect 同一份代码: Stat/Cpu.hpp cost_rows):
// 两腿按真实换手扣, 每 (t, h) 行只有新进者 (t−h 不在该组) 付建仓冲击 + fixed_impact; 均摊成本 cost(腿) 随 h 变 (h 越长留仓越多).
// 买入冲击按 Config::max_impact 封顶后进 (装载时). 吃不满深度的格 (成本 NaN) 不计入 → 大金额段均值偏低, cover 把它亮出来.
// Run 每因子每金额段都攒 (落 stat.costs), Inspect 期限结构净线从它算
struct CostTier {
  int amt = 0;                                            // 万 (= FeatureTable::costs[c].key)
  factor::stat::CostAcc acc[factor::stat::kMaxHold] = {}; // [hd.n], 与 hold 同序
  float cover = 0.f;                                      // Σ 成本有效 / Σ rx 有效 (格数比, 不分组不分 h)
};
// 因子表迷你 PDF: Inspect 同款 KLL PDF (AggPdf) 线性插值到 [lo, hi] 上等距 kMiniPdfPts 点, y 按峰值归一到 [0, 1]; lo == hi → 无 (不画)
inline constexpr int kMiniPdfPts = 32;
struct MiniPdf {
  float lo = 0.f, hi = 0.f;
  float y[kMiniPdfPts] = {};
  bool has() const { return hi > lo; }
};
// 一个根的评估结果 (Run 行 / Inspect 结果同一份; FactorsLoad finish_root 直填 valid% / rank% / hold / costs, scope / eval_ms 归调用方)
struct EvalStat {
  StatScope scope;                       // stat 的作用域
  double eval_ms = 0;                    // 子树算子节点 wall 之和 (与 hold 无关: 因子平面只算一遍, Stat 按持有期复用它)
  float valid_pct = 0.f, rank_pct = 0.f; // 池内有效率 / 可秩率 (FactorsLoad valid_pct_of / rank_pct_of)
  int n_hold = 0;
  factor::stat::HoldStat hold[factor::stat::kMaxHold]; // [hold], 毛收益口径
  std::vector<CostTier> costs;                         // [amt] = FeatureTable::y.costs 序, 每金额段 [n_hold] CostAcc (Inspect 净线从它算)
  const factor::stat::HoldStat *find_hold(int hold_m) const {
    for (int j = 0; j < n_hold; ++j)
      if (hold[j].hold == hold_m)
        return &hold[j];
    return nullptr;
  }
  const CostTier *find_cost(int amt) const {
    for (const CostTier &c : costs)
      if (c.amt == amt)
        return &c;
    return nullptr;
  }
};
// 行身份 (view_file / dup_of): "<file>#<widx>"
inline std::string row_key(std::string_view file, int widx) { return std::string(file) + "#" + std::to_string(widx); }
// 一行 = 一个因子文件的一个权重 (weights[widx]); 文件级问题 (BROKEN / 模板未搜索) 的文件只出一行 widx = −1, 无可评估表达式.
// 文件级字段 (file / name_* / expr_raw / tmpl / note / kind) 同文件各行相同
struct FactorRow : EvalStat {
  std::string file;    // 文件名 (不含目录) = 因子 (父) 身份 (edit_file / Search / 删除)
  int widx = -1;       // weights 下标; −1 = 文件级行
  int hold_src = 0;    // 该权重搜索时的持有期 (0 = 手填: 文件 expr 本身具体且无 weights, 视作唯一权重)
  int sign = 1;        // 该权重的方向 (weights[].sign); −1 → expr 是根下插了 TsNeg 的
  std::string key;     // row_key(file, widx)
  std::string name_en; // = 文件名主干 (去 .json)
  std::string name_cn; // 文件 name_cn 键 (BROKEN 行可能为空)
  FactorKind kind = FactorKind::Alpha;
  factor::stat::Frame frame = factor::stat::Frame::CS; // 有效 alpha 行的口径 (root_frame, 模板与权重同根)
  std::string expr_raw;                                // 文件 expr 原串 (可能不规范 / 不合法)
  std::string tmpl;                                    // 文件 expr 规范串 (模板, 自由参数打裸键名); 空 = 解析失败
  std::string expr;                                    // 该权重的具体规范串 (评估对象, 已含 sign); 空 = 无 (文件级行)
  std::string note;
  std::string error;  // 非空 = BROKEN (文件格式 / 表达式 / 该权重 / 组 id 数据), 文件原样留着
  std::string dup_of; // 非空 = 与该行 (key) 同 hold 同一具体规范串
  int n_ops = 0, n_feats = 0, n_slots = 0;
  RowStatus status = RowStatus::Pending;  // Done = 本轮算过 (evaluate) 或本轮不算 (只扫描)
  bool has_stat = false;                  // hold[][] 有内容 (本轮算的或文件载入的)
  bool stat_from_file = false;            // stat 来自文件 (非本进程算的); EvalStat::eval_ms = time 列
  MiniPdf ic_pdf[factor::stat::kMaxHold]; // [hold] 逐行 rIC (ok 行) 分布, 与 Inspect 左下左半同一组样本
  MiniPdf alpha_pdf;                      // alpha 分布 (截尾, 与 Inspect 左下右半同一份 alpha_dist; 与持有期无关)
  const MiniPdf *find_ic_pdf(int hold_m) const {
    for (int j = 0; j < n_hold; ++j)
      if (hold[j].hold == hold_m)
        return &ic_pdf[j];
    return nullptr;
  }
};

// 因子表的目标分量 (Search/Contract.hpp【目标】, 与 Search 打分同一份 score_of): 该行 stat 在持有期 hold 下的 每期真实毛 LS / rIC / score.
// 无 stat / stat 无该持有期 / n < 3 → ok = false. ric_pow 取契约默认
factor::search::Score RowScore(const FactorRow &r, int hold);

// 一个因子文件 → 行 (每权重一行; 文件级问题一行 widx −1) 追加进 out; 解析 / 校验失败 → error (文件不动). 跨行查重 (dup_of) 不在这里.
// FactorsService::scan 逐文件调; Portfolio 按 (文件, 具体规范串) 找回条目的权重也走它
void ScanFactorFile(const std::filesystem::path &p, const FeatureTable &ft, std::vector<FactorRow> &out);

// 进度面 = 自身 (Published): Scanning → (evaluate) Loading 期 done/total = 天 → Running 期 = 共享 DAG 算子节点
class FactorsService : public Service<FactorsService, FactorsRequest>, public Published {
  friend Service<FactorsService, FactorsRequest>;

public:
  static constexpr const char *kName = "FactorsWorker";
  FactorsService() { Start(); }
  ~FactorsService() { Stop(); }

  void SetFeatureTable(std::shared_ptr<const FeatureTable> t) { feats_ = std::move(t); } // 首个 Request 前 (进页) 调一次
  const FeatureTable &feats() const { return *feats_; }

  int broken() const { return broken_.load(std::memory_order_relaxed); }

  // UI 持 mutex 读
  std::vector<FactorRow> rows;
  FactorsRequest current;
  std::string message; // 本轮说明 (如 "特征库无数据"), 空 = 无

private:
  static constexpr Phase kStart = Phase::Scanning;
  Progress &board() { return *this; }
  bool run(FactorsRequest &req);
  void scan(const FactorsRequest &req, std::vector<FactorRow> &out);
  bool evaluate(const FactorsRequest &req, misc::exec::ForkJoin &ex); // false = 取消
  // 一个因子文件的全部待评权重 (idx = rows 下标, 同文件同模板 → 同叶 → 同评估期 / A 轴): 作用域块补装 + 共享 DAG 走引擎 E
  // (FactorsEngine Engine<Cpu | Gpu>, 本轮一个). false = 取消
  template <class E>
  bool evaluate_file(const FactorsRequest &req, misc::exec::ForkJoin &ex, LibReaders &lr, ScopeCache &sc, E &eng, const std::vector<size_t> &idx,
                     const std::vector<factor::expr::Expr> &exprs, const std::vector<factor::stat::Frame> &frames);

  std::shared_ptr<const FeatureTable> feats_;
  std::unique_ptr<misc::exec::ForkJoin> ex_; // 首个 evaluate 懒建, 随 worker 活
  std::atomic<int> broken_{0};
};

// 英文名 = 文件名主干: 非空, ≤64, 仅 [A-Za-z0-9_]
bool ValidFactorName(std::string_view name);
// 中文名 (文件 name_cn 键): 合法 UTF-8, 纯汉字 (CJK 统一表意文字 U+4E00..U+9FFF), 1..10 字
bool ValidFactorNameCn(std::string_view name);

// 新因子文件: 两个名字合法 (调用方保证, 断言) 且模板合法 (parse 过 + root_frame 过) →
// <factor_dir>/<name>.json = {"type": "alpha", "name_cn": …, "expr": canon, "note"?}.
// 返回文件名; 表达式不合法 / 同名已存在 → 空串 + err. GUI 线程调 (随后 Request(evaluate=false) 重扫)
std::string AddFactorFile(const std::string &factor_dir, const FeatureTable &feats, std::string_view name, std::string_view name_cn,
                          std::string_view expr_src, std::string_view note, std::string &err);

// 编辑已有文件 (file = 表格里的文件名, 含 .json): expr 换成 canon (模板规范串变了 → 丢 weights: 都是旧模板的实例), name_cn / note 覆盖
// (note 空 = 删键), 其他键保留 (BROKEN 非对象文件从空重建); new_name ≠ 主干 → 改名. 返回新文件名; 表达式不合法 / 改名目标已存在 → "" + err.
// 删除: 直接删文件. 两者只在服务空闲时调 (evaluate 会写回文件)
std::string UpdateFactorFile(const std::string &factor_dir, const FeatureTable &feats, std::string_view file, std::string_view new_name,
                             std::string_view name_cn, std::string_view expr_src, std::string_view note, std::string &err);
void DeleteFactorFile(const std::string &factor_dir, std::string_view file);

// 参数搜索落盘 (SearchService worker 调, 服务空闲时): weights 整个换成 ws (每项 {hold, sign, expr}, 无 stat; 调用方随后 Run 重算,
// 因子表 score / util 等全部由 stat 来), 其他键保留. 文件须存在且是对象 (扫描时已解析过)
struct SearchWeight {
  int hold = 0;
  int sign = 1;     // ±1
  std::string expr; // 具体规范串 (模板实例, 不含 sign)
};
void WriteFactorWeights(const std::string &factor_dir, std::string_view file, const std::vector<SearchWeight> &ws);

} // namespace GUI::Factors
