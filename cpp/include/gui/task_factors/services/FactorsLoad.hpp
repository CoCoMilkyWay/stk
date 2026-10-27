// FactorsLoad — 因子评估的特征库装载层 (四服务共用; 只装数据, 根上的统计归 FactorsProbe, 净值 / 篮子归 FactorsBook): 作用域层 (FactorsScope) 之上.
//   X ↔ Y ↔ B 三侧同一份逐天并行骨架 (read_days): XSpec → XPlanes (因子输入叶) / YSpec → YPlanes (截面门控 + 两腿平面 标签 [hold] /
//   成本 [amt] + 满仓只数) / BPlanes (日层篮子原料: close / 复权 / 股本 / 估值 / 行业); n_full_of 只扫门控出满仓只数;
//   ScopeCache + ensure_xy = 作用域块 (Y 常驻, X 按服务策略留 / 补); labels_of / labels_pick 标签的契约视图 (未预处理, 会话 open 时做; cap = n_full);
//   stat_scope = 结果的作用域头.
#pragma once

#include "factor/Check.hpp" // check::Plane
#include "factor/Stat/Contract.hpp"
#include "gui/task_factors/services/FactorsScope.hpp" // EvalScope / Chunk / Rc
#include "misc/exec.hpp"
#include "shared/Progress.hpp" // Progress

#include <atomic>
#include <cstdint>
#include <map>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace GUI::Factors {

// ---- 装载: X ↔ Y ↔ B 三侧同形 (Spec → Planes, load_x / load_y / load_b 同一份逐天骨架) ----
//   X = 因子输入叶 (随因子变): 叶按各自 ts_valid 门控
//   Y = 门控 + 两腿平面 (只随作用域变): 截面门控 = cs_valid ∧ 当日有成交 (∧ 子池成员); 两腿同一份折叠 (池内 ∧ 两腿有限, 腿按封顶截;
//   可成交 / 顺延已在标签里, 不另读)
//   B = 日层篮子原料 (只 Portfolio 池线): close / 复权 / 股本 / 估值 / 行业, 装完即交 FactorsBook baskets (与同块 Y 合算), 不进作用域块
// 张量形 (三侧同一份; T = days · seg)
struct Shape {
  int T = 0, A = 0, days = 0;
  int seg = 0; // 因子层每日行数 (= FeatureTable::seg)
  size_t n() const { return static_cast<size_t>(T) * A; }
  bool operator==(const Shape &) const = default;
};
// 金额段 (LegCol::key, 与 factor::stat::Holds 对仗)
struct Amts {
  int n = 0;
  int a[factor::stat::kMaxCost] = {};
};
factor::stat::Holds holds_of(const std::vector<int> &keys); // 容量断言 kMaxHold; 非空再 assert_holds 由装载按 seg 做
Amts amts_of(const std::vector<int> &keys);                 // 容量断言 kMaxCost
factor::stat::Holds all_holds(const FeatureTable &ft);      // 字段表全部持有期 (y.labels 序)
Amts all_amts(const FeatureTable &ft);                      // 字段表全部金额段 (y.costs 序)

struct XSpec {
  std::vector<std::string> codes; // 叶 (FeatureTable 叶名; 平面下标 = 此序)
};
struct YSpec {
  factor::stat::Holds hd; // 标签组 (LegCol::key, 字段表里有)
  Amts ad;                // 成本段 (LegCol::key, 字段表里有; 空 = 不装)
  float max_impact = 0.f; // 买入冲击封顶 (Config::max_impact; ad 非空时 > 0): buy = min(原值, 封顶)
  double cap = 0.0;       // 满仓只数 n_full: 0 = 本块门控自算; > 0 = 用它 (多块作业 = 全期池宽 n_full_of, 各块同一个; FactorsEngine plan_job 置)
  std::string key() const;
};
// 全部持有期 + 全部金额段 (Run / Inspect)
YSpec y_full(const FeatureTable &ft, double max_impact);
// 每格平面字节 (check::Plane: float + u8)
inline constexpr double kPlaneBytes = 5.0;
// 宿主装载量 (字节 / 格): X = 叶 5 · codes; Y = 门控 1 + 标签两腿 (lv, sv, m) 5 · hd + 成本两腿 (无 m) 4 · ad
double cell_bytes(const XSpec &s);
double cell_bytes(const YSpec &s);
// B 装载 + 篮子期峰值 (字节 / 格): close 5 + 价 4 + 权重 4 + 复权 4 + 估值 (股本 / 净利 / 权益 / 营收 / 经营现金流 / 股息率) 24 + 行业 1
// + 上一有效 close 行 4 (baskets)
inline constexpr double kBasketCellBytes = kPlaneBytes + 4.0 + 4.0 + 4.0 + 24.0 + 1.0 + 4.0;

// 两腿平面 (标签 / 成本同形): fp16 位 (与落盘同格式, Stat 直接吃) + 有效位 (标签: 池内 ∧ 两腿皆有限; 成本无: 标签有效格恒有限, 其余 0)
struct LegPlane {
  std::vector<uint16_t> v[factor::stat::kLegs];
  std::vector<uint8_t> m;
};
// 宿主平面 → 契约视图 (整张量, 未偏). ry = 预处理的做多标签 r16 (nullptr = 未预处理, 会话 open 时做)
inline factor::stat::Label label_of(const LegPlane &p, const uint16_t *ry) {
  return {{p.v[factor::stat::kLong].data(), p.v[factor::stat::kShort].data()}, p.m.data(), ry};
}
inline factor::stat::Cost cost_of(const LegPlane &p) { return {{p.v[factor::stat::kLong].data(), p.v[factor::stat::kShort].data()}}; }

struct XPlanes {
  Shape sh;
  std::vector<std::string> codes;      // [leaf] = XSpec::codes (平面下标)
  std::vector<factor::check::Plane> v; // [leaf] 值 + 掩码 (按 ts_valid 门控)
  const factor::check::Plane *find(std::string_view code) const;
};
struct YPlanes {
  Shape sh;
  std::vector<uint8_t> gate;    // 截面池掩码 g = 当日在池 ∧ cs_valid ∧ 当日有成交 (ts_valid 任一行 ≠ 0; 整日常量)
  factor::stat::Holds hd;       // [hold] = YSpec::hd
  std::vector<LegPlane> labels; // [hold] 两腿毛收益: 做多 / 做空; 本层行 t 取本层标签列行 t (建仓非两向可成交 → 特征层 NaN → 有效位 0; 平仓顺延已在值里)
  Amts ad;                      // [amt] = YSpec::ad
  std::vector<LegPlane> costs;  // [amt] 两腿建仓冲击 (比例): 买入 (封顶) / 卖出; 采样同标签, 标签有效格恒有限 (装载断言), 无有效位
  double n_full = 0.0;          // 满仓一腿只数 (Stat 契约【持仓口径】的 cap) = 池内平均每行有效资产数 / kGroups: 池宽决定, 与因子无关 (YSpec::cap > 0 → 它)
};
// 全部金额段的成本视图 [amt] (= y.ad 序; cost_rows 的 c, C = y.ad.n)
inline std::vector<factor::stat::Cost> costs_of(const YPlanes &y) {
  std::vector<factor::stat::Cost> c;
  for (const LegPlane &p : y.costs)
    c.push_back(cost_of(p));
  return c;
}
// 日层篮子原料 (load_b; 标签库网格层本层列, 交 FactorsBook baskets)
struct BPlanes {
  Shape sh;
  factor::check::Plane close;        // 本行有成交的后复权 close (估值 / 换手漂移, 跨日除权不进收益)
  std::vector<float> mcap;           // 本行总市值 = close_day × total_shares (加权版的权; close 无效 / 缺股本 → 0)
  std::vector<float> px, shares;     // 本行有成交的 close_day (否则 0) / 总股本 (缺 → NaN)
  std::vector<float> val[kPoolVals]; // [PoolVal] 估值输入 (YCols::val 原值; 缺 → NaN)
  std::vector<uint8_t> ind;          // 行业 ID (SW2021 一级; 0 = 未知 / 缺 / 越界)
};
// 标签的契约视图 (未预处理, ry 空; cap = n_full). 会话 open (cpu / gpu stat_open) 自己预处理
factor::stat::Labels labels_of(const YPlanes &y);
// L 里只取 hd 的那几组 (hd.n = 0 → L 原样; 键须都在 L.hd 里), 预处理位跟着走
factor::stat::Labels labels_pick(const factor::stat::Labels &L, const factor::stat::Holds &hd);

// 逐天并行读 (ex 抢单) es.days[d0, d1) 按 spec 装进 X / Y / B (覆盖; 形 days × A → T, 容量断言在此). 三侧同一份骨架:
// 一张读计划 (analysis::read_plan, 同 (池, 层) 一源, 网格行 level_asof 投影), 列全部 Col::ref(es.pool);
// X 列段 = 叶 (按所在 (池, 层) 投影); Y 列段 = ts_valid | cs_valid | 标签 [hd][kLegs] | 成本 [ad][kLegs];
// B 列段 = ts_valid | close adj_factor total_shares val[kPoolVals] ind_l1 (标签库网格层本层列).
// lr 须按 es.rs.libs 开好. 返回 false = 取消 或 库判废 (调用方查 lr.stale()). 四个装载同一份进度: prog.begin Loading (日数), 每天 publish(1)
bool load_x(const FeatureTable &ft, const EvalScope &es, const LibReaders &lr, size_t d0, size_t d1, const XSpec &spec, XPlanes &X,
            misc::exec::ForkJoin &ex, const std::atomic<bool> &cancel, Progress &prog);
bool load_y(const FeatureTable &ft, const EvalScope &es, const LibReaders &lr, size_t d0, size_t d1, const YSpec &spec, YPlanes &Y,
            misc::exec::ForkJoin &ex, const std::atomic<bool> &cancel, Progress &prog);
bool load_b(const FeatureTable &ft, const EvalScope &es, const LibReaders &lr, size_t d0, size_t d1, BPlanes &B, misc::exec::ForkJoin &ex,
            const std::atomic<bool> &cancel, Progress &prog);
// es.days[d0, d1) 的满仓只数 (与 load_y 的 YPlanes::n_full 同一份门控同式), 只读 ts_valid / cs_valid 两列, 不留平面 (多块作业的全期 cap).
// false = 取消 / 库判废
bool n_full_of(const FeatureTable &ft, const EvalScope &es, const LibReaders &lr, size_t d0, size_t d1, misc::exec::ForkJoin &ex,
               const std::atomic<bool> &cancel, Progress &prog, double &out);

// ---- 作用域块: Y 按 (层, 作用域, 日段, YSpec) 常驻; X 挂在同一块上 (Y 换 → X 清) ----
//   Run = 每轮一块 (Y 跨文件复用, X retain 只留本文件的叶); Inspect = 服务一块 (X 只增); Search = 每块一个
//   块 = 作用域日 [d0, d1) 的行 [0, T); 统计只从 t_lo 起 ([0, t_lo) = 前带 carry, 只给回看 / lag; Search 契约【分块流式】)
struct ScopeCache {
  std::string key; // scope_key; 空 = 未装
  std::vector<misc::Ymd> dates;
  size_t d0 = 0, d1 = 0;
  size_t nd = 0;    // 作用域总日数 (跨块落地平面的行轴 = nd · seg)
  int t_lo = 0;     // 块内统计首行 (= 前带行数, ensure_xy 置)
  uint64_t gen = 0; // 每次 reset +1 (引擎据此判会话作废)
  XPlanes x;
  YPlanes y;
  mutable std::map<std::pair<std::string, factor::Dom>, std::string> dom; // 叶值域缓存 (叶, 严格域) → 越界值 (空 = 合); FactorsEngine plan_forest 填
  void reset() {
    const uint64_t g = gen + 1;
    *this = ScopeCache{};
    gen = g;
  }
  factor::stat::Span span() const { return {y.sh.T, y.sh.A, factor::geo_of(y.sh.seg), static_cast<int>(d0) * y.sh.seg, t_lo}; }
  int rows_all() const { return static_cast<int>(nd) * y.sh.seg; }
};
std::string scope_key(const FeatureTable &ft, const EvalScope &es, size_t d0, size_t d1, const YSpec &spec);
// 作用域块一步装齐块 k (四服务同一份): Y 键 (或日序列) 变 → reset + 装 Y; retain → 丢 codes 以外的叶; 缺的叶补装
// (codes 空 ∧ retain = 只 Y); t_lo = 前带行数. Ok / Cancel / Stale (判废 → 块 reset)
Rc ensure_xy(ScopeCache &c, const FeatureTable &ft, const EvalScope &es, const LibReaders &lr, const Chunk &k, const YSpec &ys,
             const std::vector<std::string> &codes, bool retain, misc::exec::ForkJoin &ex, const std::atomic<bool> &cancel, Progress &prog);
// ensure_xy 之后块上的叶数 (预算): retain 或键 key 不中 → codes; 否则 = 已留 ∪ codes
size_t resident_leaves(const ScopeCache &c, const std::string &key, const std::vector<std::string> &codes, bool retain);
// 叶平面视图 (DAG 输入: 下标 = codes 下标 = Dag::feats 下标); 全部须已装
std::vector<const factor::check::Plane *> leaves(const ScopeCache &c, const std::vector<std::string> &codes);

// 结果的作用域头 (Run 行 / Inspect 结果同一份): 池 / 期 / 形 / 封顶 / 满仓只数 / 后端 / 时刻
StatScope stat_scope(const FactorScope &sc, const EvalScope &es, const YPlanes &y, const char *backend);

} // namespace GUI::Factors
