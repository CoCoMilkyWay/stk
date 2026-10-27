// FactorsLoad — 因子评估的特征库装载层 (四服务共用; 只装数据, 根上的统计归 FactorsProbe, 净值 / 篮子归 FactorsBook): 作用域层 (FactorsScope) 之上.
//   X ↔ Y ↔ B 三侧同一份逐天并行骨架 (read_days, 环境 Io): 叶 LeafId → XPlanes (因子输入叶) / YSpec → YPlanes (截面门控 + 两腿平面 标签 [hold] /
//   成本 [amt] + 满仓只数) / 篮子 → BPlanes (篮子原料: close / 复权 / 股本 / 估值 / 行业); n_full_of 只扫门控出满仓只数;
//   Block + ensure_block = 块 (BlockSpec 三侧: Y / B 常驻, X 按服务策略留 / 补); labels_of / labels_pick 标签的契约视图 (未预处理, 会话 open 时做; cap = n_full);
//   stat_scope = 结果的作用域头.
#pragma once

#include "factor/Check.hpp" // check::Plane
#include "factor/Stat/Contract.hpp"
#include "gui/task_factors/services/FactorsScope.hpp" // Domain / Chunk / Rc
#include "misc/exec.hpp"
#include "shared/Progress.hpp" // Progress

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <iterator>
#include <map>
#include <string>
#include <utility>
#include <vector>

namespace GUI::Factors {

// ---- 装载: X ↔ Y ↔ B 三侧同形 (XCols / YCols / BCols → XPlanes / YPlanes / BPlanes, load_x / load_y / load_b 同一份逐天骨架) ----
//   X = 因子输入叶 (随因子变): 叶按各自 ts_valid 门控
//   Y = 门控 + 两腿平面 (只随评估域变): 截面门控 = cs_valid ∧ 当日有成交 (∧ 子池成员); 两腿同一份折叠 (池内 ∧ 两腿有限, 腿按封顶截;
//   可成交 / 顺延已在标签里, 不另读)
//   B = 篮子原料 (Portfolio 池线): 本层 close + 日频复权 / 股本 / 估值 / 行业 (as-of), BlockSpec::b 时随 Y 进块, 块钩子交 FactorsBook baskets
// 张量形 (三侧同一份; T = days · seg)
struct Shape {
  int T = 0, A = 0, days = 0;
  int seg = 0; // 因子层每日行数 (= FeatureTable::seg = Geo::S)
  size_t n() const { return static_cast<size_t>(T) * A; }
  bool operator==(const Shape &) const = default;
};
// 键集 (LegCol::key): 持有期 Holds ↔ 金额段 Amts 同形 (factor::stat::Keys)
using Holds = factor::stat::Holds;
using Amts = factor::stat::Amts;
// 键表 → 键集 K (容量断言 kMaxHold / kMaxCost; 非空 Holds 再 assert_holds 由装载按 seg 做)
template <class K>
K keys_of(const std::vector<int> &keys) {
  K o;
  assert(keys.size() <= std::size(o.k) && "键数超 Stat 容量 (kMaxHold / kMaxCost)");
  o.n = static_cast<int>(keys.size());
  std::copy(keys.begin(), keys.end(), o.k);
  return o;
}
// 两腿列组的全部键 (cols 序; 字段表 y.labels → Holds / y.costs → Amts)
template <class K>
K keys_of(const std::vector<LegCol> &cols) {
  std::vector<int> keys;
  for (const LegCol &c : cols)
    keys.push_back(c.key);
  return keys_of<K>(keys);
}

struct YSpec {
  Holds hd;               // 标签组 (LegCol::key, 字段表里有)
  Amts ad;                // 成本段 (LegCol::key, 字段表里有; 空 = 不装; 固定段无平面)
  float max_impact = 0.f; // 买入冲击封顶 (Config::max_impact; 有盘口段时 > 0): buy = min(原值, 封顶)
  double cap = 0.0;       // 满仓只数 n_full: 0 = 本块门控自算; > 0 = 用它 (多块作业 = 全期池宽 n_full_of, 各块同一个; FactorsEngine plan_job 置)
  std::string key() const;
};
// 全部持有期 + 全部金额段 (Run / Inspect)
YSpec y_full(const FeatureTable &ft, double max_impact);
// 块规格 (X ↔ Y ↔ B 三侧各一项): X = 叶 / Y = YSpec / B = 是否装篮子原料
struct BlockSpec {
  std::vector<LeafId> x;
  YSpec y;
  bool b = false;
};
// 每格平面字节 (check::Plane: float + u8)
inline constexpr double kPlaneBytes = 5.0;
// B 装载 + 篮子期峰值 (字节 / 格): close 5 + 价 4 + 权重 4 + 复权 4 + 估值 (股本 / 净利 / 权益 / 营收 / 经营现金流 / 股息率) 24 + 行业 1
// + 上一有效 close 行 4 + 补价副本 4 (baskets)
inline constexpr double kBasketCellBytes = kPlaneBytes + 4.0 + 4.0 + 4.0 + 24.0 + 1.0 + 4.0 + 4.0;
// 宿主装载量 (字节 / 格): X = 叶 kPlaneBytes · nx (块上实际留的叶数, 见 resident_leaves); Y = 门控 1 + 建仓可成交码 1 + 标签两腿 (lv, sv, m) 5 · hd
// + 盘口段成本两腿 (无 m) 4 · 段数 (固定段无平面); B = kBasketCellBytes
double cell_bytes(const FeatureTable &ft, const BlockSpec &s, size_t nx);

// 两腿平面 (标签 / 成本同形): fp16 位 (与落盘同格式, Stat 直接吃) + 有效位 (标签: 池内 ∧ 两腿皆有限; 成本无: 逐腿池内 ∧ 有限, 其余 0)
struct LegPlane {
  std::vector<uint16_t> v[factor::stat::kLegs];
  std::vector<uint8_t> m;
};
// 宿主平面 → 契约视图 (整张量, 未偏). eo = 建仓可成交码平面 (YPlanes::eo); ry = 预处理的做多标签 r16 (nullptr = 未预处理, 会话 open 时做)
inline factor::stat::Label label_of(const LegPlane &p, const uint8_t *eo, const uint16_t *ry) {
  return {{p.v[factor::stat::kLong].data(), p.v[factor::stat::kShort].data()}, p.m.data(), eo, ry};
}
// 成本平面 → 金额段视图; 固定段 (无平面) → 空 Cost
inline factor::stat::Cost cost_of(const LegPlane &p) {
  return p.v[factor::stat::kLong].empty() ? factor::stat::Cost{} : factor::stat::Cost{{p.v[factor::stat::kLong].data(), p.v[factor::stat::kShort].data()}};
}

struct XPlanes {
  Shape sh;
  std::vector<factor::check::Plane> v; // [LeafId] 值 + 掩码 (按 ts_valid 门控); 空 = 未装
  const factor::check::Plane *find(LeafId id) const { return id < v.size() && !v[id].v.empty() ? &v[id] : nullptr; }
  size_t count() const; // 已装叶数
};
struct YPlanes {
  Shape sh;
  std::vector<uint8_t> gate;    // 截面池掩码 g = 当日在池 ∧ cs_valid ∧ 当日有成交 (ts_valid 任一行 ≠ 0; 整日常量)
  std::vector<uint8_t> eo;      // 建仓可成交码 kEoBuy | kEoSell (lb_entry_ok; 池外 / 缺 → 0); 各持有期共用 (Stat 契约【仓位感知】)
  Holds hd;                     // [hold] = YSpec::hd
  std::vector<LegPlane> labels; // [hold] 两腿毛收益: 做多 / 做空; 本层行 t 取本层标签列行 t (封板照写, 能否建仓看 eo; 平仓顺延已在值里)
  Amts ad;                      // [amt] = YSpec::ad
  std::vector<LegPlane> costs;  // [amt] 两腿建仓冲击 (比例): 买入 (封顶) / 卖出; 采样同标签, 该腿可建仓 (eo) 的格恒有限 (装载断言), 无有效位; 固定段空
  double n_full = 0.0;          // 满仓一腿只数 (Stat 契约【持仓口径】的 cap) = 池内平均每行有效资产数 / kGroups: 池宽决定, 与因子无关 (YSpec::cap > 0 → 它)
};
// 全部金额段的成本视图 [amt] (= y.ad 序; cost_rows 的 c, C = y.ad.n)
inline std::vector<factor::stat::Cost> costs_of(const YPlanes &y) {
  std::vector<factor::stat::Cost> c;
  for (const LegPlane &p : y.costs)
    c.push_back(cost_of(p));
  return c;
}
// 篮子原料 (load_b; 本层 close + 日频原料 as-of, BlockSpec::b 时随 Y 进块, 交 FactorsBook baskets)
struct BPlanes {
  Shape sh;
  factor::check::Plane close;        // 本行有成交的后复权 close (估值 / 换手漂移, 跨日除权不进收益)
  std::vector<float> mcap;           // 本行总市值 = close × total_shares (加权版的权; close 无效 / 缺股本 → 0)
  std::vector<float> px, shares;     // 本行有成交的 close (不复权; 否则 0) / 总股本 (缺 → NaN)
  std::vector<float> val[kPoolVals]; // [PoolVal] 估值输入 (BCols::val 原值; 缺 → NaN)
  std::vector<uint8_t> ind;          // 行业 ID (SW2021 一级; 0 = 未知 / 缺 / 越界)
};
// 标签的契约视图 (未预处理, ry 空; cap = n_full). 会话 open (cpu / gpu stat_open) 自己预处理
factor::stat::Labels labels_of(const YPlanes &y);
// 所评持有期在 Y 全部持有期里的下标: at[i] = want.k[i] 在 all 里的位置 (探针 hi → 会话 / 标签组下标; want 的键须都在 all 里).
// 根打开时定一次 (EngineBase::roots), 两后端同一份
struct Pick {
  int n = 0;
  int at[factor::stat::kMaxHold] = {};
};
Pick pick_of(const Holds &all, const Holds &want);
// L 里按 p 取那几组 (L.hd = 取前的全部持有期), 预处理位跟着走
factor::stat::Labels labels_pick(const factor::stat::Labels &L, const Pick &p);

// 装载环境 (三侧装载 / 门控预扫 / 块同一份): 字段表 / 评估域 / 已开的库 (按 dm.rs.libs 开好) / 线程池 / 取消旗 / 进度
struct Io {
  const FeatureTable &ft;
  const Domain &dm;
  const LibReaders &lr;
  misc::exec::ForkJoin &ex;
  const std::atomic<bool> &cancel;
  Progress &prog;
};
// 逐天并行读 (ex 抢单) 评估域日 [k.d0, k.d1) 装进 X (叶 ids, 须未装; 其余叶不动, 失败 → ids 的平面清空) / Y / B (覆盖)
// (形 days × A → T, 容量断言在此). 三侧同一份骨架:
// 一张读计划 (analysis::read_plan, 同 (池, 层, 盘后) 一源, 网格行 level_asof 投影), 列全部 Col::ref(dm.pool);
// X 列段 = 叶 (按所在 (池, 层) 投影); Y 列段 = ts_valid | cs_valid | 标签 [hd][kLegs] | 成本 [ad][kLegs];
// B 列段 = ts_valid close (标签库网格层本层列) | adj_factor total_shares val[kPoolVals] ind_l1 (日频原料 bar.1d).
// 返回 false = 取消 或 库判废 (调用方查 lr.stale()). 四个装载同一份进度: prog.begin Loading (日数), 每天 publish(1)
bool load_x(const Io &io, const Chunk &k, const std::vector<LeafId> &ids, XPlanes &X);
bool load_y(const Io &io, const Chunk &k, const YSpec &s, YPlanes &Y);
bool load_b(const Io &io, const Chunk &k, BPlanes &B);
// [k.d0, k.d1) 的满仓只数 (与 load_y 的 YPlanes::n_full 同一份门控同式), 只读 ts_valid / cs_valid 两列, 不留平面 (多块作业的全期 cap).
// false = 取消 / 库判废
bool n_full_of(const Io &io, const Chunk &k, double &out);

// ---- 块: Y (+ B) 按 (层, 评估域, 日段, YSpec, B) 常驻; X 挂在同一块上 (Y 换 → X 清) ----
//   整期一块 = worker 的一块 (Run: Y 跨文件复用, X retain 只留本文件的叶; Inspect: X 只增); 多块 / Search / 池篮子 = 作业自有
//   块 = 评估域日 [d0, d1) 的行 [0, T); 统计只从 t_lo 起 ([0, t_lo) = 前带 carry, 只给回看 / lag; Search 契约【分块流式】)
struct Block {
  std::string key; // block_key; 空 = 未装
  std::vector<misc::Ymd> dates;
  size_t d0 = 0, d1 = 0;
  size_t nd = 0;                    // 评估域总日数 (跨块落地平面的行轴 = nd · seg)
  int t_lo = 0;                     // 块内统计首行 (= 前带行数, ensure_block 置)
  uint64_t gen = 0;                 // 每次 reset 取进程内新值 (引擎据此判会话作废; 作业自有块析构后同址新建也不撞)
  const FeatureTable *ft = nullptr; // 叶名 → LeafId (ensure_block 每次置; 同层字段表进程内不变)
  XPlanes x;
  YPlanes y;
  BPlanes b;                                                         // BlockSpec::b 时随 Y 装
  mutable std::map<std::pair<LeafId, factor::Dom>, std::string> dom; // 叶值域缓存 (叶, 严格域) → 越界值 (空 = 合); FactorsEngine plan_forest 填
  void reset() {
    static std::atomic<uint64_t> next{0};
    *this = Block{};
    gen = ++next;
  }
  factor::stat::Span span() const { return {y.sh.T, y.sh.A, factor::geo_of(y.sh.seg), static_cast<int>(d0) * y.sh.seg, t_lo}; }
  int rows_all() const { return static_cast<int>(nd) * y.sh.seg; }
  double bytes() const; // X / Y / B 平面占的宿主字节
};
// 块键 = 层 | 评估域 | 日段 | Y 规格 | B (X 不进键: 叶挂在块上按 ids 补)
std::string block_key(const FeatureTable &ft, const Domain &dm, const Chunk &k, const BlockSpec &s);
// 一步装齐块 k (四服务同一份): 键 (或日序列) 变 → reset + 装 Y (+ B); retain → 丢 s.x 以外的叶; 缺的叶补装
// (s.x 空 ∧ retain = 只 Y); t_lo = 前带行数. Ok / Cancel / Stale (判废 → 块 reset)
Rc ensure_block(Block &c, const Io &io, const Chunk &k, const BlockSpec &s, bool retain);
// ensure_block 之后块上的叶数 (预算): retain 或键 key 不中 → ids; 否则 = 已留 ∪ ids
size_t resident_leaves(const Block &c, const std::string &key, const std::vector<LeafId> &ids, bool retain);
// Dag 叶的平面视图 (下标 = Dag::feats 下标); 全部须已装
std::vector<const factor::check::Plane *> leaves(const Block &c, const std::vector<std::string> &feats);

// 结果的作用域头 (Run 行 / Inspect 结果同一份): 池 / 期 / 形 / 封顶 / 满仓只数 / 后端 / 时刻
StatScope stat_scope(const FactorScope &sc, const Domain &dm, const YPlanes &y, const char *backend);

} // namespace GUI::Factors
