// FactorsLoad — 因子评估的特征库装载层, FactorsService (全表 Run) / InspectService (单因子深看) / SearchService 共用:
//   open_libs / EvalScope = 一个因子的评估作用域 (eval_scope: 所用库 + 评估期 = 落盘日交集 + A 轴 + 子池成员), worker 按因子解析;
//   XSpec → XPlanes (因子输入叶) ↔ YSpec → YPlanes (截面门控 + 两腿平面 标签 [hold] / 成本 [amt] + 池等权指数 + 满仓只数):
//   load_x / load_y 同一份逐天并行骨架 (n_full_of 只扫门控出满仓只数); ScopeCache + ensure_y / ensure_x = 作用域块 (Y 常驻, X 按服务策略留 / 补);
//   Rc = 装载 / 评估的唯一结局; plan_forest 按 OpTable in 列查特征叶数据值域, 逐根归因;
//   valid_count / rank_sum 根平面有效率 / 可秩率 (块间可加); cost_tiers 换手成本 (块间续攒);
//   labels_of / labels_pick 标签的契约视图 (未预处理, 会话 open 时做; cap = n_full);
//   fold_exit 净值引擎 (Stat 契约【净值】: 逐行批收益在平仓行实现 1/h̄, 按 stride 求和; Inspect 分层 / Portfolio 因子线 / 池篮子指数同一份) + compound;
//   alpha_dist alpha 分布 (Inspect 大图与因子表小图同一份).
// 四个服务的差别只在装载之后的探针 (FactorsEngine Probes): 引擎同一份, 探针元组各自声明.
#pragma once

#include "factor/Check.hpp" // check::Plane
#include "factor/Dag.hpp"
#include "factor/Stat/Contract.hpp"
#include "features/Method/Fundamental.hpp"              // fund::SW2021_L1_COUNT
#include "features/Storage/DayBatchPlane.hpp"           // Day / LibReaders
#include "gui/task_factors/services/FactorsService.hpp" // FeatureTable / MiniPdf
#include "misc/exec.hpp"
#include "shared/Analysis.hpp" // AggPdf / ReadScope
#include "shared/Pool.hpp"     // pool::Panel

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace factor::cpu::stat {
struct Workspace;
}

namespace GUI::Factors {

// ---- 净值引擎 (Stat 契约【净值】; Inspect 分层 / Portfolio 因子线 / 池篮子指数 同一份) ----
// 期收益表: [n][P], 线 i 点 p
struct BookLines {
  int n = 0, P = 0; // 线数 / 每线点数 (= T / stride)
  std::vector<float> v;
  const float *at(int i) const { return v.data() + static_cast<size_t>(i) * static_cast<size_t>(P); }
};
// 逐行批收益 x(i, t) (线 i 行 t 这一批, 占这批资金; 不 ok 记 0) → 平仓行 u = hold_exit_row(t) 记 x / h̄ (h̄ = hold_span) → 每 stride 行求和一点
// (stride 1 = 逐行 / S = 日). 平仓行越过 T 的批不计. 线间并行, x 须可并发调
template <class X>
void fold_exit(misc::exec::ForkJoin &ex, int hold, int seg, int T, int n, int stride, const X &x, BookLines &out) {
  const factor::Geo g = factor::geo_of(seg);
  assert(n >= 1 && stride >= 1 && T % stride == 0 && T % g.S == 0);
  const double h = factor::stat::hold_span(hold, g);
  out.n = n, out.P = T / stride;
  out.v.assign(static_cast<size_t>(n) * static_cast<size_t>(out.P), 0.f);
  ex.rows(n, [&](int i0, int i1, int) {
    std::vector<double> inc(static_cast<size_t>(T));
    for (int i = i0; i < i1; ++i) {
      std::fill(inc.begin(), inc.end(), 0.0);
      for (int t = 0; t < T; ++t)
        if (const int u = factor::stat::hold_exit_row(t, hold, g); u < T)
          inc[static_cast<size_t>(u)] += x(i, t) / h;
      float *dst = out.v.data() + static_cast<size_t>(i) * static_cast<size_t>(out.P);
      for (int p = 0; p < out.P; ++p) {
        double s = 0.0;
        for (int u = p * stride; u < (p + 1) * stride; ++u)
          s += inc[static_cast<size_t>(u)];
        dst[p] = static_cast<float>(s);
      }
    }
  });
}
// 期收益 → 净值 (起点 1 不含): put(p, Π_{q ≤ p} (1 + r_q)). 一切净值 / 指数的复利只此一处 (UI: Inspect 分层 / Portfolio 组合 / 池指数)
template <class R, class Put>
void compound(const R *r, size_t n, Put &&put) {
  double l = 1.0;
  for (size_t p = 0; p < n; ++p) {
    l *= 1.0 + static_cast<double>(r[p]);
    put(p, l);
  }
}

// ---- 评估作用域 (一个因子; 同文件各权重同叶 → 同一份) ----
struct EvalScope {
  PoolId pool = kAllPool;            // 因子池 (目录)
  PoolMask used = 0;                 // 所用库: 标签库 + 叶所在库 (+ all: 池成员)
  analysis::ReadScope rs;            // main = 标签库, libs = used 各库清单, axis = A 轴 (标签库轴 / 子池并集), to = A → 各库轴
  std::vector<misc::Ymd> dates;      // 评估期 = used 各库落盘日交集 (升序, 非空)
  std::vector<Day> days;             // [dates] 当日 + 承接日 (标签库日期轴上的前一日, 0 = 轴首) 及其落盘位 (承接日按库判)
  std::optional<pool::Panel> member; // 子池成员 (Lob 池 × Bar 标签层; 网格层 as-of, 池轴 = A 轴)
  std::string key() const;           // 缓存键: 池 | 所用库目录 + 池轴 | 评估期首末 + 天数
};
// worker: 作用域 sc 里已建的库全开 (清单没变的复用; 各因子只读自己用到的). abort = 库判废时拉起的取消旗
void open_libs(const FactorScope &sc, LibReaders &lr, std::atomic<bool> *abort);
// 作用域 sc + 叶 feats (FeatureTable 叶名) → 作用域. 所用库 = 读计划各列的库 (Col::ref) 之并 (+ all: 池成员);
// 所用库未建 / 无共同落盘日 → false + why (正常输入)
bool eval_scope(const FeatureTable &ft, const FactorScope &sc, const std::vector<std::string> &feats, EvalScope &es, std::string &why);

// 宿主内存预算: 可用内存 × kHostFrac (Search 分块同一口径)
inline constexpr double kHostFrac = 0.6;
double host_budget();
// 整张量装载 (Run / Inspect) 放不下 → assert (who / 规模进消息): 该因子层太细或评估期太长, 拒绝评估
void assert_fits(const char *who, double bytes, int days, int A, int seg);

// ---- 装载: X ↔ Y 两侧同形 (Spec → Planes, load_x / load_y 同签名同一份逐天骨架) ----
//   X = 因子输入叶 (随因子变): 叶按各自 ts_valid 门控
//   Y = 门控 + 两腿平面 + 池篮子指数 (只随作用域变): 截面门控 = cs_valid ∧ 当日有成交 (∧ 子池成员); 两腿同一份折叠 (池内 ∧ 两腿有限, 腿按封顶截;
//   可成交 / 顺延已在标签里, 不另读)
// 张量形 (两侧同一份; T = days · seg)
struct Shape {
  int T = 0, A = 0, days = 0;
  int seg = 0; // 因子层每日行数 (= FeatureTable::seg)
  size_t n() const { return static_cast<size_t>(T) * A; }
  factor::stat::Span span() const { return {T, A, factor::geo_of(seg)}; } // 整张量 (t0_abs = t_lo = 0)
  bool operator==(const Shape &) const = default;
};
// 金额段 (LegCol::key, 与 factor::stat::Holds 对仗)
struct Amts {
  int n = 0;
  int a[factor::stat::kMaxCost] = {};
};
factor::stat::Holds holds_of(const std::vector<int> &keys); // 容量断言 kMaxHold; 非空再 assert_holds 由装载按 seg 做
Amts amts_of(const std::vector<int> &keys);                 // 容量断言 kMaxCost

struct XSpec {
  std::vector<std::string> codes; // 叶 (FeatureTable 叶名; 平面下标 = 此序)
};
// 池篮子指数的档 (YSpec::index; 平面装完即算, 不留)
enum class IndexKind : uint8_t { None, // 不装
                                 Pool, // 篮子 0 (池整体) 等权, 每持有期一条, 任意层; Inspect K 线叠加
                                 Full  // 篮子 (池整体 + 行业) 两权重指数 / 估值 + 池换手, 只日层, 持有期只 T+1C (+ close_day × adj_factor 现算后复权 + 权重 + 估值 + 行业);
                                       // Portfolio 池线
};
struct YSpec {
  factor::stat::Holds hd;            // 标签组 (LegCol::key, 字段表里有)
  Amts ad;                           // 成本段 (LegCol::key, 字段表里有; 空 = 不装)
  float max_impact = 0.f;            // 买入冲击封顶 (Config::max_impact; ad 非空时 > 0): buy = min(原值, 封顶)
  IndexKind index = IndexKind::None; // 池篮子指数
  std::string key() const;
};
// 每格平面字节 (check::Plane: float + u8)
inline constexpr double kPlaneBytes = 5.0;
// 宿主装载量 (字节 / 格): X = 叶 5 · codes; Y = 门控 1 + 标签两腿 (lv, sv, m) 5 · hd + 成本两腿 (无 m) 4 · ad
//   + close 5 + 价 4 + 权重 4 + 估值 (股本 / 净利 / 权益 / 营收 / 经营现金流 / 股息率) 24 + 行业 1 + 上一有效 close 行 4 (index Full, 装载期峰值)
double cell_bytes(const XSpec &s);
double cell_bytes(const YSpec &s);

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
  // 篮子 (YSpec::index; 池整体与行业同形): 篮子 0 = 池整体, k = 1..kBaskets−1 = SW2021 一级行业 k (本池成分按行业切, 全A 池 = 申万一级;
  //   行业未知的只进篮子 0); Full 线 index_line(k, w) = kWeights · k + w; Pool 线 i = 持有期 hd.h[i] 的篮子 0 等权
  static constexpr size_t kBaskets = fund::SW2021_L1_COUNT;
  static constexpr int index_line(size_t k, int w) { return kWeights * static_cast<int>(k) + w; }
  BookLines index;                             // 指数的逐行收益 (stride 1; 净值 = compound, 调用方) = 净值引擎 fold_exit: 行 t 一批 = 成分 (行 t 在池 ∧ 该持有期
                                               //   标签有效; ∧ 该行 ind_l1 = k) 做多标签按权平均 (等权 1 / 总市值加权 = 行 t 的 close_day × total_shares, 不复权;
                                               //   缺股本权 0), 平仓行实现 1/h̄. Full 日层 T+1C = C(D) → C(D+1) 复权, 可成交 / 顺延在标签里
  std::vector<float> val[kWeights][kPoolVals]; // [Weight][PoolVal] 估值 (Full): 行 t 成分 = 上一行在池 (∧ 上一行 ind_l1 = k); 市值 = 本行收盘价 (停牌带上一有效价) × 上一行总股本;
                                               //   个股收益率 = 分子 / 市值, 按权平均: 等权 = 算术平均 (= 等权组合每元的分子) / 加权 (权 = 市值) = 整体法 Σ分子 / Σ市值;
                                               //   分子: EP = 归母净利TTM, BP = 归母权益MRQ, SP = 营业总收入TTM, CP = 经营现金流TTM, DY = 分红,
                                               //   分红 = 上一行 dy_ttm × 上一行收盘价 × 上一行总股本 (= Fund 的分母口径还原近 365 日税前分红额);
                                               //   财务同取上一行 (Fund 行 D = 次日盘前态 ⇒ 上一行 = 当日适用); 每口只收该口输入有限的成分 (亏损 / 负权益 / 负现金流照计为负);
                                               //   Σ权 ≤ 0 / 无成分 → NaN
  double turn[kWeights] = {};                  // [Weight] (Full) 篮子 0 的每行单边换手 ½ Σ|w_t − w̃_{t−1}| (w̃ = 上一成分行的权按本行收益漂移; 含再平衡 + 成分进出)
                                               //   沿成分行均值, 首行建仓不计
  double n_full = 0.0;                         // 满仓一腿只数 (Stat 契约【持仓口径】的 cap) = 池内平均每行有效资产数 / kGroups: 池宽决定, 与因子无关
};
// 标签的契约视图 (未预处理, ry 空; cap = n_full). 会话 open (cpu / gpu stat_open) 自己预处理
factor::stat::Labels labels_of(const YPlanes &y);
// L 里只取 hd 的那几组 (hd.n = 0 → L 原样; 键须都在 L.hd 里), 预处理位跟着走
factor::stat::Labels labels_pick(const factor::stat::Labels &L, const factor::stat::Holds &hd);

// 逐天并行读 (ex 抢单) es.days[d0, d1) 按 spec 装进 X / Y (覆盖; 形 days × A → T, 容量断言在此). 两侧同一份骨架:
// 一张读计划 (analysis::read_plan, 同 (池, 层) 一源, 网格行 level_asof 投影), 列全部 Col::ref(es.pool);
// X 列段 = 叶 (按所在 (池, 层) 投影); Y 列段 = ts_valid | cs_valid | close adj_factor total_shares np_ttm eq_mrq rev_ttm ocf_ttm dy_ttm ind_l1
//   (index Full) | 标签 [hd][kLegs] | 成本 [ad][kLegs]
// (标签库网格层本层列).
// lr 须按 es.rs.libs 开好. 返回 false = 取消 或 库判废 (调用方查 lr.stale()). 每天 prog.publish(1), 不 begin
bool load_x(const FeatureTable &ft, const EvalScope &es, const LibReaders &lr, size_t d0, size_t d1, const XSpec &spec, XPlanes &X,
            misc::exec::ForkJoin &ex, const std::atomic<bool> &cancel, Progress &prog);
bool load_y(const FeatureTable &ft, const EvalScope &es, const LibReaders &lr, size_t d0, size_t d1, const YSpec &spec, YPlanes &Y,
            misc::exec::ForkJoin &ex, const std::atomic<bool> &cancel, Progress &prog);
// es.days[d0, d1) 的满仓只数 (与 load_y 的 YPlanes::n_full 同一份门控同式), 只读 ts_valid / cs_valid 两列, 不留平面 (Search 多块时的全期 cap).
// prog.begin Loading. false = 取消 / 库判废
bool n_full_of(const FeatureTable &ft, const EvalScope &es, const LibReaders &lr, size_t d0, size_t d1, misc::exec::ForkJoin &ex,
               const std::atomic<bool> &cancel, Progress &prog, double &out);

// ---- 评估结局 (装载 / 评估 / 四服务同一个) ----
enum class Rc : uint8_t { Ok,
                          Empty,  // 作用域不成 (所用库未建 / 无共同落盘日), why
                          Broken, // 特征叶值域越界 (全部根), why
                          Cancel,
                          Stale }; // 库判废 (块已 reset; 调用方置 kStaleMsg)
inline constexpr const char *kStaleMsg = "特征库判废 (池轴/字段表与当前不符), 需重算特征";

// ---- 作用域块: Y 按 (层, 作用域, 日段, YSpec) 常驻; X 挂在同一块上 (Y 换 → X 清) ----
//   Run = 每轮一块 (Y 跨文件复用, X retain 只留本文件的叶); Inspect = 服务一块 (X 只增); Search = 每块一个
//   块 = 作用域日 [d0, d1) 的行 [0, T); 统计只从 t_lo 起 ([0, t_lo) = 前带 carry, 只给回看 / lag; Search 契约【分块流式】)
struct ScopeCache {
  std::string key; // scope_key; 空 = 未装
  std::vector<misc::Ymd> dates;
  size_t d0 = 0, d1 = 0;
  size_t nd = 0;    // 作用域总日数 (跨块落地平面的行轴 = nd · seg)
  int t_lo = 0;     // 块内统计首行 (调用方装完置)
  uint64_t gen = 0; // 每次 reset +1 (引擎据此判会话作废)
  XPlanes x;
  YPlanes y;
  mutable std::map<std::pair<std::string, factor::Dom>, std::string> dom; // 叶值域缓存 (叶, 严格域) → 越界值 (空 = 合); plan_forest 填
  void reset() {
    const uint64_t g = gen + 1;
    *this = ScopeCache{};
    gen = g;
  }
  factor::stat::Span span() const { return {y.sh.T, y.sh.A, factor::geo_of(y.sh.seg), static_cast<int>(d0) * y.sh.seg, t_lo}; }
  int rows_all() const { return static_cast<int>(nd) * y.sh.seg; }
};
std::string scope_key(const FeatureTable &ft, const EvalScope &es, size_t d0, size_t d1, const YSpec &spec);
// 键 (或日序列) 变 → reset + 装 Y (prog.begin Loading). 键中 → 不动. false = 取消 / 判废 (块已 reset)
bool ensure_y(ScopeCache &c, const FeatureTable &ft, const EvalScope &es, const LibReaders &lr, size_t d0, size_t d1, const YSpec &spec,
              misc::exec::ForkJoin &ex, const std::atomic<bool> &cancel, Progress &prog);
// 先 ensure_y. retain → 先丢 codes 以外的叶; 缺的叶补装 (prog.begin Loading). false = 取消 / 判废 (已有的叶不动)
bool ensure_x(ScopeCache &c, const FeatureTable &ft, const EvalScope &es, const LibReaders &lr, const std::vector<std::string> &codes,
              bool retain, misc::exec::ForkJoin &ex, const std::atomic<bool> &cancel, Progress &prog);
// 作用域块一步装齐 (四服务同一份): ensure_y → ensure_x (codes 空 ∧ retain = 只 Y). Ok / Cancel / Stale (判废 → 块 reset)
Rc ensure_xy(ScopeCache &c, const FeatureTable &ft, const EvalScope &es, const LibReaders &lr, size_t d0, size_t d1, const YSpec &ys,
             const std::vector<std::string> &codes, bool retain, misc::exec::ForkJoin &ex, const std::atomic<bool> &cancel, Progress &prog);
// 叶平面视图 (DAG 输入: 下标 = codes 下标 = Dag::feats 下标); 全部须已装
std::vector<const factor::check::Plane *> leaves(const ScopeCache &c, const std::vector<std::string> &codes);

// 森林的评估计划 (四服务同一份): 算子节点的特征叶元按 OpTable in 列的严格域逐元查块上数据 (c.dom 缓存);
// why[k] = 根 k 子树里首个越界 (非空 = BROKEN, 不评), need = 好根子树之并 (引擎 walk 掩码). 返回好根数
int plan_forest(misc::exec::ForkJoin &ex, const ScopeCache &c, const factor::Dag &d, std::vector<std::string> &why, std::vector<uint8_t> &need);

// 池内有效计数 (格 [i0, i1), 块间可加): on += Σ(m ∧ gate), all += Σgate (gate = 截面池 cs_valid; 池外格不进分子分母)
void valid_count(const uint8_t *m, const uint8_t *gate, size_t i0, size_t i1, uint64_t &on, uint64_t &all);
// 可秩率行和 (行 [t0, t1), 块间可加): 逐行 = 所在并列块 (同 r16) 不超过 ⌈nv / kGroups⌉ 只的格数 / nv (nv = 该行 m ∧ gate 格数;
// 整行 kRankNone → 0), sum += 各 nv ≥ 1 行之和, rows += 行数. rx = 该因子的 r16 [T][A] (rank_x 出)
void rank_sum(misc::exec::ForkJoin &ex, const uint16_t *rx, const uint8_t *m, const uint8_t *gate, int t0, int t1, int A, double &sum, long long &rows);

// alpha 分布 (Inspect 左下右半图 / Run 落因子表小图, 同一份): 样本 = 池内有效格 (rx 有效 ∧ alpha 有效), 组号 = grp_of(rx)
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
// 两趟, 每趟条带并行: 每线程一份 KLL + 缓冲批量 addBatch, 末尾合并进线程 0 那份.
//   趟 1: 全体样本 → 矩 + 截尾范围 [lo, hi]; 同趟逐行: 截面均值 (series) + 各组 min / max → 相邻组边界中点, 各组和 / 数
//   趟 2: 只收 [lo, hi] 内样本 → pdf
void alpha_dist(misc::exec::ForkJoin &ex, const float *v, const uint8_t *m, const uint16_t *rx, int T, int A, AlphaDist &o);

// 因子表迷你 PDF: AggPdf → MiniPdf (n_pts < 2 → 空)
MiniPdf mini_pdf_of(const analysis::AggPdf &p);

// 换手成本 (Stat 契约【换手成本】, Run / Inspect 同一份): lc = 腿码 [T][A] (leg_codes(rx); 两后端同一份), s = 块形 (统计行 [t_lo, T)),
// L = 所评持有期的标签 (有效位 / cap), Stat cost_rows (每 t 一遍 A 轴攒全部 (h, c)) → 续攒进 out[c].acc[hi] (c 按 y.ad 序, hi 按 L.hd 序;
// out 空 → 先按 y.ad 建). w = Stat CPU 工作区 (ensure 过 C = y.ad.n); 无金额段 → out 空
void cost_tiers(misc::exec::ForkJoin &ex, const uint8_t *lc, const factor::stat::Span &s, const factor::stat::Labels &L, const YPlanes &y,
                factor::cpu::stat::Workspace &w, std::vector<CostTier> &out);

} // namespace GUI::Factors
