// FactorsLoad — 因子评估的特征库装载层 (全部因子服务共用; 只装数据, 根上的统计归 FactorsProbe, 净值 / 篮子归 FactorsBook): 作用域层 (FactorsScope) 之上.
//   X ↔ Y ↔ B 三侧同一份逐天并行骨架 (read_days, 环境 Io): 叶 LeafId → Leaf (因子输入叶) / Y → YCore (截面门控 + 建仓可成交码) + 标签两侧平面 [hold] /
//   买入冲击 [amt] / 池篮子 → BPlanes (全部池成员位 + T+1C 标签 + close / 复权 / 股本 / 估值 / 行业, 一趟装齐, 不缓存); n_full_of 只扫门控出满仓只数;
//   Store = 进程唯一的计算面 (线程池 / 库 / 平面缓存 / 计算权): 平面按 (层, 评估域, 日段, 角色) 逐张缓存, 块 Block = 钉住一组平面的视图 (Store::pin);
//   y_spec = Y 规格 (持有期 + 探针要的成本段 Segs); labels_of / labels_pick 标签的契约视图 (ry = 块上钉的标签秩, 只 CPU 要; cap = n_full);
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
#include <vector>

namespace GUI::Factors {

// ---- 装载: X ↔ Y ↔ B 三侧同形 (XCols / YCols / BCols → XPlanes / YPlanes / BPlanes, load_x / load_y / load_b 同一份逐天骨架) ----
//   X = 因子输入叶 (随因子变): 叶按各自 ts_valid 门控
//   Y = 门控 + 标签 / 成本平面 (只随评估域变): 截面门控 = 标签库 cs_valid (all: 当日活跃; 子池: 当日成员) ∧ 当日有成交; 标签 = 池内 ∧ 两侧有限,
//   买入冲击 = 池内原值 (盘口缺 → +inf; 封顶在取用时, Stat Cost::top); 可成交 / 顺延已在标签里, 不另读)
//   B = 池篮子输入 (Portfolio 池线, all 池评估域): 全部池的成员位 + T+1C 标签 + 本层 close + 日频复权 / 股本 / 估值 / 行业 (as-of), 一趟读齐交 FactorsBook baskets
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
// 两侧列组的全部键 (cols 序; 字段表 y.labels → Holds / y.costs → Amts)
template <class K>
K keys_of(const std::vector<LegCol> &cols) {
  std::vector<int> keys;
  for (const LegCol &c : cols)
    keys.push_back(c.key);
  return keys_of<K>(keys);
}
// 键集之并 (升序去重; 字段表列组本是升序, 与之同序)
template <class K>
K keys_union(const K &a, const K &b) {
  std::vector<int> keys(a.k, a.k + a.n);
  keys.insert(keys.end(), b.k, b.k + b.n);
  std::sort(keys.begin(), keys.end());
  keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
  return keys_of<K>(keys);
}

struct YSpec {
  Holds hd;               // 标签组 (LegCol::key, 字段表里有)
  Amts ad;                // 成本段 (LegCol::key, 字段表里有且评估域可用; 空 = 不装; 固定段无平面)
  float max_impact = 0.f; // 买入冲击封顶 (Config::max_impact; 有盘口段时 > 0): 取用时 min(原值, 封顶)
  std::string key() const;
};
// 成本段取法 (探针声明 FactorsProbe Ps::segs, 管线据此定 YSpec::ad): 不装 (Search) / 主段 (Portfolio 出参线: 首个盘口段, 无则固定段) /
// 全部 (Run / Inspect 各段 CostAcc)
enum class Segs : uint8_t { None,
                            First,
                            All };
// 块的 Y 规格: 持有期 hd (字段表里有) + 成本段 sg (评估域 dm 可用的段: 固定段恒在, 盘口段要 dm 含其所在 Lob 库); 有段才带封顶 max_impact
YSpec y_spec(const FeatureTable &ft, const Domain &dm, const Holds &hd, Segs sg, double max_impact);
// 块规格 (X ↔ Y 两侧各一项): X = 叶 / Y = YSpec; ry = 钉上标签秩 (Engine<Cpu> 要; GPU 会话在显存里自己预处理)
struct BlockSpec {
  std::vector<LeafId> x;
  YSpec y;
  bool ry = false;
};
// 每格平面字节 (check::Plane: float + u8)
inline constexpr double kPlaneBytes = 5.0;
// B 装载 + 篮子期峰值 (字节 / 格): 池成员位 8 + 标签两侧 (lv, sv, m) 5 + 建仓可成交码 1 + close 5 + 价 4 + 权重 4 + 复权 4
// + 估值 (股本 / 净利 / 权益 / 营收 / 经营现金流 / 股息率) 24 + 行业 1 + 上一有效 close 行 4 + 补价副本 4 + all 内行业码 1 (baskets)
inline constexpr double kBasketCellBytes = 8.0 + kPlaneBytes + 1.0 + kPlaneBytes + 4.0 + 4.0 + 4.0 + 24.0 + 1.0 + 4.0 + 4.0 + 1.0;
// 宿主装载量 (字节 / 格): X = 叶 kPlaneBytes · nx; Y = 门控 1 + 建仓可成交码 1 + 标签两侧 (lv, sv, m) 5 · hd
// + 盘口段买入冲击 (无 m) 2 · 段数 (固定段无平面)
double cell_bytes(const FeatureTable &ft, const BlockSpec &s, size_t nx);

// ---- 平面 (装好即只读, Store 逐张缓存; 块只持视图) ----
// 叶平面: 值 + 掩码 (按 ts_valid 门控) + 值域缓存 (严格域 → 首个越界值, 空 = 合; FactorsEngine plan_forest 填, 随平面一代)
struct Leaf : factor::check::Plane {
  mutable std::map<factor::Dom, std::string> dom;
};
// Y 的公共部分 (各持有期 / 金额段共用)
struct YCore {
  std::vector<uint8_t> gate; // 截面池掩码 g = 当日在池 (标签库 cs_valid) ∧ 当日有成交 (ts_valid 任一行 ≠ 0; 整日常量)
  std::vector<uint8_t> eo;   // 建仓可成交码 kEoBuy | kEoSell (lb_entry_ok; 池外 / 缺 → 0); 各持有期共用 (Stat 契约【仓位感知】)
  long long on = 0;          // 门控在池格数 (整期一块的满仓只数由它出)
};
// 标签两侧平面: fp16 位 (与落盘同格式, Stat 直接吃) + 有效位 (池内 ∧ 两侧皆有限)
struct LegPlane {
  std::vector<uint16_t> v[factor::stat::kLegs];
  std::vector<uint8_t> m;
};
// 宿主平面 → 契约视图 (整张量, 未偏). eo = 建仓可成交码平面 (YCore::eo); ry = 预处理的做多标签 r16 (nullptr = 未预处理, GPU 会话自做)
inline factor::stat::Label label_of(const LegPlane &p, const uint8_t *eo, const uint16_t *ry) {
  return {{p.v[factor::stat::kLong].data(), p.v[factor::stat::kShort].data()}, p.m.data(), eo, ry};
}

// ---- 视图 (块上, 指向钉住的平面) ----
struct XPlanes {
  Shape sh;
  std::vector<const Leaf *> v; // [LeafId] 钉住的叶; nullptr = 不在块上
  const Leaf *find(LeafId id) const { return id < v.size() ? v[id] : nullptr; }
};
struct YPlanes {
  Shape sh;
  const uint8_t *gate = nullptr;                       // = YCore::gate
  const uint8_t *eo = nullptr;                         // = YCore::eo
  Holds hd;                                            // [hold] = YSpec::hd
  const LegPlane *labels[factor::stat::kMaxHold] = {}; // [hold] 两侧毛收益: 做多 / 做空; 本层行 t 取本层标签列行 t (封板照写, 能否建仓看 eo; 平仓顺延已在值里)
  const uint16_t *ry[factor::stat::kMaxHold] = {};     // [hold] 做多标签逐行 r16 (prep_label; BlockSpec::ry, 否则 nullptr)
  Amts ad;                                             // [amt] = YSpec::ad
  const uint16_t *costs[factor::stat::kMaxCost] = {};  // [amt] 买入冲击 (比例, 原值) fp16 位; 采样同标签 (行末分钟), 池内格恒有值 (盘口缺 = +inf), 无有效位; 固定段 nullptr
  float max_impact = 0.f;                              // = YSpec::max_impact (Cost::top)
  double n_full = 0.0;                                 // 满仓一组只数 (Stat 契约【持仓口径】的 cap) = 全期池内平均每行有效资产数 / kGroups: 池宽决定, 与因子 / 分块无关
};
// 全部金额段的成本视图 [amt] (= y.ad 序; cost_rows 的 c, C = y.ad.n): 盘口段带封顶, 固定段空 Cost
inline std::vector<factor::stat::Cost> costs_of(const YPlanes &y) {
  std::vector<factor::stat::Cost> c;
  for (int k = 0; k < y.ad.n; ++k)
    c.push_back(y.costs[k] ? factor::stat::Cost{y.costs[k], y.max_impact} : factor::stat::Cost{});
  return c;
}
// 池篮子输入 (load_b; all 池评估域 = 全轴, 一趟读齐, 交 FactorsBook baskets)
struct BPlanes {
  Shape sh;
  std::vector<PoolMask> in;          // 池成员位 (整日常量): 位 kAllPool = all 截面门控 (同 YPlanes::gate), 位 p = 第 p 池当日成员 (池列 as-of) ∧ 当日有成交
  LegPlane lab;                      // T+1C 两侧毛收益 (同 YPlanes::labels 的折叠, 池内 = 在任一池 in ≠ 0)
  std::vector<uint8_t> eo;           // 建仓可成交码 (同 YPlanes::eo, 池内 = in ≠ 0)
  factor::check::Plane close;        // 本行有成交的后复权 close (估值 / 换手漂移, 跨日除权不进收益)
  std::vector<float> mcap;           // 本行总市值 = close × total_shares (加权版的权; close 无效 / 缺股本 → 0)
  std::vector<float> px, shares;     // 本行有成交的 close (不复权; 否则 0) / 总股本 (缺 → NaN)
  std::vector<float> val[kPoolVals]; // [PoolVal] 估值输入 (BCols::val 原值; 缺 → NaN)
  std::vector<uint8_t> ind;          // 行业 ID (SW2021 一级; 0 = 未知 / 缺 / 越界)
};
// 标签的契约视图 (ry = YPlanes::ry: CPU 会话的块钉了标签秩; GPU 的块不钉, stat_open 在显存里自己预处理; cap = n_full)
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

// 装载环境 (三侧装载 / 门控预扫同一份; lr / ex 取自 Store): 字段表 / 评估域 / 已开的库 (按 dm.rs.libs 开好) / 线程池 / 取消旗 / 进度
struct Io {
  const FeatureTable &ft;
  const Domain &dm;
  const LibReaders &lr;
  misc::exec::ForkJoin &ex;
  const std::atomic<bool> &cancel;
  Progress &prog;
};
// 逐天并行读 (ex 抢单) 评估域日 [k.d0, k.d1) (形 days × A → T, 容量断言在此). 三侧同一份骨架:
// 一张读计划 (analysis::read_plan, 同 (库, 层, 盘后) 一源, 网格行 level_asof 投影), 列全部 Col::ref(dm.pool);
// X 列段 = 叶 ids (按所在 (库, 层) 投影) → out[ids 序];
// Y 列段 = [ts_valid | cs_valid | lb_entry_ok (have 空才读, 进 core)] | 标签 [hd][kLegs] | 盘口段买入冲击 [ad] → labels [hd 序] / costs [ad 序]
//   (门控取 have 或刚装的 core; ad 只含盘口段);
// B 列段 = ts_valid cs_valid lb_entry_ok T+1C 标签 [kLegs] close (标签库网格层本层列) | adj_factor total_shares val[kPoolVals] ind_l1 池列 (日频原料 bar.1d);
// B 只在 all 池评估域 (assert).
// 返回 false = 取消 或 库判废 (调用方查 lr.stale()). 四个装载同一份进度: prog.begin Loading (日数), 每天 publish(1)
bool load_x(const Io &io, const Chunk &k, const std::vector<LeafId> &ids, std::vector<Leaf> &out);
bool load_y(const Io &io, const Chunk &k, const YCore *have, YCore &core, const Holds &hd, const Amts &ad, std::vector<LegPlane> &labels,
            std::vector<std::vector<uint16_t>> &costs);
bool load_b(const Io &io, const Chunk &k, BPlanes &B);
// [k.d0, k.d1) 的满仓只数 (与整期 YCore::on 同一份门控同式), 只读 ts_valid / cs_valid 两列, 不留平面 (多块作业的全期 cap).
// false = 取消 / 库判废
bool n_full_of(const Io &io, const Chunk &k, double &out);

// ---- 块 = 评估域日 [d0, d1) 的一组钉住的平面 (Store::pin 建; 视图 x / y 指向 pins) ----
//   行 [0, T); 统计行 [t_lo, T − t_back) (前带只给回看 / lag / 交接链, 后带只给续持; Search 契约【分块流式】)
struct Block {
  size_t d0 = 0, d1 = 0;
  size_t nd = 0;                    // 评估域总日数 (跨块落地平面的行轴 = nd · seg)
  int t_lo = 0, t_back = 0;         // 前带 / 后带行数
  uint64_t gen = 0;                 // Y 视图的代 = (层, 评估域, 日段, YSpec, 缓存代) 的散列: 同代 = 同一份 Y (引擎据此判会话作废 / 块钩子据此复用)
  bool fresh = false;               // 本次 pin 真读了库 (否则全中缓存)
  const FeatureTable *ft = nullptr; // 叶名 → LeafId
  XPlanes x;
  YPlanes y;
  std::vector<std::shared_ptr<const void>> pins; // 钉住的平面 (块活着, 缓存就不淘汰它们)
  factor::stat::Span span() const { return {y.sh.T, y.sh.A, factor::geo_of(y.sh.seg), static_cast<int>(d0) * y.sh.seg, t_lo, t_back}; }
  int rows_all() const { return static_cast<int>(nd) * y.sh.seg; }
};
// Dag 叶的平面视图 (下标 = Dag::feats 下标); 全部须在块上
std::vector<const factor::check::Plane *> leaves(const Block &c, const std::vector<std::string> &feats);

// ---- Store: 进程唯一的计算面 (GUI 各因子服务 + agent 同一份) ----
//   线程池 ex / 已开的库 lr / 平面缓存 / 计算权, 四者同归一个持有者: 计算权在手才可碰其余三样 (assert 持有线程).
//   计算权: 同时只一个作业持有; 交互 (Inspect / Lens / Portfolio) 先于批 (Run / Search), 批在文件 / 趟边界让位 (FactorsEngine Lease).
//   平面缓存: 键 = 层 | Domain::key | 日段 | 角色 (门控 / 标签组 / 金额段 / 标签秩 / 叶), 与请求方无关 (跨服务命中);
//   淘汰 = 未钉的按 LRU, 至 缓存 + 新装 + 调用方预留 ≤ 宿主预算; 满仓只数按评估域记 (整期一块 = 门控在池格数, 多块 = 全期预扫)
enum class Prio : uint8_t { Batch,
                            Interactive };
class Store {
public:
  static Store &get();
  misc::exec::ForkJoin ex;
  LibReaders lr;

  // 计算权: 按 p 排队 (同级先到先得), 等待中 cancel → false (不持有)
  bool acquire(Prio p, const std::atomic<bool> &cancel);
  void release();
  // 钉住块 k (s.y 的持有期 / 金额段 + s.x 的叶 + s.ry 的标签秩): 缺的平面一趟补读, 先腾地 (room: need + reserve).
  // reserve = 调用方在块之外要的宿主字节 (引擎 + 探针). Ok / Cancel / Stale
  Rc pin(Block &c, const Io &io, const Chunk &k, const BlockSpec &s, double reserve);
  // 腾出 bytes: 未钉的按 LRU 淘汰, 至 缓存 + bytes ≤ 宿主预算 (kHostFrac · 总内存 − 缓存外的已用)
  void room(double bytes);
  // 宿主预算 (规划口径): 缓存全可让, 算回预算
  double budget() const;
  // 弃全部缓存 (库判废 / 请求带 reload); 已钉的随块析构释放
  void clear();

private:
  Store();
  struct Entry {
    std::shared_ptr<const void> p;
    double bytes = 0.0;
    uint64_t tick = 0;
  };
  template <class T>
  std::shared_ptr<const T> find(const std::string &key);
  template <class T>
  std::shared_ptr<const T> put(const std::string &key, T &&v, double bytes);
  void own() const { assert(owner_ == std::this_thread::get_id() && "Store 只给计算权持有者"); }

  std::mutex mu_;
  std::condition_variable cv_;
  bool busy_ = false;
  int waiting_[2] = {}; // [Prio] 排队数
  std::thread::id owner_;
  std::map<std::string, Entry> cache_;
  std::map<std::string, double> n_full_; // 层 | Domain::key → 满仓只数
  double bytes_ = 0.0;
  uint64_t tick_ = 0, epoch_ = 0; // LRU 时钟 / 缓存代 (clear 进一)
};

// 结果的作用域头 (Run 行 / Inspect 结果同一份): 池 / 期 / 形 / 封顶 / 满仓只数 / 后端 / 时刻
StatScope stat_scope(const FactorScope &sc, const Domain &dm, int seg, double n_full, const char *backend);

} // namespace GUI::Factors
