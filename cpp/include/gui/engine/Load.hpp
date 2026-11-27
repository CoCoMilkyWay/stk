// Load — 评估域 / 分块计划 / 两侧特征库装载 / 进程唯一计算面 Store / 作业层 (全部因子服务、篮子账与观察工具共用):
//   结局 Rc; 评估域 Domain (domain_of: 所用库 + 评估期 + 预热 + A 轴) → 预算 Budget + 前后带 Bands → 分块 Plan (plan_chunks);
//   两侧严格对仗 (共享逐天并行骨架 read_days, 统一经 Store::pin 钉成 Block):
//     X (特征输入叶: LeafId → Leaf → XPlanes, load_x; 含池篮子复用的 bar.1d 估值七叶)
//     Y (截面门控 + 建仓可成交码 YCore + 标签两侧 [hold] + 买入冲击 [amt] → YPlanes, load_y)
//   Store = 进程唯一的计算面 (线程池 ex + 库 lr + 宿主平面 LRU + 常驻算子/评估工作区 ExecSpace + 计算权优先级队列):
//     计算句柄 FactorFlow + 独占租约 Lease (计算权在手才可碰其余全部资源, 跨文件 / 跨趟零重分配复用).
//   作业层 = 规格 Need × 块消费者: Need → open_job → Job; 唯一的块循环 run_job: init → { want → 装载 → block } → finish.
//     块消费者两种对仗 (与探针的 init ⇄ finish 同构): 森林 ForestPass (Engine.hpp, forest_need) ⇄ 篮子 BasketPass (Ledger.hpp, basket_need).
#pragma once

#include "factor/Check.hpp" // check::Plane
#include "factor/Stat/Book.hpp"
#include "factor/Stat/Contract.hpp"
#include "features/Storage/FeaturePlane.hpp" // Day / LibReaders
#include "gui/engine/Table.hpp"              // FeatureTable / FactorScope / StatScope
#include "misc/epoch.hpp"
#include "misc/exec.hpp"
#include "misc/lru.hpp"
#include "shared/Progress.hpp"    // Progress
#include "shared/ReadRequest.hpp" // ReadScope

#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <thread>
#include <vector>

namespace GUI::Engine {

// ---- 评估结局 (装载 / 评估 / 全部服务同一个) ----
enum class Rc : uint8_t { Ok,
                          Empty,  // 评估域不成 (所用库未建 / 无共同落盘日 / 整期一块装不下), why
                          Broken, // 特征叶值域越界 (全部根), why
                          Cancel,
                          Stale }; // 库判废 (弃平面缓存 + 置 kStaleMsg)
inline constexpr const char *kStaleMsg = "特征库判废 (池轴/字段表与当前不符), 需重算特征";
inline Rc rc_of(bool ok, const LibReaders &lr) { return ok ? Rc::Ok : lr.stale() ? Rc::Stale
                                                                                 : Rc::Cancel; }
inline bool aborted(Rc rc) { return rc == Rc::Cancel || rc == Rc::Stale; }

// ---- 评估域 Domain 与分块计划 Plan ----
// 预热: FactorScope::years > 0 时评估期之前另装 max(⌈统计日 · kWarmFrac⌉, warm_days) 日, 只给时序回看, 不计统计 (库首不够则预热至多占一半)
inline constexpr double kWarmFrac = 0.1;
// 单窗最长回看 kMaxD 行折成的本层日数: 盖住 TS 口径根 TsRankRoll(ts_norm_d) 与任一单层窗, 统计期首行即满窗
constexpr size_t warm_days(int seg) { return static_cast<size_t>((factor::kMaxD + seg - 1) / seg); }
struct Domain {
  PoolId pool = kAllPool;       // 因子池
  LibMask used = 0;             // 所用库: 标签库 ∪ 叶所在库 ∪ (盘口成本时) Lob 库
  reader::ReadScope rs;         // main = 标签库, libs = used 各库清单, axis = A 轴, to = A → 各库轴
  std::vector<misc::Ymd> dates; // 装载日 = 预热 [0, warm) + 评估期 [warm, n) (升序; 评估期非空)
  std::vector<Day> days;        // [dates] 当日 + 承接日及其落盘位
  size_t warm = 0;              // 预热日数
  std::string key() const;      // 缓存键: 池 | 所用库目录 + 池轴 | 装载日首末 + 天数
  size_t stat_days() const { return dates.size() - warm; }
  misc::Ymd first() const { return dates[warm]; }
  std::vector<misc::Ymd> stat_dates() const { return {dates.begin() + static_cast<std::ptrdiff_t>(warm), dates.end()}; }
  bool uses(const Col &c) const { return used & lib_bit(lib_id(pool, c.tier)); }
};
void open_libs(const FactorScope &sc, LibReaders &lr);
LibMask used_libs(const FeatureTable &ft, PoolId pool, std::span<const LeafId> leaves, const CostSpec &cost);
bool span_of(const FeatureTable &ft, const FactorScope &sc, std::span<const LeafId> leaves, size_t keep, Domain &dm, std::string &why);
bool domain_of(const FeatureTable &ft, const FactorScope &sc, std::span<const LeafId> leaves, size_t keep, Domain &dm, std::string &why);
StatScope stat_scope(const FactorScope &sc, const Domain &dm, int seg, double n_full, const char *backend);

// 宿主内存预算 (总内存 × kHostFrac − 当前已用) 与空闲显存预算 (空闲显存 × kVramFrac); Store::budget / vram 另加回本计算面可复用部分
inline constexpr double kHostFrac = 0.8;
inline constexpr double kVramFrac = 0.85;
double host_budget();
double vram_budget();

// 一块 = 评估域日 [d0, d1): 前带 [d0, s0) 只给回看 / lag / 交接链, 统计日 [s0, s1), 后带 [s1, d1) 只给续持; 首块 d0 = 0 (含整段预热)
struct Chunk {
  size_t d0 = 0, s0 = 0, s1 = 0, d1 = 0;
  int days() const { return static_cast<int>(d1 - d0); }
};
// 字节 / 格: load = 宿主块 (X + Y); work = 引擎宿主槽与暂存; dev = 显存; fixed = 与格无关的宿主常驻 (字节; 由作业的 Bytes 按评估域形状现乘)
struct Budget {
  double load = 0.0, work = 0.0, fixed = 0.0, dev = 0.0;
  double host(double n) const { return n * (load + work) + fixed; }
  double vram(double n) const { return n * dev; }
};
// 前后带日数; front < 0 → 整期一块
struct Bands {
  int front = -1, back = 0;
};
struct Plan {
  std::vector<Chunk> chunks;
  int D = 0;            // 每块统计日上限
  double per_day = 0.0; // 每日格数 (seg × A)
  double host = 0.0;    // 宿主可用预算 (字节, 已扣 fixed)
  double vram = 0.0;    // 显存可用预算 (字节; dev = 0 → 0)
  double cache = 0.0;   // 全部块的装载量 (字节)
  int max_days() const;
  double max_cells() const { return max_days() * per_day; }
};
Plan plan_chunks(const Domain &dm, int seg, const Budget &b, Bands bd, double host, double vram = 0.0);

// ---- 两侧规格与平面: X ↔ Y 同形 ----
struct Shape {
  int T = 0, A = 0, days = 0, seg = 0;
  size_t n() const { return static_cast<size_t>(T) * A; }
  bool operator==(const Shape &) const = default;
};
// 评估域全段 (含预热) 的形状
inline Shape dom_shape(const Domain &dm, int seg) {
  const int days = static_cast<int>(dm.days.size());
  return {days * seg, static_cast<int>(dm.rs.axis.size()), days, seg};
}
// 线性字节式 (探针 / 账的常驻与暂存; 按评估域形状现乘): cell · T·A + row · T + day · 日数
struct Bytes {
  double cell = 0.0, row = 0.0, day = 0.0;
  constexpr Bytes operator+(const Bytes &o) const { return {cell + o.cell, row + o.row, day + o.day}; }
  constexpr Bytes operator*(double k) const { return {cell * k, row * k, day * k}; }
  double at(const Shape &sh) const { return cell * static_cast<double>(sh.n()) + row * sh.T + day * sh.days; }
};
// 成本段的要求 (探针声明; 落成哪几段由作用域成本口径 CostSpec 定): 不要 / 口径那一段 / 口径取盘口段时本域全部段 (固定段恒在)
enum class Segs : uint8_t { None,
                            Spec,
                            All };
struct YSpec { // Y 的身份 (装载 / 缓存 / 设备标签会话按它): 持有期 + 金额段
  Holds hd;
  Amts ad;
  std::string key() const;
};
YSpec y_spec(const FeatureTable &ft, const Domain &dm, const Holds &hd, Segs sg, const CostSpec &cost);

struct BlockSpec {
  std::vector<LeafId> x;
  YSpec y;
  bool ry = false;
  float top = 0.f; // 盘口冲击封顶 (只进账集 Cost, 不进 Y 的身份)
};
inline constexpr double kPlaneBytes = 5.0;
double cell_bytes(const BlockSpec &s, size_t nx);

struct Leaf : factor::check::Plane {
  mutable std::map<factor::Dom, std::string> dom;
};
struct YCore {
  std::vector<uint8_t> gate;
  std::vector<uint8_t> eo;
  std::vector<long long> on_day; // [块日] 门控格数 (满仓 n_full 的原料, Store 按 (库, 日) 留存)
};
// 一个持有期的标签平面: 做多收益 lv / 名义收益 nv (只偏移型, 锚点型空) / 有效位 m / 池基准 mk [T] (逐行 m 格的 lv 均值, 契约 Label::mk)
struct LabPlane {
  std::vector<uint16_t> lv, nv;
  std::vector<uint8_t> m;
  std::vector<float> mk;
};
inline factor::stat::Label label_of(const LabPlane &p, const uint8_t *eo, const uint16_t *ry) {
  return {p.lv.data(), p.nv.empty() ? nullptr : p.nv.data(), p.m.data(), eo, ry, p.mk.data()};
}

struct XPlanes {
  Shape sh;
  std::vector<const Leaf *> v; // [LeafId] 钉住的叶; nullptr = 不在块上
  const Leaf *find(LeafId id) const { return id < v.size() ? v[id] : nullptr; }
};
struct YPlanes {
  Shape sh;
  const uint8_t *gate = nullptr;
  const uint8_t *eo = nullptr;
  Holds hd;
  const LabPlane *labels[kHoldCount] = {};
  const uint16_t *ry[kHoldCount] = {};
  Amts ad;
  const uint16_t *costs[kAmtCount] = {}; // 盘口段: [kSides][n] 两侧冲击平面首尾相接 (买 [0, n), 卖 [n, 2n)); 固定段 nullptr
  float top = 0.f;                       // 盘口冲击封顶 (BlockSpec::top)
  double n_full = 0.0;
};
// 块首的盘口段冲击 (账集 Book.hpp; 固定段无平面)
inline factor::stat::Cost cost_of(const YPlanes &y) {
  const size_t n = y.sh.n();
  factor::stat::Cost c;
  c.top = y.top;
  for (int k = 0; k < y.ad.n; ++k)
    if (y.costs[k])
      c.imp[y.ad.k[k] - 1][kBuy] = y.costs[k], c.imp[y.ad.k[k] - 1][kSell] = y.costs[k] + n;
  return c;
}
factor::stat::Labels labels_of(const YPlanes &y);

struct Io {
  const FeatureTable &ft;
  const Domain &dm;
  const LibReaders &lr;
  misc::exec::ForkJoin &ex;
  const std::atomic<bool> &cancel;
  Progress &prog;
};
bool load_x(const Io &io, const Chunk &k, std::span<const LeafId> ids, std::vector<Leaf> &out);
bool load_y(const Io &io, const Chunk &k, const YCore *have, YCore &core, const Holds &hd, const Amts &ad, std::vector<LabPlane> &labels,
            std::vector<std::vector<uint16_t>> &costs);
bool gate_days(const Io &io, const Chunk &k, std::vector<long long> &on_day); // 只读门控: 块日 [d0, d1) 各日门控格数

// 块视图: 评估域日段 ck 的钉住平面 (x_gen = 层|域|日段|代, gen = x_gen + YSpec)
struct Block {
  Chunk ck;
  size_t nd = 0, warm = 0;
  int t_lo = 0, t_back = 0;
  uint64_t x_gen = 0, gen = 0;
  bool fresh = false;
  XPlanes x;
  YPlanes y;
  std::vector<std::shared_ptr<const void>> pins;
  factor::stat::Span span() const { return {y.sh.T, y.sh.A, factor::geo_of(y.sh.seg), t_lo, t_back}; }
  int rows_stat() const { return static_cast<int>(nd - warm) * y.sh.seg; }
  int land0() const { return (static_cast<int>(ck.d0) - static_cast<int>(warm)) * y.sh.seg; }
};
std::vector<const factor::check::Plane *> leaves_of(const Block &c, std::span<const LeafId> ids);

// ---- Store: 进程唯一计算面 + 计算句柄 FactorFlow + 独占租约 Lease ----
enum class Prio : uint8_t { Batch,
                            Interactive };

// 引擎常驻执行环境接口 (Engine.cpp 实现唯一实例挂入 Store; 宿主与显存占用纳入 Store 统一预算)
struct ExecSpace {
  virtual ~ExecSpace() = default;
  virtual double host_bytes() const = 0;
  virtual double vram_bytes() const = 0;
  virtual void drop_host() = 0;
  virtual void clear() = 0;
};

class Store {
public:
  static Store &get();
  misc::exec::ForkJoin ex;
  LibReaders lr;

  bool acquire(Prio p, const std::atomic<bool> &cancel);
  void release();

  Rc pin(Block &c, const Io &io, const Chunk &k, const BlockSpec &s, double reserve);
  void room(double need, double reserve = 0.0);
  double budget() const;
  double vram() const;
  void clear();

  ExecSpace *exec() const {
    own();
    return exec_.get();
  }
  void set_exec(std::unique_ptr<ExecSpace> e) {
    own();
    exec_ = std::move(e);
  }

private:
  friend class misc::Epochal<Store>;
  Store();
  template <class T>
  std::shared_ptr<const T> find(const std::string &key);
  template <class T>
  std::shared_ptr<const T> put(const std::string &key, T &&v, double bytes);
  template <class T, class Set>
  bool hit(Block &c, const std::string &key, Set &&set);
  template <class T, class Set>
  void land(Block &c, const std::string &key, T &&v, double n, Set &&set);
  void own() const { assert(owner_ == std::this_thread::get_id() && "Store 只给计算权持有者"); }

  std::mutex mu_;
  std::condition_variable cv_;
  bool busy_ = false;
  int waiting_[2] = {};
  std::thread::id owner_;
  misc::Lru<std::string, void> cache_;        // 宿主平面 (代价 = 字节; 钉着的不逐)
  std::map<std::string, long long> gate_day_; // (标签库 + 轴, 日) → 该日门控格数: 满仓 n_full 跨评估域 / 跨块复用, 缺日才补读
  std::unique_ptr<ExecSpace> exec_;
  uint64_t epoch_ = 0;
};

struct FactorFlow {
  Store &S;
  const std::atomic<bool> &cancel;
  Progress &prog;
  Prio prio;
  FactorFlow(const std::atomic<bool> &cancel, Progress &prog, Prio prio = Prio::Interactive) : S(Store::get()), cancel(cancel), prog(prog), prio(prio) {}
  Io io(const FeatureTable &ft, const Domain &dm) const { return {ft, dm, S.lr, S.ex, cancel, prog}; }
};

class Lease {
public:
  explicit Lease(FactorFlow &F) : S_(F.S), held_(F.S.acquire(F.prio, F.cancel)) {}
  ~Lease() {
    if (held_)
      S_.release();
  }
  Lease(const Lease &) = delete;
  Lease &operator=(const Lease &) = delete;
  explicit operator bool() const { return held_; }

private:
  Store &S_;
  const bool held_;
};

// ---- 作业层: Need → open_job → Job → run_job (唯一的块循环) ----
// 前后带: 前带 = 回看 look 行 + 最长持有期 (rAC lag / 前一批) 折日 + 交接链 kChainDays 日; 后带 = 最长持有期折日 (续持)
inline Bands bands_of(int look, const Holds &hd, int seg) {
  const int h = factor::stat::hold_reach(hd, seg);
  return {(look + h + seg - 1) / seg + kChainDays, (h + seg - 1) / seg};
}

// 一个作业对装载与预算的全部要求 (森林作业 forest_need ⇄ 篮子作业 basket_need; 唯一消费方 open_job)
struct Need {
  std::vector<LeafId> x;  // X 叶
  Holds hd;               // Y 标签的持有期
  Segs segs = Segs::None; // Y 成本段
  bool ry = false;        // Y 标签秩 (Stat 根 · CPU)
  Bands bands;            // 前后带 (front < 0 = 整期一块: 装不下即封顶年数)
  Budget cell;            // 每格的 load (两侧规格之外) / work / dev; fixed 由下一项现乘
  Bytes fixed;            // 与格无关的宿主常驻 (按评估域形状现乘; 年数封顶时随之重算)
};

struct Job {
  const char *who = "";
  const FeatureTable *ft = nullptr;
  BlockSpec spec;
  Bands bands;
  Budget b;
  Domain dm;
  Plan P;
  double n_full = 0.0;
  std::string why;
  Io io(FactorFlow &F) const { return F.io(*ft, dm); }
  double reserve() const { return P.max_cells() * b.work + b.fixed; }
  StatScope scope(const FactorScope &sc, const char *backend) const { return stat_scope(sc, dm, ft->seg, n_full, backend); }
};

// 唯一的作业入口: 评估域 → 两侧规格 → 前后带 → 预算 (整期一块装不下即封顶年数, Empty) → 分块计划
Rc open_job(FactorFlow &F, const FactorScope &sc, Need n, Job &j);

inline Rc load_block(FactorFlow &F, Job &j, size_t k, Block &c) {
  assert(k < j.P.chunks.size());
  const Rc rc = F.S.pin(c, j.io(F), j.P.chunks[k], j.spec, j.reserve());
  if (rc == Rc::Ok)
    j.n_full = c.y.n_full;
  return rc;
}

// 唯一的块循环. 块消费者 p (ForestPass ⇄ BasketPass): init(j) ⇄ finish(j) 首块前 / 末块后各一次; want(块) 挑块;
//   block(块, 块号, 末块, 装载秒) → Rc. 装载失败 / block 非 Ok / 取消即返回 (不 finish)
template <class P>
Rc run_job(FactorFlow &F, Job &j, P &&p) {
  using Clock = std::chrono::steady_clock;
  const size_t n = j.P.chunks.size();
  assert(n >= 1);
  p.init(j);
  for (size_t ci = 0; ci < n; ++ci) {
    if (!p.want(j.P.chunks[ci]))
      continue;
    const Clock::time_point t0 = Clock::now();
    Block c;
    if (const Rc rc = load_block(F, j, ci, c); rc != Rc::Ok)
      return rc;
    if (const Rc rc = p.block(c, ci, ci + 1 == n, std::chrono::duration<double>(Clock::now() - t0).count()); rc != Rc::Ok)
      return rc;
    if (F.cancel.load(std::memory_order_relaxed))
      return Rc::Cancel;
  }
  p.finish(j);
  return Rc::Ok;
}

} // namespace GUI::Engine
