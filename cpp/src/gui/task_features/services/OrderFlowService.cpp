// OrderFlowService Implementation — 线程模型与发布协议见头文件 / shared/OrderFlow.hpp
#include "gui/task_features/services/OrderFlowService.hpp"

#include "codec/OrderFrame.hpp"
#include "gui/util/Service.hpp"
#include "lob/LimitOrderBook.hpp"
#include "misc/profiler.hpp"
#include "shared/FeatureLib.hpp"
#include "shared/SharedData.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstring>
#include <initializer_list>
#include <limits>
#include <optional>
#include <thread>

namespace GUI::Features {

// ============================================================================
// 路表 (三路一行一路; 固定列 = 读计划首部 [0, k0), 尾挂列 = overlay 末尾 (已选中则不重复); 与 overlay 同一条读路径;
// 列下标按字段表 code 查, 字段表里没有 = assert). 读计划不许空 (read_plan 断言) → 每路至少一列固定列
//          网格     承接   固定列 [0, k0)                                              overlay 尾挂
//   Min:   L_1min  否     all  L_1day: open_day high_day low_day close_day ts_valid   —
//   Sec:   L_sec   是     main L_1min: ts_valid                                       main L_1min vwap (只 Lob 档)
//   Pick:  L_1min  是     all  L_1day: st_level list_age delist_age ind_l1            —
//   (Min 底图一日一根, 分钟 K 等要看就选成 overlay; Pick = Fund 节点日频收盘行, 不看当日有无分钟)
// ============================================================================

struct OrderFlowService::LaneDef {
  size_t grid; // 网格层 (行末对齐到此层的行)
  bool carry;  // 粗层开盘承接前日末行
  std::vector<ColRef> (*fixed)(const Feature &, PoolId main);
  std::vector<ColRef> (*extra)(const Feature &, PoolId main);
};

namespace {
enum MinCol : size_t { MIN_OPEN = OrderFlow::Min::kOpen,
                       MIN_HIGH = OrderFlow::Min::kHigh,
                       MIN_LOW = OrderFlow::Min::kLow,
                       MIN_CLOSE = OrderFlow::Min::kClose,
                       MIN_TS,
                       MIN_COUNT };
enum SecCol : size_t { SEC_TS = 0,
                       SEC_COUNT };
enum PickCol : size_t { PICK_RISK_WARN = 0,
                        PICK_LIST_AGE,
                        PICK_DELIST_AGE,
                        PICK_INDUSTRY,
                        PICK_COUNT };

std::vector<ColRef> refs(const Feature &f, PoolId pool, size_t level, std::initializer_list<const char *> codes) {
  std::vector<ColRef> out;
  for (const char *code : codes)
    out.push_back(f.ref(pool, level, code));
  return out;
}
std::vector<ColRef> min_cols(const Feature &f, PoolId) {
  return refs(f, kAllPool, L_1day, {"open_day", "high_day", "low_day", "close_day", "ts_valid"});
}
std::vector<ColRef> sec_cols(const Feature &f, PoolId main) {
  return refs(f, main, L_1min, {"ts_valid"});
}
std::vector<ColRef> pick_cols(const Feature &f, PoolId) {
  return refs(f, kAllPool, L_1day, {"st_level", "list_age", "delist_age", "ind_l1"});
}
std::vector<ColRef> no_cols(const Feature &, PoolId) {
  return {};
}
std::vector<ColRef> sec_extra(const Feature &f, PoolId main) {
  return pool_tier(main) == TierId::Lob ? refs(f, main, L_1min, {"vwap"}) : std::vector<ColRef>{};
}

// 第 i 位 = overlay 线 i 是其层 ts_valid (展示幅值 = micro price)
uint32_t ts_price_of(const Feature &feature, const std::vector<ColRef> &feats) {
  uint32_t m = 0;
  for (size_t i = 0; i < feats.size(); ++i)
    if (std::strcmp(feature.meta(feats[i]).code, "ts_valid") == 0)
      m |= 1u << i;
  return m;
}

// ts_valid 格有数据 (哨兵 = 门控不过 / 无源, 不算)
inline bool has_data(feature_storage_t g) { return fmeta::finite(g) && fmeta::data_valid(static_cast<float>(g)); }
// 格有源 (门控过; 值为 NaN 也算: 只排除 kInvalidBits)
inline bool present(feature_storage_t g) { return std::bit_cast<uint16_t>(g) != fmeta::kInvalidBits; }

// ============================================================================
// worker 私有常驻读端: 池集 reader (清单没变复用) + 双缓冲尾部行平面预读 (稳态零分配)
//   Min = stream (分批流式, 与 Dist / Transform / Corr 同一骨架), Sec / Pick = now (单日)
// ============================================================================
struct RowRead {
  LibReaders libs;
  Readers rd; // ids 指向 scope.to: scope 须活过本代 (lane 存在 cur_ 里)
  Prefetch<RowPlane> pf;

  // 换代: 按范围开库 (新请求 = 重读库: 重算后新库已落地, 判废解除; 判废拉起 abort), 按计划 × 批段数备容量
  void reset(const analysis::ReadScope &scope, const ReadPlan &plan, size_t segs, std::atomic<bool> *abort) {
    libs.open(scope.libs, abort);
    libs.revive();
    rd = scope.readers(libs.ptr);
    pf.prepare(analysis::threads_for(segs), plan, scope.sizes(), SIZE_MAX);
    for (RowPlane &p : pf.buf)
      p.prepare(scope.axis.size(), plan, segs);
  }
  // 段 [s0, s0 + bs) 的 IO (Prefetch 跑; 只写传入的平面)
  auto load(const ReadPlan &plan, const SegPlan &sp, const std::atomic<bool> &cancel) {
    return [this, &plan, &sp, &cancel](size_t s0, size_t bs) {
      return [this, &plan, &sp, &cancel, s0, bs](RowPlane &pl, misc::exec::ForkJoin &io, std::vector<IoSlot> &slots) {
        pl.load_batch(io, slots, rd, plan, sp, s0, bs, cancel);
      };
    };
  }
  // 单日 (sp = plan_day) 读进 front; false = 被打断 / 库判废 (abort 即 cancel)
  bool now(const ReadPlan &plan, const SegPlan &sp, const std::atomic<bool> &cancel) {
    assert(sp.size() == 1);
    pf.now(load(plan, sp, cancel)(0, 1));
    return !cancel.load(std::memory_order_relaxed);
  }
};

// 单资产范围: 资产在池集任一池内
bool any_pool(const analysis::ReadScope &scope) {
  return std::ranges::any_of(pool_ids(), [&](PoolId l) { return scope.has(l) && (scope.to[l].empty() || scope.to[l][0] != kAbsent); });
}

// overlay 各线值域 (worker 私有累加; 发布时整组写进 Overlay)
struct LineRange {
  std::array<float, OrderFlowConst::MAX_LINES> lo, hi;
  void reset() {
    lo.fill((std::numeric_limits<float>::max)());
    hi.fill(std::numeric_limits<float>::lowest());
  }
};

// overlay 线: 平面列 [k0, n) → 线 i = c − k0 (资产槽 0, 批内段 j). X = x0 + 网格行 Source::at[r] (尾部对齐; 网格 = plan.level);
// 开盘承接值先落在 x0 (粗层开盘时的已知值; 不承接 = 哨兵). 哨兵 (门控不过 / 无源 / NaN) 跳过;
// 同 X 覆盖 = 留末个 (细于网格的列每格取末值)
void emit_rows(const ReadPlan &plan, const RowPlane &p, size_t j, size_t k0, double x0, uint32_t ts_price,
               OrderFlow::Overlay &ov, LineRange &rg) {
  for (const Source &s : plan.srcs) {
    for (const uint32_t c : s.slot) {
      if (c < k0)
        continue;
      const size_t i = c - k0;
      const bool price = ts_price >> i & 1u;
      OrderFlow::FeatLine &fl = ov.line[i];
      const auto put = [&](double x, feature_storage_t raw) {
        if (fmeta::nan(raw))
          return;
        float v = static_cast<float>(raw);
        if (price)
          v = fmeta::price(v);
        if (!fl.x.empty() && fl.x.back() == x) {
          fl.y.back() = static_cast<double>(v);
        } else {
          fl.x.push_back(x);
          fl.y.push_back(static_cast<double>(v));
        }
        rg.lo[i] = std::min(rg.lo[i], v);
        rg.hi[i] = std::max(rg.hi[i], v);
      };
      put(x0, p.carry(c, 0, j));
      const feature_storage_t *v = p.series(c, 0, j);
      for (size_t r = 0; r < s.at.size(); ++r)
        put(x0 + static_cast<double>(s.at[r]), v[r]);
    }
  }
}

// 背槽待 GUI ack (帧首翻面, 无 cv 信号): 限时睡轮询; false = 等待中被新请求 / Stop 打断
template <class Slot>
bool await_back(const PingPong<Slot> &pp, const std::atomic<bool> &cancel) {
  while (!pp.writable()) {
    if (cancel.load(std::memory_order_relaxed))
      return false;
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  return true;
}
} // namespace

static_assert(OrderFlowConst::PRICE_SLOTS == PRICE_RANGE_SIZE, "热力图稠密价梯 = 订单簿价位下标空间");

//                                            网格     承接   固定列     尾挂
const OrderFlowService::LaneDef OrderFlowService::kMinDef{L_1min, false, min_cols, no_cols};
const OrderFlowService::LaneDef OrderFlowService::kSecDef{L_sec, true, sec_cols, sec_extra};
const OrderFlowService::LaneDef OrderFlowService::kPickDef{L_1min, true, pick_cols, no_cols};

// ============================================================================
// 三个 worker (gui/util/Service.hpp): board = SharedData 里各自的发布面
// ============================================================================

class OrderFlowService::MinLane : public Service<MinLane, RowReq> {
  friend Service<MinLane, RowReq>;

public:
  static constexpr const char *kName = "OF_MinLane";
  explicit MinLane(SharedData &data) : data_(data) { Start(); }
  ~MinLane() { Stop(); }

private:
  static constexpr Phase kStart = Phase::Running;
  static constexpr size_t kDaysPerBatch = 16; // 一批天数 = 发布粒度
  Progress &board() { return data_.orderflow.min; }
  bool run(RowReq &req);
  void emit(const RowPlane &p, size_t b0, size_t bd); // 批 [b0, b0 + bd) → 追加 OHLC + overlay
  void publish(size_t b0, size_t bd);                 // overlay 计数 / 值域 → pub word

  SharedData &data_;
  RowRead read_;
  RowReq cur_{};
  SegPlan days_;              // 日轴 (随 rescan 重扫; 池集变了 GUI 必带 rescan)
  PoolMask pools_ = 0;        // days_ 的池集
  std::vector<double> bar_x_; // [OHLC 层每日有效行] 一根 K 线的日内 X (所跨分钟的中点)
  LineRange range_;           // 本代 overlay 值域 (跨批累加)
};

class OrderFlowService::SecLane : public Service<SecLane, RowReq> {
  friend Service<SecLane, RowReq>;

public:
  static constexpr const char *kName = "OF_SecLane";
  explicit SecLane(SharedData &data) : data_(data) { Start(); }
  ~SecLane() { Stop(); }

private:
  struct Res {
    L2::FrameDecoder decoder; // 对仗 Lob 管线 Runner 的 decoder (日文件 reader 按请求现开)
    LimitOrderBook lob;       // 奢侈容量, 常驻复用 (对仗 Lob 管线 Runner 的工作区)
    TickData tick_data;       // 无特征侧绑定的盘口写出口
    OrderFlow::Sec::HeatScratch scratch;
    Res() : lob(L2::LOB_ORDER_CAPACITY) {}
  };
  static constexpr Phase kStart = Phase::Running;
  Progress &board() { return data_.orderflow.sec; }
  bool run(RowReq &req); // 重放 + 构建 + 背槽发布

  SharedData &data_;
  std::optional<Res> res_; // 首个请求时在 worker 线程建
  RowRead read_;
  RowReq cur_{};
};

class OrderFlowService::PickLane : public Service<PickLane, RowReq> {
  friend Service<PickLane, RowReq>;

public:
  static constexpr const char *kName = "OF_PickLane";
  explicit PickLane(SharedData &data) : data_(data) { Start(); }
  ~PickLane() { Stop(); }

private:
  static constexpr Phase kStart = Phase::Running;
  Progress &board() { return data_.orderflow.pick.slots; }
  bool run(RowReq &req); // 逐日 filter 列 → 全资产状态 + 背槽发布

  SharedData &data_;
  RowRead read_; // A 轴 = 主池轴 (候选范围); 状态列在 all 池 (Fund 只在 all 库落盘)
  RowReq cur_{};
};

// ============================================================================
// Lifecycle
// ============================================================================

OrderFlowService::OrderFlowService() = default;

OrderFlowService::~OrderFlowService() {
  Stop();
}

void OrderFlowService::Start(SharedData &data) {
  if (min_)
    return;
  min_ = std::make_unique<MinLane>(data);
  sec_ = std::make_unique<SecLane>(data);
  pick_ = std::make_unique<PickLane>(data);
}

void OrderFlowService::Stop() { // worker 析构即 Stop (join)
  min_.reset();
  sec_.reset();
  pick_.reset();
}

// ============================================================================
// Requests (GUI 线程; 最新覆盖旧的)
// ============================================================================

// 固定列在前, overlay (选中 + 尾挂) 在后; 尾部行 (各列按自己的层, 网格定行末对齐与开盘承接); 日轴 = 池集并集.
// 主池未建 → 退回 all 并丢 overlay (主池本就是 all 则不退)
bool OrderFlowService::make(const SharedData &data, RowReq &req, const LaneDef &def, PoolId main, const std::vector<ColRef> &feats,
                            std::vector<uint32_t> assets) const {
  assert(feats.size() <= OrderFlowConst::MAX_FEATURES);
  const auto attempt = [&](PoolId m, std::vector<ColRef> lines) {
    for (const ColRef &c : def.extra(data.feature, m))
      if (std::ranges::find(lines, c) == lines.end())
        lines.push_back(c);
    assert(lines.size() <= OrderFlowConst::MAX_LINES);
    std::vector<ColRef> cols = def.fixed(data.feature, m);
    req.spec.k0 = cols.size();
    req.spec.ts_price = ts_price_of(data.feature, lines);
    cols.insert(cols.end(), lines.begin(), lines.end());
    return analysis::read_request(data.config, data.feature,
                                  {.main = m,
                                   .days = analysis::DayAxis::Union,
                                   .level = def.grid,
                                   .rows = analysis::Rows::Tail,
                                   .carry = def.carry,
                                   .cols = std::move(cols),
                                   .assets = assets},
                                  req.scope, req.spec.plan);
  };
  return attempt(main, feats) || (main != kAllPool && attempt(kAllPool, {}));
}

// A 轴: Min / Sec = 选中资产, Pick = 主池轴
void OrderFlowService::Request(const SharedData &data, uint32_t gen, const OrderFlow::MinKey &key, bool rescan) {
  assert(min_ && "Request 先于 Start");
  assert(std::ranges::all_of(key.feats, [](const ColRef &c) { return c.level >= L_1min; }) && "Min 网格 = L_1min, 细层列不进");
  RowReq req{.spec = {.gen = gen, .rescan = rescan}};
  if (make(data, req, kMinDef, kAllPool, key.feats, {static_cast<uint32_t>(key.asset)}))
    min_->Request(std::move(req));
}

void OrderFlowService::Request(const SharedData &data, uint32_t gen, const OrderFlow::SecKey &key) {
  assert(sec_ && "Request 先于 Start");
  RowReq req{.spec = {.gen = gen, .date = key.date}};
  if (make(data, req, kSecDef, key.main, key.feats, {static_cast<uint32_t>(key.asset)}))
    sec_->Request(std::move(req));
}

void OrderFlowService::Request(const SharedData &data, uint32_t gen, const OrderFlow::PickKey &key) {
  assert(pick_ && "Request 先于 Start");
  RowReq req{.spec = {.gen = gen, .date = key.date}};
  if (make(data, req, kPickDef, key.main, {}, {}))
    pick_->Request(std::move(req));
}

// ============================================================================
// Min: 换代 + 分批流式 (Prefetch::stream)
// ============================================================================

bool OrderFlowService::MinLane::run(RowReq &req) {
  TraceN("OF_MinBegin");
  OrderFlow::Min &out = data_.orderflow.min;
  cur_ = std::move(req);
  const RowSpec &sp = cur_.spec;
  assert(sp.date == 0 && sp.k0 == MIN_COUNT);

  // 此刻 GUI 请求代 == req.gen, 发布代 == 旧代 → GUI 不读 min, dates/数组可安全重建
  if (sp.rescan || days_.dates.empty()) {
    out.begin(Phase::Scanning);
    days_ = analysis::plan_segs(cur_.scope, L_1min, analysis::DayOrder::Sequential, 0);
    pools_ = cur_.scope.days;
    out.dates = days_.dates;
  }
  assert(pools_ == cur_.scope.days && "池集变了须带 rescan (日轴随池集)");
  read_.reset(cur_.scope, sp.plan, kDaysPerBatch, &cancel_);
  range_.reset();

  // OHLC 层: 行 m 跨网格行 [上一行末 + 1, 行末] → 画在中点; 半宽按每根跨的分钟数折
  const size_t bar_lvl = sp.plan.cols[MIN_TS].level, R = level_valid_rows(bar_lvl);
  bar_x_.resize(R);
  for (size_t m = 0, lo = 0; m < R; ++m) {
    const size_t hi = level_row_of_sec(L_1min, level_end_sec(bar_lvl, m) - 1);
    bar_x_[m] = 0.5 * static_cast<double>(lo + hi);
    lo = hi + 1;
  }
  const double half_width = OrderFlowConst::CANDLESTICK_HALF_WIDTH * static_cast<double>(level_valid_rows(L_1min)) / static_cast<double>(R);

  out.begin_generation(sp.gen, std::vector<ColRef>(sp.plan.cols.begin() + sp.k0, sp.plan.cols.end()), half_width);
  out.reserve_capacity(R); // 首点发布前完成: 之后 data() 恒稳定

  // 请求侧 asset 是全局轴下标. 不在池集任一池内 (初始默认选中 / 换池后的残留选中,
  // 候选就绪后 GUI 会自动换代) → 无列可读, 直接发布"完整但空"的一代
  if (!any_pool(cur_.scope)) {
    out.pub.store(OrderFlow::Min::pack(sp.gen, out.dates.size(), 0), std::memory_order_release);
    return true;
  }

  // 批与批之间可被新代打断; 特征库被判废删除 (字段表改了) 经 abort 同样拉起 cancel_: 停流, 别把空转的陈旧列喂进 K 线;
  // 重算后 needs_rescan 自愈
  out.begin(Phase::Running, days_.size());
  return read_.pf.stream(
      days_.size(), kDaysPerBatch, cancel_, read_.load(sp.plan, days_, cancel_),
      [&](const RowPlane &p, size_t b0, size_t bd) { emit(p, b0, bd); },
      [&](size_t b0, size_t bd) { publish(b0, bd); });
}

void OrderFlowService::MinLane::emit(const RowPlane &p, size_t b0, size_t bd) {
  TraceN("OF_MinBatch");
  OrderFlow::Min &out = data_.orderflow.min;
  const RowSpec &sp = cur_.spec;
  for (size_t j = 0; j < bd; ++j) {
    const double x0 = static_cast<double>((b0 + j) * OrderFlowConst::MIN_CAPACITY);

    // OHLC: all 池日 K (all 池缺这天 = 门控哨兵, 无 K 线); 列 [MIN_OPEN, MIN_CLOSE] 与 out.ohlc 同序
    const feature_storage_t *ts = p.series(MIN_TS, 0, j);
    for (size_t m = 0; m < bar_x_.size(); ++m) {
      if (!has_data(ts[m]))
        continue;
      out.x.push_back(x0 + bar_x_[m]);
      for (size_t c = 0; c < out.ohlc.size(); ++c)
        out.ohlc[c].push_back(static_cast<double>(p.series(MIN_OPEN + c, 0, j)[m]));
    }

    // 特征 overlay: X = 日起点 + 行末秒所在分钟 (尾部对齐; 不承接 → 开盘承接值是哨兵, 不落点)
    emit_rows(sp.plan, p, j, sp.k0, x0, sp.ts_price, out.feat, range_);
  }
}

// 发布: 先 overlay 计数 / 值域 (release), 后 pub word (release) — GUI acquire 读 pub 即见全量
void OrderFlowService::MinLane::publish(size_t b0, size_t bd) {
  OrderFlow::Min &out = data_.orderflow.min;
  out.feat.publish(range_.lo.data(), range_.hi.data());
  out.pub.store(OrderFlow::Min::pack(cur_.spec.gen, b0 + bd, out.x.size()), std::memory_order_release);
  out.publish(bd);
}

// ============================================================================
// Pick: 承接日 L_1day filter 列 → 全资产 PIT 状态 → 背槽发布
//   Fund L_1day 列是盘后列 (行 D = 次日盘前态): 锚点日状态 = 开盘 as-of = 承接日 (日轴前一日) 的 L_1day 行 (RowPlane::carry; 轴首无承接 = 哨兵);
//   承接格无源 (轴首 / 资产不在 all 池 / all 缺承接日) = 状态不可判读 → has_data=false; 当日有无分钟不论
// ============================================================================

bool OrderFlowService::PickLane::run(RowReq &req) {
  PingPong<OrderFlow::Pick::Slot> &pp = data_.orderflow.pick.slots;
  if (!await_back(pp, cancel_))
    return false;
  TraceN("OF_PickBuild");
  cur_ = std::move(req);
  const RowSpec &sp = cur_.spec;
  assert(sp.date != 0 && sp.k0 == PICK_COUNT && sp.plan.n() == PICK_COUNT);
  read_.reset(cur_.scope, sp.plan, 1, &cancel_);
  if (!read_.now(sp.plan, analysis::plan_day(cur_.scope, sp.date), cancel_)) // 状态取承接日 L_1day 行
    return false;

  OrderFlow::Pick::Slot &out = pp.back();
  out.clear();
  out.gen = sp.gen;
  out.date = sp.date;

  // meta 槽位 = 全局轴下标 (TabOrderFlow 按 items 下标消费); 只填主池轴内的资产, 轴外恒 has_data = false (自然被候选过滤掉)
  out.meta.assign(data_.asset.items.size(), OrderFlow::Pick::Meta{});

  // 状态取承接日 L_1day 行; 承接格无源 = 状态不可判读
  const RowPlane &p = read_.pf.front();
  const PoolAxis &axis = cur_.scope.axis;
  for (size_t a = 0; a < axis.size(); ++a) {
    if (!present(p.carry(PICK_LIST_AGE, a, 0)))
      continue;
    OrderFlow::Pick::Meta &meta = out.meta[axis.global(a)];
    meta.has_data = true;

    const float rw = static_cast<float>(p.carry(PICK_RISK_WARN, a, 0));
    meta.risk_warn = (rw >= 0.0f && rw <= 3.0f) ? static_cast<uint8_t>(rw + 0.5f) : 0;

    const float ind = static_cast<float>(p.carry(PICK_INDUSTRY, a, 0));
    meta.industry_l1 = (ind >= 0.0f && ind < static_cast<float>(fund::SW2021_L1_COUNT))
                           ? static_cast<uint8_t>(ind + 0.5f)
                           : 0;

    // list_age/delist_age: 非 NaN = 已上市/已退市 (>= 0 日历日), NaN = 未发生
    const float lage = static_cast<float>(p.carry(PICK_LIST_AGE, a, 0));
    const float dage = static_cast<float>(p.carry(PICK_DELIST_AGE, a, 0));
    meta.delisted = !std::isnan(dage);
    meta.listed = !std::isnan(lage) && !meta.delisted;
  }

  pp.commit();
  return true;
}

// ============================================================================
// Sec: 日文件逐笔重放 → 秒序列 + 热力图 → overlay → 背槽发布
// ============================================================================

bool OrderFlowService::SecLane::run(RowReq &in) {
  PingPong<OrderFlow::Sec> &pp = data_.orderflow.sec;
  if (!await_back(pp, cancel_))
    return false;
  TraceN("OF_SecBuild");
  if (!res_)
    res_.emplace();
  Res &res = *res_;
  cur_ = std::move(in);
  const RowSpec &req = cur_.spec;
  const analysis::ReadScope &scope = cur_.scope;
  assert(req.date != 0 && scope.axis.size() == 1 && req.k0 == SEC_COUNT);
  read_.reset(scope, req.plan, 1, &cancel_);
  const size_t asset_idx = scope.axis.global(0);

  OrderFlow::Sec &out = pp.back();
  out.clear();
  out.gen = req.gen;
  out.date = req.date;
  out.asset_idx = asset_idx;

  // ---- 重放当天日文件里这个资产的帧 (无帧 = 停牌/未编码/早于逐笔, 发布空槽) ----
  // 帧目录快照定位 (与 TS worker 同一条路); 快照与重扫描并发安全, 本请求期间持有.
  const AssetItem &asset = data_.asset.items[asset_idx];
  const std::shared_ptr<const OrdersCatalog> orders = data_.asset.orders();
  const FrameRef &ref = orders->at(orders->day_idx(req.date), asset_idx);
  L2::FrameView view;
  if (!ref.empty()) {
    TraceN("OF_Decode");
    view = DayReader(data_.config.orders_dir, req.date).get(ref, res.decoder); // 解出来的在 decoder 缓冲里, fd 随即关掉
  }

  if (view.head.count > 0) {
    const size_t order_num = view.head.count;
    out.order_count = order_num;

    LimitOrderBook &lob = res.lob;
    lob.bind(&res.tick_data, asset_idx, asset.exchange_type); // 无特征侧绑定
    lob.begin_frame(view);

    constexpr size_t N = OrderFlowConst::LOB_DEPTH;
    const LOB_Feature &lf = res.tick_data.lob;
    OrderFlow::Sec::HeatScratch &scratch = res.scratch;
    out.begin(scratch, lf.price_base); // begin_frame 已按当日文件头定 price_base

    // 秒末全簿快照 → 热力图 (只提交出过有效盘口快照的秒; 全价位, 不限近端 30 个价位)
    auto commit_second = [&](size_t t) {
      if (out.row_of[t] == OrderFlow::Sec::kNoRow)
        return;
      scratch.cur.clear();
      lob.for_each_visible_level([&](float price, int32_t net_qty) {
        const int32_t amount_rmb = round_amount_to_rmb(static_cast<float>(net_qty) * price); // 股 × 元, SIGNED
        if (amount_rmb != 0)
          scratch.cur.emplace_back(price_to_key(price), amount_rmb); // 位图升序 → key 升序
      });
      out.heat_commit(scratch, t);
    };

    {
      TraceN("OF_Replay");
      size_t cur_sec = SIZE_MAX;
      for (size_t i = 0; i < order_num; ++i) {
        const L2::Order &order = view.orders[i];
        const size_t t = Clock_to_L_sec(L2::tick_hour(order.tick), L2::tick_minute(order.tick), L2::tick_second(order.tick)); // 钳在 [0, 15300)
        if (t != cur_sec) {
          if (cur_sec != SIZE_MAX)
            commit_second(cur_sec); // 上一秒结束: 秒末终值全簿入热力图
          cur_sec = t;
        }
        lob.process(order);
        if (!lf.depth_updated)
          continue;

        // 盘口顶无真实价位 (纯哨兵): 本秒不出快照
        const Level *bid1 = lf.depth_buffer[N], *ask1 = lf.depth_buffer[N - 1];
        if (!LOB_Feature::is_real(bid1) || !LOB_Feature::is_real(ask1))
          continue;

        // 竞价预撮合 (订单簿竞价分支产出; ref > 0 = 竞价且簿交叉, 其余恒 0)
        const float ref_price = lf.auction_ref_price;
        out.put(t, lf.price_of(bid1), lf.price_of(ask1), ref_price, ref_price * static_cast<float>(lf.auction_matched_qty),
                ref_price * static_cast<float>(lf.auction_imbalance));
      }
      if (cur_sec != SIZE_MAX)
        commit_second(cur_sec); // 收尾: 最后一秒
    }
    lob.unbind(); // 归还干净簿 (下次 bind 断言)
    out.end();
  }

  // ---- 特征列 (overlay 含尾挂 VWAP, 与盘口独立: 一次读计划, 与图2 同源; 盘口重放走日文件, 不受池轴约束, 上面照常构建) ----
  // 承接日 = 日轴上的前一日 (plan_day; 各源池当日已落盘才承接): 粗层开盘时的已知值 = 前日末行
  {
    TraceN("OF_SecFeats");
    if (!read_.now(req.plan, analysis::plan_day(scope, req.date), cancel_))
      return false;
  }

  // overlay: X = 行末秒 − 1 (尾部对齐); 开盘承接值落在 X = 0
  out.feat.begin(std::vector<ColRef>(req.plan.cols.begin() + req.k0, req.plan.cols.end()));
  LineRange range;
  range.reset();
  emit_rows(req.plan, read_.pf.front(), 0, req.k0, 0.0, req.ts_price, out.feat, range);
  out.feat.publish(range.lo.data(), range.hi.data());

  // ---- 背槽整体发布 (GUI 帧首 ack 翻面) ----
  pp.commit();
  return true;
}

} // namespace GUI::Features
