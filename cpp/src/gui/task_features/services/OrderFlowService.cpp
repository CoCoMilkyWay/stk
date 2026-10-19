// OrderFlowService Implementation — 线程模型与发布协议见头文件 / shared/OrderFlow.hpp
#include "gui/task_features/services/OrderFlowService.hpp"

#include "codec/OrderFrame.hpp"
#include "features/Storage/FeatureRead.hpp"
#include "gui/util/Service.hpp"
#include "lob/LimitOrderBook.hpp"
#include "misc/profiler.hpp"
#include "shared/FeatureLib.hpp"
#include "shared/SharedData.hpp"

#include <algorithm>
#include <array>
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
// 固定列 (读计划首部, 与 overlay 同一条读路径; 列下标按字段表 code 查, 字段表里没有 = assert)
//   Kline: 主库 L_min OHLC + ts_valid      Depth: 主库 L_min ts_valid + vwap
//   Pick:  all 库 L_min ts_valid (有无数据) + L_day 收盘行状态列 (Fund 节点日频)
// ============================================================================

namespace {
enum KlineCol : size_t { KL_OPEN = 0,
                         KL_HIGH,
                         KL_LOW,
                         KL_CLOSE,
                         KL_TS,
                         KL_COUNT };
enum DepthCol : size_t { DP_TS = 0,
                         DP_VWAP,
                         DP_COUNT };
enum PickCol : size_t { PICK_TS = 0,
                        PICK_RISK_WARN,
                        PICK_LIST_AGE,
                        PICK_DELIST_AGE,
                        PICK_INDUSTRY,
                        PICK_COUNT };

std::vector<ColRef> fixed_cols(const Feature &feature, LibId lib, size_t level, std::initializer_list<const char *> codes) {
  const Feature::Metadata &meta = feature.meta(lib_tier(lib));
  std::vector<ColRef> out;
  for (const char *code : codes)
    out.push_back({lib, static_cast<uint8_t>(level), static_cast<uint16_t>(meta.col_of(level, code))});
  return out;
}
std::vector<ColRef> kline_cols(const Feature &f, LibId main) {
  return fixed_cols(f, main, L_min, {"lob_open", "lob_high", "lob_low", "lob_close", "ts_valid"});
}
std::vector<ColRef> depth_cols(const Feature &f, LibId main) { return fixed_cols(f, main, L_min, {"ts_valid", "vwap"}); }
std::vector<ColRef> pick_cols(const Feature &f) {
  std::vector<ColRef> out = fixed_cols(f, kAllLib, L_min, {"ts_valid"});
  for (const ColRef &c : fixed_cols(f, kAllLib, L_day, {"st_level", "list_age", "delist_age", "ind_l1"}))
    out.push_back(c);
  return out;
}

// 固定列 + overlay (读计划列序)
std::vector<ColRef> with_feats(std::vector<ColRef> fixed, const std::vector<ColRef> &feats) {
  assert(feats.size() <= OrderFlowConst::MAX_FEATURES);
  fixed.insert(fixed.end(), feats.begin(), feats.end());
  return fixed;
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

// ============================================================================
// worker 私有常驻读端: 库集 reader (清单没变复用) + IO 暂存 + 原生行平面 (稳态零分配)
// ============================================================================
struct RowRead {
  LibReaders libs;
  Readers rd; // ids 指向 scope.to: scope 须活过本代 (lane 存在 cur_ 里)
  IoSlot io;
  RowPlane plane;

  // 换代: 按范围开库 (新请求 = 重读库: 重算后新库已落地, 判废解除), 按计划备容量
  void reset(const analysis::ReadScope &scope, const ReadPlan &plan) {
    libs.open(scope.libs, nullptr);
    libs.revive();
    rd = scope.readers(libs.ptr);
    io.prepare(plan, scope.sizes(), SIZE_MAX);
    plane.prepare(scope.axis.size(), plan);
  }
  // 一天 (prev 0 = 不承接); false = 库判废
  bool load(const ReadPlan &plan, misc::Ymd date, misc::Ymd prev) {
    plane.load_day(rd, plan, date, prev, io);
    return !libs.stale();
  }
};

// 单资产范围: 资产在库集任一库内
bool any_lib(const analysis::ReadScope &scope) {
  return std::ranges::any_of(lib_ids(), [&](LibId l) { return scope.has(l) && (scope.to[l].empty() || scope.to[l][0] != kAbsent); });
}

// overlay 线: 平面列 [k0, n) → 线 i = c − k0 (资产槽 0). 哨兵 (门控不过 / 无源 / NaN) 跳过, X = x_of(层, 行) (尾部对齐);
// 同 X 覆盖 = 留末个 (细于网格的列每格取末值). x_open ≥ 0 → 开盘承接值先落在 x_open (粗层开盘时的已知值)
template <class XOf>
void emit_rows(const ReadPlan &plan, const RowPlane &p, size_t k0, uint32_t ts_price, double x_open, XOf x_of,
               OrderFlow::FeatLine *lines, float *y_min, float *y_max) {
  for (size_t c = k0; c < plan.n(); ++c) {
    const size_t i = c - k0, lvl = plan.cols[c].level;
    OrderFlow::FeatLine &fl = lines[i];
    const auto put = [&](double x, feature_storage_t raw) {
      if (fmeta::nan(raw))
        return;
      float v = static_cast<float>(raw);
      if (ts_price >> i & 1u)
        v = fmeta::price(v);
      if (!fl.x.empty() && fl.x.back() == x) {
        fl.y.back() = static_cast<double>(v);
      } else {
        fl.x.push_back(x);
        fl.y.push_back(static_cast<double>(v));
      }
      y_min[i] = std::min(y_min[i], v);
      y_max[i] = std::max(y_max[i], v);
    };
    if (x_open >= 0.0)
      put(x_open, p.carry(c, 0));
    const feature_storage_t *s = p.series(c, 0);
    for (size_t r = 0; r < p.rows[c]; ++r)
      put(x_of(lvl, r), s[r]);
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

// ============================================================================
// 三个 worker (gui/util/Service.hpp): board = SharedData 里各自的发布面
// ============================================================================

class OrderFlowService::KlineLane : public Service<KlineLane, KlineReq> {
  friend Service<KlineLane, KlineReq>;

public:
  static constexpr const char *kName = "OF_KlineLane";
  explicit KlineLane(SharedData &data) : data_(data) { Start(); }
  ~KlineLane() { Stop(); }

private:
  static constexpr Phase kStart = Phase::Running;
  Progress &board() { return data_.orderflow.kline; }
  bool run(KlineReq &req);
  bool step(size_t d); // 装一天并发布; false = 库判废

  SharedData &data_;
  RowRead read_;
  KlineReq cur_{};
  double y_min_ = 0.0, y_max_ = 0.0; // OHLC 运行范围
  std::vector<float> feat_min_, feat_max_;
};

class OrderFlowService::DepthLane : public Service<DepthLane, DepthReq> {
  friend Service<DepthLane, DepthReq>;

public:
  static constexpr const char *kName = "OF_DepthLane";
  explicit DepthLane(SharedData &data) : data_(data) { Start(); }
  ~DepthLane() { Stop(); }

private:
  struct Res {
    L2::FrameDecoder decoder; // 对仗 Lob 管线 Runner 的 decoder (日文件 reader 按请求现开)
    LimitOrderBook lob;       // 奢侈容量, 常驻复用 (对仗 Lob 管线 Runner 的工作区)
    TickData tick_data;       // 无特征侧绑定的盘口写出口
    OrderFlow::Depth::HeatmapScratch scratch;
    std::vector<int32_t> sec_slot; // 秒 -> ticks 下标 (重放临时, -1 = 未见)
    std::vector<uint8_t> sec_data; // 秒 -> 有逐笔
    Res() : lob(L2::LOB_ORDER_CAPACITY) {}
  };
  static constexpr Phase kStart = Phase::Running;
  Progress &board() { return data_.orderflow.depth; }
  bool run(DepthReq &req); // 重放 + 构建 + 背槽发布

  SharedData &data_;
  std::optional<Res> res_; // 首个请求时在 worker 线程建
  RowRead read_;
  DepthReq cur_{};
};

class OrderFlowService::PickLane : public Service<PickLane, PickReq> {
  friend Service<PickLane, PickReq>;

public:
  static constexpr const char *kName = "OF_PickLane";
  explicit PickLane(SharedData &data) : data_(data) { Start(); }
  ~PickLane() { Stop(); }

private:
  static constexpr Phase kStart = Phase::Running;
  Progress &board() { return data_.orderflow.pick.slots; }
  bool run(PickReq &req); // 逐日 filter 列 → 全资产状态 + 背槽发布

  SharedData &data_;
  RowRead read_; // A 轴 = 主库轴 (候选范围); 状态列在 all 库 (Fund 只在 all 库落盘)
  PickReq cur_{};
};

// ============================================================================
// Lifecycle
// ============================================================================

OrderFlowService::OrderFlowService(LibId main) : main_(main) { assert(lib_tier(main) == TierId::Lob && "OrderFlow 主库 = Lob 档库"); }

OrderFlowService::~OrderFlowService() {
  Stop();
}

void OrderFlowService::Start(SharedData &data) {
  if (kline_)
    return;
  kline_ = std::make_unique<KlineLane>(data);
  depth_ = std::make_unique<DepthLane>(data);
  pick_ = std::make_unique<PickLane>(data);
}

void OrderFlowService::Stop() { // worker 析构即 Stop (join)
  kline_.reset();
  depth_.reset();
  pick_.reset();
}

// ============================================================================
// Requests (GUI 线程; 最新覆盖旧的)
// ============================================================================

// 网格: Kline = L_min (不承接: 前一日的点已在图上), Depth = L_sec (粗层开盘承接前日末行); A 轴 = 选中资产
void OrderFlowService::RequestKline(const SharedData &data, uint32_t gen, size_t asset_idx, const std::vector<ColRef> &feats,
                                    bool rescan_dates) {
  assert(kline_ && "Request 先于 Start");
  std::vector<ColRef> cols = with_feats(kline_cols(data.feature, main_), feats);
  KlineReq req{.gen = gen, .asset = asset_idx, .ts_price = ts_price_of(data.feature, feats), .rescan = rescan_dates};
  req.scope = analysis::read_scope(data.config, main_, cols, {static_cast<uint32_t>(asset_idx)});
  if (req.scope.months.empty())
    return;
  req.plan = analysis::read_rows(data.feature, main_, L_min, std::move(cols));
  kline_->Request(std::move(req));
}

void OrderFlowService::RequestDepth(const SharedData &data, uint32_t gen, misc::Ymd date, size_t asset_idx,
                                    const std::vector<ColRef> &feats) {
  assert(depth_ && "Request 先于 Start");
  std::vector<ColRef> cols = with_feats(depth_cols(data.feature, main_), feats);
  DepthReq req{.gen = gen, .date = date, .asset = asset_idx, .ts_price = ts_price_of(data.feature, feats)};
  req.scope = analysis::read_scope(data.config, main_, cols, {static_cast<uint32_t>(asset_idx)});
  if (req.scope.months.empty())
    return;
  req.plan = analysis::read_rows(data.feature, main_, L_sec, std::move(cols));
  depth_->Request(std::move(req));
}

void OrderFlowService::RequestPick(const SharedData &data, uint32_t gen, misc::Ymd date) {
  assert(pick_ && "Request 先于 Start");
  std::vector<ColRef> cols = pick_cols(data.feature);
  PickReq req{.gen = gen, .date = date};
  req.scope = analysis::read_scope(data.config, main_, cols);
  if (req.scope.months.empty())
    return;
  req.plan = analysis::read_rows(data.feature, main_, L_min, std::move(cols));
  pick_->Request(std::move(req));
}

// ============================================================================
// 日期扫描: 库清单日轴 (lib.json dates, 升序) 中已落盘的日
// ============================================================================

namespace {
void scan_dates(const FeatureLib &lib, std::vector<misc::Ymd> &out) {
  TraceN("OF_ScanDates");
  for (const misc::Ymd date : lib.dates)
    if (FeatureRead::has_date(lib.tier, lib.dir, date))
      out.push_back(date);
}
} // namespace

// ============================================================================
// Kline: 换代 + 逐日流式
// ============================================================================

bool OrderFlowService::KlineLane::run(KlineReq &req) {
  TraceN("OF_KlineBegin");
  OrderFlow::Kline &k = data_.orderflow.kline;
  cur_ = std::move(req);
  read_.reset(cur_.scope, cur_.plan);

  // 此刻 GUI 请求代 == req.gen, 发布代 == 旧代 → GUI 不读 kline, dates/数组可安全重建
  if (cur_.rescan || k.dates.empty()) {
    k.begin(Phase::Scanning);
    k.dates.clear();
    scan_dates(cur_.scope.lib(), k.dates);
  }

  const size_t nf = cur_.plan.n() - KL_COUNT;
  y_min_ = (std::numeric_limits<double>::max)();
  y_max_ = std::numeric_limits<double>::lowest();
  feat_min_.assign(nf, (std::numeric_limits<float>::max)());
  feat_max_.assign(nf, std::numeric_limits<float>::lowest());

  k.begin_generation(cur_.gen, cur_.asset, nf);
  k.reserve_capacity(); // 发布前完成: 之后 data() 恒稳定

  // 请求侧 asset 是全局轴下标. 不在库集任一库内 (初始默认选中 / 换票池后的残留选中,
  // 候选就绪后 GUI 会自动换代) → 无列可读, 直接发布"完整但空"的一代
  if (!any_lib(cur_.scope)) {
    k.pub.store(OrderFlow::Kline::pack(cur_.gen, k.dates.size(), 0), std::memory_order_release);
    return true;
  }

  k.begin(Phase::Running, k.dates.size());
  for (size_t d = 0; d < k.dates.size(); ++d) {
    if (cancel_.load(std::memory_order_relaxed))
      return false; // 一天一步: 新请求在天与天之间打断
    if (!step(d))
      return false; // 特征库被判废删除 (字段表改了): 停流, 别把空转的陈旧列喂进 K 线; 重算后 needs_rescan 自愈
    k.publish(1);
  }
  return true;
}

bool OrderFlowService::KlineLane::step(size_t d) {
  TraceN("OF_KlineDay");
  OrderFlow::Kline &k = data_.orderflow.kline;
  assert(d < k.dates.size());
  // 一天一次读计划 (逐源 (库, 层) 选列读); 不承接: 前一日的点已在图上
  if (!read_.load(cur_.plan, k.dates[d], 0))
    return false;
  const RowPlane &p = read_.plane;
  const double x0 = static_cast<double>(d * OrderFlowConst::MIN_CAPACITY);
  const size_t nf = cur_.plan.n() - KL_COUNT;

  const feature_storage_t *ts = p.series(KL_TS, 0);
  for (size_t m = 0; m < level_valid_rows(L_min); ++m) {
    if (!has_data(ts[m]))
      continue;

    const double o = static_cast<double>(p.series(KL_OPEN, 0)[m]);
    const double h = static_cast<double>(p.series(KL_HIGH, 0)[m]);
    const double l = static_cast<double>(p.series(KL_LOW, 0)[m]);
    const double c = static_cast<double>(p.series(KL_CLOSE, 0)[m]);

    k.x.push_back(x0 + static_cast<double>(m));
    k.open.push_back(o);
    k.high.push_back(h);
    k.low.push_back(l);
    k.close.push_back(c);
    y_min_ = std::min(y_min_, l);
    y_max_ = std::max(y_max_, h);
  }

  // 特征 overlay: X = 日起点 + 行末秒所在分钟 (尾部对齐)
  const auto x_of = [x0](size_t lvl, size_t r) {
    return x0 + static_cast<double>(L_sec_to_L_min(level_end_sec(lvl, r) - 1));
  };
  emit_rows(cur_.plan, p, KL_COUNT, cur_.ts_price, -1.0, x_of, k.feat.data(), feat_min_.data(), feat_max_.data());

  // 发布: 先各数组计数/范围 (release), 后 pub word (release) — GUI acquire 读 pub 即见全量
  if (y_min_ <= y_max_) {
    k.y_min.store(y_min_, std::memory_order_relaxed);
    k.y_max.store(y_max_, std::memory_order_relaxed);
  }
  for (size_t i = 0; i < nf; ++i) {
    k.feat_y_min[i].store(feat_min_[i], std::memory_order_relaxed);
    k.feat_y_max[i].store(feat_max_[i], std::memory_order_relaxed);
    k.feat_n[i].store(k.feat[i].x.size(), std::memory_order_release);
  }
  k.pub.store(OrderFlow::Kline::pack(cur_.gen, d + 1, k.x.size()), std::memory_order_release);
  return true;
}

// ============================================================================
// Pick: 锚点日的 L_min filter 列 → 全资产 PIT 状态 → 背槽发布
//   Fund 是 onDay 算一次 / onMinute 原样广播, 故当日任一有效分钟的值即当日状态;
//   取首个 ts_valid 有效的分钟 (无有效分钟 = 落盘缓冲清零, 状态不可判读 → has_data=false)
// ============================================================================

bool OrderFlowService::PickLane::run(PickReq &req) {
  PingPong<OrderFlow::Pick::Slot> &pp = data_.orderflow.pick.slots;
  if (!await_back(pp, cancel_))
    return false;
  TraceN("OF_PickBuild");
  cur_ = std::move(req);
  read_.reset(cur_.scope, cur_.plan);
  if (!read_.load(cur_.plan, cur_.date, 0)) // 状态取当日 L_day 行 0, 不承接
    return false;

  OrderFlow::Pick::Slot &slot = pp.back();
  slot.clear();
  slot.gen = cur_.gen;
  slot.date = cur_.date;

  // meta 槽位 = 全局轴下标 (TabOrderFlow 按 items 下标消费); 只填主库轴内的资产, 轴外恒 has_data = false (自然被候选过滤掉)
  slot.meta.assign(data_.asset.items.size(), OrderFlow::Pick::Meta{});

  // 每资产首个有效分钟即判有数据 (无有效分钟 / all 库缺这天 = 状态不可判读); 状态取 L_day 行 0
  const RowPlane &p = read_.plane;
  const LibAxis &axis = cur_.scope.axis;
  for (size_t a = 0; a < axis.size(); ++a) {
    const feature_storage_t *ts = p.series(PICK_TS, a);
    if (std::none_of(ts, ts + p.rows[PICK_TS], has_data))
      continue;
    OrderFlow::Pick::Meta &meta = slot.meta[axis.global(a)];
    meta.has_data = true;

    const float rw = static_cast<float>(p.series(PICK_RISK_WARN, a)[0]);
    meta.risk_warn = (rw >= 0.0f && rw <= 3.0f) ? static_cast<uint8_t>(rw + 0.5f) : 0;

    const float ind = static_cast<float>(p.series(PICK_INDUSTRY, a)[0]);
    meta.industry_l1 = (ind >= 0.0f && ind < static_cast<float>(fund::SW2021_L1_COUNT))
                           ? static_cast<uint8_t>(ind + 0.5f)
                           : 0;

    // list_age/delist_age: 非 NaN = 已上市/已退市 (>= 0 日历日), NaN = 未发生
    const float lage = static_cast<float>(p.series(PICK_LIST_AGE, a)[0]);
    const float dage = static_cast<float>(p.series(PICK_DELIST_AGE, a)[0]);
    meta.delisted = !std::isnan(dage);
    meta.listed = !std::isnan(lage) && !meta.delisted;
  }

  pp.commit();
  return true;
}

// ============================================================================
// Depth: 日文件逐笔重放 → 秒级快照 → plot/heatmap → 背槽发布
// ============================================================================

bool OrderFlowService::DepthLane::run(DepthReq &in) {
  PingPong<OrderFlow::Depth> &pp = data_.orderflow.depth;
  if (!await_back(pp, cancel_))
    return false;
  TraceN("OF_DepthBuild");
  if (!res_)
    res_.emplace();
  Res &res = *res_;
  cur_ = std::move(in);
  const DepthReq &req = cur_;
  read_.reset(req.scope, req.plan);

  OrderFlow::Depth &slot = pp.back();
  slot.clear();
  slot.gen = req.gen;
  slot.date = req.date;
  slot.asset_idx = req.asset;

  // ---- 重放当天日文件里这个资产的帧 (无帧 = 停牌/未编码, 发布空槽) ----
  // 帧目录快照定位 (与 TS worker 同一条路); 快照与重扫描并发安全, 本请求期间持有.
  const AssetItem &asset = data_.asset.items[req.asset];
  const std::shared_ptr<const OrdersCatalog> orders = data_.asset.orders();
  const FrameRef &ref = orders->at(orders->day_idx(req.date), req.asset);
  L2::FrameView view;
  if (!ref.empty()) {
    TraceN("OF_Decode");
    view = DayReader(data_.config.orders_dir, req.date).get(ref, res.decoder); // 解出来的在 decoder 缓冲里, fd 随即关掉
  }

  if (view.head.count > 0) {
    const size_t order_num = view.head.count;
    slot.order_count = order_num;

    LimitOrderBook &lob = res.lob;
    lob.bind(&res.tick_data, req.asset, asset.exchange_type); // 无特征侧绑定
    lob.begin_frame(view);

    auto &sec_slot = res.sec_slot;
    auto &sec_data = res.sec_data;
    auto &scratch = res.scratch;
    sec_slot.assign(level_valid_rows(L_sec), -1);
    sec_data.assign(level_valid_rows(L_sec), 0);
    slot.ticks.reserve(level_valid_rows(L_sec));
    slot.heatmap_begin(scratch);

    constexpr size_t N = OrderFlowConst::LOB_DEPTH;
    constexpr float NAN_F = std::numeric_limits<float>::quiet_NaN();
    constexpr float VOLUME_TO_LOT = 0.01f; // 股 → 手
    const LOB_Feature &lf = res.tick_data.lob;

    // 秒末全簿快照 → 热力图 (只提交出过有效盘口快照的秒; 全价位, 不限近端 30 个价位)
    auto commit_second = [&](size_t t) {
      if (sec_slot[t] < 0)
        return;
      scratch.current_tick.clear();
      lob.for_each_visible_level([&](float price, int32_t net_qty) {
        const int32_t amount_rmb = round_amount_to_rmb(static_cast<float>(net_qty) * price); // 股 × 元, SIGNED
        if (amount_rmb != 0)
          scratch.current_tick.emplace_back(price_to_key(price), amount_rmb); // 位图升序 → key 升序
      });
      slot.heatmap_commit_tick(scratch, t);
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
        sec_data[t] = 1;
        if (!lf.depth_updated)
          continue;

        // 盘口顶无真实价位 (纯哨兵): 本秒不出快照
        if (!LOB_Feature::is_real(lf.depth_buffer[N]) || !LOB_Feature::is_real(lf.depth_buffer[N - 1]))
          continue;

        // 秒内覆盖写: 终值 = 秒末盘口 (分钟语义, 粒度换成秒)
        int32_t &si = sec_slot[t];
        if (si < 0) {
          assert(slot.ticks.empty() || t > slot.ticks.back().tick_idx);
          si = static_cast<int32_t>(slot.ticks.size());
          slot.ticks.push_back({});
          slot.ticks.back().tick_idx = t;
        }
        OrderFlow::Depth::Tick &tk = slot.ticks[static_cast<size_t>(si)];
        // 竞价预撮合 (订单簿竞价分支产出; ref > 0 = 竞价且簿交叉, 其余恒 0)
        tk.ref_price = lf.auction_ref_price;
        tk.matched_amount = lf.auction_ref_price * static_cast<float>(lf.auction_matched_qty);
        tk.imbalance_amount = lf.auction_ref_price * static_cast<float>(lf.auction_imbalance);
        for (size_t k = 0; k < N; ++k) {
          const Level *bl = lf.depth_buffer[N + k];     // 买 k+1 价位
          const Level *al = lf.depth_buffer[N - 1 - k]; // 卖 k+1 价位
          const bool br = LOB_Feature::is_real(bl), ar = LOB_Feature::is_real(al);
          tk.bid_price[k] = br ? lf.price_of(bl) : NAN_F;
          tk.bid_volume[k] = br ? static_cast<float>(bl->net_quantity) * VOLUME_TO_LOT : 0.0f; // > 0
          tk.ask_price[k] = ar ? lf.price_of(al) : NAN_F;
          tk.ask_volume[k] = ar ? static_cast<float>(al->net_quantity) * VOLUME_TO_LOT : 0.0f; // < 0
        }
        // 竞价交叉秒: mid = 预撮合参考价 (图1 mid 线在竞价段即预撮合曲线)
        tk.mid_price = tk.ref_price > 0.0f ? tk.ref_price
                                           : (tk.bid_price[0] + tk.ask_price[0]) * 0.5f;
      }
      if (cur_sec != SIZE_MAX)
        commit_second(cur_sec); // 收尾: 最后一秒
    }
    lob.unbind(); // 归还干净簿 (下次 bind 断言)

    for (uint8_t v : sec_data)
      slot.data_valid_count += v;
    slot.has_data = !slot.ticks.empty();

    if (slot.has_data) {
      slot.build_plot();           // 热力图已在重放中增量建好
      slot.build_auto_threshold(); // 依赖 plot 的初始 Y 视野
    }
  }

  // ---- 特征列 (VWAP + overlay, 与盘口独立: 一次读计划, 与图2 同源; 盘口重放走日文件, 不受库轴约束, 上面照常构建) ----
  // 承接日 = 主库日期轴上的前一日 (已落盘才承接): 粗层开盘时的已知值 = 前日末行
  const std::vector<misc::Ymd> &days = req.scope.lib().dates;
  const auto it = std::lower_bound(days.begin(), days.end(), req.date);
  misc::Ymd prev = it != days.begin() ? *(it - 1) : 0;
  if (prev != 0 && !read_.rd.lib().has_date(prev))
    prev = 0;
  {
    TraceN("OF_DepthFeats");
    if (!read_.load(req.plan, req.date, prev))
      return false;
  }
  const RowPlane &p = read_.plane;

  // 分钟 VWAP 阶梯: 逐有效分钟一点, X = 分钟起始秒; 末尾补末分钟末秒收口
  {
    const feature_storage_t *ts = p.series(DP_TS, 0), *vwap = p.series(DP_VWAP, 0);
    auto &px = slot.plot.vwap_x;
    auto &py = slot.plot.vwap_y;
    for (size_t m = 0; m < level_valid_rows(L_min); ++m) {
      if (!has_data(ts[m]))
        continue;
      px.push_back(static_cast<double>(L_min_to_L_sec(m)));
      py.push_back(static_cast<double>(vwap[m]));
    }
    if (!px.empty()) {
      px.push_back(px.back() + 60.0);
      py.push_back(py.back());
    }
  }

  // overlay: X = 行末秒 − 1 (尾部对齐); 开盘承接值落在 X = 0
  slot.n_feat = req.plan.n() - DP_COUNT;
  slot.feat_refs.assign(req.plan.cols.begin() + DP_COUNT, req.plan.cols.end());
  slot.feat_y_min.fill((std::numeric_limits<float>::max)());
  slot.feat_y_max.fill(std::numeric_limits<float>::lowest());
  const auto x_of = [](size_t lvl, size_t r) { return static_cast<double>(level_end_sec(lvl, r) - 1); };
  emit_rows(req.plan, p, DP_COUNT, req.ts_price, 0.0, x_of, slot.feat.data(), slot.feat_y_min.data(), slot.feat_y_max.data());

  // ---- 背槽整体发布 (GUI 帧首 ack 翻面) ----
  pp.commit();
  return true;
}

} // namespace GUI::Features
