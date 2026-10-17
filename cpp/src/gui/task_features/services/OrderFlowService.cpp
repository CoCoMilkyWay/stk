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
#include <limits>
#include <optional>
#include <string>
#include <thread>
#include <tuple>

namespace GUI::Features {

// ============================================================================
// Pick 状态列 (L1, Fund 节点日频广播; 顺序 = pick_cols 的列下标)
// ============================================================================

namespace {
enum PickCol : size_t { PICK_META = 0,
                       PICK_RISK_WARN,
                       PICK_LIST_AGE,
                       PICK_DELIST_AGE,
                       PICK_INDUSTRY };
constexpr size_t PICK_COL_COUNT = 5;

// ============================================================================
// worker 私有常驻资源 (首个请求时在 worker 线程建, 稳态零分配)
// ============================================================================

// 特征库读端: 特征列 = 库轴 (Lob: 池轴; Bar: 全轴); 请求侧资产是全局下标, 经 sub_of 映射
struct LibRead {
  FeatureLib lib;
  const LibAxis &axis = lib.axis;
  FeatureRead reader;
  std::vector<size_t> columns; // 选列缓冲

  explicit LibRead(FeatureLib l) : lib(std::move(l)), reader(lib) {}
};

// 常驻资源按库清单建 (首个请求时, worker 线程); 任一库未建 → false (本请求放弃; 算完库后服务重建再来)
template <class Res, class... Names>
bool ensure_res(std::optional<Res> &res, const Config &cfg, Names... names) {
  if (res)
    return true;
  std::array<FeatureLib, sizeof...(Names)> libs{load_lib(cfg, names)...};
  if (!std::all_of(libs.begin(), libs.end(), [](const FeatureLib &l) { return l.ready(); }))
    return false;
  std::apply([&](auto &...l) { res.emplace(std::move(l)...); }, libs);
  return true;
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
  KlineLane(SharedData &data, const std::string &pool) : data_(data), pool_(pool) { Start(); }
  ~KlineLane() { Stop(); }

private:
  struct Res {
    LibRead lib;
    FeatureRead::DayColumns cols; // OHLC 4 + ts_valid + 特征
    explicit Res(FeatureLib l) : lib(std::move(l)) { cols.preallocate(lib.axis.size(), 1, 5 + OrderFlowConst::MAX_FEATURES); }
  };
  static constexpr Phase kStart = Phase::Running;
  Progress &board() { return data_.orderflow.kline; }
  bool run(KlineReq &req);
  bool step(size_t d); // 装一天并发布; false = 库判废

  SharedData &data_;
  const std::string pool_; // 票池库名 (服务一代一个, 换池重建服务)
  std::optional<Res> res_;
  KlineReq cur_{};
  size_t sub_ = 0;                   // 选中资产的库轴下标 (run 映射一次)
  double y_min_ = 0.0, y_max_ = 0.0; // OHLC 运行范围
  std::vector<float> feat_min_, feat_max_;
};

class OrderFlowService::DepthLane : public Service<DepthLane, DepthReq> {
  friend Service<DepthLane, DepthReq>;

public:
  static constexpr const char *kName = "OF_DepthLane";
  DepthLane(SharedData &data, const std::string &pool) : data_(data), pool_(pool) { Start(); }
  ~DepthLane() { Stop(); }

private:
  struct Res {
    LibRead lib;
    L2::FrameDecoder decoder;                 // 对仗 Lob 管线 Runner 的 decoder (日文件 reader 按请求现开)
    LimitOrderBook lob;                       // 奢侈容量, 常驻复用 (对仗 Lob 管线 Runner 的工作区)
    TickData tick_data;                       // 无特征侧绑定的盘口写出口
    FeatureRead::DayColumns l1_cols, l0_cols; // ts_valid + VWAP / 特征
    OrderFlow::Depth::HeatmapScratch scratch;
    std::vector<int32_t> sec_slot; // 秒 -> ticks 下标 (重放临时, -1 = 未见)
    std::vector<uint8_t> sec_data; // 秒 -> 有逐笔
    explicit Res(FeatureLib l) : lib(std::move(l)), lob(L2::LOB_ORDER_CAPACITY) {
      l1_cols.preallocate(lib.axis.size(), 1, 1 + OrderFlowConst::MAX_FEATURES);
      l0_cols.preallocate(lib.axis.size(), 0, 1 + OrderFlowConst::MAX_FEATURES);
    }
  };
  static constexpr Phase kStart = Phase::Running;
  Progress &board() { return data_.orderflow.depth; }
  bool run(DepthReq &req); // 重放 + 构建 + 背槽发布

  SharedData &data_;
  const std::string pool_;
  std::optional<Res> res_;
};

class OrderFlowService::PickLane : public Service<PickLane, PickReq> {
  friend Service<PickLane, PickReq>;

public:
  static constexpr const char *kName = "OF_PickLane";
  PickLane(SharedData &data, const std::string &pool) : data_(data), pool_(pool) { Start(); }
  ~PickLane() { Stop(); }

private:
  struct Res {
    LibRead lib;                  // 只用其池轴 (候选范围)
    LibRead bar;                  // 状态列在 Bar 档 (Fund 只在 Bar 库落盘)
    FeatureRead::DayColumns cols; // 资产筛选状态列 (全轴)
    Res(FeatureLib l, FeatureLib b) : lib(std::move(l)), bar(std::move(b)) {
      cols.preallocate(bar.axis.size(), 1, PICK_COL_COUNT);
    }
  };
  static constexpr Phase kStart = Phase::Running;
  Progress &board() { return data_.orderflow.pick.slots; }
  bool run(PickReq &req); // 逐日 filter 列 → 全资产状态 + 背槽发布

  SharedData &data_;
  const std::string pool_;
  std::optional<Res> res_;
};

// ============================================================================
// Lifecycle
// ============================================================================

OrderFlowService::OrderFlowService(std::string pool) : pool_(std::move(pool)) {}

OrderFlowService::~OrderFlowService() {
  Stop();
}

void OrderFlowService::Start(SharedData &data) {
  if (kline_)
    return;
  kline_ = std::make_unique<KlineLane>(data, pool_);
  depth_ = std::make_unique<DepthLane>(data, pool_);
  pick_ = std::make_unique<PickLane>(data, pool_);
}

void OrderFlowService::Stop() { // worker 析构即 Stop (join)
  kline_.reset();
  depth_.reset();
  pick_.reset();
}

// ============================================================================
// Requests (GUI 线程; 最新覆盖旧的)
// ============================================================================

void OrderFlowService::RequestKline(uint32_t gen, size_t asset_idx, std::vector<int> feats, bool rescan_dates) {
  assert(kline_ && "Request 先于 Start");
  assert(feats.size() <= OrderFlowConst::MAX_FEATURES);
  kline_->Request(KlineReq{gen, asset_idx, std::move(feats), rescan_dates});
}

void OrderFlowService::RequestDepth(uint32_t gen, misc::Ymd date, size_t asset_idx, int feat_level, std::vector<int> feats) {
  assert(depth_ && "Request 先于 Start");
  assert(feats.size() <= OrderFlowConst::MAX_FEATURES);
  assert(feat_level == 0 || feat_level == 1);
  depth_->Request(DepthReq{gen, date, asset_idx, feat_level, std::move(feats)});
}

void OrderFlowService::RequestPick(uint32_t gen, misc::Ymd date) {
  assert(pick_ && "Request 先于 Start");
  pick_->Request(PickReq{gen, date});
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
  if (!ensure_res(res_, data_.config, pool_))
    return false;
  OrderFlow::Kline &k = data_.orderflow.kline;

  // 此刻 GUI 请求代 == req.gen, 发布代 == 旧代 → GUI 不读 kline, dates/数组可安全重建
  if (req.rescan || k.dates.empty()) {
    k.begin(Phase::Scanning);
    res_->lib.reader.revive(); // 重扫 = 库换代 (重算清库/判废后新库落地), 判废解除
    k.dates.clear();
    scan_dates(res_->lib.lib, k.dates);
  }

  cur_ = std::move(req);
  y_min_ = (std::numeric_limits<double>::max)();
  y_max_ = std::numeric_limits<double>::lowest();
  feat_min_.assign(cur_.feats.size(), (std::numeric_limits<float>::max)());
  feat_max_.assign(cur_.feats.size(), std::numeric_limits<float>::lowest());

  k.begin_generation(cur_.gen, cur_.asset, cur_.feats.size());
  k.reserve_capacity(); // 发布前完成: 之后 data() 恒稳定

  // 请求侧 asset 是全局轴下标, 特征列按票池库轴索引. 不在库轴内 (初始
  // 默认选中 / 换票池后的残留选中, 候选就绪后 GUI 会自动换代) → 无特征
  // 列可读, 直接发布"完整但空"的一代 —— 与全轴时代读到恒零列的语义一致.
  sub_ = res_->lib.axis.sub_of(static_cast<uint32_t>(cur_.asset));
  if (sub_ >= res_->lib.axis.size()) {
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
  LibRead &lib = res_->lib;
  const FeatureRead::DayColumns &dc = res_->cols;

  // 选列: [lob_open, lob_high, lob_low, lob_close, ts_valid, 特征...] — L1 日文件, 每日只读 n 列的载荷
  auto &cols = lib.columns;
  cols.assign({lob::L1_Field::lob_open, lob::L1_Field::lob_high, lob::L1_Field::lob_low,
               lob::L1_Field::lob_close, lob::L1_Field::ts_valid});
  for (int f : cur_.feats)
    cols.push_back(static_cast<size_t>(f));
  lib.reader.load_day_columns(k.dates[d], cols, res_->cols);
  if (lib.reader.stale())
    return false;

  // 选中资产的库轴下标 (run 映射并保证在轴内, 否则不会流式到这)
  const size_t a = sub_;
  assert(a < lib.axis.size());
  const double x0 = static_cast<double>(d * OrderFlowConst::L1_CAPACITY);
  const size_t nf = cur_.feats.size();

  for (size_t m = 0; m < level_valid_rows(1); ++m) {
    if (!fmeta::data_valid(static_cast<float>(dc.get(m, 4, a))))
      continue;

    const double o = static_cast<double>(dc.get(m, 0, a));
    const double h = static_cast<double>(dc.get(m, 1, a));
    const double l = static_cast<double>(dc.get(m, 2, a));
    const double c = static_cast<double>(dc.get(m, 3, a));

    k.x.push_back(x0 + static_cast<double>(m));
    k.open.push_back(o);
    k.high.push_back(h);
    k.low.push_back(l);
    k.close.push_back(c);
    y_min_ = std::min(y_min_, l);
    y_max_ = std::max(y_max_, h);

    for (size_t i = 0; i < nf; ++i) {
      const float v = static_cast<float>(dc.get(m, 5 + i, a));
      if (v != v) // NaN
        continue;
      k.feat[i].x.push_back(x0 + static_cast<double>(m));
      k.feat[i].y.push_back(static_cast<double>(v));
      feat_min_[i] = std::min(feat_min_[i], v);
      feat_max_[i] = std::max(feat_max_[i], v);
    }
  }

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
// Pick: 锚点日的 L1 filter 列 → 全资产 PIT 状态 → 背槽发布
//   Fund 是 onDay 算一次 / onMinute 原样广播, 故当日任一有效分钟的值即当日状态;
//   取首个 ts_valid 有效的分钟 (无有效分钟 = 落盘缓冲清零, 状态不可判读 → has_data=false)
// ============================================================================

bool OrderFlowService::PickLane::run(PickReq &req) {
  if (!ensure_res(res_, data_.config, pool_, Config::kAllTierDir))
    return false;
  PingPong<OrderFlow::Pick::Slot> &pp = data_.orderflow.pick.slots;
  if (!await_back(pp, cancel_))
    return false;
  TraceN("OF_PickBuild");
  LibRead &lib = res_->lib;
  FeatureRead &reader = res_->bar.reader;
  reader.revive(); // 新请求 = 重读库 (重算后新库已落地), 判废解除

  OrderFlow::Pick::Slot &slot = pp.back();
  slot.clear();
  slot.gen = req.gen;
  slot.date = req.date;

  // meta 槽位 = 全局轴下标 (TabOrderFlow 按 items 下标消费); 状态列 = Bar 档全轴 (下标即全局),
  // 只填库轴内的资产, 库轴外恒 has_data = false (自然被候选过滤掉)
  slot.meta.assign(data_.asset.items.size(), OrderFlow::Pick::Meta{});

  auto &cols = lib.columns;
  cols.assign({static_cast<size_t>(bar::L1_Field::ts_valid), static_cast<size_t>(bar::L1_Field::st_level),
               static_cast<size_t>(bar::L1_Field::list_age), static_cast<size_t>(bar::L1_Field::delist_age),
               static_cast<size_t>(bar::L1_Field::ind_l1)});
  assert(cols.size() == PICK_COL_COUNT);
  reader.load_day_columns(req.date, cols, res_->cols);

  // 时间外层 / 资产内层 (布局 [n][T][A]: 内层连续); 每资产只取首个有效分钟, 全部填完即止
  const FeatureRead::DayColumns &c = res_->cols;
  const size_t A = lib.axis.size();
  assert(res_->bar.axis.size() == c.A);
  size_t remaining = A;
  for (size_t m = 0; m < level_valid_rows(1) && remaining > 0; ++m) {
    for (size_t a = 0; a < A; ++a) {
      const size_t g = lib.axis.global(a);
      OrderFlow::Pick::Meta &meta = slot.meta[g];
      if (meta.has_data) // 已取到当日状态 (日频广播, 后续分钟同值)
        continue;
      if (!fmeta::data_valid(static_cast<float>(c.get(m, PICK_META, g))))
        continue;

      meta.has_data = true;
      --remaining;

      const float rw = static_cast<float>(c.get(m, PICK_RISK_WARN, g));
      meta.risk_warn = (rw >= 0.0f && rw <= 3.0f) ? static_cast<uint8_t>(rw + 0.5f) : 0;

      const float ind = static_cast<float>(c.get(m, PICK_INDUSTRY, g));
      meta.industry_l1 = (ind >= 0.0f && ind < static_cast<float>(fund::SW2021_L1_COUNT))
                             ? static_cast<uint8_t>(ind + 0.5f)
                             : 0;

      // list_age/delist_age: 非 NaN = 已上市/已退市 (>= 0 日历日), NaN = 未发生
      const float lage = static_cast<float>(c.get(m, PICK_LIST_AGE, g));
      const float dage = static_cast<float>(c.get(m, PICK_DELIST_AGE, g));
      meta.delisted = !std::isnan(dage);
      meta.listed = !std::isnan(lage) && !meta.delisted;
    }
  }

  pp.commit();
  return true;
}

// ============================================================================
// Depth: 日文件逐笔重放 → 秒级快照 → plot/heatmap → 背槽发布
// ============================================================================

bool OrderFlowService::DepthLane::run(DepthReq &req) {
  if (!ensure_res(res_, data_.config, pool_))
    return false;
  PingPong<OrderFlow::Depth> &pp = data_.orderflow.depth;
  if (!await_back(pp, cancel_))
    return false;
  TraceN("OF_DepthBuild");
  Res &res = *res_;
  res.lib.reader.revive(); // 新请求 = 重读库 (重算后新库已落地), 判废解除

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
    sec_slot.assign(level_valid_rows(0), -1);
    sec_data.assign(level_valid_rows(0), 0);
    slot.ticks.reserve(level_valid_rows(0));
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
        const size_t t = Clock_to_L0(L2::tick_hour(order.tick), L2::tick_minute(order.tick), L2::tick_second(order.tick)); // 钳在 [0, 15300)
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

  // 请求侧 asset 是全局轴下标, 特征列按库轴索引; 不在库轴内 → 无特征列
  // 可读, VWAP / overlay 留空 (盘口重放走日文件, 不受库轴约束, 上面照常构建)
  const size_t depth_sub = res.lib.axis.sub_of(static_cast<uint32_t>(req.asset));

  // ---- 分钟 VWAP 阶梯: L1 特征列 vwap, 逐有效分钟一点, X = 分钟起始秒; 末尾补末分钟末秒收口 ----
  if (depth_sub < res.lib.axis.size()) {
    TraceN("OF_DepthVwap");
    auto &cols = res.lib.columns;
    cols.assign({static_cast<size_t>(lob::L1_Field::ts_valid), static_cast<size_t>(lob::L1_Field::vwap)});
    res.lib.reader.load_day_columns(req.date, cols, res.l1_cols);
    auto &px = slot.plot.vwap_x;
    auto &py = slot.plot.vwap_y;
    for (size_t m = 0; m < level_valid_rows(1); ++m) {
      if (!fmeta::data_valid(static_cast<float>(res.l1_cols.get(m, 0, depth_sub))))
        continue;
      px.push_back(static_cast<double>(L1_to_L0(m)));
      py.push_back(static_cast<double>(res.l1_cols.get(m, 1, depth_sub)));
    }
    if (!px.empty()) {
      px.push_back(px.back() + 60.0);
      py.push_back(py.back());
    }
  }

  // ---- 特征 overlay (与盘口独立: 当前选中层特征列 + ts_valid 选列读, 与图2 同源指标;
  //      L0 = data_valid 秒; L1 = 有效分钟, X 映射分钟起始秒 → 只取当日日内段) ----
  if (!req.feats.empty() && depth_sub < res.lib.axis.size()) {
    TraceN("OF_DepthFeats");
    assert(req.feat_level == 0 || req.feat_level == 1);
    const size_t meta_col = req.feat_level == 0 ? static_cast<size_t>(lob::L0_Field::ts_valid)
                                                : static_cast<size_t>(lob::L1_Field::ts_valid);
    auto &cols = res.lib.columns;
    cols.assign({meta_col});
    for (int f : req.feats)
      cols.push_back(static_cast<size_t>(f));
    FeatureRead::DayColumns &day_cols = req.feat_level == 0 ? res.l0_cols : res.l1_cols;
    res.lib.reader.load_day_columns(req.date, cols, day_cols);

    const size_t a = depth_sub;
    slot.n_feat = req.feats.size();
    slot.feat_level = req.feat_level;
    slot.feat_y_min.fill((std::numeric_limits<float>::max)());
    slot.feat_y_max.fill(std::numeric_limits<float>::lowest());

    for (size_t r = 0; r < level_valid_rows(static_cast<size_t>(req.feat_level)); ++r) {
      const float meta = static_cast<float>(day_cols.get(r, 0, a));
      if (!fmeta::data_valid(meta))
        continue;
      const double x = req.feat_level == 0 ? static_cast<double>(r) : static_cast<double>(L1_to_L0(r));
      for (size_t i = 0; i < req.feats.size(); ++i) {
        float v = static_cast<float>(day_cols.get(r, 1 + i, a));
        if (static_cast<size_t>(req.feats[i]) == meta_col)
          v = fmeta::price(v); // ts_valid 被选中时展示幅值 = micro price
        if (v != v)            // NaN
          continue;
        slot.feat[i].x.push_back(x);
        slot.feat[i].y.push_back(static_cast<double>(v));
        slot.feat_y_min[i] = std::min(slot.feat_y_min[i], v);
        slot.feat_y_max[i] = std::max(slot.feat_y_max[i], v);
      }
    }
  }

  // ---- 背槽整体发布 (GUI 帧首 ack 翻面) ----
  pp.commit();
  return true;
}

} // namespace GUI::Features
