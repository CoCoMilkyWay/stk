// OrderFlowService Implementation — 线程模型与发布协议见头文件 / shared/OrderFlow.hpp
#include "gui/task_features/services/OrderFlowService.hpp"

#include "codec/binary_decoder_L2.hpp"
#include "features/Backend/FeatureRead.hpp"
#include "lob/LimitOrderBook.hpp"
#include "misc/profiler.hpp"
#include "shared/SharedData.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <limits>

namespace GUI::Features {

// ============================================================================
// Universe 状态列 (L1, Fund 算子日频广播; 顺序 = uni_cols 的列下标)
// ============================================================================

namespace {
enum UniCol : size_t { UNI_META = 0,
                       UNI_RISK_WARN,
                       UNI_LIST_AGE,
                       UNI_DELIST_AGE,
                       UNI_INDUSTRY };
constexpr size_t UNIVERSE_COL_COUNT = 5;
} // namespace

// ============================================================================
// Impl — worker 线程私有常驻资源 (稳态零分配)
// ============================================================================

struct OrderFlowService::Impl {
  FeatureRead reader;
  L2::BinaryDecoder_L2 decoder;
  LimitOrderBook lob; // 奢侈容量, 常驻复用 (对仗 sequential_worker 的工作区)
  TickData tick_data; // 无特征侧绑定的盘口写出口
  FeatureRead::DayColumns l1_cols, l0_cols, uni_cols;
  OrderFlow::Depth::HeatmapScratch scratch;
  std::vector<int32_t> sec_slot; // 秒 -> ticks 下标 (重放临时, -1 = 未见)
  std::vector<uint8_t> sec_data; // 秒 -> 有逐笔
  std::vector<size_t> columns;   // 选列缓冲

  Impl(const std::string &features_dir, size_t A)
      : reader(features_dir),
        decoder(L2::DEFAULT_ENCODER_ORDER_SIZE),
        lob(L2::LOB_ORDER_CAPACITY) {
    l1_cols.preallocate(A, 1, 5 + OrderFlowConst::MAX_FEATURES); // OHLC 4 + _meta + 特征
    l0_cols.preallocate(A, 0, 1 + OrderFlowConst::MAX_FEATURES); // _meta + 特征
    uni_cols.preallocate(A, 1, UNIVERSE_COL_COUNT);              // 资产筛选状态列
  }
};

// ============================================================================
// Lifecycle
// ============================================================================

OrderFlowService::OrderFlowService() = default;

OrderFlowService::~OrderFlowService() {
  Stop();
}

void OrderFlowService::Start(SharedData &data) {
  if (thread_.joinable())
    return;
  data_ = &data;
  stop_.store(false, std::memory_order_relaxed);
  thread_ = std::thread(&OrderFlowService::worker_loop, this);
}

void OrderFlowService::Stop() {
  if (!thread_.joinable())
    return;
  {
    std::lock_guard<std::mutex> lock(req_mutex_);
    stop_.store(true, std::memory_order_relaxed);
  }
  req_cv_.notify_all();
  thread_.join();
}

// ============================================================================
// Requests (GUI 线程; 最新覆盖旧的)
// ============================================================================

void OrderFlowService::RequestKline(uint32_t gen, size_t asset_idx, std::vector<int> feats, bool rescan_dates) {
  assert(feats.size() <= OrderFlowConst::MAX_FEATURES);
  {
    std::lock_guard<std::mutex> lock(req_mutex_);
    pending_kline_ = KlineReq{gen, asset_idx, std::move(feats), rescan_dates};
  }
  req_cv_.notify_all();
}

void OrderFlowService::RequestDepth(uint32_t gen, std::string date, size_t asset_idx, std::vector<int> feats) {
  assert(feats.size() <= OrderFlowConst::MAX_FEATURES);
  {
    std::lock_guard<std::mutex> lock(req_mutex_);
    pending_depth_ = DepthReq{gen, std::move(date), asset_idx, std::move(feats)};
  }
  req_cv_.notify_all();
}

void OrderFlowService::RequestUniverse(uint32_t gen, std::string date) {
  {
    std::lock_guard<std::mutex> lock(req_mutex_);
    pending_universe_ = UniverseReq{gen, std::move(date)};
  }
  req_cv_.notify_all();
}

// ============================================================================
// Worker loop — Depth 重放优先, Kline 逐日流式垫底, 每步之间轮询新请求
// ============================================================================

void OrderFlowService::worker_loop() {
  OrderFlow &of = data_->orderflow;
  impl_ = std::make_unique<Impl>(data_->config.feature_dir, data_->asset.items.size());

  while (true) {
    std::optional<KlineReq> kreq;
    std::optional<DepthReq> dreq;
    std::optional<UniverseReq> ureq;
    {
      std::unique_lock<std::mutex> lock(req_mutex_);
      auto has_work = [&] {
        return stop_.load(std::memory_order_relaxed) || pending_kline_.has_value() ||
               (pending_depth_.has_value() && !of.depth_pending.load(std::memory_order_acquire)) ||
               (pending_universe_.has_value() && !of.universe.pending.load(std::memory_order_acquire)) ||
               (kline_active_ && kline_next_day_ < of.kline.dates.size());
      };
      if (!has_work()) {
        if (pending_depth_.has_value() || pending_universe_.has_value()) // 背槽待 GUI ack: 无 cv 信号, 限时睡
          req_cv_.wait_for(lock, std::chrono::milliseconds(2), has_work);
        else
          req_cv_.wait(lock, has_work);
      }
      if (stop_.load(std::memory_order_relaxed))
        return;
      if (pending_kline_) {
        kreq = std::move(pending_kline_);
        pending_kline_.reset();
      }
      if (pending_depth_ && !of.depth_pending.load(std::memory_order_acquire)) {
        dreq = std::move(pending_depth_);
        pending_depth_.reset();
      }
      if (pending_universe_ && !of.universe.pending.load(std::memory_order_acquire)) {
        ureq = std::move(pending_universe_);
        pending_universe_.reset();
      }
    }

    // 取出的请求必须全部执行 —— 已从 pending_ 摘走, 丢掉就永久丢失
    // (GUI 侧期望态已更新, 不会重发 → 发布代永远追不上请求代)
    if (kreq)
      kline_begin(*kreq);
    if (ureq)
      universe_build(*ureq); // 资产选择依赖它, 先于 depth
    if (dreq)
      depth_build(*dreq);

    // 流式垫底: 仅在无按需请求时推进 (下一轮轮询即可被新请求打断)
    if (!ureq && !dreq && kline_active_)
      kline_active_ = kline_step();
  }
}

// ============================================================================
// 日期扫描: features/YYYY/MM/DD 有整层文件的日, 升序
// ============================================================================

void OrderFlowService::scan_dates(std::vector<std::string> &out) const {
  TraceN("OF_ScanDates");
  const std::string &dir = data_->config.feature_dir;
  if (!std::filesystem::exists(dir))
    return;

  for (const auto &year_entry : std::filesystem::directory_iterator(dir)) {
    if (!year_entry.is_directory() || year_entry.path().filename().string().size() != 4)
      continue;
    const std::string year = year_entry.path().filename().string();

    for (const auto &month_entry : std::filesystem::directory_iterator(year_entry.path())) {
      if (!month_entry.is_directory() || month_entry.path().filename().string().size() != 2)
        continue;
      const std::string month = month_entry.path().filename().string();

      for (const auto &day_entry : std::filesystem::directory_iterator(month_entry.path())) {
        if (!day_entry.is_directory() || day_entry.path().filename().string().size() != 2)
          continue;
        const std::string date = year + month + day_entry.path().filename().string();
        if (FeatureRead::has_date(data_->config.feature_dir, date))
          out.push_back(date);
      }
    }
  }
  std::sort(out.begin(), out.end());
}

// ============================================================================
// Kline: 换代 + 逐日流式
// ============================================================================

void OrderFlowService::kline_begin(const KlineReq &req) {
  TraceN("OF_KlineBegin");
  OrderFlow::Kline &k = data_->orderflow.kline;

  // 此刻 GUI 请求代 == req.gen, 发布代 == 旧代 → GUI 不读 kline, dates/数组可安全重建
  if (req.rescan || k.dates.empty()) {
    k.dates.clear();
    scan_dates(k.dates);
  }

  kline_cur_ = req;
  kline_next_day_ = 0;
  kline_active_ = !k.dates.empty();
  kline_y_min_ = (std::numeric_limits<double>::max)();
  kline_y_max_ = std::numeric_limits<double>::lowest();
  kline_feat_min_.assign(req.feats.size(), (std::numeric_limits<float>::max)());
  kline_feat_max_.assign(req.feats.size(), std::numeric_limits<float>::lowest());

  k.begin_generation(req.gen, req.asset, req.feats.size());
  k.reserve_capacity(); // 发布前完成: 之后 data() 恒稳定
}

bool OrderFlowService::kline_step() {
  TraceN("OF_KlineDay");
  OrderFlow::Kline &k = data_->orderflow.kline;
  const size_t d = kline_next_day_;
  assert(d < k.dates.size());

  // 选列: [open, high, low, close, _meta, 特征...] — L1 逐列文件, 每日只碰 n 个小文件
  auto &cols = impl_->columns;
  cols.assign({L1_Field::_ohlc_open, L1_Field::_ohlc_high, L1_Field::_ohlc_low,
               L1_Field::_ohlc_close, L1_Field::_meta});
  for (int f : kline_cur_.feats)
    cols.push_back(static_cast<size_t>(f));
  impl_->reader.load_day_columns(k.dates[d], cols, impl_->l1_cols);

  const size_t a = kline_cur_.asset;
  assert(a < impl_->l1_cols.A);
  const double x0 = static_cast<double>(d * OrderFlowConst::L1_CAPACITY);
  const size_t nf = kline_cur_.feats.size();

  for (size_t m = 0; m < level_valid_rows(1); ++m) {
    if (!fmeta::data_valid(static_cast<float>(impl_->l1_cols.get(m, 4, a))))
      continue;

    const double o = static_cast<double>(impl_->l1_cols.get(m, 0, a));
    const double h = static_cast<double>(impl_->l1_cols.get(m, 1, a));
    const double l = static_cast<double>(impl_->l1_cols.get(m, 2, a));
    const double c = static_cast<double>(impl_->l1_cols.get(m, 3, a));

    k.x.push_back(x0 + static_cast<double>(m));
    k.open.push_back(o);
    k.high.push_back(h);
    k.low.push_back(l);
    k.close.push_back(c);
    kline_y_min_ = std::min(kline_y_min_, l);
    kline_y_max_ = std::max(kline_y_max_, h);

    for (size_t i = 0; i < nf; ++i) {
      const float v = static_cast<float>(impl_->l1_cols.get(m, 5 + i, a));
      if (v != v) // NaN
        continue;
      k.feat[i].x.push_back(x0 + static_cast<double>(m));
      k.feat[i].y.push_back(static_cast<double>(v));
      kline_feat_min_[i] = std::min(kline_feat_min_[i], v);
      kline_feat_max_[i] = std::max(kline_feat_max_[i], v);
    }
  }

  // 发布: 先各数组计数/范围 (release), 后 pub word (release) — GUI acquire 读 pub 即见全量
  if (kline_y_min_ <= kline_y_max_) {
    k.y_min.store(kline_y_min_, std::memory_order_relaxed);
    k.y_max.store(kline_y_max_, std::memory_order_relaxed);
  }
  for (size_t i = 0; i < nf; ++i) {
    k.feat_y_min[i].store(kline_feat_min_[i], std::memory_order_relaxed);
    k.feat_y_max[i].store(kline_feat_max_[i], std::memory_order_relaxed);
    k.feat_n[i].store(k.feat[i].x.size(), std::memory_order_release);
  }
  k.pub.store(OrderFlow::Kline::pack(kline_cur_.gen, d + 1, k.x.size()), std::memory_order_release);

  ++kline_next_day_;
  return kline_next_day_ < k.dates.size();
}

// ============================================================================
// Universe: 锚点日的 L1 filter 列 → 全资产 PIT 状态 → 背槽发布
//   Fund 是 onDay 算一次 / onMinute 原样广播, 故当日任一有效分钟的值即当日状态;
//   取首个 _meta 有效的分钟 (无有效分钟 = 落盘缓冲清零, 状态不可判读 → has_data=false)
// ============================================================================

void OrderFlowService::universe_build(const UniverseReq &req) {
  TraceN("OF_UniverseBuild");
  OrderFlow &of = data_->orderflow;
  auto &uni = of.universe;
  assert(!uni.pending.load(std::memory_order_acquire) && "universe_build: 背槽未 ack");

  OrderFlow::Universe::Slot &slot = uni.slot[1 - uni.front.load(std::memory_order_acquire)];
  slot.clear();
  slot.gen = req.gen;
  slot.date = req.date;

  const size_t A = data_->asset.items.size();
  slot.meta.assign(A, OrderFlow::Universe::Meta{});

  auto &cols = impl_->columns;
  cols.assign({static_cast<size_t>(L1_Field::_meta), static_cast<size_t>(L1_Field::risk_warn),
               static_cast<size_t>(L1_Field::list_age), static_cast<size_t>(L1_Field::delist_age),
               static_cast<size_t>(L1_Field::industry_l1)});
  assert(cols.size() == UNIVERSE_COL_COUNT);
  impl_->reader.load_day_columns(req.date, cols, impl_->uni_cols);

  // 时间外层 / 资产内层 (布局 [T][n][A]: 内层连续); 每资产只取首个有效分钟, 全部填完即止
  const FeatureRead::DayColumns &c = impl_->uni_cols;
  assert(A <= c.A);
  size_t remaining = A;
  for (size_t m = 0; m < level_valid_rows(1) && remaining > 0; ++m) {
    for (size_t a = 0; a < A; ++a) {
      OrderFlow::Universe::Meta &meta = slot.meta[a];
      if (meta.has_data) // 已取到当日状态 (日频广播, 后续分钟同值)
        continue;
      if (!fmeta::data_valid(static_cast<float>(c.get(m, UNI_META, a))))
        continue;

      meta.has_data = true;
      --remaining;

      const float rw = static_cast<float>(c.get(m, UNI_RISK_WARN, a));
      meta.risk_warn = (rw >= 0.0f && rw <= 3.0f) ? static_cast<uint8_t>(rw + 0.5f) : 0;

      const float ind = static_cast<float>(c.get(m, UNI_INDUSTRY, a));
      meta.industry_l1 = (ind >= 0.0f && ind < static_cast<float>(fund::SW2021_L1_COUNT))
                             ? static_cast<uint8_t>(ind + 0.5f)
                             : 0;

      // list_age/delist_age: 非 NaN = 已上市/已退市 (>= 0 日历日), NaN = 未发生
      const float lage = static_cast<float>(c.get(m, UNI_LIST_AGE, a));
      const float dage = static_cast<float>(c.get(m, UNI_DELIST_AGE, a));
      meta.delisted = !std::isnan(dage);
      meta.listed = !std::isnan(lage) && !meta.delisted;
    }
  }

  uni.pending.store(true, std::memory_order_release);
}

// ============================================================================
// Depth: orders/*.bin 逐笔重放 → 秒级快照 → plot/heatmap → 背槽发布
// ============================================================================

void OrderFlowService::depth_build(const DepthReq &req) {
  TraceN("OF_DepthBuild");
  OrderFlow &of = data_->orderflow;
  assert(!of.depth_pending.load(std::memory_order_acquire) && "depth_build: 背槽未 ack");

  OrderFlow::Depth &slot = of.depth[1 - of.depth_front.load(std::memory_order_acquire)];
  slot.clear();
  slot.gen = req.gen;
  slot.date = req.date;
  slot.asset_idx = req.asset;

  // ---- 重放 .bin (单资产单日一文件; 缺文件 = 停牌/未编码, 发布空槽) ----
  const AssetItem &asset = data_->asset.items[req.asset];
  const std::string orders_file = Utils::generate_orders_path(
      data_->config.orders_dir, req.date, asset.asset_code, asset.exchange, config::BINARY_EXTENSION);

  size_t order_num = 0;
  const L2::Order *orders = nullptr;
  {
    TraceN("OF_Decode");
    orders = impl_->decoder.decode_orders_stream(orders_file, order_num);
  }

  if (orders != nullptr) {
    slot.order_count = order_num;

    LimitOrderBook &lob = impl_->lob;
    lob.bind(&impl_->tick_data, req.asset, asset.exchange_type); // 无特征侧绑定
    lob.set_price_base(impl_->decoder.last_price_base());

    auto &sec_slot = impl_->sec_slot;
    auto &sec_data = impl_->sec_data;
    auto &scratch = impl_->scratch;
    sec_slot.assign(level_valid_rows(0), -1);
    sec_data.assign(level_valid_rows(0), 0);
    slot.ticks.reserve(level_valid_rows(0));
    slot.heatmap_begin(scratch);

    constexpr size_t N = OrderFlowConst::LOB_DEPTH;
    constexpr float NAN_F = std::numeric_limits<float>::quiet_NaN();
    constexpr float VOLUME_TO_LOT = 0.01f; // 股 → 手 (对仗 CoreSequential 的 DEPTH 快照)
    const LOB_Feature &lf = impl_->tick_data.lob;
    const float base = static_cast<float>(impl_->decoder.last_price_base());

    // 档位下标空间的哨兵判据: 低端 [1, N] 与高端 [RANGE-1-N, RANGE-1] 是构造期哨兵档
    // + 编码期停靠档 (离盘口无穷远), 见 init_sentinel_levels / BinaryEncoder_L2::park_price
    auto is_sentinel = [](uint32_t idx) {
      return idx <= OrderFlowConst::LOB_DEPTH || idx >= PRICE_RANGE_SIZE - 1 - OrderFlowConst::LOB_DEPTH;
    };

    // 秒末全簿快照 → 热力图 (只提交出过有效盘口快照的秒; 全档, 不限近端 30 档)
    auto commit_second = [&](size_t t) {
      if (sec_slot[t] < 0)
        return;
      scratch.current_tick.clear();
      lob.for_each_visible_level([&](uint32_t idx, int32_t net_qty) {
        const float price = (base + static_cast<float>(idx)) * 0.01f;                        // 元
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
        const size_t t = Clock_to_L0(orders[i].hour, orders[i].minute, orders[i].second); // 钳在 [0, 15300)
        if (t != cur_sec) {
          if (cur_sec != SIZE_MAX)
            commit_second(cur_sec); // 上一秒结束: 秒末终值全簿入热力图
          cur_sec = t;
        }
        lob.process(orders[i]);
        sec_data[t] = 1;
        if (!lf.depth_updated)
          continue;

        // 盘口顶无真实档 (纯哨兵): 本秒不出快照
        const Level *bid1 = lf.depth_buffer[N];
        const Level *ask1 = lf.depth_buffer[N - 1];
        if (is_sentinel(bid1->price) || is_sentinel(ask1->price))
          continue;

        // 秒内覆盖写: 终值 = 秒末盘口 (与 DEPTH 落盘的分钟语义对仗, 粒度换成秒)
        int32_t &si = sec_slot[t];
        if (si < 0) {
          assert(slot.ticks.empty() || t > slot.ticks.back().tick_idx);
          si = static_cast<int32_t>(slot.ticks.size());
          slot.ticks.push_back({});
          slot.ticks.back().tick_idx = t;
        }
        OrderFlow::Depth::Tick &tk = slot.ticks[static_cast<size_t>(si)];
        for (size_t k = 0; k < N; ++k) {
          const Level *bl = lf.depth_buffer[N + k];     // 买 k+1 档
          const Level *al = lf.depth_buffer[N - 1 - k]; // 卖 k+1 档
          const bool bs = is_sentinel(bl->price), as = is_sentinel(al->price);
          tk.bid_price[k] = bs ? NAN_F : (base + static_cast<float>(bl->price)) * 0.01f;
          tk.bid_volume[k] = bs ? 0.0f : static_cast<float>(bl->net_quantity) * VOLUME_TO_LOT; // > 0
          tk.ask_price[k] = as ? NAN_F : (base + static_cast<float>(al->price)) * 0.01f;
          tk.ask_volume[k] = as ? 0.0f : static_cast<float>(al->net_quantity) * VOLUME_TO_LOT; // < 0
        }
        tk.mid_price = (tk.bid_price[0] + tk.ask_price[0]) * 0.5f;
      }
      if (cur_sec != SIZE_MAX)
        commit_second(cur_sec); // 收尾: 最后一秒
    }
    lob.clear(); // 归还干净簿 (下次 bind 断言)

    for (uint8_t v : sec_data)
      slot.data_valid_count += v;
    slot.has_data = !slot.ticks.empty();

    if (slot.has_data)
      slot.build_plot(); // 热力图已在重放中增量建好
  }

  // ---- L0 特征 overlay (与盘口独立: 特征列 + _meta 选列读, data_valid 秒) ----
  if (!req.feats.empty()) {
    TraceN("OF_DepthFeats");
    auto &cols = impl_->columns;
    cols.assign({static_cast<size_t>(L0_Field::_meta)});
    for (int f : req.feats)
      cols.push_back(static_cast<size_t>(f));
    impl_->reader.load_day_columns(req.date, cols, impl_->l0_cols);

    const size_t a = req.asset;
    assert(a < impl_->l0_cols.A);
    slot.n_feat = req.feats.size();
    slot.feat_y_min.fill((std::numeric_limits<float>::max)());
    slot.feat_y_max.fill(std::numeric_limits<float>::lowest());

    for (size_t t = 0; t < level_valid_rows(0); ++t) {
      const float meta = static_cast<float>(impl_->l0_cols.get(t, 0, a));
      if (!fmeta::data_valid(meta))
        continue;
      for (size_t i = 0; i < req.feats.size(); ++i) {
        float v = static_cast<float>(impl_->l0_cols.get(t, 1 + i, a));
        if (req.feats[i] == static_cast<int>(L0_Field::_meta))
          v = fmeta::price(v); // _meta 被选中时展示幅值 = micro price
        if (v != v)            // NaN
          continue;
        slot.feat[i].x.push_back(static_cast<double>(t));
        slot.feat[i].y.push_back(static_cast<double>(v));
        slot.feat_y_min[i] = std::min(slot.feat_y_min[i], v);
        slot.feat_y_max[i] = std::max(slot.feat_y_max[i], v);
      }
    }
  }

  // ---- 背槽整体发布 (GUI 帧首 ack 翻面) ----
  of.depth_pending.store(true, std::memory_order_release);
}

} // namespace GUI::Features
