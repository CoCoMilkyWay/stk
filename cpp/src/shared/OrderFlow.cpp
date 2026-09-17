// OrderFlow Implementation
#include "shared/OrderFlow.hpp"
#include "features/Backend/FeatureLevels.hpp" // level_valid_rows
#include "misc/profiler.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdio>
#include <limits>

// ============================================================================
// OrderFlow::Kline Implementation
// ============================================================================

void OrderFlow::Kline::begin_generation(uint32_t gen, size_t asset, size_t n_feats) {
  assert(n_feats <= OrderFlowConst::MAX_FEATURES);
  // 换代顺序: 先零计数 (release), 后置 gen (release) —— GUI 见到新 gen 时旧计数必已归零
  for (auto &n : feat_n)
    n.store(0, std::memory_order_release);
  y_min.store(0.0, std::memory_order_relaxed);
  y_max.store(0.0, std::memory_order_relaxed);
  for (size_t i = 0; i < OrderFlowConst::MAX_FEATURES; ++i) {
    feat_y_min[i].store(0.0f, std::memory_order_relaxed);
    feat_y_max[i].store(0.0f, std::memory_order_relaxed);
  }
  pub.store(pack(gen, 0, 0), std::memory_order_release);

  asset_idx = asset;
  n_feat = n_feats;
  x.clear();
  open.clear();
  high.clear();
  low.clear();
  close.clear();
  for (auto &f : feat)
    f.clear();
}

void OrderFlow::Kline::reserve_capacity() {
  // 满容量 = 日数 × 有效分钟数; 之后只 push_back → data() 恒稳定 (前缀读安全)
  const size_t cap = dates.size() * level_valid_rows(1);
  x.reserve(cap);
  open.reserve(cap);
  high.reserve(cap);
  low.reserve(cap);
  close.reserve(cap);
  for (size_t i = 0; i < n_feat; ++i) {
    feat[i].x.reserve(cap);
    feat[i].y.reserve(cap);
  }
}

void OrderFlow::Kline::clear() {
  pub.store(0, std::memory_order_release);
  for (auto &n : feat_n)
    n.store(0, std::memory_order_release);
  dates.clear();
  asset_idx = SIZE_MAX;
  x.clear();
  open.clear();
  high.clear();
  low.clear();
  close.clear();
  for (auto &f : feat)
    f.clear();
  n_feat = 0;
}

// ============================================================================
// OrderFlow::Depth Implementation
// ============================================================================

void OrderFlow::Depth::Plot::clear() {
  x.clear();
  mid_price.clear();
  best_bid.clear();
  best_ask.clear();
  tick_idx_map.clear();
  y_min = y_max = 0.0;
  y_min_with_margin = y_max_with_margin = 0.0;
}

void OrderFlow::Depth::HeatmapMerged::clear() {
  levels.clear();
  rect_count = 0;
}

void OrderFlow::Depth::HeatmapScratch::clear() {
  price_to_level.clear();
  current_tick.clear();
}

size_t OrderFlow::Depth::plot_idx_from_x(double gx) const {
  auto it = std::lower_bound(plot.x.begin(), plot.x.end(), gx);
  if (it == plot.x.end())
    return plot.x.empty() ? SIZE_MAX : plot.x.size() - 1;
  return static_cast<size_t>(it - plot.x.begin());
}

size_t OrderFlow::Depth::snap_to_valid_plot_idx(double gx) const {
  // Fast path: O(1) 秒下标 → plot_idx
  const size_t tick_idx = static_cast<size_t>(std::max(0.0, gx));
  if (tick_idx < plot.tick_idx_map.size()) {
    const size_t mapped = plot.tick_idx_map[tick_idx];
    if (mapped != SIZE_MAX)
      return mapped;
  }
  return plot_idx_from_x(gx);
}

OrderFlow::Depth::Snapshot OrderFlow::Depth::query_depth(size_t plot_idx) const {
  Snapshot result;
  if (plot_idx >= ticks.size())
    return result;

  const Tick &tick = ticks[plot_idx];
  result.mid_price = tick.mid_price;
  result.ref_price = tick.ref_price;
  result.matched_amount = tick.matched_amount;
  result.imbalance_amount = tick.imbalance_amount;
  result.bid_price = &tick.bid_price;
  result.ask_price = &tick.ask_price;
  result.bid_volume = &tick.bid_volume;
  result.ask_volume = &tick.ask_volume;
  result.tick_idx = tick.tick_idx;

  const ClockTime ct = L0_to_Clock(tick.tick_idx);
  result.time.hour = ct.hour;
  result.time.minute = ct.minute;
  result.time.second = ct.second;
  result.valid = true;
  return result;
}

void OrderFlow::Depth::build_plot() {
  plot.clear();

  plot.x.reserve(ticks.size());
  plot.mid_price.reserve(ticks.size());
  plot.best_bid.reserve(ticks.size());
  plot.best_ask.reserve(ticks.size());
  plot.tick_idx_map.assign(OrderFlowConst::L0_CAPACITY, SIZE_MAX);

  constexpr double NAN_D = std::numeric_limits<double>::quiet_NaN();
  double y_min = (std::numeric_limits<double>::max)();
  double y_max = std::numeric_limits<double>::lowest();
  double y_min_all = y_min, y_max_all = y_max; // 全时段兜底 (当日无连续竞价秒才用)

  for (size_t i = 0; i < ticks.size(); ++i) {
    const Tick &tick = ticks[i];
    assert(tick.tick_idx < OrderFlowConst::L0_CAPACITY && "tick_idx out of intra-day range");

    // 竞价交叉秒: bid/ask/spread 无意义 → NaN 断线不画, mid 已是预撮合参考价
    const bool crossed = tick.ref_price > 0.0f;

    plot.tick_idx_map[tick.tick_idx] = i;
    plot.x.push_back(static_cast<double>(tick.tick_idx));
    plot.mid_price.push_back(static_cast<double>(tick.mid_price));
    plot.best_bid.push_back(crossed ? NAN_D : static_cast<double>(tick.bid_price[0]));
    plot.best_ask.push_back(crossed ? NAN_D : static_cast<double>(tick.ask_price[0]));

    // 范围: bid/ask 极值 (bid <= mid <= ask, 初始视图不切掉 spread 边缘, 与图1 双击 fit
    // 口径一致); 只统计连续竞价秒 —— 竞价预撮合价可能摸涨跌停, 不应撑大初始视野
    const double lo = crossed ? static_cast<double>(tick.mid_price) : static_cast<double>(tick.bid_price[0]);
    const double hi = crossed ? static_cast<double>(tick.mid_price) : static_cast<double>(tick.ask_price[0]);
    y_min_all = std::min(y_min_all, lo);
    y_max_all = std::max(y_max_all, hi);
    if (tick.tick_idx >= OrderFlowConst::SEG_PREOPEN_END &&
        tick.tick_idx < OrderFlowConst::SEG_AUCTION_CLOSE_BEGIN) {
      y_min = std::min(y_min, lo);
      y_max = std::max(y_max, hi);
    }
  }
  if (y_min > y_max) { // 当日只有竞价段快照: 退回全时段
    y_min = y_min_all;
    y_max = y_max_all;
  }

  if (!plot.mid_price.empty()) {
    plot.y_min = y_min;
    plot.y_max = y_max;

    const double margin = std::max((plot.y_max - plot.y_min) * OrderFlowConst::Y_MARGIN_RATIO, 0.1);
    plot.y_min_with_margin = plot.y_min - margin;
    plot.y_max_with_margin = plot.y_max + margin;
  }
}

// 自动阈值 = 「档·秒」权重下 log10(净额) 的上分位数:
//   权重 w = 矩形持续秒数 (档高恒 1 tick → w ∝ 该档在图上的着色面积),
//   自高额 bin 向低累计到总权重的 HEATMAP_AUTO_INK_RATIO 即停 → 着色面积 / 有量面积
//   恒定, 故不同日/标的的色块浓度观感一致. 单趟 O(rect_count), 无分配.
void OrderFlow::Depth::build_auto_threshold() {
  constexpr float THR_MIN = OrderFlowConst::HEATMAP_LOG_THR_MIN;
  constexpr float STEP = OrderFlowConst::HEATMAP_LOG_THR_STEP;
  constexpr size_t NBIN =
      static_cast<size_t>((OrderFlowConst::HEATMAP_LOG_THR_MAX - THR_MIN) / STEP + 0.5f);

  auto_log_threshold = OrderFlowConst::HEATMAP_LOG_THR_DEFAULT;
  if (merged.rect_count == 0)
    return;

  const float min_amount = std::pow(10.0f, THR_MIN); // 低于滑条下限的档永远不显示, 不计入总量
  std::array<double, NBIN> hist{};
  double total = 0.0;
  for (const auto &level : merged.levels) {
    // 初始视野外的价位不影响观感 (双击复位即此视野)
    if (level.price < plot.y_min_with_margin || level.price > plot.y_max_with_margin)
      continue;
    for (const auto &r : level.rects) {
      const float a = std::abs(static_cast<float>(r.amount_rmb));
      if (a < min_amount)
        continue;
      const size_t b = std::min(NBIN - 1, static_cast<size_t>((std::log10(a) - THR_MIN) / STEP));
      const double w = static_cast<double>(r.tick_end - r.tick_start);
      hist[b] += w;
      total += w;
    }
  }
  if (total <= 0.0)
    return;

  const double target = total * OrderFlowConst::HEATMAP_AUTO_INK_RATIO;
  double cum = 0.0;
  size_t bin = 0; // 累计不到目标 (ratio >= 1) 才落在下限
  for (size_t i = NBIN; i-- > 0;) {
    cum += hist[i];
    if (cum >= target) {
      bin = i;
      break;
    }
  }
  auto_log_threshold = THR_MIN + static_cast<float>(bin) * STEP; // 该 bin 下沿
}

void OrderFlow::Depth::heatmap_begin(HeatmapScratch &scratch) {
  merged.clear();
  scratch.clear();
  merged.levels.reserve(OrderFlowConst::ESTIMATED_PRICE_LEVELS);
}

// 一秒的全簿状态 (scratch.current_tick, price_key 升序) 并入合并矩形
void OrderFlow::Depth::heatmap_commit_tick(HeatmapScratch &scratch, size_t tick_idx) {
  // 单价位更新: amount 不变则延长上一矩形, 变了则闭合并开新矩形
  auto update_price_level = [&](int price_key, int32_t amount_rmb) {
    auto [it, inserted] = scratch.price_to_level.try_emplace(price_key, 0);
    if (inserted) {
      it->second = merged.levels.size();
      merged.levels.push_back({static_cast<float>(price_key) / OrderFlowConst::PRICE_SCALE, {}});
    }

    auto &level = merged.levels[it->second];
    if (!level.rects.empty()) {
      auto &last_rect = level.rects.back();
      if (last_rect.amount_rmb == amount_rmb) {
        last_rect.tick_end = tick_idx + 1; // 挂单未动: 延长
        return;
      }
      last_rect.tick_end = tick_idx; // 闭合 (首尾相连)
    }

    if (amount_rmb == 0 && level.rects.empty())
      return;

    // Bid (amount > 0): rect 向下延伸; Ask (amount < 0): 向上
    const float price = level.price;
    const bool is_bid = (amount_rmb > 0);
    const float price_high = is_bid ? price : price + OrderFlowConst::TICK_SIZE;
    const float price_low = is_bid ? price - OrderFlowConst::TICK_SIZE : price;
    level.rects.push_back({tick_idx, tick_idx + 1, price_high, price_low, amount_rmb});
    ++merged.rect_count;
  };

  if (scratch.current_tick.empty())
    return;

  // 更新集 = 当前全簿价位 ∪ 簿范围内有活跃矩形的历史价位 (归零闭合) — 两序列双指针归并
  auto cur = scratch.current_tick.begin();
  const auto cur_end = scratch.current_tick.end();
  auto act = scratch.price_to_level.lower_bound(scratch.current_tick.front().first);
  const int max_key = scratch.current_tick.back().first;

  while (true) {
    const bool has_cur = (cur != cur_end);
    const bool has_act = (act != scratch.price_to_level.end() && act->first <= max_key);
    if (!has_cur && !has_act)
      break;

    if (!has_act || (has_cur && cur->first < act->first)) {
      update_price_level(cur->first, cur->second); // 新价位插在 act 已扫过的区间, 迭代器不受影响
      ++cur;
    } else if (!has_cur || act->first < cur->first) {
      if (!merged.levels[act->second].rects.empty())
        update_price_level(act->first, 0); // 价位从簿上消失: 归零闭合
      ++act;
    } else {
      update_price_level(cur->first, cur->second);
      ++cur;
      ++act;
    }
  }
}

void OrderFlow::Depth::clear() {
  gen = 0;
  date.clear();
  asset_idx = SIZE_MAX;
  has_data = false;
  order_count = 0;
  data_valid_count = 0;
  ticks.clear();
  plot.clear();
  merged.clear();
  auto_log_threshold = OrderFlowConst::HEATMAP_LOG_THR_DEFAULT;
  for (auto &f : feat)
    f.clear();
  feat_y_min.fill(0.0f);
  feat_y_max.fill(0.0f);
  n_feat = 0;
  feat_level = 0;
}

// ============================================================================
// OrderFlow::Universe Implementation
// ============================================================================

void OrderFlow::Universe::Slot::clear() {
  gen = 0;
  date.clear();
  meta.clear();
}

bool OrderFlow::Universe::passes(const Meta &m, int board) const {
  // 前置: 当日无有效分钟 → 状态位全为落盘零值, 不可判读
  if (!m.has_data)
    return false;

  if (!st_filter.empty() && !st_filter.count(static_cast<int>(m.risk_warn)))
    return false;

  // 0=在市 1=退市; 未上市 (两者皆否) 在任何非空选择下都排除
  if (!listed_filter.empty()) {
    const int state = m.listed ? 0 : (m.delisted ? 1 : -1);
    if (state < 0 || !listed_filter.count(state))
      return false;
  }

  if (!board_filter.empty() && !board_filter.count(board))
    return false;

  if (!industry_filter.empty() && !industry_filter.count(static_cast<int>(m.industry_l1)))
    return false;

  return true;
}

void OrderFlow::Universe::build_display_order(
    const std::vector<std::pair<std::string, std::string>> &exch_code) {
  display_order.resize(exch_code.size());
  for (size_t i = 0; i < exch_code.size(); ++i)
    display_order[i] = i;
  std::sort(display_order.begin(), display_order.end(), [&](size_t a, size_t b) {
    return exch_code[a] < exch_code[b]; // 市场 → 代码 (pair 字典序)
  });
}

void OrderFlow::Universe::rebuild_candidates(const Slot &s, const std::vector<int> &boards) {
  assert(display_order.size() == boards.size() && "boards 与 display_order 不同长");
  candidates.clear();
  // 按 display_order 遍历 → 候选天然保持 市场 → 代码 序
  for (size_t idx : display_order) {
    if (idx < s.meta.size() && passes(s.meta[idx], boards[idx]))
      candidates.push_back(idx);
  }
  cached_gen = s.gen;
  cached_epoch = filter_epoch;
}

void OrderFlow::Universe::clear() {
  slot[0].clear();
  slot[1].clear();
  front.store(0, std::memory_order_relaxed);
  pending.store(false, std::memory_order_relaxed);
  st_filter.clear();
  listed_filter = {0};
  board_filter.clear();
  industry_filter.clear();
  candidates.clear();
  display_order.clear();
  cached_gen = UINT32_MAX;
  filter_epoch = 0;
  cached_epoch = UINT64_MAX;
  gen = 0;
  req_date.clear();
}

// ============================================================================
// OrderFlow::HeatmapColored Implementation (GUI 线程)
// ============================================================================

// Map log10(amount) to color intensity [0, 1]
static float map_amount_to_intensity(float amount, float log_threshold) {
  const float abs_amount = std::abs(amount);
  if (abs_amount < std::pow(10.0f, log_threshold))
    return 0.0f;

  const float log_amount = std::log10(abs_amount);
  const float log_max = std::log10(OrderFlowConst::AMOUNT_MAX_VISIBLE);
  const float normalized = (log_amount - log_threshold) / (log_max - log_threshold);
  return std::min(1.0f, std::max(0.0f, normalized));
}

// 日内时段 → 色系 (用户可一眼分辨三段): 0 = 集合竞价 (开盘/收盘, 蓝/品红),
// 1 = 盘前5分钟 (单色灰, 不分买卖), 2 = 连续竞价 (绿/红, 原色系)
static int tick_segment(size_t t) {
  if (t < OrderFlowConst::SEG_AUCTION_OPEN_END || t >= OrderFlowConst::SEG_AUCTION_CLOSE_BEGIN)
    return 0;
  return t < OrderFlowConst::SEG_PREOPEN_END ? 1 : 2;
}

// Signed amount → RGBA (ABGR packed), 按时段换色系
static uint32_t amount_to_color(int32_t amount_rmb, float log_threshold, int segment) {
  const float intensity = map_amount_to_intensity(static_cast<float>(amount_rmb), log_threshold);
  if (intensity <= 0.0f)
    return 0; // Transparent

  const uint8_t alpha = static_cast<uint8_t>(intensity * 200 + 55); // [55, 255]
  auto pack = [alpha](uint8_t r, uint8_t g, uint8_t b) {
    return static_cast<uint32_t>(alpha) << 24 | static_cast<uint32_t>(b) << 16 |
           static_cast<uint32_t>(g) << 8 | static_cast<uint32_t>(r);
  };

  if (segment == 1) { // 盘前5分钟: 单色灰 (簿冻结, 无方向语义)
    const uint8_t v = static_cast<uint8_t>(110 + intensity * 110);
    return pack(v, v, v);
  }
  if (segment == 0) { // 集合竞价: bid = 蓝, ask = 品红
    if (amount_rmb > 0)
      return pack(0, static_cast<uint8_t>(60 + intensity * 60), static_cast<uint8_t>(150 + intensity * 105));
    const uint8_t v = static_cast<uint8_t>(150 + intensity * 105);
    return pack(v, 0, v);
  }
  // 连续竞价: bid = 绿, ask = 红
  if (amount_rmb > 0) {
    const uint8_t g = static_cast<uint8_t>(100 + intensity * 155);
    return pack(0, g, static_cast<uint8_t>(intensity * 100));
  }
  return pack(static_cast<uint8_t>(150 + intensity * 105), static_cast<uint8_t>(intensity * 50), 0);
}

void OrderFlow::HeatmapColored::build(const Depth &src, float log_threshold) {
  Trace;
  rects.clear();
  metadata.clear();
  rects.reserve(src.merged.rect_count);
  metadata.reserve(src.merged.rect_count);

  // 时段边界 (跨段的合并矩形按边界切开, 各段各自色系)
  constexpr size_t SEG_BOUNDS[3] = {OrderFlowConst::SEG_AUCTION_OPEN_END,
                                    OrderFlowConst::SEG_PREOPEN_END,
                                    OrderFlowConst::SEG_AUCTION_CLOSE_BEGIN};

  for (const auto &level : src.merged.levels) {
    for (const auto &mr : level.rects) {
      size_t t0 = mr.tick_start;
      while (t0 < mr.tick_end) {
        size_t t1 = mr.tick_end;
        for (size_t b : SEG_BOUNDS) {
          if (b > t0 && b < t1) {
            t1 = b;
            break;
          }
        }
        const uint32_t color = amount_to_color(mr.amount_rmb, log_threshold, tick_segment(t0));
        if (color != 0) {
          rects.push_back({static_cast<double>(t0), static_cast<double>(mr.price_high),
                           static_cast<double>(t1), static_cast<double>(mr.price_low), color});
          // bid (正) 展示 price_high, ask (负) 展示 price_low
          metadata.push_back({mr.amount_rmb,
                              mr.amount_rmb > 0 ? mr.price_high : mr.price_low,
                              t0, t1});
        }
        t0 = t1;
      }
    }
  }

  gen = src.gen;
  threshold = log_threshold;
}

void OrderFlow::HeatmapColored::clear() {
  rects.clear();
  metadata.clear();
  gen = UINT32_MAX;
  threshold = -1.0f;
}

// ============================================================================
// OrderFlow::DepthProfile Implementation (GUI 线程)
// ============================================================================

void OrderFlow::DepthProfile::build(const Depth &src, size_t tick_idx) {
  Trace;
  price.clear();
  amount.clear();
  bid_cum_x.clear();
  bid_cum_y.clear();
  ask_cum_x.clear();
  ask_cum_y.clear();
  bid_top = ask_low = 0;
  cum_max = 0;

  // 锚点秒全簿截面: 每价位在 rects (tick_start 升序, 首尾相连) 里二分找覆盖秒
  std::vector<std::pair<double, double>> rows; // (price, SIGNED amount 元)
  rows.reserve(src.merged.levels.size());
  for (const auto &level : src.merged.levels) {
    const auto &rects = level.rects;
    auto it = std::upper_bound(rects.begin(), rects.end(), tick_idx,
                               [](size_t t, const Depth::HeatmapMerged::Rect &r) { return t < r.tick_start; });
    if (it == rects.begin())
      continue;
    const auto &r = *(it - 1);
    if (r.tick_end <= tick_idx || r.amount_rmb == 0)
      continue;
    rows.emplace_back(static_cast<double>(level.price), static_cast<double>(r.amount_rmb));
  }
  std::sort(rows.begin(), rows.end()); // 价升序

  price.reserve(rows.size());
  amount.reserve(rows.size());
  for (const auto &[p, a] : rows) {
    price.push_back(p);
    amount.push_back(a);
    if (a > 0)
      bid_top = p; // 价升序: 最终值 = 最高买档
    else if (ask_low == 0)
      ask_low = p; // 首个卖档 = 最低卖价
  }

  // 累计曲线 (万元): 买自最高买价向下, 卖自最低卖价向上
  constexpr double TO_WAN = 1e-4;
  double cum = 0;
  for (size_t i = rows.size(); i-- > 0;) {
    if (rows[i].second <= 0)
      continue;
    cum += rows[i].second * TO_WAN;
    bid_cum_x.push_back(cum);
    bid_cum_y.push_back(rows[i].first);
  }
  cum_max = cum;
  cum = 0;
  for (const auto &[p, a] : rows) {
    if (a >= 0)
      continue;
    cum += -a * TO_WAN;
    ask_cum_x.push_back(cum);
    ask_cum_y.push_back(p);
  }
  cum_max = std::max(cum_max, cum);

  gen = src.gen;
  tick = tick_idx;
}

void OrderFlow::DepthProfile::clear() {
  price.clear();
  amount.clear();
  bid_cum_x.clear();
  bid_cum_y.clear();
  ask_cum_x.clear();
  ask_cum_y.clear();
  bid_top = ask_low = 0;
  cum_max = 0;
  gen = UINT32_MAX;
  tick = SIZE_MAX;
}

// ============================================================================
// OrderFlow::UI / OrderFlow Implementation
// ============================================================================

void OrderFlow::UI::clear() {
  *this = UI{};
}

void OrderFlow::clear() {
  kline.clear();
  depth[0].clear();
  depth[1].clear();
  depth_front.store(0, std::memory_order_relaxed);
  depth_pending.store(false, std::memory_order_relaxed);
  universe.clear();
  heatmap_colored.clear();
  depth_profile.clear();
  ui.clear();
  needs_rescan.store(false, std::memory_order_relaxed);
}
