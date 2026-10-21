// OrderFlow Implementation
#include "shared/OrderFlow.hpp"
#include "features/Storage/FeatureLevels.hpp" // level_valid_rows
#include "misc/profiler.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdio>
#include <limits>
#include <utility>

// ============================================================================
// OrderFlow::Overlay Implementation
// ============================================================================

void OrderFlow::Overlay::begin(std::vector<ColRef> cols) {
  assert(cols.size() <= OrderFlowConst::MAX_LINES);
  for (size_t i = 0; i < OrderFlowConst::MAX_LINES; ++i) {
    n[i].store(0, std::memory_order_release);
    lo[i].store(0.0f, std::memory_order_relaxed);
    hi[i].store(0.0f, std::memory_order_relaxed);
    line[i].clear();
  }
  refs = std::move(cols);
}

// 先值域后点数 (release): GUI acquire 读到点数即见同批值域
void OrderFlow::Overlay::publish(const float *lo_, const float *hi_) {
  for (size_t i = 0; i < size(); ++i) {
    lo[i].store(lo_[i], std::memory_order_relaxed);
    hi[i].store(hi_[i], std::memory_order_relaxed);
    n[i].store(line[i].x.size(), std::memory_order_release);
  }
}

void OrderFlow::Overlay::clear() {
  begin({});
}

// ============================================================================
// OrderFlow::Min Implementation
// ============================================================================

OrderFlow::Min::Prefix OrderFlow::Min::prefix(uint32_t gen) const {
  Prefix p;
  uint32_t pub_gen;
  unpack(pub.load(std::memory_order_acquire), pub_gen, p.days, p.points);
  p.ready = pub_gen == (gen & 0xFFFF);
  if (!p.ready)
    p.days = p.points = 0;
  return p;
}

void OrderFlow::Min::begin_generation(uint32_t gen, std::vector<ColRef> refs, double half_width_) {
  // 换代顺序: 先零计数 / 清数组 / 定 overlay 列, 后置 gen (release) —— GUI 见到新 gen 时一切已就位
  // (此刻 GUI 请求代已是新代, 发布代仍是旧代 → GUI 不读 min, 数组可安全重建)
  feat.begin(std::move(refs));
  x.clear();
  for (auto &v : ohlc)
    v.clear();
  half_width = half_width_;
  pub.store(pack(gen, 0, 0), std::memory_order_release);
}

void OrderFlow::Min::reserve_capacity(size_t ohlc_rows) {
  // 满容量 = 日数 × 每日有效行 (OHLC: 其层; overlay: 网格 L_1min); 之后只 push_back → data() 恒稳定 (前缀读安全)
  const size_t bars = dates.size() * ohlc_rows, cap = dates.size() * level_valid_rows(L_1min);
  x.reserve(bars);
  for (auto &v : ohlc)
    v.reserve(bars);
  for (size_t i = 0; i < feat.size(); ++i) {
    feat.line[i].x.reserve(cap);
    feat.line[i].y.reserve(cap);
  }
}

void OrderFlow::Min::clear() {
  idle();
  pub.store(0, std::memory_order_release);
  dates.clear();
  x.clear();
  for (auto &v : ohlc)
    v.clear();
  feat.clear();
}

// ============================================================================
// OrderFlow::Sec Implementation
// ============================================================================

const OrderFlow::Sec::Heat::Rect *OrderFlow::Sec::Heat::Level::at(size_t t) const {
  auto it = std::upper_bound(rects.begin(), rects.end(), t, [](size_t v, const Rect &r) { return v < r.t0; });
  if (it == rects.begin())
    return nullptr;
  --it;
  return t < it->t1 ? &*it : nullptr;
}

std::pair<size_t, size_t> OrderFlow::Sec::Heat::span(double lo, double hi) const {
  const auto first = std::lower_bound(levels.begin(), levels.end(), lo, [](const Level &l, double v) { return l.price < v; });
  const auto last = std::upper_bound(first, levels.end(), hi, [](double v, const Level &l) { return v < l.price; });
  return {static_cast<size_t>(first - levels.begin()), static_cast<size_t>(last - levels.begin())};
}

void OrderFlow::Sec::Heat::clear() {
  levels.clear();
  rect_count = 0;
}

size_t OrderFlow::Sec::row_at(double gx) const {
  if (x.empty())
    return SIZE_MAX;
  const size_t t = static_cast<size_t>(std::max(0.0, gx));
  if (t < row_of.size() && row_of[t] != kNoRow)
    return row_of[t];
  const auto it = std::lower_bound(x.begin(), x.end(), gx);
  return it == x.end() ? x.size() - 1 : static_cast<size_t>(it - x.begin());
}

void OrderFlow::Sec::begin(HeatScratch &s, uint32_t base) {
  constexpr size_t T = OrderFlowConst::SEC_CAPACITY;
  x.reserve(T);
  mid.reserve(T);
  for (auto &v : best)
    v.reserve(T);
  ref.reserve(T);
  matched.reserve(T);
  imbalance.reserve(T);
  row_of.assign(T, kNoRow);
  heat.levels.reserve(OrderFlowConst::ESTIMATED_PRICE_LEVELS);

  s.base = base;
  s.level_of.assign(OrderFlowConst::PRICE_SLOTS, HeatScratch::kNoLevel);
  s.prev.clear();
  s.cur.clear();
  s.t_prev = 0;
}

// 秒内覆盖写: 终值 = 秒末盘口 (分钟语义, 粒度换成秒)
void OrderFlow::Sec::put(size_t t, float bid1, float ask1, float ref_price, float matched_amount, float imbalance_amount) {
  assert(t < OrderFlowConst::SEC_CAPACITY && "秒下标越出日内");
  uint32_t &r = row_of[t];
  if (r == kNoRow) {
    assert((x.empty() || static_cast<double>(t) > x.back()) && "秒须升序");
    r = static_cast<uint32_t>(size());
    x.push_back(static_cast<double>(t));
    mid.push_back(0.0);
    for (auto &v : best)
      v.push_back(0.0);
    ref.push_back(0.0);
    matched.push_back(0.0);
    imbalance.push_back(0.0);
  }
  // 竞价交叉秒: bid/ask/spread 无意义 → NaN 断线不画, mid = 预撮合参考价 (图1 mid 线在竞价段即预撮合曲线)
  constexpr double NAN_D = std::numeric_limits<double>::quiet_NaN();
  const bool crossed = ref_price > 0.0f;
  mid[r] = crossed ? static_cast<double>(ref_price) : 0.5 * (static_cast<double>(bid1) + static_cast<double>(ask1));
  best[Bid][r] = crossed ? NAN_D : static_cast<double>(bid1);
  best[Ask][r] = crossed ? NAN_D : static_cast<double>(ask1);
  ref[r] = static_cast<double>(ref_price);
  matched[r] = static_cast<double>(matched_amount);
  imbalance[r] = static_cast<double>(imbalance_amount);
}

// 一秒的全簿 (s.cur, key 升序) 并入合并矩形: 与上一提交秒 (s.prev) 归并 —— 同额延长 / 变额断开 / 消失闭合 / 新现开矩形.
// 挂单在两提交秒之间 (无快照的秒) 不变, 故延长 / 闭合都盖到 t.
void OrderFlow::Sec::heat_commit(HeatScratch &s, size_t t) {
  const uint32_t t32 = static_cast<uint32_t>(t);
  const auto rects_of = [&](int key) -> std::vector<Heat::Rect> & {
    const size_t i = static_cast<size_t>(key) - s.base;
    assert(i < s.level_of.size() && s.level_of[i] != HeatScratch::kNoLevel);
    return heat.levels[s.level_of[i]].rects;
  };
  const auto open = [&](int key, int32_t amount) {
    const size_t i = static_cast<size_t>(key) - s.base;
    assert(i < s.level_of.size() && "价位下标越出订单簿下标空间");
    uint32_t &li = s.level_of[i];
    if (li == HeatScratch::kNoLevel) {
      li = static_cast<uint32_t>(heat.levels.size());
      heat.levels.push_back({static_cast<float>(key) / OrderFlowConst::PRICE_SCALE, 0, {}});
    }
    heat.levels[li].rects.push_back({t32, t32 + 1, amount});
  };
  const auto close = [&](std::vector<Heat::Rect> &rs) { // 挂单持续到本秒前; 零宽 (边界处新开的段首) 弃
    rs.back().t1 = t32;
    if (rs.back().t0 == rs.back().t1)
      rs.pop_back();
  };

  // 时段边界 b ∈ (上一提交秒, t]: 在场价位于 b 断开重开 (段首零宽, 下面归并延长或弃) —— 矩形不跨段, 各段各自色系
  for (const size_t b : OrderFlowConst::SEG_BOUNDS) {
    if (!(s.t_prev < b && b <= t))
      continue;
    const uint32_t b32 = static_cast<uint32_t>(b);
    for (const auto &[key, amount] : s.prev) {
      std::vector<Heat::Rect> &rs = rects_of(key);
      rs.back().t1 = b32;
      rs.push_back({b32, b32, amount});
    }
  }

  const auto &P = s.prev, &C = s.cur;
  size_t i = 0, j = 0;
  while (i < P.size() || j < C.size()) {
    if (j == C.size() || (i < P.size() && P[i].first < C[j].first)) {
      close(rects_of(P[i].first)); // 消失
      ++i;
    } else if (i == P.size() || C[j].first < P[i].first) {
      open(C[j].first, C[j].second); // 新现
      ++j;
    } else {
      std::vector<Heat::Rect> &rs = rects_of(C[j].first);
      assert(rs.back().amount == P[i].second);
      if (P[i].second == C[j].second) {
        rs.back().t1 = t32 + 1; // 未动: 延长
      } else {
        close(rs); // 变额: 断开
        open(C[j].first, C[j].second);
      }
      ++i;
      ++j;
    }
  }
  std::swap(s.prev, s.cur);
  s.t_prev = t;
}

void OrderFlow::Sec::end() {
  // 价升序 (s.level_of 随即作废, 只在重放期用) + 拼接下标
  std::sort(heat.levels.begin(), heat.levels.end(), [](const Heat::Level &a, const Heat::Level &b) { return a.price < b.price; });
  size_t off = 0;
  for (Heat::Level &l : heat.levels) {
    l.off = off;
    off += l.rects.size();
  }
  heat.rect_count = off;

  has_data = !x.empty();
  if (!has_data)
    return;
  build_view();
  build_auto_threshold();
}

void OrderFlow::Sec::build_view() {
  double y_min = (std::numeric_limits<double>::max)();
  double y_max = std::numeric_limits<double>::lowest();
  double y_min_all = y_min, y_max_all = y_max; // 全时段兜底 (当日无连续竞价秒才用)
  for (size_t i = 0; i < size(); ++i) {
    // 范围: bid/ask 极值 (bid <= mid <= ask, 初始视图不切掉 spread 边缘, 与图1 双击 fit
    // 口径一致); 只统计连续竞价秒 —— 竞价预撮合价可能摸涨跌停, 不应撑大初始视野
    const bool crossed = ref[i] > 0.0;
    const double lo = crossed ? mid[i] : best[Bid][i];
    const double hi = crossed ? mid[i] : best[Ask][i];
    y_min_all = std::min(y_min_all, lo);
    y_max_all = std::max(y_max_all, hi);
    if (x[i] >= static_cast<double>(OrderFlowConst::SEG_PREOPEN_END) &&
        x[i] < static_cast<double>(OrderFlowConst::SEG_AUCTION_CLOSE_BEGIN)) {
      y_min = std::min(y_min, lo);
      y_max = std::max(y_max, hi);
    }
  }
  if (y_min > y_max) { // 当日只有竞价段快照: 退回全时段
    y_min = y_min_all;
    y_max = y_max_all;
  }
  const double margin = std::max((y_max - y_min) * OrderFlowConst::Y_MARGIN_RATIO, 0.1);
  y_lo = y_min - margin;
  y_hi = y_max + margin;
}

// 自动阈值 = 「价位·秒」权重下 log10(净额) 的上分位数:
//   权重 w = 矩形持续秒数 (价位高恒 1 tick → w ∝ 该价位在图上的着色面积),
//   自高额 bin 向低累计到总权重的 HEATMAP_AUTO_INK_RATIO 即停 → 着色面积 / 有量面积
//   恒定, 故不同日/资产的色块浓度观感一致. 单趟 O(rect_count), 无分配.
void OrderFlow::Sec::build_auto_threshold() {
  constexpr float THR_MIN = OrderFlowConst::HEATMAP_LOG_THR_MIN;
  constexpr float STEP = OrderFlowConst::HEATMAP_LOG_THR_STEP;
  constexpr size_t NBIN =
      static_cast<size_t>((OrderFlowConst::HEATMAP_LOG_THR_MAX - THR_MIN) / STEP + 0.5f);

  auto_log_threshold = OrderFlowConst::HEATMAP_LOG_THR_DEFAULT;
  if (heat.rect_count == 0)
    return;

  const float min_amount = std::pow(10.0f, THR_MIN); // 低于滑条下限的价位永远不显示, 不计入总量
  std::array<double, NBIN> hist{};
  double total = 0.0;
  // 初始视野外的价位不影响观感 (双击复位即此视野)
  const auto [l0, l1] = heat.span(y_lo, y_hi);
  for (size_t l = l0; l < l1; ++l) {
    for (const Heat::Rect &r : heat.levels[l].rects) {
      const float a = std::abs(static_cast<float>(r.amount));
      if (a < min_amount)
        continue;
      const size_t b = std::min(NBIN - 1, static_cast<size_t>((std::log10(a) - THR_MIN) / STEP));
      const double w = static_cast<double>(r.t1 - r.t0);
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

void OrderFlow::Sec::clear() {
  gen = 0;
  date = 0;
  asset_idx = SIZE_MAX;
  has_data = false;
  order_count = 0;
  x.clear();
  mid.clear();
  for (auto &v : best)
    v.clear();
  ref.clear();
  matched.clear();
  imbalance.clear();
  row_of.clear();
  y_lo = y_hi = 0.0;
  heat.clear();
  auto_log_threshold = OrderFlowConst::HEATMAP_LOG_THR_DEFAULT;
  feat.clear();
}

// ============================================================================
// OrderFlow::Pick Implementation
// ============================================================================

void OrderFlow::Pick::Slot::clear() {
  gen = 0;
  date = 0;
  meta.clear();
}

bool OrderFlow::Pick::passes(const Meta &m, int board) const {
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

void OrderFlow::Pick::build_display_order(
    const std::vector<std::pair<std::string, std::string>> &exch_code) {
  display_order.resize(exch_code.size());
  for (size_t i = 0; i < exch_code.size(); ++i)
    display_order[i] = i;
  std::sort(display_order.begin(), display_order.end(), [&](size_t a, size_t b) {
    return exch_code[a] < exch_code[b]; // 市场 → 代码 (pair 字典序)
  });
}

void OrderFlow::Pick::rebuild_candidates(const Slot &s, const std::vector<int> &boards) {
  assert(display_order.size() == boards.size() && "boards 与 display_order 不同长");
  candidates.clear();
  // 按 display_order 遍历 → 候选天然保持 市场 → 代码 序
  for (size_t idx : display_order) {
    if (idx < s.meta.size() && passes(s.meta[idx], boards[idx]))
      candidates.push_back(idx);
  }
}

void OrderFlow::Pick::clear() {
  slots.clear();
  st_filter.clear();
  listed_filter = {0};
  board_filter.clear();
  industry_filter.clear();
  candidates.clear();
  display_order.clear();
  filter_epoch = 0;
  key.reset();
}

// ============================================================================
// OrderFlow::HeatColor Implementation (GUI 线程)
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

// 矩形已在 worker 按时段边界断开 (不跨段) → 一矩形一色, 无分配
void OrderFlow::HeatColor::build(const Sec &src, float log_threshold) {
  Trace;
  color.resize(src.heat.rect_count);
  for (const Sec::Heat::Level &l : src.heat.levels)
    for (size_t k = 0; k < l.rects.size(); ++k)
      color[l.off + k] = amount_to_color(l.rects[k].amount, log_threshold, tick_segment(l.rects[k].t0));
}

void OrderFlow::HeatColor::clear() {
  color.clear();
  key.reset();
}

// ============================================================================
// OrderFlow::Profile Implementation (GUI 线程)
// ============================================================================

void OrderFlow::Profile::build(const Sec &src, size_t t) {
  Trace;
  price.clear();
  amount.clear();
  for (Curve &c : cum) {
    c.x.clear();
    c.y.clear();
  }
  edge = {0.0, 0.0};
  cum_max = 0;

  // 锚点秒全簿截面: 价位已价升序, 每价位二分找覆盖秒的矩形
  for (const Sec::Heat::Level &l : src.heat.levels) {
    if (const Sec::Heat::Rect *r = l.at(t)) {
      price.push_back(static_cast<double>(l.price));
      amount.push_back(static_cast<double>(r->amount));
    }
  }
  const size_t n = price.size();
  for (size_t k = 0; k < n; ++k) {
    if (amount[k] > 0)
      edge[Bid] = price[k]; // 价升序: 末个买价位 = 最高买价
    else if (edge[Ask] == 0)
      edge[Ask] = price[k]; // 首个卖价位 = 最低卖价
  }

  // 累计曲线 (万元): 自内沿向外 —— Bid 价降序, Ask 价升序
  constexpr double TO_WAN = 1e-4;
  PerSide<double> sum{0.0, 0.0};
  const auto add = [&](Side s, size_t k) {
    if ((amount[k] > 0) != (s == Bid))
      return;
    sum[s] += std::abs(amount[k]) * TO_WAN;
    cum[s].x.push_back(sum[s]);
    cum[s].y.push_back(price[k]);
  };
  for (size_t k = n; k-- > 0;)
    add(Bid, k);
  for (size_t k = 0; k < n; ++k)
    add(Ask, k);
  cum_max = std::max(sum[Bid], sum[Ask]);
}

void OrderFlow::Profile::clear() {
  price.clear();
  amount.clear();
  for (Curve &c : cum) {
    c.x.clear();
    c.y.clear();
  }
  edge = {0.0, 0.0};
  cum_max = 0;
  key.reset();
}

// ============================================================================
// OrderFlow::EventMarks Implementation (GUI 线程)
// ============================================================================

void OrderFlow::EventMarks::build(const std::vector<misc::Ymd> &dates, const std::vector<AssetEvent> *evs) {
  for (auto &l : lane)
    l.clear();
  events = evs;
  if (!evs || dates.empty())
    return;
  constexpr double W = static_cast<double>(OrderFlowConst::MIN_CAPACITY);
  for (size_t i = 0; i < evs->size(); ++i) {
    const AssetEvent &e = (*evs)[i];
    // 首日 = d0 起首个日轴日 (单点早于轴首丢; 区间早于轴首的部分裁掉), 末日 = d1 及之前末个日轴日
    if (event_def(e.kind).shape == EventShape::Point && e.d0 < dates.front())
      continue;
    const size_t i0 = static_cast<size_t>(std::lower_bound(dates.begin(), dates.end(), e.d0) - dates.begin());
    const size_t i1 = event_def(e.kind).shape == EventShape::Point
                          ? i0
                          : static_cast<size_t>(std::upper_bound(dates.begin(), dates.end(), e.d1) - dates.begin()) - 1;
    if (i0 >= dates.size() || i1 == SIZE_MAX || i1 < i0)
      continue;
    lane[static_cast<size_t>(event_def(e.kind).group)].push_back(
        {static_cast<double>(i0) * W, static_cast<double>(i1 + 1) * W, static_cast<uint32_t>(i)});
  }
  for (const auto &l : lane)
    assert(std::is_sorted(l.begin(), l.end(), [](const Mark &a, const Mark &b) { return a.x0 < b.x0; }) &&
           "事件须 d0 升序 (AssetEvents::by_code 契约)");
}

void OrderFlow::EventMarks::clear() {
  events = nullptr;
  for (auto &l : lane)
    l.clear();
  key.reset();
}

// ============================================================================
// OrderFlow::MissingDays Implementation (GUI 线程)
// ============================================================================

// 每日二分判定有无 K 线点 (x 升序)
void OrderFlow::MissingDays::build(const Min &src, size_t pub_days, size_t pub_points) {
  days.clear();
  const auto end = src.x.begin() + static_cast<long>(pub_points);
  for (size_t d = 0; d < pub_days; ++d) {
    const double lo = static_cast<double>(d * OrderFlowConst::MIN_CAPACITY);
    const double hi = lo + static_cast<double>(OrderFlowConst::MIN_CAPACITY);
    const auto it = std::lower_bound(src.x.begin(), end, lo);
    if (it == end || *it >= hi)
      days.push_back(d);
  }
}

void OrderFlow::MissingDays::clear() {
  days.clear();
  key.reset();
}

// ============================================================================
// OrderFlow::UI / OrderFlow Implementation
// ============================================================================

void OrderFlow::UI::clear() {
  *this = UI{};
}

void OrderFlow::clear() {
  min.clear();
  sec.clear();
  pick.clear();
  heat_color.clear();
  profile.clear();
  event_marks.clear();
  missing_days.clear();
  ui.clear();
  needs_rescan.store(false, std::memory_order_relaxed);
}
