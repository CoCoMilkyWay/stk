// ReplayService — 见头文件
#include "gui/task_agent/services/ReplayService.hpp"

#include "features/OrderBook/LimitOrderBook.hpp"
#include "misc/profiler.hpp"
#include "orders/OrderFrame.hpp"
#include "orders/OrdersDay.hpp"

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <limits>
#include <mutex>
#include <utility>
#include <vector>

namespace GUI::Agent {

struct ReplayService::Res {
  L2::FrameDecoder decoder;
  LimitOrderBook lob;
  TickData tick_data;
  Res() : lob(L2::LOB_ORDER_CAPACITY) {}
};

namespace {

constexpr size_t kDepthLevels = 5;      // 前几档挂单额
constexpr size_t kCheckEvery = 1 << 16; // 每这么多笔查一次取消 / 推一次进度

int clock_of(uint32_t tick) { return L2::tick_hour(tick) * 3600 + L2::tick_minute(tick) * 60 + L2::tick_second(tick); }
int cs_of(uint32_t tick) { return L2::tick_millisecond(tick); }
int sec_of(uint32_t tick) { return trade_sec(clock_of(tick)); }
// 挂出 a → 事件 b 的交易秒 (午休不计; 10ms 精度)
float life_of(uint32_t a, uint32_t b) { return std::max(0.f, static_cast<float>(sec_of(b) - sec_of(a)) + 0.01f * static_cast<float>(cs_of(b) - cs_of(a))); }
bool continuous(L2::MarketState s) { return s == L2::MarketState::CONTINUOUS_TRADING_MORNING || s == L2::MarketState::CONTINUOUS_TRADING_AFTERNOON; }

// 当日新挂单额的 q 分位 (无有价新挂 → 不设大单)
float quantile_of(const L2::FrameView &v, float q) {
  std::vector<float> amt;
  for (size_t i = 0; i < v.head.count; ++i)
    if (const L2::Order &o = v.orders[i]; o.order_type == L2::OrderType::MAKER && o.price > 0)
      amt.push_back(static_cast<float>(o.price) * 0.01f * static_cast<float>(o.volume));
  if (amt.empty())
    return std::numeric_limits<float>::max();
  const auto k = amt.begin() + static_cast<std::ptrdiff_t>(q * static_cast<float>(amt.size() - 1));
  std::nth_element(amt.begin(), k, amt.end());
  return *k;
}

} // namespace

ReplayService::ReplayService() { Start(); }
ReplayService::~ReplayService() { Stop(); }

bool ReplayService::run(ReplayRequest &req) {
  TraceN("ReplayBuild");
  const BookSpec &sp = req.spec;
  const int s0 = trade_sec(sp.from), s1 = trade_sec(sp.to - 1) + 1, at = trade_sec(sp.at); // 窗口 [s0, s1) / 锚点 (交易秒)
  assert(req.orders && s0 < s1 && at >= s0 && at < s1);
  if (!res_)
    res_ = std::make_unique<Res>();
  Res &R = *res_;
  Book out{.spec = sp};
  const auto publish_result = [&] {
    std::lock_guard<std::mutex> lock(mutex);
    result = std::move(out);
  };

  // ---- 帧目录快照定位 → 解码 (无帧 = 停牌 / 未编码 / 早于逐笔) ----
  const size_t didx = req.orders->day_idx(sp.date);
  const FrameRef &ref = req.orders->at(didx, req.asset);
  L2::FrameView view;
  if (!ref.empty())
    view = DayReader(sp.date).get(ref, R.decoder);
  if (view.head.count == 0) {
    out.error = didx == OrdersCatalog::kNoDay ? "逐笔日文件里没有这一天 (非交易日, 或该日未编码)"
                : ref.tombstone()             ? "当日该股逐笔为空 (停牌或没有委托)"
                                              : "当日日文件里没有该股 (未上市 / 已退市, 或未编码)";
    publish_result();
    return true;
  }
  const size_t n = view.head.count;
  begin(Phase::Running, n);
  out.orders = static_cast<int>(n);
  const float big = sp.big > 0.0 ? static_cast<float>(sp.big) : quantile_of(view, kBigQuantile);
  out.big = big;

  // ---- 逐笔重放 ----
  LimitOrderBook &lob = R.lob;
  lob.bind(&R.tick_data, req.asset, req.exchange);
  lob.begin_frame(view);
  constexpr size_t N = L2::LOB_DEPTH;
  const LOB_Feature &lf = R.tick_data.lob;
  out.secs.resize(static_cast<size_t>(s1 - s0));
  for (int t = s0; t < s1; ++t)
    out.secs[static_cast<size_t>(t - s0)].clock = trade_clock(t);
  const auto sec_at = [&](int t) -> BookSec & { return out.secs[static_cast<size_t>(t - s0)]; };

  // 盘口态 (随 depth_updated 刷新) → 逐秒秒末; 锚点秒结束时 (下一秒的首笔之前) 取全簿
  BookSec cur;
  int settled = s0; // [s0, settled) 的秒末态已写
  bool booked = false;
  const auto settle_until = [&](int t) {
    if (!booked && t > at) {
      lob.for_each_visible_level([&](float price, int32_t net_qty) {
        if (net_qty != 0)
          out.book.emplace_back(price, price * static_cast<float>(net_qty));
      });
      booked = true;
    }
    for (; settled < std::min(t, s1); ++settled) {
      BookSec &x = sec_at(settled);
      x.bid = cur.bid, x.ask = cur.ask, x.depth[0] = cur.depth[0], x.depth[1] = cur.depth[1];
      x.ref = cur.ref, x.matched = cur.matched, x.imbalance = cur.imbalance;
    }
  };
  const auto refresh = [&] {
    const Level *b1 = lf.depth_buffer[N], *a1 = lf.depth_buffer[N - 1];
    cur.bid = LOB_Feature::is_real(b1) ? lf.price_of(b1) : 0.f;
    cur.ask = LOB_Feature::is_real(a1) ? lf.price_of(a1) : 0.f;
    for (size_t s = 0; s < 2; ++s) {
      cur.depth[s] = 0.f;
      for (size_t i = 0; i < kDepthLevels; ++i)
        if (const Level *l = lf.depth_buffer[s == 0 ? N + i : N - 1 - i]; LOB_Feature::is_real(l))
          cur.depth[s] += lf.price_of(l) * static_cast<float>(std::abs(l->net_quantity));
    }
    cur.ref = lf.auction_ref_price;
    cur.matched = cur.ref * static_cast<float>(lf.auction_matched_qty);
    cur.imbalance = cur.ref * static_cast<float>(lf.auction_imbalance);
  };

  const auto in_win = [&](int t) { return t >= s0 && t < s1; };
  const auto event = [&](uint32_t tick, BookKind k, int side, int levels, float price, float first, float amount, float life) {
    out.events.push_back({clock_of(tick), cs_of(tick), k, side, levels, price, first, amount, life});
  };
  // 扫单: 同一主动 id 的连续成交 (连续竞价里; 主动 id 缺失的不并)
  struct Sweep {
    bool on = false;
    uint32_t id = 0, tick = 0;
    int side = 0, levels = 0;
    float first = 0.f, last = 0.f;
    double amount = 0.0;
  } sw;
  const auto close_sweep = [&] {
    if (sw.on && in_win(sec_of(sw.tick)) && (sw.levels >= 2 || sw.amount >= big))
      event(sw.tick, BookKind::Sweep, sw.side, sw.levels, sw.last, sw.first, static_cast<float>(sw.amount), -1.f);
    sw.on = false;
  };

  for (size_t i = 0; i < n; ++i) {
    if (i > 0 && i % kCheckEvery == 0) {
      if (cancel_.load(std::memory_order_relaxed)) {
        lob.unbind();
        return false;
      }
      publish(kCheckEvery);
    }
    const L2::Order &o = view.orders[i];
    const int t = sec_of(o.tick);
    settle_until(t);
    lob.process(o);
    if (lf.depth_updated)
      refresh();
    const bool win = in_win(t);
    const uint8_t s = static_cast<uint8_t>(o.order_dir); // MAKER / CANCEL: 委托侧; TAKER: 主动方
    const float px = static_cast<float>(o.price) * 0.01f, vol = static_cast<float>(o.volume);
    BookSec *x = win ? &sec_at(t) : nullptr;
    switch (o.order_type) {
    case L2::OrderType::MAKER: {
      close_sweep();
      if (!win)
        break;
      x->maker[s] += px * vol;
      if (px > 0.f && px * vol >= big)
        event(o.tick, BookKind::New, s, 0, px, px, px * vol, -1.f);
      break;
    }
    case L2::OrderType::CANCEL: {
      close_sweep();
      if (!win)
        break;
      const float p = px > 0.f ? px : lf.ord_price[s]; // 深市撤单不带价, 取所在价位
      x->cancel[s] += p * vol;
      if (p > 0.f && p * vol >= big)
        event(o.tick, BookKind::Cancel, s, 0, p, p, p * vol, ord_reliable(lf, s) ? life_of(lf.ord_tick[s], o.tick) : -1.f);
      break;
    }
    case L2::OrderType::TAKER: {
      out.open = out.open > 0.f ? out.open : px;
      out.high = std::max(out.high, px);
      out.low = out.low > 0.f ? std::min(out.low, px) : px;
      out.close = px;
      out.amount += static_cast<double>(px) * vol;
      const uint8_t p = static_cast<uint8_t>(1 - s); // 被动方
      if (win) {
        x->taker[s] += px * vol, x->last = px;
        if (ord_reliable(lf, p) && lf.ord_rest[p] <= o.volume)
          if (const float a = lf.ord_price[p] * static_cast<float>(lf.ord_orig[p]); a >= big)
            event(o.tick, BookKind::Filled, p, 0, lf.ord_price[p], lf.ord_price[p], a, life_of(lf.ord_tick[p], o.tick));
      }
      const uint32_t aid = s == 0 ? o.bid_order_id : o.ask_order_id;
      if (sw.on && sw.id != aid)
        close_sweep();
      if (aid == 0 || !continuous(lf.market_state))
        break;
      if (!sw.on)
        sw = {true, aid, o.tick, s, 1, px, px, 0.0};
      else if (px != sw.last)
        ++sw.levels, sw.last = px;
      sw.amount += static_cast<double>(px) * vol;
      break;
    }
    default:
      break;
    }
  }
  close_sweep();
  settle_until(s1); // s1 > at: 末笔之后锚点秒尚未取簿的, 此刻的簿即其秒末
  lob.unbind();
  std::stable_sort(out.events.begin(), out.events.end(), [](const BookEvent &a, const BookEvent &b) { return std::pair(a.clock, a.cs) < std::pair(b.clock, b.cs); }); // 扫单收尾才入列
  publish_result();
  return true;
}

} // namespace GUI::Agent
