// view_book — 看盘口重放: 输入 code / date / time / step / at / big / n → BookRes (当日与窗口概况 / 逐秒序列 / 逐时表 / 大单事件 / 全簿剖面) → 原文; 展示见 ui/look/ViewBook.cpp
#include "gui/task_agent/Config.hpp"
#include "gui/task_agent/tools/look/Kernel.hpp"

#include "features/OrderBook/LimitOrderBook.hpp"
#include "orders/OrderFrame.hpp"
#include "orders/OrdersDay.hpp"

#include <limits>

namespace GUI::Agent {

using namespace look;
using json = nlohmann::ordered_json;

namespace {

constexpr int kAmOpen = 9 * 3600 + 15 * 60, kAmClose = 11 * 3600 + 30 * 60, kPmOpen = 13 * 3600, kPmClose = 15 * 3600;
constexpr int kOpenCall = 9 * 3600 + 25 * 60, kCloseCall = 14 * 3600 + 57 * 60;
constexpr int kTradeSecs = (kAmClose - kAmOpen) + (kPmClose - kPmOpen);
constexpr float kBigQuantile = 0.99f;
constexpr size_t kDepthLevels = 5;
constexpr size_t kCheckEvery = 1 << 16;
constexpr size_t kProfile = 8;
constexpr int kBarCells = 12;
constexpr int kSteps[] = {1, 2, 3, 5, 10, 15, 20, 30, 60, 120, 300, 600};
enum BookKind : uint8_t { BkNew,
                          BkCancel,
                          BkFilled,
                          BkSweep,
                          kBookKindCount };
static_assert(std::size(kBookKinds) == kBookKindCount);

// 逐笔重放的常驻资源: 每个 Lab worker 线程一份, 首次 view_book 时建
struct Replay {
  L2::FrameDecoder decoder;
  LimitOrderBook lob;
  TickData tick_data;
  Replay() : lob(L2::LOB_ORDER_CAPACITY) {}
};

constexpr bool in_session(int clock) { return (clock >= kAmOpen && clock < kAmClose) || (clock >= kPmOpen && clock < kPmClose); }
constexpr int trade_sec(int clock) {
  return clock < kAmOpen    ? 0
         : clock < kAmClose ? clock - kAmOpen
         : clock < kPmOpen  ? kAmClose - kAmOpen
         : clock < kPmClose ? kAmClose - kAmOpen + clock - kPmOpen
                            : kTradeSecs - 1;
}
constexpr int trade_clock(int sec) { return sec < kAmClose - kAmOpen ? kAmOpen + sec : kPmOpen + sec - (kAmClose - kAmOpen); }

int parse_clock(const std::string &s) {
  if (!hm_shape(s) || !(s.size() == 5 || (s.size() == 8 && s[5] == ':' && std::isdigit(static_cast<unsigned char>(s[6])) && std::isdigit(static_cast<unsigned char>(s[7])))))
    return -1;
  const int h = std::stoi(s.substr(0, 2)), m = std::stoi(s.substr(3, 2)), x = s.size() == 8 ? std::stoi(s.substr(6, 2)) : 0;
  return h < 24 && m < 60 && x < 60 ? h * 3600 + m * 60 + x : -1;
}
std::string hms(int c) {
  char b[16];
  std::snprintf(b, sizeof(b), "%02d:%02d:%02d", c / 3600, c / 60 % 60, c % 60);
  return b;
}
int step_of(int want, int secs) {
  if (want > 0)
    return want;
  for (const int s : kSteps)
    if ((secs + s - 1) / s <= kBookRows)
      return s;
  return kSteps[std::size(kSteps) - 1];
}
std::string w4(double yuan) {
  const double v = yuan / 1e4;
  return v == 0.0 ? "0" : std::fabs(v) < 10.0 ? fmt("%.1f", v)
                                              : fmt("%.0f", v);
}
std::string px2(double p) { return p > 0.0 ? fmt("%.2f", p) : "—"; }

int tick_clock(uint32_t tick) { return L2::tick_hour(tick) * 3600 + L2::tick_minute(tick) * 60 + L2::tick_second(tick); }
int tick_cs(uint32_t tick) { return L2::tick_millisecond(tick); }
int tick_sec(uint32_t tick) { return trade_sec(tick_clock(tick)); }
float life_of(uint32_t a, uint32_t b) { return std::max(0.f, static_cast<float>(tick_sec(b) - tick_sec(a)) + 0.01f * static_cast<float>(tick_cs(b) - tick_cs(a))); }
bool continuous(L2::MarketState s) { return s == L2::MarketState::CONTINUOUS_TRADING_MORNING || s == L2::MarketState::CONTINUOUS_TRADING_AFTERNOON; }

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

std::string BookRes::text() const {
  std::string s = head + " (额 万元; 价差 / 距盘口按 0.01 元一跳)\n";
  s += "当日: 逐笔 " + str(orders) + " 条 · " +
       (traded ? "开 " + px2(open) + " 高 " + px2(high) + " 低 " + px2(low) + " 收 " + px2(close) + " · 成交额 " + w4(amt) : std::string("无成交")) + " · 大单门槛 " + w4(big) +
       (big_given ? " (给定)" : " (当日新挂单额 " + fmt("%.0f", kBigQuantile * 100.0) + " 分位)") + "\n";
  s += "窗口: 主动买 " + w4(taker[0]) + " / 主动卖 " + w4(taker[1]) + " · 新挂 买 " + w4(maker[0]) + " / 卖 " + w4(maker[1]) + " · 撤单 买 " + w4(cancel[0]) + " / 卖 " +
       w4(cancel[1]) + (last > 0.0 ? " · 成交价 " + px2(first) + " → " + px2(last) : std::string(" · 无成交")) +
       (std::isfinite(spread) ? " · 平均价差 " + fmt("%.1f", spread) + " 跳" : std::string()) + "\n";
  s += "- 逐秒成交价 " + spark(price) + "\n- 累计主动买 − 主动卖 (万) " + spark(net) + "\n";

  s += "\n逐时表 (每行 " + std::to_string(step) + " 秒; 主买 / 主卖 = 主动成交额, 挂 / 撤 = 新挂 / 撤单额, 五档 = 前 5 价位挂单额" +
       (call ? std::string("; 竞价段: 参考价 / 匹配额 / 未匹配额 (+ 买剩 − 卖剩)") : std::string()) + ")\n";
  std::vector<std::string> ths{"起", "末价", "买一", "卖一", "价差", "主买", "主卖", "挂买", "挂卖", "撤买", "撤卖", "买五档", "卖五档"};
  if (call)
    ths.push_back("参考价"), ths.push_back("匹配"), ths.push_back("未匹配");
  text::Table ttbl(ths);
  for (const Row &z : rows) {
    std::vector<std::string> row{hms(z.clock), px2(z.last), px2(z.bid), px2(z.ask), z.bid > 0.0 && z.ask > 0.0 ? fmt("%.0f", std::round((z.ask - z.bid) * 100.0)) : std::string("—"),
                                 w4(z.taker[0]), w4(z.taker[1]), w4(z.maker[0]), w4(z.maker[1]), w4(z.cancel[0]), w4(z.cancel[1]), w4(z.depth[0]), w4(z.depth[1])};
    if (call)
      row.push_back(px2(z.ref)), row.push_back(w4(z.matched)), row.push_back(w4(z.imbalance));
    ttbl.add(row);
  }
  s += ttbl.str();

  s += "\n大单事件 (窗口内共 " + str(events) + " 起; 侧 = 委托所在侧, 扫单 = 主动方; 寿命 = 挂出到撤 / 吃完的交易秒):\n";
  for (size_t k = 0; k < kBookKindCount; ++k) {
    s += "- " + std::string(kBookKinds[k]) + ": 买 " + std::to_string(count[k][0]) + " 起 " + w4(amount[k][0]) + " / 卖 " + std::to_string(count[k][1]) + " 起 " + w4(amount[k][1]);
    if (k == BkSweep && count[k][0] + count[k][1] > 0)
      s += " · 平均吃 买 " + (count[k][0] ? fmt("%.1f", levels[0] / count[k][0]) : std::string("—")) + " / 卖 " + (count[k][1] ? fmt("%.1f", levels[1] / count[k][1]) : std::string("—")) +
           " 价位";
    if (k == BkCancel && (std::isfinite(life[0]) || std::isfinite(life[1])))
      s += " · 寿命中位 买 " + (std::isfinite(life[0]) ? fmt("%.1f", life[0]) : std::string("—")) + " / 卖 " + (std::isfinite(life[1]) ? fmt("%.1f", life[1]) : std::string("—")) + " 秒";
    s += "\n";
  }
  if (!top.empty()) {
    s += "\n额最大的 " + str(top.size()) + " 起 (按时序; 距盘口 = 与上一秒末同侧最优价差几跳: 0 在盘口, + 在盘口后方, − 越过盘口):\n";
    text::Table etbl({"时刻", "事件", "侧", "价", "距盘口", "额", "寿命 秒"});
    for (const Event &e : top) {
      const bool sweep = e.kind == BkSweep;
      const std::string dist = sweep        ? "吃 " + std::to_string(e.levels) + " 价位"
                               : e.best > 0 ? fmt("%+.0f", std::round((e.side == 0 ? e.best - e.price : e.price - e.best) * 100.0))
                                            : std::string("—");
      etbl.add({hms(e.clock) + fmt(".%02.0f", e.cs), kBookKinds[e.kind], e.side == 0 ? "买" : "卖", sweep ? px2(e.first) + "→" + px2(e.price) : px2(e.price), dist, w4(e.amount),
                e.life >= 0.0 ? fmt("%.1f", e.life) : "—"});
    }
    s += etbl.str();
  }

  s += "\n全簿剖面 @ " + hms(at) + " 秒末 (全簿 买 " + w4(book_bid) + " / 卖 " + w4(book_ask) + "; 列盘口两侧各至多 " + str(kProfile) + " 价位):\n";
  if (profile.empty())
    return s + "簿空\n";
  double mx = 0.0;
  for (const Level &l : profile)
    mx = std::max(mx, l.amount);
  text::Table ptbl({"档", "价", "额", ""});
  for (const Level &l : profile) {
    std::string bar;
    for (long k = std::lround(l.amount / mx * kBarCells); k > 0; --k)
      bar += "█";
    ptbl.add({std::string(l.bid ? "买" : "卖") + std::to_string(l.rank), px2(l.price), w4(l.amount), bar});
  }
  return s + ptbl.str();
}

ToolOut ViewBook(const Call &c, Bay &bay) {
  TraceN("AgentBookReplay");
  const ViewEnv &env = bay.env;
  Engine::FactorFlow &F = bay.flow;
  thread_local Replay R;
  std::string err, code;
  uint32_t asset_id = 0;
  if (!resolve_code(c.str("code"), env, code, asset_id, err))
    return Fail(std::move(err));
  const int date = ymd_of(c.str("date"));
  if (date == 0)
    return Fail("date: 日期写成 YYYY-MM-DD");
  const std::string w = c.str("time");
  const size_t dash = w.find('-');
  const int from = dash == std::string::npos ? -1 : parse_clock(w.substr(0, dash));
  const int to = dash == std::string::npos ? -1 : parse_clock(w.substr(dash + 1));
  if (from < 0 || to < 0)
    return Fail("time: 写成 HH:MM-HH:MM 或 HH:MM:SS-HH:MM:SS (如 10:00-10:30)");
  if (from >= to || !in_session(from) || !in_session(to - 1))
    return Fail("time: " + w + " 须起 < 止, 两端在 09:15–11:30 / 13:00–15:00 内 (止是开区间, 可写 11:30 / 15:00)");
  const int n_secs = trade_sec(to - 1) + 1 - trade_sec(from);
  if (n_secs > kBookMins * 60)
    return Fail("time: " + w + " 有 " + std::to_string((n_secs + 59) / 60) + " 分钟交易时间, 至多 " + std::to_string(kBookMins) + " 分钟; 收窄窗口");
  if (const int sp = c.num("step"); sp > 0 && (n_secs + sp - 1) / sp > kBookRows)
    return Fail("step: 窗口 " + std::to_string(n_secs) + " 秒按每行 " + std::to_string(sp) + " 秒有 " + std::to_string((n_secs + sp - 1) / sp) + " 行, 至多 " +
                std::to_string(kBookRows) + " 行; 加大 step 或给 0 自动");
  int at_clock = to - 1;
  if (const std::string a = c.str("at"); !a.empty()) {
    at_clock = parse_clock(a);
    if (at_clock < from || at_clock >= to || !in_session(at_clock))
      return Fail("at: 写成窗口内交易时段里的 HH:MM:SS");
  }

  assert(env.orders && "逐笔帧目录未扫描");
  const std::string head_s = "## " + code + " · " + date_of(date) + " · " + hms(from) + "–" + hms(to) + " 逐笔重放";
  const size_t didx = env.orders->day_idx(date);
  const FrameRef &ref = env.orders->at(didx, asset_id);
  L2::FrameView view;
  if (!ref.empty())
    view = DayReader(date).get(ref, R.decoder);
  if (view.head.count == 0) {
    const char *why = didx == OrdersCatalog::kNoDay ? "逐笔日文件里没有这一天 (非交易日, 或该日未编码)"
                      : ref.tombstone()             ? "当日该股逐笔为空 (停牌或没有委托)"
                                                    : "当日日文件里没有该股 (未上市 / 已退市, 或未编码)";
    return Fail(head_s + "\n没重放成: " + why);
  }

  const int s0 = trade_sec(from), s1 = trade_sec(to - 1) + 1, at_s = trade_sec(at_clock);
  const size_t n_ord = view.head.count;
  F.prog.begin(Phase::Running, n_ord);
  const float big = c.num("big") > 0 ? static_cast<float>(c.num("big") * 1e4) : quantile_of(view, kBigQuantile);

  struct BookSec {
    int clock = 0;
    float bid = 0, ask = 0, depth[2] = {}, ref = 0, matched = 0, imbalance = 0;
    float taker[2] = {}, maker[2] = {}, cancel[2] = {}, last = 0;
  };
  struct BookEvent {
    int clock = 0, cs = 0;
    BookKind kind = BkNew;
    int side = 0, levels = 0;
    float price = 0, first = 0, amount = 0, life = -1;
  };

  float day_open = 0, day_high = 0, day_low = 0, day_close = 0;
  double day_amt = 0;
  std::vector<BookSec> secs(static_cast<size_t>(s1 - s0));
  for (int t = s0; t < s1; ++t)
    secs[static_cast<size_t>(t - s0)].clock = trade_clock(t);
  std::vector<BookEvent> events;
  std::vector<std::pair<float, float>> profile;

  LimitOrderBook &lob = R.lob;
  lob.bind(&R.tick_data, asset_id, env.exchanges[asset_id]);
  lob.begin_frame(view);
  constexpr size_t N = L2::LOB_DEPTH;
  const LOB_Feature &lf = R.tick_data.lob;
  const auto sec_at = [&](int t) -> BookSec & { return secs[static_cast<size_t>(t - s0)]; };

  BookSec cur;
  int settled = s0;
  bool booked = false;
  const auto settle_until = [&](int t) {
    if (!booked && t > at_s) {
      lob.for_each_visible_level([&](float price, int32_t net_qty) {
        if (net_qty != 0)
          profile.emplace_back(price, price * static_cast<float>(net_qty));
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
    for (size_t sd = 0; sd < 2; ++sd) {
      cur.depth[sd] = 0.f;
      for (size_t i = 0; i < kDepthLevels; ++i)
        if (const Level *l = lf.depth_buffer[sd == 0 ? N + i : N - 1 - i]; LOB_Feature::is_real(l))
          cur.depth[sd] += lf.price_of(l) * static_cast<float>(std::abs(l->net_quantity));
    }
    cur.ref = lf.auction_ref_price;
    cur.matched = cur.ref * static_cast<float>(lf.auction_matched_qty);
    cur.imbalance = cur.ref * static_cast<float>(lf.auction_imbalance);
  };
  const auto in_win = [&](int t) { return t >= s0 && t < s1; };
  const auto push_ev = [&](uint32_t tick, BookKind k, int side, int levels, float price, float first, float amount, float life) {
    events.push_back({tick_clock(tick), tick_cs(tick), k, side, levels, price, first, amount, life});
  };
  struct Sweep {
    bool on = false;
    uint32_t id = 0, tick = 0;
    int side = 0, levels = 0;
    float first = 0.f, last = 0.f;
    double amount = 0.0;
  } sw;
  const auto close_sweep = [&] {
    if (sw.on && in_win(tick_sec(sw.tick)) && (sw.levels >= 2 || sw.amount >= big))
      push_ev(sw.tick, BkSweep, sw.side, sw.levels, sw.last, sw.first, static_cast<float>(sw.amount), -1.f);
    sw.on = false;
  };

  for (size_t i = 0; i < n_ord; ++i) {
    if (i > 0 && i % kCheckEvery == 0) {
      if (F.cancel.load(std::memory_order_relaxed)) {
        lob.unbind();
        return {};
      }
      F.prog.publish(kCheckEvery);
    }
    const L2::Order &o = view.orders[i];
    const int t = tick_sec(o.tick);
    settle_until(t);
    lob.process(o);
    if (lf.depth_updated)
      refresh();
    const bool win = in_win(t);
    const uint8_t sd = static_cast<uint8_t>(o.order_dir);
    const float px = static_cast<float>(o.price) * 0.01f, vol = static_cast<float>(o.volume);
    BookSec *x = win ? &sec_at(t) : nullptr;
    switch (o.order_type) {
    case L2::OrderType::MAKER:
      close_sweep();
      if (win) {
        x->maker[sd] += px * vol;
        if (px > 0.f && px * vol >= big)
          push_ev(o.tick, BkNew, sd, 0, px, px, px * vol, -1.f);
      }
      break;
    case L2::OrderType::CANCEL: {
      close_sweep();
      if (!win)
        break;
      const float p = px > 0.f ? px : lf.ord_price[sd];
      x->cancel[sd] += p * vol;
      if (p > 0.f && p * vol >= big)
        push_ev(o.tick, BkCancel, sd, 0, p, p, p * vol, ord_reliable(lf, sd) ? life_of(lf.ord_tick[sd], o.tick) : -1.f);
      break;
    }
    case L2::OrderType::TAKER: {
      day_open = day_open > 0.f ? day_open : px;
      day_high = std::max(day_high, px);
      day_low = day_low > 0.f ? std::min(day_low, px) : px;
      day_close = px;
      day_amt += static_cast<double>(px) * vol;
      const uint8_t p = static_cast<uint8_t>(1 - sd);
      if (win) {
        x->taker[sd] += px * vol, x->last = px;
        if (ord_reliable(lf, p) && lf.ord_rest[p] <= o.volume)
          if (const float a = lf.ord_price[p] * static_cast<float>(lf.ord_orig[p]); a >= big)
            push_ev(o.tick, BkFilled, p, 0, lf.ord_price[p], lf.ord_price[p], a, life_of(lf.ord_tick[p], o.tick));
      }
      const uint32_t aid = sd == 0 ? o.bid_order_id : o.ask_order_id;
      if (sw.on && sw.id != aid)
        close_sweep();
      if (aid == 0 || !continuous(lf.market_state))
        break;
      if (!sw.on)
        sw = {true, aid, o.tick, sd, 1, px, px, 0.0};
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
  settle_until(s1);
  lob.unbind();
  std::stable_sort(events.begin(), events.end(), [](const BookEvent &a, const BookEvent &b) { return std::pair(a.clock, a.cs) < std::pair(b.clock, b.cs); });

  // ---- 汇总 ----
  auto res = std::make_shared<BookRes>();
  BookRes &B = *res;
  const int n = static_cast<int>(secs.size()), sp = step_of(c.num("step"), n), R_rows = (n + sp - 1) / sp;
  B.head = head_s, B.orders = n_ord;
  B.traded = day_close > 0, B.open = day_open, B.high = day_high, B.low = day_low, B.close = day_close, B.amt = day_amt;
  B.big = big, B.big_given = c.num("big") > 0;
  B.from = from, B.step = sp, B.call = from < kOpenCall || to > kCloseCall;

  double spread = 0.0, cum = 0.0;
  int nsp = 0;
  float first = 0.f, carry = 0.f;
  B.price.assign(static_cast<size_t>(n), kNaN), B.net.assign(static_cast<size_t>(n), 0.0);
  for (int i = 0; i < n; ++i) {
    const BookSec &x = secs[static_cast<size_t>(i)];
    for (int e = 0; e < 2; ++e)
      B.taker[e] += x.taker[e], B.maker[e] += x.maker[e], B.cancel[e] += x.cancel[e];
    if (x.last > 0.f)
      first = first > 0.f ? first : x.last, carry = x.last;
    B.price[static_cast<size_t>(i)] = carry > 0.f ? carry : kNaN;
    cum += x.taker[0] - x.taker[1], B.net[static_cast<size_t>(i)] = cum / 1e4;
    if (x.bid > 0.f && x.ask > x.bid)
      spread += (x.ask - x.bid) * 100.0, ++nsp;
  }
  B.first = first, B.last = carry, B.spread = nsp ? spread / nsp : kNaN;

  for (int r = 0; r < R_rows; ++r) {
    const int i0 = r * sp, i1 = std::min(n, i0 + sp);
    BookRes::Row &row = B.rows.emplace_back();
    for (int i = i0; i < i1; ++i)
      for (int e = 0; e < 2; ++e) {
        const BookSec &x = secs[static_cast<size_t>(i)];
        row.taker[e] += x.taker[e], row.maker[e] += x.maker[e], row.cancel[e] += x.cancel[e];
      }
    const BookSec &z = secs[static_cast<size_t>(i1 - 1)];
    row.clock = secs[static_cast<size_t>(i0)].clock, row.last = B.price[static_cast<size_t>(i1 - 1)], row.bid = z.bid, row.ask = z.ask;
    row.depth[0] = z.depth[0], row.depth[1] = z.depth[1], row.ref = z.ref, row.matched = z.matched, row.imbalance = z.imbalance;
  }

  std::vector<double> life[2];
  for (const BookEvent &e : events) {
    ++B.count[e.kind][e.side], B.amount[e.kind][e.side] += e.amount;
    if (e.kind == BkSweep)
      B.levels[e.side] += e.levels;
    if (e.kind == BkCancel && e.life >= 0.f)
      life[e.side].push_back(e.life);
  }
  for (int e = 0; e < 2; ++e)
    B.life[e] = median_of(life[e]);
  B.events = events.size();

  std::vector<size_t> ix(events.size());
  std::iota(ix.begin(), ix.end(), size_t{0});
  const size_t m = std::min(ix.size(), static_cast<size_t>(c.num("n")));
  std::partial_sort(ix.begin(), ix.begin() + static_cast<std::ptrdiff_t>(m), ix.end(), [&](size_t x, size_t y) { return events[x].amount > events[y].amount; });
  ix.resize(m);
  std::sort(ix.begin(), ix.end());
  for (const size_t i : ix) {
    const BookEvent &e = events[i];
    const BookSec &z = secs[static_cast<size_t>(std::clamp(trade_sec(e.clock) - s0 - 1, 0, n - 1))];
    B.top.push_back({e.clock, e.cs, e.kind, e.side, e.levels, e.price, e.first, e.amount, e.life, e.side == 0 ? z.bid : z.ask});
  }

  std::vector<std::pair<float, float>> bids, asks;
  B.at = at_clock;
  for (const auto &[p, a] : profile)
    if (a > 0.f)
      bids.emplace_back(p, a), B.book_bid += a;
    else
      asks.emplace_back(p, -a), B.book_ask -= a;
  std::reverse(bids.begin(), bids.end());
  bids.resize(std::min(bids.size(), kProfile)), asks.resize(std::min(asks.size(), kProfile));
  for (size_t i = asks.size(); i-- > 0;)
    B.profile.push_back({false, static_cast<int>(i) + 1, asks[i].first, asks[i].second});
  for (size_t i = 0; i < bids.size(); ++i)
    B.profile.push_back({true, static_cast<int>(i) + 1, bids[i].first, bids[i].second});
  return Done(std::move(res));
}

json SampleViewBook(Feed &p) {
  static const char *kWins[] = {"09:15-09:30", "09:30-09:50", "10:00-10:20", "13:00-13:15", "14:30-14:57", "14:50-15:00"};
  return {{"code", p.code()}, {"date", p.day()}, {"time", kWins[p.below(static_cast<int>(std::size(kWins)))]}, {"step", 0}, {"at", ""}, {"big", 0}, {"n", 12 + p.below(9)}};
}

} // namespace GUI::Agent
