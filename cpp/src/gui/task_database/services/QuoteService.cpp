#include "gui/task_database/services/QuoteService.hpp"

#include "misc/fs.hpp"
#include "misc/profiler.hpp"
#include "shared/Config.hpp"

#include <algorithm>
#include <cassert>
#include <cctype>
#include <chrono>
#include <cstring>
#include <ctime>
#include <fstream>
#include <iterator>
#include <type_traits>

namespace GUI::Database {

namespace {

constexpr int kCloseSec = 15 * 3600 + 60; // 此后的成交 (盘后固定价格) 不进分钟 K

struct Clock {
  misc::Ymd today;
  int sec;     // 当日秒
  int weekday; // Mon=0 .. Sun=6
};

Clock clock_now() {
  const std::time_t t = std::time(nullptr);
  std::tm tm{};
  localtime_r(&t, &tm);
  return {(tm.tm_year + 1900) * 10000 + (tm.tm_mon + 1) * 100 + tm.tm_mday, tm.tm_hour * 3600 + tm.tm_min * 60 + tm.tm_sec,
          (tm.tm_wday + 6) % 7};
}

// 快照时刻 → 所在根: 竞价并入首根, 午休并入 11:30, 收盘后并入 15:00
int slot_at(int sec) {
  const int m = sec / 60;
  return m < 9 * 60 + 30 ? 0 : m < 11 * 60 + 30 ? m - (9 * 60 + 30)
                           : m < 13 * 60        ? 119
                           : m < 15 * 60        ? 120 + m - 13 * 60
                                                : kSlots - 1;
}

// 截至 sec 已过的连续竞价时长 [s] (断档判据: 午休 / 盘前盘后的停顿不算)
int trading_sec(int sec) {
  return std::clamp(sec, 9 * 3600 + 30 * 60, 11 * 3600 + 30 * 60) - (9 * 3600 + 30 * 60) +
         std::clamp(sec, 13 * 3600, 15 * 3600) - 13 * 3600;
}

// 交易所出新快照的时段 (竞价起, 收盘后留余量收尾)
bool in_session(const Clock &c) {
  return c.weekday < 5 && ((c.sec >= 9 * 3600 + 14 * 60 + 30 && c.sec < 11 * 3600 + 31 * 60) ||
                           (c.sec >= 12 * 3600 + 59 * 60 && c.sec <= 15 * 3600 + 90));
}

// 缓存文件 = Head + n × (Rec + nbars × Cell (槽升序, 只存有成交的根)); 本机字节序
constexpr uint32_t kMagic = 0x4B4C4E53; // "SNLK"
struct Head {
  uint32_t magic;
  misc::Ymd date;
  int32_t sec;
  uint32_t n;
};
struct Rec {
  char sym[12];
  double vol, amt;
  float high, low;
  int16_t lo, hi;
  uint16_t nbars;
  uint64_t has[4], exact[4];
};

void pack(const std::bitset<kSlots> &b, uint64_t (&w)[4]) {
  for (int s = 0; s < kSlots; ++s)
    w[s / 64] |= static_cast<uint64_t>(b[s]) << (s % 64);
}
void unpack(const uint64_t (&w)[4], std::bitset<kSlots> &b) {
  for (int s = 0; s < kSlots; ++s)
    b[s] = w[s / 64] >> (s % 64) & 1;
}

// 全局轴码 "600000.SH" → 新浪 "sh600000"
std::string sina_sym(const std::string &code) {
  const size_t dot = code.find('.');
  assert(dot != std::string::npos);
  std::string s = code.substr(dot + 1) + code.substr(0, dot);
  std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return s;
}

// 快照的日根: 开 / 高 / 低缺 (0) → 现价
Cell candle_of(const sina::Quote &q) {
  return {q.open > 0 ? q.open : q.price, q.high > 0 ? q.high : q.price, q.low > 0 ? q.low : q.price, q.price, q.volume, q.amount};
}

} // namespace

QuoteService::QuoteService(std::vector<std::string> syms, misc::Ymd fund_end, std::vector<misc::Ymd> trading)
    : syms_(std::move(syms)), fund_end_(fund_end), trading_(std::move(trading)), tracks_(syms_.size()), last_(syms_.size()) {
  assert(!syms_.empty() && !trading_.empty());
  index_.reserve(syms_.size());
  for (uint32_t i = 0; i < syms_.size(); ++i) {
    [[maybe_unused]] const bool fresh = index_.emplace(syms_[i], i).second;
    assert(fresh && "QuoteService: 代码重复");
  }
  poll_ = std::thread(&QuoteService::poll, this);
  fillers_.reserve(kFillers);
  for (int k = 0; k < kFillers; ++k)
    fillers_.emplace_back(&QuoteService::fill, this);
}

QuoteService::~QuoteService() {
  {
    std::lock_guard<std::mutex> lock(mu_);
    stop_ = true;
  }
  cv_.notify_all();
  poll_.join();
  for (std::thread &t : fillers_)
    t.join();
  if (changed_) {
    const std::string blob = serialize();
    misc::atomic_write(::config::SINA_CACHE, blob.data(), blob.size());
  }
}

uint32_t QuoteService::find(const std::string &code) const {
  const auto it = index_.find(sina_sym(code));
  return it == index_.end() ? ~0u : it->second;
}

void QuoteService::select(uint32_t i) {
  {
    std::lock_guard<std::mutex> lock(mu_);
    assert(i == ~0u || i < syms_.size());
    sel_ = i;
    schedule();
  }
  cv_.notify_all();
}

QuoteStatus QuoteService::status() const {
  std::lock_guard<std::mutex> lock(mu_);
  return {date_, sec_, todo_.size() + inflight_, fetched_, date_ > fund_end_};
}

void QuoteService::today(uint32_t i, Day &out, sina::Quote &q) const {
  std::lock_guard<std::mutex> lock(mu_);
  q = last_[i];
  out.date = 0;
  if (!traded(q))
    return;
  out.date = date_, out.prev = prev_, out.ref = q.last_close;
  out.candle = {candle_of(q), date_, 0};
  out.bars = tracks_[i].bars;
  out.bars.date = date_;
}

void QuoteService::today(const Members &m, std::span<const double> level, std::span<Day> out) const {
  assert(m.assets() == syms_.size());
  Fold fold(out.size());
  fold.open(true);
  std::lock_guard<std::mutex> lock(mu_);
  for (uint32_t i = 0; i < syms_.size(); ++i) {
    const sina::Quote &q = last_[i];
    const Cell k = candle_of(q);
    fold.add({q.last_close, traded(q) ? &k : nullptr, &tracks_[i].bars}, m.of(i));
  }
  fold.finish(date_, prev_, level, out);
}

// ---------------------------------------------------------------------------
// 快照
// ---------------------------------------------------------------------------
void QuoteService::poll() {
  TraceThread("SinaPoll");
  sina::Client c(::config::SINA_HQ_HOST);
  std::vector<std::string> chunk;
  std::vector<sina::Quote> qs;
  std::string blob;
  auto saved = std::chrono::steady_clock::now();
  while (true) {
    const auto t0 = std::chrono::steady_clock::now();
    qs.clear();
    for (size_t a = 0; a < syms_.size(); a += sina::kQuoteMax) {
      chunk.assign(syms_.begin() + a, syms_.begin() + std::min(syms_.size(), a + sina::kQuoteMax));
      std::vector<sina::Quote> part = c.quotes(chunk);
      std::move(part.begin(), part.end(), std::back_inserter(qs));
    }
    misc::Ymd d = 0;
    for (const sina::Quote &q : qs)
      d = std::max(d, q.date);
    {
      std::unique_lock<std::mutex> lock(mu_);
      if (stop_)
        return;
      if (d > date_) { // 换日 (含启动首轮): 从零, 盘上有同日缓存则续上
        const auto p = std::lower_bound(trading_.begin(), trading_.end(), d);
        assert(p != trading_.begin() && "交易日历不早于行情日");
        date_ = d;
        prev_ = *std::prev(p);
        ++gen_;
        for (Track &t : tracks_)
          t = Track{};
        todo_.clear();
        sec_ = 0;
        load();
      }
      int cur = 0;
      for (const sina::Quote &q : qs)
        if (q.date == date_)
          cur = std::max(cur, q.sec);
      const int gap = sec_ > 0 && trading_sec(cur) - trading_sec(sec_) > kGap ? slot_at(sec_) : -1;
      for (const sina::Quote &q : qs)
        absorb(index_.at(q.sym), q, gap);
      schedule();
      if (changed_ && t0 - saved >= std::chrono::seconds(kSave)) {
        blob = serialize();
        changed_ = false;
        saved = t0;
      }
    }
    cv_.notify_all();
    if (!blob.empty()) {
      misc::atomic_write(::config::SINA_CACHE, blob.data(), blob.size());
      blob.clear();
    }
    std::unique_lock<std::mutex> lock(mu_);
    cv_.wait_until(lock, t0 + std::chrono::seconds(in_session(clock_now()) ? kTick : kIdleTick), [&] { return stop_; });
    if (stop_)
      return;
  }
}

void QuoteService::absorb(uint32_t i, const sina::Quote &q, int gap) {
  last_[i] = q;
  if (q.date != date_) // 停牌等: 只留快照
    return;
  sec_ = std::max(sec_, q.sec);
  Track &t = tracks_[i];
  if (t.vol < 0) { // 首快照: 基线. 已有成交 ⇒ 所在根及以前快照没盯住
    t.vol = q.volume;
    t.amt = q.amount;
    t.high = q.high;
    t.low = q.low;
    if (q.volume > 0)
      mark(t, 0, slot_at(q.sec));
    changed_ = true;
    return;
  }
  const double dv = q.volume - t.vol, da = q.amount - t.amt;
  changed_ = changed_ || dv != 0;
  if (dv > 0 && gap >= 0)
    mark(t, gap, slot_at(q.sec));
  if (dv > 0 && q.price > 0 && q.sec <= kCloseSec) {
    const int s = slot_at(q.sec);
    if (!t.exact[s]) {
      float hi = q.price, lo = q.price;
      if (q.high > t.high)
        hi = std::max(hi, q.high);
      if (q.low > 0 && (t.low <= 0 || q.low < t.low))
        lo = std::min(lo, q.low);
      t.bars.add(s, {q.price, hi, lo, q.price, dv, da});
    }
  }
  t.vol = q.volume;
  t.amt = q.amount;
  t.high = q.high;
  t.low = q.low;
}

// ---------------------------------------------------------------------------
// 分钟 K
// ---------------------------------------------------------------------------
int QuoteService::settled() const {
  const Clock c = clock_now();
  if (c.today != date_)
    return kSlots - 1;
  int s = kSlots - 1;
  while (s >= 0 && slot_label(s) * 60 + kSettle > c.sec)
    --s;
  return s;
}

int QuoteService::live_slot() const {
  const Clock c = clock_now();
  return c.today != date_ ? kSlots - 1 : slot_at(c.sec);
}

void QuoteService::schedule() {
  if (date_ <= fund_end_)
    return;
  const int st = settled();
  if (sel_ != ~0u && tracks_[sel_].vol >= 0) { // 选中: 定稿部分补成精确, 插队
    Track &t = tracks_[sel_];
    int s = 0;
    while (s <= st && t.exact[s])
      ++s;
    if (s <= st)
      mark(t, s, st);
    if (t.fill == Fill::Queued && todo_.front() != sel_) {
      todo_.erase(std::find(todo_.begin(), todo_.end(), sel_));
      todo_.push_front(sel_);
    }
  }
  for (uint32_t i = 0; i < tracks_.size(); ++i) {
    Track &t = tracks_[i];
    if (t.fill == Fill::Idle && t.lo <= t.hi && (i == sel_ ? t.lo <= st : t.hi <= st)) {
      t.fill = Fill::Queued;
      i == sel_ ? todo_.push_front(i) : todo_.push_back(i);
    }
  }
}

void QuoteService::fill() {
  TraceThread("SinaKline");
  sina::Client c(::config::SINA_KLINE_HOST);
  while (true) {
    uint32_t i;
    int lo, n;
    uint64_t gen;
    {
      std::unique_lock<std::mutex> lock(mu_);
      cv_.wait(lock, [&] { return stop_ || !todo_.empty(); });
      if (stop_)
        return;
      i = todo_.front();
      todo_.pop_front();
      Track &t = tracks_[i];
      t.fill = Fill::Busy;
      lo = t.lo;
      n = live_slot() - lo + 2; // [lo, 此刻所在根] 至多这么多根 (+1 防取的途中跨分钟)
      assert(n >= 2);
      gen = gen_;
      ++inflight_;
    }
    const std::vector<sina::Bar> bars = c.kline(syms_[i], n);
    std::lock_guard<std::mutex> lock(mu_);
    --inflight_;
    if (gen != gen_) // 取的途中换了日 (Track 已重置)
      continue;
    tracks_[i].fill = Fill::Idle;
    merge(i, lo, bars, settled());
  }
}

void QuoteService::merge(uint32_t i, int lo, const std::vector<sina::Bar> &bars, int settled) {
  Track &t = tracks_[i];
  assert(lo <= settled);
  for (int s = lo; s <= settled; ++s) {
    t.bars.has.reset(s);
    t.exact.set(s);
  }
  for (const sina::Bar &b : bars) {
    if (b.date != date_)
      continue;
    const int s = label_slot(b.minute);
    assert(s >= 0 && "sina: 分钟 K 标号不在交易时段");
    if (s >= lo && s <= settled)
      t.bars.add(s, {b.open, b.high, b.low, b.close, b.volume, b.amount});
  }
  if (t.lo >= lo)
    t.lo = settled + 1;
  if (t.lo > t.hi)
    t.lo = kSlots, t.hi = -1;
  ++fetched_;
  changed_ = true;
}

// ---------------------------------------------------------------------------
// 落盘
// ---------------------------------------------------------------------------
std::string QuoteService::serialize() const {
  static_assert(std::is_trivially_copyable_v<Cell>);
  Head h{kMagic, date_, sec_, 0};
  std::string out(sizeof(Head), '\0');
  for (uint32_t i = 0; i < tracks_.size(); ++i) {
    const Track &t = tracks_[i];
    if (t.vol < 0)
      continue;
    Rec r{};
    assert(syms_[i].size() < sizeof(r.sym));
    std::memcpy(r.sym, syms_[i].data(), syms_[i].size());
    r.vol = t.vol, r.amt = t.amt, r.high = t.high, r.low = t.low;
    r.lo = static_cast<int16_t>(t.lo), r.hi = static_cast<int16_t>(t.hi);
    r.nbars = static_cast<uint16_t>(t.bars.has.count());
    pack(t.bars.has, r.has);
    pack(t.exact, r.exact);
    out.append(reinterpret_cast<const char *>(&r), sizeof(r));
    for (int s = 0; s < kSlots; ++s)
      if (t.bars.has[s])
        out.append(reinterpret_cast<const char *>(&t.bars.cell[s]), sizeof(Cell));
    ++h.n;
  }
  std::memcpy(out.data(), &h, sizeof(h));
  return out;
}

void QuoteService::load() {
  std::ifstream f(::config::SINA_CACHE, std::ios::binary);
  if (!f)
    return;
  const std::string blob((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
  assert(blob.size() >= sizeof(Head) && "sina 缓存: 文件截断");
  Head h;
  std::memcpy(&h, blob.data(), sizeof(h));
  assert(h.magic == kMagic && "sina 缓存: 文件头不符");
  if (h.date != date_)
    return;
  size_t at = sizeof(Head);
  for (uint32_t k = 0; k < h.n; ++k) {
    assert(at + sizeof(Rec) <= blob.size() && "sina 缓存: 文件截断");
    Rec r;
    std::memcpy(&r, blob.data() + at, sizeof(r));
    at += sizeof(r);
    assert(at + r.nbars * sizeof(Cell) <= blob.size() && "sina 缓存: 文件截断");
    const auto it = index_.find(std::string(r.sym, strnlen(r.sym, sizeof(r.sym))));
    if (it != index_.end()) { // 不在本次全市场的 (已退市) 跳过
      Track &t = tracks_[it->second];
      t.vol = r.vol, t.amt = r.amt, t.high = r.high, t.low = r.low;
      t.lo = r.lo, t.hi = r.hi;
      unpack(r.has, t.bars.has);
      unpack(r.exact, t.exact);
      assert(t.bars.has.count() == r.nbars);
      size_t c = at;
      for (int s = 0; s < kSlots; ++s)
        if (t.bars.has[s]) {
          std::memcpy(&t.bars.cell[s], blob.data() + c, sizeof(Cell));
          c += sizeof(Cell);
        }
    }
    at += r.nbars * sizeof(Cell);
  }
  assert(at == blob.size() && "sina 缓存: 文件尾有多余字节");
  sec_ = h.sec;
}

} // namespace GUI::Database
