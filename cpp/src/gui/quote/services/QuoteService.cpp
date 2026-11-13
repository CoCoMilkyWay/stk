#include "gui/quote/services/QuoteService.hpp"

#include "disk/Disk.hpp"
#include "gui/quote/PriceLib.hpp" // Handoff
#include "misc/exec.hpp"
#include "misc/profiler.hpp"

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cctype>
#include <chrono>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <iterator>
#include <utility>

namespace GUI::Quote {

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

// 缓存文件 = FileRec + n × AssetRec + n × kSlots × SlotRec (定长, 本机字节序); 头不符 = 别的格式 / 别的行情日, 弃.
//   AssetRec / SlotRec 各是一个原子条目: 整条写入, busy 先置后清 —— 读回 busy 的 = 写到一半进程没了
constexpr uint32_t kMagic = 0x484C5651; // "QVLH"
struct FileRec {
  uint32_t magic;
  misc::Ymd date;
  int32_t sec; // 末轮快照时刻
  uint32_t n;
};
struct AssetRec {
  std::array<char, 12> sym;
  int32_t lo, hi;
  double vol, amt;
  float high, low;
  uint8_t busy;
};
struct SlotRec {
  Cell bar;
  uint8_t has, exact, hole, busy;
};

size_t file_bytes(size_t n) { return sizeof(FileRec) + n * (sizeof(AssetRec) + kSlots * sizeof(SlotRec)); }
FileRec &file_rec(std::byte *m) { return *reinterpret_cast<FileRec *>(m); }
AssetRec &asset_rec(std::byte *m, uint32_t i) { return reinterpret_cast<AssetRec *>(m + sizeof(FileRec))[i]; }
SlotRec &slot_rec(std::byte *m, size_t n, uint32_t i, int s) {
  return reinterpret_cast<SlotRec *>(m + sizeof(FileRec) + n * sizeof(AssetRec))[static_cast<size_t>(i) * kSlots + static_cast<size_t>(s)];
}

template <class R>
void store(R &dst, R src) {
  std::atomic_ref<uint8_t>(dst.busy).store(1, std::memory_order_relaxed);
  std::atomic_signal_fence(std::memory_order_seq_cst);
  src.busy = 1;
  dst = src;
  std::atomic_ref<uint8_t>(dst.busy).store(0, std::memory_order_release);
}

// 全局轴码 "600000.SH" → 新浪 "sh600000"
std::string sina_sym(const std::string &code) {
  const size_t dot = code.find('.');
  assert(dot != std::string::npos);
  std::string s = code.substr(dot + 1) + code.substr(0, dot);
  std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return s;
}

// 快照的日根
Cell candle_of(const sina::Quote &q) { return cell(q.open, q.high, q.low, q.price, q.volume, q.amount); }

} // namespace

QuoteService::QuoteService(std::shared_ptr<const PoolAxis> axis, misc::Ymd fund_end, std::vector<misc::Ymd> trading)
    : axis_(std::move(axis)), trading_(std::move(trading)), fund_end_(fund_end), tracks_(axis_->size()),
      last_(std::make_shared<const std::vector<sina::Quote>>(axis_->size())) {
  assert(axis_->size() > 0 && !trading_.empty());
  syms_.reserve(axis_->size());
  index_.reserve(axis_->size());
  for (uint32_t i = 0; i < axis_->size(); ++i) {
    syms_.push_back(sina_sym(asset_axis().code(axis_->global(i))));
    [[maybe_unused]] const bool fresh = index_.emplace(syms_[i], i).second;
    assert(fresh && "QuoteService: 新浪码重复");
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
  wake_.notify_all();
  poll_.join();
  for (std::thread &t : fillers_)
    t.join();
  if (map_)
    munmap(map_, bytes_);
}

void QuoteService::select(uint32_t id) {
  const size_t i = id == ~0u ? axis_->size() : axis_->sub_of(id);
  {
    std::lock_guard<std::mutex> lock(mu_);
    sel_ = i < axis_->size() ? static_cast<uint32_t>(i) : ~0u;
    schedule();
    post();
  }
  cv_.notify_all();
}

void QuoteService::set_fund_end(misc::Ymd e) {
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (e == fund_end_)
      return;
    fund_end_ = e;
    schedule();
    post();
  }
  cv_.notify_all();
}

QuoteStatus QuoteService::status() const {
  std::lock_guard<std::mutex> lock(pub_mu_);
  return status_;
}

std::shared_ptr<const Live> QuoteService::live() const {
  std::lock_guard<std::mutex> lock(pub_mu_);
  return live_;
}

void QuoteService::post() {
  const bool fill = date_ > fund_end_;
  std::lock_guard<std::mutex> lock(pub_mu_);
  status_ = {date_, prev_, sec_, todo_.size() + inflight_, fetched_, fill ? gaps_ : 0, fill ? holes_ : 0, fill, bytes_, saved_};
}

void QuoteService::recount() {
  gaps_ = static_cast<size_t>(std::ranges::count_if(tracks_, [](const Track &t) { return t.gap(); }));
  holes_ = static_cast<size_t>(std::ranges::count_if(tracks_, [](const Track &t) { return t.bars.hole.any(); }));
}

void QuoteService::bind(const Handoff &h) {
  assert(!h.level.empty());
  std::vector<uint32_t> row(h.axis.size(), UINT32_MAX); // [库轴] h.mem 的行
  for (size_t k = 0; k < h.mem.size(); ++k)
    row[h.mem.id[k]] = static_cast<uint32_t>(k);
  Members m; // 资产 = 全市场
  for (size_t q = 0; q < axis_->size(); ++q) {
    if (const size_t a = h.axis.sub_of(axis_->global(q)); a < h.axis.size() && row[a] != UINT32_MAX)
      for (const Members::Weight &w : h.mem.of(row[a]))
        m.add(w.key, w.w);
    m.next(static_cast<uint32_t>(q));
  }
  {
    std::lock_guard<std::mutex> lock(mu_);
    basis_ = std::make_shared<const Basis>(Basis{std::move(m), h.level});
    dirty_ = true;
  }
  publish();
}

void QuoteService::publish() {
  std::lock_guard<std::mutex> hold(fold_mu_);
  auto y = std::make_shared<Live>();
  std::shared_ptr<const Basis> b;
  int slots = 0, freeze = 0, touched = kSlots;
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (!stale())
      return;
    dirty_ = false;
    y->date = date_, y->prev = prev_, y->sec = sec_, y->axis = axis_, y->quotes = last_;
    y->stocks.resize(tracks_.size());
    for (size_t i = 0; i < tracks_.size(); ++i) {
      const sina::Quote &q = (*last_)[i];
      if (!traded(q))
        continue;
      Track &t = tracks_[i];
      if (!t.day) {
        auto d = std::make_shared<Day>();
        d->prev = prev_;
        d->candle = {candle_of(q), date_, q.last_close};
        d->bars = t.bars;
        d->bars.date = date_;
        d->flow = net_flow(d->bars, d->candle.ref);
        t.day = std::move(d);
      }
      y->stocks[i] = t.day;
    }
    touched = std::exchange(touched_, kSlots);
    if ((b = basis_)) {
      slots = std::max(live_slot(), sec_slot(sec_)) + 1;
      freeze = std::min(settled() + 1, slots);
      if (date_ > fund_end_) // 待补的根还会被分钟 K 改写
        for (const Track &t : tracks_)
          if (t.lo <= t.hi)
            freeze = std::min(freeze, t.lo);
    }
  }
  if (b && !fold(*y, b, slots, freeze, touched))
    return;
  std::lock_guard<std::mutex> lock(pub_mu_);
  y->ver = ++ver_;
  live_ = std::move(y);
}

bool QuoteService::fold(Live &y, const std::shared_ptr<const Basis> &b, int slots, int freeze, int touched) {
  const Members &M = b->members;
  const uint32_t n = static_cast<uint32_t>(M.size());
  const std::vector<sina::Quote> &qs = *y.quotes;
  Folding &g = folding_;
  if (g.basis != b || g.date != y.date)
    g = {b, y.date, std::make_unique<Fold>(b->level.size()), std::vector<Fold::Carry>(n), 0};
  if (touched < g.frozen || freeze < g.frozen)
    g.frozen = 0;
  enum { kDone,
         kRebase,
         kGone };
  const auto pass = [&](int from) {
    g.fold->open(from, freeze, slots);
    for (uint32_t k0 = 0; k0 < n; k0 += kFoldChunk) {
      std::lock_guard<std::mutex> lock(mu_);
      if (date_ != y.date)
        return kGone;
      for (uint32_t k = k0, k1 = std::min(k0 + kFoldChunk, n); k < k1; ++k) {
        const uint32_t i = M.id[k];
        const sina::Quote &q = qs[i];
        const Cell c = candle_of(q);
        if (!g.fold->add({q.last_close, traded(q) ? &c : nullptr, &tracks_[i].bars}, M.of(k), g.carry[k]))
          return kRebase;
      }
    }
    return kDone;
  };
  auto r = pass(g.frozen);
  if (r == kRebase)
    r = pass(0);
  if (r == kGone) {
    g = {};
    return false;
  }
  assert(r == kDone);
  g.frozen = freeze;
  auto day = std::make_shared<std::vector<Day>>(b->level.size());
  g.fold->finish(y.date, y.prev, *day);
  y.baskets.resize(day->size());
  for (size_t k = 0; k < day->size(); ++k)
    if (Day &d = (*day)[k]; d.candle.date) {
      rebase(d, b->level[k]);
      y.baskets[k] = std::shared_ptr<const Day>(day, &d);
    }
  return true;
}

// ---------------------------------------------------------------------------
// 快照
// ---------------------------------------------------------------------------
void QuoteService::poll() {
  TraceThread("SinaPoll");
  std::vector<std::vector<std::string>> chunks; // 一批一路: 各自一条连接
  for (size_t a = 0; a < syms_.size(); a += sina::kQuoteMax)
    chunks.emplace_back(syms_.begin() + static_cast<ptrdiff_t>(a),
                        syms_.begin() + static_cast<ptrdiff_t>(std::min(syms_.size(), a + sina::kQuoteMax)));
  const int batches = static_cast<int>(chunks.size());
  std::deque<sina::Client> clients;
  for (int k = 0; k < batches; ++k)
    clients.emplace_back(sina::kQuoteHost);
  misc::exec::ForkJoin ex(batches);
  std::vector<std::vector<sina::Quote>> parts(chunks.size());
  std::vector<sina::Quote> qs;
  while (true) {
    const auto t0 = std::chrono::steady_clock::now();
    ex.tasks(batches, [&](int k, int) { parts[static_cast<size_t>(k)] = clients[static_cast<size_t>(k)].quotes(chunks[static_cast<size_t>(k)]); });
    qs.clear();
    for (std::vector<sina::Quote> &part : parts)
      std::move(part.begin(), part.end(), std::back_inserter(qs));
    misc::Ymd d = 0;
    for (const sina::Quote &q : qs)
      d = std::max(d, q.date);
    assert(d > 0 && "sina: 全市场快照一只都没回");
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
        dirty_ = true;
        load();
      }
      int cur = 0;
      for (const sina::Quote &q : qs)
        if (q.date == date_)
          cur = std::max(cur, q.sec);
      const int gap = sec_ > 0 && sec_slot(cur) != sec_slot(sec_) && trading_sec(cur) - trading_sec(sec_) > kGap ? sec_slot(sec_) : -1;
      auto last = std::make_shared<std::vector<sina::Quote>>(*last_);
      for (const sina::Quote &q : qs) {
        const uint32_t i = index_.at(q.sym);
        sina::Quote &o = (*last)[i];
        dirty_ = dirty_ || o.date != q.date || o.sec != q.sec; // 盘口随快照时刻
        o = q;
        if (q.date != date_) // 停牌等: 只留快照
          continue;
        sec_ = std::max(sec_, q.sec);
        const bool first = tracks_[i].vol < 0;
        bool changed = false;
        touch(i, [&](Track &t) { return changed = t.absorb(q, gap); });
        if (changed) {
          const int s = sec_slot(q.sec), from = first ? 0 : gap >= 0 ? gap
                                                                     : s; // 改到的根: 首快照 / 断档的缺口起
          for (int k = from; k <= s; ++k)
            put(i, k);
          put(i);
          dirty_ = true;
          touched_ = std::min(touched_, from);
        }
      }
      last_ = std::move(last);
      std::atomic_ref<int32_t>(file_rec(map_).sec).store(sec_, std::memory_order_release); // 本轮各只落定之后
      schedule();
      post();
    }
    cv_.notify_all();
    publish();
    // 到下一轮快照之前: 补根改了当日 → 距上次发布满 kFoldGap 即发布
    const auto due = t0 + std::chrono::seconds(in_session(clock_now()) ? kTick : kIdleTick);
    std::unique_lock<std::mutex> lock(mu_);
    while (true) {
      wake_.wait_until(lock, std::min(due, std::chrono::steady_clock::now() + std::chrono::seconds(kFoldGap)), [&] { return stop_; });
      if (stop_)
        return;
      if (!wake_.wait_until(lock, due, [&] { return stop_ || stale(); }))
        break;
      if (stop_)
        return;
      lock.unlock();
      publish();
      lock.lock();
    }
  }
}

void QuoteService::Track::hole(int l, int h) {
  if (l > h)
    return;
  mark(l, h);
  for (int s = l; s <= h; ++s)
    if (!exact[static_cast<size_t>(s)])
      bars.hole.set(static_cast<size_t>(s)), bars.has.reset(static_cast<size_t>(s));
}

bool QuoteService::Track::holed(int done) const {
  for (int s = lo; s <= std::min(hi, done); ++s)
    if (bars.hole[static_cast<size_t>(s)])
      return true;
  return false;
}

bool QuoteService::Track::absorb(const sina::Quote &q, int gap) {
  const int s = sec_slot(q.sec);
  if (vol < 0) { // 首快照: 基线. 已有成交 ⇒ 所在根以前快照没盯住 (缺口); 所在根暂写现价 (量 0)
    vol = q.volume, amt = q.amount, high = q.high, low = q.low;
    if (q.volume > 0) {
      mark(0, s);
      hole(0, s - 1);
      if (q.price > 0)
        bars.add(s, {q.price, q.price, q.price, q.price, 0, 0});
    }
    return true;
  }
  const double dv = q.volume - vol, da = q.amount - amt;
  if (dv > 0 && gap >= 0) {
    mark(gap, s);
    hole(gap + 1, s - 1);
  }
  if (dv > 0 && q.price > 0 && q.sec <= kCloseSec) {
    if (!exact[static_cast<size_t>(s)] && !bars.hole[static_cast<size_t>(s)]) {
      float h = q.price, l = q.price;
      if (q.high > high)
        h = std::max(h, q.high);
      if (q.low > 0 && (low <= 0 || q.low < low))
        l = std::min(l, q.low);
      bars.add(s, {q.price, h, l, q.price, dv, da});
    }
  }
  vol = q.volume, amt = q.amount, high = q.high, low = q.low;
  return dv != 0;
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
  return c.today != date_ ? kSlots - 1 : sec_slot(c.sec);
}

void QuoteService::schedule() {
  if (date_ <= fund_end_)
    return;
  const int st = settled(), dn = done();
  if (sel_ != ~0u && tracks_[sel_].vol >= 0) { // 选中: 定稿部分补成精确
    const Track &t = tracks_[sel_];
    int s = 0;
    while (s <= st && t.exact[s])
      ++s;
    if (s <= st) {
      touch(sel_, [&](Track &u) { return u.mark(s, st), false; });
      put(sel_);
    }
  }
  for (uint32_t i = 0; i < tracks_.size(); ++i) { // 有缺口的排在补精确的之前
    Track &t = tracks_[i];
    if (t.fill != Fill::Idle || t.lo > t.hi)
      continue;
    const bool holed = t.holed(dn);
    if (holed || (i == sel_ ? t.lo <= st : t.hi <= st || t.lo + kLag <= st)) {
      t.fill = Fill::Queued;
      holed ? todo_.push_front(i) : todo_.push_back(i);
    }
  }
  if (sel_ != ~0u && tracks_[sel_].fill == Fill::Queued && todo_.front() != sel_) { // 选中插队
    todo_.erase(std::find(todo_.begin(), todo_.end(), sel_));
    todo_.push_front(sel_);
  }
}

void QuoteService::fill() {
  TraceThread("SinaKline");
  sina::Client c(sina::kBarHost);
  while (true) {
    uint32_t i;
    int lo, live, n;
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
      live = live_slot();
      n = live - lo + 2; // [lo, 此刻所在根] 至多这么多根 (+1 防取的途中跨分钟)
      assert(n >= 2);
      gen = gen_;
      ++inflight_;
      post();
    }
    const std::vector<sina::Bar> bars = c.kline(syms_[i], n);
    std::lock_guard<std::mutex> lock(mu_);
    --inflight_;
    if (gen != gen_) { // 取的途中换了日 (Track 已重置)
      post();
      continue;
    }
    tracks_[i].fill = Fill::Idle;
    const int st = settled();
    int end = 0;
    touch(i, [&](Track &t) { return end = t.merge(lo, st, live, date_, bars), true; });
    for (int s = lo; s <= end; ++s)
      put(i, s);
    put(i);
    ++fetched_;
    dirty_ = true;
    touched_ = std::min(touched_, lo);
    post();
    wake_.notify_one();
  }
}

int QuoteService::Track::merge(int l, int settled, int live, misc::Ymd date, const std::vector<sina::Bar> &got) {
  const int end = std::max(settled, std::min(hi, live - 1)); // 暂写段 (settled, end]: 待补区间内取时已走完的根 (l > settled = 全段暂写)
  for (int s = l; s <= end; ++s)
    bars.has.reset(static_cast<size_t>(s)), bars.hole.reset(static_cast<size_t>(s));
  for (int s = l; s <= settled; ++s)
    exact.set(static_cast<size_t>(s));
  for (const sina::Bar &b : got) {
    if (b.date != date)
      continue;
    const int s = label_slot(b.minute);
    assert(s >= 0 && "sina: 分钟 K 标号不在交易时段");
    if (s >= l && s <= end)
      bars.add(s, {b.open, b.high, b.low, b.close, b.volume, b.amount});
  }
  if (lo >= l)
    lo = std::max(lo, settled + 1);
  if (lo > hi)
    lo = kSlots, hi = -1;
  return end;
}

// ---------------------------------------------------------------------------
// 落盘
// ---------------------------------------------------------------------------
void QuoteService::put(uint32_t i) {
  const Track &t = tracks_[i];
  AssetRec a{};
  assert(syms_[i].size() < a.sym.size());
  std::copy(syms_[i].begin(), syms_[i].end(), a.sym.begin());
  a.lo = t.lo, a.hi = t.hi, a.vol = t.vol, a.amt = t.amt, a.high = t.high, a.low = t.low;
  store(asset_rec(map_, i), a);
  saved_ = std::time(nullptr);
}

void QuoteService::put(uint32_t i, int s) {
  const Track &t = tracks_[i];
  const size_t k = static_cast<size_t>(s);
  store(slot_rec(map_, tracks_.size(), i, s), SlotRec{t.bars.cell[k], t.bars.has[k], t.exact[k], t.bars.hole[k], 0});
}

void QuoteService::load() {
  if (map_)
    munmap(map_, bytes_);
  if (disk::exists(disk::QuoteLive)) {
    const std::string blob = disk::read(disk::QuoteLive);
    FileRec f{};
    std::memcpy(&f, blob.data(), std::min(blob.size(), sizeof(f)));
    if (f.magic == kMagic && f.date == date_) {
      assert(blob.size() == file_bytes(f.n) && "sina 缓存: 文件长度与只数不符");
      const char *as = blob.data() + sizeof(FileRec), *ss = as + f.n * sizeof(AssetRec);
      for (uint32_t k = 0; k < f.n; ++k) {
        AssetRec a;
        std::memcpy(&a, as + k * sizeof(a), sizeof(a));
        const auto it = index_.find(std::string(a.sym.data(), strnlen(a.sym.data(), a.sym.size())));
        if (a.busy || it == index_.end()) // 写到一半 (该只从零) / 不在本次全市场 (已退市)
          continue;
        Track &t = tracks_[it->second];
        t.lo = a.lo, t.hi = a.hi, t.vol = a.vol, t.amt = a.amt, t.high = a.high, t.low = a.low;
        for (int s = 0; s < kSlots; ++s) {
          SlotRec r;
          std::memcpy(&r, ss + (static_cast<size_t>(k) * kSlots + static_cast<size_t>(s)) * sizeof(r), sizeof(r));
          if (r.busy || r.hole) { // 缺口照并入待补 (格先于状态落盘, 状态里的区间可能还没盖到它)
            t.hole(s, s);
            continue;
          }
          t.bars.has[static_cast<size_t>(s)] = r.has;
          t.bars.cell[static_cast<size_t>(s)] = r.bar;
          t.exact[static_cast<size_t>(s)] = r.exact;
        }
      }
      sec_ = f.sec;
    }
  }
  recount();
  // 重写成本次布局: 临时文件建满再 rename, 映射随 inode 留用
  const uint32_t n = static_cast<uint32_t>(tracks_.size());
  bytes_ = file_bytes(n);
  const std::filesystem::path p = disk::path(disk::QuoteLive);
  std::filesystem::path tmp = p;
  tmp += ".tmp";
  std::filesystem::create_directories(p.parent_path());
  const int fd = ::open(tmp.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
  assert(fd >= 0 && "sina 缓存: 临时文件建不出来");
  [[maybe_unused]] const int sized = ftruncate(fd, static_cast<off_t>(bytes_));
  assert(sized == 0 && "sina 缓存: 定长扩不出来");
  void *m = mmap(nullptr, bytes_, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  assert(m != MAP_FAILED && "sina 缓存: 映射失败");
  ::close(fd);
  map_ = static_cast<std::byte *>(m);
  file_rec(map_) = {kMagic, date_, sec_, n};
  for (uint32_t i = 0; i < n; ++i) {
    for (int s = 0; s < kSlots; ++s)
      put(i, s);
    put(i);
  }
  std::filesystem::rename(tmp, p);
}

} // namespace GUI::Quote