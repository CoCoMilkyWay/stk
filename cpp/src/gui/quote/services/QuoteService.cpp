#include "gui/quote/services/QuoteService.hpp"

#include "disk/Disk.hpp"
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
constexpr uint32_t kMagic = 0x56494C51; // "QLIV"
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
  uint8_t has, exact, busy;
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

QuoteService::QuoteService(PoolAxis axis, misc::Ymd fund_end, std::vector<misc::Ymd> trading)
    : axis_(std::move(axis)), fund_end_(fund_end), trading_(std::move(trading)), tracks_(axis_.size()), last_(axis_.size()) {
  assert(axis_.size() > 0 && !trading_.empty());
  syms_.reserve(axis_.size());
  index_.reserve(axis_.size());
  for (uint32_t i = 0; i < axis_.size(); ++i) {
    syms_.push_back(sina_sym(asset_axis().code(axis_.global(i))));
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
  poll_.join();
  for (std::thread &t : fillers_)
    t.join();
  if (map_)
    munmap(map_, bytes_);
}

void QuoteService::select(uint32_t id) {
  const size_t i = id == ~0u ? axis_.size() : axis_.sub_of(id);
  {
    std::lock_guard<std::mutex> lock(mu_);
    sel_ = i < axis_.size() ? static_cast<uint32_t>(i) : ~0u;
    schedule();
  }
  cv_.notify_all();
}

QuoteStatus QuoteService::status() const {
  std::lock_guard<std::mutex> lock(mu_);
  const bool fill = date_ > fund_end_;
  const size_t gaps = fill ? static_cast<size_t>(std::ranges::count_if(tracks_, [](const Track &t) { return t.lo <= t.hi; })) : 0;
  return {date_, prev_, sec_, todo_.size() + inflight_, fetched_, gaps, fill, ver_, bytes_, saved_};
}

void QuoteService::bind(Members m, std::vector<double> level) {
  assert((m.id.empty() || m.id.back() < axis_.size()) && !level.empty());
  {
    std::lock_guard<std::mutex> lock(mu_);
    basis_ = std::make_shared<const Basis>(Basis{std::move(m), std::move(level)});
    dirty_ = true;
  }
  fold();
}

std::shared_ptr<const std::vector<Day>> QuoteService::today(Side s, std::span<const uint32_t> keys) const {
  std::lock_guard<std::mutex> lock(mu_);
  if (s == kBasket) {
    for (size_t k = 0; k < keys.size(); ++k)
      assert(keys[k] == k && "QuoteService::today: 篮子键集须是全表");
    if (!baskets_)
      return std::make_shared<const std::vector<Day>>(keys.size());
    assert(baskets_->size() == keys.size() && "QuoteService::today: 篮子键集与成员表的篮子数不符");
    return baskets_;
  }
  auto out = std::make_shared<std::vector<Day>>(keys.size());
  for (size_t k = 0; k < keys.size(); ++k) {
    Day &o = (*out)[k];
    const size_t i = axis_.sub_of(keys[k]);
    if (i == axis_.size() || !traded(last_[i]))
      continue;
    o.prev = prev_;
    o.candle = {candle_of(last_[i]), date_, last_[i].last_close};
    o.bars = tracks_[i].bars;
    o.bars.date = date_;
    o.flow = net_flow(o.bars, o.candle.ref);
  }
  return out;
}

std::shared_ptr<const std::vector<Candle>> QuoteService::candles(std::span<const uint32_t> keys) const {
  auto out = std::make_shared<std::vector<Candle>>(keys.size());
  std::lock_guard<std::mutex> lock(mu_);
  for (size_t k = 0; k < keys.size(); ++k) {
    const size_t i = axis_.sub_of(keys[k]);
    if (i != axis_.size() && traded(last_[i]))
      (*out)[k] = {candle_of(last_[i]), date_, last_[i].last_close};
  }
  return out;
}

sina::Quote QuoteService::book(uint32_t id) const {
  const size_t i = axis_.sub_of(id);
  if (i == axis_.size())
    return {};
  std::lock_guard<std::mutex> lock(mu_);
  return last_[i];
}

void QuoteService::fold() {
  std::lock_guard<std::mutex> hold(fold_mu_);
  std::shared_ptr<const Basis> b;
  misc::Ymd date, prev;
  int slots, freeze, touched;
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (!basis_ || date_ == 0 || !dirty_)
      return;
    dirty_ = false;
    b = basis_, date = date_, prev = prev_;
    slots = std::max(live_slot(), sec_slot(sec_)) + 1;
    freeze = std::min(settled() + 1, slots);
    if (date_ > fund_end_) // 待补的根还会被分钟 K 改写
      for (const Track &t : tracks_)
        if (t.lo <= t.hi)
          freeze = std::min(freeze, t.lo);
    touched = std::exchange(touched_, kSlots);
  }
  const Members &M = b->members;
  const uint32_t n = static_cast<uint32_t>(M.size());
  Folding &g = folding_;
  if (g.basis != b || g.date != date)
    g = {b, date, std::make_unique<Fold>(b->level.size()), std::vector<Fold::Carry>(n), 0};
  if (touched < g.frozen || freeze < g.frozen)
    g.frozen = 0;
  enum { kDone,
         kRebase,
         kGone };
  const auto pass = [&](int from) {
    g.fold->open(from, freeze, slots);
    for (uint32_t k0 = 0; k0 < n; k0 += kFoldChunk) {
      std::lock_guard<std::mutex> lock(mu_);
      if (date_ != date)
        return kGone;
      for (uint32_t k = k0, k1 = std::min(k0 + kFoldChunk, n); k < k1; ++k) {
        const uint32_t i = M.id[k];
        const sina::Quote &q = last_[i];
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
    return;
  }
  assert(r == kDone);
  g.frozen = freeze;
  auto out = std::make_shared<std::vector<Day>>(b->level.size());
  g.fold->finish(date, prev, *out);
  for (size_t k = 0; k < out->size(); ++k)
    if (Day &y = (*out)[k]; y.candle.date)
      rebase(y, b->level[k]);
  std::lock_guard<std::mutex> lock(mu_);
  if (date_ != date || basis_ != b)
    return;
  baskets_ = std::move(out);
  ++ver_[kBasket];
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
      for (const sina::Quote &q : qs) {
        const uint32_t i = index_.at(q.sym);
        last_[i] = q;
        if (q.date != date_) // 停牌等: 只留快照
          continue;
        sec_ = std::max(sec_, q.sec);
        if (tracks_[i].absorb(q, gap)) {
          put(i, sec_slot(q.sec));
          put(i);
          dirty_ = true;
          touched_ = std::min(touched_, sec_slot(q.sec));
        }
      }
      std::atomic_ref<int32_t>(file_rec(map_).sec).store(sec_, std::memory_order_release); // 本轮各只落定之后
      ++ver_[kStock];
      schedule();
    }
    cv_.notify_all();
    fold();
    std::unique_lock<std::mutex> lock(mu_);
    cv_.wait_until(lock, t0 + std::chrono::seconds(in_session(clock_now()) ? kTick : kIdleTick), [&] { return stop_; });
    if (stop_)
      return;
  }
}

bool QuoteService::Track::absorb(const sina::Quote &q, int gap) {
  if (vol < 0) { // 首快照: 基线. 已有成交 ⇒ 所在根及以前快照没盯住
    vol = q.volume, amt = q.amount, high = q.high, low = q.low;
    if (q.volume > 0)
      mark(0, sec_slot(q.sec));
    return true;
  }
  const double dv = q.volume - vol, da = q.amount - amt;
  if (dv > 0 && gap >= 0)
    mark(gap, sec_slot(q.sec));
  if (dv > 0 && q.price > 0 && q.sec <= kCloseSec) {
    const int s = sec_slot(q.sec);
    if (!exact[static_cast<size_t>(s)]) {
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
  const int st = settled();
  if (sel_ != ~0u && tracks_[sel_].vol >= 0) { // 选中: 定稿部分补成精确, 插队
    Track &t = tracks_[sel_];
    int s = 0;
    while (s <= st && t.exact[s])
      ++s;
    if (s <= st) {
      t.mark(s, st);
      put(sel_);
    }
    if (t.fill == Fill::Queued && todo_.front() != sel_) {
      todo_.erase(std::find(todo_.begin(), todo_.end(), sel_));
      todo_.push_front(sel_);
    }
  }
  for (uint32_t i = 0; i < tracks_.size(); ++i) {
    Track &t = tracks_[i];
    if (t.fill == Fill::Idle && t.lo <= t.hi && (i == sel_ ? t.lo <= st : t.hi <= st || t.lo + kLag <= st)) {
      t.fill = Fill::Queued;
      i == sel_ ? todo_.push_front(i) : todo_.push_back(i);
    }
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
    }
    const std::vector<sina::Bar> bars = c.kline(syms_[i], n);
    std::lock_guard<std::mutex> lock(mu_);
    --inflight_;
    if (gen != gen_) // 取的途中换了日 (Track 已重置)
      continue;
    Track &t = tracks_[i];
    t.fill = Fill::Idle;
    const int st = settled();
    const int end = t.merge(lo, st, live, date_, bars);
    for (int s = lo; s <= end; ++s)
      put(i, s);
    put(i);
    ++fetched_;
    ++ver_[kStock];
    dirty_ = true;
    touched_ = std::min(touched_, lo);
  }
}

int QuoteService::Track::merge(int l, int settled, int live, misc::Ymd date, const std::vector<sina::Bar> &got) {
  assert(l <= settled);
  const int end = std::max(settled, std::min(hi, live) - 1); // 暂写段 (settled, end]: 待补区间内取时已走完的根
  for (int s = l; s <= end; ++s)
    bars.has.reset(static_cast<size_t>(s));
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
    lo = settled + 1;
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
  store(slot_rec(map_, tracks_.size(), i, s), SlotRec{t.bars.cell[k], t.bars.has[k], t.exact[k], 0});
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
          if (r.busy) {
            t.mark(s, s);
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