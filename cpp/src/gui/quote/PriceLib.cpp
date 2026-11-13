#include "gui/quote/PriceLib.hpp"

#include "account/Ledger.hpp"
#include "features/Bar1mDay.hpp"
#include "features/Panel/PoolSelect.hpp"      // all_bar / picks / extends
#include "features/Storage/FeatureLevels.hpp" // feature_storage_t
#include "features/Storage/FeatureLib.hpp"
#include "features/Storage/FeatureRead.hpp"
#include "fundamental/Scan.hpp"
#include "gui/quote/BasketRow.hpp"
#include "gui/quote/services/QuoteService.hpp" // 新浪归档
#include "misc/snap.hpp"
#include "shared/Basket.hpp" // basket_table

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cassert>
#include <cmath>
#include <cstdio>
#include <filesystem>
#include <mutex>
#include <numeric>
#include <set>
#include <string>
#include <unordered_map>

namespace GUI::Quote {

namespace {

constexpr uint64_t kMagic = 0x4441454852504B53ULL;    // "SKPRHEAD" 小端
constexpr uint64_t kSegMagic = 0x4D47455352504B53ULL; // "SKPRSEGM"
constexpr size_t kB = PriceLib::kBlock;
constexpr disk::Node kSideDir[kMarkets] = {disk::LibPriceStock, disk::LibPriceBasket}; // [侧]
constexpr disk::Node kBlockFile[kMarkets] = {disk::LibPriceStockBlock, disk::LibPriceBasketBlock};
constexpr disk::Node kDayFile[kMarkets] = {disk::LibPriceStockDay, disk::LibPriceBasketDay};

// 一行一日的日根 (只存有日根的): 存价口径 (个股 = 原价, 篮子 = 比), ref = 前收 (篮子 = 1); lo / step = 当日分钟格的价基 / 步长
struct DayRec {
  double f; // 因子 (过链后)
  float open, high, low, close, ref, amount;
  DayFlow flow;
  float lo, step;
};
// 分钟一槽: 价 = lo + q × step, 额 = expm1(a / kAmt), 净流入 = n / kNet × 额; c = kNoBar 即无根
struct Slot {
  uint16_t o, h, l, c, a;
  int16_t n;
};
struct MinRec {
  Slot s[kSlots];
};
constexpr uint16_t kNoBar = 0xFFFF;
constexpr double kQMax = 65534, kAmt = 2048, kNet = 32767;

// 段 = SegHead + RowAt [rows] + DayRec [recs] + MinRec [mins]
struct SegHead {
  uint64_t magic;
  uint32_t days, rows;
  uint64_t recs, mins;
};
// 段里的一行: 日根 / 分钟根的位图 (位 j = 段内第 j 日), 记录各自 at / mat 起按日序连续
struct RowAt {
  uint32_t key, at, mat, pad;
  uint64_t day, min;
};
size_t seg_bytes(const SegHead &h) { return sizeof(SegHead) + h.rows * sizeof(RowAt) + h.recs * sizeof(DayRec) + h.mins * sizeof(MinRec); }
uint64_t below(size_t j) { return (uint64_t{1} << j) - 1; }
// m 的各位 j 升序: f(j)
template <class F>
void bits(uint64_t m, F &&f) {
  for (; m; m &= m - 1)
    f(static_cast<size_t>(std::countr_zero(m)));
}

// 一段的只读映射
class Seg {
public:
  explicit Seg(const std::filesystem::path &p) {
    const int fd = ::open(p.c_str(), O_RDONLY);
    assert(fd >= 0 && "price/: 段打不开");
    struct stat sb{};
    [[maybe_unused]] const int st = ::fstat(fd, &sb);
    assert(st == 0);
    n_ = static_cast<size_t>(sb.st_size);
    void *m = ::mmap(nullptr, n_, PROT_READ, MAP_SHARED, fd, 0);
    ::close(fd);
    assert(m != MAP_FAILED && "price/: 段映射失败");
    ::madvise(m, n_, MADV_RANDOM);
    base_ = static_cast<const std::byte *>(m);
    h_ = reinterpret_cast<const SegHead *>(base_);
    assert(n_ >= sizeof(SegHead) && h_->magic == kSegMagic && seg_bytes(*h_) == n_ && "price/: 段损坏");
    rows_ = {reinterpret_cast<const RowAt *>(h_ + 1), h_->rows};
    recs_ = reinterpret_cast<const DayRec *>(rows_.data() + rows_.size());
    mins_ = reinterpret_cast<const MinRec *>(recs_ + h_->recs);
  }
  Seg(const Seg &) = delete;
  Seg &operator=(const Seg &) = delete;
  ~Seg() { ::munmap(const_cast<std::byte *>(base_), n_); }

  size_t days() const { return h_->days; }
  std::span<const RowAt> rows() const { return rows_; }
  const RowAt *row(uint32_t key) const {
    const auto it = std::ranges::lower_bound(rows_, key, {}, &RowAt::key);
    return it != rows_.end() && it->key == key ? &*it : nullptr;
  }
  // 行 r 段内第 j 日 (须有日根) 的日根 / 分钟根 (无 → nullptr)
  const DayRec &day(const RowAt &r, size_t j) const {
    assert(r.day >> j & 1);
    return recs_[r.at + static_cast<size_t>(std::popcount(r.day & below(j)))];
  }
  const MinRec *min(const RowAt &r, size_t j) const { return r.min >> j & 1 ? &mins_[r.mat + static_cast<size_t>(std::popcount(r.min & below(j)))] : nullptr; }
  // 行 r 的记录先一并发出读
  void will(const RowAt &r, bool minute) const {
    advise(recs_ + r.at, static_cast<size_t>(std::popcount(r.day)) * sizeof(DayRec));
    if (minute)
      advise(mins_ + r.mat, static_cast<size_t>(std::popcount(r.min)) * sizeof(MinRec));
  }

private:
  static void advise(const void *p, size_t n) {
    static const uintptr_t pg = static_cast<uintptr_t>(::sysconf(_SC_PAGESIZE));
    const uintptr_t a = reinterpret_cast<uintptr_t>(p) / pg * pg;
    if (n > 0)
      ::madvise(reinterpret_cast<void *>(a), reinterpret_cast<uintptr_t>(p) + n - a, MADV_WILLNEED);
  }
  const std::byte *base_ = nullptr;
  size_t n_ = 0;
  const SegHead *h_ = nullptr;
  std::span<const RowAt> rows_;
  const DayRec *recs_ = nullptr;
  const MinRec *mins_ = nullptr;
};

// 一段的写: tmp 上顺序写, done 换上
class SegOut {
public:
  explicit SegOut(std::filesystem::path p) : p_(std::move(p)), tmp_(p_) {
    tmp_ += ".tmp";
    f_ = std::fopen(tmp_.c_str(), "wb");
    assert(f_ && "price/: 段建不出来");
  }
  template <class T>
  void all(std::span<const T> x) {
    [[maybe_unused]] const size_t n = std::fwrite(x.data(), sizeof(T), x.size(), f_);
    assert(n == x.size() && "price/: 段写入失败");
  }
  template <class T>
  void put(const T &x) {
    all(std::span<const T>(&x, 1));
  }
  void done() {
    [[maybe_unused]] const int c = std::fclose(f_);
    assert(c == 0 && "price/: 段写入失败");
    std::filesystem::rename(tmp_, p_);
  }

private:
  std::filesystem::path p_, tmp_;
  std::FILE *f_ = nullptr;
};

disk::Key at_day(const disk::Key &lib, misc::Ymd d) {
  disk::Key k = lib;
  k.date = d;
  return k;
}

// 两侧逐个: f.template operator()<侧>()
template <class F>
void each_side(F &&f) {
  f.template operator()<kStock>();
  f.template operator()<kBasket>();
}

using misc::snap::items;

// 个股存价落成 f16 (库段本就是, 逐位不变) / f16 在 x 处的步长
float f16(float v) { return static_cast<float>(static_cast<feature_storage_t>(v)); }
float ulp16(float x) {
  int e = 0;
  std::frexp(x, &e);
  return std::ldexp(1.f, std::max(e - 11, -24));
}

// 一日分钟根 b (各根净流入 net) → m, 价基 / 步长记进 r
template <Side S>
void encode(const Bars &b, std::span<const double, kSlots> net, DayRec &r, MinRec &m) {
  const auto px = [](float v) { return S == kStock ? f16(v) : v; };
  float lo = INFINITY, hi = -INFINITY;
  for (size_t s = 0; s < static_cast<size_t>(kSlots); ++s)
    if (b.has[s])
      for (const float v : {b.cell[s].open, b.cell[s].high, b.cell[s].low, b.cell[s].close})
        lo = std::min(lo, px(v)), hi = std::max(hi, px(v));
  float step = S == kStock ? ulp16(lo) : static_cast<float>((hi - lo) / kQMax);
  while (S == kStock && (hi - lo) / step > kQMax)
    step *= 2;
  r.lo = lo, r.step = step;
  const auto q = [&](float v) { return static_cast<uint16_t>(step > 0 ? std::clamp(std::lround((px(v) - lo) / step), 0L, static_cast<long>(kQMax)) : 0L); };
  for (size_t s = 0; s < static_cast<size_t>(kSlots); ++s) {
    if (!b.has[s]) {
      m.s[s] = {0, 0, 0, kNoBar, 0, 0};
      continue;
    }
    const Cell &c = b.cell[s];
    const long a = std::lround(std::log1p(std::max(c.amount, 0.0)) * kAmt), n = c.amount > 0 ? std::lround(net[s] / c.amount * kNet) : 0L;
    m.s[s] = {q(c.open), q(c.high), q(c.low), q(c.close), static_cast<uint16_t>(std::clamp(a, 0L, 65535L)), static_cast<int16_t>(std::clamp(n, -32767L, 32767L))};
  }
}

// 一行一日过链 z: base = 当日前收 (存价口径), close = 当日收; origin = 首个有根日的因子. 返回当日因子
double step(PriceLib::Link &z, float base, float close, double origin) {
  assert(close > 0);
  if (z.ref > 0 && base > 0)
    z.f *= z.ref / base, z.ref = base;
  if (!(z.f > 0))
    z.f = origin;
  z.ref = close;
  return z.f;
}
template <Side S>
constexpr double origin() { return S == kStock ? 1.0 : PriceLib::kBase; }

// 一行一日的 收 / 前收: 成员过日的漂移比
float rel_of(const DayRec &r) { return r.ref > 0 ? r.close / r.ref : 1.f; }
float rel_of(const Candle &c) { return traded(c) && c.ref > 0 ? c.close / c.ref : 1.f; }

// 存价 → 有效价的倍数: 个股 1 (原价), 篮子 = 因子
template <Side S>
double scale_of(const DayRec &r) { return S == kBasket ? r.f : 1.0; }

template <Side S>
Candle candle_of(const DayRec &r, misc::Ymd d) {
  const double a = scale_of<S>(r);
  const auto at = [a](float v) { return static_cast<float>(v * a); };
  return {{at(r.open), at(r.high), at(r.low), at(r.close), r.amount}, d, at(r.ref), r.flow};
}

// 一行一日 → 一日分钟格 o [kSlots][Series::kFields] (价 = 有效价; m = nullptr → 全 NaN)
template <Side S>
void decode(const MinRec *m, const DayRec &r, float *o) {
  constexpr size_t F = Series::kFields;
  if (!m) {
    std::fill_n(o, static_cast<size_t>(kSlots) * F, NAN);
    return;
  }
  const double a = scale_of<S>(r);
  const auto px = [&](uint16_t q) { return static_cast<float>((r.lo + static_cast<float>(q) * r.step) * a); };
  for (size_t s = 0; s < static_cast<size_t>(kSlots); ++s) {
    const Slot &x = m->s[s];
    float *q = o + s * F;
    if (x.c == kNoBar) {
      std::fill_n(q, F, NAN);
      continue;
    }
    const double amt = std::expm1(x.a / kAmt);
    q[Series::kOpen] = px(x.o), q[Series::kHigh] = px(x.h), q[Series::kLow] = px(x.l), q[Series::kClose] = px(x.c);
    q[Series::kAmount] = static_cast<float>(amt), q[Series::kNet] = static_cast<float>(x.n / kNet * amt);
  }
}

// kBlock 个单日段 ds (日序) 并成一块 p
void seal_block(const std::filesystem::path &p, std::span<const Seg *const> ds) {
  assert(ds.size() == kB);
  std::vector<RowAt> rows;
  for (const Seg *d : ds)
    for (const RowAt &r : d->rows())
      rows.push_back({r.key, 0, 0, 0, 0, 0});
  std::ranges::sort(rows, {}, &RowAt::key);
  rows.erase(std::ranges::unique(rows, {}, &RowAt::key).begin(), rows.end());
  for (size_t j = 0; j < kB; ++j)
    for (const RowAt &r : ds[j]->rows()) {
      RowAt &x = *std::ranges::lower_bound(rows, r.key, {}, &RowAt::key);
      x.day |= uint64_t{1} << j, x.min |= (r.min & 1) << j;
    }
  uint64_t recs = 0, mins = 0;
  for (RowAt &x : rows) {
    x.at = static_cast<uint32_t>(recs), x.mat = static_cast<uint32_t>(mins);
    recs += static_cast<uint64_t>(std::popcount(x.day)), mins += static_cast<uint64_t>(std::popcount(x.min));
  }
  SegOut o(p);
  o.put(SegHead{kSegMagic, static_cast<uint32_t>(kB), static_cast<uint32_t>(rows.size()), recs, mins});
  o.all(std::span<const RowAt>(rows));
  for (const RowAt &x : rows)
    bits(x.day, [&](size_t j) { o.put(ds[j]->day(*ds[j]->row(x.key), 0)); });
  for (const RowAt &x : rows)
    bits(x.min, [&](size_t j) { o.put(*ds[j]->min(*ds[j]->row(x.key), 0)); });
  o.done();
}

} // namespace

// 两侧的段 (日序): 一段 = 首日下标 + 两侧各一 (同日界)
struct PriceStore {
  struct Part {
    size_t day = 0;
    PerMarket<std::unique_ptr<Seg>> side;
  };
  std::vector<Part> parts;
  size_t days() const { return parts.empty() ? 0 : parts.back().day + parts.back().side[kStock]->days(); }
};

// 一侧一日的件: 行按键升序追加, 过链后落成单日段
struct DayBuild {
  std::vector<uint32_t> keys;
  std::vector<DayRec> recs;
  std::vector<uint8_t> has; // [行] 带分钟根
  std::vector<MinRec> mins;

  void clear() { keys.clear(), recs.clear(), has.clear(), mins.clear(); }
  // 一行: 日根 c (存价口径) + 前收 ref + 资金流摘要 + 分钟根 b (nullptr / 无根 = 无; 各根净流入 net)
  template <Side S>
  void add(uint32_t key, const Cell &c, float ref, const DayFlow &flow, const Bars *b, std::span<const double, kSlots> net) {
    assert(keys.empty() || keys.back() < key);
    DayRec r{0, c.open, c.high, c.low, c.close, ref, static_cast<float>(c.amount), flow, 0, 0};
    const bool m = b && b->has.any();
    if (m)
      encode<S>(*b, net, r, mins.emplace_back());
    keys.push_back(key), recs.push_back(r), has.push_back(m);
  }
  // 个股一只 ← 源的一行 (无成交不收; 净流入 = 本只分钟涨跌法)
  void put_stock(uint32_t id, const Leg &g) {
    if (!g.day)
      return;
    std::array<double, kSlots> net{};
    DayFlow fl;
    if (g.bars) {
      net_flow(*g.bars, g.base, net);
      fl = day_flow(*g.bars, net);
    }
    add<kStock>(id, *g.day, g.base, fl, g.bars, net);
  }
  // 篮子各行 ← Lane 的一日 (前日 = 1)
  void put_baskets(std::span<const Day> ys) {
    for (size_t b = 0; b < ys.size(); ++b)
      if (const Day &y = ys[b]; y.candle.date)
        add<kBasket>(static_cast<uint32_t>(b), y.candle, 1.f, y.candle.flow, &y.bars, y.bars.net);
  }
  void write(const std::filesystem::path &p) const {
    std::vector<RowAt> rows(keys.size());
    uint32_t m = 0;
    for (size_t j = 0; j < keys.size(); ++j) {
      rows[j] = {keys[j], static_cast<uint32_t>(j), m, 0, 1, has[j]};
      m += has[j];
    }
    SegOut o(p);
    o.put(SegHead{kSegMagic, 1, static_cast<uint32_t>(rows.size()), recs.size(), mins.size()});
    o.all(std::span<const RowAt>(rows));
    o.all(std::span<const DayRec>(recs));
    o.all(std::span<const MinRec>(mins));
    o.done();
  }
};

namespace {

// 补段一日的件 (逐批复用): 合成件 + 源的读 (基本面段 = bar1m, 新浪段 = 归档的日根 / 分钟根 [全局 id]) + 两侧的件
struct TailWork {
  explicit TailWork(size_t baskets) : lane(baskets) {}
  Lane lane;
  Bar1mDay bd;
  fundamental::Scratch xs;
  std::vector<Candle> spx;
  std::vector<Bars> sbars;
  PerMarket<DayBuild> b;
};

} // namespace

PriceLib::PriceLib(std::shared_ptr<const Calendar> cal, std::function<void(const Handoff &)> bind) : cal_(std::move(cal)), bind_(std::move(bind)) {
  assert(cal_ && !cal_->days.empty() && bind_);
  Start();
}
PriceLib::~PriceLib() {
  quit_.store(true, std::memory_order_relaxed);
  Stop();
}

template <class Ar>
void PriceLib::snap(Ar &ar) {
  ar(prefix_);
  items(ar, dates_);
  ar(lib_days_);
  ar(fund_days_);
  ar(sealed_);
  for (std::vector<Link> &l : link_)
    items(ar, l);
}

void PriceLib::save_head() {
  snap_write(disk::LibPriceHead, lib_, head_, [&](auto &ar) { snap(ar); });
}

PriceStamp PriceLib::stamp() const { return {head_.lib, dates_.back(), dates_.size(), lib_days_, fund_days_}; }

bool PriceLib::open_store() {
  const size_t D = dates_.size(), B = sealed_ * kB;
  std::set<misc::Ymd> blocks, days;
  for (size_t b = 0; b < sealed_; ++b)
    blocks.insert(dates_[b * kB]);
  for (size_t i = B; i < D; ++i)
    days.insert(dates_[i]);
  bool whole = true;
  for (int s = 0; s < kMarkets; ++s) {
    for (const disk::Key &k : disk::list(kBlockFile[s], lib_))
      if (!blocks.contains(k.date))
        disk::remove(kBlockFile[s], k);
    for (const disk::Key &k : disk::list(kDayFile[s], lib_))
      if (!days.contains(k.date))
        disk::remove(kDayFile[s], k);
    for (const misc::Ymd d : blocks)
      whole = whole && disk::exists(kBlockFile[s], at_day(lib_, d));
    for (const misc::Ymd d : days)
      whole = whole && disk::exists(kDayFile[s], at_day(lib_, d));
  }
  if (!whole)
    return false;
  auto st = std::make_unique<PriceStore>();
  for (size_t i = 0; i < D; i += i < B ? kB : 1) {
    PriceStore::Part p{i, {}};
    for (int s = 0; s < kMarkets; ++s)
      p.side[static_cast<size_t>(s)] = std::make_unique<Seg>(disk::path(i < B ? kBlockFile[s] : kDayFile[s], at_day(lib_, dates_[i])));
    assert(p.side[kStock]->days() == p.side[kBasket]->days() && p.side[kStock]->days() == (i < B ? kB : 1) && "price/: 段的日数与头不符");
    st->parts.push_back(std::move(p));
  }
  store_ = std::move(st);
  return true;
}

void PriceLib::wipe(const std::vector<misc::Ymd> &dates) {
  store_.reset();
  dates_ = {dates.front()}, lib_days_ = 1, fund_days_ = 0, sealed_ = 0;
  link_[kStock].assign(asset_axis().size(), Link{});
  link_[kBasket].assign(basket_table().size(), Link{});
  prefix_ = asset_axis().hash_at(rows(kStock));
  disk::remove(disk::LibPrice, lib_);
  const Day2 empty{};
  for (int s = 0; s < kMarkets; ++s) {
    disk::ensure(kSideDir[s], lib_);
    empty[static_cast<size_t>(s)].write(disk::path(kDayFile[s], at_day(lib_, dates_[0])));
  }
  save_head();
  [[maybe_unused]] const bool ok = open_store();
  assert(ok);
}

void PriceLib::reopen(const FeatureLib &lib, const std::vector<misc::Ymd> &dates) {
  lib_ = lib_key(lib.id);
  const AssetAxis &ax = asset_axis();
  const Head want{kMagic, basket_table().hash(), lib.axis.hash, dates.front()};
  const auto fits = [&] { return head_ == want && pool::extends(std::span(dates_).first(lib_days_), dates); };
  if (!(store_ && fits())) {
    store_.reset();
    head_ = {}, prefix_ = 0, dates_.clear(), lib_days_ = fund_days_ = sealed_ = 0, link_ = {}, at_ = {};
    if (snap_read(disk::LibPriceHead, lib_, want, [&](auto &ar) { snap(ar); })) {
      head_ = want;
      assert(lib_days_ >= 1 && lib_days_ + fund_days_ <= dates_.size() && sealed_ * kB <= lib_days_ && "price/head.bin: 段日数越界");
      assert(rows(kBasket) == basket_table().size() && "price/head.bin: 篮子链长与篮子表不符");
    }
    if (!(fits() && rows(kStock) <= ax.size() && ax.hash_at(rows(kStock)) == prefix_ && open_store())) {
      head_ = want;
      wipe(dates);
    }
  }
  if (rows(kStock) < ax.size()) { // 资产轴长了: 新行的链自无起
    link_[kStock].resize(ax.size());
    prefix_ = ax.hash_at(rows(kStock));
    save_head();
  }
  axis_ = lib.axis;
}

void PriceLib::commit(misc::Ymd d, Day2 &b, Links &z) {
  PriceStore::Part p{dates_.size(), {}};
  each_side([&]<Side S>() {
    DayBuild &x = b[S];
    for (size_t j = 0; j < x.keys.size(); ++j) {
      DayRec &r = x.recs[j];
      r.f = step(z[S][x.keys[j]], r.ref, r.close, origin<S>());
    }
    const std::filesystem::path f = disk::path(kDayFile[S], at_day(lib_, d));
    x.write(f);
    p.side[S] = std::make_unique<Seg>(f);
  });
  store_->parts.push_back(std::move(p));
  dates_.push_back(d);
}

void PriceLib::seal() {
  std::vector<PriceStore::Part> &ps = store_->parts;
  while ((sealed_ + 1) * kB <= lib_days_) {
    const size_t i0 = sealed_ * kB, at = sealed_; // 块前各段都是块 ⇒ 首个单日段在 parts[sealed_]
    assert(ps[at].day == i0 && ps[at + kB - 1].day == i0 + kB - 1);
    PriceStore::Part blk{i0, {}};
    each_side([&]<Side S>() {
      std::vector<const Seg *> ds;
      for (size_t j = 0; j < kB; ++j)
        ds.push_back(ps[at + j].side[S].get());
      const std::filesystem::path f = disk::path(kBlockFile[S], at_day(lib_, dates_[i0]));
      seal_block(f, ds);
      blk.side[S] = std::make_unique<Seg>(f);
    });
    ++sealed_;
    save_head();
    ps.erase(ps.begin() + static_cast<ptrdiff_t>(at), ps.begin() + static_cast<ptrdiff_t>(at + kB));
    ps.insert(ps.begin() + static_cast<ptrdiff_t>(at), std::move(blk));
    for (size_t j = 0; j < kB; ++j)
      for (int s = 0; s < kMarkets; ++s)
        disk::remove(kDayFile[s], at_day(lib_, dates_[i0 + j]));
  }
}

void PriceLib::cut(size_t D) {
  assert(D >= lib_days_ && D <= dates_.size());
  const std::vector<misc::Ymd> gone(dates_.begin() + static_cast<ptrdiff_t>(D), dates_.end());
  dates_.resize(D), fund_days_ = std::min(fund_days_, D - lib_days_);
  save_head();
  while (store_->days() > D)
    store_->parts.pop_back();
  for (const misc::Ymd d : gone)
    for (int s = 0; s < kMarkets; ++s)
      disk::remove(kDayFile[s], at_day(lib_, d));
}

bool PriceLib::sync(const FeatureLib &lib, const std::vector<misc::Ymd> &dates, misc::Ymd mark) {
  assert(dates.size() >= 2 && mark > 0);
  reopen(lib, dates);
  const fundamental::Catalog cat;
  const std::vector<misc::Ymd> &trading = cal_->days;
  const auto lo = std::upper_bound(trading.begin(), trading.end(), dates.back());
  const auto mid = std::max(lo, std::upper_bound(trading.begin(), trading.end(), mark));
  auto hi = mid;
  while (hi != trading.end() && disk::exists(disk::QuoteDay, {.date = *hi}))
    ++hi;
  const std::vector<misc::Ymd> tail(lo, hi);
  const size_t F = static_cast<size_t>(mid - lo);
  const bool grow = lib_days_ < dates.size();
  size_t keep = 0; // 补段留着的日: 库段没长时, 打头 (日, 截) 都不变的
  if (!grow)
    while (keep < std::min(dates_.size() - lib_days_, tail.size()) && dates_[lib_days_ + keep] == tail[keep] && (keep < fund_days_) == (keep < F))
      ++keep;
  const bool fresh = !grow && keep == tail.size() && dates_.size() == lib_days_ + keep;
  if (fresh && at_ == stamp())
    return true;
  at_ = {};
  FeatureRead rd(lib, &quit_);
  const std::shared_ptr<const pool::Picks> pk = pool::picks(lib, &quit_);
  if (!pk)
    return false;
  if (dates_.size() > lib_days_ + keep)
    cut(lib_days_ + keep);

  // 库段一趟 (lib_pass): 各日两侧的件在并行段各自填, 串行段按日序过链落盘; 每批落一次头 (满块即封)
  if (grow) {
    std::vector<Day2> bs(static_cast<size_t>(pool_.threads()));
    const auto each = [&](size_t k, uint32_t id, const Leg &g) { bs[k][kStock].put_stock(id, g); };
    const auto chain = [&](size_t i, size_t k, LibWork &w) {
      assert(dates_.size() == i);
      Day2 &b = bs[k];
      b[kBasket].put_baskets(w.lane.day);
      commit(dates[i], b, link_);
      for (DayBuild &x : b)
        x.clear();
    };
    const auto batch = [&] {
      lib_days_ = dates_.size();
      seal();
      save_head();
    };
    if (!lib_pass(lib.axis, rd, *pk, lib_days_, lib_days_, pool_, quit_, *this, each, chain, batch)) {
      if (rd.stale()) // 库在重算: 下次从头核对
        head_ = {};
      return false;
    }
  }
  if (!fill(*pk, cat, tail, F, keep))
    return false;
  drop_archived(dates_[lib_days_ + fund_days_ - 1]);
  at_ = stamp();
  bind_(Handoff{mem_, level_});
  return true;
}

bool PriceLib::fill(const pool::Picks &pk, const fundamental::Catalog &cat, const std::vector<misc::Ymd> &tail, size_t F, size_t keep) {
  const size_t N = rows(kStock), NB = rows(kBasket), T = tail.size(), L = lib_days_;
  assert(F <= T && keep <= T && dates_.size() == L + keep && store_->days() == dates_.size());
  Members mem;
  members(pk, axis_, cal_->next(dates_[L - 1]), mem);
  level_.resize(NB);
  for (size_t b = 0; b < NB; ++b) {
    const Link &z = link_[kBasket][b];
    level_[b] = z.f > 0 ? z.f * z.ref : kBase;
  }
  Links z = link_;
  std::vector<float> rel(N);          // [全局 id] 当日 收 / 前收 (无日根 1)
  for (size_t k = 0; k < keep; ++k) { // 留着的日: 链 / 成员照盘上的根重放
    const PriceStore::Part &p = store_->parts[store_->parts.size() - keep + k];
    std::ranges::fill(rel, 1.f);
    each_side([&]<Side S>() {
      const Seg &sg = *p.side[S];
      for (const RowAt &r : sg.rows()) {
        const DayRec &x = sg.day(r, 0);
        step(z[S][r.key], x.ref, x.close, origin<S>());
        if constexpr (S == kStock)
          rel[r.key] = rel_of(x);
        else
          level_[r.key] = x.f * x.close;
      }
    });
    drift(mem, tail[k], cal_->next(tail[k]), rel);
  }
  if (keep < T) {
    begin(Phase::Loading, T - keep);
    fundamental::Ids ids(fundamental::Ids::Cover::Part); // 资产轴前 N 条, 下标 = 全局 id
    for (size_t a = 0; a < N; ++a)
      ids.add(asset_axis().code(a));
    // 基本面段要算的日的日根: bar1d 行 [日 − keep][全局 id] (无行 → 收 0)
    std::vector<Candle> px(F > keep ? (F - keep) * N : 0);
    if (F > keep) {
      fundamental::Scratch xs;
      const auto f0 = tail.begin() + static_cast<std::ptrdiff_t>(keep), f1 = tail.begin() + static_cast<std::ptrdiff_t>(F);
      for (const fundamental::Shard &sh : cat.shards(kBar1d)) {
        if (sh.ym < misc::ym_of(*f0) || sh.ym > misc::ym_of(f1[-1]))
          continue;
        each_daily(xs, sh, ids, [&](uint32_t a, const Candle &c) {
          const auto it = std::lower_bound(f0, f1, c.date);
          if (it != f1 && *it == c.date)
            px[static_cast<size_t>(it - f0) * N + a] = c;
        });
      }
    }
    const size_t W = std::min(static_cast<size_t>(pool_.threads()), T - keep);
    std::vector<std::unique_ptr<TailWork>> works;
    for (size_t j = 0; j < W; ++j)
      works.push_back(std::make_unique<TailWork>(NB));
    const auto day_px = [&](size_t k, const TailWork &w) {
      return k < F ? std::span<const Candle>(px.data() + (k - keep) * N, N) : std::span<const Candle>(w.spx);
    };
    // 每批 W 日: 并行读源 → 串行接成员 (只看日根) → 并行合成 → 串行过链落盘
    for (size_t k0 = keep; k0 < T; k0 += W) {
      if (quit_.load(std::memory_order_relaxed))
        return false;
      const size_t n = std::min(W, T - k0);
      pool_.tasks(static_cast<int>(n), [&](int j, int) {
        const size_t k = k0 + static_cast<size_t>(j);
        TailWork &w = *works[static_cast<size_t>(j)];
        if (k < F)
          w.bd.load(cat, ids, w.xs, tail[k]);
        else
          archived_day(tail[k], N, w.spx, w.sbars);
      });
      for (size_t j = 0; j < n; ++j) {
        const size_t k = k0 + j;
        TailWork &w = *works[j];
        w.lane.mem = mem;
        const std::span<const Candle> c = day_px(k, w);
        for (size_t a = 0; a < N; ++a)
          rel[a] = rel_of(c[a]);
        drift(mem, tail[k], cal_->next(tail[k]), rel);
      }
      pool_.tasks(static_cast<int>(n), [&](int j, int) {
        const size_t k = k0 + static_cast<size_t>(j);
        TailWork &w = *works[static_cast<size_t>(j)];
        const auto put = [&](uint32_t id, const Leg &g) { w.b[kStock].put_stock(id, g); };
        if (k < F)
          w.lane.run(FundDay{tail[k], day_px(k, w), w.bd}, put);
        else
          w.lane.run(SinaDay{tail[k], w.spx, w.sbars}, put);
        w.b[kBasket].put_baskets(w.lane.day);
      });
      for (size_t j = 0; j < n; ++j) {
        TailWork &w = *works[j];
        commit(tail[k0 + j], w.b, z);
        const DayBuild &bb = w.b[kBasket];
        for (size_t r = 0; r < bb.keys.size(); ++r)
          level_[bb.keys[r]] = bb.recs[r].f * bb.recs[r].close;
        for (DayBuild &x : w.b)
          x.clear();
        publish(1);
      }
    }
    fund_days_ = F;
    save_head();
  }
  mem_ = std::move(mem);
  return true;
}

bool PriceLib::run(PriceRequest &req) {
  assert(std::ranges::count(req.wants, false, &Want::minute) <= static_cast<ptrdiff_t>(kKeep) &&
         std::ranges::count(req.wants, true, &Want::minute) <= static_cast<ptrdiff_t>(kKeepMinutes) && "请求超出留存");
  assert(std::ranges::none_of(req.wants, [](const Want &w) { return w.ref.side == kAccount; }) && "账户一侧整条随发布");
  const pool::AllBar ab = pool::all_bar("行情历史由 all.bar 特征库合成");
  if (!ab.lack.empty()) {
    if (!result || result->lack != ab.lack)
      put(std::make_shared<PriceView>(PriceView{.lack = ab.lack}));
    return true;
  }
  if (!sync(ab.lib, ab.dates, req.mark))
    return false;
  if (!(built_ == at_))
    rebuild();
  if (!account_ || ledger_ != req.ledger || !(spot_ == req.spot))
    replay(req.ledger, req.spot);
  for (const Want &w : req.wants) {
    if (cancel_.load(std::memory_order_relaxed))
      return false;
    if (w.minute)
      minute_.get(w.ref, at_, [&](Series &s) { return series(w.ref, true, s); });
    else if (w.ref.side == kStock)
      daily_.get(w.ref, at_, [&](Series &s) { return series(w.ref, false, s); });
  }
  // 发布 = 本戳留着的全部 (不只本次要的: 切页不闪), 键升序; 连分钟的盖过不连的
  using Item = std::pair<uint32_t, std::shared_ptr<const Series>>;
  std::vector<Item> ks, bm;
  minute_.each(at_, [&](Ref r, const std::shared_ptr<const Series> &s) { (r.side == kStock ? ks : bm).emplace_back(r.key, s); });
  daily_.each(at_, [&](Ref r, const std::shared_ptr<const Series> &s) { ks.emplace_back(r.key, s); });
  std::ranges::stable_sort(ks, {}, &Item::first);
  ks.erase(std::ranges::unique(ks, {}, &Item::first).begin(), ks.end());
  auto v = std::make_shared<PriceView>();
  v->stamp = at_;
  v->dates = spine_;
  auto st = std::make_shared<Tape>();
  st->tails = tails_;
  for (auto &[k, s] : ks)
    st->keys.push_back(k), st->list.push_back(std::move(s));
  std::shared_ptr<const Tape> bt = baskets_;
  if (!bm.empty()) {
    auto t = std::make_shared<Tape>(*baskets_);
    for (auto &[k, s] : bm)
      if (const size_t j = t->find(k); s && j < t->keys.size())
        t->list[j] = std::move(s);
    bt = std::move(t);
  }
  const PriceView *o = result && result->lack.empty() && result->stamp == at_ ? result.get() : nullptr;
  const bool same_stocks = o && o->tape[kStock]->keys == st->keys && o->tape[kStock]->list == st->list;
  const bool same_baskets = o && o->tape[kBasket]->list == bt->list;
  v->tape[kStock] = same_stocks ? o->tape[kStock] : std::move(st);
  v->tape[kBasket] = same_baskets ? o->tape[kBasket] : std::move(bt);
  v->tape[kAccount] = account_;
  if (o && same_stocks && same_baskets && o->tape[kAccount] == account_)
    return true;
  put(std::move(v));
  return true;
}

void PriceLib::put(std::shared_ptr<const PriceView> v) {
  std::lock_guard<std::mutex> lock(mutex);
  result = std::move(v);
  Progress::publish();
}

void PriceLib::rebuild() {
  built_ = at_;
  account_ = nullptr;
  spine_ = std::make_shared<const std::vector<misc::Ymd>>(dates_);
  // 个股日脊末段: 自末段往前, 各只取够 kTailDays 个有根日
  const size_t N = rows(kStock);
  std::vector<std::array<Candle, kTailDays>> c(N);
  std::vector<uint8_t> n(N, 0);
  for (auto p = store_->parts.rbegin(); p != store_->parts.rend(); ++p) {
    const Seg &sg = *p->side[kStock];
    for (const RowAt &r : sg.rows())
      for (uint64_t m = r.day; m && n[r.key] < kTailDays;) {
        const size_t j = static_cast<size_t>(63 - std::countl_zero(m));
        c[r.key][n[r.key]++] = candle_of<kStock>(sg.day(r, j), dates_[p->day + j]);
        m &= ~(uint64_t{1} << j);
      }
  }
  auto t = std::make_shared<Tails>();
  for (uint32_t id = 0; id < N; ++id) {
    if (!n[id])
      continue;
    t->ids.push_back(id);
    for (size_t k = n[id]; k-- > 0;)
      t->daily.v.push_back(c[id][k]);
    t->daily.off.push_back(static_cast<uint32_t>(t->daily.v.size()));
  }
  tails_ = std::move(t);
  auto b = std::make_shared<Tape>();
  b->keys.resize(rows(kBasket));
  std::iota(b->keys.begin(), b->keys.end(), 0u);
  b->list.resize(b->keys.size());
  pool_.tasks(static_cast<int>(b->keys.size()), [&](int j, int) {
    if (auto s = std::make_shared<Series>(); series({kBasket, b->keys[static_cast<size_t>(j)]}, false, *s))
      b->list[static_cast<size_t>(j)] = std::move(s);
  });
  census(mem_, b->keys, *b);
  baskets_ = std::move(b);
}

bool PriceLib::series(Ref ref, bool minute, Series &out, size_t from) const {
  assert(ref.ok() && ref.side < kMarkets);
  if (ref.key >= rows(ref.side))
    return false;
  out.minute = minute;
  each_side([&]<Side S>() {
    if (S != ref.side)
      return;
    constexpr size_t W = static_cast<size_t>(kSlots) * Series::kFields;
    std::vector<std::pair<const Seg *, const RowAt *>> on; // 有该行的段
    std::vector<size_t> day0;
    for (const PriceStore::Part &p : store_->parts)
      if (p.day + p.side[S]->days() > from)
        if (const RowAt *r = p.side[S]->row(ref.key)) {
          p.side[S]->will(*r, minute);
          on.emplace_back(p.side[S].get(), r), day0.push_back(p.day);
        }
    for (size_t q = 0; q < on.size(); ++q) {
      const auto [sg, r] = on[q];
      bits(r->day, [&](size_t j) {
        if (const size_t i = day0[q] + j; i >= from) {
          const DayRec &x = sg->day(*r, j);
          out.daily.push_back(candle_of<S>(x, dates_[i]));
          out.adj.push_back(S == kStock ? x.f : 1.0);
          if (minute) {
            out.px.resize(out.px.size() + W);
            decode<S>(sg->min(*r, j), x, out.px.data() + out.px.size() - W);
          }
        }
      });
    }
  });
  out.px.shrink_to_fit();
  return !out.daily.empty();
}

void PriceLib::replay(uint64_t ledger, const account::Spot &spot) {
  ledger_ = ledger, spot_ = spot;
  auto t = std::make_shared<Tape>();
  t->keys = {0};
  t->list = {nullptr};
  const account::Ledger l = account::load();
  const size_t D = dates_.size();
  const size_t i0 = l.rows.empty() ? D : static_cast<size_t>(std::ranges::lower_bound(dates_, l.rows.front().date) - dates_.begin());
  if (i0 < D) {
    // 各证券自 i0 起的全史 (连分钟; 无根的不收 → 回放按成交价 / 券商现价插值)
    std::unordered_map<std::string, Series> held;
    account::Closes px;
    for (const std::string &s : account::securities(l.rows))
      if (const size_t id = asset_axis().find(account::asset_code(s)); id < rows(kStock))
        if (Series se; series({kStock, static_cast<uint32_t>(id)}, true, se, i0)) {
          for (const Candle &c : se.daily)
            px[s][c.date] = c.close;
          held.emplace(s, std::move(se));
        }
    const std::vector<account::Day> ds =
        account::replay(l.rows, l.rate, std::vector<misc::Ymd>(dates_.begin() + static_cast<ptrdiff_t>(i0), dates_.end()), px, spot);
    assert(ds.size() == D - i0 && "账户回放: 日数与日脊不符");
    auto se = std::make_shared<Series>();
    se->minute = true;
    se->px.resize(ds.size() * kSlots * Series::kFields);
    std::array<float, kSlots> path;
    std::array<double, kSlots> idx;
    for (size_t k = 0; k < ds.size(); ++k) {
      const misc::Ymd date = dates_[i0 + k];
      const account::Day &d = ds[k];
      // 一只当日的逐槽收 (无根沿用上一根, 首根前 = 前收); 当日无分钟 → 空
      const auto path_of = [&](const std::string &code) -> std::span<const float> {
        const auto it = held.find(code);
        if (it == held.end())
          return {};
        const Series &h = it->second;
        const size_t j = h.find(date);
        if (j == h.daily.size())
          return {};
        float at = h.daily[j].ref;
        const float *p = h.slots(j);
        for (size_t s = 0; s < static_cast<size_t>(kSlots); ++s) {
          if (const float c = p[s * Series::kFields + Series::kClose]; c > 0)
            at = c;
          path[s] = at;
        }
        return path;
      };
      if (k > 0)
        account::intraday(d, ds[k - 1].nav, ds[k - 1].index, path_of, idx);
      else
        idx.fill(d.index);
      const float ref = static_cast<float>(k > 0 ? ds[k - 1].index : 1.0), x = static_cast<float>(d.index);
      float hi = std::max(ref, x), lo = std::min(ref, x), prev = ref;
      float *p = se->px.data() + k * kSlots * Series::kFields;
      for (size_t s = 0; s < static_cast<size_t>(kSlots); ++s) {
        const float v = static_cast<float>(idx[s]);
        float *q = p + s * Series::kFields;
        q[Series::kOpen] = prev, q[Series::kHigh] = std::max(prev, v), q[Series::kLow] = std::min(prev, v), q[Series::kClose] = v;
        q[Series::kAmount] = q[Series::kNet] = 0;
        hi = std::max(hi, v), lo = std::min(lo, v), prev = v;
      }
      se->daily.push_back({cell(ref, hi, lo, x, 0), date, ref, {}});
      se->adj.push_back(1.0);
    }
    t->list[0] = std::move(se);
    t->nav = ds.back().nav;
    t->after = account::flow_after(l.rows, ds.back().date);
    for (const account::Day &d : ds)
      t->in += d.flow;
  }
  account_ = std::move(t);
}

} // namespace GUI::Quote
