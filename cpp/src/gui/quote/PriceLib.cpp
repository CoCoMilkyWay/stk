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

constexpr uint64_t kMagic = 0x5453414D52504B53ULL;    // "SKPRMAST" 小端
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

// 一槽的额格 → 额 (查表)
double amount_of(uint16_t a) {
  static const std::vector<double> t = [] {
    std::vector<double> v(65536);
    for (size_t q = 0; q < v.size(); ++q)
      v[q] = std::expm1(static_cast<double>(q) / kAmt);
    return v;
  }();
  return t[a];
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
    const double amt = amount_of(x.a);
    q[Series::kOpen] = px(x.o), q[Series::kHigh] = px(x.h), q[Series::kLow] = px(x.l), q[Series::kClose] = px(x.c);
    q[Series::kAmount] = static_cast<float>(amt), q[Series::kNet] = static_cast<float>(x.n / kNet * amt);
  }
}

// 篮子的源 = 个股侧一段的段内第 j 日 (行 = 全局 id; 基准 = 存的前收, 根 = 存价即原价; 该日无根的行 = 无日根无分钟根)
struct SegDay {
  misc::Ymd date;
  const Seg &sg;
  size_t j;
  size_t size() const { return sg.rows().size(); }
  uint32_t id(size_t r) const { return sg.rows()[r].key; }
  Leg leg(size_t r, Cell &c, Bars &b) const {
    const RowAt &x = sg.rows()[r];
    b.has.reset();
    if (!(x.day >> j & 1))
      return {1.f, nullptr, &b};
    const DayRec &d = sg.day(x, j);
    c = cell(d.open, d.high, d.low, d.close, d.amount);
    if (const MinRec *m = sg.min(x, j)) {
      const auto px = [&](uint16_t q) { return d.lo + static_cast<float>(q) * d.step; };
      for (size_t s = 0; s < static_cast<size_t>(kSlots); ++s)
        if (const Slot &y = m->s[s]; y.c != kNoBar)
          b.add(static_cast<int>(s), cell(px(y.o), px(y.h), px(y.l), px(y.c), amount_of(y.a)));
    }
    return {d.ref, &c, &b};
  }
};

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

// 两侧的段 (各自日序): 一段 = 首日下标 + 映射
struct PriceStore {
  struct Part {
    size_t day = 0;
    std::unique_ptr<Seg> seg;
  };
  PerMarket<std::vector<Part>> side;
  // 一侧日 i 所在的段 + 段内日
  std::pair<const Seg *, size_t> at(Side s, size_t i) const {
    const std::vector<Part> &ps = side[s];
    const auto it = std::ranges::upper_bound(ps, i, {}, &Part::day);
    assert(it != ps.begin() && i - it[-1].day < it[-1].seg->days());
    return {it[-1].seg.get(), i - it[-1].day};
  }
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
  // 个股各行 ← 源的一日 (无成交不收; 净流入 = 本只分钟涨跌法)
  template <class Src>
  void put_stocks(const Src &src) {
    Cell c;
    Bars b;
    std::array<double, kSlots> net;
    for (size_t j = 0; j < src.size(); ++j)
      if (const Leg g = src.leg(j, c, b); g.day) {
        net.fill(0);
        net_flow(*g.bars, g.base, net);
        add<kStock>(src.id(j), *g.day, g.base, day_flow(*g.bars, net), g.bars, net);
      }
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

// 个股补段一日的件 (逐批复用): 源的读 (基本面段 = bar1m, 新浪段 = 归档的日根 / 分钟根 [全局 id]) + 一日的件
struct TailWork {
  Bar1mDay bd;
  fundamental::Scratch xs;
  std::vector<Candle> spx;
  std::vector<Bars> sbars;
  DayBuild b;
};

// 篮子一日的件 (逐批复用): 合成件 + 一日的件
struct FoldWork {
  explicit FoldWork(size_t baskets) : lane(baskets) {}
  Lane lane;
  DayBuild b;
};

// 一侧盘上第 i 日的根照样过链 z (留着的日: 只重放链, 不动段)
template <Side S>
void rechain(const PriceStore &st, size_t i, std::vector<PriceLib::Link> &z) {
  const auto [sg, j] = st.at(S, i);
  for (const RowAt &r : sg->rows())
    if (r.day >> j & 1) {
      const DayRec &x = sg->day(r, j);
      step(z[r.key], x.ref, x.close, origin<S>());
    }
}

// 一侧的首日 (只定成员, 无根): 空段
std::unique_ptr<Seg> first_day(Side s, const disk::Key &lib, misc::Ymd d) {
  disk::ensure(kSideDir[s], lib);
  const std::filesystem::path p = disk::path(kDayFile[s], at_day(lib, d));
  DayBuild{}.write(p);
  return std::make_unique<Seg>(p);
}

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
  ar(table_);
  items(ar, dates_);
  ar(lib_days_);
  ar(fund_days_);
  ar(bdays_);
  for (size_t &n : sealed_)
    ar(n);
  for (std::vector<Link> &l : link_)
    items(ar, l);
}

void PriceLib::save_head() {
  snap_write(disk::LibPriceHead, lib_, head_, [&](auto &ar) { snap(ar); });
}

PriceStamp PriceLib::stamp() const { return {head_.lib, table_, dates_.back(), dates_.size(), lib_days_, fund_days_}; }

bool PriceLib::open_side(Side s) {
  const size_t D = days(s), B = sealed_[s] * kB;
  std::set<misc::Ymd> blocks, singles;
  for (size_t b = 0; b < sealed_[s]; ++b)
    blocks.insert(dates_[b * kB]);
  for (size_t i = B; i < D; ++i)
    singles.insert(dates_[i]);
  for (const disk::Key &k : disk::list(kBlockFile[s], lib_))
    if (!blocks.contains(k.date))
      disk::remove(kBlockFile[s], k);
  for (const disk::Key &k : disk::list(kDayFile[s], lib_))
    if (!singles.contains(k.date))
      disk::remove(kDayFile[s], k);
  if (!(std::ranges::all_of(blocks, [&](misc::Ymd d) { return disk::exists(kBlockFile[s], at_day(lib_, d)); }) &&
        std::ranges::all_of(singles, [&](misc::Ymd d) { return disk::exists(kDayFile[s], at_day(lib_, d)); })))
    return false;
  std::vector<PriceStore::Part> &ps = store_->side[s];
  ps.clear();
  for (size_t i = 0; i < D; i += i < B ? kB : 1) {
    ps.push_back({i, std::make_unique<Seg>(disk::path(i < B ? kBlockFile[s] : kDayFile[s], at_day(lib_, dates_[i])))});
    assert(ps.back().seg->days() == (i < B ? kB : 1) && "price/: 段的日数与头不符");
  }
  return true;
}

void PriceLib::wipe(const std::vector<misc::Ymd> &dates) {
  store_ = std::make_unique<PriceStore>();
  dates_ = {dates.front()}, lib_days_ = 1, fund_days_ = 0, sealed_[kStock] = 0;
  link_[kStock].assign(asset_axis().size(), Link{});
  prefix_ = asset_axis().hash_at(rows(kStock));
  disk::remove(disk::LibPrice, lib_);
  store_->side[kStock].push_back({0, first_day(kStock, lib_, dates_[0])});
  wipe_baskets();
}

void PriceLib::wipe_baskets() {
  table_ = basket_table().hash(), bdays_ = 1, sealed_[kBasket] = 0;
  link_[kBasket].assign(basket_table().size(), Link{});
  disk::remove(kSideDir[kBasket], lib_);
  store_->side[kBasket].clear();
  store_->side[kBasket].push_back({0, first_day(kBasket, lib_, dates_[0])});
  save_head();
}

void PriceLib::reopen(const FeatureLib &lib, const std::vector<misc::Ymd> &dates) {
  lib_ = lib_key(lib.id);
  const AssetAxis &ax = asset_axis();
  const Head want{kMagic, lib.axis.hash, dates.front()};
  const auto fits = [&] { return head_ == want && pool::extends(std::span(dates_).first(lib_days_), dates); };
  if (!(store_ && fits())) {
    store_ = std::make_unique<PriceStore>();
    head_ = {}, prefix_ = table_ = 0, dates_.clear(), lib_days_ = fund_days_ = bdays_ = 0, sealed_ = {}, link_ = {}, at_ = {};
    if (snap_read(disk::LibPriceHead, lib_, want, [&](auto &ar) { snap(ar); })) {
      head_ = want;
      assert(lib_days_ >= 1 && lib_days_ + fund_days_ <= dates_.size() && bdays_ >= 1 && bdays_ <= dates_.size() && "price/head.bin: 段日数越界");
      assert(sealed_[kStock] * kB <= lib_days_ && sealed_[kBasket] * kB <= std::min(bdays_, lib_days_) && "price/head.bin: 封块数越界");
    }
    if (!(fits() && rows(kStock) <= ax.size() && ax.hash_at(rows(kStock)) == prefix_ && open_side(kStock))) {
      head_ = want;
      wipe(dates);
    } else if (!open_side(kBasket)) {
      wipe_baskets();
    }
  }
  if (table_ != basket_table().hash()) // 篮子表变了: 篮子侧从头, 个股侧不动
    wipe_baskets();
  if (rows(kStock) < ax.size()) { // 资产轴长了: 新行的链自无起
    link_[kStock].resize(ax.size());
    prefix_ = ax.hash_at(rows(kStock));
    save_head();
  }
  axis_ = lib.axis;
}

template <Side S>
void PriceLib::commit(misc::Ymd d, DayBuild &b, std::vector<Link> &z) {
  const size_t i = days(S);
  assert(S == kStock || (i < dates_.size() && dates_[i] == d));
  for (size_t j = 0; j < b.keys.size(); ++j) {
    DayRec &r = b.recs[j];
    r.f = step(z[b.keys[j]], r.ref, r.close, origin<S>());
  }
  const std::filesystem::path f = disk::path(kDayFile[S], at_day(lib_, d));
  b.write(f);
  b.clear();
  store_->side[S].push_back({i, std::make_unique<Seg>(f)});
  if constexpr (S == kStock)
    dates_.push_back(d);
  else
    ++bdays_;
}

template <Side S>
void PriceLib::seal() {
  std::vector<PriceStore::Part> &ps = store_->side[S];
  size_t &n = sealed_[S];
  while ((n + 1) * kB <= std::min(days(S), lib_days_)) {
    const size_t i0 = n * kB, at = n; // 块前各段都是块 ⇒ 首个单日段在 ps[n]
    assert(ps[at].day == i0 && ps[at + kB - 1].day == i0 + kB - 1);
    std::vector<const Seg *> ds;
    for (size_t j = 0; j < kB; ++j)
      ds.push_back(ps[at + j].seg.get());
    const std::filesystem::path f = disk::path(kBlockFile[S], at_day(lib_, dates_[i0]));
    seal_block(f, ds);
    ++n;
    save_head();
    ps.erase(ps.begin() + static_cast<ptrdiff_t>(at), ps.begin() + static_cast<ptrdiff_t>(at + kB));
    ps.insert(ps.begin() + static_cast<ptrdiff_t>(at), PriceStore::Part{i0, std::make_unique<Seg>(f)});
    for (size_t j = 0; j < kB; ++j)
      disk::remove(kDayFile[S], at_day(lib_, dates_[i0 + j]));
  }
}

void PriceLib::cut(size_t D) {
  assert(D >= lib_days_ && D <= dates_.size());
  const std::vector<misc::Ymd> gone(dates_.begin() + static_cast<ptrdiff_t>(D), dates_.end());
  dates_.resize(D), fund_days_ = std::min(fund_days_, D - lib_days_), bdays_ = std::min(bdays_, D);
  save_head();
  for (std::vector<PriceStore::Part> &ps : store_->side)
    while (ps.back().day >= D)
      ps.pop_back();
  for (const misc::Ymd d : gone)
    for (const disk::Node n : kDayFile)
      disk::remove(n, at_day(lib_, d));
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
  const bool fresh = !grow && keep == tail.size() && dates_.size() == lib_days_ + keep && bdays_ == dates_.size();
  if (fresh && at_ == stamp())
    return true;
  at_ = {};
  const std::shared_ptr<const pool::Picks> pk = pool::picks(lib, &quit_);
  if (!pk || pk->tape->dates != dates) // 库在落新日: 下趟再补
    return false;
  if (dates_.size() > lib_days_ + keep)
    cut(lib_days_ + keep);
  // 进度一次定总量: 个股库段新日 + 补段要算的日 + 篮子侧要合成的日 (日脊终长 = 库日轴 + 补段)
  begin(Phase::Loading, (grow ? dates.size() - lib_days_ : 0) + (tail.size() - keep) + (dates.size() + tail.size() - bdays_));

  // 个股库段一趟 (lib_pass): 各日的件在并行段各自填, 串行段按日序过链落盘; 每批落一次头 (满块即封)
  if (grow) {
    FeatureRead rd(lib, &quit_);
    std::vector<DayBuild> bs(static_cast<size_t>(pool_.threads()));
    const auto each = [&](size_t k, const LibDay &src) { bs[k].put_stocks(src); };
    const auto chain = [&](size_t i, size_t k) {
      assert(dates_.size() == i);
      commit<kStock>(dates[i], bs[k], link_[kStock]);
      publish(1);
    };
    const auto batch = [&] {
      lib_days_ = dates_.size();
      seal<kStock>();
      save_head();
    };
    if (!lib_pass(lib.axis, rd, dates, lib_days_, pool_, quit_, each, chain, batch)) {
      if (rd.stale()) // 库在重算: 下次从头核对
        head_ = {};
      return false;
    }
  }
  if (!fill(cat, tail, F, keep) || !fold(*pk))
    return false;
  drop_archived(dates_[lib_days_ + fund_days_ - 1]);
  at_ = stamp();
  bind_(Handoff{mem_, level_});
  return true;
}

bool PriceLib::fill(const fundamental::Catalog &cat, const std::vector<misc::Ymd> &tail, size_t F, size_t keep) {
  const size_t N = rows(kStock), T = tail.size(), L = lib_days_;
  assert(F <= T && keep <= T && dates_.size() == L + keep);
  std::vector<Link> z = link_[kStock];
  for (size_t i = L; i < L + keep; ++i) // 留着的日: 链照盘上的根重放
    rechain<kStock>(*store_, i, z);
  if (keep < T) {
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
      works.push_back(std::make_unique<TailWork>());
    // 每批 W 日: 并行读源合成 → 串行过链落盘
    for (size_t k0 = keep; k0 < T; k0 += W) {
      if (quit_.load(std::memory_order_relaxed))
        return false;
      const size_t n = std::min(W, T - k0);
      pool_.tasks(static_cast<int>(n), [&](int j, int) {
        const size_t k = k0 + static_cast<size_t>(j);
        TailWork &w = *works[static_cast<size_t>(j)];
        if (k < F) {
          w.bd.load(cat, ids, w.xs, tail[k]);
          w.b.put_stocks(FundDay{tail[k], std::span<const Candle>(px.data() + (k - keep) * N, N), w.bd});
        } else {
          archived_day(tail[k], N, w.spx, w.sbars);
          w.b.put_stocks(SinaDay{tail[k], w.spx, w.sbars});
        }
      });
      for (size_t j = 0; j < n; ++j) {
        commit<kStock>(tail[k0 + j], works[j]->b, z);
        publish(1);
      }
    }
    fund_days_ = F;
    save_head();
  }
  return true;
}

bool PriceLib::fold(const pool::Picks &pk) {
  const size_t D = dates_.size(), L = lib_days_, N = rows(kStock), NB = rows(kBasket);
  assert(bdays_ >= 1 && bdays_ <= D);
  Members tm; // 补段的成员: 库末日之后冻结, 逐日过 drift (漂移比 = 个股侧当日的 收 / 前收; 无日根 1)
  members(pk, axis_, cal_->next(dates_[L - 1]), tm);
  std::vector<float> rel(N);
  const auto pass = [&](size_t i) {
    std::ranges::fill(rel, 1.f);
    const auto [sg, j] = store_->at(kStock, i);
    for (const RowAt &r : sg->rows())
      if (r.day >> j & 1)
        rel[r.key] = rel_of(sg->day(r, j));
    drift(tm, dates_[i], cal_->next(dates_[i]), rel);
  };
  std::vector<Link> z = link_[kBasket]; // 补段的链 (库段的链落在 link_)
  for (size_t i = L; i < bdays_; ++i) { // 留着的补段日: 链 / 成员照盘上的根重放
    rechain<kBasket>(*store_, i, z);
    pass(i);
  }
  if (bdays_ < D) {
    const size_t W = std::min(static_cast<size_t>(pool_.threads()), D - bdays_);
    std::vector<std::unique_ptr<FoldWork>> works;
    for (size_t j = 0; j < W; ++j)
      works.push_back(std::make_unique<FoldWork>(NB));
    // 每批 W 日: 串行接补段日的成员 → 并行合成 (库段日各取选股表) → 串行过链落盘
    while (bdays_ < D) {
      if (quit_.load(std::memory_order_relaxed))
        return false;
      const size_t i0 = bdays_, n = std::min(W, D - i0);
      for (size_t j = 0; j < n; ++j)
        if (const size_t i = i0 + j; i >= L) {
          works[j]->lane.mem = tm;
          pass(i);
        }
      pool_.tasks(static_cast<int>(n), [&](int jj, int) {
        const size_t j = static_cast<size_t>(jj), i = i0 + j;
        FoldWork &w = *works[j];
        if (i < L)
          members(pk, axis_, dates_[i], w.lane.mem);
        const auto [sg, d] = store_->at(kStock, i);
        w.lane.run(SegDay{dates_[i], *sg, d});
        w.b.put_baskets(w.lane.day);
      });
      for (size_t j = 0; j < n; ++j) {
        const size_t i = i0 + j;
        commit<kBasket>(dates_[i], works[j]->b, i < L ? link_[kBasket] : z);
        if (i + 1 == L)
          z = link_[kBasket];
        publish(1);
      }
      seal<kBasket>();
      save_head();
    }
  }
  const std::vector<Link> &e = D > L ? z : link_[kBasket];
  level_.resize(NB);
  for (size_t b = 0; b < NB; ++b)
    level_[b] = e[b].f > 0 ? e[b].f * e[b].ref : kBase;
  mem_ = std::move(tm);
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
  v->tails = tails_, v->census = census_, v->equity = equity_;
  auto st = std::make_shared<Tape>();
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
  if (o && same_stocks && same_baskets && o->tape[kAccount] == account_ && o->tails == tails_ && o->census == census_)
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
  for (auto p = store_->side[kStock].rbegin(); p != store_->side[kStock].rend(); ++p) {
    const Seg &sg = *p->seg;
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
  baskets_ = std::move(b);
  census_ = std::make_shared<const Census>(census(mem_, rows(kBasket)));
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
    for (const PriceStore::Part &p : store_->side[S])
      if (p.day + p.seg->days() > from)
        if (const RowAt *r = p.seg->row(ref.key)) {
          p.seg->will(*r, minute);
          on.emplace_back(p.seg.get(), r), day0.push_back(p.day);
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
  ledger_ = ledger, spot_ = spot, equity_ = {};
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
    se->daily.reserve(ds.size()), se->adj.reserve(ds.size()), se->px.reserve(ds.size() * kSlots * Series::kFields);
    Bars b;
    std::array<float, kSlots> path;
    std::array<double, kSlots> idx;
    for (size_t k = 0; k < ds.size(); ++k) {
      const misc::Ymd date = dates_[i0 + k];
      const account::Day &d = ds[k];
      // 一只当日的逐槽收 (closes; 历史 / 当日同一份); 当日无分钟 → 空
      const auto path_of = [&](const std::string &code) -> std::span<const float> {
        const auto it = held.find(code);
        if (it == held.end())
          return {};
        const Series &h = it->second;
        const size_t j = h.find(date);
        if (j == h.daily.size() || !h.bars(j, b))
          return {};
        closes(b, h.daily[j].ref, path);
        return path;
      };
      if (k > 0)
        account::intraday(d, ds[k - 1].nav, ds[k - 1].index, path_of, idx);
      else
        idx.fill(d.index);
      se->push(path_day(date, static_cast<float>(k > 0 ? ds[k - 1].index : 1.0), idx), 1.0);
    }
    t->list[0] = std::move(se);
    equity_.nav = ds.back().nav;
    equity_.after = account::flow_after(l.rows, ds.back().date);
    for (const account::Day &d : ds)
      equity_.in += d.flow;
  }
  account_ = std::move(t);
}

} // namespace GUI::Quote
