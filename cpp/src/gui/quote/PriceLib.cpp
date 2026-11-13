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
#include "live/Broker.hpp"                     // kMktSuffix / market_of
#include "misc/snap.hpp"
#include "shared/Basket.hpp" // basket_table

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <cmath>
#include <filesystem>
#include <mutex>
#include <numeric>
#include <string>
#include <type_traits>
#include <unordered_map>

namespace GUI::Quote {

namespace {

constexpr uint64_t kMagic = 0x3345434952504B53ULL; // "SKPRICE3" 小端
constexpr size_t kB = PriceLib::kBlock;
constexpr disk::Node kDayFile[kSides] = {disk::LibPriceStockDay, disk::LibPriceBasketDay}; // [侧]
constexpr disk::Node kMinFile[kSides] = {disk::LibPriceStockMin, disk::LibPriceBasketMin};

// 两侧的存价: 个股 = 原价 (f16, 与库逐位一致), 篮子 = 比 (f32)
template <Side S>
using Px = std::conditional_t<S == kStock, feature_storage_t, float>;
// 分钟根一槽的格 (Series::Field 的前缀): 高 低 收 额 (+ 篮子的净流入)
template <Side S>
constexpr size_t kCells = S == kStock ? Series::kAmount + 1 : Series::kNet + 1;

// 一行一日的日根: f = 因子 (0 = 无日根), 价 = 存价, ref = 前收 (存价口径: 个股 = 交易所前收, 篮子 = 1), flow = 资金流摘要
struct DayRec {
  double f, amount;
  float open, high, low, close, ref;
  DayFlow flow;
};
// 一行一日的分钟根: [槽][kCells] (无根 = NaN; 个股的额 = log1p)
template <Side S>
struct MinRec {
  Px<S> x[kSlots * kCells<S>];
};

// 一侧一种记录的块文件: [块][行][kBlock], 整文件读写映射; 日 i 行 r = at(i, r)
template <class R>
class Blocks {
public:
  Blocks() = default;
  Blocks(const Blocks &) = delete;
  Blocks &operator=(const Blocks &) = delete;
  ~Blocks() { unmap(); }

  static size_t bytes(size_t N, size_t days) { return (days + kB - 1) / kB * N * kB * sizeof(R); }
  // 映射 p (无则建): N 行, 长不足 days 日 → 扩
  void map(const std::filesystem::path &p, size_t N, size_t days) {
    unmap();
    N_ = N;
    const int fd = ::open(p.c_str(), O_RDWR | O_CREAT, 0644);
    assert(fd >= 0 && "price/: 打不开");
    struct stat sb{};
    [[maybe_unused]] const int st = ::fstat(fd, &sb);
    assert(st == 0);
    n_ = std::max(static_cast<size_t>(sb.st_size), bytes(N, days));
    if (static_cast<size_t>(sb.st_size) < n_) {
      [[maybe_unused]] const int t = ::ftruncate(fd, static_cast<off_t>(n_));
      assert(t == 0 && "price/: 扩不出来");
    }
    if (n_ > 0) {
      void *m = ::mmap(nullptr, n_, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
      assert(m != MAP_FAILED && "price/: 映射失败");
      ::madvise(m, n_, MADV_RANDOM);
      base_ = static_cast<R *>(m);
    }
    ::close(fd);
  }
  void unmap() {
    if (base_)
      ::munmap(base_, n_);
    base_ = nullptr, n_ = 0;
  }
  R &at(size_t i, size_t r) { return base_[index(i, r)]; }
  const R &at(size_t i, size_t r) const { return base_[index(i, r)]; }
  // 行 r 的日 [lo, hi) 先一并发出读 (逐块一段连续)
  void will(size_t r, size_t lo, size_t hi) const {
    static const size_t pg = static_cast<size_t>(::sysconf(_SC_PAGESIZE));
    for (size_t b = lo / kB; b * kB < hi; ++b) {
      const uintptr_t p0 = reinterpret_cast<uintptr_t>(&at(std::max(lo, b * kB), r)), p1 = reinterpret_cast<uintptr_t>(&at(std::min(hi, (b + 1) * kB) - 1, r) + 1);
      ::madvise(reinterpret_cast<void *>(p0 / pg * pg), p1 - p0 / pg * pg, MADV_WILLNEED);
    }
  }

private:
  size_t index(size_t i, size_t r) const {
    assert(base_ && r < N_ && index_end(i) <= n_ / sizeof(R));
    return (i / kB * N_ + r) * kB + i % kB;
  }
  size_t index_end(size_t i) const { return (i / kB + 1) * N_ * kB; }
  R *base_ = nullptr;
  size_t n_ = 0, N_ = 0;
};

// 两侧逐个: f.template operator()<侧>()
template <class F>
void each_side(F &&f) {
  f.template operator()<kStock>();
  f.template operator()<kBasket>();
}

using misc::snap::items;

// 分钟根 → 一行一日 (b = nullptr / 无根的槽 = NaN)
template <Side S>
void put(MinRec<S> &r, const Bars *b) {
  constexpr size_t C = kCells<S>;
  std::fill(std::begin(r.x), std::end(r.x), static_cast<Px<S>>(NAN));
  if (!b)
    return;
  for (size_t s = 0; s < static_cast<size_t>(kSlots); ++s)
    if (b->has[s]) {
      const Cell &c = b->cell[s];
      Px<S> *p = r.x + s * C;
      p[Series::kHigh] = static_cast<Px<S>>(c.high), p[Series::kLow] = static_cast<Px<S>>(c.low), p[Series::kClose] = static_cast<Px<S>>(c.close);
      if constexpr (S == kStock)
        p[Series::kAmount] = static_cast<Px<S>>(static_cast<float>(std::log1p(c.amount)));
      else
        p[Series::kAmount] = static_cast<float>(c.amount), p[Series::kNet] = static_cast<float>(b->net[s]);
    }
}

// 一日根 → 一行一日 (因子待过链: 有日根先记 1); ref = 存价口径的前收
DayRec day_rec(const Cell *c, float ref, DayFlow flow = {}) {
  if (!c)
    return {0, 0, 0, 0, 0, 0, ref, {}};
  return {1, c->amount, c->open, c->high, c->low, c->close, ref, flow};
}
// 一行一日的 收 / 前收 (无日根 1): 成员过日的漂移比
float rel_of(const DayRec &r) { return r.f > 0 && r.ref > 0 ? r.close / r.ref : 1.f; }

// 补段一日的件 (逐批复用): 合成件 + 源的读 (基本面段 = bar1m, 新浪段 = 归档的日根 / 分钟根 [库轴])
struct TailWork {
  explicit TailWork(size_t baskets) : lane(baskets) {}
  Lane lane;
  Bar1mDay bd;
  fundamental::Scratch xs;
  std::vector<Candle> spx;
  std::vector<Bars> sbars;
};

// 一行一日过链 z: base = 当日前收 (存价口径), close = 当日收 (0 = 无日根); origin = 首个有根日的因子. 返回当日因子 (0 = 无日根)
double step(PriceLib::Link &z, float base, float close, double origin) {
  if (z.ref > 0 && base > 0)
    z.f *= z.ref / base, z.ref = base;
  if (!(close > 0))
    return 0;
  if (!(z.f > 0))
    z.f = origin;
  z.ref = close;
  return z.f;
}

// 存价 → 有效价的倍数: 个股 1 (原价), 篮子 = 因子
template <Side S>
double scale_of(const DayRec &r) { return S == kBasket ? r.f : 1.0; }

template <Side S>
Candle candle_of(const DayRec &r, misc::Ymd d) {
  const double a = scale_of<S>(r);
  const auto at = [a](float v) { return static_cast<float>(v * a); };
  return {{at(r.open), at(r.high), at(r.low), at(r.close), 0, r.amount}, d, at(r.ref)};
}

// 一行一日 → 一日分钟格 o [kSlots][Series::kFields] (价 = 有效价; 个股的净流入 = 本只分钟涨跌法, 首根对前收)
template <Side S>
void slots_of(const MinRec<S> &m, const DayRec &r, float *o) {
  constexpr size_t C = kCells<S>, F = Series::kFields;
  const double a = scale_of<S>(r);
  double px = r.ref;
  int dir = 0;
  for (size_t s = 0; s < static_cast<size_t>(kSlots); ++s) {
    const Px<S> *p = m.x + s * C;
    float *q = o + s * F;
    const float c = static_cast<float>(p[Series::kClose]);
    if (!(c > 0)) {
      std::fill_n(q, F, NAN);
      continue;
    }
    q[Series::kHigh] = static_cast<float>(static_cast<float>(p[Series::kHigh]) * a);
    q[Series::kLow] = static_cast<float>(static_cast<float>(p[Series::kLow]) * a);
    q[Series::kClose] = static_cast<float>(c * a);
    if constexpr (S == kStock) {
      const double amt = std::expm1(static_cast<double>(static_cast<float>(p[Series::kAmount])));
      dir = tick_dir(c, px, dir), px = c;
      q[Series::kAmount] = static_cast<float>(amt), q[Series::kNet] = static_cast<float>(dir * amt);
    } else {
      q[Series::kAmount] = p[Series::kAmount], q[Series::kNet] = p[Series::kNet];
    }
  }
}

} // namespace

// 四个块文件的映射
struct PriceFiles {
  PerSide<Blocks<DayRec>> day;
  Blocks<MinRec<kStock>> stock;
  Blocks<MinRec<kBasket>> basket;
  template <Side S>
  auto &min() {
    if constexpr (S == kStock)
      return stock;
    else
      return basket;
  }
  template <Side S>
  const auto &min() const {
    if constexpr (S == kStock)
      return stock;
    else
      return basket;
  }
  // 四文件映射到至少 days 日 (N = 两侧行数)
  void map(const disk::Key &lib, const PerSide<size_t> &N, size_t days) {
    each_side([&]<Side S>() {
      day[S].map(disk::path(kDayFile[S], lib), N[S], days);
      min<S>().map(disk::path(kMinFile[S], lib), N[S], days);
    });
  }
  // 第 i 日个股一行 ← 源的一只
  void put_stock(size_t i, size_t a, const Leg &g) {
    const Bars *b = g.day ? g.bars : nullptr;
    DayFlow fl;
    if (b) {
      std::array<double, kSlots> net;
      net_flow(*b, g.base, net);
      fl = day_flow(*b, net);
    }
    day[kStock].at(i, a) = day_rec(g.day, g.base, fl);
    put<kStock>(stock.at(i, a), b);
  }
  // 第 i 日篮子各行 ← Lane 的一日 (前日 = 1)
  void put_baskets(size_t i, std::span<const Day> ys) {
    for (size_t b = 0; b < ys.size(); ++b) {
      const Day &y = ys[b];
      const bool on = y.candle.date != 0;
      day[kBasket].at(i, b) = day_rec(on ? &y.candle : nullptr, 1.f, on ? day_flow(y.bars) : DayFlow{});
      put<kBasket>(basket.at(i, b), on ? &y.bars : nullptr);
    }
  }
};

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
  for (std::vector<uint32_t> &k : keys_)
    items(ar, k);
  items(ar, dates_);
  ar(lib_days_);
  ar(fund_days_);
  for (std::vector<Link> &l : link_)
    items(ar, l);
}

void PriceLib::save_head() {
  snap_write(disk::LibPriceHead, lib_, head_, [&](auto &ar) { snap(ar); });
}

size_t PriceLib::row_of(Ref ref) const {
  assert(ref.ok());
  const std::vector<uint32_t> &ks = keys_[ref.side];
  if (ref.side == kBasket)
    return std::min<size_t>(ref.key, ks.size());
  const auto it = std::ranges::lower_bound(ks, ref.key);
  return it != ks.end() && *it == ref.key ? static_cast<size_t>(it - ks.begin()) : ks.size();
}

PriceStamp PriceLib::stamp() const { return {axis_.hash, dates_.back(), dates_.size(), lib_days_, fund_days_}; }

void PriceLib::reopen(const FeatureLib &lib, const std::vector<misc::Ymd> &dates) {
  lib_ = lib_key(lib.id);
  const BasketTable &bt = basket_table();
  std::vector<uint32_t> baskets(bt.size());
  std::iota(baskets.begin(), baskets.end(), 0u);
  const PerSide<std::vector<uint32_t>> keys{lib.axis.ids, std::move(baskets)};
  const Head want{kMagic, bt.hash(), dates.front()};
  const auto fits = [&] { return head_ == want && keys_ == keys && pool::extends(std::span(dates_).first(lib_days_), dates); };
  if (files_ && fits())
    return;
  files_.reset();
  head_ = {}, keys_ = {}, dates_.clear(), lib_days_ = fund_days_ = 0, link_ = {}, at_ = {};
  if (snap_read(disk::LibPriceHead, lib_, want, [&](auto &ar) { snap(ar); })) {
    head_ = want;
    assert(lib_days_ >= 1 && lib_days_ + fund_days_ <= dates_.size() && "price/head.bin: 段日数越界");
    assert(link_[kStock].size() == keys_[kStock].size() && link_[kBasket].size() == keys_[kBasket].size() && "price/head.bin: 链长与键表不符");
  }
  bool whole = fits();
  each_side([&]<Side S>() {
    const auto have = [&](disk::Node n, size_t bytes) {
      std::error_code ec;
      const uintmax_t got = std::filesystem::file_size(disk::path(n, lib_), ec);
      return !ec && got >= bytes;
    };
    const size_t N = keys_[S].size(), D = dates_.size();
    whole = whole && have(kDayFile[S], Blocks<DayRec>::bytes(N, D)) && have(kMinFile[S], Blocks<MinRec<S>>::bytes(N, D));
  });
  if (!whole) {
    head_ = want, keys_ = keys, dates_ = {dates.front()}, lib_days_ = 1, fund_days_ = 0;
    each_side([&]<Side S>() { link_[S].assign(keys_[S].size(), Link{}); });
    disk::remove(disk::LibPrice, lib_);
    disk::ensure(disk::LibPrice, lib_);
    save_head();
  }
  axis_ = lib.axis;
  files_ = std::make_unique<PriceFiles>();
  files_->map(lib_, {keys_[kStock].size(), keys_[kBasket].size()}, dates_.size());
}

void PriceLib::chain(size_t i, PerSide<std::vector<Link>> &z) {
  each_side([&]<Side S>() {
    for (size_t r = 0; r < keys_[S].size(); ++r) {
      DayRec &d = files_->day[S].at(i, r);
      d.f = step(z[S][r], d.ref, d.f > 0 ? d.close : 0.f, S == kStock ? 1.0 : kBase);
    }
  });
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
  if (dates_.size() > lib_days_ + keep) { // 补段截到留着的日 (盘上的头先不认截去的, 再改写其块)
    dates_.resize(lib_days_ + keep), fund_days_ = std::min(fund_days_, keep);
    save_head();
  }

  // 库段一趟 (lib_pass): 个股的根在并行段逐只写 (各日各行互不相干), 篮子的在串行段取 Lane 的; 按日序过链, 每批落一次头
  if (grow) {
    files_->map(lib_, {keys_[kStock].size(), keys_[kBasket].size()}, dates.size());
    const auto each = [&](size_t i, size_t a, const Leg &g) { files_->put_stock(i, a, g); };
    const auto chain = [&](size_t i, size_t, LibWork &w) {
      files_->put_baskets(i, w.lane.day);
      this->chain(i, link_);
      dates_.push_back(dates[i]);
    };
    const auto batch = [&] {
      lib_days_ = dates_.size();
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
  bind_(Handoff{axis_, mem_, level_});
  return true;
}

bool PriceLib::fill(const pool::Picks &pk, const fundamental::Catalog &cat, const std::vector<misc::Ymd> &tail, size_t F, size_t keep) {
  const size_t A = keys_[kStock].size(), NB = keys_[kBasket].size(), T = tail.size(), L = lib_days_;
  assert(F <= T && keep <= T && dates_.size() == L + keep);
  Members mem;
  members(pk, axis_, cal_->next(dates_[L - 1]), mem);
  level_.resize(NB);
  for (size_t b = 0; b < NB; ++b) {
    const Link &z = link_[kBasket][b];
    level_[b] = z.f > 0 ? z.f * z.ref : kBase;
  }
  PerSide<std::vector<Link>> z = link_;
  std::vector<float> rel(A); // [库轴] 当日 收 / 前收 (无日根 1)
  // 第 i 日 (根已落盘) 过链, 篮子末日点位跟上
  const auto settle = [&](size_t i) {
    chain(i, z);
    for (size_t b = 0; b < NB; ++b)
      if (const DayRec &r = files_->day[kBasket].at(i, b); r.f > 0)
        level_[b] = r.f * r.close;
  };
  for (size_t k = 0; k < keep; ++k) { // 留着的日: 链 / 成员照盘上的根重放
    const size_t i = L + k;
    settle(i);
    for (size_t a = 0; a < A; ++a)
      rel[a] = rel_of(files_->day[kStock].at(i, a));
    drift(mem, tail[k], cal_->next(tail[k]), rel);
  }
  if (keep < T) {
    files_->map(lib_, {A, NB}, L + T);
    begin(Phase::Loading, T - keep);
    fundamental::Ids ids(fundamental::Ids::Cover::Part);
    for (size_t a = 0; a < A; ++a)
      ids.add(asset_axis().code(axis_.global(a)));
    // 基本面段要算的日的日根: bar1d 行 [日 − keep][库轴] (无行 → 收 0)
    std::vector<Candle> px(F > keep ? (F - keep) * A : 0);
    if (F > keep) {
      fundamental::Scratch xs;
      const auto f0 = tail.begin() + static_cast<std::ptrdiff_t>(keep), f1 = tail.begin() + static_cast<std::ptrdiff_t>(F);
      for (const fundamental::Shard &sh : cat.shards(kBar1d)) {
        if (sh.ym < misc::ym_of(*f0) || sh.ym > misc::ym_of(f1[-1]))
          continue;
        each_daily(xs, sh, ids, [&](uint32_t a, const Candle &c) {
          const auto it = std::lower_bound(f0, f1, c.date);
          if (it != f1 && *it == c.date)
            px[static_cast<size_t>(it - f0) * A + a] = c;
        });
      }
    }
    const size_t W = std::min(static_cast<size_t>(pool_.threads()), T - keep);
    std::vector<std::unique_ptr<TailWork>> works;
    for (size_t j = 0; j < W; ++j)
      works.push_back(std::make_unique<TailWork>(NB));
    const auto day_px = [&](size_t k, const TailWork &w) {
      return k < F ? std::span<const Candle>(px.data() + (k - keep) * A, A) : std::span<const Candle>(w.spx);
    };
    // 每批 W 日: 并行读源 → 串行接成员 (只看日根) → 并行合成落盘 → 串行过链
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
          archived_day(tail[k], axis_, w.spx, w.sbars);
      });
      for (size_t j = 0; j < n; ++j) {
        const size_t k = k0 + j;
        TailWork &w = *works[j];
        w.lane.mem = mem;
        const std::span<const Candle> c = day_px(k, w);
        for (size_t a = 0; a < A; ++a)
          rel[a] = rel_of(day_rec(traded(c[a]) ? &c[a] : nullptr, c[a].ref));
        drift(mem, tail[k], cal_->next(tail[k]), rel);
      }
      pool_.tasks(static_cast<int>(n), [&](int j, int) {
        const size_t k = k0 + static_cast<size_t>(j), i = L + k;
        TailWork &w = *works[static_cast<size_t>(j)];
        const misc::Ymd prev = k > 0 ? tail[k - 1] : dates_[L - 1];
        const auto put = [&](size_t a, const Leg &g) { files_->put_stock(i, a, g); };
        if (k < F)
          w.lane.run(FundDay{tail[k], day_px(k, w), w.bd}, prev, put);
        else
          w.lane.run(SinaDay{tail[k], w.spx, w.sbars}, prev, put);
        files_->put_baskets(i, w.lane.day);
      });
      for (size_t j = 0; j < n; ++j) {
        settle(L + k0 + j);
        dates_.push_back(tail[k0 + j]);
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
  if (!book_ || book_->ledger != req.ledger || !(spot_ == req.spot))
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
  v->book = book_;
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
  if (o && same_stocks && same_baskets && o->book == v->book)
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
  book_ = nullptr;
  spine_ = std::make_shared<const std::vector<misc::Ymd>>(dates_);
  const size_t D = dates_.size();
  auto t = std::make_shared<Tails>();
  t->ids = keys_[kStock];
  const Blocks<DayRec> &sd = files_->day[kStock];
  Candle c[kTailDays];
  DayFlow fl[kTailDays];
  for (size_t r = 0; r < t->ids.size(); ++r) {
    size_t n = 0;
    for (size_t i = D; i-- > 0 && n < kTailDays;)
      if (const DayRec &x = sd.at(i, r); x.f > 0)
        c[n] = candle_of<kStock>(x, dates_[i]), fl[n++] = x.flow;
    for (size_t k = n; k-- > 0;)
      t->daily.v.push_back(c[k]), t->flow.v.push_back(fl[k]);
    t->daily.off.push_back(static_cast<uint32_t>(t->daily.v.size()));
    t->flow.off.push_back(static_cast<uint32_t>(t->flow.v.size()));
  }
  tails_ = std::move(t);
  auto b = std::make_shared<Tape>();
  b->keys = keys_[kBasket];
  b->list.resize(b->keys.size());
  pool_.tasks(static_cast<int>(b->keys.size()), [&](int j, int) {
    auto s = std::make_shared<Series>();
    series({kBasket, b->keys[static_cast<size_t>(j)]}, false, *s);
    b->list[static_cast<size_t>(j)] = std::move(s);
  });
  census(mem_, axis_, b->keys, *b);
  baskets_ = std::move(b);
}

bool PriceLib::series(Ref ref, bool minute, Series &out) const {
  const size_t row = row_of(ref), D = dates_.size();
  if (row == keys_[ref.side].size())
    return false;
  out.minute = minute;
  each_side([&]<Side S>() {
    if (S != ref.side)
      return;
    constexpr size_t W = static_cast<size_t>(kSlots) * Series::kFields;
    const Blocks<DayRec> &dr = files_->day[S];
    const auto &mr = files_->min<S>();
    dr.will(row, 0, D);
    if (minute) {
      mr.will(row, 0, D);
      out.px.reserve(D * W);
    }
    for (size_t i = 0; i < D; ++i) {
      const DayRec &r = dr.at(i, row);
      if (!(r.f > 0))
        continue;
      out.daily.push_back(candle_of<S>(r, dates_[i]));
      out.flow.push_back(r.flow);
      out.adj.push_back(S == kStock ? r.f : 1.0);
      if (minute) {
        out.px.resize(out.px.size() + W);
        slots_of<S>(mr.at(i, row), r, out.px.data() + out.px.size() - W);
      }
    }
  });
  out.px.shrink_to_fit();
  return true;
}

void PriceLib::replay(uint64_t ledger, const account::Spot &spot) {
  auto bk = std::make_shared<Book>();
  bk->ledger = ledger;
  spot_ = spot;
  const account::Ledger l = account::load();
  const size_t D = dates_.size();
  const size_t i0 = l.rows.empty() ? D : static_cast<size_t>(std::ranges::lower_bound(dates_, l.rows.front().date) - dates_.begin());
  if (i0 < D) {
    const AssetAxis &ax = asset_axis();
    std::unordered_map<std::string, size_t> row; // 代码 → 个股行 (库轴外的不在)
    for (const std::string &s : account::securities(l.rows))
      if (const size_t id = ax.find(s + live::kMktSuffix[static_cast<int>(live::market_of(s))]); id < ax.size())
        if (const size_t r = row_of({kStock, static_cast<uint32_t>(id)}); r < keys_[kStock].size())
          row.emplace(s, r);
    const Blocks<DayRec> &sd = files_->day[kStock];
    account::Closes px;
    for (const auto &[s, r] : row)
      for (size_t i = i0; i < D; ++i)
        if (const DayRec &x = sd.at(i, r); x.f > 0)
          px[s][dates_[i]] = x.close;
    const std::vector<account::Day> ds =
        account::replay(l.rows, l.rate, std::vector<misc::Ymd>(dates_.begin() + static_cast<ptrdiff_t>(i0), dates_.end()), px, spot);
    assert(ds.size() == D - i0 && "账户回放: 日数与日脊不符");
    auto se = std::make_shared<Series>();
    se->minute = true;
    se->px.resize(ds.size() * kSlots * Series::kFields);
    constexpr size_t C = kCells<kStock>;
    std::array<double, kSlots> nav;
    for (size_t k = 0; k < ds.size(); ++k) {
      const size_t i = i0 + k;
      const account::Day &d = ds[k];
      // 当日收后持仓按分钟收盘计价, 末槽对齐当日净值: nav_s = nav + Σ 股 × (分钟收_s − 末槽收)
      nav.fill(d.nav);
      for (const auto &[code, q] : d.hold) {
        const auto it = row.find(code);
        if (it == row.end())
          continue;
        const DayRec &x = sd.at(i, it->second);
        if (!(x.f > 0))
          continue;
        const MinRec<kStock> &m = files_->stock.at(i, it->second);
        std::array<float, kSlots> c;
        float at = x.ref;
        for (size_t s = 0; s < static_cast<size_t>(kSlots); ++s) {
          if (const float v = static_cast<float>(m.x[s * C + Series::kClose]); v > 0)
            at = v;
          c[s] = at;
        }
        for (size_t s = 0; s < static_cast<size_t>(kSlots); ++s)
          nav[s] += q * (c[s] - c[kSlots - 1]);
      }
      const double ref = k > 0 ? ds[k - 1].index : 1.0, nav0 = k > 0 ? ds[k - 1].nav : 0.0;
      float hi = static_cast<float>(ref), lo = hi;
      float *p = se->px.data() + k * kSlots * Series::kFields;
      for (size_t s = 0; s < static_cast<size_t>(kSlots); ++s) {
        const float x = static_cast<float>(k > 0 ? ref * account::growth(nav0, nav[s], d.flow) : d.index);
        float *q = p + s * Series::kFields;
        q[Series::kHigh] = q[Series::kLow] = q[Series::kClose] = x;
        q[Series::kAmount] = q[Series::kNet] = 0;
        hi = std::max(hi, x), lo = std::min(lo, x);
      }
      const float r = static_cast<float>(ref), x = static_cast<float>(d.index);
      se->daily.push_back({cell(r, std::max(hi, x), std::min(lo, x), x, 0, 0), dates_[i], r});
      se->flow.push_back({});
      se->adj.push_back(1.0);
    }
    bk->se = std::move(se);
    bk->nav = ds.back().nav;
    bk->after = account::flow_after(l.rows, ds.back().date);
    for (const account::Day &d : ds)
      bk->in += d.flow;
  }
  book_ = std::move(bk);
}

} // namespace GUI::Quote
