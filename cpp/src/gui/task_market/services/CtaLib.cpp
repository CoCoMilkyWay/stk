#include "gui/task_market/services/CtaLib.hpp"

#include "features/Storage/FeatureLevels.hpp" // feature_storage_t
#include "features/Storage/FeatureLib.hpp"
#include "features/Storage/FeatureRead.hpp"
#include "gui/quote/BasketRow.hpp"
#include "misc/snap.hpp"
#include "shared/Basket.hpp" // basket_table

#include <algorithm>
#include <cassert>
#include <filesystem>
#include <fstream>
#include <memory>
#include <numeric>
#include <span>
#include <tuple>
#include <type_traits>

namespace GUI::Market {

namespace {

using Quote::kBasket;
using Quote::kStock;
using Quote::Side;

constexpr uint64_t kMagic = 0x5850415443544B53ULL;                                   // "SKTCTAPX" 小端
constexpr size_t kSlots = Quote::kSlots, kCell = kSlots * CtaLib::kFields;           // 一行一日的值数
constexpr disk::Node kFile[Quote::kSides] = {disk::LibCtaStock, disk::LibCtaBasket}; // [侧]

// 两侧的价: 个股 = 库存原价 (f16, 与库逐位一致), 篮子 = 日内点位 (f32)
template <Side S>
using Px = std::conditional_t<S == kStock, feature_storage_t, float>;

// 一行一日
template <Side S>
struct Rec {
  double f;       // 因子 (0 = 当日无根)
  Px<S> x[kCell]; // [槽][高 低 收] (无根 = NaN)
};

// 一侧 N 行 days 日的文件长
template <Side S>
size_t bytes(size_t N, size_t days) { return days * N * sizeof(Rec<S>); }

// 一侧一日的合成: 记录 (因子按日序补) + 前收 / 收盘
template <Side S>
struct Out {
  std::vector<Rec<S>> rec;      // [N]
  std::vector<float> pre, last; // [N] D 的前收 / 收盘 (0 = 当日无根)
};
using DayOut = std::tuple<Out<kStock>, Out<kBasket>>; // [侧]

// 两侧逐个: f.template operator()<侧>()
template <class F>
void each_side(F &&f) {
  f.template operator()<kStock>();
  f.template operator()<kBasket>();
}

using misc::snap::items;

// 一行一日的价 ← 分钟根 (无根的槽 = NaN)
template <Side S>
void put(Rec<S> &r, const Quote::Bars &b) {
  std::fill(r.x, r.x + kCell, static_cast<Px<S>>(cta::kNaN));
  for (size_t s = 0; s < kSlots; ++s)
    if (b.has[s]) {
      const Quote::Cell &c = b.cell[s];
      Px<S> *p = r.x + s * CtaLib::kFields;
      p[CtaLib::kHigh] = static_cast<Px<S>>(c.high), p[CtaLib::kLow] = static_cast<Px<S>>(c.low), p[CtaLib::kClose] = static_cast<Px<S>>(c.close);
    }
}

// 一根 (高 低 收) × 因子 f → 有效根: 收 ≤ 0 / NaN 不收; 高 / 低缺 → 收, 且包住收
template <class T>
void push(cta::Series &se, size_t t, const T *p, double f) {
  const float c = static_cast<float>(p[CtaLib::kClose]);
  if (!(c > 0))
    return;
  const float h = static_cast<float>(p[CtaLib::kHigh]), l = static_cast<float>(p[CtaLib::kLow]);
  const auto at = [f](float v) { return static_cast<float>(v * f); };
  se.c.push_back(at(c));
  se.h.push_back(at(h > 0 ? std::max(h, c) : c));
  se.l.push_back(at(l > 0 ? std::min(l, c) : c));
  se.t.push_back(static_cast<uint32_t>(t));
}

// 一侧第 row 行 (共 N 行) 的 D 日 → 有效根
template <Side S>
void load(const disk::Key &lib, size_t N, size_t D, size_t row, cta::Series &se) {
  std::ifstream f(disk::path(kFile[S], lib), std::ios::binary);
  Rec<S> r;
  for (size_t i = 0; i < D; ++i) {
    f.seekg(static_cast<std::streamoff>((i * N + row) * sizeof(Rec<S>)));
    f.read(reinterpret_cast<char *>(&r), sizeof(Rec<S>));
    assert(f && "cta/: 短于头");
    if (r.f > 0)
      for (size_t s = 0; s < kSlots; ++s)
        push(se, i * kSlots + s, r.x + s * CtaLib::kFields, r.f);
  }
}

// 一个线程的合成件 (逐日复用): 一日合成件 + D−1 / D 日根行 + 读件
struct Work {
  Work(size_t A, size_t B) : lane(B) {
    Quote::row_alloc(dc, A);
    Quote::minute_alloc(mc, A);
  }
  Quote::Lane lane;
  Quote::Row prev, cur;
  FeatureRead::DayColumns dc, mc; // 日根行 / 分钟的读
};

} // namespace

template <class Ar>
void CtaLib::snap(Ar &ar) {
  for (std::vector<uint32_t> &k : keys_)
    items(ar, k);
  items(ar, dates_);
  for (std::vector<Link> &l : link_)
    items(ar, l);
}

void CtaLib::save_head() {
  std::vector<std::byte> blob;
  misc::snap::Save ar(blob);
  ar(head_);
  snap(ar);
  disk::write(disk::LibCtaHead, lib_, {reinterpret_cast<const char *>(blob.data()), blob.size()});
}

bool CtaLib::sync(const FeatureLib &lib, const FeatureRead &rd, const pool::Picks &pk) {
  const std::vector<misc::Ymd> &dates = pk.tape->dates;
  assert(dates.size() >= 2);
  lib_ = lib_key(lib.id);
  const BasketTable &bt = basket_table();
  std::vector<uint32_t> baskets(bt.size());
  std::iota(baskets.begin(), baskets.end(), 0u);
  const Quote::PerSide<std::vector<uint32_t>> keys{lib.axis.ids, std::move(baskets)};
  const size_t NA = keys[kStock].size(), NB = keys[kBasket].size();
  const Head want{kMagic, bt.hash(), dates.front()};
  const auto fits = [&] { return head_ == want && keys_ == keys && pool::extends(dates_, dates); };
  if (!fits()) {
    head_ = {}, keys_ = {}, dates_.clear(), link_ = {};
    if (disk::exists(disk::LibCtaHead, lib_)) {
      const std::string blob = disk::read(disk::LibCtaHead, lib_);
      misc::snap::Load ar(std::as_bytes(std::span(blob)));
      ar(head_);
      if (head_ == want) {
        snap(ar);
        assert(ar.done() && "cta/head.bin: 文件尾有多余字节");
        assert(link_[kStock].size() == keys_[kStock].size() && link_[kBasket].size() == keys_[kBasket].size() && "cta/head.bin: 链长与键表不符");
      }
    }
    bool whole = fits();
    each_side([&]<Side S>() {
      std::error_code ec;
      const uintmax_t have = std::filesystem::file_size(disk::path(kFile[S], lib_), ec);
      whole = whole && !ec && have >= bytes<S>(keys_[S].size(), dates_.size());
    });
    if (!whole) {
      head_ = want, keys_ = keys, dates_ = {dates.front()};
      disk::remove(disk::LibCta, lib_);
      disk::ensure(disk::LibCta, lib_);
      each_side([&]<Side S>() {
        link_[S].assign(keys_[S].size(), Link{});
        std::ofstream f(disk::path(kFile[S], lib_), std::ios::binary | std::ios::trunc);
        const std::vector<Rec<S>> none(keys_[S].size(), Rec<S>{});
        f.write(reinterpret_cast<const char *>(none.data()), static_cast<std::streamsize>(none.size() * sizeof(Rec<S>)));
        assert(f && "cta/: 写失败");
      });
      save_head();
    }
    each_side([&]<Side S>() { std::filesystem::resize_file(disk::path(kFile[S], lib_), bytes<S>(keys_[S].size(), dates_.size())); }); // 头之后写了一半的块
  }
  if (dates_.size() == dates.size())
    return true;

  // dates[i] (i ≥ 已合成日数) 逐日: 按线程一日一件 (读 D−1 / D 日根行 + D 的 L_1min, 两侧同出) → 按日序过链落盘, 每批落一次头
  Quote::PerSide<std::ofstream> f;
  each_side([&]<Side S>() { f[S].open(disk::path(kFile[S], lib_), std::ios::binary | std::ios::app); });
  const size_t N = dates.size(), T = static_cast<size_t>(pool_.threads());
  std::vector<std::unique_ptr<Work>> works;
  for (size_t t = 0; t < T; ++t)
    works.push_back(std::make_unique<Work>(NA, NB));
  std::vector<DayOut> out(T);
  const std::vector<double> one(NB, 1.0);
  x_.begin(Phase::Loading, N - dates_.size());
  for (size_t i0 = dates_.size(); i0 < N; i0 += T) {
    if (cancel_.load(std::memory_order_relaxed) || rd.stale())
      break;
    const size_t n = std::min(T, N - i0);
    pool_.tasks(static_cast<int>(n), [&](int k, int tid) {
      Work &w = *works[static_cast<size_t>(tid)];
      Quote::Lane &L = w.lane;
      auto &[so, bo] = out[static_cast<size_t>(k)];
      const size_t i = i0 + static_cast<size_t>(k);
      const misc::Ymd d = dates[i];
      Quote::read_row(rd, dates[i - 1], w.dc, w.prev);
      Quote::read_row(rd, d, w.dc, w.cur);
      Quote::read_minutes(rd, d, w.mc);
      Quote::members(pk, lib.axis, d, L.mem);
      so.rec.assign(NA, {});
      so.pre.assign(w.prev.pre.begin(), w.prev.pre.end());
      so.last.assign(NA, 0.f);
      L.run(Quote::LibDay{d, w.prev, w.cur, &w.mc}, dates[i - 1], one, [&](size_t a, const Quote::Leg &g) {
        if (g.day && g.bars->has.any()) {
          so.last[a] = g.day->close;
          put(so.rec[a], *g.bars);
        }
      });
      bo.rec.assign(NB, {});
      bo.pre.assign(NB, 1.f);
      bo.last.assign(NB, 0.f);
      for (size_t b = 0; b < NB; ++b)
        if (const Quote::Day &y = L.day[b]; y.candle.date) {
          bo.last[b] = y.candle.close;
          put(bo.rec[b], y.bars);
        }
    });
    if (rd.stale())
      break;
    for (size_t k = 0; k < n; ++k) {
      each_side([&]<Side S>() {
        Out<S> &o = std::get<S>(out[k]);
        std::vector<Link> &ln = link_[S];
        for (size_t r = 0; r < o.rec.size(); ++r) {
          Link &z = ln[r];
          if (const float p = o.pre[r]; z.ref > 0 && p > 0)
            z.f *= z.ref / p, z.ref = p;
          if (o.last[r] > 0) {
            if (!(z.f > 0))
              z.f = 1.0;
            z.ref = o.last[r];
            o.rec[r].f = z.f;
          }
        }
        f[S].write(reinterpret_cast<const char *>(o.rec.data()), static_cast<std::streamsize>(o.rec.size() * sizeof(Rec<S>)));
      });
      dates_.push_back(dates[i0 + k]);
      x_.publish(1);
    }
    each_side([&]<Side S>() {
      f[S].flush();
      assert(f[S] && "cta/: 写失败");
    });
    save_head();
  }
  if (dates_.size() == N)
    return true;
  if (rd.stale()) // 库在重算: 下次从头核对
    head_ = {};
  return false;
}

bool CtaLib::series(Side side, uint32_t key, cta::Series &se) const {
  const std::vector<uint32_t> &ks = keys_[side];
  const size_t row = static_cast<size_t>(std::find(ks.begin(), ks.end(), key) - ks.begin());
  if (row == ks.size())
    return false;
  se = {};
  each_side([&]<Side S>() {
    if (S == side)
      load<S>(lib_, ks.size(), dates_.size(), row, se);
  });
  return true;
}

} // namespace GUI::Market