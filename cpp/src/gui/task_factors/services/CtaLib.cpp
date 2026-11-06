#include "gui/task_factors/services/CtaLib.hpp"

#include "features/Storage/FeatureLevels.hpp" // feature_storage_t
#include "features/Storage/FeatureRead.hpp"
#include "gui/task_database/services/BasketRow.hpp"
#include "misc/fs.hpp"
#include "misc/snap.hpp"
#include "shared/Basket.hpp" // basket_group / basket_count
#include "shared/FeatureLib.hpp"

#include <algorithm>
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <span>
#include <tuple>
#include <type_traits>

namespace GUI::Factors {

namespace {

using Database::kBasket;
using Database::kStock;
using Database::Side;

constexpr uint64_t kMagic = 0x5850415443544B53ULL;                             // "SKTCTAPX" 小端
constexpr size_t kSlots = Database::kSlots, kCell = kSlots * CtaLib::kFields;  // 一行一日的值数
constexpr const char *kFile[Database::kSides] = {"/stock.bin", "/basket.bin"}; // [侧]

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

// 定长元素的序列: 长度 + 逐个
template <class Ar, class T>
void items(Ar &ar, std::vector<T> &v) {
  uint64_t n = v.size();
  ar(n);
  if constexpr (Ar::kLoad)
    v.resize(n);
  for (T &x : v)
    ar(x);
}

// 一行一日的价 ← 分钟根 (无根的槽 = NaN)
template <Side S>
void put(Rec<S> &r, const Database::Bars &b) {
  std::fill(r.x, r.x + kCell, static_cast<Px<S>>(cta::kNaN));
  for (size_t s = 0; s < kSlots; ++s)
    if (b.has[s]) {
      const Database::Cell &c = b.cell[s];
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
void load(const std::string &dir, size_t N, size_t D, size_t row, cta::Series &se) {
  std::ifstream f(dir + kFile[S], std::ios::binary);
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

// 一个线程的合成件 (逐日复用)
struct Lane {
  Lane(size_t A, size_t B, size_t nd, size_t nm) : fold(B), day(B) {
    dc.preallocate(A, L_1day, nd);
    mc.preallocate(A, L_1min, nm);
  }
  FeatureRead::DayColumns dc, mc;
  Database::Row prev, cur; // D−1 / D 收盘行
  Database::Fold fold;
  std::vector<Database::Day> day; // [B]
  Database::Bars bars;            // 一只一日的分钟根
  Database::Members mem;
  std::vector<Database::Members::Weight> ws; // 一只滤到篮子行的所属线
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
  misc::atomic_write(dir_ + "/head.bin", reinterpret_cast<const char *>(blob.data()), blob.size());
}

bool CtaLib::sync(const FeatureLib &lib, const FeatureRead &rd, const std::vector<misc::Ymd> &dates) {
  assert(dates.size() >= 2);
  dir_ = lib.dir + "/cta";
  std::vector<uint32_t> lines;
  for (int g = 0; g < kBasketGroups; ++g)
    for (const size_t l : basket_group(static_cast<BasketGroup>(g)))
      lines.push_back(static_cast<uint32_t>(l));
  const Database::PerSide<std::vector<uint32_t>> keys{lib.axis.ids, std::move(lines)};
  const size_t NA = keys[kStock].size(), NB = keys[kBasket].size();
  const Head want{kMagic, concepts().hash, dates.front()};
  const auto fits = [&] {
    return head_ == want && keys_ == keys && !dates_.empty() && dates_.size() <= dates.size() && std::equal(dates_.begin(), dates_.end(), dates.begin());
  };
  if (!fits()) {
    head_ = {}, keys_ = {}, dates_.clear(), link_ = {};
    if (std::ifstream f(dir_ + "/head.bin", std::ios::binary); f) {
      const std::vector<char> blob((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
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
      const uintmax_t have = std::filesystem::file_size(dir_ + kFile[S], ec);
      whole = whole && !ec && have >= bytes<S>(keys_[S].size(), dates_.size());
    });
    if (!whole) {
      head_ = want, keys_ = keys, dates_ = {dates.front()};
      std::filesystem::remove_all(dir_);
      std::filesystem::create_directories(dir_);
      each_side([&]<Side S>() {
        link_[S].assign(keys_[S].size(), Link{});
        std::ofstream f(dir_ + kFile[S], std::ios::binary | std::ios::trunc);
        const std::vector<Rec<S>> none(keys_[S].size(), Rec<S>{});
        f.write(reinterpret_cast<const char *>(none.data()), static_cast<std::streamsize>(none.size() * sizeof(Rec<S>)));
        assert(f && "cta/: 写失败");
      });
      save_head();
    }
    each_side([&]<Side S>() { std::filesystem::resize_file(dir_ + kFile[S], bytes<S>(keys_[S].size(), dates_.size())); }); // 头之后写了一半的块
  }
  if (dates_.size() == dates.size())
    return true;

  // dates[i] (i ≥ 已合成日数) 逐日: 按线程一日一件 (读 D−1 / D 收盘行 + D 的 L_1min, 两侧同出) → 按日序过链落盘, 每批落一次头
  std::vector<uint32_t> line(basket_count(), static_cast<uint32_t>(NB)); // basket_line → 篮子行 (NB = 不在)
  for (size_t j = 0; j < NB; ++j)
    line[keys_[kBasket][j]] = static_cast<uint32_t>(j);
  Database::PerSide<std::ofstream> f;
  each_side([&]<Side S>() { f[S].open(dir_ + kFile[S], std::ios::binary | std::ios::app); });
  const size_t NP = pool_count(), N = dates.size(), T = static_cast<size_t>(pool_.threads());
  const std::vector<size_t> dcols = Database::row_cols(), mcols = Database::minute_cols();
  std::vector<std::unique_ptr<Lane>> lanes;
  for (size_t t = 0; t < T; ++t)
    lanes.push_back(std::make_unique<Lane>(NA, NB, dcols.size(), mcols.size()));
  std::vector<DayOut> out(T);
  const std::vector<double> one(NB, 1.0);
  x_.begin(Phase::Loading, N - dates_.size());
  for (size_t i0 = dates_.size(); i0 < N; i0 += T) {
    if (cancel_.load(std::memory_order_relaxed) || rd.stale())
      break;
    const size_t n = std::min(T, N - i0);
    pool_.tasks(static_cast<int>(n), [&](int k, int tid) {
      Lane &L = *lanes[static_cast<size_t>(tid)];
      auto &[so, bo] = out[static_cast<size_t>(k)];
      const size_t i = i0 + static_cast<size_t>(k);
      const misc::Ymd d = dates[i];
      rd.load_day_columns(dates[i - 1], dcols, L.dc);
      Database::read_row(L.dc, NA, NP, L.prev);
      rd.load_day_columns(d, dcols, L.dc);
      Database::read_row(L.dc, NA, NP, L.cur);
      rd.load_day_columns(d, mcols, L.mc);
      Database::members(L.prev, lib.axis, NP, L.mem);
      L.fold.open(Database::kSlots);
      so.rec.assign(NA, {});
      so.pre.assign(L.prev.pre.begin(), L.prev.pre.end());
      so.last.assign(NA, 0.f);
      for (size_t a = 0; a < NA; ++a) {
        const float c = L.cur.px[3][a];
        const bool on = L.cur.trade[a] && c > 0;
        const Database::Cell day = Database::cell(L.cur.px[0][a], L.cur.px[1][a], L.cur.px[2][a], c, 0, L.cur.amount[a]);
        Database::bars_of(L.mc, a, d, L.bars);
        if (on && L.bars.has.any()) {
          so.last[a] = c;
          put(so.rec[a], L.bars);
        }
        L.ws.clear();
        for (const Database::Members::Weight &w : L.mem.of(a))
          if (const uint32_t j = line[w.basket]; j < NB)
            L.ws.push_back({j, w.w});
        if (!L.ws.empty())
          L.fold.add({L.prev.pre[a], on ? &day : nullptr, &L.bars}, L.ws);
      }
      L.fold.finish(d, dates[i - 1], one, L.day);
      bo.rec.assign(NB, {});
      bo.pre.assign(NB, 1.f);
      bo.last.assign(NB, 0.f);
      for (size_t b = 0; b < NB; ++b)
        if (const Database::Day &y = L.day[b]; y.candle.date) {
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
      load<S>(dir_, ks.size(), dates_.size(), row, se);
  });
  return true;
}

} // namespace GUI::Factors
