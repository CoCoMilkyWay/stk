#include "gui/task_database/services/IndexService.hpp"

#include "features/Method/Fundamental.hpp" // fund::SW2021_L1_NAMES
#include "features/Storage/FeatureRead.hpp"
#include "shared/AssetAxis.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <cctype>
#include <cmath>
#include <string_view>

namespace GUI::Database {

namespace {

constexpr size_t kS = kSlots;
constexpr size_t kAuctionRows = 15; // L_1min 行 [0, 15) = 09:15–09:29 竞价段, 与行 15 (09:30 那分钟) 同归首根; 其后行 t → 槽 t − 15

// all Bar 字段表里 code 的列下标 (落盘变换须是 tf)
size_t col_of(size_t lvl, std::string_view code, ts::TfId tf) {
  const LevelInfo &li = level_info(TierId::Bar, lvl);
  for (size_t i = 0; i < li.field_count; ++i)
    if (code == li.fields[i].code) {
      assert(li.fields[i].ts_tf == tf && "all.bar: 列的落盘变换与读法不符");
      return i;
    }
  assert(false && "all.bar: 字段表里没有这一列");
  return 0;
}

float val(const FeatureRead::DayColumns &dc, size_t t, size_t i, size_t a) {
  const float x = static_cast<float>(dc.get(t, i, a));
  return std::isfinite(x) ? x : 0.f;
}

// 一日的 L_1day 收盘行 (A 轴); 缺 / 非有限 → 0
enum DayCol : size_t { kCs,
                       kTs,
                       kOpen,
                       kHigh,
                       kLow,
                       kClose,
                       kAmount,
                       kAdj,
                       kShares,
                       kInd,
                       kPools }; // 子池 p ∈ [1, NP) 的池列 = kPools + p − 1
struct Row {
  std::vector<float> px[4], amount, adj, shares; // 开 高 低 收
  std::vector<uint8_t> live, trade, ind;
  std::vector<uint8_t> pool; // [(p − 1) · A + a]
};

void read_row(const FeatureRead::DayColumns &dc, size_t A, size_t NP, Row &r) {
  for (std::vector<float> &v : r.px)
    v.resize(A);
  r.amount.resize(A), r.adj.resize(A), r.shares.resize(A);
  r.live.resize(A), r.trade.resize(A), r.ind.resize(A);
  r.pool.resize((NP - 1) * A);
  for (size_t a = 0; a < A; ++a) {
    r.live[a] = val(dc, 0, kCs, a) > 0.5f;
    r.trade[a] = val(dc, 0, kTs, a) > 0.5f;
    for (size_t j = 0; j < 4; ++j)
      r.px[j][a] = val(dc, 0, kOpen + j, a);
    r.amount[a] = std::expm1(val(dc, 0, kAmount, a));
    r.adj[a] = val(dc, 0, kAdj, a);
    r.shares[a] = val(dc, 0, kShares, a);
    const float k = val(dc, 0, kInd, a);
    r.ind[a] = k >= 1.f && k < static_cast<float>(fund::SW2021_L1_COUNT) ? static_cast<uint8_t>(k + 0.5f) : 0;
    for (size_t p = 1; p < NP; ++p)
      r.pool[(p - 1) * A + a] = val(dc, 0, kPools + p - 1, a) > 0.5f;
  }
}

// 收盘行 r 定次日各篮子成员 (Weight::i = 池轴下标) / 权重. 篮子序: 全A 等权 0, 全A 加权 1, 子池 p → 1 + p, 行业 k → NP + k
void members(const Row &r, size_t A, size_t NP, std::vector<std::vector<QuoteService::Weight>> &mem) {
  for (std::vector<QuoteService::Weight> &m : mem)
    m.clear();
  for (uint32_t a = 0; a < A; ++a) {
    const float c = r.px[3][a];
    if (!(c > 0))
      continue;
    const float cap = c * r.shares[a];
    if (r.live[a]) {
      mem[0].push_back({a, 1.f});
      if (cap > 0) {
        mem[1].push_back({a, cap});
        if (r.ind[a])
          mem[NP + r.ind[a]].push_back({a, cap});
      }
    }
    for (size_t p = 1; p < NP; ++p)
      if (r.pool[(p - 1) * A + a])
        mem[1 + p].push_back({a, 1.f});
  }
}

// 全局轴码 "600000.SH" → 新浪 "sh600000"
std::string sina_sym(const std::string &code) {
  const size_t dot = code.find('.');
  assert(dot != std::string::npos);
  std::string s = code.substr(dot + 1) + code.substr(0, dot);
  std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return s;
}

} // namespace

bool IndexService::run(IndexRequest &req) {
  const FeatureLib &lib = req.lib;
  assert(lib.ready() && lib.id == kAllBar);
  FeatureRead rd(lib, &cancel_);
  std::vector<misc::Ymd> dates;
  for (auto it = lib.dates.rbegin(); it != lib.dates.rend() && dates.size() < kDailyDays + 2; ++it)
    if (rd.has_date(*it))
      dates.push_back(*it);
  std::reverse(dates.begin(), dates.end());
  auto out = std::make_shared<IndexData>();
  const auto publish = [&] {
    std::lock_guard<std::mutex> lock(x_.mutex);
    x_.gen = req.gen;
    x_.data = std::move(out);
    x_.publish();
    return true;
  };
  if (dates.size() < 3) // 首日只给基准, 次日起才有成员行: 落盘 (清单日期轴可能还在算) 不足 3 日 → 空发布
    return publish();

  const size_t A = lib.axis.size(), NP = pool_count(), N = dates.size(), NB = NP + fund::SW2021_L1_COUNT;
  std::vector<size_t> dcols = {col_of(L_1day, "cs_valid", ts::TfId::None), col_of(L_1day, "ts_valid", ts::TfId::None),
                               col_of(L_1day, "open_day", ts::TfId::None), col_of(L_1day, "high_day", ts::TfId::None),
                               col_of(L_1day, "low_day", ts::TfId::None), col_of(L_1day, "close_day", ts::TfId::None),
                               col_of(L_1day, "amount_day", ts::TfId::Log), col_of(L_1day, "adj_factor", ts::TfId::None),
                               col_of(L_1day, "total_shares", ts::TfId::None), col_of(L_1day, "ind_l1", ts::TfId::None)};
  assert(dcols.size() == kPools);
  for (PoolId p = 1; p < NP; ++p)
    dcols.push_back(pool_def(p).pool_col);
  const std::vector<size_t> mcols = {col_of(L_1min, "ts_valid", ts::TfId::None), col_of(L_1min, "open", ts::TfId::None),
                                     col_of(L_1min, "high", ts::TfId::None), col_of(L_1min, "low", ts::TfId::None),
                                     col_of(L_1min, "close", ts::TfId::None), col_of(L_1min, "amount", ts::TfId::Log)};
  FeatureRead::DayColumns dc, mc;
  dc.preallocate(A, L_1day, dcols.size());
  mc.preallocate(A, L_1min, mcols.size());

  out->last = dates.back();
  out->baskets.resize(NB);
  out->baskets[0].name = "全A等权";
  out->baskets[1].name = "全A加权";
  for (PoolId p = 1; p < NP; ++p)
    out->baskets[1 + p].name = pool_def(p).name_cn;
  for (size_t k = 1; k < fund::SW2021_L1_COUNT; ++k) {
    Basket &b = out->baskets[NP + k];
    b.name = fund::SW2021_L1_NAMES[k];
    b.industry = true;
  }

  Row rows[3]; // 第 i 日 = rows[i % 3]
  std::vector<std::vector<QuoteService::Weight>> mem(NB);
  std::vector<double> level(NB, kBase);
  std::vector<float> base(A), ratio(A * 4), mratio(A * kS * 4); // 基准 / 当日 开 高 低 收 比 / 分钟日各根 开 高 低 收 比
  std::vector<double> mamount(A * kS);
  std::vector<std::array<double, 5>> acc(kS); // 一个篮子一日各根: 开 高 低 收 (比 × 权) / 额
  x_.begin(Phase::Loading, N);
  for (size_t i = 0; i < N; ++i) {
    if (cancel_.load(std::memory_order_relaxed))
      return false;
    const misc::Ymd d = dates[i];
    rd.load_day_columns(d, dcols, dc);
    if (rd.stale())
      return false;
    Row &cur = rows[i % 3];
    read_row(dc, A, NP, cur);
    x_.publish(1);
    if (i < 2)
      continue;
    const Row &prev = rows[(i - 1) % 3], &pp = rows[(i - 2) % 3];
    members(prev, A, NP, mem);
    for (size_t a = 0; a < A; ++a) {
      const float c = prev.px[3][a];
      base[a] = c > 0 && pp.adj[a] > 0 && prev.adj[a] > 0 ? c * pp.adj[a] / prev.adj[a] : c;
      const float x = cur.px[3][a];
      const bool on = cur.trade[a] && x > 0 && base[a] > 0;
      for (size_t j = 0; j < 4; ++j)
        ratio[a * 4 + j] = on ? (cur.px[j][a] > 0 ? cur.px[j][a] : x) / base[a] : 1.f;
    }
    const bool minutes = i + kMinuteDays >= N;
    if (minutes) {
      rd.load_day_columns(d, mcols, mc);
      if (rd.stale())
        return false;
      for (size_t a = 0; a < A; ++a) {
        if (!(base[a] > 0))
          continue;
        float *r = mratio.data() + a * kS * 4;
        double *m = mamount.data() + a * kS;
        const float inv = 1.f / base[a];
        float c = 1.f;
        bool any = false; // 本根已有成交
        for (size_t t = 0; t < level_valid_rows(L_1min); ++t) {
          const size_t s = t <= kAuctionRows ? 0 : t - kAuctionRows;
          if (t == 0 || t > kAuctionRows) { // 新的一根: 先按上根收盘铺平
            r[s * 4 + 0] = r[s * 4 + 1] = r[s * 4 + 2] = r[s * 4 + 3] = c;
            m[s] = 0;
            any = false;
          }
          const float x = val(mc, t, 4, a);
          if (!(val(mc, t, 0, a) > 0.5f && x > 0))
            continue;
          const float o = val(mc, t, 1, a), h = val(mc, t, 2, a), l = val(mc, t, 3, a);
          float *q = r + s * 4;
          q[0] = any ? q[0] : (o > 0 ? o : x) * inv;
          q[1] = any ? std::max(q[1], (h > 0 ? h : x) * inv) : (h > 0 ? h : x) * inv;
          q[2] = any ? std::min(q[2], (l > 0 ? l : x) * inv) : (l > 0 ? l : x) * inv;
          q[3] = c = x * inv;
          m[s] += std::expm1(val(mc, t, 5, a));
          any = true;
        }
      }
    }
    for (size_t b = 0; b < NB; ++b) {
      double sw = 0, k[4] = {}, amt = 0;
      for (const QuoteService::Weight &w : mem[b]) {
        if (!(base[w.i] > 0))
          continue;
        sw += w.w;
        for (size_t j = 0; j < 4; ++j)
          k[j] += w.w * ratio[w.i * 4 + j];
        if (cur.trade[w.i])
          amt += cur.amount[w.i];
      }
      if (!(sw > 0))
        continue;
      Basket &bk = out->baskets[b];
      const double f = level[b] / sw;
      bk.daily.push_back({d, 0, static_cast<float>(k[0] * f), static_cast<float>(k[1] * f), static_cast<float>(k[2] * f),
                          static_cast<float>(k[3] * f), 0, amt});
      if (minutes) {
        std::fill(acc.begin(), acc.end(), std::array<double, 5>{});
        for (const QuoteService::Weight &w : mem[b]) {
          if (!(base[w.i] > 0))
            continue;
          const float *r = mratio.data() + w.i * kS * 4;
          const double *m = mamount.data() + w.i * kS;
          for (size_t s = 0; s < kS; ++s) {
            for (size_t j = 0; j < 4; ++j)
              acc[s][j] += w.w * r[s * 4 + j];
            acc[s][4] += m[s];
          }
        }
        std::vector<Candle> &day = bk.minutes.emplace_back();
        day.reserve(kS);
        for (int s = 0; s < kSlots; ++s) {
          const std::array<double, 5> &c = acc[static_cast<size_t>(s)];
          day.push_back({d, s, static_cast<float>(c[0] * f), static_cast<float>(c[1] * f), static_cast<float>(c[2] * f),
                         static_cast<float>(c[3] * f), 0, c[4]});
        }
      }
      level[b] = bk.daily.back().close;
    }
  }

  std::vector<uint32_t> qi(A); // 池轴 → 全市场下标
  for (size_t a = 0; a < A; ++a)
    qi[a] = live_.find(sina_sym(asset_axis().code(lib.axis.global(a))));
  members(rows[(N - 1) % 3], A, NP, mem);
  for (size_t b = 0; b < NB; ++b)
    for (const QuoteService::Weight &w : mem[b])
      if (qi[w.i] != ~0u)
        out->baskets[b].live.push_back({qi[w.i], w.w});
  return publish();
}

} // namespace GUI::Database
