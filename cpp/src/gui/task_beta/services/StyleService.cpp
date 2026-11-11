#include "gui/task_beta/services/StyleService.hpp"

#include "features/Storage/FeatureLib.hpp"
#include "misc/date.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <functional>
#include <limits>
#include <mutex>
#include <span>
#include <string>
#include <utility>

namespace GUI::Beta {

namespace {

constexpr float kNaN = std::numeric_limits<float>::quiet_NaN();

// 累计中位: 逐个 push, 随时取中位 (偶数个取中间两个的均值)
class RunMedian {
  std::vector<float> lo_, hi_; // 较小一半 (大顶堆) / 较大一半 (小顶堆); |lo| − |hi| ∈ {0, 1}

public:
  void push(float x) {
    if (lo_.empty() || x <= lo_.front())
      lo_.push_back(x), std::ranges::push_heap(lo_);
    else
      hi_.push_back(x), std::ranges::push_heap(hi_, std::greater{});
    if (lo_.size() > hi_.size() + 1) {
      std::ranges::pop_heap(lo_);
      hi_.push_back(lo_.back()), lo_.pop_back();
      std::ranges::push_heap(hi_, std::greater{});
    } else if (hi_.size() > lo_.size()) {
      std::ranges::pop_heap(hi_, std::greater{});
      lo_.push_back(hi_.back()), hi_.pop_back();
      std::ranges::push_heap(lo_);
    }
  }
  float mid() const { return lo_.size() > hi_.size() ? lo_.front() : 0.5f * (lo_.front() + hi_.front()); }
};

// 日收益 r / 日增仓量 buy (首个有仓日 first) 在区间 [d0, d1) 内: 各满一年的滚动窗的年化 / 夏普 → 平均 / 中位 + 累计中位夏普; 整段回撤 / 换手.
// 年化在对数空间平均 / 取中位再换回简单收益; 窗内各和 = 前缀和之差; 一年前的边界随收尾日单调右移
void fill(StyleLine &l, std::span<const misc::Ymd> dates, const std::vector<float> &r, const std::vector<float> &buy, size_t first, size_t d0, size_t d1) {
  const size_t D = dates.size(), f = std::max(first, d0 + 1);
  l.first = first;
  l.from = d1;
  l.med.assign(D, kNaN);
  l.stat = {};
  if (f >= d1)
    return;
  const auto years = [&](size_t base, size_t t) { return static_cast<double>((misc::to_days(dates[t]) - misc::to_days(dates[base])).count()) / 365.25; };
  // 前缀和 (下标 = 日 + 1; 起点前为 0): Σ ln(1 + r) / Σ r / Σ r²; 整段回撤 (峰自基日 = 1 起) / 增仓
  std::vector<double> G(d1 + 1, 0.0), S(d1 + 1, 0.0), SS(d1 + 1, 0.0);
  double nav = 1.0, peak = 1.0, mdd = 0.0, b = 0.0;
  for (size_t d = f; d < d1; ++d) {
    const double x = r[d];
    G[d + 1] = G[d] + std::log1p(x), S[d + 1] = S[d] + x, SS[d + 1] = SS[d] + x * x;
    nav *= 1.0 + x, peak = std::max(peak, nav), mdd = std::max(mdd, 1.0 - nav / peak);
    b += buy[d];
  }
  l.stat.mdd = static_cast<float>(mdd), l.stat.turn = static_cast<float>(b / years(f - 1, d1 - 1));
  RunMedian ann_m, shp_m; // 年化 = 对数年化
  double ann_s = 0.0, shp_s = 0.0;
  size_t ys = 0; // 首个晚于 dates[t] 前推一年的日 = 窗起
  for (size_t t = f; t < d1; ++t) {
    while (dates[ys] <= dates[t] - 10000)
      ++ys;
    if (ys < f)
      continue;
    if (l.from == d1)
      l.from = t;
    const double n = static_cast<double>(t + 1 - ys), y = years(ys - 1, t);
    const double m = (S[t + 1] - S[ys]) / n, sd = std::sqrt(std::max((SS[t + 1] - SS[ys]) / n - m * m, 0.0));
    const float a = static_cast<float>((G[t + 1] - G[ys]) / y), h = sd > 0.0 ? static_cast<float>(m / sd * std::sqrt(n / y)) : 0.0f;
    ann_s += a, shp_s += h;
    ann_m.push(a), shp_m.push(h);
    l.med[t] = shp_m.mid();
  }
  if (l.from == d1)
    return;
  const double k = static_cast<double>(d1 - l.from);
  l.stat.ann = static_cast<float>(std::expm1(ann_s / k)), l.stat.ann_med = std::expm1(ann_m.mid());
  l.stat.sharpe = static_cast<float>(shp_s / k), l.stat.sharpe_med = l.med[d1 - 1];
}

} // namespace

bool StyleService::publish(const StyleRequest &req, std::string lack, std::shared_ptr<StyleResult> out) {
  if (!out)
    out = std::make_shared<StyleResult>();
  out->gen = req.gen, out->grid = req.grid;
  out->lack = std::move(lack);
  std::lock_guard<std::mutex> lock(mutex);
  result = std::move(out);
  Progress::publish();
  return true;
}

std::shared_ptr<const pool::Ranks> StyleService::ranks(const FeatureLib &lib, pool::Style st) {
  const auto hit = std::find_if(keys_.begin(), keys_.end(), [&](const Key &k) { return k.style == st; });
  const Key k = hit != keys_.end() ? *hit : Key{st, std::make_shared<pool::Ranks>()};
  if (hit != keys_.end())
    keys_.erase(hit);
  if (k.rk->rows() < tape_->days()) {
    auto rk = std::make_shared<pool::Ranks>(*k.rk);
    begin(Phase::Loading, tape_->days() - rk->rows());
    if (!pool::rank_rows(lib, *tape_, st, pool_, cancel_, [this] { Progress::publish(1); }, *rk))
      return nullptr;
    keys_.insert(keys_.begin(), Key{st, std::move(rk)});
  } else
    keys_.insert(keys_.begin(), k);
  if (keys_.size() > kKeep)
    keys_.pop_back();
  return keys_.front().rk;
}

bool StyleService::run(StyleRequest &req) {
  const pool::AllBar ab = pool::all_bar("风格篮子由 all.bar 特征库选出");
  if (!ab.lack.empty())
    return publish(req, ab.lack);
  begin(Phase::Loading, ab.dates.size() - (tape_ && tape_->axis == ab.lib.axis.hash && pool::extends(tape_->dates, ab.dates) ? tape_->days() : 0));
  const std::shared_ptr<const pool::Tape> t = pool::tape(ab.lib, &cancel_, [this] { Progress::publish(1); });
  if (!t)
    return false;
  if (t != tape_) {
    if (!(tape_ && tape_->axis == t->axis && pool::extends(tape_->dates, t->dates)))
      keys_.clear();
    books_.clear(), bench_ = nullptr;
    tape_ = t;
  }
  const pool::Style style = req.grid.style();
  std::erase_if(books_, [&](const auto &kv) { return kv.first.keyed() && kv.first.style() != style; });

  const std::vector<pool::Spec> specs = pool::specs_of(req.grid);
  std::vector<std::shared_ptr<const pool::Book>> books(specs.size());
  std::vector<size_t> todo; // 未留账的组
  bool keyed = false;       // 待算的组里有要键的
  for (size_t i = 0; i < specs.size(); ++i)
    if (const auto it = books_.find(specs[i]); it != books_.end())
      books[i] = it->second;
    else
      todo.push_back(i), keyed |= specs[i].keyed();
  std::shared_ptr<const pool::Ranks> rk;
  if (keyed && !(rk = ranks(ab.lib, style)))
    return false;
  const bool bench = !bench_;
  begin(Phase::Running, todo.size() + bench);
  pool_.tasks(static_cast<int>(todo.size() + bench), [&](int k, int) {
    if (cancel_.load(std::memory_order_relaxed))
      return;
    auto b = std::make_shared<pool::Book>();
    if (static_cast<size_t>(k) == todo.size())
      pool::run_book(*tape_, pool::kRebalDay, [&](size_t r, std::vector<uint32_t> &out) { pool::live_at(*tape_, r, out); }, *b), bench_ = std::move(b);
    else {
      const size_t i = todo[static_cast<size_t>(k)];
      const pool::Selector sel(specs[i]);
      pool::run_book(*tape_, specs[i].rebal(), [&](size_t r, std::vector<uint32_t> &out) { sel.at(pool::row_view(*tape_, rk.get(), r), out); }, *b);
      books[i] = std::move(b);
    }
    Progress::publish(1);
  });
  for (const size_t i : todo) // 取消前算完的照留
    if (books[i])
      books_.emplace(specs[i], books[i]);
  if (cancel_.load(std::memory_order_relaxed))
    return false;

  const std::vector<misc::Ymd> &dates = tape_->dates;
  auto out = std::make_shared<StyleResult>();
  const size_t D = dates.size();
  const size_t d0 = static_cast<size_t>(std::ranges::lower_bound(dates, req.lo) - dates.begin()),
               d1 = req.hi ? static_cast<size_t>(std::ranges::upper_bound(dates, req.hi) - dates.begin()) : D;
  assert(d0 < d1);
  out->d0 = d0, out->d1 = d1;
  const pool::Book &bb = *bench_;
  out->nav.assign(D, 1.0f);
  double nav = 1.0;
  for (size_t d = bb.first; d < D; ++d)
    out->nav[d] = static_cast<float>(nav *= 1.0 + bb.ret[kWeightEq][d]);
  for (int w = 0; w < kWeights; ++w) {
    out->bench[w].weight = static_cast<Weight>(w);
    fill(out->bench[w], dates, bb.ret[w], bb.buy[w], bb.first, d0, d1);
  }
  std::vector<size_t> src; // [线] 所属的组
  for (size_t i = 0; i < specs.size(); ++i) {
    if (books[i]->first >= dates.size()) {
      ++out->empty;
      continue;
    }
    for (int w = 0; w < kWeights; ++w)
      if (req.grid.weight >> w & 1) {
        StyleLine &l = out->lines.emplace_back();
        l.spec = specs[i], l.weight = static_cast<Weight>(w);
        src.push_back(i);
      }
  }
  pool_.tasks(static_cast<int>(out->lines.size()), [&](int k, int) {
    StyleLine &l = out->lines[static_cast<size_t>(k)];
    const pool::Book &b = *books[src[static_cast<size_t>(k)]];
    fill(l, dates, b.ret[l.weight], b.buy[l.weight], b.first, d0, d1);
  });
  out->dates = dates;
  return publish(req, {}, std::move(out));
}

} // namespace GUI::Beta