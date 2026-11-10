#include "gui/task_beta/services/StyleService.hpp"

#include "features/Storage/FeatureLib.hpp"
#include "features/Storage/FeatureRead.hpp"
#include "misc/date.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <limits>
#include <mutex>
#include <span>
#include <string>
#include <utility>

namespace GUI::Beta {

namespace {

constexpr float kNaN = std::numeric_limits<float>::quiet_NaN();

// 日收益 r (对全A 同权重日收益 rb) 自 first 起的净值与绩效; 年数 = 建仓行 (first − 1) 到末日的日历跨度
void fill(StyleLine &l, std::span<const misc::Ymd> dates, const std::vector<float> &r, const std::vector<float> &rb, size_t first, double buy) {
  const size_t D = dates.size();
  l.first = first;
  l.nav.assign(D, 1.0f);
  if (first >= D) {
    l.ann = l.sharpe = l.mdd = l.exc = l.ir = l.turn = kNaN;
    return;
  }
  assert(first >= 1);
  const double years = static_cast<double>((misc::to_days(dates.back()) - misc::to_days(dates[first - 1])).count()) / 365.25;
  const double n = static_cast<double>(D - first), ppy = n / years;
  double nav = 1.0, peak = 1.0, mdd = 0.0, rel = 1.0, s = 0.0, ss = 0.0, e = 0.0, ee = 0.0;
  for (size_t d = first; d < D; ++d) {
    const double x = r[d], y = (1.0 + x) / (1.0 + rb[d]) - 1.0;
    nav *= 1.0 + x, rel *= 1.0 + y;
    l.nav[d] = static_cast<float>(nav);
    peak = std::max(peak, nav), mdd = std::max(mdd, 1.0 - nav / peak);
    s += x, ss += x * x, e += y, ee += y * y;
  }
  const auto ratio = [&](double m, double m2) {
    const double sd = std::sqrt(std::max(m2 / n - (m / n) * (m / n), 0.0));
    return sd > 0.0 ? static_cast<float>(m / n / sd * std::sqrt(ppy)) : kNaN;
  };
  l.ann = static_cast<float>(std::pow(nav, 1.0 / years) - 1.0);
  l.sharpe = ratio(s, ss);
  l.mdd = static_cast<float>(mdd);
  l.exc = static_cast<float>(std::pow(rel, 1.0 / years) - 1.0);
  l.ir = ratio(e, ee);
  l.turn = static_cast<float>(buy / years);
}

} // namespace

bool StyleService::publish(const StyleRequest &req, std::string lack, std::shared_ptr<StyleResult> out) {
  if (!out)
    out = std::make_shared<StyleResult>();
  out->gen = req.gen, out->style = req.style, out->grid = req.grid;
  out->lack = std::move(lack);
  std::lock_guard<std::mutex> lock(mutex);
  result = std::move(out);
  Progress::publish();
  return true;
}

std::shared_ptr<const pool::Ranks> StyleService::ranks(const FeatureRead &rd, pool::Style st) {
  const auto hit = std::find_if(keys_.begin(), keys_.end(), [&](const Key &k) { return k.style == st; });
  if (hit != keys_.end()) {
    const Key k = *hit;
    keys_.erase(hit);
    keys_.insert(keys_.begin(), k);
    return k.rk;
  }
  const size_t D = tape_.days();
  begin(Phase::Loading, (pool::key_reads(st) ? D : 0) + D);
  const pool::Tick tick = [this] { Progress::publish(1); };
  std::vector<float> key;
  if (!pool::load_key(rd, tape_, st, pool_, cancel_, tick, key))
    return nullptr;
  auto rk = std::make_shared<pool::Ranks>();
  if (!pool::rank_key(tape_, key, pool_, cancel_, tick, *rk))
    return nullptr;
  keys_.insert(keys_.begin(), Key{st, rk});
  if (keys_.size() > kKeep)
    keys_.pop_back();
  return rk;
}

bool StyleService::run(StyleRequest &req) {
  const FeatureLib lib = load_lib(kAllBar);
  if (!lib.ready())
    return publish(req, "风格篮子由 all.bar 特征库选出: 先在 Beta → Compute 算好 all 池的 Bar 库.");
  FeatureRead rd(lib, &cancel_);
  std::vector<misc::Ymd> dates; // 已落盘日 (清单日期轴可能还在算)
  for (const misc::Ymd d : lib.dates)
    if (rd.has_date(d))
      dates.push_back(d);
  if (dates.size() < 2)
    return publish(req, "all.bar 特征库落盘不足 2 日.");

  if (!(tape_.A > 0 && tape_.axis == lib.axis.hash && tape_.dates == dates)) {
    keys_.clear(), books_.clear();
    begin(Phase::Loading, dates.size());
    if (!pool::load_tape(lib, rd, dates, pool_, cancel_, [this] { Progress::publish(1); }, tape_)) {
      tape_ = pool::Tape{};
      return false;
    }
  }
  std::erase_if(books_, [&](const auto &kv) { return kv.first.n > 0 && kv.first.style != req.style; });

  std::vector<pool::Spec> specs = pool::specs_of(req.style, req.grid);
  specs.push_back(pool::canon(pool::Spec{.n = 0})); // 末位 = 全A
  std::vector<std::shared_ptr<const pool::Book>> books(specs.size());
  std::vector<size_t> todo; // 未留账的组
  bool keyed = false;       // 待算的组里有要键的
  for (size_t i = 0; i < specs.size(); ++i)
    if (const auto it = books_.find(specs[i]); it != books_.end())
      books[i] = it->second;
    else
      todo.push_back(i), keyed |= specs[i].n > 0;
  std::shared_ptr<const pool::Ranks> rk;
  if (keyed && !(rk = ranks(rd, req.style)))
    return false;
  begin(Phase::Running, todo.size());
  pool_.tasks(static_cast<int>(todo.size()), [&](int k, int) {
    if (cancel_.load(std::memory_order_relaxed))
      return;
    const size_t i = todo[static_cast<size_t>(k)];
    auto b = std::make_shared<pool::Book>();
    pool::run_book(tape_, rk.get(), specs[i], *b);
    books[i] = std::move(b);
    Progress::publish(1);
  });
  for (const size_t i : todo) // 取消前算完的照留
    if (books[i])
      books_.emplace(specs[i], books[i]);
  if (cancel_.load(std::memory_order_relaxed))
    return false;

  auto out = std::make_shared<StyleResult>();
  const pool::Book &bb = *books.back();
  for (int w = 0; w < kWeights; ++w) {
    out->bench[w].spec = specs.back(), out->bench[w].weight = static_cast<Weight>(w);
    fill(out->bench[w], dates, bb.ret[w], bb.ret[w], bb.first, bb.buy[w]);
  }
  std::vector<size_t> src; // [线] 所属的组
  for (size_t i = 0; i + 1 < specs.size(); ++i) {
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
    fill(l, dates, b.ret[l.weight], bb.ret[l.weight], b.first, b.buy[l.weight]);
  });
  out->dates = std::move(dates);
  return publish(req, {}, std::move(out));
}

} // namespace GUI::Beta