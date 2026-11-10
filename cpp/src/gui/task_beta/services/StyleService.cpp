#include "gui/task_beta/services/StyleService.hpp"

#include "features/Storage/FeatureLib.hpp"
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
  const pool::Book &bb = *bench_;
  for (int w = 0; w < kWeights; ++w) {
    out->bench[w].weight = static_cast<Weight>(w);
    fill(out->bench[w], dates, bb.ret[w], bb.ret[w], bb.first, bb.buy[w]);
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
    fill(l, dates, b.ret[l.weight], bb.ret[l.weight], b.first, b.buy[l.weight]);
  });
  out->dates = dates;
  return publish(req, {}, std::move(out));
}

} // namespace GUI::Beta