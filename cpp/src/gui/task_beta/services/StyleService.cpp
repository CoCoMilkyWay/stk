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

// 日收益 r (对全A 同权重日收益 rb) / 日增仓量 buy 自 first 起的净值, 与各窗的绩效 (starts[W − 1] = 末 W 年的窗起).
// 自末日倒推一趟: 窗左端 lo 每前移一日, 窗内各和 / 回撤随之累加, 到某窗的左端 max(窗起, first) 即记该窗
void fill(StyleLine &l, std::span<const misc::Ymd> dates, std::span<const size_t> starts, const std::vector<float> &r, const std::vector<float> &rb,
          const std::vector<float> &buy, size_t first) {
  const size_t D = dates.size(), Ws = starts.size();
  l.first = first;
  l.nav.assign(D, 1.0f);
  l.win.assign(Ws, {kNaN, kNaN, kNaN, kNaN, kNaN, kNaN});
  if (first >= D)
    return;
  assert(first >= 1);
  double nav = 1.0;
  for (size_t d = first; d < D; ++d)
    l.nav[d] = static_cast<float>(nav *= 1.0 + r[d]);
  // 窗内 Σ ln(1 + r) / Σ r / Σ r² · 相对日收益 y 同三项 · Σ 增仓; low = 窗内净值最低, mdd = 峰在 [基日, 末日) 的最大回撤
  double g = 0.0, s = 0.0, ss = 0.0, h = 0.0, e = 0.0, ee = 0.0, b = 0.0, mdd = 0.0;
  float low = l.nav[D - 1];
  size_t W = 0; // 待记的窗 (下标)
  for (size_t lo = D - 1; lo >= first && W < Ws; --lo) {
    const double x = r[lo], y = (1.0 + x) / (1.0 + rb[lo]) - 1.0;
    g += std::log1p(x), s += x, ss += x * x, h += std::log1p(y), e += y, ee += y * y, b += buy[lo];
    low = std::min(low, l.nav[lo]);
    mdd = std::max(mdd, 1.0 - static_cast<double>(low) / l.nav[lo - 1]);
    for (; W < Ws && std::max(starts[W], first) >= lo; ++W) {
      const double years = static_cast<double>((misc::to_days(dates.back()) - misc::to_days(dates[lo - 1])).count()) / 365.25;
      const double n = static_cast<double>(D - lo), ppy = n / years;
      const auto ratio = [&](double m, double m2) {
        const double sd = std::sqrt(std::max(m2 / n - (m / n) * (m / n), 0.0));
        return sd > 0.0 ? static_cast<float>(m / n / sd * std::sqrt(ppy)) : kNaN;
      };
      l.win[W] = {static_cast<float>(std::expm1(g / years)), ratio(s, ss), static_cast<float>(mdd), static_cast<float>(std::expm1(h / years)),
                  ratio(e, ee), static_cast<float>(b / years)};
    }
  }
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
  out->years = win_years(dates);
  std::vector<size_t> starts(static_cast<size_t>(out->years));
  for (int W = 1; W <= out->years; ++W)
    starts[static_cast<size_t>(W - 1)] = win_start(dates, dates.size() - 1, W);
  const pool::Book &bb = *bench_;
  for (int w = 0; w < kWeights; ++w) {
    out->bench[w].weight = static_cast<Weight>(w);
    fill(out->bench[w], dates, starts, bb.ret[w], bb.ret[w], bb.buy[w], bb.first);
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
    fill(l, dates, starts, b.ret[l.weight], bb.ret[l.weight], b.buy[l.weight], b.first);
  });
  out->dates = dates;
  return publish(req, {}, std::move(out));
}

} // namespace GUI::Beta