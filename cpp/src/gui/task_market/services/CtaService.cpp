#include "gui/task_market/services/CtaService.hpp"

#include "misc/date.hpp"

#include <algorithm>
#include <cassert>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace GUI::Market {

namespace {

// 一根 (高 低 收) × 因子 f → 有效根: 收 ≤ 0 / NaN 不收; 高 / 低缺 → 收, 且包住收
void push(cta::Series &se, size_t t, const float *p, double f) {
  const float c = p[Minutes::kClose];
  if (!(c > 0))
    return;
  const float h = p[Minutes::kHigh], l = p[Minutes::kLow];
  const auto at = [f](float v) { return static_cast<float>(v * f); };
  se.c.push_back(at(c));
  se.h.push_back(at(h > 0 ? std::max(h, c) : c));
  se.l.push_back(at(l > 0 ? std::min(l, c) : c));
  se.t.push_back(static_cast<uint32_t>(t));
}

} // namespace

bool CtaService::publish(const CtaRequest &req, std::string lack, std::shared_ptr<CtaResult> out) {
  if (!out)
    out = std::make_shared<CtaResult>();
  out->gen = req.gen, out->ref = req.ref, out->spec = req.spec;
  out->lack = std::move(lack);
  std::lock_guard<std::mutex> lock(mutex);
  result = std::move(out);
  Progress::publish();
  return true;
}

bool CtaService::run(CtaRequest &req) {
  const CtaSpec &sp = req.spec;
  assert(sp.roll >= static_cast<int>(cta::kMinRoll) && sp.roll <= static_cast<int>(cta::kMaxRoll));
  assert(req.ref.ok() && req.dates && req.minutes && "CTA 请求须有标的与其分钟价");
  constexpr size_t S = Quote::kSlots, F = Minutes::kFields;
  const std::vector<misc::Ymd> &spine = *req.dates;
  const std::shared_ptr<const cta::Series> se = kept_.get(req.ref, req.stamp, [&](cta::Series &s) {
    const Minutes &m = *req.minutes;
    for (size_t j = 0, i = 0; j < m.dates.size(); ++j, ++i) { // 有根的日 ⊂ 日脊: 同向走
      while (spine[i] < m.dates[j])
        ++i;
      assert(spine[i] == m.dates[j] && "分钟价的日不在日脊上");
      for (size_t k = 0; k < S; ++k)
        push(s, i * S + k, m.px.data() + (j * S + k) * F, m.f[j]);
    }
    return true;
  });
  const size_t W = static_cast<size_t>(sp.roll), nd = cta::days_of(*se, S).size();
  if (nd < 2)
    return publish(req, "有效根只跨 " + std::to_string(nd) + " 日, 至少 2 日.");
  auto out = std::make_shared<CtaResult>();
  out->dates = spine;
  out->se = se;
  const double years = static_cast<double>((misc::to_days(out->dates.back()) - misc::to_days(out->dates.front())).count() + 1) / 365.25;
  const cta::Setup u{req.rates, {sp.vol, sp.buffer}, static_cast<double>(out->dates.size() * S) / years};
  cta::Grid grid = cta::grid_of(S, sp.kind, static_cast<size_t>(sp.grid));
  begin(Phase::Running, nd - 1);
  const bool done = cta::trace(
      *se, std::move(grid), sp.goal, static_cast<size_t>(sp.dir), W, u, pool_, out->tr, [&] { return cancel_.load(std::memory_order_relaxed); },
      [&] { Progress::publish(1); });
  return done && publish(req, {}, std::move(out));
}

} // namespace GUI::Market