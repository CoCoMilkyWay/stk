#include "gui/task_market/services/CtaService.hpp"

#include "misc/date.hpp"

#include <algorithm>
#include <cassert>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace GUI::Market {

namespace {

// 一根 (高 低 收) × 复权倍数 f → 有效根: 收 ≤ 0 不收; 高 / 低缺 → 收, 且包住收
void push(cta::Series &se, size_t t, const Quote::Cell &b, double f) {
  const float c = b.close, h = b.high, l = b.low;
  if (!(c > 0))
    return;
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
  const cta::Spec &sp = req.spec;
  assert(req.ref.ok() && req.dates && req.series && req.series->minute && "CTA 请求须有标的与其连分钟的全史");
  constexpr size_t S = Quote::kSlots;
  const std::vector<misc::Ymd> &spine = *req.dates;
  if (!(stamp_ == req.stamp))
    kept_.clear(), stamp_ = req.stamp;
  const std::shared_ptr<const cta::Series> se = kept_.get(
      req.ref, [&] {
        auto s = std::make_shared<cta::Series>();
        const Quote::Series &m = *req.series;
        Quote::Cell b;
        for (size_t j = 0, i = 0; j < m.daily.size(); ++j, ++i) { // 有根的日 ⊂ 日脊: 同向走
          while (spine[i] < m.daily[j].date)
            ++i;
          assert(spine[i] == m.daily[j].date && "全史的日不在日脊上");
          if (m.has(j))
            for (size_t k = 0; k < S; ++k)
              if (m.bar(j, static_cast<int>(k), b))
                push(*s, i * S + k, b, m.adj[j]);
        }
        return s;
      },
      kKeep);
  const cta::Days days = cta::days_of(*se, S);
  const size_t nd = days.size();
  if (nd < 2)
    return publish(req, "有效根只跨 " + std::to_string(nd) + " 日, 至少 2 日.");
  auto out = std::make_shared<CtaResult>();
  out->dates = spine;
  out->se = se;
  cta::Col hi(nd, cta::kNaN), lo(nd, cta::kNaN); // 估值线 [日]: 日 d 盘中用前一个有根的日收盘时的 (收盘 × 倍数), 不看当日
  if (req.up && req.dn) {
    assert(req.up->size() == spine.size() && req.dn->size() == spine.size() && "估值线 [日脊]");
    const size_t d0 = se->t.front() / S;
    float ph = cta::kNaN, pl = cta::kNaN;
    for (size_t d = 0; d < nd; ++d) {
      hi[d] = ph, lo[d] = pl;
      if (days.off[d] < days.off[d + 1]) {
        const float c = se->c[days.off[d + 1] - 1];
        ph = c * (*req.up)[d0 + d], pl = c * (*req.dn)[d0 + d];
      }
    }
  }
  begin(Phase::Running, nd - 1);
  std::optional<cta::Track> tr = cta::trace(
      *se, S, sp, req.rates, hi, lo, pool_, [&] { return cancel_.load(std::memory_order_relaxed); }, [&] { Progress::publish(1); });
  if (!tr)
    return false;
  out->tr = std::make_shared<const cta::Track>(std::move(*tr));
  return publish(req, {}, std::move(out));
}

} // namespace GUI::Market