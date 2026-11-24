#include "gui/task_live/Books.hpp"

#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <unordered_set>

namespace GUI::Live {

namespace {

using Secs = std::chrono::duration<double>;
constexpr Secs kGap{0.5}, kStale{5};

} // namespace

void merge(live::Quotes &q, const std::vector<sina::Quote> &xs) {
  for (const sina::Quote &x : xs)
    q[x.sym.substr(2)] = live::quote_of(x);
}

std::optional<live::Quotes> quotes_of(sina::Client &cl, const std::vector<live::Sec> &secs, int tries) {
  live::Quotes q;
  std::vector<std::string> syms;
  for (size_t i = 0; i < secs.size(); ++i) {
    syms.push_back(live::sina_sym(secs[i].mkt, secs[i].code));
    if (syms.size() == static_cast<size_t>(sina::kQuoteMax) || i + 1 == secs.size()) {
      const std::optional<std::vector<sina::Quote>> got = cl.try_quotes(syms, tries);
      if (!got)
        return std::nullopt;
      merge(q, *got);
      syms.clear();
    }
  }
  return q;
}

struct Books::Feed {
  struct Got {
    live::Quote q;
    Clock::time_point at; // 收到于
  };
  std::mutex mu;
  std::condition_variable cv;
  bool stop = false;
  std::unordered_map<std::string, Got> q; // 代码 → 最近收到的
};

Books::Books(const std::vector<live::Sec> &secs) : feed_(std::make_shared<Feed>()) {
  std::vector<live::Sec> uniq;
  std::unordered_set<std::string> seen;
  for (const live::Sec &s : secs)
    if (seen.insert(s.code).second)
      uniq.push_back(s);
  if (uniq.empty())
    return;
  std::thread([f = feed_, uniq = std::move(uniq)] {
    sina::Client cl(sina::kQuoteHost);
    std::unique_lock lk(f->mu);
    while (!f->stop) {
      lk.unlock();
      std::optional<live::Quotes> got;
      std::ignore = live::guard([&] { got = quotes_of(cl, uniq, 1); });
      const Clock::time_point now = Clock::now();
      lk.lock();
      if (got)
        for (auto &[code, q] : *got)
          f->q[code] = {std::move(q), now};
      f->cv.wait_for(lk, kGap, [&] { return f->stop; });
    }
  }).detach();
}

Books::~Books() {
  {
    std::lock_guard lk(feed_->mu);
    feed_->stop = true;
  }
  feed_->cv.notify_all();
}

Books::Book Books::get(const live::Sec &s) {
  {
    std::lock_guard lk(feed_->mu);
    const auto i = feed_->q.find(s.code);
    if (i != feed_->q.end())
      if (const Secs age = Clock::now() - i->second.at; age <= kStale)
        return {i->second.q, age.count(), true};
  }
  live::need(!down_ || Clock::now() - *down_ > kStale, "新浪 %.0fs 内刚取不到行情, 本单不再现取", kStale.count());
  if (!cl_)
    cl_.emplace(sina::kQuoteHost);
  const std::optional<live::Quotes> got = quotes_of(*cl_, {s}, 1);
  if (!got)
    down_ = Clock::now();
  live::need(got.has_value(), "新浪取不到行情");
  const auto i = got->find(s.code);
  live::need(i != got->end(), "新浪查无此码");
  return {i->second};
}

} // namespace GUI::Live
