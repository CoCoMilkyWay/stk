#include "fundamental/Sync.hpp"

#include "fundamental/Remote.hpp"

#include <chrono>
#include <string>

namespace fundamental {

namespace sync {

std::vector<Task> plan(const Spec &s, const Round &r) {
  std::vector<Task> out;
  for (const Slot &sl : slots(s, r)) {
    if (sl.st == State::Frozen || (sl.st == State::Open && s.shape == Shape::Daily))
      continue;
    if (sl.sh && r.now.at - sl.sh->mtime < std::chrono::seconds(kDedupSeconds))
      continue;
    Task t{&s, sl.at, sl.w, Write::Replace};
    if (sl.st == State::Open) {
      const Ymd m = mark(s, *sl.sh);
      if (m != 0) {
        if (s.shape == Shape::Month && s.snap == Snap::Day)
          continue; // 月初快照已入盘
        t.w.lo = misc::add_days(m, 1);
        if (s.shape == Shape::Month)
          t.how = Write::Append;
      }
    }
    if (!s.vd.empty() && t.w.lo > t.w.hi)
      continue;
    out.push_back(t);
  }
  return out;
}

std::int64_t run(Clients &c, const Task &t) {
  const auto tbl = fetch(c, t);
  put(t, tbl);
  return tbl->num_rows();
}

} // namespace sync

std::string label(const Task &t) {
  if (t.spec->shape == Shape::Daily)
    return t.spec->name + " " + misc::dash(t.at);
  const std::string where = t.spec->name + " " + (t.at == 0 ? "_meta" : misc::dash_ym(misc::ym_of(t.at)));
  return t.how == Write::Append ? where + " +[" + misc::dense(t.w.lo) + "..]" : where;
}

} // namespace fundamental
