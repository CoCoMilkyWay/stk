#include "fundamental/Sync.hpp"

#include "fundamental/Remote.hpp"
#include "shared/Config.hpp"

#include <iostream>

namespace fundamental {

namespace {

template <class Client>
void run(Client &client, const std::vector<Task> &tasks, const char *tag) {
  std::cout << "[" << tag << ".sync] " << tasks.size() << " task(s) from " << ::config::PIPELINE_START_DATE
            << " (lookback=" << ::config::PIPELINE_LOOKBACK_DAYS
            << "d, dedup=" << ::config::PIPELINE_DEDUP_WINDOW_SECONDS << "s)" << std::endl;
  const Spec *cur = nullptr;
  for (const Task &t : tasks) {
    if (t.spec != cur) {
      cur = t.spec;
      std::cout << "\n[" << cur->name << "]" << std::endl;
    }
    std::cout << "  " << (t.ym == 0 ? "_meta" : misc::dash_ym(t.ym))
              << (t.append ? " +[" + misc::dense(t.w.lo) + "..] ... " : " ... ") << std::flush;
    const auto tbl = fetch(client, t);
    put(t, tbl);
    std::cout << tbl->num_rows() << " rows" << std::endl;
  }
  std::cout << "\n[" << tag << ".sync] done" << std::endl;
}

} // namespace

template <class Src>
std::vector<Task> plan(const Catalog &cat, const Now &now) {
  std::vector<Task> out;
  for (const Spec &s : SPECS) {
    if (!from<Src>(s))
      continue;
    const std::vector<Task> v = plan(s, cat, now);
    out.insert(out.end(), v.begin(), v.end());
  }
  return out;
}

void sync(bigquant::Client &client, const std::vector<Task> &tasks) { run(client, tasks, "bigquant"); }
void sync(tushare::Client &client, const std::vector<Task> &tasks) { run(client, tasks, "tushare"); }

template std::vector<Task> plan<Bq>(const Catalog &, const Now &);
template std::vector<Task> plan<Tu>(const Catalog &, const Now &);

} // namespace fundamental
