#include "fundamental/Sync.hpp"

#include "api/bigquant/fetch.hpp"
#include "api/tushare/fetch.hpp"
#include "shared/Config.hpp"

#include <iostream>

namespace fundamental {

namespace {

template <class Src>
struct Client;
template <>
struct Client<Dai> {
  using type = bigquant::DaiClient;
  static type make() { return {}; }
  static constexpr const char *tag = "bigquant";
};
template <>
struct Client<Tu> {
  using type = tushare::Http;
  static type make() { return type(::config::TUSHARE_TOKEN); }
  static constexpr const char *tag = "tushare";
};

} // namespace

template <class Src>
bool pending(const Catalog &cat, Ymd today) {
  for (const Spec &s : SPECS)
    if (from<Src>(s) && !plan(s, cat, today).empty())
      return true;
  return false;
}

template <class Src>
void sync(const Catalog &cat, Ymd today) {
  auto client = Client<Src>::make();
  std::cout << "[" << Client<Src>::tag << ".sync] " << ::config::PIPELINE_START_DATE << " ~ " << today
            << " (lookback=" << ::config::PIPELINE_LOOKBACK_DAYS
            << "d, dedup=" << ::config::PIPELINE_DEDUP_WINDOW_SECONDS << "s)" << std::endl;
  for (const Spec &s : SPECS) {
    if (!from<Src>(s))
      continue;
    const std::vector<Task> tasks = plan(s, cat, today);
    std::cout << "\n[" << s.name << "] " << tasks.size() << " task(s)" << std::endl;
    for (const Task &t : tasks) {
      std::cout << "  " << (t.ym == 0 ? "_meta" : misc::dash_ym(t.ym))
                << (t.w.inc != 0 ? " +[" + misc::dense(t.w.inc) + "..] ... " : " ... ") << std::flush;
      const auto tbl = fetch(client, s, std::get<Src>(s.src), t.w); // ADL: bigquant::fetch / tushare::fetch
      put(s, t, tbl);
      std::cout << tbl->num_rows() << " rows" << std::endl;
    }
  }
  std::cout << "\n[" << Client<Src>::tag << ".sync] done" << std::endl;
}

template bool pending<Dai>(const Catalog &, Ymd);
template bool pending<Tu>(const Catalog &, Ymd);
template void sync<Dai>(const Catalog &, Ymd);
template void sync<Tu>(const Catalog &, Ymd);

} // namespace fundamental
