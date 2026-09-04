#include "api/tushare/pipeline.hpp"

#include "api/tushare/http.hpp"
#include "api/tushare/parse.hpp"
#include "api/tushare/spec.hpp"
#include "misc/date.hpp"
#include "misc/parquet.hpp"
#include "misc/schedule.hpp"
#include "shared/Config.hpp"

#include <iostream>
#include <string_view>
#include <vector>

namespace tushare {

namespace {

// 单月 fetch: range API 1 次调用 / per-day API 月内逐日 × day_params;
// 全部响应合并转一张 arrow::Table (0 行月 → 0 行表).
// 增量 (m.inc_from 非空): 下界抬到 inc_from, 只拉 / 只逐日迭代新增天.
std::shared_ptr<arrow::Table> fetch_month(Http &http, const InterfaceSpec &spec,
                                          const misc::FetchMonth &m) {
  const std::string &s = m.inc_from.empty() ? m.start : m.inc_from;
  std::vector<yyjson_doc *> docs;
  if (spec.day_params.empty()) {
    docs.push_back(
        http.call(spec.api, {{"start_date", s}, {"end_date", m.end}}));
  } else {
    for (const std::string &day : misc::iter_days(s, m.end)) {
      for (const std::string &p : spec.day_params) {
        docs.push_back(http.call(spec.api, {{p, day}}));
      }
    }
  }
  auto t = docs_to_table(docs, spec);
  for (yyjson_doc *d : docs)
    yyjson_doc_free(d);
  return t;
}

} // namespace

// ============================================================================
// Tushare 月度流水线 (与 bigquant::update 完全对仗)
//   misc::plan_months → fetch_month → data/YYYY-MM/<name>.parquet
//   (关月/初次: 整段覆盖; 开放月: 水位增量 append)
// Tushare 无历史额度限制: 历史月缺失 (如初次建库) 自动在线回补.
// ============================================================================
void update(std::string_view start, std::string_view end,
            const std::vector<InterfaceSpec> &specs, int lookback_days) {
  Http http(::config::TUSHARE_TOKEN);

  std::cout << "[tushare.update] " << start << " ~ " << end << " ("
            << specs.size() << " interfaces, lookback=" << lookback_days
            << "d, dedup=" << ::config::PIPELINE_DEDUP_WINDOW_SECONDS << "s)"
            << std::endl;

  for (const auto &spec : specs) {
    auto months =
        misc::plan_months(spec.name, spec.visible_date, spec.avail_hour, start,
                          end, lookback_days,
                          ::config::PIPELINE_DEDUP_WINDOW_SECONDS);
    std::cout << "\n[" << spec.name << "] " << months.size() << " month(s)"
              << std::endl;
    for (const auto &m : months) {
      bool inc = !m.inc_from.empty();
      std::cout << "  " << m.ym << (inc ? " +[" + m.inc_from + "..] ... " : " ... ")
                << std::flush;
      auto t = fetch_month(http, spec, m);
      auto p = misc::pq::month_path(m.ym, spec.name);
      if (inc)
        misc::pq::append_table_atomic(p, t);
      else
        misc::pq::write_table_atomic(p, t);
      std::cout << t->num_rows() << " rows" << std::endl;
    }
  }

  std::cout << "\n[tushare.update] done" << std::endl;
}

bool pending(std::string_view start, std::string_view end,
             const std::vector<InterfaceSpec> &specs, int lookback_days) {
  for (const auto &spec : specs) {
    if (!misc::plan_months(spec.name, spec.visible_date, spec.avail_hour, start,
                           end, lookback_days,
                           ::config::PIPELINE_DEDUP_WINDOW_SECONDS)
             .empty())
      return true;
  }
  return false;
}

void probe() {
  Http http(::config::TUSHARE_TOKEN);
  yyjson_doc *doc = http.call("forecast_vip", {{"period", "19900331"}});
  yyjson_doc_free(doc);
}

} // namespace tushare
