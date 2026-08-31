#include "api/bigquant/pipeline.hpp"

#include "api/bigquant/dai.hpp"
#include "api/bigquant/spec.hpp"
#include "shared/Config.hpp"
#include "misc/parquet.hpp"
#include "misc/schedule.hpp"

#include <arrow/table.h>

#include <iostream>
#include <string_view>
#include <vector>

namespace bigquant {

// ============================================================================
// BigQuant DAI 月度流水线 (与 tushare::update 完全对仗)
//   Static / Snapshot → data/_meta/<name>.parquet 单文件整刷 (meta_fresh 判定)
//   其余              → misc::plan_months → fetch(月) → data/YYYY-MM/<name>.parquet
//                       (关月/初次: 整段覆盖; 开放月: 水位增量 append)
// 落盘 = 服务端响应原样 (单文件 tmp+rename 原子; 0 行月也落 0 行文件).
// ============================================================================
void update(std::string_view start, std::string_view end,
            const std::vector<TableSpec> &specs, int lookback_days) {
  DaiClient client;

  std::cout << "[bigquant.update] " << start << " ~ " << end << " ("
            << specs.size() << " tables, lookback=" << lookback_days
            << "d, dedup=" << ::config::PIPELINE_DEDUP_WINDOW_SECONDS << "s)"
            << std::endl;

  for (const auto &spec : specs) {
    bool is_meta = (spec.kind == FetchKind::Static) ||
                   (spec.kind == FetchKind::Snapshot);

    if (is_meta) {
      auto p = misc::pq::meta_path(spec.name);
      if (misc::meta_fresh(p, spec.visible_date, spec.avail_hour, end,
                           ::config::PIPELINE_DEDUP_WINDOW_SECONDS)) {
        std::cout << "\n[" << spec.name << "] skip (fresh)" << std::endl;
        continue;
      }
      std::cout << "\n[" << spec.name << "] meta refresh ..." << std::flush;
      // Static 忽略窗口; Snapshot 取窗口内 MAX(date) 一天 (= 最新一份真盘前快照).
      auto t = fetch(client, spec, start, end);
      misc::pq::write_table_atomic(p, t);
      std::cout << " " << t->num_rows() << " rows -> _meta" << std::endl;
      continue;
    }

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
      auto t = fetch(client, spec, m.start, m.end, m.inc_from);
      auto p = misc::pq::month_path(m.ym, spec.name);
      if (inc)
        misc::pq::append_table_atomic(p, t);
      else
        misc::pq::write_table_atomic(p, t);
      std::cout << t->num_rows() << " rows" << std::endl;
    }
  }

  std::cout << "\n[bigquant.update] done" << std::endl;
}

bool pending(std::string_view start, std::string_view end,
             const std::vector<TableSpec> &specs, int lookback_days) {
  for (const auto &spec : specs) {
    bool is_meta = (spec.kind == FetchKind::Static) ||
                   (spec.kind == FetchKind::Snapshot);
    if (is_meta) {
      if (!misc::meta_fresh(misc::pq::meta_path(spec.name), spec.visible_date,
                            spec.avail_hour, end,
                            ::config::PIPELINE_DEDUP_WINDOW_SECONDS))
        return true;
    } else if (!misc::plan_months(spec.name, spec.visible_date, spec.avail_hour,
                                  start, end, lookback_days,
                                  ::config::PIPELINE_DEDUP_WINDOW_SECONDS)
                    .empty()) {
      return true;
    }
  }
  return false;
}

} // namespace bigquant
