#include "api/tushare/fetch.hpp"

#include "api/tushare/parse.hpp"

#include <cassert>
#include <string>
#include <vector>

namespace tushare {

std::shared_ptr<arrow::Table> fetch(Http &http, const fundamental::Spec &s,
                                    const fundamental::Tu &src, const fundamental::Window &w) {
  assert(s.shape == fundamental::Shape::Month && s.pick == fundamental::Pick::All &&
         "Tushare 只有 Month·All 事件表");
  assert((w.inc == 0 || (w.inc >= w.lo && w.inc <= w.hi)) && "增量下界须落在窗口内");
  const fundamental::Ymd lo = w.inc != 0 ? w.inc : w.lo;
  std::vector<yyjson_doc *> docs;
  if (src.day_params.empty()) {
    docs.push_back(http.call(src.api, {{"start_date", misc::dense(lo)}, {"end_date", misc::dense(w.hi)}}));
  } else {
    for (fundamental::Ymd d = lo; d <= w.hi; d = misc::add_days(d, 1))
      for (const std::string &p : src.day_params)
        docs.push_back(http.call(src.api, {{p, misc::dense(d)}}));
  }
  auto t = docs_to_table(docs, src);
  for (yyjson_doc *d : docs)
    yyjson_doc_free(d);
  return t;
}

} // namespace tushare
