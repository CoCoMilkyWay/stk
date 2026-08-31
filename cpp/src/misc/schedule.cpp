#include "misc/schedule.hpp"

#include "misc/date.hpp"
#include "misc/parquet.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <ctime>
#include <filesystem>
#include <string>

namespace misc {

namespace fs = std::filesystem;

namespace {

// "YYYYMMDD" → "YYYY-MM"
std::string ym_of(std::string_view yyyymmdd) {
  assert(yyyymmdd.size() == 8);
  std::string ym(yyyymmdd.substr(0, 4));
  ym += '-';
  ym += yyyymmdd.substr(4, 2);
  return ym;
}

// "YYYY-MM" → 下月 "YYYY-MM"
std::string next_ym(std::string_view ym) {
  int y = std::stoi(std::string(ym.substr(0, 4)));
  int m = std::stoi(std::string(ym.substr(5, 2)));
  if (++m == 13) {
    m = 1;
    ++y;
  }
  char buf[8];
  std::snprintf(buf, sizeof(buf), "%04d-%02d", y, m);
  return buf;
}

// 文件 mtime 距今秒数; 不存在返回 -1.
long file_age_seconds(const fs::path &p) {
  if (!fs::exists(p))
    return -1;
  auto mt = fs::last_write_time(p);
  auto now = fs::file_time_type::clock::now();
  return std::chrono::duration_cast<std::chrono::seconds>(now - mt).count();
}

// 文件 mtime → 写盘日 "YYYYMMDD"; 不存在返回空串.
std::string file_write_date(const fs::path &p) {
  if (!fs::exists(p))
    return {};
  auto sys = std::chrono::clock_cast<std::chrono::system_clock>(
      fs::last_write_time(p));
  return fmt_yyyymmdd(std::chrono::floor<std::chrono::days>(sys));
}

// 本地时区当前小时 [0, 23].
int now_hour_local() {
  std::time_t t = std::time(nullptr);
  std::tm tm{};
  localtime_r(&t, &tm);
  return tm.tm_hour;
}

// horizon = 当前时刻已完整入库的最晚数据日.
//   avail_hour 0 → 恒 today (排程提前); 24 → 恒昨天 (全天涓流, 次日才完整).
std::string data_horizon(int avail_hour, std::string_view today) {
  assert(avail_hour >= 0 && avail_hour <= 24);
  return now_hour_local() >= avail_hour ? std::string(today)
                                        : add_days(today, -1);
}

// parquet 内 max(vd) → "YYYYMMDD"; 0 行 / 全 null → 空串.
std::string file_max_vd(const fs::path &p, std::string_view vd_col) {
  pq::TableView tv(pq::read_table(p));
  if (tv.rows() == 0)
    return {};
  pq::Col c = tv.col(vd_col);
  std::int32_t mx = 0;
  for (std::int64_t i = 0; i < tv.rows(); ++i)
    mx = std::max(mx, c.yyyymmdd(i));
  if (mx == 0)
    return {};
  char buf[16];
  std::snprintf(buf, sizeof(buf), "%08d", mx);
  return buf;
}

} // namespace

std::vector<FetchMonth> plan_months(std::string_view name,
                                    std::string_view vd_col, int avail_hour,
                                    std::string_view start_date,
                                    std::string_view today, int lookback_days,
                                    int dedup_seconds) {
  assert(start_date.size() == 8 && today.size() == 8 && start_date <= today);
  assert(!vd_col.empty() && "月度表必须配 visible_date (Static 走 _meta)");
  std::vector<FetchMonth> out;

  // 关月判定线: 月末 < frozen_before 的月不再变动.
  std::string frozen_before = add_days(today, -lookback_days);
  std::string horizon = data_horizon(avail_hour, today);

  std::string end_ym = ym_of(today);
  for (std::string ym = ym_of(start_date);; ym = next_ym(ym)) {
    std::string yyyymm = ym.substr(0, 4) + ym.substr(5, 2);
    std::string m_first = yyyymm + "01";
    std::string m_last = yyyymm + month_last_dd(yyyymm);

    fs::path p = pq::month_path(ym, name);
    bool closed = m_last < frozen_before;

    if (closed) {
      // 冻结条件 = 写盘日 ≥ 月末 + lookback: 开放月只增量到水位, 写盘时该月
      // 已出 lookback 窗口才能保证 "整月行 + 全部回填" 都已入盘. 月中写的
      // 半月文件在跑批空窗期后关月, 此处整月重拉一次 (写盘日=today ⇒ 之后
      // 永久冻结). 这也是月内增量漏掉的服务端回填的唯一兜底.
      std::string wd = file_write_date(p);
      if (wd.empty() || wd < add_days(m_last, lookback_days)) {
        out.push_back(
            {ym, std::max(m_first, std::string(start_date)), m_last, ""});
      }
    } else {
      // 开放月: 水位增量. 只为 vd ∈ [W+1, min(m末, horizon)] 的新行付流量;
      // 已到水位连查询都不发. 0 行响应 append 是 no-op (mtime / pool cache 不动).
      long age = file_age_seconds(p);
      std::string w_start = std::max(m_first, std::string(start_date));
      std::string w_end = std::min(m_last, horizon);
      if ((age < 0 || age >= dedup_seconds) && w_start <= w_end) {
        if (age < 0) {
          out.push_back({ym, w_start, w_end, ""}); // 初次: 整段覆盖
        } else {
          std::string w = file_max_vd(p, vd_col);
          if (w.empty()) {
            out.push_back({ym, w_start, w_end, ""}); // 0 行: 整段重试
          } else {
            std::string inc = add_days(w, 1);
            if (inc <= w_end)
              out.push_back({ym, w_start, w_end, inc});
          }
        }
      }
    }

    if (ym == end_ym)
      break;
  }
  return out;
}

bool meta_fresh(const fs::path &p, std::string_view vd_col, int avail_hour,
                std::string_view today, int dedup_seconds) {
  long age = file_age_seconds(p);
  if (age < 0)
    return false;
  if (age < dedup_seconds)
    return true;
  if (vd_col.empty())
    return file_write_date(p) == today;
  return file_max_vd(p, vd_col) >= data_horizon(avail_hour, today);
}

} // namespace misc
