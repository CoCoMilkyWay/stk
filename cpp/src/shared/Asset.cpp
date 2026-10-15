#include "shared/Asset.hpp"
#include "gui/coro/CoroManager.hpp"
#include "gui/task_database/infrastructure/ScanThreadPool.hpp"
#include "shared/AssetAxis.hpp"
#include "shared/AssetInfo.hpp"
#include "shared/Config.hpp"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>

#include <cassert>
#include <chrono>
#include <filesystem>
#include <future>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace {

// 等一批线程池任务跑完, 期间每 50ms 让一次步给 GUI 渲染.
// (重活全在线程池上, GUI 线程只轮询, 让步间隔可以放宽)
boost::asio::awaitable<void> await_futures(boost::asio::io_context &io,
                                           std::vector<std::future<void>> &futures) {
  while (true) {
    bool all_done = true;
    for (auto &future : futures) {
      if (future.valid() &&
          future.wait_for(std::chrono::milliseconds(0)) != std::future_status::ready) {
        all_done = false;
        break;
      }
    }
    if (all_done)
      break;
    co_await Coro::Yield(io, std::chrono::milliseconds(50));
  }

  // get() 而不是只等 —— 任务里的异常要在这里炸出来, 不能吞在 future 里
  for (auto &future : futures) {
    if (future.valid())
      future.get();
  }
}

} // namespace

// ============================================================================
// AssetItem Implementation
// ============================================================================

AssetItem::AssetItem(size_t id, std::string code, std::string name, std::string exch, misc::Ymd start, misc::Ymd end)
    : asset_id(id),
      asset_code(std::move(code)),
      asset_name(std::move(name)),
      exchange(std::move(exch)),
      exchange_type(L2::infer_exchange_type(asset_code)),
      start_date(start),
      end_date(end) {}

// ============================================================================
// Asset Implementation
// ============================================================================

// ============================================================================
// Binary Database Scan (Coroutine Version)
// ============================================================================

boost::asio::awaitable<void> Asset::coro_scan_binary_database(
    boost::asio::io_context &io,
    const std::string &orders_dir,
    std::shared_ptr<GUI::Database::ScanThreadPool> thread_pool) {

  namespace fs = std::filesystem;

  // 统计缓存作废 —— 底下的 date_info 正要被改写. 扫描末尾由
  // coro_compute_coverage_statistics 一次重建 (见 ScanService Phase 5);
  // 这中间 Table / Encode 页面按"空"渲染成 Waiting, 不给半旧的数.
  date_stats.clear();
  date_gaps.clear();
  asset_stats.clear();

  scan_days_done.store(0, std::memory_order_relaxed);
  scan_days_total.store(0, std::memory_order_relaxed);

  binary.scanned = true;
  binary.path = orders_dir;
  binary.exists = fs::exists(orders_dir) && fs::is_directory(orders_dir);

  // 整体重建: 轴、全部 date_info、账目. 不做增量 (见 Asset.hpp 日期轴的说明).
  day_records.clear();
  date_axis.clear();
  for (auto &item : items)
    item.date_info.clear();
  all_dates.clear();
  binary.dates.clear();
  binary.min_date = 0;
  binary.max_date = 0;
  binary.total_orders = 0;
  binary.orders_size_gb = 0.0;
  binary.encoded_assets = 0; // 由 coro_compute_coverage_statistics 填

  if (!binary.exists)
    co_return;

  // ------------------------------------------------------------------
  // 1. 列出全部日文件 (orders/YYYY/MM/YYYYMMDD<ext>, 两层 readdir 共几百个
  //    条目). 别的东西 (编码中的 .tmp 等) 不认.
  // ------------------------------------------------------------------
  struct DayPath {
    std::string path;
    misc::Ymd date;
  };
  std::vector<DayPath> days;

  for (const auto &year_entry : fs::directory_iterator(orders_dir)) {
    if (!year_entry.is_directory())
      continue;
    for (const auto &month_entry : fs::directory_iterator(year_entry.path())) {
      if (!month_entry.is_directory())
        continue;
      for (const auto &day_entry : fs::directory_iterator(month_entry.path())) {
        if (!day_entry.is_regular_file() || day_entry.path().extension().string() != config::BINARY_EXTENSION)
          continue;
        const std::string stem = day_entry.path().stem().string();
        const misc::Ymd date = stem.size() == 8 ? misc::parse_ymd(stem) : 0;
        if (!date)
          continue;
        days.push_back({day_entry.path().string(), date});
      }
    }
  }
  std::sort(days.begin(), days.end(),
            [](const DayPath &a, const DayPath &b) { return a.date < b.date; });

  // ------------------------------------------------------------------
  // 2. 轴 = 全部日文件 (升序); 每个资产的向量一次扩到位, 之后各天的任务只往
  //    自己那一列写 (不同天 = 不同下标, 线程间不相交, 免锁免合并).
  // ------------------------------------------------------------------
  date_axis.reserve(days.size());
  for (const auto &day : days)
    date_axis.push_back(day.date);
  all_dates = date_axis;
  for (auto &item : items)
    item.date_info.assign(days.size(), DateInfo{});

  // ------------------------------------------------------------------
  // 3. 每天一个任务: 一次 open, pread 头 + 尾部目录, 灌进当天那一列.
  // ------------------------------------------------------------------
  struct DayOut {
    EncodeDayRecord record; // 账目
    size_t orders = 0;      // 当天条数
    double bytes = 0.0;     // 当天帧体积
    size_t frames = 0;      // 当天有帧的资产数
  };
  std::vector<DayOut> outs(days.size());

  assert(items.size() == asset_axis().size() && "items 未与 A 轴对齐 (AssetLoader::load 没跑?)");

  auto scan_day = [this, &days, &outs](size_t d) {
    DayOut &out = outs[d];
    std::vector<EncodeDayIndexEntry> index;
    const bool found = read_day_file(days[d].path, out.record, index);
    assert(found && "日文件在 readdir 与 open 之间消失 (扫描期间有人在动 orders?)");
    (void)found;

    for (const auto &entry : index) {
      if (entry.is_tombstone())
        continue;
      assert(entry.asset_id < items.size() && "日文件目录里的 A 轴下标超出 items");
      DateInfo &di = items[entry.asset_id].date_info[d];
      di.offset = entry.offset;
      di.order_count = entry.order_count;
      di.compressed_size = entry.compressed_size;
      di.price_base = entry.price_base;
      out.orders += entry.order_count;
      out.bytes += static_cast<double>(entry.compressed_size);
      ++out.frames;
    }

    scan_days_done.fetch_add(1, std::memory_order_relaxed);
  };

  scan_days_total.store(days.size(), std::memory_order_relaxed);

  std::vector<std::future<void>> futures;
  futures.reserve(days.size());
  for (size_t d = 0; d < days.size(); ++d)
    futures.push_back(thread_pool->submit([&scan_day, d]() { scan_day(d); }));

  co_await await_futures(io, futures);

  // ------------------------------------------------------------------
  // 4. 汇总 (按天, 几百项; 全库 date_info 那趟遍历归 coverage 阶段)
  // ------------------------------------------------------------------
  size_t total_orders = 0;
  double total_bytes = 0.0;
  for (size_t d = 0; d < days.size(); ++d) {
    DayOut &out = outs[d];
    total_orders += out.orders;
    total_bytes += out.bytes;
    // 一帧都没有的日文件 (全是墓碑) 不算"有这天", 否则会把 min/max 区间往外撑
    if (out.frames > 0)
      binary.dates.insert(days[d].date);
    day_records[days[d].date] = out.record;
  }

  binary.total_orders = total_orders;
  binary.orders_size_gb = static_cast<float>(total_bytes / (1024.0 * 1024.0 * 1024.0));

  if (!binary.dates.empty()) {
    binary.min_date = *binary.dates.begin();
    binary.max_date = *binary.dates.rbegin();
  }

  co_return;
}

// ============================================================================
// Archive Database Scan (Coroutine Version)
// ============================================================================

boost::asio::awaitable<void> Asset::coro_scan_archive_database(
    boost::asio::io_context &io,
    const std::string &archive_dir,
    const std::string &archive_extension,
    std::shared_ptr<GUI::Database::ScanThreadPool> thread_pool) {

  namespace fs = std::filesystem;

  // 同 binary 扫描: 统计缓存作废, 扫描末尾一次重建
  date_stats.clear();
  date_gaps.clear();
  asset_stats.clear();

  archive.scanned = true;
  archive.path = archive_dir;
  archive.exists = fs::exists(archive_dir) && fs::is_directory(archive_dir);

  if (!archive.exists) {
    archive.dates.clear();
    archive.min_date = 0;
    archive.max_date = 0;
    archive.total_files = 0;
    archive.total_size_gb = 0.0;
    co_return; // all_dates 保留 binary 扫出的日期
  }

  // Month path structure
  struct MonthPath {
    std::string path;
  };

  // Collect all month paths (archive_dir/YYYY/YYYYMM/).
  // 与 binary 扫描同理: 按年切只有十来个任务, 按月切才喂得满线程池.
  std::vector<MonthPath> month_paths;
  for (const auto &year_entry : fs::directory_iterator(archive_dir)) {
    if (!year_entry.is_directory())
      continue;
    for (const auto &month_entry : fs::directory_iterator(year_entry.path())) {
      if (!month_entry.is_directory())
        continue;
      month_paths.push_back({month_entry.path().string()});
    }
  }

  // Shared result accumulator
  struct ScanResult {
    std::mutex mutex;
    std::set<misc::Ymd> archive_dates;
    size_t total_files = 0;
    float total_size = 0.0;
  };
  auto result = std::make_shared<ScanResult>();

  // Lambda for scanning a single month (runs in thread pool)
  auto scan_month = [&archive_extension, result, this](const MonthPath &month_path) {
    std::set<misc::Ymd> local_dates;
    size_t local_files = 0;
    float local_size = 0.0;

    try {
      for (const auto &file_entry : fs::directory_iterator(month_path.path)) {
        if (!file_entry.is_regular_file())
          continue;

        const std::string ext = file_entry.path().extension().string();
        if (ext == archive_extension) {
          const std::string filename = file_entry.path().stem().string();
          if (const misc::Ymd date = filename.size() == 8 ? misc::parse_ymd(filename) : 0) {
            local_dates.insert(date);
            local_files++;
            try {
              local_size += static_cast<float>(fs::file_size(file_entry.path()));
            } catch (...) {
            }
          }
        }
      }
    } catch (...) {
    }

    // Merge into shared result
    {
      std::lock_guard<std::mutex> lock(result->mutex);
      result->archive_dates.insert(local_dates.begin(), local_dates.end());
      result->total_files += local_files;
      result->total_size += local_size;
    }

    scan_days_done.fetch_add(1, std::memory_order_relaxed);
  };

  // Submit all month scan tasks to thread pool
  scan_days_done.store(0, std::memory_order_relaxed);
  scan_days_total.store(month_paths.size(), std::memory_order_relaxed);

  std::vector<std::future<void>> futures;
  futures.reserve(month_paths.size());
  for (const auto &month_path : month_paths) {
    futures.push_back(thread_pool->submit([scan_month, month_path]() { scan_month(month_path); }));
  }

  co_await await_futures(io, futures);

  // Merge results into Asset
  archive.dates = result->archive_dates;
  archive.total_files = result->total_files;
  archive.total_size_gb = result->total_size / (1024.0 * 1024.0 * 1024.0);

  if (!archive.dates.empty()) {
    archive.min_date = *archive.dates.begin();
    archive.max_date = *archive.dates.rbegin();
  } else {
    archive.min_date = 0;
    archive.max_date = 0;
  }

  // all_dates = binary ∪ archive.
  // 不能只在 binary 为空时才取 archive: 那样 binary 一旦有日期, all_dates 就
  // 永远等于"已编码的日子", encode 遍历时全部命中 skip, 新到的 archive 日子
  // 再也进不来 (增量编码静默失效).
  {
    std::set<misc::Ymd> merged(all_dates.begin(), all_dates.end());
    merged.insert(archive.dates.begin(), archive.dates.end());
    all_dates.assign(merged.begin(), merged.end());
  }

  co_return;
}

// ============================================================================
// Backtest Coverage Analysis
// ============================================================================

void Asset::compute_backtest_coverage(misc::Ymd start, misc::Ymd end,
                                      const AssetInfo &assetinfo) {
  // Clear previous results
  backtest.start = start;
  backtest.end = end;
  backtest.required_dates.clear();
  backtest.covered_dates.clear();
  backtest.missing_dates.clear();
  backtest.can_encode.clear();
  backtest.need_download.clear();

  // Step 1: Ground truth = 基本面交易日历 (权威, archive 自身缺日也能发现)
  // 调用方保证基本面 Ready (ScanService assert)
  const auto &stock_days = assetinfo.get_stock_days();
  assert(!stock_days.empty() && "基本面交易日历未就绪");
  for (const CalendarDay &day : stock_days)
    if (day.trading && day.date >= start && day.date <= end)
      backtest.required_dates.insert(day.date);

  // Step 2: Compute binary coverage
  for (const misc::Ymd date : backtest.required_dates) {
    if (binary.dates.count(date)) {
      backtest.covered_dates.insert(date);
    } else {
      backtest.missing_dates.insert(date);
    }
  }

  // Step 3: Check archive availability for missing dates
  if (archive.scanned && archive.exists) {
    for (const misc::Ymd date : backtest.missing_dates) {
      if (archive.dates.count(date)) {
        backtest.can_encode.insert(date);
      } else {
        backtest.need_download.insert(date);
      }
    }
  } else {
    // No archive, all missing dates need download
    backtest.need_download = backtest.missing_dates;
  }

  // Step 4: 区间内有数据的天数 (区间内条数/体积要遍历 date_info, 归
  // coro_compute_coverage_statistics 那趟)
  size_t backtest_order_days = 0;
  for (const misc::Ymd date : binary.dates) {
    if (date >= start && date <= end)
      ++backtest_order_days;
  }
  binary.backtest_order_days = backtest_order_days;
}

// ============================================================================
// Coverage Statistics (Browser / Table / Encode 共用的一份统计)
// ============================================================================

boost::asio::awaitable<void> Asset::coro_compute_coverage_statistics(
    boost::asio::io_context &io,
    const AssetInfo &assetinfo,
    std::shared_ptr<GUI::Database::ScanThreadPool> thread_pool) {
  const auto &stock_info = assetinfo.get_stock_info();
  const auto &stock_days = assetinfo.get_stock_days();
  const auto &suspended = assetinfo.get_suspended();

  // 全程算在局部, 最后一次性换进成员 —— 中途 UI 读到的是上一版完整结果
  std::unordered_map<misc::Ymd, DateStats> local_date_stats;
  std::map<misc::Ymd, DateGap> local_date_gaps;
  std::vector<AssetStats> local_asset_stats(items.size());

  std::vector<std::future<void>> futures;
  const size_t n_workers = thread_pool->get_num_workers();

  // 全库 date_info 唯一的一趟遍历, 按资产段切给线程池: 每资产的全库口径两列
  // (Table 的 Days / Orders) 写各自的 local_asset_stats[i], 不相交, 免锁;
  // 顺路把回测区间内的条数/体积和"至少有一天有数据"的资产数按段累加.
  // 区间判定按轴下标预算一次.
  struct RangeSum {
    size_t orders = 0;
    double bytes = 0.0;
    size_t encoded_assets = 0;
  };
  std::vector<uint8_t> in_range(date_axis.size(), 0);
  for (size_t d = 0; d < date_axis.size(); ++d)
    in_range[d] = date_axis[d] >= backtest.start && date_axis[d] <= backtest.end;

  const size_t per_task = std::max<size_t>(1, (items.size() + n_workers - 1) / n_workers);
  std::vector<RangeSum> range_sums((items.size() + per_task - 1) / per_task);
  for (size_t t = 0; t < range_sums.size(); ++t) {
    const size_t begin = t * per_task;
    const size_t end = std::min(items.size(), begin + per_task);
    RangeSum *sum = &range_sums[t];
    futures.push_back(thread_pool->submit([this, &local_asset_stats, &in_range, sum, begin, end]() {
      for (size_t i = begin; i < end; ++i) {
        AssetStats &st = local_asset_stats[i];
        const auto &di = items[i].date_info;
        assert(di.size() <= in_range.size() && "date_info 下标超出日期轴");
        for (size_t d = 0; d < di.size(); ++d) {
          if (!di[d].has_binaries())
            continue; // 密集向量: 零值槽位不是"有数据的天"
          ++st.total_days;
          st.total_orders += di[d].order_count;
          if (in_range[d]) {
            sum->orders += di[d].order_count;
            sum->bytes += static_cast<double>(di[d].compressed_size);
          }
        }
        if (st.total_days > 0)
          ++sum->encoded_assets;
      }
    }));
  }

  // 每资产的常量先摊平: 交易所小写全码 / 上市 / 退市. 这些原先是在
  // 日期×资产的内循环里现算的, 五百万次 string 拼接 + map 查找.
  struct AssetKey {
    std::string full_code;     // "sh.600128"
    misc::Ymd list_date = 0;   // 0 = 不限
    misc::Ymd delist_date = 0; // 0 = 不限
    bool excluded = false;     // 北交所: L2 archive 从不覆盖
  };
  std::vector<AssetKey> keys(items.size());
  std::unordered_map<std::string, size_t> key_to_idx; // 停牌名单 (全码) → 资产下标
  key_to_idx.reserve(items.size());
  for (size_t i = 0; i < items.size(); ++i) {
    AssetKey &k = keys[i];
    if (items[i].exchange == "BJ") {
      k.excluded = true;
      continue;
    }
    std::string exchange_lower = items[i].exchange;
    std::transform(exchange_lower.begin(), exchange_lower.end(), exchange_lower.begin(), ::tolower);
    k.full_code = exchange_lower + "." + items[i].asset_code;
    key_to_idx[k.full_code] = i;

    auto info_it = stock_info.find(k.full_code);
    if (info_it != stock_info.end()) {
      k.list_date = info_it->second.ipo_date;
      k.delist_date = info_it->second.out_date;
    }
  }

  const bool has_db_range = binary.min_date && binary.max_date;
  const bool has_bt_range = backtest.start && backtest.end;

  // 要算的交易日先挑出来 (区间外的不进任务, 进度分母也只数这些).
  // per-date 统计沿用全库范围; per-asset 缺口只看回测区间 —— 区间外没编
  // 码不算缺, 那不是要跑的行情.
  struct DayJob {
    misc::Ymd date;
    size_t didx; // 日期轴下标, 内循环 O(1) 定址 (原先是 资产数 次 hash find)
    bool in_db_range;
    bool in_backtest;
    bool archive_has_day;
  };
  std::vector<DayJob> jobs;
  jobs.reserve(stock_days.size());
  for (const CalendarDay &day : stock_days) {
    const bool in_db_range =
        !has_db_range || (day.date >= binary.min_date && day.date <= binary.max_date);
    const bool in_backtest = has_bt_range && day.trading &&
                             day.date >= backtest.start && day.date <= backtest.end;
    if (!in_db_range && !in_backtest)
      continue;

    DayJob job;
    job.date = day.date;
    job.didx = date_idx(day.date);
    job.in_db_range = in_db_range;
    job.in_backtest = in_backtest;
    job.archive_has_day = archive.dates.count(day.date) > 0;
    jobs.push_back(job);
  }

  // 这个双重循环 (交易日 × 全部资产) 是整个 coverage 阶段唯一要等的部分,
  // 进度就报它. 上面那几趟 items 遍历相比之下可以忽略.
  scan_days_done.store(0, std::memory_order_relaxed);
  scan_days_total.store(jobs.size(), std::memory_order_relaxed);

  // 按连续的交易日段切任务: 每段自己的 date_stats / date_gaps (按天, 段间
  // 不相交) 和一份 per-asset 部分和, 结束后按段序合并 —— 缺失日期样本取的
  // 是"最早的 kMissingSample 个", 段序 = 时间序才能保住这一点.
  // 段数取线程数的两倍摊平尾部; 每段一份 AssetStats 向量是几百 KB.
  struct Chunk {
    std::unordered_map<misc::Ymd, DateStats> date_stats;
    std::map<misc::Ymd, DateGap> date_gaps;
    std::vector<AssetStats> asset_stats; // 只填回测口径 (expected/missing/样本)
  };
  const size_t n_chunks = std::max<size_t>(1, std::min(jobs.size(), n_workers * 2));
  std::vector<Chunk> chunks(n_chunks);
  const size_t per_chunk = jobs.empty() ? 1 : (jobs.size() + n_chunks - 1) / n_chunks;

  auto run_chunk = [this, &jobs, &keys, &key_to_idx, &suspended](size_t begin, size_t end, Chunk &out) {
    out.asset_stats.assign(items.size(), AssetStats{});
    std::vector<uint8_t> susp(items.size(), 0); // 当日停牌位图, 按资产下标

    for (size_t j = begin; j < end; ++j) {
      const DayJob &job = jobs[j];

      // 当日停牌名单 (无条目 = 该日无人停牌) → 位图, 内循环免字符串 hash
      std::fill(susp.begin(), susp.end(), 0);
      auto susp_it = suspended.find(job.date);
      if (susp_it != suspended.end()) {
        for (const auto &code : susp_it->second) {
          auto kit = key_to_idx.find(code);
          if (kit != key_to_idx.end())
            susp[kit->second] = 1;
        }
      }

      DateStats *ds = job.in_db_range ? &out.date_stats[job.date] : nullptr;
      // 一天一个条目, 哪怕零缺口 —— By Date 表要能说"这天检查过, 没事"
      DateGap *dg = job.in_backtest ? &out.date_gaps[job.date] : nullptr;

      for (size_t i = 0; i < items.size(); ++i) {
        const AssetKey &k = keys[i];
        if (k.excluded || susp[i])
          continue;
        if (k.list_date && job.date < k.list_date)
          continue;
        // 退市日当天已经不交易了 (最后交易日是它之前那个交易日), 用 > 的话
        // 每只退市股都会平白多出一天缺口 —— 实测 145 只退市股各缺 1 天, 缺
        // 的正是各自的 delist_date.
        if (k.delist_date && job.date >= k.delist_date)
          continue;

        const bool has_orders = items[i].date_at(job.didx).has_binaries();

        if (ds) {
          ds->total_assets++;
          if (has_orders)
            ds->assets_with_orders++;
        }

        if (job.in_backtest) {
          AssetStats &st = out.asset_stats[i];
          st.expected_days++;
          dg->expected++;
          if (!has_orders) {
            st.orders_missing++;
            dg->orders_missing++;
            if (st.orders_missing_sample.size() < kMissingSample)
              st.orders_missing_sample.push_back(job.date);
          }
          if (!job.archive_has_day) {
            st.archive_missing++;
            dg->archive_missing++;
            if (st.archive_missing_sample.size() < kMissingSample)
              st.archive_missing_sample.push_back(job.date);
          }
        }
      }

      scan_days_done.fetch_add(1, std::memory_order_relaxed);
    }
  };

  for (size_t c = 0; c < n_chunks; ++c) {
    const size_t begin = c * per_chunk;
    const size_t end = std::min(jobs.size(), begin + per_chunk);
    if (begin >= end)
      break;
    Chunk *out = &chunks[c];
    futures.push_back(thread_pool->submit([&run_chunk, begin, end, out]() { run_chunk(begin, end, *out); }));
  }

  co_await await_futures(io, futures);

  {
    RangeSum total;
    for (const RangeSum &s : range_sums) {
      total.orders += s.orders;
      total.bytes += s.bytes;
      total.encoded_assets += s.encoded_assets;
    }
    binary.backtest_orders = total.orders;
    binary.backtest_orders_size_gb = static_cast<float>(total.bytes / (1024.0 * 1024.0 * 1024.0));
    binary.encoded_assets = total.encoded_assets;
  }

  // 按段序合并. 段的 per-asset 部分和只有回测口径那几列, 全库口径两列已由
  // 上面的资产段任务直接写进 local_asset_stats.
  for (Chunk &chunk : chunks) {
    local_date_stats.merge(chunk.date_stats);
    local_date_gaps.merge(chunk.date_gaps);
    for (size_t i = 0; i < chunk.asset_stats.size(); ++i) {
      AssetStats &st = local_asset_stats[i];
      AssetStats &cs = chunk.asset_stats[i];
      st.expected_days += cs.expected_days;
      st.orders_missing += cs.orders_missing;
      st.archive_missing += cs.archive_missing;
      for (const misc::Ymd d : cs.orders_missing_sample)
        if (st.orders_missing_sample.size() < kMissingSample)
          st.orders_missing_sample.push_back(d);
      for (const misc::Ymd d : cs.archive_missing_sample)
        if (st.archive_missing_sample.size() < kMissingSample)
          st.archive_missing_sample.push_back(d);
    }
  }

  date_stats = std::move(local_date_stats);
  date_gaps = std::move(local_date_gaps);
  asset_stats = std::move(local_asset_stats);
  ++asset_stats_generation;

  co_return;
}
