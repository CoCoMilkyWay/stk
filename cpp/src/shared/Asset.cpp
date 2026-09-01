#include "shared/Asset.hpp"
#include "codec/binary_decoder_L2.hpp"
#include "gui/task_database/infrastructure/ScanThreadPool.hpp"
#include "shared/AssetInfo.hpp"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <cassert>
#include <chrono>
#include <filesystem>
#include <future>
#include <mutex>
#include <set>
#include <unordered_map>

// ============================================================================
// AssetItem Implementation
// ============================================================================

AssetItem::AssetItem(size_t id, std::string code, std::string name, std::string exch, std::string start, std::string end)
    : asset_id(id),
      asset_code(std::move(code)),
      asset_name(std::move(name)),
      exchange(std::move(exch)),
      exchange_type(L2::infer_exchange_type(asset_code)),
      start_date(std::move(start)),
      end_date(std::move(end)) {}

// ============================================================================
// Asset Implementation
// ============================================================================

// ============================================================================
// Binary Database Scan (Coroutine Version)
// ============================================================================

boost::asio::awaitable<void> Asset::coro_scan_binary_database(
    boost::asio::io_context &io,
    const std::string &orders_dir,
    const std::string &binary_extension,
    std::shared_ptr<GUI::Database::ScanThreadPool> thread_pool) {

  namespace fs = std::filesystem;
  using boost::asio::use_awaitable;

  // Clear browser statistics cache - will be recomputed on next Browser tab access
  date_stats.clear();
  asset_stats.clear(); // 同理: Encode 页首次渲染时重算

  binary.scanned = true;
  binary.path = orders_dir;
  binary.exists = fs::exists(orders_dir) && fs::is_directory(orders_dir);

  if (!binary.exists) {
    binary.dates.clear();
    binary.min_date.clear();
    binary.max_date.clear();
    binary.encoded_assets = 0;
    binary.total_orders = 0;
    binary.orders_size_gb = 0.0;
    binary.day_mtimes.clear(); // 库没了, 增量基线也作废
    binary.dirty_dates.clear();
    all_dates.clear();
    for (auto &item : items)
      item.date_info.clear();
    co_return;
  }

  // Day path structure
  struct DayPath {
    std::string path;
    std::string date_str; // YYYYMMDD
  };

  // Collect all day paths.
  //
  // 并行粒度是"天"而不是"月": 每个 .bin 的读头在冷页缓存下都是一次随机 IO,
  // 按月切的话新库只有一两个月目录 = 实际单线程 (实测 9.4 万文件 8.0s);
  // 按天切能把 NVMe 的队列深度喂满 (同样 9.4 万文件 0.6s).
  std::vector<DayPath> day_paths;
  std::set<std::string> current_dates;             // 这次 readdir 到的全部天
  std::unordered_map<std::string, int64_t> mtimes; // date -> 本次 mtime

  for (const auto &year_entry : fs::directory_iterator(orders_dir)) {
    if (!year_entry.is_directory())
      continue;
    std::string year_str = year_entry.path().filename().string();
    for (const auto &month_entry : fs::directory_iterator(year_entry.path())) {
      if (!month_entry.is_directory())
        continue;
      std::string month_str = month_entry.path().filename().string();
      for (const auto &day_entry : fs::directory_iterator(month_entry.path())) {
        if (!day_entry.is_directory())
          continue;
        const std::string date_str =
            year_str + month_str + day_entry.path().filename().string();
        current_dates.insert(date_str);

        // 取不到 mtime 就当它变了 (重扫), 不去猜
        std::error_code ec;
        const auto wt = fs::last_write_time(day_entry.path(), ec);
        mtimes[date_str] = ec ? 0 : wt.time_since_epoch().count();

        day_paths.push_back({day_entry.path().string(), date_str});
      }
    }
  }

  // 只重扫"目录动过的"和"编码动过的"; 其余沿用上次的 date_info
  std::vector<DayPath> days_to_scan;
  std::set<std::string> dates_to_purge;
  for (const auto &dp : day_paths) {
    auto prev = binary.day_mtimes.find(dp.date_str);
    const bool unchanged = prev != binary.day_mtimes.end() &&
                           prev->second != 0 &&
                           prev->second == mtimes[dp.date_str] &&
                           binary.dirty_dates.count(dp.date_str) == 0;
    if (unchanged)
      continue;
    days_to_scan.push_back(dp);
    dates_to_purge.insert(dp.date_str);
  }

  // 整个日目录被删掉的, 旧条目也要清 —— 否则它会一直冒充"这天有数据"
  for (const auto &[date, mtime] : binary.day_mtimes) {
    if (!current_dates.count(date))
      dates_to_purge.insert(date);
  }

  for (auto &item : items) {
    for (const auto &date : dates_to_purge)
      item.date_info.erase(date);
  }

  binary.day_mtimes = std::move(mtimes);
  binary.dirty_dates.clear();

  // Build asset lookup map
  std::unordered_map<std::string, size_t> asset_map;
  for (size_t i = 0; i < items.size(); ++i) {
    asset_map[items[i].asset_code + "." + items[i].exchange] = i;
  }

  // Shared result accumulator.
  // 只装本次重扫的天; 聚合量 (总条数/体积/每天覆盖数) 最后从完整的 date_info
  // 统一重算, 否则沿用下来的那些天会被漏掉.
  struct ScanResult {
    std::mutex mutex;
    std::unordered_map<size_t, std::unordered_map<std::string, DateInfo>> asset_date_info;
  };
  auto result = std::make_shared<ScanResult>();

  // Lambda for scanning a single day (runs in thread pool).
  //
  // 目录是扁平的: orders/YYYY/MM/DD/<CODE>.<EX>.bin, 一天一层 readdir 就够,
  // 不再是"一天下面几千个每资产目录、每个目录再 readdir 一次".
  auto scan_day = [&asset_map, &binary_extension, result, this](const DayPath &day_path) {
    std::unordered_map<size_t, DateInfo> local_date_info;

    for (const auto &file_entry : fs::directory_iterator(day_path.path)) {
      const std::string filename = file_entry.path().filename().string();
      if (!filename.ends_with(binary_extension))
        continue;

      // "000023.SZ.bin" → "000023.SZ"
      const std::string asset_full =
          filename.substr(0, filename.size() - binary_extension.size());

      auto it = asset_map.find(asset_full);
      if (it == asset_map.end())
        continue;

      DateInfo di;
      di.orders_encoded = 1;

      // 一次读头同时拿到条数和体积 (文件总长 = 32 + compressed_size), 不再
      // 额外 stat. 头损坏的文件当作没有数据 —— 它本来也解不出来.
      size_t order_count = 0, file_size = 0;
      if (!L2::BinaryDecoder_L2::read_file_stats(file_entry.path().string(), order_count, file_size))
        continue;

      di.order_count = order_count;
      di.orders_file_size = file_size;
      local_date_info[it->second] = std::move(di);
    }

    // Merge into shared result
    {
      std::lock_guard<std::mutex> lock(result->mutex);
      for (auto &[asset_idx, info] : local_date_info) {
        result->asset_date_info[asset_idx][day_path.date_str] = std::move(info);
      }
    }

    scan_days_done.fetch_add(1, std::memory_order_relaxed);
  };

  // Submit all day scan tasks to thread pool
  scan_days_done.store(0, std::memory_order_relaxed);
  scan_days_total.store(days_to_scan.size(), std::memory_order_relaxed);

  std::vector<std::future<void>> futures;
  futures.reserve(days_to_scan.size());
  for (const auto &day_path : days_to_scan) {
    futures.push_back(thread_pool->submit([scan_day, day_path]() { scan_day(day_path); }));
  }

  // Wait for all tasks, yielding to GUI periodically
  while (true) {
    bool all_done = true;
    for (auto &future : futures) {
      if (future.valid() && future.wait_for(std::chrono::milliseconds(0)) != std::future_status::ready) {
        all_done = false;
        break;
      }
    }
    if (all_done)
      break;

    boost::asio::steady_timer timer(io, std::chrono::milliseconds(50));
    co_await timer.async_wait(use_awaitable);
  }

  // Collect all futures
  for (auto &future : futures) {
    if (future.valid()) {
      future.get();
    }
  }

  // 灌入本次重扫的天 (要重扫的那些天的旧条目已在上面清掉了)
  for (const auto &[asset_idx, date_map] : result->asset_date_info) {
    for (const auto &[date, info] : date_map) {
      items[asset_idx].date_info[date] = info;
    }
  }

  all_dates.assign(current_dates.begin(), current_dates.end());

  // 聚合量从完整的 date_info 重算 —— 增量扫描下 result 里只有本次重扫的天,
  // 沿用下来的那些天必须一起算进来.
  binary.total_orders = 0;
  binary.dates.clear();
  float total_orders_size = 0.0;
  for (const auto &item : items) {
    for (const auto &[date, info] : item.date_info) {
      binary.total_orders += info.order_count;
      total_orders_size += static_cast<float>(info.orders_file_size);
      binary.dates.insert(date);
    }
  }
  binary.orders_size_gb = total_orders_size / (1024.0 * 1024.0 * 1024.0);

  // 取自 binary.dates 而不是 all_dates: 后者含空日目录 (readdir 到了但里面
  // 没有一个能对上 asset_map 的文件), 会把区间往外撑.
  if (!binary.dates.empty()) {
    binary.min_date = *binary.dates.begin();
    binary.max_date = *binary.dates.rbegin();
  } else {
    binary.min_date.clear();
    binary.max_date.clear();
  }

  binary.encoded_assets = 0;
  for (const auto &item : items) {
    if (!item.date_info.empty()) {
      binary.encoded_assets++;
    }
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
  using boost::asio::use_awaitable;

  // Clear browser statistics cache - will be recomputed on next Browser tab access
  date_stats.clear();
  asset_stats.clear(); // 同理: Encode 页首次渲染时重算

  archive.scanned = true;
  archive.path = archive_dir;
  archive.exists = fs::exists(archive_dir) && fs::is_directory(archive_dir);

  if (!archive.exists) {
    archive.dates.clear();
    archive.min_date.clear();
    archive.max_date.clear();
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
    std::set<std::string> archive_dates;
    size_t total_files = 0;
    float total_size = 0.0;
  };
  auto result = std::make_shared<ScanResult>();

  // Lambda for scanning a single month (runs in thread pool)
  auto scan_month = [&archive_extension, result, this](const MonthPath &month_path) {
    std::set<std::string> local_dates;
    size_t local_files = 0;
    float local_size = 0.0;

    try {
      for (const auto &file_entry : fs::directory_iterator(month_path.path)) {
        if (!file_entry.is_regular_file())
          continue;

        const std::string ext = file_entry.path().extension().string();
        if (ext == archive_extension) {
          const std::string filename = file_entry.path().stem().string();
          if (filename.size() == 8 && std::all_of(filename.begin(), filename.end(), ::isdigit)) {
            local_dates.insert(filename);
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

  // Wait for all tasks, yielding to GUI periodically
  while (true) {
    bool all_done = true;
    for (auto &future : futures) {
      if (future.valid() && future.wait_for(std::chrono::milliseconds(0)) != std::future_status::ready) {
        all_done = false;
        break;
      }
    }
    if (all_done)
      break;

    boost::asio::steady_timer timer(io, std::chrono::milliseconds(50));
    co_await timer.async_wait(use_awaitable);
  }

  // Collect all futures
  for (auto &future : futures) {
    if (future.valid()) {
      future.get();
    }
  }

  // Merge results into Asset
  archive.dates = result->archive_dates;
  archive.total_files = result->total_files;
  archive.total_size_gb = result->total_size / (1024.0 * 1024.0 * 1024.0);

  if (!archive.dates.empty()) {
    archive.min_date = *archive.dates.begin();
    archive.max_date = *archive.dates.rbegin();
  } else {
    archive.min_date.clear();
    archive.max_date.clear();
  }

  // all_dates = binary ∪ archive.
  // 不能只在 binary 为空时才取 archive: 那样 binary 一旦有日期, all_dates 就
  // 永远等于"已编码的日子", encode 遍历时全部命中 skip, 新到的 archive 日子
  // 再也进不来 (增量编码静默失效).
  {
    std::set<std::string> merged(all_dates.begin(), all_dates.end());
    merged.insert(archive.dates.begin(), archive.dates.end());
    all_dates.assign(merged.begin(), merged.end());
  }

  co_return;
}

// ============================================================================
// Backtest Coverage Analysis
// ============================================================================

void Asset::compute_backtest_coverage(const std::string &start, const std::string &end,
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
  // stock_days 行格式 ["YYYY-MM-DD", "0"/"1"], 此处转 compact "YYYYMMDD" 与
  // binary/archive dates 对齐; 调用方保证基本面 Ready (ScanService assert)
  const auto &stock_days = assetinfo.get_stock_days();
  assert(!stock_days.empty() && "基本面交易日历未就绪");
  for (const auto &day : stock_days) {
    if (day.size() < 2 || day[1] != "1")
      continue; // 非交易日
    const std::string &dashed = day[0];
    std::string date = dashed.substr(0, 4) + dashed.substr(5, 2) + dashed.substr(8, 2);
    if (date >= start && date <= end) {
      backtest.required_dates.insert(std::move(date));
    }
  }

  // Step 2: Compute binary coverage
  for (const auto &date : backtest.required_dates) {
    if (binary.dates.count(date)) {
      backtest.covered_dates.insert(date);
    } else {
      backtest.missing_dates.insert(date);
    }
  }

  // Step 3: Check archive availability for missing dates
  if (archive.scanned && archive.exists) {
    for (const auto &date : backtest.missing_dates) {
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

  // Step 4: Calculate backtest range statistics
  //
  // 有数据的天数直接从 binary.dates 取交集 —— 原先为此遍历全部 items 的
  // date_info (五百万条) 只为数出几百个日期.
  binary.backtest_orders = 0;
  float backtest_orders_size = 0.0;
  std::set<std::string> order_dates_in_backtest;

  for (const auto &date : binary.dates) {
    if (date >= start && date <= end)
      order_dates_in_backtest.insert(date);
  }

  for (const auto &item : items) {
    for (const auto &[date, info] : item.date_info) {
      if (date >= start && date <= end && info.orders_encoded) {
        binary.backtest_orders += info.order_count;
        backtest_orders_size += static_cast<float>(info.orders_file_size);
      }
    }
  }

  binary.backtest_orders_size_gb = backtest_orders_size / (1024.0 * 1024.0 * 1024.0);
  binary.backtest_order_days = order_dates_in_backtest.size();
}
