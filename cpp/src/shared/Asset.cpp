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
#include <map>
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

size_t AssetItem::get_total_trading_days() const {
  return date_info.size();
}

size_t AssetItem::get_encoded_count() const {
  size_t count = 0;
  for (const auto &[_, di] : date_info) {
    if (di.is_fully_encoded())
      count++;
  }
  return count;
}

size_t AssetItem::get_orders_encoded_count() const {
  size_t count = 0;
  for (const auto &[_, di] : date_info)
    count += di.orders_encoded;
  return count;
}

size_t AssetItem::get_missing_count() const {
  return get_total_trading_days() - get_encoded_count();
}

size_t AssetItem::get_analyzed_count() const {
  size_t count = 0;
  for (const auto &[_, di] : date_info)
    count += di.analyzed;
  return count;
}

size_t AssetItem::get_total_order_count() const {
  size_t total = 0;
  for (const auto &[_, di] : date_info)
    total += di.order_count;
  return total;
}

std::vector<std::string> AssetItem::get_missing_dates() const {
  std::vector<std::string> missing;
  for (const auto &[date_str, di] : date_info) {
    if (!di.is_fully_encoded()) {
      missing.push_back(date_str);
    }
  }
  std::sort(missing.begin(), missing.end());
  return missing;
}

std::string AssetItem::get_display_name() const {
  return asset_code + " " + asset_name;
}

// ============================================================================
// Asset Implementation
// ============================================================================

void Asset::compute_asset_statistics() {
  asset_stats.assign(items.size(), AssetStats{});
  const bool has_backtest = !backtest.start.empty() && !backtest.end.empty();

  for (size_t i = 0; i < items.size(); ++i) {
    const AssetItem &item = items[i];
    AssetStats &stats = asset_stats[i];
    stats.total_days = item.date_info.size();

    size_t archive_available = 0, archive_available_bt = 0;
    size_t order_encoded = 0, order_encoded_bt = 0;

    for (const auto &[date, info] : item.date_info) {
      const bool in_backtest =
          has_backtest && date >= backtest.start && date <= backtest.end;
      if (in_backtest)
        stats.backtest_days++;

      if (archive.dates.count(date)) {
        archive_available++;
        if (in_backtest)
          archive_available_bt++;
      }
      if (info.orders_encoded) {
        order_encoded++;
        if (in_backtest)
          order_encoded_bt++;
      }
    }

    stats.archive_missing_db = stats.total_days - archive_available;
    stats.archive_missing_bt = stats.backtest_days - archive_available_bt;
    stats.order_missing_db = stats.total_days - order_encoded;
    stats.order_missing_bt = stats.backtest_days - order_encoded_bt;
  }

  asset_stats_generation++;
}

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
    binary.total_assets = 0;
    binary.encoded_assets = 0;
    binary.complete_assets = 0;
    binary.total_orders = 0;
    binary.orders_size_gb = 0.0;
    all_dates.clear();
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
        day_paths.push_back({day_entry.path().string(),
                             year_str + month_str +
                                 day_entry.path().filename().string()});
      }
    }
  }

  // Build asset lookup map
  std::unordered_map<std::string, size_t> asset_map;
  for (size_t i = 0; i < items.size(); ++i) {
    asset_map[items[i].asset_code + "." + items[i].exchange] = i;
  }

  // Shared result accumulator
  struct ScanResult {
    std::mutex mutex;
    std::set<std::string> all_dates;
    std::set<std::string> order_dates;
    std::map<std::string, size_t> date_coverage;
    size_t total_orders = 0;
    float total_orders_size = 0.0;
    std::unordered_map<size_t, std::unordered_map<std::string, DateInfo>> asset_date_info;
  };
  auto result = std::make_shared<ScanResult>();

  // Lambda for scanning a single day (runs in thread pool).
  //
  // 目录是扁平的: orders/YYYY/MM/DD/<CODE>.<EX>.bin, 一天一层 readdir 就够,
  // 不再是"一天下面几千个每资产目录、每个目录再 readdir 一次".
  auto scan_day = [&asset_map, &binary_extension, result](const DayPath &day_path) {
    size_t local_coverage = 0;
    size_t local_total_orders = 0;
    float local_orders_size = 0.0;
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
      di.orders_file = file_entry.path().string();
      di.orders_encoded = 1;
      // 条数来自文件头 (8 字节), 文件名里不再冗余存放
      di.order_count = L2::BinaryDecoder_L2::read_order_count(di.orders_file);
      try {
        di.orders_file_size = fs::file_size(file_entry.path());
      } catch (...) {
        di.orders_file_size = 0;
      }

      local_total_orders += di.order_count;
      local_orders_size += static_cast<float>(di.orders_file_size);
      local_coverage++;
      local_date_info[it->second] = std::move(di);
    }

    // Merge into shared result
    {
      std::lock_guard<std::mutex> lock(result->mutex);
      result->all_dates.insert(day_path.date_str);
      result->total_orders += local_total_orders;
      result->total_orders_size += local_orders_size;
      if (local_coverage > 0) {
        result->order_dates.insert(day_path.date_str);
        result->date_coverage[day_path.date_str] += local_coverage;
      }

      for (auto &[asset_idx, info] : local_date_info) {
        result->asset_date_info[asset_idx][day_path.date_str] = std::move(info);
      }
    }
  };

  // Submit all day scan tasks to thread pool
  std::vector<std::future<void>> futures;
  futures.reserve(day_paths.size());
  for (const auto &day_path : day_paths) {
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

  // Merge results into Asset
  all_dates.assign(result->all_dates.begin(), result->all_dates.end());
  binary.total_assets = items.size();
  binary.total_orders = result->total_orders;
  binary.orders_size_gb = result->total_orders_size / (1024.0 * 1024.0 * 1024.0);
  binary.database_order_days = result->order_dates.size();

  binary.dates.clear();
  for (const auto &[date, count] : result->date_coverage) {
    if (count > 0) {
      binary.dates.insert(date);
    }
  }

  for (const auto &[asset_idx, date_map] : result->asset_date_info) {
    for (const auto &[date, info] : date_map) {
      items[asset_idx].date_info[date] = info;
    }
  }

  if (!all_dates.empty()) {
    binary.min_date = all_dates.front();
    binary.max_date = all_dates.back();
  } else {
    binary.min_date.clear();
    binary.max_date.clear();
  }

  binary.encoded_assets = 0;
  binary.complete_assets = 0;
  for (const auto &item : items) {
    if (!item.date_info.empty()) {
      binary.encoded_assets++;
      bool is_complete = true;
      for (const auto &[date, info] : item.date_info) {
        if (!info.is_fully_encoded()) {
          is_complete = false;
          break;
        }
      }
      if (is_complete) {
        binary.complete_assets++;
      }
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
  auto scan_month = [&archive_extension, result](const MonthPath &month_path) {
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
  };

  // Submit all month scan tasks to thread pool
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
  backtest.coverage_percent = 0.0;

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

  // Step 4: Compute coverage percentage
  if (!backtest.required_dates.empty()) {
    backtest.coverage_percent = 100.0 * static_cast<float>(backtest.covered_dates.size()) /
                                static_cast<float>(backtest.required_dates.size());
  } else {
    backtest.coverage_percent = 0.0;
  }

  // Step 5: Calculate backtest range statistics (single pass)
  binary.backtest_orders = 0;
  float backtest_orders_size = 0.0;
  std::set<std::string> order_dates_in_backtest;

  // Single pass through all assets and dates
  for (const auto &item : items) {
    for (const auto &[date, info] : item.date_info) {
      if (date >= start && date <= end && info.orders_encoded) {
        binary.backtest_orders += info.order_count;
        backtest_orders_size += static_cast<float>(info.orders_file_size);
        order_dates_in_backtest.insert(date);
      }
    }
  }

  binary.backtest_orders_size_gb = backtest_orders_size / (1024.0 * 1024.0 * 1024.0);
  binary.backtest_order_days = order_dates_in_backtest.size();
}
