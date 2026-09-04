#include "shared/Asset.hpp"
#include "codec/binary_decoder_L2.hpp"
#include "gui/coro/CoroManager.hpp"
#include "gui/task_database/infrastructure/ScanThreadPool.hpp"
#include "misc/logging.hpp"
#include "shared/AssetInfo.hpp"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>

#include <cassert>
#include <chrono>
#include <filesystem>
#include <future>
#include <mutex>
#include <set>
#include <unordered_map>
#include <utility>
#include <vector>

namespace {

// 每处理这么多个"外层元素"(资产 / 交易日) 让一次步.
//
// 批不能太大也不能太小: 一批的耗时就是那一帧的卡顿, 而一次让步至少要等到
// 下一帧才会被 Poll 回来 (让 N 次 ≈ 多花 N 帧). 按一批 5~10ms 取, 全库
// (5800 资产 × 885 交易日) 大约分十几到几十批, 帧不掉, 总时长也就多半秒.
constexpr size_t kAggregateChunk = 512; // 遍历资产的 date_info
constexpr size_t kCoverageChunk = 32;   // 遍历交易日 × 全部资产

// 等一批线程池任务跑完, 期间每 50ms 让一次步给 GUI 渲染.
// (扫描本身在线程池上, 这里只是轮询, 让步间隔可以放宽)
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

  // 统计缓存作废 —— 底下的 date_info 正要被改写. 扫描末尾由
  // coro_compute_coverage_statistics 一次重建 (见 ScanService Phase 5);
  // 这中间 Table / Encode 页面按"空"渲染成 Waiting, 不给半旧的数.
  date_stats.clear();
  date_gaps.clear();
  asset_stats.clear();

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
    day_records.clear();
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
  for (const auto &date : dates_to_purge)
    day_records.erase(date);

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
    std::unordered_map<std::string, EncodeDayRecord> day_records;
  };
  auto result = std::make_shared<ScanResult>();

  // Lambda for scanning a single day (runs in thread pool).
  //
  // 目录是扁平的: orders/YYYY/MM/DD/<CODE>.<EX>.bin, 一天一层 readdir 就够,
  // 不再是"一天下面几千个每资产目录、每个目录再 readdir 一次".
  auto scan_day = [&asset_map, &binary_extension, result, this](const DayPath &day_path) {
    // readdir 一趟拿到当天的 .bin 名单. 它同时是下面那张明细表的采信依据 ——
    // 名单对不上就不用表里的数.
    std::vector<std::pair<size_t, std::string>> bins; // (资产下标, 完整路径)
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

      bins.emplace_back(it->second, file_entry.path().string());
    }

    // 当天的统计文件 —— 账目 (缺口的原因只有编码器知道) 与逐资产明细都在里面.
    // 读不到就是这天既没编过、也没被扫描回填过.
    EncodeDayRecord local_record;
    const bool has_stat = read_encode_day_stat(day_path.path, local_record);

    std::unordered_map<size_t, DateInfo> local_date_info;

    // 快路径: 明细里已经有条数和体积 (见 EncodeDayRecord.hpp), 一个 .bin 都
    // 不用打开. 采信的前提是它记的 .bin 与 readdir 的名单是同一批 —— 个数
    // 相等还不够, 一增一删就会个数相同而内容错位.
    //
    // 墓碑条目不参与比对: 它记的正是"这个资产当天没有 .bin".
    size_t index_bins = 0;
    for (const auto &entry : local_record.assets)
      if (!entry.is_tombstone())
        ++index_bins;

    bool index_usable = has_stat && index_bins == bins.size();
    if (index_usable) {
      for (const auto &entry : local_record.assets) {
        if (entry.is_tombstone())
          continue;
        DateInfo di;
        di.orders_encoded = 1;
        di.order_count = entry.order_count;
        di.orders_file_size = entry.orders_file_size;
        local_date_info[entry.asset_id] = di;
      }
      for (const auto &[asset_idx, path] : bins) {
        if (local_date_info.count(asset_idx) == 0) {
          index_usable = false;
          local_date_info.clear();
          break;
        }
      }
    }

    // 慢路径: 逐个读 32 字节文件头 —— 全库四百多万次随机 open 就出在这里.
    // 读完把明细写回去, 于是没有明细的老库一轮扫描即自愈.
    //
    // 头损坏的文件当作没有数据 (它本来也解不出来), 但那样明细就配不上名单,
    // 这天以后每次都会走到这条慢路径 —— 正是想要的: 坏文件不该被缓存成"已知".
    if (!index_usable) {
      // 账目部分原样保留 (accounted / complete / 分类账 都只有编码器填得起),
      // 只换掉明细 —— 否则一次扫描就会把编码器的齐备标记抹掉, 增量从此每轮
      // 都要把全库重新列举一遍.
      std::vector<EncodeDayIndexEntry> rebuilt;
      rebuilt.reserve(bins.size());

      for (const auto &[asset_idx, path] : bins) {
        // 一次读头同时拿到条数和体积 (文件总长 = 32 + compressed_size), 不再
        // 额外 stat.
        size_t order_count = 0, file_size = 0;
        if (!L2::BinaryDecoder_L2::read_file_stats(path, order_count, file_size))
          continue;

        DateInfo di;
        di.orders_encoded = 1;
        di.order_count = order_count;
        di.orders_file_size = file_size;
        local_date_info[asset_idx] = di;

        rebuilt.push_back(make_day_index_entry(asset_idx, order_count, file_size));
      }

      // 墓碑是编码器的结论, 扫描无从重建 (要归档才知道"源数据只有表头"),
      // 原样搬过去 —— 丢了它们, 下一轮增量会把那些资产白解一遍.
      for (const auto &entry : local_record.assets)
        if (entry.is_tombstone())
          rebuilt.push_back(entry);

      // 走到这里而且账目是编码器填过的 ⇒ 它声明齐备之后产物被动过 (最常见是
      // 手工删掉了损坏的 .bin). 齐备标记必须跟着作废: 只把明细改成与盘上一致
      // 而留着 complete=1, 等于亲手把缺口抹平 —— 增量的整天快路径从此永远
      // 跳过这天, 删掉的再也补不回来.
      //
      // 账目的其余计数留着: 那些仍是上一轮的事实, 界面照旧能按日拆解原因.
      if (local_record.accounted && local_record.complete) {
        local_record.complete = false;
        Logger::log("encoding", "[STALE STAT] " + day_path.path +
                                    " — .bin 与明细不符, 齐备标记作废, 待增量重编");
      }

      local_record.assets = std::move(rebuilt);
      write_encode_day_stat(day_path.path, local_record);
    }

    // 明细不进 day_records: 全库 885 天 × 5200 条是五十多兆, 而且与刚灌好的
    // date_info 是同一份数据 (见 EncodeDayRecord::assets).
    local_record.assets.clear();
    local_record.assets.shrink_to_fit();

    // Merge into shared result
    {
      std::lock_guard<std::mutex> lock(result->mutex);
      for (auto &[asset_idx, info] : local_date_info) {
        result->asset_date_info[asset_idx][day_path.date_str] = std::move(info);
      }
      // 只有账目填过的天才进去 —— 扫描自己回填出来的 .stat 只有明细, 界面
      // 不该把它显示成"编过但不齐备".
      if (local_record.accounted)
        result->day_records[day_path.date_str] = std::move(local_record);
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

  co_await await_futures(io, futures);

  // 灌入本次重扫的天 (要重扫的那些天的旧条目已在上面清掉了)
  for (const auto &[asset_idx, date_map] : result->asset_date_info) {
    for (const auto &[date, info] : date_map) {
      items[asset_idx].date_info[date] = info;
    }
  }
  for (const auto &[date, record] : result->day_records)
    day_records[date] = record;

  all_dates.assign(current_dates.begin(), current_dates.end());

  // 聚合量从完整的 date_info 重算 —— 增量扫描下 result 里只有本次重扫的天,
  // 沿用下来的那些天必须一起算进来. 五百万条 date_info 一趟走完 (条数/体积/
  // 日期集合/已编码资产数), 按资产分批让步.
  size_t total_orders = 0;
  size_t encoded_assets = 0;
  float total_orders_size = 0.0;
  std::set<std::string> dates;
  for (size_t i = 0; i < items.size(); ++i) {
    for (const auto &[date, info] : items[i].date_info) {
      total_orders += info.order_count;
      total_orders_size += static_cast<float>(info.orders_file_size);
      dates.insert(date);
    }
    if (!items[i].date_info.empty())
      ++encoded_assets;

    if ((i + 1) % kAggregateChunk == 0)
      co_await Coro::Yield(io);
  }

  binary.total_orders = total_orders;
  binary.encoded_assets = encoded_assets;
  binary.orders_size_gb = total_orders_size / (1024.0 * 1024.0 * 1024.0);
  binary.dates = std::move(dates);

  // 取自 binary.dates 而不是 all_dates: 后者含空日目录 (readdir 到了但里面
  // 没有一个能对上 asset_map 的文件), 会把区间往外撑.
  if (!binary.dates.empty()) {
    binary.min_date = *binary.dates.begin();
    binary.max_date = *binary.dates.rbegin();
  } else {
    binary.min_date.clear();
    binary.max_date.clear();
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

  co_await await_futures(io, futures);

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

boost::asio::awaitable<void> Asset::coro_compute_backtest_coverage(
    boost::asio::io_context &io,
    const std::string &start, const std::string &end,
    const AssetInfo &assetinfo) {
  // 进度计数换阶段: 底下几个 Step 都是内存里的集合运算, 快到不值得报进度,
  // 归零后界面显示不带 (x/y) 的裸标签. 真正要等的是紧随其后的
  // coro_compute_coverage_statistics, 它自己会把总量立起来.
  scan_days_done.store(0, std::memory_order_relaxed);
  scan_days_total.store(0, std::memory_order_relaxed);

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
  size_t backtest_order_days = 0;
  for (const auto &date : binary.dates) {
    if (date >= start && date <= end)
      ++backtest_order_days;
  }

  // 区间内的条数和体积仍得逐条累加 (per-date 明细只有 date_info 有), 五百万
  // 条走一趟, 按资产分批让步.
  size_t backtest_orders = 0;
  float backtest_orders_size = 0.0;
  for (size_t i = 0; i < items.size(); ++i) {
    for (const auto &[date, info] : items[i].date_info) {
      if (date >= start && date <= end && info.orders_encoded) {
        backtest_orders += info.order_count;
        backtest_orders_size += static_cast<float>(info.orders_file_size);
      }
    }
    if ((i + 1) % kAggregateChunk == 0)
      co_await Coro::Yield(io);
  }

  binary.backtest_orders = backtest_orders;
  binary.backtest_orders_size_gb = backtest_orders_size / (1024.0 * 1024.0 * 1024.0);
  binary.backtest_order_days = backtest_order_days;

  co_return;
}

// ============================================================================
// Coverage Statistics (Browser / Table / Encode 共用的一份统计)
// ============================================================================

boost::asio::awaitable<void> Asset::coro_compute_coverage_statistics(
    boost::asio::io_context &io,
    const AssetInfo &assetinfo) {
  const auto &stock_info = assetinfo.get_stock_info();
  const auto &stock_days = assetinfo.get_stock_days();
  const auto &suspended = assetinfo.get_suspended();

  auto date_to_dense = [](const std::string &date_dashed) -> std::string {
    if (date_dashed.size() == 10 && date_dashed[4] == '-' && date_dashed[7] == '-') {
      return date_dashed.substr(0, 4) + date_dashed.substr(5, 2) + date_dashed.substr(8, 2);
    }
    return "";
  };

  // 全程算在局部, 最后一次性换进成员 —— 中途 UI 读到的是上一版完整结果
  std::unordered_map<std::string, DateStats> local_date_stats;
  std::map<std::string, DateGap> local_date_gaps;
  std::vector<AssetStats> local_asset_stats(items.size());

  // 全库口径的两列 (Table 的 Days / Orders) 只跟 date_info 有关, 单独扫一遍
  for (size_t i = 0; i < items.size(); ++i) {
    AssetStats &st = local_asset_stats[i];
    st.total_days = items[i].date_info.size();
    for (const auto &[date, info] : items[i].date_info)
      st.total_orders += info.order_count;

    if ((i + 1) % kAggregateChunk == 0)
      co_await Coro::Yield(io);
  }

  // 每资产的常量先摊平: 交易所小写全码 / 上市 / 退市. 这些原先是在
  // 日期×资产的内循环里现算的, 五百万次 string 拼接 + map 查找.
  struct AssetKey {
    std::string full_code; // "sh.600128"
    std::string list_date; // YYYYMMDD, 空 = 不限
    std::string delist_date;
    bool excluded = false; // 北交所: L2 archive 从不覆盖
  };
  std::vector<AssetKey> keys(items.size());
  for (size_t i = 0; i < items.size(); ++i) {
    AssetKey &k = keys[i];
    if (items[i].exchange == "BJ") {
      k.excluded = true;
      continue;
    }
    std::string exchange_lower = items[i].exchange;
    std::transform(exchange_lower.begin(), exchange_lower.end(), exchange_lower.begin(), ::tolower);
    k.full_code = exchange_lower + "." + items[i].asset_code;

    auto info_it = stock_info.find(k.full_code);
    if (info_it != stock_info.end()) {
      if (!info_it->second.ipoDate.empty())
        k.list_date = date_to_dense(info_it->second.ipoDate);
      if (!info_it->second.outDate.empty())
        k.delist_date = date_to_dense(info_it->second.outDate);
    }
  }

  const bool has_db_range = !binary.min_date.empty() && !binary.max_date.empty();
  const bool has_bt_range = !backtest.start.empty() && !backtest.end.empty();

  // 这个双重循环 (交易日 × 全部资产) 是整个 coverage 阶段唯一要等的部分,
  // 进度就报它. 上面那几趟 items 遍历相比之下可以忽略.
  scan_days_done.store(0, std::memory_order_relaxed);
  scan_days_total.store(stock_days.size(), std::memory_order_relaxed);

  size_t days_done = 0;
  for (const auto &day_info : stock_days) {
    if (++days_done % kCoverageChunk == 0)
      co_await Coro::Yield(io);
    scan_days_done.store(days_done, std::memory_order_relaxed);

    if (day_info.size() < 2)
      continue;

    const std::string date_dense = date_to_dense(day_info[0]);
    if (date_dense.empty())
      continue;

    // per-date 统计沿用全库范围; per-asset 缺口只看回测区间 —— 区间外没编
    // 码不算缺, 那不是要跑的行情.
    const bool in_db_range =
        !has_db_range || (date_dense >= binary.min_date && date_dense <= binary.max_date);
    const bool in_backtest = has_bt_range && day_info[1] == "1" &&
                             date_dense >= backtest.start && date_dense <= backtest.end;
    if (!in_db_range && !in_backtest)
      continue;

    // 当日停牌名单 (无条目 = 该日无人停牌)
    auto susp_it = suspended.find(date_dense);
    const auto *susp_today = (susp_it != suspended.end()) ? &susp_it->second : nullptr;

    const bool archive_has_day = archive.dates.count(date_dense) > 0;
    DateStats *ds = in_db_range ? &local_date_stats[date_dense] : nullptr;
    // 一天一个条目, 哪怕零缺口 —— By Date 表要能说"这天检查过, 没事"
    DateGap *dg = in_backtest ? &local_date_gaps[date_dense] : nullptr;

    for (size_t i = 0; i < items.size(); ++i) {
      const AssetKey &k = keys[i];
      if (k.excluded)
        continue;
      if (susp_today && susp_today->count(k.full_code))
        continue;
      if (!k.list_date.empty() && date_dense < k.list_date)
        continue;
      // 退市日当天已经不交易了 (最后交易日是它之前那个交易日), 用 > 的话
      // 每只退市股都会平白多出一天缺口 —— 实测 145 只退市股各缺 1 天, 缺
      // 的正是各自的 delist_date.
      if (!k.delist_date.empty() && date_dense >= k.delist_date)
        continue;

      auto date_it = items[i].date_info.find(date_dense);
      const bool has_orders = date_it != items[i].date_info.end() && date_it->second.orders_encoded;

      if (ds) {
        ds->total_assets++;
        if (has_orders)
          ds->assets_with_orders++;
      }

      if (in_backtest) {
        AssetStats &st = local_asset_stats[i];
        st.expected_days++;
        dg->expected++;
        if (!has_orders) {
          st.orders_missing++;
          dg->orders_missing++;
          if (st.orders_missing_sample.size() < kMissingSample)
            st.orders_missing_sample.push_back(date_dense);
        }
        if (!archive_has_day) {
          st.archive_missing++;
          dg->archive_missing++;
          if (st.archive_missing_sample.size() < kMissingSample)
            st.archive_missing_sample.push_back(date_dense);
        }
      }
    }
  }

  date_stats = std::move(local_date_stats);
  date_gaps = std::move(local_date_gaps);
  asset_stats = std::move(local_asset_stats);
  ++asset_stats_generation;

  co_return;
}
