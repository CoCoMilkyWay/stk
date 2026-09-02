#pragma once

#include "boost/asio/awaitable.hpp"
#include "codec/L2_DataType.hpp"
#include "shared/EncodeDayRecord.hpp"

#include <algorithm>
#include <atomic>
#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

// Forward declarations
namespace L2 {
class BinaryDecoder_L2;
}
namespace GUI::Database {
class ScanThreadPool;
}
struct AssetInfo;

// ============================================================================
// PER-DATE STATUS
// ============================================================================

// 一个 (资产, 日期). 只有 orders 一种产物 —— 快照不再编码 (全项目无人读取,
// 特征计算只吃 orders 并靠 LimitOrderBook 重建盘口).
// 路径不存字段 —— 由 (date, code, exchange) 经 Utils::generate_orders_path
// 现算. 全库五百万条 DateInfo, 每条存一份 35 字节路径就是几百 MB 堆分配.
struct DateInfo {
  // 两者都来自同一次 32 字节读头 (见 BinaryDecoder_L2::read_file_stats)
  size_t order_count = 0;
  size_t orders_file_size = 0;

  uint8_t orders_encoded = 0; // 0=无二进制, 1=已编码

  bool has_binaries() const {
    return orders_encoded != 0;
  }
};

// ============================================================================
// SINGLE ASSET
// ============================================================================

struct AssetItem {
  // IDENTITY (immutable)
  size_t asset_id = 0;
  std::string asset_code; // "000001" (6 digits)
  std::string asset_name; // "平安银行"
  std::string exchange;   // "SH"/"SZ"
  L2::ExchangeType exchange_type = L2::ExchangeType::UNKNOWN;

  // DATE RANGE
  std::string start_date; // YYYYMMDD
  std::string end_date;   // YYYYMMDD

  // PER-DATE STATUS (sparse storage - only dates with actual data)
  std::unordered_map<std::string, DateInfo> date_info;

  // WORKER ASSIGNMENT
  int assigned_worker_id = -1;

  // CONSTRUCTORS
  AssetItem() = default;
  AssetItem(size_t id, std::string code, std::string name, std::string exch, std::string start, std::string end);

  // 这里不再提供"缺失天数"一类的统计: date_info 是"有文件才插入"的稀疏表,
  // 拿它当全集去做减法恒得 0. 缺口的唯一真相是交易日历 —— 按天看
  // Asset::backtest.missing_dates, 按全市场完整性看 Asset::date_stats.
};

// ============================================================================
// ALL ASSETS WITH DATABASE METADATA
// ============================================================================

struct Asset {
  // ========================================
  // Core Data
  // ========================================
  std::vector<AssetItem> items;
  std::vector<std::string> all_dates; // All known trading days (from scan)

  // ========================================
  // Per-Date Statistics (for Browser, computed once after loading stock_info)
  // ========================================
  struct DateStats {
    size_t total_assets = 0;       // Total assets listed on this date (considering delist)
    size_t assets_with_orders = 0; // Assets with order data
  };
  std::unordered_map<std::string, DateStats> date_stats; // date -> stats (computed once)

  // ========================================
  // Per-Date Gaps (Encode 页的 By Date 分析表)
  // ========================================
  // 与 asset_stats 同一次遍历产出, 只是把同一批缺口按日期而不是按资产归堆:
  // 一天缺一大片通常是那天的源出了事 (归档没下到 / 逐笔流缺片), 按资产看
  // 反而会摊成几百行各缺一天, 看不出来.
  struct DateGap {
    size_t expected = 0;        // 当天本该有逐笔的标的数
    size_t orders_missing = 0;  // 其中没有 .bin 的
    size_t archive_missing = 0; // 其中归档也没有的 (归档按天存, 要么全缺要么不缺)
  };
  std::map<std::string, DateGap> date_gaps; // date -> 缺口; 只含回测区间内的天

  // 每天的编码账目, 由扫描从 orders/YYYY/MM/DD/.day_complete 读入 (见
  // shared/EncodeDayRecord.hpp). 缺口的"原因"只有编码器知道, 而 date_gaps
  // 只知道"缺了几个" —— 两者在 By Date 表里按日期对齐.
  std::unordered_map<std::string, EncodeDayRecord> day_records;

  // ========================================
  // Per-Asset Statistics (Encode 缺失表 / Table 的 Days·Orders·Orders%)
  // ========================================
  // 全库编完之后 date_info 是满的 (资产数 × 交易日数, 五百万量级), 在 GUI
  // 里逐帧重算这些计数会把帧时间拖到几百毫秒. 与 date_stats 同样的惰性缓存:
  // 扫描时清空, 首次渲染时算一次.
  //
  // 缺口口径与 date_stats 完全一致 (同一次遍历产出): 分母是"本该有逐笔"的
  // 交易日 —— 已上市未退市, 排除北交所与当日停牌. 不能拿 date_info 当分母,
  // 它是"有文件才插入"的稀疏表, 减出来的缺失恒为 0.
  static constexpr size_t kMissingSample = 10; // 每资产留几个缺失日期给 UI

  struct AssetStats {
    // 全库口径 (date_info)
    size_t total_days = 0;
    size_t total_orders = 0;

    // 回测区间口径
    size_t expected_days = 0;   // 本该有数据的交易日
    size_t orders_missing = 0;  // 其中没有 .bin 的
    size_t archive_missing = 0; // 其中归档源也没有的

    // 前 kMissingSample 个缺失日期; 全存的话是几百万条字符串
    std::vector<std::string> orders_missing_sample;
    std::vector<std::string> archive_missing_sample;

    float orders_coverage_percent() const {
      return expected_days > 0
                 ? 100.0f * static_cast<float>(expected_days - orders_missing) / static_cast<float>(expected_days)
                 : 0.0f;
    }
  };
  std::vector<AssetStats> asset_stats; // 按 asset_id 索引; 空 = 待重算
  uint64_t asset_stats_generation = 0; // 每次重算 +1, 驱动 GUI 表格视图失效

  // 扫描进度 (GUI 轮询). 粒度是"天" —— 一天一个线程池任务, 4500 个文件.
  std::atomic<size_t> scan_days_done{0};
  std::atomic<size_t> scan_days_total{0};

  // ========================================
  // Binary Database Metadata
  // ========================================
  struct {
    bool scanned = false;
    bool exists = false;
    std::string path;

    // Date coverage
    std::string min_date;        // YYYYMMDD
    std::string max_date;        // YYYYMMDD
    std::set<std::string> dates; // All fully encoded dates

    // 增量扫描: 上次扫完时每个日目录的 mtime.
    //
    // 全量一趟是 451 万次 open+pread+close, 约 3 秒 (readdir 本身只要 0.03
    // 秒, 成本全在读头), 而启动 / Overview 刷新 / 编码完成都会触发扫描.
    // 目录内容没动过的天直接沿用上次结果.
    //
    // 新增/删除/重命名覆盖都会改目录 mtime (实测三种都变, 覆盖同名也变 ——
    // rename 按 POSIX 要更新目标目录的 mtime, 何况编码的 .tmp 就落在同目录,
    // 那个条目的增删本身已经改了 mtime), 所以手动删 .bin 也能被发现.
    std::unordered_map<std::string, int64_t> day_mtimes;

    // 编码动过的天 —— 无条件重扫. mtime 那层已经够用, 这里是把"谁改了库"
    // 变成编码路径的显式契约, 不让正确性依赖文件系统 mtime 的细节语义.
    std::set<std::string> dirty_dates;

    // Statistics (computed from items)
    size_t encoded_assets = 0; // Assets with any encoded data

    // Whole database statistics
    size_t total_orders = 0;
    float orders_size_gb = 0.0;

    // Backtest range statistics (only within backtest period)
    size_t backtest_orders = 0;
    float backtest_orders_size_gb = 0.0;
    size_t backtest_order_days = 0;
  } binary;

  // ========================================
  // Archive Database Metadata
  // ========================================
  struct {
    bool scanned = false;
    bool exists = false;
    std::string path;

    // Date coverage
    std::string min_date;        // YYYYMMDD
    std::string max_date;        // YYYYMMDD
    std::set<std::string> dates; // All available archive dates

    // Statistics (computed from file scan)
    size_t total_files = 0;
    float total_size_gb = 0.0;
  } archive;

  // ========================================
  // Backtest Coverage (computed on demand)
  // ========================================
  struct {
    std::string start; // From config
    std::string end;   // From config

    // Ground truth (from archive if exists, else from binary)
    std::set<std::string> required_dates;

    // Binary coverage
    std::set<std::string> covered_dates;
    std::set<std::string> missing_dates;

    // Archive availability for missing dates
    std::set<std::string> can_encode;
    std::set<std::string> need_download;
  } backtest;

  // ========================================
  // Methods
  // ========================================
  // Scan operations (asynchronous coroutine-based)
  //
  // 一趟扫完就把三个页面要的数据全部备齐 (覆盖判定 / 每资产条数体积 /
  // 完整性), 之后 Encode、Table、Browser 直接读缓存, 不再各自重算.
  boost::asio::awaitable<void> coro_scan_binary_database(
      boost::asio::io_context &io,
      const std::string &orders_dir,
      const std::string &binary_extension,
      std::shared_ptr<GUI::Database::ScanThreadPool> thread_pool);

  boost::asio::awaitable<void> coro_scan_archive_database(
      boost::asio::io_context &io,
      const std::string &archive_dir,
      const std::string &archive_extension,
      std::shared_ptr<GUI::Database::ScanThreadPool> thread_pool);

  // Coverage analysis (lightweight, call after config changes)
  // Also computes backtest range statistics using cached data
  // required_dates 的 ground truth = 基本面交易日历 (assetinfo.stock_days)
  void compute_backtest_coverage(const std::string &start, const std::string &end,
                                 const AssetInfo &assetinfo);

  // Sync AssetItem fields from AssetInfo (call after AssetInfo updates)
  template <typename StockInfoMap>
  void sync_from_asset_info(const StockInfoMap &stock_info) {
    for (auto &asset : items) {
      // Build stock key: "exchange.code" (lowercase exchange)
      std::string exchange_lower = asset.exchange;
      std::transform(exchange_lower.begin(), exchange_lower.end(), exchange_lower.begin(), ::tolower);
      std::string stock_key = exchange_lower + "." + asset.asset_code;

      // Find stock info
      auto info_it = stock_info.find(stock_key);
      if (info_it != stock_info.end()) {
        const auto &info = info_it->second;

        // Update name
        asset.asset_name = info.name;

        // Update start_date (ipoDate: YYYY-MM-DD -> YYYYMMDD)
        if (!info.ipoDate.empty()) {
          std::string ipo_date = info.ipoDate;
          ipo_date.erase(std::remove(ipo_date.begin(), ipo_date.end(), '-'), ipo_date.end());
          asset.start_date = ipo_date;
        }

        // Update end_date (outDate: YYYY-MM-DD -> YYYYMMDD)
        if (!info.outDate.empty()) {
          std::string out_date = info.outDate;
          out_date.erase(std::remove(out_date.begin(), out_date.end(), '-'), out_date.end());
          asset.end_date = out_date;
        } else {
          asset.end_date = "20991231"; // Not delisted
        }
      }
    }
  }

  // Compute per-date browser statistics (requires stock_info for delist dates)
  // Should be called once after loading stock_info and stock_days
  //
  // 分母 = 当日"本该有逐笔"的标的: 已上市未退市, 且排除
  //   - 北交所 (L2 archive 从不覆盖 .BJ)
  //   - 当日全天停牌 (suspended, 无逐笔可编码)
  // 这两项不剔掉的话全市场完整性会被压到 ~94%, 掩盖真实缺口.
  template <typename StockInfoMap, typename StockDaysVec, typename SuspendedMap>
  void compute_coverage_statistics(const StockInfoMap &stock_info, const StockDaysVec &stock_days,
                                   const SuspendedMap &suspended) {
    date_stats.clear();
    date_gaps.clear();
    asset_stats.assign(items.size(), AssetStats{});
    ++asset_stats_generation;

    auto date_to_dense = [](const std::string &date_dashed) -> std::string {
      if (date_dashed.size() == 10 && date_dashed[4] == '-' && date_dashed[7] == '-') {
        return date_dashed.substr(0, 4) + date_dashed.substr(5, 2) + date_dashed.substr(8, 2);
      }
      return "";
    };

    // 全库口径的两列 (Table 的 Days / Orders) 只跟 date_info 有关, 单独扫一遍
    for (size_t i = 0; i < items.size(); ++i) {
      AssetStats &st = asset_stats[i];
      st.total_days = items[i].date_info.size();
      for (const auto &[date, info] : items[i].date_info)
        st.total_orders += info.order_count;
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

    for (const auto &day_info : stock_days) {
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
      DateStats *ds = in_db_range ? &date_stats[date_dense] : nullptr;
      // 一天一个条目, 哪怕零缺口 —— By Date 表要能说"这天检查过, 没事"
      DateGap *dg = in_backtest ? &date_gaps[date_dense] : nullptr;

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
          AssetStats &st = asset_stats[i];
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
  }
};

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

namespace Utils {
// orders/2023/01/03/000023.SZ.bin
//
// 一个 (资产, 日期) 就一个文件, 所以没有"每资产目录"这一层. 文件名里也不带
// 条数 —— 条数由文件头的 original_size 精确推出 (见 BinaryDecoder_L2), 写在
// 名字里纯属冗余, 还会让"这天编过了吗"退化成通配符匹配而不是一次 exists.

inline std::string generate_archive_path(const std::string &base_dir, const std::string &date_str, const std::string &extension) {
  return base_dir + "/" + date_str.substr(0, 4) + "/" + date_str.substr(0, 6) + "/" + date_str + extension;
}

// orders/YYYY/MM/DD
inline std::string generate_date_dir(const std::string &orders_dir, const std::string &date_str) {
  return orders_dir + "/" + date_str.substr(0, 4) + "/" + date_str.substr(4, 2) + "/" + date_str.substr(6, 2);
}

// orders/YYYY/MM/DD/<CODE>.<EX>.bin
inline std::string generate_orders_path(const std::string &orders_dir, const std::string &date_str,
                                        const std::string &asset_code, const std::string &exchange,
                                        const std::string &binary_extension) {
  return generate_date_dir(orders_dir, date_str) + "/" + asset_code + "." + exchange + binary_extension;
}

} // namespace Utils
