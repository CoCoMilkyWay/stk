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

// 前向声明
namespace L2 {
class BinaryDecoder_L2;
}
namespace GUI::Database {
class ScanThreadPool;
}
struct AssetInfo;

// ============================================================================
// 单日状态
// ============================================================================

// 一个 (资产, 日期). 只有 orders 一种产物 —— 快照不编码 (全项目无人读取,
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
// 单资产
// ============================================================================

struct AssetItem {
  // 标识 (不可变)
  size_t asset_id = 0;                                        // 在 Asset::items 中的下标, 全库唯一
  std::string asset_code;                                     // "000001" (6 digits)
  std::string asset_name;                                     // "平安银行"
  std::string exchange;                                       // "SH"/"SZ"
  L2::ExchangeType exchange_type = L2::ExchangeType::UNKNOWN; // 由 exchange 派生

  // 上市区间
  std::string start_date; // YYYYMMDD
  std::string end_date;   // YYYYMMDD

  // 按日状态: 按全局日期轴 (Asset::date_axis) 下标密集存储.
  //
  // 密集向量 = 资产数 × 日期数 × 24B ≈ 400MB, 定址 O(1). 轴 append-only,
  // 下标进程内稳定; "无数据"就是零值元素 (orders_encoded == 0), 增量重扫
  // 清空脏日用赋零而不是 erase. 向量按需增长 (date_at 对越界返回零值).
  std::vector<DateInfo> date_info;

  static constexpr DateInfo kEmptyDateInfo{};

  // 读: 越界 (含 Asset::kNoDate) = 该日无数据
  const DateInfo &date_at(size_t date_idx) const {
    return date_idx < date_info.size() ? date_info[date_idx] : kEmptyDateInfo;
  }

  // 写: 自动扩容到轴下标
  DateInfo &date_slot(size_t date_idx) {
    if (date_idx >= date_info.size())
      date_info.resize(date_idx + 1);
    return date_info[date_idx];
  }

  // 构造
  AssetItem() = default;
  AssetItem(size_t id, std::string code, std::string name, std::string exch, std::string start, std::string end);

  // 这里不提供"缺失天数"一类的统计: date_info 只对有文件的日子有非零值,
  // 拿它当全集去做减法恒得 0. 缺口的唯一真相是交易日历 —— 按天看
  // Asset::backtest.missing_dates, 按全市场完整性看 Asset::date_stats.
};

// ============================================================================
// 全资产 + 库元数据
// ============================================================================

struct Asset {
  // ========================================
  // 核心数据
  // ========================================
  std::vector<AssetItem> items;
  std::vector<std::string> all_dates; // 扫描得到的所有已知交易日

  // ========================================
  // 日期轴 (append-only): AssetItem::date_info 的下标空间
  // ========================================
  // 与 all_dates 分开: all_dates 每次扫描按字典序重建 (下标不稳定), 而
  // date_info 的下标必须跨增量重扫稳定 —— 轴只追加不排序不删除, 脏日在
  // 各资产里赋零. 只活在内存里: 进程重启后整库重扫, 轴随之重建.
  std::vector<std::string> date_axis;                      // idx -> "YYYYMMDD"
  std::unordered_map<std::string, uint32_t> date_axis_idx; // date -> idx

  static constexpr size_t kNoDate = SIZE_MAX;

  // 查, 不存在返回 kNoDate (date_at(kNoDate) 自然给零值)
  size_t date_idx(const std::string &date) const {
    auto it = date_axis_idx.find(date);
    return it != date_axis_idx.end() ? it->second : kNoDate;
  }

  // 查/增 (仅扫描合并路径调用, 单线程)
  size_t date_idx_add(const std::string &date) {
    auto [it, inserted] = date_axis_idx.try_emplace(date, static_cast<uint32_t>(date_axis.size()));
    if (inserted)
      date_axis.push_back(date);
    return it->second;
  }

  // ========================================
  // 按日统计 (Browser 用, 加载 stock_info 后算一次)
  // ========================================
  struct DateStats {
    size_t total_assets = 0;       // 当日已上市未退市的标的数
    size_t assets_with_orders = 0; // 其中有逐笔数据的标的数
  };
  std::unordered_map<std::string, DateStats> date_stats; // date -> 统计 (算一次后缓存)

  // ========================================
  // 按日缺口 (Encode 页的 By Date 分析表)
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

  // 每天的编码账目, 由扫描从 orders/YYYY/MM/DD/.stat 读入 (见shared/EncodeDayRecord.hpp).
  // 缺口的"原因"只有编码器知道, 而 date_gaps只知道"缺了几个" —— 两者在 By Date 表里按日期对齐.
  std::unordered_map<std::string, EncodeDayRecord> day_records;

  // ========================================
  // 按资产统计 (Encode 缺失表 / Table 的 Days·Orders·Orders%)
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

  // 扫描进度 (GUI 轮询), 各阶段轮流用. 单位随阶段变: binary 扫描是天
  // (一天一个线程池任务), archive 扫描是月, coverage 是交易日.
  // 阶段开头自己置总量; 置 0 表示"这一段不值得报进度", 界面显示裸标签.
  std::atomic<size_t> scan_days_done{0};
  std::atomic<size_t> scan_days_total{0};

  // ========================================
  // 二进制库元数据
  // ========================================
  struct {
    bool scanned = false; // 是否已完成扫描
    bool exists = false;  // 二进制库目录是否存在
    std::string path;     // 二进制库根目录

    // 日期覆盖
    std::string min_date;        // YYYYMMDD, 已编码日期的最早
    std::string max_date;        // YYYYMMDD, 已编码日期的最晚
    std::set<std::string> dates; // 所有已完整编码的日期

    // 增量扫描: 上次扫完时每个日目录的 mtime.
    //
    // 一天的开销是"一次 readdir + 一次读 .stat" (当天统计, 见
    // shared/EncodeDayRecord.hpp); 只有明细缺失或与名单对不上的天才退回逐个
    // 读头 —— 那条路全库是 451 万次 open+pread+close 约 3 秒.
    //
    // 这份 mtime 基线是更上一层: 目录内容没动过的天连 readdir 都省掉.
    // 只活在内存里, 进程重启后的首次扫描仍要过一遍全部日目录 —— 挡住那趟
    // 全量读头的是盘上的明细, 不是这里.
    //
    // 新增/删除/重命名覆盖都会改目录 mtime (覆盖同名也变 —— rename 按 POSIX
    // 要更新目标目录的 mtime, 何况编码的 .tmp 就落在同目录, 那个条目的增删
    // 本身已经改了 mtime), 所以手动删 .bin 也能被发现.
    std::unordered_map<std::string, int64_t> day_mtimes;

    // 编码动过的天 —— 无条件重扫. mtime 那层之外, 这里把"谁改了库"变成
    // 编码路径的显式契约, 不让正确性依赖文件系统 mtime 的细节语义.
    std::set<std::string> dirty_dates;

    // 统计 (由 items 算出)
    size_t encoded_assets = 0; // 至少有一天有编码数据的资产数

    // 全库统计
    size_t total_orders = 0;    // 全库逐笔条数
    float orders_size_gb = 0.0; // 全库 .bin 体积

    // 回测区间统计 (仅回测期内)
    size_t backtest_orders = 0;          // 回测区间内逐笔条数
    float backtest_orders_size_gb = 0.0; // 回测区间内 .bin 体积
    size_t backtest_order_days = 0;      // 回测区间内有逐笔的 (资产, 日期) 对数
  } binary;

  // ========================================
  // 归档库元数据
  // ========================================
  struct {
    bool scanned = false; // 是否已完成扫描
    bool exists = false;  // 归档目录是否存在
    std::string path;     // 归档根目录

    // 日期覆盖
    std::string min_date;        // YYYYMMDD, 归档日期的最早
    std::string max_date;        // YYYYMMDD, 归档日期的最晚
    std::set<std::string> dates; // 所有可用归档日期

    // 统计 (由文件扫描算出)
    size_t total_files = 0;    // 归档文件总数
    float total_size_gb = 0.0; // 归档总体积
  } archive;

  // ========================================
  // 回测覆盖 (按需计算)
  // ========================================
  struct {
    std::string start; // 回测起始日 (来自配置)
    std::string end;   // 回测结束日 (来自配置)

    // 基准: 回测区间内应有的交易日 (归档存在时取归档, 否则取二进制库)
    std::set<std::string> required_dates;

    // 二进制库覆盖情况
    std::set<std::string> covered_dates; // 已编码
    std::set<std::string> missing_dates; // 缺失

    // 缺失日对应的归档可用性
    std::set<std::string> can_encode;    // 归档有, 可直接编码
    std::set<std::string> need_download; // 归档也没有, 需下载
  } backtest;

  // ========================================
  // 方法
  // ========================================
  // 扫描 (协程异步): 一趟扫完就把三个页面要的数据全部备齐 (覆盖判定 /
  // 每资产条数体积 / 完整性), 之后 Encode、Table、Browser 直接读缓存,
  // 不各自重算.
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

  // 覆盖分析 (扫描完成或配置变更后调用), 同时用缓存数据算回测区间统计.
  // required_dates 的基准 = 基本面交易日历 (assetinfo.stock_days)
  //
  // 协程而不是普通函数: 末段要遍历全库 date_info (资产数 × 交易日, 五百万条),
  // 单线程不间断跑会卡住一帧 —— 按资产分批, 每批让步 (见 Coro::Yield).
  boost::asio::awaitable<void> coro_compute_backtest_coverage(
      boost::asio::io_context &io,
      const std::string &start, const std::string &end,
      const AssetInfo &assetinfo);

  // 用 AssetInfo 同步 AssetItem 字段 (AssetInfo 更新后调用)
  template <typename StockInfoMap>
  void sync_from_asset_info(const StockInfoMap &stock_info) {
    for (auto &asset : items) {
      // 拼 stock key: "exchange.code" (exchange 小写)
      std::string exchange_lower = asset.exchange;
      std::transform(exchange_lower.begin(), exchange_lower.end(), exchange_lower.begin(), ::tolower);
      std::string stock_key = exchange_lower + "." + asset.asset_code;

      // 查 stock info
      auto info_it = stock_info.find(stock_key);
      if (info_it != stock_info.end()) {
        const auto &info = info_it->second;

        // 更新 name
        asset.asset_name = info.name;

        // 更新 start_date (ipoDate: YYYY-MM-DD -> YYYYMMDD)
        if (!info.ipoDate.empty()) {
          std::string ipo_date = info.ipoDate;
          ipo_date.erase(std::remove(ipo_date.begin(), ipo_date.end(), '-'), ipo_date.end());
          asset.start_date = ipo_date;
        }

        // 更新 end_date (outDate: YYYY-MM-DD -> YYYYMMDD)
        if (!info.outDate.empty()) {
          std::string out_date = info.outDate;
          out_date.erase(std::remove(out_date.begin(), out_date.end(), '-'), out_date.end());
          asset.end_date = out_date;
        } else {
          asset.end_date = "20991231"; // 未退市
        }
      }
    }
  }

  // 按日统计 (Browser 完整性 / Table 的 Orders% / Encode 的缺失表,
  // 同一份统计一次算完)
  //
  // 分母 = 当日"本该有逐笔"的标的: 已上市未退市, 且排除
  //   - 北交所 (L2 archive 从不覆盖 .BJ)
  //   - 当日全天停牌 (suspended, 无逐笔可编码)
  // 这两项不剔掉的话全市场完整性会被压到 ~94%, 掩盖真实缺口.
  //
  // 协程而不是普通函数: 交易日 × 资产的双重遍历 (885 × 5800 量级), 按交易日
  // 分批让步 (见 Coro::Yield). 结果先算在局部, 算完一次性换进来 —— 中途打开
  // 页面看到的是"上一版或没有", 不会是半成品.
  boost::asio::awaitable<void> coro_compute_coverage_statistics(
      boost::asio::io_context &io,
      const AssetInfo &assetinfo);
};

// ============================================================================
// 工具函数
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
