#pragma once

#include "boost/asio/awaitable.hpp"
#include "codec/L2_DataType.hpp"
#include "misc/date.hpp"
#include "shared/EncodeDayRecord.hpp"

#include <atomic>
#include <map>
#include <memory>
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
  misc::Ymd start_date = 0;
  misc::Ymd end_date = 0;

  // 按日状态: 按全局日期轴 (Asset::date_axis) 下标密集存储.
  //
  // 密集向量 = 资产数 × 日期数 × 24B ≈ 400MB, 定址 O(1). 每次扫描整体重建
  // (轴与全部 date_info 一起), "无数据"就是零值元素 (orders_encoded == 0).
  // 扫描之后新上市的资产 (AssetLoader 追加) 向量为空, date_at 对越界返回零值.
  std::vector<DateInfo> date_info;

  static constexpr DateInfo kEmptyDateInfo{};

  // 读: 越界 (含 Asset::kNoDate) = 该日无数据
  const DateInfo &date_at(size_t date_idx) const {
    return date_idx < date_info.size() ? date_info[date_idx] : kEmptyDateInfo;
  }

  // 构造
  AssetItem() = default;
  AssetItem(size_t id, std::string code, std::string name, std::string exch, misc::Ymd start, misc::Ymd end);

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
  std::vector<misc::Ymd> all_dates; // 扫描得到的所有已知交易日

  // ========================================
  // 日期轴: AssetItem::date_info 的下标空间
  // ========================================
  // 有 .bin 目录的天, 升序. 每次二进制扫描与全部 date_info 一起整体重建,
  // 中间不做增量修补 —— 一次重建是 800 次 stat + 读 .stat, 百毫秒量级,
  // 不值得为它维护脏日/基线那套状态.
  std::vector<misc::Ymd> date_axis; // idx -> date, 升序

  static constexpr size_t kNoDate = SIZE_MAX;

  // 查, 不存在返回 kNoDate (date_at(kNoDate) 自然给零值)
  size_t date_idx(misc::Ymd date) const {
    auto it = std::lower_bound(date_axis.begin(), date_axis.end(), date);
    return it != date_axis.end() && *it == date ? static_cast<size_t>(it - date_axis.begin()) : kNoDate;
  }

  // ========================================
  // 按日统计 (Browser 用, 加载 stock_info 后算一次)
  // ========================================
  struct DateStats {
    size_t total_assets = 0;       // 当日已上市未退市的标的数
    size_t assets_with_orders = 0; // 其中有逐笔数据的标的数
  };
  std::unordered_map<misc::Ymd, DateStats> date_stats; // date -> 统计 (算一次后缓存)

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
  std::map<misc::Ymd, DateGap> date_gaps; // date -> 缺口; 只含回测区间内的天

  // 每天的编码账目, 由扫描从 orders/YYYY/MM/DD/.stat 读入 (见shared/EncodeDayRecord.hpp).
  // 缺口的"原因"只有编码器知道, 而 date_gaps只知道"缺了几个" —— 两者在 By Date 表里按日期对齐.
  std::unordered_map<misc::Ymd, EncodeDayRecord> day_records;

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
    std::vector<misc::Ymd> orders_missing_sample;
    std::vector<misc::Ymd> archive_missing_sample;

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
    misc::Ymd min_date = 0;    // 已编码日期的最早
    misc::Ymd max_date = 0;    // 已编码日期的最晚
    std::set<misc::Ymd> dates; // 所有已完整编码的日期

    // 统计 (由 coro_compute_coverage_statistics 的逐资产遍历算出)
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
    misc::Ymd min_date = 0;    // 归档日期的最早
    misc::Ymd max_date = 0;    // 归档日期的最晚
    std::set<misc::Ymd> dates; // 所有可用归档日期

    // 统计 (由文件扫描算出)
    size_t total_files = 0;    // 归档文件总数
    float total_size_gb = 0.0; // 归档总体积
  } archive;

  // ========================================
  // 回测覆盖 (按需计算)
  // ========================================
  struct {
    misc::Ymd start = 0; // 回测起始日 (来自配置)
    misc::Ymd end = 0;   // 回测结束日 (来自配置)

    // 基准: 回测区间内应有的交易日 (归档存在时取归档, 否则取二进制库)
    std::set<misc::Ymd> required_dates;

    // 二进制库覆盖情况
    std::set<misc::Ymd> covered_dates; // 已编码
    std::set<misc::Ymd> missing_dates; // 缺失

    // 缺失日对应的归档可用性
    std::set<misc::Ymd> can_encode;    // 归档有, 可直接编码
    std::set<misc::Ymd> need_download; // 归档也没有, 需下载
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
      std::shared_ptr<GUI::Database::ScanThreadPool> thread_pool);

  boost::asio::awaitable<void> coro_scan_archive_database(
      boost::asio::io_context &io,
      const std::string &archive_dir,
      const std::string &archive_extension,
      std::shared_ptr<GUI::Database::ScanThreadPool> thread_pool);

  // 覆盖分析 (扫描完成或配置变更后调用): 回测区间内哪些交易日有/缺/可编/
  // 需下载. required_dates 的基准 = 基本面交易日历 (assetinfo.stock_days).
  // 纯集合运算, 几百个日期, 同步跑完. 区间内的条数/体积 (要遍历 date_info)
  // 归 coro_compute_coverage_statistics 那趟一起算.
  void compute_backtest_coverage(misc::Ymd start, misc::Ymd end,
                                 const AssetInfo &assetinfo);

  // 按日统计 (Browser 完整性 / Table 的 Orders% / Encode 的缺失表,
  // 同一份统计一次算完)
  //
  // 分母 = 当日"本该有逐笔"的标的: 已上市未退市, 且排除
  //   - 北交所 (L2 archive 从不覆盖 .BJ)
  //   - 当日全天停牌 (suspended, 无逐笔可编码)
  // 这两项不剔掉的话全市场完整性会被压到 ~94%, 掩盖真实缺口.
  //
  // 全库 date_info 只在这里遍历一趟 (按资产段并行): 每资产 total_days /
  // total_orders, 全库与回测区间的条数/体积, encoded_assets.
  //
  // 交易日 × 资产的双重遍历 (885 × 5800 量级) 切成连续的交易日段扔进线程池,
  // 每段各自累加, 结束后按段的先后顺序合并 (缺失日期样本要保持时间序);
  // GUI 线程只轮询等待. 结果先算在局部, 算完一次性换进来 —— 中途打开页面
  // 看到的是"上一版或没有", 不会是半成品.
  boost::asio::awaitable<void> coro_compute_coverage_statistics(
      boost::asio::io_context &io,
      const AssetInfo &assetinfo,
      std::shared_ptr<GUI::Database::ScanThreadPool> thread_pool);
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

// archive/YYYY/YYYYMM/YYYYMMDD<ext>
inline std::string generate_archive_path(const std::string &base_dir, misc::Ymd date, const std::string &extension) {
  const std::string d = misc::dense(date);
  return base_dir + "/" + d.substr(0, 4) + "/" + d.substr(0, 6) + "/" + d + extension;
}

// orders/YYYY/MM/DD
inline std::string generate_date_dir(const std::string &orders_dir, misc::Ymd date) {
  return orders_dir + "/" + misc::slash(date);
}

// orders/YYYY/MM/DD/<CODE>.<EX>.bin
inline std::string generate_orders_path(const std::string &orders_dir, misc::Ymd date,
                                        const std::string &asset_code, const std::string &exchange,
                                        const std::string &binary_extension) {
  return generate_date_dir(orders_dir, date) + "/" + asset_code + "." + exchange + binary_extension;
}

} // namespace Utils
