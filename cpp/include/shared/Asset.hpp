#pragma once

#include "codec/L2_DataType.hpp"
#include "misc/date.hpp"
#include "shared/OrdersDay.hpp"

#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

// 前向声明
namespace misc::exec {
class ForkJoin;
}
struct AssetInfo;
struct AssetScan;
struct Progress;

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

  // 按日的帧不在这里, 在 Asset::orders() (日主序, 见 shared/OrdersDay.hpp).

  // 构造
  AssetItem() = default;
  AssetItem(size_t id, std::string code, std::string name, std::string exch, misc::Ymd start, misc::Ymd end);

  // "000001.SZ": A 轴 / pool::Panel / 归档目录的键
  std::string code_ex() const {
    return asset_code + "." + exchange;
  }
};

// ============================================================================
// 全资产 + 库元数据
// ============================================================================

struct Asset {
  // ========================================
  // 核心数据
  // ========================================
  std::vector<AssetItem> items;

  // ========================================
  // 全库帧目录: [日][资产] 一格一个 FrameRef + 每天的编码账目
  // ========================================
  // 由二进制扫描整体重建 (见 shared/OrdersDay.hpp), 建好即只读, 以快照发布:
  // 读者 orders() 取一份, 持有期间内容不变, 与重扫描 / 重编码并发安全. 账目里有
  // 缺口的"原因" —— 只有编码器知道, date_gaps 只知道"缺了几个", 两者在 By Date
  // 表里按日期对齐.
  std::shared_ptr<const OrdersCatalog> orders() const;
  void publish_orders(std::shared_ptr<const OrdersCatalog> catalog);

  // ========================================
  // 按日统计 (Browser 用, 加载 stock_info 后算一次)
  // ========================================
  struct DateStats {
    size_t total_assets = 0;       // 当日已上市未退市的资产数
    size_t assets_with_orders = 0; // 其中有逐笔数据的资产数
  };
  std::unordered_map<misc::Ymd, DateStats> date_stats; // date -> 统计 (算一次后缓存)

  // ========================================
  // 按日缺口 (Encode 页的 By Date 分析表)
  // ========================================
  // 与 asset_stats 同一次遍历产出, 只是把同一批缺口按日期而不是按资产归堆:
  // 一天缺一大片通常是那天的源出了事 (归档没下到 / 逐笔流缺片), 按资产看
  // 反而会摊成几百行各缺一天, 看不出来.
  struct DateGap {
    size_t expected = 0;        // 当天本该有逐笔的资产数
    size_t orders_missing = 0;  // 其中没有帧的
    size_t archive_missing = 0; // 其中归档也没有的 (归档按天存, 要么全缺要么不缺)
  };
  std::vector<DateGap> date_gaps; // 与 backtest.days 逐行对齐; 空 = 待重算

  // ========================================
  // 按资产统计 (Encode 缺失表 / Table 的 Days·Orders·Orders%)
  // ========================================
  // 全库编完之后帧目录是满的 (资产数 × 交易日数, 五百万量级), 在 GUI
  // 里逐帧重算这些计数会把帧时间拖到几百毫秒. 与 date_stats 同一趟扫描算好, 随产物换进.
  //
  // 缺口口径与 date_stats 完全一致 (同一次遍历产出): 分母是"本该有逐笔"的
  // 交易日 —— 已上市未退市, 排除北交所 / 当日停牌 / 墓碑. 不能拿帧目录当分母,
  // 它只有有日文件的天, 减出来的缺失恒为 0.
  static constexpr size_t kMissingSample = 10; // 每资产留几个缺失日期给 UI

  struct AssetStats {
    // 全库口径 (帧目录)
    size_t total_days = 0;
    size_t total_orders = 0;

    // 回测区间口径
    size_t expected_days = 0;   // 本该有数据的交易日
    size_t orders_missing = 0;  // 其中没有帧的
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
  uint64_t asset_stats_generation = 0; // 每次换进 +1, 驱动 GUI 表格视图失效

  // ========================================
  // 二进制库元数据
  // ========================================
  struct Binary {
    bool scanned = false; // 是否已完成扫描
    bool exists = false;  // 二进制库目录是否存在
    std::string path;     // 二进制库根目录

    // 日期覆盖
    misc::Ymd min_date = 0;    // 已编码日期的最早
    misc::Ymd max_date = 0;    // 已编码日期的最晚
    std::set<misc::Ymd> dates; // 所有已完整编码的日期

    // 统计 (由 compute_coverage_statistics 的逐资产遍历算出)
    size_t encoded_assets = 0; // 至少有一天有编码数据的资产数

    // 全库统计
    size_t total_orders = 0;    // 全库逐笔条数
    float orders_size_gb = 0.0; // 全库帧体积

    // 回测区间统计 (仅回测期内)
    size_t backtest_orders = 0;          // 回测区间内逐笔条数
    float backtest_orders_size_gb = 0.0; // 回测区间内帧体积
    size_t backtest_order_days = 0;      // 回测区间内有逐笔的 (资产, 日期) 对数
  } binary;

  // ========================================
  // 归档库元数据
  // ========================================
  struct Archive {
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
  // 回测日历 (覆盖分析产出): 交易日历 ∩ [start, end], 一天一行
  // ========================================
  // 编码 / 特征计算 / 覆盖判定共用的唯一日期轴. 两源有无按行对齐, 缺失 / 可编 /
  // 需下载都是行上的谓词, 不另存集合.
  struct BacktestDay {
    misc::Ymd date = 0;
    bool binary = false;  // 日文件里至少一帧
    bool archive = false; // 归档有这天

    bool missing() const { return !binary; }
    bool can_encode() const { return !binary && archive; }
    bool need_download() const { return !binary && !archive; }
  };
  struct Backtest {
    misc::Ymd start = 0;           // 回测起始日 (来自配置)
    misc::Ymd end = 0;             // 回测结束日 (来自配置)
    std::vector<BacktestDay> days; // 升序

    template <typename Pred>
    size_t count(Pred pred) const {
      size_t n = 0;
      for (const BacktestDay &day : days)
        n += pred(day) ? 1 : 0;
      return n;
    }
  } backtest;

  // 回测日历的日期列: 编码 producer 与特征计算的日期轴
  std::vector<misc::Ymd> backtest_dates() const;

  // ========================================
  // 扫描 (Scan worker 线程, 同步): 一趟扫完就把三个页面要的数据全部备齐 (覆盖判定 /
  // 每资产条数体积 / 完整性), 之后 Encode、Table、Browser 直接读缓存, 不各自重算.
  // 四步同一形状: 只读本对象 (items / 上一版两源缓存) + 只写 AssetScan, 批内并行走 ex.tasks,
  // 进度 begin(阶段, 总量) / publish(1). 产物由 GUI 帧首 adopt 整体换进 —— 中途页面看到的是上一版完整结果.
  // ========================================
  void scan_binary(AssetScan &out, const std::string &orders_dir, misc::exec::ForkJoin &ex, Progress &p) const;
  static void scan_archive(AssetScan &out, const std::string &archive_dir, const std::string &archive_extension,
                           misc::exec::ForkJoin &ex, Progress &p);

  // 覆盖分析: 建回测日历 —— 行 = 基本面交易日历 (assetinfo.stock_days) ∩ [start, end], 每行标两源有无 (out 的两源缓存).
  // 几百个日期, 单线程. 区间内的条数/体积 (要遍历帧目录) 归 compute_coverage_statistics 那趟一起算.
  static void compute_backtest_coverage(AssetScan &out, misc::Ymd start, misc::Ymd end, const AssetInfo &assetinfo);

  // 按日统计 (Browser 完整性 / Table 的 Orders% / Encode 的缺失表,
  // 同一份统计一次算完)
  //
  // 分母 = 当日"本该有逐笔"的资产: 已上市未退市, 且排除
  //   - 北交所 (L2 archive 从不覆盖 .BJ)
  //   - 当日全天停牌 (suspended, 无逐笔可编码)
  //   - 墓碑 (编码器判定当天源数据为空, 帧目录格子三态之一)
  // 前两项不剔掉的话全市场完整性会被压到 ~94%, 掩盖真实缺口; 剔掉墓碑后缺失
  // 只剩重跑增量能补回来的那部分.
  //
  // 全库帧目录只在这里遍历一趟 (按天段并行): 每资产 total_days /
  // total_orders, 全库与回测区间的条数/体积, encoded_assets.
  //
  // 交易日 × 资产的双重遍历 (885 × 5800 量级) 切成连续的交易日段 (一段一个 task),
  // 每段各自累加, 结束后按段的先后顺序合并 (缺失日期样本要保持时间序).
  void compute_coverage_statistics(AssetScan &out, const AssetInfo &assetinfo, misc::exec::ForkJoin &ex, Progress &p) const;

  // GUI 线程: 一趟扫描的产物整体换进 (帧目录随之发布)
  void adopt(AssetScan &&scan);

private:
  mutable std::mutex orders_mutex_;
  std::shared_ptr<const OrdersCatalog> orders_ = std::make_shared<const OrdersCatalog>();
};

// 一趟扫描的全部产物 = Asset 里扫描负责的那几块 + 帧目录快照 (Scan worker 私有算出, Asset::adopt 换进)
struct AssetScan {
  Asset::Binary binary;
  Asset::Archive archive;
  Asset::Backtest backtest;
  std::shared_ptr<const OrdersCatalog> orders = std::make_shared<const OrdersCatalog>();
  std::unordered_map<misc::Ymd, Asset::DateStats> date_stats;
  std::vector<Asset::DateGap> date_gaps;
  std::vector<Asset::AssetStats> asset_stats;
};
