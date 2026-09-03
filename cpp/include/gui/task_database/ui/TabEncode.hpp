// Tab Encode - L2 Binary Database Encoding Control Panel
// Controls CSV→Binary conversion with worker management
#pragma once

#include <cstdint>
#include <string>
#include <vector>

// Forward declarations
namespace GUI::Database {
class EncodingService;
class ScanService;
struct EncodingProgress;
} // namespace GUI::Database

struct Asset;

namespace GUI::Database {

// ============================================================================
// Encode Tab State
// ============================================================================

// 缺失资产表格的一个视图 = 过滤 + 排序后的 asset_id 列表.
//
// 行统计本身缓存在 Asset::asset_stats (扫描后算一次); 这里再缓存一层顺序,
// 因为几千行的排序 (含字符串比较) 逐帧重做同样是白烧. 只在统计代数 / 排序
// 规则变化时重建.
struct AssetTableView {
  std::vector<size_t> rows; // asset_id, 只含该维度确有缺失的
  uint64_t generation = 0;  // 对应的 Asset::asset_stats_generation
  bool built = false;
};

// 按日期的缺口分析表的一个视图 (Encode 页的 By Date 页签).
//
// 与 AssetTableView 同构, 只是行是日期而不是资产. 列不是固定的: 判据有十几
// 条, 一次扫描下来通常只命中三四条, 全列出来会让表宽得没法看 —— 所以只给
// "这批日期里真出现过"的原因出列 (见 columns), 列集合与行一起缓存.
struct DateTableView {
  std::vector<std::string> rows; // 日期, 已过滤已排序

  // 出列的判据位 (L2::Check 的移位量). 固定的处置分类列不在此列.
  std::vector<size_t> check_columns;

  // 固定分类列出不出, 同样看这批日期里有没有发生过
  bool has_skipped = false;
  bool has_corrupt = false;
  bool has_invalid = false;
  bool has_failed = false;
  bool has_no_archive = false;
  bool has_unaccounted = false;

  // 排序规则存在视图里而不是每帧问表: 列集合要在 BeginTable 之前就定下来
  // (列数是构造参数), 而排序规则只能在表内部问到 —— 所以表内读到变化只是
  // 把 built 打掉, 下一帧重建.
  int sort_column = 1;         // 默认按 Miss
  bool sort_ascending = false; // 缺得最多的排最前

  uint64_t generation = 0; // 对应的 Asset::asset_stats_generation
  bool built = false;
};

struct EncodeState {
  int num_workers = 0; // 0 means auto-detect (use max cores)
  bool skip_existing = true;
  bool show_missing_details = false;

  // 两个页签各一份缺失表视图, 结构与列完全对仗
  AssetTableView archive_view;
  AssetTableView order_view;

  // By Date 页签: 同样一档 archive 一档 orders
  DateTableView archive_date_view;
  DateTableView order_date_view;

  // Encoding dialog states
  bool show_confirm_dialog = false;
  bool show_progress_fullscreen = false;
  bool skip_file_check_ack = false; // 未通过 File Check 时的"风险自负"确认

  // Encode 支持增量: 归档不存在/不覆盖回测区间时也能先编已有的部分, 慢慢补.
  // 这两个"风险自负"确认解锁对应的硬性前置条件.
  bool skip_archive_exists_ack = false;
  bool skip_archive_range_ack = false;

  // Trigger for starting encoding (set by UI, consumed by TaskDatabase)
  bool trigger_start = false;
};

// ============================================================================
// Render Function
// ============================================================================

void RenderTabEncode(
    EncodingService *encoding_service,
    ScanService *scan_service,
    EncodeState &state,
    Asset &asset);

} // namespace GUI::Database
