// Tab Encode - L2 Binary Database Encoding Control Panel
// Controls CSV→Binary conversion with worker management
#pragma once

#include <cstdint>
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

// 资产表格的一个视图 = 过滤 + 排序后的 asset_id 列表.
//
// 行统计本身缓存在 Asset::asset_stats (扫描后算一次); 这里再缓存一层顺序,
// 因为几千行的排序 (含字符串比较) 逐帧重做同样是白烧. 只在统计代数 / 过滤
// 开关 / 排序规则变化时重建.
struct AssetTableView {
  std::vector<size_t> rows;  // asset_id
  uint64_t generation = 0;   // 对应的 Asset::asset_stats_generation
  bool missing_only = false; // 对应的过滤开关
  bool built = false;
};

struct EncodeState {
  int num_workers = 0; // 0 means auto-detect (use max cores)
  bool skip_existing = true;
  bool show_missing_assets = true;
  bool show_missing_details = false;

  // 两个页签各自的表格视图 (Archive / Binary Order)
  AssetTableView archive_view;
  AssetTableView order_view;

  // Encoding dialog states
  bool show_confirm_dialog = false;
  bool show_progress_fullscreen = false;
  bool skip_file_check_ack = false; // 未通过 File Check 时的"风险自负"确认

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
