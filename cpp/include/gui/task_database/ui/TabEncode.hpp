// Tab Encode - L2 Binary Database Encoding Control Panel
// Controls CSV→Binary conversion with worker management
#pragma once

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

struct EncodeState {
  int num_workers = 0; // 0 means auto-detect (use max cores)
  bool skip_existing = true;
  bool show_missing_assets = true;
  bool show_missing_details = false;

  // Encoding dialog states
  bool show_confirm_dialog = false;
  bool show_progress_fullscreen = false;
  
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
