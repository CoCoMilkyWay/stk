// Tab Encode - L2 Binary Database Encoding Control Panel
// Controls CSV→Binary conversion with worker management
#pragma once

// Forward declarations
namespace GUI::Database {
class EncodingService;
struct EncodingProgress;
}

struct Asset;

namespace GUI::Database {

// ============================================================================
// Encode Tab State
// ============================================================================

struct EncodeState {
  int num_workers = 4;
  bool skip_existing = true;
  bool show_missing_assets = true;
  bool show_missing_details = false;
};

// ============================================================================
// Render Function
// ============================================================================

void RenderTabEncode(
    EncodingService *service,
    EncodeState &state,
    Asset &asset);

} // namespace GUI::Database

