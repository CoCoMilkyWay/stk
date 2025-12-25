#include "shared/TaskState.hpp"
#include "imgui.h"

// ============================================================================
// Settings Status
// ============================================================================

const char *TaskState::Settings::status_text() const {
  switch (status) {
  case Status::Initializing:
    return "initializing";
  case Status::Syncing:
    return "syncing";
  case Status::Writing:
    return "writing";
  case Status::Synced:
    return "synced";
  default:
    return "";
  }
}

ImVec4 TaskState::Settings::status_color() const {
  switch (status) {
  case Status::Syncing:
    return ImVec4(0.7f, 0.4f, 1.0f, 1.0f); // Purple
  case Status::Synced:
    return ImVec4(0.0f, 1.0f, 0.0f, 1.0f); // Green
  case Status::Writing:
    return ImVec4(1.0f, 1.0f, 0.0f, 1.0f); // Yellow
  case Status::Initializing:
    return ImVec4(0.5f, 0.5f, 0.5f, 1.0f); // Gray
  default:
    return ImVec4(0.0f, 1.0f, 1.0f, 1.0f); // Cyan
  }
}

// ============================================================================
// Database Status
// ============================================================================

const char *TaskState::Database::status_text() const {
  switch (status) {
  case Status::Initializing:
    return "initializing";
  case Status::Incomplete:
    return "incomplete";
  case Status::Error:
    return "error";
  case Status::Ready:
    return "ready";
  default:
    return "";
  }
}

ImVec4 TaskState::Database::status_color() const {
  switch (status) {
  case Status::Ready:
    return ImVec4(0.0f, 1.0f, 0.0f, 1.0f); // Green
  case Status::Incomplete:
    return ImVec4(1.0f, 1.0f, 0.0f, 1.0f); // Yellow
  case Status::Error:
    return ImVec4(1.0f, 0.0f, 0.0f, 1.0f); // Red
  case Status::Initializing:
    return ImVec4(0.5f, 0.5f, 0.5f, 1.0f); // Gray
  default:
    return ImVec4(0.0f, 1.0f, 1.0f, 1.0f); // Cyan
  }
}

// ============================================================================
// Features Status
// ============================================================================

const char *TaskState::Features::status_text() const {
  switch (status) {
  case Status::Waiting:
    return "waiting";
  case Status::Selecting:
    return "selecting";
  case Status::Computing:
    return "computing";
  case Status::Ready:
    return "ready";
  case Status::Error:
    return "error";
  default:
    return "";
  }
}

ImVec4 TaskState::Features::status_color() const {
  switch (status) {
  case Status::Computing:
    return ImVec4(0.7f, 0.4f, 1.0f, 1.0f); // Purple
  case Status::Ready:
    return ImVec4(0.0f, 1.0f, 0.0f, 1.0f); // Green
  case Status::Waiting:
    return ImVec4(0.5f, 0.5f, 0.5f, 1.0f); // Gray
  case Status::Selecting:
    return ImVec4(1.0f, 0.8f, 0.0f, 1.0f); // Orange
  case Status::Error:
    return ImVec4(1.0f, 0.0f, 0.0f, 1.0f); // Red
  default:
    return ImVec4(0.0f, 1.0f, 1.0f, 1.0f); // Cyan
  }
}

