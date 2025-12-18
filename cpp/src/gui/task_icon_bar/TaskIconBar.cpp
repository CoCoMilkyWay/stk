#include "gui/task_icon_bar/TaskIconBar.hpp"
#include "gui/Config.hpp"
#include "gui/task_icon_bar/CoroNetwork.hpp"
#include "imgui.h"
#include "shared/GuiState.hpp"
#include <algorithm>
#include <array>
#include <chrono>
#include <windows.h>
#include <pdh.h>
#include <pdhmsg.h>

// ============================================================================
// Configuration Parameters
// ============================================================================
namespace IconBarConfig {
// Update intervals
constexpr int UPDATE_INTERVAL_MS = 500; // CPU, Memory update interval

// Smoothing
constexpr int SMOOTHING_WINDOW_MS = 2000; // 2-second smoothing window (2s / 500ms = 4 samples)
constexpr int FPS_HISTORY_SIZE = SMOOTHING_WINDOW_MS / UPDATE_INTERVAL_MS;
constexpr int CPU_HISTORY_SIZE = SMOOTHING_WINDOW_MS / UPDATE_INTERVAL_MS;
constexpr int MEM_HISTORY_SIZE = SMOOTHING_WINDOW_MS / UPDATE_INTERVAL_MS;

// Network ping targets
constexpr const char *PING_TARGET_GOOGLE = "1.1.1.1"; // Cloudflare DNS
constexpr const char *PING_TARGET_GOOGLE_NAME = "Cloudflare";
constexpr const char *PING_TARGET_BAIDU = "www.baidu.com";
constexpr const char *PING_TARGET_BAIDU_NAME = "Baidu";

// Thresholds for color coding
namespace Thresholds {
// CPU (percentage)
constexpr float CPU_GREEN = 50.0f;
constexpr float CPU_YELLOW = 80.0f;

// Memory (percentage)
constexpr float MEM_GREEN = 70.0f;
constexpr float MEM_YELLOW = 90.0f;

// FPS
constexpr float FPS_GREEN = 55.0f;
constexpr float FPS_YELLOW = 30.0f;

// Network (milliseconds)
constexpr int NET_GREEN = 50;
constexpr int NET_YELLOW = 100;
} // namespace Thresholds
} // namespace IconBarConfig

namespace GUI::TaskIconBar {
namespace {

// Icon bar for compact status display
class IconBar {
private:
  // FPS tracking with smoothing
  std::array<float, IconBarConfig::FPS_HISTORY_SIZE> fps_history = {};
  int fps_history_idx = 0;
  float fps_avg = 0.0f;
  std::chrono::steady_clock::time_point last_fps_update;
  int frame_count = 0;

  // CPU tracking with smoothing
  std::array<float, IconBarConfig::CPU_HISTORY_SIZE> cpu_history = {};
  int cpu_history_idx = 0;
  float cpu_avg = 0.0f;
  PDH_HQUERY cpu_query = nullptr;
  PDH_HCOUNTER cpu_counter = nullptr;
  std::chrono::steady_clock::time_point last_cpu_update;

  // Memory tracking with smoothing
  std::array<float, IconBarConfig::MEM_HISTORY_SIZE> mem_history = {};
  int mem_history_idx = 0;
  float mem_avg = 0.0f;
  std::chrono::steady_clock::time_point last_mem_update;

  // Network status (read from global coroutine-managed state)
  using NetworkStatus = NetworkMonitor::Status;

public:
  IconBar() {
    last_fps_update = std::chrono::steady_clock::now();
    last_cpu_update = std::chrono::steady_clock::now();
    last_mem_update = std::chrono::steady_clock::now();

    // Initialize history arrays with reasonable defaults
    for (auto &val : fps_history)
      val = 60.0f;
    for (auto &val : cpu_history)
      val = 0.0f;
    for (auto &val : mem_history)
      val = 0.0f;

    // Initialize Windows PDH for CPU monitoring
    PdhOpenQueryW(nullptr, 0, &cpu_query);
    PdhAddCounterW(cpu_query, L"\\Processor(_Total)\\% Processor Time", 0, &cpu_counter);
    PdhCollectQueryData(cpu_query); // Initial sample
  }

  ~IconBar() {
    if (cpu_query) {
      PdhCloseQuery(cpu_query);
    }
  }

  void Draw() {
    UpdateMetrics();

    // Ultra-compact layout with minimal spacing
    ImGui::PushStyleVar(ImGuiStyleVar_ItemSpacing, ImVec2(2.0f, 0.0f));
    ImGui::BeginGroup();

    // Network icon
    DrawNetworkIcon();
    ImGui::SameLine(0, 2.0f);
    ImGui::TextDisabled("|");

    // CPU icon
    ImGui::SameLine(0, 2.0f);
    DrawCPUIcon();
    ImGui::SameLine(0, 2.0f);
    ImGui::TextDisabled("|");

    // Memory icon
    ImGui::SameLine(0, 2.0f);
    DrawMemoryIcon();
    ImGui::SameLine(0, 2.0f);
    ImGui::TextDisabled("|");

    // FPS icon
    ImGui::SameLine(0, 2.0f);
    DrawFPSIcon();

    ImGui::EndGroup();
    ImGui::PopStyleVar();
  }

private:
  void UpdateMetrics() {
    auto now = std::chrono::steady_clock::now();

    // Update FPS (smooth with moving average)
    frame_count++;
    auto fps_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_fps_update);
    if (fps_elapsed.count() >= IconBarConfig::UPDATE_INTERVAL_MS) {
      float current_fps = frame_count * 1000.0f / fps_elapsed.count();
      fps_history[fps_history_idx] = current_fps;
      fps_history_idx = (fps_history_idx + 1) % IconBarConfig::FPS_HISTORY_SIZE;

      // Calculate moving average
      float sum = 0.0f;
      for (const auto &val : fps_history)
        sum += val;
      fps_avg = sum / IconBarConfig::FPS_HISTORY_SIZE;

      frame_count = 0;
      last_fps_update = now;
    }

    // Update CPU (smooth with moving average)
    auto cpu_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_cpu_update);
    if (cpu_elapsed.count() >= IconBarConfig::UPDATE_INTERVAL_MS) {
      float current_cpu = GetCPUUsage();
      cpu_history[cpu_history_idx] = current_cpu;
      cpu_history_idx = (cpu_history_idx + 1) % IconBarConfig::CPU_HISTORY_SIZE;

      // Calculate moving average
      float sum = 0.0f;
      for (const auto &val : cpu_history)
        sum += val;
      cpu_avg = sum / IconBarConfig::CPU_HISTORY_SIZE;

      last_cpu_update = now;
    }

    // Update Memory (smooth with moving average)
    auto mem_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_mem_update);
    if (mem_elapsed.count() >= IconBarConfig::UPDATE_INTERVAL_MS) {
      float current_mem = GetMemoryUsage();
      mem_history[mem_history_idx] = current_mem;
      mem_history_idx = (mem_history_idx + 1) % IconBarConfig::MEM_HISTORY_SIZE;

      // Calculate moving average
      float sum = 0.0f;
      for (const auto &val : mem_history)
        sum += val;
      mem_avg = sum / IconBarConfig::MEM_HISTORY_SIZE;

      last_mem_update = now;
    }
  }

  float GetCPUUsage() {
    if (!cpu_query || !cpu_counter) return 0.0f;
    
    PDH_FMT_COUNTERVALUE counter_value;
    PdhCollectQueryData(cpu_query);
    if (PdhGetFormattedCounterValue(cpu_counter, PDH_FMT_DOUBLE, nullptr, &counter_value) == ERROR_SUCCESS) {
      return static_cast<float>(counter_value.doubleValue);
    }
    return 0.0f;
  }

  float GetMemoryUsage() {
    MEMORYSTATUSEX mem_info;
    mem_info.dwLength = sizeof(mem_info);
    if (GlobalMemoryStatusEx(&mem_info)) {
      return static_cast<float>(mem_info.dwMemoryLoad);
    }
    return 0.0f;
  }

  void DrawCPUIcon() {
    using namespace IconBarConfig::Thresholds;

    ImVec4 color;
    float cpu_clamped = std::min(cpu_avg, 99.0f);
    if (cpu_avg < CPU_GREEN) {
      color = ImVec4(0.0f, 1.0f, 0.0f, 1.0f); // Green
    } else if (cpu_avg < CPU_YELLOW) {
      color = ImVec4(1.0f, 1.0f, 0.0f, 1.0f); // Yellow
    } else {
      color = ImVec4(1.0f, 0.0f, 0.0f, 1.0f); // Red
    }

    ImGui::Text("C:");
    ImGui::SameLine(0, 0);
    ImGui::TextColored(color, "%2.0f", cpu_clamped);

    if (ImGui::IsItemHovered()) {
      ImGui::BeginTooltip();
      ImGui::Text("CPU Usage: %.1f%% (2s avg)", cpu_avg);
      ImGui::Separator();
      ImGui::TextColored(ImVec4(0.0f, 1.0f, 0.0f, 1.0f), "Green:  < %.0f%%", CPU_GREEN);
      ImGui::TextColored(ImVec4(1.0f, 1.0f, 0.0f, 1.0f), "Yellow: %.0f-%.0f%%", CPU_GREEN, CPU_YELLOW);
      ImGui::TextColored(ImVec4(1.0f, 0.0f, 0.0f, 1.0f), "Red:    > %.0f%%", CPU_YELLOW);
      ImGui::EndTooltip();
    }
  }

  void DrawMemoryIcon() {
    using namespace IconBarConfig::Thresholds;

    ImVec4 color;
    float mem_clamped = std::min(mem_avg, 99.0f);
    if (mem_avg < MEM_GREEN) {
      color = ImVec4(0.0f, 1.0f, 0.0f, 1.0f); // Green
    } else if (mem_avg < MEM_YELLOW) {
      color = ImVec4(1.0f, 1.0f, 0.0f, 1.0f); // Yellow
    } else {
      color = ImVec4(1.0f, 0.0f, 0.0f, 1.0f); // Red
    }

    ImGui::Text("M:");
    ImGui::SameLine(0, 0);
    ImGui::TextColored(color, "%2.0f", mem_clamped);

    if (ImGui::IsItemHovered()) {
      ImGui::BeginTooltip();
      ImGui::Text("Memory Usage: %.1f%% (2s avg)", mem_avg);
      ImGui::Separator();
      ImGui::TextColored(ImVec4(0.0f, 1.0f, 0.0f, 1.0f), "Green:  < %.0f%%", MEM_GREEN);
      ImGui::TextColored(ImVec4(1.0f, 1.0f, 0.0f, 1.0f), "Yellow: %.0f-%.0f%%", MEM_GREEN, MEM_YELLOW);
      ImGui::TextColored(ImVec4(1.0f, 0.0f, 0.0f, 1.0f), "Red:    > %.0f%%", MEM_YELLOW);
      ImGui::EndTooltip();
    }
  }

  void DrawNetworkIcon() {
    using namespace IconBarConfig::Thresholds;

    // Read network status from global coroutine-managed state
    auto &net = NetworkMonitor::Instance();
    auto status = net.GetStatus();
    int ping = net.GetPingMs();

    // Get color based on status
    ImVec4 color;
    const char *icon;

    switch (status) {
    case NetworkStatus::Good:
      color = ImVec4(0.0f, 1.0f, 0.0f, 1.0f); // Green
      icon = "NET";
      break;
    case NetworkStatus::Medium:
      color = ImVec4(1.0f, 1.0f, 0.0f, 1.0f); // Yellow
      icon = "NET";
      break;
    case NetworkStatus::Bad:
      color = ImVec4(1.0f, 0.5f, 0.0f, 1.0f); // Orange
      icon = "NET";
      break;
    case NetworkStatus::Error:
      color = ImVec4(1.0f, 0.0f, 0.0f, 1.0f); // Red
      icon = "NET";
      break;
    default:
      color = ImVec4(0.5f, 0.5f, 0.5f, 1.0f); // Gray
      icon = "NET";
      break;
    }

    ImGui::TextColored(color, "%s", icon);

    if (ImGui::IsItemHovered()) {
      ImGui::BeginTooltip();
      ImGui::Text("Network: %s", GetStatusString(status));
      ImGui::Separator();

      // Display all target pings dynamically
      auto target_pings = net.GetTargetPings();
      const char *target_names[] = {
          IconBarConfig::PING_TARGET_GOOGLE_NAME,
          IconBarConfig::PING_TARGET_BAIDU_NAME};
      const char *target_hosts[] = {
          IconBarConfig::PING_TARGET_GOOGLE,
          IconBarConfig::PING_TARGET_BAIDU};

      for (size_t i = 0; i < target_pings.size(); ++i) {
        const char *name = (i < 2) ? target_names[i] : "Unknown";
        const char *host = (i < 2) ? target_hosts[i] : "unknown";

        if (target_pings[i] >= 0) {
          ImGui::Text("%s (%s): %d ms", name, host, target_pings[i]);
        } else {
          ImGui::TextColored(ImVec4(1.0f, 0.0f, 0.0f, 1.0f), "%s (%s): Timeout", name, host);
        }
      }

      ImGui::Text("Best: %d ms", ping);
      ImGui::Separator();
      ImGui::TextColored(ImVec4(0.0f, 1.0f, 0.0f, 1.0f), "Green:  < %dms", NET_GREEN);
      ImGui::TextColored(ImVec4(1.0f, 1.0f, 0.0f, 1.0f), "Yellow: %d-%dms", NET_GREEN, NET_YELLOW);
      ImGui::TextColored(ImVec4(1.0f, 0.5f, 0.0f, 1.0f), "Orange: > %dms", NET_YELLOW);
      ImGui::TextColored(ImVec4(1.0f, 0.0f, 0.0f, 1.0f), "Red:    Offline");
      ImGui::Text("(Async ASIO coroutine)");
      ImGui::EndTooltip();
    }
  }

  void DrawFPSIcon() {
    using namespace IconBarConfig::Thresholds;

    // Color based on smoothed FPS
    ImVec4 color;
    float fps_clamped = std::min(fps_avg, 99.0f);
    if (fps_avg >= FPS_GREEN) {
      color = ImVec4(0.0f, 1.0f, 0.0f, 1.0f); // Green
    } else if (fps_avg >= FPS_YELLOW) {
      color = ImVec4(1.0f, 1.0f, 0.0f, 1.0f); // Yellow
    } else {
      color = ImVec4(1.0f, 0.0f, 0.0f, 1.0f); // Red
    }

    ImGui::Text("F:");
    ImGui::SameLine(0, 0);
    ImGui::TextColored(color, "%2.0f", fps_clamped);

    if (ImGui::IsItemHovered()) {
      ImGui::BeginTooltip();
      ImGui::Text("FPS: %.1f (2s avg)", fps_avg);
      ImGui::Separator();
      ImGui::TextColored(ImVec4(0.0f, 1.0f, 0.0f, 1.0f), "Green:  >= %.0f", FPS_GREEN);
      ImGui::TextColored(ImVec4(1.0f, 1.0f, 0.0f, 1.0f), "Yellow: %.0f-%.0f", FPS_YELLOW, FPS_GREEN);
      ImGui::TextColored(ImVec4(1.0f, 0.0f, 0.0f, 1.0f), "Red:    < %.0f", FPS_YELLOW);
      ImGui::Separator();
      ImGui::TextDisabled("Configuration:");
      ImGui::Text("TARGET_FPS:   %.1f", TARGET_FPS);
      ImGui::Text("HIGH_FPS_ON_EVENTS: %s", HIGH_FPS_ON_EVENTS ? "Enabled" : "Disabled");
      ImGui::Text("VSYNC_ENABLE:  %s", VSYNC_ENABLE ? "Enabled" : "Disabled");
      ImGui::Separator();
      ImGui::TextDisabled("To modify: cpp/include/gui/Config.hpp");
      ImGui::EndTooltip();
    }
  }

  const char *GetStatusString(NetworkStatus status) {
    switch (status) {
    case NetworkStatus::Good:
      return "Good";
    case NetworkStatus::Medium:
      return "Medium";
    case NetworkStatus::Bad:
      return "Poor";
    case NetworkStatus::Error:
      return "Offline";
    default:
      return "Unknown";
    }
  }
};

// Global icon bar instance
static IconBar *g_icon_bar = nullptr;

// Network monitoring coroutine (managed by IconBar)
static std::unique_ptr<CoroNetwork> g_coro_network;

} // namespace

void InitIconBar(GuiState &gui) {
  if (!g_icon_bar) {
    g_icon_bar = new IconBar();
  }

  // Initialize network monitoring with IconBar-specific targets
  if (!g_coro_network) {
    g_coro_network = std::make_unique<CoroNetwork>();

    // Configure ping targets (IconBar business logic)
    std::vector<CoroNetwork::PingTarget> targets = {
        {IconBarConfig::PING_TARGET_GOOGLE, IconBarConfig::PING_TARGET_GOOGLE_NAME},
        {IconBarConfig::PING_TARGET_BAIDU, IconBarConfig::PING_TARGET_BAIDU_NAME}};

    // Initialize NetworkMonitor singleton with number of targets
    NetworkMonitor::Instance().Initialize(targets.size());

    // Start network monitoring coroutine
    g_coro_network->Start(gui.Coro(), targets, std::chrono::seconds(5));
  }
}

void DrawIconBar() {
  if (g_icon_bar) {
    g_icon_bar->Draw();
  }
}

void CleanupIconBar() {
  if (g_coro_network) {
    g_coro_network->Stop();
    g_coro_network.reset();
  }

  if (g_icon_bar) {
    delete g_icon_bar;
    g_icon_bar = nullptr;
  }
}

} // namespace GUI::TaskIconBar
