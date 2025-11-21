#include "imgui.h"
#include "gui/GuiState.hpp"
#include "gui/coro/CoroNetwork.hpp"
#include <array>
#include <chrono>
#include <fstream>
#include <sstream>
#include <string>
#include <memory>
#include <sys/sysinfo.h>

// ============================================================================
// Configuration Parameters
// ============================================================================
namespace IconBarConfig {
// Update intervals
constexpr int UPDATE_INTERVAL_MS = 500;     // CPU, Memory update interval

// Smoothing
constexpr int SMOOTHING_WINDOW_MS = 2000; // 2-second smoothing window (2s / 500ms = 4 samples)
constexpr int FPS_HISTORY_SIZE = SMOOTHING_WINDOW_MS / UPDATE_INTERVAL_MS;
constexpr int CPU_HISTORY_SIZE = SMOOTHING_WINDOW_MS / UPDATE_INTERVAL_MS;
constexpr int MEM_HISTORY_SIZE = SMOOTHING_WINDOW_MS / UPDATE_INTERVAL_MS;

// Network ping targets
constexpr const char* PING_TARGET_GOOGLE = "1.1.1.1";  // Cloudflare DNS
constexpr const char* PING_TARGET_GOOGLE_NAME = "Cloudflare";
constexpr const char* PING_TARGET_BAIDU = "www.baidu.com";
constexpr const char* PING_TARGET_BAIDU_NAME = "Baidu";

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
  long last_cpu_total = 0;
  long last_cpu_idle = 0;
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
  }

  void Draw() {
    UpdateMetrics();

    ImGui::BeginGroup();

    // Network icon
    DrawNetworkIcon();

    ImGui::SameLine();
    ImGui::Text("|");
    ImGui::SameLine();

    // CPU icon
    DrawCPUIcon();

    ImGui::SameLine();
    ImGui::Text("|");
    ImGui::SameLine();

    // Memory icon
    DrawMemoryIcon();

    ImGui::SameLine();
    ImGui::Text("|");
    ImGui::SameLine();

    // FPS icon
    DrawFPSIcon();

    ImGui::EndGroup();
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
    std::ifstream stat_file("/proc/stat");
    std::string line;
    if (std::getline(stat_file, line)) {
      std::istringstream ss(line);
      std::string cpu;
      long user, nice, system, idle;
      ss >> cpu >> user >> nice >> system >> idle;

      long total = user + nice + system + idle;
      long total_diff = total - last_cpu_total;
      long idle_diff = idle - last_cpu_idle;

      last_cpu_total = total;
      last_cpu_idle = idle;

      if (total_diff > 0) {
        return 100.0f * (1.0f - (float)idle_diff / (float)total_diff);
      }
    }
    return 0.0f;
  }

  float GetMemoryUsage() {
    struct sysinfo si;
    if (sysinfo(&si) == 0) {
      return 100.0f * (1.0f - (float)si.freeram / (float)si.totalram);
    }
    return 0.0f;
  }

  void DrawCPUIcon() {
    using namespace IconBarConfig::Thresholds;

    ImVec4 color;
    if (cpu_avg < CPU_GREEN) {
      color = ImVec4(0.0f, 1.0f, 0.0f, 1.0f); // Green
    } else if (cpu_avg < CPU_YELLOW) {
      color = ImVec4(1.0f, 1.0f, 0.0f, 1.0f); // Yellow
    } else {
      color = ImVec4(1.0f, 0.0f, 0.0f, 1.0f); // Red
    }

    ImGui::Text("C:");
    ImGui::SameLine();
    ImGui::TextColored(color, "%3.0f", cpu_avg);

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
    if (mem_avg < MEM_GREEN) {
      color = ImVec4(0.0f, 1.0f, 0.0f, 1.0f); // Green
    } else if (mem_avg < MEM_YELLOW) {
      color = ImVec4(1.0f, 1.0f, 0.0f, 1.0f); // Yellow
    } else {
      color = ImVec4(1.0f, 0.0f, 0.0f, 1.0f); // Red
    }

    ImGui::Text("M:");
    ImGui::SameLine();
    ImGui::TextColored(color, "%3.0f", mem_avg);

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
    auto& net = NetworkMonitor::Instance();
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
      const char* target_names[] = {
        IconBarConfig::PING_TARGET_GOOGLE_NAME,
        IconBarConfig::PING_TARGET_BAIDU_NAME
      };
      const char* target_hosts[] = {
        IconBarConfig::PING_TARGET_GOOGLE,
        IconBarConfig::PING_TARGET_BAIDU
      };
      
      for (size_t i = 0; i < target_pings.size(); ++i) {
        const char* name = (i < 2) ? target_names[i] : "Unknown";
        const char* host = (i < 2) ? target_hosts[i] : "unknown";
        
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
    if (fps_avg >= FPS_GREEN) {
      color = ImVec4(0.0f, 1.0f, 0.0f, 1.0f); // Green
    } else if (fps_avg >= FPS_YELLOW) {
      color = ImVec4(1.0f, 1.0f, 0.0f, 1.0f); // Yellow
    } else {
      color = ImVec4(1.0f, 0.0f, 0.0f, 1.0f); // Red
    }

    ImGui::Text("F:");
    ImGui::SameLine();
    ImGui::TextColored(color, "%3.0f", fps_avg);

    if (ImGui::IsItemHovered()) {
      ImGui::BeginTooltip();
      ImGui::Text("FPS: %.1f (2s avg)", fps_avg);
      ImGui::Separator();
      ImGui::TextColored(ImVec4(0.0f, 1.0f, 0.0f, 1.0f), "Green:  >= %.0f", FPS_GREEN);
      ImGui::TextColored(ImVec4(1.0f, 1.0f, 0.0f, 1.0f), "Yellow: %.0f-%.0f", FPS_YELLOW, FPS_GREEN);
      ImGui::TextColored(ImVec4(1.0f, 0.0f, 0.0f, 1.0f), "Red:    < %.0f", FPS_YELLOW);
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

void InitIconBar(GuiState& gui_state) {
  if (!g_icon_bar) {
    g_icon_bar = new IconBar();
  }
  
  // Initialize network monitoring with IconBar-specific targets
  if (!g_coro_network) {
    g_coro_network = std::make_unique<CoroNetwork>();
    
    // Configure ping targets (IconBar business logic)
    std::vector<CoroNetwork::PingTarget> targets = {
      {IconBarConfig::PING_TARGET_GOOGLE, IconBarConfig::PING_TARGET_GOOGLE_NAME},
      {IconBarConfig::PING_TARGET_BAIDU, IconBarConfig::PING_TARGET_BAIDU_NAME}
    };
    
    // Start network monitoring coroutine
    g_coro_network->Start(gui_state.Coro(), targets, std::chrono::seconds(5));
  }
}

void DrawIconBar() {
  if (g_icon_bar) {
    g_icon_bar->Draw();
  }
}

void CleanupIconBar() {
  if (g_icon_bar) {
    delete g_icon_bar;
    g_icon_bar = nullptr;
  }
}
