#include "gui/task_icon_bar/TaskIconBar.hpp"
#include "gui/Config.hpp"
#include "gui/coro/CoroManager.hpp"
#include "gui/task_icon_bar/CoroNetwork.hpp"
#include "imgui.h"
#include "misc/system_metrics.hpp"
#include <algorithm>
#include <array>
#include <chrono>

// ============================================================================
// Configuration Parameters
// ============================================================================
namespace IconBarConfig {
// Update intervals
constexpr int UPDATE_INTERVAL_MS = 500; // CPU, Memory update interval

// Smoothing
constexpr int SMOOTHING_WINDOW_MS = 2000; // 2-second smoothing window (2s / 500ms = 4 samples)
constexpr int HISTORY_SIZE = SMOOTHING_WINDOW_MS / UPDATE_INTERVAL_MS;

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

namespace sysmon = misc::sysmon;

// 2 秒滑动平均。采样间隔固定, 所以定长环形缓冲就是精确的时间窗, 且不用分配。
class Smoothed {
public:
  explicit Smoothed(float initial) : average(initial) { samples.fill(initial); }

  void push(float value) {
    samples[index] = value;
    index = (index + 1) % IconBarConfig::HISTORY_SIZE;
    float sum = 0.0f;
    for (float sample : samples)
      sum += sample;
    average = sum / IconBarConfig::HISTORY_SIZE;
  }

  float average;

private:
  std::array<float, IconBarConfig::HISTORY_SIZE> samples = {};
  int index = 0;
};

// Icon bar for compact status display
class IconBar {
private:
  // CPU / 内存来自共享采集层 (misc/system_metrics), FPS 由本地帧计数得到
  Smoothed fps{60.0f};
  Smoothed cpu{0.0f};
  Smoothed mem{0.0f};
  std::chrono::steady_clock::time_point last_update;
  int frame_count = 0;

  // Network status (read from global coroutine-managed state)
  using NetworkStatus = NetworkMonitor::Status;

public:
  IconBar() : last_update(std::chrono::steady_clock::now()) {}

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
    ++frame_count;

    const auto now = std::chrono::steady_clock::now();
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_update);
    if (elapsed.count() < IconBarConfig::UPDATE_INTERVAL_MS)
      return;

    // 与 SystemInfo 面板共用同一次采样; 面板开着时它已经按 100ms 刷过了, 这里直接读结果
    sysmon::Monitor &monitor = sysmon::Monitor::instance();
    monitor.poll(sysmon::Scope::Basic, std::chrono::milliseconds(IconBarConfig::UPDATE_INTERVAL_MS));
    const sysmon::Sample &sample = monitor.sample();

    fps.push(static_cast<float>(frame_count) * 1000.0f / static_cast<float>(elapsed.count()));
    cpu.push(sample.cpu_total_percent);
    mem.push(sample.mem_used_percent);

    frame_count = 0;
    last_update = now;
  }

  static constexpr ImVec4 GREEN = ImVec4(0.0f, 1.0f, 0.0f, 1.0f);
  static constexpr ImVec4 YELLOW = ImVec4(1.0f, 1.0f, 0.0f, 1.0f);
  static constexpr ImVec4 RED = ImVec4(1.0f, 0.0f, 0.0f, 1.0f);

  // "越低越好"型指标 (CPU / 内存占用) 的统一画法。
  // 明细行走回调而不是现成的字符串: 不悬停时一个字节都不用格式化。
  template <typename DetailFn>
  static void DrawUsageIcon(const char *prefix, const char *title, float value,
                            float green_below, float yellow_below, DetailFn &&detail) {
    const ImVec4 color = value < green_below ? GREEN : (value < yellow_below ? YELLOW : RED);

    ImGui::Text("%s", prefix);
    ImGui::SameLine(0, 0);
    ImGui::TextColored(color, "%2.0f", std::min(value, 99.0f));

    if (ImGui::IsItemHovered()) {
      ImGui::BeginTooltip();
      ImGui::Text("%s: %.1f%% (2s avg)", title, value);
      detail();
      ImGui::Separator();
      ImGui::TextColored(GREEN, "Green:  < %.0f%%", green_below);
      ImGui::TextColored(YELLOW, "Yellow: %.0f-%.0f%%", green_below, yellow_below);
      ImGui::TextColored(RED, "Red:    > %.0f%%", yellow_below);
      ImGui::EndTooltip();
    }
  }

  void DrawCPUIcon() {
    DrawUsageIcon("C:", "CPU Usage", cpu.average,
                  IconBarConfig::Thresholds::CPU_GREEN, IconBarConfig::Thresholds::CPU_YELLOW, []() {
                    const sysmon::StaticInfo &info = sysmon::Monitor::instance().info();
                    ImGui::TextDisabled("%d logical / %d physical", info.logical_cores, info.physical_cores);
                    ImGui::TextDisabled("%s", info.cpu_model.c_str());
                  });
  }

  void DrawMemoryIcon() {
    DrawUsageIcon("M:", "Memory Usage", mem.average,
                  IconBarConfig::Thresholds::MEM_GREEN, IconBarConfig::Thresholds::MEM_YELLOW, []() {
                    const sysmon::Monitor &monitor = sysmon::Monitor::instance();
                    constexpr double GB = 1024.0 * 1024.0 * 1024.0;
                    ImGui::TextDisabled("%.1f / %.0f GB", monitor.sample().mem_used_bytes / GB,
                                        monitor.info().ram_total_bytes / GB);
                  });
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

    // FPS 与占用率相反: 越高越好, 所以阈值方向反过来
    const ImVec4 color = fps.average >= FPS_GREEN ? GREEN : (fps.average >= FPS_YELLOW ? YELLOW : RED);

    ImGui::Text("F:");
    ImGui::SameLine(0, 0);
    ImGui::TextColored(color, "%2.0f", std::min(fps.average, 99.0f));

    if (ImGui::IsItemHovered()) {
      ImGui::BeginTooltip();
      ImGui::Text("FPS: %.1f (2s avg)", fps.average);
      ImGui::Separator();
      ImGui::TextColored(GREEN, "Green:  >= %.0f", FPS_GREEN);
      ImGui::TextColored(YELLOW, "Yellow: %.0f-%.0f", FPS_YELLOW, FPS_GREEN);
      ImGui::TextColored(RED, "Red:    < %.0f", FPS_YELLOW);
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

void InitIconBar(CoroManager &coromgr) {
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
    g_coro_network->Start(coromgr, targets, std::chrono::seconds(5));
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
