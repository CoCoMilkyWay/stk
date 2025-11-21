#include "gui/GuiState.hpp"
#include "gui/GuiTask.hpp"
#include "imgui.h"
#include "implot.h"
#include "shared/SharedData.hpp"
#include <array>
#include <chrono>
#include <cstring>
#include <deque>
#include <fstream>
#include <sstream>
#include <sys/sysinfo.h>
#include <sys/utsname.h>
#include <thread>
#include <unistd.h>
#include <vector>

// SystemInfo task - display hardware and OS info with dynamic monitoring
class SystemInfoTask : public IGuiTask {
private:
  // Dynamic monitoring data
  static constexpr int HISTORY_SIZE = 100;
  std::array<float, HISTORY_SIZE> mem_history = {};
  int history_offset = 0;

  // Per-core CPU monitoring
  struct CoreStats {
    std::array<float, HISTORY_SIZE> history = {};
    std::deque<std::pair<std::chrono::steady_clock::time_point, float>> samples; // For 2s smoothing
    long last_total = 0;
    long last_idle = 0;
  };
  std::vector<CoreStats> core_stats;

  // GPU monitoring
  struct GPUStats {
    std::array<float, HISTORY_SIZE> usage_history = {};
    std::array<float, HISTORY_SIZE> memory_history = {};
    std::deque<std::pair<std::chrono::steady_clock::time_point, float>> usage_samples;
    std::deque<std::pair<std::chrono::steady_clock::time_point, float>> memory_samples;
    float current_usage = 0.0f;
    float current_memory = 0.0f;
    float total_memory_gb = 0.0f;
    bool available = false;
    std::string name;
  };
  GPUStats gpu_stats;

  // Cached system info
  std::string os_name;
  std::string kernel_version;
  std::string hostname;
  int cpu_cores = 0;
  long total_ram_gb = 0;

  bool initialized = false;
  bool is_expanded = false;
  std::chrono::steady_clock::time_point last_update_time;
  bool gpu_detection_logged = false;

public:
  const char *GetName() const override {
    return "SystemInfo";
  }

  const char *GetStatus() const override {
    return is_expanded ? "live" : "";
  }

  StatusColor GetStatusColor() const override {
    return is_expanded ? StatusColor::Purple() : StatusColor::None();
  }

  void OnExpand() override {
    is_expanded = true;
    if (!initialized) {
      InitSystemInfo();
      initialized = true;
    }
  }

  void OnCollapse() override {
    is_expanded = false;
  }

  void DrawPanel(SharedData & /*data*/, GuiState &gui_state) override {
    if (!initialized) {
      InitSystemInfo();
      initialized = true;
      last_update_time = std::chrono::steady_clock::now();
      gui_state.AddTerminalLog("SystemInfo initialized");
    }

    // Log GPU detection result once
    if (!gpu_detection_logged) {
      if (gpu_stats.available) {
        char buf[256];
        snprintf(buf, sizeof(buf), "GPU detected: %s (%.1f GB)", 
                 gpu_stats.name.c_str(), gpu_stats.total_memory_gb);
        gui_state.AddTerminalLog(buf);
      } else {
        gui_state.AddTerminalLog("No GPU detected (nvidia-smi not available)");
      }
      gpu_detection_logged = true;
    }

    // Only update dynamic stats when expanded and at fixed interval (100ms)
    if (is_expanded) {
      auto now = std::chrono::steady_clock::now();
      auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_update_time);
      if (elapsed.count() >= 100) {
        UpdateDynamicStats();
        last_update_time = now;
      }
    }

    // Compact layout: all info in one view without collapsing headers
    DrawCompactMonitor(gui_state);
  }

private:
  void InitSystemInfo() {
    // Get OS info
    struct utsname uts;
    if (uname(&uts) == 0) {
      os_name = uts.sysname;
      kernel_version = uts.release;
      hostname = uts.nodename;
    }

    // Get CPU cores
    cpu_cores = std::thread::hardware_concurrency();
    core_stats.resize(cpu_cores);

    // Get total RAM
    struct sysinfo si;
    if (sysinfo(&si) == 0) {
      total_ram_gb = si.totalram / (1024 * 1024 * 1024);
    }

    // Detect GPU
    DetectGPU();
  }

  void UpdateDynamicStats() {
    auto now = std::chrono::steady_clock::now();

    // Update per-core CPU usage
    UpdatePerCoreCPU(now);

    // Update memory usage
    float mem_usage = GetMemoryUsage();
    mem_history[history_offset] = mem_usage;

    // Update GPU stats
    UpdateGPUStats(now);

    history_offset = (history_offset + 1) % HISTORY_SIZE;
  }

  void UpdatePerCoreCPU(std::chrono::steady_clock::time_point now) {
    std::ifstream stat_file("/proc/stat");
    std::string line;

    // Skip the first line (aggregate cpu stats)
    std::getline(stat_file, line);

    // Read per-core stats
    for (int i = 0; i < cpu_cores && std::getline(stat_file, line); ++i) {
      if (line.find("cpu") != 0)
        break;

      std::istringstream ss(line);
      std::string cpu;
      long user, nice, system, idle, iowait, irq, softirq;
      ss >> cpu >> user >> nice >> system >> idle >> iowait >> irq >> softirq;

      long total = user + nice + system + idle + iowait + irq + softirq;
      long total_diff = total - core_stats[i].last_total;
      long idle_diff = idle - core_stats[i].last_idle;

      core_stats[i].last_total = total;
      core_stats[i].last_idle = idle;

      float usage = 0.0f;
      if (total_diff > 0) {
        usage = 100.0f * (1.0f - (float)idle_diff / (float)total_diff);
      }

      // Add sample for smoothing
      core_stats[i].samples.push_back({now, usage});

      // Remove samples older than 2 seconds
      auto cutoff = now - std::chrono::seconds(2);
      while (!core_stats[i].samples.empty() && core_stats[i].samples.front().first < cutoff) {
        core_stats[i].samples.pop_front();
      }

      // Calculate smoothed value
      float smoothed = 0.0f;
      if (!core_stats[i].samples.empty()) {
        for (const auto &[t, v] : core_stats[i].samples) {
          smoothed += v;
        }
        smoothed /= core_stats[i].samples.size();
      }

      core_stats[i].history[history_offset] = smoothed;
    }
  }

  float GetMemoryUsage() {
    struct sysinfo si;
    if (sysinfo(&si) == 0) {
      return 100.0f * (1.0f - (float)si.freeram / (float)si.totalram);
    }
    return 0.0f;
  }

  void DetectGPU() {
    // Try NVIDIA
    FILE *pipe = popen("nvidia-smi --query-gpu=name,memory.total --format=csv,noheader,nounits 2>&1", "r");
    if (pipe) {
      char buffer[256];
      if (fgets(buffer, sizeof(buffer), pipe)) {
        std::string line(buffer);
        // Check if it's an error message
        if (line.find("NVIDIA-SMI has failed") == std::string::npos && 
            line.find("command not found") == std::string::npos) {
          auto comma_pos = line.rfind(',');
          if (comma_pos != std::string::npos) {
            gpu_stats.name = line.substr(0, comma_pos);
            // Trim trailing whitespace
            gpu_stats.name.erase(gpu_stats.name.find_last_not_of(" \n\r\t") + 1);
            gpu_stats.total_memory_gb = std::stof(line.substr(comma_pos + 1)) / 1024.0f;
            gpu_stats.available = true;
            pclose(pipe);
            return;
          }
        }
      }
      pclose(pipe);
    }

    // Try AMD
    pipe = popen("which radeontop 2>/dev/null", "r");
    if (pipe) {
      char buffer[256];
      if (fgets(buffer, sizeof(buffer), pipe)) {
        gpu_stats.name = "AMD GPU";
        gpu_stats.available = true;
        pclose(pipe);
        return;
      }
      pclose(pipe);
    }

    // Try Intel
    pipe = popen("which intel_gpu_top 2>/dev/null", "r");
    if (pipe) {
      char buffer[256];
      if (fgets(buffer, sizeof(buffer), pipe)) {
        gpu_stats.name = "Intel GPU";
        gpu_stats.available = true;
        pclose(pipe);
        return;
      }
      pclose(pipe);
    }

    gpu_stats.available = false;
  }

  void UpdateGPUStats(std::chrono::steady_clock::time_point now) {
    if (!gpu_stats.available)
      return;

    float usage = 0.0f;
    float memory_used = 0.0f;

    // Try NVIDIA
    FILE *pipe = popen("nvidia-smi --query-gpu=utilization.gpu,memory.used --format=csv,noheader,nounits 2>/dev/null", "r");
    if (pipe) {
      char buffer[128];
      if (fgets(buffer, sizeof(buffer), pipe)) {
        std::istringstream ss(buffer);
        std::string token;
        if (std::getline(ss, token, ',')) {
          usage = std::stof(token);
        }
        if (std::getline(ss, token, ',')) {
          memory_used = std::stof(token);
        }
      }
      pclose(pipe);
    }

    // Add samples for smoothing
    gpu_stats.usage_samples.push_back({now, usage});
    gpu_stats.memory_samples.push_back({now, memory_used});

    // Remove samples older than 2 seconds
    auto cutoff = now - std::chrono::seconds(2);
    while (!gpu_stats.usage_samples.empty() && gpu_stats.usage_samples.front().first < cutoff) {
      gpu_stats.usage_samples.pop_front();
    }
    while (!gpu_stats.memory_samples.empty() && gpu_stats.memory_samples.front().first < cutoff) {
      gpu_stats.memory_samples.pop_front();
    }

    // Calculate smoothed values
    if (!gpu_stats.usage_samples.empty()) {
      float sum = 0.0f;
      for (const auto &[t, v] : gpu_stats.usage_samples) {
        sum += v;
      }
      gpu_stats.current_usage = sum / gpu_stats.usage_samples.size();
      gpu_stats.usage_history[history_offset] = gpu_stats.current_usage;
    }

    if (!gpu_stats.memory_samples.empty()) {
      float sum = 0.0f;
      for (const auto &[t, v] : gpu_stats.memory_samples) {
        sum += v;
      }
      float smoothed_mem = sum / gpu_stats.memory_samples.size();
      gpu_stats.current_memory = (smoothed_mem / 1024.0f) / gpu_stats.total_memory_gb * 100.0f;
      gpu_stats.memory_history[history_offset] = gpu_stats.current_memory;
    }
  }

  void DrawCompactMonitor(GuiState & /*gui_state*/) {
    // ===== Top: System Info (horizontal compact layout) =====
    ImGui::BeginGroup();
    ImGui::Text("OS: %s | Kernel: %s | CPU: %d cores | RAM: %ld GB", 
                os_name.c_str(), kernel_version.c_str(), cpu_cores, total_ram_gb);
    if (gpu_stats.available) {
      ImGui::SameLine();
      ImGui::Text("| GPU: %s (%.1f GB)", gpu_stats.name.c_str(), gpu_stats.total_memory_gb);
    }
    ImGui::EndGroup();
    
    ImGui::Spacing();
    ImGui::Separator();
    ImGui::Spacing();
    
    // Two column layout: left = compact table, right = plots
    ImGui::Columns(2, "CPULayout", true);
    ImGui::SetColumnWidth(0, 180.0f);

    // ===== LEFT COLUMN: Compact Stats Table =====
    if (ImGui::BeginTable("StatsTable", 2, ImGuiTableFlags_Borders | ImGuiTableFlags_RowBg | ImGuiTableFlags_ScrollY, ImVec2(0, -1))) {
      ImGui::TableSetupColumn("Item", ImGuiTableColumnFlags_WidthFixed, 50.0f);
      ImGui::TableSetupColumn("%", ImGuiTableColumnFlags_WidthStretch);
      ImGui::TableHeadersRow();

      // CPU cores
      for (int i = 0; i < cpu_cores; ++i) {
        ImGui::TableNextRow();
        float usage = core_stats[i].history[(history_offset - 1 + HISTORY_SIZE) % HISTORY_SIZE];
        ImVec4 color = GetUsageColor(usage);

        ImGui::TableSetColumnIndex(0);
        ImGui::TextColored(color, "CPU%d", i);

        ImGui::TableSetColumnIndex(1);
        char buf[16];
        snprintf(buf, sizeof(buf), "%.1f%%", usage);
        ImGui::ProgressBar(usage / 100.0f, ImVec2(-1, 0), buf);
      }

      // Memory
      ImGui::TableNextRow();
      float mem_current = mem_history[(history_offset - 1 + HISTORY_SIZE) % HISTORY_SIZE];
      ImGui::TableSetColumnIndex(0);
      ImGui::TextColored(GetUsageColor(mem_current), "RAM");
      ImGui::TableSetColumnIndex(1);
      char buf[16];
      snprintf(buf, sizeof(buf), "%.1f%%", mem_current);
      ImGui::ProgressBar(mem_current / 100.0f, ImVec2(-1, 0), buf);

      // GPU
      if (gpu_stats.available) {
        ImGui::TableNextRow();
        ImGui::TableSetColumnIndex(0);
        ImGui::TextColored(GetUsageColor(gpu_stats.current_usage), "GPU");
        ImGui::TableSetColumnIndex(1);
        snprintf(buf, sizeof(buf), "%.1f%%", gpu_stats.current_usage);
        ImGui::ProgressBar(gpu_stats.current_usage / 100.0f, ImVec2(-1, 0), buf);

        ImGui::TableNextRow();
        ImGui::TableSetColumnIndex(0);
        ImGui::TextColored(GetUsageColor(gpu_stats.current_memory), "VRAM");
        ImGui::TableSetColumnIndex(1);
        snprintf(buf, sizeof(buf), "%.1f%%", gpu_stats.current_memory);
        ImGui::ProgressBar(gpu_stats.current_memory / 100.0f, ImVec2(-1, 0), buf);
      }

      ImGui::EndTable();
    }

    // ===== RIGHT COLUMN: Plots =====
    ImGui::NextColumn();

    // Top: Multi-line CPU plot (takes most space)
    float plot_height = gpu_stats.available ? 220.0f : 340.0f;
    if (ImPlot::BeginPlot("CPU Cores", ImVec2(-1, plot_height))) {
      ImPlot::SetupAxes("Time", "%", ImPlotAxisFlags_NoTickLabels, 0);
      ImPlot::SetupAxisLimits(ImAxis_X1, 0, HISTORY_SIZE, ImGuiCond_Always);
      ImPlot::SetupAxisLimits(ImAxis_Y1, 0, 100, ImGuiCond_Always);
      ImPlot::SetupLegend(ImPlotLocation_NorthEast, ImPlotLegendFlags_Outside);

      for (int i = 0; i < cpu_cores; ++i) {
        ImVec4 color = GetCoreColor(i, cpu_cores);
        char label[16];
        snprintf(label, sizeof(label), "CPU%d", i);
        ImPlot::PushStyleColor(ImPlotCol_Line, color);
        ImPlot::PlotLine(label, core_stats[i].history.data(), HISTORY_SIZE, 1.0, 0.0, 0, history_offset);
        ImPlot::PopStyleColor();
      }
      ImPlot::EndPlot();
    }

    // Bottom row: 3 plots side by side (Memory + GPU + VRAM)
    float bottom_height = 130.0f;
    float plot_width = gpu_stats.available ? ImGui::GetContentRegionAvail().x / 3.0f - 5 : ImGui::GetContentRegionAvail().x;

    // Memory plot
    ImGui::BeginGroup();
    if (ImPlot::BeginPlot("RAM", ImVec2(plot_width, bottom_height))) {
      ImPlot::SetupAxes("", "%", ImPlotAxisFlags_NoDecorations, 0);
      ImPlot::SetupAxisLimits(ImAxis_X1, 0, HISTORY_SIZE, ImGuiCond_Always);
      ImPlot::SetupAxisLimits(ImAxis_Y1, 0, 100, ImGuiCond_Always);
      ImPlot::PushStyleColor(ImPlotCol_Fill, ImVec4(0.2f, 0.6f, 1.0f, 0.3f));
      ImPlot::PlotShaded("", mem_history.data(), HISTORY_SIZE, 0.0, 1.0, 0.0, 0, history_offset);
      ImPlot::PopStyleColor();
      ImPlot::PushStyleColor(ImPlotCol_Line, ImVec4(0.2f, 0.6f, 1.0f, 1.0f));
      ImPlot::PlotLine("", mem_history.data(), HISTORY_SIZE, 1.0, 0.0, 0, history_offset);
      ImPlot::PopStyleColor();
      ImPlot::EndPlot();
    }
    ImGui::EndGroup();

    if (gpu_stats.available) {
      ImGui::SameLine();
      ImGui::BeginGroup();
      if (ImPlot::BeginPlot("GPU", ImVec2(plot_width, bottom_height))) {
        ImPlot::SetupAxes("", "%", ImPlotAxisFlags_NoDecorations, 0);
        ImPlot::SetupAxisLimits(ImAxis_X1, 0, HISTORY_SIZE, ImGuiCond_Always);
        ImPlot::SetupAxisLimits(ImAxis_Y1, 0, 100, ImGuiCond_Always);
        ImPlot::PushStyleColor(ImPlotCol_Fill, ImVec4(0.2f, 1.0f, 0.4f, 0.3f));
        ImPlot::PlotShaded("", gpu_stats.usage_history.data(), HISTORY_SIZE, 0.0, 1.0, 0.0, 0, history_offset);
        ImPlot::PopStyleColor();
        ImPlot::PushStyleColor(ImPlotCol_Line, ImVec4(0.2f, 1.0f, 0.4f, 1.0f));
        ImPlot::PlotLine("", gpu_stats.usage_history.data(), HISTORY_SIZE, 1.0, 0.0, 0, history_offset);
        ImPlot::PopStyleColor();
        ImPlot::EndPlot();
      }
      ImGui::EndGroup();

      ImGui::SameLine();
      ImGui::BeginGroup();
      if (ImPlot::BeginPlot("VRAM", ImVec2(plot_width, bottom_height))) {
        ImPlot::SetupAxes("", "%", ImPlotAxisFlags_NoDecorations, 0);
        ImPlot::SetupAxisLimits(ImAxis_X1, 0, HISTORY_SIZE, ImGuiCond_Always);
        ImPlot::SetupAxisLimits(ImAxis_Y1, 0, 100, ImGuiCond_Always);
        ImPlot::PushStyleColor(ImPlotCol_Fill, ImVec4(1.0f, 0.6f, 0.2f, 0.3f));
        ImPlot::PlotShaded("", gpu_stats.memory_history.data(), HISTORY_SIZE, 0.0, 1.0, 0.0, 0, history_offset);
        ImPlot::PopStyleColor();
        ImPlot::PushStyleColor(ImPlotCol_Line, ImVec4(1.0f, 0.6f, 0.2f, 1.0f));
        ImPlot::PlotLine("", gpu_stats.memory_history.data(), HISTORY_SIZE, 1.0, 0.0, 0, history_offset);
        ImPlot::PopStyleColor();
        ImPlot::EndPlot();
      }
      ImGui::EndGroup();
    }

    ImGui::Columns(1);
  }

  // Helper function to get color based on usage percentage
  ImVec4 GetUsageColor(float usage) {
    if (usage < 30.0f) {
      return ImVec4(0.2f, 1.0f, 0.3f, 1.0f); // Green
    } else if (usage < 60.0f) {
      return ImVec4(1.0f, 1.0f, 0.2f, 1.0f); // Yellow
    } else if (usage < 85.0f) {
      return ImVec4(1.0f, 0.6f, 0.2f, 1.0f); // Orange
    } else {
      return ImVec4(1.0f, 0.2f, 0.2f, 1.0f); // Red
    }
  }

  // Helper function to get distinct color for each CPU core
  ImVec4 GetCoreColor(int core_idx, int total_cores) {
    // Generate distinct colors using HSV color space
    float hue = (float)core_idx / (float)total_cores;
    float saturation = 0.8f;
    float value = 0.9f;

    // Convert HSV to RGB
    float h = hue * 6.0f;
    int i = (int)h;
    float f = h - (float)i;
    float p = value * (1.0f - saturation);
    float q = value * (1.0f - saturation * f);
    float t = value * (1.0f - saturation * (1.0f - f));

    switch (i % 6) {
    case 0:
      return ImVec4(value, t, p, 1.0f);
    case 1:
      return ImVec4(q, value, p, 1.0f);
    case 2:
      return ImVec4(p, value, t, 1.0f);
    case 3:
      return ImVec4(p, q, value, 1.0f);
    case 4:
      return ImVec4(t, p, value, 1.0f);
    case 5:
    default:
      return ImVec4(value, p, q, 1.0f);
    }
  }
};

// Factory function
IGuiTask *CreateSystemInfoTask() {
  return new SystemInfoTask();
}
