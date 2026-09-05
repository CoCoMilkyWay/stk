#include "gui/task_system_info/TaskSystemInfo.hpp"
#include "gui/Tasks.hpp"
#include "imgui.h"
#include "implot.h"
#include "misc/system_metrics.hpp"
#include "shared/SharedData.hpp"

#include <array>
#include <chrono>
#include <cstdio>
#include <span>
#include <string>
#include <vector>

// SystemInfo 面板: 静态硬件信息 + 动态负载曲线。
//
// 这里只负责画。所有探测与采样都在 misc/system_metrics —— 与左下角 IconBar
// 共用同一份数据, 面板关着的时候一次系统调用都不会发生。
namespace GUI::Tasks {
namespace {

namespace sysmon = misc::sysmon;

constexpr int HISTORY_SAMPLES = 100;
constexpr auto UPDATE_INTERVAL = std::chrono::milliseconds(100); // 曲线窗口 = 100 × 100ms = 10s
constexpr float PLOT_LABEL_X = 40.0f;
constexpr float PLOT_LABEL_Y = 95.0f;

const ImVec4 COLOR_LABEL = ImVec4(0.65f, 0.65f, 0.65f, 1.0f);
const ImVec4 COLOR_VALUE = ImVec4(0.95f, 0.95f, 0.95f, 1.0f);
const ImVec4 COLOR_DIM = ImVec4(0.50f, 0.50f, 0.50f, 1.0f);
const ImVec4 COLOR_COMMENT = ImVec4(0.50f, 0.70f, 0.90f, 1.0f);
const ImVec4 COLOR_INFO = ImVec4(0.50f, 0.80f, 1.00f, 1.0f);
const ImVec4 COLOR_WARNING = ImVec4(1.00f, 0.80f, 0.20f, 1.0f);
const ImVec4 COLOR_GOOD = ImVec4(0.20f, 1.00f, 0.30f, 1.0f);    // 支持 (标准扩展)
const ImVec4 COLOR_APPLE = ImVec4(1.00f, 0.90f, 0.20f, 1.0f);   // 支持 (厂商私有)
const ImVec4 COLOR_MISSING = ImVec4(0.30f, 0.30f, 0.30f, 1.0f); // 不支持

const ImVec4 COLOR_RAM = ImVec4(0.20f, 0.60f, 1.00f, 1.0f);
const ImVec4 COLOR_GPU = ImVec4(0.20f, 1.00f, 0.40f, 1.0f);
const ImVec4 COLOR_VRAM = ImVec4(1.00f, 0.60f, 0.20f, 1.0f);
const ImVec4 COLOR_RX = ImVec4(0.50f, 1.00f, 0.80f, 1.0f);
const ImVec4 COLOR_TX = ImVec4(1.00f, 0.80f, 0.50f, 1.0f);
const ImVec4 COLOR_READ = ImVec4(0.60f, 0.80f, 1.00f, 1.0f);
const ImVec4 COLOR_WRITE = ImVec4(1.00f, 0.70f, 0.60f, 1.0f);
const ImVec4 COLOR_BUSY = ImVec4(0.80f, 0.60f, 1.00f, 1.0f);

ImVec4 UsageColor(float percent) {
  if (percent < 30.0f)
    return ImVec4(0.2f, 1.0f, 0.3f, 1.0f);
  if (percent < 60.0f)
    return ImVec4(1.0f, 1.0f, 0.2f, 1.0f);
  if (percent < 85.0f)
    return ImVec4(1.0f, 0.6f, 0.2f, 1.0f);
  return ImVec4(1.0f, 0.2f, 0.2f, 1.0f);
}

// 按核序号在色相环上均匀取色, 保证 72 条曲线彼此可区分
ImVec4 CoreColor(int index, int total) {
  const float h = 6.0f * static_cast<float>(index) / static_cast<float>(total);
  const int sector = static_cast<int>(h) % 6;
  const float f = h - static_cast<float>(static_cast<int>(h));
  constexpr float v = 0.9f, s = 0.8f;
  const float p = v * (1.0f - s);
  const float q = v * (1.0f - s * f);
  const float t = v * (1.0f - s * (1.0f - f));
  switch (sector) {
  case 0:
    return ImVec4(v, t, p, 1.0f);
  case 1:
    return ImVec4(q, v, p, 1.0f);
  case 2:
    return ImVec4(p, v, t, 1.0f);
  case 3:
    return ImVec4(p, q, v, 1.0f);
  case 4:
    return ImVec4(t, p, v, 1.0f);
  default:
    return ImVec4(v, p, q, 1.0f);
  }
}

float GiB(std::size_t bytes) { return static_cast<float>(bytes) / (1024.0f * 1024.0f * 1024.0f); }

// 环形历史: 写指针全局共用一个, 所有曲线共享同一条时间轴
using History = std::array<float, HISTORY_SAMPLES>;

struct Series {
  const char *name = nullptr;
  const float *data = nullptr;
  ImVec4 color;
  bool shaded = false;
};

class SystemInfoTask {
public:
  SystemInfoTask() {
    const sysmon::StaticInfo &info = sysmon::Monitor::instance().info();
    cores_.resize(static_cast<std::size_t>(info.logical_cores));
    core_series_.reserve(cores_.size());
    for (std::size_t i = 0; i < cores_.size(); ++i) {
      char label[16];
      snprintf(label, sizeof(label), "CPU%zu", i);
      core_labels_.emplace_back(label);
    }
    for (std::size_t i = 0; i < cores_.size(); ++i) {
      core_series_.push_back({core_labels_[i].c_str(), cores_[i].data(),
                              CoreColor(static_cast<int>(i), info.logical_cores), false});
    }
  }

  const char *GetName() const { return "SystemInfo"; }

  // 面板重新打开时清空曲线: 关着的这段时间没有采样, 留着旧数据会被误读成"刚刚发生的"
  void OnExpand() {
    Reset();
    is_live_ = true;
  }
  void OnCollapse() { is_live_ = false; }

  // 任务行状态标签: 选中采样中 = live, 其余无 (关着不采样)
  TaskStatus Status(const SharedData & /*data*/, int /*idx*/) const {
    if (is_live_)
      return {TaskStatus::Kind::Ready, "live"};
    return {};
  }

  void Draw(SharedData & /*data*/, int /*idx*/) {
    sysmon::Monitor &monitor = sysmon::Monitor::instance();
    const auto now = std::chrono::steady_clock::now();
    if (now - last_update_ >= UPDATE_INTERVAL) {
      monitor.poll(sysmon::Scope::Full, UPDATE_INTERVAL);
      Record(monitor.sample());
      last_update_ = now;
    }

    RenderHardware(monitor.info());
    ImGui::Spacing();
    ImGui::Separator();
    ImGui::Spacing();
    RenderMonitoring(monitor.info(), monitor.sample());
  }

private:
  bool is_live_ = false; // 选中(展开)期间采样中, 左栏标 [live]

  // ==========================================================================
  // 历史数据
  // ==========================================================================
  void Reset() {
    for (History &h : cores_)
      h.fill(0.0f);
    for (History *h : {&mem_, &gpu_, &vram_, &net_rx_, &net_tx_, &disk_read_, &disk_write_, &disk_busy_})
      h->fill(0.0f);
    write_ = 0;
  }

  void Record(const sysmon::Sample &s) {
    for (std::size_t i = 0; i < cores_.size(); ++i)
      cores_[i][write_] = s.cpu_core_percent[i];
    mem_[write_] = s.mem_used_percent;
    gpu_[write_] = s.gpu_percent;
    vram_[write_] = s.vram_percent;
    net_rx_[write_] = s.net_rx_percent;
    net_tx_[write_] = s.net_tx_percent;
    disk_read_[write_] = s.disk_read_percent;
    disk_write_[write_] = s.disk_write_percent;
    disk_busy_[write_] = s.disk_busy_percent;
    write_ = (write_ + 1) % HISTORY_SAMPLES;
  }

  // ==========================================================================
  // 静态硬件信息
  // ==========================================================================
  static void Divider() {
    ImGui::SameLine(0, 8);
    ImGui::TextColored(COLOR_DIM, "│");
    ImGui::SameLine(0, 8);
  }

  static void Labeled(const char *label, const std::string &value) {
    ImGui::TextColored(COLOR_LABEL, "%s", label);
    ImGui::SameLine(0, 2);
    ImGui::TextColored(COLOR_VALUE, "%s", value.c_str());
  }

  void RenderHardware(const sysmon::StaticInfo &info) {
    ImGui::BeginGroup();

    // 第一行: OS / 主机名 / CPU 型号
    Labeled("OS:", info.os_name + " " + info.kernel_version);
    Divider();
    Labeled("Host:", info.hostname);
    Divider();
    Labeled("CPU:", info.cpu_model.empty() ? std::string("Unknown") : info.cpu_model);

    // 第二行: 核数 / 缓存 / 内存
    ImGui::TextColored(COLOR_LABEL, "     ");
    ImGui::SameLine(0, 2);
    ImGui::TextColored(COLOR_VALUE, "%s %s | %d Logical (%d Physical)",
                       info.arch_name.c_str(), info.cpu_vendor.c_str(),
                       info.logical_cores, info.physical_cores);
    if (info.cache_l1d_kb > 0 || info.cache_l2_kb > 0 || info.cache_l3_kb > 0) {
      Divider();
      char cache[96];
      snprintf(cache, sizeof(cache), "L1d:%ldK L2:%ldK L3:%ldK",
               info.cache_l1d_kb, info.cache_l2_kb, info.cache_l3_kb);
      Labeled("Cache:", cache);
    }
    Divider();
    char ram[32];
    snprintf(ram, sizeof(ram), "%.0fGB", GiB(info.ram_total_bytes));
    Labeled("RAM:", ram);

    // 第三行: GPU
    ImGui::TextColored(COLOR_LABEL, "GPU:");
    ImGui::SameLine(0, 2);
    RenderGpuIdentity(info);

    ImGui::Spacing();
    ImGui::Separator();
    ImGui::Spacing();

    ImGui::TextColored(COLOR_LABEL, "ISA:");
    ImGui::SameLine(0, 4);
    ImGui::TextColored(COLOR_COMMENT, "Instruction Set Architecture (x64 vs AArch64)");
    RenderIsa(info);

    ImGui::EndGroup();
  }

  static void RenderGpuIdentity(const sysmon::StaticInfo &info) {
    if (!info.gpu_probed) {
      ImGui::TextColored(COLOR_DIM, "probing...");
      return;
    }
    if (info.gpu_vendor == sysmon::GpuVendor::None) {
      ImGui::TextColored(COLOR_WARNING, "None");
      return;
    }

    ImVec4 color = COLOR_VALUE;
    const char *tag = "";
    switch (info.gpu_vendor) {
    case sysmon::GpuVendor::NVIDIA:
      color = ImVec4(0.3f, 0.9f, 0.3f, 1.0f);
      tag = "[NV]";
      break;
    case sysmon::GpuVendor::AMD:
      color = ImVec4(0.9f, 0.3f, 0.3f, 1.0f);
      tag = "[AMD]";
      break;
    case sysmon::GpuVendor::Intel:
      color = ImVec4(0.3f, 0.6f, 0.9f, 1.0f);
      tag = "[Intel]";
      break;
    case sysmon::GpuVendor::Apple:
      color = ImVec4(0.9f, 0.9f, 0.9f, 1.0f);
      tag = "[Apple]";
      break;
    default:
      break;
    }
    ImGui::TextColored(color, "%s", tag);
    ImGui::SameLine(0, 4);
    ImGui::TextColored(COLOR_VALUE, "%s", info.gpu_name.c_str());
    if (info.vram_available) {
      ImGui::SameLine(0, 4);
      ImGui::TextColored(COLOR_INFO, "(%.1fGB)", GiB(info.vram_total_bytes));
    }
    if (!info.gpu_usage_available) {
      ImGui::SameLine(0, 8);
      ImGui::TextColored(COLOR_WARNING, "⚠");
      ImGui::SameLine(0, 2);
      ImGui::TextColored(COLOR_INFO, "usage counter unavailable");
    }
  }

  static void RenderIsa(const sysmon::StaticInfo &info) {
    constexpr float CATEGORY_WIDTH = 70.0f;
    constexpr float COMMENT_WIDTH = 200.0f;
    constexpr float X64_POS = CATEGORY_WIDTH + COMMENT_WIDTH;
    constexpr float ARM_POS = X64_POS + 300.0f;

    const bool is_x64 = info.arch == sysmon::CpuArch::X86_64;
    const bool is_arm = info.arch == sysmon::CpuArch::AArch64;

    const auto features = [](std::span<const sysmon::IsaFeature> list, bool current_arch) {
      bool first = true;
      for (const sysmon::IsaFeature &f : list) {
        if (!first)
          ImGui::SameLine(0, 2);
        first = false;
        ImVec4 color = COLOR_MISSING;
        if (f.present && current_arch)
          color = f.proprietary ? COLOR_APPLE : COLOR_GOOD;
        ImGui::TextColored(color, "%s", f.name);
      }
    };

    for (const sysmon::IsaRow &row : info.isa) {
      const float start_x = ImGui::GetCursorPosX();
      ImGui::TextColored(COLOR_LABEL, "%s", row.category);
      ImGui::SameLine(start_x + CATEGORY_WIDTH);
      ImGui::TextColored(COLOR_COMMENT, "%s", row.comment);

      ImGui::SameLine(start_x + X64_POS);
      ImGui::TextColored(COLOR_LABEL, "x64:");
      ImGui::SameLine(0, 2);
      features(row.x64, is_x64);

      ImGui::SameLine(start_x + ARM_POS);
      ImGui::TextColored(COLOR_LABEL, "AArch64:");
      ImGui::SameLine(0, 2);
      features(row.arm, is_arm);
    }
  }

  // ==========================================================================
  // 动态监控
  // ==========================================================================
  void RenderMonitoring(const sysmon::StaticInfo &info, const sysmon::Sample &s) {
    ImGui::Columns(2, "MonitorLayout", true);
    ImGui::SetColumnWidth(0, 280.0f);
    RenderStats(info, s);
    ImGui::NextColumn();
    RenderPlots(info, s);
    ImGui::Columns(1);
  }

  static void StatRow(const char *name, bool available, float percent, const char *value) {
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    ImGui::TextColored(available ? UsageColor(percent) : COLOR_DIM, "%s", name);
    ImGui::TableSetColumnIndex(1);
    if (available) {
      char label[16];
      snprintf(label, sizeof(label), "%.1f%%", percent);
      ImGui::ProgressBar(percent * 0.01f, ImVec2(-1, 0), label);
    } else {
      ImGui::TextColored(COLOR_DIM, "N/A");
    }
    ImGui::TableSetColumnIndex(2);
    ImGui::Text("%s", value);
  }

  void RenderStats(const sysmon::StaticInfo &info, const sysmon::Sample &s) {
    const float height = ImGui::GetContentRegionAvail().y;
    ImGui::PushStyleVar(ImGuiStyleVar_CellPadding, ImVec2(4.0f, 0.0f));
    ImGui::PushStyleVar(ImGuiStyleVar_ItemSpacing, ImVec2(4.0f, 0.0f));

    if (ImGui::BeginTable("StatsTable", 3,
                          ImGuiTableFlags_Borders | ImGuiTableFlags_RowBg | ImGuiTableFlags_NoPadOuterX,
                          ImVec2(0, height))) {
      ImGui::TableSetupColumn("Item", ImGuiTableColumnFlags_WidthFixed, 60.0f);
      ImGui::TableSetupColumn("Usage", ImGuiTableColumnFlags_WidthStretch);
      ImGui::TableSetupColumn("Value", ImGuiTableColumnFlags_WidthFixed, 90.0f);
      ImGui::TableHeadersRow();

      char value[32];
      StatRow("CPU", true, s.cpu_total_percent, "all cores");
      for (std::size_t i = 0; i < cores_.size(); ++i)
        StatRow(core_labels_[i].c_str(), true, s.cpu_core_percent[i], "-");

      snprintf(value, sizeof(value), "%.1f/%.0fGB", GiB(s.mem_used_bytes), GiB(info.ram_total_bytes));
      StatRow("RAM", true, s.mem_used_percent, value);

      if (info.gpu_vendor != sysmon::GpuVendor::None) {
        StatRow("GPU", info.gpu_usage_available, s.gpu_percent, "-");
        snprintf(value, sizeof(value), "%.1f/%.1fGB", GiB(s.vram_used_bytes), GiB(info.vram_total_bytes));
        StatRow("VRAM", info.vram_available, s.vram_percent, info.vram_available ? value : "-");
      }

      snprintf(value, sizeof(value), "%.1f Mb/s", s.net_rx_mbps);
      StatRow("Net RX", info.net_available, s.net_rx_percent, value);
      snprintf(value, sizeof(value), "%.1f Mb/s", s.net_tx_mbps);
      StatRow("Net TX", info.net_available, s.net_tx_percent, value);

      snprintf(value, sizeof(value), "R:%.0f W:%.0f", s.disk_read_mbps, s.disk_write_mbps);
      StatRow("Disk", info.disk_available, s.disk_busy_percent, info.disk_available ? value : "-");

      ImGui::EndTable();
    }
    ImGui::PopStyleVar(2);
  }

  // 所有曲线统一是 0..100 的百分比, 角标写清楚 100% 对应的物理量
  void RenderPlot(const char *id, ImVec2 size, const char *corner, std::span<const Series> series) const {
    if (!ImPlot::BeginPlot(id, size))
      return;
    ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_NoDecorations, ImPlotAxisFlags_NoDecorations);
    ImPlot::SetupAxisLimits(ImAxis_X1, 0, HISTORY_SAMPLES, ImGuiCond_Always);
    ImPlot::SetupAxisLimits(ImAxis_Y1, 0, 100, ImGuiCond_Always);
    ImPlot::SetupLegend(ImPlotLocation_NorthWest, ImPlotLegendFlags_None);

    for (const Series &s : series) {
      if (s.shaded) {
        ImPlot::PushStyleColor(ImPlotCol_Fill, ImVec4(s.color.x, s.color.y, s.color.z, 0.3f));
        ImPlot::PlotShaded(s.name, s.data, HISTORY_SAMPLES, 0.0, 1.0, 0.0, 0, write_);
        ImPlot::PopStyleColor();
      }
      ImPlot::PushStyleColor(ImPlotCol_Line, s.color);
      ImPlot::PlotLine(s.name, s.data, HISTORY_SAMPLES, 1.0, 0.0, 0, write_);
      ImPlot::PopStyleColor();
    }

    ImPlot::PlotText(corner, PLOT_LABEL_X, PLOT_LABEL_Y);
    ImPlot::EndPlot();
  }

  void RenderPlots(const sysmon::StaticInfo &info, const sysmon::Sample &s) const {
    const float width = ImGui::GetContentRegionAvail().x;
    const float height = ImGui::GetContentRegionAvail().y;
    constexpr float MARGIN = 3.0f;

    RenderPlot("##CPUCores", ImVec2(width, height * 0.5f - MARGIN), "100%", core_series_);

    const bool show_gpu = info.gpu_usage_available;
    const bool show_vram = info.vram_available;
    const int count = 1 + (show_gpu ? 1 : 0) + (show_vram ? 1 : 0) +
                      (info.net_available ? 1 : 0) + (info.disk_available ? 1 : 0);
    const float small_w = (width - MARGIN * static_cast<float>(count - 1)) / static_cast<float>(count);
    const ImVec2 small(small_w, height * 0.5f - MARGIN);

    char corner[32];
    const Series ram[] = {{"RAM", mem_.data(), COLOR_RAM, true}};
    snprintf(corner, sizeof(corner), "%.0fGB", GiB(info.ram_total_bytes));
    RenderPlot("##RAM", small, corner, ram);

    if (show_gpu) {
      ImGui::SameLine();
      const Series gpu[] = {{"GPU", gpu_.data(), COLOR_GPU, true}};
      RenderPlot("##GPU", small, "100%", gpu);
    }
    if (show_vram) {
      ImGui::SameLine();
      const Series vram[] = {{"VRAM", vram_.data(), COLOR_VRAM, true}};
      snprintf(corner, sizeof(corner), "%.1fGB", GiB(info.vram_total_bytes));
      RenderPlot("##VRAM", small, corner, vram);
    }
    if (info.net_available) {
      ImGui::SameLine();
      const Series net[] = {{"RX", net_rx_.data(), COLOR_RX, false},
                            {"TX", net_tx_.data(), COLOR_TX, false}};
      snprintf(corner, sizeof(corner), "%.0fMb/s", s.net_scale_mbps);
      RenderPlot("##Network", small, corner, net);
    }
    if (info.disk_available) {
      ImGui::SameLine();
      const Series disk[] = {{"Busy", disk_busy_.data(), COLOR_BUSY, true},
                             {"R", disk_read_.data(), COLOR_READ, false},
                             {"W", disk_write_.data(), COLOR_WRITE, false}};
      snprintf(corner, sizeof(corner), "%.0fMB/s", s.disk_scale_mbps);
      RenderPlot("##DiskIO", small, corner, disk);
    }
  }

  // ==========================================================================
  std::vector<History> cores_;
  std::vector<std::string> core_labels_;
  std::vector<Series> core_series_;
  History mem_{}, gpu_{}, vram_{};
  History net_rx_{}, net_tx_{};
  History disk_read_{}, disk_write_{}, disk_busy_{};
  int write_ = 0;
  std::chrono::steady_clock::time_point last_update_{};
};

} // namespace

TaskHandle CreateSystemInfoTask() {
  auto instance = std::make_shared<SystemInfoTask>();

  TaskHandle handle;
  handle.name = instance->GetName();
  handle.storage = instance;
  handle.OnExpand = [instance]() { instance->OnExpand(); };
  handle.OnCollapse = [instance]() { instance->OnCollapse(); };
  handle.Status = [instance](const SharedData &data, int idx) { return instance->Status(data, idx); };
  handle.Draw = [instance](SharedData &data, int idx) { instance->Draw(data, idx); };
  handle.Destroy = [instance]() mutable { instance.reset(); };

  return handle;
}

} // namespace GUI::Tasks
