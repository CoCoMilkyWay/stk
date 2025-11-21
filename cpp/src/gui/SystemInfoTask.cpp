#include "gui/GuiState.hpp"
#include "gui/GuiTask.hpp"
#include "imgui.h"
#include "implot.h"
#include "shared/SharedData.hpp"
#include <array>
#include <chrono>
#include <cstring>
#include <deque>
#include <filesystem>
#include <fstream>
#include <set>
#include <sstream>
#include <sys/sysinfo.h>
#include <sys/utsname.h>
#include <thread>
#include <unistd.h>
#include <vector>

// SystemInfo task - display hardware and OS info with dynamic monitoring
class SystemInfoTask : public IGuiTask {
private:
  // ============================================================================
  // CONSTANTS
  // ============================================================================
  static constexpr int HISTORY_SAMPLES = 100;
  static constexpr int UPDATE_INTERVAL_MS = 100;
  static constexpr int GPU_UPDATE_INTERVAL_MS = 1000; // GPU queries are slow, update less frequently
  static constexpr int SMOOTH_WINDOW_SECONDS = 2;
  static constexpr int PLOT_LABEL_X_POS = 40;
  static constexpr int PLOT_LABEL_Y_POS = 95;

  // ============================================================================
  // STATIC HARDWARE INFO (initialized once)
  // ============================================================================
  struct HardwareInfo {
    // OS Info
    std::string os_name;
    std::string kernel_version;
    std::string hostname;
    bool is_wsl = false;

    // CPU Info
    int cpu_logical_cores = 0;
    int cpu_physical_cores = 0;
    long cpu_cache_l1_kb = 0;
    long cpu_cache_l2_kb = 0;
    long cpu_cache_l3_kb = 0;
    std::string cpu_vendor;
    std::string cpu_model;

    // CPU Instruction Sets
    bool cpu_has_sse = false;
    bool cpu_has_sse2 = false;
    bool cpu_has_sse3 = false;
    bool cpu_has_sse4_1 = false;
    bool cpu_has_sse4_2 = false;
    bool cpu_has_avx = false;
    bool cpu_has_avx2 = false;
    bool cpu_has_avx512 = false;
    bool cpu_has_fma = false;
    bool cpu_has_aes = false;

    // Memory Info
    long ram_total_gb = 0;

    // GPU Info
    enum class GPUVendor { None,
                           NVIDIA,
                           AMD,
                           Intel };
    GPUVendor gpu_vendor = GPUVendor::None;
    std::string gpu_name;
    float gpu_vram_total_gb = 0.0f;
    bool gpu_tool_available = false;
    std::string gpu_install_message;
  };
  HardwareInfo hw_info;

  // System tools detection
  struct SystemTools {
    bool has_lspci = false;
    bool has_dmidecode = false;
    bool has_nvidia_smi = false;
    bool has_radeontop = false;
    bool has_intel_gpu_top = false;
    std::vector<std::string> missing_tools;
  };
  SystemTools sys_tools;

  // ============================================================================
  // DYNAMIC MONITORING DATA (updated in loop)
  // ============================================================================

  // Per-core CPU monitoring
  struct CoreDynamicData {
    std::array<float, HISTORY_SAMPLES> usage_history = {};
    std::deque<std::pair<std::chrono::steady_clock::time_point, float>> usage_samples;
    long stat_total_prev = 0;
    long stat_idle_prev = 0;
  };
  std::vector<CoreDynamicData> cpu_cores_data;

  // Memory monitoring
  struct MemoryDynamicData {
    std::array<float, HISTORY_SAMPLES> usage_percent_history = {};
    float usage_percent_current = 0.0f;
    float used_gb_current = 0.0f;
  };
  MemoryDynamicData mem_data;

  // GPU monitoring
  struct GPUDynamicData {
    std::array<float, HISTORY_SAMPLES> usage_percent_history = {};
    std::array<float, HISTORY_SAMPLES> vram_percent_history = {};
    std::deque<std::pair<std::chrono::steady_clock::time_point, float>> usage_samples;
    std::deque<std::pair<std::chrono::steady_clock::time_point, float>> vram_samples;
    float usage_percent_current = 0.0f;
    float vram_percent_current = 0.0f;
    float vram_used_gb_current = 0.0f;
  };
  GPUDynamicData gpu_data;

  // Network monitoring
  struct NetworkDynamicData {
    std::array<float, HISTORY_SAMPLES> rx_percent_history = {};
    std::array<float, HISTORY_SAMPLES> tx_percent_history = {};
    long rx_bytes_prev = 0;
    long tx_bytes_prev = 0;
    float rx_mbps_current = 0.0f;
    float tx_mbps_current = 0.0f;
    float rx_percent_current = 0.0f;
    float tx_percent_current = 0.0f;
    long link_speed_mbps = 1000;
  };
  NetworkDynamicData net_data;

  // Disk IO monitoring
  struct DiskDynamicData {
    std::array<float, HISTORY_SAMPLES> busy_percent_history = {};
    std::array<float, HISTORY_SAMPLES> read_mbps_history = {};
    std::array<float, HISTORY_SAMPLES> write_mbps_history = {};
    long read_bytes_prev = 0;
    long write_bytes_prev = 0;
    long io_time_ms_prev = 0;
    float read_mbps_current = 0.0f;
    float write_mbps_current = 0.0f;
    float busy_percent_current = 0.0f;
    float read_mbps_max = 100.0f;
    float write_mbps_max = 100.0f;
  };
  DiskDynamicData disk_data;

  // ============================================================================
  // STATE MANAGEMENT
  // ============================================================================
  bool initialized = false;
  bool is_expanded = false;
  int history_write_index = 0;
  std::chrono::steady_clock::time_point last_update_time;
  std::chrono::steady_clock::time_point last_gpu_update_time;

public:
  // ============================================================================
  // INTERFACE IMPLEMENTATION
  // ============================================================================
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
      InitializeHardware();
      initialized = true;
    }
  }

  void OnCollapse() override {
    is_expanded = false;
  }

  void DrawPanel(SharedData & /*data*/, GuiState & /*gui_state*/) override {
    // One-time initialization
    if (!initialized) {
      InitializeHardware();
      initialized = true;
      last_update_time = std::chrono::steady_clock::now();
      last_gpu_update_time = last_update_time;
    }

    // Dynamic stats update (only when expanded)
    if (is_expanded) {
      auto now = std::chrono::steady_clock::now();
      auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_update_time).count();
      if (elapsed_ms >= UPDATE_INTERVAL_MS) {
        UpdateDynamicMonitoring();
        last_update_time = now;
      }
    }

    // Render UI
    RenderUI();
  }

private:
  // ============================================================================
  // INITIALIZATION (RUN ONCE)
  // ============================================================================

  void InitializeHardware() {
    DetectSystemTools();
    DetectOS();
    DetectCPU();
    DetectCPUInstructions();
    DetectMemory();
    DetectGPU();

    // Allocate dynamic data structures
    cpu_cores_data.resize(hw_info.cpu_logical_cores);
  }

  // ----------------------------------------------------------------------------
  // System Tools Detection
  // ----------------------------------------------------------------------------

  void DetectSystemTools() {
    auto check_tool = [](const char *tool_name) -> bool {
      std::string cmd = "which ";
      cmd += tool_name;
      cmd += " 2>/dev/null";
      FILE *pipe = popen(cmd.c_str(), "r");
      if (!pipe)
        return false;
      char buffer[256];
      bool found = (fgets(buffer, sizeof(buffer), pipe) != nullptr);
      pclose(pipe);
      return found;
    };

    sys_tools.has_lspci = check_tool("lspci");
    sys_tools.has_dmidecode = check_tool("dmidecode");
    sys_tools.has_nvidia_smi = check_tool("nvidia-smi");
    sys_tools.has_radeontop = check_tool("radeontop");
    sys_tools.has_intel_gpu_top = check_tool("intel_gpu_top");

    // Build missing tools list
    if (!sys_tools.has_lspci)
      sys_tools.missing_tools.push_back("lspci (install: sudo apt install pciutils)");
    if (!sys_tools.has_dmidecode)
      sys_tools.missing_tools.push_back("dmidecode (install: sudo apt install dmidecode)");
  }

  // ----------------------------------------------------------------------------
  // OS Detection
  // ----------------------------------------------------------------------------

  void DetectOS() {
    struct utsname uts;
    if (uname(&uts) == 0) {
      hw_info.os_name = uts.sysname;
      hw_info.kernel_version = uts.release;
      hw_info.hostname = uts.nodename;
    }

    // Check if running under WSL
    std::ifstream proc_version("/proc/version");
    if (proc_version.is_open()) {
      std::string line;
      std::getline(proc_version, line);
      hw_info.is_wsl = (line.find("microsoft") != std::string::npos ||
                        line.find("WSL") != std::string::npos);
    }
  }

  // ----------------------------------------------------------------------------
  // CPU Detection
  // ----------------------------------------------------------------------------

  void DetectCPU() {
    // Get logical and physical core counts
    hw_info.cpu_logical_cores = std::thread::hardware_concurrency();

    std::set<int> physical_core_ids;
    for (int i = 0; i < hw_info.cpu_logical_cores; ++i) {
      std::string path = "/sys/devices/system/cpu/cpu" + std::to_string(i) + "/topology/core_id";
      std::ifstream file(path);
      if (file.is_open()) {
        int core_id;
        file >> core_id;
        physical_core_ids.insert(core_id);
      }
    }
    hw_info.cpu_physical_cores = physical_core_ids.empty()
                                     ? hw_info.cpu_logical_cores
                                     : physical_core_ids.size();

    // Get CPU vendor and model from /proc/cpuinfo
    std::ifstream cpuinfo("/proc/cpuinfo");
    std::string line;
    while (std::getline(cpuinfo, line)) {
      if (line.find("vendor_id") == 0) {
        auto colon_pos = line.find(':');
        if (colon_pos != std::string::npos) {
          hw_info.cpu_vendor = line.substr(colon_pos + 2);
        }
      }
      if (line.find("model name") == 0) {
        auto colon_pos = line.find(':');
        if (colon_pos != std::string::npos) {
          hw_info.cpu_model = line.substr(colon_pos + 2);
        }
        break; // Only need first occurrence
      }
    }

    // Get cache sizes
    auto read_cache_size = [](const std::string &path) -> long {
      std::ifstream file(path);
      if (!file.is_open())
        return 0;

      std::string size_str;
      file >> size_str;
      if (size_str.empty())
        return 0;

      long size = std::stol(size_str);
      char unit = size_str.back();
      if (unit == 'K')
        return size;
      if (unit == 'M')
        return size * 1024;
      return size;
    };

    for (int idx = 0; idx < 10; ++idx) {
      std::string base = "/sys/devices/system/cpu/cpu0/cache/index" + std::to_string(idx);
      std::string level_path = base + "/level";
      std::string size_path = base + "/size";

      std::ifstream level_file(level_path);
      if (!level_file.is_open())
        break;

      int level;
      level_file >> level;
      long size = read_cache_size(size_path);

      if (level == 1 && hw_info.cpu_cache_l1_kb == 0) {
        hw_info.cpu_cache_l1_kb = size;
      } else if (level == 2 && hw_info.cpu_cache_l2_kb < size) {
        hw_info.cpu_cache_l2_kb = size;
      } else if (level == 3 && hw_info.cpu_cache_l3_kb < size) {
        hw_info.cpu_cache_l3_kb = size;
      }
    }
  }

  // ----------------------------------------------------------------------------
  // CPU Instruction Set Detection
  // ----------------------------------------------------------------------------

  void DetectCPUInstructions() {
    std::ifstream cpuinfo("/proc/cpuinfo");
    std::string line;

    while (std::getline(cpuinfo, line)) {
      if (line.find("flags") == 0 || line.find("Features") == 0) {
        // Convert to lowercase for easier matching
        for (auto &c : line)
          c = std::tolower(c);

        hw_info.cpu_has_sse = line.find(" sse ") != std::string::npos;
        hw_info.cpu_has_sse2 = line.find(" sse2 ") != std::string::npos;
        hw_info.cpu_has_sse3 = line.find(" sse3 ") != std::string::npos ||
                               line.find(" pni ") != std::string::npos;
        hw_info.cpu_has_sse4_1 = line.find(" sse4_1 ") != std::string::npos;
        hw_info.cpu_has_sse4_2 = line.find(" sse4_2 ") != std::string::npos;
        hw_info.cpu_has_avx = line.find(" avx ") != std::string::npos;
        hw_info.cpu_has_avx2 = line.find(" avx2 ") != std::string::npos;
        hw_info.cpu_has_avx512 = line.find(" avx512") != std::string::npos;
        hw_info.cpu_has_fma = line.find(" fma ") != std::string::npos;
        hw_info.cpu_has_aes = line.find(" aes ") != std::string::npos;
        break; // Only need first occurrence
      }
    }
  }

  // ----------------------------------------------------------------------------
  // Memory Detection
  // ----------------------------------------------------------------------------

  void DetectMemory() {
    struct sysinfo si;
    if (sysinfo(&si) == 0) {
      hw_info.ram_total_gb = si.totalram / (1024 * 1024 * 1024);
    }
  }

  // ----------------------------------------------------------------------------
  // GPU Detection
  // ----------------------------------------------------------------------------

  void DetectGPU() {
    // Detect GPU hardware presence
    bool has_nvidia = DetectNVIDIAHardware();
    bool has_amd = DetectAMDHardware();
    bool has_intel = DetectIntelHardware();

    // Initialize GPU based on what's found (priority: NVIDIA > AMD > Intel)
    if (has_nvidia) {
      InitializeNVIDIAGPU();
    } else if (has_amd) {
      InitializeAMDGPU();
    } else if (has_intel) {
      InitializeIntelGPU();
    } else {
      hw_info.gpu_vendor = HardwareInfo::GPUVendor::None;
      hw_info.gpu_name = "No GPU detected";
    }
  }

  bool DetectNVIDIAHardware() {
    if (std::filesystem::exists("/proc/driver/nvidia/version") ||
        std::filesystem::exists("/dev/nvidiactl")) {
      return true;
    }

    if (sys_tools.has_lspci) {
      FILE *pipe = popen("lspci 2>/dev/null | grep -i 'NVIDIA\\|GeForce'", "r");
      if (pipe) {
        char buffer[256];
        bool found = (fgets(buffer, sizeof(buffer), pipe) != nullptr);
        pclose(pipe);
        return found;
      }
    }

    return false;
  }

  bool DetectAMDHardware() {
    // Check sysfs vendor ID
    if (std::filesystem::exists("/sys/class/drm")) {
      for (const auto &entry : std::filesystem::directory_iterator("/sys/class/drm")) {
        std::string name = entry.path().filename().string();
        if (name.find("card") == 0 && name.find("-") == std::string::npos) {
          std::string vendor_path = entry.path().string() + "/device/vendor";
          std::ifstream vendor_file(vendor_path);
          if (vendor_file.is_open()) {
            std::string vendor_id;
            vendor_file >> vendor_id;
            if (vendor_id == "0x1002")
              return true;
          }
        }
      }
    }

    // Check lspci
    if (sys_tools.has_lspci) {
      FILE *pipe = popen("lspci 2>/dev/null | grep -i 'AMD\\|ATI\\|Radeon'", "r");
      if (pipe) {
        char buffer[256];
        bool found = (fgets(buffer, sizeof(buffer), pipe) != nullptr);
        pclose(pipe);
        return found;
      }
    }

    return false;
  }

  bool DetectIntelHardware() {
    // Check sysfs vendor ID
    if (std::filesystem::exists("/sys/class/drm")) {
      for (const auto &entry : std::filesystem::directory_iterator("/sys/class/drm")) {
        std::string name = entry.path().filename().string();
        if (name.find("card") == 0 && name.find("-") == std::string::npos) {
          std::string vendor_path = entry.path().string() + "/device/vendor";
          std::ifstream vendor_file(vendor_path);
          if (vendor_file.is_open()) {
            std::string vendor_id;
            vendor_file >> vendor_id;
            if (vendor_id == "0x8086")
              return true;
          }
        }
      }
    }

    // Check lspci
    if (sys_tools.has_lspci) {
      FILE *pipe = popen("lspci 2>/dev/null | grep -i 'VGA.*Intel\\|Display.*Intel'", "r");
      if (pipe) {
        char buffer[256];
        bool found = (fgets(buffer, sizeof(buffer), pipe) != nullptr);
        pclose(pipe);
        return found;
      }
    }

    return false;
  }

  void InitializeNVIDIAGPU() {
    hw_info.gpu_vendor = HardwareInfo::GPUVendor::NVIDIA;

    if (!sys_tools.has_nvidia_smi) {
      hw_info.gpu_name = "NVIDIA GPU (driver detected)";
      hw_info.gpu_tool_available = false;
      hw_info.gpu_install_message = "Install nvidia-smi: sudo apt install nvidia-utils";
      return;
    }

    FILE *pipe = popen("nvidia-smi --query-gpu=name,memory.total --format=csv,noheader,nounits 2>&1", "r");
    if (pipe) {
      char buffer[256];
      if (fgets(buffer, sizeof(buffer), pipe)) {
        std::string line(buffer);
        if (line.find("NVIDIA-SMI has failed") == std::string::npos &&
            line.find("command not found") == std::string::npos) {
          auto comma_pos = line.rfind(',');
          if (comma_pos != std::string::npos) {
            hw_info.gpu_name = line.substr(0, comma_pos);
            hw_info.gpu_name.erase(hw_info.gpu_name.find_last_not_of(" \n\r\t") + 1);
            hw_info.gpu_vram_total_gb = std::stof(line.substr(comma_pos + 1)) / 1024.0f;
            hw_info.gpu_tool_available = true;
          }
        } else {
          hw_info.gpu_name = "NVIDIA GPU (driver error)";
          hw_info.gpu_tool_available = false;
          hw_info.gpu_install_message = "nvidia-smi failed - reinstall NVIDIA drivers";
        }
      }
      pclose(pipe);
    }
  }

  void InitializeAMDGPU() {
    hw_info.gpu_vendor = HardwareInfo::GPUVendor::AMD;

    // Get GPU name
    ReadAMDGPUName();

    // Get VRAM size
    ReadAMDVRAMSize();

    // Check if monitoring tool is available
    if (sys_tools.has_radeontop && !hw_info.is_wsl) {
      hw_info.gpu_tool_available = true;
    } else {
      hw_info.gpu_tool_available = false;
      if (hw_info.is_wsl) {
        hw_info.gpu_install_message = "WSL: GPU monitoring limited - radeontop may not work";
      } else {
        hw_info.gpu_install_message = "Install radeontop: sudo apt install radeontop";
      }
    }
  }

  void InitializeIntelGPU() {
    hw_info.gpu_vendor = HardwareInfo::GPUVendor::Intel;

    // Get GPU name
    ReadIntelGPUName();

    // Check if monitoring tool is available
    if (sys_tools.has_intel_gpu_top) {
      hw_info.gpu_tool_available = true;
    } else {
      hw_info.gpu_tool_available = false;
      hw_info.gpu_install_message = "Install intel_gpu_top: sudo apt install intel-gpu-tools";
    }
  }

  void ReadAMDGPUName() {
    // Try lspci first
    if (sys_tools.has_lspci) {
      FILE *pipe = popen("lspci 2>/dev/null | grep -i 'VGA.*AMD\\|Display.*AMD\\|Radeon'", "r");
      if (pipe) {
        char buffer[256];
        if (fgets(buffer, sizeof(buffer), pipe)) {
          std::string line(buffer);
          auto pos = line.find(":");
          if (pos != std::string::npos && pos + 2 < line.length()) {
            hw_info.gpu_name = line.substr(pos + 2);
            hw_info.gpu_name.erase(hw_info.gpu_name.find_last_not_of(" \n\r\t") + 1);
            pclose(pipe);
            return;
          }
        }
        pclose(pipe);
      }
    }

    // Try sysfs
    if (std::filesystem::exists("/sys/class/drm")) {
      for (const auto &entry : std::filesystem::directory_iterator("/sys/class/drm")) {
        std::string name = entry.path().filename().string();
        if (name.find("card") == 0 && name.find("-") == std::string::npos) {
          std::string device_name_path = entry.path().string() + "/device/product_name";
          std::ifstream file(device_name_path);
          if (file.is_open()) {
            std::getline(file, hw_info.gpu_name);
            if (!hw_info.gpu_name.empty())
              return;
          }
        }
      }
    }

    hw_info.gpu_name = "AMD Radeon Graphics";
  }

  void ReadAMDVRAMSize() {
    if (!std::filesystem::exists("/sys/class/drm"))
      return;

    for (const auto &entry : std::filesystem::directory_iterator("/sys/class/drm")) {
      std::string name = entry.path().filename().string();
      if (name.find("card") != std::string::npos && name.find("-") == std::string::npos) {
        std::string vram_path = entry.path().string() + "/device/mem_info_vram_total";
        std::ifstream file(vram_path);
        if (file.is_open()) {
          long vram_bytes;
          file >> vram_bytes;
          hw_info.gpu_vram_total_gb = vram_bytes / (1024.0f * 1024.0f * 1024.0f);
          return;
        }
      }
    }
  }

  void ReadIntelGPUName() {
    // Try lspci first
    if (sys_tools.has_lspci) {
      FILE *pipe = popen("lspci 2>/dev/null | grep -i 'VGA.*Intel\\|Display.*Intel'", "r");
      if (pipe) {
        char buffer[256];
        if (fgets(buffer, sizeof(buffer), pipe)) {
          std::string line(buffer);
          auto pos = line.find("Intel");
          if (pos != std::string::npos) {
            hw_info.gpu_name = line.substr(pos);
            hw_info.gpu_name.erase(hw_info.gpu_name.find_last_not_of(" \n\r\t") + 1);
            pclose(pipe);
            return;
          }
        }
        pclose(pipe);
      }
    }

    hw_info.gpu_name = "Intel Integrated Graphics";
  }

  // ============================================================================
  // DYNAMIC MONITORING (RUN IN LOOP)
  // ============================================================================

  void UpdateDynamicMonitoring() {
    auto now = std::chrono::steady_clock::now();

    UpdateCPUUsage(now);
    UpdateMemoryUsage();
    UpdateGPUUsage(now);
    UpdateNetworkUsage();
    UpdateDiskUsage();

    history_write_index = (history_write_index + 1) % HISTORY_SAMPLES;
  }

  // ----------------------------------------------------------------------------
  // CPU Usage Update
  // ----------------------------------------------------------------------------

  void UpdateCPUUsage(std::chrono::steady_clock::time_point now) {
    std::ifstream stat_file("/proc/stat");
    std::string line;

    // Skip aggregate CPU stats
    std::getline(stat_file, line);

    // Read per-core stats
    for (int i = 0; i < hw_info.cpu_logical_cores && std::getline(stat_file, line); ++i) {
      if (line.find("cpu") != 0)
        break;

      std::istringstream ss(line);
      std::string cpu_label;
      long user, nice, system, idle, iowait, irq, softirq;
      ss >> cpu_label >> user >> nice >> system >> idle >> iowait >> irq >> softirq;

      long total = user + nice + system + idle + iowait + irq + softirq;
      long total_diff = total - cpu_cores_data[i].stat_total_prev;
      long idle_diff = idle - cpu_cores_data[i].stat_idle_prev;

      cpu_cores_data[i].stat_total_prev = total;
      cpu_cores_data[i].stat_idle_prev = idle;

      float usage = 0.0f;
      if (total_diff > 0) {
        usage = 100.0f * (1.0f - (float)idle_diff / (float)total_diff);
      }

      // Smooth over time window
      cpu_cores_data[i].usage_samples.push_back({now, usage});
      auto cutoff = now - std::chrono::seconds(SMOOTH_WINDOW_SECONDS);
      while (!cpu_cores_data[i].usage_samples.empty() &&
             cpu_cores_data[i].usage_samples.front().first < cutoff) {
        cpu_cores_data[i].usage_samples.pop_front();
      }

      float smoothed = 0.0f;
      if (!cpu_cores_data[i].usage_samples.empty()) {
        for (const auto &[t, v] : cpu_cores_data[i].usage_samples) {
          smoothed += v;
        }
        smoothed /= cpu_cores_data[i].usage_samples.size();
      }

      cpu_cores_data[i].usage_history[history_write_index] = smoothed;
    }
  }

  // ----------------------------------------------------------------------------
  // Memory Usage Update
  // ----------------------------------------------------------------------------

  void UpdateMemoryUsage() {
    struct sysinfo si;
    if (sysinfo(&si) == 0) {
      long used_ram = si.totalram - si.freeram;
      mem_data.used_gb_current = used_ram / (1024.0f * 1024.0f * 1024.0f);
      mem_data.usage_percent_current = 100.0f * (1.0f - (float)si.freeram / (float)si.totalram);
      mem_data.usage_percent_history[history_write_index] = mem_data.usage_percent_current;
    }
  }

  // ----------------------------------------------------------------------------
  // GPU Usage Update
  // ----------------------------------------------------------------------------

  void UpdateGPUUsage(std::chrono::steady_clock::time_point now) {
    // GPU queries are slow (external commands), update less frequently
    auto gpu_elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_gpu_update_time).count();
    if (gpu_elapsed_ms < GPU_UPDATE_INTERVAL_MS) {
      return;
    }
    last_gpu_update_time = now;

    if (hw_info.gpu_vendor == HardwareInfo::GPUVendor::None || !hw_info.gpu_tool_available) {
      return;
    }

    float usage = 0.0f;
    float memory_used_mb = 0.0f;

    if (hw_info.gpu_vendor == HardwareInfo::GPUVendor::NVIDIA) {
      UpdateNVIDIAUsage(usage, memory_used_mb);
    } else if (hw_info.gpu_vendor == HardwareInfo::GPUVendor::AMD) {
      UpdateAMDUsage(usage, memory_used_mb);
    } else if (hw_info.gpu_vendor == HardwareInfo::GPUVendor::Intel) {
      UpdateIntelUsage(usage, memory_used_mb);
    }

    // Smooth usage over time window
    gpu_data.usage_samples.push_back({now, usage});
    gpu_data.vram_samples.push_back({now, memory_used_mb});

    auto cutoff = now - std::chrono::seconds(SMOOTH_WINDOW_SECONDS);
    while (!gpu_data.usage_samples.empty() && gpu_data.usage_samples.front().first < cutoff) {
      gpu_data.usage_samples.pop_front();
    }
    while (!gpu_data.vram_samples.empty() && gpu_data.vram_samples.front().first < cutoff) {
      gpu_data.vram_samples.pop_front();
    }

    // Calculate smoothed values
    if (!gpu_data.usage_samples.empty()) {
      float sum = 0.0f;
      for (const auto &[t, v] : gpu_data.usage_samples)
        sum += v;
      gpu_data.usage_percent_current = sum / gpu_data.usage_samples.size();
      gpu_data.usage_percent_history[history_write_index] = gpu_data.usage_percent_current;
    }

    if (!gpu_data.vram_samples.empty()) {
      float sum = 0.0f;
      for (const auto &[t, v] : gpu_data.vram_samples)
        sum += v;
      float smoothed_mem_mb = sum / gpu_data.vram_samples.size();
      gpu_data.vram_used_gb_current = smoothed_mem_mb / 1024.0f;
      if (hw_info.gpu_vram_total_gb > 0) {
        gpu_data.vram_percent_current = (gpu_data.vram_used_gb_current / hw_info.gpu_vram_total_gb) * 100.0f;
      }
      gpu_data.vram_percent_history[history_write_index] = gpu_data.vram_percent_current;
    }
  }

  void UpdateNVIDIAUsage(float &usage, float &memory_used_mb) {
    FILE *pipe = popen("nvidia-smi --query-gpu=utilization.gpu,memory.used --format=csv,noheader,nounits 2>/dev/null", "r");
    if (pipe) {
      char buffer[128];
      if (fgets(buffer, sizeof(buffer), pipe)) {
        std::istringstream ss(buffer);
        std::string token;
        if (std::getline(ss, token, ','))
          usage = std::stof(token);
        if (std::getline(ss, token, ','))
          memory_used_mb = std::stof(token);
      }
      pclose(pipe);
    }
  }

  void UpdateAMDUsage(float &usage, float &memory_used_mb) {
    // Try sysfs for VRAM usage
    if (std::filesystem::exists("/sys/class/drm")) {
      for (const auto &entry : std::filesystem::directory_iterator("/sys/class/drm")) {
        std::string name = entry.path().filename().string();
        if (name.find("card") != std::string::npos && name.find("-") == std::string::npos) {
          std::string vram_used_path = entry.path().string() + "/device/mem_info_vram_used";
          std::ifstream file(vram_used_path);
          if (file.is_open()) {
            long vram_used_bytes;
            file >> vram_used_bytes;
            memory_used_mb = vram_used_bytes / (1024.0f * 1024.0f);
            break;
          }
        }
      }
    }

    // Try radeontop for usage (less reliable, so optional)
    FILE *pipe = popen("timeout 0.1 radeontop -d - -l 1 2>/dev/null | grep -oP 'gpu \\K[0-9.]+'", "r");
    if (pipe) {
      char buffer[64];
      if (fgets(buffer, sizeof(buffer), pipe)) {
        usage = std::stof(buffer);
      }
      pclose(pipe);
    }
  }

  void UpdateIntelUsage(float &usage, float &memory_used_mb) {
    (void)usage;
    (void)memory_used_mb;
    // Intel GPU monitoring typically requires root access for intel_gpu_top
    // For now, leave unimplemented or try sysfs if available
  }

  // ----------------------------------------------------------------------------
  // Network Usage Update
  // ----------------------------------------------------------------------------

  void UpdateNetworkUsage() {
    long total_rx = 0;
    long total_tx = 0;

    std::ifstream net_file("/proc/net/dev");
    std::string line;

    // Skip header lines
    std::getline(net_file, line);
    std::getline(net_file, line);

    while (std::getline(net_file, line)) {
      auto colon_pos = line.find(':');
      if (colon_pos == std::string::npos)
        continue;

      std::string iface = line.substr(0, colon_pos);
      iface.erase(0, iface.find_first_not_of(" \t"));

      if (iface == "lo")
        continue; // Skip loopback

      std::istringstream ss(line.substr(colon_pos + 1));
      long rx_bytes, rx_packets, rx_errs, rx_drop, rx_fifo, rx_frame, rx_compressed, rx_multicast;
      long tx_bytes, tx_packets, tx_errs, tx_drop, tx_fifo, tx_colls, tx_carrier, tx_compressed;

      ss >> rx_bytes >> rx_packets >> rx_errs >> rx_drop >> rx_fifo >> rx_frame >> rx_compressed >> rx_multicast >> tx_bytes >> tx_packets >> tx_errs >> tx_drop >> tx_fifo >> tx_colls >> tx_carrier >> tx_compressed;

      total_rx += rx_bytes;
      total_tx += tx_bytes;
    }

    // Calculate speed (bytes per 100ms -> Mbps)
    if (net_data.rx_bytes_prev > 0) {
      long rx_diff = total_rx - net_data.rx_bytes_prev;
      long tx_diff = total_tx - net_data.tx_bytes_prev;

      net_data.rx_mbps_current = (rx_diff * 10.0f * 8.0f) / (1024.0f * 1024.0f);
      net_data.tx_mbps_current = (tx_diff * 10.0f * 8.0f) / (1024.0f * 1024.0f);

      // Use practical max of 80 Mbps (10 MB/s) for utilization percentage
      constexpr float PRACTICAL_MAX_MBPS = 80.0f;
      net_data.rx_percent_current = std::min(100.0f, (net_data.rx_mbps_current / PRACTICAL_MAX_MBPS) * 100.0f);
      net_data.tx_percent_current = std::min(100.0f, (net_data.tx_mbps_current / PRACTICAL_MAX_MBPS) * 100.0f);
    }

    net_data.rx_bytes_prev = total_rx;
    net_data.tx_bytes_prev = total_tx;
    net_data.rx_percent_history[history_write_index] = net_data.rx_percent_current;
    net_data.tx_percent_history[history_write_index] = net_data.tx_percent_current;
  }

  // ----------------------------------------------------------------------------
  // Disk Usage Update
  // ----------------------------------------------------------------------------

  void UpdateDiskUsage() {
    long total_read = 0;
    long total_write = 0;
    long total_io_time = 0;

    std::ifstream disk_file("/proc/diskstats");
    std::string line;

    while (std::getline(disk_file, line)) {
      std::istringstream ss(line);
      int major, minor;
      std::string device;
      long reads_completed, reads_merged, sectors_read, time_reading;
      long writes_completed, writes_merged, sectors_written, time_writing;
      long ios_in_progress, time_io, weighted_time_io;

      ss >> major >> minor >> device >> reads_completed >> reads_merged >> sectors_read >> time_reading >> writes_completed >> writes_merged >> sectors_written >> time_writing >> ios_in_progress >> time_io >> weighted_time_io;

      // Only count physical disks
      if (device.find("sd") == 0 || device.find("nvme") == 0 || device.find("vd") == 0) {
        // Skip partitions
        bool is_partition = false;
        if (device.find("nvme") == 0) {
          if (device.find("p") != std::string::npos)
            is_partition = true;
        } else {
          for (char c : device) {
            if (std::isdigit(c)) {
              is_partition = true;
              break;
            }
          }
        }

        if (!is_partition) {
          total_read += sectors_read * 512;
          total_write += sectors_written * 512;
          total_io_time += time_io;
        }
      }
    }

    // Calculate speed (bytes per 100ms -> MB/s)
    if (disk_data.read_bytes_prev > 0) {
      long read_diff = total_read - disk_data.read_bytes_prev;
      long write_diff = total_write - disk_data.write_bytes_prev;
      long io_time_diff = total_io_time - disk_data.io_time_ms_prev;

      disk_data.read_mbps_current = (read_diff * 10.0f) / (1024.0f * 1024.0f);
      disk_data.write_mbps_current = (write_diff * 10.0f) / (1024.0f * 1024.0f);
      disk_data.busy_percent_current = std::min(100.0f, (io_time_diff * 0.01f) * 100.0f);

      // Update historical max
      disk_data.read_mbps_max = std::max(disk_data.read_mbps_max * 0.995f, disk_data.read_mbps_current);
      disk_data.write_mbps_max = std::max(disk_data.write_mbps_max * 0.995f, disk_data.write_mbps_current);
      disk_data.read_mbps_max = std::max(10.0f, disk_data.read_mbps_max);
      disk_data.write_mbps_max = std::max(10.0f, disk_data.write_mbps_max);
    }

    disk_data.read_bytes_prev = total_read;
    disk_data.write_bytes_prev = total_write;
    disk_data.io_time_ms_prev = total_io_time;
    disk_data.busy_percent_history[history_write_index] = disk_data.busy_percent_current;
    disk_data.read_mbps_history[history_write_index] = disk_data.read_mbps_current;
    disk_data.write_mbps_history[history_write_index] = disk_data.write_mbps_current;
  }

  // ============================================================================
  // UI RENDERING
  // ============================================================================

  void RenderUI() {
    RenderStaticHardwareInfo();
    ImGui::Spacing();
    ImGui::Separator();
    ImGui::Spacing();
    RenderDynamicMonitoring();
  }

  // ----------------------------------------------------------------------------
  // Static Hardware Info Rendering
  // ----------------------------------------------------------------------------

  void RenderStaticHardwareInfo() {
    const ImVec4 COLOR_LABEL = ImVec4(0.65f, 0.65f, 0.65f, 1.0f);
    const ImVec4 COLOR_VALUE = ImVec4(0.95f, 0.95f, 0.95f, 1.0f);
    const ImVec4 COLOR_GOOD = ImVec4(0.2f, 1.0f, 0.3f, 1.0f);
    const ImVec4 COLOR_WARNING = ImVec4(1.0f, 0.8f, 0.2f, 1.0f);
    const ImVec4 COLOR_INFO = ImVec4(0.5f, 0.8f, 1.0f, 1.0f);
    const ImVec4 COLOR_DIM = ImVec4(0.5f, 0.5f, 0.5f, 1.0f);

    ImGui::BeginGroup();

    // ========== LINE 1: System & CPU ==========
    // OS
    ImGui::TextColored(COLOR_LABEL, "OS:");
    ImGui::SameLine(0, 2);
    ImGui::TextColored(COLOR_VALUE, "%s %s", hw_info.os_name.c_str(), hw_info.kernel_version.c_str());
    ImGui::SameLine(0, 8);
    ImGui::TextColored(COLOR_DIM, "│");
    ImGui::SameLine(0, 8);
    ImGui::TextColored(COLOR_LABEL, "Host:");
    ImGui::SameLine(0, 2);
    ImGui::TextColored(COLOR_VALUE, "%s", hw_info.hostname.c_str());
    if (hw_info.is_wsl) {
      ImGui::SameLine(0, 4);
      ImGui::TextColored(COLOR_INFO, "[WSL]");
    }

    // CPU Model
    ImGui::SameLine(0, 8);
    ImGui::TextColored(COLOR_DIM, "│");
    ImGui::SameLine(0, 8);
    ImGui::TextColored(COLOR_LABEL, "CPU:");
    ImGui::SameLine(0, 2);
    if (!hw_info.cpu_model.empty()) {
      // Shorten CPU model name if too long
      std::string cpu_name = hw_info.cpu_model;
      if (cpu_name.length() > 50) {
        cpu_name = cpu_name.substr(0, 47) + "...";
      }
      ImGui::TextColored(COLOR_VALUE, "%s", cpu_name.c_str());
    } else {
      ImGui::TextColored(COLOR_VALUE, "Unknown CPU");
    }

    // ========== LINE 2: CPU Details & RAM ==========
    // Cores
    ImGui::TextColored(COLOR_LABEL, "     ");
    ImGui::SameLine(0, 2);
    ImGui::TextColored(COLOR_VALUE, "%dLogical(%dPhysical)", hw_info.cpu_logical_cores, hw_info.cpu_physical_cores);

    // Cache
    if (hw_info.cpu_cache_l1_kb > 0 || hw_info.cpu_cache_l2_kb > 0 || hw_info.cpu_cache_l3_kb > 0) {
      ImGui::SameLine(0, 8);
      ImGui::TextColored(COLOR_DIM, "│");
      ImGui::SameLine(0, 8);
      ImGui::TextColored(COLOR_LABEL, "Cache:");
      ImGui::SameLine(0, 2);
      bool first = true;
      char cache_buf[64] = "";
      if (hw_info.cpu_cache_l1_kb > 0) {
        snprintf(cache_buf + strlen(cache_buf), sizeof(cache_buf) - strlen(cache_buf), "L1:%ldK", hw_info.cpu_cache_l1_kb);
        first = false;
      }
      if (hw_info.cpu_cache_l2_kb > 0) {
        if (!first)
          snprintf(cache_buf + strlen(cache_buf), sizeof(cache_buf) - strlen(cache_buf), " ");
        snprintf(cache_buf + strlen(cache_buf), sizeof(cache_buf) - strlen(cache_buf), "L2:%ldK", hw_info.cpu_cache_l2_kb);
        first = false;
      }
      if (hw_info.cpu_cache_l3_kb > 0) {
        if (!first)
          snprintf(cache_buf + strlen(cache_buf), sizeof(cache_buf) - strlen(cache_buf), " ");
        snprintf(cache_buf + strlen(cache_buf), sizeof(cache_buf) - strlen(cache_buf), "L3:%ldK", hw_info.cpu_cache_l3_kb);
      }
      ImGui::TextColored(COLOR_VALUE, "%s", cache_buf);
    }

    // ISA
    ImGui::SameLine(0, 8);
    ImGui::TextColored(COLOR_DIM, "│");
    ImGui::SameLine(0, 8);
    ImGui::TextColored(COLOR_LABEL, "ISA:");
    ImGui::SameLine(0, 2);

    bool first_isa = true;
    if (hw_info.cpu_has_sse4_2) {
      if (!first_isa) {
        ImGui::SameLine(0, 2);
      }
      ImGui::TextColored(COLOR_GOOD, "SSE4.2");
      first_isa = false;
    }
    if (hw_info.cpu_has_avx) {
      if (!first_isa) {
        ImGui::SameLine(0, 2);
      } else {
        first_isa = false;
      }
      ImGui::TextColored(COLOR_GOOD, "AVX");
    }
    if (hw_info.cpu_has_avx2) {
      ImGui::SameLine(0, 2);
      ImGui::TextColored(COLOR_GOOD, "AVX2");
    } else if (hw_info.cpu_has_avx) {
      ImGui::SameLine(0, 2);
      ImGui::TextColored(COLOR_WARNING, "AVX2✗");
    }
    if (hw_info.cpu_has_avx512) {
      ImGui::SameLine(0, 2);
      ImGui::TextColored(COLOR_GOOD, "AVX512");
    }
    if (hw_info.cpu_has_fma) {
      ImGui::SameLine(0, 2);
      ImGui::TextColored(COLOR_GOOD, "FMA");
    }
    if (hw_info.cpu_has_aes) {
      ImGui::SameLine(0, 2);
      ImGui::TextColored(COLOR_GOOD, "AES");
    }
    if (first_isa) {
      ImGui::TextColored(COLOR_WARNING, "Limited");
    }

    // RAM
    ImGui::SameLine(0, 8);
    ImGui::TextColored(COLOR_DIM, "│");
    ImGui::SameLine(0, 8);
    ImGui::TextColored(COLOR_LABEL, "RAM:");
    ImGui::SameLine(0, 2);
    ImGui::TextColored(COLOR_VALUE, "%ldGB", hw_info.ram_total_gb);

    // ========== LINE 3: GPU & Tools Status ==========
    ImGui::TextColored(COLOR_LABEL, "GPU:");
    ImGui::SameLine(0, 2);

    if (hw_info.gpu_vendor != HardwareInfo::GPUVendor::None) {
      ImVec4 gpu_color = COLOR_VALUE;
      const char *vendor_tag = "";
      if (hw_info.gpu_vendor == HardwareInfo::GPUVendor::NVIDIA) {
        gpu_color = ImVec4(0.3f, 0.9f, 0.3f, 1.0f);
        vendor_tag = "[NV]";
      } else if (hw_info.gpu_vendor == HardwareInfo::GPUVendor::AMD) {
        gpu_color = ImVec4(0.9f, 0.3f, 0.3f, 1.0f);
        vendor_tag = "[AMD]";
      } else if (hw_info.gpu_vendor == HardwareInfo::GPUVendor::Intel) {
        gpu_color = ImVec4(0.3f, 0.6f, 0.9f, 1.0f);
        vendor_tag = "[Intel]";
      }

      ImGui::TextColored(gpu_color, "%s", vendor_tag);
      ImGui::SameLine(0, 4);

      // Shorten GPU name if needed
      std::string gpu_name = hw_info.gpu_name;
      if (gpu_name.length() > 60) {
        gpu_name = gpu_name.substr(0, 57) + "...";
      }
      ImGui::TextColored(COLOR_VALUE, "%s", gpu_name.c_str());

      if (hw_info.gpu_vram_total_gb > 0) {
        ImGui::SameLine(0, 4);
        ImGui::TextColored(COLOR_INFO, "(%.1fGB)", hw_info.gpu_vram_total_gb);
      }

      if (!hw_info.gpu_tool_available && !hw_info.gpu_install_message.empty()) {
        ImGui::SameLine(0, 8);
        ImGui::TextColored(COLOR_WARNING, "⚠");
        ImGui::SameLine(0, 2);
        ImGui::TextColored(COLOR_INFO, "%s", hw_info.gpu_install_message.c_str());
      }
    } else {
      ImGui::TextColored(COLOR_WARNING, "None");
      if (hw_info.is_wsl) {
        ImGui::SameLine(0, 8);
        ImGui::TextColored(COLOR_INFO, "(WSL: need Windows drivers + kernel≥5.10.43.3)");
      }
    }

    // System Tools
    if (!sys_tools.missing_tools.empty()) {
      ImGui::SameLine(0, 8);
      ImGui::TextColored(COLOR_DIM, "│");
      ImGui::SameLine(0, 8);
      ImGui::TextColored(COLOR_WARNING, "⚙");
      ImGui::SameLine(0, 2);
      ImGui::TextColored(COLOR_INFO, "%zu tools missing:", sys_tools.missing_tools.size());
      ImGui::SameLine(0, 4);
      for (size_t i = 0; i < sys_tools.missing_tools.size(); ++i) {
        if (i > 0) {
          ImGui::SameLine(0, 2);
          ImGui::TextColored(COLOR_DIM, ",");
          ImGui::SameLine(0, 2);
        }
        // Extract just the tool name without install command
        std::string tool = sys_tools.missing_tools[i];
        size_t paren_pos = tool.find(" (");
        if (paren_pos != std::string::npos) {
          tool = tool.substr(0, paren_pos);
        }
        ImGui::TextColored(COLOR_INFO, "%s", tool.c_str());
      }
    }

    ImGui::EndGroup();
  }

  // ----------------------------------------------------------------------------
  // Dynamic Monitoring Rendering
  // ----------------------------------------------------------------------------

  void RenderDynamicMonitoring() {
    ImGui::Columns(2, "MonitorLayout", true);
    ImGui::SetColumnWidth(0, 280.0f);

    // Left column: stats table
    RenderStatsTable();

    // Right column: plots
    ImGui::NextColumn();
    RenderPlots();

    ImGui::Columns(1);
  }

  void RenderStatsTable() {
    float table_height = ImGui::GetContentRegionAvail().y;
    ImGui::PushStyleVar(ImGuiStyleVar_CellPadding, ImVec2(4.0f, 0.0f));
    ImGui::PushStyleVar(ImGuiStyleVar_ItemSpacing, ImVec2(4.0f, 0.0f));

    if (ImGui::BeginTable("StatsTable", 3,
                          ImGuiTableFlags_Borders | ImGuiTableFlags_RowBg | ImGuiTableFlags_NoPadOuterX,
                          ImVec2(0, table_height))) {
      ImGui::TableSetupColumn("Item", ImGuiTableColumnFlags_WidthFixed, 60.0f);
      ImGui::TableSetupColumn("Usage", ImGuiTableColumnFlags_WidthStretch);
      ImGui::TableSetupColumn("Value", ImGuiTableColumnFlags_WidthFixed, 90.0f);
      ImGui::TableHeadersRow();

      // CPU cores
      for (int i = 0; i < hw_info.cpu_logical_cores; ++i) {
        ImGui::TableNextRow();
        float usage = cpu_cores_data[i].usage_history[(history_write_index - 1 + HISTORY_SAMPLES) % HISTORY_SAMPLES];

        ImGui::TableSetColumnIndex(0);
        ImGui::TextColored(ColorFromUsagePercent(usage), "CPU%d", i);

        ImGui::TableSetColumnIndex(1);
        char buf[32];
        snprintf(buf, sizeof(buf), "%.1f%%", usage);
        ImGui::ProgressBar(usage * 0.01f, ImVec2(-1, 0), buf);

        ImGui::TableSetColumnIndex(2);
        ImGui::Text("-");
      }

      // Memory
      ImGui::TableNextRow();
      ImGui::TableSetColumnIndex(0);
      ImGui::TextColored(ColorFromUsagePercent(mem_data.usage_percent_current), "RAM");
      ImGui::TableSetColumnIndex(1);
      char buf[32];
      snprintf(buf, sizeof(buf), "%.1f%%", mem_data.usage_percent_current);
      ImGui::ProgressBar(mem_data.usage_percent_current * 0.01f, ImVec2(-1, 0), buf);
      ImGui::TableSetColumnIndex(2);
      snprintf(buf, sizeof(buf), "%.1f/%ldGB", mem_data.used_gb_current, hw_info.ram_total_gb);
      ImGui::Text("%s", buf);

      // GPU
      if (hw_info.gpu_vendor != HardwareInfo::GPUVendor::None && hw_info.gpu_tool_available) {
        ImGui::TableNextRow();
        ImGui::TableSetColumnIndex(0);
        ImGui::TextColored(ColorFromUsagePercent(gpu_data.usage_percent_current), "GPU");
        ImGui::TableSetColumnIndex(1);
        snprintf(buf, sizeof(buf), "%.1f%%", gpu_data.usage_percent_current);
        ImGui::ProgressBar(gpu_data.usage_percent_current * 0.01f, ImVec2(-1, 0), buf);
        ImGui::TableSetColumnIndex(2);
        ImGui::Text("-");

        if (hw_info.gpu_vram_total_gb > 0) {
          ImGui::TableNextRow();
          ImGui::TableSetColumnIndex(0);
          ImGui::TextColored(ColorFromUsagePercent(gpu_data.vram_percent_current), "VRAM");
          ImGui::TableSetColumnIndex(1);
          snprintf(buf, sizeof(buf), "%.1f%%", gpu_data.vram_percent_current);
          ImGui::ProgressBar(gpu_data.vram_percent_current * 0.01f, ImVec2(-1, 0), buf);
          ImGui::TableSetColumnIndex(2);
          snprintf(buf, sizeof(buf), "%.1f/%.1fGB",
                   gpu_data.vram_used_gb_current,
                   hw_info.gpu_vram_total_gb);
          ImGui::Text("%s", buf);
        }
      }

      // Network RX
      ImGui::TableNextRow();
      ImGui::TableSetColumnIndex(0);
      ImGui::TextColored(ColorFromUsagePercent(net_data.rx_percent_current), "Net RX");
      ImGui::TableSetColumnIndex(1);
      snprintf(buf, sizeof(buf), "%.1f%%", net_data.rx_percent_current);
      ImGui::ProgressBar(net_data.rx_percent_current * 0.01f, ImVec2(-1, 0), buf);
      ImGui::TableSetColumnIndex(2);
      snprintf(buf, sizeof(buf), "%.2f Mbps", net_data.rx_mbps_current);
      ImGui::Text("%s", buf);

      // Network TX
      ImGui::TableNextRow();
      ImGui::TableSetColumnIndex(0);
      ImGui::TextColored(ColorFromUsagePercent(net_data.tx_percent_current), "Net TX");
      ImGui::TableSetColumnIndex(1);
      snprintf(buf, sizeof(buf), "%.1f%%", net_data.tx_percent_current);
      ImGui::ProgressBar(net_data.tx_percent_current * 0.01f, ImVec2(-1, 0), buf);
      ImGui::TableSetColumnIndex(2);
      snprintf(buf, sizeof(buf), "%.2f Mbps", net_data.tx_mbps_current);
      ImGui::Text("%s", buf);

      // Disk Busy
      ImGui::TableNextRow();
      ImGui::TableSetColumnIndex(0);
      ImGui::TextColored(ColorFromUsagePercent(disk_data.busy_percent_current), "Disk Busy");
      ImGui::TableSetColumnIndex(1);
      snprintf(buf, sizeof(buf), "%.1f%%", disk_data.busy_percent_current);
      ImGui::ProgressBar(disk_data.busy_percent_current * 0.01f, ImVec2(-1, 0), buf);
      ImGui::TableSetColumnIndex(2);
      snprintf(buf, sizeof(buf), "R:%.0f W:%.0f",
               disk_data.read_mbps_current,
               disk_data.write_mbps_current);
      ImGui::Text("%s", buf);

      ImGui::EndTable();
    }

    ImGui::PopStyleVar(2);
  }

  void RenderPlots() {
    float available_height = ImGui::GetContentRegionAvail().y;
    float available_width = ImGui::GetContentRegionAvail().x;
    float plot_margin = 3.0f;

    // Top: CPU cores plot (50% height)
    RenderCPUPlot(available_width, available_height * 0.5f - plot_margin);

    // Bottom: Small plots (50% height)
    RenderSmallPlots(available_width, available_height * 0.5f - plot_margin);
  }

  void RenderCPUPlot(float width, float height) {
    if (ImPlot::BeginPlot("##CPUCores", ImVec2(width, height))) {
      ImPlot::SetupAxes(nullptr, nullptr,
                        ImPlotAxisFlags_NoDecorations,
                        ImPlotAxisFlags_NoDecorations);
      ImPlot::SetupAxisLimits(ImAxis_X1, 0, HISTORY_SAMPLES, ImGuiCond_Always);
      ImPlot::SetupAxisLimits(ImAxis_Y1, 0, 100, ImGuiCond_Always);
      ImPlot::SetupLegend(ImPlotLocation_NorthWest, ImPlotLegendFlags_None);

      for (int i = 0; i < hw_info.cpu_logical_cores; ++i) {
        ImVec4 color = ColorFromCoreIndex(i, hw_info.cpu_logical_cores);
        char label[16];
        snprintf(label, sizeof(label), "CPU%d", i);
        ImPlot::PushStyleColor(ImPlotCol_Line, color);
        ImPlot::PlotLine(label,
                         cpu_cores_data[i].usage_history.data(),
                         HISTORY_SAMPLES,
                         1.0, 0.0, 0,
                         history_write_index);
        ImPlot::PopStyleColor();
      }

      ImPlot::PlotText("100%", PLOT_LABEL_X_POS * 0.5f, PLOT_LABEL_Y_POS);
      ImPlot::EndPlot();
    }
  }

  void RenderSmallPlots(float width, float height) {
    int num_plots = 3; // RAM, Network, Disk
    if (hw_info.gpu_vendor != HardwareInfo::GPUVendor::None && hw_info.gpu_tool_available) {
      num_plots += (hw_info.gpu_vram_total_gb > 0 ? 2 : 1);
    }

    float plot_margin = 2.0f;
    float plot_width = (width - plot_margin * (num_plots - 1)) / num_plots;

    // RAM
    RenderMemoryPlot(plot_width, height);

    // GPU
    if (hw_info.gpu_vendor != HardwareInfo::GPUVendor::None && hw_info.gpu_tool_available) {
      ImGui::SameLine();
      RenderGPUUsagePlot(plot_width, height);

      if (hw_info.gpu_vram_total_gb > 0) {
        ImGui::SameLine();
        RenderGPUVRAMPlot(plot_width, height);
      }
    }

    // Network
    ImGui::SameLine();
    RenderNetworkPlot(plot_width, height);

    // Disk
    ImGui::SameLine();
    RenderDiskPlot(plot_width, height);
  }

  void RenderMemoryPlot(float width, float height) {
    if (ImPlot::BeginPlot("##RAM", ImVec2(width, height))) {
      ImPlot::SetupAxes(nullptr, nullptr,
                        ImPlotAxisFlags_NoDecorations,
                        ImPlotAxisFlags_NoDecorations);
      ImPlot::SetupAxisLimits(ImAxis_X1, 0, HISTORY_SAMPLES, ImGuiCond_Always);
      ImPlot::SetupAxisLimits(ImAxis_Y1, 0, 100, ImGuiCond_Always);
      ImPlot::SetupLegend(ImPlotLocation_NorthWest, ImPlotLegendFlags_None);

      ImPlot::PushStyleColor(ImPlotCol_Fill, ImVec4(0.2f, 0.6f, 1.0f, 0.3f));
      ImPlot::PlotShaded("RAM",
                         mem_data.usage_percent_history.data(),
                         HISTORY_SAMPLES,
                         0.0, 1.0, 0.0, 0,
                         history_write_index);
      ImPlot::PopStyleColor();

      ImPlot::PushStyleColor(ImPlotCol_Line, ImVec4(0.2f, 0.6f, 1.0f, 1.0f));
      ImPlot::PlotLine("RAM",
                       mem_data.usage_percent_history.data(),
                       HISTORY_SAMPLES,
                       1.0, 0.0, 0,
                       history_write_index);
      ImPlot::PopStyleColor();

      ImPlot::PlotText("100%", PLOT_LABEL_X_POS, PLOT_LABEL_Y_POS);
      ImPlot::EndPlot();
    }
  }

  void RenderGPUUsagePlot(float width, float height) {
    if (ImPlot::BeginPlot("##GPU", ImVec2(width, height))) {
      ImPlot::SetupAxes(nullptr, nullptr,
                        ImPlotAxisFlags_NoDecorations,
                        ImPlotAxisFlags_NoDecorations);
      ImPlot::SetupAxisLimits(ImAxis_X1, 0, HISTORY_SAMPLES, ImGuiCond_Always);
      ImPlot::SetupAxisLimits(ImAxis_Y1, 0, 100, ImGuiCond_Always);
      ImPlot::SetupLegend(ImPlotLocation_NorthWest, ImPlotLegendFlags_None);

      ImPlot::PushStyleColor(ImPlotCol_Fill, ImVec4(0.2f, 1.0f, 0.4f, 0.3f));
      ImPlot::PlotShaded("GPU",
                         gpu_data.usage_percent_history.data(),
                         HISTORY_SAMPLES,
                         0.0, 1.0, 0.0, 0,
                         history_write_index);
      ImPlot::PopStyleColor();

      ImPlot::PushStyleColor(ImPlotCol_Line, ImVec4(0.2f, 1.0f, 0.4f, 1.0f));
      ImPlot::PlotLine("GPU",
                       gpu_data.usage_percent_history.data(),
                       HISTORY_SAMPLES,
                       1.0, 0.0, 0,
                       history_write_index);
      ImPlot::PopStyleColor();

      ImPlot::PlotText("100%", PLOT_LABEL_X_POS, PLOT_LABEL_Y_POS);
      ImPlot::EndPlot();
    }
  }

  void RenderGPUVRAMPlot(float width, float height) {
    if (ImPlot::BeginPlot("##VRAM", ImVec2(width, height))) {
      ImPlot::SetupAxes(nullptr, nullptr,
                        ImPlotAxisFlags_NoDecorations,
                        ImPlotAxisFlags_NoDecorations);
      ImPlot::SetupAxisLimits(ImAxis_X1, 0, HISTORY_SAMPLES, ImGuiCond_Always);
      ImPlot::SetupAxisLimits(ImAxis_Y1, 0, 100, ImGuiCond_Always);
      ImPlot::SetupLegend(ImPlotLocation_NorthWest, ImPlotLegendFlags_None);

      ImPlot::PushStyleColor(ImPlotCol_Fill, ImVec4(1.0f, 0.6f, 0.2f, 0.3f));
      ImPlot::PlotShaded("VRAM",
                         gpu_data.vram_percent_history.data(),
                         HISTORY_SAMPLES,
                         0.0, 1.0, 0.0, 0,
                         history_write_index);
      ImPlot::PopStyleColor();

      ImPlot::PushStyleColor(ImPlotCol_Line, ImVec4(1.0f, 0.6f, 0.2f, 1.0f));
      ImPlot::PlotLine("VRAM",
                       gpu_data.vram_percent_history.data(),
                       HISTORY_SAMPLES,
                       1.0, 0.0, 0,
                       history_write_index);
      ImPlot::PopStyleColor();

      char label[32];
      snprintf(label, sizeof(label), "%.1fGB", hw_info.gpu_vram_total_gb);
      ImPlot::PlotText(label, PLOT_LABEL_X_POS, PLOT_LABEL_Y_POS);
      ImPlot::EndPlot();
    }
  }

  void RenderNetworkPlot(float width, float height) {
    if (ImPlot::BeginPlot("##Network", ImVec2(width, height))) {
      ImPlot::SetupAxes(nullptr, nullptr,
                        ImPlotAxisFlags_NoDecorations,
                        ImPlotAxisFlags_NoDecorations);
      ImPlot::SetupAxisLimits(ImAxis_X1, 0, HISTORY_SAMPLES, ImGuiCond_Always);
      ImPlot::SetupAxisLimits(ImAxis_Y1, 0, 100, ImGuiCond_Always);
      ImPlot::SetupLegend(ImPlotLocation_NorthWest, ImPlotLegendFlags_None);

      ImPlot::PushStyleColor(ImPlotCol_Line, ImVec4(0.5f, 1.0f, 0.8f, 1.0f));
      ImPlot::PlotLine("RX",
                       net_data.rx_percent_history.data(),
                       HISTORY_SAMPLES,
                       1.0, 0.0, 0,
                       history_write_index);
      ImPlot::PopStyleColor();

      ImPlot::PushStyleColor(ImPlotCol_Line, ImVec4(1.0f, 0.8f, 0.5f, 1.0f));
      ImPlot::PlotLine("TX",
                       net_data.tx_percent_history.data(),
                       HISTORY_SAMPLES,
                       1.0, 0.0, 0,
                       history_write_index);
      ImPlot::PopStyleColor();

      ImPlot::PlotText("10MB/s", PLOT_LABEL_X_POS, PLOT_LABEL_Y_POS);
      ImPlot::EndPlot();
    }
  }

  void RenderDiskPlot(float width, float height) {
    float max_speed = std::max(disk_data.read_mbps_max, disk_data.write_mbps_max);

    if (ImPlot::BeginPlot("##DiskIO", ImVec2(width, height))) {
      ImPlot::SetupAxes(nullptr, nullptr,
                        ImPlotAxisFlags_NoDecorations,
                        ImPlotAxisFlags_NoDecorations);
      ImPlot::SetupAxisLimits(ImAxis_X1, 0, HISTORY_SAMPLES, ImGuiCond_Always);
      ImPlot::SetupAxisLimits(ImAxis_Y1, 0, max_speed, ImGuiCond_Always);
      ImPlot::SetupLegend(ImPlotLocation_NorthWest, ImPlotLegendFlags_None);

      ImPlot::SetupAxis(ImAxis_Y2, nullptr,
                        ImPlotAxisFlags_NoDecorations | ImPlotAxisFlags_Opposite);
      ImPlot::SetupAxisLimits(ImAxis_Y2, 0, 100, ImGuiCond_Always);

      // Plot speeds on left axis
      ImPlot::SetAxes(ImAxis_X1, ImAxis_Y1);
      ImPlot::PushStyleColor(ImPlotCol_Line, ImVec4(0.6f, 0.8f, 1.0f, 1.0f));
      ImPlot::PlotLine("R",
                       disk_data.read_mbps_history.data(),
                       HISTORY_SAMPLES,
                       1.0, 0.0, 0,
                       history_write_index);
      ImPlot::PopStyleColor();

      ImPlot::PushStyleColor(ImPlotCol_Line, ImVec4(1.0f, 0.7f, 0.6f, 1.0f));
      ImPlot::PlotLine("W",
                       disk_data.write_mbps_history.data(),
                       HISTORY_SAMPLES,
                       1.0, 0.0, 0,
                       history_write_index);
      ImPlot::PopStyleColor();

      // Plot busy% on right axis
      ImPlot::SetAxes(ImAxis_X1, ImAxis_Y2);
      ImPlot::PushStyleColor(ImPlotCol_Fill, ImVec4(0.8f, 0.6f, 1.0f, 0.2f));
      ImPlot::PlotShaded("Busy",
                         disk_data.busy_percent_history.data(),
                         HISTORY_SAMPLES,
                         0.0, 1.0, 0.0, 0,
                         history_write_index);
      ImPlot::PopStyleColor();

      ImPlot::PushStyleColor(ImPlotCol_Line, ImVec4(0.8f, 0.6f, 1.0f, 0.6f));
      ImPlot::PlotLine("Busy",
                       disk_data.busy_percent_history.data(),
                       HISTORY_SAMPLES,
                       1.0, 0.0, 0,
                       history_write_index);
      ImPlot::PopStyleColor();

      // Speed label
      ImPlot::SetAxes(ImAxis_X1, ImAxis_Y1);
      char label[32];
      snprintf(label, sizeof(label), "%.0fMB/s", max_speed);
      ImPlot::PlotText(label, PLOT_LABEL_X_POS, PLOT_LABEL_Y_POS * 0.01f * max_speed);

      ImPlot::EndPlot();
    }
  }

  // ============================================================================
  // HELPER FUNCTIONS
  // ============================================================================

  ImVec4 ColorFromUsagePercent(float percent) {
    if (percent < 30.0f)
      return ImVec4(0.2f, 1.0f, 0.3f, 1.0f); // Green
    if (percent < 60.0f)
      return ImVec4(1.0f, 1.0f, 0.2f, 1.0f); // Yellow
    if (percent < 85.0f)
      return ImVec4(1.0f, 0.6f, 0.2f, 1.0f); // Orange
    return ImVec4(1.0f, 0.2f, 0.2f, 1.0f);   // Red
  }

  ImVec4 ColorFromCoreIndex(int core_idx, int total_cores) {
    // Generate distinct colors using HSV
    float hue = (float)core_idx / (float)total_cores;
    float saturation = 0.8f;
    float value = 0.9f;

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
