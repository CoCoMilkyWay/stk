#include "gui/task_system_info/TaskSystemInfo.hpp"
#include "gui/Tasks.hpp"
#include "imgui.h"
#include "implot.h"
#include "shared/SharedData.hpp"
#include <algorithm>
#include <array>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <deque>
#include <thread>
#include <vector>

// Platform-specific headers
#ifdef _WIN32
#include <windows.h>
#include <pdh.h>
#include <pdhmsg.h>
#include <iphlpapi.h>
#include <sysinfoapi.h>
#include <versionhelpers.h>
#include <intrin.h>
#include <setupapi.h>
#include <dxgi.h>

#pragma comment(lib, "pdh.lib")
#pragma comment(lib, "iphlpapi.lib")
#pragma comment(lib, "dxguid.lib")
#elif __APPLE__
#include <sys/types.h>
#include <sys/sysctl.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <mach/mach.h>
#include <mach/task.h>
#else // Linux
#include <sys/sysinfo.h>
#include <sys/utsname.h>
#include <unistd.h>
#endif

namespace GUI::Tasks {
namespace {

// SystemInfo task - display hardware and OS info with dynamic monitoring
class SystemInfoTask {
private:
  // ============================================================================
  // CONSTANTS
  // ============================================================================
  static constexpr int HISTORY_SAMPLES = 100;
  static constexpr int UPDATE_INTERVAL_MS = 100;
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

    // CPU Info
    int cpu_logical_cores = 0;
    int cpu_physical_cores = 0;
    long cpu_cache_l1_kb = 0;
    long cpu_cache_l2_kb = 0;
    long cpu_cache_l3_kb = 0;
    std::string cpu_vendor;
    std::string cpu_model;
    std::string cpu_architecture; // x86_64, aarch64, etc.

    // x64 Instruction Sets (Intel/AMD)
    struct X64InstructionSets {
      // Legacy SIMD
      bool sse = false;
      bool sse2 = false;
      bool sse3 = false;
      bool ssse3 = false;
      bool sse4_1 = false;
      bool sse4_2 = false;
      // SIMD main
      bool avx = false;
      bool avx2 = false;
      bool fma = false;
      bool f16c = false;
      // AVX512 base
      bool avx512f = false;
      bool avx512cd = false;
      bool avx512vl = false;
      bool avx512bw = false;
      bool avx512dq = false;
      // AI/ML acceleration
      bool avx512_fp16 = false;
      bool avx512_bf16 = false;
      bool avx512_vnni = false;
      bool avx_vnni = false;
      bool amx_tile = false;
      // Crypto/Hash
      bool aes = false;
      bool sha = false;
      bool gfni = false;
      bool avx512_ifma = false;
      // Memory/Cache
      bool prefetchw = false;
      bool clflushopt = false;
      bool clwb = false;
      bool movdir64b = false;
      bool rtm = false;
      // System
      bool rdrand = false;
      bool rdseed = false;
    } x64_isa;

    // AArch64 Instruction Sets (ARM/Apple)
    struct AArch64InstructionSets {
      // Legacy SIMD (对应x86的SSE系列)
      bool neon = false;
      bool neon_fp16 = false;
      bool neon_bf16 = false;
      bool neon_i8mm = false;
      bool neon_dotprod = false;
      bool neon_fcma = false;
      // SIMD main (对应x86的AVX系列)
      bool sve = false;
      bool sve2 = false;
      bool sme = false;
      bool sme2 = false;
      // Matrix extensions (对应x86的AMX)
      bool amx = false;           // Apple Matrix Extension
      bool neural_engine = false; // Apple Neural Engine
      // Float/Math (标准扩展)
      bool fp64 = false;
      // AI/ML acceleration (对应x86的VNNI)
      bool bf16 = false;
      bool i8mm = false;
      // Crypto/Hash (对应x86的AES/SHA)
      bool aes = false;
      bool sha1 = false;
      bool sha2 = false;
      bool sha3 = false;
      bool sha512 = false;
      bool pmull = false;
      bool crc32 = false;
      // Memory/Cache (对应x86的prefetch系列)
      bool prefetch = false;
      bool dc_zva = false;
      // System (对应x86的RNG和事务内存)
      bool rndr = false;   // Random number (对应RDRAND)
      bool rndrrs = false; // Reseeded random (对应RDSEED)
      bool pac = false;    // Pointer Authentication
      bool mte = false;    // Memory Tagging Extension
    } aarch64_isa;

    // Memory Info
    long ram_total_gb = 0;

    // GPU Info
    enum class GPUVendor { None,
                           NVIDIA,
                           AMD,
                           Intel,
                           Apple };
    GPUVendor gpu_vendor = GPUVendor::None;
    std::string gpu_name;
    float gpu_vram_total_gb = 0.0f;
    bool gpu_tool_available = false;
    std::string gpu_install_message;
  };
  HardwareInfo hw_info;

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
    uint64_t rx_bytes_prev = 0;
    uint64_t tx_bytes_prev = 0;
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

  // PDH (Performance Data Helper) handles for monitoring (Windows only)
#ifdef _WIN32
  PDH_HQUERY pdh_query = nullptr;
  std::vector<PDH_HCOUNTER> pdh_cpu_counters;
  PDH_HCOUNTER pdh_disk_read_counter = nullptr;
  PDH_HCOUNTER pdh_disk_write_counter = nullptr;
  PDH_HCOUNTER pdh_disk_busy_counter = nullptr;
#endif

public:
  // ============================================================================
  // CONSTRUCTOR/DESTRUCTOR
  // ============================================================================
  SystemInfoTask() {
    InitializeHardware();
    InitializePDH();
    initialized = true;
    last_update_time = std::chrono::steady_clock::now();
    last_gpu_update_time = last_update_time;
  }

  ~SystemInfoTask() {
    // Cleanup PDH resources (Windows only)
#ifdef _WIN32
    if (pdh_query) {
      PdhCloseQuery(pdh_query);
    }
#endif
  }

  // ============================================================================
  // INTERFACE IMPLEMENTATION
  // ============================================================================
  const char *GetName() const {
    return "SystemInfo";
  }

  void OnExpand() {
    is_expanded = true;
  }

  void OnCollapse() {
    is_expanded = false;
  }

  void DrawPanel(SharedData & /*data*/) {
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
    DetectOS();
    DetectCPU();
    DetectX64Instructions();
    DetectAArch64Instructions();
    DetectMemory();
    DetectGPU();

    // Allocate dynamic data structures
    cpu_cores_data.resize(hw_info.cpu_logical_cores);
  }

  // ----------------------------------------------------------------------------
  // OS Detection
  // ----------------------------------------------------------------------------

  void DetectOS() {
#ifdef _WIN32
    hw_info.os_name = "Windows";

    // Get Windows version
    OSVERSIONINFOEXW osvi = {};
    osvi.dwOSVersionInfoSize = sizeof(osvi);
    
    // Use RtlGetVersion for accurate version (GetVersionEx is deprecated)
    typedef LONG(WINAPI *RtlGetVersionFunc)(OSVERSIONINFOEXW*);
    HMODULE ntdll = GetModuleHandleW(L"ntdll.dll");
    if (ntdll) {
      auto RtlGetVersion = (RtlGetVersionFunc)GetProcAddress(ntdll, "RtlGetVersion");
      if (RtlGetVersion) {
        RtlGetVersion(&osvi);
        char version_buf[64];
        snprintf(version_buf, sizeof(version_buf), "%lu.%lu.%lu", 
                 osvi.dwMajorVersion, osvi.dwMinorVersion, osvi.dwBuildNumber);
        hw_info.kernel_version = version_buf;
      }
    }

    // Get computer name
    DWORD size = MAX_COMPUTERNAME_LENGTH + 1;
    char hostname_buf[MAX_COMPUTERNAME_LENGTH + 1];
    if (GetComputerNameA(hostname_buf, &size)) {
      hw_info.hostname = hostname_buf;
    }
#elif __APPLE__
    hw_info.os_name = "macOS";
    
    // Get kernel version from sysctl
    char version_str[256] = {};
    size_t size = sizeof(version_str);
    if (sysctlbyname("kern.osrelease", version_str, &size, nullptr, 0) == 0) {
      hw_info.kernel_version = version_str;
    }
    
    // Get hostname
    char hostname_buf[256] = {};
    if (gethostname(hostname_buf, sizeof(hostname_buf)) == 0) {
      hw_info.hostname = hostname_buf;
    }
#else // Linux
    hw_info.os_name = "Linux";
    
    // Get kernel version
    struct utsname uname_data;
    if (uname(&uname_data) == 0) {
      hw_info.kernel_version = uname_data.release;
      hw_info.hostname = uname_data.nodename;
    }
#endif
  }

  // ----------------------------------------------------------------------------
  // CPU Detection
  // ----------------------------------------------------------------------------

  void DetectCPU() {
#ifdef _WIN32
    // Detect CPU architecture from system info
    SYSTEM_INFO sysInfo;
    GetNativeSystemInfo(&sysInfo);
    
    switch (sysInfo.wProcessorArchitecture) {
      case PROCESSOR_ARCHITECTURE_AMD64:
        hw_info.cpu_architecture = "x86_64";
        break;
      case PROCESSOR_ARCHITECTURE_ARM64:
        hw_info.cpu_architecture = "aarch64";
        break;
      case PROCESSOR_ARCHITECTURE_INTEL:
        hw_info.cpu_architecture = "i686";
        break;
      case PROCESSOR_ARCHITECTURE_ARM:
        hw_info.cpu_architecture = "arm";
        break;
      default:
        hw_info.cpu_architecture = "unknown";
    }

    // Get CPU vendor and model name using CPUID
    int cpuInfo[4] = {0};
    __cpuid(cpuInfo, 0);
    
    // Extract vendor string (12 chars from EBX, EDX, ECX)
    char vendor[13] = {0};
    *reinterpret_cast<int*>(vendor) = cpuInfo[1];  // EBX
    *reinterpret_cast<int*>(vendor + 4) = cpuInfo[3];  // EDX
    *reinterpret_cast<int*>(vendor + 8) = cpuInfo[2];  // ECX
    hw_info.cpu_vendor = vendor;

    // Get CPU model name (requires CPUID with EAX=0x80000002-0x80000004)
    char brand[49] = {0};
    for (unsigned int i = 0x80000002; i <= 0x80000004; i++) {
      __cpuid(cpuInfo, i);
      memcpy(brand + (i - 0x80000002) * 16, cpuInfo, sizeof(cpuInfo));
    }
    hw_info.cpu_model = brand;
    // Trim leading/trailing spaces
    size_t start = hw_info.cpu_model.find_first_not_of(" \t");
    size_t end = hw_info.cpu_model.find_last_not_of(" \t");
    if (start != std::string::npos && end != std::string::npos) {
      hw_info.cpu_model = hw_info.cpu_model.substr(start, end - start + 1);
    }

    // Get logical and physical core counts using GetLogicalProcessorInformationEx
    DWORD bufferSize = 0;
    GetLogicalProcessorInformationEx(RelationAll, nullptr, &bufferSize);
    
    std::vector<char> buffer(bufferSize);
    auto info = reinterpret_cast<SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX*>(buffer.data());
    
    if (GetLogicalProcessorInformationEx(RelationAll, info, &bufferSize)) {
      hw_info.cpu_logical_cores = 0;
      hw_info.cpu_physical_cores = 0;
      
      DWORD offset = 0;
      while (offset < bufferSize) {
        auto current = reinterpret_cast<SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX*>(buffer.data() + offset);
        
        if (current->Relationship == RelationProcessorCore) {
          hw_info.cpu_physical_cores++;
          // Count logical processors in this core
          for (WORD group = 0; group < current->Processor.GroupCount; group++) {
            KAFFINITY mask = current->Processor.GroupMask[group].Mask;
            hw_info.cpu_logical_cores += __popcnt64(mask);
          }
        } else if (current->Relationship == RelationCache) {
          auto cache = &current->Cache;
          long size_kb = cache->CacheSize / 1024;
          
          if (cache->Level == 1 && hw_info.cpu_cache_l1_kb == 0) {
            hw_info.cpu_cache_l1_kb = size_kb;
          } else if (cache->Level == 2 && size_kb > hw_info.cpu_cache_l2_kb) {
            hw_info.cpu_cache_l2_kb = size_kb;
          } else if (cache->Level == 3 && size_kb > hw_info.cpu_cache_l3_kb) {
            hw_info.cpu_cache_l3_kb = size_kb;
          }
        }
        
        offset += current->Size;
      }
    }
    
    // Fallback if GetLogicalProcessorInformationEx fails
    if (hw_info.cpu_logical_cores == 0) {
      hw_info.cpu_logical_cores = std::thread::hardware_concurrency();
      hw_info.cpu_physical_cores = hw_info.cpu_logical_cores;
    }
#elif __APPLE__
    // macOS: Simplified CPU detection
    hw_info.cpu_architecture = "aarch64"; // Assume Apple Silicon for now
    hw_info.cpu_vendor = "Apple";
    
    // Get model name using sysctl
    char model[256] = {};
    size_t size = sizeof(model);
    if (sysctlbyname("hw.model", model, &size, nullptr, 0) == 0) {
      hw_info.cpu_model = model;
    }
    
    // Get core counts
    int ncpus = 0;
    size = sizeof(ncpus);
    if (sysctlbyname("hw.logicalcpu_max", &ncpus, &size, nullptr, 0) == 0) {
      hw_info.cpu_logical_cores = ncpus;
    } else {
      hw_info.cpu_logical_cores = std::thread::hardware_concurrency();
    }
    
    int physical_cores = 0;
    size = sizeof(physical_cores);
    if (sysctlbyname("hw.physicalcpu_max", &physical_cores, &size, nullptr, 0) == 0) {
      hw_info.cpu_physical_cores = physical_cores;
    } else {
      hw_info.cpu_physical_cores = hw_info.cpu_logical_cores;
    }
    
    // Get L3 cache size
    uint64_t l3_size = 0;
    size = sizeof(l3_size);
    if (sysctlbyname("hw.l3cachesize", &l3_size, &size, nullptr, 0) == 0) {
      hw_info.cpu_cache_l3_kb = l3_size / 1024;
    }
#else // Linux
    // Linux: Simplified CPU detection
    hw_info.cpu_logical_cores = std::thread::hardware_concurrency();
    hw_info.cpu_physical_cores = hw_info.cpu_logical_cores;
    
    // Try to read CPU info from /proc/cpuinfo (basic parsing)
    hw_info.cpu_vendor = "Unknown";
    hw_info.cpu_model = "Unknown";
    hw_info.cpu_architecture = "unknown";
#endif

    // Call instruction set detection
    if (hw_info.cpu_architecture == "x86_64" || hw_info.cpu_architecture == "i686") {
      DetectX64Instructions();
    } else if (hw_info.cpu_architecture == "aarch64" || hw_info.cpu_architecture == "arm") {
      DetectAArch64Instructions();
    }
  }

  // ----------------------------------------------------------------------------
  // x64 Instruction Set Detection (Intel/AMD) - Windows only
  void DetectX64Instructions() {
    // Only detect x64 instructions on x64 architecture
    if (hw_info.cpu_architecture != "x86_64" && hw_info.cpu_architecture != "i686") {
      return;
    }

#ifdef _WIN32
    int cpuInfo[4] = {0};
    
    // CPUID Function 1: Processor Info and Feature Bits
    __cpuidex(cpuInfo, 1, 0);
    int ecx1 = cpuInfo[2];
    int edx1 = cpuInfo[3];
    
    // SSE series (EDX bits)
    hw_info.x64_isa.sse = (edx1 & (1 << 25)) != 0;
    hw_info.x64_isa.sse2 = (edx1 & (1 << 26)) != 0;
    
    // SSE3+ (ECX bits)
    hw_info.x64_isa.sse3 = (ecx1 & (1 << 0)) != 0;
    hw_info.x64_isa.ssse3 = (ecx1 & (1 << 9)) != 0;
    hw_info.x64_isa.sse4_1 = (ecx1 & (1 << 19)) != 0;
    hw_info.x64_isa.sse4_2 = (ecx1 & (1 << 20)) != 0;
    
    // SIMD main
    hw_info.x64_isa.avx = (ecx1 & (1 << 28)) != 0;
    hw_info.x64_isa.fma = (ecx1 & (1 << 12)) != 0;
    hw_info.x64_isa.f16c = (ecx1 & (1 << 29)) != 0;
    hw_info.x64_isa.aes = (ecx1 & (1 << 25)) != 0;
    hw_info.x64_isa.rdrand = (ecx1 & (1 << 30)) != 0;
    
    // CPUID Function 7: Extended Features
    __cpuidex(cpuInfo, 7, 0);
    int ebx7 = cpuInfo[1];
    int ecx7 = cpuInfo[2];
    int edx7 = cpuInfo[3];
    
    // AVX2 and AVX512 base
    hw_info.x64_isa.avx2 = (ebx7 & (1 << 5)) != 0;
    hw_info.x64_isa.avx512f = (ebx7 & (1 << 16)) != 0;
    hw_info.x64_isa.avx512dq = (ebx7 & (1 << 17)) != 0;
    hw_info.x64_isa.avx512_ifma = (ebx7 & (1 << 21)) != 0;
    hw_info.x64_isa.avx512cd = (ebx7 & (1 << 28)) != 0;
    hw_info.x64_isa.avx512bw = (ebx7 & (1 << 30)) != 0;
    hw_info.x64_isa.avx512vl = (ebx7 & (1 << 31)) != 0;
    
    // AI/ML and crypto
    hw_info.x64_isa.avx512_vnni = (ecx7 & (1 << 11)) != 0;
    hw_info.x64_isa.gfni = (ecx7 & (1 << 8)) != 0;
    
    // Memory/Cache operations
    hw_info.x64_isa.clflushopt = (ebx7 & (1 << 23)) != 0;
    hw_info.x64_isa.clwb = (ebx7 & (1 << 24)) != 0;
    hw_info.x64_isa.sha = (ebx7 & (1 << 29)) != 0;
    hw_info.x64_isa.prefetchw = (ecx7 & (1 << 0)) != 0;
    
    // Advanced features
    hw_info.x64_isa.avx512_bf16 = (ecx7 & (1 << 5)) != 0;
    hw_info.x64_isa.movdir64b = (ecx7 & (1 << 28)) != 0;
    hw_info.x64_isa.rdseed = (ebx7 & (1 << 18)) != 0;
    hw_info.x64_isa.rtm = (ebx7 & (1 << 11)) != 0;
    
    // CPUID Function 7 Sub-leaf 1: More extended features
    __cpuidex(cpuInfo, 7, 1);
    int eax7_1 = cpuInfo[0];
    int edx7_1 = cpuInfo[3];
    
    hw_info.x64_isa.avx_vnni = (eax7_1 & (1 << 4)) != 0;
    hw_info.x64_isa.avx512_fp16 = (edx7_1 & (1 << 23)) != 0;
    hw_info.x64_isa.amx_tile = (edx7_1 & (1 << 24)) != 0;
#else
    // Non-Windows: x64 ISA detection not implemented
    // TODO: Use cpuid inline assembly or __builtin_cpu_supports on gcc/clang
#endif
  }

  // ----------------------------------------------------------------------------
  // AArch64 Instruction Set Detection (ARM/Apple)
  // ----------------------------------------------------------------------------

  void DetectAArch64Instructions() {
    // Only detect AArch64 instructions on ARM architecture
    if (hw_info.cpu_architecture != "aarch64" && hw_info.cpu_architecture != "arm64") {
      return;
    }

#ifdef _WIN32
    // Use IsProcessorFeaturePresent for ARM feature detection on Windows
    hw_info.aarch64_isa.neon = IsProcessorFeaturePresent(PF_ARM_NEON_INSTRUCTIONS_AVAILABLE);
    hw_info.aarch64_isa.aes = IsProcessorFeaturePresent(PF_ARM_V8_CRYPTO_INSTRUCTIONS_AVAILABLE);
    hw_info.aarch64_isa.crc32 = IsProcessorFeaturePresent(PF_ARM_V8_CRC32_INSTRUCTIONS_AVAILABLE);
#else
    // Non-Windows ARM: assume ARMv8+ features are available on modern ARM64 systems
    hw_info.aarch64_isa.neon = true;  // Standard on ARMv8+
    hw_info.aarch64_isa.aes = true;
    hw_info.aarch64_isa.crc32 = true;
#endif
    
    // Standard on ARMv8+
    hw_info.aarch64_isa.fp64 = true;
    hw_info.aarch64_isa.prefetch = true;
    hw_info.aarch64_isa.dc_zva = true;
    
    // Detect Apple Silicon
    bool is_apple = (hw_info.cpu_model.find("Apple") != std::string::npos);
    
    // Apple-specific features
    if (is_apple) {
      hw_info.aarch64_isa.amx = (hw_info.cpu_model.find("M4") != std::string::npos ||
                                 hw_info.cpu_model.find("M5") != std::string::npos);
      hw_info.aarch64_isa.neural_engine = true;
    }
  }

  // ----------------------------------------------------------------------------
  // Memory Detection
  // ----------------------------------------------------------------------------

  void DetectMemory() {
#ifdef _WIN32
    ULONGLONG totalMemoryKB = 0;
    if (GetPhysicallyInstalledSystemMemory(&totalMemoryKB)) {
      hw_info.ram_total_gb = totalMemoryKB / (1024 * 1024);
    } else {
      // Fallback to GlobalMemoryStatusEx
      MEMORYSTATUSEX memInfo;
      memInfo.dwLength = sizeof(MEMORYSTATUSEX);
      if (GlobalMemoryStatusEx(&memInfo)) {
        hw_info.ram_total_gb = memInfo.ullTotalPhys / (1024 * 1024 * 1024);
      }
    }
#elif __APPLE__
    uint64_t size = 0;
    size_t len = sizeof(size);
    if (sysctlbyname("hw.memsize", &size, &len, nullptr, 0) == 0) {
      hw_info.ram_total_gb = size / (1024 * 1024 * 1024);
    }
#else // Linux
    struct sysinfo info;
    if (sysinfo(&info) == 0) {
      hw_info.ram_total_gb = info.totalram / (1024 * 1024 * 1024);
    }
#endif
  }

  // ----------------------------------------------------------------------------
  // GPU Detection
  // ----------------------------------------------------------------------------

  void DetectGPU() {
#ifdef _WIN32
    // Use DXGI to enumerate GPUs
    IDXGIFactory* pFactory = nullptr;
    HRESULT hr = CreateDXGIFactory(__uuidof(IDXGIFactory), (void**)&pFactory);
    
    if (FAILED(hr) || !pFactory) {
      hw_info.gpu_vendor = HardwareInfo::GPUVendor::None;
      hw_info.gpu_name = "No GPU detected";
      return;
    }

    IDXGIAdapter* pAdapter = nullptr;
    UINT adapterIndex = 0;
    
    // Get first adapter (primary GPU)
    if (pFactory->EnumAdapters(adapterIndex, &pAdapter) == DXGI_ERROR_NOT_FOUND) {
      pFactory->Release();
      hw_info.gpu_vendor = HardwareInfo::GPUVendor::None;
      hw_info.gpu_name = "No GPU detected";
      return;
    }

    DXGI_ADAPTER_DESC desc;
    pAdapter->GetDesc(&desc);

    // Convert wide string to narrow string
    char name[128];
    WideCharToMultiByte(CP_UTF8, 0, desc.Description, -1, name, sizeof(name), NULL, NULL);
    hw_info.gpu_name = name;

    // Determine vendor from VendorId
    switch (desc.VendorId) {
      case 0x10DE:
        hw_info.gpu_vendor = HardwareInfo::GPUVendor::NVIDIA;
        break;
      case 0x1002:
        hw_info.gpu_vendor = HardwareInfo::GPUVendor::AMD;
        break;
      case 0x8086:
        hw_info.gpu_vendor = HardwareInfo::GPUVendor::Intel;
        break;
      default:
        hw_info.gpu_vendor = HardwareInfo::GPUVendor::None;
    }

    // Get VRAM size (in bytes, convert to GB)
    hw_info.gpu_vram_total_gb = desc.DedicatedVideoMemory / (1024.0f * 1024.0f * 1024.0f);

    // GPU monitoring is available on Windows (via PDH)
    hw_info.gpu_tool_available = true;

    pAdapter->Release();
    pFactory->Release();
#elif __APPLE__
    // macOS: Simplified GPU detection
    hw_info.gpu_name = "Apple GPU";
    hw_info.gpu_vendor = HardwareInfo::GPUVendor::Apple;
    
    // Try to get GPU VRAM (Metal doesn't expose this easily)
    uint64_t vram = 0;
    size_t len = sizeof(vram);
    if (sysctlbyname("hw.gpumem_total", &vram, &len, nullptr, 0) == 0) {
      hw_info.gpu_vram_total_gb = vram / (1024.0f * 1024.0f * 1024.0f);
    }
    hw_info.gpu_tool_available = false; // Metal doesn't provide easy GPU monitoring
#else // Linux
    // Linux: Simplified GPU detection (would need to parse /proc or use libdrm in real implementation)
    hw_info.gpu_name = "Unknown GPU";
    hw_info.gpu_vendor = HardwareInfo::GPUVendor::None;
    hw_info.gpu_vram_total_gb = 0.0f;
    hw_info.gpu_tool_available = false;
#endif
  }

  // ============================================================================
  // PDH INITIALIZATION
  // ============================================================================

  void InitializePDH() {
#ifdef _WIN32
    // Open PDH query
    PDH_STATUS status = PdhOpenQuery(NULL, 0, &pdh_query);
    assert(status == ERROR_SUCCESS);

    // Add per-core CPU counters
    pdh_cpu_counters.resize(hw_info.cpu_logical_cores);
    for (int i = 0; i < hw_info.cpu_logical_cores; i++) {
      wchar_t counterPath[256];
      swprintf(counterPath, 256, L"\\Processor(%d)\\%% Processor Time", i);
      status = PdhAddCounterW(pdh_query, counterPath, 0, &pdh_cpu_counters[i]);
      assert(status == ERROR_SUCCESS);
    }

    // Add disk counters
    status = PdhAddCounterW(pdh_query, L"\\PhysicalDisk(_Total)\\Disk Read Bytes/sec", 0, &pdh_disk_read_counter);
    assert(status == ERROR_SUCCESS);
    
    status = PdhAddCounterW(pdh_query, L"\\PhysicalDisk(_Total)\\Disk Write Bytes/sec", 0, &pdh_disk_write_counter);
    assert(status == ERROR_SUCCESS);
    
    status = PdhAddCounterW(pdh_query, L"\\PhysicalDisk(_Total)\\% Disk Time", 0, &pdh_disk_busy_counter);
    assert(status == ERROR_SUCCESS);

    // Initial collect (first call always returns 0)
    PdhCollectQueryData(pdh_query);
#else
    // Non-Windows platforms: Initialize empty PDH structures
    // Real monitoring is done in UpdateCPUUsage, UpdateMemoryUsage, UpdateDiskUsage
#endif
  }

  // ============================================================================
  // DYNAMIC MONITORING (RUN IN LOOP)
  // ============================================================================

  void UpdateDynamicMonitoring() {
    auto now = std::chrono::steady_clock::now();

    // Collect PDH data for all counters (Windows only)
#ifdef _WIN32
    PdhCollectQueryData(pdh_query);
#endif

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
#ifdef _WIN32
    for (int i = 0; i < hw_info.cpu_logical_cores; i++) {
      PDH_FMT_COUNTERVALUE counterValue;
      PDH_STATUS status = PdhGetFormattedCounterValue(pdh_cpu_counters[i], PDH_FMT_DOUBLE, NULL, &counterValue);
      
      float usage = 0.0f;
      if (status == ERROR_SUCCESS) {
        usage = static_cast<float>(counterValue.doubleValue);
        // Clamp to valid range
        if (usage < 0.0f) usage = 0.0f;
        if (usage > 100.0f) usage = 100.0f;
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
#else
    // Non-Windows: Placeholder implementation
    // TODO: Implement CPU monitoring using /proc/stat on Linux, mach on macOS
#endif
  }

  // ----------------------------------------------------------------------------
  // Memory Usage Update
  // ----------------------------------------------------------------------------

  void UpdateMemoryUsage() {
#ifdef _WIN32
    MEMORYSTATUSEX memInfo;
    memInfo.dwLength = sizeof(MEMORYSTATUSEX);
    
    if (GlobalMemoryStatusEx(&memInfo)) {
      ULONGLONG used_ram = memInfo.ullTotalPhys - memInfo.ullAvailPhys;
      mem_data.used_gb_current = used_ram / (1024.0f * 1024.0f * 1024.0f);
      mem_data.usage_percent_current = static_cast<float>(memInfo.dwMemoryLoad);
      mem_data.usage_percent_history[history_write_index] = mem_data.usage_percent_current;
    }
#elif __APPLE__
    struct mach_task_basic_info info;
    mach_msg_type_number_t info_count = MACH_TASK_BASIC_INFO_COUNT;
    if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO, (task_info_t)&info, &info_count) == KERN_SUCCESS) {
      // This gives process memory, not system memory - simplified approach
      mem_data.usage_percent_current = 50.0f; // TODO: Calculate actual system memory usage
      mem_data.usage_percent_history[history_write_index] = mem_data.usage_percent_current;
    }
#else // Linux
    struct sysinfo info;
    if (sysinfo(&info) == 0) {
      uint64_t used = info.totalram - info.freeram;
      mem_data.usage_percent_current = (float)(used * 100.0 / info.totalram);
      mem_data.used_gb_current = used / (1024.0f * 1024.0f * 1024.0f);
      mem_data.usage_percent_history[history_write_index] = mem_data.usage_percent_current;
    }
#endif
  }

  // ----------------------------------------------------------------------------
  // GPU Usage Update
  // ----------------------------------------------------------------------------

  void UpdateGPUUsage(std::chrono::steady_clock::time_point now) {
    if (hw_info.gpu_vendor == HardwareInfo::GPUVendor::None || !hw_info.gpu_tool_available) {
      return;
    }

    // For Windows, GPU monitoring is simplified - we'll use estimation based on DXGI
    // A proper implementation would require vendor-specific APIs (NVML, ADL, etc.)
    // For now, set basic values to prevent crashes
    float usage = 0.0f;
    float memory_used_mb = 0.0f;

    // Note: Windows doesn't expose GPU usage easily without vendor SDKs
    // This is a placeholder that maintains the UI structure
    // Real implementation would need NVML (NVIDIA), ADL (AMD), etc.

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

  // ----------------------------------------------------------------------------
  // Network Usage Update
  // ----------------------------------------------------------------------------

  void UpdateNetworkUsage() {
#ifdef _WIN32
    ULONG64 total_rx = 0;
    ULONG64 total_tx = 0;

    MIB_IF_TABLE2* pIfTable = nullptr;
    DWORD dwRetVal = GetIfTable2(&pIfTable);
    
    if (dwRetVal == NO_ERROR && pIfTable) {
      for (ULONG i = 0; i < pIfTable->NumEntries; i++) {
        MIB_IF_ROW2* pIfRow = &pIfTable->Table[i];
        
        // Skip loopback
        if (pIfRow->Type == IF_TYPE_SOFTWARE_LOOPBACK) {
          continue;
        }
        
        // Only count physical network interfaces
        if (pIfRow->Type == IF_TYPE_ETHERNET_CSMACD ||
            pIfRow->Type == IF_TYPE_IEEE80211) {
          total_rx += pIfRow->InOctets;
          total_tx += pIfRow->OutOctets;
        }
      }
      FreeMibTable(pIfTable);
    }

    // Calculate speed (bytes per 100ms -> Mbps)
    if (net_data.rx_bytes_prev > 0) {
      uint64_t rx_diff = (total_rx >= net_data.rx_bytes_prev) ? (total_rx - net_data.rx_bytes_prev) : 0;
      uint64_t tx_diff = (total_tx >= net_data.tx_bytes_prev) ? (total_tx - net_data.tx_bytes_prev) : 0;

      net_data.rx_mbps_current = (rx_diff * 10.0f * 8.0f) / (1024.0f * 1024.0f);
      net_data.tx_mbps_current = (tx_diff * 10.0f * 8.0f) / (1024.0f * 1024.0f);

      // Use practical max of 80 Mbps (10 MB/s) for utilization percentage
      constexpr float PRACTICAL_MAX_MBPS = 80.0f;
      net_data.rx_percent_current = std::min(100.0f, (net_data.rx_mbps_current / PRACTICAL_MAX_MBPS) * 100.0f);
      net_data.tx_percent_current = std::min(100.0f, (net_data.tx_mbps_current / PRACTICAL_MAX_MBPS) * 100.0f);
    }

    net_data.rx_bytes_prev = total_rx;
    net_data.tx_bytes_prev = total_tx;
#else
    // Non-Windows: Placeholder network monitoring
    // TODO: Implement using /proc/net/dev on Linux, netstat on macOS
#endif
    net_data.rx_percent_history[history_write_index] = net_data.rx_percent_current;
    net_data.tx_percent_history[history_write_index] = net_data.tx_percent_current;
  }
  // ----------------------------------------------------------------------------
  // Disk Usage Update
  // ----------------------------------------------------------------------------

  void UpdateDiskUsage() {
#ifdef _WIN32
    // Get disk read/write speeds from PDH
    PDH_FMT_COUNTERVALUE readValue, writeValue, busyValue;
    
    PDH_STATUS status = PdhGetFormattedCounterValue(pdh_disk_read_counter, PDH_FMT_DOUBLE, NULL, &readValue);
    if (status == ERROR_SUCCESS) {
      disk_data.read_mbps_current = static_cast<float>(readValue.doubleValue / (1024.0 * 1024.0));
    }
    
    status = PdhGetFormattedCounterValue(pdh_disk_write_counter, PDH_FMT_DOUBLE, NULL, &writeValue);
    if (status == ERROR_SUCCESS) {
      disk_data.write_mbps_current = static_cast<float>(writeValue.doubleValue / (1024.0 * 1024.0));
    }
    
    status = PdhGetFormattedCounterValue(pdh_disk_busy_counter, PDH_FMT_DOUBLE, NULL, &busyValue);
    if (status == ERROR_SUCCESS) {
      disk_data.busy_percent_current = static_cast<float>(busyValue.doubleValue);
      // Clamp to valid range
      if (disk_data.busy_percent_current < 0.0f) disk_data.busy_percent_current = 0.0f;
      if (disk_data.busy_percent_current > 100.0f) disk_data.busy_percent_current = 100.0f;
    }
#else
    // Non-Windows: Placeholder disk monitoring
    // TODO: Implement using /proc/diskstats on Linux, iostat on macOS
#endif

    // Update historical max
    disk_data.read_mbps_max = std::max(disk_data.read_mbps_max * 0.995f, disk_data.read_mbps_current);
    disk_data.write_mbps_max = std::max(disk_data.write_mbps_max * 0.995f, disk_data.write_mbps_current);
    disk_data.read_mbps_max = std::max(10.0f, disk_data.read_mbps_max);
    disk_data.write_mbps_max = std::max(10.0f, disk_data.write_mbps_max);

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
    const ImVec4 COLOR_WARNING = ImVec4(1.0f, 0.8f, 0.2f, 1.0f);
    const ImVec4 COLOR_INFO = ImVec4(0.5f, 0.8f, 1.0f, 1.0f);
    const ImVec4 COLOR_DIM = ImVec4(0.5f, 0.5f, 0.5f, 1.0f);
    const ImVec4 COLOR_COMMENT = ImVec4(0.5f, 0.7f, 0.9f, 1.0f);

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
    ImGui::TextColored(COLOR_VALUE, "%d Logical(%d Physical)", hw_info.cpu_logical_cores, hw_info.cpu_physical_cores);

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
    }

    // ========== ISA: Instruction Set Architecture ==========
    ImGui::Spacing();
    ImGui::Separator();
    ImGui::Spacing();

    ImGui::TextColored(COLOR_LABEL, "ISA:");
    ImGui::SameLine(0, 4);
    ImGui::TextColored(COLOR_COMMENT, "Instruction Set Architecture (x64 vs AArch64)");

    RenderCPUInstructionSets();

    ImGui::EndGroup();
  }

  // ----------------------------------------------------------------------------
  // CPU Instruction Sets Rendering (categorized, side-by-side, dual-architecture)
  // ----------------------------------------------------------------------------

  void RenderCPUInstructionSets() {
    const ImVec4 COLOR_LABEL = ImVec4(0.65f, 0.65f, 0.65f, 1.0f);
    const ImVec4 COLOR_GOOD = ImVec4(0.2f, 1.0f, 0.3f, 1.0f);    // Green - available (standard)
    const ImVec4 COLOR_APPLE = ImVec4(1.0f, 0.9f, 0.2f, 1.0f);   // Yellow - Apple proprietary
    const ImVec4 COLOR_MISSING = ImVec4(0.3f, 0.3f, 0.3f, 1.0f); // Gray - not available
    const ImVec4 COLOR_COMMENT = ImVec4(0.5f, 0.7f, 0.9f, 1.0f);

    bool is_x64 = (hw_info.cpu_architecture == "x86_64" || hw_info.cpu_architecture == "i686");
    bool is_aarch64 = (hw_info.cpu_architecture == "aarch64" || hw_info.cpu_architecture == "arm64");

    struct Extension {
      const char *name;
      bool has_ext;
      bool is_proprietary = false; // Apple-only feature (default: false)
    };

    // Fixed column widths for alignment
    const float CATEGORY_WIDTH = 70.0f;
    const float COMMENT_WIDTH = 200.0f;
    const float X64_LABEL_POS = CATEGORY_WIDTH + COMMENT_WIDTH;
    const float AARCH64_LABEL_POS = X64_LABEL_POS + 300.0f; // Reserve space for x64 extensions

    auto render_isa_row = [&](const char *category_label, const char *comment,
                              const std::vector<Extension> &x64_exts,
                              const std::vector<Extension> &aarch64_exts) {
      float start_x = ImGui::GetCursorPosX();

      // Category label (fixed width)
      ImGui::TextColored(COLOR_LABEL, "%s", category_label);

      // Comment (fixed position)
      ImGui::SameLine(start_x + CATEGORY_WIDTH);
      ImGui::TextColored(COLOR_COMMENT, "%s", comment);

      // x64 column (fixed position)
      ImGui::SameLine(start_x + X64_LABEL_POS);
      ImGui::TextColored(COLOR_LABEL, "x64:");
      ImGui::SameLine(0, 2);
      bool first = true;
      for (const auto &ext : x64_exts) {
        if (!first) {
          ImGui::SameLine(0, 2);
        }
        ImVec4 color = ext.has_ext ? (is_x64 ? COLOR_GOOD : COLOR_MISSING) : COLOR_MISSING;
        if (!is_x64 && ext.has_ext)
          color.w = 0.5f; // Semi-transparent if not current arch
        ImGui::TextColored(color, "%s", ext.name);
        first = false;
      }

      // AArch64 column (fixed position)
      ImGui::SameLine(start_x + AARCH64_LABEL_POS);
      ImGui::TextColored(COLOR_LABEL, "AArch64:");
      ImGui::SameLine(0, 2);
      first = true;
      for (const auto &ext : aarch64_exts) {
        if (!first) {
          ImGui::SameLine(0, 2);
        }
        ImVec4 color;
        if (ext.has_ext) {
          if (is_aarch64) {
            color = ext.is_proprietary ? COLOR_APPLE : COLOR_GOOD;
          } else {
            color = COLOR_MISSING;
            color.w = 0.5f; // Semi-transparent if not current arch
          }
        } else {
          color = COLOR_MISSING;
        }
        ImGui::TextColored(color, "%s", ext.name);
        first = false;
      }
    };

    // Legacy SIMD (128-bit)
    render_isa_row("Legacy:", "128-bit SIMD",
                   {{"SSE", hw_info.x64_isa.sse}, {"SSE2", hw_info.x64_isa.sse2}, {"SSE3", hw_info.x64_isa.sse3}, {"SSSE3", hw_info.x64_isa.ssse3}, {"SSE4.1", hw_info.x64_isa.sse4_1}, {"SSE4.2", hw_info.x64_isa.sse4_2}},
                   {{"NEON", hw_info.aarch64_isa.neon}, {"FP16", hw_info.aarch64_isa.neon_fp16}, {"DotProd", hw_info.aarch64_isa.neon_dotprod}, {"FCMA", hw_info.aarch64_isa.neon_fcma}});

    // SIMD Main (256/512-bit or scalable)
    render_isa_row("SIMD:", "256+bit wide vector",
                   {{"AVX", hw_info.x64_isa.avx}, {"AVX2", hw_info.x64_isa.avx2}, {"FMA", hw_info.x64_isa.fma}, {"F16C", hw_info.x64_isa.f16c}},
                   {{"SVE", hw_info.aarch64_isa.sve}, {"SVE2", hw_info.aarch64_isa.sve2}, {"SME", hw_info.aarch64_isa.sme}, {"SME2", hw_info.aarch64_isa.sme2}});

    // AVX512 / Advanced SIMD
    render_isa_row("AVX512:", "512-bit SIMD modules",
                   {{"F", hw_info.x64_isa.avx512f}, {"CD", hw_info.x64_isa.avx512cd}, {"VL", hw_info.x64_isa.avx512vl}, {"BW", hw_info.x64_isa.avx512bw}, {"DQ", hw_info.x64_isa.avx512dq}},
                   {{"FP64", hw_info.aarch64_isa.fp64}, {"BF16", hw_info.aarch64_isa.neon_bf16}, {"I8MM", hw_info.aarch64_isa.neon_i8mm}});

    // AI/ML Acceleration
    render_isa_row("AI/ML:", "FP16/BF16/INT8 accel",
                   {{"FP16", hw_info.x64_isa.avx512_fp16}, {"BF16", hw_info.x64_isa.avx512_bf16}, {"VNNI", hw_info.x64_isa.avx512_vnni}, {"AVX-VNNI", hw_info.x64_isa.avx_vnni}, {"AMX", hw_info.x64_isa.amx_tile}},
                   {{"BF16", hw_info.aarch64_isa.bf16}, {"I8MM", hw_info.aarch64_isa.i8mm}, {"AMX", hw_info.aarch64_isa.amx, true}, {"NeuralEngine", hw_info.aarch64_isa.neural_engine, true}});

    // Crypto/Hash
    render_isa_row("Crypto:", "AES/SHA hardware accel",
                   {{"AES", hw_info.x64_isa.aes}, {"SHA", hw_info.x64_isa.sha}, {"GFNI", hw_info.x64_isa.gfni}, {"IFMA", hw_info.x64_isa.avx512_ifma}},
                   {{"AES", hw_info.aarch64_isa.aes}, {"SHA1", hw_info.aarch64_isa.sha1}, {"SHA2", hw_info.aarch64_isa.sha2}, {"SHA3", hw_info.aarch64_isa.sha3}, {"PMULL", hw_info.aarch64_isa.pmull}, {"CRC32", hw_info.aarch64_isa.crc32}});

    // Memory/Cache
    render_isa_row("Memory:", "cache/prefetch ops",
                   {{"PREFETCHW", hw_info.x64_isa.prefetchw}, {"CLFLUSHOPT", hw_info.x64_isa.clflushopt}, {"CLWB", hw_info.x64_isa.clwb}, {"MOVDIR64B", hw_info.x64_isa.movdir64b}, {"RTM", hw_info.x64_isa.rtm}},
                   {{"PREFETCH", hw_info.aarch64_isa.prefetch}, {"DC_ZVA", hw_info.aarch64_isa.dc_zva}});

    // System
    render_isa_row("System:", "RNG/security features",
                   {{"RDRAND", hw_info.x64_isa.rdrand}, {"RDSEED", hw_info.x64_isa.rdseed}},
                   {{"RNDR", hw_info.aarch64_isa.rndr}, {"RNDRRS", hw_info.aarch64_isa.rndrrs}, {"PAC", hw_info.aarch64_isa.pac}, {"MTE", hw_info.aarch64_isa.mte}});
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

} // namespace

TaskHandle CreateSystemInfoTask() {
  auto instance = std::make_shared<SystemInfoTask>();

  TaskHandle handle;
  handle.name = instance->GetName();
  handle.task_instance = instance.get();
  handle.storage = instance;
  handle.OnExpand = [instance]() { instance->OnExpand(); };
  handle.OnCollapse = [instance]() { instance->OnCollapse(); };
  handle.DrawPanel = [instance](SharedData &data) { instance->DrawPanel(data); };
  handle.Destroy = [instance]() mutable { instance.reset(); };

  return handle;
}

} // namespace GUI::Tasks
