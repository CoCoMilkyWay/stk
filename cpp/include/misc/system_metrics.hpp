#pragma once

#include <chrono>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

// 系统监控采集层。GUI 的 SystemInfo 面板与左下角 IconBar 共用这一份数据,
// 不再各自去读一遍 /proc (Windows 下也不再各开一个 PDH query)。
//
// 机器的算力要留给编码/特征计算, 监控本身必须近乎免费:
//   * 静态信息 (OS / CPU 型号 / 缓存 / 指令集 / 内存总量) 只在构造时探测一次;
//   * 每个数据源各记各的上次采样时刻, 没到点 poll() 就只是一次时间比较, 零系统调用;
//   * Linux 下 /proc 句柄常驻, 采样 = lseek(0) + read + 手写整数解析, 稳态零堆分配;
//   * GPU 走 NVML, 只有 SystemInfo 面板真被打开 (Scope::Full) 时才 dlopen 并初始化。
//
// 线程约束: 只在 GUI 线程调用, 内部无锁。
namespace misc::sysmon {

// ============================================================================
// 静态信息
// ============================================================================

enum class CpuArch { Unknown,
                     X86_64,
                     AArch64 };

enum class GpuVendor { None,
                       NVIDIA,
                       AMD,
                       Intel,
                       Apple };

// 指令集对照表: 一行一个类别, 左右两列分别是 x64 / AArch64 里对应的扩展。
// 非当前架构的那一列恒为 present=false, 保留是为了两种架构的能力对照。
struct IsaFeature {
  const char *name = nullptr;
  bool present = false;
  bool proprietary = false; // 厂商私有 (Apple AMX / ANE), UI 用另一种颜色
};

struct IsaRow {
  const char *category = nullptr;
  const char *comment = nullptr;
  std::vector<IsaFeature> x64;
  std::vector<IsaFeature> arm;
};

struct StaticInfo {
  // OS
  std::string os_name;
  std::string kernel_version;
  std::string hostname;

  // CPU
  CpuArch arch = CpuArch::Unknown;
  std::string arch_name;
  std::string cpu_vendor;
  std::string cpu_model;
  int logical_cores = 0;
  int physical_cores = 0;
  long cache_l1d_kb = 0;
  long cache_l2_kb = 0;
  long cache_l3_kb = 0;
  std::vector<IsaRow> isa;

  // 内存
  std::size_t ram_total_bytes = 0;

  // 网络 / 磁盘: 是否拿得到计数器 (拿不到时 UI 显示 N/A, 而不是画一条 0 线)
  bool net_available = false;
  bool disk_available = false;

  // GPU: 首次 Scope::Full 采样时才探测, 在那之前 gpu_probed=false
  bool gpu_probed = false;
  GpuVendor gpu_vendor = GpuVendor::None;
  std::string gpu_name;
  std::size_t vram_total_bytes = 0;
  bool gpu_usage_available = false;
  bool vram_available = false;
};

// ============================================================================
// 动态采样
// ============================================================================

// Basic: CPU (总量 + 每核) + 内存, 给 IconBar。
// Full : 追加 GPU / 网络 / 磁盘, 给 SystemInfo 面板。
enum class Scope { Basic,
                   Full };

// 所有 *_percent 均为 0..100。速率单位统一为 MiB/s (网络按 Mib/s, 与网卡额定速率同口径)。
struct Sample {
  // CPU
  float cpu_total_percent = 0.0f;
  std::vector<float> cpu_core_percent; // size == StaticInfo::logical_cores

  // 内存
  float mem_used_percent = 0.0f;
  std::size_t mem_used_bytes = 0;

  // GPU
  float gpu_percent = 0.0f;
  float vram_percent = 0.0f;
  std::size_t vram_used_bytes = 0;

  // 网络 (所有物理网卡合计)。scale = 链路额定速率, 拿不到时退化为自适应峰值。
  float net_rx_mbps = 0.0f;
  float net_tx_mbps = 0.0f;
  float net_rx_percent = 0.0f;
  float net_tx_percent = 0.0f;
  float net_scale_mbps = 0.0f;

  // 磁盘 (所有物理盘合计; busy 取各盘最大值)。scale = 自适应峰值。
  float disk_read_mbps = 0.0f;
  float disk_write_mbps = 0.0f;
  float disk_read_percent = 0.0f;
  float disk_write_percent = 0.0f;
  float disk_scale_mbps = 0.0f;
  float disk_busy_percent = 0.0f;
};

// ============================================================================
// 采集器
// ============================================================================

class Monitor {
public:
  static Monitor &instance();

  Monitor(const Monitor &) = delete;
  Monitor &operator=(const Monitor &) = delete;

  const StaticInfo &info() const { return info_; }
  const Sample &sample() const { return sample_; }

  // 到点才真正采样。各数据源独立计时, 所以两个调用方各按各的间隔请求互不干扰,
  // 实际刷新率 = 两者中较高的那个。
  void poll(Scope scope, std::chrono::milliseconds interval);

private:
  Monitor();
  ~Monitor();

  struct Backend;
  std::unique_ptr<Backend> backend_;
  StaticInfo info_;
  Sample sample_;

  std::chrono::steady_clock::time_point cpu_at_{};
  std::chrono::steady_clock::time_point io_at_{};
  std::chrono::steady_clock::time_point gpu_at_{};
};

} // namespace misc::sysmon
