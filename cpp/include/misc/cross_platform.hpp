#pragma once

#include <cassert>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <ctime>

#include <fcntl.h>

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <io.h>
#include <windows.h>
#elif defined(__APPLE__)
#include <mach/mach.h>
#include <sys/sysctl.h>
#include <unistd.h>
#else
#include <unistd.h>
#endif

// 读文件开头 n 字节, 返回实际读到的字节数 (0 = 打不开或读不到).
//
// 走裸 open/pread 而不是 std::ifstream: 全库扫描要给五百万个 .bin 各读一次
// 32 字节头, 每个文件构造一次 ifstream (含 streambuf 分配) 实测把这一步从
// 4.2s 拖到 15s. 451 万文件 / 72 线程的实测:
//   readdir only          0.11s
//   ifstream + file_size 17.65s
//   ifstream (无 stat)   14.94s
//   pread    (无 stat)    4.25s
inline std::size_t read_file_head(const char *path, void *buf, std::size_t n) {
#ifdef _WIN32
  int fd = _open(path, _O_RDONLY | _O_BINARY);
  if (fd < 0)
    return 0;
  const int got = _read(fd, buf, static_cast<unsigned int>(n));
  _close(fd);
  return got > 0 ? static_cast<std::size_t>(got) : 0;
#else
  const int fd = ::open(path, O_RDONLY);
  if (fd < 0)
    return 0;
  const ssize_t got = ::pread(fd, buf, n, 0);
  ::close(fd);
  return got > 0 ? static_cast<std::size_t>(got) : 0;
#endif
}

// Cross-platform localtime (thread-safe)
inline std::tm safe_localtime(const std::time_t *timer) {
  std::tm result{};
#ifdef _WIN32
  localtime_s(&result, timer);
#else
  localtime_r(timer, &result);
#endif
  return result;
}

// Cross-platform process id (临时文件唯一命名用)
inline int safe_getpid() {
#ifdef _WIN32
  return static_cast<int>(GetCurrentProcessId());
#else
  return static_cast<int>(getpid());
#endif
}

// Cross-platform popen/pclose
inline FILE *safe_popen(const char *command, const char *mode) {
#ifdef _WIN32
  return _popen(command, mode);
#else
  return popen(command, mode);
#endif
}

inline int safe_pclose(FILE *stream) {
#ifdef _WIN32
  return _pclose(stream);
#else
  return pclose(stream);
#endif
}

// 物理内存总量 (字节). 用于按机器规模决定预读缓冲上限.
inline std::size_t physical_ram_bytes() {
#ifdef _WIN32
  MEMORYSTATUSEX status;
  status.dwLength = sizeof(status);
  GlobalMemoryStatusEx(&status);
  return static_cast<std::size_t>(status.ullTotalPhys);
#elif defined(__APPLE__)
  std::size_t bytes = 0;
  std::size_t len = sizeof(bytes);
  int mib[2] = {CTL_HW, HW_MEMSIZE};
  sysctl(mib, 2, &bytes, &len, nullptr, 0);
  return bytes;
#else
  const long pages = sysconf(_SC_PHYS_PAGES);
  const long page_size = sysconf(_SC_PAGE_SIZE);
  return static_cast<std::size_t>(pages) * static_cast<std::size_t>(page_size);
#endif
}

// 内存占用 (已用, 总量), 字节.
//
// Linux 下"已用"必须按 MemTotal - MemAvailable 算, 不能用 sysinfo().freeram:
// freeram 不含可回收的 page cache, 而全库扫描/编码会把几乎全部空闲内存变成
// 归档与 .bin 的页缓存 (本机 131GB 里 75GB 是 Cached, MemFree 只剩 0.8GB),
// freeram 口径于是长期误报 ~99%, 而内核估算的真实可用量 MemAvailable 还有
// 82GB. MemAvailable 需要 Linux 3.14+.
struct MemoryUsage {
  std::size_t used_bytes = 0;
  std::size_t total_bytes = 0;

  float used_percent() const {
    assert(total_bytes > 0);
    return static_cast<float>(used_bytes * 100.0 / total_bytes);
  }
};

inline MemoryUsage memory_usage() {
#ifdef _WIN32
  MEMORYSTATUSEX status;
  status.dwLength = sizeof(status);
  GlobalMemoryStatusEx(&status);
  return {static_cast<std::size_t>(status.ullTotalPhys - status.ullAvailPhys),
          static_cast<std::size_t>(status.ullTotalPhys)};
#elif defined(__APPLE__)
  // Linux 的 MemAvailable 在这里的对应物: 空闲 + 非活跃 + 可清除 + 文件页,
  // 都是内核在有压力时能立刻收回的部分.
  vm_statistics64_data_t vm{};
  mach_msg_type_number_t count = HOST_VM_INFO64_COUNT;
  const kern_return_t kr =
      host_statistics64(mach_host_self(), HOST_VM_INFO64,
                        reinterpret_cast<host_info64_t>(&vm), &count);
  assert(kr == KERN_SUCCESS && "host_statistics64 失败");
  (void)kr;
  const std::size_t page = static_cast<std::size_t>(vm_kernel_page_size);
  const std::size_t total = physical_ram_bytes();
  const std::size_t available =
      (static_cast<std::size_t>(vm.free_count) + vm.inactive_count +
       vm.purgeable_count + vm.external_page_count) *
      page;
  assert(available <= total);
  return {total - available, total};
#else
  // MemTotal / MemFree / MemAvailable 固定是 /proc/meminfo 的前三行, 512B 足够.
  // 走 read_file_head 而不是 fopen: stdio 每次打开都要 malloc 一个 4KB 缓冲区,
  // 而这个函数在 GUI 里是每秒都要调的.
  char buf[512];
  const std::size_t got = read_file_head("/proc/meminfo", buf, sizeof(buf) - 1);
  assert(got > 0 && "/proc/meminfo 读不到");
  buf[got] = '\0';

  const auto field_kb = [&buf](const char *key) -> std::size_t {
    const char *p = std::strstr(buf, key);
    assert(p && "/proc/meminfo 缺字段 (MemAvailable 需要 Linux 3.14+)");
    unsigned long long value = 0;
    const int matched = std::sscanf(p + std::strlen(key), " %llu", &value);
    assert(matched == 1);
    (void)matched;
    return static_cast<std::size_t>(value);
  };

  const std::size_t total_kb = field_kb("MemTotal:");
  const std::size_t available_kb = field_kb("MemAvailable:");
  assert(total_kb > 0 && available_kb <= total_kb);
  return {(total_kb - available_kb) * 1024, total_kb * 1024};
#endif
}
