#pragma once

#include <cstddef>
#include <cstdio>
#include <ctime>

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#elif defined(__APPLE__)
#include <sys/sysctl.h>
#else
#include <unistd.h>
#endif

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
