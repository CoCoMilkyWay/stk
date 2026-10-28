#pragma once

#include <cassert>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#ifdef _WIN32
#include <windows.h>
#elif defined(__linux__)
#include <pthread.h>
#include <sched.h>
#endif

namespace misc {

// 当前线程名 (Linux comm, ≤ 15 字节): perf / top / gdb 按它区分线程
inline void name_thread(const std::string &name) {
  assert(name.size() <= 15 && "线程名超过 comm 上限 15 字节");
#if defined(__linux__)
  [[maybe_unused]] const int rc = pthread_setname_np(pthread_self(), name.c_str());
  assert(rc == 0 && "pthread_setname_np 失败");
#endif
}

class Affinity {
public:
  // Set current thread to single core
  static bool pin_to_core(unsigned int core_id) {
    return pin_to_cores({core_id});
  }

  // Set current thread to multiple cores
  static bool pin_to_cores(const std::vector<unsigned int> &cores) {
    if (!validate_cores(cores))
      return false;
#ifdef _WIN32
    return SetThreadAffinityMask(GetCurrentThread(), make_mask(cores)) != 0;
#elif defined(__linux__)
    const cpu_set_t set = make_cpuset(cores);
    return pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &set) == 0;
#else
    return false;
#endif
  }

  // Set specific thread to single core
  static bool pin_thread(std::thread::native_handle_type thread, unsigned int core_id) {
    return pin_thread_cores(thread, {core_id});
  }

  // Set specific thread to multiple cores
  static bool pin_thread_cores(std::thread::native_handle_type thread, const std::vector<unsigned int> &cores) {
    if (!validate_cores(cores))
      return false;
#ifdef _WIN32
    return SetThreadAffinityMask(thread, make_mask(cores)) != 0;
#elif defined(__linux__)
    const cpu_set_t set = make_cpuset(cores);
    return pthread_setaffinity_np(thread, sizeof(cpu_set_t), &set) == 0;
#else
    return false;
#endif
  }

  // Get number of CPU cores
  static unsigned int core_count() {
    unsigned int count = std::thread::hardware_concurrency();
    return count > 0 ? count : 1;
  }

  // 物理核拓扑: [物理核][该核上的逻辑 CPU 号 (升序)], 物理核按首个逻辑号升序; 进程内只扫一次.
  // Linux 读 sysfs thread_siblings_list; 其他平台一逻辑 CPU 当一物理核 (不区分超线程).
  static const std::vector<std::vector<unsigned int>> &physical_cores() {
    static const std::vector<std::vector<unsigned int>> cores = scan_physical_cores();
    return cores;
  }

  // Check if affinity is supported
  static bool supported() {
#if defined(_WIN32) || defined(__linux__)
    return true;
#else
    return false;
#endif
  }

private:
  static bool validate_cores(const std::vector<unsigned int> &cores) {
    if (cores.empty())
      return false;
    unsigned int max_cores = core_count();
    for (unsigned int core : cores) {
      if (core >= max_cores)
        return false;
    }
    return true;
  }

#ifdef _WIN32
  static DWORD_PTR make_mask(const std::vector<unsigned int> &cores) {
    DWORD_PTR mask = 0;
    for (unsigned int core : cores) {
      mask |= (static_cast<DWORD_PTR>(1) << core);
    }
    return mask;
  }
#elif defined(__linux__)
  // 按值返回: 各 worker 启动时并发 pin, 共享的 static cpuset 会互相覆盖
  static cpu_set_t make_cpuset(const std::vector<unsigned int> &cores) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    for (unsigned int core : cores) {
      CPU_SET(core, &cpuset);
    }
    return cpuset;
  }
#endif

  static std::vector<std::vector<unsigned int>> scan_physical_cores() {
    const unsigned int n = core_count();
    std::vector<std::vector<unsigned int>> cores;
    for (unsigned int cpu = 0; cpu < n; ++cpu) {
      std::vector<unsigned int> siblings;
#if defined(__linux__)
      // 格式 "34,70" / "0-1" (升序区间列表)
      std::ifstream f("/sys/devices/system/cpu/cpu" + std::to_string(cpu) + "/topology/thread_siblings_list");
      assert(f && "读不到 CPU 拓扑 (sysfs thread_siblings_list)");
      unsigned int lo = 0, hi = 0;
      while (f >> lo) {
        hi = lo;
        if (f.peek() == '-') {
          f.get();
          f >> hi;
        }
        for (unsigned int c = lo; c <= hi; ++c)
          siblings.push_back(c);
        if (f.peek() == ',')
          f.get();
      }
#else
      siblings.push_back(cpu);
#endif
      assert(!siblings.empty() && siblings.front() <= cpu && siblings.back() < n && "CPU 拓扑与在线逻辑 CPU 数不符");
      if (siblings.front() == cpu)
        cores.push_back(std::move(siblings));
    }
    return cores;
  }
};

} // namespace misc