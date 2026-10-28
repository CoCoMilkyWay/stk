#pragma once

#include <chrono>
#include <filesystem>
#include <string>
#include <sys/types.h>
#include <vector>

// ============================================================================
// perf 采样: 对本进程 perf record 一个采样窗, 停后出文本报告.
//   <dir>/perf.data, perf.log         原始样本 / perf 自身输出
//   <dir>/threads.txt                 全进程按线程名 (comm) 的样本占比
//   <dir>/<组>/<线程>.txt              组首 / 组尾两线程: (dso, 符号) 热点 (同组线程同构, 两端够看离散)
//   <dir>/<组>.sym.txt / .srcline.txt  组内线程合并: 符号 / 源码行热点
//   <dir>/report.done                 报告出齐 (之前读到的是半成品)
// 报告 = perf report 两遍 (comm,dso,sym / comm,srcline), 解析后按线程 / 组拆文件; 百分比 = 占本文件所含线程的样本.
// 子进程 (record / report) 绑起它的线程 (pdeathsig): 线程死即被杀, 本进程退出不留残余.
// 源码行要 -g (PROFILE 模式带). 采样要 kernel.perf_event_paranoid ≤ 1.
// ============================================================================
namespace misc::perf {

#ifdef PROFILE_MODE
inline constexpr bool kEnabled = true;
#else
inline constexpr bool kEnabled = false;
#endif
inline constexpr int kFreq = 999;        // Hz, 每线程
inline constexpr double kSeconds = 30.0; // 采样窗上限: 满载线程 ~3 万样本

// 报告分组: 同一类线程 (组名 = 文件名)
struct Group {
  std::string name;
  std::vector<std::string> comms;
};

class Capture {
public:
  explicit Capture(const std::filesystem::path &dir); // 建 dir, 起 perf record -p <本进程>
  ~Capture();
  Capture(const Capture &) = delete;
  Capture &operator=(const Capture &) = delete;

  double seconds() const;                              // 已采时长 (停后定格)
  void stop();                                         // SIGINT → 等 perf 落盘 (幂等)
  void report(const std::vector<Group> &groups) const; // 出报告, 阻塞到出齐 (须已 stop)

private:
  std::filesystem::path dir_;
  pid_t pid_ = -1;
  std::chrono::steady_clock::time_point t0_, t1_;
};

} // namespace misc::perf
