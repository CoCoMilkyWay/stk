#include "misc/perf.hpp"

#include <cassert>
#include <csignal>
#include <fcntl.h>
#include <fstream>
#include <spawn.h>
#include <sstream>
#include <sys/wait.h>
#include <unistd.h>

extern char **environ;

namespace misc::perf {

namespace fs = std::filesystem;

namespace {

int paranoid() {
  std::ifstream f("/proc/sys/kernel/perf_event_paranoid");
  int v = 0;
  f >> v;
  assert(f && "读不到 /proc/sys/kernel/perf_event_paranoid");
  return v;
}

// argv 起子进程, stdout / stderr 追加进 log; detach = 新会话 (本进程退出 / Ctrl+C 不连带)
pid_t spawn(const std::vector<std::string> &argv, const fs::path &log, bool detach) {
  std::vector<char *> args;
  for (const std::string &a : argv)
    args.push_back(const_cast<char *>(a.c_str()));
  args.push_back(nullptr);
  posix_spawn_file_actions_t fa;
  posix_spawn_file_actions_init(&fa);
  posix_spawn_file_actions_addopen(&fa, STDOUT_FILENO, log.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
  posix_spawn_file_actions_adddup2(&fa, STDOUT_FILENO, STDERR_FILENO);
  posix_spawnattr_t attr;
  posix_spawnattr_init(&attr);
  if (detach)
    posix_spawnattr_setflags(&attr, POSIX_SPAWN_SETSID);
  pid_t pid = -1;
  [[maybe_unused]] const int rc = posix_spawnp(&pid, args[0], &fa, &attr, args.data(), environ);
  assert(rc == 0 && "posix_spawnp 失败");
  posix_spawnattr_destroy(&attr);
  posix_spawn_file_actions_destroy(&fa);
  return pid;
}

std::string join(const std::vector<std::string> &v, char sep) {
  std::string s;
  for (const std::string &x : v)
    s += (s.empty() ? "" : std::string(1, sep)) + x;
  return s;
}

} // namespace

Capture::Capture(const fs::path &dir) : dir_(fs::absolute(dir)) {
  assert(paranoid() <= 1 && "perf 采样须 kernel.perf_event_paranoid ≤ 1: sudo sysctl kernel.perf_event_paranoid=1");
  fs::create_directories(dir_);
  t0_ = t1_ = std::chrono::steady_clock::now();
  pid_ = spawn({"perf", "record", "-F", std::to_string(kFreq), "-p", std::to_string(getpid()), "-o", (dir_ / "perf.data").string()},
               dir_ / "perf.log", false);
}

Capture::~Capture() { assert(pid_ < 0 && "Capture: 析构前须 stop"); }

double Capture::seconds() const {
  const auto t = pid_ < 0 ? t1_ : std::chrono::steady_clock::now();
  return std::chrono::duration<double>(t - t0_).count();
}

void Capture::stop() {
  if (pid_ < 0)
    return;
  t1_ = std::chrono::steady_clock::now();
  ::kill(pid_, SIGINT);
  int status = 0;
  [[maybe_unused]] const pid_t r = waitpid(pid_, &status, 0);
  assert(r == pid_);
  pid_ = -1;
  // SIGINT 下 perf 落盘后按信号退出; 其余退出 = 没采起来 (未装 perf / 权限)
  assert(((WIFSIGNALED(status) && WTERMSIG(status) == SIGINT) || (WIFEXITED(status) && WEXITSTATUS(status) == 0)) &&
         "perf record 异常退出, 见 perf.log");
  assert(fs::file_size(dir_ / "perf.data") > 0);
}

void Capture::report(const std::vector<Group> &groups) const {
  assert(pid_ < 0 && "report: 须先 stop");
  // 慢的 (srcline 逐地址 addr2line) 排最后; 先出的文件先可读
  const std::string R = "perf report -i perf.data --stdio --no-children --percentage relative";
  std::ostringstream sh;
  sh << "cd '" << dir_.string() << "' || exit 1\n";
  sh << R << " --sort comm > threads.txt 2>/dev/null\n";
  for (const Group &g : groups) {
    sh << "mkdir -p '" << g.name << "'\n";
    for (const std::string &c : g.comms)
      sh << R << " --comms " << c << " --sort dso,sym --percent-limit 0.3 > '" << g.name << "/" << c << ".txt' 2>/dev/null\n";
  }
  for (const Group &g : groups)
    sh << R << " --comms " << join(g.comms, ',') << " --sort dso,sym --percent-limit 0.2 > '" << g.name << ".sym.txt' 2>/dev/null\n";
  for (const Group &g : groups)
    sh << R << " --comms " << join(g.comms, ',') << " --sort srcline --percent-limit 0.2 > '" << g.name << ".srcline.txt' 2>/dev/null\n";
  sh << "touch report.done\n";
  // 外层 sh 把报告脚本甩进后台即退 (收尸即返回), 报告进程归 init 收养, 不留僵尸
  const pid_t pid = spawn({"sh", "-c", "nice -n 19 sh -c \"$1\" &", "sh", sh.str()}, dir_ / "perf.log", true);
  int status = 0;
  [[maybe_unused]] const pid_t r = waitpid(pid, &status, 0);
  assert(r == pid && WIFEXITED(status) && WEXITSTATUS(status) == 0);
}

} // namespace misc::perf
