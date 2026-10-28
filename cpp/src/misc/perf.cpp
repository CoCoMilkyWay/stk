#include "misc/perf.hpp"

#include <algorithm>
#include <cassert>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <fcntl.h>
#include <fstream>
#include <map>
#include <span>
#include <spawn.h>
#include <string_view>
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

// argv 起子进程: stderr 追加进 log, stdout 进 out (−1 = 同 log).
// setpriv --pdeathsig: 起它的线程一死子进程即被杀, 本进程退出 (含崩溃) 不留残余
pid_t spawn(const std::vector<std::string> &argv, const fs::path &log, int out = -1) {
  std::vector<std::string> full{"setpriv", "--pdeathsig", "KILL", "--"};
  full.insert(full.end(), argv.begin(), argv.end());
  std::vector<char *> args;
  for (std::string &a : full)
    args.push_back(a.data());
  args.push_back(nullptr);
  posix_spawn_file_actions_t fa;
  posix_spawn_file_actions_init(&fa);
  posix_spawn_file_actions_addopen(&fa, STDERR_FILENO, log.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
  posix_spawn_file_actions_adddup2(&fa, out >= 0 ? out : STDERR_FILENO, STDOUT_FILENO);
  pid_t pid = -1;
  [[maybe_unused]] const int rc = posix_spawnp(&pid, args[0], &fa, nullptr, args.data(), environ);
  assert(rc == 0 && "posix_spawnp 失败");
  posix_spawn_file_actions_destroy(&fa);
  return pid;
}

void wait_ok(pid_t pid) {
  int status = 0;
  [[maybe_unused]] const pid_t r = waitpid(pid, &status, 0);
  assert(r == pid && WIFEXITED(status) && WEXITSTATUS(status) == 0 && "perf report 异常退出, 见 perf.log");
}

// perf report 一遍: 每行 = period + sort 键列 (tab 分隔, 列内去空白), 表头 / 空行跳过
template <class F>
void scan(const fs::path &dir, const std::string &sort, F &&row) {
  int fd[2];
  [[maybe_unused]] const int rc = pipe2(fd, O_CLOEXEC);
  assert(rc == 0);
  const pid_t pid = spawn({"perf", "report", "-i", (dir / "perf.data").string(), "--stdio", "--no-children",
                           "--sort", sort, "-F", "period," + sort, "-t", "\t"},
                          dir / "perf.log", fd[1]);
  close(fd[1]);
  std::string out;
  char buf[1 << 16];
  for (ssize_t n; (n = read(fd[0], buf, sizeof(buf))) > 0;)
    out.append(buf, static_cast<size_t>(n));
  close(fd[0]);
  wait_ok(pid);

  std::vector<std::string_view> cols;
  for (size_t at = 0; at < out.size();) {
    const size_t eol = std::min(out.find('\n', at), out.size());
    const std::string_view line(out.data() + at, eol - at);
    at = eol + 1;
    if (line.empty() || line.front() == '#')
      continue;
    cols.clear();
    for (size_t b = 0; b <= line.size();) {
      const size_t e = std::min(line.find('\t', b), line.size());
      std::string_view c = line.substr(b, e - b);
      c.remove_prefix(std::min(c.find_first_not_of(' '), c.size()));
      c.remove_suffix(c.size() - std::min(c.find_last_not_of(' ') + 1, c.size()));
      cols.push_back(c);
      b = e + 1;
    }
    row(std::stoull(std::string(cols[0])), std::span<const std::string_view>(cols).subspan(1));
  }
}

// 热点表: 键 → period. 落盘按 period 降序, 百分比 = 占本表合计, 低于 limit (%) 的不列
using Hot = std::map<std::string, uint64_t>;

void write_hot(const fs::path &path, const std::string &title, const Hot &hot, double limit) {
  uint64_t total = 0;
  for (const auto &[k, p] : hot)
    total += p;
  std::vector<std::pair<uint64_t, const std::string *>> rows;
  for (const auto &[k, p] : hot)
    rows.emplace_back(p, &k);
  std::sort(rows.begin(), rows.end(), [](const auto &a, const auto &b) { return a.first > b.first; });
  std::ofstream f(path);
  assert(f && "报告文件写不开");
  f << "# " << title << "  period " << total << "\n";
  for (const auto &[p, k] : rows) {
    const double pct = total ? 100.0 * static_cast<double>(p) / static_cast<double>(total) : 0.0;
    if (pct < limit)
      break;
    char head[16];
    std::snprintf(head, sizeof(head), "%7.2f%%  ", pct);
    f << head << *k << "\n";
  }
}

std::string dso_sym(std::string_view dso, std::string_view sym) {
  std::string k(dso);
  k.resize(std::max<size_t>(k.size(), 24), ' ');
  return k.append("  ").append(sym);
}

} // namespace

Capture::Capture(const fs::path &dir) : dir_(fs::absolute(dir)) {
  assert(paranoid() <= 1 && "perf 采样须 kernel.perf_event_paranoid ≤ 1: sudo sysctl kernel.perf_event_paranoid=1");
  fs::create_directories(dir_);
  t0_ = t1_ = std::chrono::steady_clock::now();
  pid_ = spawn({"perf", "record", "-F", std::to_string(kFreq), "-p", std::to_string(getpid()), "-o", (dir_ / "perf.data").string()},
               dir_ / "perf.log");
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
  std::map<std::string, size_t, std::less<>> group_of; // comm → 组下标
  for (size_t g = 0; g < groups.size(); ++g) {
    assert(!groups[g].comms.empty());
    for (const std::string &c : groups[g].comms)
      group_of.emplace(c, g);
  }
  auto dumped = [](const Group &g, std::string_view c) { return c == g.comms.front() || c == g.comms.back(); };

  Hot threads;
  std::map<std::string, Hot, std::less<>> per_comm;
  std::vector<Hot> per_group(groups.size()), per_group_src(groups.size());
  scan(dir_, "comm,dso,sym", [&](uint64_t p, std::span<const std::string_view> c) {
    threads[std::string(c[0])] += p;
    if (const auto it = group_of.find(c[0]); it != group_of.end()) {
      const std::string k = dso_sym(c[1], c[2]);
      if (dumped(groups[it->second], c[0]))
        per_comm[it->first][k] += p;
      per_group[it->second][k] += p;
    }
  });
  write_hot(dir_ / "threads.txt", "threads", threads, 0.0);
  for (size_t g = 0; g < groups.size(); ++g) {
    fs::create_directories(dir_ / groups[g].name);
    for (const std::string &c : {groups[g].comms.front(), groups[g].comms.back()})
      write_hot(dir_ / groups[g].name / (c + ".txt"), c, per_comm[c], 0.3);
    write_hot(dir_ / (groups[g].name + ".sym.txt"), groups[g].name, per_group[g], 0.2);
  }

  scan(dir_, "comm,srcline", [&](uint64_t p, std::span<const std::string_view> c) {
    if (const auto it = group_of.find(c[0]); it != group_of.end())
      per_group_src[it->second][std::string(c[1])] += p;
  });
  for (size_t g = 0; g < groups.size(); ++g)
    write_hot(dir_ / (groups[g].name + ".srcline.txt"), groups[g].name, per_group_src[g], 0.2);

  std::ofstream(dir_ / "report.done");
}

} // namespace misc::perf
