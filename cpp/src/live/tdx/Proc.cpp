#include "live/tdx/Proc.hpp"

#include "live/tdx/Trace.hpp"

#include <cassert>
#include <cerrno>
#include <cstdlib>
#include <cstring>

#include <fcntl.h>
#include <poll.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

extern char **environ;

namespace live::tdx {

namespace {

constexpr size_t kTail = 600; // 输出记进日志的尾部字节数

const Ran &noted(const std::vector<std::string> &argv, const Ran &r, std::chrono::steady_clock::time_point t0) {
  trace::Kw kw = {{"argv", argv}, {"rc", r.rc}, {"ms", trace::ms_since(t0)}};
  for (const auto &[k, s] : {std::pair{"err", &r.err}, std::pair{"out", &r.out}})
    if (!s->empty())
      kw[k] = s->size() <= kTail ? *s : s->substr(s->size() - kTail);
  trace::ev("run", std::move(kw));
  return r;
}

} // namespace

Ran run(const std::vector<std::string> &argv, const std::string &in, const std::filesystem::path &cwd) {
  assert(!argv.empty());
  const auto t0 = std::chrono::steady_clock::now();
  int pin[2], pout[2], perr[2];
  const int piped = pipe2(pin, O_CLOEXEC) | pipe2(pout, O_CLOEXEC) | pipe2(perr, O_CLOEXEC);
  assert(piped == 0);
  (void)piped;
  posix_spawn_file_actions_t fa;
  posix_spawn_file_actions_init(&fa);
  posix_spawn_file_actions_adddup2(&fa, pin[0], 0);
  posix_spawn_file_actions_adddup2(&fa, pout[1], 1);
  posix_spawn_file_actions_adddup2(&fa, perr[1], 2);
  if (!cwd.empty())
    posix_spawn_file_actions_addchdir_np(&fa, cwd.c_str());
  std::vector<char *> args;
  for (const std::string &a : argv)
    args.push_back(const_cast<char *>(a.c_str()));
  args.push_back(nullptr);
  pid_t pid = 0;
  const int rc = posix_spawnp(&pid, args[0], &fa, nullptr, args.data(), environ);
  posix_spawn_file_actions_destroy(&fa);
  ::close(pin[0]);
  ::close(pout[1]);
  ::close(perr[1]);
  Ran r;
  if (rc != 0) {
    ::close(pin[1]);
    ::close(pout[0]);
    ::close(perr[0]);
    r.err = std::string("起不来 ") + argv[0] + ": " + std::strerror(rc);
    return noted(argv, r, t0);
  }
  for (size_t off = 0; off < in.size();) {
    const ssize_t k = ::write(pin[1], in.data() + off, in.size() - off);
    if (k <= 0)
      break;
    off += static_cast<size_t>(k);
  }
  ::close(pin[1]);
  pollfd fds[2] = {{pout[0], POLLIN, 0}, {perr[0], POLLIN, 0}};
  std::string *sink[2] = {&r.out, &r.err};
  for (int open = 2; open > 0;) {
    if (::poll(fds, 2, -1) < 0) {
      assert(errno == EINTR);
      continue;
    }
    for (int i = 0; i < 2; ++i) {
      if (fds[i].fd < 0 || !(fds[i].revents & (POLLIN | POLLHUP | POLLERR)))
        continue;
      char buf[4096];
      const ssize_t k = ::read(fds[i].fd, buf, sizeof buf);
      if (k > 0) {
        sink[i]->append(buf, static_cast<size_t>(k));
        continue;
      }
      ::close(fds[i].fd);
      fds[i].fd = -1;
      --open;
    }
  }
  int st = 0;
  while (::waitpid(pid, &st, 0) < 0)
    assert(errno == EINTR);
  r.rc = WIFEXITED(st) ? WEXITSTATUS(st) : 128 + WTERMSIG(st);
  return noted(argv, r, t0);
}

bool on_path(const std::string &exe) {
  const char *path = std::getenv("PATH");
  if (!path)
    return false;
  for (std::string_view p = path; !p.empty();) {
    const size_t c = p.find(':');
    const std::filesystem::path f = std::filesystem::path(std::string(p.substr(0, c))) / exe;
    if (::access(f.c_str(), X_OK) == 0)
      return true;
    p = c == std::string_view::npos ? std::string_view{} : p.substr(c + 1);
  }
  return false;
}

} // namespace live::tdx
