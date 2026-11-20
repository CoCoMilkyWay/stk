#include "live/tdx/Shim.hpp"

#include "disk/Disk.hpp"
#include "live/Broker.hpp"
#include "live/Trace.hpp"
#include "live/tdx/Proc.hpp"
#include "live/tdx/Until.hpp"

#include <boost/beast/core/detail/sha1.hpp>

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <cstdlib>
#include <fstream>
#include <mutex>
#include <sstream>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

namespace fs = std::filesystem;

namespace live::tdx::shim {

namespace {

constexpr const char *kGcc = "i686-w64-mingw32-gcc";
constexpr const char *kSysroot = "/usr/i686-w64-mingw32";
constexpr std::string_view kBusy = "err target thread busy (timeout)";
constexpr double kBusyWait = 3;

fs::path src_dir() { return disk::root() / "cpp/src/live/tdx/win32"; }
fs::path out_dir() { return disk::ensure(disk::RunLiveTdxShim); }

std::string slurp(const fs::path &p) {
  std::ifstream f(p, std::ios::binary);
  assert(f);
  std::ostringstream s;
  s << f.rdbuf();
  return s.str();
}

// 源码哈希 (sha1(shim.c ‖ inject.c) 前 8 位十六进制)
const std::string &hash() {
  static const std::string h = [] {
    boost::beast::detail::sha1_context ctx;
    boost::beast::detail::init(ctx);
    for (const char *n : {"shim.c", "inject.c"}) {
      const std::string s = slurp(src_dir() / n);
      boost::beast::detail::update(ctx, s.data(), s.size());
    }
    unsigned char d[boost::beast::detail::sha1_context::digest_size];
    boost::beast::detail::finish(ctx, d);
    return strf("%02x%02x%02x%02x", d[0], d[1], d[2], d[3]);
  }();
  return h;
}

std::string dll_name() { return "shim_" + hash() + ".dll"; }
int port() { return 20000 + static_cast<int>(std::stoul(hash(), nullptr, 16) % 10000); }

fs::path wine_temp() {
  const char *p = std::getenv("WINEPREFIX");
  const fs::path prefix = p && *p ? fs::path(p) : fs::path(std::getenv("HOME")) / ".wine";
  return prefix / "drive_c/windows/temp";
}

std::string target_proc() {
  const char *p = std::getenv("SHIM_TARGET_PROC");
  return p && *p ? p : "tdxw.exe";
}

void prune(const fs::path &d) {
  for (const auto &e : fs::directory_iterator(d)) {
    const std::string n = e.path().filename().string();
    if (n.starts_with("shim_") && n.ends_with(".dll") && n != dll_name())
      fs::remove(e.path());
  }
}

fs::path libgcc_dir() {
  std::vector<fs::path> cands;
  const fs::path root = "/usr/lib/gcc/i686-w64-mingw32";
  if (fs::is_directory(root))
    for (const auto &e : fs::directory_iterator(root))
      if (fs::exists(e.path() / "libgcc.a"))
        cands.push_back(e.path());
  need(!cands.empty(), "未找到 libgcc.a (装 gcc-mingw-w64-i686)");
  std::sort(cands.begin(), cands.end(), [](const fs::path &a, const fs::path &b) {
    const bool wa = a.string().find("win32") != std::string::npos, wb = b.string().find("win32") != std::string::npos;
    return wa != wb ? wa : a < b;
  });
  return cands[0];
}

std::vector<std::string> toolchain() {
  if (on_path("clang") && fs::is_directory(fs::path(kSysroot) / "include"))
    return {"clang", "--target=i686-w64-windows-gnu", std::string("--sysroot=") + kSysroot, "-fuse-ld=lld", "-static-libgcc", "-L" + libgcc_dir().string()};
  if (on_path(kGcc))
    return {kGcc};
  fail("没有可用的交叉编译器: 装 mingw-w64-i686-dev + binutils-mingw-w64-i686 (已有 clang) 或 gcc-mingw-w64-i686");
}

void build() {
  const fs::path dir = out_dir(), src = src_dir();
  prune(dir);
  if (fs::exists(dir / dll_name()))
    return;
  const std::vector<std::string> base = toolchain();
  trace::ev("build", {{"dll", dll_name()}, {"cc", base[0]}});
  const std::vector<std::vector<std::string>> steps = {
      {"-shared", "-o", dll_name(), (src / "shim.c").string(), "-lws2_32", "-luser32", "-lkernel32", "-lgdi32"},
      {"-o", "inject.exe", (src / "inject.c").string(), "-lkernel32"},
  };
  for (const auto &args : steps) {
    std::vector<std::string> argv = base;
    argv.insert(argv.end(), args.begin(), args.end());
    const Ran r = run(argv, "", dir);
    if (r.rc != 0)
      fail(strf("编译 %s 失败 rc=%d\n%s", args[2].c_str(), r.rc, r.err.c_str()));
  }
}

void inject() {
  trace::ev("inject", {{"dll", dll_name()}, {"port", port()}, {"proc", target_proc()}});
  const fs::path tmp = wine_temp();
  fs::create_directories(tmp);
  prune(tmp);
  fs::copy_file(out_dir() / dll_name(), tmp / dll_name(), fs::copy_options::overwrite_existing);
  std::ofstream(tmp / "tdx_shim_port.txt") << port();
  const Ran r = run({"wine", (out_dir() / "inject.exe").string(), "C:\\windows\\temp\\" + dll_name(), target_proc()});
  if (r.rc != 0)
    fail(strf("注入失败 rc=%d\n%s", r.rc, r.err.c_str()));
}

void set_timeout(int fd, double timeout) {
  const double t = std::max(timeout, 0.05);
  timeval tv{static_cast<time_t>(t), static_cast<suseconds_t>((t - static_cast<time_t>(t)) * 1e6)};
  ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
  ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);
}

int dial(double timeout) {
  const int s = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  assert(s >= 0);
  int one = 1;
  ::setsockopt(s, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
  set_timeout(s, timeout);
  sockaddr_in a{};
  a.sin_family = AF_INET;
  a.sin_port = htons(static_cast<uint16_t>(port()));
  a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if (::connect(s, reinterpret_cast<const sockaddr *>(&a), sizeof a) != 0) {
    ::close(s);
    return -1;
  }
  return s;
}

bool send_line(int fd, const std::string &line) {
  assert(line.find('\n') == std::string::npos);
  const std::string req = line + "\n";
  for (size_t off = 0; off < req.size();) {
    const ssize_t k = ::send(fd, req.data() + off, req.size() - off, MSG_NOSIGNAL);
    if (k <= 0)
      return false;
    off += static_cast<size_t>(k);
  }
  return true;
}

// 复用的命令长连接 (单连接, 互斥串行, 断线自动重连)
struct Link {
  std::mutex mu;
  int fd = -1;
  std::string buf;

  void drop() {
    if (fd >= 0) {
      ::close(fd);
      fd = -1;
    }
    buf.clear();
  }

  // 发一行、读到 ok / err 行; 0 = 正常结束 (lines 末行即 ok/err), -1 = 断线, -2 = 超时
  int round(const std::string &line, double timeout, std::vector<std::string> &lines, size_t &bytes) {
    if (fd < 0) {
      fd = dial(timeout);
      if (fd < 0)
        return -1;
    }
    set_timeout(fd, timeout);
    if (!send_line(fd, line)) {
      drop();
      return -1;
    }
    lines.clear();
    bytes = 0;
    const auto end = Clock::now() + Secs(timeout);
    while (true) {
      while (true) {
        const size_t nl = buf.find('\n');
        if (nl == std::string::npos)
          break;
        size_t e = nl;
        if (e > 0 && buf[e - 1] == '\r')
          --e;
        std::string row(buf.data(), e);
        bytes += nl + 1;
        buf.erase(0, nl + 1);
        const bool fin = row == "ok" || row.starts_with("err ");
        lines.push_back(std::move(row));
        if (fin)
          return 0;
      }
      const double rem = std::chrono::duration<double>(end - Clock::now()).count();
      if (rem <= 0) {
        drop();
        return -2;
      }
      set_timeout(fd, rem);
      char tmp[65536];
      const ssize_t k = ::recv(fd, tmp, sizeof tmp, 0);
      if (k > 0) {
        buf.append(tmp, static_cast<size_t>(k));
        continue;
      }
      drop();
      return (k < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) ? -2 : -1;
    }
  }
};

Link &link() {
  static Link l;
  return l;
}

} // namespace

bool online() {
  const int s = dial(1);
  if (s < 0)
    return false;
  ::close(s);
  return true;
}

void ensure(double timeout) {
  build();
  if (online())
    return;
  inject();
  until(said(strf("shim 注入后应答 (端口 %d)", port())), online, timeout, 0.1);
  trace::ev("online", {{"port", port()}});
}

void reset() {
  Link &l = link();
  std::lock_guard lk(l.mu);
  l.drop();
}

std::vector<nlohmann::json> call(const std::string &line, double timeout) {
  const std::string cmd = line.substr(0, line.find(' '));
  trace::shim_sent(std::string_view(line).substr(0, line.find(' ', cmd.size() + 1)));
  const Clock::time_point t0 = Clock::now();
  const auto left = [&] { return timeout - std::chrono::duration<double>(Clock::now() - t0).count(); };

  Link &l = link();
  std::vector<std::string> lines;
  size_t bytes = 0;
  {
    std::lock_guard lk(l.mu);
    do {
      int rc = l.round(line, std::max(left(), 0.1), lines, bytes);
      if (rc == -1) // 复用旧连接已断: 立刻重连一次
        rc = l.round(line, std::max(left(), 0.1), lines, bytes);
      if (rc == -2)
        fail(strf("%s: %gs 内没应答 (客户端卡着?)", cmd.c_str(), timeout));
      if (rc == -1)
        fail("shim 不在线 (" + cmd + ")");
    } while (!lines.empty() && lines.back() == kBusy && left() > kBusyWait);
  }
  if (lines.empty() || lines.back() != "ok")
    fail(cmd + ": " + (lines.empty() ? std::string("连接被关, 无应答") : lines.back()));

  std::vector<nlohmann::json> out;
  out.reserve(lines.size() - 1);
  for (size_t i = 0; i + 1 < lines.size(); ++i) {
    nlohmann::json j = nlohmann::json::parse(lines[i], nullptr, false);
    assert(!j.is_discarded());
    out.push_back(std::move(j));
  }
  if (trace::on()) {
    trace::Kw kw = {{"cmd", line}, {"ms", trace::ms_since(t0)}};
    if (bytes <= trace::kRetCap) {
      kw["ret"] = nlohmann::json(out);
    } else {
      kw["n"] = out.size();
      kw["kb"] = (bytes + 512) / 1024;
    }
    trace::ev("shim", std::move(kw));
  }
  return out;
}

Stream::Stream(const std::string &cmd) {
  fd_ = dial(2);
  need(fd_ >= 0, "shim 推流连接失败 (" + cmd + ")");
  if (!send_line(fd_, cmd)) {
    close();
    fail("shim 推流发送失败 (" + cmd + ")");
  }
}

Stream::~Stream() { close(); }

void Stream::close() {
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
  buf_.clear();
}

bool Stream::next(nlohmann::json &out, double timeout) {
  if (fd_ < 0)
    fail("shim 推流已关");
  const auto end = Clock::now() + Secs(timeout);
  while (true) {
    while (true) {
      const size_t nl = buf_.find('\n');
      if (nl == std::string::npos)
        break;
      size_t e = nl;
      if (e > 0 && buf_[e - 1] == '\r')
        --e;
      std::string row(buf_.data(), e);
      buf_.erase(0, nl + 1);
      if (row.empty() || row == "null")
        continue; // 1s 心跳
      if (row.starts_with("err "))
        fail("shim 推流: " + row.substr(4));
      out = nlohmann::json::parse(row, nullptr, false);
      assert(!out.is_discarded());
      return true;
    }
    const double rem = std::chrono::duration<double>(end - Clock::now()).count();
    if (rem <= 0)
      return false;
    pollfd pfd{fd_, POLLIN, 0};
    const int pr = ::poll(&pfd, 1, static_cast<int>(std::max(1.0, rem * 1000.0)));
    if (pr == 0)
      return false;
    if (pr < 0) {
      if (errno == EINTR)
        continue;
      close();
      fail("shim 推流 poll 失败");
    }
    char tmp[65536];
    const ssize_t k = ::recv(fd_, tmp, sizeof tmp, 0);
    if (k <= 0) {
      close();
      fail("shim 推流断开");
    }
    buf_.append(tmp, static_cast<size_t>(k));
  }
}

} // namespace live::tdx::shim
