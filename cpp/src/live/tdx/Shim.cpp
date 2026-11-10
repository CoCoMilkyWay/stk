#include "live/tdx/Shim.hpp"

#include "live/Broker.hpp"
#include "live/tdx/Proc.hpp"

#include <boost/beast/core/detail/sha1.hpp>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <thread>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace fs = std::filesystem;

namespace live::tdx::shim {

namespace {

constexpr const char *kGcc = "i686-w64-mingw32-gcc";
constexpr const char *kSysroot = "/usr/i686-w64-mingw32";

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
      const std::string s = slurp(shim_dir() / n);
      boost::beast::detail::update(ctx, s.data(), s.size());
    }
    unsigned char d[boost::beast::detail::sha1_context::digest_size];
    boost::beast::detail::finish(ctx, d);
    return strf("%02x%02x%02x%02x", d[0], d[1], d[2], d[3]);
  }();
  return h;
}

std::string dll_name() { return "shim_" + hash() + ".dll"; }

fs::path wine_temp() {
  const char *p = std::getenv("WINEPREFIX");
  const fs::path prefix = p && *p ? fs::path(p) : fs::path(std::getenv("HOME")) / ".wine";
  return prefix / "drive_c/windows/temp";
}

std::string target_proc() {
  const char *p = std::getenv("SHIM_TARGET_PROC");
  return p && *p ? p : "tdxw.exe";
}

// 删掉 d 下非当前哈希的 shim_*.dll (已注入进程映射的旧 DLL 被 unlink 不受影响)
void prune(const fs::path &d) {
  for (const auto &e : fs::directory_iterator(d)) {
    const std::string n = e.path().filename().string();
    if (n.starts_with("shim_") && n.ends_with(".dll") && n != dll_name())
      fs::remove(e.path());
  }
}

// clang + lld 不会自动搜 gcc 的 libgcc 目录: 取含 libgcc.a 的目录 (win32 线程模型优先, 免拖 libwinpthread)
fs::path libgcc_dir() {
  std::vector<fs::path> cands;
  const fs::path root = "/usr/lib/gcc/i686-w64-mingw32";
  if (fs::is_directory(root))
    for (const auto &e : fs::directory_iterator(root))
      if (fs::exists(e.path() / "libgcc.a"))
        cands.push_back(e.path());
  if (cands.empty())
    fail("未找到 libgcc.a (装 gcc-mingw-w64-i686)");
  std::sort(cands.begin(), cands.end(), [](const fs::path &a, const fs::path &b) {
    const bool wa = a.string().find("win32") != std::string::npos, wb = b.string().find("win32") != std::string::npos;
    return wa != wb ? wa : a < b;
  });
  return cands[0];
}

std::vector<std::string> toolchain() {
  if (on_path("clang") && fs::is_directory(fs::path(kSysroot) / "include"))
    // -static-libgcc: 否则 -shared 时先链 -lgcc_s, 导入 Wine 里没有的 libgcc_s_dw2-1.dll
    return {"clang", "--target=i686-w64-windows-gnu", std::string("--sysroot=") + kSysroot, "-fuse-ld=lld", "-static-libgcc", "-L" + libgcc_dir().string()};
  if (on_path(kGcc))
    return {kGcc};
  fail("没有可用的交叉编译器: 装 mingw-w64-i686-dev + binutils-mingw-w64-i686 (已有 clang) 或 gcc-mingw-w64-i686");
}

void build() {
  const fs::path dir = shim_dir();
  prune(dir);
  if (fs::exists(dir / dll_name()))
    return;
  const std::vector<std::string> base = toolchain();
  const std::vector<std::vector<std::string>> steps = {
      {"-shared", "-o", dll_name(), "shim.c", "-lws2_32", "-luser32", "-lkernel32", "-lgdi32"},
      {"-o", "inject.exe", "inject.c", "-lkernel32"},
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
  const fs::path tmp = wine_temp();
  fs::create_directories(tmp);
  prune(tmp);
  fs::copy_file(shim_dir() / dll_name(), tmp / dll_name(), fs::copy_options::overwrite_existing);
  std::ofstream(tmp / "tdx_shim_port.txt") << port();
  const Ran r = run({"wine", (shim_dir() / "inject.exe").string(), "C:\\windows\\temp\\" + dll_name(), target_proc()});
  if (r.rc != 0)
    fail(strf("注入失败 rc=%d\n%s", r.rc, r.err.c_str()));
}

int connect_to(double timeout) {
  const int s = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  assert(s >= 0);
  timeval tv{static_cast<time_t>(timeout), static_cast<suseconds_t>((timeout - static_cast<time_t>(timeout)) * 1e6)};
  ::setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
  ::setsockopt(s, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);
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

} // namespace

int port() { return 20000 + static_cast<int>(std::stoul(hash(), nullptr, 16) % 10000); }

bool online() {
  const int s = connect_to(1);
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
  const auto end = std::chrono::steady_clock::now() + std::chrono::duration<double>(timeout);
  while (!online()) {
    if (std::chrono::steady_clock::now() >= end)
      fail(strf("shim 注入后 %gs 无应答 (端口 %d)", timeout, port()));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

std::vector<nlohmann::json> call(const std::string &line, double timeout) {
  assert(line.find('\n') == std::string::npos);
  const std::string cmd = line.substr(0, line.find(' '));
  const int s = connect_to(timeout);
  if (s < 0)
    fail("shim 不在线 (" + cmd + ")");
  const std::string req = line + "\n";
  for (size_t off = 0; off < req.size();) {
    const ssize_t k = ::send(s, req.data() + off, req.size() - off, MSG_NOSIGNAL);
    if (k <= 0) {
      ::close(s);
      fail("shim 发送失败 (" + cmd + ")");
    }
    off += static_cast<size_t>(k);
  }
  std::string buf;
  for (char tmp[65536];;) {
    const ssize_t k = ::recv(s, tmp, sizeof tmp, 0);
    if (k > 0) {
      buf.append(tmp, static_cast<size_t>(k));
      continue;
    }
    if (k < 0) {
      ::close(s);
      fail(strf("%s: %gs 内没应答 (客户端卡着?)", cmd.c_str(), timeout));
    }
    break;
  }
  ::close(s);
  std::vector<std::string_view> lines;
  for (std::string_view v = buf; !v.empty();) {
    const size_t e = v.find('\n');
    std::string_view l = v.substr(0, e);
    if (l.ends_with('\r'))
      l.remove_suffix(1);
    lines.push_back(l);
    v = e == std::string_view::npos ? std::string_view{} : v.substr(e + 1);
  }
  if (lines.empty() || lines.back() != "ok")
    fail(cmd + ": " + (lines.empty() ? std::string("连接被关, 无应答") : std::string(lines.back())));
  std::vector<nlohmann::json> out;
  for (size_t i = 0; i + 1 < lines.size(); ++i) {
    nlohmann::json j = nlohmann::json::parse(lines[i], nullptr, false);
    assert(!j.is_discarded());
    out.push_back(std::move(j));
  }
  return out;
}

} // namespace live::tdx::shim
