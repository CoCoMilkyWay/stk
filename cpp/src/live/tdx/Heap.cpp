#include "live/tdx/Heap.hpp"

#include "disk/Disk.hpp"
#include "live/Base.hpp"
#include "live/Trace.hpp"
#include "live/tdx/Proc.hpp"

#include <algorithm>
#include <cassert>
#include <cctype>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <sstream>

#include <fcntl.h>
#include <iconv.h>
#include <unistd.h>

namespace fs = std::filesystem;

namespace live::tdx::mem {

namespace {

constexpr Addr kMaxRegion = 400ull << 20; // 跳过超大段 (映射文件等), 只扫堆
constexpr Addr kLowest = 0x10000;         // 其下不映射, 试读即无
constexpr size_t kPage = 4096;
constexpr const char *kPtraceScope = "/proc/sys/kernel/yama/ptrace_scope";

std::string slurp(const fs::path &p) {
  std::ifstream f(p, std::ios::binary);
  if (!f)
    return {};
  std::ostringstream s;
  s << f.rdbuf();
  return s.str();
}

std::string lower(std::string s) {
  std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return s;
}

int ptrace_scope() {
  const std::string s = slurp(kPtraceScope);
  return s.empty() ? 0 : std::atoi(s.c_str());
}

// 确保能跨进程读 /proc/<pid>/mem: ptrace_scope ≠ 0 时用本机存的密码 sudo 关一次 (同用户进程即可互读), 到重启前有效
void ensure_access() {
  if (ptrace_scope() == 0)
    return;
  trace::ev("ptrace", {{"scope", ptrace_scope()}});
  const fs::path pass = disk::path(disk::LiveSudoFile);
  if (!fs::exists(pass))
    fail("需 sudo 放开 ptrace, 但缺密码文件 " + pass.string());
  std::string pw = slurp(pass);
  while (!pw.empty() && std::isspace(static_cast<unsigned char>(pw.back())))
    pw.pop_back();
  run({"sudo", "-S", "sysctl", "-w", "kernel.yama.ptrace_scope=0"}, pw + "\n");
  if (ptrace_scope() != 0)
    fail("sudo 设 ptrace_scope=0 失败 (密码错?)");
}

struct Region {
  Addr lo, hi;
};

// 堆段: 可读写、私有、非文件映射 (Wine 堆即匿名 rwxp), 排除超大段; near 给定时只留含其中任一地址的段
std::vector<Region> regions(int pid, const std::vector<Addr> *near) {
  std::ifstream f("/proc/" + std::to_string(pid) + "/maps");
  if (!f)
    fail(strf("tdxw.exe 进程 %d 已不在", pid));
  std::vector<Region> out;
  for (std::string line; std::getline(f, line);) {
    std::istringstream ss(line);
    std::string range, perms, off, dev, inode, path;
    ss >> range >> perms >> off >> dev >> inode;
    std::getline(ss >> std::ws, path);
    const size_t dash = range.find('-');
    const Addr lo = std::stoull(range.substr(0, dash), nullptr, 16), hi = std::stoull(range.substr(dash + 1), nullptr, 16);
    if (perms.size() < 4 || perms[0] != 'r' || perms[1] != 'w' || perms[3] != 'p' || path.starts_with("/"))
      continue;
    if (hi - lo > kMaxRegion)
      continue;
    if (near && std::none_of(near->begin(), near->end(), [&](Addr a) { return lo <= a && a < hi; }))
      continue;
    out.push_back({lo, hi});
  }
  return out;
}

size_t pread_full(int fd, char *buf, size_t n, Addr at) {
  size_t got = 0;
  while (got < n) {
    const ssize_t k = ::pread(fd, buf + got, n - got, static_cast<off_t>(at + got));
    if (k <= 0)
      break;
    got += static_cast<size_t>(k);
  }
  return got;
}

template <class T>
std::optional<uint32_t> word(int fd, Addr a) {
  T v = 0;
  if (a < kLowest || pread_full(fd, reinterpret_cast<char *>(&v), sizeof v, a) != sizeof v)
    return std::nullopt;
  return v;
}

iconv_t gbk_cd() {
  thread_local iconv_t cd = [] {
    const iconv_t c = iconv_open("UTF-8", "GBK");
    assert(c != reinterpret_cast<iconv_t>(-1));
    return c;
  }();
  iconv(cd, nullptr, nullptr, nullptr, nullptr);
  return cd;
}

} // namespace

Heap::Heap(int pid) : pid_(pid), fd_(::open(("/proc/" + std::to_string(pid) + "/mem").c_str(), O_RDONLY | O_CLOEXEC)) {
  if (fd_ < 0)
    fail(strf("打不开 tdxw.exe 进程 %d 的内存: %s", pid, std::strerror(errno)));
}

Heap::~Heap() { ::close(fd_); }

std::optional<uint32_t> Heap::u16(Addr a) const { return word<uint16_t>(fd_, a); }

std::optional<uint32_t> Heap::u32(Addr a) const { return word<uint32_t>(fd_, a); }

std::string Heap::bytes(Addr a, size_t n) const {
  std::string out(n, '\0');
  for (size_t i = 0; i < n;) {
    const size_t k = std::min(n - i, kPage - static_cast<size_t>((a + i) % kPage));
    pread_full(fd_, out.data() + i, k, a + i);
    i += k;
  }
  return out;
}

void Heap::each(const std::function<void(Addr lo, std::string_view data)> &fn, const std::vector<Addr> *near) const {
  std::unique_ptr<char[]> buf;
  size_t cap = 0;
  for (const Region &r : regions(pid_, near)) {
    const size_t n = r.hi - r.lo;
    if (n > cap) {
      buf = std::make_unique_for_overwrite<char[]>(n);
      cap = n;
    }
    // 个别段运行时不可读 (守护页等) 属正常, 跳过
    const size_t got = pread_full(fd_, buf.get(), n, r.lo);
    if (got)
      fn(r.lo, std::string_view(buf.get(), got));
  }
}

int find_pid() {
  for (const auto &e : fs::directory_iterator("/proc")) {
    const std::string n = e.path().filename().string();
    if (n.empty() || !std::all_of(n.begin(), n.end(), [](unsigned char c) { return std::isdigit(c); }))
      continue;
    std::string comm = lower(slurp(e.path() / "comm"));
    while (!comm.empty() && comm.back() == '\n')
      comm.pop_back();
    const std::string cmd = lower(slurp(e.path() / "cmdline"));
    const std::string argv0 = cmd.substr(0, cmd.find('\0'));
    if (comm == "tdxw.exe" || argv0.ends_with("tdxw.exe"))
      return std::stoi(n);
  }
  return 0;
}

HeapP attach() {
  const int pid = find_pid();
  if (!pid)
    fail("找不到 tdxw.exe 进程");
  trace::ev("attach", {{"pid", pid}});
  ensure_access();
  return std::make_shared<const Heap>(pid);
}

std::string gbk(std::string_view raw) {
  const iconv_t cd = gbk_cd();
  std::string out;
  char buf[4096];
  char *in = const_cast<char *>(raw.data());
  size_t inl = raw.size();
  while (inl > 0) {
    char *o = buf;
    size_t ol = sizeof(buf);
    const size_t r = iconv(cd, &in, &inl, &o, &ol);
    out.append(buf, static_cast<size_t>(o - buf));
    if (r != static_cast<size_t>(-1) || errno == E2BIG)
      continue;
    out += static_cast<unsigned char>(*in) == 0x80 ? "€" : "\xEF\xBF\xBD";
    ++in;
    --inl;
    iconv(cd, nullptr, nullptr, nullptr, nullptr);
  }
  return out;
}

} // namespace live::tdx::mem
