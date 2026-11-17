#include "live/tdx/Mem.hpp"

#include "disk/Disk.hpp"
#include "live/tdx/Proc.hpp"

#include <cctype>
#include <cerrno>
#include <cmath>
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

bool digit(char c) { return std::isdigit(static_cast<unsigned char>(c)); }

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

bool clock_text(std::string_view s) {
  return s.size() == 8 && digit(s[0]) && digit(s[1]) && s[2] == ':' && digit(s[3]) && digit(s[4]) && s[5] == ':' && digit(s[6]) && digit(s[7]);
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

long integer(const Shape &t, std::string_view s) {
  const std::string z(s);
  char *e = nullptr;
  const long v = std::strtol(z.c_str(), &e, 10);
  if (z.empty() || !e || *e)
    fail(strf("%s列「%s」不是整数", t.name, z.c_str()));
  return v;
}

} // namespace

// ---------- 堆 ----------

Heap::Heap(int pid) : pid_(pid), fd_(::open(("/proc/" + std::to_string(pid) + "/mem").c_str(), O_RDONLY | O_CLOEXEC)) {
  if (fd_ < 0)
    fail(strf("打不开 tdxw.exe 进程 %d 的内存: %s", pid, std::strerror(errno)));
}

Heap::~Heap() { ::close(fd_); }

std::string Heap::read(Addr addr, size_t n) const {
  std::string out(n, '\0');
  const size_t got = pread_full(fd_, out.data(), n, addr);
  if (got != n)
    fail(strf("%#llx 只读到 %zu/%zu 字节", static_cast<unsigned long long>(addr), got, n));
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

bool isclose(double a, double b, double rel, double abs) { return std::fabs(a - b) <= std::max(rel * std::max(std::fabs(a), std::fabs(b)), abs); }

void need_row(const Shape &t, bool ok, const std::string &key, const char *what) {
  if (!ok)
    fail(strf("%s %s %s", t.name, key.c_str(), what));
}

// ---------- 列 ----------

std::string_view field(const Fields &f, int c) {
  assert(c >= 0 && static_cast<size_t>(c) < f.size());
  return f[static_cast<size_t>(c)];
}

namespace as {

std::string text::get(const Shape &, const Fields &f, int c) { return std::string(field(f, c)); }

std::string name::get(const Shape &t, const Fields &f, int c) {
  std::string s(field(f, c));
  if (s.empty())
    fail(strf("%s名称为空", t.name));
  return s;
}

std::string code::get(const Shape &t, const Fields &f, int c) {
  const std::string_view s = field(f, c);
  if (s.size() != 6 || !std::all_of(s.begin(), s.end(), digit))
    fail(strf("%s非法代码「%.*s」", t.name, static_cast<int>(s.size()), s.data()));
  return std::string(s);
}

std::string clock::get(const Shape &t, const Fields &f, int c) {
  const std::string_view s = field(f, c);
  if (!clock_text(s))
    fail(strf("%s时间「%.*s」不认识", t.name, static_cast<int>(s.size()), s.data()));
  return std::string(s);
}

misc::Ymd ymd::get(const Shape &t, const Fields &f, int c) {
  const std::string_view s = field(f, c);
  const misc::Ymd d = s.size() == 8 ? misc::parse_ymd(s) : 0;
  if (!d)
    fail(strf("%s日期「%.*s」不认识", t.name, static_cast<int>(s.size()), s.data()));
  return d;
}

double num::get(const Shape &t, const Fields &f, int c) {
  const std::string z(field(f, c));
  char *e = nullptr;
  const double v = std::strtod(z.c_str(), &e);
  while (e && *e && std::isspace(static_cast<unsigned char>(*e)))
    ++e;
  if (z.empty() || !e || *e)
    fail(strf("%s列「%s」不是数", t.name, z.c_str()));
  return v;
}

std::optional<double> num_or::get(const Shape &t, const Fields &f, int c) {
  if (field(f, c).empty())
    return std::nullopt;
  return num::get(t, f, c);
}

Mkt flag::get(const Shape &t, const Fields &f, int c) {
  const std::string_view s = field(f, c);
  switch (integer(t, s)) {
  case 0:
    return Mkt::SZ;
  case 1:
    return Mkt::SH;
  case 60:
    return Mkt::BJ;
  default:
    fail(strf("%s沪深标志「%.*s」不认识", t.name, static_cast<int>(s.size()), s.data()));
  }
}

int side::get(const Shape &t, const Fields &f, int c) {
  const std::string_view s = field(f, c);
  if (s == "买入")
    return kBuy;
  if (s == "卖出")
    return kSell;
  fail(strf("%s买卖方向「%.*s」不认识", t.name, static_cast<int>(s.size()), s.data()));
}

BySide sides::get(const Shape &t, const Fields &f, int c) {
  BySide b;
  b[kBuy] = num::get(t, f, c);
  b[kSell] = num::get(t, f, c + 1);
  return b;
}

std::array<std::pair<std::string, std::string>, 3> groups::get(const Shape &, const Fields &f, int c) {
  std::array<std::pair<std::string, std::string>, 3> g;
  for (int i = 0; i < 3; ++i)
    g[static_cast<size_t>(i)] = {std::string(field(f, c + 3 * i)), std::string(field(f, c + 3 * i + 1))};
  return g;
}

} // namespace as

// ---------- 索引 ----------

Keyed<Flow> Tally::build(const Shape &t, const std::vector<Fill> &rows) {
  Keyed<Flow> out;
  for (const Fill &r : rows) {
    const auto [it, fresh] = out.at.emplace(r.code, out.list.size());
    if (fresh) {
      Flow f;
      static_cast<Sec &>(f) = r;
      out.list.push_back(f);
    }
    Flow &a = out.list[it->second];
    if (a.mkt != r.mkt)
      fail(strf("%s %s 沪深标志不一致", t.name, r.code.c_str()));
    a.traded[r.side] += r.qty;
    a.amount[r.side] += r.amount;
  }
  return out;
}

} // namespace live::tdx::mem
