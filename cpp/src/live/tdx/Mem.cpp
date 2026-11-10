#include "live/tdx/Mem.hpp"

#include "disk/Disk.hpp"
#include "live/tdx/Proc.hpp"

#include <cassert>
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

int ptrace_scope() {
  const std::string s = slurp(kPtraceScope);
  return s.empty() ? 0 : std::atoi(s.c_str());
}

// 确保能跨进程读 /proc/<pid>/mem: ptrace_scope ≠ 0 时用本机存的密码 sudo 关一次 (同用户进程即可互读), 到重启前有效
void ensure_access() {
  if (ptrace_scope() == 0)
    return;
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

// 堆段: 可读写、私有、非文件映射 (Wine 堆即匿名 rwxp), 排除超大段
std::vector<Region> regions(int pid) {
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

int open_mem(int pid) {
  const int fd = ::open(("/proc/" + std::to_string(pid) + "/mem").c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0)
    fail(strf("打不开 tdxw.exe 进程 %d 的内存: %s", pid, std::strerror(errno)));
  return fd;
}

const char *memfind(std::string_view hay, std::string_view needle, size_t from) {
  if (from >= hay.size())
    return nullptr;
  return static_cast<const char *>(::memmem(hay.data() + from, hay.size() - from, needle.data(), needle.size()));
}

// 块首整块匹配: 表头一行 + 其后连续的数据行 (行 = 到首个 \r 或 \n 为止, 须以 \r\n 结尾)
struct Match {
  size_t len = 0, head = 0, rows = 0;
  std::string_view tag;
  long n = -1;
};

std::optional<Match> match(const Shape &t, std::string_view d) {
  if (!d.starts_with("0|"))
    return std::nullopt;
  const auto line_at = [&](size_t at, std::string_view &line) {
    const size_t e = d.find_first_of("\r\n", at);
    if (e == std::string_view::npos || d[e] != '\r' || e + 1 >= d.size() || d[e + 1] != '\n')
      return false;
    line = d.substr(at, e - at);
    return true;
  };
  Match m;
  std::string_view line;
  if (!line_at(0, line) || !t.head(line, m.tag, m.n))
    return std::nullopt;
  m.head = m.len = line.size() + 2;
  while (m.len < d.size() && line_at(m.len, line) && t.row(line)) {
    m.len += line.size() + 2;
    ++m.rows;
  }
  if (!m.rows && !t.empty)
    return std::nullopt;
  return m;
}

// 整块 → 块字节: 表头带行数 n 即表头 + 前 n 行, 后随不足 n 行或不该空的表 n = 0 → 无; 不带即整块
std::optional<std::string> body(const Shape &t, std::string_view d, const Match &m) {
  if (m.n < 0)
    return std::string(d.substr(0, m.len));
  if (!((m.n || t.empty) && static_cast<size_t>(m.n) <= m.rows))
    return std::nullopt;
  size_t at = m.head;
  for (long i = 0; i < m.n; ++i)
    at = d.find("\r\n", at) + 2;
  return std::string(d.substr(0, at));
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

int attach() {
  const int pid = find_pid();
  if (!pid)
    fail("找不到 tdxw.exe 进程");
  ensure_access();
  return pid;
}

std::string read_at(int pid, Addr addr, size_t n) {
  const int fd = open_mem(pid);
  std::string out(n, '\0');
  const size_t got = pread_full(fd, out.data(), n, addr);
  ::close(fd);
  if (got != n)
    fail(strf("%#llx 只读到 %zu/%zu 字节", static_cast<unsigned long long>(addr), got, n));
  return out;
}

std::vector<Hit> find(const Shape &t, Addr lo, std::string_view data) {
  const Addr end = lo + data.size();
  std::vector<Hit> out;
  for (const char *p = memfind(data, t.anchor, 0); p; p = memfind(data, t.anchor, static_cast<size_t>(p - data.data()) + 1)) {
    const size_t i = static_cast<size_t>(p - data.data());
    for (size_t hs = i > t.back ? i - t.back : 0; hs < i; ++hs) {
      if (data[hs] != '0' || data[hs + 1] != '|')
        continue;
      const std::string_view d = data.substr(hs);
      const auto m = match(t, d);
      if (!m)
        continue;
      if (auto b = body(t, d, *m)) {
        const Addr a = lo + hs;
        const size_t len = b->size();
        out.push_back({a, std::string(m->tag), std::move(*b), std::min<Addr>(end, a + len + t.slack)});
      }
      break;
    }
  }
  return out;
}

std::optional<std::string> reread(const Shape &t, int pid, const Hit &h) {
  const std::string d = read_at(pid, h.addr, h.end - h.addr);
  const auto m = match(t, d);
  if (!m || m->tag != h.tag)
    return std::nullopt;
  return body(t, d, *m);
}

void each_region(int pid, const std::function<void(Addr lo, std::string_view data)> &fn, const std::vector<Addr> *near) {
  const int fd = open_mem(pid);
  std::string buf;
  for (const Region &r : regions(pid)) {
    if (near && std::none_of(near->begin(), near->end(), [&](Addr a) { return r.lo <= a && a < r.hi; }))
      continue;
    buf.resize(r.hi - r.lo);
    // 个别段运行时不可读 (守护页等) 属正常, 跳过
    const size_t got = pread_full(fd, buf.data(), buf.size(), r.lo);
    if (got)
      fn(r.lo, std::string_view(buf.data(), got));
  }
  ::close(fd);
}

std::vector<std::vector<Hit>> scan(int pid, const std::vector<const Shape *> &shapes, const std::vector<Addr> *near) {
  std::vector<std::vector<Hit>> hits(shapes.size());
  each_region(
      pid,
      [&](Addr lo, std::string_view data) {
        for (size_t i = 0; i < shapes.size(); ++i) {
          std::vector<Hit> h = find(*shapes[i], lo, data);
          hits[i].insert(hits[i].end(), std::make_move_iterator(h.begin()), std::make_move_iterator(h.end()));
        }
      },
      near);
  return hits;
}

Split split(const Shape &t, std::string_view raw, const int *views, size_t nviews) {
  Split sp;
  sp.text.resize(raw.size() * 3 / 2 + 16);
  char *in = const_cast<char *>(raw.data());
  size_t inl = raw.size();
  char *out = sp.text.data();
  size_t outl = sp.text.size();
  const iconv_t cd = gbk_cd();
  while (inl) {
    if (iconv(cd, &in, &inl, &out, &outl) != static_cast<size_t>(-1))
      continue;
    if (errno != E2BIG)
      fail(strf("%s明文含非 GBK 字节, 疑似读到写入中途的坏块", t.name));
    const size_t used = static_cast<size_t>(out - sp.text.data());
    sp.text.resize(sp.text.size() * 2);
    out = sp.text.data() + used;
    outl = sp.text.size() - used;
  }
  sp.text.resize(static_cast<size_t>(out - sp.text.data()));
  std::vector<std::string_view> lines;
  for (std::string_view v = sp.text;;) {
    const size_t e = v.find("\r\n");
    lines.push_back(v.substr(0, e));
    if (e == std::string_view::npos)
      break;
    v = v.substr(e + 2);
  }
  // 首行表头, 末项为块尾 \r\n 之后的空串
  for (size_t i = 1; i + 1 < lines.size(); ++i) {
    std::vector<std::string_view> f;
    for (std::string_view l = lines[i];;) {
      const size_t c = l.find('|');
      f.push_back(l.substr(0, c));
      if (c == std::string_view::npos)
        break;
      l = l.substr(c + 1);
    }
    if (!sp.rows.empty() && f.size() != sp.rows[0].size())
      fail(strf("%s行列数不一致 (%zu / %zu), 块可能被截断 / 串行", t.name, sp.rows[0].size(), f.size()));
    sp.rows.push_back(std::move(f));
  }
  if (sp.rows.empty())
    return sp;
  const size_t nf = sp.rows[0].size();
  const int *v = std::find(views, views + nviews, static_cast<int>(nf));
  if (v == views + nviews)
    fail(strf("%s %zu 列, 不认识的布局", t.name, nf));
  sp.view = static_cast<size_t>(v - views);
  return sp;
}

std::optional<double> num_or(const Shape &t, std::string_view s) {
  if (s.empty())
    return std::nullopt;
  return num(t, s);
}

double num(const Shape &t, std::string_view s) {
  const std::string z(s);
  char *e = nullptr;
  const double v = std::strtod(z.c_str(), &e);
  while (e && *e && std::isspace(static_cast<unsigned char>(*e)))
    ++e;
  if (z.empty() || !e || *e)
    fail(strf("%s列「%s」不是数", t.name, z.c_str()));
  return v;
}

long integer(const Shape &t, std::string_view s) {
  const std::string z(s);
  char *e = nullptr;
  const long v = std::strtol(z.c_str(), &e, 10);
  if (z.empty() || !e || *e)
    fail(strf("%s列「%s」不是整数", t.name, z.c_str()));
  return v;
}

void check_code(const Shape &t, std::string_view code) {
  if (code.size() != 6 || !std::all_of(code.begin(), code.end(), [](unsigned char c) { return std::isdigit(c); }))
    fail(strf("%s非法代码「%.*s」", t.name, static_cast<int>(code.size()), code.data()));
}

Mkt market_flag(const Shape &t, std::string_view code, std::string_view flag) {
  switch (integer(t, flag)) {
  case 0:
    return Mkt::SZ;
  case 1:
    return Mkt::SH;
  case 60:
    return Mkt::BJ;
  default:
    fail(strf("%s %.*s 沪深标志 %.*s", t.name, static_cast<int>(code.size()), code.data(), static_cast<int>(flag.size()), flag.data()));
  }
}

int side_text(const Shape &t, std::string_view s) {
  if (s == "买入")
    return kBuy;
  if (s == "卖出")
    return kSell;
  fail(strf("%s买卖方向「%.*s」不认识", t.name, static_cast<int>(s.size()), s.data()));
}

bool clock_text(std::string_view s) {
  const auto d = [&](size_t i) { return std::isdigit(static_cast<unsigned char>(s[i])); };
  return s.size() == 8 && d(0) && d(1) && s[2] == ':' && d(3) && d(4) && s[5] == ':' && d(6) && d(7);
}

bool isclose(double a, double b, double rel, double abs) { return std::fabs(a - b) <= std::max(rel * std::max(std::fabs(a), std::fabs(b)), abs); }

} // namespace live::tdx::mem
