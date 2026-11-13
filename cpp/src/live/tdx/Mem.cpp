#include "live/tdx/Mem.hpp"

#include "disk/Disk.hpp"
#include "live/tdx/Proc.hpp"
#include "misc/exec.hpp"

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
constexpr int kScanThreads = 4;           // 扫段并行数 (各线程一段一缓冲, 峰值内存 ≈ 这么多段)
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

const char *memfind(std::string_view hay, std::string_view needle, size_t from) {
  if (from >= hay.size())
    return nullptr;
  return static_cast<const char *>(::memmem(hay.data() + from, hay.size() - from, needle.data(), needle.size()));
}

misc::exec::ForkJoin &pool() {
  static misc::exec::ForkJoin p(std::min(kScanThreads, misc::exec::hw()));
  return p;
}
std::mutex &pool_mu() { // 池不可重入: 各线程的扫描排队用它
  static std::mutex m;
  return m;
}

// ---------- 模式 ----------

bool clock_text(std::string_view s) {
  return s.size() == 8 && digit(s[0]) && digit(s[1]) && s[2] == ':' && digit(s[3]) && digit(s[4]) && s[5] == ':' && digit(s[6]) && digit(s[7]);
}

long count(std::string_view d) { return d.size() > 9 ? 1'000'000'000L : std::stol(std::string(d)); }

// 模式 p 开头的 {…} → 其内文字, p 移过它
std::string_view tok(std::string_view &p) {
  const size_t e = p.find('}');
  assert(p.front() == '{' && e != std::string_view::npos);
  const std::string_view t = p.substr(1, e - 1);
  p.remove_prefix(e + 1);
  return t;
}
// 数字记号的位数: {code} 6, {xK} K, 不带位数 0 (不定宽)
size_t width(std::string_view t) {
  if (t == "code")
    return 6;
  size_t w = 0;
  for (const char c : t.substr(1)) {
    assert(digit(c));
    w = w * 10 + static_cast<size_t>(c - '0');
  }
  return w;
}
// 字面段: 到下一个记号 / * / ... 为止
size_t literal(std::string_view p) {
  size_t k = 0;
  while (k < p.size() && p[k] != '{' && p[k] != '*' && !p.substr(k).starts_with("..."))
    ++k;
  return k;
}
// 吃掉 s 开头的数字: 恰 w 位 (w > 0) / ≥ 1 位 (w = 0) → 那段
bool eat_digits(std::string_view &s, size_t w, std::string_view &got) {
  size_t i = 0;
  while (i < s.size() && digit(s[i]) && (!w || i < w))
    ++i;
  if (w ? i != w : !i)
    return false;
  got = s.substr(0, i);
  s.remove_prefix(i);
  return true;
}

struct Caps {
  std::string_view tag;
  long n = -1; // 表头不带行数
};

// 一行 l 恰合模式 p, 记号取到的填进 c
bool fit(std::string_view p, std::string_view l, Caps &c) {
  while (!p.empty()) {
    if (p.starts_with("...")) {
      p.remove_prefix(3);
      assert(literal(p) == p.size());
      return l.ends_with(p);
    }
    if (p[0] == '*') {
      p.remove_prefix(1);
      l.remove_prefix(std::min(l.find('|'), l.size()));
      continue;
    }
    if (p[0] != '{') {
      const size_t k = literal(p);
      if (!l.starts_with(p.substr(0, k)))
        return false;
      l.remove_prefix(k);
      p.remove_prefix(k);
      continue;
    }
    const std::string_view t = tok(p);
    if (t == "clock") {
      if (l.size() < 8 || !clock_text(l.substr(0, 8)))
        return false;
      l.remove_prefix(8);
    } else if (t == "name") {
      const size_t e = std::min(l.find('|'), l.size());
      if (!e || digit(l[0]))
        return false;
      l.remove_prefix(e);
    } else {
      std::string_view d;
      if (!eat_digits(l, width(t), d))
        return false;
      if (t[0] == 'n')
        c.n = count(d);
      else if (t[0] == 't')
        c.tag = d;
      else
        assert(t[0] == 'd' || t == "code");
    }
  }
  return l.empty();
}

bool row_ok(const Shape &t, std::string_view l) {
  Caps c;
  return l.find('\0') == std::string_view::npos && fit(t.row, l, c);
}

// 表头的锚点 = 最长的字面段 (末段连 \r\n); back = 锚点前各段的最宽字节数
struct Anchor {
  std::string lit;
  size_t back = 0;
};
Anchor anchor_of(std::string_view p) {
  Anchor best;
  size_t at = 0;
  while (!p.empty() && !p.starts_with("...")) {
    if (p[0] == '{') {
      const std::string_view t = tok(p);
      const size_t w = width(t);
      at += w ? w : kDigits;
      continue;
    }
    assert(p[0] != '*');
    const size_t k = literal(p);
    std::string lit(p.substr(0, k));
    p.remove_prefix(k);
    if (p.empty())
      lit += "\r\n";
    if (lit.size() > best.lit.size())
      best = {std::move(lit), at};
    at += k;
  }
  assert(!best.lit.empty());
  return best;
}

// ---------- 块 ----------

// 块首整块匹配: 表头一行 + 其后连续的数据行 (行 = 到首个 \r 或 \n 为止, 须以 \r\n 结尾)
struct Match {
  size_t len = 0, head = 0, rows = 0;
  Caps caps;
};

std::optional<Match> match(const Shape &t, std::string_view d) {
  const auto line_at = [&](size_t at, std::string_view &line) {
    const size_t e = d.find_first_of("\r\n", at);
    if (e == std::string_view::npos || d[e] != '\r' || e + 1 >= d.size() || d[e + 1] != '\n')
      return false;
    line = d.substr(at, e - at);
    return true;
  };
  Match m;
  std::string_view line;
  if (!line_at(0, line) || !fit(t.head, line, m.caps))
    return std::nullopt;
  m.head = m.len = line.size() + 2;
  while (m.len < d.size() && line_at(m.len, line) && row_ok(t, line)) {
    m.len += line.size() + 2;
    ++m.rows;
  }
  if (!m.rows && !t.empty)
    return std::nullopt;
  return m;
}

// 整块 → 块字节: 表头带行数 n 即表头 + 前 n 行, 后随不足 n 行或不该空的表 n = 0 → 无; 不带即整块
std::optional<std::string> body(const Shape &t, std::string_view d, const Match &m) {
  const long n = m.caps.n;
  if (n < 0)
    return std::string(d.substr(0, m.len));
  if (!((n || t.empty) && static_cast<size_t>(n) <= m.rows))
    return std::nullopt;
  size_t at = m.head;
  for (long i = 0; i < n; ++i)
    at = d.find("\r\n", at) + 2;
  return std::string(d.substr(0, at));
}

// 锚点在 i: 往前至多 back 字节找表头起点, 首个整块匹配的即块 (块字节不成立即无)
std::optional<Hit> hit_at(const Shape &t, Addr lo, Addr end, std::string_view data, size_t i, size_t back) {
  for (size_t hs = i > back ? i - back : 0; hs <= i; ++hs) {
    if (data[hs] != '0' || hs + 1 >= data.size() || data[hs + 1] != '|')
      continue;
    const std::string_view d = data.substr(hs);
    const auto m = match(t, d);
    if (!m)
      continue;
    auto b = body(t, d, *m);
    if (!b)
      return std::nullopt;
    const Addr a = lo + hs;
    const Addr e = std::min<Addr>(end, a + b->size() + t.slack);
    return Hit{a, std::string(m->caps.tag), std::move(*b), e};
  }
  return std::nullopt;
}

// 一遍扫的各形状按锚点归组: 同锚点的共一遍查找
class Finder {
public:
  explicit Finder(const std::vector<const Shape *> &shapes) : shapes_(shapes) {
    for (size_t i = 0; i < shapes.size(); ++i) {
      Anchor a = anchor_of(shapes[i]->head);
      auto g = std::find_if(groups_.begin(), groups_.end(), [&](const Group &x) { return x.anchor == a.lit; });
      if (g == groups_.end())
        g = groups_.insert(groups_.end(), Group{std::move(a.lit), {}});
      g->shapes.push_back({i, a.back});
    }
  }

  void find(Addr lo, std::string_view data, std::vector<std::vector<Hit>> &out) const {
    const Addr end = lo + data.size();
    for (const Group &g : groups_)
      for (const char *p = memfind(data, g.anchor, 0); p; p = memfind(data, g.anchor, static_cast<size_t>(p - data.data()) + 1)) {
        const size_t i = static_cast<size_t>(p - data.data());
        for (const auto &[s, back] : g.shapes)
          if (auto h = hit_at(*shapes_[s], lo, end, data, i, back))
            out[s].push_back(std::move(*h));
      }
  }

private:
  struct Group {
    std::string anchor;
    std::vector<std::pair<size_t, size_t>> shapes; // (形状下标, back)
  };
  const std::vector<const Shape *> &shapes_;
  std::vector<Group> groups_;
};

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

std::vector<std::vector<Hit>> Heap::scan(const std::vector<const Shape *> &shapes, const std::vector<Addr> *near) const {
  const Finder finder(shapes);
  const std::vector<Region> rs = regions(pid_, near);
  std::vector<std::vector<std::vector<Hit>>> per(rs.size(), std::vector<std::vector<Hit>>(shapes.size()));
  {
    std::lock_guard lk(pool_mu());
    pool().tasks(static_cast<int>(rs.size()), [&](int i, int) {
      const Region &r = rs[static_cast<size_t>(i)];
      const size_t n = r.hi - r.lo;
      const auto buf = std::make_unique_for_overwrite<char[]>(n);
      const size_t got = pread_full(fd_, buf.get(), n, r.lo);
      if (got)
        finder.find(r.lo, std::string_view(buf.get(), got), per[static_cast<size_t>(i)]);
    });
  }
  std::vector<std::vector<Hit>> out(shapes.size());
  for (auto &seg : per)
    for (size_t s = 0; s < shapes.size(); ++s)
      out[s].insert(out[s].end(), std::make_move_iterator(seg[s].begin()), std::make_move_iterator(seg[s].end()));
  return out;
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

std::optional<std::string> reread(const Shape &t, const Heap &h, const Hit &hit) {
  const std::string d = h.read(hit.addr, hit.end - hit.addr);
  const auto m = match(t, d);
  if (!m || m->caps.tag != hit.tag)
    return std::nullopt;
  return body(t, d, *m);
}

Split split(const Shape &t, std::string_view raw, std::span<const int> views) {
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
    Fields f;
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
  const auto v = std::find(views.begin(), views.end(), static_cast<int>(nf));
  if (v == views.end())
    fail(strf("%s %zu 列, 不认识的布局", t.name, nf));
  sp.view = static_cast<size_t>(v - views.begin());
  return sp;
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
