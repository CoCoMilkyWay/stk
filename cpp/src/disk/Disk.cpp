#include "disk/Disk.hpp"
#include "misc/cross_platform.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <ctime>
#include <fstream>

namespace disk {

namespace fs = std::filesystem;

namespace {

fs::path find_root() {
  fs::path p = fs::current_path();
  while (!fs::exists(p / ".git")) {
    assert(p != p.parent_path() && "仓库根找不到 (cwd 之上无 .git)");
    p = p.parent_path();
  }
  return p;
}

// 根之下到 n 的节点链 (不含 Root)
std::vector<Node> chain(Node n) {
  std::vector<Node> c;
  for (; n != Root; n = def(n).parent)
    c.push_back(n);
  std::reverse(c.begin(), c.end());
  return c;
}

bool digits(std::string_view s, size_t n) {
  return s.size() == n && std::all_of(s.begin(), s.end(), [](char c) { return c >= '0' && c <= '9'; });
}
int num(std::string_view s) {
  int v = 0;
  for (const char c : s)
    v = v * 10 + (c - '0');
  return v;
}
// 名字段: 非空, 只含 [A-Za-z0-9_]
bool token(std::string_view s) {
  return !s.empty() && std::all_of(s.begin(), s.end(), [](char c) { return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_'; });
}
bool valid_ymd(int y, int m, int d) { return y >= 1900 && m >= 1 && m <= 12 && d >= 1 && d <= 31; }

int year_of(misc::Ymd d) { return d / 10000; }
int month_of(misc::Ymd d) { return d / 100 % 100; }
int day_of(misc::Ymd d) { return d % 100; }

// 字符串段在 Key 里的字段
template <class K>
auto &field(K &k, Seg s) {
  switch (s) {
  case Seg::Pool:
    return k.pool;
  case Seg::Tier:
    return k.tier;
  case Seg::Lvl:
    return k.lvl;
  case Seg::Level:
    return k.level;
  case Seg::Hold:
    return k.hold;
  case Seg::Stamp:
    return k.stamp;
  default:
    return k.name;
  }
}

// n 的段能否由 k 定
bool resolved(Node n, const Key &k) {
  switch (def(n).seg) {
  case Seg::Lit:
  case Seg::Mode:
    return true;
  case Seg::Year:
    return year_of(k.date) > 0;
  case Seg::Month:
    return month_of(k.date) > 0;
  case Seg::Day:
  case Seg::Ymd:
  case Seg::Date:
    return day_of(k.date) > 0;
  case Seg::Ym:
    return k.ym > 0 || day_of(k.date) > 0;
  default:
    return !field(k, def(n).seg).empty();
  }
}

std::string two(int v) {
  char b[8];
  std::snprintf(b, sizeof(b), "%02d", v);
  return b;
}

// n 在 k 下的名字 (段须已定且写法合法)
std::string name_of(Node n, const Key &k) {
  assert(resolved(n, k) && "路径段未定");
  const Def &d = def(n);
  std::string v;
  switch (d.seg) {
  case Seg::Lit:
    break;
  case Seg::Mode:
    v = k.test ? "test" : "main";
    break;
  case Seg::Year:
    v = std::to_string(year_of(k.date));
    break;
  case Seg::Month:
    v = two(month_of(k.date));
    break;
  case Seg::Day:
    v = two(day_of(k.date));
    break;
  case Seg::Ymd:
    v = misc::dense(k.date);
    break;
  case Seg::Date:
    v = misc::dash(k.date);
    break;
  case Seg::Ym:
    v = misc::dash_ym(k.ym ? k.ym : misc::ym_of(k.date));
    break;
  default:
    v = field(k, d.seg);
    break;
  }
  const std::string s = d.prefix + v + d.suffix;
  [[maybe_unused]] Key probe = k;
  assert(parse(n, s, probe) && "路径段写法不合布局");
  return s;
}

void expand(const fs::path &at, const std::vector<Node> &c, size_t i, const Key &k, std::vector<Key> &out) {
  if (i == c.size()) {
    out.push_back(k);
    return;
  }
  const Node n = c[i];
  const bool last = i + 1 == c.size();
  const auto fits = [&](const fs::path &p) { return def(n).kind == Kind::File ? fs::is_regular_file(p) : fs::is_directory(p); };
  if (resolved(n, k)) {
    const fs::path p = at / name_of(n, k);
    if (last ? fits(p) : fs::is_directory(p))
      expand(p, c, i + 1, k, out);
    return;
  }
  std::vector<fs::path> entries;
  std::error_code ec; // 别的线程可能正把这层整个删掉: 读到哪算哪
  for (fs::directory_iterator it(at, ec), end; !ec && it != end; it.increment(ec))
    entries.push_back(it->path());
  std::sort(entries.begin(), entries.end());
  for (const fs::path &p : entries) {
    Key kk = k;
    if (fits(p) && parse(n, p.filename().string(), kk))
      expand(p, c, i + 1, kk, out);
  }
}

} // namespace

fs::path root() {
  static const fs::path r = find_root();
  return r;
}

bool parse(Node n, std::string_view name, Key &k) {
  const Def &d = def(n);
  const std::string_view pre = d.prefix, suf = d.suffix;
  if (d.seg == Seg::Lit)
    return name == pre;
  if (name.size() <= pre.size() + suf.size() || !name.starts_with(pre) || !name.ends_with(suf))
    return false;
  const std::string_view v = name.substr(pre.size(), name.size() - pre.size() - suf.size());
  switch (d.seg) {
  case Seg::Mode:
    if (v != "main" && v != "test")
      return false;
    k.test = v == "test";
    return true;
  case Seg::Year:
    if (!digits(v, 4) || num(v) < 1900)
      return false;
    k.date = num(v) * 10000;
    return true;
  case Seg::Month:
    if (!digits(v, 2) || num(v) < 1 || num(v) > 12 || year_of(k.date) == 0)
      return false;
    k.date = year_of(k.date) * 10000 + num(v) * 100;
    return true;
  case Seg::Day:
    if (!digits(v, 2) || num(v) < 1 || num(v) > 31 || month_of(k.date) == 0)
      return false;
    k.date = k.date / 100 * 100 + num(v);
    return true;
  case Seg::Ymd: {
    if (!digits(v, 8))
      return false;
    const misc::Ymd x = num(v);
    if (!valid_ymd(year_of(x), month_of(x), day_of(x)) || (year_of(k.date) && year_of(k.date) != year_of(x)) || (month_of(k.date) && month_of(k.date) != month_of(x)))
      return false;
    k.date = x;
    return true;
  }
  case Seg::Date: {
    if (v.size() != 10 || v[4] != '-' || v[7] != '-' || !digits(v.substr(0, 4), 4) || !digits(v.substr(5, 2), 2) || !digits(v.substr(8, 2), 2))
      return false;
    const misc::Ymd x = num(v.substr(0, 4)) * 10000 + num(v.substr(5, 2)) * 100 + num(v.substr(8, 2));
    if (!valid_ymd(year_of(x), month_of(x), day_of(x)) || (k.ym && k.ym != misc::ym_of(x)))
      return false;
    k.date = x;
    return true;
  }
  case Seg::Ym: {
    if (v.size() != 7 || v[4] != '-' || !digits(v.substr(0, 4), 4) || !digits(v.substr(5, 2), 2))
      return false;
    const misc::Ym m = num(v.substr(0, 4)) * 100 + num(v.substr(5, 2));
    if (m / 100 < 1900 || m % 100 < 1 || m % 100 > 12 || (day_of(k.date) && misc::ym_of(k.date) != m))
      return false;
    k.ym = m;
    return true;
  }
  case Seg::Stamp:
    if (v.size() != 15 || v[8] != '-' || !digits(v.substr(0, 8), 8) || !digits(v.substr(9, 6), 6))
      return false;
    k.stamp = v;
    return true;
  default:
    if (!token(v))
      return false;
    field(k, d.seg) = v;
    return true;
  }
}

fs::path path(Node n, const Key &k) {
  fs::path p = root();
  for (const Node c : chain(n))
    p /= name_of(c, k);
  return p;
}

bool resolvable(Node n, const Key &k) {
  const std::vector<Node> c = chain(n);
  return std::all_of(c.begin(), c.end(), [&](Node x) { return resolved(x, k); });
}

std::vector<Key> list(Node n, const Key &k) {
  std::vector<Key> out;
  expand(root(), chain(n), 0, k, out);
  return out;
}

std::string stamp_now() {
  const std::time_t t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
  const std::tm tm = safe_localtime(&t);
  char b[32];
  std::strftime(b, sizeof(b), "%Y%m%d-%H%M%S", &tm);
  return b;
}

bool exists(Node n, const Key &k) {
  const fs::path p = path(n, k);
  return def(n).kind == Kind::File ? fs::is_regular_file(p) : fs::is_directory(p);
}

std::string read(Node n, const Key &k) {
  assert(def(n).kind == Kind::File);
  std::ifstream f(path(n, k), std::ios::binary);
  assert(f.is_open() && "disk::read: 文件不在");
  return {std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>()};
}

void write(Node n, const Key &k, std::string_view bytes) {
  assert(def(n).kind == Kind::File);
  const fs::path p = path(n, k);
  fs::create_directories(p.parent_path());
  fs::path tmp = p;
  tmp += ".tmp";
  {
    std::ofstream f(tmp, std::ios::binary | std::ios::trunc);
    assert(f.is_open() && "disk::write: 临时文件建不出来");
    f.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    assert(f.good() && "disk::write: 写入失败");
  }
  fs::rename(tmp, p);
}

void append(Node n, const Key &k, std::string_view line) {
  assert(def(n).kind == Kind::File);
  const fs::path p = path(n, k);
  fs::create_directories(p.parent_path());
  std::ofstream f(p, std::ios::binary | std::ios::app);
  assert(f.is_open() && "disk::append: 文件打不开");
  f << line << '\n';
  assert(f.good() && "disk::append: 写入失败");
}

void remove(Node n, const Key &k) {
  const fs::path p = path(n, k);
  if (def(n).kind == Kind::File) {
    fs::remove(p);
    return;
  }
  static std::atomic<std::uint64_t> seq{0};
  fs::path grave = p;
  grave += ".trash." + std::to_string(std::chrono::system_clock::now().time_since_epoch().count()) + "." + std::to_string(seq.fetch_add(1));
  std::error_code ec;
  fs::rename(p, grave, ec);
  if (ec) {
    assert(ec == std::errc::no_such_file_or_directory && "disk::remove: rename 成墓碑失败");
    return;
  }
  fs::remove_all(grave);
}

void rename(Node n, const Key &from, const Key &to) {
  const fs::path a = path(n, from), b = path(n, to);
  assert(fs::exists(a) && !fs::exists(b) && "disk::rename: 源须在, 目标须不在");
  fs::create_directories(b.parent_path());
  fs::rename(a, b);
}

fs::path ensure(Node n, const Key &k) {
  assert(def(n).kind != Kind::File);
  const fs::path p = path(n, k);
  fs::create_directories(p);
  return p;
}

} // namespace disk
