#include "disk/Disk.hpp"
#include "misc/cross_platform.hpp"

#include "factor/Stat/Contract.hpp"           // hold_parse / hold_token
#include "features/Storage/FeatureLevels.hpp" // TIER_IDS / has_level
#include "features/Storage/FeatureLib.hpp"    // feature_pools
#include "fundamental/Spec.hpp"               // SPECS
#include "gui/engine/File.hpp"                // ValidFactorName
#include "gui/engine/Table.hpp"               // kFactorLevels
#include "gui/task_agent/Notes.hpp"           // kNoteKinds
#include "gui/task_agent/Prompt.hpp"          // kMsgs
#include "gui/task_agent/Tools.hpp"           // Tools

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <ctime>
#include <fstream>
#include <functional>
#include <optional>

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

// ---- 写法 ----

enum class Form : std::uint8_t { Lit,
                                 Flag,
                                 Date,
                                 Shape,
                                 Word,
                                 Dotted };

struct SegDef {
  Form form;
  std::string_view pat;
  std::string Key::*str; // Shape / Word / Dotted 的字段
};
constexpr SegDef kSegs[] = {
#define DISK_SEG_DEF(id, form, pat, field) {Form::form, pat, field},
    DISK_SEGS(DISK_SEG_DEF)
#undef DISK_SEG_DEF
};
const SegDef &seg(Node n) { return kSegs[static_cast<size_t>(def(n).seg)]; }

bool digit(char c) { return c >= '0' && c <= '9'; }
bool word(std::string_view s) {
  return !s.empty() && std::all_of(s.begin(), s.end(), [](char c) { return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || digit(c) || c == '_'; });
}
bool dotted(std::string_view s) {
  for (size_t i = 0, j = 0; i <= s.size(); j = ++i) {
    i = std::min(s.find_first_of(".+", i), s.size());
    if (!word(s.substr(j, i - j)))
      return false;
  }
  return true;
}
// 模式串的一位: 日期分量 (年 0 / 月 1 / 日 2), 数字 (-1), 字面 (-2)
int slot(char p) { return p == 'Y' ? 0 : p == 'M' ? 1
                                     : p == 'D'   ? 2
                                     : p == '#'   ? -1
                                                  : -2; }
bool shaped(std::string_view v, std::string_view pat) {
  return v.size() == pat.size() && std::equal(v.begin(), v.end(), pat.begin(), [](char c, char p) { return slot(p) == -2 ? c == p : digit(c); });
}

using Ymd3 = std::array<int, 3>; // [年, 月, 日]; 0 = 未定
Ymd3 split(misc::Ymd d) { return {d / 10000, d / 100 % 100, d % 100}; }
bool dated(const Ymd3 &v) { return v[0] >= 1900 && v[1] <= 12 && v[2] <= 31 && (v[2] == 0 || v[1] > 0); }

// 段值由 k 定得出
bool has(const SegDef &s, const Key &k) {
  switch (s.form) {
  case Form::Lit:
  case Form::Flag:
    return true;
  case Form::Date: {
    const Ymd3 v = split(k.date);
    return std::all_of(s.pat.begin(), s.pat.end(), [&](char p) { return slot(p) < 0 || v[slot(p)] > 0; });
  }
  default:
    return !(k.*s.str).empty();
  }
}

// 段值 → 写法
std::string put(const SegDef &s, const Key &k) {
  switch (s.form) {
  case Form::Lit:
    return {};
  case Form::Flag:
    return k.test ? "test" : "main";
  case Form::Date: {
    Ymd3 v = split(k.date);
    std::string out(s.pat);
    for (size_t i = out.size(); i-- > 0;)
      if (const int a = slot(out[i]); a >= 0)
        out[i] = static_cast<char>('0' + v[a] % 10), v[a] /= 10;
    return out;
  }
  default:
    return k.*s.str;
  }
}

// 写法 → 段值, 记进 k
bool take(const SegDef &s, std::string_view v, Key &k) {
  switch (s.form) {
  case Form::Lit:
    return v.empty();
  case Form::Flag:
    if (v != "main" && v != "test")
      return false;
    k.test = v == "test";
    return true;
  case Form::Date: {
    if (!shaped(v, s.pat))
      return false;
    Ymd3 got{}, had = split(k.date);
    unsigned carried = 0;
    for (size_t i = 0; i < v.size(); ++i)
      if (const int a = slot(s.pat[i]); a >= 0)
        got[a] = got[a] * 10 + (v[i] - '0'), carried |= 1u << a;
    for (int a = 0; a < 3; ++a) {
      if (!(carried >> a & 1))
        continue;
      if (got[a] == 0 || (had[a] && had[a] != got[a]))
        return false;
      had[a] = got[a];
    }
    if (!dated(had))
      return false;
    k.date = had[0] * 10000 + had[1] * 100 + had[2];
    return true;
  }
  case Form::Shape:
    if (!shaped(v, s.pat))
      return false;
    break;
  case Form::Word:
    if (!word(v))
      return false;
    break;
  case Form::Dotted:
    if (!dotted(v))
      return false;
    break;
  }
  k.*s.str = v;
  return true;
}

// ---- 值域 ----

// <因子>_w<权重号>_top<N>_{log,snap}
bool export_name(std::string_view s) {
  const auto digits = [](std::string_view x) { return !x.empty() && std::all_of(x.begin(), x.end(), digit); };
  for (const std::string_view tail : {"_log", "_snap"}) {
    if (!s.ends_with(tail))
      continue;
    std::string_view r = s.substr(0, s.size() - tail.size());
    const size_t top = r.rfind("_top"), w = top == std::string_view::npos ? top : r.rfind("_w", top);
    return w != std::string_view::npos && digits(r.substr(top + 4)) && digits(r.substr(w + 2, top - w - 2)) && GUI::Engine::ValidFactorName(r.substr(0, w));
  }
  return false;
}

std::optional<TierId> tier_of(std::string_view name) {
  for (const TierId t : TIER_IDS)
    if (name == tier_name(t))
      return t;
  return std::nullopt;
}

fundamental::Shape table_shape(Node n) {
  switch (n) {
  case FundShard:
    return fundamental::Shape::Month;
  case FundDaily:
    return fundamental::Shape::Daily;
  default:
    return fundamental::Shape::Meta;
  }
}

// 段值在各模块的真相表里
bool known(Node n, const Key &k) {
  switch (def(n).seg) {
  case Seg::Pool: {
    const auto pools = feature_pools();
    return std::any_of(pools.begin(), pools.end(), [&](const PoolDef &p) { return k.pool == p.name; });
  }
  case Seg::Tier:
    return tier_of(k.tier).has_value();
  case Seg::Lvl: {
    const std::optional<TierId> t = tier_of(k.tier);
    for (size_t l = 0; t && l < LEVEL_COUNT; ++l)
      if (has_level(*t, l) && k.lvl == level_name(l))
        return true;
    return false;
  }
  case Seg::Level:
    return std::any_of(GUI::Engine::kFactorLevels.begin(), GUI::Engine::kFactorLevels.end(), [&](const auto &l) { return k.level == l.name; });
  case Seg::Hold: {
    int h = 0;
    return factor::stat::hold_parse(k.hold, h) && factor::stat::hold_token(h) == k.hold;
  }
  case Seg::Table:
    return std::any_of(fundamental::SPECS.begin(), fundamental::SPECS.end(), [&](const fundamental::Spec &s) {
      return s.name == k.name && (n == FundAuditSheet || s.shape == table_shape(n));
    });
  case Seg::Factor:
  case Seg::Portfolio:
    return GUI::Engine::ValidFactorName(k.name);
  case Seg::Export:
    return export_name(k.name);
  case Seg::Tool: {
    const auto tools = GUI::Agent::Tools();
    return std::any_of(tools.begin(), tools.end(), [&](const GUI::Agent::ToolDef &t) { return k.name == t.name; });
  }
  case Seg::Note:
    return std::any_of(std::begin(GUI::Agent::kNoteKinds), std::end(GUI::Agent::kNoteKinds), [&](const auto &d) { return k.name == d.id; });
  case Seg::Prompt:
    return std::any_of(std::begin(GUI::Agent::kMsgs), std::end(GUI::Agent::kMsgs), [&](const auto &d) { return k.name == d.id; });
  default:
    return true;
  }
}

// ---- 盘 ----

bool is(Node n, const fs::path &p) { return is_dir(n) ? fs::is_directory(p) : fs::is_regular_file(p); }
bool is(Node n, const fs::directory_entry &e) { return is_dir(n) ? e.is_directory() : e.is_regular_file(); }

// 盘上 n 的实例 (k 已定的段照用, 未定的逐级展开), 按路径序逐个交 f(键, 路径)
using Visit = std::function<void(const Key &, const fs::path &)>;
void each(Node n, const Key &k, const Visit &f) {
  if (n == Root) {
    f(k, root());
    return;
  }
  each(def(n).parent, k, [&](const Key &up, const fs::path &dir) {
    if (has(seg(n), up)) {
      if (const fs::path p = dir / name(n, up); is(n, p))
        f(up, p);
      return;
    }
    std::vector<fs::directory_entry> entries;
    std::error_code ec; // 别的线程可能正把这层整个删掉: 读到哪算哪
    for (fs::directory_iterator it(dir, ec), end; !ec && it != end; it.increment(ec))
      entries.push_back(*it);
    std::sort(entries.begin(), entries.end());
    for (const fs::directory_entry &e : entries)
      if (Key kk = up; is(n, e) && parse(n, e.path().filename().string(), kk))
        f(kk, e.path());
  });
}

} // namespace

fs::path root() {
  static const fs::path r = find_root();
  return r;
}

std::string name(Node n, const Key &k) {
  assert(has(seg(n), k) && "路径段未定");
  const std::string s = def(n).prefix + put(seg(n), k) + def(n).suffix;
  [[maybe_unused]] Key probe = k;
  assert(parse(n, s, probe) && probe == k && "路径段不合布局 (写法 / 值域)");
  return s;
}

bool parse(Node n, std::string_view name, Key &k) {
  const std::string_view pre = def(n).prefix, suf = def(n).suffix;
  if (name.size() < pre.size() + suf.size() || !name.starts_with(pre) || !name.ends_with(suf))
    return false;
  Key kk = k;
  if (!take(seg(n), name.substr(pre.size(), name.size() - pre.size() - suf.size()), kk) || !known(n, kk))
    return false;
  k = std::move(kk);
  return true;
}

fs::path path(Node n, const Key &k) { return n == Root ? root() : path(def(n).parent, k) / name(n, k); }

std::vector<Key> list(Node n, const Key &k) {
  std::vector<Key> out;
  each(n, k, [&](const Key &kk, const fs::path &) { out.push_back(kk); });
  return out;
}

std::string stamp_now() {
  const std::time_t t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
  const std::tm tm = safe_localtime(&t);
  char b[32];
  std::strftime(b, sizeof(b), "%Y%m%d-%H%M%S", &tm);
  return b;
}

bool exists(Node n, const Key &k) { return is(n, path(n, k)); }

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
  assert(is_dir(n));
  const fs::path p = path(n, k);
  fs::create_directories(p);
  return p;
}

} // namespace disk
