// shim probe — 探进程堆 (只读): 给新表找块、定列. 先在客户端点过要探的那页查询 (如信用资产、融资标的、担保品), 再跑.
//   tables: 堆里全部 0|…|\r\n 打头、\r\n 分行、| 分列的块: 址 / 副本数 / 行数 / 列数 / 表头 / 前几行, 同表头同首行的副本只列一次;
//   text <词…>: 各词 (空白分隔) 按 GBK、UTF-16LE 两种编码逐处列出址与前后字节 (按同一编码解码, 控制字符转义)
//   obj <址> [字数] [槽数]: 对象逐字标去处 (模块+偏移 / 指向的对象及其 RTTI 类名), 首字是 vtable 时另列各槽; 摸请求层的对象图用
//   ref <值> [回看字节]: 堆里存着该值的各处及其所在对象 (类名 + 字段偏移); 如拿窗口句柄找持有它的对象
//   class <类名>: 该 RTTI 类的各张虚表 (嵌套类写 内@外); 给 shim vt 定钩哪张表
//   schema [功能号…]: 协议字典 (Schema.hpp): 空 = 各功能一行 (功能号 / 名称 / 入参数 / 出参数), 给了即列各功能的入参与出参 (字段号 + 名称)
#include "live/tdx/shim/Tool.hpp"

#include "live/Base.hpp"
#include "live/tdx/Heap.hpp"
#include "live/tdx/Image.hpp"
#include "live/tdx/Schema.hpp"
#include "live/tdx/Shim.hpp"
#include "live/tdx/Win.hpp"

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <iconv.h>
#include <map>
#include <optional>
#include <set>

namespace live::tdx::tool {

namespace {

constexpr size_t kHeadMax = 200, kRowsMax = 5000, kShowRows = 3, kContext = 300;

// ---------- 编解码 ----------

void put_utf8(std::string &out, uint32_t cp) {
  if (cp < 0x80)
    out += static_cast<char>(cp);
  else if (cp < 0x800) {
    out += static_cast<char>(0xC0 | (cp >> 6));
    out += static_cast<char>(0x80 | (cp & 0x3F));
  } else if (cp < 0x10000) {
    out += static_cast<char>(0xE0 | (cp >> 12));
    out += static_cast<char>(0x80 | ((cp >> 6) & 0x3F));
    out += static_cast<char>(0x80 | (cp & 0x3F));
  } else {
    out += static_cast<char>(0xF0 | (cp >> 18));
    out += static_cast<char>(0x80 | ((cp >> 12) & 0x3F));
    out += static_cast<char>(0x80 | ((cp >> 6) & 0x3F));
    out += static_cast<char>(0x80 | (cp & 0x3F));
  }
}

std::vector<uint32_t> cps_of(std::string_view s) { // 合法 UTF-8
  std::vector<uint32_t> out;
  for (size_t i = 0; i < s.size();) {
    const auto b = static_cast<unsigned char>(s[i]);
    const int n = b < 0x80 ? 1 : b < 0xE0 ? 2
                             : b < 0xF0   ? 3
                                          : 4;
    uint32_t cp = n == 1 ? b : b & (0x3F >> (n - 1));
    for (int k = 1; k < n; ++k)
      cp = (cp << 6) | (static_cast<unsigned char>(s[i + static_cast<size_t>(k)]) & 0x3F);
    out.push_back(cp);
    i += static_cast<size_t>(n);
  }
  return out;
}

iconv_t open_cd(const char *to, const char *from) {
  const iconv_t cd = iconv_open(to, from);
  assert(cd != reinterpret_cast<iconv_t>(-1));
  return cd;
}

std::string utf8_gbk(const std::string &s) {
  thread_local const iconv_t cd = open_cd("GBK", "UTF-8");
  std::string out(s.size() * 2 + 4, '\0');
  char *in = const_cast<char *>(s.data()), *o = out.data();
  size_t inl = s.size(), ol = out.size();
  iconv(cd, nullptr, nullptr, nullptr, nullptr);
  need(iconv(cd, &in, &inl, &o, &ol) != static_cast<size_t>(-1), "「" + s + "」含 GBK 编不了的字");
  out.resize(out.size() - ol);
  return out;
}

std::string utf16_utf8(std::string_view raw) { // UTF-16LE → UTF-8, 落单的代理与奇数尾字节换 U+FFFD
  std::string out;
  const auto at = [&](size_t i) { return static_cast<uint32_t>(static_cast<unsigned char>(raw[i]) | (static_cast<unsigned char>(raw[i + 1]) << 8)); };
  size_t i = 0;
  for (; i + 1 < raw.size(); i += 2) {
    const uint32_t u = at(i);
    if (u >= 0xD800 && u < 0xDC00 && i + 3 < raw.size() && at(i + 2) >= 0xDC00 && at(i + 2) < 0xE000) {
      put_utf8(out, 0x10000 + ((u - 0xD800) << 10) + (at(i + 2) - 0xDC00));
      i += 2;
    } else
      put_utf8(out, u >= 0xD800 && u < 0xE000 ? 0xFFFD : u);
  }
  if (i < raw.size())
    put_utf8(out, 0xFFFD);
  return out;
}

std::string utf8_utf16(const std::string &s) {
  std::string out;
  const auto unit = [&](uint32_t u) {
    out += static_cast<char>(u & 0xFF);
    out += static_cast<char>(u >> 8);
  };
  for (const uint32_t cp : cps_of(s))
    if (cp < 0x10000)
      unit(cp);
    else {
      unit(0xD800 + ((cp - 0x10000) >> 10));
      unit(0xDC00 + ((cp - 0x10000) & 0x3FF));
    }
  return out;
}

// 解码后不可打印的单字节字符 (C0 / DEL / C1 / NBSP / 软连字符) 转 \xNN
std::string esc(std::string_view raw, bool utf16 = false) {
  std::string out;
  for (const uint32_t cp : cps_of(utf16 ? utf16_utf8(raw) : mem::gbk(raw)))
    if (cp < 0x20 || (cp >= 0x7F && cp <= 0xA0) || cp == 0xAD)
      out += strf("\\x%02x", cp);
    else
      put_utf8(out, cp);
  return out;
}

// ---------- 明文表 ----------

// 行尾 (首个 \r \n \0); 其后须是 \r\n
std::optional<size_t> line_end(std::string_view d, size_t p) {
  const size_t e = d.find_first_of(std::string_view("\r\n\0", 3), p);
  if (e == std::string_view::npos || e + 1 >= d.size() || d[e] != '\r' || d[e + 1] != '\n')
    return std::nullopt;
  return e;
}

// p 起 0|<至多 200 字节>|\r\n 表头, 其后 1..5000 行 (含 |), 各以 \r\n 结尾 → 各行 (不含 \r\n)
std::optional<std::vector<std::string_view>> table_at(std::string_view d, size_t p, size_t &end) {
  const std::optional<size_t> he = line_end(d, p);
  if (!he || *he - p < 3 || *he - p > kHeadMax + 3 || d[*he - 1] != '|')
    return std::nullopt;
  std::vector<std::string_view> lines{d.substr(p, *he - p)};
  size_t q = *he + 2;
  while (lines.size() <= kRowsMax) {
    const std::optional<size_t> e = line_end(d, q);
    if (!e || d.substr(q, *e - q).find('|') == std::string_view::npos)
      break;
    lines.push_back(d.substr(q, *e - q));
    q = *e + 2;
  }
  if (lines.size() < 2)
    return std::nullopt;
  end = q;
  return lines;
}

struct Table {
  mem::Addr addr;
  long n = 1;
  std::vector<std::string> lines;
};

std::string tables(const mem::Heap &h) {
  std::vector<Table> seen;
  std::map<std::pair<std::string, std::string>, size_t> at;
  h.each([&](mem::Addr lo, std::string_view d) {
    for (size_t p = d.find("0|"); p != std::string_view::npos;) {
      size_t end = 0;
      const auto lines = table_at(d, p, end);
      if (!lines) {
        p = d.find("0|", p + 1);
        continue;
      }
      const auto key = std::make_pair(std::string((*lines)[0]), std::string((*lines)[1]));
      if (const auto i = at.find(key); i != at.end())
        ++seen[i->second].n;
      else {
        at.emplace(key, seen.size());
        seen.push_back({lo + p, 1, std::vector<std::string>(lines->begin(), lines->end())});
      }
      p = d.find("0|", end);
    }
  });
  std::string out;
  for (const Table &t : seen) {
    std::set<size_t> cols;
    for (size_t i = 1; i < t.lines.size(); ++i)
      cols.insert(static_cast<size_t>(std::count(t.lines[i].begin(), t.lines[i].end(), '|')) + 1);
    std::string cs;
    for (const size_t c : cols)
      cs += (cs.empty() ? "" : ", ") + std::to_string(c);
    out += strf("== 0x%llx  副本 %ld  行 %zu  列 [%s]  表头 %s\n", static_cast<unsigned long long>(t.addr), t.n, t.lines.size() - 1, cs.c_str(),
                esc(t.lines[0]).c_str());
    for (size_t i = 1; i < t.lines.size() && i <= kShowRows; ++i) {
      const std::string s = esc(t.lines[i]);
      out += "   ";
      size_t k = 0;
      for (size_t a = 0;; ++k) {
        const size_t b = std::min(s.find('|', a), s.size());
        out += strf("%s%zu:", k ? " | " : "", k) + s.substr(a, b - a);
        if (b == s.size())
          break;
        a = b + 1;
      }
      out += "\n";
    }
  }
  return out.empty() ? "（没有明文表）" : out;
}

// ---------- 文本 ----------

std::string texts(const mem::Heap &h, const std::vector<std::string> &words) {
  struct Pat {
    std::string word;
    bool utf16;
    std::string bytes;
  };
  std::vector<Pat> pats;
  for (const std::string &w : words) {
    pats.push_back({w, false, utf8_gbk(w)});
    pats.push_back({w, true, utf8_utf16(w)});
  }
  std::string out;
  h.each([&](mem::Addr lo, std::string_view d) {
    for (const Pat &p : pats)
      for (size_t i = d.find(p.bytes); i != std::string_view::npos; i = d.find(p.bytes, i + 1)) {
        const size_t a = std::max(i % 2, i < kContext ? 0 : i - kContext), b = std::min(d.size(), i + p.bytes.size() + kContext);
        out += strf("== '%s' %s @ 0x%llx\n", p.word.c_str(), p.utf16 ? "utf-16-le" : "gbk", static_cast<unsigned long long>(lo + i)) +
               esc(d.substr(a, b - a), p.utf16) + "\n\n";
      }
  });
  return out.empty() ? "（没找到）" : out;
}

// ---------- 对象 ----------

// 一个值的去处: 模块+偏移 / 对象 (首字是落在模块里的 vtable; 有 RTTI 即类名) / 空
std::string where(const Image &p, uint32_t v) {
  if (p.mod(v))
    return p.at(v);
  const std::optional<uint32_t> vt = p.heap->u32(v);
  if (!vt)
    return "";
  if (p.mod(*vt)) {
    const std::optional<std::string> c = p.rtti(*vt);
    return strf("→ obj %s (vt %s)", c ? c->c_str() : "?", p.at(*vt).c_str());
  }
  return "→ 可读";
}

// 对象 addr 起 n 个字 (4 字节), 每字标去处; 首字是 vtable 时另列其前 slots 个槽
std::string object(const Image &p, mem::Addr addr, size_t n, size_t slots) {
  std::string out;
  const std::optional<uint32_t> vt = p.heap->u32(addr);
  need(vt.has_value(), "%#llx 读不到", static_cast<unsigned long long>(addr));
  if (const std::optional<std::string> c = p.mod(*vt) ? p.rtti(*vt) : std::nullopt)
    out += "类 " + *c + "\n";
  for (size_t i = 0; i < n; ++i)
    if (const std::optional<uint32_t> v = p.heap->u32(addr + 4 * i))
      out += strf("+0x%03zx  %08x  %s\n", 4 * i, *v, where(p, *v).c_str());
  if (p.mod(*vt))
    for (size_t i = 0; i < slots; ++i)
      if (const std::optional<uint32_t> f = p.heap->u32(*vt + 4 * i))
        out += strf("vt[0x%03zx]  %08x  %s\n", 4 * i, *f, where(p, *f).c_str());
  return out;
}

// 32 位进程堆 (4G 以下) 里存着值 v 的各处: 往前至多 back 字节找首个「落在模块里、有 RTTI 的 vtable」即所在对象,
// 逐处报 址 / 对象址 / 类 / 字段偏移; 认不出所在对象的只计数
std::string refs(const mem::Heap &h, const Image &p, uint32_t v, size_t back) {
  std::string out;
  size_t n = 0, bare = 0;
  h.each([&](mem::Addr lo, std::string_view d) {
    if (lo >= (1ull << 32))
      return;
    for (size_t i = 0; i + 4 <= d.size(); i += 4) {
      uint32_t x;
      std::memcpy(&x, d.data() + i, 4);
      if (x != v)
        continue;
      ++n;
      bool found = false;
      for (size_t k = 0; k <= back && k <= i && !found; k += 4) {
        uint32_t vt;
        std::memcpy(&vt, d.data() + i - k, 4);
        if (const std::optional<std::string> c = p.mod(vt) ? p.rtti(vt) : std::nullopt) {
          out += strf("%#llx  对象 %#llx %s +0x%zx\n", static_cast<unsigned long long>(lo + i), static_cast<unsigned long long>(lo + i - k), c->c_str(), k);
          found = true;
        }
      }
      bare += !found;
    }
  });
  return out + strf("共 %zu 处 (认不出所在对象的 %zu 处)\n", n, bare);
}

} // namespace

int probe(const std::vector<std::string> &a) {
  const bool ok = !a.empty() && (a[0] == "tables"  ? a.size() == 1
                                 : a[0] == "text"  ? a.size() > 1
                                 : a[0] == "obj"   ? a.size() >= 2 && a.size() <= 4
                                 : a[0] == "class" ? a.size() == 2
                                 : a[0] == "schema"
                                     ? true
                                     : a[0] == "ref" && a.size() >= 2 && a.size() <= 3);
  need(ok, "用法: shim probe tables | text <词…> | obj <址> [字数 = 32] [槽数 = 0] | ref <值> [回看字节 = 0x200] | class <类名> | schema [功能号…]");
  if (a[0] == "schema") {
    if (!shim::online())
      win::attach();
    const int pid = mem::find_pid();
    need(pid != 0, "找不到 tdxw.exe 进程");
    const Schema s = schema(pid);
    std::printf("%s: 字段 %zu · 功能 %zu\n", s.file.c_str(), s.fields.size(), s.funcs.size());
    const auto title = [&](int fn) { return s.titles.contains(fn) ? s.titles.at(fn) : ""; };
    if (a.size() == 1)
      for (const auto &[fn, f] : s.funcs)
        std::printf("%5d  %-12s 入 %2zu  出 %2zu\n", fn, title(fn).c_str(), f.in.size(), f.out.size());
    for (size_t i = 1; i < a.size(); ++i) {
      const int fn = std::stoi(a[i]);
      need(s.funcs.contains(fn), "没有功能 %d", fn);
      std::printf("== %d %s\n", fn, title(fn).c_str());
      for (const auto &[side, v] : {std::pair{"入", &s.funcs.at(fn).in}, std::pair{"出", &s.funcs.at(fn).out}})
        for (size_t k = 0; k < v->size(); ++k)
          std::printf("  %s %2zu  0x%04x  %s\n", side, k, (*v)[k], s.name((*v)[k]).c_str());
    }
    return 0;
  }
  const mem::HeapP h = mem::attach();
  if (a[0] == "class") {
    const Image p(h);
    for (const mem::Addr vt : p.vtables(a[1]))
      std::printf("0x%08llx  %s\n", static_cast<unsigned long long>(vt), p.at(vt).c_str());
    return 0;
  }
  if (a[0] == "obj" || a[0] == "ref") {
    const Image p(h);
    const auto num = [&](size_t i, unsigned long long dflt) { return a.size() > i ? std::stoull(a[i], nullptr, 0) : dflt; };
    std::fputs((a[0] == "obj" ? object(p, num(1, 0), num(2, 32), num(3, 0)) : refs(*h, p, static_cast<uint32_t>(num(1, 0)), num(2, 0x200))).c_str(), stdout);
    return 0;
  }
  std::fputs((a[0] == "tables" ? tables(*h) : texts(*h, {a.begin() + 1, a.end()})).c_str(), stdout);
  std::fputs("\n", stdout);
  return 0;
}

} // namespace live::tdx::tool
