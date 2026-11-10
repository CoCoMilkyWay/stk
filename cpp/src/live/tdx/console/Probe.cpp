// 探内存 (只读): 给新表找块、定列. 先在客户端点过要探的那页查询 (如信用资产、融资标的、担保品), 再跑.
//   列出明文表: 堆里全部 0|…|\r\n 打头、\r\n 分行、| 分列的块: 址 / 副本数 / 行数 / 列数 / 表头 / 前几行, 同表头同首行的副本只列一次;
//   找文本: 各词 (空白分隔) 按 GBK、UTF-16LE 两种编码逐处列出址与前后字节 (按同一编码解码, 控制字符转义)
#include "live/tdx/Mem.hpp"
#include "live/tdx/console/Model.hpp"

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <iconv.h>
#include <map>
#include <set>
#include <thread>

namespace live::tdx::console {

namespace {

using Lock = std::lock_guard<std::mutex>;

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

// GBK → UTF-8, 非法字节换 U+FFFD (0x80 即 €)
std::string gbk_utf8(std::string_view raw) {
  thread_local const iconv_t cd = open_cd("UTF-8", "GBK");
  std::string out;
  char buf[4096];
  char *in = const_cast<char *>(raw.data());
  size_t inl = raw.size();
  iconv(cd, nullptr, nullptr, nullptr, nullptr);
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
  for (const uint32_t cp : cps_of(utf16 ? utf16_utf8(raw) : gbk_utf8(raw)))
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

std::vector<std::string> words_of(const std::string &s) {
  std::vector<std::string> out;
  for (size_t i = 0; i < s.size();) {
    const size_t a = s.find_first_not_of(" \t\r\n", i);
    if (a == std::string::npos)
      break;
    const size_t b = std::min(s.find_first_of(" \t\r\n", a), s.size());
    out.push_back(s.substr(a, b - a));
    i = b;
  }
  return out;
}

void run(const std::shared_ptr<Core> &cp, std::vector<std::string> words) {
  Core &c = *cp;
  {
    Lock lk(c.mu);
    if (c.probing)
      return;
    c.probing = true;
  }
  std::thread([cp, words = std::move(words)] {
    std::string out;
    try {
      const mem::HeapP h = mem::attach();
      out = words.empty() ? tables(*h) : texts(*h, words);
    } catch (const Fail &f) {
      out = "✗ " + f.why;
    }
    Lock lk(cp->mu);
    cp->probe_out = std::move(out);
    cp->probing = false;
    ++cp->probe_ver;
  }).detach();
}

} // namespace

void draw_probe(const std::shared_ptr<Core> &c, View &v) {
  ImGui::TextDisabled("先在客户端点过要探的那页查询（如信用资产、融资标的、担保品），再跑");
  ImGui::BeginDisabled(v.probing);
  if (ImGui::Button("列出明文表"))
    run(c, {});
  ImGui::SameLine();
  ImGui::SetNextItemWidth(-ImGui::GetFontSize() * 12);
  const bool enter = input_text("##words", v.words, "要找的文本（空白分隔多个）", ImGuiInputTextFlags_EnterReturnsTrue);
  ImGui::SameLine();
  if ((ImGui::Button("找文本") || enter) && !words_of(v.words).empty())
    run(c, words_of(v.words));
  ImGui::EndDisabled();
  ImGui::SameLine();
  if (ImGui::Button("复制##probe"))
    ImGui::SetClipboardText(v.probe_out.c_str());
  if (v.probing) {
    ImGui::SameLine();
    ImGui::TextDisabled("探测中…");
  }
  out_box("##probeout", v.probe_out, 0);
}

} // namespace live::tdx::console
