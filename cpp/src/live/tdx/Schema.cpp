#include "live/tdx/Schema.hpp"

#include "live/Broker.hpp"
#include "live/tdx/Trace.hpp"

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <regex>
#include <sstream>

namespace live::tdx {

namespace {

namespace fs = std::filesystem;

constexpr size_t kRecord = 35, kNeedle = 4096;
constexpr int kFuncMark = 0x4bd;

// 客户端目录: 进程映射里 TdxW.exe 所在
fs::path client_dir(int pid) {
  std::ifstream in("/proc/" + std::to_string(pid) + "/maps");
  for (std::string line; std::getline(in, line);) {
    const size_t slash = line.find('/');
    if (slash == std::string::npos)
      continue;
    std::string low = line.substr(slash);
    std::transform(low.begin(), low.end(), low.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    if (low.ends_with("/tdxw.exe"))
      return fs::path(line.substr(slash)).parent_path();
  }
  fail("进程映射里没有 TdxW.exe");
}

std::string slurp(const fs::path &p) {
  std::ifstream in(p, std::ios::binary);
  need(in.good(), "读不了 " + p.string());
  return {std::istreambuf_iterator<char>(in), {}};
}

uint16_t u16(std::string_view b, size_t i) {
  uint16_t v;
  std::memcpy(&v, b.data() + i, 2);
  return v;
}

bool in_heap(const mem::Heap &h, std::string_view needle) {
  bool hit = false;
  h.each([&](mem::Addr, std::string_view d) { hit = hit || d.find(needle) != std::string_view::npos; });
  return hit;
}

} // namespace

std::string Schema::name(int fid) const {
  const auto i = fields.find(fid);
  return i == fields.end() ? "?" : i->second;
}

Schema schema(const mem::Heap &h) {
  const fs::path dir = client_dir(h.pid()) / "T0002" / "wt_cache" / "TcApi_Cache";
  std::vector<std::pair<fs::path, std::string>> live;
  for (const fs::directory_entry &e : fs::directory_iterator(dir)) {
    const std::string fn = e.path().filename().string();
    if (!fn.starts_with("40-BRANCH-"))
      continue;
    std::string b = slurp(e.path());
    const size_t text_end = b.find(std::string_view("\r\n\0", 3));
    if (text_end != std::string::npos && in_heap(h, std::string_view(b).substr(text_end + 3, kNeedle)))
      live.emplace_back(e.path(), std::move(b));
  }
  need(live.size() == 1, strf("%s 下字典段在客户端堆里的缓存有 %zu 份%s", dir.string().c_str(), live.size(), live.empty() ? " (客户端交易还没登录?)" : ""));
  const std::string_view b = live[0].second;
  Schema s{live[0].first.string(), {}, {}, {}};

  const size_t text_end = b.find(std::string_view("\r\n\0", 3));
  std::istringstream text(std::string(b.substr(0, text_end)));
  const std::regex wtgn(R"(^WTGN_\d+=(\d+),(.*)$)");
  for (std::string line; std::getline(text, line);) {
    if (!line.empty() && line.back() == '\r')
      line.pop_back();
    std::smatch m;
    const std::string u = mem::gbk(line);
    if (std::regex_match(u, m, wtgn))
      s.titles.emplace(std::stoi(m[1]), m[2]);
  }

  size_t p = text_end + 3;
  const size_t nf = u16(b, p);
  p += 2;
  need(p + nf * kRecord <= b.size(), "字段字典越过文件尾");
  for (size_t i = 0; i < nf; ++i, p += kRecord) {
    const std::string_view r = b.substr(p, kRecord);
    s.fields.emplace(u16(r, 0), mem::gbk(r.substr(6, r.find('\0', 6) == std::string_view::npos ? kRecord - 6 : r.find('\0', 6) - 6)));
  }

  std::vector<int> w;
  for (size_t i = p; i + 2 <= b.size(); i += 2)
    w.push_back(u16(b, i));
  // 逐格认: 标记、出参数之后恰是功能号 + 1、入参出参的字段号都在字典里 (同号取先见的)
  const auto known = [&](auto b, auto e) { return std::all_of(b, e, [&](int f) { return s.fields.contains(f); }); };
  for (size_t i = 0; i + 3 < w.size(); ++i) {
    const size_t nin = static_cast<size_t>(w[i + 1]), j = i + 3 + nin;
    if (w[i] != kFuncMark || j + 1 >= w.size() || w[j + 1] != w[i + 2] + 1 || j + 2 + static_cast<size_t>(w[j]) > w.size())
      continue;
    const auto in = w.begin() + static_cast<long>(i) + 3, out = w.begin() + static_cast<long>(j) + 2, end = out + w[j];
    if (known(in, in + static_cast<long>(nin)) && known(out, end))
      s.funcs.emplace(w[i + 2], Schema::Func{{in, in + static_cast<long>(nin)}, {out, end}});
  }
  need(!s.fields.empty() && !s.funcs.empty(), "协议字典为空: " + s.file);
  return s;
}

} // namespace live::tdx
