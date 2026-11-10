#include "misc/file_check.hpp"
#include "misc/cross_platform.hpp"

#include <algorithm>
#include <cassert>
#include <cctype>
#include <cstdlib>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace FileCheck {

namespace fs = std::filesystem;

namespace {

bool is_digits(std::string_view s, size_t n) {
  return s.size() == n && std::all_of(s.begin(), s.end(), [](unsigned char c) { return std::isdigit(c); });
}

bool tools_available() {
  for (const char *tool : {"unrar", "7z", "rar"})
    if (std::system((std::string("which ") + tool + " > /dev/null 2>&1").c_str()) != 0)
      return false;
  return true;
}

// RAR 4.x 主头 (标记块后第一块, 类型 0x73) 的 flags bit 3 = 固实. RAR 5.x 的固实位在 vint 编码的主头里, 不判
bool solid_rar4(const std::string &path) {
  unsigned char h[14] = {};
  if (read_file_head(path.c_str(), h, sizeof(h)) < sizeof(h))
    return false;
  const bool rar4 = h[4] == 0x1a && h[5] == 0x07 && h[6] == 0x00;
  const unsigned head_type = h[9];
  const unsigned head_flags = h[10] | (h[11] << 8);
  return rar4 && head_type == 0x73 && (head_flags & 0x0008) != 0;
}

// `unrar lb` 只读索引 (不解压数据): 非零退出 / 损坏标记 = Integrity; 否则找不到 YYYYMMDD/ 开头的条目 = Structure.
// stdout / stderr 合流, 诊断可能排在文件列表前面, 所以逐行找而不是只看首行
std::optional<Finding> probe(const std::string &path) {
  FILE *pipe = safe_popen(("unrar lb \"" + path + "\" 2>&1").c_str(), "r");
  assert(pipe && "popen for unrar lb failed");
  std::string text;
  char buf[4096];
  while (fgets(buf, sizeof(buf), pipe))
    text += buf;
  const int code = safe_pclose(pipe);

  if (code != 0 || text.find("Unexpected end of archive") != std::string::npos ||
      text.find("Corrupt header") != std::string::npos)
    return Finding{Issue::Integrity, path, "unrar exit " + std::to_string(code)};
  for (size_t pos = 0; pos < text.size();) {
    const size_t nl = std::min(text.find('\n', pos), text.size());
    const std::string_view line(text.data() + pos, nl - pos);
    if (line.size() >= 9 && line[8] == '/' && is_digits(line.substr(0, 8), 8))
      return std::nullopt;
    pos = nl + 1;
  }
  return Finding{Issue::Structure, path, "no YYYYMMDD/ entry"};
}

const char *format_name(Format f) {
  constexpr const char *kNames[] = {"rar", "7z", "zip", "unknown"};
  return kNames[static_cast<size_t>(f)];
}

} // namespace

Format format_of(const std::string &path) {
  unsigned char h[6] = {};
  read_file_head(path.c_str(), h, sizeof(h));
  if (h[0] == 'R' && h[1] == 'a' && h[2] == 'r' && h[3] == '!')
    return Format::Rar;
  if (h[0] == '7' && h[1] == 'z' && h[2] == 0xbc && h[3] == 0xaf && h[4] == 0x27 && h[5] == 0x1c)
    return Format::SevenZip;
  if (h[0] == 'P' && h[1] == 'K' && h[2] == 0x03 && h[3] == 0x04)
    return Format::Zip;
  return Format::Unknown;
}

size_t FileCheckResult::count(Issue i) const {
  return static_cast<size_t>(std::count_if(findings.begin(), findings.end(), [i](const Finding &f) { return f.issue == i; }));
}

size_t FileCheckResult::repairable() const {
  return static_cast<size_t>(std::count_if(findings.begin(), findings.end(), [](const Finding &f) { return FileCheck::repairable(f.issue); }));
}

FileCheckResult check(const std::string &archive_dir, const ProgressCallback &progress) {
  FileCheckResult r;
  r.ran = true;
  r.dir_exists = fs::exists(archive_dir);
  if (!r.dir_exists)
    return r;
  r.tools_ok = tools_available();
  if (!r.tools_ok)
    return r;

  // 一遍走树: 命名 → 格式 (读魔数, 廉价) 当场定; 合格的 RAR 留待 unrar 顺序探测
  auto naming = [&](const fs::path &p, std::string detail) { r.findings.push_back({Issue::Naming, p.string(), std::move(detail)}); };
  std::vector<std::string> to_probe;
  for (const auto &year : fs::directory_iterator(archive_dir)) {
    const std::string y = year.path().filename().string();
    if (!year.is_directory() || !is_digits(y, 4)) {
      naming(year.path(), "expected YYYY/");
      continue;
    }
    for (const auto &month : fs::directory_iterator(year.path())) {
      const std::string m = month.path().filename().string();
      if (!month.is_directory() || !is_digits(m, 6) || !m.starts_with(y)) {
        naming(month.path(), "expected " + y + "/YYYYMM/");
        continue;
      }
      for (const auto &file : fs::directory_iterator(month.path())) {
        const fs::path &p = file.path();
        const std::string stem = p.stem().string();
        const std::string ext = p.extension().string();
        const bool container_ext = ext == ".zip" || ext == ".7z";
        if (!file.is_regular_file() || !(ext == ".rar" || container_ext) || !is_digits(stem, 8) || !stem.starts_with(m)) {
          naming(p, "expected " + m + "/" + m + "DD.rar");
          continue;
        }
        const Format f = format_of(p.string());
        if (f == Format::Unknown)
          r.findings.push_back({Issue::Integrity, p.string(), "unknown format"});
        else if (container_ext || f != Format::Rar)
          r.findings.push_back({Issue::Container, p.string(), std::string(format_name(f)) + " in " + ext});
        else if (solid_rar4(p.string()))
          r.findings.push_back({Issue::Solid, p.string(), "solid RAR 4.x"});
        else
          to_probe.push_back(p.string());
      }
    }
  }

  std::sort(to_probe.begin(), to_probe.end());
  for (size_t i = 0; i < to_probe.size(); ++i) {
    if (std::optional<Finding> f = probe(to_probe[i]))
      r.findings.push_back(std::move(*f));
    else
      ++r.valid;
    if (progress)
      progress(i + 1, to_probe.size(), to_probe[i]);
  }

  std::sort(r.findings.begin(), r.findings.end(), [](const Finding &a, const Finding &b) { return a.path < b.path; });
  return r;
}

} // namespace FileCheck
