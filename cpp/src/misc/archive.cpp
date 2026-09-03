#include "misc/archive.hpp"

#include "misc/cross_platform.hpp"

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>

namespace misc {

namespace {

// 去掉行尾 \r\n 与两端空白
std::string trim(const char *begin, const char *end) {
  while (begin < end && (*begin == ' ' || *begin == '\t'))
    ++begin;
  while (end > begin && (end[-1] == '\n' || end[-1] == '\r' || end[-1] == ' ' || end[-1] == '\t'))
    --end;
  return std::string(begin, static_cast<std::size_t>(end - begin));
}

// "   Name: xxx" → 若前缀匹配 key, 返回值部分, 否则空 optional 语义 (empty + found=false)
bool field(const char *line, const char *key, std::string &out) {
  const char *p = line;
  while (*p == ' ' || *p == '\t')
    ++p;
  const std::size_t klen = std::strlen(key);
  if (std::strncmp(p, key, klen) != 0)
    return false;
  p += klen;
  out = trim(p, p + std::strlen(p));
  return true;
}

} // namespace

ArchiveListStatus list_archive(const std::string &archive_path, const std::string &archive_tool,
                               std::vector<ArchiveEntry> &entries) {
  entries.clear();
  if (!std::filesystem::exists(archive_path))
    return ArchiveListStatus::Missing;

  // vt = technical list: Name / Type / Size 各占一行, 对含空格的文件名也安全
  // (普通 `l` 是定宽列, 文件名在末列, 有空格就没法可靠切分).
  const std::string cmd = archive_tool + " vt \"" + archive_path + "\"";
  FILE *pipe = safe_popen(cmd.c_str(), "r");
  assert(pipe && "list_archive: popen 失败");

  char line[8192];
  std::string pending_name;
  std::size_t pending_size = 0;
  bool pending_is_file = false;
  bool pending_has_size = false;

  auto flush = [&]() {
    if (pending_name.empty() || !pending_is_file || !pending_has_size)
      return;
    entries.push_back({pending_name, pending_size, entries.size()});
  };

  std::string value;
  while (fgets(line, sizeof(line), pipe)) {
    if (field(line, "Name:", value)) {
      flush();
      pending_name = value;
      pending_size = 0;
      pending_is_file = false;
      pending_has_size = false;
    } else if (field(line, "Type:", value)) {
      pending_is_file = (value == "File");
    } else if (field(line, "Size:", value)) {
      pending_size = std::strtoull(value.c_str(), nullptr, 10);
      pending_has_size = true;
    }
  }
  flush();

  // 非零退出 = 包损坏 (头链断裂/截断). 半份条目表不可信, 整个丢掉.
  if (safe_pclose(pipe) != 0) {
    entries.clear();
    return ArchiveListStatus::Corrupt;
  }

  return ArchiveListStatus::Ok;
}

bool stream_archive_files(const std::string &archive_path,
                          const std::string &archive_tool,
                          const std::vector<std::string> &paths,
                          const std::vector<std::size_t> &sizes,
                          const FileSink &on_file,
                          const std::atomic<bool> *cancel) {
  assert(paths.size() == sizes.size() && "stream_archive_files: paths/sizes 长度不等");
  if (paths.empty())
    return true;

  // 名单走 unrar 的 @listfile — 整天的文件名单可达数百 KB, 远超 execve
  // 单参数上限 (Linux MAX_ARG_STRLEN 128KB), 拼在命令行上 popen 会直接失败.
  static std::atomic<std::uint64_t> list_seq{0};
  const std::string list_path =
      (std::filesystem::temp_directory_path() /
       ("stk_unrar_" + std::to_string(safe_getpid()) + "_" +
        std::to_string(list_seq.fetch_add(1)) + ".lst"))
          .string();
  {
    std::ofstream list(list_path, std::ios::binary | std::ios::trunc);
    assert(list.is_open() && "stream_archive_files: 名单临时文件打不开");
    for (const auto &p : paths)
      list << p << '\n';
    list.close();
    assert(!list.fail() && "stream_archive_files: 名单临时文件写入失败");
  }

  // -inul 抑制所有提示, stdout 只剩文件内容
  const std::string cmd =
      archive_tool + " p -inul \"" + archive_path + "\" \"@" + list_path + "\"";

  std::size_t max_size = 0;
  for (const std::size_t size : sizes)
    if (size > max_size)
      max_size = size;

  FILE *pipe = safe_popen(cmd.c_str(), "r");
  assert(pipe && "stream_archive_files: popen 失败");

  auto cleanup_list = [&list_path]() {
    std::error_code ec;
    std::filesystem::remove(list_path, ec);
  };

  // 单个缓冲复用: 回调就地把字节解析成结构体, 不需要同时持有多个文件
  std::vector<char> buffer(max_size);

  for (std::size_t i = 0; i < paths.size(); ++i) {
    if (cancel && cancel->load()) {
      safe_pclose(pipe);
      cleanup_list();
      return true;
    }
    std::size_t got = 0;
    while (got < sizes[i]) {
      const std::size_t n = std::fread(buffer.data() + got, 1, sizes[i] - got, pipe);
      if (n == 0)
        break;
      got += n;
    }
    // 短读 = 包在这里断了. 后面所有文件的边界都跟着错位, 不能再往下切,
    // 这一块也不交给回调 (半截 CSV 解出来的是垃圾).
    if (got != sizes[i]) {
      safe_pclose(pipe);
      cleanup_list();
      return false;
    }
    on_file(i, buffer.data(), got);
  }

  // 流应当恰好读完 — 多出字节说明尺寸表与实际不符
  char extra;
  const bool has_extra = std::fread(&extra, 1, 1, pipe) != 0;

  // unrar 非零退出多为成员 CRC 失败: 字节数可能正好, 内容却是坏的
  const bool clean_exit = safe_pclose(pipe) == 0;
  cleanup_list();
  return clean_exit && !has_extra;
}

} // namespace misc
