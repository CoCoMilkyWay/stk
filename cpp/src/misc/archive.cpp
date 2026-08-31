#include "misc/archive.hpp"

#include "misc/cross_platform.hpp"

#include <cassert>
#include <cstdio>
#include <cstring>
#include <filesystem>

namespace misc {

std::unordered_set<std::string> list_archive_assets(const std::string &archive_path,
                                                    const std::string &archive_tool) {
  std::unordered_set<std::string> assets;
  if (!std::filesystem::exists(archive_path))
    return assets;

  const std::string cmd = archive_tool + " lb \"" + archive_path + "\"";
  FILE *pipe = safe_popen(cmd.c_str(), "r");
  assert(pipe && "list_archive_assets: popen 失败");

  char line[4096];
  while (fgets(line, sizeof(line), pipe)) {
    // "20260803/000001.SZ/行情.csv" → "000001.SZ"
    const char *first = std::strchr(line, '/');
    if (!first)
      continue;
    const char *second = std::strchr(first + 1, '/');
    if (!second)
      continue;
    assets.emplace(first + 1, static_cast<std::size_t>(second - first - 1));
  }

  const int exit_code = safe_pclose(pipe);
  static_cast<void>(exit_code); // NDEBUG 构建下 assert 消失, 但 pclose 必须留
  assert(exit_code == 0 && "list_archive_assets: 归档损坏或 unrar 报错");

  return assets;
}

} // namespace misc
