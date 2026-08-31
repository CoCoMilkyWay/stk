#include "misc/fs.hpp"

#include <cassert>
#include <fstream>
#include <sstream>

namespace misc {

namespace fs = std::filesystem;

namespace {

fs::path find_git_root() {
  fs::path p = fs::current_path();
  for (;;) {
    if (fs::exists(p / ".git"))
      return p;
    if (p == p.parent_path())
      break;
    p = p.parent_path();
  }
  assert(false && "git root not found (need .git)");
  return {};
}

} // namespace

fs::path git_root() {
  static fs::path root = find_git_root();
  return root;
}

std::string read_file_all(const fs::path &path) {
  std::ifstream f(path, std::ios::binary);
  std::stringstream ss;
  ss << f.rdbuf();
  return ss.str();
}

void atomic_write(const fs::path &path, const char *data, std::size_t len) {
  fs::create_directories(path.parent_path());
  fs::path tmp = path;
  tmp += ".tmp";
  {
    std::ofstream f(tmp, std::ios::binary | std::ios::trunc);
    f.write(data, static_cast<std::streamsize>(len));
  }
  fs::rename(tmp, path);
}

void atomic_write_json(const fs::path &path, yyjson_mut_doc *doc) {
  assert(doc);
  size_t len = 0;
  char *json = yyjson_mut_write(
      doc, YYJSON_WRITE_PRETTY_TWO_SPACES | YYJSON_WRITE_ALLOW_INF_AND_NAN,
      &len);
  assert(json);
  atomic_write(path, json, len);
  std::free(json);
}

} // namespace misc
