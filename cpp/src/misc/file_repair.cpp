#include "misc/file_repair.hpp"
#include "misc/cross_platform.hpp"

#include <cassert>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <vector>

namespace FileRepair {

namespace fs = std::filesystem;

namespace {

std::string quote(const fs::path &p) {
  const std::string s = p.string();
  assert(s.find('\'') == std::string::npos && "路径含单引号");
  return "'" + s + "'";
}

void sh(const std::string &cmd) {
  const int code = std::system(cmd.c_str());
  assert(code == 0 && "归档工具失败");
  (void)code;
}

void unpack(const fs::path &src, const fs::path &in) {
  fs::create_directories(in);
  switch (FileCheck::format_of(src.string())) {
  case FileCheck::Format::Rar:
    sh("unrar x -inul -y " + quote(src) + " " + quote(in / ""));
    return;
  case FileCheck::Format::SevenZip:
  case FileCheck::Format::Zip:
    sh("7z x -y -bso0 -bsp0 " + quote("-o" + in.string()) + " " + quote(src));
    return;
  case FileCheck::Format::Unknown:
    break;
  }
  assert(false && "不可修的格式");
}

fs::path root_at(const fs::path &work, const std::string &date) {
  const fs::path in = work / "in";
  const std::vector<fs::path> top(fs::directory_iterator(in), {});
  if (top.size() == 1 && fs::is_directory(top[0]) && top[0].filename() == date)
    return in;
  const fs::path out = work / "out";
  fs::create_directories(out);
  fs::rename(in, out / date);
  return out;
}

void pack(const fs::path &root, const fs::path &tmp) {
  fs::remove(tmp);
  sh("cd " + quote(root) + " && rar a -m3 -ma5 -r -idq " + quote(tmp) + " .");
  const int fd = file_open_read(tmp.c_str());
  assert(fd >= 0 && "打不开新包");
  file_sync(fd);
  file_close(fd);
}

void repack(const fs::path &src, const fs::path &stage) {
  const std::string date = src.stem().string();
  const fs::path dst = src.parent_path() / (date + ".rar");
  const fs::path tmp = src.parent_path() / (".tmp_" + date + ".rar");
  const fs::path work = stage / date;
  fs::remove_all(work);

  unpack(src, work / "in");
  pack(root_at(work, date), tmp);
  fs::rename(tmp, dst);
  if (src != dst)
    fs::remove(src);
  fs::remove_all(work);
}

} // namespace

void repair(const std::vector<FileCheck::Finding> &findings, const std::filesystem::path &stage,
            const FileCheck::ProgressCallback &progress) {
  std::vector<fs::path> todo;
  for (const FileCheck::Finding &f : findings)
    if (FileCheck::repairable(f.issue))
      todo.push_back(f.path);

  for (size_t i = 0; i < todo.size(); ++i) {
    repack(todo[i], stage);
    if (progress)
      progress(i + 1, todo.size(), todo[i].string());
  }
}

} // namespace FileRepair
