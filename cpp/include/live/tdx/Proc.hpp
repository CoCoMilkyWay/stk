// 通达信适配: 起外部进程 (交叉编译 / wine 注入 / sudo) 并收其输出
#pragma once

#include <filesystem>
#include <string>
#include <vector>

namespace live::tdx {

struct Ran {
  int rc = -1;
  std::string out, err;
};
// argv[0] 按 PATH 找; in 写进标准输入; cwd 空 = 当前目录
Ran run(const std::vector<std::string> &argv, const std::string &in = "", const std::filesystem::path &cwd = {});
bool on_path(const std::string &exe);

// py/app/tdx 下与浏览器控制台共用的件: shim 源码与构建产物、本机 sudo 密码 (已 gitignore)
std::filesystem::path shim_dir();
std::filesystem::path sudo_pass_file();

} // namespace live::tdx
