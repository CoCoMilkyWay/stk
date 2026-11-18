// shim — 通达信客户端的独立调试程序 (与注入件 shim.dll 同名: 主程序经它驱动客户端, 这里直接拿适配代码对着活客户端用, 不起 GUI)
//   在仓库内任一目录运行 (定位仓库根与 output/); 客户端须开着, shim.dll 不在线即编译 + 注入 (同 Driver 接客户端)
//     shim cmd <命令…>        原样发一条 shim 命令 (协议与命令表见 win32/shim.c 文件头), 应答逐行 JSON 打到 stdout
//     shim probe tables       进程堆里全部明文表 (首行 0|…|): 址 / 副本数 / 行数 / 列数 / 表头 / 前几行
//     shim probe text <词…>   各词按 GBK、UTF-16LE 逐处列出址与前后字节
//     shim probe obj <址> [字数] [槽数] / ref <值> [回看字节] / class <类名>   对象逐字标去处 / 谁存着这个值 / 类的虚表
//     shim probe schema [功能号…]   协议字典: 各功能 / 某功能的入参与出参
//     shim rec <秒>           录制: 消息 / 窗口树 / 绘制层的变化 → output/run/live/tdx/rec/<时刻>.jsonl, 各条一行打到 stdout
//     shim vt <秒> <类名 | 0x虚表>…   虚表追踪: 经这些虚表的每次调用 (参数 / 所指文本 / 返回) → 同上目录, 各一行打到 stdout
//     shim test [用例…]       在线用例 (空 = 全部; list = 列出); 每例一行 PASS / FAIL, 有 FAIL 退出码 1; 各问的请求层日志 (req) 照记
//   界面动作只做查询类 (切页 / 开查询页 / 点查询), 不下单; 交互日志照常落 output/run/live/trace/
#include "live/tdx/shim/Tool.hpp"

#include "live/Broker.hpp"
#include "live/tdx/Heap.hpp"
#include "live/tdx/Req.hpp"
#include "live/tdx/Shim.hpp"
#include "live/tdx/Win.hpp"

#include <cstdio>
#include <string>
#include <vector>

namespace {

using namespace live::tdx;

constexpr const char *kUsage =
    "用法: shim cmd <命令…> | probe tables | probe text <词…> | probe obj <址> [字数] [槽数] | probe ref <值> [回看字节] | probe class <类名> | probe schema [功能号…] | rec <秒> | "
    "vt <秒> <类名 | 0x虚表>… | test [用例… | list]\n";

int cmd(const std::vector<std::string> &a) {
  std::string line;
  for (const std::string &s : a)
    line += (line.empty() ? "" : " ") + s;
  for (const nlohmann::json &j : shim::call(line, 30))
    std::printf("%s\n", j.dump().c_str());
  return 0;
}

int run(const std::string &sub, const std::vector<std::string> &rest) {
  if (sub == "probe")
    return tool::probe(rest);
  if (!shim::online())
    win::attach();
  if (sub == "cmd" && !rest.empty())
    return cmd(rest);
  if (sub == "rec" && rest.size() == 1)
    return tool::rec(std::stod(rest[0]));
  if (sub == "vt" && rest.size() >= 2)
    return tool::vt(std::stod(rest[0]), {rest.begin() + 1, rest.end()});
  if (sub == "test") {
    req::arm(mem::attach()); // 用例各段里另记请求层日志 (同 Driver)
    return tool::test(rest);
  }
  std::fputs(kUsage, stderr);
  return 2;
}

} // namespace

int main(int argc, char **argv) {
  const std::vector<std::string> args(argv + 1, argv + argc);
  if (args.empty()) {
    std::fputs(kUsage, stderr);
    return 2;
  }
  try {
    return run(args[0], {args.begin() + 1, args.end()});
  } catch (const live::Fail &f) {
    std::fprintf(stderr, "✗ %s\n", f.why.c_str());
    return 1;
  }
}
