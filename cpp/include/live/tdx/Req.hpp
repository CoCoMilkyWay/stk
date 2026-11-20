// 通达信适配: 请求层直发 — 进程内定位 CTcJob / CTcUser 主虚表 + 读协议字典, 只钩 5 个必要入口槽
//   (CTcUser::slot7 捕获插件 CTcUser*; CTcJob::6/7/9/25 捕获功能号、入参、回包 lParam; 零返回改写、零等待)
//   arm(pid)  进程变了即经 shim vtable / mods / memhas 定位虚表与协议字典, 钩上 5 槽
//   flush()   取回 vt drain 并更新 func → {ins, lp} 与 CTcUser*
//   seen(f)   本进程内插件已发过功能 f
//   ask(f,o)  直发查询 (入参照搬插件模板, o 覆盖已有字段; 带定位串 0x506 的功能自动翻页到底)
#pragma once

#include <map>
#include <string>
#include <vector>

namespace live::tdx::req {

struct Answer {
  int cols = 0;
  std::vector<std::vector<std::string>> rows; // 列序即协议字典出参序
  std::string pos;                            // 定位串
};

void arm(int pid);
void flush();
bool seen(int func);
Answer ask(int func, std::map<int, std::string> over = {});

} // namespace live::tdx::req
