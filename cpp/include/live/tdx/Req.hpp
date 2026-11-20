// 通达信适配: 请求层直发 — 常驻钩住 tc.dll 的请求对象 CTcComm::CTcJob 与 CTcUser (shim 命令 vt; 各槽含义见 TDX.md「请求层」),
//   记下插件发的请求, 供直发照搬其入参 (不经界面、不切页)
//   arm(pid)  接上客户端后调 (Driver 接入 / shim 程序): 进程变了即经 shim (vtable / mods) 按 RTTI 进程内找两张主虚表、读协议字典
//             (同一进程只找一次); 钩上两张虚表 (已钩照旧), 只录 5 个入口槽: CTcUser 槽 7 (建 job: 插件调它的 this 即其所用的 CTcUser) +
//             CTcJob 槽 6 / 7 / 9 / 25 (设功能号 / 写串入参 / 写整数入参 / 发送); 不改写返回、不等插件读应答
//   flush()   每问查完 (ui::query 等到 done) 调: 取出 shim 录下的调用, 插件 (Addin*) 发的请求记下 功能号 → 入参 (原样、按序;
//             tc.dll 建 job 时自填的不算, 直发建 job 时它照样填) / 回包 lParam, 与其建 job 所用的 CTcUser; 没 arm 过即不做
//   seen(f)   插件在本客户端进程里发过 f (ask 有入参可照搬); 客户端换了进程即都没见过
//   ask(f,o)  直发 (shim 命令 req, 只读白名单功能号): 入参照搬插件最近一次发同一功能号时的 (须已见过; 缺一个字段
//             实测应答回不来、客户端随后崩溃), o 只覆盖其中已有的字段 (如资金流水的起止日).
//             应答各列按协议字典的出参逐个字段号取 (服务器的列序未必同出参序, 如担保品), 故列序恒为出参序.
//             入参带定位字串 (0x506) 的功能 (标的证券 / 担保品) 每页要 1000 行 (服务器至多给七百余行), 以上页的定位串接着问到空页, 各页接起
#pragma once

#include <map>
#include <string>
#include <vector>

namespace live::tdx::req {

struct Answer {
  int cols = 0;                               // = 出参数
  std::vector<std::vector<std::string>> rows; // 各行各列 (UTF-8), 列序即出参序 (应答没有的字段为空串)
  std::string pos;                            // 定位串 (末行的市场 + 代码)
};

void arm(int pid);
void flush();
bool seen(int func);
Answer ask(int func, std::map<int, std::string> over = {});

} // namespace live::tdx::req
