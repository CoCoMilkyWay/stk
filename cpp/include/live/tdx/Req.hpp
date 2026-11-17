// 通达信适配: 请求层日志 — 常驻钩住 tc.dll 的请求对象 CTcComm::CTcJob (shim 命令 vt; 各槽含义见 TDX.md「请求层」),
//   每问查完 (ui::query 等到 done) 把其间客户端发出的请求整理成 req 事件记进交互日志: 只看日志即知每问走了哪些请求、
//   带了什么入参、多久应答、应答几行几列、插件读了什么.
//   arm(堆)  接上客户端后调 (Broker 接入 / shim 程序): 按 RTTI 找虚表、读协议字典 (同一进程只找一次);
//            钩上 job 与 CTcUser 两张虚表 (已钩照旧), 记哪些槽每次都设一遍 (shim vt 会改)
//   flush()  取出 shim 记下的调用, 各请求一条 req (字段见 Trace.hpp); 没 arm 过即不做.
//            不是适配发起的请求 (人点的、客户端自己定时查的) 一并记, 以 ago (发出距此刻) 区分.
//            插件发的请求另记下 功能号 → 入参 (原样、按序) / 回包 lParam, 与其建 job 所用的 CTcUser (另钩其槽 7), 供 ask 照搬
//   ask()    直发 (shim 命令 req, 只读白名单功能号): 入参照搬插件最近一次发同一功能号时的 (须已见过; 缺一个字段
//            实测应答回不来、客户端随后崩溃), over 只覆盖其中已有的字段 (如资金流水的起止日); 不经界面、不切页.
//            应答各列按协议字典的出参逐个字段号取 (服务器的列序未必同出参序, 如担保品), 故列序恒为出参序.
//            入参带定位字串的功能 (标的证券 / 担保品) 每页要 1000 行 (服务器至多给七百余行), 以上页的定位串接着问到空页, 各页接起
//   seen()   插件在本客户端进程里发过 func (ask 有入参可照搬); 客户端换了进程即都没见过
#pragma once

#include "live/tdx/Heap.hpp"

#include <map>
#include <string>
#include <vector>

namespace live::tdx::req {

struct Answer {
  int cols = 0;                               // = 出参数
  std::vector<std::vector<std::string>> rows; // 各行各列 (UTF-8), 列序即出参序 (应答没有的字段为空串)
  std::string pos;                            // 定位串 (末行的市场 + 代码)
};

void arm(const mem::HeapP &h);
void flush();
bool seen(int func);
Answer ask(int func, std::map<int, std::string> over = {});

} // namespace live::tdx::req
