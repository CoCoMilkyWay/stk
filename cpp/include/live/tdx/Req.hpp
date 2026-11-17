// 通达信适配: 请求层日志 — 常驻钩住 tc.dll 的请求对象 CTcComm::CTcJob (shim 命令 vt; 各槽含义见 TDX.md「请求层」),
//   每问查完 (ui::query / press 等到 done) 把其间客户端发出的请求整理成 req 事件记进交互日志: 只看日志即知每问走了哪些请求、
//   带了什么入参、多久应答、应答几行几列、插件读了什么.
//   arm(堆)  接上客户端后调 (Broker 接入 / shim 程序): 按 RTTI 找虚表、读协议字典 (同一进程只找一次);
//            shim 里这张表没钩着 (新注入的 shim / 被 shim vt 卸了) 即钩上; 记哪些槽每次都设一遍
//   flush()  取出 shim 记下的调用, 各请求一条 req (字段见 Trace.hpp); 没 arm 过即不做.
//            不是适配发起的请求 (人点的、客户端自己定时查的) 一并记, 以 ago (发出距此刻) 区分
#pragma once

#include "live/tdx/Mem.hpp"

namespace live::tdx::req {

void arm(const mem::Heap &h);
void flush();

} // namespace live::tdx::req
