# bridge — 通用 Win32 消息桥（Linux 侧）。
# shimctl 负责把 shim.dll 交叉编译并注入 Wine 里的 TdxW.exe，再经 TCP 控制通道发命令。
# 作为库被 console.py 复用；shim/ 是被注入的 DLL 的 C 源码。
