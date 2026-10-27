#!/usr/bin/env python3
# bridge.shimctl — 通用 Win32 消息桥控制器（Linux 侧），纯库，无 CLI。
# 负责编译/注入 shim 到 Wine 目标进程，并通过 TCP 控制通道发命令：
#   spy 抓任意窗口消息、post/send 回放任意消息、enum/find 按 class/title 解析窗口、tree 等。
# 业务无关：不假设"持仓"或任何具体动作。动作由调用方按 (target, msg, wparam, lparam) 给出。
#
# 环境无关：WINEPREFIX 取环境变量或默认 ~/.wine；路径一律 __file__ 相对；编译器自动探测
# (clang+mingw 运行时优先，gcc-mingw 次之)；注入用 C:\windows\temp 路径，不依赖 Z: 盘映射。
#
# 协议（见 shim.c 头注释）：一行命令 → 0..N 行 JSON 记录 + 末行 ok / err。
# 构建产物按源码哈希 H 命名：shim_<H>.dll，控制端口也由 H 推出，无需额外状态文件。
#
# 库用法（统一入口 console.py 启动时会自动 ensure）：
#   from bridge.shimctl import Shim, ensure
#   ensure()                                  # 编译+注入(若未在线)
#   s = Shim()
#   s("spy", "on"); ...; s("spy", "dump")     # 抓消息 → [{msg,hwnd,class,wparam,lparam}]
#   s("post", "0x503ca", 0x111, 0x2721, 0)    # 回放 → [{ret}]
#   s("tree"); s("enum"); s("find", "class=TdxW_Main")   # → [节点记录]

import os
import sys
import json
import subprocess
import socket
import hashlib
import shutil

HERE = os.path.dirname(os.path.abspath(__file__))
SHIM_DIR = os.path.join(HERE, "shim")
INJECT_EXE = os.path.join(SHIM_DIR, "inject.exe")
WINEPREFIX = os.environ.get("WINEPREFIX") or os.path.expanduser("~/.wine")
WINE_TEMP = os.path.join(WINEPREFIX, "drive_c", "windows", "temp")
PORT_FILE = os.path.join(WINE_TEMP, "tdx_shim_port.txt")
TARGET_PROC = os.environ.get("SHIM_TARGET_PROC", "tdxw.exe")

GCC = "i686-w64-mingw32-gcc"
MINGW_SYSROOT = "/usr/i686-w64-mingw32"


def _src_hash():
    h = hashlib.sha1()
    for n in ("shim.c", "inject.c"):
        with open(os.path.join(SHIM_DIR, n), "rb") as f:
            h.update(f.read())
    return h.hexdigest()[:8]


H = _src_hash()
DLL_NAME = f"shim_{H}.dll"
DLL_PATH = os.path.join(SHIM_DIR, DLL_NAME)
PORT = 20000 + int(H, 16) % 10000


# ---------- 编译 ----------


def _have(cmd):
    return shutil.which(cmd) is not None


def _runtime_here():
    return os.path.isdir(os.path.join(MINGW_SYSROOT, "include"))


def clang_usable():
    return _have("clang") and _runtime_here()


def gcc_usable():
    return _have(GCC)


def _apt_install(pkgs):
    print(f"[*] sudo apt 安装 {' '.join(pkgs)}（会提示输密码；只此一步需要 root）")
    subprocess.run(["sudo", "apt-get", "install", "-y", *pkgs], check=True)


def _libgcc_dir():
    """clang+lld 不会自动搜 gcc 的 libgcc 目录；探测之（优先 win32 线程模型，
    避免拖 libwinpthread 运行时依赖）。返回含 libgcc.a 的目录。"""
    import glob

    cands = glob.glob("/usr/lib/gcc/i686-w64-mingw32/*/libgcc.a")
    assert cands, "未找到 libgcc.a（装 gcc-mingw-w64-i686）"
    cands.sort(key=lambda p: ("win32" not in p, p))  # win32 排前
    return os.path.dirname(cands[0])


def _clang_base():
    return [
        "clang",
        "--target=i686-w64-windows-gnu",
        f"--sysroot={MINGW_SYSROOT}",
        "-fuse-ld=lld",
        f"-L{_libgcc_dir()}",
    ]


def _toolchain():
    """编译命令前缀：clang+mingw 运行时优先，gcc-mingw 次之；都没有就 apt 装。"""
    if not (clang_usable() or gcc_usable()):
        _apt_install(
            ["mingw-w64-i686-dev", "binutils-mingw-w64-i686"]
            if _have("clang")
            else ["gcc-mingw-w64-i686"]
        )
    if clang_usable():
        return _clang_base()
    assert gcc_usable(), "安装后仍无可用交叉编译器"
    return [GCC]


def _prune(d):
    """删掉 d 下非当前哈希的 shim_*.dll（已注入进程映射的旧 DLL 被 unlink 不受影响）。"""
    import glob

    for p in glob.glob(os.path.join(d, "shim_*.dll")):
        if os.path.basename(p) != DLL_NAME:
            os.remove(p)


def build():
    """构建 shim_<H>.dll + inject.exe；同哈希已构建则跳过。"""
    _prune(SHIM_DIR)
    if os.path.exists(DLL_PATH):
        print(f"[*] {DLL_NAME} 已构建，跳过编译（端口 {PORT}）")
        return
    base = _toolchain()
    print(f"[*] 编译 {DLL_NAME} / inject.exe（端口 {PORT}）")
    for args in (
        [
            "-shared",
            "-o",
            DLL_PATH,
            "shim.c",
            "-lws2_32",
            "-luser32",
            "-lkernel32",
            "-lgdi32",
        ],
        ["-o", INJECT_EXE, "inject.c", "-lkernel32"],
    ):
        subprocess.run(base + args, cwd=SHIM_DIR, check=True)


# ---------- 注入 ----------


def inject():
    """DLL 拷进 wine 的 C:\\windows\\temp（不依赖 Z: 盘）、写端口文件，再远程 LoadLibrary。"""
    os.makedirs(WINE_TEMP, exist_ok=True)
    _prune(WINE_TEMP)
    shutil.copyfile(DLL_PATH, os.path.join(WINE_TEMP, DLL_NAME))
    with open(PORT_FILE, "w") as f:
        f.write(str(PORT))
    dll_win = f"C:\\windows\\temp\\{DLL_NAME}"
    print(f"[*] 注入 {dll_win}（pid 由 inject.exe 自找 {TARGET_PROC}）")
    r = subprocess.run(
        ["wine", INJECT_EXE, dll_win, TARGET_PROC], capture_output=True, text=True
    )
    sys.stdout.write(r.stdout)
    assert r.returncode == 0, f"注入失败 rc={r.returncode}\n{r.stderr}"


# ---------- Shim 控制通道 ----------


class Shim:
    """s(cmd, *args) → JSON 记录列表；应答末行非 ok 即 assert。"""

    def __init__(self, port=PORT):
        self.port = port

    def online(self):
        with socket.socket() as s:
            return s.connect_ex(("127.0.0.1", self.port)) == 0

    def _open(self, cmd, args):
        line = " ".join(map(str, (cmd, *args)))
        assert "\n" not in line, f"命令不能含换行: {line!r}"
        s = socket.create_connection(("127.0.0.1", self.port), timeout=5)
        s.sendall(line.encode() + b"\n")
        return s

    def __call__(self, cmd, *args):
        with self._open(cmd, args) as s:
            *recs, tail = s.makefile("rb").read().splitlines()
        assert tail == b"ok", f"{cmd}: {tail.decode(errors='replace')}"
        return [json.loads(r) for r in recs]

    def lines(self, cmd, *args):
        """长连接命令（treewatch / gdiwatch）：逐行 yield 原始 JSON 文本，{} 为心跳。"""
        with self._open(cmd, args) as s:
            for line in s.makefile("rb"):
                assert not line.startswith(
                    b"err"
                ), f"{cmd}: {line.decode(errors='replace')}"
                yield line.decode().rstrip("\n")


def ensure():
    """编译+注入直到 shim 在线。已在线则跳过。"""
    build()
    s = Shim()
    if not s.online():
        inject()
        assert s.online(), f"shim 注入后无应答（端口 {s.port}）"
    print(f"[+] shim 在线: pid={s('ping')[0]['pid']}")
    return s
