#!/usr/bin/env python3
# mem/common — 各内存方案的共享底座（非通用基建，仅服务 mem/ 下的方案）。
# 职责：定位 TdxW 进程、枚举堆段、确保有跨进程读权限（本地存密码自动 sudo）、
#       "连读两次一致才采信"的完整性原语，以及各方案共用的换代等待。
# 具体某块数据怎么找、怎么解、怎么校验，由各方案文件（如 mem/positions.py）自管。
#
# 方案协议：每个方案模块出一对同形接口，快照为不可变值，== 即「同一代」，均带 pid。
#   read(pid=None) → 快照    全量读（pid 缺省即找进程）
#   follow(snap)   → 快照    以最省的办法看是否换代，没换代回 snap 本身
# wait(方案, snap) 轮询 follow 等客户端重查后的换代，两方案共用。

import os
import subprocess
import time

HERE = os.path.dirname(os.path.abspath(__file__))
PASS_FILE = os.path.join(HERE, os.pardir, ".sudo_pass")  # 本地 sudo 密码，已 gitignore
PTRACE_SCOPE = "/proc/sys/kernel/yama/ptrace_scope"
MAX_REGION = 400 * 1024 * 1024  # 跳过超大段（映射文件等），只扫堆
BUY, SELL = +1, -1  # 方向即符号，各方案与上层同用
SIDES = (BUY, SELL)


def find_tdxw_pid():
    out = subprocess.check_output(["ps", "-eo", "pid,comm,args"], text=True)
    for line in out.splitlines():
        if "tdxw.exe" in line.lower() and "grep" not in line:
            return line.split()[0]
    return None


def _ptrace_scope():
    if not os.path.exists(PTRACE_SCOPE):
        return 0
    return int(open(PTRACE_SCOPE).read().strip())


def ensure_access():
    """确保能跨进程读 /proc/<pid>/mem。ptrace_scope!=0 时用本地存的密码 sudo 关一次
    （同用户进程即可互读），到重启前有效，之后直接读、不再交互询问。
    密码放 scripts/.sudo_pass（已 gitignore）。"""
    if _ptrace_scope() == 0:
        return
    assert os.path.exists(PASS_FILE), f"需 sudo 放开 ptrace，但缺密码文件 {PASS_FILE}"
    pw = open(PASS_FILE).read().strip()
    subprocess.run(
        ["sudo", "-S", "sysctl", "-w", "kernel.yama.ptrace_scope=0"],
        input=pw + "\n",
        text=True,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    assert _ptrace_scope() == 0, "sudo 设置 ptrace_scope=0 失败（密码错？）"


def attach(pid=None):
    """定位进程（给了 pid 即用）并确保有读权限 → pid。"""
    pid = pid or find_tdxw_pid()
    assert pid, "找不到 tdxw.exe 进程"
    ensure_access()
    return pid


def read_maps(pid):
    """返回 pid 的堆段 [(lo, hi, path)]：可读写、私有、非文件映射（Wine 堆即匿名 rwxp），排除超大段。
    运行时生成的数据只落在这类段里；映像/只读/共享段跳过，扫描量约减半。"""
    maps = []
    with open(f"/proc/{pid}/maps") as f:
        for line in f:
            p = line.split()
            lo, hi = (int(x, 16) for x in p[0].split("-"))
            path = p[5] if len(p) > 5 else ""
            if p[1][:2] != "rw" or p[1][3] != "p" or path.startswith("/"):
                continue
            if hi - lo > MAX_REGION:
                continue
            maps.append((lo, hi, path))
    return maps


def iter_regions(pid, near=None):
    """逐段读 pid 堆段，yield (段起址, bytes)；near 给定时只读含该地址的那一段。调用前应先 ensure_access()。
    个别段运行时不可读（守护页等）属正常，跳过即可。"""
    with open(f"/proc/{pid}/mem", "rb", buffering=0) as mem:
        for lo, hi, _ in read_maps(pid):
            if near is not None and not lo <= near < hi:
                continue
            try:
                mem.seek(lo)
                yield lo, mem.read(hi - lo)
            except OSError:
                continue


def read_at(pid, addr, n):
    """读 pid 内存 [addr, addr+n)，须整段可读。调用前应先 ensure_access()。"""
    with open(f"/proc/{pid}/mem", "rb", buffering=0) as mem:
        mem.seek(addr)
        data = mem.read(n)
    assert len(data) == n, f"{addr:#x} 只读到 {len(data)}/{n} 字节"
    return data


def stable(fn, tries=5, gap=0.01):
    """完整性原语：反复调用 fn()，直到相邻两次结果完全一致才采信，
    躲开客户端正改写堆（torn read）的瞬间。fn 返回 None 视为未就绪。
    tries 次内仍未稳定即 assert（宁可不给数也不给错数）。"""
    prev = fn()
    for _ in range(tries - 1):
        time.sleep(gap)
        cur = fn()
        if cur is not None and cur == prev:
            return cur
        prev = cur
    assert False, f"连读 {tries} 次仍不一致，内存正被改写"


def wait(scheme, snap, timeout=3.0, gap=0.01):
    """客户端重查后，轮询 scheme.follow 等 snap 换代即回新快照，不等固定时长。
    timeout 内没换代（应答与旧的逐字节相同，如收盘后；或 follow 盯不到新块）即全量 scheme.read。"""
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        new = scheme.follow(snap)
        if new != snap:
            return new
        time.sleep(gap)
    return scheme.read(snap.pid)
