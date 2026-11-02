#!/usr/bin/env python3
# mem/common — 各内存方案的共享底座（非通用基建，仅服务 mem/ 下的方案）。
# 职责：定位 TdxW 进程、枚举堆段、确保有跨进程读权限（本地存密码自动 sudo）、
#       "连读两次一致才采信"的完整性原语、明文表的切块与解析，以及各方案共用的全量读与换代等待。
# 具体某张表怎么选块、怎么校验、怎么按代码汇总，由各方案文件（mem/positions.py、mem/fills.py）自管。
#
# 方案协议：每个方案模块出同一组接口，快照 Snap 为不可变值，== 即「同一代」。
#   find(lo, data) → [命中]   段内找块（纯形状，不读别处）
#   pick(pid, hits) → Snap    全部命中 → 快照（选块、解析、校验、汇总）
#   follow(snap)   → Snap     以最省的办法看是否换代，没换代回 snap 本身
# read(方案们, pid) 一遍扫全部堆段出各方案快照；wait(方案, snap) 轮询 follow 等客户端重查后的换代。

import os
import re
import subprocess
import time
from collections import namedtuple

HERE = os.path.dirname(os.path.abspath(__file__))
PASS_FILE = os.path.join(HERE, os.pardir, ".sudo_pass")  # 本地 sudo 密码，已 gitignore
PTRACE_SCOPE = "/proc/sys/kernel/yama/ptrace_scope"
MAX_REGION = 400 * 1024 * 1024  # 跳过超大段（映射文件等），只扫堆
BUY, SELL = +1, -1  # 方向即符号，各方案与上层同用
SIDES = (BUY, SELL)

# 快照：mark = 换代凭据（假即无可盯），rows = 行记录，by_code = {代码: 记录}（由 rows 派生）。
Snap = namedtuple("Snap", "pid mark rows by_code")


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
    """逐段读 pid 堆段，yield (段起址, bytes)；near（地址集）给定时只读含其中任一地址的段。调用前应先 attach()。
    个别段运行时不可读（守护页等）属正常，跳过即可。"""
    with open(f"/proc/{pid}/mem", "rb", buffering=0) as mem:
        for lo, hi, _ in read_maps(pid):
            if near is not None and not any(lo <= a < hi for a in near):
                continue
            try:
                mem.seek(lo)
                yield lo, mem.read(hi - lo)
            except OSError:
                continue


def read_at(pid, addr, n):
    """读 pid 内存 [addr, addr+n)，须整段可读。调用前应先 attach()。"""
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


def scan(pid, finds, near=None):
    """一遍读堆段（near 见 iter_regions），每段交给各 find → 与 finds 同序的命中列表。"""
    hits = [[] for _ in finds]
    for lo, data in iter_regions(pid, near):
        for h, find in zip(hits, finds):
            h += find(lo, data)
    return hits


def read(schemes, pid=None):
    """全量：定位进程，一遍扫全部堆段，各方案 pick 出快照 → 与 schemes 同序。"""
    pid = attach(pid)
    hits = scan(pid, [s.find for s in schemes])
    return tuple(s.pick(pid, h) for s, h in zip(schemes, hits))


def wait(scheme, snap, timeout=3.0, gap=0.01):
    """客户端重查后，轮询 scheme.follow 等 snap 换代即回新快照，不等固定时长。
    timeout 内没换代（应答与旧的逐字节相同，如收盘后；或 follow 盯不到新块）即全量读。"""
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        new = scheme.follow(snap)
        if new != snap:
            return new
        time.sleep(gap)
    return read((scheme,), snap.pid)[0]


# ---------- 明文表：\r\n 分行、| 分列、GBK，首行表头 ----------


def blocks(lo, data, anchor, back, block_re):
    """段（起址 lo）里每处定长锚点，往前 back 字节内取首个能 match 出整块的 0| 起点 → [(块地址, match)]。"""
    out = []
    i = data.find(anchor)
    while i != -1:
        for hs in range(max(0, i - back), i):
            m = data.startswith(b"0|", hs) and block_re.match(data, hs)
            if m:
                out.append((lo + hs, m))
                break
        i = data.find(anchor, i + 1)
    return out


def _take(f, conv, at):
    """取一列转型；at 为 None 即该布局无此列（取 None），为 {方向: 列号} 即取成 {方向: 值}。"""
    if at is None:
        return None
    if isinstance(at, dict):
        return {s: conv(f[i]) for s, i in at.items()}
    return conv(f[at])


def rows(raw, views, cols, check):
    """块 → [记录]：解码一次、无替换字符，首行表头其后每行一条；列数全表一致，即 views 里第几个布局，
    按 cols {字段: (转型, 各布局列号…)} 取列，再逐条 check（不过即 assert，原样返回）。"""
    text = raw.decode("gbk", errors="replace")
    assert "\ufffd" not in text, "明文含 GBK 替换字符，疑似读到写入中途的坏块"
    lines = [ln.split("|") for ln in text.split("\r\n")[1:-1]]
    nfs = {len(f) for f in lines}
    assert len(nfs) == 1, f"行列数不一致 {nfs}，块可能被截断/串行"
    nf = nfs.pop()
    assert nf in views, f"{nf} 列，不认识的布局（认识 {views}）"
    at = 1 + views.index(nf)
    return [check({k: _take(f, c[0], c[at]) for k, c in cols.items()}) for f in lines]


def check_id(r):
    """各表行首共有的标识：代码 6 位、沪深标志 0 / 1。"""
    assert re.fullmatch(r"\d{6}", r["code"]), f"非法代码 {r['code']!r}"
    assert r["market_flag"] in (0, 1), f"{r['code']} 沪深标志 {r['market_flag']}"
