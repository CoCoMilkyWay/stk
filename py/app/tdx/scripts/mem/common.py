#!/usr/bin/env python3
# mem/common — 读内存的底座与流程（非通用基建，仅服务 mem/tables 里的表）。
# 职责：定位 TdxW 进程、枚举堆段、确保有跨进程读权限（本地存密码自动 sudo）、
#       "连读两次一致才采信"的完整性原语、明文表的找块 / 复读 / 解析，以及各表共用的流程。
#
# 一张表 Table 由四条互不相干的轴拼成，流程只此一份、只认这四条轴：
#   形状  anchor 定长锚点  back 锚点往前找 0| 起点的字节数  block 整块（组 1 为表头标签 tag；
#         有命名组 n 即表头带行数，块只取前 n 行，没有即整块）  empty 空表（n = 0、无行）算不算块
#         slack 复读窗口在块尾外多留的字节
#   列    views 各布局列数  cols {字段: (转型, 各布局列号…)}  check(行) 行级校验
#   读法  mode = Mode(choose(表, 命中) → 要的那些, merge(表, 各块的行) → 行,
#                     same(snap) 以最省的办法看是否没换代, rescan(snap) 换代后重扫 → 命中)
#         live 活动块 / union 副本并集 / ASK 查询前后比对 / WHOLE 全表；后两种没有 same / rescan
#   索引  key(表, 行) → by_code：unique 键不重 / tally 按代码合计 / pairs 名称 → 数值
#   find(表, 段)          段内命中 Hit（纯形状，不读别处）
#   pick(表, pid, 命中)   选取 → 按址连读一致 → 解析校验 → 合并 → 索引 → Snap
#   follow(snap)          没换代回 snap 本身，换代即 rescan 再 pick
# read(表们, pid) 一遍扫全部堆段出各表快照；wait(snap) 轮询 follow 等客户端重查后的换代（没有块可盯即等一会儿全量读）。
# 读法 ASK 的表（表内没有新旧标记）经 ask(表, act) 读：act（点查询）前后各扫一遍，之后新冒出的命中即这次的应答。

import os
import re
import subprocess
import time
from collections import namedtuple

HERE = os.path.dirname(os.path.abspath(__file__))
PASS_FILE = os.path.join(HERE, os.pardir, ".sudo_pass")  # 本地 sudo 密码，已 gitignore
PTRACE_SCOPE = "/proc/sys/kernel/yama/ptrace_scope"
MAX_REGION = 400 * 1024 * 1024  # 跳过超大段（映射文件等），只扫堆
NUM_RE = re.compile(r"-?\d+(\.\d+)?")

# 编码，各表与上层同用：方向即符号；沪深标志 → 代码前缀（新浪同用）；证券 = 标识三字段
BUY, SELL = +1, -1
SIDES = (BUY, SELL)
SZ, SH, BJ = 0, 1, 60
MARKETS = {SZ: "sz", SH: "sh", BJ: "bj"}
SEC = ("code", "name", "market_flag")

# 命中：块地址、表头标签、块字节、复读窗口尾（不越段尾）；值相等即同址同内容。
Hit = namedtuple("Hit", "addr tag body end")
# 快照：不可变值，== 即「同一代」。mark = 选中的各块（Hit，body 为连读一致的那份），
# rows = 行记录，by_code = 索引（由 rows 派生）。
Snap = namedtuple("Snap", "table pid mark rows by_code")
Table = namedtuple(
    "Table", "name anchor back block empty slack views cols check mode key"
)
Mode = namedtuple("Mode", "choose merge same rescan")


def market(code):
    """股票 / 基金代码的沪深标志：北 4 / 8 / 92 开头，沪 5 / 6 / 9，其余深。"""
    return BJ if code.startswith(("4", "8", "92")) else SH if code[0] in "569" else SZ


def sec(x):
    """x 的证券标识 {code, name, market_flag}。"""
    return {k: x[k] for k in SEC}


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
    躲开客户端正改写堆（torn read）的瞬间；一致的 None 即「已不在」。
    tries 次内仍未稳定即 assert（宁可不给数也不给错数）。"""
    prev = fn()
    for _ in range(tries - 1):
        time.sleep(gap)
        cur = fn()
        if cur == prev:
            return cur
        prev = cur
    assert False, f"连读 {tries} 次仍不一致，内存正被改写"


# ---------- 形状与列：\r\n 分行、| 分列、GBK，首行表头 ----------


def table(name, anchor, back, head, row, empty, slack, views, cols, check, mode, key):
    """声明一张表：表头 head 后随 row 若干行即整块，empty 即可以一行都没有。"""
    block = re.compile(head + rb"(?:" + row + (rb")*" if empty else rb")+"))
    return Table(name, anchor, back, block, empty, slack, views, cols, check, mode, key)


def body(t, m):
    """整块 match → 块字节：表头带行数 n 即表头 + 前 n 行，后随不足 n 行（被覆盖了一半的旧缓冲 / 分页块）
    或不该空的表 n = 0 回 None；不带即整块。"""
    if "n" not in t.block.groupindex:
        return m.group()
    n, lines = int(m["n"]), m.group().split(b"\r\n")
    return (
        b"\r\n".join(lines[: n + 1]) + b"\r\n"
        if (n or t.empty) and n < len(lines) - 1
        else None
    )


def find(t, lo, data):
    """段（起址 lo）里每处锚点，往前 back 字节内取首个能 match 出整块的 0| 起点，body 成形即一个命中。"""
    end, out = lo + len(data), []
    i = data.find(t.anchor)
    while i != -1:
        for hs in range(max(0, i - t.back), i):
            m = data.startswith(b"0|", hs) and t.block.match(data, hs)
            if m:
                b, a = body(t, m), lo + hs
                if b:
                    out.append(Hit(a, m.group(1), b, min(end, a + len(b) + t.slack)))
                break
        i = data.find(t.anchor, i + 1)
    return out


def reread(t, pid, h):
    """按址复读命中 h 的窗口，在块首重新匹配；块已不在（被释放 / 改作他用 / 标签变了）回 None。"""
    m = t.block.match(read_at(pid, h.addr, h.end - h.addr))
    return body(t, m) if m and m.group(1) == h.tag else None


def _take(f, conv, at):
    """取一列转型；at 为 None 即该布局无此列（取 None），为 {方向: 列号} 即取成 {方向: 值}。"""
    if at is None:
        return None
    if isinstance(at, dict):
        return {s: conv(f[i]) for s, i in at.items()}
    return conv(f[at])


def parse(t, raw):
    """块 → [记录]：解码一次、无替换字符，首行表头其后每行一条；列数全表一致，即 views 里第几个布局，
    按 cols 取列，再逐条 check（不过即 assert，原样返回）。"""
    text = raw.decode("gbk", errors="replace")
    assert "\ufffd" not in text, f"{t.name}明文含 GBK 替换字符，疑似读到写入中途的坏块"
    lines = [ln.split("|") for ln in text.split("\r\n")[1:-1]]
    if not lines:
        return []
    nfs = {len(f) for f in lines}
    assert len(nfs) == 1, f"{t.name}行列数不一致 {nfs}，块可能被截断/串行"
    nf = nfs.pop()
    assert nf in t.views, f"{t.name} {nf} 列，不认识的布局（认识 {t.views}）"
    at = 1 + t.views.index(nf)
    return [
        t.check({k: _take(f, c[0], c[at]) for k, c in t.cols.items()}) for f in lines
    ]


def check_id(r):
    """各表行内共有的标识：代码 6 位、沪深标志认识。"""
    assert re.fullmatch(r"\d{6}", r["code"]), f"非法代码 {r['code']!r}"
    assert r["market_flag"] in MARKETS, f"{r['code']} 沪深标志 {r['market_flag']}"


# ---------- 读法 ----------


def _concat(t, tables):
    return [r for x in tables for r in x]


def _equal(t, tables):
    assert all(x == tables[0] for x in tables), f"{t.name}整块 {len(tables)} 份内容不一"
    return tables[0] if tables else []


def live(tag):
    """活动块：客户端在用的那块表头标签为 tag、全进程唯一（一块都没有即还没查过，不选、不算错）。
    same 按址复读比字节（约 0.03ms，可高频）；rescan 先扫上次所在段，段内不恰为一块再全量（唯一性校验随之回到全进程）。"""

    def mine(hits):
        return [h for h in hits if h.tag == tag]

    def choose(t, hits):
        got = mine(hits)
        assert (
            len(got) == 1 or not hits
        ), f"{t.name}活动块应唯一，实得 {len(got)}（共 {len(hits)} 份），客户端行为已变或正在刷新"
        return got

    def same(snap):
        return all(reread(snap.table, snap.pid, h) == h.body for h in snap.mark)

    def rescan(snap):
        near = segs(snap)
        return near if len(mine(near)) == 1 else full(snap)

    return Mode(choose, _concat, same, rescan)


def union(tag, uid, order):
    """副本并集：表头标签为 tag() 的全部副本（只增不改，旧副本是新的子集），按 uid 取并集、同号逐字段一致，按 order 排。
    same 扫上次各副本所在段，段内的都在上次的里面（旧副本被释放不算换代；还没有副本恒为真）；rescan 即全量。"""

    def choose(t, hits):
        want = tag()
        return [h for h in hits if h.tag == want]

    def merge(t, tables):
        uniq = {}
        for r in _concat(t, tables):
            k = tuple(r[f] for f in uid)
            assert (
                uniq.setdefault(k, r) == r
            ), f"{t.name} {k} 两份副本不一致：{uniq[k]} ≠ {r}"
        return sorted(uniq.values(), key=lambda r: tuple(r[f] for f in order))

    def same(snap):
        return set(choose(snap.table, segs(snap))) <= set(snap.mark)

    return Mode(choose, merge, same, full)


# 查询前后比对：ask 给的新命中各份相同，取一份
ASK = Mode(lambda t, hits: hits[:1], _concat, None, None)
# 全表：收齐的整块可能留着几份，须逐字节一致，取一份
WHOLE = Mode(lambda t, hits: hits, _equal, None, None)


# ---------- 索引 ----------


def unique(*key):
    """以 key 各字段为键（单字段即其值，多字段即元组）→ {键: 行}；键不重。"""

    def index(t, rows):
        out = {
            (r[key[0]] if len(key) == 1 else tuple(r[k] for k in key)): r for r in rows
        }
        assert len(out) == len(
            rows
        ), f"{t.name} {key} 有重复（{len(rows)} 行 {len(out)} 个）"
        return out

    return index


def tally(sums, uid=None):
    """按代码合计 → {代码: 证券标识 | {sums 各键: {方向: Σ sums[键](行)}}}；同代码沪深标志须一致，
    给了 uid 即该字段各行不重。index.blank() 为没有行的那只的合计。"""

    def blank():
        return {k: dict.fromkeys(SIDES, 0.0) for k in sums}

    def index(t, rows):
        if uid:
            ids = {r[uid] for r in rows}
            assert len(ids) == len(
                rows
            ), f"{t.name} {uid} 有重复（{len(rows)} 行 {len(ids)} 个）"
        out = {}
        for r in rows:
            a = out.setdefault(r["code"], sec(r) | blank())
            assert (
                a["market_flag"] == r["market_flag"]
            ), f"{t.name} {r['code']} 沪深标志不一致"
            for k, f in sums.items():
                a[k][r["side"]] += f(r)
        return out

    index.blank = blank
    return index


def pairs(t, rows):
    """每行几组「名称 | 数值」（列 k0 v0 k1 v1 …，空组名称为空）→ {名称: 数值}（能转即 float，如「无」「300.00%」原样）；名称不重。"""
    kv = [
        (r[f"k{i}"], r[f"v{i}"]) for r in rows for i in range(len(r) // 2) if r[f"k{i}"]
    ]
    out = {k: float(v) if NUM_RE.fullmatch(v) else v for k, v in kv}
    assert len(out) == len(kv), f"{t.name}名称有重复：{[k for k, _ in kv]}"
    return out


# ---------- 流程 ----------


def scan(pid, tables, near=None):
    """一遍读堆段（near 见 iter_regions），每段交给各表 find → 与 tables 同序的命中列表。"""
    hits = [[] for _ in tables]
    for lo, data in iter_regions(pid, near):
        for h, t in zip(hits, tables):
            h += find(t, lo, data)
    return hits


def segs(snap):
    """段：只扫 snap 各块所在的段 → 该表命中。"""
    return scan(snap.pid, (snap.table,), [h.addr for h in snap.mark])[0]


def full(snap):
    """全：扫全部堆段 → 该表命中。"""
    return scan(snap.pid, (snap.table,))[0]


def pick(t, pid, hits):
    """选取 → 各块按址连读一致（其间被释放的略去，但不能全没了）→ 解析校验 → 合并 → 索引 → 快照。"""
    chosen = t.mode.choose(t, hits)
    mark = tuple(
        h._replace(body=b) for h in chosen if (b := stable(lambda: reread(t, pid, h)))
    )
    assert (
        mark or not chosen
    ), f"{t.name}选中的 {len(chosen)} 块在复读前全被释放，客户端正在重查"
    rows = t.mode.merge(t, [parse(t, h.body) for h in mark])
    return Snap(t, pid, mark, rows, t.key(t, rows))


def follow(snap):
    """没换代回 snap 本身；换代即重扫再 pick。"""
    t = snap.table
    assert t.mode.same, f"{t.name}的读法盯不了换代"
    return snap if t.mode.same(snap) else pick(t, snap.pid, t.mode.rescan(snap))


def read(tables, pid=None):
    """全量：定位进程，一遍扫全部堆段，各表 pick 出快照 → 与 tables 同序。"""
    pid = attach(pid)
    return tuple(pick(t, pid, h) for t, h in zip(tables, scan(pid, tables)))


def wait(snap, timeout=3.0, blind=1.0, gap=0.01):
    """客户端重查后，轮询 follow 等 snap 换代即回新快照，不等固定时长。
    timeout 内没换代（应答与旧的逐字节相同，如收盘后；或 same 盯不到新块）即全量读。
    snap 没有块（还没查过 / 今日无成交）即无从盯，等 blind 秒全量读。"""
    end = time.monotonic() + (timeout if snap.mark else blind)
    while time.monotonic() < end:
        new = follow(snap)
        if new != snap:
            return new
        time.sleep(gap)
    return read((snap.table,), snap.pid)[0]


def ask(t, act, pid=None, timeout=5.0):
    """act 让客户端重查一次：其前后各扫一遍全部堆段，之后新冒出的命中（新址或新内容）即这次的应答，
    各份须逐字节一致 → 快照（mark 为新命中）。timeout 内没冒出即 assert。"""
    assert t.mode is ASK, f"{t.name}不经 ask 读"
    pid = attach(pid)
    old = set(scan(pid, (t,))[0])
    act()
    end = time.monotonic() + timeout
    while not (new := [h for h in scan(pid, (t,))[0] if h not in old]):
        assert time.monotonic() < end, f"{timeout}s 内没等到{t.name}的新应答"
    assert len({h.body for h in new}) == 1, f"{t.name}新应答 {len(new)} 份内容不一"
    return pick(t, pid, new)
