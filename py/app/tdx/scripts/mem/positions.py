#!/usr/bin/env python3
# mem/positions — 内存方案：持仓明文表（customize，非通用基建）。
# 客户端点过一次「持仓查询」后，解密明文驻留堆里。表头 0||<n>||P00，
# 其后每行一只持仓：\r\n 分行、| 分列、行首 6 位代码、GBK 编码。
# 两种列布局：≥22 列完整视图 / 18 列精简视图（重登后常见），列序见 COLS。
#
# 选块（实测）：每次查询在网络缓冲里多留一份 n=行数 的应答副本（历史快照，永不更新）；
# 客户端在用的活动块 n=0、全进程唯一，新查询到来即释放旧块、另建新块。只认活动块。
# 新旧活动块总在同一个堆段里来回换址：重读时先只扫上次所在段（106MB vs 全量 580MB），没有再全量。
# 换代检测：按址复读旧块比字节（约 0.03ms），可高频盯，见 follow。
#
# 三级范围，同一动作（找活动块），代价递增：
#   窗 = 块址 + SLACK（_reread）  段 = 上次所在段（_locate near）  全 = 全部堆段（_locate）
# 对外一对（协议见 common）：read 全，follow 窗（换代才落到段）；没有活动块时 follow 盯不到新块，只能靠 read。
#
# 完整性（读内存最大的坑是读到写入中途的坏块）：
#   1) read 定位一次活动块（全量须唯一），再按址只复读该块，连读两次字节一致才采信（common.stable）；
#   2) parse 边解析边校验：块级无 GBK 替换字符、列布局全表一致；行级代码 6 位、沪深标志合法、
#      且「市值 ≈ 现价 × 余额」交叉验证列没错位。
# 任一校验不过直接 assert（早失败，宁可不给数也不给错数）。

import re
from collections import namedtuple

from . import common
from .common import BUY, SELL

# 快照：不可变值，== 即「同一代」（held 由 raw 派生）。win = 活动块复读窗口 (表头地址, 窗口尾)；
# held = {代码: 持仓}，按表内顺序。没有活动块（没查询过）时 win / raw 为 None、held 为 {}。
Snap = namedtuple("Snap", "pid win raw held")

ANCHOR = b"||P00"  # find 用的定长锚点，表头起点在其前 0||<n>
ROW = rb"\d{6}\|(?:[^|\r\n]*\|){4}[^\r\n]*\r\n"  # 6 位代码 + 至少 5 个 |
BLOCK_RE = re.compile(rb"0\|\|(\d+)\|\|P00[^\r\n]*\r\n(?:" + ROW + rb")+")
LIVE = b"0"  # 活动块表头的 n
SLACK = 64 * 1024  # 复读窗口在块尾外多留的字节，容块在两次读之间长大


def _num(x):
    return float(x) if x else None


# 字段 → (转型, 完整视图列号, 精简视图列号)；None = 该视图没有此列（取值 None）。
# 完整视图列号对照客户端持仓表核过；精简视图少的三列（今买/今卖/融资）对不上号，宁缺。
FULL, COMPACT = 1, 2
# fmt: off
COLS = {
    "code":         (str,  0,  0),
    "name":         (str,  2,  2),  # 列 1 对 ETF 是短码名（如 5GETF），列 2 是全称（通信ETF华夏）
    "balance":      (_num, 3,  3),     # 证券数量
    "available":    (_num, 4,  4),     # 可卖数量
    "financed":     (_num, 7,  None),  # 融资数量
    "cost":         (_num, 8,  7),     # 参考成本价
    "price":        (_num, 9,  8),
    "market_value": (_num, 10, 9),
    "pnl":          (_num, 11, 10),    # 参考浮动盈亏（券商口径，≠ (现价-成本价)×量）
    "pnl_pct":      (_num, 12, 11),    # = 现价/参考成本价 - 1
    "position_pct": (_num, 13, 12),    # 仓位(%)
    "market_flag":  (int,  14, 13),
    "collateral":   (_num, 16, None),  # 担保折算率
    "shareholder":  (str,  19, 15),
}
# fmt: on
TRADED = {BUY: 5, SELL: 6}  # 今买 / 今卖数量的完整视图列号，取成 traded = {方向: 股}；精简视图无，traded 为 None


def _blocks(lo, data):
    """data（起址 lo）里每个表头起的整块 → (表头地址, n, 块)。find 定位锚点，回退到 0|| 处正则匹配。"""
    ho = data.find(ANCHOR)
    while ho != -1:
        hs = data.rfind(b"0||", max(0, ho - 12), ho)
        m = hs != -1 and BLOCK_RE.match(data, hs)
        if m:
            yield lo + hs, m.group(1), m.group()
        ho = data.find(ANCHOR, ho + 1)


def _locate(pid, near=None):
    """扫一遍 → 活动块复读窗口 (表头地址, 窗口尾)；一份块都没有（没查询过）回 None。窗口不越段尾。
    near：只扫含该地址的段，段内活动块不恰为一个即退回全量（唯一性校验随之限于该段）。"""
    hits = [
        (addr, min(lo + len(data), addr + len(b) + SLACK), n)
        for lo, data in common.iter_regions(pid, near)
        for addr, n, b in _blocks(lo, data)
    ]
    live = [h[:2] for h in hits if h[2] == LIVE]
    if near is not None and len(live) != 1:
        return _locate(pid)
    assert (
        len(live) == 1 or not hits
    ), f"活动块（0||0||P00）应唯一，实得 {len(live)}（共 {len(hits)} 份），客户端行为已变或正在刷新"
    return live[0] if live else None


def _reread(pid, addr, end):
    """按址复读窗口，在表头处重新匹配活动块；块已不在（被释放/改作他用）回 None。"""
    m = BLOCK_RE.match(common.read_at(pid, addr, end - addr))
    return m.group() if m and m.group(1) == LIVE else None


def _position(f, at):
    """一行字段 → 持仓；按 COLS 取 at 视图的列转型，按 TRADED 取今买 / 今卖。"""
    p = {k: None if c[at] is None else c[0](f[c[at]]) for k, c in COLS.items()}
    p["traded"] = {s: _num(f[i]) for s, i in TRADED.items()} if at == FULL else None
    return p


def _check(p):
    """一只持仓的结构校验，不过即 assert；原样返回。"""
    assert re.fullmatch(r"\d{6}", p["code"]), f"非法代码 {p['code']!r}"
    assert p["name"], f"{p['code']} 名称为空"
    assert p["market_flag"] in (0, 1), f"{p['code']} 沪深标志异常 {p['market_flag']}"
    mv, price, bal = p["market_value"], p["price"], p["balance"]
    if mv and price and bal and price > 0:  # 停牌价为 0 时跳过
        exp = price * bal
        assert abs(exp - mv) <= max(
            1.0, exp * 0.02
        ), f"{p['code']} 市值({mv})≠现价×余额({exp:.2f})，数值列疑似错位"
    return p


def parse(raw):
    """解析活动块 → {代码: 持仓}，边解析边校验。块由 BLOCK_RE 保证：首行表头，其后全是持仓行（至少一行）。
    块级：解码一次、无替换字符、列数全表一致并据此定布局、代码不重；行级：_position 取列、_check 校验。"""
    text = raw.decode("gbk", errors="replace")
    assert "\ufffd" not in text, "明文含 GBK 替换字符，疑似读到写入中途的坏块"
    lines = [ln.split("|") for ln in text.split("\r\n")[1:-1]]
    nfs = {len(f) for f in lines}
    assert len(nfs) == 1, f"行列数不一致 {nfs}，块可能被截断/串行"
    nf = nfs.pop()
    at = FULL if nf >= 22 else COMPACT
    assert nf > max(
        c[at] for c in COLS.values() if c[at] is not None
    ), f"仅 {nf} 列，不认识的布局"
    held = {p["code"]: p for p in (_check(_position(f, at)) for f in lines)}
    assert len(held) == len(lines), f"持仓代码有重复（{len(lines)} 行 {len(held)} 只）"
    return held


def _read(pid, near):
    """定位活动块 → 按址连读一致 → 解析校验 → Snap。near 见 _locate。
    块不存在（没点过查询）时 win / raw 为 None、held 为 {}，不算错误。"""
    win = _locate(pid, near)
    if win is None:
        return Snap(pid, None, None, {})
    raw = common.stable(lambda: _reread(pid, *win))
    return Snap(pid, win, raw, parse(raw))


def read(pid=None):
    """全：定位进程，扫全部堆段。"""
    return _read(common.attach(pid), None)


def follow(snap):
    """窗：按址复读活动块，未换代回 snap 本身；换代（被释放/改写，实测重查后约 100ms）即在原段重读。
    没有活动块时无从盯，原样返回。"""
    if snap.win is None or _reread(snap.pid, *snap.win) == snap.raw:
        return snap
    return _read(snap.pid, snap.win[0])
