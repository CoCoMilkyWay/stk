#!/usr/bin/env python3
# mem/positions — 内存方案：持仓明文表（customize，非通用基建）。
# 客户端点过一次「持仓查询」后，解密明文驻留堆里。表头 0||<n>||P00，
# 其后每行一只持仓：\r\n 分行、| 分列、行首 6 位代码、GBK 编码。
# 两种列布局：23 列完整视图 / 18 列精简视图（重登后常见），列序见 COLS。
#
# 选块（实测）：每次查询在网络缓冲里多留一份 n=行数 的应答副本（历史快照，永不更新）；
# 客户端在用的活动块 n=0、全进程唯一，新查询到来即释放旧块、另建新块。只认活动块。
# 新旧活动块总在同一个堆段里来回换址：换代后先只扫上次所在段，没有再全量。
#
# 三级范围，同一动作（找活动块），代价递增：
#   窗 = 块址 + SLACK（_reread）  段 = 上次所在段  全 = 全部堆段
# follow 盯窗（约 0.03ms，可高频），换代才落到段；没有活动块时无从盯，只能靠全量（协议见 common）。
#
# 完整性（读内存最大的坑是读到写入中途的坏块）：
#   1) 全量定位活动块须唯一，再按址只复读该块，连读两次字节一致才采信（common.stable）；
#   2) 边解析边校验：块级无 GBK 替换字符、列数全表一致且认识（common.rows）；行级代码 6 位、
#      沪深标志合法、且「市值 ≈ 现价 × 余额」交叉验证列没错位（_check）；代码不重（_by_code）。
# 任一校验不过直接 assert（早失败，宁可不给数也不给错数）。

import math
import re

from . import common
from .common import BUY, SELL, Snap

ANCHOR = b"||P00"  # 定长锚点，表头起点在其前 0||<n>
BACK = 12  # 锚点往前找表头起点的字节数
HEAD = rb"0\|\|(\d+)\|\|P00[^\r\n]*\r\n"
ROW = rb"\d{6}\|(?:[^|\r\n]*\|){4}[^\r\n]*\r\n"  # 6 位代码 + 至少 5 个 |
BLOCK_RE = re.compile(HEAD + rb"(?:" + ROW + rb")+")
LIVE = b"0"  # 活动块表头的 n
SLACK = 64 * 1024  # 复读窗口在块尾外多留的字节，容块在两次读之间长大


def _num(x):
    return float(x) if x else None


VIEWS = (23, 18)  # 列数：完整视图 / 精简视图
# 字段 → (转型, 完整视图列号, 精简视图列号)；None = 该视图没有此列（取值 None），{方向: 列号} 取成 {方向: 值}。
# 完整视图列号对照客户端持仓表核过；精简视图少的三列（今买/今卖/融资）对不上号，宁缺。
# fmt: off
COLS = {
    "code":         (str,  0,                  0),
    "name":         (str,  2,                  2),     # 列 1 对 ETF 是短码名（如 5GETF），列 2 是全称（通信ETF华夏）
    "balance":      (_num, 3,                  3),     # 证券数量
    "available":    (_num, 4,                  4),     # 可卖数量
    "traded":       (_num, {BUY: 5, SELL: 6},  None),  # 今买 / 今卖数量 {方向: 股}，与成交合计同形
    "financed":     (_num, 7,                  None),  # 融资数量
    "cost":         (_num, 8,                  7),     # 参考成本价
    "price":        (_num, 9,                  8),
    "market_value": (_num, 10,                 9),
    "pnl":          (_num, 11,                 10),    # 参考浮动盈亏（券商口径，≠ (现价-成本价)×量）
    "pnl_pct":      (_num, 12,                 11),    # = 现价/参考成本价 - 1
    "position_pct": (_num, 13,                 12),    # 仓位(%)
    "market_flag":  (int,  14,                 13),
    "collateral":   (_num, 16,                 None),  # 担保折算率
    "shareholder":  (str,  19,                 15),
}
# fmt: on


def find(lo, data):
    """段内每份块 → [(表头地址, n, 复读窗口尾)]；窗口尾不越段尾。"""
    end = lo + len(data)
    return [
        (a, m.group(1), min(end, lo + m.end() + SLACK))
        for a, m in common.blocks(lo, data, ANCHOR, BACK, BLOCK_RE)
    ]


def _live(hits):
    """命中里的活动块复读窗口 [(表头地址, 窗口尾)]。"""
    return [(a, e) for a, n, e in hits if n == LIVE]


def _reread(pid, addr, end):
    """按址复读窗口，在表头处重新匹配活动块；块已不在（被释放/改作他用）回 None。"""
    m = BLOCK_RE.match(common.read_at(pid, addr, end - addr))
    return m.group() if m and m.group(1) == LIVE else None


def _check(r):
    """一只持仓的结构校验，不过即 assert；原样返回。停牌价为 0 时不核市值。"""
    common.check_id(r)
    assert r["name"], f"{r['code']} 名称为空"
    mv, px, bal = r["market_value"], r["price"], r["balance"]
    assert not (mv and px and bal) or math.isclose(
        px * bal, mv, rel_tol=0.02, abs_tol=1.0
    ), f"{r['code']} 市值({mv})≠现价×余额({px * bal:.2f})，数值列疑似错位"
    return r


def _by_code(rows):
    """持仓 → {代码: 持仓}，按表内顺序；代码不重。"""
    out = {r["code"]: r for r in rows}
    assert len(out) == len(rows), f"持仓代码有重复（{len(rows)} 行 {len(out)} 只）"
    return out


def pick(pid, hits):
    """活动块须唯一 → 按址连读一致 → 解析校验 → 快照，mark = (复读窗口, 块)。
    一份块都没有（没点过查询）时 mark 为 None、空表，不算错误。"""
    live = _live(hits)
    assert (
        len(live) == 1 or not hits
    ), f"活动块（0||0||P00）应唯一，实得 {len(live)}（共 {len(hits)} 份），客户端行为已变或正在刷新"
    if not live:
        return Snap(pid, None, [], {})
    win = live[0]
    raw = common.stable(lambda: _reread(pid, *win))
    rows = common.rows(raw, VIEWS, COLS, _check)
    return Snap(pid, (win, raw), rows, _by_code(rows))


def follow(snap):
    """窗：按址复读活动块，未换代回 snap 本身；换代（被释放/改写，实测重查后约 100ms）即扫原段，
    段内活动块不恰为一个即全量（唯一性校验随之回到全进程）。没有活动块时无从盯，原样返回。"""
    if snap.mark is None:
        return snap
    win, raw = snap.mark
    if _reread(snap.pid, *win) == raw:
        return snap
    near = common.scan(snap.pid, (find,), (win[0],))[0]
    hits = near if len(_live(near)) == 1 else common.scan(snap.pid, (find,))[0]
    return pick(snap.pid, hits)
