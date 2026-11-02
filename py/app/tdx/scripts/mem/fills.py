#!/usr/bin/env python3
# mem/fills — 内存方案：当日成交明文表（customize，非通用基建）。
# 客户端查过一次「当日成交」后，明文驻留堆里。表头 0|<日期 8 位><10 位>|<n>||P|，
# 其后 n 行、每行一笔成交：\r\n 分行、| 分列、GBK 编码，列序见 COLS。
#
# 选块（实测）：每次查询在网络缓冲与客户端各留几份副本，旧查询的副本不即时释放，且没有活动块标记。
# 成交只增不改，故不选块：当日全部完整副本按成交编号取并集即全量（旧副本是其子集），同号须逐字段一致。
#
# 两级范围，代价递增：段 = 上次各副本所在段  全 = 全部堆段
# same 扫段，段内今日副本都在上次的里面（旧副本被释放不算换代）；rescan 即全量。今日还没有副本时无段可盯，恒为假（流程见 common）。
#
# 完整性：只认表头日期为本机今日（choose）、且后随至少 n 行的副本（被新内容覆盖了一半的旧缓冲不算），只取前 n 行（body）；
# 各副本按址连读一致（common.pick）；块级同持仓（common.parse）；行级校验代码、沪深标志、时间、方向、状态、
# 价 × 量 ≈ 金额（check）；同一笔在各副本里逐字段一致（merge），任一不过即 assert。

import math
import re
import time

from . import common
from .common import BUY, SELL, SIDES

ANCHOR = b"||P|\r\n"
BACK = 40
HEAD = rb"0\|(\d{8})\d{10}\|(\d+)\|\|P\|\r\n"  # 标签 = 日期
ROW = rb"\d\d:\d\d:\d\d\|\d{6}\|[^\x00\r\n]*\|\|\r\n"  # 时间 + 6 位代码 + 行尾空列，细校验在 check
BLOCK_RE = re.compile(HEAD + rb"(?:" + ROW + rb")+")
SLACK = 0  # 副本只取前 n 行，复读即这么长
TIME_RE = re.compile(r"\d\d:\d\d:\d\d")
SIDE_TEXT = {"买入": BUY, "卖出": SELL}


def _side(x):
    assert x in SIDE_TEXT, f"买卖方向「{x}」不认识"
    return SIDE_TEXT[x]


VIEWS = (17,)  # 列数：15 列 + 行尾两个空列
# 字段 → (转型, 列号)
# fmt: off
COLS = {
    "time":        (str,   0),
    "code":        (str,   1),
    "name":        (str,   3),   # 列 2 对 ETF 是短码名（如 5GETF），列 3 是全称（同持仓表）
    "market_flag": (int,   4),
    "side":        (_side, 5),   # 方向 BUY / SELL
    "status":      (str,   6),
    "price":       (float, 7),
    "qty":         (float, 8),   # 股
    "amount":      (float, 9),   # 元
    "fill_id":     (str,   10),  # 成交编号
    "order_id":    (str,   11),  # 委托编号
}
# fmt: on


def _flow():
    return {"traded": dict.fromkeys(SIDES, 0.0), "amount": dict.fromkeys(SIDES, 0.0)}


# 该只今日没有成交的合计（只读）
NO_FLOW = _flow()


def body(m):
    """表头 + 前 n 行；后随不足 n 行（旧缓冲被覆盖了一半）回 None。"""
    n, lines = int(m.group(2)), m.group().split(b"\r\n")
    return b"\r\n".join(lines[: n + 1]) + b"\r\n" if 0 < n < len(lines) - 1 else None


def check(r):
    """一笔成交的结构校验，不过即 assert；原样返回。"""
    common.check_id(r)
    assert TIME_RE.fullmatch(r["time"]), f"成交时间 {r['time']!r}"
    assert r["status"] == "成交", f"{r['code']} 成交状态「{r['status']}」不认识：{r}"
    assert r["qty"] > 0 and r["price"] > 0 and r["fill_id"], f"成交行数值异常：{r}"
    assert math.isclose(
        r["price"] * r["qty"], r["amount"], rel_tol=1e-4, abs_tol=0.02
    ), f"{r['code']} 金额≠价×量：{r}"
    return r


def choose(hits):
    """表头日期为本机今日的各副本。"""
    day = time.strftime("%Y%m%d").encode()
    return [h for h in hits if h.tag == day]


def merge(tables):
    """各副本取并集，按成交时间、成交编号排序；同一笔（成交编号 + 委托编号 + 代码）须逐字段一致。"""
    uniq = {}
    for r in (r for t in tables for r in t):
        k = (r["fill_id"], r["order_id"], r["code"])
        assert uniq.setdefault(k, r) == r, f"成交 {k} 两份副本不一致：{uniq[k]} ≠ {r}"
    return sorted(uniq.values(), key=lambda r: (r["time"], r["fill_id"]))


def by_code(rows):
    """成交按代码合计 → {代码: {code, name, market_flag, traded: {方向: 股}, amount: {方向: 元}}}，
    traded 与持仓同形可直接比对；同代码沪深标志须一致。"""
    out = {}
    for r in rows:
        a = out.setdefault(r["code"], {k: r[k] for k in ("code", "name", "market_flag")} | _flow())
        assert a["market_flag"] == r["market_flag"], f"成交 {r['code']} 沪深标志不一致"
        a["traded"][r["side"]] += r["qty"]
        a["amount"][r["side"]] += r["amount"]
    return out


def same(snap):
    """段：上次各副本所在段里的今日副本（同址同内容）都在上次的里面。今日还没有副本恒为假。"""
    return bool(snap.mark) and set(choose(common.segs(snap))) <= set(snap.mark)


def rescan(snap):
    """全量。"""
    return common.full(snap)
