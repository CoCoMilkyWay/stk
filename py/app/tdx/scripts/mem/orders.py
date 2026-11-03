#!/usr/bin/env python3
# mem/orders — 内存方案：可撤委托明文表（交易栏「撤单」页，即还挂着的单；customize，非通用基建）。
# 表头 0||<n>||P|，其后 n 行、每行一笔：委托时间 / 代码 / 短名 / 全称 / 买卖标志 / 买卖 / 委托类别 / 状态 /
# 委托价 / 委托量 / 委托编号 / 成交价 / 成交量 / ? / 操作 / 股东代码 / 沪深标志 / …，再接两个空列（23 列）。没有挂单时 n = 0、无行。
#
# 选块同信用资产（mem/credit）：每查在新址留两份、旧的不释放，表内没有新旧标记，只经 common.ask 读。
# 与融资负债汇总（mem/debts）表头同形，行首为时间可分；空表二者同为 0||0||P|，故只认 ask 冒出的新块。
# 完整性：行数恰为表头 n（body）；块级同持仓（common.parse）；行级代码、沪深标志、时间、方向、量价（check）；委托编号不重（by_code）。

import re

from . import common
from .common import BUY, SELL, SIDES

ANCHOR = b"||P|\r\n"
BACK = 12
HEAD = rb"0\|\|(\d+)\|\|P\|\r\n"  # 标签 = n
ROW = rb"\d\d:\d\d:\d\d\|\d{6}\|[^\r\n\x00]*\r\n"
BLOCK_RE = re.compile(HEAD + rb"(?:" + ROW + rb")*")
SLACK = 0
TIME_RE = re.compile(r"\d\d:\d\d:\d\d")
SIDE_TEXT = {"买入": BUY, "卖出": SELL}


def _side(x):
    assert x in SIDE_TEXT, f"买卖方向「{x}」不认识"
    return SIDE_TEXT[x]


VIEWS = (23,)
# 字段 → (转型, 列号)
# fmt: off
COLS = {
    "time":        (str,   0),
    "code":        (str,   1),
    "name":        (str,   3),   # 列 2 对 ETF 是短码名，列 3 是全称（同持仓表）
    "side":        (_side, 5),   # 方向 BUY / SELL（列 4 是买卖标志）
    "kind":        (str,   6),   # 委托类别（信用交易 / …）
    "status":      (str,   7),   # 已报 / 部成 / …
    "price":       (float, 8),
    "qty":         (float, 9),   # 委托量（股）
    "order_id":    (str,   10),
    "filled":      (float, 12),  # 已成交量（股）
    "market_flag": (int,   16),
}
# fmt: on


def body(m):
    """表头 + 前 n 行；后随不足 n 行回 None。"""
    n, lines = int(m.group(1)), m.group().split(b"\r\n")
    return b"\r\n".join(lines[: n + 1]) + b"\r\n" if n < len(lines) - 1 else None


def check(r):
    """一笔挂单的结构校验，不过即 assert；原样返回。"""
    common.check_id(r)
    assert TIME_RE.fullmatch(r["time"]), f"委托时间 {r['time']!r}"
    assert r["status"] and r["order_id"], f"挂单状态 / 委托编号为空：{r}"
    assert r["price"] > 0 and r["qty"] > 0 and 0 <= r["filled"] < r["qty"], f"挂单量价异常：{r}"
    return r


def choose(hits):
    """ask 给的新命中各份相同，取一份。"""
    return hits[:1]


def merge(tables):
    return [r for t in tables for r in t]


def by_code(rows):
    """挂单按代码合计 → {代码: {code, name, market_flag, pending: {方向: 未成股}}}；委托编号不重。"""
    ids = {r["order_id"] for r in rows}
    assert len(ids) == len(rows), f"挂单委托编号有重复（{len(rows)} 行 {len(ids)} 个）"
    out = {}
    for r in rows:
        a = out.setdefault(
            r["code"],
            {k: r[k] for k in ("code", "name", "market_flag")} | {"pending": dict.fromkeys(SIDES, 0.0)},
        )
        a["pending"][r["side"]] += r["qty"] - r["filled"]
    return out
