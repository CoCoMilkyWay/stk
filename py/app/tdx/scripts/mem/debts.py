#!/usr/bin/env python3
# mem/debts — 内存方案：融资负债汇总明文表（customize，非通用基建）。
# 客户端点「融资负债汇总」查询后明文驻留堆里。表头 0||<n>||P|，其后 n 行、每行一只有融资负债的证券：
# 沪深标志 / 交易所 / 代码 / 名称 / 长简称 / 持仓量 / 期初负债数量 / 当前负债数量 / 期初负债金额 / 当前负债金额 /
# 负债买入数量 / 负债买入金额 / 币种，再接两个空列（15 列）。没有负债时 n = 0、无行。
#
# 选块同信用资产（mem/credit）：网络缓冲每查留一份、客户端一份每查换址，只经 common.ask 读。
# 完整性：行数恰为表头 n（body）；块级同持仓（common.parse）；行级代码、沪深标志、数值非负（check）；代码不重（by_code）。

import re

from . import common

ANCHOR = b"||P|\r\n"
BACK = 12
HEAD = rb"0\|\|(\d+)\|\|P\|\r\n"  # 标签 = n
ROW = rb"\d+\|[^|\r\n\x00]*\|\d{6}\|[^\r\n\x00]*\r\n"
BLOCK_RE = re.compile(HEAD + rb"(?:" + ROW + rb")*")
SLACK = 0

VIEWS = (15,)
# 字段 → (转型, 列号)
# fmt: off
COLS = {
    "market_flag": (int,   0),
    "code":        (str,   2),
    "name":        (str,   4),
    "balance":     (float, 5),  # 持仓量
    "qty":         (float, 7),  # 当前负债数量（股）
    "amount":      (float, 9),  # 当前负债金额（元）
}
# fmt: on


def body(m):
    """表头 + 前 n 行；后随不足 n 行回 None。"""
    n, lines = int(m.group(1)), m.group().split(b"\r\n")
    return b"\r\n".join(lines[: n + 1]) + b"\r\n" if n < len(lines) - 1 else None


def check(r):
    common.check_id(r)
    assert r["qty"] >= 0 and r["amount"] >= 0, f"融资负债数值异常：{r}"
    return r


def choose(hits):
    """ask 给的新命中各份相同，取一份。"""
    return hits[:1]


def merge(tables):
    return [r for t in tables for r in t]


def by_code(rows):
    out = {r["code"]: r for r in rows}
    assert len(out) == len(rows), f"融资负债代码有重复（{len(rows)} 行 {len(out)} 只）"
    return out
