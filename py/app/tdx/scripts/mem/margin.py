#!/usr/bin/env python3
# mem/margin — 内存方案：标的证券全表（customize，非通用基建）。
# 「标的证券查询」页点「查询全部」后，客户端分页收应答（每页一块、表头都是 0||<总数>|||），收齐后另拼一整块，
# 表头同为 0||<总数>|||、其后恰总数行，驻留堆里。每行一只：代码 / 名称 / 长简称 / 沪深标志 / 交易所 /
# 融资保证金比例 / 融券保证金比例 / 备注（融资标的,融券标的）/ 融券状态 / 当日可融 / 融资状态，再接三个空列（14 列）。
#
# 选块：只认行数够总数的整块（分页块不够，body 回 None）；整块可能留着几份（每查全部一次一份），须逐字节一致（merge）。
# 完整性：块级同持仓（common.parse）；行级代码 6 位、沪深标志认识、保证金比例为正（check）；
# 代码跨市场会重，by_code 同担保品以 (沪深标志, 代码) 为键、不重。

import re

ANCHOR = b"|||\r\n"
BACK = 16
HEAD = rb"0\|\|(\d+)\|\|\|\r\n"  # 标签 = 总数
ROW = rb"\d{6}\|[^\r\n\x00]*\r\n"
BLOCK_RE = re.compile(HEAD + rb"(?:" + ROW + rb")+")
SLACK = 0
MARKETS = (0, 1, 60)  # 深 / 沪 / 北

VIEWS = (14,)
# fmt: off
COLS = {
    "code":        (str,   0),
    "name":        (str,   2),  # 长简称（同持仓表名称列）
    "market_flag": (int,   3),
    "ratio":       (float, 5),  # 融资保证金比例
    "remark":      (str,   7),
    "status":      (str,   10), # 融资状态
}
# fmt: on


def body(m):
    """表头 + 前总数行；后随不足总数（分页块）回 None。"""
    n, lines = int(m.group(1)), m.group().split(b"\r\n")
    return b"\r\n".join(lines[: n + 1]) + b"\r\n" if 0 < n < len(lines) - 1 else None


def check(r):
    assert re.fullmatch(r"\d{6}", r["code"]), f"非法代码 {r['code']!r}"
    assert r["market_flag"] in MARKETS, f"{r['code']} 沪深标志 {r['market_flag']}"
    assert r["ratio"] > 0, f"{r['code']} 融资保证金比例 {r['ratio']}"
    return r


def choose(hits):
    return hits


def merge(tables):
    """各份整块须一致，取一份。"""
    assert all(t == tables[0] for t in tables), f"标的证券整块 {len(tables)} 份内容不一"
    return tables[0] if tables else []


def by_code(rows):
    out = {(r["market_flag"], r["code"]): r for r in rows}
    assert len(out) == len(rows), f"标的证券代码有重复（{len(rows)} 行 {len(out)} 只）"
    return out
