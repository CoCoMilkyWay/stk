#!/usr/bin/env python3
# mem/collat — 内存方案：担保品全表（customize，非通用基建）。
# 「担保品查询」页点「查询全部」后，客户端分页收应答（每页一块、表头 0||<该页行数>||P<定位串>|），
# 收齐后另拼一整块，表头 0||0||P<末页定位串>|、其后全部行，驻留堆里。每行一只：沪深标志 / 交易所 / 代码 / 名称 /
# 长简称 / 担保折算率 / 担保状态 / 担保物比例 / 静态市盈率 / 公允价格 / 公允价格启用标志 / 集中度分组 / 备注，
# 再接两个空列（15 列）。
#
# 选块：整块表头 n 恒为 0、不带总数，收齐与否由调用方对客户端列表的条数核；分页块 n > 0 不认。
# 整块可能留着几份（每查全部一次一份），须逐字节一致（merge）。
# 完整性：块级同持仓（common.parse）；行级代码 6 位、沪深标志认识、折算率在 [0, 1]（check）；
# 代码跨市场会重（沪市债券与深市基金同号），by_code 以 (沪深标志, 代码) 为键、不重。

import re

from .margin import MARKETS

ANCHOR = b"||0||P"
BACK = 1
HEAD = rb"0\|\|0\|\|P(\d+)\|\r\n"  # 标签 = 末页定位串
ROW = rb"\d+\|[^|\r\n\x00]*\|\d{6}\|[^\r\n\x00]*\r\n"
BLOCK_RE = re.compile(HEAD + rb"(?:" + ROW + rb")+")
SLACK = 0

VIEWS = (15,)
# fmt: off
COLS = {
    "market_flag": (int,   0),
    "code":        (str,   2),
    "name":        (str,   4),  # 长简称（同持仓表名称列）
    "rate":        (float, 5),  # 担保折算率
    "status":      (str,   6),  # 担保状态
    "remark":      (str,   12),
}
# fmt: on


def body(m):
    return m.group()


def check(r):
    assert re.fullmatch(r"\d{6}", r["code"]), f"非法代码 {r['code']!r}"
    assert r["market_flag"] in MARKETS, f"{r['code']} 沪深标志 {r['market_flag']}"
    assert 0 <= r["rate"] <= 1, f"{r['code']} 担保折算率 {r['rate']}"
    return r


def choose(hits):
    return hits


def merge(tables):
    """各份整块须一致，取一份。"""
    assert all(t == tables[0] for t in tables), f"担保品整块 {len(tables)} 份内容不一"
    return tables[0] if tables else []


def by_code(rows):
    out = {(r["market_flag"], r["code"]): r for r in rows}
    assert len(out) == len(rows), f"担保品代码有重复（{len(rows)} 行 {len(out)} 只）"
    return out
