#!/usr/bin/env python3
# mem/credit — 内存方案：信用资产明文表（customize，非通用基建）。
# 客户端点「信用资产」查询后明文驻留堆里。表头 0||<n>|||（n = 19），其后 n 行，
# 每行三组「名称|数值|」再接两个空列（11 列），如 担保资产|3421235.32||最高融资额度|2047032.11||…|||；空组名称为空。
#
# 选块（实测）：每次查询在网络缓冲留一份（历史快照，永不更新），客户端另有一份、每查一次换址；
# 同一次应答各份逐字节相同，表内没有新旧标记，故只经 common.ask 读（查询前后各扫一遍，新冒出的即本次应答）。
#
# 完整性：行数恰为表头 n（body）；块级同持仓（common.parse）；首组名称非空（check）；名称不重（by_code）。

import re

from . import common

ANCHOR = b"|||\r\n"
BACK = 12
HEAD = rb"0\|\|(\d+)\|\|\|\r\n"  # 标签 = n
# 名称打头（资金汇总、标的表的行以数字打头）
ROW = rb"[^\d|\r\n\x00][^|\r\n\x00]*\|[^\r\n\x00]*\r\n"
BLOCK_RE = re.compile(HEAD + rb"(?:" + ROW + rb")+")
SLACK = 0
NUM_RE = re.compile(r"-?\d+(\.\d+)?")

VIEWS = (11,)
# fmt: off
COLS = {
    "k0": (str, 0), "v0": (str, 1),
    "k1": (str, 3), "v1": (str, 4),
    "k2": (str, 6), "v2": (str, 7),
}
# fmt: on


def body(m):
    """表头 + 前 n 行；后随不足 n 行回 None。"""
    n, lines = int(m.group(1)), m.group().split(b"\r\n")
    return b"\r\n".join(lines[: n + 1]) + b"\r\n" if 0 < n < len(lines) - 1 else None


def check(r):
    assert r["k0"], f"信用资产行首组名称为空：{r}"
    return r


def choose(hits):
    """ask 给的新命中各份相同，取一份。"""
    return hits[:1]


def merge(tables):
    return [r for t in tables for r in t]


def _val(s):
    return float(s) if NUM_RE.fullmatch(s) else s


def by_code(rows):
    """→ {名称: 数值}（能转即 float，如「无」「300.00%」原样）；名称不重。"""
    pairs = [(r[f"k{i}"], r[f"v{i}"])
             for r in rows for i in range(3) if r[f"k{i}"]]
    out = {k: _val(v) for k, v in pairs}
    assert len(out) == len(pairs), f"信用资产名称有重复：{[k for k, _ in pairs]}"
    return out
