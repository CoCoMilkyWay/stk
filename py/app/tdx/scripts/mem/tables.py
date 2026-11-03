#!/usr/bin/env python3
# mem/tables — 客户端查过的各明文表（customize，依赖具体内存布局，非通用基建）。
# 每表一份声明：形状（锚点 / 表头 / 行）、列、读法、索引，四条轴的含义与流程见 common。
# 明文 GBK，\r\n 分行、| 分列，首行表头 0|…|；列号对照客户端各页表头核过，列数含行尾空列。
# 列 1 / 2 对 ETF 是短码名（如 5GETF），全称在下一列，各表名称一律取全称。
#
# 持仓      0||<n>||P00。每查在网络缓冲多留一份 n = 行数的副本（永不更新）；客户端在用的活动块 n = 0、全进程唯一，
#           新查询到来即释放旧块、另建新块，总在同一个堆段里来回换址（点「刷新」后约 100ms）。
# 成交      0|<日期 8 位><10 位>|<n>||P|。每查在网络缓冲与客户端各留几份副本、旧的不即时释放、没有活动块标记；
#           成交只增不改，当日全部副本取并集即全量。无成交时只回 0||0||P|（不带日期，认不出），重查只能等一会儿全量读。
# 挂单      交易栏「撤单」页（还挂着的单），0||<n>||P|，与融资负债同形、行首为时间可分。
# 融资负债  「融资负债汇总」，0||<n>||P|。
# 信用资产  0||<n>|||（n = 19），每行三组「名称|数值|」。
#           以上三表每查在新址留一两份、旧的不释放，同一次应答各份逐字节相同，表内没有新旧标记，只认 ask 冒出的新块
#           （空表 0||0||P| 与无成交的成交表同形）。
# 标的证券  「查询全部」后客户端分页收应答（每页一块），收齐另拼一整块：表头 0||<总数>|||、其后恰总数行，分页块行数不够。
# 担保品    同上：分页块 0||<该页行数>||P<定位串>|（n > 0，不认），整块 0||0||P<末页定位串>|、不带总数，
#           收齐与否由调用方对页上条数。
#           两表整块可能留着几份（每查全部一次一份）；代码跨市场会重（沪市债券与深市基金同号），以 (沪深标志, 代码) 为键。

import math
import re
import time

from .common import (
    ASK,
    BUY,
    SELL,
    WHOLE,
    check_id,
    live,
    pairs,
    table,
    tally,
    union,
    unique,
)

TIME_RE = re.compile(r"\d\d:\d\d:\d\d")
SIDE_TEXT = {"买入": BUY, "卖出": SELL}


def _num(x):
    return float(x) if x else None


def _side(x):
    assert x in SIDE_TEXT, f"买卖方向「{x}」不认识"
    return SIDE_TEXT[x]


def _today():
    return time.strftime("%Y%m%d").encode()


# ---------- 持仓 ----------


def _check_position(r):
    """「市值 ≈ 现价 × 余额」交叉验证数值列没错位；停牌价为 0 时不核。"""
    check_id(r)
    assert r["name"], f"{r['code']} 名称为空"
    mv, px, bal = r["market_value"], r["price"], r["balance"]
    assert not (mv and px and bal) or math.isclose(
        px * bal, mv, rel_tol=0.02, abs_tol=1.0
    ), f"{r['code']} 市值({mv})≠现价×余额({px * bal:.2f})，数值列疑似错位"
    return r


# 两种布局：23 列完整视图 / 18 列精简视图（重登后常见）；精简视图少的三列（今买 / 今卖 / 融资）对不上号，宁缺
# fmt: off
POSITIONS = table(
    "持仓",
    anchor=b"||P00",
    back=12,
    head=rb"0\|\|(\d+)\|\|P00[^\r\n]*\r\n",              # 标签 = n
    row=rb"\d{6}\|(?:[^|\r\n]*\|){4}[^\r\n]*\r\n",       # 6 位代码 + 至少 5 个 |
    empty=False,
    slack=64 * 1024,                                     # 容块在两次读之间长大
    views=(23, 18),
    cols={
        "code":         (str,  0,                  0),
        "name":         (str,  2,                  2),
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
    },
    check=_check_position,
    mode=live(b"0"),
    key=unique("code"),
)
# fmt: on


# ---------- 成交 ----------


def _check_fill(r):
    check_id(r)
    assert TIME_RE.fullmatch(r["time"]), f"成交时间 {r['time']!r}"
    assert r["status"] == "成交", f"{r['code']} 成交状态「{r['status']}」不认识：{r}"
    assert r["qty"] > 0 and r["price"] > 0 and r["fill_id"], f"成交行数值异常：{r}"
    assert math.isclose(
        r["price"] * r["qty"], r["amount"], rel_tol=1e-4, abs_tol=0.02
    ), f"{r['code']} 金额≠价×量：{r}"
    return r


# fmt: off
FILLS = table(
    "成交",
    anchor=b"||P|\r\n",
    back=40,
    head=rb"0\|(\d{8})\d{10}\|(?P<n>\d+)\|\|P\|\r\n",    # 标签 = 日期
    row=rb"\d\d:\d\d:\d\d\|\d{6}\|[^\x00\r\n]*\|\|\r\n", # 时间 + 6 位代码 + 行尾空列
    empty=False,
    slack=0,
    views=(17,),
    cols={
        "time":        (str,   0),
        "code":        (str,   1),
        "name":        (str,   3),
        "side":        (_side, 5),   # 列 4 是买卖标志
        "status":      (str,   6),
        "price":       (float, 7),
        "qty":         (float, 8),   # 股
        "amount":      (float, 9),   # 元
        "fill_id":     (str,   10),  # 成交编号
        "order_id":    (str,   11),  # 委托编号
        "market_flag": (int,   13),  # 与列 12 股东号同市场
    },
    check=_check_fill,
    mode=union(_today, uid=("fill_id", "order_id", "code"), order=("time", "fill_id")),
    key=tally({"traded": lambda r: r["qty"], "amount": lambda r: r["amount"]}),
)
# fmt: on
# 该只今日没有成交的合计（只读）
NO_FLOW = FILLS.key.blank()


# ---------- 挂单 ----------


def _check_order(r):
    check_id(r)
    assert TIME_RE.fullmatch(r["time"]), f"委托时间 {r['time']!r}"
    assert r["status"] and r["order_id"], f"挂单状态 / 委托编号为空：{r}"
    assert (
        r["price"] > 0 and r["qty"] > 0 and 0 <= r["filled"] < r["qty"]
    ), f"挂单量价异常：{r}"
    return r


# fmt: off
ORDERS = table(
    "挂单",
    anchor=b"||P|\r\n",
    back=12,
    head=rb"0\|\|(?P<n>\d+)\|\|P\|\r\n",                 # 标签 = n
    row=rb"\d\d:\d\d:\d\d\|\d{6}\|[^\r\n\x00]*\r\n",     # 时间 + 6 位代码
    empty=True,
    slack=0,
    views=(23,),
    cols={
        "time":        (str,   0),
        "code":        (str,   1),
        "name":        (str,   3),
        "side":        (_side, 5),   # 列 4 是买卖标志
        "kind":        (str,   6),   # 委托类别（信用交易 / …）
        "status":      (str,   7),   # 已报 / 部成 / …
        "price":       (float, 8),
        "qty":         (float, 9),   # 委托量（股）
        "order_id":    (str,   10),
        "filled":      (float, 12),  # 已成交量（股）
        "market_flag": (int,   16),
    },
    check=_check_order,
    mode=ASK,
    key=tally({"pending": lambda r: r["qty"] - r["filled"]}, uid="order_id"),
)
# fmt: on


# ---------- 融资负债 ----------


def _check_debt(r):
    check_id(r)
    assert r["qty"] >= 0 and r["amount"] >= 0, f"融资负债数值异常：{r}"
    return r


# fmt: off
DEBTS = table(
    "融资负债",
    anchor=b"||P|\r\n",
    back=12,
    head=rb"0\|\|(?P<n>\d+)\|\|P\|\r\n",                 # 标签 = n
    row=rb"\d+\|[^|\r\n\x00]*\|\d{6}\|[^\r\n\x00]*\r\n", # 沪深标志 + 交易所 + 6 位代码
    empty=True,
    slack=0,
    views=(15,),
    cols={
        "market_flag": (int,   0),
        "code":        (str,   2),
        "name":        (str,   4),
        "balance":     (float, 5),   # 持仓量
        "qty":         (float, 7),   # 当前负债数量（股）
        "amount":      (float, 9),   # 当前负债金额（元）
    },
    check=_check_debt,
    mode=ASK,
    key=unique("code"),
)
# fmt: on


# ---------- 信用资产 ----------


def _check_credit(r):
    assert r["k0"], f"信用资产行首组名称为空：{r}"
    return r


# fmt: off
CREDIT = table(
    "信用资产",
    anchor=b"|||\r\n",
    back=12,
    head=rb"0\|\|(?P<n>\d+)\|\|\|\r\n",                  # 标签 = n
    row=rb"[^\d|\r\n\x00][^|\r\n\x00]*\|[^\r\n\x00]*\r\n",  # 名称打头（标的表的行以数字打头）
    empty=False,
    slack=0,
    views=(11,),
    cols={
        "k0": (str, 0), "v0": (str, 1),
        "k1": (str, 3), "v1": (str, 4),
        "k2": (str, 6), "v2": (str, 7),
    },
    check=_check_credit,
    mode=ASK,
    key=pairs,
)
# fmt: on


# ---------- 标的证券 ----------


def _check_margin(r):
    check_id(r)
    assert r["ratio"] > 0, f"{r['code']} 融资保证金比例 {r['ratio']}"
    return r


# fmt: off
MARGIN = table(
    "标的证券",
    anchor=b"|||\r\n",
    back=16,
    head=rb"0\|\|(?P<n>\d+)\|\|\|\r\n",                  # 标签 = 总数
    row=rb"\d{6}\|[^\r\n\x00]*\r\n",                     # 6 位代码
    empty=False,
    slack=0,
    views=(14,),
    cols={
        "code":        (str,   0),
        "name":        (str,   2),
        "market_flag": (int,   3),
        "ratio":       (float, 5),   # 融资保证金比例
        "remark":      (str,   7),   # 融资标的,融券标的
        "status":      (str,   10),  # 融资状态
    },
    check=_check_margin,
    mode=WHOLE,
    key=unique("market_flag", "code"),
)
# fmt: on


# ---------- 担保品 ----------


def _check_collat(r):
    check_id(r)
    assert 0 <= r["rate"] <= 1, f"{r['code']} 担保折算率 {r['rate']}"
    return r


# fmt: off
COLLAT = table(
    "担保品",
    anchor=b"||0||P",
    back=1,
    head=rb"0\|\|0\|\|P(\d+)\|\r\n",                     # 标签 = 末页定位串
    row=rb"\d+\|[^|\r\n\x00]*\|\d{6}\|[^\r\n\x00]*\r\n", # 沪深标志 + 交易所 + 6 位代码
    empty=False,
    slack=0,
    views=(15,),
    cols={
        "market_flag": (int,   0),
        "code":        (str,   2),
        "name":        (str,   4),
        "rate":        (float, 5),   # 担保折算率
        "status":      (str,   6),   # 担保状态
        "remark":      (str,   12),
    },
    check=_check_collat,
    mode=WHOLE,
    key=unique("market_flag", "code"),
)
# fmt: on
