#!/usr/bin/env python3
# mem/fills — 内存方案：当日成交明文表（customize，非通用基建）。
# 客户端查过一次「当日成交」后，明文驻留堆里。表头 0|<日期 8 位><10 位>|<n>||P|，
# 其后 n 行、每行一笔成交：\r\n 分行、| 分列、GBK 编码，列序见 COLS。
#
# 选块（实测）：每次查询在网络缓冲与客户端各留几份副本，旧查询的副本不即时释放，且没有活动块标记。
# 成交只增不改，故不选块：当日全部完整副本按成交编号取并集即全量（旧副本是其子集），同号须逐字段一致。
# 换代检测：副本地址集变了即客户端重查过（内容可能不变）；没有活动块可盯，follow 即全量 read（协议见 common）。
#
# 完整性：只认表头日期为本机今日、且后随至少 n 行的副本（被新内容覆盖了一半的旧缓冲不算），只取前 n 行；
# 行级校验列数、时间、代码、沪深标志、方向、状态、价 × 量 ≈ 金额，任一不过即 assert。

import re
import time
from collections import namedtuple

from . import common
from .common import BUY, SELL, SIDES

# 快照：addrs = 当日完整副本地址集（今日没查过即空），rows = 并集，按成交时间、成交编号排序；
# flow = rows 按代码合计 {代码: {code, name, market_flag, qty: {方向: 股}, amount: {方向: 元}}}。
Fills = namedtuple("Fills", "pid addrs rows flow")

ANCHOR = b"||P|\r\n"
HEAD_RE = re.compile(rb"0\|(\d{8})\d{10}\|(\d+)\|\|P\|\r\n")
HEAD_MAX = 40  # 表头最长字节数（锚点往前回溯的窗口）
ROW_RE = re.compile(rb"\d\d:\d\d:\d\d\|\d{6}\|[^\x00\r\n]*\|\|")  # 选块用的整行形状，细校验在 _fill
ROW_MAX = 512
NF = 17  # 15 列 + 行尾两个空列
TIME_RE = re.compile(r"\d\d:\d\d:\d\d")

# 字段 → (转型, 列号)
# fmt: off
COLS = {
    "time":        (str,   0),
    "code":        (str,   1),
    "name":        (str,   3),   # 列 2 对 ETF 是短码名（如 5GETF），列 3 是全称（同持仓表）
    "market_flag": (int,   4),
    "side":        (str,   5),
    "status":      (str,   6),
    "price":       (float, 7),
    "qty":         (float, 8),   # 股
    "amount":      (float, 9),   # 元
    "fill_id":     (str,   10),  # 成交编号
    "order_id":    (str,   11),  # 委托编号
}
# fmt: on
SIDE_TEXT = {"买入": BUY, "卖出": SELL}


def _blocks(lo, data, day):
    """data（起址 lo）里表头日期为 day 的完整副本 → (表头地址, 前 n 行 bytes 列表)。"""
    i = data.find(ANCHOR)
    while i != -1:
        end = i + len(ANCHOR)
        m = HEAD_RE.search(data, max(0, i - HEAD_MAX), end)
        if m and m.end() == end and m.group(1) == day:
            n = int(m.group(2))
            lines = data[end : end + ROW_MAX * n].split(b"\r\n")[:n]
            if len(lines) == n and all(ROW_RE.fullmatch(ln) for ln in lines):
                yield lo + m.start(), lines
        i = data.find(ANCHOR, end)


def _fill(line):
    """一行 → 成交；按 COLS 取列转型、逐项校验，不过即 assert。side 换成方向 BUY / SELL。"""
    text = line.decode("gbk", errors="replace")
    assert "\ufffd" not in text, f"成交行含 GBK 替换字符，疑似读到写入中途：{text!r}"
    f = text.split("|")
    assert len(f) == NF, f"成交行 {len(f)} 列（应 {NF}），布局已变：{text!r}"
    r = {k: t(f[i]) for k, (t, i) in COLS.items()}
    assert TIME_RE.fullmatch(r["time"]), f"成交时间 {r['time']!r}"
    assert re.fullmatch(r"\d{6}", r["code"]), f"成交代码 {r['code']!r}"
    assert r["market_flag"] in (0, 1), f"{r['code']} 沪深标志 {r['market_flag']}"
    assert r["side"] in SIDE_TEXT, f"{r['code']} 买卖方向「{r['side']}」不认识：{text!r}"
    assert r["status"] == "成交", f"{r['code']} 成交状态「{r['status']}」不认识：{text!r}"
    assert r["qty"] > 0 and r["price"] > 0 and r["fill_id"], f"成交行数值异常：{text!r}"
    exp = r["price"] * r["qty"]
    assert abs(exp - r["amount"]) <= max(0.02, exp * 1e-4), f"{r['code']} 金额≠价×量：{text!r}"
    r["side"] = SIDE_TEXT[r["side"]]
    return r


def _flow(rows):
    """成交按代码合计 → {代码: {code, name, market_flag, qty: {方向: 股}, amount: {方向: 元}}}；同代码沪深标志须一致。"""
    out = {}
    for r in rows:
        a = out.setdefault(
            r["code"],
            {k: r[k] for k in ("code", "name", "market_flag")}
            | {"qty": dict.fromkeys(SIDES, 0.0), "amount": dict.fromkeys(SIDES, 0.0)},
        )
        assert a["market_flag"] == r["market_flag"], f"成交 {r['code']} 沪深标志不一致"
        a["qty"][r["side"]] += r["qty"]
        a["amount"][r["side"]] += r["amount"]
    return out


def read(pid=None):
    """全量扫一遍 → Fills。同一笔（成交编号 + 委托编号 + 代码）在各副本里须逐字段一致。"""
    pid = common.attach(pid)
    day = time.strftime("%Y%m%d").encode()
    addrs, uniq = set(), {}
    for lo, data in common.iter_regions(pid):
        for addr, lines in _blocks(lo, data, day):
            addrs.add(addr)
            for f in map(_fill, lines):
                k = (f["fill_id"], f["order_id"], f["code"])
                assert uniq.setdefault(k, f) == f, f"成交 {k} 两份副本不一致：{uniq[k]} ≠ {f}"
    rows = sorted(uniq.values(), key=lambda f: (f["time"], f["fill_id"]))
    return Fills(pid, frozenset(addrs), rows, _flow(rows))


def follow(snap):
    """没有活动块可盯：全量重读，副本地址集变了即换代。"""
    return read(snap.pid)
