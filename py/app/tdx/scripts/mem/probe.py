#!/usr/bin/env python3
# mem/probe — 探内存布局（只读）：给新表找块、定列。
#   无参数  列出堆里全部明文表（首行 0|…|，\r\n 分行、| 分列、GBK）：址 / 表头 / 行数 / 列数 / 前几行，
#           同表头同首行的副本只列一次并计数；
#   带参数  逐个在堆里找该文本（GBK、UTF-16LE 两种编码），列出各处址与前后字节（GBK 解码，控制字符转义）。
# 先在客户端点过要探的那页查询（如信用资产、融资标的、担保品），再跑。
#
# 用法：python3 scripts/mem/probe.py [文本 ...] > probe.txt

import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from mem import common  # noqa: E402

TABLE_RE = re.compile(
    rb"0\|[^\r\n\x00]{0,200}\|\r\n(?:[^\r\n\x00]*\|[^\r\n\x00]*\r\n){1,5000}"
)
SHOW_ROWS = 3
CONTEXT = 300  # 文本命中前后各取的字节数


def _esc(raw, enc="gbk"):
    s = raw.decode(enc, errors="replace")
    return "".join(
        c if c.isprintable() else f"\\x{ord(c):02x}" if ord(c) < 256 else c for c in s
    )


def tables(pid):
    seen = {}
    for lo, data in common.iter_regions(pid):
        for m in TABLE_RE.finditer(data):
            lines = m.group().split(b"\r\n")[:-1]
            key = (lines[0], lines[1])
            if key in seen:
                seen[key][1] += 1
                continue
            seen[key] = [lo + m.start(), 1, lines]
    for addr, n, lines in seen.values():
        cols = sorted({ln.count(b"|") + 1 for ln in lines[1:]})
        print(
            f"== {addr:#x}  副本 {n}  行 {len(lines) - 1}  列 {cols}  表头 {_esc(lines[0])}"
        )
        for ln in lines[1 : 1 + SHOW_ROWS]:
            print(
                "   "
                + " | ".join(f"{i}:{c}" for i, c in enumerate(_esc(ln).split("|")))
            )


def texts(pid, words):
    pats = [(w, enc, w.encode(enc)) for w in words for enc in ("gbk", "utf-16-le")]
    for lo, data in common.iter_regions(pid):
        for w, enc, p in pats:
            i = data.find(p)
            while i != -1:
                a, b = max(i % 2, i - CONTEXT), min(len(data), i + len(p) + CONTEXT)
                print(f"== {w!r} {enc} @ {lo + i:#x}\n{_esc(data[a:b], enc)}\n")
                i = data.find(p, i + 1)


def main():
    pid = common.attach()
    if len(sys.argv) > 1:
        texts(pid, sys.argv[1:])
    else:
        tables(pid)


if __name__ == "__main__":
    main()
