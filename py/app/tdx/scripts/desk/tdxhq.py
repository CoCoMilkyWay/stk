# desk/tdxhq — 通达信行情（客户端 connect.cfg 的行情主站）与 K 线：一条长连接（Tdx），全进程只此一路请求。
# 取新浪没有的通达信自编指数（880xxx 等），与 sina 同形：SRC、quotes(secs)、kline(s, kind, n)、VOL_BAND。
# 帧（小端）：请求 0c | 标识 u32 | 01 | 长 u16 ×2（自命令号起）| 命令号 u16 | 体；
#   应答 16 字节头（… | 压缩长 u16 | 原长 u16）+ 体，两长不等即 zlib。
#   价为变长整数（_int）：首字节低 6 位、0x40 负、0x80 有续，续字节各 7 位；量 / 额为通达信 4 字节浮点（_float）。
# 建连先两步握手（HELLO，抓自客户端，含券商名与客户端版本；不需账户，不握手的会话查什么都回空）。之后：
#   0x0004 服务器当前交易日 u32 YYYYMMDD；
#   0x053e 行情：各只 市场 u8 + 代码 6s → 现价 ×100、昨收 / 今开 / 最高 / 最低与现价之差、…、额（指数没有盘口）；
#   0x052d K 线：周期 KIND、自最新往前跳 start 根取 count 根（一页至多 PAGE），旧在前；×1000，
#     各根开为与上根收之差（首根自 0 起）、收 / 高 / 低为与本根开之差；指数每根另带涨跌家数。
# 量一律取成交额：指数分钟量与日线量口径不一（差十余倍且不定），分钟额逐日合计与日线额只差浮点舍入。
# 行情不带时刻（指数那格不是时刻），time 为空。

import configparser
import os
import socket
import struct
import threading
import time
import zlib

from mem.common import BUY, MARKETS, SELL

from . import ROOT

SRC = "tdx"
CFG = os.path.join(ROOT, "gjzq", "connect.cfg")
HELLO = (
    bytes.fromhex("0c0218940001030003000d0002"),
    bytes.fromhex(
        "0c031899000120002000db0fb9fabdf0d6a4c8af0000000ad72741040000000000000000000000000005"
    ),
)
MKT = {"sz": 0, "sh": 1, "bj": 2}
PAGE = 800
QUOTE_MAX = 80
VOL_BAND = (1 - 1e-6, 1 + 1e-6)


def _host():
    """connect.cfg [HQHOST] 的主站 (地址, 端口)。"""
    c = configparser.ConfigParser(interpolation=None, strict=False)
    assert c.read(CFG, encoding="gbk"), f"没有 {CFG}"
    h = c["HQHOST"]
    i = int(h["PrimaryHost"]) + 1
    return h[f"IPAddress{i:02d}"], int(h[f"Port{i:02d}"])


class Tdx:
    """长连接：多线程共用一条，加锁串行。闲置超 IDLE 秒先重建（重新握手），免得撞上对端已关的连接；
    请求失败照常抛出，半途断掉的连接作废、下次重建（帧没读完，不能续用）。"""

    IDLE = 10

    def __init__(self):
        self.lock = threading.Lock()
        self.sock, self.used = None, 0.0

    def _read(self, n):
        b = b""
        while len(b) < n:
            c = self.sock.recv(n - len(b))
            assert c, "通达信行情连接被对端关了"
            b += c
        return b

    def _call(self, pkg):
        self.sock.sendall(pkg)
        *_, zs, us = struct.unpack("<IIIHH", self._read(16))
        body = self._read(zs)
        return zlib.decompress(body) if zs != us else body

    def call(self, cmd, body):
        n = len(body) + 2
        pkg = struct.pack("<BIBHHH", 0x0C, 0, 1, n, n, cmd) + body
        with self.lock:
            stale = self.sock is None or time.monotonic() - self.used > self.IDLE
            self.used = 0.0
            if stale:
                if self.sock:
                    self.sock.close()
                self.sock = socket.create_connection(_host(), timeout=3)
                for h in HELLO:
                    self._call(h)
            out = self._call(pkg)
            self.used = time.monotonic()
        return out


TQ = Tdx()


def _int(d, p):
    b = d[p]
    v, neg, sh = b & 0x3F, b & 0x40, 6
    while b & 0x80:
        p += 1
        b = d[p]
        v += (b & 0x7F) << sh
        sh += 7
    return (-v if neg else v), p + 1


def _ints(d, p, n):
    out = []
    for _ in range(n):
        v, p = _int(d, p)
        out.append(v)
    return out, p


def _float(i):
    """通达信 4 字节浮点：首字节为阶，其余三字节为尾数（次高字节高位置位时尾数另倍）。"""
    e = (i >> 24) * 2
    hi, mid, lo = (i >> 16) & 0xFF, (i >> 8) & 0xFF, i & 0xFF
    a = 2.0 ** (e - 0x7F)
    if hi > 0x80:
        b = 2.0 ** (e - 0x86) * 128 + (hi & 0x7F) * 2.0 ** (e - 0x85)
    else:
        b = 2.0 ** (e - 0x86) * hi
    k = 2 if hi & 0x80 else 1
    return a + b + k * (2.0 ** (e - 0x8E) * mid + 2.0 ** (e - 0x96) * lo)


def _code(s):
    return struct.pack("<B6s", MKT[s[:2]], s[2:].encode())


def sym(x):
    return MARKETS[x["market_flag"]] + x["code"]


def _date():
    d = TQ.call(0x0004, b"")
    assert len(d) == 10, f"交易日回包 {d.hex()}"
    (x,) = struct.unpack_from("<I", d, 6)
    return f"{x // 10000}-{x // 100 % 100:02d}-{x % 100:02d}"


def quotes(secs):
    """各只（带 code / market_flag）批量取行情 → {代码: {src, sym, price, last_close, open, high, low, volume,
    book, date, time}}，须每只都有。volume 为成交额，book 两侧皆空，date 为服务器当前交易日，time 空。
    现价为 0（开盘前/停牌）时取昨收。"""
    if not secs:
        return {}
    assert len(secs) <= QUOTE_MAX, f"一次至多 {QUOTE_MAX} 只，给了 {len(secs)}"
    syms = {x["code"]: sym(x) for x in secs}
    d = TQ.call(
        0x053E,
        struct.pack("<HIHH", 5, 0, 0, len(secs)) + b"".join(map(_code, syms.values())),
    )
    date = _date()
    (n,) = struct.unpack_from("<H", d, 2)
    p, out = 4, {}
    for _ in range(n):
        _, code, _ = struct.unpack_from("<B6sH", d, p)
        v, p = _ints(d, p + 9, 9)
        (amt,) = struct.unpack_from("<I", d, p)
        # 内外盘与保留 4 项、5 档 × (买, 卖, 买量, 卖量)、保留 u16 + 4 项 + u32：指数这段不是盘口，跳过
        _, p = _ints(d, p + 4, 24)
        _, p = _ints(d, p + 2, 4)
        p += 4
        px, last = v[0], v[0] + v[1]
        c = code.decode()
        out[c] = {
            "src": SRC,
            "sym": syms[c],
            "price": (px or last) / 100,
            "last_close": last / 100,
            "open": (px + v[2]) / 100,
            "high": (px + v[3]) / 100,
            "low": (px + v[4]) / 100,
            "volume": _float(amt),
            "book": {BUY: [], SELL: []},
            "date": date,
            "time": "",
        }
    assert p == len(d), f"行情回包 {len(d)} 字节，解到 {p}"
    assert set(out) == set(syms), f"缺行情：{set(syms) - set(out)}"
    return out


def _day(d, p):
    (x,) = struct.unpack_from("<I", d, p)
    return f"{x // 10000}-{x // 100 % 100:02d}-{x % 100:02d}"


def _minute(d, p):
    """当日上午最后一根标作 13:00（历史日为 11:30），统一成 11:30。"""
    day, m = struct.unpack_from("<HH", d, p)
    y, md, m = (day >> 11) + 2004, day % 2048, 690 if m == 780 else m
    return f"{y}-{md // 100:02d}-{md % 100:02d} {m // 60:02d}:{m % 60:02d}:00"


# 种类 → (周期, 标号解法)
KIND = {"day": (4, _day), "min": (8, _minute)}


def _page(s, kind, start, count):
    """s 周期 kind 自最新往前跳 start 根的 count 根 → [(标号, 开, 高, 低, 收, 额)]（旧在前）。"""
    cat, stamp = KIND[kind]
    d = TQ.call(
        0x052D,
        struct.pack(
            "<H6sHHHHIIH", MKT[s[:2]], s[2:].encode(), cat, 1, start, count, 0, 0, 0
        ),
    )
    (n,) = struct.unpack_from("<H", d)
    p, prev, out = 2, 0, []
    for _ in range(n):
        t = stamp(d, p)
        (o, c, h, l), p = _ints(d, p + 4, 4)
        (amt,) = struct.unpack_from("<I", d, p + 4)
        p += 12
        o += prev
        prev = o + c
        out.append(
            (t, o / 1000, (o + h) / 1000, (o + l) / 1000, (o + c) / 1000, _float(amt))
        )
    assert p == len(d), f"{s} K 线回包 {len(d)} 字节，解到 {p}（不是指数？）"
    return out


def kline(s, kind, n):
    """s 周期 kind 最近 n 根 → [(标号, 开, 高, 低, 收, 额)]（旧在前），一页 PAGE 根往前翻；
    翻页之间新出了一根即前一页与已取的错开一根，按标号去重。"""
    rows = []
    while len(rows) < n:
        k = min(PAGE, n - len(rows))
        page = _page(s, kind, len(rows), k)
        rows = page + [b for b in rows if not page or b[0] > page[-1][0]]
        if len(page) < k:
            break
    assert rows, f"{s} K 线为空"
    return rows
