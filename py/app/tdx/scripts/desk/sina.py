# desk/sina — 新浪行情与 K 线：两个接口各一条 keep-alive 连接（Sina），全进程各只此一路请求。
# 行情（hq.sinajs.cn）一行一只：var hq_str_<sym>="名称,今开,昨收,现价,最高,最低,买一,卖一,量,额,
#   买1量,买1价,…,买5量,买5价,卖1量,卖1价,…,卖5量,卖5价,日期,时间,…"；量单位股。
# K 线（quotes.sina.cn）CN_MarketDataService.getKLineData：scale 周期（分钟）、datalen 根数，JSON 列表、旧在前。

import gzip
import http.client
import json
import threading
import time

from mem.common import BUY, SELL

HDR = {"Referer": "https://finance.sina.com.cn", "Accept-Encoding": "gzip"}
KLINE_KEYS = ("day", "open", "high", "low", "close", "volume")


class Sina:
    """keep-alive 长连接（复用省掉每次建连，约 58→31ms）：多线程共用一条，加锁串行。
    闲置超 IDLE 秒先重建，免得撞上对端已关的连接；请求失败照常抛出，连接最迟 IDLE 秒后重建。
    要 gzip（实测行情 786→374 字节、1500 根分钟线 200K→25K）。"""

    IDLE = 10

    def __init__(self, host):
        self.host = host
        self.lock = threading.Lock()
        self.conn, self.used = None, 0.0

    def get(self, path):
        with self.lock:
            if self.conn is None or time.monotonic() - self.used > self.IDLE:
                if self.conn:
                    self.conn.close()
                self.conn = http.client.HTTPConnection(self.host, timeout=3)
            self.conn.request("GET", path, headers=HDR)
            r = self.conn.getresponse()
            body = r.read()
            self.used = time.monotonic()
        enc = r.getheader("Content-Encoding")
        assert enc in (None, "gzip"), f"{self.host} 回了 {enc} 编码"
        return gzip.decompress(body) if enc else body


HQ = Sina("hq.sinajs.cn")
KQ = Sina("quotes.sina.cn")


def sym(x):
    return ("sz", "sh")[x["market_flag"]] + x["code"]


def _levels(f, at):
    """f[at:at+10] 的 5 档 (量, 价) → [(价, 股)]，略去空档。"""
    lv = [(float(f[at + 2 * i + 1]), float(f[at + 2 * i])) for i in range(5)]
    return [(p, v) for p, v in lv if p > 0 and v > 0]


def quotes(secs):
    """各只（带 code / market_flag）批量取行情 → {代码: {sym, price, last_close, open, high, low, volume,
    book, date, time}}，须每只都有。book = {方向: 该方向吃的那侧 5 档 [(价, 股)]}（买吃卖盘、卖吃买盘）。
    现价为 0（开盘前/停牌）时取昨收，当日盈亏即 0；今开 / 最高 / 最低此时为 0。"""
    if not secs:
        return {}
    codes = {sym(x): x["code"] for x in secs}
    body = HQ.get("/list=" + ",".join(codes)).decode("gbk")
    out = {}
    for line in body.split(";\n"):
        if not line.strip():
            continue
        k, v = line.split("=", 1)
        f = v.strip().strip('"').split(",")
        s = k.rsplit("_", 1)[1]
        assert s in codes and len(f) >= 32, f"{s} 行情不认识或字段不足：{v[:60]}"
        last = float(f[2])
        out[codes[s]] = {
            "sym": s,
            "price": float(f[3]) or last,
            "last_close": last,
            "open": float(f[1]),
            "high": float(f[4]),
            "low": float(f[5]),
            "volume": float(f[8]),
            "book": {BUY: _levels(f, 20), SELL: _levels(f, 10)},
            "date": f[30],
            "time": f[31],
        }
    assert set(out) == set(codes.values()
                           ), f"缺行情：{set(codes.values()) - set(out)}"
    return out


def kline(s, scale, n):
    """s 周期 scale 最近 n 根 → [(标号, 开, 高, 低, 收, 量)]（原样字符串，旧在前）。"""
    body = KQ.get(
        "/cn/api/json_v2.php/CN_MarketDataService.getKLineData"
        f"?symbol={s}&scale={scale}&ma=no&datalen={n}"
    )
    rows = json.loads(body)
    assert isinstance(rows, list) and rows, f"{s} K 线：{body[:80]!r}"
    return [tuple(r[k] for k in KLINE_KEYS) for r in rows]
