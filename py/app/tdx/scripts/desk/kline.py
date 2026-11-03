# desk/kline — K 线（新浪，本地落盘）。
# 一根一项 (标号, 开, 高, 低, 收, 量)（价 3 位小数、量单位股），旧在前。日线 Day / 分钟线 Min 走同一套流程（KCache），
# 差异全在两个类的同名成员：
#   scale 新浪周期（分钟）  fmt 标号格式  per 每个交易日根数  keep 保留交易日数  cap 单次取数上限
#   settle 定稿延迟（秒）：标号不晚于「此刻 − settle」的不再变（日线即今日以前，分钟线即 90 秒以前）
#   slot(t) 标号 t 是当日第几根  valid(t) 标号合法  tail(q) 由行情合成的进行中那根
#   close(d) 日期 d 的最后一根标号（定稿线到了即收完）  verify 落盘前核对
# 新浪（实测）：1 分钟线按 09:31 … 11:30、13:01 … 15:00 标号，无成交的分钟没有这根（一天约 237~238 根），
# 刚过去的那根还会被修正；单次至多 1970 根；日线盘中不含当日。
# 落盘 DATA_DIR/<kind>/<sym>.csv（time,open,high,low,close,volume）：只含收完的日子，变了才整写；
# 各 kind 目录下只留 <sym>.csv，其余随启动删掉。未定稿的只在内存；超出保留期的随写入删掉。盘口不留历史。
# 取数只追不改：
#   该取：定稿线又越过了一根（gap(上次定稿线, 此刻定稿线) > 0）：日线每个交易日后至多一次，
#         分钟线每分钟至多一次，午休、盘后、周末不取。
#   增量：取「已定稿最后一根之后至今的根数上界（gap，节假日只会多估）+ OVERLAP」根，
#         重叠部分须与已定稿逐根一致才接上（无缝、未改）；没有已定稿、盘上不足保留期（本进程首取时）
#         或上界超出单次上限即取满上限，掐掉最早那个可能被截断的日子，早于已定稿的补在前，
#         接不上已定稿就以新取的为准。
# 完整性：标号严格递增且合法、OHLC 自洽（取到的、读盘的都核）；分钟线落盘前逐日与当日日线核对
#   开 / 高 / 低 / 收 恰等、量在 [VOL_FLOOR, 1] × 日线量（实测深市恰等，沪市分钟合计少 0.05%~0.3%）。
# 今日日线由行情合成（今开 / 最高 / 最低 / 现价 / 量，同日线口径）；进行中的分钟线前端随持仓帧用现价改写。

import datetime
import itertools
import os
import re
import shutil
import threading
import time

from . import ROOT, sina

DATA_DIR = os.path.join(ROOT, "data", "kline")
CSV_HEAD = "time,open,high,low,close,volume\n"
CSV_NAME = re.compile(r"(sh|sz)\d{6}\.csv")
OVERLAP = 3
VOL_FLOOR = 0.95


def _wd(d):
    return datetime.date.fromisoformat(d).weekday() < 5


def _weekdays(d0, d1):
    """日期 d0、d1 之间（不含两端）的工作日数。"""
    a, b = (datetime.date.fromisoformat(d) for d in (d0, d1))
    return sum(
        (a + datetime.timedelta(i)).weekday() < 5 for i in range(1, (b - a).days)
    )


def _bar(t, o, h, l, c, v):
    return (t, *(round(float(x), 3) for x in (o, h, l, c)), int(float(v)))


def _check(what, bars, kind):
    """标号严格递增且合法、OHLC 自洽；原样返回。"""
    for a, b in zip(bars, bars[1:]):
        assert a[0] < b[0], f"{what} K 线标号不递增：{a[0]} → {b[0]}"
    for t, o, h, l, c, v in bars:
        assert kind.valid(t), f"{what} {kind.name} 标号 {t} 不合法"
        assert l <= min(o, c) and max(o, c) <= h and v >= 0, f"{what} {t} OHLC 不自洽"
    return bars


def _load(path, kind):
    """读一个 CSV 并校验；没有文件回 []。"""
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        assert f.readline() == CSV_HEAD, f"{path} 表头不对"
        return _check(path, [_bar(*ln.rstrip("\n").split(",")) for ln in f], kind)


def _save(path, bars):
    """整写：先写临时文件再改名，中途断掉不留半个文件。"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path + ".tmp", "w", encoding="utf-8") as f:
        f.write(CSV_HEAD)
        f.writelines(
            f"{t},{o:.3f},{h:.3f},{l:.3f},{c:.3f},{v}\n" for t, o, h, l, c, v in bars
        )
    os.replace(path + ".tmp", path)


def _splice(sym, old, new, full):
    """已定稿的 old 接上新取的 new：重叠部分须逐根一致，new 早于 old 的补在前、晚于 old 的接在后；
    new 是取满上限的（full）且够不着 old 即以 new 为准。"""
    if not old:
        return new
    first, last = old[0][0], old[-1][0]
    lap = [b for b in new if first <= b[0] <= last]
    if not lap:
        assert (
            full
        ), f"{sym} 新取 {len(new)} 根与已有无重叠（最早 {new[0][0]}，已有到 {last}）"
        return new
    tail = [b for b in old if b[0] >= lap[0][0]]
    assert lap == tail, f"{sym} 已定稿的 K 线被改过：{tail} → {lap}"
    return [b for b in new if b[0] < first] + old + [b for b in new if b[0] > last]


def _verify(sym, day, g, k):
    """收完的一天分钟线 g 与当日日线 k 核对：开 / 高 / 低 / 收 恰等，量在 [VOL_FLOOR, 1] × 日线量。"""
    assert k, f"{sym} 日线缺 {day}，分钟线无从核对"
    ohlc = (g[0][1], max(b[2] for b in g), min(b[3] for b in g), g[-1][4])
    assert ohlc == k[1:5], f"{sym} {day} 分钟线合成 {ohlc} ≠ 日线 {k[1:5]}"
    v = sum(b[5] for b in g)
    assert VOL_FLOOR * k[5] <= v <= k[5], f"{sym} {day} 分钟线量 {v} 与日线 {k[5]} 不符"


class Kind:
    """两种 K 线共用的推算与读写：标号、定稿线、两标号间至多几根、取数、盘上一个代码一个文件。"""

    def path(self, sym):
        return os.path.join(DATA_DIR, self.name, sym + ".csv")

    def load(self, sym):
        return _load(self.path(sym), self)

    def save(self, sym, bars):
        _save(self.path(sym), bars)

    def label(self, t):
        return time.strftime(self.fmt, time.localtime(t))

    def final(self, t):
        return self.label(t - self.settle)

    def gap(self, a, b):
        """标号 a 之后、b 及以前至多有几根：工作日每日 per 根，日内按 slot 计。"""
        da, db = a[:10], b[:10]
        if da == db:
            return (self.slot(b) - self.slot(a)) * _wd(da)
        return (
            (self.per - self.slot(a)) * _wd(da)
            + self.per * _weekdays(da, db)
            + self.slot(b) * _wd(db)
        )

    def fetch(self, sym, n):
        """取 n 根并校验。"""
        return _check(sym, [_bar(*r) for r in sina.kline(sym, self.scale, n)], self)


class Day(Kind):
    name, scale, fmt = "day", 240, "%Y-%m-%d"
    # cap：盘后新浪日线含当日，比 keep 多取几根
    per, keep, cap, settle = 1, 360, 370, 86400

    def slot(self, t):
        return 1

    def valid(self, t):
        return re.fullmatch(r"\d{4}-\d\d-\d\d", t) is not None

    def tail(self, q):
        """今日那根由行情合成；未开盘没有。"""
        bar = (q["date"], q["open"], q["high"], q["low"], q["price"], q["volume"])
        return _bar(*bar) if q["open"] > 0 else None

    def close(self, d):
        return d

    def verify(self, sym, bars, daily):
        pass


class Min(Kind):
    name, scale, fmt = "min", 1, "%Y-%m-%d %H:%M:%S"
    # cap：新浪单次上限（实测 1970 可、1980 回 null），约 8 个交易日
    per, keep, cap, settle = 240, 7, 1970, 90

    def slot(self, t):
        """当日到此为止的根数（09:31 = 1 … 11:30 = 120、13:01 = 121 … 15:00 = 240）。"""
        m = int(t[11:13]) * 60 + int(t[14:16])
        return min(max(m - 570, 0), 120) + min(max(m - 780, 0), 120)

    def valid(self, t):
        hm = t[11:16]
        return re.fullmatch(r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d", t) is not None and (
            "09:31" <= hm <= "11:30" or "13:01" <= hm <= "15:00"
        )

    def tail(self, q):
        return None

    def close(self, d):
        return d + " 15:00:00"

    def verify(self, sym, bars, daily):
        """逐日与日线 daily() 核对。"""
        k = daily()
        for day, g in itertools.groupby(bars, key=lambda b: b[0][:10]):
            _verify(sym, day, list(g), k.get(day))


DAY, MIN = Day(), Min()
KINDS = {k.name: k for k in (DAY, MIN)}


def _prune():
    """各 kind 目录下只留 <sym>.csv。"""
    for kind in KINDS.values():
        d = os.path.join(DATA_DIR, kind.name)
        for name in os.listdir(d) if os.path.isdir(d) else ():
            p = os.path.join(d, name)
            if os.path.isdir(p):
                shutil.rmtree(p)
            elif not CSV_NAME.fullmatch(name):
                os.remove(p)


class KCache:
    """以盘上为准，内存多出未定稿的；取数与读写盘串行（一把锁，分钟线落盘核对时重入取日线）。"""

    def __init__(self):
        self.lock = threading.RLock()
        # (种类, sym) → {bars, final：此标号及以前已定稿，None = 只读了盘、还没取过；saved：盘上的}
        self.series = {}
        _prune()

    def _grow(self, kind, sym, s, now, q):
        """增量取数接上已定稿部分，按保留期裁剪，收完的日子变了即核对、落盘。须持 lock。"""
        fin = (
            s["bars"]
            if s["final"] is None
            else [b for b in s["bars"] if b[0] <= s["final"]]
        )
        short = s["final"] is None and len({b[0][:10] for b in fin}) < kind.keep
        n = (
            kind.gap(fin[-1][0], kind.label(now)) + OVERLAP
            if fin and not short
            else kind.cap
        )
        full = n >= kind.cap
        new = kind.fetch(sym, min(n, kind.cap))
        if full:
            new = [b for b in new if b[0][:10] > new[0][0][:10]]
            assert new, f"{sym} 取满 {kind.cap} 根仍不够一天"
        bars = _splice(sym, fin, new, full)
        today = kind.label(now)[:10]
        past = sorted({b[0][:10] for b in bars if b[0][:10] < today})[-kind.keep :]
        s["bars"] = [b for b in bars if b[0][:10] >= (past[0] if past else today)]
        s["final"] = kind.final(now)
        done = [b for b in s["bars"] if kind.close(b[0][:10]) <= s["final"]]
        if done != s["saved"]:
            kind.verify(sym, done, lambda: {b[0]: b for b in self._live(DAY, sym, q)})
            kind.save(sym, done)
            s["saved"] = done

    def _live(self, kind, sym, q):
        """盘上的 + 增量取到的（定稿线又越过一根才取）+ 行情合成的进行中那根。须持 lock。"""
        s = self.series.get((kind.name, sym))
        if s is None:
            bars = kind.load(sym)
            s = self.series[kind.name, sym] = {
                "bars": bars,
                "final": None,
                "saved": bars,
            }
        now = time.time()
        if s["final"] is None or kind.gap(s["final"], kind.final(now)) > 0:
            self._grow(kind, sym, s, now, q)
        t = kind.tail(q)
        return s["bars"] + [t] if t and t[0] > s["bars"][-1][0] else s["bars"]

    def bars(self, kind, sym, q, n):
        """近 n 个交易日（有今日即含今日）。"""
        assert 1 <= n <= kind.keep, f"{kind.name} 天数 {n}，可选 1~{kind.keep}"
        with self.lock:
            bars = self._live(kind, sym, q)
        keep = sorted({b[0][:10] for b in bars})[-n:]
        return [b for b in bars if b[0][:10] >= keep[0]]


KCACHE = KCache()
