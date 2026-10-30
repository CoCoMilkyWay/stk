#!/usr/bin/env python3
# console.py — 统一入口：shim 接口控制台（Web 版，stdlib http.server，无三方依赖）。
# 启动时自动 ensure（bridge.shimctl 编译 + 注入 shim 到 Wine 里的 TdxW.exe，若未在线），再起服务。
# 非入口的支撑代码都在包里：bridge/（消息桥）、mem/（读内存方案）。
#
# 启动：python3 scripts/console.py [port]      默认 8765
# 浏览器：http://127.0.0.1:8765
#
# 提供的 API：
#   GET  /                                   控制台页面
#   GET  /api/ping                           {pid}（离线为 null）
#   GET  /api/watch?s=路,路                  SSE：所列各路合成一条连接，每条 {s 路名, r 记录}，r 为 {} 是该路心跳。路：
#          tree  shim treewatch 原样转发（窗口树版本号）
#          gdi   shim gdiwatch 原样转发（窗口绘制层）
#          pos   持仓页帧（内存持仓 × 内存当日成交 × 新浪实时行情含 5 档，每只一行含盘口列），变了才推
#          ord   最近各单状态（逐步）
#          rec   录制器 {st 状态, recs 新记录}
#   POST /api/kline      {code,kind,days}    持仓页行情里该只近 days 个交易日的 K 线（新浪，本地落盘）：kind = min 分钟 / day 日线
#   POST /api/shim            {cmd,args}     透传 shim 命令，回 JSON 记录列表（命令表见 shim.c）
#   POST /api/clicktext {text,class?,title?} 点可见窗口上画着 text 的按钮（须唯一）
#   POST /api/refresh                        让客户端重查当日成交与持仓，等内存换代后重读，新帧经 pos 推送
#   POST /api/rec             {on}           开 / 停录制
#   POST /api/recmark         {text}         录制中打点
#   POST /api/order {code,s,size,px,credit}  持仓行闪电下单（后台跑），回该单状态；s = 1 买 / -1 卖
# 浏览器对同一地址至多 6 条 HTTP/1.1 连接，长连接一律走 /api/watch 合路，留出连接给请求。
# 任何失败（assert 等）统一回 {err}。

import os
import re
import sys
import gzip
import json
import queue
import datetime
import time
import types
import urllib.parse
import itertools
import threading
import collections
import subprocess
import http.client
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from bridge.shimctl import Shim, ensure  # noqa: E402  消息桥（编译/注入/控制通道）
from mem import common as memcommon  # noqa: E402  内存方案协议（read / follow / wait）
from mem import positions as mempos  # noqa: E402  内存持仓方案（含完整性校验）
from mem import fills as memfills  # noqa: E402  内存当日成交方案
from mem.common import BUY, SELL, SIDES  # noqa: E402  方向即符号

HTML_FILE = os.path.join(HERE, "console.html")
REFRESH_BTN = {"class": "MHPToolBar", "title": "MainViewBar", "text": "刷新"}
POS_BTN = {
    **REFRESH_BTN,
    "text": "持仓",
}  # 刷新只刷当前页，下单后客户端会切去委托 / 成交页
FILLS_BTN = {**REFRESH_BTN, "text": "成交"}
UI = threading.Lock()  # 客户端界面自动化（刷新点按钮 / 下单走闪电窗）一次只许一路
SHIM = Shim()


# ---------- 按绘制文字点按钮 ----------


def click_text(b):
    """在 class/title（给了才比）匹配的可见窗口的绘制层里找文字恰为 text 的项，
    向该窗口投递其中心点的左键单击（自绘控件谁画谁处理点击）。须唯一命中。"""
    want = {k: b[k] for k in ("class", "title") if b.get(k)}
    wins = {
        n["hwnd"]: n
        for n in SHIM("tree")
        if n["vis"] and all(n[k] == v for k, v in want.items())
    }
    hits = [
        (wins[d["hwnd"]], d["off"], it)
        for d in SHIM("gdidump")
        if d["hwnd"] in wins
        for it in d["items"]
        if len(it) > 5 and it[5].strip() == b["text"]
    ]
    assert len(hits) == 1, f"{b} 命中 {len(hits)} 处，需唯一（加 class/title）"
    n, off, (l, t, r, bt, *_) = hits[0]
    x = n["rect"][0] + off[0] + (l + r) // 2
    y = n["rect"][1] + off[1] + (t + bt) // 2
    return SHIM("clickat", n["hwnd"], x, y)


# ---------- 推送源 ----------
# pos / ord / rec 三路同一写法：源每变一次 bump 一个版本，订阅者每见新版推 view(上次所见版本)，
# HEARTBEAT 秒无新版推 {}（顺带让断开的订阅者及时退出）。

HEARTBEAT = 15


class Feed:
    def __init__(self, view):
        self.cond = threading.Condition()  # 兼管源自身的状态：view 在其内求值
        self.seq, self.view = 0, view

    def bump(self):
        with self.cond:
            self.seq += 1
            self.cond.notify_all()

    def watch(self):
        seen = -1
        while True:
            with self.cond:
                fresh = self.cond.wait_for(lambda: self.seq != seen, HEARTBEAT)
                r = self.view(seen) if fresh else {}
                seen = self.seq
            yield json.dumps(r, ensure_ascii=False)


# ---------- 行情（新浪）----------
# 一行一只：var hq_str_<sym>="名称,今开,昨收,现价,最高,最低,买一,卖一,量,额,
#   买1量,买1价,…,买5量,买5价,卖1量,卖1价,…,卖5量,卖5价,日期,时间,…"；量单位股。

SINA_HDR = {"Referer": "https://finance.sina.com.cn", "Accept-Encoding": "gzip"}


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
            self.conn.request("GET", path, headers=SINA_HDR)
            r = self.conn.getresponse()
            body = r.read()
            self.used = time.monotonic()
        enc = r.getheader("Content-Encoding")
        assert enc in (None, "gzip"), f"{self.host} 回了 {enc} 编码"
        return gzip.decompress(body) if enc else body


SINA = Sina("hq.sinajs.cn")


def _sym(x):
    return ("sz", "sh")[x["market_flag"]] + x["code"]


def _levels(f, at):
    """f[at:at+10] 的 5 档 (量, 价) → [(价, 股)]，略去空档。"""
    lv = [(float(f[at + 2 * i + 1]), float(f[at + 2 * i])) for i in range(5)]
    return [(p, v) for p, v in lv if p > 0 and v > 0]


def quotes(secs):
    """各只（带 code / market_flag）批量取行情 → {代码: {sym, price, last_close, open, high, low, volume, bid, ask, date, time}}，
    须每只都有。现价为 0（开盘前/停牌）时取昨收，当日盈亏即 0；今开 / 最高 / 最低此时为 0。"""
    if not secs:
        return {}
    codes = {_sym(x): x["code"] for x in secs}
    body = SINA.get("/list=" + ",".join(codes)).decode("gbk")
    out = {}
    for line in body.split(";\n"):
        if not line.strip():
            continue
        k, v = line.split("=", 1)
        f = v.strip().strip('"').split(",")
        sym = k.rsplit("_", 1)[1]
        assert sym in codes and len(f) >= 32, f"{sym} 行情不认识或字段不足：{v[:60]}"
        last = float(f[2])
        out[codes[sym]] = {
            "sym": sym,
            "price": float(f[3]) or last,
            "last_close": last,
            "open": float(f[1]),
            "high": float(f[4]),
            "low": float(f[5]),
            "volume": float(f[8]),
            "bid": _levels(f, 10),
            "ask": _levels(f, 20),
            "date": f[30],
            "time": f[31],
        }
    assert set(out) == set(codes.values()), f"缺行情：{set(codes.values()) - set(out)}"
    return out


# ---------- 盘口（5 档，相对参考价）----------
# 方向即符号 s：买 +1 吃卖盘、价位往上；卖 -1 吃买盘、价位往下。
# 滑点与价位两组列同构：(量, s)，滑点乘 s → 正=成本。只出数字，格式化归前端。

BOOK = {BUY: "ask", SELL: "bid"}
NOTIONALS = (10000, 50000, 100000)  # 吃 n 元
PCTS = (1, 2)  # 偏 p%
SLIP_COLS = [(n, BUY) for n in reversed(NOTIONALS)] + [(n, SELL) for n in NOTIONALS]
LEVEL_COLS = [(p, s) for p in PCTS for s in BOOK]
GROUPS = {
    "green": {
        "600697",
        "301006",
        "300535",
        "605567",
        "603214",
        "600561",
        "300645",
        "301037",
    },
    "red": set(),
}


def _grp(code):
    return next((g for g, codes in GROUPS.items() if code in codes), "")


def _book(q, s):
    """方向 s 吃的那侧 5 档 [(价, 股)]（quotes 已略去空档）。"""
    return q[BOOK[s]]


def _micro(q):
    """卖一/买一按对手量加权；任一侧空盘回 None。"""
    ask, bid = _book(q, BUY), _book(q, SELL)
    if not ask or not bid:
        return None
    (a, av), (b, bv) = ask[0], bid[0]
    return (a * bv + b * av) / (av + bv)


def _auction(q):
    """集合竞价：盘口只给虚拟撮合价，卖一 ≤ 买一（锁定 / 交叉），此时滑点无意义。"""
    ask, bid = _book(q, BUY), _book(q, SELL)
    return bool(ask and bid and ask[0][0] <= bid[0][0])


def _ref(q):
    """参考价：micro price → 现价 → 昨收，取首个可用；都没有回 None。"""
    return _micro(q) or next((x for x in (q["price"], q["last_close"]) if x > 0), None)


def _eat(book, notional):
    """按档吃 notional 元 → (均价, 是否吃满)。"""
    spent = shares = 0.0
    for price, vol in book:
        take = min(vol, (notional - spent) / price)
        spent += take * price
        shares += take
        if spent >= notional - 1e-6:
            break
    return (spent / shares if shares else None), spent >= notional - 1e-6


def _slip(q, ref, n, s):
    """吃 n 元均价相对 ref 的万分比 × s（正=成本）；full=5 档吃满，否则实际更差。"""
    avg, full = _eat(_book(q, s), n)
    return {"pct": s * (avg - ref) / ref * 1e4 if avg else None, "full": full}


def _level(ref, p, s):
    """ref 朝方向 s 偏 p% 的价位。"""
    return ref * (1 + s * p / 100)


def _agg(rows, i):
    """第 i 列滑点按占比加权。"""
    pairs = [
        (r["slips"][i]["pct"], r["weight"])
        for r in rows
        if r["slips"] and r["slips"][i]["pct"] is not None and r["weight"]
    ]
    den = sum(w for _, w in pairs)
    return sum(v * w for v, w in pairs) / den if den else None


# ---------- 持仓页（内存持仓 × 当日成交 × 行情，按代码对齐）----------
# 三张表同以代码为键：持仓快照 held、成交合计 flow、行情 quotes；对齐成 (h, f)：
#   h 为 None = 今日清仓（持仓表已无此行，只有当日列），f 为 None = 成交还没读过。
# 一行由几组列拼成（PARTS），每组 (h, f, q) → 值元组，缺了它要的输入即整组 None：
#   标识：代码 / 名称 / 分组 / 是否今日清仓 / 现价 / 行情（盘口、K 线页用）；
#   持仓：量、成本、券商盈亏取自内存快照，随价变的按 Δ价 × 量 平移（保留券商口径）；
#   当日：按成交价自算（毛，不扣费）= 现市值 − 昨收 × 昨仓 − Σ 方向 × 额，昨仓 = 现量 − Σ 方向 × 量，
#         % 的分母为昨收 × 昨仓 + 今买额；成交没读过、或与持仓的今买 / 今卖对不上即空；
#   盘口：各列滑点（竞价时空）与价位，无参考价皆空。
# 占比两列同构 = 市值 / 分母：仓位% 的分母是券商口径的资产（快照的 市值 / 仓位 反推，取仓位最大那只、
# 舍入误差最小，再随总市值平移），占比的分母是总市值。

NO_FLOW = {"qty": dict.fromkeys(SIDES, 0.0), "amount": dict.fromkeys(SIDES, 0.0)}
TOTALS = ("market_value", "pnl", "day_pnl")


def _join(st):
    """持仓 × 成交合计按代码对齐 → [(h, f)]，持仓在前、今日清仓在后；两表沪深标志须一致。"""
    held = st.pos.held
    flow = None if st.fills is None else st.fills.flow
    codes = [*held, *(c for c in flow or {} if c not in held)]
    pairs = [
        (held.get(c), None if flow is None else flow.get(c, NO_FLOW)) for c in codes
    ]
    assert all(
        h is None
        or f is None
        or f.get("market_flag", h["market_flag"]) == h["market_flag"]
        for h, f in pairs
    ), "成交与持仓的沪深标志不一致"
    return pairs


def _synced(h, f):
    """今买 / 今卖须与成交合计相符，不符即两表之间又有成交（其一旧了）；精简视图与今日清仓无可核。"""
    return h is None or h["traded"] is None or h["traded"] == f["qty"]


def synced(st):
    """成交读过、且每只都对得上。"""
    return st.fills is not None and all(_synced(h, f) for h, f in _join(st))


def _ident(h, f, q):
    x = h or f
    return x["code"], x["name"], _grp(x["code"]), h is None, q["price"], q


def _hold(h, f, q):
    if h is None:
        return None
    px = q["price"]
    d = (px - h["price"]) * h["balance"]
    return (
        h["market_value"] + d,
        h["pnl"] + d,
        (px / h["cost"] - 1) * 100 if h["cost"] > 0 else None,
        None if h["financed"] is None else h["financed"] * px,
        h["available"] * px,
        h["collateral"],
        h["cost"],
    )


def _today(h, f, q):
    if f is None or not _synced(h, f):
        return None
    bal = 0.0 if h is None else h["balance"]
    net = {k: sum(s * f[k][s] for s in SIDES) for k in ("qty", "amount")}
    base = (bal - net["qty"]) * q["last_close"]
    day = bal * q["price"] - base - net["amount"]
    den = base + f["amount"][BUY]
    return day, day / den * 100 if den else None


def _depth(h, f, q):
    if h is None:
        return None
    ref, auction = _ref(q), _auction(q)
    return (
        auction,
        [_slip(q, ref, n, s) for n, s in SLIP_COLS] if ref and not auction else None,
        [_level(ref, p, s) for p, s in LEVEL_COLS] if ref else None,
    )


# fmt: off
PARTS = (
    (("code", "name", "group", "closed", "price", "quote"),                                        _ident),
    (("market_value", "pnl", "pnl_pct", "margin_value", "sellable_value", "collateral", "cost"), _hold),
    (("day_pnl", "day_pnl_pct"),                                                                 _today),
    (("auction", "slips", "levels"),                                                             _depth),
)
# fmt: on


def _row(h, f, q):
    return {
        k: v
        for keys, part in PARTS
        for k, v in zip(keys, part(h, f, q) or (None,) * len(keys), strict=True)
    }


def _asset(held, mv):
    """券商口径的资产：快照的 市值 / 仓位 反推，再随总市值 mv 平移；快照无仓位回 None。"""
    top = max(held.values(), key=lambda h: h["position_pct"] or 0, default=None)
    if not (top and top["position_pct"]):
        return None
    base = top["market_value"] / top["position_pct"] * 100
    return base + mv - sum(h["market_value"] for h in held.values())


def _pct(x, den):
    return x / den * 100 if x is not None and den else None


def view(st):
    """State → 持仓页一帧（只出数字，格式化归前端）。"""
    rows = [_row(h, f, st.quotes[(h or f)["code"]]) for h, f in _join(st)]
    mv = sum(r["market_value"] or 0 for r in rows)
    shares = {"position_pct": _asset(st.pos.held, mv), "weight": mv}
    for r in rows:
        r |= {k: _pct(r["market_value"], den) for k, den in shares.items()}
    return {
        "pid": st.pos.pid,
        "rows": rows,
        "slip_cols": SLIP_COLS,
        "level_cols": LEVEL_COLS,
        "totals": {k: round(sum(r[k] or 0 for r in rows), 2) for k in TOTALS}
        | {"slips": [_agg(rows, i) for i in range(len(SLIP_COLS))]},
        "fills": None if st.fills is None else len(st.fills.rows),
        "synced": synced(st),
        "time": max((q["time"] for q in st.quotes.values()), default=""),
    }


# ---------- 持仓推送（单一后台循环）----------
# 状态 State = (持仓快照, 成交快照, 行情)，帧是它的纯函数：与当前帧所据的不同才重算、推给全部 pos 订阅。
# 一个线程管全部订阅者，有订阅者才转：每 TICK follow 持仓（约 0.03ms；客户端自己重查过，如成交后，即换代），
# 每 FULL_EVERY 全量 read；每 QUOTE_GAP 取一次行情，State 变了即补取（保证覆盖新的那只）。
# 行情只此一路（盘口列同帧复用其 5 档），开几个页面都不加量，免被限频封禁。
# 当日成交只在客户端重查时更新，由 refresh 让客户端依 SOURCES 连查成交与持仓（两方案同一写法）；
# 触发它的事件（_due）：还没读过成交、console 下单成功后的 KICKS 定点、客户端自己重查了持仓却与成交对不上
# （交易端成交了）。定时刷新与手动刷新走前端 /api/refresh，同一个 refresh。

TICK = 0.1
QUOTE_GAP = 1.5  # 新浪每 3s 一帧，1.5s 取一次：延迟 ≤1.5s、约 40 次/分
FULL_EVERY = 60  # 全量扫描最长间隔：补全进程「活动块唯一」校验、跟上客户端换进程
KICKS = (1, 6)  # console 下单成功后隔几秒各刷新一次（限价单可能稍后才成交）
SYNC_TRIES = 2  # 一次 refresh 里成交与持仓对不上（两次查询之间又成交）时最多连查几轮
State = collections.namedtuple("State", "pos fills quotes")
SOURCES = (
    ("fills", FILLS_BTN, memfills),
    ("pos", POS_BTN, mempos),
)  # 先成交后持仓，最后停在持仓页


class Hub:
    def __init__(self):
        self.lock = threading.RLock()  # 管以下各项：后台循环与 /api/refresh 互斥
        self.st = State(
            None, None, {}
        )  # mempos.Snap × memfills.Fills（None = 还没读过）× {代码: 行情}
        self.shown = None  # 当前帧所据的 st；置 None 即下一轮必重算、推帧
        self.subs = 0
        self.scan_at = self.quote_at = 0.0
        self.checked = None  # 最近一次 refresh 读到的持仓快照：客户端自己换了代才再核对
        self.kicks = []  # 待到点刷新的时刻（monotonic）
        self.frame = {}
        self.feed = Feed(lambda seen: self.frame)

    def _set(self, **kw):
        self.st = self.st._replace(**kw)

    def _pos(self):
        """当前持仓快照，还没有即全量读一次。须持 lock。"""
        if self.st.pos is None:
            self._set(pos=self._scan())
        return self.st.pos

    def _scan(self):
        """全量读（并重找进程）。"""
        self.scan_at = time.monotonic()
        return mempos.read()

    def _quote(self):
        """持仓与今日清仓的各只一并取行情。"""
        self.quote_at = time.monotonic()
        return quotes([h or f for h, f in _join(self.st)])

    def _due(self, now):
        """该让客户端重查成交与持仓了（事件见本节开头）。须持 lock。"""
        if self.st.fills is None:
            return True
        if self.kicks and now >= self.kicks[0]:
            self.kicks.pop(0)
            return True
        return self.st.pos != self.checked and not synced(self.st)

    def kick(self):
        """console 下单成功后：隔 KICKS 秒各刷新一次。"""
        with self.lock:
            now = time.monotonic()
            self.kicks = sorted(self.kicks + [now + k for k in KICKS])

    def _push(self, frame):
        with self.feed.cond:
            self.frame = frame
            self.feed.bump()

    def step(self):
        now = time.monotonic()
        with self.lock:
            if not self.subs:
                return
            full = self.st.pos is None or now - self.scan_at >= FULL_EVERY
            self._set(pos=self._scan() if full else mempos.follow(self.st.pos))
            due = self._due(now)
        if due and not UI.locked():  # 下单占着界面就等下一轮
            self.refresh()
        with self.lock:
            if self.st != self.shown or now - self.quote_at >= QUOTE_GAP:
                self._set(quotes=self._quote())
            if self.st != self.shown:
                self.shown = self.st
                self._push(view(self.st))

    def run(self):
        while True:
            try:
                self.step()
            except (
                Exception
            ) as e:  # 唯一错误边界（同 _route）：推 {err}，恢复后必推新帧
                with self.lock:
                    self.shown = None
                self._push({"err": f"{type(e).__name__}: {e}"})
                time.sleep(1)
            time.sleep(TICK)

    def refresh(self):
        """SOURCES 依次点交易栏页签、点「刷新」，等该方案换代（各实测约 0.25s），推帧。
        两次查询之间又有成交即对不上，连查至多 SYNC_TRIES 轮；仍不对留给前端标出，不再自动重刷。"""
        with UI, self.lock:
            pid = self._pos().pid
            for _ in range(SYNC_TRIES):
                for key, tab, scheme in SOURCES:
                    old = getattr(self.st, key) or scheme.read(pid)
                    click_text(tab)
                    click_text(REFRESH_BTN)
                    self._set(**{key: memcommon.wait(scheme, old)})
                if synced(self.st):
                    break
            self.checked = self.st.pos
            self.shown = None  # 没换代也推一帧，前端借此结束「刷新中」
        return {}

    def frames(self):
        """SSE：订阅者到来即重取行情、重算推帧，其余同 Feed。"""
        with self.lock:
            self.subs += 1
            self.shown = None
        try:
            yield from self.feed.watch()
        finally:
            with self.lock:
                self.subs -= 1

    def held(self, code):
        """持仓里 code 那只。"""
        with self.lock:
            h = self._pos().held.get(code)
        assert h, f"持仓里没有 {code}"
        return h

    def quote(self, code):
        """持仓页行情里 code 那只（持仓与今日清仓）。"""
        with self.lock:
            q = self.st.quotes.get(code)
        assert q, f"持仓页行情里没有 {code}"
        return q


HUB = Hub()


# ---------- K 线（新浪，本地落盘）----------
# 一根一项 [标号, 开, 高, 低, 收, 量]（价 3 位小数、量单位股），旧在前。日线 Day / 分钟线 Min 走同一套流程（KCache），
# 差异全在两个类的同名成员：
#   scale 新浪周期（分钟）  fmt 标号格式  per 每个交易日根数  keep 保留交易日数  cap 单次取数上限
#   settle 定稿延迟（秒）：标号不晚于「此刻 − settle」的不再变（日线即今日以前，分钟线即 90 秒以前）
#   slot(t) 标号 t 是当日第几根  valid(t) 标号合法  tail(q) 由行情合成的进行中那根
#   load / save 盘上布局
# 新浪（实测）：1 分钟线按 09:31 … 11:30、13:01 … 15:00 标号，无成交的分钟没有这根（一天约 237~238 根），
# 刚过去的那根还会被修正；单次至多 1970 根；日线盘中不含当日。
# 落盘 DATA_DIR，CSV（time,open,high,low,close,volume）：
#   day/<sym>.csv          已定稿的日线，变了整写；
#   min/<sym>/<日期>.csv    收完的一天（定稿已过当日 15:00）的分钟线，与当日日线核对后写，写后不改。
# 未定稿的只在内存；超出保留期的行 / 文件随写入删掉。盘口不留历史。
# 取数只追不改：
#   该取：定稿线又越过了一根（gap(上次定稿线, 此刻定稿线) > 0）：日线每个交易日后至多一次，
#         分钟线每分钟至多一次，午休、盘后、周末不取。
#   增量：取「已定稿最后一根之后至今的根数上界（gap，节假日只会多估）+ OVERLAP」根，
#         重叠部分须与已定稿逐根一致才接上（无缝、未改）；没有已定稿或上界超出单次上限即取满上限，
#         掐掉最早那个可能被截断的日子，接不上已定稿就以新取的为准。
# 完整性：标号严格递增且合法、OHLC 自洽（取到的、读盘的都核）；分钟线每天落盘前与当日日线核对
#   开 / 高 / 低 / 收 恰等、量在 [VOL_FLOOR, 1] × 日线量（实测深市恰等，沪市分钟合计少 0.05%~0.3%）。
# 今日日线由行情合成（今开 / 最高 / 最低 / 现价 / 量，同日线口径）；进行中的分钟线前端随持仓帧用现价改写。

DATA_DIR = os.path.join(os.path.dirname(HERE), "data", "kline")
CSV_HEAD = "time,open,high,low,close,volume\n"
OVERLAP = 3
VOL_FLOOR = 0.95
SINA_K = Sina("quotes.sina.cn")


def _wd(d):
    return datetime.date.fromisoformat(d).weekday() < 5


def _weekdays(d0, d1):
    """日期 d0、d1 之间（不含两端）的工作日数。"""
    a, b = (datetime.date.fromisoformat(d) for d in (d0, d1))
    return sum(
        (a + datetime.timedelta(i)).weekday() < 5 for i in range(1, (b - a).days)
    )


def _bar(t, o, h, l, c, v):
    return [t, *(round(float(x), 3) for x in (o, h, l, c)), int(float(v))]


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
    """已定稿的 old 接上新取的 new：重叠部分须逐根一致；new 是取满上限的（full）且够不着 old 即以 new 为准。"""
    if not old:
        return new
    last = old[-1][0]
    lap = [b for b in new if b[0] <= last]
    if not lap:
        assert (
            full
        ), f"{sym} 新取 {len(new)} 根与已有无重叠（最早 {new[0][0]}，已有到 {last}）"
        return new
    tail = [b for b in old if b[0] >= lap[0][0]]
    assert lap == tail, f"{sym} 已定稿的 K 线被改过：{tail} → {lap}"
    return old + [b for b in new if b[0] > last]


def _verify(sym, day, g, k):
    """收完的一天分钟线 g 与当日日线 k 核对：开 / 高 / 低 / 收 恰等，量在 [VOL_FLOOR, 1] × 日线量。"""
    assert k, f"{sym} 日线缺 {day}，分钟线无从核对"
    ohlc = [g[0][1], max(b[2] for b in g), min(b[3] for b in g), g[-1][4]]
    assert ohlc == k[1:5], f"{sym} {day} 分钟线合成 {ohlc} ≠ 日线 {k[1:5]}"
    v = sum(b[5] for b in g)
    assert VOL_FLOOR * k[5] <= v <= k[5], f"{sym} {day} 分钟线量 {v} 与日线 {k[5]} 不符"


class Kind:
    """两种 K 线共用的推算：标号、定稿线、两标号间至多几根、取数。"""

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
        body = SINA_K.get(
            "/cn/api/json_v2.php/CN_MarketDataService.getKLineData"
            f"?symbol={sym}&scale={self.scale}&ma=no&datalen={n}"
        )
        rows = json.loads(body)
        assert isinstance(rows, list) and rows, f"{sym} K 线：{body[:80]!r}"
        keys = ("day", "open", "high", "low", "close", "volume")
        return _check(sym, [_bar(*(k[x] for x in keys)) for k in rows], self)


class Day(Kind):
    name, scale, fmt = "day", 240, "%Y-%m-%d"
    per, keep, cap, settle = (
        1,
        180,
        190,
        86400,
    )  # cap：盘后新浪日线含当日，比 keep 多取几根

    def slot(self, t):
        return 1

    def valid(self, t):
        return re.fullmatch(r"\d{4}-\d\d-\d\d", t) is not None

    def tail(self, q):
        """今日那根由行情合成；未开盘没有。"""
        bar = (q["date"], q["open"], q["high"], q["low"], q["price"], q["volume"])
        return _bar(*bar) if q["open"] > 0 else None

    def load(self, sym):
        return _load(os.path.join(DATA_DIR, "day", sym + ".csv"), self)

    def save(self, sym, bars, final, daily):
        """已定稿的整写，与盘上相同即不写。"""
        path = os.path.join(DATA_DIR, "day", sym + ".csv")
        fin = [b for b in bars if b[0] <= final]
        if fin != _load(path, self):
            _save(path, fin)


class Min(Kind):
    name, scale, fmt = "min", 1, "%Y-%m-%d %H:%M:%S"
    per, keep, cap, settle = (
        240,
        7,
        1970,
        90,
    )  # cap：新浪单次上限（实测 1970 可、1980 回 null），约 8 个交易日

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

    def load(self, sym):
        """各天文件按日期接起来，每个文件只含该日。"""
        d = os.path.join(DATA_DIR, "min", sym)
        names = sorted(os.listdir(d)) if os.path.isdir(d) else []
        bars = []
        for name in names:
            assert re.fullmatch(
                r"\d{4}-\d\d-\d\d\.csv", name
            ), f"{d} 里有别的文件 {name}"
            g = _load(os.path.join(d, name), self)
            assert g and all(
                b[0][:10] == name[:10] for b in g
            ), f"{d}/{name} 混了别的日子"
            bars += g
        return _check(d, bars, self)

    def save(self, sym, bars, final, daily):
        """收完的日子（定稿已过当日 15:00）与日线 daily() 核对后各写一个文件，写过的不重写；保留期外的文件删掉。"""
        d = os.path.join(DATA_DIR, "min", sym)
        have = set(os.listdir(d)) if os.path.isdir(d) else set()
        keep = set()
        for day, g in itertools.groupby(bars, key=lambda b: b[0][:10]):
            name = day + ".csv"
            keep.add(name)
            if name in have or final < day + " 15:00:00":
                continue
            g = list(g)
            _verify(sym, day, g, daily().get(day))
            _save(os.path.join(d, name), g)
        for name in have - keep:
            os.remove(os.path.join(d, name))


DAY, MIN = Day(), Min()
KINDS = {k.name: k for k in (DAY, MIN)}


class KCache:
    """以盘上为准，内存多出未定稿的；取数与读写盘串行（一把锁，分钟线落盘核对时重入取日线）。"""

    def __init__(self):
        self.lock = threading.RLock()
        self.series = (
            {}
        )  # (种类, sym) → {bars, final：此标号及以前已定稿；None = 只读了盘、还没取过}

    def _grow(self, kind, sym, s, now, q):
        """增量取数接上已定稿部分，按保留期裁剪，落盘。须持 lock。"""
        fin = (
            s["bars"]
            if s["final"] is None
            else [b for b in s["bars"] if b[0] <= s["final"]]
        )
        n = kind.gap(fin[-1][0], kind.label(now)) + OVERLAP if fin else kind.cap
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
        kind.save(
            sym,
            s["bars"],
            s["final"],
            lambda: {b[0]: b for b in self._live(DAY, sym, q)},
        )

    def _live(self, kind, sym, q):
        """盘上的 + 增量取到的（定稿线又越过一根才取）+ 行情合成的进行中那根。须持 lock。"""
        s = self.series.get((kind.name, sym))
        if s is None:
            s = self.series[kind.name, sym] = {"bars": kind.load(sym), "final": None}
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


def kline(b):
    """b = {code, kind: day|min, days} → 持仓页行情里（持仓与今日清仓）该只近 days 个交易日的 K 线。"""
    assert b["kind"] in KINDS, f"K 线种类 {b['kind']!r}，可选 {list(KINDS)}"
    q = HUB.quote(b["code"])
    return KCACHE.bars(KINDS[b["kind"]], q["sym"], q, int(b["days"]))


# ---------- 下单（闪电下单窗 UI 自动化）----------
# 流程照录像 rec-20261026-143432，一步一函数，ORDER 即流程也即前端步骤条；每步 (o 本单上下文) → 该单要更新的字段：
#   行情  按盘口定委托价
#   切换  图表切到该只（闪电窗只认图表当前证券）：向主窗投代码按键 + 回车（键盘精灵）
#   开窗  主窗 WM_COMMAND 打开闪电窗
#   填单  窗内证券代码再核一遍，填价格 / 数量
#   确认  点提交，「…交易确认」框核对正文（代码 / 价格 / 数量）
#   回报  点 7015，「提示」框读回报（成功含合同号）点 7015 关掉
# 开窗 / 确认 / 回报同形：做一个动作，等它弹出的那个对话框（_popup）。
# 一次只跑一单，逐步推状态；任一步不符或超时即停在那一步，已弹出的窗口留给人工处理。

WM_KEYDOWN, WM_KEYUP, WM_COMMAND = 0x100, 0x101, 0x111
VK_RETURN, SC_RETURN = 0x0D, 0x1C
WM_MDIGETACTIVE = 0x229
AFX_PANE = (
    59648  # MFC AFX_IDW_PANE_FIRST：主框架下的 MDIClient、MDI 子窗下的视图都是这个 id
)
MAIN_WND = "class=TdxW_MainFrame_Class"
# 闪电窗按 (方向, 信用方式) 取：主窗命令 id、窗标题、控件 id（证券代码 / 价格 / 数量 / 数量单位 / 报价方式 /
# 最大可买·可卖·可融（股）/ 提交）；卖券还款未录到，没有 (SELL, margin)
_FLASH_IDS = {
    "code": 12005,
    "price": 12006,
    "qty": 12007,
    "unit": 12016,
    "quote": 3067,
    "max": 2022,
    "ok": 1,
}
FLASH = {
    (BUY, "plain"): (0x13DD, "闪电买入", _FLASH_IDS),
    (BUY, "margin"): (
        0x1441,
        "闪电融资买入",
        {
            "code": 313,
            "price": 314,
            "qty": 315,
            "unit": 320,
            "quote": 220,
            "max": 199,
            "ok": 1,
        },
    ),
    (SELL, "plain"): (0x1404, "闪电卖出", {**_FLASH_IDS, "max": 2065}),
}
CONFIRM_TITLES = ("买入交易确认", "卖出交易确认", "交易确认")
RESULT_TITLE = "提示"
MSG_TEXT, MSG_YES = 7013, 7015  # 确认框 / 回报框：正文 Static、确认按钮
PX_MODES = {
    "opp": (True, 0),
    "opp1": (True, 1),
    "own": (False, 0),
}  # (吃对手盘, 再偏 %)
ORDER_CAP = 20


def _order_qty(code, px, size, s, cap):
    """size 元（0 = 最小单位）折股：科创板起 200 股、零股递增，北交所起 100 股、零股递增，其余整手；
    再以闪电窗给的最大可买 / 可卖 cap 封顶（钱或券不够就按能做的最大量）：买入按步长取整、须够起买量，
    卖出不足即全卖（零股只能一次卖完）。"""
    lot = 200 if code.startswith("688") else 100
    step = 1 if code.startswith(("688", "4", "8", "92")) else 100
    n = int(size / px) // step * step if size else lot
    assert n >= lot, f"{size} 元按 {px} 不足最小单位 {lot} 股"
    if s == BUY:
        cap = cap // step * step
        assert cap >= lot, f"最大可买 {cap} 股，不足最小单位 {lot} 股"
    assert cap >= 1, "最大可卖为 0"
    return min(n, cap)


def _max_qty(h):
    t = _text(h)
    assert re.fullmatch(r"\d+(\.\d+)?", t), f"最大可买/可卖读到 {t!r}"
    return int(float(t))


def _post(h, msg, wp, lp=0):
    assert SHIM("post", h, msg, wp, lp)[0]["ret"], f"PostMessage {h} {msg:#x} 失败"


def _press(h, vk, sc):
    """投递按下 + 抬起（同 shim post_key），WM_CHAR 由客户端消息泵照常生成。"""
    lp = 1 | sc << 16
    _post(h, WM_KEYDOWN, vk, lp)
    _post(h, WM_KEYUP, vk, lp | 0xC0000000)


def _title(h):
    return SHIM("rect", h)[0]["title"]


def _text(h):
    return SHIM("gettext", h)[0]["text"]


def _ctls(h, ids):
    """对话框 h 的直接子控件，按 {名: 控件 id} 各取唯一一个 → {名: hwnd}。"""
    kids = [n for n in SHIM("enum", h) if n["depth"] == 1]
    out = {}
    for k, cid in ids.items():
        hits = [n["hwnd"] for n in kids if n["id"] == cid]
        assert len(hits) == 1, f"{h}「{_title(h)}」控件 {k}({cid}) 命中 {len(hits)} 个"
        out[k] = hits[0]
    return out


def _tops():
    """可见顶层窗口 {hwnd: 节点}。"""
    return {n["hwnd"]: n for n in SHIM("enum") if n["depth"] == 0 and n["vis"]}


def _wait(what, fn, timeout):
    """what 可为可调用（超时时才求值，好报出当时的状态）。"""
    end = time.monotonic() + timeout
    while not (r := fn()):
        assert (
            time.monotonic() < end
        ), f"{timeout}s 内未等到{what() if callable(what) else what}"
        time.sleep(0.05)
    return r


def _body(h):
    """确认 / 提示框正文（7013），没有该控件回空串。"""
    hits = [
        n["hwnd"] for n in SHIM("enum", h) if n["depth"] == 1 and n["id"] == MSG_TEXT
    ]
    return " ".join(_text(hits[0]).split()) if len(hits) == 1 else ""


def _command(dlg, cid):
    """点对话框 dlg 上 id 为 cid 的按钮：向 dlg 投 WM_COMMAND(cid, 按钮)。"""
    _post(dlg, WM_COMMAND, cid, _ctls(dlg, {"btn": cid})["btn"])


def _popup(titles, timeout, act):
    """做 act，等其后新出现、标题在 titles 内的可见对话框（须唯一）→ hwnd；超时报出期间冒出的新窗口及正文。"""
    before = _tops()
    act()
    want = "/".join(titles)

    def new():
        return {
            h: n
            for h, n in _tops().items()
            if h not in before and n["class"] == "#32770"
        }

    def got():
        hits = [h for h, n in new().items() if n["title"] in titles]
        assert len(hits) <= 1, f"「{want}」同时出现 {len(hits)} 个"
        return hits[0] if hits else None

    return _wait(
        lambda: f"「{want}」，期间新窗口："
        + ("；".join(f"「{n['title']}」{_body(h)}" for h, n in new().items()) or "无"),
        got,
        timeout,
    )


def _goto(main, code, name):
    """图表切到 code：主窗标题已是「…-名称]」即跳过，否则向活动 MDI 子窗的视图投代码按键
    （键盘精灵由视图处理，投给主框架不经过它），等精灵弹出后向其输入框投回车，再等标题换过来。"""
    want = f"-{name}]"
    if _title(main).endswith(want):
        return
    mdi = _ctls(main, {"mdi": AFX_PANE})["mdi"]
    child = f"0x{SHIM('send', mdi, WM_MDIGETACTIVE, 0, 0)[0]['ret'] & 0xFFFFFFFF:08x}"
    view = _ctls(child, {"view": AFX_PANE})["view"]
    before = _tops()
    for ch in code:
        _press(
            view, ord(ch), 11 if ch == "0" else int(ch) + 1
        )  # 数字键扫描码 1..9→2..10、0→11
    spirit = _wait(
        f"键盘精灵弹出（已向「{_title(child)}」视图 {view} 投 {code}）",
        lambda: next((h for h in _tops() if h not in before), None),
        1,
    )
    time.sleep(0.3)  # 精灵按输入筛候选，留出时间再回车
    edits = [
        n["hwnd"]
        for n in SHIM("enum", spirit)
        if n["depth"] == 1 and n["class"] == "Edit"
    ]
    _press(edits[0] if len(edits) == 1 else spirit, VK_RETURN, SC_RETURN)
    _wait(
        lambda: f"主窗切到「{name}」，现为「{_title(main)}」",
        lambda: _title(main).endswith(want),
        3,
    )


def _settext(h, s):
    assert SHIM("settext", h, s)[0]["ret"], f"写 {h} 失败"
    got = _text(h)
    assert got == s, f"写入 {s!r} 后读回 {got!r}"


def _field(body, pat, what):
    m = re.search(pat, body)
    assert m, f"确认框缺{what}：{body}"
    return m.group(1)


def _price(o):
    """方向 s 按 mode 定委托价（未舍入）：对手价 / 对手价再朝成交方向偏 1% / 本方价。"""
    q = quotes([o.h])[o.code]
    opp, pct = PX_MODES[o.mode]
    assert not _auction(q), "集合竞价中，盘口无连续价"
    book = _book(q, o.s if opp else -o.s)
    assert book, "盘口为空"
    o.px = book[0][0] * (1 + o.s * pct / 100)
    return {}


def _switch(o):
    main = SHIM("find", MAIN_WND)
    assert len(main) == 1, "找不到通达信主窗"
    o.main = main[0]["hwnd"]
    _goto(o.main, o.code, o.h["name"])
    return {}


def _open(o):
    cmd, title, _ = o.flash
    assert all(
        n["title"] != title for n in _tops().values()
    ), f"已有未关的「{title}」窗"
    o.dlg = _popup((title,), 3, lambda: _post(o.main, WM_COMMAND, cmd))
    return {}


def _form(o):
    c = _ctls(o.dlg, o.flash[2])
    got = _text(c["code"])
    assert got == o.code, f"闪电窗证券 {got} ≠ {o.code}"
    quote = _text(c["quote"])
    assert "限价" in quote, f"报价方式为「{quote}」，需限价委托"
    dflt = _wait("闪电窗默认价", lambda: _text(c["price"]), 1)
    assert "." in dflt, f"默认价 {dflt!r} 定不出小数位"
    o.px_s = f"{o.px:.{len(dflt.split('.')[1])}f}"
    unit = _text(c["unit"])
    assert unit in ("股", "元"), f"数量单位「{unit}」不认识"
    _wait("闪电窗最大可买/可卖", lambda: _text(c["max"]), 1)
    cap0 = _max_qty(c["max"])
    _settext(c["price"], o.px_s)
    # 最大可买按默认价算；改价后客户端未必即时重算，按价反比折一份，与重读的取小
    cap = _max_qty(c["max"])
    cap = min(cap, int(cap0 * float(dflt) / float(o.px_s)) if o.s == BUY else cap0)
    o.n = _order_qty(o.code, float(o.px_s), o.size, o.s, cap)
    _settext(c["qty"], str(o.n) if unit == "股" else f"{o.n * float(o.px_s):.2f}")
    return {"detail": f"{o.n}股 @ {o.px_s}（最大 {cap}）"}


def _confirm(o):
    o.box = _popup(CONFIRM_TITLES, 5, lambda: _command(o.dlg, o.flash[2]["ok"]))
    body = _body(o.box)
    got = (
        _field(body, r"股票代码: *(\d{6})", "股票代码"),
        float(_field(body, r"委托价格: *([\d.]+)", "委托价格")),
        int(_field(body, r"委托数量: *(\d+)股", "委托数量")),
    )
    want = (o.code, float(o.px_s), o.n)
    assert got == want, f"确认框 {got} ≠ 预期 {want}：{body}"
    return {}


def _reply(o):
    res = _popup((RESULT_TITLE,), 10, lambda: _command(o.box, MSG_YES))
    body = _body(res)
    _command(res, MSG_YES)
    assert "合同号" in body, f"委托未成功：{body}"
    return {"msg": body}


ORDER = (
    ("行情", _price),
    ("切换", _switch),
    ("开窗", _open),
    ("填单", _form),
    ("确认", _confirm),
    ("回报", _reply),
)


class Orders:
    def __init__(self):
        self.jobs = collections.deque(
            maxlen=ORDER_CAP
        )  # 下单线程改状态，同在 feed.cond 内
        self.ids = itertools.count(1)
        self.feed = Feed(lambda seen: list(self.jobs)[::-1])

    def _put(self, job, **kw):
        with self.feed.cond:
            job.update(kw)
            self.feed.bump()

    def submit(self, b):
        """b = {code, s: BUY|SELL, size: 元（0 = 最小单位）, px: PX_MODES 键, credit: plain|margin}。"""
        s, size = int(b["s"]), int(b["size"])
        assert (
            (s, b["credit"]) in FLASH and size >= 0 and b["px"] in PX_MODES
        ), f"参数不对：{b}"
        h = HUB.held(b["code"])
        with self.feed.cond:
            assert not any(j["state"] == "run" for j in self.jobs), "上一单还没结束"
            job = {
                "id": next(self.ids),
                "t": time.strftime("%H:%M:%S"),
                "code": h["code"],
                "name": h["name"],
                "s": s,
                "label": f"{size // 10000}W" if size else "最小",
                "steps": [name for name, _ in ORDER],
                "at": 0,
                "state": "run",
                "detail": "",
                "msg": "",
            }
            self.jobs.append(job)
            self.feed.bump()
        o = types.SimpleNamespace(
            h=h,
            code=h["code"],
            s=s,
            size=size,
            mode=b["px"],
            flash=FLASH[s, b["credit"]],
        )
        threading.Thread(target=self._run, args=(job, o), daemon=True).start()
        return job

    def _run(self, job, o):
        try:
            with UI:
                for at, (_, step) in enumerate(ORDER):
                    self._put(job, at=at)
                    self._put(job, **step(o))
            self._put(job, state="ok")
            HUB.kick()
        except Exception as e:  # 错误边界（同 Hub.run）：停在当前步，报原因
            self._put(job, state="err", msg=f"{type(e).__name__}: {e}")


ORDERS = Orders()


# ---------- 录制器（只读，不投递任何输入）----------
# 每 REC_GAP 秒取一次 spy 消息（WM_COMMAND + 发给非标准控件的私有消息）、交易请求、
# 窗口树、绘制层，
# 与上一轮比对，变了的逐条写 logs/rec-<时刻>.jsonl。录制期间独占 spy（每轮 dump 后 clear）。
# 一行一条 {t 距开录秒, kind, …}：start / tree（开录时全树）/ msg（相邻同样的合并，n 为条数）/
# req（客户端内部请求字段）/ new / del / chg（窗口出现、消失、属性变化）/
# gdi（可见窗口绘制文字）/ mark（打点）/ err / stop。
# t 是取到的那一轮的时刻，同一轮内只有 msg 之间保序。

REC_DIR = os.path.join(os.path.dirname(HERE), "logs")
REC_GAP = 0.25
REC_KEYS = ("vis", "en", "title", "rect", "style")
REC_TAIL = 500  # 推给页面的最近条数（不含 tree）


def _brief(n):
    return {
        k: n[k] for k in ("hwnd", "depth", "id", "class", "title", "rect", "vis", "en")
    }


def _texts(layer):
    return [it[5] for it in layer["items"] if len(it) > 5 and it[5].strip()]


class Recorder:
    def __init__(self):
        self.f, self.path, self.n = (
            None,
            None,
            0,
        )  # f=None 即未在录；录制线程与打点同在 feed.cond 内
        self.tail = collections.deque(maxlen=REC_TAIL)  # (版本, 记录)
        self.halt = threading.Event()
        self.feed = Feed(self._since)

    def _since(self, seen):
        """状态与版本 seen 之后的记录。"""
        return {"st": self.status(), "recs": [r for s, r in self.tail if s > seen]}

    def status(self):
        with self.feed.cond:
            return {"on": self.f is not None, "file": self.path, "n": self.n}

    def log(self, kind, **kw):
        with self.feed.cond:
            assert self.f, "未在录制"
            rec = {"t": round(time.monotonic() - self.t0, 3), "kind": kind, **kw}
            self.f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            self.n += 1
            self.feed.bump()
            if kind != "tree":
                self.tail.append((self.feed.seq, rec))

    def start(self):
        with self.feed.cond:
            assert not self.f, f"已在录制 {self.path}"
            os.makedirs(REC_DIR, exist_ok=True)
            self.path = os.path.join(REC_DIR, time.strftime("rec-%Y%m%d-%H%M%S.jsonl"))
            self.f = open(self.path, "w", encoding="utf-8", buffering=1)
            self.t0, self.n = time.monotonic(), 0
            self.halt.clear()
            self.thread = threading.Thread(target=self.run, daemon=True)
            self.thread.start()
        return self.status()

    def stop(self):
        assert self.status()["on"], "未在录制"
        self.halt.set()
        self.thread.join()
        return self.status()

    def mark(self, text):
        self.log("mark", text=text)
        return self.status()

    def run(self):
        try:
            self._loop()
        except Exception as e:  # 错误边界（同 Hub.run）：记下即停录，不重试
            self.log("err", err=f"{type(e).__name__}: {e}")
        SHIM("spy", "off")
        SHIM("req", "off")
        with self.feed.cond:
            self.log("stop")
            self.f.close()
            self.f = None

    def _loop(self):
        self.log("start", file=self.path, pid=SHIM("ping")[0]["pid"], gap=REC_GAP)
        SHIM("spy", "on", "cmd")
        SHIM("spy", "clear")
        SHIM("req", "on")
        SHIM("req", "clear")
        prev = {n["hwnd"]: n for n in SHIM("tree")}
        self.log("tree", nodes=[_brief(n) for n in prev.values()])
        gdi = {}
        while not self.halt.wait(REC_GAP):
            ev = SHIM("spy", "dump")
            SHIM("spy", "clear")
            for e in SHIM("req", "drain"):
                self.log("req", **e)
            cur = {n["hwnd"]: n for n in SHIM("tree")}
            for e, run in itertools.groupby(ev):
                w = cur.get(e["hwnd"]) or prev.get(e["hwnd"]) or {}
                self.log(
                    "msg",
                    **e,
                    n=sum(1 for _ in run),
                    id=w.get("id"),
                    title=w.get("title"),
                    vis=w.get("vis"),
                )
            for h, n in cur.items():
                if h not in prev:
                    self.log("new", node=_brief(n))
                    continue
                d = {k: [prev[h][k], n[k]] for k in REC_KEYS if prev[h][k] != n[k]}
                if d and (n["vis"] or prev[h]["vis"]):
                    self.log("chg", node=_brief(n), diff=d)
            for h in prev.keys() - cur.keys():
                self.log("del", node=_brief(prev[h]))
            vis = {h for h, n in cur.items() if n["vis"]}
            now = {d["hwnd"]: _texts(d) for d in SHIM("gdidump") if d["hwnd"] in vis}
            for h, tx in now.items():
                if gdi.get(h) != tx:
                    n = cur[h]
                    self.log("gdi", hwnd=h, cls=n["class"], id=n["id"], texts=tx)
            prev, gdi = cur, now


REC = Recorder()


# ---------- 合路推送 ----------
# 各路是独立的生成器（各自阻塞、各自心跳），每路一个线程抽取进同一队列：两路 shim 长连接原样转发，三路 Feed。
# 连接断开时置 done，各路在下一条（至迟一个心跳）时自行收尾。

WATCH = {
    "tree": lambda: SHIM.lines("treewatch"),
    "gdi": lambda: SHIM.lines("gdiwatch"),
    "pos": HUB.frames,
    "ord": ORDERS.feed.watch,
    "rec": REC.feed.watch,
}


def watch(b):
    names = b["s"].split(",")
    assert all(n in WATCH for n in names), f"订阅 {names}，可选 {list(WATCH)}"
    q = queue.Queue()
    done = threading.Event()

    def pump(name):
        try:
            with closing(WATCH[name]()) as g:
                for x in g:
                    if done.is_set():
                        return
                    q.put((name, f'{{"s": "{name}", "r": {x}}}'))
        finally:
            q.put((name, None))  # 该路断了（出错或结束）：整条连接跟着断，前端自动重连

    for n in names:
        threading.Thread(target=pump, args=(n,), daemon=True).start()
    try:
        while True:
            name, x = q.get()
            assert x is not None, f"推送 {name} 断了"
            yield x
    finally:
        done.set()


# ---------- 路由 ----------
# 处理函数一律 fn(body) → 应答；应答类型决定载体：str=页面、生成器=SSE、其余=JSON。
# GET 的 body 是查询串。

GET = {
    "/": lambda b: open(HTML_FILE, encoding="utf-8").read(),
    "/api/ping": lambda b: {"pid": SHIM("ping")[0]["pid"] if SHIM.online() else None},
    "/api/watch": watch,
}

POST = {
    "/api/shim": lambda b: SHIM(b["cmd"], *b["args"]),
    "/api/clicktext": click_text,
    "/api/refresh": lambda b: HUB.refresh(),
    "/api/rec": lambda b: REC.start() if b["on"] else REC.stop(),
    "/api/recmark": lambda b: REC.mark(b["text"]),
    "/api/order": ORDERS.submit,
    "/api/kline": kline,
}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass  # 静默

    def handle_one_request(self):
        # SSE 长连接，浏览器刷新/关页会中途断连，sendall 撞 BrokenPipe；属正常，静默。
        try:
            super().handle_one_request()
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True

    def do_GET(self):
        path, _, qs = self.path.partition("?")
        self._route(GET, path, dict(urllib.parse.parse_qsl(qs)))

    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0))
        self._route(POST, self.path, json.loads(self.rfile.read(n)) if n else {})

    def _route(self, table, path, b):
        try:
            body = table[path](b)
        except Exception as e:  # 唯一错误边界：任何失败都回 {err} 给前端显示
            body = {"err": f"{type(e).__name__}: {e}"}
        if isinstance(body, str):
            self._send("text/html; charset=utf-8", body.encode())
        elif isinstance(body, types.GeneratorType):
            self._stream(body)
        else:
            self._send(
                "application/json; charset=utf-8",
                json.dumps(body, ensure_ascii=False).encode(),
            )

    def _send(self, ctype, data):
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _stream(self, gen):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        with closing(gen):
            for x in gen:
                self.wfile.write(f"data: {x}\n\n".encode())
                self.wfile.flush()


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8765
    ensure()  # 编译 + 注入 shim（若未在线），统一入口一条命令搞定
    # noraise 不抬窗：重扫模块，补钩 shim 注入后才加载的 DLL
    # gdi 绘制层：自绘按钮的文字/色块（首次装入口钩子并重绘全部窗口）
    for cmd in ("noraise", "gdi"):
        r = SHIM(cmd, "on")
        assert r[-1]["on"] and not any("skip" in x for x in r), f"{cmd} on: {r}"
    threading.Thread(target=HUB.run, daemon=True).start()
    srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    url = f"http://127.0.0.1:{port}"
    print(f"[*] 控制台: {url}  (Ctrl+C 退出)")
    # 不用 webbrowser：它优先读 $BROWSER，IDE 终端可能把它指向内置浏览器
    subprocess.Popen(
        ["xdg-open", url], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    srv.serve_forever()


if __name__ == "__main__":
    main()
