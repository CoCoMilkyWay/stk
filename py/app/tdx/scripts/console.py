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
#   GET  /api/treewatch                      SSE：shim treewatch 原样转发（窗口树版本号）
#   GET  /api/gdiwatch                       SSE：shim gdiwatch 原样转发（窗口绘制层）
#   GET  /api/poswatch                       SSE：持仓页帧（内存持仓 × 新浪实时价 + 5 档滑点表），变了才推，{} 为心跳
#   POST /api/shim            {cmd,args}     透传 shim 命令，回 JSON 记录列表（命令表见 shim.c）
#   POST /api/clicktext {text,class?,title?} 点可见窗口上画着 text 的按钮（须唯一）
#   POST /api/refresh                        点交易栏「刷新」，等内存换代后重读，新帧经 poswatch 推送
#   GET  /api/recwatch                       SSE：录制器 {st 状态, recs 新记录}，{} 为心跳
#   POST /api/rec             {on}           开 / 停录制
#   POST /api/recmark         {text}         录制中打点
# 任何失败（assert 等）统一回 {err}。

import os
import sys
import json
import time
import types
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
from mem import positions as mempos  # noqa: E402  内存持仓方案（含完整性校验）

HTML_FILE = os.path.join(HERE, "console.html")
REFRESH_BTN = {"class": "MHPToolBar", "title": "MainViewBar", "text": "刷新"}
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


# ---------- 行情（新浪）----------
# 一行一只：var hq_str_<sym>="名称,今开,昨收,现价,最高,最低,买一,卖一,量,额,
#   买1量,买1价,…,买5量,买5价,卖1量,卖1价,…,卖5量,卖5价,日期,时间,…"；量单位股。

SINA_HOST = "hq.sinajs.cn"
SINA_HDR = {"Referer": "https://finance.sina.com.cn"}


class Sina:
    """keep-alive 长连接（复用省掉每次建连，约 58→31ms）：多线程共用一条，加锁串行。
    闲置超 IDLE 秒先重建，免得撞上对端已关的连接；请求失败照常抛出，连接最迟 IDLE 秒后重建。"""

    IDLE = 10

    def __init__(self):
        self.lock = threading.Lock()
        self.conn, self.used = None, 0.0

    def get(self, path):
        with self.lock:
            if self.conn is None or time.monotonic() - self.used > self.IDLE:
                if self.conn:
                    self.conn.close()
                self.conn = http.client.HTTPConnection(SINA_HOST, timeout=3)
            self.conn.request("GET", path, headers=SINA_HDR)
            body = self.conn.getresponse().read()
            self.used = time.monotonic()
            return body


SINA = Sina()


def _sym(p):
    return ("sz", "sh")[p["market_flag"]] + p["code"]


def _levels(f, at):
    """f[at:at+10] 的 5 档 (量, 价) → [(价, 股)]，略去空档。"""
    lv = [(float(f[at + 2 * i + 1]), float(f[at + 2 * i])) for i in range(5)]
    return [(p, v) for p, v in lv if p > 0 and v > 0]


def quotes(positions):
    """持仓批量取行情 → {sym: {price, last_close, ask, bid, time}}，须每只都有。
    现价为 0（开盘前/停牌）时取昨收，当日盈亏即 0。"""
    if not positions:
        return {}
    syms = [_sym(p) for p in positions]
    body = SINA.get("/list=" + ",".join(syms)).decode("gbk")
    out = {}
    for line in body.split(";\n"):
        if not line.strip():
            continue
        k, v = line.split("=", 1)
        f = v.strip().strip('"').split(",")
        sym = k.rsplit("_", 1)[1]
        assert len(f) >= 32, f"{sym} 行情字段不足：{v[:60]}"
        last = float(f[2])
        out[sym] = {
            "price": float(f[3]) or last,
            "last_close": last,
            "bid": _levels(f, 10),
            "ask": _levels(f, 20),
            "time": f[31],
        }
    assert set(out) == set(syms), f"缺行情：{set(syms) - set(out)}"
    return out


# ---------- 持仓页（内存快照 × 实时价）----------
# 量、成本、券商盈亏取自内存快照，随价变的列按 Δ价 × 量 平移（保留券商口径）。


def _live(p, q):
    """一只持仓按实时价平移。今日有买卖的拆不出昨仓与成交价，当日两列给 None；精简视图缺列同理。"""
    px, bal = q["price"], p["balance"]
    d = (px - p["price"]) * bal
    flat = p["bought"] == 0 and p["sold"] == 0
    return {
        "code": p["code"],
        "name": p["name"],
        "price": px,
        "market_value": p["market_value"] + d,
        "pnl": p["pnl"] + d,
        "pnl_pct": (px / p["cost"] - 1) * 100 if p["cost"] > 0 else None,
        "day_pnl": (px - q["last_close"]) * bal if flat else None,
        "day_pnl_pct": (px / q["last_close"] - 1) * 100 if flat else None,
        "margin_value": None if p["financed"] is None else p["financed"] * px,
        "sellable_value": p["available"] * px,
        "collateral": p["collateral"],
        "d": d,
    }


def view(m, q):
    """内存快照 m × 行情 q → 持仓页一帧（只出数字，格式化归前端）。仓位(%) 的分母（券商口径的资产）
    由快照反推：市值/仓位，取仓位最大那只（舍入误差最小），再随总市值变动平移。"""
    ps = m.rows
    rows = [_live(p, q[_sym(p)]) for p in ps]
    top = max(ps, key=lambda p: p["position_pct"] or 0, default=None)
    base = None
    if top and top["position_pct"]:
        base = top["market_value"] / top["position_pct"] * 100 + sum(
            r["d"] for r in rows
        )
    for r in rows:
        del r["d"]
        r["position_pct"] = r["market_value"] / base * 100 if base else None
    tot = lambda k: round(sum(r[k] or 0 for r in rows), 2)
    return {
        "pid": m.pid,
        "positions": rows,
        "totals": {k: tot(k) for k in ("market_value", "pnl", "day_pnl")},
        "slippage": slippage(ps, q),
        "time": max((x["time"] for x in q.values()), default=""),
    }


# ---------- 持仓推送（单一后台循环）----------
# 一个线程管全部订阅者：每 TICK 复读活动块（约 0.03ms），换代（客户端自己重查过，如成交后）即重读内存；
# 有订阅者时每 QUOTE_GAP 取一次行情；内存或行情变了才重算一帧、唤醒所有 /api/poswatch。
# 行情只此一路（滑点同帧复用其 5 档），开几个页面都不加量，免被限频封禁。

TICK = 0.1
QUOTE_GAP = 1.5  # 新浪每 3s 一帧，1.5s 取一次：延迟 ≤1.5s、约 40 次/分
FULL_EVERY = 60  # 全量扫描最长间隔：补全进程「活动块唯一」校验、跟上客户端换进程
HEARTBEAT = 15  # 无新帧时的心跳间隔，顺带让断开的订阅者及时退出


class Hub:
    def __init__(self):
        self.lock = threading.RLock()  # 管 mem/q：后台循环与 /api/refresh 互斥
        self.cond = threading.Condition()  # 管 frame/seq：唤醒订阅者
        self.mem = None  # mempos.Snap
        self.q = {}
        self.frame, self.seq = {}, 0
        self.subs = 0
        self.stale = True  # mem/q 变了、帧未重算
        self.scan_at = self.quote_at = 0.0

    def _put(self, k, v):
        """mem / q 同一写法：总换成新值，变了才标 stale。"""
        self.stale |= v != getattr(self, k)
        setattr(self, k, v)

    def _scan(self):
        """全量扫描（并重找进程）。"""
        self.scan_at = time.monotonic()
        return mempos.read()

    def _quote(self):
        self.quote_at = time.monotonic()
        return quotes(self.mem.rows)

    def _snap(self):
        """当前快照，还没有即全量扫一次。须持 lock。"""
        if self.mem is None:
            self._put("mem", self._scan())
        return self.mem

    def _push(self, frame):
        with self.cond:
            self.frame, self.seq = frame, self.seq + 1
            self.cond.notify_all()

    def step(self):
        now = time.monotonic()
        with self.lock:
            full = self.mem is None or now - self.scan_at >= FULL_EVERY
            self._put("mem", self._scan() if full else mempos.follow(self.mem))
            if not self.subs:
                return
            if self.stale or now - self.quote_at >= QUOTE_GAP:
                self._put("q", self._quote())  # 内存变了即补取，保证行情覆盖新持仓
            if self.stale:
                self.stale = False
                self._push(view(self.mem, self.q))

    def run(self):
        while True:
            try:
                self.step()
            except (
                Exception
            ) as e:  # 唯一错误边界（同 _route）：推 {err}，恢复后必推新帧
                self.stale = True
                self._push({"err": f"{type(e).__name__}: {e}"})
                time.sleep(1)
            time.sleep(TICK)

    def refresh(self):
        """点交易栏「刷新」让客户端重查，等活动块换代即重读并推帧（实测约 0.25s）。"""
        with self.lock:
            snap = self._snap()
            click_text(REFRESH_BTN)
            self._put("mem", mempos.wait(snap))
            self.stale = True  # 没换代也推一帧，前端借此结束「刷新中」
        return {}

    def frames(self):
        """SSE：先推当前帧，此后有新帧即推；HEARTBEAT 秒无新帧推 {}。订阅者到来即重取行情、重算。"""
        with self.lock:
            self.subs += 1
            self.stale = True
        try:
            seen = -1
            while True:
                with self.cond:
                    self.cond.wait_for(lambda: self.seq != seen, HEARTBEAT)
                    fresh, seen, frame = self.seq != seen, self.seq, self.frame
                yield json.dumps(frame, ensure_ascii=False) if fresh else "{}"
        finally:
            with self.lock:
                self.subs -= 1


HUB = Hub()


# ---------- 滑点（持仓 × 5 档盘口，相对参考价）----------
# 方向即符号 s：买 +1 吃卖盘、价位往上；卖 -1 吃买盘、价位往下。
# 滑点与价位两组列同构：(量, s)，滑点乘 s → 正=成本。只出数字，格式化归前端。

BUY, SELL = +1, -1
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


def _fill(book, notional):
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
    avg, full = _fill(_book(q, s), n)
    return {"pct": s * (avg - ref) / ref * 1e4 if avg else None, "full": full}


def _level(ref, p, s):
    """ref 朝方向 s 偏 p% 的价位。"""
    return ref * (1 + s * p / 100)


def _row(pos, q, weight):
    """一只持仓：各列滑点 + 各列价位；无参考价时两组皆 None，竞价时滑点 None。"""
    ref = _ref(q)
    auction = _auction(q)
    return {
        "code": pos["code"],
        "name": pos["name"],
        "group": _grp(pos["code"]),
        "weight": weight,
        "auction": auction,
        "slips": (
            [_slip(q, ref, n, s) for n, s in SLIP_COLS] if ref and not auction else None
        ),
        "levels": [_level(ref, p, s) for p, s in LEVEL_COLS] if ref else None,
    }


def _agg(rows, i):
    """第 i 列滑点按市值占比加权。"""
    pairs = [
        (r["slips"][i]["pct"], r["weight"])
        for r in rows
        if r["slips"] and r["slips"][i]["pct"] is not None and r["weight"] > 0
    ]
    den = sum(w for _, w in pairs)
    return sum(v * w for v, w in pairs) / den if den else None


def slippage(positions, q):
    """持仓 × 行情 q（含 5 档）→ 每只各列滑点 + 各列价位 + 实时市值占比；市值加权 aggregate。"""
    mv = [p["balance"] * q[_sym(p)]["price"] for p in positions]
    total = sum(mv) or 1
    rows = [_row(p, q[_sym(p)], v / total) for p, v in zip(positions, mv)]
    return {
        "slip_cols": SLIP_COLS,
        "level_cols": LEVEL_COLS,
        "agg": [_agg(rows, i) for i in range(len(SLIP_COLS))],
        "rows": rows,
    }


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
        self.cond = (
            threading.Condition()
        )  # 管 f/n/tail/seq：录制线程与打点互斥，唤醒订阅者
        self.f, self.path, self.n = None, None, 0  # f=None 即未在录
        self.tail, self.seq = collections.deque(maxlen=REC_TAIL), 0  # tail: (seq, 记录)
        self.halt = threading.Event()

    def status(self):
        with self.cond:
            return {"on": self.f is not None, "file": self.path, "n": self.n}

    def log(self, kind, **kw):
        with self.cond:
            assert self.f, "未在录制"
            rec = {"t": round(time.monotonic() - self.t0, 3), "kind": kind, **kw}
            self.f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            self.n += 1
            self.seq += 1
            if kind != "tree":
                self.tail.append((self.seq, rec))
            self.cond.notify_all()

    def start(self):
        with self.cond:
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
        with self.cond:
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

    def watch(self):
        """SSE：先推状态与最近 REC_TAIL 条，此后有新记录即推 {st, recs}；HEARTBEAT 秒无新记录推 {}。"""
        seen = -1
        while True:
            with self.cond:
                self.cond.wait_for(lambda: self.seq != seen, HEARTBEAT)
                recs = [r for s, r in self.tail if s > seen]
                fresh, seen = self.seq != seen, self.seq
                st = self.status()
            yield (
                json.dumps({"st": st, "recs": recs}, ensure_ascii=False)
                if fresh
                else "{}"
            )


REC = Recorder()


# ---------- 路由 ----------
# 处理函数一律 fn(body) → 应答；应答类型决定载体：str=页面、生成器=SSE、其余=JSON。

GET = {
    "/": lambda b: open(HTML_FILE, encoding="utf-8").read(),
    "/api/ping": lambda b: {"pid": SHIM("ping")[0]["pid"] if SHIM.online() else None},
    "/api/treewatch": lambda b: SHIM.lines("treewatch"),
    "/api/gdiwatch": lambda b: SHIM.lines("gdiwatch"),
    "/api/poswatch": lambda b: HUB.frames(),
    "/api/recwatch": lambda b: REC.watch(),
}

POST = {
    "/api/shim": lambda b: SHIM(b["cmd"], *b["args"]),
    "/api/clicktext": click_text,
    "/api/refresh": lambda b: HUB.refresh(),
    "/api/rec": lambda b: REC.start() if b["on"] else REC.stop(),
    "/api/recmark": lambda b: REC.mark(b["text"]),
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
        self._route(GET, {})

    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0))
        self._route(POST, json.loads(self.rfile.read(n)) if n else {})

    def _route(self, table, b):
        try:
            body = table[self.path](b)
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
