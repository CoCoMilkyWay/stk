# desk/rec — 录制器（只读，不投递任何输入）。
# 每 REC_GAP 秒取一次 spy 消息（WM_COMMAND + 发给非标准控件的私有消息）、交易请求、窗口树、绘制层，
# 与上一轮比对，变了的逐条记进 feed.Journal（logs/rec-<时刻>.jsonl）。录制期间独占 spy（每轮 dump 后 clear）。
# 种类：start / tree（开录时全树，只落盘）/ msg（相邻同样的合并，n 为条数）/
# req（客户端内部请求字段）/ new / del / chg（窗口出现、消失、属性变化）/
# gdi（可见窗口绘制文字）/ mark（打点）/ err / stop。
# t 是取到的那一轮的时刻，同一轮内只有 msg 之间保序。

import itertools
import threading

from .feed import Feed, Journal, err
from .ui import SHIM

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
        # 最近一次录制（None = 还没录过），其 f 为 None 即已停
        self.log = None
        self.halt = threading.Event()
        self.feed = Feed(self._since)

    def _since(self, seen):
        """状态与版本 seen 之后的记录。"""
        return {"st": self.status(), "recs": self.log.since(seen) if self.log else []}

    def status(self):
        with self.feed.cond:
            j = self.log
            return {"on": bool(j and j.f), "file": j and j.path, "n": j.n if j else 0}

    def start(self):
        with self.feed:
            assert not self.status()["on"], f"已在录制 {self.log.path}"
            self.log = Journal("rec", self.feed, REC_TAIL, quiet=("tree",))
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
        assert self.status()["on"], "未在录制"
        self.log("mark", text=text)
        return self.status()

    def run(self):
        try:
            self._loop()
        except Exception as e:  # 错误边界：记下即停录，不重试
            self.log("err", err=err(e))
        SHIM("spy", "off")
        SHIM("req", "off")
        self.log("stop")
        self.log.close()

    def _loop(self):
        log = self.log
        log("start", file=log.path, pid=SHIM("ping")[0]["pid"], gap=REC_GAP)
        SHIM("spy", "on", "cmd")
        SHIM("spy", "clear")
        SHIM("req", "on")
        SHIM("req", "clear")
        prev = {n["hwnd"]: n for n in SHIM("tree")}
        log("tree", nodes=[_brief(n) for n in prev.values()])
        gdi = {}
        while not self.halt.wait(REC_GAP):
            ev = SHIM("spy", "dump")
            SHIM("spy", "clear")
            for e in SHIM("req", "drain"):
                log("req", **e)
            cur = {n["hwnd"]: n for n in SHIM("tree")}
            for e, run in itertools.groupby(ev):
                w = cur.get(e["hwnd"]) or prev.get(e["hwnd"]) or {}
                log(
                    "msg",
                    **e,
                    n=sum(1 for _ in run),
                    id=w.get("id"),
                    title=w.get("title"),
                    vis=w.get("vis"),
                )
            for h, n in cur.items():
                if h not in prev:
                    log("new", node=_brief(n))
                    continue
                d = {k: [prev[h][k], n[k]] for k in REC_KEYS if prev[h][k] != n[k]}
                if d and (n["vis"] or prev[h]["vis"]):
                    log("chg", node=_brief(n), diff=d)
            for h in prev.keys() - cur.keys():
                log("del", node=_brief(prev[h]))
            vis = {h for h, n in cur.items() if n["vis"]}
            now = {d["hwnd"]: _texts(d) for d in SHIM("gdidump") if d["hwnd"] in vis}
            for h, tx in now.items():
                if gdi.get(h) != tx:
                    n = cur[h]
                    log("gdi", hwnd=h, cls=n["class"], id=n["id"], texts=tx)
            prev, gdi = cur, now


REC = Recorder()
