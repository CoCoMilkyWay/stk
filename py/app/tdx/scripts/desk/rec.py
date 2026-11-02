# desk/rec — 录制器（只读，不投递任何输入）。
# 每 REC_GAP 秒取一次 spy 消息（WM_COMMAND + 发给非标准控件的私有消息）、交易请求、窗口树、绘制层，
# 与上一轮比对，变了的逐条写 logs/rec-<时刻>.jsonl。录制期间独占 spy（每轮 dump 后 clear）。
# 一行一条 {t 距开录秒, kind, …}：start / tree（开录时全树）/ msg（相邻同样的合并，n 为条数）/
# req（客户端内部请求字段）/ new / del / chg（窗口出现、消失、属性变化）/
# gdi（可见窗口绘制文字）/ mark（打点）/ err / stop。
# t 是取到的那一轮的时刻，同一轮内只有 msg 之间保序。

import collections
import itertools
import json
import os
import threading
import time

from . import ROOT
from .feed import Feed, err
from .ui import SHIM

REC_DIR = os.path.join(ROOT, "logs")
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
        # f 为 None 即未在录；录制线程与打点同在 feed 内
        self.f, self.path, self.n = None, None, 0
        # (版本, 记录)
        self.tail = collections.deque(maxlen=REC_TAIL)
        self.halt = threading.Event()
        self.feed = Feed(self._since)

    def _since(self, seen):
        """状态与版本 seen 之后的记录。"""
        return {"st": self.status(), "recs": [r for s, r in self.tail if s > seen]}

    def status(self):
        with self.feed.cond:
            return {"on": self.f is not None, "file": self.path, "n": self.n}

    def log(self, kind, **kw):
        with self.feed:
            assert self.f, "未在录制"
            rec = {"t": round(time.monotonic() - self.t0, 3),
                   "kind": kind, **kw}
            self.f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            self.n += 1
            if kind != "tree":
                self.tail.append((self.feed.seq, rec))

    def start(self):
        with self.feed:
            assert not self.f, f"已在录制 {self.path}"
            os.makedirs(REC_DIR, exist_ok=True)
            self.path = os.path.join(
                REC_DIR, time.strftime("rec-%Y%m%d-%H%M%S.jsonl"))
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
        except Exception as e:  # 错误边界：记下即停录，不重试
            self.log("err", err=err(e))
        SHIM("spy", "off")
        SHIM("req", "off")
        with self.feed:
            self.log("stop")
            self.f.close()
            self.f = None

    def _loop(self):
        self.log("start", file=self.path, pid=SHIM(
            "ping")[0]["pid"], gap=REC_GAP)
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
                d = {k: [prev[h][k], n[k]]
                     for k in REC_KEYS if prev[h][k] != n[k]}
                if d and (n["vis"] or prev[h]["vis"]):
                    self.log("chg", node=_brief(n), diff=d)
            for h in prev.keys() - cur.keys():
                self.log("del", node=_brief(prev[h]))
            vis = {h for h, n in cur.items() if n["vis"]}
            now = {d["hwnd"]: _texts(d)
                   for d in SHIM("gdidump") if d["hwnd"] in vis}
            for h, tx in now.items():
                if gdi.get(h) != tx:
                    n = cur[h]
                    self.log("gdi", hwnd=h,
                             cls=n["class"], id=n["id"], texts=tx)
            prev, gdi = cur, now


REC = Recorder()
