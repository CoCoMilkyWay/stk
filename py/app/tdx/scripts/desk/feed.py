# desk/feed — 推送源、后台事务、日志与错误边界。
# Feed：pos / ord / reb / rec 四路同一写法，源在 with feed: 里改状态即升一个版本，订阅者每见新版推 view(上次所见版本)，
#   HEARTBEAT 秒无新版推 null（顺带让断开的订阅者及时退出）。只读不改的地方用 with feed.cond:。
# Jobs：后台事务一次一件（下单 / 调仓），每件 {id, t, state: run|ok|err, msg, …}，留最近几件，改动经所属 Feed 推出。
# Journal：一件事一份 logs/<名>-<时刻>.jsonl（调仓 / 录制），每条 {t 距开头秒数, kind, …}，同时留尾巴给页面。

import collections
import itertools
import json
import os
import threading
import time

from . import ROOT

HEARTBEAT = 15
LOG_DIR = os.path.join(ROOT, "logs")


def err(e):
    """各错误边界（路由、持仓循环、刷新线程、后台事务、录制线程）统一的报错文本。"""
    return f"{type(e).__name__}: {e}"


class Feed:
    def __init__(self, view):
        # 兼管源自身的状态：view 在其内求值
        self.cond = threading.Condition()
        self.seq, self.view = 0, view

    def __enter__(self):
        self.cond.acquire()
        self.seq += 1
        return self

    def __exit__(self, *exc):
        self.cond.notify_all()
        self.cond.release()

    def watch(self):
        seen = -1
        while True:
            with self.cond:
                fresh = self.cond.wait_for(lambda: self.seq != seen, HEARTBEAT)
                r = self.view(seen) if fresh else None
                seen = self.seq
            yield json.dumps(r, ensure_ascii=False)


class Jobs:
    def __init__(self, feed, cap):
        self.feed, self.ids = feed, itertools.count(1)
        self.list = collections.deque(maxlen=cap)

    def last(self):
        return self.list[-1] if self.list else None

    def open(self, **kw):
        """登记一件 → job；上一件还在跑即 assert。"""
        with self.feed:
            j = self.last()
            assert not (
                j and j["state"] == "run"
            ), f"上一件（{j and j['t']} 起）还没结束"
            job = {
                "id": next(self.ids),
                "t": time.strftime("%H:%M:%S"),
                "state": "run",
                "msg": "",
            } | kw
            self.list.append(job)
        return job

    def put(self, job, **kw):
        with self.feed:
            job.update(kw)

    def run(self, job, fn):
        """当前线程跑完 fn(job) → job。"""
        try:
            fn(job)
            self.put(job, state="ok")
        except Exception as e:  # 错误边界：停在当时那一步，报原因
            self.put(job, state="err", msg=err(e))
        return job

    def spawn(self, job, fn):
        """后台跑 fn(job) → job（跑前的样子）。"""
        threading.Thread(target=self.run, args=(job, fn), daemon=True).start()
        return job


class Journal:
    """quiet 里的种类只落盘、不留尾巴；写、读尾巴都在 feed 内。"""

    def __init__(self, name, feed, cap, quiet=()):
        os.makedirs(LOG_DIR, exist_ok=True)
        self.path = os.path.join(LOG_DIR, time.strftime(f"{name}-%Y%m%d-%H%M%S.jsonl"))
        self.f = open(self.path, "w", encoding="utf-8", buffering=1)
        self.feed, self.quiet, self.n = feed, quiet, 0
        self.t0 = time.monotonic()
        # (版本, 记录)
        self.tail = collections.deque(maxlen=cap)

    def __call__(self, kind, **kw):
        with self.feed:
            assert self.f, f"{self.path} 已收尾"
            rec = {"t": round(time.monotonic() - self.t0, 3), "kind": kind, **kw}
            self.f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
            self.n += 1
            if kind not in self.quiet:
                self.tail.append((self.feed.seq, rec))

    def since(self, seen):
        """版本 seen 之后的尾巴（-1 即全部）。"""
        return [r for s, r in self.tail if s > seen]

    def close(self):
        with self.feed:
            self.f.close()
            self.f = None
