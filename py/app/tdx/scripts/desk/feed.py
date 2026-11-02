# desk/feed — 推送源与错误边界。
# pos / ord / rec 三路同一写法：源在 with feed: 里改状态即升一个版本，订阅者每见新版推 view(上次所见版本)，
# HEARTBEAT 秒无新版推 null（顺带让断开的订阅者及时退出）。只读不改的地方用 with feed.cond:。

import json
import threading

HEARTBEAT = 15


def err(e):
    """各错误边界（路由、持仓循环、下单线程、录制线程）统一的报错文本。"""
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
