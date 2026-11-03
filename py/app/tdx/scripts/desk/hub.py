# desk/hub — 持仓推送（单一后台循环）。
# 状态 State = (持仓快照, 成交快照, 行情)，帧是它的纯函数（board.view）：与当前帧所据的不同才重算、推给全部 pos 订阅。
# 一个线程管全部订阅者，有订阅者才转：每 TICK follow 持仓（约 0.03ms；客户端自己重查过，如成交后，即换代），
# 每 FULL_EVERY 一遍全量扫出 SOURCES 各方案；每 QUOTE_GAP 取一次行情，State 变了即补取（保证覆盖新的那只）。
# 行情只此一路（盘口列同帧复用其 5 档），开几个页面都不加量，免被限频封禁。
# refresh 让客户端依 SOURCES 连查成交与持仓（两方案同一写法）；触发它的事件（_due）：还没 refresh 过、
# console 下单成功后的 KICKS 定点、客户端自己重查了持仓却与成交对不上（交易端成交了）。
# 定时刷新与手动刷新走前端 /api/refresh，同一个 refresh。
# 持仓之外要盯行情的证券（调仓名单）经 watch 并进同一路行情；订阅者（pos 帧与调仓帧）经 sub 计数，
# 每推一帧把 State 交给 hooks（调仓据此重算）。

import contextlib
import threading
import time

from mem import common as memcommon
from mem import fills as memfills
from mem import positions as mempos

from . import board, sina, ui
from .feed import Feed, err

TICK = 0.1
QUOTE_GAP = 1.5  # 新浪每 3s 一帧，1.5s 取一次：延迟 ≤1.5s、约 40 次/分
FULL_EVERY = 60  # 全量扫描最长间隔：补全进程「活动块唯一」校验、跟上客户端换进程
KICKS = (1, 6)  # console 下单成功后隔几秒各刷新一次（限价单可能稍后才成交）
SYNC_TRIES = 2  # 一次 refresh 里成交与持仓对不上（两次查询之间又成交）时最多连查几轮
# (State 字段, 交易栏页签, 方案)：先成交后持仓，最后停在持仓页
SOURCES = (
    ("fills", "成交", memfills),
    ("pos", "持仓", mempos),
)


class Hub:
    def __init__(self):
        # 管以下各项：后台循环与 /api/refresh 互斥
        self.lock = threading.RLock()
        # 快照 None = 还没扫过
        self.st = board.State(None, None, {})
        # 当前帧所据的 st；置 None 即下一轮必重算、推帧
        self.shown = None
        self.subs = 0
        self.scan_at = self.quote_at = 0.0
        # 最近一次 refresh 读到的持仓快照（None = 还没 refresh 过）：客户端自己换了代才再核对
        self.checked = None
        # 待到点刷新的时刻（monotonic）
        self.kicks = []
        # 持仓之外一并取行情的证券 {代码: {code, market_flag}}
        self.extra = {}
        self.hooks = []
        self.frame = {}
        self.feed = Feed(lambda seen: self.frame)

    def _set(self, **kw):
        self.st = self.st._replace(**kw)

    def _scan(self):
        """一遍全量扫出 SOURCES 各方案快照（并重找进程）。须持 lock。"""
        self.scan_at = time.monotonic()
        snaps = memcommon.read([scheme for _, _, scheme in SOURCES])
        self._set(**{key: snap for (key, _, _), snap in zip(SOURCES, snaps)})

    def _load(self):
        """还没扫过即全量扫一次。须持 lock。"""
        if self.st.pos is None:
            self._scan()

    def _secs(self):
        """要取行情的：持仓、今日清仓与 extra → {代码: 证券}。须持 lock。"""
        return {x["code"]: x for x in [*self.extra.values(), *(h or f for h, f in board.join(self.st))]}

    def _quote(self):
        self.quote_at = time.monotonic()
        return sina.quotes(list(self._secs().values()))

    def watch(self, secs):
        """持仓之外一并取行情的证券换成 secs，下一轮即重取。"""
        with self.lock:
            self.extra = {x["code"]: x for x in secs}
            self.shown = None

    def _due(self, now):
        """该让客户端重查成交与持仓了（事件见本节开头）。须持 lock。"""
        if self.checked is None:
            return True
        if self.kicks and now >= self.kicks[0]:
            self.kicks.pop(0)
            return True
        return self.st.pos != self.checked and not board.synced(self.st)

    def kick(self):
        """console 下单成功后：隔 KICKS 秒各刷新一次。"""
        with self.lock:
            now = time.monotonic()
            self.kicks = sorted(self.kicks + [now + k for k in KICKS])

    def _push(self, frame):
        with self.feed:
            self.frame = frame

    def step(self):
        now = time.monotonic()
        with self.lock:
            if not self.subs:
                return
            if self.st.pos is None or now - self.scan_at >= FULL_EVERY:
                self._scan()
            else:
                self._set(pos=memcommon.follow(self.st.pos))
            due = self._due(now)
        # 下单占着界面就等下一轮
        if due and not ui.UI.locked():
            self.refresh()
        with self.lock:
            if self.st != self.shown or now - self.quote_at >= QUOTE_GAP:
                self._set(quotes=self._quote())
            if self.st != self.shown:
                self.shown = self.st
                self._push(board.view(self.st))
                for fn in self.hooks:
                    fn(self.st)

    def run(self):
        while True:
            try:
                self.step()
            except Exception as e:  # 错误边界：推 {err}，恢复后必推新帧
                with self.lock:
                    self.shown = None
                self._push({"err": err(e)})
                time.sleep(1)
            time.sleep(TICK)

    def refresh(self):
        """SOURCES 依次点交易栏页签、点「刷新」，等该方案换代（各实测约 0.25s），推帧。
        两次查询之间又有成交即对不上，连查至多 SYNC_TRIES 轮；仍不对留给前端标出，不再自动重刷。"""
        with ui.UI, self.lock:
            self._load()
            for _ in range(SYNC_TRIES):
                for key, tab, _ in SOURCES:
                    old = getattr(self.st, key)
                    ui.click_tool(tab)
                    ui.click_tool("刷新")
                    self._set(**{key: memcommon.wait(old)})
                if board.synced(self.st):
                    break
            self.checked = self.st.pos
            # 没换代也推一帧，前端借此结束「刷新中」
            self.shown = None
        return {}

    @contextlib.contextmanager
    def sub(self):
        """订阅期间循环转着；到来即重取行情、重算推帧。"""
        with self.lock:
            self.subs += 1
            self.shown = None
        try:
            yield
        finally:
            with self.lock:
                self.subs -= 1

    def frames(self):
        """SSE：同 Feed，订阅期间循环转着。"""
        with self.sub():
            yield from self.feed.watch()

    def state(self):
        """当前 State：还没扫过即全量扫一次，行情没覆盖到 _secs 即当场补取。"""
        with self.lock:
            self._load()
            if not self._secs().keys() <= self.st.quotes.keys():
                self._set(quotes=self._quote())
            return self.st

    def held(self, code):
        """持仓里 code 那只。"""
        with self.lock:
            self._load()
            h = self.st.pos.by_code.get(code)
        assert h, f"持仓里没有 {code}"
        return h

    def quote(self, code):
        """持仓页行情里 code 那只（持仓与今日清仓）。"""
        with self.lock:
            q = self.st.quotes.get(code)
        assert q, f"持仓页行情里没有 {code}"
        return q


HUB = Hub()
