# desk/hub — 持仓推送与刷新（两个后台线程共用一份 State）。
# 状态 State = (持仓快照, 成交快照, 挂单快照, 信用账户, 行情, 指数名单)，帧是它的纯函数（board.view）加刷新进度 prog：
# 与当前帧所据的不同才重算、推给全部 pos 订阅。
# 推送线程（run）管全部订阅者，有订阅者才转：每 TICK follow 持仓（约 0.03ms；客户端自己重查过，如成交后，即换代），
# 每 FULL_EVERY 一遍全量扫出 SCANNED 各表；每 QUOTE_GAP 取一次行情，State 变了即补取（保证覆盖新的那只）。
# 行情两源各只此一路：持仓与调仓名单走新浪（盘口列同帧复用其 5 档），指数名单（INDEX_FILE，每轮重读）走通达信；
# 开几个页面都不加量，免被限频封禁。
# 刷新线程（_refresher）一轮按 REFRESH 让客户端依次重查成交、持仓（两表对不上再连查）、挂单、信用账户，
# 每查完一样即并进 State、随下一帧推出，不等整轮；进度（各样名称 / 等界面 / 正查哪样 / 各样读于几时 / 上轮失败原因）同帧推。
# 一轮的起因（_due）：有人要（request / sync）、有订阅者而还没刷过或距上轮满 every 秒（订阅者久别重连即刷）、
# console 下单成功后的 KICKS 定点、客户端自己重查了持仓却与成交对不上（交易端成交了）。失败不自动重试，等下一个起因。
# 任一线程出错，推送线程下一轮先接回客户端（shim 不在线即重注入）、全量重扫（重找进程）。
# 持仓之外要盯行情的证券（调仓名单）经 watch 并进同一路行情；订阅者（pos 帧与调仓帧）经 sub 计数，
# 每推一帧把 State 交给 hooks（调仓据此重算）。

import contextlib
import os
import re
import threading
import time

from mem import common as memcommon
from mem import tables

from . import ROOT, board, credit, load_csv, sina, tdxhq, ui
from .feed import Feed, err

TICK = 0.1
QUOTE_GAP = 1.5  # 新浪每 3s 一帧，1.5s 取一次：延迟 ≤1.5s、约 40 次/分
FULL_EVERY = 60  # 全量扫描最长间隔：补全进程「活动块唯一」校验、跟上客户端换进程
EVERY = 60  # 定时刷新周期（秒，0 = 不定时），页上可改
KICKS = (1, 6)  # console 下单成功后隔几秒各刷新一次（限价单可能稍后才成交）
SYNC_TRIES = 2  # 一轮里成交与持仓对不上（两次查询之间又成交）时最多连查几遍
# 推送线程全量扫的：State 字段 → 表
SCANNED = {"fills": tables.FILLS, "pos": tables.POSITIONS}
# 指数名单：每行 sym（市场前缀 + 6 位代码）, name，行序即显示序；没有文件即无
INDEX_FILE = os.path.join(ROOT, "index.csv")
MARKET_OF = {v: k for k, v in memcommon.MARKETS.items()}


def _indices():
    """INDEX_FILE → 证券 [{code, name, market_flag}]。"""
    out = []
    for s, name in load_csv(INDEX_FILE, ("sym", "name")) or []:
        assert re.fullmatch(
            rf"({'|'.join(MARKET_OF)})\d{{6}}", s
        ), f"{INDEX_FILE} 代码 {s!r}"
        out.append({"code": s[2:], "name": name, "market_flag": MARKET_OF[s[:2]]})
    return out


def _tab(tab, old):
    """点交易栏页签 tab、点「刷新」，等快照 old 换代（实测约 0.25s）。"""
    ui.click_tool(tab)
    ui.click_tool("刷新")
    return memcommon.wait(old)


def _orders():
    """「撤单」页：切页与刷新同在 ask 内（切页自带一次查询，其应答可能早于刷新）。"""
    ui.click_tool("撤单")
    ui.click_tool("刷新")


# 刷新一轮 (State 字段, 名称, State → 新的那样)，须持 ui.UI；前 PAIRED 样须彼此对得上，对不上即连查
# fmt: off
REFRESH = (
    ("fills",  "成交",     lambda st: _tab("成交", st.fills)),
    ("pos",    "持仓",     lambda st: _tab("持仓", st.pos)),
    ("orders", "挂单",     lambda st: memcommon.ask(tables.ORDERS, _orders)),
    ("acct",   "信用账户", lambda st: credit.account(st.pos)),
)
# fmt: on
PAIRED = 2


class Hub:
    def __init__(self):
        # 管以下各项：两个线程与各请求互斥；cv 等刷新轮次
        self.lock = threading.RLock()
        self.cv = threading.Condition(self.lock)
        # 快照 None = 还没扫过 / 还没读过
        self.st = board.State(None, None, None, None, {}, [])
        # 当前帧所据的 st；置 None 即下一轮必重算、推帧
        self.shown = None
        self.subs = 0
        self.scan_at = self.quote_at = 0.0
        # 出过错：推送线程下一轮先接回客户端、全量重扫
        self.lost = False
        # 刷新轮次：要到第几轮 / 刷完第几轮，上一轮结束时刻（0 = 还没刷过）与当时的持仓快照
        self.asked = self.done = 0
        self.last = 0.0
        self.checked = None
        # 待到点刷新的时刻（monotonic）
        self.kicks = []
        # 持仓之外一并取行情的证券 {代码: {code, market_flag}}
        self.extra = {}
        self.hooks = []
        self.frame = {}
        # 刷新进度：steps 各样 (State 字段, 名称)，step 正做的（None = 闲），at {State 字段: 读于}，
        # err 上一轮失败原因，every 定时周期
        self.prog = {
            "steps": [(key, name) for key, name, _ in REFRESH],
            "step": None,
            "at": {},
            "err": None,
            "every": EVERY,
        }
        self.feed = Feed(lambda seen: self.frame | {"prog": self._prog()})

    def _set(self, **kw):
        self.st = self.st._replace(**kw)

    def _note(self, **kw):
        """改刷新进度并推帧。"""
        with self.feed:
            self.prog = self.prog | kw

    def _prog(self):
        """进度另带 acct_stale：信用账户读于持仓变动之前（新一轮还没查到账户）。"""
        a, p = self.st.acct, self.st.pos
        return self.prog | {"acct_stale": bool(a and p and a.bal != credit.balances(p))}

    def _fresh(self, key, snap):
        """客户端重查过的一样并进 State，记下读于几时。"""
        with self.lock:
            self._set(**{key: snap})
        with self.feed:
            self.prog = self.prog | {
                "at": self.prog["at"] | {key: time.strftime("%H:%M:%S")}
            }

    def _scan(self):
        """一遍全量扫出 SCANNED 各表快照（并重找进程）。须持 lock。"""
        self.scan_at = time.monotonic()
        self._set(**dict(zip(SCANNED, memcommon.read(list(SCANNED.values())))))

    def _load(self):
        """还没扫过即全量扫一次。须持 lock。"""
        if self.st.pos is None:
            self._scan()

    def _secs(self):
        """要取行情的：持仓、今日清仓与 extra → {代码: 证券}。须持 lock。"""
        return {
            x["code"]: x
            for x in [*self.extra.values(), *(h or f for h, f in board.join(self.st))]
        }

    def _quote(self):
        """两源各取一遍并进 State：_secs 走新浪、指数名单走通达信，两源代码不重。须持 lock。"""
        self.quote_at = time.monotonic()
        idx = _indices()
        a, b = sina.quotes(list(self._secs().values())), tdxhq.quotes(idx)
        assert (
            not a.keys() & b.keys()
        ), f"指数与持仓 / 调仓名单代码重了：{sorted(a.keys() & b.keys())}"
        self._set(quotes=a | b, indices=idx)

    def watch(self, secs):
        """持仓之外一并取行情的证券换成 secs，下一轮即重取。"""
        with self.lock:
            self.extra = {x["code"]: x for x in secs}
            self.shown = None

    def kick(self):
        """console 下单成功后：隔 KICKS 秒各刷新一次。"""
        with self.lock:
            now = time.monotonic()
            self.kicks = sorted(self.kicks + [now + k for k in KICKS])

    def _push(self, frame):
        with self.feed:
            self.frame = frame

    # ---------- 推送线程 ----------

    def step(self):
        now = time.monotonic()
        with self.lock:
            if not self.subs:
                return
            if self.lost:
                # 客户端不在即 assert，不去空注入
                memcommon.attach()
                if not ui.SHIM.online():
                    ui.attach()
            if self.lost or self.st.pos is None or now - self.scan_at >= FULL_EVERY:
                self._scan()
                self.lost = False
            else:
                pos = memcommon.follow(self.st.pos)
                if pos != self.st.pos:
                    self._fresh("pos", pos)
            if self.st != self.shown or now - self.quote_at >= QUOTE_GAP:
                self._quote()
            if self.st != self.shown:
                self.shown = self.st
                self._push(board.view(self.st))
                for fn in self.hooks:
                    fn(self.st)

    def run(self):
        threading.Thread(target=self._refresher, daemon=True).start()
        while True:
            try:
                self.step()
            except Exception as e:  # 错误边界：推 {err}，接回后必推新帧
                with self.lock:
                    self.shown, self.lost = None, True
                self._push({"err": err(e)})
                time.sleep(1)
            time.sleep(TICK)

    # ---------- 刷新线程 ----------

    def _due(self, now):
        """该刷新一轮了（起因见本节开头）。须持 lock。"""
        if self.asked > self.done:
            return True
        if not self.subs:
            return False
        if self.kicks and now >= self.kicks[0]:
            self.kicks.pop(0)
            return True
        every = self.prog["every"]
        if not self.last or (every and now - self.last >= every):
            return True
        return (
            self.checked is not None
            and self.st.pos != self.checked
            and not board.synced(self.st)
        )

    def _steps(self, steps):
        for key, name, fetch in steps:
            self._note(step=name)
            self._fresh(key, fetch(self.st))

    def _round(self):
        """一轮：REFRESH 依次查；前 PAIRED 样（成交、持仓）两次查询之间又有成交即对不上，连查至多 SYNC_TRIES 遍，
        仍不对留给前端标出；查完回持仓页。撤单页上另画着一个撤单动作键，故切去时须在持仓页（页签唯一）。"""
        if ui.UI.who:
            self._note(step=f"等界面（{ui.UI.who}）")
        with ui.UI("刷新"):
            with self.lock:
                self._load()
            ui.show_bar()
            for _ in range(SYNC_TRIES):
                self._steps(REFRESH[:PAIRED])
                if board.synced(self.st):
                    break
            self._steps(REFRESH[PAIRED:])
            ui.click_tool("持仓")

    def _tick(self):
        with self.cv:
            if not self.cv.wait_for(lambda: self._due(time.monotonic()), TICK):
                return
            n = self.asked
        msg = None
        try:
            self._round()
        except Exception as e:  # 错误边界：报在进度里，等下一个起因再刷
            msg = err(e)
        with self.cv:
            self.checked, self.last, self.done = self.st.pos, time.monotonic(), n
            self.lost = self.lost or msg is not None
            self._note(step=None, err=msg)
            self.cv.notify_all()

    def _refresher(self):
        while True:
            try:
                self._tick()
            except (
                Exception
            ) as e:  # 错误边界：起因判不了（State 自相矛盾）也只报在进度里
                self._note(step=None, err=err(e))
                time.sleep(1)

    def request(self):
        """要一轮刷新（不等）→ 轮号；进度与各样结果随帧推。"""
        with self.cv:
            self.asked += 1
            self.cv.notify_all()
            return self.asked

    def sync(self):
        """要一轮刷新并等它刷完，失败即 assert。"""
        n = self.request()
        with self.cv:
            self.cv.wait_for(lambda: self.done >= n)
            assert not self.prog["err"], f"刷新失败：{self.prog['err']}"

    def every(self, s):
        """定时刷新周期改为 s 秒（0 = 不定时）。"""
        assert s >= 0, f"刷新周期 {s}"
        with self.cv:
            self._note(every=s)
            self.cv.notify_all()
        return {}

    # ---------- 订阅与取数 ----------

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
                self._quote()
            return self.st

    def held(self, code):
        """持仓里 code 那只。"""
        with self.lock:
            self._load()
            h = self.st.pos.by_code.get(code)
        assert h, f"持仓里没有 {code}"
        return h

    def quote(self, code):
        """持仓页行情里 code 那只（持仓、今日清仓与指数）。"""
        with self.lock:
            q = self.st.quotes.get(code)
        assert q, f"持仓页行情里没有 {code}"
        return q


HUB = Hub()
