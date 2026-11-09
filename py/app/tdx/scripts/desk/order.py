# desk/order — 下单（闪电下单窗 UI 自动化）。
# 一步一函数，ORDER 即流程也即前端步骤条；每步 (o 本单上下文) → 该单要更新的字段：
#   行情  按盘口定委托价
#   切换  图表切到该只（闪电窗只认图表当前证券）：向主窗投通达信外部联动消息，等标题换过来
#   开窗  主窗 WM_COMMAND 打开闪电窗
#   填单  窗内证券代码再核一遍，填价格 / 数量（数量单位为元即按额：只收整数元，客户端按价折股、向下取整步长，以预估股数核对）
#   确认  点提交，「…交易确认」框核对正文（代码 / 价格 / 数量）
#   回报  点 7015，「提示」框读回报（成功含合同号）点 7015 关掉，等各框与闪电窗都关了
# 开窗 / 确认 / 回报同形：做一个动作，等它弹出的那个对话框（_popup）。
# 一次只跑一单（feed.Jobs），逐步推状态；任一步不符或超时即停在那一步，已弹出的窗口留给人工处理。
# 证券 sec = common.sec：持仓行（console 下单）或两融条款里的（调仓买新标的）。
# submit 后台跑（持仓页按钮），成功后按 HUB.kick 定点刷新；place 当前线程跑完回该单（调仓逐单），不刷新，由调仓整批下完再刷。

import math
import re
import time
import types

from mem.common import BUY, SELL, SH, SZ, sec

from . import book, sina, ui
from .feed import Feed, Jobs
from .hub import HUB
from .ui import SHIM, UI

WM_COMMAND = 0x111
# 通达信外部联动：注册消息「Stock」投给主窗，wParam = 市场前缀 × 10⁶ + 代码，图表即切到该只
LINK = "Stock"
LINK_MKT = {SZ: 6, SH: 7}
LINK_REPOST, LINK_TIMEOUT = 1, 6  # 标题没换过来每几秒重投、共等几秒
MAIN_WND = "class=TdxW_MainFrame_Class"
# 闪电窗按 (方向, 信用方式) 取：主窗命令 id、窗标题、FLASH_CTLS 各控件 id
# （证券代码 / 价格 / 数量 / 数量单位 / 报价方式 / 最大可买·可卖·可融（股）/ 提交）；卖券还款未录到，没有 (SELL, margin)
FLASH_CTLS = ("code", "price", "qty", "unit", "quote", "max", "ok")
# fmt: off
FLASH = {
    (BUY,  "plain"):  (0x13DD, "闪电买入",     (12005, 12006, 12007, 12016, 3067, 2022, 1)),
    (BUY,  "margin"): (0x1441, "闪电融资买入", (313,   314,   315,   320,   220,  199,  1)),
    (SELL, "plain"):  (0x1404, "闪电卖出",     (12005, 12006, 12007, 12016, 3067, 2065, 1)),
}
# fmt: on
CONFIRM_TITLES = ("买入交易确认", "卖出交易确认", "交易确认")
RESULT_TITLE = "提示"
MSG_TEXT, MSG_YES = 7013, 7015  # 确认框 / 回报框：正文 Static、确认按钮
EST = 8915  # 闪电窗按额时的「预估金额:…(N股)」
EST_RE = re.compile(r"\((\d+)股\)$")
ORDER_CAP = 5  # 留、推的最近单数


def _order_qty(o, px, cap):
    """股数：给了 qty 即用，否则 size 元按 book.lot 折股；再以闪电窗给的最大可买 / 可卖 cap 封顶
    （钱或券不够就按能做的最大量）：买入按步长取整、须够起买量；按额卖出不足起卖量即抬到起卖量，
    可卖不足即全卖（零股只能一次卖完）。"""
    lot, step = book.lot(o.code)
    n = o.qty or int(o.size / px) // step * step
    if o.s == BUY:
        assert n >= lot, f"{o.size} 元 / {o.qty} 股按 {px} 不足起买量 {lot} 股"
        cap = cap // step * step
        assert cap >= lot, f"最大可买 {cap} 股，不足起买量 {lot} 股"
    elif not o.qty:
        n = max(n, lot)
    assert cap >= 1, "最大可卖为 0"
    return min(n, cap)


def _max_qty(h):
    t = ui.text(h)
    assert re.fullmatch(r"\d+(\.\d+)?", t), f"最大可买/可卖读到 {t!r}"
    return int(float(t))


def _tops():
    """可见顶层窗口 {hwnd: 节点}。"""
    return {n["hwnd"]: n for n in SHIM("enum") if n["depth"] == 0 and n["vis"]}


def _body(h):
    """确认 / 提示框正文（7013），没有该控件回空串。"""
    hits = [
        n["hwnd"] for n in SHIM("enum", h) if n["depth"] == 1 and n["id"] == MSG_TEXT
    ]
    return " ".join(ui.text(hits[0]).split()) if len(hits) == 1 else ""


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

    return ui.wait(
        lambda: f"「{want}」，期间新窗口："
        + ("；".join(f"「{n['title']}」{_body(h)}" for h, n in new().items()) or "无"),
        got,
        timeout,
    )


def _bare(s):
    """去掉空白：各表与标题里名称的空格数不一（如「鲁  泰Ａ」「鲁 泰Ａ」）。"""
    return "".join(s.split())


def _goto(main, x):
    """图表切到证券 x：主窗标题已是「…-名称]」即跳过，否则向主窗投联动消息、等标题换过来。
    切到同一只重投无副作用：界面线程忙时消息可能落空，标题没换过来即每 LINK_REPOST 秒重投。"""
    want = _bare(f"-{x['name']}]")
    if _bare(ui.title(main)).endswith(want):
        return
    mkt = x["market_flag"]
    assert mkt in LINK_MKT, f"{x['code']} 沪深标志 {mkt} 没有联动前缀"
    msg = SHIM("regmsg", LINK)[0]["msg"]
    wp = LINK_MKT[mkt] * 10**6 + int(x["code"])
    t0 = time.monotonic()
    n = 0
    while not _bare(ui.title(main)).endswith(want):
        dt = time.monotonic() - t0
        assert dt < LINK_TIMEOUT, (
            f"{LINK_TIMEOUT}s 内主窗未切到「{x['name']}」，现为「{ui.title(main)}」"
            f"（联动 {wp} 已投 {n} 次）"
        )
        if dt >= n * LINK_REPOST:
            ui.post(main, msg, wp)
            n += 1
        time.sleep(0.05)


def _settext(h, s):
    """写入后客户端的 EN_CHANGE 处理会先改写一下，等读回恰为 s。"""
    assert SHIM("settext", h, s)[0]["ret"], f"写 {h} 失败"
    ui.wait(lambda: f"写入 {s!r} 后读回 {ui.text(h)!r}", lambda: ui.text(h) == s, 1)


def _field(body, pat, what):
    m = re.search(pat, body)
    assert m, f"确认框缺{what}：{body}"
    return m.group(1)


def _price(o):
    q = sina.quotes([o.sec])[o.code]
    r = book.ref(q)
    assert r, "无参考价"
    o.px = book.order_px(q, o.s, o.mode, o.size or o.qty * r)
    assert o.px, "集合竞价中或盘口为空，无连续价"
    return {}


def _switch(o):
    main = SHIM("find", MAIN_WND)
    assert len(main) == 1, "找不到通达信主窗"
    o.main = main[0]["hwnd"]
    _goto(o.main, o.sec)
    return {}


def _open(o):
    assert all(
        n["title"] != o.title for n in _tops().values()
    ), f"已有未关的「{o.title}」窗"
    o.dlg = _popup((o.title,), 3, lambda: ui.post(o.main, WM_COMMAND, o.cmd))
    return {}


def _form(o):
    c = ui.ctls(o.dlg, o.ids)
    got = ui.text(c["code"])
    assert got == o.code, f"闪电窗证券 {got} ≠ {o.code}"
    quote = ui.text(c["quote"])
    assert "限价" in quote, f"报价方式为「{quote}」，需限价委托"
    dflt = ui.wait("闪电窗默认价", lambda: ui.text(c["price"]), 1)
    assert "." in dflt, f"默认价 {dflt!r} 定不出小数位"
    o.px_s = f"{o.px:.{len(dflt.split('.')[1])}f}"
    unit = ui.text(c["unit"])
    assert unit in ("股", "元"), f"数量单位「{unit}」不认识"
    ui.wait("闪电窗最大可买/可卖", lambda: ui.text(c["max"]), 1)
    cap0 = _max_qty(c["max"])
    _settext(c["price"], o.px_s)
    # 最大可买按默认价算；改价后客户端未必即时重算，按价反比折一份，与重读的取小
    cap = _max_qty(c["max"])
    cap = min(cap, int(cap0 * float(dflt) / float(o.px_s)) if o.s == BUY else cap0)
    o.n = _order_qty(o, float(o.px_s), cap)
    if unit == "股":
        _settext(c["qty"], str(o.n))
    else:
        # 向上取整到元：多出的不足一股，折回恰为 n 股
        _settext(c["qty"], str(math.ceil(round(o.n * float(o.px_s), 2))))
        est = ui.ctls(o.dlg, {"est": EST})["est"]
        ui.wait(
            lambda: f"预估 {o.n} 股，现为「{ui.text(est)}」",
            lambda: (m := EST_RE.search(ui.text(est))) and int(m.group(1)) == o.n,
            1,
        )
    return {
        "detail": f"{o.n}股 @ {o.px_s}（最大 {cap}）",
        "qty": o.n,
        "px": float(o.px_s),
    }


def _confirm(o):
    o.box = _popup(CONFIRM_TITLES, 5, lambda: ui.command(o.dlg, o.ids["ok"]))
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
    """读回报、点掉；等回报框、确认框、闪电窗都关了才算完（下一单紧接着切图表、开窗）。"""
    res = _popup((RESULT_TITLE,), 10, lambda: ui.command(o.box, MSG_YES))
    body = _body(res)
    ui.command(res, MSG_YES)
    assert "合同号" in body, f"委托未成功：{body}"
    ui.wait(
        lambda: "回报框 / 确认框 / 闪电窗关掉，还开着："
        + "、".join(
            f"「{n['title']}」" for h, n in _tops().items() if h in (res, o.box, o.dlg)
        ),
        lambda: not {res, o.box, o.dlg} & _tops().keys(),
        3,
    )
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
        self.feed = Feed(lambda seen: list(self.jobs.list)[::-1])
        self.jobs = Jobs(self.feed, ORDER_CAP)

    def _new(self, x, s, size, qty, mode, credit):
        """登记一单 → (job, 本单上下文 o)；x 为证券，量为 size 元或 qty 股（二者恰给其一），
        mode 为 book.PX_MODES 键，credit 为 plain|margin。"""
        assert (
            (s, credit) in FLASH
            and min(size, qty) == 0 < max(size, qty)
            and mode in book.PX_MODES
        ), f"参数不对：{(x['code'], s, size, qty, mode, credit)}"
        job = self.jobs.open(
            **sec(x),
            s=s,
            credit=credit,
            label=f"{qty}股" if qty else f"{size / 10000:.4g}W",
            steps=[name for name, _ in ORDER],
            at=0,
            detail="",
        )
        cmd, title, ids = FLASH[s, credit]
        o = types.SimpleNamespace(
            sec=sec(x),
            code=x["code"],
            s=s,
            size=size,
            qty=qty,
            mode=mode,
            cmd=cmd,
            title=title,
            ids=dict(zip(FLASH_CTLS, ids, strict=True)),
        )
        return job, o

    def _steps(self, o, kick):
        def run(job):
            with UI("下单"):
                for at, (_, step) in enumerate(ORDER):
                    self.jobs.put(job, at=at)
                    self.jobs.put(job, **step(o))
            if kick:
                HUB.kick()

        return run

    def submit(self, b):
        """b = {code, s: BUY|SELL, size, px, credit}：持仓里 code 那只，后台跑，回该单。"""
        job, o = self._new(
            HUB.held(b["code"]), int(b["s"]), int(b["size"]), 0, b["px"], b["credit"]
        )
        return self.jobs.spawn(job, self._steps(o, True))

    def place(self, x, s, qty, mode, credit):
        """当前线程跑完 qty 股一单 → 该单（state ok / err，填单后带 qty / px；成交另看成交表）。"""
        job, o = self._new(x, s, 0, qty, mode, credit)
        return self.jobs.run(job, self._steps(o, False))


ORDERS = Orders()
