# desk/order — 下单（闪电下单窗 UI 自动化）。
# 一步一函数，ORDER 即流程也即前端步骤条；每步 (o 本单上下文) → 该单要更新的字段：
#   行情  按盘口定委托价
#   切换  图表切到该只（闪电窗只认图表当前证券）：向主窗投代码按键 + 回车（键盘精灵）
#   开窗  主窗 WM_COMMAND 打开闪电窗
#   填单  窗内证券代码再核一遍，填价格 / 数量
#   确认  点提交，「…交易确认」框核对正文（代码 / 价格 / 数量）
#   回报  点 7015，「提示」框读回报（成功含合同号）点 7015 关掉
# 开窗 / 确认 / 回报同形：做一个动作，等它弹出的那个对话框（_popup）。
# 一次只跑一单，逐步推状态；任一步不符或超时即停在那一步，已弹出的窗口留给人工处理。
# 证券 sec = {code, name, market_flag}：持仓行（console 下单）或两融条款里的（调仓买新标的）。
# submit 后台跑（持仓页按钮），place 当前线程跑完回该单（调仓逐单）。

import collections
import itertools
import re
import threading
import time
import types

from mem.common import BUY, SELL

from . import book, sina, ui
from .feed import Feed, err
from .hub import HUB
from .ui import SHIM, UI

WM_KEYDOWN, WM_KEYUP, WM_COMMAND = 0x100, 0x101, 0x111
VK_RETURN, SC_RETURN = 0x0D, 0x1C
WM_MDIGETACTIVE = 0x229
# MFC AFX_IDW_PANE_FIRST：主框架下的 MDIClient、MDI 子窗下的视图都是这个 id
AFX_PANE = 59648
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
ORDER_CAP = 5  # 留、推的最近单数


def _order_qty(o, px, cap):
    """股数：给了 qty 即用，否则 size 元（0 = 最小单位）按 book.lot 折股；再以闪电窗给的最大可买 / 可卖 cap 封顶
    （钱或券不够就按能做的最大量）：买入按步长取整、须够起买量，卖出不足即全卖（零股只能一次卖完）。"""
    lot, step = book.lot(o.code)
    n = o.qty or (int(o.size / px) // step * step if o.size else lot)
    assert n >= (
        lot if o.s == BUY or not o.qty else 1), f"{o.size} 元 / {o.qty} 股按 {px} 不足最小单位 {lot} 股"
    if o.s == BUY:
        cap = cap // step * step
        assert cap >= lot, f"最大可买 {cap} 股，不足最小单位 {lot} 股"
    assert cap >= 1, "最大可卖为 0"
    return min(n, cap)


def _max_qty(h):
    t = ui.text(h)
    assert re.fullmatch(r"\d+(\.\d+)?", t), f"最大可买/可卖读到 {t!r}"
    return int(float(t))


def _press(h, vk, sc):
    """投递按下 + 抬起（同 shim post_key），WM_CHAR 由客户端消息泵照常生成。"""
    lp = 1 | sc << 16
    ui.post(h, WM_KEYDOWN, vk, lp)
    ui.post(h, WM_KEYUP, vk, lp | 0xC0000000)


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
        + ("；".join(f"「{n['title']}」{_body(h)}" for h,
           n in new().items()) or "无"),
        got,
        timeout,
    )


def _bare(s):
    """去掉空白：各表与标题里名称的空格数不一（如「鲁  泰Ａ」「鲁 泰Ａ」）。"""
    return "".join(s.split())


def _goto(main, code, name):
    """图表切到 code：主窗标题已是「…-名称]」即跳过，否则向活动 MDI 子窗的视图投代码按键
    （键盘精灵由视图处理，投给主框架不经过它），等精灵弹出后向其输入框投回车，再等标题换过来。"""
    want = _bare(f"-{name}]")
    if _bare(ui.title(main)).endswith(want):
        return
    mdi = ui.ctls(main, {"mdi": AFX_PANE})["mdi"]
    child = f"0x{SHIM('send', mdi, WM_MDIGETACTIVE, 0, 0)[0]['ret'] & 0xFFFFFFFF:08x}"
    view = ui.ctls(child, {"view": AFX_PANE})["view"]
    before = _tops()
    for ch in code:
        # 数字键扫描码 1..9→2..10、0→11
        _press(view, ord(ch), 11 if ch == "0" else int(ch) + 1)
    spirit = ui.wait(
        f"键盘精灵弹出（已向「{ui.title(child)}」视图 {view} 投 {code}）",
        lambda: next((h for h in _tops() if h not in before), None),
        1,
    )
    # 精灵按输入筛候选，留出时间再回车
    time.sleep(0.3)
    edits = [
        n["hwnd"]
        for n in SHIM("enum", spirit)
        if n["depth"] == 1 and n["class"] == "Edit"
    ]
    _press(edits[0] if len(edits) == 1 else spirit, VK_RETURN, SC_RETURN)
    ui.wait(
        lambda: f"主窗切到「{name}」，现为「{ui.title(main)}」",
        lambda: _bare(ui.title(main)).endswith(want),
        3,
    )


def _settext(h, s):
    assert SHIM("settext", h, s)[0]["ret"], f"写 {h} 失败"
    got = ui.text(h)
    assert got == s, f"写入 {s!r} 后读回 {got!r}"


def _field(body, pat, what):
    m = re.search(pat, body)
    assert m, f"确认框缺{what}：{body}"
    return m.group(1)


def _price(o):
    q = sina.quotes([o.sec])[o.code]
    o.px = book.order_px(q, o.s, o.mode)
    assert o.px, "集合竞价中或盘口为空，无连续价"
    return {}


def _switch(o):
    main = SHIM("find", MAIN_WND)
    assert len(main) == 1, "找不到通达信主窗"
    o.main = main[0]["hwnd"]
    _goto(o.main, o.code, o.sec["name"])
    return {}


def _open(o):
    assert all(n["title"] != o.title for n in _tops().values()
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
    cap = min(cap, int(cap0 * float(dflt) / float(o.px_s))
              if o.s == BUY else cap0)
    o.n = _order_qty(o, float(o.px_s), cap)
    _settext(c["qty"], str(o.n) if unit ==
             "股" else f"{o.n * float(o.px_s):.2f}")
    return {"detail": f"{o.n}股 @ {o.px_s}（最大 {cap}）", "qty": o.n, "px": float(o.px_s)}


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
    res = _popup((RESULT_TITLE,), 10, lambda: ui.command(o.box, MSG_YES))
    body = _body(res)
    ui.command(res, MSG_YES)
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
        # 下单线程改状态，同在 feed 内
        self.jobs = collections.deque(maxlen=ORDER_CAP)
        self.ids = itertools.count(1)
        self.feed = Feed(lambda seen: list(self.jobs)[::-1])

    def _put(self, job, **kw):
        with self.feed:
            job.update(kw)

    def _open(self, sec, s, size, qty, mode, credit):
        """登记一单 → (job, 本单上下文)；量为 size 元（0 = 最小单位）或 qty 股（给了即用），
        mode 为 book.PX_MODES 键，credit 为 plain|margin。"""
        assert (
            (s, credit) in FLASH and size >= 0 and qty >= 0 and mode in book.PX_MODES
        ), f"参数不对：{(sec['code'], s, size, qty, mode, credit)}"
        with self.feed:
            assert not any(j["state"] == "run" for j in self.jobs), "上一单还没结束"
            job = {
                "id": next(self.ids),
                "t": time.strftime("%H:%M:%S"),
                "code": sec["code"],
                "name": sec["name"],
                "s": s,
                "credit": credit,
                "label": f"{qty}股" if qty else f"{size / 10000:.4g}W" if size else "最小",
                "steps": [name for name, _ in ORDER],
                "at": 0,
                "state": "run",
                "detail": "",
                "msg": "",
            }
            self.jobs.append(job)
        cmd, title, ids = FLASH[s, credit]
        o = types.SimpleNamespace(
            sec=sec,
            code=sec["code"],
            s=s,
            size=size,
            qty=qty,
            mode=mode,
            cmd=cmd,
            title=title,
            ids=dict(zip(FLASH_CTLS, ids, strict=True)),
        )
        return job, o

    def submit(self, b):
        """b = {code, s: BUY|SELL, size, px, credit}：持仓里 code 那只，后台跑，回该单。"""
        job, o = self._open(HUB.held(b["code"]), int(
            b["s"]), int(b["size"]), 0, b["px"], b["credit"])
        threading.Thread(target=self._run, args=(job, o), daemon=True).start()
        return job

    def place(self, sec, s, qty, mode, credit):
        """当前线程跑完 qty 股一单 → 该单（state ok / err，填单后带 qty / px；成交另看成交表）。"""
        job, o = self._open(sec, s, 0, qty, mode, credit)
        self._run(job, o)
        return job

    def _run(self, job, o):
        try:
            with UI:
                for at, (_, step) in enumerate(ORDER):
                    self._put(job, at=at)
                    self._put(job, **step(o))
            self._put(job, state="ok")
            HUB.kick()
        except Exception as e:  # 错误边界：停在当前步，报原因
            self._put(job, state="err", msg=err(e))


ORDERS = Orders()
