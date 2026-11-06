# desk/ui — 客户端界面自动化的公共件：shim 控制通道与接入、界面锁、按绘制文字点按钮、对话框控件、交易功能树与查询页。

import contextlib
import re
import threading
import time

from bridge.shimctl import Shim, ensure

SHIM = Shim()


class Gate:
    """客户端界面自动化（刷新 / 下单 / 两融条款 / 调仓）一次只许一路：with UI(名): …；who 为占着的那路（None = 空闲）。
    同一线程可重入（调仓整批占着，逐单再进），who 记最外层。"""

    def __init__(self):
        self.lock = threading.RLock()
        self.who = None

    @contextlib.contextmanager
    def __call__(self, who):
        with self.lock:
            outer = self.who
            self.who = outer or who
            try:
                yield
            finally:
                self.who = outer


UI = Gate()
WM_VSCROLL, WM_COMMAND, SB_PAGEDOWN, SB_TOP = 0x115, 0x111, 3, 6
# 主窗交易栏：页签「成交」「持仓」与「刷新」都是画在其上的字；刷新只刷当前页
TOOLBAR = {"class": "MHPToolBar", "title": "MainViewBar"}
# 交易栏左侧功能树（查询 / 资产分析 / …），右侧为所选功能的页；树项只认真实光标处的点击，clickat 会钉光标
FUNC_TREE = ("SysTreeView32", 59653)
# 主窗顶栏的「交易」：交易栏收起（整个停靠条高 0、功能树与 MainViewBar 都不可见）时点它展开
TRADE_NAV = {"class": "AfxWnd100", "text": "交易"}
# 查询页：列表上方「第1--N条」/「共N条」、「查询全部」按钮
PAGE_COUNT, PAGE_ALL = 166, 454
COUNT_RE = re.compile(r"(?:第1--|共)(\d+)条")


def attach():
    """接入客户端：shim 不在线即编译 + 注入；再开 noraise 不抬窗（重扫模块，补钩 shim 注入后才加载的 DLL）、
    gdi 绘制层（首次装入口钩子并重绘全部窗口）。启动时与客户端重启后各一次。"""
    ensure()
    for cmd in ("noraise", "gdi"):
        r = SHIM(cmd, "on")
        assert r[-1]["on"] and not any("skip" in x for x in r), f"{cmd} on: {r}"


def post(h, msg, wp, lp=0):
    assert SHIM("post", h, msg, wp, lp)[0]["ret"], f"PostMessage {h} {msg:#x} 失败"


def title(h):
    return SHIM("rect", h)[0]["title"]


def text(h, timeout=5):
    return SHIM("gettext", h, timeout=timeout)[0]["text"]


def ctls(h, ids):
    """对话框 h 的直接子控件，按 {名: 控件 id} 各取唯一一个 → {名: hwnd}。"""
    kids = [n for n in SHIM("enum", h) if n["depth"] == 1]
    out = {}
    for k, cid in ids.items():
        hits = [n["hwnd"] for n in kids if n["id"] == cid]
        assert len(hits) == 1, f"{h}「{title(h)}」控件 {k}({cid}) 命中 {len(hits)} 个"
        out[k] = hits[0]
    return out


def command(dlg, cid):
    """点对话框 dlg 上 id 为 cid 的按钮：向 dlg 投 WM_COMMAND(cid, 按钮)。"""
    post(dlg, WM_COMMAND, cid, ctls(dlg, {"btn": cid})["btn"])


def wait(what, fn, timeout):
    """what 可为可调用（超时时才求值，好报出当时的状态）。"""
    end = time.monotonic() + timeout
    while not (r := fn()):
        assert (
            time.monotonic() < end
        ), f"{timeout}s 内未等到{what() if callable(what) else what}"
        time.sleep(0.05)
    return r


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


def click_tool(text):
    """点交易栏上画着 text 的按钮。"""
    return click_text({**TOOLBAR, "text": text})


def _func_trees():
    cls, cid = FUNC_TREE
    return [
        n for n in SHIM("tree") if n["vis"] and n["class"] == cls and n["id"] == cid
    ]


def show_bar():
    """交易栏收起了即点顶栏「交易」，等功能树出来。用交易栏（页签 / 刷新 / 功能树）的事务开头各调一次。"""
    if not _func_trees():
        click_text(TRADE_NAV)
        wait("点顶栏「交易」后交易栏展开", _func_trees, 5)


def click_func(name):
    """点交易功能树上的 name：树只给可见项，滚到顶后逐页往下找，翻不动即到底。"""
    trees = _func_trees()
    assert len(trees) == 1, f"交易功能树命中 {len(trees)} 个"
    t = trees[0]
    SHIM("send", t["hwnd"], WM_VSCROLL, SB_TOP, 0)
    seen = None
    while True:
        items = SHIM("detail", t["hwnd"])[0]["items"]
        hits = [it for it in items if it[5] == name]
        if hits:
            l, tp, r, b = hits[0][:4]
            return SHIM(
                "clickat",
                t["hwnd"],
                t["rect"][0] + (l + r) // 2,
                t["rect"][1] + (tp + b) // 2,
            )
        names = [it[5] for it in items]
        assert names != seen, f"交易功能树里没有「{name}」"
        seen = names
        SHIM("send", t["hwnd"], WM_VSCROLL, SB_PAGEDOWN, 0)


def _pages(timeout):
    """可见的查询页（条数标签所在的对话框）。"""
    nodes = SHIM("tree", timeout=timeout)
    out = []
    for i, n in enumerate(nodes):
        if n["vis"] and n["class"] == "Static" and n["id"] == PAGE_COUNT:
            j = i
            while nodes[j]["depth"] >= n["depth"]:
                j -= 1
            out.append(nodes[j]["hwnd"])
    return out


def _cols(dlg, timeout):
    """查询页列表的表头各列名。detail 在界面线程里只等 3s，先 gettext（一直等）把卡顿等过去。"""
    text(dlg, timeout)
    hdr = [
        n["hwnd"]
        for n in SHIM("tree", dlg, timeout=timeout)
        if n["vis"] and n["class"] == "SysHeader32"
    ]
    return [
        it[5]
        for h in hdr
        for it in SHIM("detail", h, timeout=timeout)[0]["items"]
        if len(it) > 5
    ]


def open_page(func, col, timeout=180):
    """功能树点 func，等表头含 col 的查询页出来 → 其 hwnd；页一开客户端即重查（大表要卡一阵），故等得长。"""
    click_func(func)
    return wait(
        f"「{func}」页（表头含「{col}」）",
        lambda: next((d for d in _pages(timeout) if col in _cols(d, timeout)), None),
        timeout,
    )


def count_label(dlg):
    """查询页 dlg 的条数标签 → hwnd（客户端卡着时 enum 也读不到，须事先取好）。"""
    return ctls(dlg, {"n": PAGE_COUNT})["n"]


def page_count(lbl, timeout=5):
    """条数标签 lbl 上的列表条数（「第1--N条」/「共N条」），重查中标签清空回 None；客户端卡着时 gettext 一直等到它缓过来。"""
    t = text(lbl, timeout)
    if not t:
        return None
    m = COUNT_RE.fullmatch(t)
    assert m, f"条数标签读到 {t!r}"
    return int(m.group(1))
