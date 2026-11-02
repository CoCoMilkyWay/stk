# desk/ui — 客户端界面自动化的公共件：shim 控制通道、界面锁、按绘制文字点按钮。

import threading

from bridge.shimctl import Shim

SHIM = Shim()
# 客户端界面自动化（刷新点按钮 / 下单走闪电窗）一次只许一路
UI = threading.Lock()
# 主窗交易栏：页签「成交」「持仓」与「刷新」都是画在其上的字；刷新只刷当前页
TOOLBAR = {"class": "MHPToolBar", "title": "MainViewBar"}


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
