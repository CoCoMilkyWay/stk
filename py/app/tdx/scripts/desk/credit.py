# desk/credit — 信用账户：两融条款（标的证券 × 担保品，按日落盘）、账户（信用资产 + 融资负债，随查随读）、
# 可用保证金随持仓与行情的现值。
#
# 条款：功能树开「标的证券查询」「担保品查询」页各点「查询全部」，等内存里拼出行数恰为页上条数的整表
#   （担保品近万只，客户端要卡一两分钟），合成 {代码: Term} 落盘 data/credit/terms-<日期>.csv，当日再用即读盘。
#   两表以 (沪深标志, 代码) 为键，合成时按代码归一（跨市场同号的只留股票 / 基金那只）。
#   ratio 融资保证金比例（不在标的表为 None）、financeable 可融资买入（融资标的且融资状态正常）、
#   rate 担保折算率（不在担保品表为 0）、pledgeable 可作担保品买入（担保状态正常）。
# 账户：持仓推送的刷新一轮里接在持仓之后，开「信用资产」「融资负债汇总」页，common.ask 读本次应答，
#   记下同一轮的持仓股数与读完即取的现价（校准用）。
# 可用保证金（券商口径）X = 现金 + S − 负债利息，持仓部分
#   S = Σ 折算率 × 非融资部分市值 + Σ g(融资部分市值 − 负债金额) − Σ 负债金额 × 融资保证金比例，
#   融资部分股数 = min(负债数量, 持仓)，g(x) = 折算率 × x（浮盈） / x（浮亏）。
#   公式与券商口径差几千元，现值按读时校准：X = 读时客户端 X + S(现在) − S(读时)。

import collections
import glob
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from mem import common, tables

from . import ROOT, load_csv, sina, save_csv, ui

DIR = os.path.join(ROOT, "data", "credit")
# (功能树项, 认页的表头列, 表)
PAGES = (
    ("标的证券查询", "融资保证金比例", tables.MARGIN),
    ("担保品查询", "担保折算率", tables.COLLAT),
)
FULL_WAIT = 300  # 查询全部最长等多久（秒）
LIVE_GAP = 1  # 拼表期间读内存、报实时进度的间隔（秒）
NORMAL = "正常"
# 条款各字段 → (落盘, 读盘)
# fmt: off
TERM = {
    "code":        (str,                                       str),
    "name":        (str,                                       str),
    "market_flag": (str,                                       int),
    "ratio":       (lambda v: "" if v is None else v,          lambda s: float(s) if s else None),
    "financeable": (int,                                       lambda s: s == "1"),
    "rate":        (str,                                       float),
    "pledgeable":  (int,                                       lambda s: s == "1"),
}
# fmt: on
Term = collections.namedtuple("Term", TERM)
# 读时：时刻、担保资产、可融额度余量（最高融资额度 − 融资负债含费用）、负债总额、净资产、
# 可用保证金、现金、{代码: (负债数量, 负债金额)}、同一轮持仓 {代码: 股}、读完即取的现价 {代码: 价}
Acct = collections.namedtuple("Acct", "at assets line debt net avail cash debts bal px")


def _path(day):
    return os.path.join(DIR, f"terms-{day}.csv")


def _full(log, live, func, col, t):
    """开 func 页、点查询全部，等内存整表行数到页上条数 → {(沪深标志, 代码): 行}。
    进度：开页、点查询全部、（页上条数, 内存行数）每变一次记 log；开页时报 live {func}，
    拼表期间每 LIVE_GAP 秒报 {func, s 已等秒数, got 内存行数, n 页上条数, hung 条数标签多久没读到}。
    条数标签经客户端界面线程读，客户端卡着时读不动，故另起线程读；内存直接读，不受卡顿影响。"""
    log("page", func=func)
    live(func=func)
    dlg = ui.open_page(func, col)
    lbl = ui.count_label(dlg)
    ui.command(dlg, ui.PAGE_ALL)
    log("all", func=func)
    t0 = time.monotonic()
    end, seen, label, stop = t0 + FULL_WAIT, None, (None, t0), threading.Event()

    def poll():
        nonlocal label
        while not stop.is_set():
            label = ui.page_count(lbl, timeout=FULL_WAIT), time.monotonic()
            stop.wait(LIVE_GAP)

    with ThreadPoolExecutor(1) as ex:
        fut = ex.submit(poll)
        try:
            while True:
                if fut.done():
                    fut.result()  # poll 不到 stop 不会结束，结束即出错，在此抛出
                n, at = label
                rows = common.read([t])[0].by_code
                now = time.monotonic()
                live(
                    func=func,
                    s=round(now - t0),
                    got=len(rows),
                    n=n,
                    hung=round(now - at),
                )
                if (n, len(rows)) != seen:
                    seen = n, len(rows)
                    log("rows", func=func, n=n, got=len(rows))
                if len(rows) == n:
                    return rows
                assert (
                    now < end
                ), f"{FULL_WAIT}s 内「{func}」没拼齐（页上 {n} 条，内存 {len(rows)} 行）"
                time.sleep(LIVE_GAP)
        finally:
            stop.set()


def _term(key, mg, co):
    m, c = mg.get(key), co.get(key)
    x = c or m
    return Term(
        key[1],
        x["name"],
        x["market_flag"],
        m["ratio"] if m else None,
        bool(m and "融资标的" in m["remark"] and m["status"] == NORMAL),
        c["rate"] if c else 0.0,
        bool(c and c["status"] == NORMAL),
    )


def _merge(mg, co):
    """两表 {(沪深标志, 代码): 行} → {代码: Term}；跨市场同号的（沪市债券与深市基金）只留 common.market 那只。"""
    keys = collections.defaultdict(list)
    for k in sorted(mg.keys() | co.keys()):
        keys[k[1]].append(k)
    out = {}
    for code, ks in keys.items():
        if len(ks) > 1:
            ks = [k for k in ks if k[0] == common.market(code)]
            assert len(ks) == 1, f"{code} 跨市场同号，按代码定不出市场"
        out[code] = _term(ks[0], mg, co)
    return out


def pull(log, live):
    """客户端重查两融条款、落盘当日一份（删旧的）→ {代码: Term}；log(kind, **kw) 记进度、live(**kw) 报实时进度。"""
    with ui.UI("两融条款"):
        mg, co = (_full(log, live, *p) for p in PAGES)
        ui.click_tool("持仓")
    terms = _merge(mg, co)
    _save(terms)
    return terms


def _save(terms):
    p = _path(time.strftime("%Y%m%d"))
    save_csv(
        p, TERM, ([w(v) for (w, _), v in zip(TERM.values(), t)] for t in terms.values())
    )
    for old in glob.glob(_path("*")):
        if old != p:
            os.remove(old)


def load():
    """当日落盘的条款 → {代码: Term}；当日还没拉过回 None。"""
    rows = load_csv(_path(time.strftime("%Y%m%d")), TERM)
    if rows is None:
        return None
    terms = (
        Term(*(r(s) for (_, r), s in zip(TERM.values(), row, strict=True)))
        for row in rows
    )
    return {t.code: t for t in terms}


def surplus(bal, debts_, px, terms):
    """持仓部分 S：bal {代码: 股}、debts_ {代码: (负债数量, 负债金额)}、px {代码: 价} 须含两者各只。"""
    s = 0.0
    for code in bal.keys() | debts_.keys():
        n, (dq, amt) = bal.get(code, 0.0), debts_.get(code, (0.0, 0.0))
        t = terms.get(code)
        assert t, f"两融条款里没有 {code}"
        p, fin = px[code], min(dq, n)
        x = fin * p - amt
        s += t.rate * (n - fin) * p + (t.rate * x if x > 0 else x)
        if amt:
            assert t.ratio is not None, f"{code} 有融资负债却不在标的表"
            s -= amt * t.ratio
    return s


def balances(pos):
    return {k: h["balance"] for k, h in pos.by_code.items()}


def account(pos):
    """客户端重查信用资产与融资负债 → Acct（须持 ui.UI，停在融资负债汇总页）；pos 为同一轮刚读的持仓。"""
    c = common.ask(tables.CREDIT, lambda: ui.click_func("信用资产")).by_code
    d = common.ask(tables.DEBTS, lambda: ui.click_func("融资负债汇总")).by_code
    secs = {r["code"]: r for r in [*pos.by_code.values(), *d.values()]}
    return Acct(
        time.strftime("%H:%M:%S"),
        c["担保资产"],
        c["最高融资额度"] - c["融资负债（含费用）"],
        c["负债总额"],
        c["净资产"],
        c["可用保证金"],
        c["现金资产"],
        {k: (r["qty"], r["amount"]) for k, r in d.items()},
        balances(pos),
        {k: q["price"] for k, q in sina.quotes(list(secs.values())).items()},
    )


def avail(acct, bal, quotes, terms):
    """持仓为 bal、行情为 quotes 时的可用保证金（不含现金变动）= 读时客户端 X + S(现在) − S(读时)。"""
    now = {k: quotes[k]["price"] for k in bal.keys() | acct.debts.keys()}
    return (
        acct.avail
        + surplus(bal, acct.debts, now, terms)
        - surplus(acct.bal, acct.debts, acct.px, terms)
    )
