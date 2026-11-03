# desk/credit — 信用账户：两融条款（标的证券 × 担保品，按日落盘）、账户（信用资产 + 融资负债，随查随读）、
# 可用保证金随持仓与行情的现值。
#
# 条款：功能树开「标的证券查询」「担保品查询」页各点「查询全部」，等内存里拼出行数恰为页上条数的整表
#   （担保品近万只，客户端要卡一两分钟），合成 {代码: Term} 落盘 data/credit/terms-<日期>.csv，当日再用即读盘。
#   两表以 (沪深标志, 代码) 为键，合成时按代码归一（跨市场同号的只留股票 / 基金那只）。
#   ratio 融资保证金比例（不在标的表为 None）、financeable 可融资买入（融资标的且融资状态正常）、
#   rate 担保折算率（不在担保品表为 0）、pledgeable 可作担保品买入（担保状态正常）。
# 账户：开「信用资产」「融资负债汇总」页，common.ask 读本次应答，读完回持仓页；连同读时的持仓与行情算下 base。
# 可用保证金（券商口径）X = 现金 + S − 负债利息，持仓部分
#   S = Σ 折算率 × 非融资部分市值 + Σ g(融资部分市值 − 负债金额) − Σ 负债金额 × 融资保证金比例，
#   融资部分股数 = min(负债数量, 持仓)，g(x) = 折算率 × x（浮盈） / x（浮亏）。
#   公式与券商口径差几千元，现值按读时校准：X = 读时客户端 X + S(现在) − S(读时)。

import collections
import csv
import glob
import os
import time

from mem import collat, common, credit, debts, margin

from . import ROOT, ui

DIR = os.path.join(ROOT, "data", "credit")
# (功能树项, 认页的表头列, 方案)
PAGES = (
    ("标的证券查询", "融资保证金比例", margin),
    ("担保品查询", "担保折算率", collat),
)
FULL_WAIT = 300  # 查询全部最长等多久（秒）
NORMAL = "正常"
Term = collections.namedtuple(
    "Term", "code name market_flag ratio financeable rate pledgeable"
)
TERM_TYPES = (
    str,
    str,
    int,
    lambda s: float(s) if s else None,
    lambda s: s == "1",
    float,
    lambda s: s == "1",
)
# 读时：时刻、担保资产、可融额度余量（最高融资额度 − 融资负债含费用）、负债总额、净资产、
# 可用保证金、现金、{代码: (负债数量, 负债金额)}、读时的 S
Acct = collections.namedtuple("Acct", "at assets line debt net avail cash debts base")


def _path(day):
    return os.path.join(DIR, f"terms-{day}.csv")


def _full(log, func, col, scheme):
    """开 func 页、点查询全部，等内存整表行数到页上条数 → {(沪深标志, 代码): 行}；
    进度记 log：开页、点查询全部、（页上条数, 内存行数）每变一次。"""
    log("page", func=func)
    dlg = ui.open_page(func, col)
    lbl = ui.count_label(dlg)
    ui.command(dlg, ui.PAGE_ALL)
    log("all", func=func)
    end, seen = time.monotonic() + FULL_WAIT, None
    while True:
        n = ui.page_count(lbl, timeout=FULL_WAIT)
        rows = common.read([scheme])[0].by_code
        if (n, len(rows)) != seen:
            seen = n, len(rows)
            log("rows", func=func, n=n, got=len(rows))
        if len(rows) == n:
            return rows
        assert (
            time.monotonic() < end
        ), f"{FULL_WAIT}s 内「{func}」没拼齐（页上 {n} 条，内存 {len(rows)} 行）"
        time.sleep(1)


def _market(code):
    """股票 / 基金代码的沪深标志：北 4 / 8 / 92 开头，沪 5 / 6 / 9，其余深。"""
    return 60 if code.startswith(("4", "8", "92")) else 1 if code[0] in "569" else 0


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
    """两表 {(沪深标志, 代码): 行} → {代码: Term}；跨市场同号的（沪市债券与深市基金）只留 _market 那只。"""
    keys = collections.defaultdict(list)
    for k in sorted(mg.keys() | co.keys()):
        keys[k[1]].append(k)
    out = {}
    for code, ks in keys.items():
        if len(ks) > 1:
            ks = [k for k in ks if k[0] == _market(code)]
            assert len(ks) == 1, f"{code} 跨市场同号，按代码定不出市场"
        out[code] = _term(ks[0], mg, co)
    return out


def pull(log):
    """客户端重查两融条款、落盘当日一份（删旧的）→ {代码: Term}；log(ev, **kw) 收进度。"""
    with ui.UI:
        mg, co = (_full(log, *p) for p in PAGES)
        ui.click_tool("持仓")
    terms = _merge(mg, co)
    _save(terms)
    return terms


def _save(terms):
    os.makedirs(DIR, exist_ok=True)
    day = time.strftime("%Y%m%d")
    with open(_path(day) + ".tmp", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(Term._fields)
        for t in terms.values():
            w.writerow(
                ["" if v is None else int(v) if isinstance(v, bool) else v for v in t]
            )
    os.replace(_path(day) + ".tmp", _path(day))
    for p in glob.glob(_path("*")):
        if p != _path(day):
            os.remove(p)


def load():
    """当日落盘的条款 → {代码: Term}；当日还没拉过回 None。"""
    p = _path(time.strftime("%Y%m%d"))
    if not os.path.exists(p):
        return None
    with open(p, encoding="utf-8") as f:
        r = csv.reader(f)
        assert next(r) == list(Term._fields), f"{p} 表头不对"
        return {
            t.code: t
            for t in (
                Term(*(conv(v) for conv, v in zip(TERM_TYPES, row, strict=True)))
                for row in r
            )
        }


def surplus(bal, debts_, quotes, terms):
    """持仓部分 S：bal {代码: 股}、debts_ {代码: (负债数量, 负债金额)}、quotes 须含两者各只。"""
    s = 0.0
    for code in bal.keys() | debts_.keys():
        n, (dq, amt) = bal.get(code, 0.0), debts_.get(code, (0.0, 0.0))
        t = terms.get(code)
        assert t, f"两融条款里没有 {code}"
        px, fin = quotes[code]["price"], min(dq, n)
        x = fin * px - amt
        s += t.rate * (n - fin) * px + (t.rate * x if x > 0 else x)
        if amt:
            assert t.ratio is not None, f"{code} 有融资负债却不在标的表"
            s -= amt * t.ratio
    return s


def balances(pos):
    return {k: h["balance"] for k, h in pos.by_code.items()}


def account(st, terms):
    """客户端重查信用资产与融资负债 → Acct；st 为读时的持仓与行情（算 base）。"""
    with ui.UI:
        c = common.ask(credit, lambda: ui.click_func("信用资产")).by_code
        d = common.ask(debts, lambda: ui.click_func("融资负债汇总")).by_code
        ui.click_tool("持仓")
    dbt = {k: (r["qty"], r["amount"]) for k, r in d.items()}
    return Acct(
        time.strftime("%H:%M:%S"),
        c["担保资产"],
        c["最高融资额度"] - c["融资负债（含费用）"],
        c["负债总额"],
        c["净资产"],
        c["可用保证金"],
        c["现金资产"],
        dbt,
        surplus(balances(st.pos), dbt, st.quotes, terms),
    )


def avail(acct, bal, quotes, terms):
    """持仓为 bal、行情为 quotes 时的可用保证金（不含现金变动）。"""
    return acct.avail + surplus(bal, acct.debts, quotes, terms) - acct.base
