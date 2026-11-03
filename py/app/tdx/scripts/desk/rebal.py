# desk/rebal — 一键调仓：名单按排名入选、等权补齐；可选先卖，再融资买入融资标的，再现金买其余。
#
# 名单：lists/*.csv，每行首列一个 6 位代码（可有 code 表头），行序即排名（首行最高）；按修改时间新在前，默认最新的。
# 配置 cfg：list 名单文件、unit 最小调仓单元（元，不足即不做）、pct 行使比例（0–100）、px 委托价（book.PX_MODES）、
#   sells {代码: 卖出股数}（整步长，或全部可卖）。
# 试算 plan（纯函数；持仓页循环每推一帧、配置每改一次即重算推送）：
#   卖出：股数（超过可卖按全部可卖）× 参考价计额，回款 = 额 − 滑点 − 费用；
#     现金 C' = 现金 + 回款，可用保证金 X' = 按卖后持仓重算（credit.avail）+ 回款，L = 可融额度余量。
#   购买力 = C' + min(L, max(0, X'))（融资保证金比例按 100% 计）。
#   行使：C'、X'、L 各 × pct 为本次可用额度，以下都在其内。
#   买入：名单里可作担保品买入的（否则不可买：不在担保品表 / 担保状态非正常 / 北交所）等权补到共同目标市值 V，
#     缺口 need = max(0, V − 卖后市值)，多了不卖。融资标的先融资：按保证金比例从低到高分，
#     Σ 比例 × 融资额 ≤ X'、Σ 融资额 ≤ L；其余（含融不到的部分）用现金，现金买入 + 全部买入费用 ≤ C'。
#     V 二分到可行的最大。
#   入选：先融资标的、后其余，各按排名逐只试并入，并入后的 V 下未开仓的（卖后市值不足一个最小单元）缺口都够一个最小单元才收，否则跳过；
#     最小单元 = max(起买股数, unit / 参考价) 取整步长的股数 × 委托价。购买力少时只开融资标的里排名靠前的，多了即铺开、近于等权。
#   下单：缺口按委托价折股、取整手，不足起买量或不足 unit 的单不做；预期行使购买力 = Σ 买单额（参考价计）。
#   成本 = 各单按 5 档吃出的滑点（相对参考价，竞价中计 0）+ 费用（佣金万 1 双向、印花税 0.05% 卖、过户费 0.001% 双向）。
#   预期总资产 = 现总资产 + 融资额 − 成本，预期净资产 = 现净资产 − 成本，杠杆 = 总资产 / 净资产，
#   维持担保比例 = 总资产 / 负债。现总资产 = 读时现金 + 现持仓市值。
# 后台事务一次一件（拉条款 / 查账户 / 执行），状态与日志推给页面：
#   执行 = 查账户 → 试算 → 卖单逐只闪电卖出 → 等成交表里各只卖量到齐 → 重查账户、去掉卖出重算 →
#   融资单逐只闪电融资买入 → 现金单逐只闪电买入 → 刷新持仓、再查账户。
#   每单 order.ORDERS.place，任一步失败即停（已下的单不撤）；全程逐条写 logs/reb-<时刻>.jsonl。

import collections
import json
import math
import os
import re
import threading
import time

from mem.common import BUY, SELL
from mem.fills import NO_FLOW

from . import ROOT, book, credit
from .feed import Feed, err
from .hub import HUB
from .order import ORDERS

LIST_DIR = os.path.join(ROOT, "lists")
LOG_DIR = os.path.join(ROOT, "logs")
COMMISSION, STAMP, TRANSFER = 1e-4, 5e-4, 1e-5
BJ = 60
FILL_WAIT, FILL_GAP = 120, 2  # 等卖单成交：最长秒数、每轮间隔
LOG_TAIL = 200  # 推给页面的最近日志条数
SEARCH = 40  # V 二分次数


def fee(amt, s):
    return amt * (COMMISSION + TRANSFER + (STAMP if s == SELL else 0))


def lists():
    """名单文件名，新在前。"""
    if not os.path.isdir(LIST_DIR):
        return []
    names = [n for n in os.listdir(LIST_DIR) if n.endswith(".csv")]
    return sorted(
        names, key=lambda n: os.path.getmtime(os.path.join(LIST_DIR, n)), reverse=True
    )


def read_list(name):
    """名单 → [代码]（去重保序）。"""
    with open(os.path.join(LIST_DIR, name), encoding="utf-8-sig") as f:
        rows = [line.split(",")[0].strip() for line in f if line.strip()]
    if rows and rows[0].lower() == "code":
        rows = rows[1:]
    bad = [r for r in rows if not re.fullmatch(r"\d{6}", r)]
    assert not bad, f"{name} 里有非 6 位代码：{bad[:5]}"
    return list(dict.fromkeys(rows))


def _why(t):
    """不可买的原因；可买回 None。"""
    if not t:
        return "不在担保品表"
    if t.market_flag == BJ:
        return "北交所"
    if not t.pledgeable:
        return "担保状态非正常"
    return None


def _leg(q, s, n, lot, px, unit, credit_, sec):
    """一单 n 股：参考价计额、5 档吃出滑点；不足 lot 股或不足 unit 元回原因串。"""
    r = book.ref(q)
    amt = n * r
    if n < lot:
        return f"不足一手（{lot} 股）"
    if amt < unit:
        return "不足最小单元"
    slip, full = book.cost(q, s, amt)
    return {
        **sec,
        "s": s,
        "credit": credit_,
        "qty": n,
        "px": px,
        "amount": amt,
        "slip": slip or 0.0,
        "fee": fee(amt, s),
        "full": full,
    }


def _sells(st, cfg):
    """卖单与各持仓行（带卖出设置）→ (行, 卖单)。"""
    rows, orders = [], []
    for code, h in st.pos.by_code.items():
        q = st.quotes[code]
        px = book.ref(q)
        v = cfg["sells"].get(code, 0)
        lot, step = book.lot(code)
        avail = int(h["available"])
        row = {
            "code": code,
            "name": h["name"],
            "value": h["balance"] * q["price"],
            "sellable": avail * px,
            "avail": avail,
            "px": px,
            "lot": lot,
            "step": step,
            "sell": v,
            "order": None,
            "skip": None,
        }
        if v > 0:
            n = min(v, avail)
            assert n == avail or n % step == 0, f"{code} 卖 {n} 股不是 {step} 股的整数倍"
            sec = {k: h[k] for k in ("code", "name", "market_flag")}
            # 全部可卖可含零股
            o = _leg(
                q,
                SELL,
                n,
                1 if n == avail else lot,
                book.order_px(q, SELL, cfg["px"]) or px,
                cfg["unit"],
                "plain",
                sec,
            )
            if isinstance(o, str):
                row["skip"] = o
            else:
                row["order"] = o
                orders.append(o)
        rows.append(row)
    return rows, orders


def _fill(V, tgt, X, L):
    """目标市值 V 下各只缺口与融资额，及现金需求 → (need, fin, cash)；tgt 已按保证金比例从低到高排。"""
    need = {c: max(0.0, V - x["held"]) for c, x in tgt.items()}
    fin, xb, lb = {}, max(0.0, X), L
    for c, x in tgt.items():
        if x["ratio"] is None:
            continue
        f = min(need[c], lb, xb / x["ratio"])
        fin[c], xb, lb = f, xb - x["ratio"] * f, lb - f
    total = sum(need.values())
    return need, fin, total - sum(fin.values()) + fee(total, BUY)


def _level(tgt, C, X, L):
    """tgt 等权补齐可行的最大目标市值 V（二分）。"""
    if not tgt:
        return 0.0
    lo, hi = 0.0, max(x["held"] for x in tgt.values()) + C + L
    for _ in range(SEARCH):
        mid = (lo + hi) / 2
        lo, hi = (mid, hi) if _fill(mid, tgt, X, L)[2] <= C else (lo, mid)
    return lo


def _pick(tries, tgt, C, X, L):
    """按 tries 的先后逐只试并入（见文件头）→ (入选的 {代码: …}，按保证金比例排, V)。"""
    by_ratio = sorted(tgt, key=lambda c: tgt[c]["ratio"] or 0)
    picked, V = set(), 0.0
    for c in tries:
        s = {k: tgt[k] for k in by_ratio if k in picked or k == c}
        v = _level(s, C, X, L)
        if all(v - x["held"] >= x["min"] for x in s.values() if x["held"] < x["min"]):
            picked.add(c)
            V = v
    return {k: tgt[k] for k in by_ratio if k in picked}, V


def plan(st, acct, terms, codes, cfg):
    """试算一帧（见文件头）；行情还没覆盖到的回 {wait}。"""
    held, quotes = st.pos.by_code, st.quotes
    ok = [c for c in codes if not _why(terms.get(c))]
    lack = [c for c in [*held, *ok] if c not in quotes]
    if lack:
        return {"wait": f"等行情（{len(lack)} 只，如 {lack[0]}）"}
    unit, pct = cfg["unit"], cfg["pct"] / 100

    hold_rows, sells = _sells(st, cfg)
    proceeds = sum(o["amount"] - o["slip"] - o["fee"] for o in sells)
    bal = credit.balances(st.pos)
    for o in sells:
        bal[o["code"]] -= o["qty"]
    C = acct.cash + proceeds
    X = credit.avail(acct, bal, quotes, terms) + proceeds
    L = max(0.0, acct.line)
    power = C + min(L, max(0.0, X))

    tgt = {}
    for c in ok:
        t, q = terms[c], quotes[c]
        r = book.ref(q)
        assert r, f"{c} {t.name} 没有参考价"
        px = book.order_px(q, BUY, cfg["px"]) or r
        lot, step = book.lot(c)
        tgt[c] = {
            "held": bal.get(c, 0.0) * q["price"],
            "ratio": t.ratio if t.financeable else None,
            "px": px,
            "min": math.ceil(max(lot, unit / r) / step) * step * px,
        }
    tries = sorted(ok, key=lambda c: tgt[c]["ratio"] is None)
    picked, V = _pick(tries, tgt, C * pct, X * pct, L * pct)
    need, fin, _ = _fill(V, picked, X * pct, L * pct)

    rows, buys = [], {"margin": [], "plain": []}
    for i, c in enumerate(codes, 1):
        t, why = terms.get(c), _why(terms.get(c))
        row = {
            "rank": i,
            "code": c,
            "name": t.name if t else "",
            "why": why,
            "skip": None,
            "financeable": bool(t and t.financeable),
            "ratio": t.ratio if t else None,
            "rate": t.rate if t else None,
            "legs": [],
        }
        rows.append(row)
        if why:
            continue
        row["held"] = tgt[c]["held"]
        if c not in picked:
            row["skip"] = "购买力不够开仓"
            continue
        q, px = quotes[c], tgt[c]["px"]
        lot, step = book.lot(c)
        a, f = need[c], fin.get(c, 0.0)
        row["need"] = a
        sec = {"code": c, "name": t.name, "market_flag": t.market_flag}
        for cr, part in (("margin", f), ("plain", a - f)):
            if part <= 0:
                continue
            n = int(part / px) // step * step
            o = _leg(q, BUY, n, lot, px, unit, cr, sec)
            row["legs"].append(
                {"credit": cr, "want": part}
                | (
                    {"skip": o}
                    if isinstance(o, str)
                    else {"qty": o["qty"], "amount": o["amount"]}
                )
            )
            if not isinstance(o, str):
                buys[cr].append(o)

    orders = [*sells, *buys["margin"], *buys["plain"]]
    F = sum(o["amount"] for o in buys["margin"])
    use = F + sum(o["amount"] for o in buys["plain"])
    cost = sum(o["slip"] + o["fee"] for o in orders)
    mv = sum(h["balance"] * quotes[c]["price"] for c, h in held.items())
    assets = acct.cash + mv
    net = assets - acct.debt
    exp_assets, exp_net, exp_debt = assets + F - cost, net - cost, acct.debt + F
    buy_cost = sum(o["slip"] + o["fee"] for o in [*buys["margin"], *buys["plain"]])
    return {
        "now": {
            "assets": assets,
            "net": net,
            "debt": acct.debt,
            "cash": acct.cash,
            "X": credit.avail(acct, credit.balances(st.pos), quotes, terms),
            "L": L,
            "lev": assets / net,
            "maint": assets / acct.debt if acct.debt else None,
        },
        # 调仓后：可用保证金要按买后持仓与负债重算，不出
        "exp": {
            "assets": exp_assets,
            "net": exp_net,
            "debt": exp_debt,
            "cash": C - (use - F) - buy_cost,
            "X": None,
            "L": L - F,
            "lev": exp_assets / exp_net,
            "maint": exp_assets / exp_debt if exp_debt else None,
        },
        "power": power,
        "use": use,
        "financed": F,
        "cost": cost,
        "after_sell": {"C": C, "X": X},
        "V": V,
        "picked": len(picked),
        "buyable": len(ok),
        "holds": hold_rows,
        "targets": rows,
        "orders": orders,
        "time": max((q["time"] for q in quotes.values()), default=""),
    }


class Log:
    """一次事务一份 logs/reb-<时刻>.jsonl，每条 {t, ev, …}，同时留给页面；
    live(**kw) 只给页面的实时进度（覆盖上一份，不落盘）。"""

    def __init__(self, tail, live):
        os.makedirs(LOG_DIR, exist_ok=True)
        self.path = os.path.join(LOG_DIR, time.strftime("reb-%Y%m%d-%H%M%S.jsonl"))
        self.f = open(self.path, "w", encoding="utf-8", buffering=1)
        self.tail, self.live = tail, live

    def __call__(self, ev, **kw):
        rec = {"t": time.strftime("%H:%M:%S"), "ev": ev, **kw}
        self.f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
        self.tail(rec)


class Rebal:
    def __init__(self):
        # 以下各项由后台事务与配置修改改写，同在 feed 内
        self.terms = credit.load()
        self.acct = None
        names = lists()
        self.cfg = {
            "list": names[0] if names else None,
            "unit": 10000,
            "pct": 100,
            "px": "opp",
            "sells": {},
        }
        self.codes = read_list(names[0]) if names else []
        self.plan = None
        # 后台事务 {name, state: run|ok|err, msg, file, log, live 实时进度}
        self.task = None
        self.feed = Feed(lambda seen: self._frame())
        self._watch()
        HUB.hooks.append(self._calc)

    def _frame(self):
        a = self.acct
        return {
            "lists": lists(),
            "cfg": self.cfg,
            "terms": len(self.terms) if self.terms else None,
            "acct": a
            and {k: v for k, v in a._asdict().items() if k not in ("debts", "base")},
            "plan": self.plan,
            "task": self.task,
        }

    def frames(self):
        """SSE：订阅期间持仓循环转着（试算跟它的帧走）。"""
        with HUB.sub():
            yield from self.feed.watch()

    def _watch(self):
        """名单里能取行情的并进持仓页那一路行情。"""
        t = self.terms or {}
        HUB.watch(
            [
                {"code": c, "market_flag": t[c].market_flag}
                for c in self.codes
                if c in t and t[c].market_flag != BJ
            ]
        )

    def _calc(self, st):
        """重算试算、推帧（持仓循环的 hook，也在配置 / 账户 / 条款变后调）。"""
        if self.terms is None:
            p = {"wait": "当日两融条款还没拉（更新两融名单）"}
        elif self.acct is None:
            p = {"wait": "还没查信用资产（刷新信用资产）"}
        else:
            try:
                p = plan(st, self.acct, self.terms, self.codes, self.cfg)
            except Exception as e:  # 错误边界：试算出错只报在调仓提示行，不打断持仓循环
                p = {"err": err(e)}
        with self.feed:
            self.plan = p

    def set(self, b):
        """改配置（给了的键）→ 新配置；名单换了即重读、换行情。"""
        cfg = dict(self.cfg)
        if "list" in b:
            assert b["list"] in lists(), f"没有名单 {b['list']}"
            cfg["list"] = b["list"]
        if "unit" in b:
            cfg["unit"] = float(b["unit"])
            assert cfg["unit"] >= 0, f"最小调仓单元 {cfg['unit']}"
        if "pct" in b:
            cfg["pct"] = float(b["pct"])
            assert 0 <= cfg["pct"] <= 100, f"行使比例 {cfg['pct']}"
        if "px" in b:
            assert b["px"] in book.PX_MODES, f"委托价 {b['px']}"
            cfg["px"] = b["px"]
        if "sells" in b:
            assert all(
                isinstance(v, int) and v >= 0 for v in b["sells"].values()
            ), f"卖出股数 {b['sells']}"
            cfg["sells"] = {c: v for c, v in b["sells"].items() if v > 0}
        codes = (
            read_list(cfg["list"]) if cfg["list"] != self.cfg["list"] else self.codes
        )
        with self.feed:
            self.cfg, self.codes = cfg, codes
        self._watch()
        self._calc(HUB.state())
        return cfg

    # ---------- 后台事务 ----------

    def _start(self, name, fn):
        with self.feed:
            assert not (
                self.task and self.task["state"] == "run"
            ), f"「{self.task['name']}」还在跑"
            self.task = {
                "name": name,
                "state": "run",
                "msg": "",
                "file": None,
                "log": [],
                "live": None,
            }
        threading.Thread(target=self._task, args=(fn,), daemon=True).start()
        return {}

    def _tail(self, rec):
        with self.feed:
            self.task["log"] = [*self.task["log"], rec][-LOG_TAIL:]

    def _live(self, **kw):
        with self.feed:
            self.task["live"] = kw

    def _task(self, fn):
        log = Log(self._tail, self._live)
        with self.feed:
            self.task["file"] = log.path
        try:
            with HUB.sub():
                fn(log)
            log("ok")
            state, msg = "ok", ""
        except Exception as e:  # 错误边界：停在这一步，报原因（已下的单不撤）
            log("err", err=err(e))
            state, msg = "err", err(e)
        log.f.close()
        with self.feed:
            self.task |= {"state": state, "msg": msg, "live": None}
        self._calc(HUB.state())

    def pull(self, b):
        return self._start("更新两融名单", self._pull)

    def query(self, b):
        return self._start("刷新信用资产", self._query)

    def run(self, b):
        return self._start("执行调仓", self._run)

    def _pull(self, log):
        log("pull")
        terms = credit.pull(log)
        log(
            "terms",
            n=len(terms),
            financeable=sum(t.financeable for t in terms.values()),
            pledgeable=sum(t.pledgeable for t in terms.values()),
        )
        with self.feed:
            self.terms = terms
        self._watch()

    def _query(self, log):
        assert self.terms, "当日两融条款还没拉"
        acct = credit.account(HUB.state(), self.terms)
        log("acct", acct=acct._asdict())
        with self.feed:
            self.acct = acct

    def _plan(self, log):
        p = plan(HUB.state(), self.acct, self.terms, self.codes, self.cfg)
        assert "wait" not in p, p.get("wait")
        log("plan", cfg=self.cfg, codes=self.codes, plan=p)
        return p

    def _place(self, log, o):
        log("order", order=o)
        sec = {k: o[k] for k in ("code", "name", "market_flag")}
        job = ORDERS.place(sec, o["s"], o["qty"], self.cfg["px"], o["credit"])
        log("job", job=job)
        assert job["state"] == "ok", f"{o['code']} {o['name']} 下单失败：{job['msg']}"
        return job

    def _fills(self, log, want):
        """等成交表里各只卖量到 want（股）。"""
        end = time.monotonic() + FILL_WAIT
        while True:
            HUB.refresh()
            by = HUB.state().fills.by_code
            got = {c: by.get(c, NO_FLOW)["traded"][SELL] for c in want}
            log("fills", want=want, got=got)
            if all(got[c] >= want[c] for c in want):
                return
            assert (
                time.monotonic() < end
            ), f"{FILL_WAIT}s 内卖单没成交完：要 {want}，到 {got}"
            time.sleep(FILL_GAP)

    def _run(self, log):
        self._query(log)
        p = self._plan(log)
        sells = [o for o in p["orders"] if o["s"] == SELL]
        if sells:
            by = HUB.state().fills.by_code
            want = collections.Counter(
                {o["code"]: by.get(o["code"], NO_FLOW)["traded"][SELL] for o in sells}
            )
            for o in sells:
                want[o["code"]] += self._place(log, o)["qty"]
            self._fills(log, dict(want))
            with self.feed:
                self.cfg = {**self.cfg, "sells": {}}
            self._query(log)
            p = self._plan(log)
        for o in p["orders"]:
            assert o["s"] == BUY, f"去掉卖出后试算仍有卖单：{o}"
            self._place(log, o)
        HUB.refresh()
        self._query(log)


REB = Rebal()
