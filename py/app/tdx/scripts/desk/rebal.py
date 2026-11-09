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
#     缺口 need = V − 卖后市值，不足该只最小单元即不补（多了不卖）。融资标的先融资：按保证金比例从低到高分，
#     Σ 比例 × 融资额 ≤ X'、Σ 融资额 ≤ L；其余（含融不到的部分）用现金，现金买入 + 全部买入费用 ≤ C'。
#     每只的融资腿、现金腿也各为 0 或够一个最小单元：融不满时融资腿让出一个最小单元给现金腿，融资腿不足一个最小单元即全用现金。
#     V 在 [0, 上界] 里二分取可行的（精度 TOL），并入下一只时以上一个 V 为上界；
#     现金需求大体随 V 增，只在缺口与腿够上最小单元处分段跳变，二分取到的未必是最大。
#   入选：先融资标的、后其余，各按排名逐只试并入，并入后的 V 够得上其中卖后无持仓各只的最小单元才收，否则跳过；
#     已持有的不设门槛，缺口够一个最小单元即补。购买力少时只开融资标的里排名靠前的，多了即铺开、近于等权。
#   委托价：买 n 股按 n × 参考价的额定价（与 order 同算）；一腿 a 元买 n × 委托价 ≤ a 的最大整步长股数。
#     最小单元 = max(起买股数, unit / 参考价) 取整步长的股数 × 其委托价，即恰能买出这些股的最小额。
#   下单：各腿按上述折股；预期行使购买力 = Σ 买单额（参考价计）。
#     卖单与买单同形：各行 {order 单, skip 不做的原因}，持仓行一单、名单行融资 / 现金两腿。
#   成本 = 各单按 5 档吃出的滑点（相对参考价，竞价中计 0）+ 费用（佣金万 1 双向、印花税 0.05% 卖、过户费 0.001% 双向）。
#   账户现 / 调仓后两栏同形（_sheet）：预期总资产 = 现总资产 + 融资额 − 成本，预期净资产 = 现净资产 − 成本，
#     杠杆 = 总资产 / 净资产，维持担保比例 = 总资产 / 负债。现总资产 = 读时现金 + 现持仓市值。
# 账户取自持仓推送的 State（刷新一轮里接在持仓之后查），试算有什么用什么，账户新旧由持仓页进度标出。
# 后台事务一次一件（拉条款 / 执行，feed.Jobs），状态与日志（feed.Journal，logs/reb-<时刻>.jsonl）推给页面：
#   条款只认当日的，跨日即换成当日落盘的；调仓页订阅时当日还没有即后台拉（每天自动至多一次，失败了等人点）。
#   执行 = 当日条款还没有即先拉 → 刷新 → 还有挂单即停（免得重复下单、冻结的钱券没算进试算）→ 试算 → 卖单逐只闪电卖出 → 刷新到成交表里各只卖量到齐 → 去掉卖出重算 →
#   融资单逐只闪电融资买入 → 现金单逐只闪电买入 → 刷新。
#   每单 order.ORDERS.place，任一步失败即停（已下的单不撤）；卖单、买单各一整批占着界面（ui.UI）连下，其间不插刷新。

import collections
import math
import os
import re
import time

from mem.common import BJ, BUY, SELL, sec
from mem.tables import NO_FLOW

from . import ROOT, book, credit, ui
from .feed import Feed, Jobs, Journal, err
from .hub import HUB
from .order import ORDERS

LIST_DIR = os.path.join(ROOT, "lists")
COMMISSION, STAMP, TRANSFER = 1e-4, 5e-4, 1e-5
FILL_WAIT, FILL_GAP = 120, 2  # 等卖单成交：最长秒数、每轮间隔
LOG_TAIL = 200  # 推给页面的最近日志条数
TOL = 1.0  # V 二分精度（元）


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


def _least(r, unit, lot, step):
    """一单最少股数：够 lot 股、按参考价 r 计够 unit 元，取整步长。"""
    return math.ceil(max(lot, unit / r) / step) * step


def _leg(q, s, n, least, px, credit_, x):
    """证券 x 一单 n 股 → (单, None)：参考价计额、5 档吃出滑点；不足 least 股 → (None, 原因)。"""
    if n < least:
        return None, f"不足最小单元（{least} 股）"
    amt = n * book.ref(q)
    slip, full = book.cost(q, s, amt)
    return {
        **x,
        "s": s,
        "credit": credit_,
        "qty": n,
        "px": px,
        "amount": amt,
        "slip": slip or 0.0,
        "fee": fee(amt, s),
        "full": full,
    }, None


def _sells(st, terms, cfg):
    """各持仓行（带可融、卖出设置与卖单）→ (行, 卖单)。"""
    rows = []
    for code, h in st.pos.by_code.items():
        q, t = st.quotes[code], terms.get(code)
        px = book.ref(q)
        v = cfg["sells"].get(code, 0)
        lot, step = book.lot(code)
        avail = int(h["available"])
        row = {
            "code": code,
            "name": h["name"],
            "financeable": bool(t and t.financeable),
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
            assert (
                n == avail or n % step == 0
            ), f"{code} 卖 {n} 股不是 {step} 股的整数倍"
            # 全部可卖可含零股
            row["order"], row["skip"] = _leg(
                q,
                SELL,
                n,
                _least(px, cfg["unit"], *((1, 1) if n == avail else (lot, step))),
                book.order_px(q, SELL, cfg["px"], n * px) or px,
                "plain",
                sec(h),
            )
        rows.append(row)
    return rows, [r["order"] for r in rows if r["order"]]


def _px(q, mode, n):
    """买 n 股的委托价：按 n × 参考价的额定（与 order 同算）；竞价中或空盘取参考价。"""
    r = book.ref(q)
    return book.order_px(q, BUY, mode, n * r) or r


def _qty(q, mode, a, step):
    """a 元能买的最多股数（整步长）：n × _px(n) ≤ a，左边随 n 增，二分 n / step。"""
    lo, hi = 0, int(a / (step * _px(q, mode, step))) + 1
    while lo < hi:
        k = (lo + hi + 1) // 2
        lo, hi = (k, hi) if k * step * _px(q, mode, k * step) <= a else (lo, k - 1)
    return lo * step


def _targets(codes, terms, quotes, bal, cfg):
    """可买的各只 → {代码: {held 卖后市值, ratio 融资保证金比例（非融资标的为 None）, least 一单最少股数,
    min 最小单元}}。"""
    tgt = {}
    for c in codes:
        t, q = terms[c], quotes[c]
        r = book.ref(q)
        assert r, f"{c} {t.name} 没有参考价"
        n = _least(r, cfg["unit"], *book.lot(c))
        tgt[c] = {
            "held": bal.get(c, 0.0) * q["price"],
            "ratio": t.ratio if t.financeable else None,
            "least": n,
            "min": n * _px(q, cfg["px"], n),
        }
    return tgt


def _fill(V, tgt, X, L):
    """目标市值 V 下各只两腿 → ({代码: (融资额, 现金额)}, 现金需求)；tgt 已按保证金比例从低到高排。
    缺口与两腿各为 0 或够该只最小单元（见文件头）。"""
    legs, xb, lb = {}, max(0.0, X), L
    for c, x in tgt.items():
        m, g = x["min"], V - x["held"]
        if g < m:
            legs[c] = (0.0, 0.0)
            continue
        f = 0.0 if x["ratio"] is None else min(g, lb, xb / x["ratio"])
        p = g - f
        if 0 < p < m:
            f, p = g - m, m
        if f < m:
            f, p = 0.0, g
        legs[c] = (f, p)
        if f:
            xb, lb = xb - x["ratio"] * f, lb - f
    need = sum(f + p for f, p in legs.values())
    return legs, sum(p for _, p in legs.values()) + fee(need, BUY)


def _level(tgt, C, X, L, hi):
    """tgt 等权补齐可行的目标市值 V：在 [0, hi] 里二分到 TOL（见文件头）。"""
    lo = 0.0
    while hi - lo > TOL:
        mid = (lo + hi) / 2
        lo, hi = (mid, hi) if _fill(mid, tgt, X, L)[1] <= C else (lo, mid)
    return lo


def _pick(tries, tgt, C, X, L):
    """按 tries 的先后逐只试并入（见文件头）→ (入选的 {代码: …}，按保证金比例排, V)。"""
    by_ratio = sorted(tgt, key=lambda c: tgt[c]["ratio"] or 0)
    picked = set()
    V = max((x["held"] for x in tgt.values()), default=0.0) + C + L
    for c in tries:
        s = {k: tgt[k] for k in by_ratio if k in picked or k == c}
        v = _level(s, C, X, L, V)
        if all(v >= x["min"] for x in s.values() if not x["held"]):
            picked.add(c)
            V = v
    return {k: tgt[k] for k in by_ratio if k in picked}, V if picked else 0.0


def _buys(codes, terms, quotes, tgt, picked, V, legs, cfg):
    """名单各行（带融资 / 现金两腿 {credit, want, order, skip}）→ (行, 买单)，买单融资在前；各腿按 _qty 折股。"""
    rows, buys = [], []
    for i, c in enumerate(codes, 1):
        t = terms.get(c)
        why = _why(t)
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
        f, p = legs[c]
        row["need"] = f + p
        if not row["need"] and V > tgt[c]["held"]:
            row["skip"] = "缺口不足最小单元"
            continue
        q, step = quotes[c], book.lot(c)[1]
        for cr, part in (("margin", f), ("plain", p)):
            if not part:
                continue
            n = _qty(q, cfg["px"], part, step)
            o, skip = _leg(
                q, BUY, n, tgt[c]["least"], _px(q, cfg["px"], n), cr, sec(t._asdict())
            )
            row["legs"].append({"credit": cr, "want": part, "order": o, "skip": skip})
            if o:
                buys.append(o)
    return rows, sorted(buys, key=lambda o: o["credit"] != "margin")


def _sheet(assets, net, debt, cash, X, L):
    """账户一栏：总资产 / 净资产 / 负债 / 现金 / 可用保证金 / 可融余量，及杠杆、维持担保比例。"""
    return {
        "assets": assets,
        "net": net,
        "debt": debt,
        "cash": cash,
        "X": X,
        "L": L,
        "lev": assets / net,
        "maint": assets / debt if debt else None,
    }


def plan(st, terms, codes, cfg):
    """试算一帧（见文件头）；行情还没覆盖到的回 {wait}。"""
    held, quotes, acct = st.pos.by_code, st.quotes, st.acct
    ok = [c for c in codes if not _why(terms.get(c))]
    lack = [c for c in [*held, *ok] if c not in quotes]
    if lack:
        return {"wait": f"等行情（{len(lack)} 只，如 {lack[0]}）"}
    pct = cfg["pct"] / 100

    holds, sells = _sells(st, terms, cfg)
    proceeds = sum(o["amount"] - o["slip"] - o["fee"] for o in sells)
    bal = credit.balances(st.pos)
    for o in sells:
        bal[o["code"]] -= o["qty"]
    C = acct.cash + proceeds
    X = credit.avail(acct, bal, quotes, terms) + proceeds
    L = max(0.0, acct.line)

    tgt = _targets(ok, terms, quotes, bal, cfg)
    tries = sorted(ok, key=lambda c: tgt[c]["ratio"] is None)
    picked, V = _pick(tries, tgt, C * pct, X * pct, L * pct)
    legs, _ = _fill(V, picked, X * pct, L * pct)
    targets, buys = _buys(codes, terms, quotes, tgt, picked, V, legs, cfg)

    orders = [*sells, *buys]
    F = sum(o["amount"] for o in buys if o["credit"] == "margin")
    use = sum(o["amount"] for o in buys)
    cost = sum(o["slip"] + o["fee"] for o in orders)
    buy_cost = sum(o["slip"] + o["fee"] for o in buys)
    assets = acct.cash + sum(h["balance"] * quotes[c]["price"] for c, h in held.items())
    net = assets - acct.debt
    X_now = credit.avail(acct, credit.balances(st.pos), quotes, terms)
    return {
        "now": _sheet(assets, net, acct.debt, acct.cash, X_now, L),
        # 调仓后：可用保证金要按买后持仓与负债重算，不出
        "exp": _sheet(
            assets + F - cost,
            net - cost,
            acct.debt + F,
            C - (use - F) - buy_cost,
            None,
            L - F,
        ),
        "power": C + min(L, max(0.0, X)),
        "use": use,
        "financed": F,
        "cost": cost,
        "after_sell": {"C": C, "X": X},
        "V": V,
        "picked": len(picked),
        "buyable": len(ok),
        "holds": holds,
        "targets": targets,
        "orders": orders,
        "time": max((q["time"] for q in quotes.values()), default=""),
    }


class Rebal:
    def __init__(self):
        # 以下各项由后台事务与配置修改改写，同在 feed 内；terms 为 day 那天的，auto 为上次自动拉条款的日子
        self.day, self.auto = time.strftime("%Y%m%d"), None
        self.terms = credit.load()
        names = lists()
        self.cfg = {
            "list": names[0] if names else None,
            "unit": 10000,
            "pct": 100,
            "px": "sweep",
            "sells": {},
        }
        self.codes = read_list(names[0]) if names else []
        self.plan = None
        self.feed = Feed(lambda seen: self._frame())
        # 后台事务（留最近一件）{name, file, live 实时进度, …} 与其日志
        self.jobs, self.log = Jobs(self.feed, 1), None
        self._watch()
        HUB.hooks.append(self._calc)

    def _frame(self):
        task = self.jobs.last()
        return {
            "lists": lists(),
            "cfg": self.cfg,
            "codes": self.codes,
            "terms": len(self.terms) if self.terms else None,
            "plan": self.plan,
            "task": task and task | {"log": self.log.since(-1)},
        }

    def frames(self):
        """SSE：订阅期间持仓循环转着（试算跟它的帧走）；当日条款还没有、今天也还没自动拉过即后台拉。"""
        self._today()
        with self.feed:
            j = self.jobs.last()
            if (
                self.terms is None
                and self.auto != self.day
                and not (j and j["state"] == "run")
            ):
                self.auto = self.day
                self.pull({})
        with HUB.sub():
            yield from self.feed.watch()

    def _today(self):
        """跨日即把条款换成当日落盘的（还没拉过为 None）。"""
        day = time.strftime("%Y%m%d")
        if day == self.day:
            return
        with self.feed:
            self.day, self.terms = day, credit.load()
        self._watch()

    def _watch(self):
        """名单里可买的（试算要其行情）并进持仓页那一路行情。"""
        t = self.terms or {}
        HUB.watch(
            [
                {"code": c, "market_flag": t[c].market_flag}
                for c in self.codes
                if not _why(t.get(c))
            ]
        )

    def _calc(self, st):
        """重算试算、推帧（持仓循环的 hook，也在配置 / 账户 / 条款变后调）。"""
        self._today()
        if self.terms is None:
            p = {"wait": "当日两融条款还没拉（更新两融名单）"}
        elif st.acct is None:
            p = {"wait": "还没读到信用账户（等刷新）"}
        else:
            try:
                p = plan(st, self.terms, self.codes, self.cfg)
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
            job = self.jobs.open(name=name, live=None)
            self.log = Journal("reb", self.feed, LOG_TAIL)
            job["file"] = self.log.path
        self.jobs.spawn(job, lambda job: self._task(job, fn))
        return {}

    def _live(self, **kw):
        self.jobs.put(self.jobs.last(), live=kw)

    def _task(self, job, fn):
        log = self.log
        try:
            with HUB.sub():
                fn(log)
            log("ok")
        except Exception as e:  # 记下原因，再交给 Jobs 停在这一步（已下的单不撤）
            log("err", err=err(e))
            raise
        finally:
            log.close()
            self.jobs.put(job, live=None)
            self._calc(HUB.state())

    def pull(self, b):
        return self._start("更新两融名单", self._pull)

    def run(self, b):
        return self._start("执行调仓", self._run)

    def _pull(self, log):
        log("pull")
        terms = credit.pull(log, self._live)
        log(
            "terms",
            n=len(terms),
            financeable=sum(t.financeable for t in terms.values()),
            pledgeable=sum(t.pledgeable for t in terms.values()),
        )
        with self.feed:
            self.terms = terms
        self._watch()

    def _sync(self, log):
        """刷新一轮（成交、持仓、挂单、信用账户），记下账户。"""
        HUB.sync()
        a = HUB.state().acct
        log(
            "acct",
            acct={k: v for k, v in a._asdict().items() if k not in ("bal", "px")},
        )

    def _plan(self, log):
        assert self.terms, "当日两融条款还没拉"
        p = plan(HUB.state(), self.terms, self.codes, self.cfg)
        assert "wait" not in p, p.get("wait")
        log("plan", cfg=self.cfg, codes=self.codes, plan=p)
        return p

    def _place(self, log, o):
        log("order", order=o)
        job = ORDERS.place(sec(o), o["s"], o["qty"], self.cfg["px"], o["credit"])
        log("job", job=job)
        assert job["state"] == "ok", f"{o['code']} {o['name']} 下单失败：{job['msg']}"
        return job

    def _fills(self, log, want):
        """刷新到成交表里各只卖量到 want（股）。"""
        end = time.monotonic() + FILL_WAIT
        while True:
            self._sync(log)
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
        self._today()
        if self.terms is None:
            self._pull(log)
        self._sync(log)
        hang = HUB.state().orders.rows
        assert not hang, (
            f"还有 {len(hang)} 笔未成交委托（"
            + "、".join(
                f"{r['code']} {r['name']} {r['qty'] - r['filled']:.0f}股" for r in hang
            )
            + "），撤掉或等成交后再调仓"
        )
        p = self._plan(log)
        sells = [o for o in p["orders"] if o["s"] == SELL]
        if sells:
            by = HUB.state().fills.by_code
            want = collections.Counter(
                {o["code"]: by.get(o["code"], NO_FLOW)["traded"][SELL] for o in sells}
            )
            with ui.UI("调仓"):
                for o in sells:
                    want[o["code"]] += self._place(log, o)["qty"]
            self._fills(log, dict(want))
            with self.feed:
                self.cfg = {**self.cfg, "sells": {}}
            p = self._plan(log)
        with ui.UI("调仓"):
            for o in p["orders"]:
                assert o["s"] == BUY, f"去掉卖出后试算仍有卖单：{o}"
                self._place(log, o)
        self._sync(log)


REB = Rebal()
