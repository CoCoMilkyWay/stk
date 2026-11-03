# desk/book — 5 档盘口，相对参考价算滑点与价位；委托价与整手规则（下单与调仓试算同用）。
# 行情的 book = {方向: 该方向吃的那侧}：买 +1 吃卖盘、价位往上；卖 -1 吃买盘、价位往下。
# 滑点与价位两组列同构：(量, s)，滑点乘 s → 正=成本。只出数字，格式化归前端。

from mem.common import BUY, SELL, SIDES

NOTIONALS = (10000, 50000, 100000)  # 吃 n 元
PCTS = (1, 2)  # 偏 p%
# 委托价：(吃对手盘, 再偏 %)
PX_MODES = {"opp": (True, 0), "opp1": (True, 1), "own": (False, 0)}
SLIP_COLS = [(n, BUY) for n in reversed(NOTIONALS)] + [(n, SELL)
                                                       for n in NOTIONALS]
LEVEL_COLS = [(p, s) for p in PCTS for s in SIDES]


def _micro(q):
    """卖一/买一按对手量加权；任一侧空盘回 None。"""
    ask, bid = q["book"][BUY], q["book"][SELL]
    if not (ask and bid):
        return None
    (a, av), (b, bv) = ask[0], bid[0]
    return (a * bv + b * av) / (av + bv)


def auction(q):
    """集合竞价：盘口只给虚拟撮合价，卖一 ≤ 买一（锁定 / 交叉），此时滑点无意义。"""
    ask, bid = q["book"][BUY], q["book"][SELL]
    return bool(ask and bid and ask[0][0] <= bid[0][0])


def ref(q):
    """参考价：micro price → 现价 → 昨收，取首个可用；都没有回 None。"""
    return _micro(q) or next((x for x in (q["price"], q["last_close"]) if x > 0), None)


def order_px(q, s, mode):
    """方向 s 按 mode 定委托价（未舍入）：对手价 / 对手价再朝成交方向偏 1% / 本方价；竞价中或该侧空盘回 None。"""
    opp, pct = PX_MODES[mode]
    side = q["book"][s if opp else -s]
    if auction(q) or not side:
        return None
    return side[0][0] * (1 + s * pct / 100)


def lot(code):
    """(起买股数, 递增步长)：科创板起 200 股、零股递增，北交所起 100 股、零股递增，其余整手。"""
    first = 200 if code.startswith("688") else 100
    step = 1 if code.startswith(("688", "4", "8", "92")) else 100
    return first, step


def cost(q, s, notional):
    """吃 notional 元相对参考价的滑点（元，正=成本）与 5 档是否吃满；无参考价或竞价中回 (None, False)。"""
    r = ref(q)
    if not r or auction(q):
        return None, False
    avg, full = _eat(q["book"][s], notional)
    return (s * (avg - r) / r * notional if avg else None), full


def _eat(side, notional):
    """按档吃 notional 元 → (均价, 是否吃满)。"""
    spent = shares = 0.0
    for price, vol in side:
        take = min(vol, (notional - spent) / price)
        spent += take * price
        shares += take
        if spent >= notional - 1e-6:
            break
    return (spent / shares if shares else None), spent >= notional - 1e-6


def _slip(q, r, n, s):
    """吃 n 元均价相对 r 的万分比 × s（正=成本）；full=5 档吃满，否则实际更差。"""
    avg, full = _eat(q["book"][s], n)
    return {"pct": s * (avg - r) / r * 1e4 if avg else None, "full": full}


def slips(q, r):
    """SLIP_COLS 各列滑点。"""
    return [_slip(q, r, n, s) for n, s in SLIP_COLS]


def levels(r):
    """LEVEL_COLS 各列价位：r 朝方向 s 偏 p%。"""
    return [r * (1 + s * p / 100) for p, s in LEVEL_COLS]
