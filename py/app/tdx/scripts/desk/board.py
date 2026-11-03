# desk/board — 持仓页一帧：内存持仓 × 当日成交 × 行情，按代码对齐（纯函数，只出数字，格式化归前端）。
# 三张表同以代码为键：持仓 pos.by_code、成交合计 fills.by_code、行情 quotes；对齐成 (h, f)：
#   h 为 None = 今日清仓（持仓表已无此行，只有当日列），f 为 NO_FLOW = 今日该只没有成交。
# 一行由几组列拼成（PARTS），每组 (h, f, q) → 值元组，缺了它要的输入即整组 None：
#   标识：代码 / 名称 / 分组 / 是否今日清仓 / 起买股数 / 现价 / 行情（盘口、K 线页用）；
#   持仓：量、成本、券商盈亏取自内存快照，随价变的按 Δ价 × 量 平移（保留券商口径）；
#   当日：按成交价自算（毛，不扣费）= 现市值 − 昨收 × 昨仓 − Σ 方向 × 额，昨仓 = 现量 − Σ 方向 × 量，
#         % 的分母为昨收 × 昨仓 + 今买额；与持仓的今买 / 今卖对不上即空；
#   盘口：各列滑点与起买股数的买 / 卖滑点（竞价时空）、价位，无参考价皆空。
# 占比两列同构 = 市值 / 分母：仓位% 的分母是券商口径的资产（快照的 市值 / 仓位 反推，取仓位最大那只、
# 舍入误差最小，再随总市值平移），占比的分母是总市值。

import collections

from mem.common import BUY, SIDES
from mem.fills import NO_FLOW

from . import book

# 持仓快照 × 成交快照 × {代码: 行情}
State = collections.namedtuple("State", "pos fills quotes")
TOTALS = ("market_value", "pnl", "day_pnl")
# fmt: off
GROUPS = {
    "green": {"600697", "301006", "300535", "605567", "603214", "600561", "300645", "301037"},
    "red":   set(),
}
# fmt: on


def _grp(code):
    return next((g for g, codes in GROUPS.items() if code in codes), "")


def join(st):
    """持仓 × 成交合计按代码对齐 → [(h, f)]，持仓在前、今日清仓在后；两表沪深标志须一致。"""
    held, flow = st.pos.by_code, st.fills.by_code
    codes = [*held, *(c for c in flow if c not in held)]
    pairs = [(held.get(c), flow.get(c, NO_FLOW)) for c in codes]
    bad = [
        (h["code"], h["name"], h["market_flag"], f["market_flag"])
        for h, f in pairs
        if h is not None and f.get("market_flag", h["market_flag"]) != h["market_flag"]
    ]
    assert not bad, f"成交与持仓的沪深标志不一致（代码, 名称, 持仓, 成交）：{bad}"
    return pairs


def _synced(h, f):
    """今买 / 今卖须与成交合计相符，不符即两表之间又有成交（其一旧了）；精简视图与今日清仓无可核。"""
    return h is None or h["traded"] is None or h["traded"] == f["traded"]


def synced(st):
    """每只都对得上。"""
    return all(_synced(h, f) for h, f in join(st))


def _ident(h, f, q):
    x = h or f
    return (
        x["code"],
        x["name"],
        _grp(x["code"]),
        h is None,
        book.lot(x["code"])[0],
        q["price"],
        q,
    )


def _hold(h, f, q):
    if h is None:
        return None
    px = q["price"]
    d = (px - h["price"]) * h["balance"]
    return (
        h["market_value"] + d,
        h["pnl"] + d,
        (px / h["cost"] - 1) * 100 if h["cost"] > 0 else None,
        None if h["financed"] is None else h["financed"] * px,
        h["available"] * px,
        h["collateral"],
        h["cost"],
    )


def _today(h, f, q):
    if not _synced(h, f):
        return None
    bal = 0.0 if h is None else h["balance"]
    net = {k: sum(s * f[k][s] for s in SIDES) for k in ("traded", "amount")}
    base = (bal - net["traded"]) * q["last_close"]
    day = bal * q["price"] - base - net["amount"]
    den = base + f["amount"][BUY]
    return day, day / den * 100 if den else None


def _depth(h, f, q):
    if h is None:
        return None
    r, auction = book.ref(q), book.auction(q)
    live = r and not auction
    return (
        auction,
        book.slips(q, r) if live else None,
        book.lot_slips(q, r, h["code"]) if live else None,
        book.levels(r) if r else None,
    )


# fmt: off
PARTS = (
    (("code", "name", "group", "closed", "lot", "price", "quote"),                                 _ident),
    (("market_value", "pnl", "pnl_pct", "margin_value", "sellable_value", "collateral", "cost"), _hold),
    (("day_pnl", "day_pnl_pct"),                                                                 _today),
    (("auction", "slips", "lot_slips", "levels"),                                                _depth),
)
# fmt: on


def _row(h, f, q):
    return {
        k: v
        for keys, part in PARTS
        for k, v in zip(keys, part(h, f, q) or (None,) * len(keys), strict=True)
    }


def _asset(held, mv):
    """券商口径的资产：快照的 市值 / 仓位 反推，再随总市值 mv 平移；快照无仓位回 None。"""
    top = max(held.values(), key=lambda h: h["position_pct"] or 0, default=None)
    if not (top and top["position_pct"]):
        return None
    base = top["market_value"] / top["position_pct"] * 100
    return base + mv - sum(h["market_value"] for h in held.values())


def _pct(x, den):
    return x / den * 100 if x is not None and den else None


def _agg(rows, i):
    """第 i 列滑点按占比加权。"""
    pairs = [
        (r["slips"][i]["pct"], r["weight"])
        for r in rows
        if r["slips"] and r["slips"][i]["pct"] is not None and r["weight"]
    ]
    den = sum(w for _, w in pairs)
    return sum(v * w for v, w in pairs) / den if den else None


def view(st):
    """State → 持仓页一帧。"""
    pairs = join(st)
    rows = [_row(h, f, st.quotes[(h or f)["code"]]) for h, f in pairs]
    mv = sum(r["market_value"] or 0 for r in rows)
    shares = {"position_pct": _asset(st.pos.by_code, mv), "weight": mv}
    for r in rows:
        r |= {k: _pct(r["market_value"], den) for k, den in shares.items()}
    return {
        "pid": st.pos.pid,
        "rows": rows,
        "slip_cols": book.SLIP_COLS,
        "level_cols": book.LEVEL_COLS,
        "totals": {k: round(sum(r[k] or 0 for r in rows), 2) for k in TOTALS}
        | {"slips": [_agg(rows, i) for i in range(len(book.SLIP_COLS))]},
        "fills": len(st.fills.rows),
        "synced": all(_synced(h, f) for h, f in pairs),
        "time": max((q["time"] for q in st.quotes.values()), default=""),
    }
