"""排序键 (pipeline.Key): 升序 = 指标由低到高, 池子取低端 (LOW) 或高端 (HIGH) 前 N.

口径对齐 cpp L1 (features/Method/Fundamental.cpp) 与 FundamentalService 估值快照 (qmt 移植), 均在 P 上可见:
  SIZE  mcap = close (未复权) × total_shares
  PE    mcap / 归母净利 TTM   (ttm_shift shift=0 的最新可见值)
  PB    mcap / 归母权益 MRQ   (资产负债表 PIT: 最大报告期的最新可见版本)
  DY    近 DY_WINDOW 内公告 (publish_date) 的 税前每股分红 × 公告时总股本 之和 / mcap; 无分红 = 0
  分母为空 / 0 的财报事件丢弃 (回退到上一有效期次).
PE / PB 按 -分母/mcap 升序排, 亏损 / 负净资产连续接在正值高端之后: PE 2 < 1000 < -1000 < -2.
暖机: 财报 / 分红是事件流, 首个月度分片无期初快照, 数据起点后 EVENT_WARMUP 内多数标的尚无值或滑窗不满, 不出池.
"""

import numpy as np
import pandas as pd

from .data import load_table
from .pipeline import Ctx, Key

EVENT_WARMUP = pd.Timedelta(days=365)
DY_WINDOW = pd.Timedelta(days=365)


def _mcap(ctx: Ctx) -> np.ndarray:
    """市值网格; ≤ 0 置 NaN (作分母). 各键共用."""

    def build() -> np.ndarray:
        mcap = ctx.g.pivot(ctx.panel, "market_cap", np.float64, np.nan)
        mcap[~(mcap > 0)] = np.nan
        return mcap

    return ctx.memo("mcap", build)


def _np_ttm(ctx: Ctx) -> np.ndarray:
    col = "net_profit_to_parent_shareholders_ttm"
    ttm = load_table(
        "cn_stock_financial_ttm_shift",
        ctx.months,
        ["date", "instrument", col],
        filters=[("shift", "==", 0)],
    )
    ttm = ttm[ttm[col].notna() & (ttm[col] != 0)]
    return ctx.g.event_ffill(ttm, "date", col)


def _equity_mrq(ctx: Ctx) -> np.ndarray:
    """同报告期取最新可见版本; 晚到的旧报告期重述不覆盖新报告期."""
    col = "total_equity_to_parent_shareholders"
    bal = load_table(
        "cn_stock_financial_balance_general_pit",
        ctx.months,
        ["date", "instrument", "report_date", col],
    )
    bal = bal[bal[col].notna() & (bal[col] != 0)].sort_values(
        ["instrument", "date", "report_date"], kind="stable"
    )
    bal = bal[bal["report_date"] == bal.groupby("instrument")["report_date"].cummax()]
    return ctx.g.event_ffill(bal, "date", col)


def _cash_div(ctx: Ctx) -> np.ndarray:
    """DY_WINDOW 内公告的税前分红总额 [元]; 公告日 publish_date 起入窗, +DY_WINDOW 起出窗."""
    g = ctx.g
    dv = load_table(
        "cn_stock_dividend",
        ctx.months,
        ["instrument", "publish_date", "cash_before_tax"],
    )
    dv = dv[dv["instrument"].isin(g.insts) & (dv["cash_before_tax"] > 0)]
    lo = g.rows(dv["publish_date"])
    hi = g.rows(dv["publish_date"] + DY_WINDOW)
    cols = g.cols(dv["instrument"])
    amt = dv["cash_before_tax"].to_numpy() * ctx.shares[np.minimum(lo, g.n_d - 1), cols]
    cash = g.empty(np.float64, 0.0)
    for c, a, b, x in zip(cols, lo, hi, amt):
        if np.isfinite(x):
            cash[a:b, c] += x
    return cash


def _size(ctx: Ctx) -> np.ndarray:
    return _mcap(ctx)


def _pe(ctx: Ctx) -> np.ndarray:
    return -_np_ttm(ctx) / _mcap(ctx)


def _pb(ctx: Ctx) -> np.ndarray:
    return -_equity_mrq(ctx) / _mcap(ctx)


def _dy(ctx: Ctx) -> np.ndarray:
    return _cash_div(ctx) / _mcap(ctx)


SIZE = Key(pd.Timedelta(0), _size)
PE = Key(EVENT_WARMUP, _pe)
PB = Key(EVENT_WARMUP, _pb)
DY = Key(EVENT_WARMUP, _dy)
