#!/usr/bin/env python3
"""
小市值 400 动态 universe → config/universe/smallcap400.json (格式 / PIT / 起止见 common/pipeline.py).

口径 (对齐 ~/work/qmt strat_0 的 pool_b + filters, 均在 P 上判定; 可调项在下方配置区):
  母集
  - 板块 ∈ LIST_SECTORS; 申万 2021 一级行业 ∈ INDUSTRY_L1_WL (未知行业不入)
  - 未退市 (delist_date 为空或 > D; 静态表, 不能用 isna 否则未来退市被追溯剔除); 非停牌
  - 非 ST: st_status == 0 ∧ is_risk_warning == 0 (退市整理期摘 *ST 后 st_status 翻 0, 靠 is_risk_warning 兜住)
  - 上市满 NEW_LIST_DAYS 日历日
  filters (common/filters.py, 命中任一即剔除): trading_st / profit_st / revenue_st / dividend_st
  排序
  - 市值 = total_shares(总股本) × close(收盘价, 未复权), 升序取前 N
"""
import sys

import numpy as np
import pandas as pd

from common import GEM, MAIN_BOARD, STAR, Ctx, filters, run

# ============================================================================ 配置
N = 400
OUTPUT_NAME = "smallcap400"

# qmt strat_0 只要主板; 这里沪深三板都要, 北交所 (交易制度/流动性不同) 不要
LIST_SECTORS = {MAIN_BOARD, GEM, STAR}

# 申万 2021 一级行业白名单 (与 qmt strat_0 同: 不要 农林牧渔 / 房地产 / 环保 / 钢铁 / 银行)
INDUSTRY_L1_WL = filters.SW2021_L1_ALL - {"农林牧渔", "房地产", "环保", "钢铁", "银行"}

NEW_LIST_DAYS = 60
# ============================================================================


def build_excl(ctx: Ctx) -> dict[str, np.ndarray]:
    g, months = ctx.g, ctx.months
    fc = filters.forecast_intervals(g, months)
    return {
        "trading_st": filters.trading_st(g, ctx.panel),
        "profit_st": filters.profit_st(g, fc),
        "revenue_st": filters.revenue_st(g, fc, months, ctx.main_board),
        "dividend_st": filters.dividend_st(
            g, months, ctx.shares, ctx.basic, ctx.main_board
        ),
        "industry": filters.industry_bad(g, months, INDUSTRY_L1_WL),
    }


def select(
    day: pd.DataFrame,
    excl_row: np.ndarray,
    ctx: Ctx,
    asof: pd.Timestamp,
    target: pd.Timestamp,
) -> list[str]:
    """用 P=asof 收盘后的截面 day + 当行 TS 排除位 选 D=target 的池子; 返回升序代码列表."""
    df = day[
        day["list_sector"].isin(LIST_SECTORS)
        & (day["delist_date"].isna() | (day["delist_date"] > target))
        & (day["suspended"] == 0)
        & (day["st_status"] == 0)
        & (day["is_risk_warning"] == 0)
        & (day["list_date"] <= asof - pd.Timedelta(days=NEW_LIST_DAYS))
        & ~excl_row[ctx.g.cols(day["instrument"])]
        & (day["market_cap"] > 0)
    ]
    assert len(df) >= N, f"{asof.date()}: 候选池只有 {len(df)} 只, 不足 {N} 只"
    return sorted(df.nsmallest(N, "market_cap", keep="first")["instrument"].tolist())


if __name__ == "__main__":
    sys.exit(run(OUTPUT_NAME, N, build_excl, select))
