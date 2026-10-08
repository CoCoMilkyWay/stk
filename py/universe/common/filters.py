"""退市风险类 TS 排除位 (移植 ~/work/qmt 的 filters), 每个返回 bool 网格 (P 日 × 标的), True = 剔除.

全部在 P 日判定, 事件按可见日 ≤ P 回放. 阈值默认 = qmt 口径, 脚本可按需覆盖.
  trading_st   连续 W 个交易日 (close < 1 ∨ 市值 < 板块阈值) —— 面值/市值退市预警, 比交易所 20 日线提前, 换可执行退出窗口
  profit_st    年报预告预亏 (end_date 为 12 月 ∧ type ∈ {首亏, 续亏} ∧ 上年归母净利 < 0), ann_date 起生效,
               至正式年报 PIT 首次可见或次年 4/30 终止 (取早)
  revenue_st   主板 ∧ profit_st 区间内 ∧ 最新 TTM 营业总收入 < 阈值 (年报年度 ≥ 2024 为 3 亿, 否则 1 亿) —— 2021 退市新规起
  dividend_st  主板 ∧ 近两份年报归母净利均值 > 0 ∧ 近 3 年累计现金分红 < 30% 净利 ∧ < 5000 万 (数据起点/上市后 3 年内不判)
  industry_bad 申万 2021 一级行业 ∉ 白名单 (含未知); 月初 component 快照 + 月内 change_flag=1 事件回放
"""

import numpy as np
import pandas as pd

from .data import load_table
from .grid import Grid

TRADING_ST_WINDOW = 15
LOW_PRICE_THR = 1.0
LOW_MC_THR = {True: 5e8, False: 3e8}  # 主板 / 非主板 市值退市线 [元]
REV_RULE_START = pd.Timestamp(
    "2021-01-01"
)  # 营收退市新规: 预告公告日 ≥ 此且年报年度 ≥ 2021
DIV_RATIO, DIV_ABS = 0.30, 5e7
DIV_WARMUP_YEARS = 3

SW2021_L1_ALL = {
    "交通运输", "传媒", "公用事业", "农林牧渔", "医药生物", "商贸零售", "国防军工", "基础化工",
    "家用电器", "建筑材料", "建筑装饰", "房地产", "有色金属", "机械设备", "汽车", "煤炭", "环保",
    "电力设备", "电子", "石油石化", "社会服务", "纺织服饰", "综合", "美容护理", "计算机",
    "轻工制造", "通信", "钢铁", "银行", "非银金融", "食品饮料",
}  # fmt: skip


def rev_thr(end_year: int) -> float:
    """营收退市线 [元], 按年报年度."""
    return 3e8 if end_year >= 2024 else 1e8


def trading_st(
    g: Grid, panel: pd.DataFrame, window: int = TRADING_ST_WINDOW
) -> np.ndarray:
    """连续 window 个交易日 (close < 1 ∨ 市值 < 板块阈值). 缺行 (无数据) 断开连击."""
    thr = panel["main_board"].map(LOW_MC_THR).to_numpy()
    low = ((panel["close"] > 0) & (panel["close"] < LOW_PRICE_THR)) | (
        (panel["market_cap"] > 0) & (panel["market_cap"] < thr)
    )
    arr = g.pivot(panel.assign(low=low.astype(np.int8)), "low", np.int8, 0)
    run = pd.DataFrame(arr).rolling(window, min_periods=window).sum()
    return run.to_numpy() >= window


def forecast_intervals(g: Grid, months: list[str]) -> pd.DataFrame:
    """年报预亏预告 → [on_row, off_row) 行区间 (+ end_year, ann_date). off = min(对应年报 PIT 首次可见日, 次年 4/30)."""
    fc = load_table(
        "forecast",
        months,
        ["ts_code", "ann_date", "end_date", "type", "last_parent_net"],
    ).rename(columns={"ts_code": "instrument"})
    assert (
        fc[["instrument", "ann_date", "end_date"]].notna().all().all()
    ), "forecast 关键列有空"
    fc["ann_date"] = pd.to_datetime(fc["ann_date"], format="%Y%m%d")
    fc["end_date"] = pd.to_datetime(fc["end_date"], format="%Y%m%d")
    fc = fc[
        (fc["end_date"].dt.month == 12)
        & fc["type"].isin(["首亏", "续亏"])
        & (fc["last_parent_net"] < 0)
        & fc["instrument"].isin(g.insts)
    ]

    inc = load_table(
        "cn_stock_financial_income_general_pit",
        months,
        ["date", "instrument", "report_date"],
        filters=[("fs_quarter_index", "==", 4)],
    )
    first_seen = inc.groupby(["instrument", "report_date"])["date"].min()
    seen = first_seen.reindex(
        pd.MultiIndex.from_frame(fc[["instrument", "end_date"]])
    ).to_numpy()
    deadline = pd.to_datetime(
        (fc["end_date"].dt.year + 1).astype(str) + "-04-30"
    ).to_numpy()
    off = np.where(np.isnat(seen), deadline, np.minimum(seen, deadline))
    fc = fc.assign(
        on_row=g.rows(fc["ann_date"]),
        off_row=g.rows(off),
        end_year=fc["end_date"].dt.year,
    )
    return fc[fc["on_row"] < fc["off_row"]]


def profit_st(g: Grid, fc: pd.DataFrame) -> np.ndarray:
    arr = g.empty(bool, False)
    for c, on, off in zip(g.cols(fc["instrument"]), fc["on_row"], fc["off_row"]):
        arr[on:off, c] = True
    return arr


def revenue_st(
    g: Grid, fc: pd.DataFrame, months: list[str], main_board: np.ndarray
) -> np.ndarray:
    """主板 ∧ 预亏区间内 ∧ 最新 TTM 营收 < 阈值. TTM ≤ 0 视为脏值 (否则阈值恒真)."""
    ttm = load_table(
        "cn_stock_financial_ttm_shift",
        months,
        ["date", "instrument", "total_operating_revenue_ttm"],
        filters=[("shift", "==", 0)],
    )
    rev = g.event_ffill(ttm, "date", "total_operating_revenue_ttm")
    rev[~(rev > 0)] = np.nan
    fc = fc[
        (fc["end_year"] >= REV_RULE_START.year) & (fc["ann_date"] >= REV_RULE_START)
    ]
    arr = g.empty(bool, False)
    for c, on, off, y in zip(
        g.cols(fc["instrument"]), fc["on_row"], fc["off_row"], fc["end_year"]
    ):
        if main_board[c]:
            arr[on:off, c] = rev[on:off, c] < rev_thr(y)
    return arr


def dividend_st(
    g: Grid,
    months: list[str],
    shares: np.ndarray,
    basic: pd.DataFrame,
    main_board: np.ndarray,
) -> np.ndarray:
    """主板 ∧ ni > 0 ∧ 3y 累计现金分红 < DIV_RATIO × ni ∧ < DIV_ABS.
    ni = 最近可见的两份 (不同报告期) 年报归母净利均值 (只有一份取一份); 每条年报事件后阶梯更新.
    3y_sum: 每条分红事件 (可见日 = publish_date) 后重算 = Σ 此前事件中 report_date 年 ∈ [Y-3, Y-1]
    的 每股税后现金 × 当时总股本 (Y = 本次 publish 年); 无事件 = 0 (零分红也算不足).
    暖机: P 年 < max(数据起点年, 上市年) + 3 不判; list_date 缺失永不判."""
    inc = load_table(
        "cn_stock_financial_income_general_pit",
        months,
        ["date", "instrument", "report_date", "net_profit_to_parent_shareholders"],
        filters=[("fs_quarter_index", "==", 4)],
    )
    inc = inc.dropna(subset=["net_profit_to_parent_shareholders"])
    inc = inc[inc["instrument"].isin(g.insts)].sort_values("date", kind="stable")
    ni = g.empty(np.float32, np.nan)
    for inst, ev in inc.groupby("instrument", sort=False):
        c = g.col[inst]
        rows = g.rows(ev["date"])
        rds, vals = (
            ev["report_date"].to_numpy(),
            ev["net_profit_to_parent_shareholders"].to_numpy(),
        )
        latest: dict = {}  # report_date -> (val, row)  同报告期多版本取最新可见
        for i in range(len(ev)):
            latest[rds[i]] = (vals[i], rows[i])
            top = sorted(latest.values(), key=lambda t: -t[1])[:2]
            nxt = rows[i + 1] if i + 1 < len(ev) else g.n_d
            ni[rows[i] : nxt, c] = np.mean([t[0] for t in top])

    dv = load_table(
        "cn_stock_dividend",
        months,
        ["instrument", "publish_date", "report_date", "cash_after_tax"],
    )
    dv = dv[dv["instrument"].isin(g.insts)].sort_values("publish_date", kind="stable")
    sum3y = g.empty(np.float32, 0.0)
    for inst, ev in dv.groupby("instrument", sort=False):
        c = g.col[inst]
        rows = g.rows(ev["publish_date"])
        ann_y = ev["publish_date"].dt.year.to_numpy()
        rep_y = ev["report_date"].dt.year.to_numpy()
        cash = ev["cash_after_tax"].to_numpy()
        sh = shares[np.minimum(rows, g.n_d - 1), c]
        amt = np.where(np.isfinite(cash) & np.isfinite(sh), cash * sh, 0.0)
        for k in range(len(ev)):
            lo, hi = ann_y[k] - 3, ann_y[k] - 1
            s = amt[: k + 1][(rep_y[: k + 1] >= lo) & (rep_y[: k + 1] <= hi)].sum()
            nxt = rows[k + 1] if k + 1 < len(ev) else g.n_d
            sum3y[rows[k] : nxt, c] = s

    year = g.dates.year.to_numpy()[:, None]
    list_year = (
        basic.set_index("instrument")["list_date"].reindex(g.insts).dt.year.to_numpy()
    )
    warm = (
        year >= np.maximum(year[0, 0], list_year)[None, :] + DIV_WARMUP_YEARS
    )  # NaT → NaN → 永不判
    return (
        main_board[None, :]
        & warm
        & (ni > 0)
        & (sum3y < DIV_RATIO * ni)
        & (sum3y < DIV_ABS)
    )


def industry_bad(g: Grid, months: list[str], whitelist: set[str]) -> np.ndarray:
    """申万 2021 一级行业 ∉ whitelist (含未知)."""
    assert (
        whitelist <= SW2021_L1_ALL
    ), f"行业白名单拼写不在申万 2021 一级表内: {whitelist - SW2021_L1_ALL}"
    comp = load_table(
        "cn_stock_industry_component",
        months,
        ["date", "instrument", "industry_level1_name"],
        filters=[("industry", "==", "sw2021")],
    ).rename(columns={"industry_level1_name": "name"})
    chg = load_table(
        "cn_stock_industry_change",
        months,
        ["date", "instrument", "industry_name"],
        filters=[
            ("industry", "==", "sw2021"),
            ("industry_level", "==", 1),
            ("change_flag", "==", 1),
        ],
    ).rename(columns={"industry_name": "name"})
    ev = pd.concat([comp, chg], ignore_index=True)
    names = sorted(ev["name"].dropna().unique())
    assert (
        set(names) <= SW2021_L1_ALL
    ), f"数据里有申万 2021 一级表外的行业名: {set(names) - SW2021_L1_ALL}"
    ev["id"] = ev["name"].map({n: i for i, n in enumerate(names)})  # 事件前 NaN = 未知
    ind = g.event_ffill(ev, "date", "id")
    ok_ids = np.array(
        [i for i, n in enumerate(names) if n in whitelist], dtype=np.float32
    )
    return ~np.isin(ind, ok_ids)
