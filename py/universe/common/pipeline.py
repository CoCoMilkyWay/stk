"""universe 脚本的统一骨架 + 与 cpp 的 JSON 接口.

输出 config/universe/<name>.json:
  {"YYYYMMDD": {"add": [...], "del": [...]}, ...}
逐日相对前一交易日的 diff, 首日 add = 全量; 无变化的日子也写 (空 add/del), 读端据此区分 "池子没动" 和 "缺这天".
cpp 读端 (universe::Pool) 从空集按日回放: 回测区间 [start_date, end_date] 内的并集 = 静态 A 轴 (TS 特征对轴上
所有标的全程计算; 只看张量覆盖的时间段, 不是名单全史), 当日集合 → Meta 写 cs_valid (池内 1 / 池外 0),
CS 算子 / 因子评估只看它.

PIT: 交易日 D 的池子只用 D 的前一交易日 P 收盘后可见的数据决定 —— D 盘中算特征时池子已定.
区间 = 基本面数据全史: 从首个有截面数据的交易日 (作首个 P) 的下一交易日起, 到最后一个有 P 数据的交易日止;
排序键有暖机 (Key.warmup) 的, 起点推到 P ≥ 数据起点 + warmup.
python 不看 config.json 的回测区间 —— 回测/特征的时间段只由 cpp 管, 这里只负责把名单铺满全部可用数据.

调仓: freq = DAILY 逐日; WEEKLY 每周最后一个交易日 P 收盘后定池, 下周各交易日沿用.

选股口径 (所有池子同一套, 均在 P 上判定; 池子之间只差 排序键 / 高低端 / 调仓频率 / 行业白名单):
  母集
  - 板块 ∈ LIST_SECTORS; 申万 2021 一级行业 ∈ 白名单 (未知行业不入)
  - 未退市 (delist_date 为空或 > D; 静态表, 不能用 isna 否则未来退市被追溯剔除); 非停牌
  - 非 ST: st_status == 0 ∧ is_risk_warning == 0 (退市整理期摘 *ST 后 st_status 翻 0, 靠 is_risk_warning 兜住)
  - 上市满 NEW_LIST_DAYS 日历日; 市值 > 0
  filters (common/filters.py, 命中任一即剔除, 对齐 ~/work/qmt strat_0): trading_st / profit_st / revenue_st / dividend_st
  排序
  - 排序键有限值 (common/keys.py), LOW 取升序前 N, HIGH 取降序前 N
"""

import json
from dataclasses import dataclass, field
from typing import Any, Callable, Hashable

import numpy as np
import pandas as pd

from . import filters
from .data import (
    GEM,
    MAIN_BOARD,
    STAR,
    UNIVERSE_DIR,
    load_basic,
    load_panel,
    load_trading_days,
    month_range,
)
from .grid import Grid

# qmt strat_0 只要主板; 这里沪深三板都要, 北交所 (交易制度/流动性不同) 不要
LIST_SECTORS = {MAIN_BOARD, GEM, STAR}
NEW_LIST_DAYS = 60

DAILY, WEEKLY = "D", "W"  # 调仓频率 = pandas Period 频率: D 与 P 不同期即调仓
LOW, HIGH = "low", "high"  # 取排序键的低端 / 高端


@dataclass
class Ctx:
    months: list[str]
    basic: pd.DataFrame  # instrument / list_date / delist_date / list_sector
    panel: pd.DataFrame  # 日频截面 (date, instrument, ...)
    by_date: dict  # P → panel 切片
    g: Grid  # P 日 × 标的 网格
    trading_days: pd.DatetimeIndex
    main_board: np.ndarray  # 按 g.insts 的主板位
    shares: np.ndarray  # 总股本网格 (ffill)
    cache: dict = field(default_factory=dict)  # 池子间共享的中间结果, 见 memo

    def memo(self, name: Hashable, build: Callable[[], Any]) -> Any:
        """首次 build 后缓存, 各池子共用; ndarray 置只读, 共享网格不得被某个池子原地改写."""
        if name not in self.cache:
            v = build()
            if isinstance(v, np.ndarray):
                v.flags.writeable = False
            self.cache[name] = v
        return self.cache[name]


@dataclass(frozen=True)
class Key:
    """排序键: build(ctx) -> float64 网格 (P 日 × 标的), 升序 = 指标由低到高, NaN = 不参与排序."""

    warmup: pd.Timedelta  # 数据起点后多久键才完整 (事件流无期初快照)
    build: Callable[[Ctx], np.ndarray]


def build_excl(ctx: Ctx, industry_wl: set[str]) -> dict[str, np.ndarray]:
    """{filter 名: bool 网格}, 命中任一即剔除. 只有 industry 随白名单变, 其余池子间共享."""
    g, months = ctx.g, ctx.months
    fc = ctx.memo("forecast_intervals", lambda: filters.forecast_intervals(g, months))
    return {
        "trading_st": ctx.memo("trading_st", lambda: filters.trading_st(g, ctx.panel)),
        "profit_st": ctx.memo("profit_st", lambda: filters.profit_st(g, fc)),
        "revenue_st": ctx.memo(
            "revenue_st",
            lambda: filters.revenue_st(g, fc, months, ctx.main_board),
        ),
        "dividend_st": ctx.memo(
            "dividend_st",
            lambda: filters.dividend_st(
                g, months, ctx.shares, ctx.basic, ctx.main_board
            ),
        ),
        "industry": filters.industry_bad(
            *ctx.memo("industry_id", lambda: filters.industry_id(g, months)),
            industry_wl,
        ),
    }


def select(
    day: pd.DataFrame,
    excl_row: np.ndarray,
    key_row: np.ndarray,
    ctx: Ctx,
    asof: pd.Timestamp,
    target: pd.Timestamp,
    n: int,
    side: str,
) -> list[str]:
    """用 P=asof 收盘后的截面 day + 当行 TS 排除位 / 排序键 选 D=target 的池子; 返回升序代码列表."""
    cols = ctx.g.cols(day["instrument"])
    key = key_row[cols]
    ok = (
        day["list_sector"].isin(LIST_SECTORS)
        & (day["delist_date"].isna() | (day["delist_date"] > target))
        & (day["suspended"] == 0)
        & (day["st_status"] == 0)
        & (day["is_risk_warning"] == 0)
        & (day["list_date"] <= asof - pd.Timedelta(days=NEW_LIST_DAYS))
        & ~excl_row[cols]
        & (day["market_cap"] > 0)
        & np.isfinite(key)
    )
    k = pd.Series(key, index=day["instrument"].to_numpy())[ok.to_numpy()]
    assert len(k) >= n, f"{asof.date()}: 候选池只有 {len(k)} 只, 不足 {n} 只"
    top = k.nsmallest(n, keep="first") if side == LOW else k.nlargest(n, keep="first")
    return sorted(top.index.tolist())


def write_diff_json(name: str, daily: dict[str, list[str]]) -> None:
    """一天一行写 diff json; daily 为 {YYYYMMDD: 当日全量升序名单}, 按键序回放."""
    lines: list[str] = []
    prev: set[str] = set()
    for d, codes in sorted(daily.items()):
        cur = set(codes)
        add, rm = sorted(cur - prev), sorted(prev - cur)
        lines.append(f'  "{d}": {{"add": {json.dumps(add)}, "del": {json.dumps(rm)}}}')
        prev = cur
    UNIVERSE_DIR.mkdir(parents=True, exist_ok=True)
    out_path = UNIVERSE_DIR / f"{name}.json"
    out_path.write_text("{\n" + ",\n".join(lines) + "\n}\n", encoding="utf-8")
    print(f"已写入 (逐日 PIT diff): {out_path}")


def load_ctx() -> Ctx:
    """所有池子共用的截面 / 网格; 各池子在其上 run."""
    months = month_range()
    print(f"读取月份: {months[0]} ~ {months[-1]} ({len(months)} 个)")

    basic = load_basic()
    panel = load_panel(months, basic)
    by_date = {d: grp for d, grp in panel.groupby("date", sort=False)}
    g = Grid(
        pd.DatetimeIndex(sorted(by_date)),
        pd.Index(sorted(panel["instrument"].unique())),
    )
    print(f"截面数据: {len(panel)} 行, {g.n_d} 个交易日, {g.n_a} 只标的")

    main_board = (
        basic.set_index("instrument")["list_sector"]
        .reindex(g.insts)
        .eq(MAIN_BOARD)
        .to_numpy()
    )
    shares = g.pivot_ffill(panel, "total_shares")
    return Ctx(
        months,
        basic,
        panel,
        by_date,
        g,
        load_trading_days(months),
        main_board,
        shares,
    )


def run(
    ctx: Ctx,
    name: str,
    n: int,
    freq: str,
    key: Key,
    side: str,
    industry_wl: set[str],
) -> None:
    assert freq in (DAILY, WEEKLY), f"freq 只能是 DAILY / WEEKLY: {freq}"
    assert side in (LOW, HIGH), f"side 只能是 LOW / HIGH: {side}"
    g, by_date, trading_days = ctx.g, ctx.by_date, ctx.trading_days
    print(f"[{name}]")

    # 目标日 D = 前一交易日 P 有截面数据且 P 过了键暖机的交易日 (首个数据日只能当 P; 日历尾部超出数据的日子自然不出)
    has_prev = np.isin(trading_days[:-1].values, g.dates.values)
    warm = trading_days[:-1] >= g.dates[0] + key.warmup
    targets = trading_days[1:][has_prev & warm]
    assert len(targets) > 0, "没有任何交易日有前一日截面数据 (或全在键暖机内)"
    print(
        f"池子区间: {targets[0].date()} ~ {targets[-1].date()} ({len(targets)} 个交易日)"
    )

    # 命中统计只数当日在册的标的 (网格列含全部历史标的, 未上市/已退市列的 NaN 也会被 filter 判中)
    present = ctx.memo(
        "present", lambda: g.pivot(ctx.panel.assign(one=True), "one", bool, False)
    )
    excl = g.empty(bool, False)
    for fname, f in build_excl(ctx, industry_wl).items():
        assert (
            f.shape == excl.shape and f.dtype == bool
        ), f"filter {fname} 网格形状/类型不对"
        print(f"  {fname:12s} 日均命中 {(f & present).sum(axis=1).mean():.1f} 只")
        excl |= f
    print(f"  (当日在册平均 {present.sum(axis=1).mean():.0f} 只)")

    key_grid = ctx.memo(key, lambda: key.build(ctx))
    assert (
        key_grid.shape == excl.shape and key_grid.dtype == np.float64
    ), "排序键网格形状/类型不对"

    daily: dict[str, list[str]] = {}
    codes: list[str] = []
    prev: set[str] = set()
    union: set[str] = (
        set()
    )  # 全史并集 (仅统计; 回测区间内的并集由 cpp 自己按 config 算)
    turnover: list[int] = []
    n_rebal = 0
    for target in targets:
        asof = trading_days[trading_days.get_loc(target) - 1]
        assert (
            asof in by_date
        ), f"{target.date()} 的前一交易日 {asof.date()} 无截面数据, 需重新同步基本面"
        if not codes or target.to_period(freq) != asof.to_period(freq):
            r = g.row_of(asof)
            codes = select(
                by_date[asof], excl[r], key_grid[r], ctx, asof, target, n, side
            )
            assert len(codes) == n and codes == sorted(
                set(codes)
            ), f"{target.date()}: select 须返回 {n} 个去重升序代码"
            n_rebal += 1
        daily[target.strftime("%Y%m%d")] = codes
        cur = set(codes)
        if prev:
            turnover.append(len(cur - prev))
        prev = cur
        union |= cur

    print(
        f"逐日池: {len(daily)} 天 x {n} 只, 调仓 {n_rebal} 次, 全史并集 {len(union)} 只, "
        f"日均换手 {sum(turnover) / max(len(turnover), 1):.1f} 只 (最大 {max(turnover, default=0)})"
    )
    write_diff_json(name, daily)
