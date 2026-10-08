"""universe 脚本的统一骨架 + 与 cpp 的 JSON 接口.

输出 config/universe/<name>.json:
  {"YYYYMMDD": {"add": [...], "del": [...]}, ...}
逐日相对前一交易日的 diff, 首日 add = 全量; 无变化的日子也写 (空 add/del), 读端据此区分 "池子没动" 和 "缺这天".
cpp 读端 (universe::Pool) 从空集按日回放: 回测区间 [start_date, end_date] 内的并集 = 静态 A 轴 (TS 特征对轴上
所有标的全程计算; 只看张量覆盖的时间段, 不是名单全史), 当日集合 → Meta 写 cs_valid (池内 1 / 池外 0),
CS 算子 / 因子评估只看它.

PIT: 交易日 D 的池子只用 D 的前一交易日 P 收盘后可见的数据决定 —— D 盘中算特征时池子已定.
区间 = 基本面数据全史: 从首个有截面数据的交易日 (作首个 P) 的下一交易日起, 到最后一个有 P 数据的交易日止.
python 不看 config.json 的回测区间 —— 回测/特征的时间段只由 cpp 管, 这里只负责把名单铺满全部可用数据.
"""

import json
from dataclasses import dataclass
from typing import Callable

import numpy as np
import pandas as pd

from .data import (
    UNIVERSE_DIR,
    MAIN_BOARD,
    load_basic,
    load_panel,
    load_trading_days,
    month_range,
)
from .grid import Grid


@dataclass
class Ctx:
    months: list[str]
    basic: pd.DataFrame  # instrument / list_date / delist_date / list_sector
    panel: pd.DataFrame  # 日频截面 (date, instrument, ...)
    by_date: dict  # P → panel 切片
    g: Grid  # P 日 × 标的 网格
    trading_days: pd.DatetimeIndex
    targets: pd.DatetimeIndex  # 要出池子的交易日 D
    main_board: np.ndarray  # 按 g.insts 的主板位
    shares: np.ndarray  # 总股本网格 (ffill)


# select(day, excl_row, ctx, asof=P, target=D) -> 升序代码列表
SelectFn = Callable[
    [pd.DataFrame, np.ndarray, Ctx, pd.Timestamp, pd.Timestamp], list[str]
]
# build_excl(ctx) -> {filter 名: bool 网格}, 命中任一即剔除
ExclFn = Callable[[Ctx], dict[str, np.ndarray]]


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


def run(name: str, n: int, build_excl: ExclFn, select: SelectFn) -> None:
    months = month_range()
    print(f"[{name}] 读取月份: {months[0]} ~ {months[-1]} ({len(months)} 个)")

    basic = load_basic()
    panel = load_panel(months, basic)
    by_date = {d: grp for d, grp in panel.groupby("date", sort=False)}
    g = Grid(
        pd.DatetimeIndex(sorted(by_date)),
        pd.Index(sorted(panel["instrument"].unique())),
    )
    print(f"截面数据: {len(panel)} 行, {g.n_d} 个交易日, {g.n_a} 只标的")

    # 目标日 D = 前一交易日 P 有截面数据的交易日 (首个数据日只能当 P; 日历尾部超出数据的日子自然不出)
    trading_days = load_trading_days(months)
    has_prev = np.isin(trading_days[:-1].values, g.dates.values)
    targets = trading_days[1:][has_prev]
    assert len(targets) > 0, "没有任何交易日有前一日截面数据"
    print(
        f"池子区间: {targets[0].date()} ~ {targets[-1].date()} ({len(targets)} 个交易日)"
    )

    main_board = (
        basic.set_index("instrument")["list_sector"]
        .reindex(g.insts)
        .eq(MAIN_BOARD)
        .to_numpy()
    )
    shares = g.pivot_ffill(panel, "total_shares")
    ctx = Ctx(
        months, basic, panel, by_date, g, trading_days, targets, main_board, shares
    )

    # 命中统计只数当日在册的标的 (网格列含全部历史标的, 未上市/已退市列的 NaN 也会被 filter 判中)
    present = g.pivot(panel.assign(one=True), "one", bool, False)
    excl = g.empty(bool, False)
    for fname, f in build_excl(ctx).items():
        assert (
            f.shape == excl.shape and f.dtype == bool
        ), f"filter {fname} 网格形状/类型不对"
        print(f"  {fname:12s} 日均命中 {(f & present).sum(axis=1).mean():.1f} 只")
        excl |= f
    print(f"  (当日在册平均 {present.sum(axis=1).mean():.0f} 只)")

    daily: dict[str, list[str]] = {}
    prev: set[str] = set()
    union: set[str] = (
        set()
    )  # 全史并集 (仅统计; 回测区间内的并集由 cpp 自己按 config 算)
    turnover: list[int] = []
    for target in targets:
        asof = trading_days[trading_days.get_loc(target) - 1]
        assert (
            asof in by_date
        ), f"{target.date()} 的前一交易日 {asof.date()} 无截面数据, 需重新同步基本面"
        codes = select(by_date[asof], excl[g.row_of(asof)], ctx, asof, target)
        assert len(codes) == n and codes == sorted(
            set(codes)
        ), f"{target.date()}: select 须返回 {n} 个去重升序代码"
        daily[target.strftime("%Y%m%d")] = codes
        cur = set(codes)
        if prev:
            turnover.append(len(cur - prev))
        prev = cur
        union |= cur

    print(
        f"逐日池: {len(daily)} 天 x {n} 只, 全史并集 {len(union)} 只, "
        f"日均换手 {sum(turnover) / max(len(turnover), 1):.1f} 只 (最大 {max(turnover, default=0)})"
    )
    write_diff_json(name, daily)
