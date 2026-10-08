"""基本面 parquet 读取 (output/fundamental/<YYYY-MM>/ 月度分片) —— 所有 universe 脚本共用的数据入口."""

from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[3]
FUND_DIR = REPO_ROOT / "output" / "fundamental"
UNIVERSE_DIR = REPO_ROOT / "config" / "universe"

# cn_stock_basic_info.list_sector 编码
MAIN_BOARD, GEM, STAR, BSE = 1, 2, 3, 4  # 主板 / 创业板 / 科创板 / 北交所


def month_range() -> list[str]:
    """基本面全部月度分片 "YYYY-MM" (升序), 要求连续无缺月.
    所有 universe 统一铺满基本面全史: 回测/特征的时间段只由 cpp 的 config 管, 这里不看."""
    have = sorted(
        p.name for p in FUND_DIR.iterdir() if p.is_dir() and p.name[:2].isdigit()
    )
    assert have, f"未找到月度分片目录: {FUND_DIR}"
    months = pd.period_range(have[0], have[-1], freq="M").strftime("%Y-%m").tolist()
    missing = sorted(set(months) - set(have))
    assert not missing, f"月度分片不连续, 缺: {missing}"
    return months


def load_table(
    name: str, months: list[str], cols: list[str], filters=None
) -> pd.DataFrame:
    """跨月拼接一张表的指定列; filters 下推到 parquet 读取."""
    parts = [
        pd.read_parquet(FUND_DIR / m / f"{name}.parquet", columns=cols, filters=filters)
        for m in months
    ]
    return pd.concat(parts, ignore_index=True)


def load_grid_table(name: str, months: list[str], cols: list[str]) -> pd.DataFrame:
    """(date, instrument) 一行的日频表."""
    df = load_table(name, months, ["date", "instrument", *cols])
    assert not df.duplicated(
        ["date", "instrument"]
    ).any(), f"{name}: (date, instrument) 重复"
    return df


def load_trading_days(months: list[str]) -> pd.DatetimeIndex:
    """A 股交易日历 (表内含 CN / US / HK, 只取 CN)."""
    df = load_table("all_trading_days", months, ["date", "market_code"])
    days = pd.DatetimeIndex(df.loc[df["market_code"] == "CN", "date"]).sort_values()
    assert days.is_unique, "all_trading_days 跨月重复"
    return days


def load_basic() -> pd.DataFrame:
    basic = pd.read_parquet(
        FUND_DIR / "_meta" / "cn_stock_basic_info.parquet",
        columns=["instrument", "list_date", "delist_date", "list_sector"],
    )
    assert basic["instrument"].is_unique, "cn_stock_basic_info: instrument 重复"
    return basic


def load_panel(months: list[str], basic: pd.DataFrame) -> pd.DataFrame:
    """日频截面: status × shares × bar1d 按 (date, instrument) 内连接 + 静态字段 + 市值."""
    df = load_grid_table(
        "cn_stock_status", months, ["st_status", "is_risk_warning", "suspended"]
    )
    df = df.merge(
        load_grid_table("cn_stock_shares", months, ["total_shares"]),
        on=["date", "instrument"],
    )
    df = df.merge(
        load_grid_table("cn_stock_real_bar1d", months, ["close"]),
        on=["date", "instrument"],
    )
    df = df.merge(basic, on="instrument", how="inner")
    df["market_cap"] = df["total_shares"] * df["close"]
    df["main_board"] = df["list_sector"] == MAIN_BOARD
    return df
