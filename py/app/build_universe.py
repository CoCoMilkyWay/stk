#!/usr/bin/env python3
"""
从本地基本面 parquet 抽数据, 构建"小市值(非ST) 400"universe, 写入
config/universe/<name>.json (格式: ["600000.SH", ...], 供 Config::UniverseCodes 读取).

口径:
  - 市值 = total_shares(总股本) x close(最新收盘价, 未复权) —— 取最新交易日
  - 剔除: 当日 ST/*ST (cn_stock_status.st_status != 0)
  - 剔除: 已退市 (cn_stock_basic_info.delist_date 非空)
  - 剔除: 当日停牌 (cn_stock_status.suspended == 1)
  - 剔除: 次新股 (list_date 距最新交易日 < NEW_LIST_DAYS 天)
  - 剔除: 北交所 (.BJ, 交易制度/流动性与沪深不同)
  - 按市值升序取前 N 只
"""
import json
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
FUND_DIR = REPO_ROOT / "output" / "fundamental"
UNIVERSE_DIR = REPO_ROOT / "config" / "universe"

N = 400
NEW_LIST_DAYS = 180
OUTPUT_NAME = "smallcap400"


def latest_month_dir() -> Path:
    months = sorted(p.name for p in FUND_DIR.iterdir() if p.is_dir() and p.name[:2].isdigit())
    assert months, f"未找到月度分片目录: {FUND_DIR}"
    return FUND_DIR / months[-1]


def main() -> None:
    month_dir = latest_month_dir()

    status = pd.read_parquet(month_dir / "cn_stock_status.parquet")
    shares = pd.read_parquet(month_dir / "cn_stock_shares.parquet")
    bar1d = pd.read_parquet(month_dir / "cn_stock_real_bar1d.parquet")
    instruments = pd.read_parquet(month_dir / "cn_stock_instruments.parquet")
    basic = pd.read_parquet(FUND_DIR / "_meta" / "cn_stock_basic_info.parquet")

    latest_date = status["date"].max()
    assert latest_date == shares["date"].max() == bar1d["date"].max() == instruments["date"].max(), \
        "status/shares/real_bar1d/instruments 最新日期不一致, 需重新同步基本面数据"
    print(f"最新交易日: {latest_date.date()}")

    status = status[status["date"] == latest_date][["instrument", "st_status", "suspended"]]
    shares = shares[shares["date"] == latest_date][["instrument", "total_shares"]]
    bar1d = bar1d[bar1d["date"] == latest_date][["instrument", "close"]]
    # name 用 cn_stock_instruments 的 PIT 简称 (戴帽/改名当日即变), 而非
    # cn_stock_basic_info 的静态 name (改名后不追溯更新, 仅用于展示会误导).
    instruments = instruments[instruments["date"] == latest_date][["instrument", "name"]]

    df = status.merge(shares, on="instrument", how="inner") \
                .merge(bar1d, on="instrument", how="inner") \
                .merge(instruments, on="instrument", how="inner") \
                .merge(basic[["instrument", "list_date", "delist_date"]],
                       on="instrument", how="inner")
    print(f"当日在册: {len(df)}")

    df = df[df["delist_date"].isna()]
    print(f"剔除已退市: {len(df)}")

    df = df[df["st_status"] == 0]
    print(f"剔除ST/*ST: {len(df)}")

    df = df[df["suspended"] == 0]
    print(f"剔除当日停牌: {len(df)}")

    cutoff = latest_date - pd.Timedelta(days=NEW_LIST_DAYS)
    df = df[df["list_date"] <= cutoff]
    print(f"剔除次新股(上市<{NEW_LIST_DAYS}天): {len(df)}")

    df = df[~df["instrument"].str.endswith(".BJ")]
    print(f"剔除北交所(.BJ): {len(df)}")

    df["market_cap"] = df["total_shares"] * df["close"]
    df = df.sort_values("market_cap", ascending=True)

    assert len(df) >= N, f"候选池只有 {len(df)} 只, 不足 {N} 只"
    picked = df.head(N)

    print(f"最终 {len(picked)} 只, 市值区间: "
          f"{picked['market_cap'].min():.2e} ~ {picked['market_cap'].max():.2e}")
    print(picked[["instrument", "name", "market_cap"]].head(5))
    print("...")
    print(picked[["instrument", "name", "market_cap"]].tail(5))

    codes = sorted(picked["instrument"].tolist())
    UNIVERSE_DIR.mkdir(parents=True, exist_ok=True)
    out_path = UNIVERSE_DIR / f"{OUTPUT_NAME}.json"
    out_path.write_text(json.dumps(codes, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"已写入: {out_path}")


if __name__ == "__main__":
    sys.exit(main())
