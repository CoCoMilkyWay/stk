"""标的区间位置排序: (现价 - 价格低位) / (过滤插针后价格高位 - 价格低位)

用法:
    python py/rank_17.py
"""

from pathlib import Path
import urllib.request
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
FUND_DIR = REPO_ROOT / "output" / "fundamental"

START_DATE = "2026-05-01"
MEDIAN_WINDOW = 5  # 滚动中位数窗口 (交易日): 滤除持续 <= (MEDIAN_WINDOW-1)//2 天的拉板/向上插针

CODES = [
    "600512.SH",
    "600168.SH",
    "002863.SZ",
    "600805.SH",
    "603357.SH",
    "000726.SZ",
    "002521.SZ",
    "002656.SZ",
    "600218.SH",
    "603022.SH",
    "603035.SH",
    "003003.SZ",
    "300645.SZ",
    "603214.SH",
    "002316.SZ",
    "002494.SZ",
    "002513.SZ",
    "300482.SZ",
    "300923.SZ",
    "000929.SZ",
    "301131.SZ",
    "688068.SH",
    "600051.SH",
]


def fetch_realtime_quotes(
    codes: list[str],
) -> dict[str, tuple[str, float, str, float, float]]:
    """从腾讯行情接口拉取最新实时行情: 返回 {code: (name, cur_price, quote_date, pe_ttm, pb)}."""
    q_list = [
        ("sh" if c.endswith(".SH") else "sz") + c[:6] for c in codes
    ]
    url = "http://qt.gtimg.cn/q=" + ",".join(q_list)
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    raw = urllib.request.urlopen(req, timeout=10).read().decode("gbk")
    out: dict[str, tuple[str, float, str, float, float]] = {}
    for code, line in zip(codes, raw.strip().splitlines()):
        fields = line.split("~")
        assert len(fields) > 46 and float(fields[3]) > 0, f"行情解析失败: {code}"
        name = fields[1].replace(" ", "").replace("Ａ", "A")
        cur_price = float(fields[3])
        quote_date = f"{fields[30][:4]}-{fields[30][4:6]}-{fields[30][6:8]}"
        pe_ttm = float(fields[39]) if fields[39] else float("nan")
        pb = float(fields[46]) if fields[46] else float("nan")
        out[code] = (name, cur_price, quote_date, pe_ttm, pb)
    return out


def load_local_kline(codes: list[str], start_date: str) -> pd.DataFrame:
    """从本地 output/fundamental/<YYYY-MM>/cn_stock_real_bar1d.parquet 读取日线."""
    start_ym = start_date[:7]
    months = sorted(
        p.name
        for p in FUND_DIR.iterdir()
        if p.is_dir() and p.name[:2].isdigit() and p.name >= start_ym
    )
    assert months, f"未找到 >= {start_ym} 的月度分片: {FUND_DIR}"
    parts = [
        pd.read_parquet(
            FUND_DIR / m / "cn_stock_real_bar1d.parquet",
            filters=[("instrument", "in", codes)],
        )
        for m in months
    ]
    df = pd.concat(parts, ignore_index=True)
    df = df[df["date"] >= start_date].sort_values(
        ["instrument", "date"]).reset_index(drop=True)
    assert not df.empty, f"区间 >= {start_date} 无日线数据"
    return df


def _pad(s: str, width: int, align: str = "right") -> str:
    """按终端显示宽度 (GBK 字节数: 中文=2, ASCII=1) 对齐字符串."""
    disp_w = len(s.encode("gbk"))
    pad = max(0, width - disp_w)
    return (" " * pad + s) if align == "right" else (s + " " * pad)


def _ansi_gradient(ratio: float) -> str:
    """ratio in [0, 1]: 0=低估(绿) -> 0.5=中位(黄) -> 1=高估(橙红)."""
    r = int(46 + (235 - 46) * ratio)
    g = int(204 - (204 - 87) * (ratio**1.3))
    b = int(113 - (113 - 87) * ratio)
    return f"\033[38;2;{r};{g};{b}m"


def colorize_cs(series: pd.Series, width: int = 8) -> list[str]:
    """截面颜色对比: 正值按截面分位渐变 (绿->黄->红), 负值(亏损)标紫红."""
    pos_vals = series[series > 0].dropna()
    ranks = pos_vals.rank(pct=True) if len(
        pos_vals) > 1 else pd.Series(0.5, index=pos_vals.index)
    out: list[str] = []
    for idx, val in series.items():
        if pd.isna(val):
            out.append(f"{'N/A':>{width}}")
            continue
        txt = f"{val:>{width}.2f}"
        if val <= 0:
            # 亏损 (PE <= 0) 单独高亮为洋红/紫色
            out.append(f"\033[38;2;198;120;221m{txt}\033[0m")
        else:
            pct = float(ranks.loc[idx])
            ratio = (pct - 1.0 / len(pos_vals)) / \
                max(1e-9, 1.0 - 1.0 / len(pos_vals))
            out.append(f"{_ansi_gradient(ratio)}{txt}\033[0m")
    return out


def main() -> None:
    df = load_local_kline(CODES, START_DATE)
    rt = fetch_realtime_quotes(CODES)

    rows = []
    for code in CODES:
        g = df[df["instrument"] == code].sort_values(
            "date").reset_index(drop=True)
        assert not g.empty, f"缺失标的日线: {code}"

        name, cur, rt_date, pe_ttm, pb = rt[code]
        last_bar_date = g["date"].iloc[-1].strftime("%Y-%m-%d")

        # 以最新一期复权因子做前复权对齐
        ratio = g["adjust_factor"] / g["adjust_factor"].iloc[-1]
        c_adj = g["close"] * ratio
        h_adj = g["high"] * ratio
        l_adj = g["low"] * ratio

        # 若实时行情日期晚于本地最新日线日期, 把最新现价并入序列尾部
        if rt_date > last_bar_date:
            c_all = pd.concat([c_adj, pd.Series([cur])], ignore_index=True)
            h_all = pd.concat([h_adj, pd.Series([cur])], ignore_index=True)
            l_all = pd.concat([l_adj, pd.Series([cur])], ignore_index=True)
        else:
            c_all, h_all, l_all = c_adj, h_adj, l_adj

        # 过滤只持续几天的拉板/向上插针: 取滚动 MEDIAN_WINDOW 日收盘中位数的最大值作为有效高位
        med_c = c_all.rolling(MEDIAN_WINDOW, min_periods=max(
            2, MEDIAN_WINDOW // 2)).median()
        high_filtered = float(med_c.max())
        low_price = float(c_all.min())

        high_raw = float(h_all.max())
        low_raw = float(l_all.min())

        assert high_filtered > low_price, f"高低位退化: {code}"
        pos_filtered = (cur - low_price) / (high_filtered - low_price) * 100.0
        pos_raw = (cur - low_raw) / (high_raw - low_raw) * 100.0
        spike_cut = (high_raw - high_filtered) / high_filtered * 100.0

        rows.append(
            {
                "代码": code,
                "名称": name,
                "现价": cur,
                "PE": round(pe_ttm, 2),
                "PB": round(pb, 2),
                "低位(收盘min)": round(low_price, 2),
                f"去针高位({MEDIAN_WINDOW}d中位)": round(high_filtered, 2),
                "盘中最高(含针)": round(high_raw, 2),
                "削针幅度%": round(spike_cut, 2),
                "区间位置%": round(pos_filtered, 2),
                "未去针位置%": round(pos_raw, 2),
            }
        )

    res = pd.DataFrame(rows).sort_values("区间位置%").reset_index(drop=True)
    res.index = res.index + 1

    pe_colored = colorize_cs(res["PE"], width=8)
    pb_colored = colorize_cs(res["PB"], width=6)
    pos_colored = colorize_cs(res["区间位置%"], width=8)

    high_col = f"去针高位({MEDIAN_WINDOW}d中位)"
    print(f"\n=== 区间位置排序 ({START_DATE} 至今, 滚动 {MEDIAN_WINDOW} 日中位数去插针) ===")
    print("颜色说明: \033[38;2;46;204;113m绿色=截面低位/低估值\033[0m -> \033[38;2;235;87;87m红色=截面高位/高估值\033[0m, \033[38;2;198;120;221m紫色=亏损(PE<0)\033[0m\n")

    cols = [
        ("排名", 4, "right"),
        ("代码", 9, "left"),
        ("名称", 8, "left"),
        ("现价", 6, "right"),
        ("PE", 8, "right"),
        ("PB", 6, "right"),
        ("低位", 6, "right"),
        ("去针高位", 8, "right"),
        ("盘中最高", 8, "right"),
        ("削针%", 6, "right"),
        ("区间位置%", 9, "right"),
        ("未去针%", 7, "right"),
    ]
    header = "  ".join(_pad(title, w, align) for title, w, align in cols)
    print(header)
    print("-" * len(header.encode("gbk")))

    for i, r in res.iterrows():
        idx = i - 1
        row_cells = [
            _pad(str(i), 4, "right"),
            _pad(r["代码"], 9, "left"),
            _pad(r["名称"], 8, "left"),
            f"{r['现价']:>6.2f}",
            pe_colored[idx],
            pb_colored[idx],
            f"{r['低位(收盘min)']:>6.2f}",
            f"{r[high_col]:>8.2f}",
            f"{r['盘中最高(含针)']:>8.2f}",
            f"{r['削针幅度%']:>5.1f}%",
            f"{pos_colored[idx]}%",
            f"{r['未去针位置%']:>6.2f}%",
        ]
        print("  ".join(row_cells))


if __name__ == "__main__":
    main()
