"""Grid: 以有截面数据的交易日 (P 日) 为行、全部标的为列的 PIT 网格工具.

事件在 P 可见 ⟺ 可见日 ≤ P; 可见日 → 行 = 首个 ≥ 它的交易日 (searchsorted left).
与 qmt 的 CUTOFF=-1 同义 (row D 取 D-1 可见的数据; 这里直接以 P = D-1 为行).
"""

import numpy as np
import pandas as pd


class Grid:
    def __init__(self, dates: pd.DatetimeIndex, insts: pd.Index):
        assert dates.is_monotonic_increasing and dates.is_unique
        assert insts.is_unique
        self.dates, self.insts = dates, insts
        self.n_d, self.n_a = len(dates), len(insts)
        self.col = pd.Series(np.arange(self.n_a), index=insts)

    def rows(self, dts) -> np.ndarray:
        return self.dates.searchsorted(pd.DatetimeIndex(dts).values, side="left")

    def cols(self, insts) -> np.ndarray:
        c = self.col.reindex(insts)
        assert c.notna().all(), "标的不在网格列上"
        return c.to_numpy(np.int64)

    def row_of(self, p: pd.Timestamp) -> int:
        i = self.dates.get_loc(p)
        assert isinstance(i, (int, np.integer))
        return int(i)

    def empty(self, dtype, fill) -> np.ndarray:
        return np.full((self.n_d, self.n_a), fill, dtype=dtype)

    def pivot(self, df: pd.DataFrame, col: str, dtype, fill) -> np.ndarray:
        """日频表 (date, instrument, col) → 网格; 缺格 = fill."""
        arr = self.empty(dtype, fill)
        arr[self.rows(df["date"]), self.cols(df["instrument"])] = df[col].to_numpy(
            dtype
        )
        return arr

    def pivot_ffill(self, df: pd.DataFrame, col: str) -> np.ndarray:
        """日频表 → float32 网格, 缺格沿时间前向填充."""
        return ffill_rows(self.pivot(df, col, np.float32, np.nan))

    def event_ffill(self, df: pd.DataFrame, date_col: str, col: str) -> np.ndarray:
        """事件表 → float32 网格, 同格后到者覆盖, 沿时间前向填充; 事件前 NaN. 不在列上的标的忽略."""
        df = df.dropna(subset=["instrument"])
        df = df[df["instrument"].isin(self.insts)].sort_values(date_col, kind="stable")
        r, c = self.rows(df[date_col]), self.cols(df["instrument"])
        keep = r < self.n_d
        arr = self.empty(np.float32, np.nan)
        arr[r[keep], c[keep]] = df[col].to_numpy(np.float32)[keep]
        return ffill_rows(arr)


def ffill_rows(arr: np.ndarray) -> np.ndarray:
    """沿时间 (axis 0) 前向填充; 返回可写数组 (pandas CoW 下 to_numpy 是只读视图, 需拷贝)."""
    return pd.DataFrame(arr).ffill().to_numpy(copy=True)
