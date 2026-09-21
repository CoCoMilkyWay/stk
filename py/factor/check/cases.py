"""
对拍输入生成: 每个算子若干 Case (输入张量 + 参数). 合成数据为主, 刻意覆盖边界:
  NaN 洞 / 整行 NaN / 常值段 (方差 0, 全并列) / 整数取值 (并列) / 厚尾 / 零与负 (分母, 对数) / 日界落在窗内 / 短日 (样本不足).
分组列 (Group* 的 y, GroupResid 的 z) 为 0..G−1 整数 + NaN. 桶数 / 阈值 / 窗口 见 PARAMS.
"""

import zlib
from dataclasses import dataclass, field

import numpy as np

from ..ops import Param

# 形状: CUM 用 (日数, 每日分钟) 决定 T; ROLL 行 = 期; CS 行 = 截面
SHAPES = {
    "ELEM": dict(T=200, A=16),
    "CUM": dict(days=(60, 45, 3, 80), A=12),  # 含 3 分钟短日
    "ROLL": dict(T=160, A=12),
    "CS": dict(T=24, A=96),
}

# 参数: 每个算子 (按其 params 声明) 的取值列表 → 每组值一个 Case
PARAMS = {
    "d": [1, 5, 20],
    "k": {"SignedPow": [0.5, 2.0], "Clip": [1.0], "CumTopK": [3], "CumPeaks": [1.0, 0.0], "CumCountGt": [0.0], "TsEma": [0.3], "TsCountGt": [0.0], "CsBucket": [5], "CsCondRank": [4]},
    "k,k2": {"TodMask": [(10, 40), (0, 3)]},
    "d,k": {"TsCountGt": [(5, 0.0), (20, 0.5)]},
}


@dataclass
class Case:
    tag: str
    param: Param
    x: np.ndarray
    y: np.ndarray = None
    z: np.ndarray = None
    days: np.ndarray = None
    extra: dict = field(default_factory=dict)


def _base(rng, T, A, seed_mix):
    """混合分布列: 高斯 / 厚尾 / 取整 (并列) / 常值 / 零负混合."""
    x = rng.standard_normal((T, A)).astype(np.float64)
    kind = np.arange(A) % 5
    x[:, kind == 1] = rng.standard_t(2.5, (T, (kind == 1).sum())) * 3
    x[:, kind == 2] = np.round(x[:, kind == 2] * 2) / 2  # 并列
    x[:, kind == 3] = 1.5  # 常值
    x[:, kind == 4] = np.where(rng.random((T, (kind == 4).sum())) < 0.3, 0.0, x[:, kind == 4] * 5)  # 零 / 大幅
    return x * seed_mix


def _holes(rng, x, frac, full_rows):
    x = x.copy()
    x[rng.random(x.shape) < frac] = np.nan
    for r in full_rows:
        x[r] = np.nan
    return x


def _groups(rng, T, A, G, nan_frac=0.05):
    g = rng.integers(0, G, (T, A)).astype(np.float64)
    g[rng.random((T, A)) < nan_frac] = np.nan
    return g


def _days(spec):
    return np.concatenate([np.full(n, i, dtype=np.int32) for i, n in enumerate(spec)])


def _inputs(rng, op, variant):
    """variant: 'clean' 无 NaN 少边界 / 'edge' 多 NaN + 整行 NaN + 短日."""
    axis = op.axis
    if axis == "CUM":
        days = _days(SHAPES["CUM"]["days"])
        T, A = days.shape[0], SHAPES["CUM"]["A"]
    else:
        T, A = SHAPES[axis]["T"], SHAPES[axis]["A"]
        days = None
    frac, rows = (0.0, []) if variant == "clean" else (0.15, [0, T // 2, T - 1])
    x = _holes(rng, _base(rng, T, A, 1.0), frac, rows)
    y = z = None
    if op.arity >= 2:
        if op.name in ("CsGroupMean", "CsGroupRank"):
            y = _groups(rng, T, A, 6, 0.0 if variant == "clean" else 0.1)
        elif op.name in ("CumWMean", "TsWMean"):
            y = np.abs(_holes(rng, _base(rng, T, A, 0.7), frac, rows[:1]))  # 权 ≥ 0, 含 0
        else:
            y = _holes(rng, 0.6 * x + 0.8 * _base(rng, T, A, 1.0), frac, rows[1:])  # 与 x 相关
    if op.arity >= 3:
        if op.name == "CsGroupResid":
            z = _groups(rng, T, A, 5, 0.0 if variant == "clean" else 0.1)
        else:
            z = _holes(rng, _base(rng, T, A, 2.0), frac, [])
    f32 = lambda a: None if a is None else a.astype(np.float32)
    return f32(x), f32(y), f32(z), days


def _param_sets(op):
    ps = ",".join(op.params)
    if ps == "":
        return [("", Param())]
    if ps == "d":
        return [(f"d{d}", Param(d=d)) for d in PARAMS["d"]]
    if ps == "k":
        return [(f"k{k}", Param(k=k)) for k in PARAMS["k"][op.name]]
    if ps == "k,k2":
        return [(f"k{a}_{b}", Param(k=a, k2=b)) for a, b in PARAMS["k,k2"][op.name]]
    if ps == "d,k":
        return [(f"d{d}_k{k}", Param(d=d, k=k)) for d, k in PARAMS["d,k"][op.name]]
    raise AssertionError(f"未知参数组合 {ps} ({op.name})")


def cases_for(op, seed=0):
    rng = np.random.default_rng(seed + zlib.crc32(op.name.encode()) % 10_000)  # 稳定种子 (hash() 逐进程随机)
    out = []
    for ptag, p in _param_sets(op):
        for variant in ("clean", "edge"):
            x, y, z, days = _inputs(rng, op, variant)
            tag = "_".join(t for t in (ptag, variant) if t)
            out.append(Case(tag, p, x, y, z, days))
    return out
