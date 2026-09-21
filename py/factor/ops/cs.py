"""
CS 截面算子 (对仗 factor/stream/Cs.hpp). f(x[, y[, z]], p), 行 = 截面 (沿 A 维).

前六个一元复刻 cs:: 方法 (src/features/Method/CS.cpp) 的口径, 含 均值填充 / 零填充 / 总体方差 / "var ≤ 0 不动";
其余 NaN 保持. 分组算子 (Group*) 用 [T, A, A] 两两比较 (rank) 或 scatter (mean), id = floor(分组列).
"""

import math

import torch

from ._util import NAN, f64, finite, out32, sorted_median, where_nan

INF = math.inf


# ---- 公共: 有效子集统计 ----
def _cnt(m):
    return m.sum(1, keepdim=True).to(torch.float64)


def _mean(v, m):
    return torch.where(m, v, torch.zeros_like(v)).sum(1, keepdim=True) / _cnt(m).clamp(min=1)


def _pct_rank(x):
    """有效子集 pct rank (并列均秩, searchsorted 版, O(A log A)); 无效 → NaN; m = 1 → 0.5."""
    v = f64(x)
    m = finite(x)
    vi = torch.where(m, v, torch.full_like(v, INF))
    s, _ = torch.sort(vi, dim=1)
    less = torch.searchsorted(s, vi, right=False).to(torch.float64)
    le = torch.searchsorted(s, vi, right=True).to(torch.float64)
    cnt = _cnt(m)
    avg_rank = less + (le - less + 1) * 0.5  # 1-based
    pct = (avg_rank - 1) / (cnt - 1)
    pct = torch.where(cnt <= 1, torch.full_like(pct, 0.5), pct)
    return where_nan(m, pct)


def _mean_fill(y):
    m = finite(y)
    cnt = _cnt(m)
    mean = _mean(y, m)
    fill = torch.where(cnt > 0, mean, torch.zeros_like(mean))
    return torch.where(m, y, fill.expand_as(y))


def _zero_fill(y):
    return torch.where(finite(y), y, torch.zeros_like(y))


def _median_row(v, m):
    return sorted_median(v, m).unsqueeze(1)


def _winsor_mad(v, m, k=3.0):
    """cs::winsor_mad: med / mad 取上下中位均值; 样本 < 2 或 mad = 0 → 不动."""
    med = _median_row(v, m)
    mad = _median_row((v - med).abs(), m)
    skip = (_cnt(m) < 2) | (mad == 0) | ~torch.isfinite(mad)
    lo, hi = med - k * mad, med + k * mad
    c = torch.where(m, v.clamp(lo, hi), v)
    return torch.where(skip, v, c)


def _z_pop(v, m):
    """cs::z: 总体方差 (Σx²/n − μ², double), cnt < 2 或 var ≤ 0 → 不动."""
    cnt = _cnt(m)
    vz = torch.where(m, v, torch.zeros_like(v))
    mean = vz.sum(1, keepdim=True) / cnt.clamp(min=1)
    var = (vz * vz).sum(1, keepdim=True) / cnt.clamp(min=1) - mean * mean
    skip = (cnt < 2) | (var <= 0)
    z = (v - mean) / var.clamp(min=1e-300).sqrt()
    return torch.where(skip, v, torch.where(m, z, v))


def _inv_normal(p):
    """cs::inverse_normal_cdf (Beasley-Springer-Moro 简化), 越界钳 ±6."""
    a0, a1, a2, a3 = 2.50662823884, -18.61500062529, 41.39119773534, -25.44106049637
    b1, b2, b3, b4 = -8.47351093090, 23.08336743743, -21.06224101826, 3.13082909833
    lower = p < 0.5
    t = torch.where(lower, p, 1.0 - p).clamp(min=1e-300)
    t = (-2.0 * torch.log(t)).sqrt()
    num = a0 + t * (a1 + t * (a2 + t * a3))
    den = 1.0 + t * (b1 + t * (b2 + t * (b3 + t * b4)))
    r = t - num / den
    r = torch.where(lower, -r, r)
    r = torch.where(p <= 0, torch.full_like(r, -6.0), r)
    return torch.where(p >= 1, torch.full_like(r, 6.0), r)


# ---- 一元: cs:: 方法复刻 ----
def CsRank(x, p):
    return out32(_mean_fill(_pct_rank(x)))


def CsNormRank(x, p):
    """按 (值, 下标) 稳定排序 (不并列), Φ⁻¹((rank+1)/(N+1)), 缺失 → 0."""
    v = f64(x)
    m = finite(x)
    vi = torch.where(m, v, torch.full_like(v, INF))
    order = torch.argsort(vi, dim=1, stable=True)
    rank = torch.empty_like(vi)
    rank.scatter_(1, order, torch.arange(x.shape[1], device=x.device, dtype=torch.float64).expand_as(vi))
    N = _cnt(m)
    y = _inv_normal((rank + 1) / (N + 1))
    return out32(torch.where(m, y, torch.zeros_like(y)))


def CsWinsorRank(x, p):
    v, m = f64(x), finite(x)
    return out32(_mean_fill(_pct_rank(_z_pop(_winsor_mad(v, m), m))))


def CsDemean(x, p):
    v, m = f64(x), finite(x)
    cnt = _cnt(m)
    d = torch.where(cnt > 0, v - _mean(v, m), v)
    return out32(_zero_fill(d))


def CsZ(x, p):
    v, m = f64(x), finite(x)
    return out32(_zero_fill(_z_pop(v, m)))


def CsWinsorZ(x, p):
    v, m = f64(x), finite(x)
    return out32(_zero_fill(_z_pop(_winsor_mad(v, m), m)))


# ---- 一元: 聚合广播 ----
def CsMean(x, p):
    v, m = f64(x), finite(x)
    return out32(where_nan(_cnt(m) > 0, _mean(v, m)).expand_as(v).clone())


def CsMedian(x, p):
    v, m = f64(x), finite(x)
    return out32(_median_row(v, m).expand_as(v).clone())


def CsStd(x, p):
    v, m = f64(x), finite(x)
    n = _cnt(m)
    d = torch.where(m, v - _mean(v, m), torch.zeros_like(v))
    sd = ((d * d).sum(1, keepdim=True) / (n - 1).clamp(min=1)).sqrt()
    return out32(where_nan(n >= 2, sd).expand_as(v).clone())


def CsBucket(x, p):
    r = _pct_rank(x)
    return out32(torch.minimum(torch.floor(r * p.k), torch.full_like(r, p.k - 1)))


# ---- 二元 ----
def _co(x, y):
    both = finite(x) & finite(y)
    xv, yv = f64(x), f64(y)
    n = _cnt(both)
    mx, my = _mean(xv, both), _mean(yv, both)
    z = torch.zeros_like(xv)
    dx, dy = torch.where(both, xv - mx, z), torch.where(both, yv - my, z)
    return both, n, mx, my, (dx * dx).sum(1, keepdim=True), (dy * dy).sum(1, keepdim=True), (dx * dy).sum(1, keepdim=True), xv, yv


def CsResid(x, y, p):
    both, n, mx, my, _, cyy, cxy, xv, yv = _co(x, y)
    ok = both & (n >= 2) & (cyy > 0)
    return out32(where_nan(ok, (xv - mx) - cxy / cyy.clamp(min=1e-300) * (yv - my)))


def CsBeta(x, y, p):
    _, n, _, _, _, cyy, cxy, xv, _ = _co(x, y)
    return out32(where_nan((n >= 2) & (cyy > 0), cxy / cyy.clamp(min=1e-300)).expand_as(xv).clone())


def CsCorr(x, y, p):
    _, n, _, _, cxx, cyy, cxy, xv, _ = _co(x, y)
    ok = (n >= 2) & (cxx > 0) & (cyy > 0)
    return out32(where_nan(ok, cxy / (cxx.clamp(min=1e-300) * cyy.clamp(min=1e-300)).sqrt()).expand_as(xv).clone())


def CsRankDiff(x, y, p):
    return out32(_pct_rank(x) - _pct_rank(y))


def _gid(by):
    """floor(by) → int64, 非 finite → −1."""
    g = torch.where(finite(by), torch.floor(f64(by)), torch.full_like(by, -1.0, dtype=torch.float64))
    return g.to(torch.int64)


def _group_sum(v, gid, m):
    """[T, A] 按 gid 逐行 scatter_add → 每格取回所属组的 (和, 计数)."""
    G = int(gid.max().item()) + 1 if gid.numel() else 0
    G = max(G, 1)
    idx = gid.clamp(min=0)
    z = torch.zeros(v.shape[0], G, dtype=torch.float64, device=v.device)
    s = z.scatter_add(1, idx, torch.where(m, v, torch.zeros_like(v)))
    c = z.scatter_add(1, idx, m.to(torch.float64))
    return s.gather(1, idx), c.gather(1, idx)


def CsGroupMean(x, y, p):
    gid = _gid(y)
    m = finite(x) & (gid >= 0)
    s, c = _group_sum(f64(x), gid, m)
    return out32(where_nan(m & (c > 0), s / c.clamp(min=1)))


def _group_rank(x, gid):
    v = f64(x)
    m = finite(x) & (gid >= 0)
    same = (gid.unsqueeze(2) == gid.unsqueeze(1)) & m.unsqueeze(2) & m.unsqueeze(1)  # [T, i, j]
    vi, vj = v.unsqueeze(2), v.unsqueeze(1)
    less = ((vj < vi) & same).sum(2).to(torch.float64)
    eq = ((vj == vi) & same).sum(2).to(torch.float64)  # 含自身
    cnt = same.sum(2).to(torch.float64)
    pct = (less + (eq - 1) * 0.5) / (cnt - 1)
    pct = torch.where(cnt <= 1, torch.full_like(pct, 0.5), pct)
    return where_nan(m, pct)


def CsGroupRank(x, y, p):
    return out32(_group_rank(x, _gid(y)))


def CsCondRank(x, y, p):
    return out32(_group_rank(x, _gid(CsBucket(y, p))))


# ---- 三元 ----
def CsGroupResid(x, y, z, p):
    """z 分组: x, y 组内 demean → x 对 y 标量回归残差 (cs::neutralize 同式: den ≤ 0 → β = 0)."""
    gid = _gid(z)
    m = finite(x) & finite(y) & (gid >= 0)
    xv, yv = f64(x), f64(y)
    sx, c = _group_sum(xv, gid, m)
    sy, _ = _group_sum(yv, gid, m)
    zero = torch.zeros_like(xv)
    xd = torch.where(m, xv - sx / c.clamp(min=1), zero)
    yd = torch.where(m, yv - sy / c.clamp(min=1), zero)
    num, den = (xd * yd).sum(1, keepdim=True), (yd * yd).sum(1, keepdim=True)
    b = torch.where(den > 0, num / den.clamp(min=1e-300), torch.zeros_like(num))
    return out32(where_nan(m, xd - b * yd))
