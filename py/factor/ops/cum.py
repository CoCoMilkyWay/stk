"""
CUM 日内 expanding 算子 (对仗 factor/stream/Cum.hpp). f(x[, y], days, p), days int [T] 日 id.

实现: 分段前缀和 (float64 原始幂和, 中心矩由幂和合成) 一次算全历史; 需要"前缀全序列"的算子 (Rank / TopK)
逐段做 [L, L, A] 两两比较 (L ≤ 240), 极值 (Max/Min/Arg) 逐段 cummax. 集合型在 x_t NaN 时给集合值, 相对型给 NaN.
"""

import math

import torch

from ._util import NAN, f64, finite, out32, seg_cumsum, seg_info, seg_ranges, where_nan


def _sums(x, days):
    m = finite(x)
    v = torch.where(m, f64(x), torch.zeros_like(x, dtype=torch.float64))
    start_t, _ = seg_info(days)
    n = seg_cumsum(m.to(torch.float64), start_t)
    return m, v, n, start_t


def _cum(v, start_t):
    return seg_cumsum(v, start_t)


# ---- 一元: 前缀和族 ----
def CumSum(x, days, p):
    _, v, n, st = _sums(x, days)
    return out32(where_nan(n > 0, _cum(v, st)))


def CumMean(x, days, p):
    _, v, n, st = _sums(x, days)
    return out32(where_nan(n > 0, _cum(v, st) / n))


def _central(x, days, kmax):
    """返回 n, μ, m2, m3, m4 (总体中心矩 × n = Σ(x−μ)^k), 由幂和合成."""
    _, v, n, st = _sums(x, days)
    s1 = _cum(v, st)
    s2 = _cum(v * v, st)
    mu = s1 / n
    M2 = s2 - n * mu * mu
    M3 = M4 = None
    if kmax >= 3:
        s3 = _cum(v**3, st)
        M3 = s3 - 3 * mu * s2 + 2 * n * mu**3
    if kmax >= 4:
        s4 = _cum(v**4, st)
        M4 = s4 - 4 * mu * s3 + 6 * mu * mu * s2 - 3 * n * mu**4
    return n, mu, M2, M3, M4


def CumVar(x, days, p):
    n, _, M2, _, _ = _central(x, days, 2)
    return out32(where_nan(n >= 2, M2 / (n - 1)))


def CumStd(x, days, p):
    n, _, M2, _, _ = _central(x, days, 2)
    return out32(where_nan(n >= 2, (M2 / (n - 1)).clamp(min=0).sqrt()))


def CumSkew(x, days, p):
    n, _, M2, M3, _ = _central(x, days, 3)
    return out32(where_nan((n >= 3) & (M2 > 0), n.sqrt() * M3 / M2.clamp(min=1e-300) ** 1.5))


def CumKurt(x, days, p):
    n, _, M2, _, M4 = _central(x, days, 4)
    return out32(where_nan((n >= 4) & (M2 > 0), n * M4 / M2.clamp(min=1e-300) ** 2 - 3.0))


# ---- 一元: 极值 (逐段 cummax; 首个极值) ----
def _extreme(x, days, is_max, arg):
    v = f64(x)
    if not is_max:
        v = -v
    v = torch.where(finite(x), v, torch.full_like(v, -math.inf))
    out = torch.full_like(v, NAN)
    for s, e in seg_ranges(days):
        seg = v[s:e]
        cm = torch.cummax(seg, 0).values
        if arg:
            prev = torch.cat([torch.full_like(cm[:1], -math.inf), cm[:-1]])
            is_new = (seg > prev) & finite(x[s:e])
            t = torch.arange(e - s, device=x.device, dtype=torch.float64).unsqueeze(1).expand_as(seg)
            pos = torch.cummax(torch.where(is_new, t, torch.full_like(t, -1.0)), 0).values
            out[s:e] = where_nan(pos >= 0, pos)
        else:
            val = cm if is_max else -cm
            out[s:e] = where_nan(torch.isfinite(cm), val)
    return out32(out)


def CumMax(x, days, p):
    return _extreme(x, days, True, False)


def CumMin(x, days, p):
    return _extreme(x, days, False, False)


def CumArgMax(x, days, p):
    return _extreme(x, days, True, True)


def CumArgMin(x, days, p):
    return _extreme(x, days, False, True)


# ---- 一元: 需前缀全序列 (逐段 [L, L, A]) ----
def _prefix_pairs(xs):
    """段 xs [L, A] → (sample [L(t), L(s), A], valid [L, L, A]): s ≤ t 且有效."""
    L = xs.shape[0]
    tri = torch.tril(torch.ones(L, L, dtype=torch.bool, device=xs.device))  # [t, s]: s ≤ t
    sample = xs.unsqueeze(0).expand(L, L, -1)  # [t, s, A]
    valid = tri.unsqueeze(2) & finite(xs).unsqueeze(0)
    return sample, valid


def CumRank(x, days, p):
    out = torch.full_like(x, NAN, dtype=torch.float64)
    for s, e in seg_ranges(days):
        xs = f64(x[s:e])
        sample, valid = _prefix_pairs(xs)
        xe = xs.unsqueeze(1)  # [t, 1, A]
        less = ((sample < xe) & valid).sum(1).to(torch.float64)
        eq = ((sample == xe) & valid).sum(1).to(torch.float64)
        m = valid.sum(1).to(torch.float64)
        pct = (less + (eq - 1) * 0.5) / (m - 1)
        pct = torch.where(m <= 1, torch.full_like(pct, 0.5), pct)
        out[s:e] = where_nan(finite(xs), pct)
    return out32(out)


def CumHhi(x, days, p):
    _, v, _, st = _sums(x, days)
    s1 = _cum(v, st)
    s2 = _cum(v * v, st)
    return out32(where_nan(s1 != 0, s2 / (s1 * s1)))


def CumEntropy(x, days, p):
    pos = finite(x) & (x > 0)
    v = torch.where(pos, f64(x), torch.zeros_like(x, dtype=torch.float64))
    st, _ = seg_info(days)
    s = _cum(v, st)
    sl = _cum(torch.where(pos, v * torch.log(v.clamp(min=1e-300)), torch.zeros_like(v)), st)
    return out32(where_nan(s > 0, torch.log(s.clamp(min=1e-300)) - sl / s))


def CumTopK(x, days, p):
    k = int(p.k)
    _, v, n, st = _sums(x, days)
    s = _cum(v, st)
    out = torch.full_like(x, NAN, dtype=torch.float64)
    for a, e in seg_ranges(days):
        xs = f64(x[a:e])
        sample, valid = _prefix_pairs(xs)
        vals = torch.where(valid, sample, torch.full_like(sample, -math.inf))
        top = torch.topk(vals, min(k, e - a), dim=1).values
        top = torch.where(torch.isfinite(top), top, torch.zeros_like(top)).sum(1)
        out[a:e] = top
    return out32(where_nan((n >= k) & (s != 0), out / s))


def CumPeaks(x, days, p):
    m, v, n, st = _sums(x, days)
    mean = _cum(v, st) / n.clamp(min=1)  # CumMean_s (含 s)
    xf = f64(x)
    _, t_in = seg_info(days)
    T = x.shape[0]
    prev = torch.cat([xf[:1] * NAN, xf[:-1]])  # x_{s−1}
    nxt = torch.cat([xf[1:], xf[-1:] * NAN])  # x_{s+1}
    same_prev = (t_in >= 1).unsqueeze(1)  # s−1 在本日
    same_next = torch.cat([(t_in[1:] >= 1), torch.tensor([False], device=x.device)]).unsqueeze(1)  # s+1 在本日
    is_peak = m & finite(prev) & finite(nxt) & same_prev & same_next & (prev < xf) & (xf > nxt) & (xf > p.k * mean)
    ind = torch.zeros_like(xf)
    ind[1:] = is_peak[:-1].to(torch.float64)  # 峰 s 在 s+1 计入
    return out32(_cum(ind, st))


def CumCountGt(x, days, p):
    m = finite(x) & (x > p.k)
    st, _ = seg_info(days)
    return out32(_cum(m.to(torch.float64), st))


def TodMask(x, days, p):
    _, t_in = seg_info(days)
    r = ((t_in >= int(p.k)) & (t_in < int(p.k2))).to(torch.float32).unsqueeze(1)
    return r.expand_as(x).clone()


# ---- 二元 ----
def _co(x, y, days):
    both = finite(x) & finite(y)
    z = torch.zeros_like(x, dtype=torch.float64)
    xv = torch.where(both, f64(x), z)
    yv = torch.where(both, f64(y), z)
    st, _ = seg_info(days)
    n = _cum(both.to(torch.float64), st)
    sx, sy = _cum(xv, st), _cum(yv, st)
    sxx, syy, sxy = _cum(xv * xv, st), _cum(yv * yv, st), _cum(xv * yv, st)
    mx, my = sx / n.clamp(min=1), sy / n.clamp(min=1)
    cxx = sxx - n * mx * mx
    cyy = syy - n * my * my
    cxy = sxy - n * mx * my
    return both, n, mx, my, cxx, cyy, cxy


def CumCov(x, y, days, p):
    _, n, _, _, _, _, cxy = _co(x, y, days)
    return out32(where_nan(n >= 2, cxy / (n - 1)))


def CumCorr(x, y, days, p):
    _, n, _, _, cxx, cyy, cxy = _co(x, y, days)
    ok = (n >= 2) & (cxx > 0) & (cyy > 0)
    return out32(where_nan(ok, cxy / (cxx.clamp(min=1e-300) * cyy.clamp(min=1e-300)).sqrt()))


def CumBeta(x, y, days, p):
    _, n, _, _, _, cyy, cxy = _co(x, y, days)
    return out32(where_nan((n >= 2) & (cyy > 0), cxy / cyy.clamp(min=1e-300)))


def CumResid(x, y, days, p):
    both, n, mx, my, _, cyy, cxy = _co(x, y, days)
    ok = both & (n >= 2) & (cyy > 0)
    return out32(where_nan(ok, (f64(x) - mx) - cxy / cyy.clamp(min=1e-300) * (f64(y) - my)))


def CumWMean(x, y, days, p):
    both = finite(x) & finite(y)
    z = torch.zeros_like(x, dtype=torch.float64)
    st, _ = seg_info(days)
    sxw = _cum(torch.where(both, f64(x) * f64(y), z), st)
    sw = _cum(torch.where(both, f64(y), z), st)
    return out32(where_nan(sw != 0, sxw / sw))
