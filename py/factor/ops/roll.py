"""
ROLL 跨日滚动算子 (对仗 factor/stream/Roll.hpp). f(x[, y], p), 行 = 期, 窗 p.d.

实现: unfold 成 [T, A, d] (最旧 → 最新) 逐窗重算 (float64, 中心化两遍), 与流式 ring 重算严格同义.
窗未满 (t < d−1) → NaN; 窗内 NaN 跳过; 相对型 (Rank / Z / Resid) 在 x_t NaN 时 NaN. TsEma 无窗, 沿 T 递推.
"""

import math

import torch

from ._util import NAN, central, f64, finite, full_mask, out32, pct_rank_of, sorted_median, where_nan, window


def _w(x, d):
    w = window(x, d)
    valid = torch.isfinite(w)
    n = valid.sum(-1).to(torch.float64)
    full = full_mask(x.shape[0], d, x.device)
    return w, valid, n, full


def _fin(full, cond, v):
    return out32(where_nan(full & cond, v))


def _mean(w, valid, n):
    return torch.where(valid, w, torch.zeros_like(w)).sum(-1) / n.clamp(min=1)


# ---- 一元 ----
def TsDelay(x, p):
    w, _, _, full = _w(x, p.d + 1)
    return out32(torch.where(full, w[..., 0], torch.full_like(w[..., 0], NAN)))


def TsDelta(x, p):
    w, _, _, full = _w(x, p.d + 1)
    return out32(torch.where(full, w[..., -1] - w[..., 0], torch.full_like(w[..., 0], NAN)))


def TsSum(x, p):
    w, valid, n, full = _w(x, p.d)
    return _fin(full, n > 0, torch.where(valid, w, torch.zeros_like(w)).sum(-1))


def TsMean(x, p):
    w, valid, n, full = _w(x, p.d)
    return _fin(full, n > 0, _mean(w, valid, n))


def TsVar(x, p):
    w, valid, n, full = _w(x, p.d)
    return _fin(full, n >= 2, central(w, valid, _mean(w, valid, n), 2) / (n - 1).clamp(min=1))


def TsStd(x, p):
    w, valid, n, full = _w(x, p.d)
    return _fin(full, n >= 2, (central(w, valid, _mean(w, valid, n), 2) / (n - 1).clamp(min=1)).sqrt())


def TsSkew(x, p):
    w, valid, n, full = _w(x, p.d)
    mu = _mean(w, valid, n)
    m2 = central(w, valid, mu, 2) / n.clamp(min=1)
    m3 = central(w, valid, mu, 3) / n.clamp(min=1)
    return _fin(full, (n >= 3) & (m2 > 0), m3 / m2.clamp(min=1e-300) ** 1.5)


def TsKurt(x, p):
    w, valid, n, full = _w(x, p.d)
    mu = _mean(w, valid, n)
    m2 = central(w, valid, mu, 2) / n.clamp(min=1)
    m4 = central(w, valid, mu, 4) / n.clamp(min=1)
    return _fin(full, (n >= 4) & (m2 > 0), m4 / m2.clamp(min=1e-300) ** 2 - 3.0)


def _extreme(x, d, is_max, arg):
    w, valid, n, full = _w(x, d)
    v = torch.where(valid, w if is_max else -w, torch.full_like(w, -math.inf))
    best = v.amax(-1)
    if not arg:
        return _fin(full, n > 0, best if is_max else -best)
    pos = torch.arange(d, device=x.device, dtype=torch.float64)
    first = torch.where((v == best.unsqueeze(-1)) & valid, pos, torch.full_like(v, float(d))).amin(-1)  # 最旧的极值
    return _fin(full, n > 0, (d - 1) - first)


def TsMax(x, p):
    return _extreme(x, p.d, True, False)


def TsMin(x, p):
    return _extreme(x, p.d, False, False)


def TsArgMax(x, p):
    return _extreme(x, p.d, True, True)


def TsArgMin(x, p):
    return _extreme(x, p.d, False, True)


def TsMed(x, p):
    w, valid, n, full = _w(x, p.d)
    return _fin(full, n > 0, sorted_median(w, valid))


def TsMad(x, p):
    w, valid, n, full = _w(x, p.d)
    med = sorted_median(w, valid)
    dev = (w - med.unsqueeze(-1)).abs()
    return _fin(full, n > 0, sorted_median(dev, valid))


def TsRank(x, p):
    w, valid, n, full = _w(x, p.d)
    xt = w[..., -1]
    return _fin(full, torch.isfinite(xt), pct_rank_of(xt, w, valid))


def TsZ(x, p):
    w, valid, n, full = _w(x, p.d)
    xt = w[..., -1]
    mu = _mean(w, valid, n)
    var = central(w, valid, mu, 2) / (n - 1).clamp(min=1)
    return _fin(full, torch.isfinite(xt) & (n >= 2) & (var > 0), (xt - mu) / var.clamp(min=1e-300).sqrt())


def TsWma(x, p):
    w, valid, n, full = _w(x, p.d)
    wt = torch.arange(1, p.d + 1, device=x.device, dtype=torch.float64)
    wt = torch.where(valid, wt, torch.zeros_like(w))
    sw = wt.sum(-1)
    return _fin(full, sw > 0, (wt * torch.where(valid, w, torch.zeros_like(w))).sum(-1) / sw.clamp(min=1e-300))


def TsEma(x, p):
    """y = k·x + (1−k)·y, 首个有效值起; x NaN → 保持."""
    k = float(p.k)
    xf = f64(x)
    y = torch.full_like(xf[0], NAN)
    out = torch.empty_like(xf)
    for t in range(x.shape[0]):
        xt = xf[t]
        ok = torch.isfinite(xt)
        upd = torch.where(torch.isfinite(y), k * xt + (1.0 - k) * y, xt)
        y = torch.where(ok, upd, y)
        out[t] = y
    return out32(out)


def TsProduct(x, p):
    w, valid, n, full = _w(x, p.d)
    return _fin(full, n > 0, torch.where(valid, 1.0 + w, torch.ones_like(w)).prod(-1) - 1.0)


def TsSlope(x, p):
    w, valid, n, full = _w(x, p.d)
    i = torch.arange(p.d, device=x.device, dtype=torch.float64).expand_as(w)
    mi = torch.where(valid, i, torch.zeros_like(i)).sum(-1) / n.clamp(min=1)
    mx = _mean(w, valid, n)
    di = torch.where(valid, i - mi.unsqueeze(-1), torch.zeros_like(i))
    dx = torch.where(valid, w - mx.unsqueeze(-1), torch.zeros_like(w))
    num, den = (di * dx).sum(-1), (di * di).sum(-1)
    return _fin(full, (n >= 2) & (den > 0), num / den.clamp(min=1e-300))


def TsCountGt(x, p):
    w, valid, n, full = _w(x, p.d)
    return _fin(full, torch.ones_like(full), ((w > p.k) & valid).sum(-1).to(torch.float64))


# ---- 二元 ----
def _co(x, y, d):
    wx, wy = window(x, d), window(y, d)
    both = torch.isfinite(wx) & torch.isfinite(wy)
    n = both.sum(-1).to(torch.float64)
    z = torch.zeros_like(wx)
    xv, yv = torch.where(both, wx, z), torch.where(both, wy, z)
    mx, my = xv.sum(-1) / n.clamp(min=1), yv.sum(-1) / n.clamp(min=1)
    dx = torch.where(both, wx - mx.unsqueeze(-1), z)
    dy = torch.where(both, wy - my.unsqueeze(-1), z)
    cxx, cyy, cxy = (dx * dx).sum(-1), (dy * dy).sum(-1), (dx * dy).sum(-1)
    full = full_mask(x.shape[0], d, x.device)
    return wx, wy, both, n, mx, my, cxx, cyy, cxy, full


def TsCov(x, y, p):
    _, _, _, n, _, _, _, _, cxy, full = _co(x, y, p.d)
    return _fin(full, n >= 2, cxy / (n - 1).clamp(min=1))


def TsCorr(x, y, p):
    _, _, _, n, _, _, cxx, cyy, cxy, full = _co(x, y, p.d)
    return _fin(full, (n >= 2) & (cxx > 0) & (cyy > 0), cxy / (cxx.clamp(min=1e-300) * cyy.clamp(min=1e-300)).sqrt())


def TsBeta(x, y, p):
    _, _, _, n, _, _, _, cyy, cxy, full = _co(x, y, p.d)
    return _fin(full, (n >= 2) & (cyy > 0), cxy / cyy.clamp(min=1e-300))


def TsResid(x, y, p):
    wx, wy, _, n, mx, my, _, cyy, cxy, full = _co(x, y, p.d)
    xt, yt = wx[..., -1], wy[..., -1]
    ok = torch.isfinite(xt) & torch.isfinite(yt) & (n >= 2) & (cyy > 0)
    return _fin(full, ok, (xt - mx) - cxy / cyy.clamp(min=1e-300) * (yt - my))


def TsWMean(x, y, p):
    wx, wy = window(x, p.d), window(y, p.d)
    both = torch.isfinite(wx) & torch.isfinite(wy)
    z = torch.zeros_like(wx)
    sxw = torch.where(both, wx * wy, z).sum(-1)
    sw = torch.where(both, wy, z).sum(-1)
    full = full_mask(x.shape[0], p.d, x.device)
    return _fin(full, sw != 0, sxw / torch.where(sw != 0, sw, torch.ones_like(sw)))
