"""ops 公共件: NaN 掩码 / 日内分段前缀和 / 窗展开 / 并列均秩 / 中位数."""

import math

import torch

NAN = float("nan")


def f64(x):
    return x.to(torch.float64)


def out32(x):
    return x.to(torch.float32)


def finite(x):
    return torch.isfinite(x)


def where_nan(cond, v):
    return torch.where(cond, v, torch.full_like(v, NAN))


# ---- 日内分段 (CUM) ----
def seg_info(days):
    """days int [T] → (start_t [T]: 每行所属段的起始行, t_in_day [T]: 段内位置)."""
    T = days.shape[0]
    first = torch.ones(T, dtype=torch.bool, device=days.device)
    first[1:] = days[1:] != days[:-1]
    seg_id = first.to(torch.int64).cumsum(0) - 1
    start_idx = torch.nonzero(first).squeeze(1)
    start_t = start_idx[seg_id]
    t_in_day = torch.arange(T, device=days.device) - start_t
    return start_t, t_in_day


def seg_cumsum(v, start_t):
    """v [T, A] (float64) 沿 T 的分段前缀和 (每段从 0 起)."""
    c = v.cumsum(0)
    base = c[(start_t - 1).clamp(min=0)]
    base = torch.where((start_t > 0).unsqueeze(1), base, torch.zeros_like(base))
    return c - base


def seg_ranges(days):
    """[(s, e)] 段区间, 供必须逐段处理的算子 (cummax / 前缀两两比较)."""
    T = days.shape[0]
    first = torch.ones(T, dtype=torch.bool, device=days.device)
    first[1:] = days[1:] != days[:-1]
    idx = torch.nonzero(first).squeeze(1).tolist() + [T]
    return list(zip(idx[:-1], idx[1:]))


# ---- 窗 (ROLL) ----
def window(x, d):
    """x [T, A] → [T, A, d] float64, 最旧 → 最新; 前 d−1 行用 NaN 垫 (由 full 掩码剔除)."""
    pad = torch.full((d - 1, x.shape[1]), NAN, dtype=torch.float64, device=x.device)
    return torch.cat([pad, f64(x)]).unfold(0, d, 1)


def full_mask(T, d, device):
    """窗已满: t ≥ d−1."""
    return (torch.arange(T, device=device) >= d - 1).unsqueeze(1)


# ---- 统计 ----
def pct_rank_of(x, sample, valid):
    """x [...] 在样本 sample [..., n] (valid 掩码) 中的 pct rank, 并列均秩; sample 含 x 自身. m ≤ 1 → 0.5."""
    xe = x.unsqueeze(-1)
    less = ((sample < xe) & valid).sum(-1).to(torch.float64)
    eq = ((sample == xe) & valid).sum(-1).to(torch.float64)
    m = valid.sum(-1).to(torch.float64)
    pct = (less + (eq - 1) * 0.5) / (m - 1)
    return torch.where(m <= 1, torch.full_like(pct, 0.5), pct)


def sorted_median(sample, valid):
    """sample [..., n] 有效值中位数 (上下中位均值, 与 cs::median_in_place 同); m = 0 → NaN."""
    s, _ = torch.sort(torch.where(valid, sample, torch.full_like(sample, math.inf)), dim=-1)
    m = valid.sum(-1)
    lo = s.gather(-1, ((m - 1) // 2).clamp(min=0).unsqueeze(-1)).squeeze(-1)
    hi = s.gather(-1, (m // 2).clamp(max=sample.shape[-1] - 1).unsqueeze(-1)).squeeze(-1)
    return where_nan(m > 0, (lo + hi) * 0.5)


def central(w, valid, mean, k):
    """Σ (w − mean)^k 掩码和."""
    d = torch.where(valid, w - mean.unsqueeze(-1), torch.zeros_like(w))
    return (d**k).sum(-1)
