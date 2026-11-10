import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

Y, P = 10, 244
T = Y * P
L = np.log(3.0)
VOL = 0.015
SEEDS = 200
MIN = P // 4  # 窗最短一季度, 更短的年化/夏普无意义


def drift(burst_year):
    d = np.full(T, L / T)
    if burst_year is not None:
        d[:] = 0.2 * L / (T - P)
        d[burst_year * P:(burst_year + 1) * P] = 0.8 * L / P
    return d


def rolling(r, n_years):
    """每日 t 一个往回看的窗 [max(0, t+1-n年), t], 不足 n 年自然缩短; 返回各窗 年化 / 夏普 的平均"""
    w = n_years * P
    t = np.arange(MIN - 1, T)
    lo = np.maximum(t + 1 - w, 0)
    c1 = np.concatenate([[0.0], np.cumsum(r)])
    c2 = np.concatenate([[0.0], np.cumsum(r * r)])
    n = t + 1 - lo
    m = (c1[t + 1] - c1[lo]) / n
    v = (c2[t + 1] - c2[lo]) / n - m * m
    return (m * P).mean(), (m / np.sqrt(np.maximum(v, 1e-30)) * np.sqrt(P)).mean()


rng = np.random.default_rng(7)
noises = rng.normal(0.0, VOL, (SEEDS, T))
noises -= noises.mean(axis=1, keepdims=True)
cases = [("均匀", None)] + [(f"第{b + 1}年爆发", b) for b in range(3, 7)]
print(f"{'':16}" + "".join(f"{k:>14}" for k, _ in cases))
for n in (1, 2, 3, 5):
    ann = {k: rolling(drift(b), n)[0] for k, b in cases}
    shp = {k: np.mean([rolling(drift(b) + z, n)[1] for z in noises]) for k, b in cases}
    for what, vals in (("年化", ann), ("夏普", shp)):
        b = vals["均匀"]
        print(f"{f'滚动{n}年 {what}':<16}" + "".join(f"{v:7.3f} ({100 * v / b:4.0f})" for v in vals.values()))

x = np.arange(1, T + 1) / P
fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.06,
                    subplot_titles=("漂移 (算年化用)", "漂移 + 第 1 组噪声 (算夏普的 200 组之一)"))
for i, (k, b) in enumerate(cases):
    color = f"hsl({i * 72},70%,45%)"
    for row, r in ((1, drift(b)), (2, drift(b) + noises[0])):
        fig.add_trace(go.Scatter(x=x, y=np.exp(np.cumsum(r)), name=k, legendgroup=k, showlegend=row == 1,
                                 line=dict(color=color, width=1.5)), row=row, col=1)
fig.update_yaxes(type="log", title_text="NAV")
fig.update_xaxes(title_text="年", dtick=1, row=2, col=1)
fig.update_layout(height=900, hovermode="x unified")
fig.show(renderer="browser")
