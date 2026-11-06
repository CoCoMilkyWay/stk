#!/usr/bin/env python3
# plot.py — 测试台结果弹窗 (plotly, 浏览器打开)
#   bench_<freq>.tsv → 结果网格: 每测试集一行 = 排行表 | 目标 vs 下跌捕获 (磨损) | 中线 × 带 (模型 + 周期) 曲面 (其余取均值) | 平原顶处 带 k × 分层 切面
#   obj = 测试台 --target 的全期值 (只看曲面形状; 可执行的是 band@roll)
#   python3 cta/plot.py [1m|1d]; run.py (APP = bench) 跑完测试台自动调用

import csv
import os
import sys
from collections import defaultdict

import plotly.colors
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "data")
TOP = 15  # 排行条数
EXPO = 0.2  # 排行的平均仓位下限 (同 cta_bench --expo 默认)
TEXT = ("set", "model", "params")


def read(path):
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    for r in rows:
        for k, v in r.items():
            if k not in TEXT:
                r[k] = float(v)
    return rows


# "ema(n=5) | vol(n=5,k=1) | grid(N=20,h=0.1)" → [(模型, {轴: 值}), …] 按组序 (中线 / 带 / 分层)
def parts(params):
    out = []
    for s in params.split(" | "):
        name, _, rest = s.partition("(")
        out.append(
            (
                name,
                dict(x.split("=") for x in rest.rstrip(")").split(",")) if rest else {},
            )
        )
    return out


def label(name, kv, skip=()):
    p = ",".join(f"{k}={v}" for k, v in kv.items() if k not in skip)
    return f"{name}({p})" if p else name


def mean(v):
    return sum(v) / len(v) if v else None


def bench_fig(rows, freq):
    sets = list(dict.fromkeys(r["set"] for r in rows))
    models = list(dict.fromkeys(r["model"] for r in rows))
    palette = plotly.colors.qualitative.Dark24
    color = {m: palette[i % len(palette)] for i, m in enumerate(models)}
    titles = []
    for s in sets:
        titles += [
            f"{s}: 排行 (仓位 ≥ {EXPO}, ◆ = 平原顶)",
            f"{s}: 目标 vs 下跌捕获 (磨损)",
            f"{s}: 中线 × 带 (k / 分层取均值, × = 平原顶)",
            f"{s}: 平原顶处 带 k × 分层",
        ]
    fig = make_subplots(
        rows=len(sets),
        cols=4,
        column_widths=[0.38, 0.22, 0.24, 0.16],
        horizontal_spacing=0.04,
        vertical_spacing=min(0.08, 0.3 / len(sets)),
        subplot_titles=titles,
        specs=[
            [{"type": "table"}, {"type": "xy"}, {"type": "xy"}, {"type": "xy"}]
            for _ in sets
        ],
    )
    shown = set()
    for i, s in enumerate(sets, 1):
        rs = [r for r in rows if r["set"] == s]

        hold = next(r for r in rs if r["model"] == "hold")
        tab = [hold] + sorted(
            (r for r in rs if r["model"] != "hold" and r["m_expo"] >= EXPO),
            key=lambda r: -r["obj"],
        )[:TOP]
        cols = [
            ("model", "model"),
            ("params", "params"),
            ("目标", "obj"),
            ("SR", "sr"),
            ("CAGR", "cagr"),
            ("MDD", "mdd"),
            ("Dn", "m_dn"),
            ("Edge", "m_edge"),
            ("Expo", "m_expo"),
            ("Turn", "m_turn"),
        ]
        fmt = {
            "cagr": "{:.1%}",
            "mdd": "{:.1%}",
            "m_edge": "{:+.2f}",
            "m_turn": "{:.1f}",
        }

        def cell(r, k):
            if k == "model":
                return r["model"] + (" ◆" if r["plateau"] else "")
            return r[k] if k == "params" else fmt.get(k, "{:.2f}").format(r[k])

        fig.add_trace(
            go.Table(
                columnwidth=[1.1, 4.4] + [0.7] * 8,
                header=dict(values=[c[0] for c in cols], align="left"),
                cells=dict(
                    values=[[cell(r, k) for r in tab] for _, k in cols],
                    align="left",
                    font=dict(size=10),
                ),
            ),
            row=i,
            col=1,
        )

        by = defaultdict(list)
        for r in rs:
            by[r["model"]].append(r)
        for m, g in by.items():
            fig.add_trace(
                go.Scatter(
                    x=[r["m_dn"] for r in g],
                    y=[r["obj"] for r in g],
                    mode="markers",
                    name=m,
                    legendgroup=m,
                    showlegend=m not in shown,
                    marker=dict(
                        color=color[m],
                        size=[11 if r["plateau"] else 5 for r in g],
                        symbol=["diamond" if r["plateau"] else "circle" for r in g],
                        line=dict(width=0.5, color="black"),
                    ),
                    text=[r["params"] for r in g],
                    hovertemplate=m
                    + "<br>%{text}<br>目标 %{y:.2f} | Dn %{x:.2f}<extra></extra>",
                ),
                row=i,
                col=2,
            )
            shown.add(m)
        fig.update_xaxes(title_text="Dn (下跌捕获)", row=i, col=2)
        fig.update_yaxes(title_text="目标", row=i, col=2)

        # 每行: (行, 中线, 带 (模型 + 周期), 带 k, 分层)
        band = []
        for r in rs:
            if r["model"] != "band":
                continue
            (cn, cp), (wn, wp), (ln, lp) = parts(r["params"])
            band.append(
                (r, label(cn, cp), label(wn, wp, ("k",)), wp["k"], label(ln, lp))
            )
        if not band:
            continue
        cs = list(dict.fromkeys(b[1] for b in band))
        ws = list(dict.fromkeys(b[2] for b in band))
        acc = defaultdict(list)
        for r, c, w, _, _ in band:
            acc[(c, w)].append(r["obj"])
        fig.add_trace(
            go.Heatmap(
                x=ws,
                y=cs,
                z=[[mean(acc[(c, w)]) for w in ws] for c in cs],
                colorscale="RdBu",
                zmid=0,
                texttemplate="%{z:.2f}",
                showscale=False,
                hovertemplate="%{y} %{x}<br>均目标 %{z:.2f}<extra></extra>",
            ),
            row=i,
            col=3,
        )
        top = next((b for b in band if b[0]["plateau"]), None)
        if top is None:
            continue
        _, tc, tw, tk, tl = top
        fig.add_trace(
            go.Scatter(
                x=[tw],
                y=[tc],
                mode="markers",
                marker=dict(symbol="x", size=16, color="black"),
                showlegend=False,
                hoverinfo="skip",
            ),
            row=i,
            col=3,
        )
        sl = [b for b in band if (b[1], b[2]) == (tc, tw)]
        ks = sorted({b[3] for b in sl}, key=float)
        ls = list(dict.fromkeys(b[4] for b in sl))
        zz = {(b[3], b[4]): b[0]["obj"] for b in sl}
        fig.add_trace(
            go.Heatmap(
                x=ls,
                y=[f"k={k}" for k in ks],
                z=[[zz.get((k, l)) for l in ls] for k in ks],
                colorscale="RdBu",
                zmid=0,
                texttemplate="%{z:.2f}",
                showscale=False,
                hovertemplate="%{y} %{x}<br>目标 %{z:.2f}<extra></extra>",
            ),
            row=i,
            col=4,
        )
        fig.add_trace(
            go.Scatter(
                x=[tl],
                y=[f"k={tk}"],
                mode="markers",
                marker=dict(symbol="x", size=16, color="black"),
                showlegend=False,
                hoverinfo="skip",
            ),
            row=i,
            col=4,
        )
    fig.update_layout(
        title=f"CTA bench {freq}", template="plotly_white", height=560 * len(sets)
    )
    return fig


def main():
    freq = sys.argv[1] if len(sys.argv) > 1 else "1m"
    pio.write_html(
        bench_fig(read(os.path.join(DATA, f"bench_{freq}.tsv")), freq),
        os.path.join(DATA, f"bench_{freq}.html"),
        auto_open=True,
    )


if __name__ == "__main__":
    main()
