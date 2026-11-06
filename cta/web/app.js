// app.js — CTA 轨迹前端: 收集选项 → GET /api/run (cta_trace 现算) → 画; 本文件不做任何计算
//   上: 全期日线概览 (策略 vs 持有净值 + 日均仓位, 橙 = 当前窗口); 下: 窗口逐根 K 线 + band (中线 / 上下沿 / 档线) + 止损 + 加减仓点, 底部仓位与窗内净值
const $ = (id) => document.getElementById(id);
const UP = "#d62728",
  DOWN = "#2ca02c",
  BAND = "rgba(31,119,180,0.8)";
let META = null;
let DAYS = []; // 当前资产全期交易日 (YYYYMMDD), 来自上次 /api/run 的概览

const ymd = (s) => Number(s.replaceAll("-", ""));
const dash = (d) => {
  const s = String(d);
  return `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}`;
};
const hhmm = (t) => {
  const s = String(t);
  return `${s.slice(4, 6)}-${s.slice(6, 8)} ${s.slice(8, 10)}:${s.slice(10, 12)}`;
};

const kind = () => META.kinds.find((k) => k.name === $("kind").value);
const fields = () => [...$("params").querySelectorAll("[data-i]")];

function renderParams() {
  $("params").innerHTML = kind()
    .axes.map((a, i) => {
      if (a.labels.length)
        return `<label>${a.name} <select data-i="${i}">${a.labels.map((l, j) => `<option value="${a.values[j]}">${l}</option>`).join("")}</select></label>`;
      const mid = a.values[Math.floor(a.values.length / 2)];
      return (
        `<label>${a.name} <input data-i="${i}" type="number" step="any" value="${mid}" list="dl${i}">` +
        `<datalist id="dl${i}">${a.values.map((v) => `<option value="${v}">`).join("")}</datalist></label>`
      );
    })
    .join("");
}

// 参数的显示串: 类别轴写标签
const paramText = () =>
  fields()
    .map((e, i) => {
      const a = kind().axes[i];
      return `${a.name}=${a.labels.length ? e.selectedOptions[0].text : e.value}`;
    })
    .join(", ");

function query() {
  return new URLSearchParams({
    a: $("asset").value,
    kind: $("kind").value,
    p: fields()
      .map((e) => e.value)
      .join(","),
    trail: $("trail").value || 0,
    cost: ($("cost").value || 0) / 1e4,
    stop: $("stop").value,
    vol: $("vol").value || 0,
    buffer: $("buffer").value || 0,
    from: ymd($("from").value),
    days: $("days").value || 1,
  }).toString();
}

// ---- 窗口导航 ----

function randomFrom() {
  const days = Number($("days").value) || 5;
  if (DAYS.length > days) {
    const lo = Math.floor(DAYS.length * 0.2),
      hi = DAYS.length - days;
    $("from").value = dash(
      DAYS[lo + Math.floor(Math.random() * Math.max(1, hi - lo))],
    );
    return;
  }
  const A = META.assets[$("asset").value],
    t0 = Date.parse(dash(A.first)),
    t1 = Date.parse(dash(A.last));
  $("from").value = new Date(t0 + (0.2 + 0.75 * Math.random()) * (t1 - t0))
    .toISOString()
    .slice(0, 10);
}

function shift(dir) {
  const days = Number($("days").value) || 5,
    f = ymd($("from").value);
  let i = DAYS.findIndex((d) => d >= f);
  if (i < 0) i = DAYS.length - 1;
  $("from").value = dash(
    DAYS[Math.min(Math.max(0, i + dir * days), DAYS.length - 1)],
  );
  schedule();
}

// ---- 取数 ----

let seq = 0,
  timer = 0;
const schedule = () => {
  clearTimeout(timer);
  timer = setTimeout(run, 120);
};

async function run() {
  const my = ++seq,
    t0 = performance.now();
  $("busy").textContent = "计算中…";
  const d = await (await fetch("/api/run?" + query())).json();
  if (my !== seq) return;
  $("busy").textContent = `${Math.round(performance.now() - t0)} ms`;
  DAYS = d.over.t;
  drawStats(d);
  drawOver(d);
  drawWin(d);
}

// ---- 画 ----

const fmt = (f) => (v) => (v == null ? "–" : f(v));
const f0 = fmt((v) => v.toFixed(0)),
  f1 = fmt((v) => v.toFixed(1)),
  f2 = fmt((v) => v.toFixed(2));
const pct = fmt((v) => (100 * v).toFixed(1) + "%");
const ROWS = [
  ["夏普", "sr", f2],
  ["年化", "cagr", pct],
  ["最大回撤", "mdd", pct],
  ["年化波动", "vol", pct],
  ["平均仓位", "expo", f2],
  ["年换手 (单边)", "turn", f1],
  ["年成本", "cost", pct],
  ["上涨捕获 Up", "up", f2],
  ["下跌捕获 Dn", "dn", f2],
  ["Edge", "edge", f2],
  ["年在场段数", "trades", f1],
  ["胜率", "win", pct],
  ["盈亏比", "payoff", f2],
  ["平均持有 (根)", "hold", f0],
];

function drawStats(d) {
  $("stats").innerHTML =
    "<tr><th></th><th>策略</th><th>持有</th></tr>" +
    ROWS.map(
      ([n, k, f]) =>
        `<tr><td>${n}</td><td>${f(d.stat[k])}</td><td>${f(d.hold[k])}</td></tr>`,
    ).join("");
}

let overBound = false;
function drawOver(d) {
  const x = d.over.t.map(dash),
    w0 = dash(Math.floor(d.win.t[0] / 10000)),
    w1 = dash(Math.floor(d.win.t.at(-1) / 10000));
  Plotly.react(
    "over",
    [
      {
        x,
        y: d.over.hold,
        name: "持有",
        line: { color: "gray", width: 1, dash: "dot" },
      },
      { x, y: d.over.nav, name: "策略", line: { color: "black", width: 1.3 } },
      {
        x,
        y: d.over.expo,
        name: "日均仓位",
        yaxis: "y2",
        line: { color: "rgba(148,103,189,0.6)", width: 0.8 },
        fill: "tozeroy",
        fillcolor: "rgba(148,103,189,0.12)",
      },
    ],
    {
      margin: { l: 50, r: 20, t: 28, b: 24 },
      hovermode: "x unified",
      title: {
        text: "全期 (日末净值, 对数轴): 黑 = 策略, 灰 = 持有, 紫 = 日均仓位, 橙 = 当前窗口; 点击跳到该日",
        font: { size: 12 },
      },
      yaxis: { type: "log" },
      yaxis2: { overlaying: "y", range: [0, 4], visible: false },
      shapes: [
        {
          type: "rect",
          xref: "x",
          yref: "paper",
          x0: w0,
          x1: w1,
          y0: 0,
          y1: 1,
          fillcolor: "rgba(255,165,0,0.25)",
          line: { color: "orange", width: 1 },
        },
      ],
      showlegend: false,
    },
    { responsive: true },
  );
  if (!overBound) {
    $("over").on("plotly_click", (e) => {
      $("from").value = String(e.points[0].x).slice(0, 10);
      schedule();
    });
    overBound = true;
  }
}

function drawWin(d) {
  const w = d.win,
    x = w.t.map(hhmm),
    n = x.length,
    K = $("kind").value;
  const traces = [
    {
      type: "candlestick",
      x,
      open: w.o,
      high: w.h,
      low: w.l,
      close: w.c,
      name: "K",
      showlegend: false,
      increasing: { line: { color: UP, width: 1 } },
      decreasing: { line: { color: DOWN, width: 1 } },
    },
  ];
  if (w.mid.some((v) => v != null)) {
    const at = (z) =>
      w.mid.map((m, i) => (m == null ? null : m + z * w.half[i]));
    traces.push({
      x,
      y: at(-1),
      name: "下沿",
      line: { color: BAND, width: 1 },
      hoverinfo: "skip",
    });
    traces.push({
      x,
      y: at(1),
      name: "上沿",
      line: { color: BAND, width: 1 },
      fill: "tonexty",
      fillcolor: "rgba(31,119,180,0.07)",
      hoverinfo: "skip",
    });
    const N = Number(
      fields()[kind().axes.findIndex((a) => a.name === "N")].value,
    );
    for (let j = 1; j < N; j++)
      traces.push({
        x,
        y: at(-1 + (2 * j) / N),
        name: `档 ${j}/${N}`,
        line: { color: "gray", width: 0.6, dash: "dot" },
        hoverinfo: "skip",
        showlegend: false,
      });
    traces.push({
      x,
      y: w.mid,
      name: "中线",
      line: { color: "orange", width: 1.3 },
    });
  }
  if (w.stop.some((v) => v != null))
    traces.push({
      x,
      y: w.stop,
      name: "止损",
      mode: "lines",
      line: { color: UP, width: 1, dash: "dash", shape: "hv" },
    });

  const prev = (i) => (i ? w.w[i - 1] : w.w0);
  const up = [],
    dn = [];
  for (let i = 0; i < n; i++) {
    if (w.w[i] > prev(i) + 1e-9) up.push(i);
    else if (w.w[i] < prev(i) - 1e-9) dn.push(i);
  }
  const mark = (idx, isUp) => ({
    x: idx.map((i) => x[i]),
    y: idx.map((i) => w.c[i]),
    mode: "markers",
    name: isUp ? "加仓" : "减仓",
    marker: {
      symbol: isUp ? "triangle-up" : "triangle-down",
      color: isUp ? UP : DOWN,
      size: idx.map((i) => 7 + 14 * Math.abs(w.w[i] - prev(i))),
      line: { width: 0.5, color: "black" },
    },
    text: idx.map((i) => `${prev(i).toFixed(2)} → ${w.w[i].toFixed(2)}`),
    hovertemplate: (isUp ? "加仓" : "减仓") + " %{text}<extra></extra>",
  });
  traces.push(mark(up, true), mark(dn, false));

  traces.push({
    x,
    y: w.w,
    yaxis: "y2",
    name: "仓位",
    line: { shape: "hv", color: "rgba(148,103,189,0.9)", width: 1 },
    fill: "tozeroy",
    fillcolor: "rgba(148,103,189,0.15)",
  });
  traces.push({
    x,
    y: w.nav,
    yaxis: "y3",
    name: "策略净值 (窗内)",
    line: { color: "black", width: 1.2 },
  });
  traces.push({
    x,
    y: w.c.map((c) => c / w.c[0]),
    yaxis: "y3",
    name: "持有净值 (窗内)",
    line: { color: "gray", width: 1, dash: "dot" },
  });

  const A = META.assets[$("asset").value];
  Plotly.react(
    "win",
    traces,
    {
      margin: { l: 55, r: 55, t: 48, b: 40 },
      hovermode: "x",
      title: {
        text: `${A.name} (${A.set}) | ${K} ${paramText()}${Number($("trail").value) > 0 ? " + 吊灯 k=" + $("trail").value : ""} | ${n} 根, 加 ${up.length} 次 / 减 ${dn.length} 次`,
        font: { size: 12 },
      },
      xaxis: {
        type: "category",
        rangeslider: { visible: false },
        nticks: 12,
        tickangle: 0,
      },
      yaxis: { domain: [0.32, 1], title: { text: "价" } },
      yaxis2: { domain: [0, 0.26], range: [0, 1.05], title: { text: "仓位" } },
      yaxis3: {
        overlaying: "y2",
        anchor: "x",
        side: "right",
        title: { text: "窗内净值" },
        showgrid: false,
      },
      legend: { orientation: "h", x: 0, y: 1.04, font: { size: 11 } },
    },
    { responsive: true },
  );
}

// ---- 启动 ----

(async () => {
  META = await (await fetch("/api/meta")).json();
  $("asset").innerHTML = META.assets
    .map(
      (a, i) =>
        `<option value="${i}">${a.name} (${a.set}, ${dash(a.first)} … ${dash(a.last)})</option>`,
    )
    .join("");
  $("kind").innerHTML = META.kinds
    .map((k) => `<option>${k.name}</option>`)
    .join("");
  $("kind").value = "band";
  renderParams();
  randomFrom();
  $("side").addEventListener("change", (e) => {
    if (e.target.id === "kind") renderParams();
    if (e.target.id === "asset") ((DAYS = []), randomFrom());
    schedule();
  });
  $("prev").onclick = () => shift(-1);
  $("next").onclick = () => shift(1);
  $("rand").onclick = () => {
    randomFrom();
    schedule();
  };
  document.addEventListener("keydown", (e) => {
    if (e.target.tagName === "INPUT" || e.target.tagName === "SELECT") return;
    if (e.key === "ArrowLeft") shift(-1);
    if (e.key === "ArrowRight") shift(1);
  });
  run();
})();
