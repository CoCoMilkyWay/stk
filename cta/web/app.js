// app.js — CTA 轨迹前端: 收集选项 + 视窗 → GET /api/run (cta_trace 现算 / 聚合) → 画; 本文件不做任何计算
//   主图: 收盘线 + band (中线 / 上下沿 / 档线) + 加减仓点 (逐根时); 底部: 仓位 + 全期净值 (策略 vs 持有, 对数轴)
//   x = 有效根下标 (连续, 无休市空档); 框选放大即按新视窗重取, 双击回全期; 纵轴范围随视窗由后端给, x / y 同步
const $ = (id) => document.getElementById(id);
const UP = "#d62728",
  DOWN = "#2ca02c",
  BAND = "rgba(31,119,180,0.8)";
let META = null;
let view = [0, 0]; // 视窗 [lo, hi) 有效根下标; hi = 0 即到末根

const dash = (d) => {
  const s = String(d);
  return `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}`;
};
const minute = (t) => {
  const s = String(t);
  return `${dash(s.slice(0, 8))} ${s.slice(8, 10)}:${s.slice(10, 12)}`;
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

// 按测试集 + 筛选串列资产, 尽量保留当前选中; 返回选中是否变了
function renderAssets() {
  const set = $("set").value,
    f = $("filter").value.trim().toLowerCase(),
    cur = $("asset").value;
  const opts = META.assets
    .map((a, i) => [a, i])
    .filter(
      ([a]) =>
        a.set === set &&
        (!f ||
          a.name.toLowerCase().includes(f) ||
          a.code.toLowerCase().includes(f)),
    );
  $("asset").innerHTML = opts
    .map(([a, i]) => `<option value="${i}">${a.name}  ${a.code}</option>`)
    .join("");
  if (opts.some(([, i]) => String(i) === cur)) $("asset").value = cur;
  else if (opts.length) $("asset").value = String(opts[0][1]);
  return $("asset").value !== cur;
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
    vol: $("vol").value || 0,
    buffer: $("buffer").value || 0,
    lo: view[0],
    hi: view[1],
    px: Math.max(100, Math.floor($("chart").clientWidth / 2)),
  }).toString();
}

// ---- 取数 ----

let seq = 0,
  timer = 0;
const schedule = () => {
  clearTimeout(timer);
  timer = setTimeout(run, 120);
};

async function run() {
  if ($("asset").value === "") return;
  const my = ++seq,
    t0 = performance.now();
  $("busy").textContent = "计算中…";
  const d = await (await fetch("/api/run?" + query())).json();
  if (my !== seq) return;
  $("busy").textContent = `${Math.round(performance.now() - t0)} ms`;
  $("span").textContent = `${dash(d.first)} … ${dash(d.last)}, ${d.n} 根`;
  drawStats(d);
  draw(d);
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

let bound = false;
function draw(d) {
  const v = d.v,
    m = v.i.length,
    x = v.i.map((i) => i + (d.step - 1) / 2),
    lab = v.t.map(minute);
  const traces = [
    {
      x,
      y: v.c,
      text: lab,
      name: "收",
      line: { color: "#333", width: 1 },
      hovertemplate: "%{text}<br>收 %{y}<extra></extra>",
    },
  ];
  if (v.mid.some((y) => y != null)) {
    const at = (z) =>
      v.mid.map((c, k) => (c == null ? null : c + z * v.half[k]));
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
      y: v.mid,
      name: "中线",
      line: { color: "orange", width: 1.3 },
    });
  }

  const prev = (k) => (k ? v.w[k - 1] : v.w0);
  const up = [],
    dn = [];
  if (d.step === 1)
    for (let k = 0; k < m; k++) {
      if (v.w[k] > prev(k) + 1e-9) up.push(k);
      else if (v.w[k] < prev(k) - 1e-9) dn.push(k);
    }
  const mark = (idx, isUp) => ({
    x: idx.map((k) => x[k]),
    y: idx.map((k) => v.c[k]),
    mode: "markers",
    name: isUp ? "加仓" : "减仓",
    marker: {
      symbol: isUp ? "triangle-up" : "triangle-down",
      color: isUp ? UP : DOWN,
      size: idx.map((k) => 7 + 14 * Math.abs(v.w[k] - prev(k))),
      line: { width: 0.5, color: "black" },
    },
    text: idx.map(
      (k) => `${lab[k]} ${prev(k).toFixed(2)} → ${v.w[k].toFixed(2)}`,
    ),
    hovertemplate: (isUp ? "加仓" : "减仓") + " %{text}<extra></extra>",
  });
  traces.push(mark(up, true), mark(dn, false));

  traces.push({
    x,
    y: v.w,
    yaxis: "y2",
    name: "仓位",
    line: { shape: "hv", color: "rgba(148,103,189,0.9)", width: 1 },
    fill: "tozeroy",
    fillcolor: "rgba(148,103,189,0.15)",
  });
  traces.push({
    x,
    y: v.nav,
    yaxis: "y3",
    name: "策略净值",
    line: { color: "black", width: 1.2 },
  });
  traces.push({
    x,
    y: v.hold,
    yaxis: "y3",
    name: "持有净值",
    line: { color: "gray", width: 1, dash: "dot" },
  });

  // 刻度: 视窗跨 20 日以上只标日期
  const daysOnly = d.hi - d.lo > 20 * META.bpd,
    tv = [],
    tt = [];
  for (let j = 0; j < 8 && m > 0; j++) {
    const k = Math.round((j * (m - 1)) / 7);
    tv.push(x[k]);
    tt.push(daysOnly ? lab[k].slice(0, 10) : lab[k].slice(5));
  }
  const A = META.assets[$("asset").value],
    trail = Number($("trail").value) > 0 ? ` + 吊灯 k=${$("trail").value}` : "",
    res =
      d.step === 1
        ? `逐根, 加 ${up.length} 次 / 减 ${dn.length} 次`
        : `每点 ${d.step} 根 (放大到逐根看加减仓)`;
  Plotly.react(
    "chart",
    traces,
    {
      margin: { l: 55, r: 55, t: 48, b: 40 },
      hovermode: "x",
      dragmode: "zoom",
      title: {
        text: `${A.name} ${A.code} | ${$("kind").value} ${paramText()}${trail} | ${lab[0]} … ${lab[m - 1]} | ${res}`,
        font: { size: 12 },
      },
      xaxis: {
        range: [d.lo - 0.5, d.hi - 0.5],
        tickvals: tv,
        ticktext: tt,
      },
      yaxis: {
        domain: [0.32, 1],
        title: { text: "价" },
        range: d.yp,
        fixedrange: true,
      },
      yaxis2: {
        domain: [0, 0.26],
        range: [0, 1.05],
        title: { text: "仓位" },
        fixedrange: true,
      },
      yaxis3: {
        overlaying: "y2",
        anchor: "x",
        side: "right",
        type: "log",
        title: { text: "全期净值" },
        showgrid: false,
        range: d.yn,
        fixedrange: true,
      },
      legend: { orientation: "h", x: 0, y: 1.04, font: { size: 11 } },
    },
    { responsive: true },
  );
  if (!bound) {
    $("chart").on("plotly_relayout", (e) => {
      if (e["xaxis.autorange"]) view = [0, 0];
      else if ("xaxis.range[0]" in e)
        view = [
          Math.max(0, Math.round(e["xaxis.range[0]"] + 0.5)),
          Math.max(1, Math.round(e["xaxis.range[1]"] + 0.5)),
        ];
      else return;
      schedule();
    });
    bound = true;
  }
}

// ---- 启动 ----

(async () => {
  META = await (await fetch("/api/meta")).json();
  $("kind").innerHTML = META.kinds
    .map((k) => `<option>${k.name}</option>`)
    .join("");
  $("kind").value = "band";
  renderParams();
  renderAssets();
  $("filter").addEventListener("input", () => {
    if (renderAssets()) ((view = [0, 0]), schedule());
  });
  $("side").addEventListener("change", (e) => {
    if (e.target.id === "filter") return;
    if (e.target.id === "kind") renderParams();
    if (e.target.id === "set") renderAssets();
    if (e.target.id === "set" || e.target.id === "asset") view = [0, 0];
    schedule();
  });
  run();
})();
