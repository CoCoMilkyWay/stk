// app.js — CTA 轨迹前端: 收集选项 (模型 + 优化目标) + 视窗 → GET /api/run (cta_trace 滚动调参 / 现算 / 聚合) → 画; 本文件不做任何计算
//   主图: 收盘线 + 上线 / 下线 + 中线 + 层线 (grid / fade) 或吊灯线 (chase) + 加减仓点 (逐根时); 底部: 仓位 + 全期净值 (策略 vs 持有, 对数轴)
//   x = 有效根下标 (连续, 无休市空档); 框选矩形放大 (x / y 一起, plotly 原生), 横轴视窗一变就按新视窗重取数据;
//   双击还原不走 plotly 原生 (它会先按旧数据定一次范围), 取回全期数据后一次画好
//   光标 = 鼠标所在根 (竖线): 侧栏灰显该根所在选参段的参数; 鼠标离开图 = 末根
const $ = (id) => document.getElementById(id);
const UP = "#d62728",
  DOWN = "#2ca02c",
  BAND = "rgba(31,119,180,0.8)";
let META = null;
let D = null; // 最近一次 /api/run
let view = [0, 0]; // 视窗 [lo, hi) 有效根下标; hi = 0 即到末根
let rev = 0; // 还原次数, 进 uirevision
let cur = null; // 光标根下标; null = 末根

const dash = (d) => {
  const s = String(d);
  return `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}`;
};
const minute = (t) => {
  const s = String(t);
  return `${dash(s.slice(0, 8))} ${s.slice(8, 10)}:${s.slice(10, 12)}`;
};

// 部件 pt 的当前模型 / 参数显示框
const kindOf = (pt) => pt.kinds.find((k) => k.name === $(`k_${pt.name}`).value);
const fields = (pt) => [...$(`p_${pt.name}`).querySelectorAll("input")];
const part = (name) => META.parts.find((pt) => pt.name === name);
const models = () => META.parts.map((pt) => kindOf(pt).name).join(" | ");

function renderParts() {
  $("parts").innerHTML = META.parts
    .map(
      (pt) =>
        `<fieldset><legend>${pt.title}</legend>` +
        `<label>模型 <select id="k_${pt.name}">${pt.kinds.map((k) => `<option>${k.name}</option>`).join("")}</select></label>` +
        `<div id="p_${pt.name}"></div></fieldset>`,
    )
    .join("");
  META.parts.forEach(renderParams);
}

function renderParams(pt) {
  $(`p_${pt.name}`).innerHTML = kindOf(pt)
    .axes.map(
      (a) =>
        `<label>${a.name}${a.day ? " (日)" : ""} <input disabled value="–"></label>`,
    )
    .join("");
}

// 光标处 (cur) 所在选参段的参数 → 灰显框
function showAt() {
  if (!D) return;
  const i = cur ?? D.n - 1;
  let k = 0;
  while (k + 1 < D.seg.length && D.seg[k + 1].i <= i) k++;
  const g = D.seg[k];
  META.parts.forEach((pt, j) =>
    fields(pt).forEach((e, x) => (e.value = g.p ? g.p[j][x] : "–")),
  );
  const v = D.v,
    p = Math.min(v.t.length - 1, Math.max(0, Math.floor((i - D.lo) / D.step)));
  $("at").textContent =
    (cur == null ? "末根" : `光标 ${minute(v.t[p])}`) +
    (g.p ? `: 自 ${dash(g.t)} 起用` : ": 空仓 (窗内数据不足)");
}

function moveCursor(e) {
  const c = $("chart"),
    L = c._fullLayout;
  if (!D || !L) return;
  const s = L._size,
    px = e.clientX - c.getBoundingClientRect().left - s.l;
  if (px < 0 || px > s.w) return leaveCursor();
  Object.assign($("cursor").style, {
    display: "block",
    left: `${s.l + px}px`,
    top: `${s.t}px`,
    height: `${s.h}px`,
  });
  cur = Math.max(0, Math.min(D.n - 1, Math.round(L.xaxis.p2d(px))));
  showAt();
}

function leaveCursor() {
  $("cursor").style.display = "none";
  cur = null;
  showAt();
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

function query() {
  const parts = {};
  for (const pt of META.parts) parts[pt.name] = kindOf(pt).name;
  return new URLSearchParams({
    a: $("asset").value,
    ...parts,
    target: $("target").value,
    roll: $("roll").value,
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
  D = d;
  drawStats(d);
  draw(d);
  showAt();
}

// ---- 画 ----

const fmt = (f) => (v) => (v == null ? "–" : f(v));
const f0 = fmt((v) => v.toFixed(0)),
  f1 = fmt((v) => v.toFixed(1)),
  f2 = fmt((v) => v.toFixed(2));
const pct = fmt((v) => (100 * v).toFixed(1) + "%");
const ROWS = [
  ["夏普", "sr", f2],
  ["索提诺", "so", f2],
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
      name: "收",
      line: { color: "#333", width: 1 },
    },
  ];
  traces.push({
    x,
    y: v.dn,
    name: "下线",
    line: { color: DOWN, width: 1 },
  });
  traces.push({
    x,
    y: v.up,
    name: "上线",
    line: { color: UP, width: 1 },
    fill: "tonexty",
    fillcolor: "rgba(31,119,180,0.07)",
  });
  const at = (z) =>
    v.dn.map((d, k) =>
      d == null || v.up[k] == null ? null : d + z * (v.up[k] - d),
    );
  const layer = part("m");
  if (kindOf(layer).name === "chase")
    traces.push({
      x,
      y: v.stop,
      name: "吊灯线",
      line: { color: BAND, width: 1.3, dash: "dash" },
    });
  else {
    const ax = kindOf(layer).axes.find((a) => a.name === "N");
    console.assert(ax.values.length === 1, "层数 N 须为单值轴");
    const N = ax.values[0];
    for (let j = 1; j < N; j++)
      traces.push({
        x,
        y: at(j / N),
        name: `层 ${j}/${N}`,
        line: { color: "gray", width: 0.6, dash: "dot" },
        showlegend: false,
      });
  }
  traces.push({
    x,
    y: at(0.5),
    name: "中线",
    line: { color: "orange", width: 1.3 },
  });

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
    res =
      d.step === 1
        ? `逐根, 加 ${up.length} 次 / 减 ${dn.length} 次`
        : `每点 ${d.step} 根 (放大到逐根看加减仓)`;
  Plotly.react(
    "chart",
    traces,
    {
      margin: { l: 55, r: 55, t: 48, b: 40 },
      hovermode: false,
      dragmode: "zoom",
      uirevision: `${$("asset").value}/${rev}`, // 变了 = 按新数据自动定范围 (全期); 不变 = 保留用户的缩放
      title: {
        text: `${A.name} ${A.code} | band ${models()} @ ${$("target").value}, W=${$("roll").value} | ${lab[0]} … ${lab[m - 1]} | ${res}`,
        font: { size: 12 },
      },
      xaxis: { tickvals: tv, ticktext: tt },
      yaxis: { domain: [0.32, 1], title: { text: "价" } },
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
      },
      legend: { orientation: "h", x: 0, y: 1.04, font: { size: 11 } },
    },
    {
      responsive: true,
      doubleClick: false,
      modeBarButtonsToRemove: ["autoScale2d", "resetScale2d"],
    },
  );
  if (!bound) {
    $("chart").on("plotly_relayout", (e) => {
      if (!("xaxis.range[0]" in e)) return;
      view = [
        Math.max(0, Math.round(e["xaxis.range[0]"] + 0.5)),
        Math.max(1, Math.round(e["xaxis.range[1]"] + 0.5)),
      ];
      schedule();
    });
    // 还原: 先取全期数据, 到了再以新 uirevision 一次画好 (双击由 plotly 判定, 拖拽层吞掉了原生 dblclick)
    $("chart").on("plotly_doubleclick", () => {
      view = [0, 0];
      rev++;
      schedule();
    });
    bound = true;
  }
}

// ---- 启动 ----

(async () => {
  META = await (await fetch("/api/meta")).json();
  renderParts();
  renderAssets();
  $("chart").addEventListener("mousemove", moveCursor);
  $("chart").addEventListener("mouseleave", leaveCursor);
  $("filter").addEventListener("input", () => {
    if (renderAssets()) ((view = [0, 0]), schedule());
  });
  $("side").addEventListener("change", (e) => {
    if (e.target.id === "filter") return;
    if (e.target.id.startsWith("k_")) renderParams(part(e.target.id.slice(2)));
    if (e.target.id === "set") renderAssets();
    if (e.target.id === "set" || e.target.id === "asset") view = [0, 0];
    schedule();
  });
  run();
})();
