// 持仓页：持仓表、滑点表、闪电下单与各单状态

// 下单：行内按钮 → POST /api/order（后台跑闪电窗），各单逐步状态经 ord 路推送
const ORD_SIZES = [0, 10000, 50000];
const ordBtns = (p) =>
  p.closed
    ? '<span class="kv">今日清仓</span>'
    : [1, -1]
        .map((s) =>
          ORD_SIZES.map(
            (n) =>
              `<button class="ord" style="color:${SIDE[s].color}" data-s="${s}" data-size="${n}">${wan(n)}</button>`,
          ).join(""),
        )
        .join(" ");
// 买入方式只管买入，卖出一律担保品卖出
$("posTbl").onclick = async (e) => {
  const b = e.target.closest("button.ord");
  if (!b) return pickClick(e);
  const s = +b.dataset.s;
  fail(
    "ordInfo",
    await post("/api/order", {
      code: b.closest("tr").dataset.code,
      s,
      size: +b.dataset.size,
      px: $("ordPx").value,
      credit: s > 0 ? $("ordCredit").value : "plain",
    }),
  );
};
// 每单一行：步骤条（已过 / 当前 / 失败 / 未到）+ 结果
const ORD_ST = { run: "进行中", ok: "✓", err: "✗" };
const STEP_COLOR = {
  done: "var(--green)",
  run: "var(--hot)",
  err: "var(--red)",
  todo: "var(--mut)",
};
const stepOf = (j, i) =>
  i < j.at || j.state === "ok"
    ? "done"
    : i > j.at
      ? "todo"
      : j.state === "err"
        ? "err"
        : "run";
function onOrd(jobs) {
  $("ordInfo").innerHTML = $("rebOrd").innerHTML = jobs
    .map((j) => {
      const steps = j.steps
        .map((s, i) => paint(STEP_COLOR[stepOf(j, i)], s))
        .join(" › ");
      const side = paint(SIDE[j.s].color, SIDE[j.s].name);
      return (
        `<div>${j.t} ${side}${j.label} ${j.code} ${esc(j.name)} ${esc(j.detail)}` +
        ` · ${steps} · ${ORD_ST[j.state]} ${esc(j.msg)}</div>`
      );
    })
    .join("");
}

// 列注册表：key → [表头, 行 → 单元格 html, 行 → 排序键?, 单元格 class?]；持仓表、滑点表都从这里按 key 取列
const numCol = (h, k, f = fmt) => [h, (p) => f(p[k]), (p) => p[k], "num"];
const GROUP_COLOR = {
  green: "var(--green)",
  red: "var(--red)",
  "": "var(--hot)",
};
const COL = {
  code: ["代码", (p) => p.code, (p) => p.code],
  name: [
    "名称",
    (p) => paint(GROUP_COLOR[p.group], esc(p.name)),
    (p) => p.name,
  ],
  position_pct: numCol("仓位%", "position_pct"),
  weight: numCol("占比%", "weight"),
  order: ["买 / 卖", ordBtns],
  day_pnl_pct: numCol("当日盈亏%", "day_pnl_pct", fmtPnl),
  day_pnl: numCol("当日盈亏", "day_pnl", fmtPnl),
  pnl_pct: numCol("盈亏%", "pnl_pct", fmtPnl),
  pnl: numCol("盈亏", "pnl", fmtPnl),
  market_value: numCol("总市值", "market_value"),
  margin_value: numCol("融资市值", "margin_value"),
  sellable_value: numCol("可卖市值", "sellable_value"),
  collateral: numCol("担保折算率", "collateral"),
};

// 持仓页：pos、ord 两路，服务端在内存或行情变了才推一帧；出错只改提示行，表留上一帧。
// 客户端重查即推帧，定时刷新为兜底：到点 POST /api/refresh，新帧仍经推送到达。
let autoRefreshTried = false;
let nextRefresh = 0; // 下次自动刷新时刻（performance.now）
// 一帧一份 rows（每只一行，含盘口列）：持仓表、滑点表是它的两组列（同形的帧函数），行情页取其行
function onPos(r) {
  if (fail("posInfo", r)) return;
  const held = r.rows.filter((p) => !p.closed);
  table("#posTbl", posCols(r), r.rows, pickAttrs);
  table("#slipTbl", slipCols(r), held, pickAttrs);
  posInfo(r, held);
  quoteFrame(r.rows);
}
function posInfo(r, held) {
  const info = $("posInfo");
  if (!r.rows.length) {
    // 按需：内存里没持仓表时，自动点一次交易栏「刷新」让客户端查一遍
    if (!autoRefreshTried) return refreshPos();
    return void (info.textContent = "无持仓");
  }
  const t = r.totals;
  // 当日盈亏按成交自算；与持仓的今买今卖对不上时，相关行当日两列留空、总数不全
  const sync = r.synced
    ? ` · 今日成交 ${r.fills} 笔`
    : ` · <span class="off">成交与持仓今买 / 今卖对不上，当日盈亏不全（点刷新）</span>`;
  info.innerHTML =
    `${held.length} 只 · 总市值 <b>${fmt(t.market_value)}</b>` +
    ` · 盈亏 ${fmtPnl(t.pnl)} · 当日盈亏 ${fmtPnl(t.day_pnl)}` +
    sync +
    ` · 行情 ${r.time} · pid=${r.pid}`;
}
async function refreshPos(quiet) {
  autoRefreshTried = true; // 手动刷新也算一次尝试，避免空内存再自动重触发
  nextRefresh = performance.now() + +$("posAuto").value * 1000;
  if (!quiet)
    $("posInfo").textContent = "刷新中…（触发客户端重查当日成交与持仓）";
  fail("posInfo", await post("/api/refresh"));
}
const autoRefresh = () =>
  +$("posAuto").value &&
  STREAMS.pos.es &&
  performance.now() >= nextRefresh &&
  refreshPos(true);

// 滑点正=成本
const slipColor = (v) =>
  v == null
    ? ""
    : v < 10
      ? "var(--green)"
      : v < 20
        ? "var(--hot)"
        : "var(--red)";
// 按 slip_cols 顺序自上而下 n 根；无行情画灰
const slipBars = (slips, n) =>
  `<span class="bars">${(slips ?? Array(n).fill(null))
    .map(
      (x) => `<i style="background:${slipColor(x?.pct) || "var(--line)"}"></i>`,
    )
    .join("")}</span>`;
// 盘口两组列同构：后端列定义 r[cols]、行里的值 row[vals]，表头 head(列, 第几列, 帧)、格子 cell(值)、
// 排序键 key(值)、没值时 none(行)（无行情皆空，竞价时滑点出「竞价」）
const DEPTH = [
  {
    cols: "slip_cols",
    vals: "slips",
    head: ([n, s], i, r) =>
      paint(
        slipColor(r.totals.slips[i]) || "var(--mut)",
        wan(n) + SIDE[s].name,
      ),
    cell: (x) =>
      paint(
        slipColor(x.pct),
        x.pct == null ? "" : `${x.full ? "" : ">"}${x.pct.toFixed(1)}p`,
      ),
    key: (x) => x.pct,
    none: (row) => (row.auction ? '<span class="kv">竞价</span>' : ""),
  },
  {
    cols: "level_cols",
    vals: "levels",
    head: ([p, s]) => `${SIDE[s].sign}${p}%`,
    cell: (v) => `<span class="kv">${v.toFixed(2)}</span>`,
    key: (v) => v,
    none: () => "",
  },
];
const depthCols = (r) =>
  DEPTH.flatMap((d) =>
    r[d.cols].map((c, i) => [
      d.head(c, i, r),
      (row) => (row[d.vals] ? d.cell(row[d.vals][i]) : d.none(row)),
      (row) => row[d.vals] && d.key(row[d.vals][i]),
      "num",
    ]),
  );
const cols = (...keys) => keys.map((k) => COL[k]);
const posCols = (r) => [
  ...cols("code", "position_pct", "name", "order"),
  ["滑点", (p) => slipBars(p.slips, r.slip_cols.length)],
  ...cols(
    "day_pnl_pct",
    "day_pnl",
    "pnl_pct",
    "pnl",
    "market_value",
    "margin_value",
    "sellable_value",
    "collateral",
  ),
];
const slipCols = (r) => [
  ...cols("code", "name"),
  ...depthCols(r),
  ...cols("weight"),
];
