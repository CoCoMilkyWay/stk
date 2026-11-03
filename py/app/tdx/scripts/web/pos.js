// 持仓页：持仓表（行内闪电下单，按钮即滑点色块；调仓卖出滑杆见 reb.js）与各单状态

// 下单：行内按钮 → POST /api/order（后台跑闪电窗），各单逐步状态经 ord 路推送。
// 默认锁住，解锁才下得了单；锁住时点按钮同点行（选股）。
// 按钮自左而右 大买 … 小买、小卖 … 大卖，越往两边额越大；0 = 最小单位，按 起买股数 × 现价 标 1 位数的 K 或 W，
// 与 1W / 5W 同宽、各行对齐
const ORD_SIZES = [0, 10000, 50000];
function ordLabel(n, p) {
  if (n) return wan(n);
  const k = Math.max(1, Math.round((p.lot * p.price) / 1000));
  return k < 10 ? `${k}K` : `${Math.round(k / 10)}W`;
}
const ordSizes = (s) => (s > 0 ? ORD_SIZES.toReversed() : ORD_SIZES);
let ordLocked = true;
function ordLock() {
  ordLocked = !ordLocked;
  $("posTbl").classList.toggle("locked", ordLocked);
  $("ordLock").textContent = ordLocked ? "解锁买卖" : "锁定买卖";
}
// 买入方式只管买入，卖出一律担保品卖出
$("posTbl").onclick = async (e) => {
  if (e.target.closest(".sell")) return;
  const b = e.target.closest("button.ord");
  if (!b || ordLocked) return pickClick(e);
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
// 只列进行中的与最近 ORD_KEEP 单
const ORD_KEEP = 3;
function onOrd(jobs) {
  $("ordInfo").innerHTML = jobs
    .filter((j, i) => j.state === "run" || i >= jobs.length - ORD_KEEP)
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

// 列注册表：key → [表头, 行 → 单元格 html, 行 → 排序键?, 单元格 class?, 帧 → 合计?]；sum 即表尾合计各行
const sumOf = (r, k) => r.rows.reduce((a, p) => a + (p[k] ?? 0), 0);
const numCol = (h, k, f = fmt, sum = false) => [
  h,
  (p) => f(p[k]),
  (p) => p[k],
  "num",
  sum ? (r) => f(sumOf(r, k)) : null,
];
// 名称着色：今日清仓灰，在当前调仓名单里青，其余不着色
const NAME_TIP = "名称：灰 = 今日清仓，青 = 在当前调仓名单里";
const nameColor = (p) =>
  p.closed ? "var(--mut)" : REB?.codes.includes(p.code) ? "var(--acc)" : "";
// 当日盈亏按成交自算；与持仓的今买今卖对不上时，相关行当日两列留空，合计标「不全」
const DAY_GAP = "成交与持仓今买 / 今卖对不上，当日盈亏不全（点刷新）";
const COL = {
  code: [
    "代码",
    (p) => p.code,
    (p) => p.code,
    "",
    (r) => `合计 ${r.rows.filter((p) => !p.closed).length} 只`,
  ],
  name: [
    `<span data-tip="${NAME_TIP}">名称</span>`,
    (p) => paint(nameColor(p), esc(p.name)),
    (p) => p.name,
  ],
  position_pct: numCol("仓位%", "position_pct", fmt, true),
  day_pnl_pct: numCol("当日盈亏%", "day_pnl_pct", fmtPnl),
  day_pnl: [
    ...numCol("当日盈亏", "day_pnl", kwPnl).slice(0, 4),
    (r) =>
      r.synced
        ? kwPnl(sumOf(r, "day_pnl"))
        : `<span class="off" data-tip="${DAY_GAP}">不全</span>`,
  ],
  pnl_pct: numCol("盈亏%", "pnl_pct", fmtPnl),
  pnl: numCol("盈亏", "pnl", kwPnl, true),
  market_value: numCol("总市值", "market_value", kw, true),
  margin_value: numCol("融资市值", "margin_value", kw, true),
  sellable_value: numCol("可卖市值", "sellable_value", kw, true),
  collateral: numCol("担保折算率", "collateral"),
};

// 持仓页：pos、ord 两路，服务端在内存或行情变了才推一帧；出错只改提示行，表留上一帧。
// 刷新与定时都在服务端：POST /api/refresh 只是要一轮，不等；各样查完即推帧，进度随帧（prog）。
// 一帧一份 rows（每只一行，含盘口）：持仓表即其列，行情页取其行。
// 持仓表的可融列与卖出两列取自调仓帧，pos、reb 哪路来帧都重画（拖卖出滑杆时除外）
let POS = null;
const drawPos = () =>
  POS && !REB_DRAG && table("#posTbl", posCols(POS), POS.rows, pickAttrs, POS);
function onPos(r) {
  $("posProg").innerHTML = progTxt(r.prog);
  if (document.activeElement !== $("posAuto"))
    $("posAuto").value = r.prog.every;
  if (fail("posInfo", r) || !r.rows) return;
  POS = r;
  drawPos();
  posInfo(r);
  quoteFrame(r.rows);
}
// 持仓表上方一行：持仓表里没有的代码的挂单、行情时刻、pid（合计在表尾，成交与挂单在状态列）
function posInfo(r) {
  const info = $("posInfo");
  if (!r.rows.length) return void (info.textContent = "无持仓");
  const held = new Set(r.rows.map((p) => p.code));
  const stray = (r.orders ?? []).filter((o) => !held.has(o.code));
  info.innerHTML = [
    stray.length &&
      `挂单 ${stray.length} 笔：` +
        stray
          .map(
            (o) =>
              `${paint(SIDE[o.side].color, SIDE[o.side].name)} ${o.code} ${esc(o.name)} ${esc(FLOW.orders.line(o))}`,
          )
          .join("、"),
    `行情 ${r.time}`,
    `pid ${r.pid}`,
  ]
    .filter(Boolean)
    .join(" · ");
}
// 状态列：今日成交（fills）、挂单（orders，撤单页的未成交委托，还没查过为 null）按代码逐笔，同形两类：
// mark 符号（按方向着色：买绿卖红）、name 名、line(x) 悬停里的一笔
const FLOW = {
  fills: {
    mark: "●",
    name: "成交",
    line: (x) => `${x.time} ${x.qty}股 @${px3(x.price)} ${x.status}`,
  },
  orders: {
    mark: "○",
    name: "挂单",
    line: (x) =>
      `${x.time} ${x.qty - x.filled}${x.filled ? `/${x.qty}` : ""}股 @${px3(x.price)} ${x.status}`,
  },
};
const flowOf = (r, k, code) => (r[k] ?? []).filter((x) => x.code === code);
const flowCol = (r) => [
  `<span data-tip="${FLOW.fills.mark} 今日成交 · ${FLOW.orders.mark} 挂单，买绿卖红；悬停看逐笔">状态</span>`,
  (p) => {
    const marks = [],
      tips = [];
    for (const [k, f] of Object.entries(FLOW)) {
      const xs = flowOf(r, k, p.code);
      for (const s of [1, -1])
        if (xs.some((x) => x.side === s))
          marks.push(paint(SIDE[s].color, f.mark));
      if (xs.length)
        tips.push(
          `${f.name} ${xs.length} 笔`,
          ...xs.map((x) => `  ${SIDE[x.side].name} ${f.line(x)}`),
        );
    }
    return marks.length
      ? `<span data-tip="${esc(tips.join("\n"))}">${marks.join("")}</span>`
      : "";
  },
  (p) => Object.keys(FLOW).reduce((a, k) => a + flowOf(r, k, p.code).length, 0),
  "",
  () =>
    Object.entries(FLOW)
      .map(([k, f]) => `${f.mark}${r[k]?.length ?? "-"}`)
      .join(" "),
];
// 刷新进度：正做哪样（或等界面）、各样（后端 steps [字段, 名称]）读于几时、上轮失败、账户是否读于持仓变动前
function progTxt(g) {
  const at = g.steps.map(([k, n]) => `${n} ${g.at[k] ?? "—"}`).join(" · ");
  return [
    g.step && paint("var(--hot)", `刷新中：${esc(g.step)}…`),
    g.err && `<span class="off" data-tip="${esc(g.err)}">✗ 上轮刷新失败</span>`,
    `读于 ${at}`,
    g.acct_stale &&
      '<span class="off" data-tip="新一轮还没查到信用账户，调仓试算的现金与负债是旧的">账户读于持仓变动前</span>',
  ]
    .filter(Boolean)
    .join(" · ");
}
const refreshPos = async () => fail("posProg", await post("/api/refresh"));
const refreshEvery = async (s) =>
  fail("posProg", await post("/api/refresh/every", { s }));

// 滑点 p = 万分之一，正=成本；> 表示 5 档吃不满、实际更差
const slipColor = (v) =>
  v == null
    ? ""
    : v < 10
      ? "var(--green)"
      : v < 20
        ? "var(--hot)"
        : "var(--red)";
const slipTxt = (v, full = true) =>
  v == null ? "-" : `${full ? "" : ">"}${v.toFixed(1)}p`;
// 下单按钮即盘口色块：边框买卖色、填充同向同额的滑点色（无同额滑点列、竞价、无行情不填）；
// hover 出该向各额滑点、价位（后端 slip_cols / level_cols 列定义 × 行里 slips / levels，
// 最小单位另取 lot_slips 按买、卖）与占比
function ordBtns(p, r) {
  if (p.closed) return '<span class="kv">今日清仓</span>';
  const slip = (n, s) =>
    n
      ? p.slips?.[r.slip_cols.findIndex(([m, t]) => m === n && t === s)]
      : p.lot_slips?.[s > 0 ? 0 : 1];
  const slipAt = (n, s) => {
    const x = slip(n, s);
    return `${ordLabel(n, p)} ${slipTxt(x.pct, x.full)}`;
  };
  const tip = (s) =>
    [
      `${SIDE[s].name} 滑点 ` +
        (p.slips
          ? [
              slipAt(0, s),
              ...r.slip_cols.flatMap(([n, t]) =>
                t === s ? [slipAt(n, s)] : [],
              ),
            ].join(" · ")
          : p.auction
            ? "竞价中"
            : "无行情"),
      p.levels &&
        "价位 " +
          r.level_cols
            .flatMap(([pc, t], i) =>
              t === s ? [`${SIDE[s].sign}${pc}% ${px3(p.levels[i])}`] : [],
            )
            .join(" · "),
      `占比 ${fmt(p.weight)}%`,
    ]
      .filter(Boolean)
      .join("\n");
  return [1, -1]
    .map((s) => {
      const t = esc(tip(s));
      return ordSizes(s)
        .map((n) => {
          const c = slipColor(slip(n, s)?.pct);
          const fill =
            c && `;background:color-mix(in srgb, ${c} 45%, transparent)`;
          return `<button class="ord" style="border-color:${SIDE[s].color}${fill}" data-tip="${t}" data-s="${s}" data-size="${n}">${ordLabel(n, p)}</button>`;
        })
        .join("");
    })
    .join(" ");
}
const ordHead = (r) =>
  `<span data-tip="${esc(
    "滑点 p = 万分之一，正=成本；按钮填充 绿 < 10p ≤ 黄 < 20p ≤ 红\n持仓按占比加权 " +
      r.slip_cols
        .map(
          ([n, s], i) =>
            `${SIDE[s].name}${wan(n)} ${slipTxt(r.totals.slips[i])}`,
        )
        .join(" · "),
  )}">买 / 卖</span>`;
const cols = (...keys) => keys.map((k) => COL[k]);
const posCols = (r) => [
  ...cols("code", "position_pct", "name"),
  flowCol(r),
  FIN_COL,
  [ordHead(r), (p) => ordBtns(p, r)],
  ...cols(
    "day_pnl_pct",
    "day_pnl",
    "pnl_pct",
    "pnl",
    "market_value",
    "margin_value",
    "sellable_value",
  ),
  ...SELL_COLS,
  COL.collateral,
];
