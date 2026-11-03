// 持仓页：持仓表（行内闪电下单，按钮即滑点色块；调仓卖出滑杆见 reb.js）与各单状态

// 下单：行内按钮 → POST /api/order（后台跑闪电窗），各单逐步状态经 ord 路推送。
// 默认锁住，买 / 卖表头的按钮解锁才下得了单；锁住时点按钮同点行（选股）。
// 按钮自左而右 大买 … 小买、小卖 … 大卖，越往两边额越大（即后端 slip_cols 的先后）
let ordLocked = true;
function ordLock() {
  ordLocked = !ordLocked;
  $("posTbl").classList.toggle("locked", ordLocked);
  drawPos();
}
// 买入方式只管买入，卖出一律担保品卖出；只有绿色（5 档吃满且低滑点）的直接下，其余先弹窗确认
$("posTbl").onclick = async (e) => {
  if (e.target.closest(".sell")) return;
  const b = e.target.closest("button.ord");
  if (!b || ordLocked) return pickClick(e);
  const code = b.closest("tr").dataset.code,
    s = +b.dataset.s,
    size = +b.dataset.size;
  const p = POS.rows.find((x) => x.code === code),
    x = p.slips?.[+b.dataset.i];
  const slip = x ? slipTxt(x.pct, x.full) : p.auction ? "竞价中" : "无行情";
  if (
    !slipQuick(x) &&
    !confirm(
      `${SIDE[s].name} ${wan(size)} ${code} ${p.name}，滑点 ${slip}，确认下单？`,
    )
  )
    return;
  fail(
    "ordInfo",
    await post("/api/order", {
      code,
      s,
      size,
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
// 占比条列：k 占总市值的比例画成条（表尾按合计），悬停看金额与占比；按占比排序
const barOf = (v, mv, col) => {
  if (v == null || !mv) return "";
  const f = Math.min(1, Math.max(0, v / mv));
  return `<span class="pbar" data-tip="${w2(v)} · ${fmt(f * 100, 0)}%"><b style="width:${f * 100}%;background:${col}"></b></span>`;
};
const barCol = (h, tip, k, col) => [
  `<span data-tip="${tip}，占总市值；悬停看金额">${h}</span>`,
  (p) => barOf(p[k], p.market_value, col),
  (p) => (p.market_value ? p[k] / p.market_value : null),
  "",
  (r) => barOf(sumOf(r, k), sumOf(r, "market_value"), col),
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
    ...numCol("当日盈亏", "day_pnl", w2Pnl).slice(0, 4),
    (r) =>
      r.synced
        ? w2Pnl(sumOf(r, "day_pnl"))
        : `<span class="off" data-tip="${DAY_GAP}">不全</span>`,
  ],
  pnl_pct: numCol("盈亏%", "pnl_pct", fmtPnl),
  pnl: numCol("盈亏", "pnl", w2Pnl, true),
  market_value: numCol("总市值", "market_value", w2, true),
  margin_value: barCol("融资", "融资部分市值", "margin_value", "var(--hot)"),
  sellable_value: barCol(
    "可卖",
    "可卖部分市值",
    "sellable_value",
    "var(--acc)",
  ),
  collateral: numCol("担保折算率", "collateral"),
};

// 持仓页：pos、ord 两路，服务端在内存或行情变了才推一帧；出错只改提示行，表留上一帧。
// 刷新与定时都在服务端：POST /api/refresh 只是要一轮，不等；各样查完即推帧，进度随帧（prog）。
// 一帧一份 rows（每只一行，含盘口）与 indices（指数名单每只一行）：持仓表即 rows 的列，行情页取两者的行。
// 持仓表的可融列与卖出各列取自调仓帧，pos、reb 哪路来帧都重画（拖卖出滑杆时除外）
let POS = null;
const drawPos = () =>
  POS &&
  !REB_DRAG &&
  table("#posTbl", posCols(POS), POS.rows, pickAttrs, POS, (p) => p.closed);
function onPos(r) {
  if (document.activeElement !== $("posAuto"))
    $("posAuto").value = r.prog.every;
  const ok = !fail("posInfo", r) && r.rows;
  if (ok) POS = r;
  $("posProg").innerHTML = progTxt(r.prog, POS);
  if (!ok) return;
  drawPos();
  table("#idxTbl", IDX_COLS, r.indices, pickAttrs);
  posInfo(r);
  quoteFrame(r);
}
// 指数页的表：指数名单每只一行（名单序，可点表头排序），点行选股同持仓表
const idxChg = (r) => (r.quote.price / r.quote.last_close - 1) * 100;
const IDX_COLS = [
  ["代码", (r) => r.code, (r) => r.code],
  ["指数", (r) => esc(r.name), (r) => r.name],
  ["现价", (r) => fmt(r.quote.price), (r) => r.quote.price, "num"],
  ["涨幅%", (r) => fmtPnl(idxChg(r)), idxChg, "num"],
];
// 持仓表上方的提示行：无持仓、持仓表里没有的代码的挂单（合计在表尾，成交与挂单在状态列），没有即不占地
function posInfo(r) {
  const held = new Set(r.rows.map((p) => p.code));
  const stray = (r.orders ?? []).filter((o) => !held.has(o.code));
  $("posInfo").innerHTML = !r.rows.length
    ? "无持仓"
    : stray.length
      ? `挂单 ${stray.length} 笔：` +
        stray
          .map(
            (o) =>
              `${paint(SIDE[o.side].color, SIDE[o.side].name)} ${o.code} ${esc(o.name)} ${esc(FLOW.orders.line(o))}`,
          )
          .join("、")
      : "";
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
// 数据栏的状态行：先是常在的各样（后端 steps [字段, 名称]）读于几时、最近一帧 f 的行情时刻与 pid，
// 再是时有时无的：正做哪样（或等界面）、上轮失败、账户是否读于持仓变动前
function progTxt(g, f) {
  const at = g.steps.map(([k, n]) => `${n} ${g.at[k] ?? "—"}`).join(" · ");
  return [
    `读于 ${at}`,
    `行情 ${f?.time || "—"}`,
    `pid ${f?.pid ?? "—"}`,
    g.step && paint("var(--hot)", `刷新中：${esc(g.step)}…`),
    g.err && `<span class="off" data-tip="${esc(g.err)}">✗ 上轮刷新失败</span>`,
    g.acct_stale &&
      '<span class="off" data-tip="新一轮还没查到信用账户，调仓试算的现金与负债是旧的">账户读于持仓变动前</span>',
  ]
    .filter(Boolean)
    .join(" · ");
}
const refreshPos = async () => fail("posProg", await post("/api/refresh"));
const refreshEvery = async (s) =>
  fail("posProg", await post("/api/refresh/every", { s }));

// 滑点 p = 万分之一，正=成本；> 表示 5 档吃不满、实际更差；绿 < SLIP_OK ≤ 黄 < SLIP_HI ≤ 红
const SLIP_OK = 10,
  SLIP_HI = 20;
const slipColor = (v) =>
  v == null
    ? ""
    : v < SLIP_OK
      ? "var(--green)"
      : v < SLIP_HI
        ? "var(--hot)"
        : "var(--red)";
const slipQuick = (x) => !!x?.full && x.pct != null && x.pct < SLIP_OK;
const slipTxt = (v, full = true) =>
  v == null ? "-" : `${full ? "" : ">"}${v.toFixed(1)}p`;
// 下单按钮即盘口色块：一列滑点一个按钮（后端 slip_cols [额, 方向] 即按钮的额与先后），
// 边框买卖色、填充该列的滑点色（竞价、无行情不填）；hover 出该向各额滑点、价位（level_cols × levels）与占比
function ordBtns(p, r) {
  if (p.closed) return '<span class="kv">今日清仓</span>';
  const of = (s) =>
    r.slip_cols.flatMap(([n, t], i) => (t === s ? [[n, i]] : []));
  const tip = (s) =>
    [
      `${SIDE[s].name} 滑点 ` +
        (p.slips
          ? of(s)
              .map(
                ([n, i]) =>
                  `${wan(n)} ${slipTxt(p.slips[i].pct, p.slips[i].full)}`,
              )
              .join(" · ")
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
      return of(s)
        .map(([n, i]) => {
          const c = slipColor(p.slips?.[i].pct);
          const fill =
            c && `;background:color-mix(in srgb, ${c} 45%, transparent)`;
          return `<button class="ord" style="border-color:${SIDE[s].color}${fill}" data-tip="${t}" data-s="${s}" data-size="${n}" data-i="${i}">${wan(n)}</button>`;
        })
        .join("");
    })
    .join(" ");
}
const ordHead = (r) =>
  `<span data-tip="${esc(
    `滑点 p = 万分之一，正=成本；按钮填充 绿 < ${SLIP_OK}p ≤ 黄 < ${SLIP_HI}p ≤ 红，非绿的点了先确认\n持仓按占比加权 ` +
      r.slip_cols
        .map(
          ([n, s], i) =>
            `${SIDE[s].name}${wan(n)} ${slipTxt(r.totals.slips[i])}`,
        )
        .join(" · "),
  )}">买 / 卖</span> <button onclick="ordLock()">${ordLocked ? "解锁买卖" : "锁定买卖"}</button>`;
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
