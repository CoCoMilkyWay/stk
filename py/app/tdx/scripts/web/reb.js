// 调仓页

// 调仓页：reb 路每帧 {lists, cfg, terms, acct, plan, task}，试算随持仓帧与配置在服务端重算；
// 配置一改即 POST /api/reb/cfg，新帧经推送到达。持仓表里正在拖的卖出滑杆不被重画冲掉。
let REB = null;
const w2 = (v) => (v == null ? "" : fmt(v / 10000) + "万");
const yuan = (v) => (v == null ? "" : fmt(v, 0));
const ratio = (v) => (v == null ? "" : fmt(v, 2));
const pct0 = (v) => (v == null ? "" : fmt(v * 100, 0) + "%");
// 拖动中连发的改动并成一次
let rebPending = {};
const rebFlush = coalesce(async () => {
  const b = rebPending;
  rebPending = {};
  fail("rebTask", await post("/api/reb/cfg", b));
});
function rebSet(b) {
  Object.assign(rebPending, b);
  rebFlush();
}
function rebPct() {
  $("rebPctLbl").textContent = $("rebPct").value + "%";
  rebSet({ pct: +$("rebPct").value });
}
async function rebAct(what) {
  fail("rebTask", await post("/api/reb/" + what));
}
async function rebRun() {
  const p = REB?.plan;
  if (!p?.orders)
    return fail("rebTask", { err: p?.wait || p?.err || "还没有试算" });
  const n = (s, c) =>
    p.orders.filter((o) => o.s === s && (!c || o.credit === c)).length;
  const msg =
    `按当前试算执行调仓：卖 ${n(-1)} 单、融资买 ${n(1, "margin")} 单、现金买 ${n(1, "plain")} 单，` +
    `行使 ${w2(p.use)}，成本约 ${yuan(p.cost)} 元？`;
  if (confirm(msg)) rebAct("run");
}
const legOf = (t, c) => t.legs.find((l) => l.credit === c);
const legCell = (l) =>
  !l
    ? ""
    : l.skip
      ? `<span class="kv">${esc(l.skip)}（${w2(l.want)}）</span>`
      : `${l.qty}股 · ${w2(l.amount)}`;
const REB_TGT = [
  ["代码", (t) => t.code, (t) => t.code],
  ["名称", (t) => esc(t.name), (t) => t.name],
  [
    "",
    (t) =>
      t.why
        ? paint("var(--red)", esc(t.why))
        : t.financeable
          ? paint("var(--hot)", "融资标的")
          : "担保品",
    (t) => t.why ?? (t.financeable ? "0" : "1"),
  ],
  ["保证金比例", (t) => ratio(t.ratio), (t) => t.ratio, "num"],
  ["折算率", (t) => ratio(t.rate), (t) => t.rate, "num"],
  ["现市值", (t) => w2(t.held), (t) => t.held, "num"],
  ["补", (t) => w2(t.need), (t) => t.need, "num"],
  [
    "融资买入",
    (t) => legCell(legOf(t, "margin")),
    (t) => legOf(t, "margin")?.amount,
    "num",
  ],
  [
    "现金买入",
    (t) => legCell(legOf(t, "plain")),
    (t) => legOf(t, "plain")?.amount,
    "num",
  ],
];
const REB_HOLD = [
  ["代码", (h) => h.code, (h) => h.code],
  ["名称", (h) => esc(h.name), (h) => h.name],
  ["市值", (h) => w2(h.value), (h) => h.value, "num"],
  ["可卖市值", (h) => w2(h.sellable), (h) => h.sellable, "num"],
  [
    "卖出面值",
    (h) => {
      const n = rebSell(h),
        w = sellPos(h, n),
        [a, b] = sellLbl(h, n);
      return (
        `<div class="sell${sellSnap(h, h.avail) ? "" : " dis"}" data-code="${h.code}" data-n="${n}">` +
        `<div class="trk"><b style="width:${w}"></b><i style="left:${w}"></i></div>` +
        `<span>${a}</span><span>${b}</span></div>`
      );
    },
  ],
  [
    "卖出",
    (h) =>
      h.order
        ? `${h.order.qty}股 · ${w2(h.order.amount)}`
        : h.skip
          ? `<span class="kv">${esc(h.skip)}</span>`
          : "",
    (h) => h.order?.amount,
    "num",
  ],
];
// 卖出滑杆按股数线性走（0 … 全部可卖），拖动时就地 snap 到能成单的股数：0 不卖、
// first 起按 step 递增（first 够一手且够最小调仓单元）、全部可卖（够最小调仓单元才有），取最近的
function sellSnap(h, x) {
  const unit = REB.cfg.unit;
  const first = Math.ceil(Math.max(h.lot, unit / h.px) / h.step) * h.step;
  const last = Math.ceil(h.avail / h.step) * h.step - h.step;
  const cands = [0];
  if (h.avail * h.px >= unit) cands.push(h.avail);
  if (first <= last)
    cands.push(
      Math.min(last, Math.max(first, Math.round(x / h.step) * h.step)),
    );
  return cands.reduce((a, b) => (Math.abs(b - x) <= Math.abs(a - x) ? b : a));
}
// 两格读数：面值、股数（全部可卖标「全」）
const sellLbl = (h, n) =>
  n ? [w2(n * h.px), `${n === h.avail ? "全 " : ""}${n}股`] : ["不卖", ""];
function sellSame(a, b) {
  const ka = Object.keys(a);
  return ka.length === Object.keys(b).length && ka.every((k) => a[k] === b[k]);
}
// 本地拖过、服务端还没回显的卖出股数：回显前来的旧帧照本地画，免得滑块被拽回去
let REB_SELLS = null;
const rebSell = (h) => (REB_SELLS ? (REB_SELLS[h.code] ?? 0) : (h.sell ?? 0));
const rebHoldOf = (code) => REB.plan.holds.find((h) => h.code === code);
// 滑块 left 与填充宽：n / 全部可卖 × (轨道宽 − 滑块宽)
const sellPos = (h, n) =>
  `calc((100% - 10px) * ${h.avail ? Math.min(1, n / h.avail) : 0})`;
function sellShow(el, h, n) {
  el.dataset.n = n;
  el.querySelector("i").style.left = el.querySelector("b").style.width =
    sellPos(h, n);
  const [a, b] = el.querySelectorAll("span");
  [a.textContent, b.textContent] = sellLbl(h, n);
}
// 拖动：按下即捕获指针，指针横坐标按轨道换算成股数（出了两端夹到 0 / 全部可卖）再 snap；
// 拖动中不重画持仓表（重画会把滑杆从手底下换掉）
let REB_DRAG = null;
function sellDrag(e) {
  const trk = REB_DRAG.querySelector(".trk"),
    r = trk.getBoundingClientRect();
  const f = Math.min(1, Math.max(0, (e.clientX - r.left - 5) / (r.width - 10)));
  const h = rebHoldOf(REB_DRAG.dataset.code),
    n = sellSnap(h, f * h.avail);
  if (n === +REB_DRAG.dataset.n) return;
  sellShow(REB_DRAG, h, n);
  const sells = {};
  for (const el of $("rebHold").querySelectorAll(".sell"))
    if (+el.dataset.n) sells[el.dataset.code] = +el.dataset.n;
  REB_SELLS = sells;
  rebSet({ sells });
}
$("rebHold").onpointerdown = (e) => {
  const el = e.target.closest(".sell");
  if (!el || el.classList.contains("dis") || !e.target.closest(".trk")) return;
  e.preventDefault();
  REB_DRAG = el;
  el.setPointerCapture(e.pointerId);
  sellDrag(e);
};
$("rebHold").onpointermove = (e) => REB_DRAG && sellDrag(e);
$("rebHold").onpointerup = $("rebHold").onpointercancel = () =>
  (REB_DRAG = null);
const REB_EV = {
  order: (r) =>
    `${SIDE[r.order.s].name} ${r.order.credit} ${r.order.code} ${esc(r.order.name)} ${r.order.qty}股 @≈${r.order.px.toFixed(3)}`,
  job: (r) => `${ORD_ST[r.job.state]} ${esc(r.job.detail)} ${esc(r.job.msg)}`,
  plan: (r) =>
    `试算：行使 ${w2(r.plan.use)}（融资 ${w2(r.plan.financed)}）· ${r.plan.orders.length} 单 · 成本 ${yuan(r.plan.cost)}`,
  acct: (r) =>
    `账户：现金 ${w2(r.acct.cash)} · 可用保证金 ${w2(r.acct.avail)} · 负债 ${w2(r.acct.debt)}`,
  pull: () => "拉两融条款",
  page: (r) => `开「${esc(r.func)}」页，等客户端重查…`,
  all: (r) => `「${esc(r.func)}」点查询全部，等拼表…`,
  rows: (r) => `「${esc(r.func)}」内存 ${r.got} / 页上 ${r.n} 行`,
  terms: (r) =>
    `条款 ${r.n} 只（融资标的 ${r.financeable} · 担保品 ${r.pledgeable}）`,
};
// 拼表实时进度：转圈随已等秒数走，停转即后台没在推；条数标签久读不到即客户端界面线程卡着（内存照读）
const SPIN = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏";
function rebLive(v) {
  if (v.s == null) return `开「${esc(v.func)}」页，等客户端重查…`;
  return (
    `${SPIN[v.s % SPIN.length]} 「${esc(v.func)}」已等 <b>${v.s}s</b> · 内存 <b>${v.got}</b> 行 / 页上 ${v.n ?? "—"} 条` +
    (v.hung >= 3
      ? ` · <span class="off">客户端卡着 ${v.hung}s（界面线程忙，内存照读）</span>`
      : "")
  );
}
function onReb(r) {
  REB = r;
  const c = r.cfg;
  $("rebList").innerHTML = r.lists
    .map((n) => `<option${n === c.list ? " selected" : ""}>${esc(n)}</option>`)
    .join("");
  if (document.activeElement !== $("rebUnit")) $("rebUnit").value = c.unit;
  $("rebPx").value = c.px;
  if (document.activeElement !== $("rebPct")) $("rebPct").value = c.pct;
  $("rebPctLbl").textContent = $("rebPct").value + "%";
  const t = r.task;
  $("rebTask").innerHTML = t
    ? `${esc(t.name)} · ${t.state === "run" ? "进行中…" : ORD_ST[t.state]}${t.live ? " · " + rebLive(t.live) : ""} ${esc(t.msg)} · ${esc(t.file)}`
    : "";
  $("rebLog").textContent = (t?.log ?? [])
    .map(
      ({ t, ev, ...rest }) =>
        `${t} ${ev} ${REB_EV[ev]?.(rest) ?? JSON.stringify(rest).slice(0, 300)}`,
    )
    .join("\n");
  const p = r.plan;
  const a = r.acct;
  const terms = r.terms
    ? `两融条款 ${r.terms} 只`
    : '<span class="off">当日两融条款未拉</span>';
  if (!p || p.wait || p.err) {
    $("rebInfo").innerHTML = p?.err
      ? `<span class="off">✗ ${esc(p.err)}</span>`
      : esc(p?.wait ?? "（等试算）");
    $("rebNow").innerHTML = terms;
    return;
  }
  const lev = (x, y) => `${fmt(x)}（现 ${fmt(y)}）`;
  $("rebInfo").innerHTML =
    `购买力 <b>${w2(p.power)}</b> · 预期行使购买力 <b>${w2(p.use)}</b>（融资 ${w2(p.financed)}）` +
    ` · 预期总资产 <b>${w2(p.assets)}</b> · 预期调仓成本 <b>${yuan(p.cost)}</b> 元` +
    ` · 预期杠杆 <b>${lev(p.lev, p.now.lev)}</b> · 维持担保比例 ${pct0(p.maint)}（现 ${pct0(p.now.maint)}）` +
    ` · 每只目标 ${w2((p.V * c.pct) / 100)}`;
  $("rebNow").innerHTML =
    `现：总资产 ${w2(p.now.assets)} · 净资产 ${w2(p.now.net)} · 负债 ${w2(p.now.debt)} · 现金 ${w2(p.now.cash)}` +
    ` · 可用保证金 ${w2(p.now.X)} · 可融余量 ${w2(p.now.L)} · 卖后现金 ${w2(p.after_sell.C)} / 可用保证金 ${w2(p.after_sell.X)}` +
    ` · 账户读于 ${a.at} · ${terms} · 行情 ${p.time}`;
  table("#rebTgt", REB_TGT, p.targets);
  if (REB_SELLS && sellSame(REB_SELLS, c.sells)) REB_SELLS = null;
  if (!REB_DRAG) table("#rebHold", REB_HOLD, p.holds);
}
