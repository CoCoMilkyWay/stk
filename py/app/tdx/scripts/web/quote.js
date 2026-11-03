// 行情页：K 线与盘口

// 行情页：持仓表点行选股（默认第一只），画 K 线（新浪，旧在前、含进行中那根）+ 5 档盘口：
// 日线、分钟线（日内 / 近几日，可切档）、盘口同排，图高随盘口。两图各一份状态（KCH），一套画法，
// 只差 KIND 的同名成员（与后端 Day / Min 对仗）。后端给每日根数 per、各根当日第几根 slot：
// 按图宽每 g 根并一格（g 取整除 per、每格不窄于 minW px 的最小值），横轴按交易日 × per 等分、没成交的格留空，
// g 取到 per 仍放不下只画最近的。K 线每 KLINE_GAP 重取；其间每个持仓帧用现价改写进行中那根，盘口随帧刷新。
// 档 [种类, 近几个交易日, 名]：前两项即 /api/kline 的 kind / days
const KRANGES = [
  ["min", 1, "日内"],
  ["min", 2, "2日"],
  ["min", 3, "3日"],
  ["min", 4, "4日"],
];
const KDAY = ["day", 360, "日线"];
const KLINE_GAP = 15000;
const K_COST = "#569cd6";
const K_MA = [
  [5, "#d7ba7d"],
  [10, "#4ec9b0"],
  [20, "#c586c0"],
];
const HOURS = ["09:30", "10:30", "11:30/13:00", "14:00", "15:00"];
// minW 每格最窄 px；ticks(L) → [时间轴 [[标签, x]], 日界 [x]]；label(t) 信息行的时刻；patch(b, q) 用行情改写进行中那根
const KIND = {
  min: {
    minW: 3,
    // 单日标整点与午休；多日画日界、各日居中标 MM-DD
    ticks: (L) => {
      const DW = (L.per / L.g) * L.bw;
      return L.days.length === 1
        ? [HOURS.map((s, i) => [s, ((i * 60) / L.g) * L.bw]), []]
        : [
            L.days.map((d, i) => [d.slice(5), (i + 0.5) * DW]),
            L.days.slice(1).map((_, i) => (i + 1) * DW),
          ];
    },
    label: (t) => t.slice(5, 16),
    patch: (b, q) => {
      b[4] = q.price;
      b[2] = Math.max(b[2], q.price);
      b[3] = Math.min(b[3], q.price);
    },
  },
  day: {
    minW: 6,
    // 隔几根标 MM-DD
    ticks: (L) => {
      const every = Math.ceil(60 / L.bw),
        out = [];
      for (let i = L.vis.length - 1; i >= 0; i -= every)
        out.push([L.vis[i][0].slice(5), (L.pos[i] + 0.5) * L.bw]);
      return [out, []];
    },
    label: (t) => t,
    patch: (b, q) => b.splice(1, 5, q.open, q.high, q.low, q.price, q.volume),
  },
};
let POS_ROWS = []; // 最近一帧持仓行
let QCODE = null; // 选中代码
let QROW = null; // 其持仓行（含 cost / quote）
const K_NONE = { per: 1, bars: [] };
const KMEM = new Map(); // "代码|档" → 最近取到的 K 线；切股 / 切档先画它，再后台重取
// 每图：cv 画布、range 档、kb K 线 {per 每日根数, bars [[时刻, 开, 高, 低, 收, 量, slot]]}、
// ver kb 换了或改写过即加一（view 按它缓存）、view 最近一次的 kLayout（鼠标横坐标 → 第几格）、hover 悬停格在 view.vis 里的下标
const kChart = (cv, range) => ({
  cv,
  range,
  kb: K_NONE,
  ver: 0,
  view: null,
  hover: -1,
});
const KCH = [kChart("kMin", KRANGES[0]), kChart("kDay", KDAY)];
const KMIN = KCH[0];
const kKey = (ch) => `${QCODE}|${ch.range[0]}${ch.range[1]}`;
const pickAttrs = (p) =>
  `data-code="${p.code}"${p.code === QCODE ? ' class="cur"' : ""}`;
function pickClick(e) {
  const tr = e.target.closest("tbody tr");
  if (!tr) return;
  kSelect(tr.dataset.code, KMIN.range);
  setTab("botPane", "quote");
}
function quoteFrame(rows) {
  POS_ROWS = rows;
  kSelect(
    rows.some((p) => p.code === QCODE) ? QCODE : (rows[0]?.code ?? null),
    KMIN.range,
  );
}
// 选股、切分钟档同一入口：换了的图先画缓存，可见即后台重取
function kSelect(code, range) {
  const moved = KCH.filter(
    (ch) => code !== QCODE || (ch === KMIN && range !== ch.range),
  );
  QCODE = code;
  KMIN.range = range;
  for (const ch of moved) {
    ch.kb = KMEM.get(kKey(ch)) ?? K_NONE;
    ch.hover = -1;
    ch.ver++;
  }
  QROW = POS_ROWS.find((p) => p.code === code) ?? null;
  for (const tr of document.querySelectorAll("#posTbl tbody tr"))
    tr.classList.toggle("cur", tr.dataset.code === code);
  tabbar($("kRanges"), String(KRANGES.indexOf(KMIN.range)));
  $("kInfo").innerHTML = QROW
    ? `<b>${QROW.code} ${esc(QROW.name)}</b> · 行情 ${QROW.quote.date} ${QROW.quote.time}`
    : "（无持仓）";
  KCH.forEach(patchBar);
  dirty("kline", "book");
  if (shown("quote")) for (const ch of moved) ch.pull();
}
for (const ch of KCH)
  ch.pull = coalesce(async () => {
    if (!QCODE) return;
    const code = QCODE,
      range = ch.range;
    const [kind, days] = range;
    const r = await post("/api/kline", { code, kind, days });
    if (code !== QCODE || range !== ch.range) return;
    $("kErr").innerHTML = "";
    if (fail("kErr", r)) return;
    KMEM.set(kKey(ch), (ch.kb = r));
    ch.ver++;
    patchBar(ch);
    dirty("kline");
  });
const pullKline = () => KCH.forEach((ch) => ch.pull());
const pullSoon = () => shown("quote") && !document.hidden && pullKline();
// 进行中那根（今日最后一根）随行情改写
function patchBar(ch) {
  const b = ch.kb.bars.at(-1),
    q = QROW?.quote;
  if (!b || !q?.price || b[0].slice(0, 10) !== q.date) return;
  KIND[ch.range[0]].patch(b, q);
  ch.ver++;
}
const divisors = (n) =>
  Array.from({ length: n }, (_, i) => i + 1).filter((g) => n % g === 0);
// 图宽 PW 下的格：全部根每 g 根并一格（agg 各格、at 各格格号、first 各格首根在 bars 里的下标），
// 屏上只放最后 PW / bw 格（vis、pos、first 取可见的那段，pos 为屏上格号）；均线按 agg 全量算
function kLayout({ per, bars }, kind, PW) {
  const days = [...new Set(bars.map((b) => b[0].slice(0, 10)))];
  const dayAt = new Map(days.map((d, i) => [d, i]));
  const cells = (g) => (days.length * per) / g;
  const g = divisors(per).find((g) => PW / cells(g) >= kind.minW) ?? per;
  const bw = Math.max(kind.minW, PW / cells(g));
  const off = Math.max(0, cells(g) - Math.floor(PW / bw + 1e-9));
  const agg = [],
    at = [],
    first = [];
  bars.forEach((b, i) => {
    const k =
      (dayAt.get(b[0].slice(0, 10)) * per) / g + Math.ceil(b[6] / g) - 1;
    const a = agg.at(-1);
    if (at.at(-1) !== k) {
      agg.push(b.slice(0, 6));
      at.push(k);
      first.push(i);
      return;
    }
    a[0] = b[0];
    a[2] = Math.max(a[2], b[2]);
    a[3] = Math.min(a[3], b[3]);
    a[4] = b[4];
    a[5] += b[5];
  });
  const s = at.findIndex((k) => k >= off);
  return {
    per,
    g,
    bw,
    days,
    vis: agg.slice(s),
    pos: at.slice(s).map((k) => k - off),
    first: first.slice(s),
    mas: K_MA.map(([k, col]) => [k, col, maOf(agg, k).slice(s)]),
  };
}
function maOf(bars, k) {
  let s = 0;
  return bars.map((b, i) => {
    s += b[4] - (i >= k ? bars[i - k][4] : 0);
    return i >= k - 1 ? s / k : null;
  });
}
// 上：信息行 + 蜡烛（红涨绿跌）+ 均线 + 成本 / 现价虚线；下：成交量；左：价轴；右：涨跌幅轴（相对昨收）；底：时间轴
const K_AL = 52, // 左侧价轴宽
  K_AR = 56; // 右侧涨跌幅轴宽
function drawK(ch) {
  const cv = $(ch.cv);
  const W = cv.clientWidth,
    H = cv.clientHeight;
  if (!W || !H) return; // 行情页隐藏中，切回时再画
  const dpr = devicePixelRatio,
    cw = Math.round(W * dpr),
    chh = Math.round(H * dpr);
  if (cv.width !== cw || cv.height !== chh) [cv.width, cv.height] = [cw, chh];
  const c = cv.getContext("2d");
  c.setTransform(dpr, 0, 0, dpr, 0, 0);
  c.clearRect(0, 0, W, H);
  c.font = "11px ui-monospace, Consolas, monospace";
  c.textBaseline = "middle";
  const { bars } = ch.kb;
  if (!bars.length) return void (ch.view = null);
  const TX = 16, // 底部时间轴高
    TOP = 30; // 顶部信息行高
  const AL = K_AL,
    PW = W - K_AL - K_AR,
    PR = AL + PW;
  const VH = Math.round((H - TOP - TX) * 0.2);
  const PH = H - TOP - TX - VH - 6;
  const kind = KIND[ch.range[0]];
  const key = `${ch.ver}|${PW}`;
  if (ch.view?.key !== key) ch.view = { ...kLayout(ch.kb, kind, PW), key };
  const L = ch.view;
  const { vis, pos, bw: BW, mas } = L;
  const ref = QROW?.quote.last_close || vis[0][1];
  const pct = (v) =>
    `${v >= ref ? "+" : ""}${((v / ref - 1) * 100).toFixed(2)}%`;
  let lo = Math.min(...vis.map((b) => b[3])),
    hi = Math.max(...vis.map((b) => b[2]));
  const pad = (hi - lo) * 0.05 || hi * 0.01;
  lo -= pad;
  hi += pad;
  const y = (v) => TOP + ((hi - v) / (hi - lo)) * PH;
  const vTop = TOP + PH + 6;
  const vmax = Math.max(1, ...vis.map((b) => b[5]));
  const vy = (v) => vTop + VH - (v / vmax) * VH;
  const x = (i) => AL + (pos[i] + 0.5) * BW;
  const bw = Math.max(1, BW * 0.7);
  const hl = (yy, x0, x1) => {
    c.beginPath();
    c.moveTo(x0, yy);
    c.lineTo(x1, yy);
    c.stroke();
  };
  const vl = (xx) => {
    c.beginPath();
    c.moveTo(xx, TOP);
    c.lineTo(xx, vTop + VH);
    c.stroke();
  };

  // 两侧轴上的读数：左价（右对齐贴图）、右涨跌幅（左对齐贴图）
  const axes = (v, yy) => {
    c.textAlign = "right";
    c.fillText(px3(v), AL - 4, yy);
    c.textAlign = "left";
    c.fillText(pct(v), PR + 4, yy);
  };
  c.strokeStyle = "#333";
  c.fillStyle = "#888";
  c.lineWidth = 1;
  for (let k = 0; k <= 4; k++) {
    const v = lo + ((hi - lo) * k) / 4,
      yy = Math.round(y(v)) + 0.5;
    hl(yy, AL, PR);
    axes(v, yy);
  }
  hl(vTop - 2.5, AL, PR);
  const [ticks, seps] = kind.ticks(L);
  c.setLineDash([2, 3]);
  for (const xx of seps) vl(Math.round(AL + xx) + 0.5);
  c.setLineDash([]);
  // 时间轴标签居中于刻度，按给出的先后画，与已画的相叠的跳过
  c.textAlign = "center";
  const drawn = [];
  for (const [s, xx] of ticks) {
    const w = c.measureText(s).width,
      tx = Math.min(Math.max(AL + xx, AL + w / 2), PR - w / 2);
    const l = tx - w / 2 - 3,
      r = tx + w / 2 + 3;
    if (drawn.some(([a, b]) => l < b && r > a)) continue;
    c.fillText(s, tx, H - TX / 2);
    drawn.push([l, r]);
  }
  c.textAlign = "left";

  vis.forEach(([, o, h, l, cl, v], i) => {
    const xx = Math.round(x(i)) + 0.5;
    c.strokeStyle = c.fillStyle = cl >= o ? UP : DN;
    c.beginPath();
    c.moveTo(xx, y(h));
    c.lineTo(xx, y(l));
    c.stroke();
    const y0 = y(Math.max(o, cl));
    c.fillRect(xx - bw / 2, y0, bw, Math.max(1, y(Math.min(o, cl)) - y0));
    c.fillRect(xx - bw / 2, vy(v), bw, vTop + VH - vy(v));
  });

  for (const [, col, m] of mas) {
    c.strokeStyle = col;
    c.beginPath();
    let on = false;
    m.forEach((v, i) => {
      if (v == null) return;
      on ? c.lineTo(x(i), y(v)) : c.moveTo(x(i), y(v));
      on = true;
    });
    c.stroke();
  }

  // 水平虚线 + 两侧轴上的价签 / 涨跌幅签；出了价格区间不画
  const priceLine = (v, col) => {
    if (!(v >= lo && v <= hi)) return;
    const yy = Math.round(y(v)) + 0.5;
    c.strokeStyle = col;
    c.setLineDash([4, 3]);
    hl(yy, AL, PR);
    c.setLineDash([]);
    c.fillStyle = col;
    c.fillRect(0, yy - 8, AL, 16);
    c.fillRect(PR, yy - 8, K_AR, 16);
    c.fillStyle = "#111";
    axes(v, yy);
  };
  const last = vis.at(-1)[4];
  priceLine(QROW?.cost, K_COST);
  priceLine(last, last >= ref ? UP : DN);
  // 成本 → 现价的竖箭头（赚红亏绿），紧跟最后一根右侧、贴不下即贴图右缘；成本出了价格区间从图边指起
  const cost = QROW?.cost;
  if (cost && cost !== last) {
    const y0 = Math.min(Math.max(y(cost), TOP), TOP + PH),
      y1 = y(last),
      ax = Math.round(Math.min(PR - 8, x(vis.length - 1) + BW / 2 + 8)),
      d = Math.sign(y1 - y0) * Math.min(7, Math.abs(y1 - y0));
    c.strokeStyle = c.fillStyle = last > cost ? UP : DN;
    c.lineWidth = 2;
    c.beginPath();
    c.moveTo(ax, y0);
    c.lineTo(ax, y1 - d);
    c.stroke();
    c.beginPath();
    c.moveTo(ax, y1);
    c.lineTo(ax - 4, y1 - d);
    c.lineTo(ax + 4, y1 - d);
    c.fill();
    c.lineWidth = 1;
  }

  const hov = ch.hover >= 0 && ch.hover < vis.length;
  const at = hov ? ch.hover : vis.length - 1;
  if (hov) {
    c.strokeStyle = "#888";
    vl(Math.round(x(at)) + 0.5);
  }
  const [t, o, h, l, cl, v] = vis[at];
  const pc = bars[L.first[at] - 1]?.[4]; // 前一格的收 = 本格首根之前那根的收
  const chg = pc ? ` ${(((cl - pc) / pc) * 100).toFixed(2)}%` : "";
  c.fillStyle = "#ddd";
  c.fillText(
    `${kind.label(t)}${L.g > 1 ? ` (${L.g}分)` : ""}  开 ${px3(o)} 高 ${px3(h)} 低 ${px3(l)} 收 ${px3(cl)}${chg}  量 ${lots(v)}`,
    4,
    8,
  );
  let mx = 4;
  for (const [k, col, m] of mas) {
    const s = `MA${k} ${px3(m[at]) || "-"}`;
    c.fillStyle = col;
    c.fillText(s, mx, 22);
    mx += c.measureText(s).width + 12;
  }
  if (QROW?.cost) {
    c.fillStyle = K_COST;
    c.fillText(`成本 ${px3(QROW.cost)}`, mx, 22);
  }
}
// 卖五..卖一 / 买一..买五（价按昨收着色，量单位手，底色条按量占比）+ 当日行情与持仓概要
function drawBook() {
  const box = $("book");
  if (!QROW) return void (box.innerHTML = "");
  const p = QROW,
    q = p.quote,
    lc = q.last_close;
  const pc = (v) => (v ? paint(v > lc ? UP : v < lc ? DN : "", px3(v)) : "-");
  const vmax = Math.max(
    1,
    ...[1, -1].flatMap((s) => q.book[s].map((x) => x[1])),
  );
  // book[s] 是方向 s 吃的那侧：买吃卖盘、卖吃买盘，档名与底色取对侧；卖盘倒着列在上，买盘顺着列在下
  const CN = "一二三四五";
  const lv = (s, i) => {
    const x = q.book[s][i],
      o = SIDE[-s];
    const w = x ? ((x[1] / vmax) * 100).toFixed(1) : 0;
    return (
      `<div class="lv" style="background:linear-gradient(to left,${o.tint} ${w}%,transparent ${w}%)">` +
      `<span class="kv">${o.name}${CN[i]}</span><span>${x ? pc(x[0]) : ""}</span>` +
      `<span>${x ? fmt(x[1] / 100, 0) : ""}</span></div>`
    );
  };
  const chg = q.price - lc;
  const kv = [
    ["现价", pc(q.price)],
    ["涨跌", fmtPnl(chg, 3)],
    ["今开", pc(q.open)],
    ["涨幅%", fmtPnl(lc ? (chg / lc) * 100 : null)],
    ["最高", pc(q.high)],
    ["昨收", px3(lc)],
    ["最低", pc(q.low)],
    ["成交量", lots(q.volume)],
    ["成本", px3(p.cost)],
    ["盈亏%", fmtPnl(p.pnl_pct)],
    ["当日盈亏", fmtPnl(p.day_pnl)],
    ["仓位%", fmt(p.position_pct)],
  ];
  box.innerHTML =
    [4, 3, 2, 1, 0].map((i) => lv(1, i)).join("") +
    '<div style="border-top: 1px solid var(--line); margin: 2px 0"></div>' +
    [0, 1, 2, 3, 4].map((i) => lv(-1, i)).join("") +
    '<div class="sum">' +
    kv
      .map(([k, v]) => `<span class="kv">${k}</span><span>${v}</span>`)
      .join("") +
    "</div>";
}
function initK() {
  const sc = $("kRanges");
  sc.innerHTML = KRANGES.map(
    (r, i) => `<button data-tab="${i}">${r[2]}</button>`,
  ).join("");
  tabbar(sc, "0");
  sc.onclick = (e) => {
    const b = e.target.closest("button");
    if (b) kSelect(QCODE, KRANGES[+b.dataset.tab]);
  };
  for (const ch of KCH) {
    const cv = $(ch.cv);
    // 鼠标所在格及以前最近的一格（没成交的格算前一格）
    cv.addEventListener("mousemove", (e) => {
      const k = Math.floor((e.offsetX - K_AL) / (ch.view?.bw ?? 1));
      ch.hover = ch.view ? ch.view.pos.findLastIndex((p) => p <= k) : -1;
      dirty("kline");
    });
    cv.addEventListener("mouseleave", () => {
      ch.hover = -1;
      dirty("kline");
    });
    new ResizeObserver(() => dirty("kline")).observe(cv);
  }
}
