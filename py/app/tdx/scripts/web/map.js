// 布局图：缩放、按类还原标准控件外观、绘制层（shim gdiwatch）与绘制元素

let mapScale = null; // 布局图缩放；null = 待适配（有尺寸时取刚好放下）
let MAP = null; // 布局图世界坐标 {x0,y0,W,H}

// detail 子项 st 位，与 shim.c ST_* 一致
const ST = {
  SEL: 1,
  CHECK: 2,
  KIDS: 4,
  OPEN: 8,
  GRAY: 16,
  SEP: 32,
  SUB: 64,
  ROW: 128,
};

function applyMapSize() {
  if (!MAP) return;
  if (mapScale === null) {
    const wrap = $("mapWrap");
    if (!wrap.clientWidth) return; // 布局 tab 隐藏中，切回时再适配
    mapScale = Math.min(wrap.clientWidth / MAP.W, wrap.clientHeight / MAP.H);
  }
  const svg = $("map");
  svg.setAttribute("width", MAP.W * mapScale);
  svg.setAttribute("height", MAP.H * mapScale);
  $("zoomLbl").textContent = "×" + mapScale.toFixed(2);
}
function zoomReset() {
  mapScale = null;
  applyMapSize();
  const wrap = $("mapWrap");
  wrap.scrollLeft = wrap.scrollTop = 0;
}
// 鼠标事件 → 布局图世界坐标（即屏幕坐标，GetWindowRect 本就是屏幕系）
function svgPoint(ev) {
  const svg = $("map");
  const p = svg.createSVGPoint();
  p.x = ev.clientX;
  p.y = ev.clientY;
  return p.matrixTransform(svg.getScreenCTM().inverse());
}

// 布局图：按屏幕坐标、按 Win32 z 序还原控件外观（标准控件按类画，自绘区打斜纹）。
const NS = "http://www.w3.org/2000/svg";
function attrs(e, a) {
  for (const k in a) e.setAttribute(k, a[k]);
  return e;
}
function el(parent, tag, a = {}, text) {
  const e = attrs(document.createElementNS(NS, tag), a);
  if (text != null) e.textContent = text;
  parent.appendChild(e);
  return e;
}
const rectAttrs = ([l, t, r, b], cls) => ({
  x: l,
  y: t,
  width: Math.max(0, r - l),
  height: Math.max(0, b - t),
  ...(cls ? { class: cls } : {}),
});
// 嵌套 svg 作裁剪框：viewBox 与位置一致，内部仍用屏幕坐标
function clipBox(parent, [l, t, r, b]) {
  return el(parent, "svg", {
    ...rectAttrs([l, t, r, b]),
    viewBox: `${l} ${t} ${Math.max(0, r - l)} ${Math.max(0, b - t)}`,
  });
}
// 矩形内单行文字，超出裁掉；align = start/middle/end
function textIn(parent, R, s, align = "start", cls = "") {
  const [l, t, r, b] = R;
  if (!s || r <= l || b <= t) return;
  const x = align === "middle" ? (l + r) / 2 : align === "end" ? r - 3 : l + 3;
  el(
    clipBox(parent, R),
    "text",
    {
      x,
      y: (t + b) / 2,
      "text-anchor": align,
      "dominant-baseline": "central",
      class: cls,
    },
    s,
  );
}
const mnem = (s) => (s || "").replace(/&(.)/g, "$1"); // 去助记符 &X / &&

const btnType = (n) => n.style & 0xf; // BS_TYPEMASK
const isCheckLike = (n) =>
  [2, 3, 4, 5, 6, 9].includes(btnType(n)) && !(n.style & 0x1000); // 非 BS_PUSHLIKE
const btnSkin = (n) =>
  btnType(n) === 7 ? "group" : isCheckLike(n) ? "bare" : "btn";
const is = (v) => () => v;
const isWeb = (n) => /^(Chrome_|Cef)/.test(n.class);

// 子项 [l,t,r,b,st,text]（已换到屏幕坐标）
function drawButton(g, n, items) {
  const st = items[0]?.[4] || 0;
  const text = mnem(oneLine(n.title));
  const cls = n.en ? "" : "gray";
  const [l, t, r] = n.rect;
  if (btnType(n) === 7)
    return textIn(g, [l + 6, t, r, t + 14], text, "start", cls); // 分组框
  if (isCheckLike(n)) {
    const radio = [4, 9].includes(btnType(n));
    const on = st & ST.CHECK;
    return textIn(
      g,
      n.rect,
      (radio ? (on ? "◉" : "○") : on ? "☑" : "☐") + " " + text,
      "start",
      cls,
    );
  }
  if (st & (ST.SEL | ST.CHECK)) el(g, "rect", rectAttrs(n.rect, "pressed"));
  textIn(g, n.rect, text, "middle", cls);
}
function drawCombo(g, n) {
  const [l, t, r, b] = n.rect;
  const a = Math.min(b - t, 18);
  el(g, "rect", rectAttrs([r - a, t, r, b], "btnface"));
  textIn(g, [r - a, t, r, b], "▾", "middle");
  textIn(g, [l, t, r - a, b], oneLine(n.title), "start", n.en ? "" : "gray");
}
// ListBox / ComboLBox / SysListView32：ST_ROW 行先铺选中底色，其后各格文字
function drawList(g, n, items) {
  for (const it of items) {
    const [, , , , st, text] = it;
    if (st & ST.SEL) el(g, "rect", rectAttrs(it, "selbar"));
    if (!(st & ST.ROW))
      textIn(g, it, text, "start", st & ST.GRAY ? "gray" : "");
  }
}
function drawHeader(g, n, items) {
  for (const it of items) {
    el(g, "rect", rectAttrs(it, "hdritem"));
    textIn(g, it, it[5]);
  }
}
function drawTreeItems(g, n, items) {
  for (const it of items) {
    const [l, t, , b, st, text] = it;
    if (st & ST.SEL) el(g, "rect", rectAttrs(it, "selbar"));
    if (st & ST.KIDS)
      textIn(g, [l - 14, t, l, b], st & ST.OPEN ? "▾" : "▸", "middle", "gray");
    textIn(g, [l, t, n.rect[2], b], text); // 文字矩形按原字体量的，放宽到右边界免得被裁
  }
}
function drawMenu(g, n, items) {
  for (const it of items) {
    const [l, t, r, b, st, text] = it;
    if (st & ST.SEP) {
      el(g, "line", {
        x1: l + 2,
        x2: r - 2,
        y1: (t + b) / 2,
        y2: (t + b) / 2,
        class: "sep",
      });
      continue;
    }
    if (st & ST.SEL) el(g, "rect", rectAttrs(it, "selbar"));
    const cls = st & ST.GRAY ? "gray" : "";
    const [txt, acc] = mnem(text).split("\t");
    if (st & ST.CHECK) textIn(g, [l, t, l + 18, b], "✓", "middle", cls);
    textIn(g, [l + 18, t, r - 14, b], txt, "start", cls);
    if (acc) textIn(g, [l, t, r - 14, b], acc, "end", cls);
    if (st & ST.SUB) textIn(g, [r - 14, t, r, b], "▸", "middle", cls);
  }
}
function drawOther(g, n, items, leaf) {
  const [l, t, r, b] = n.rect;
  if (!leaf || r - l < 30 || b - t < 10) return;
  if (isWeb(n)) return textIn(g, n.rect, "网页 (CEF)", "middle", "gray");
  textIn(
    g,
    [l, t, r, Math.min(b, t + 12)],
    oneLine(n.title) || n.class,
    "start",
    "self-lbl",
  );
}
function drawStatic(g, n) {
  textIn(
    g,
    n.rect,
    mnem(oneLine(n.title)),
    ["start", "middle", "end"][n.style & 0x1f] || "start",
    n.en ? "" : "gray",
  );
}
function drawEdit(g, n) {
  textIn(
    g,
    n.rect,
    n.style & 0x20 ? "●".repeat(n.title.length) : oneLine(n.title), // ES_PASSWORD
  );
}

// 标准控件按类：底板皮肤、画法、要不要拉 detail 子项（各为 n 的函数）。
// 表外的类即自绘窗口：皮肤 web / self / frame，内容靠绘制层，其文字段为绘制元素。
// prettier-ignore
const CLASS = {
  //               皮肤             画法            detail
  Button: [btnSkin, drawButton, isCheckLike],
  Static: [is("bare"), drawStatic, is(false)],
  Edit: [is("field"), drawEdit, is(false)],
  ComboBox: [is("field"), drawCombo, is(false)],
  ListBox: [is("field"), drawList, is(true)],
  ComboLBox: [is("field"), drawList, is(true)],
  SysListView32: [is("field"), drawList, is(true)],
  SysTreeView32: [is("field"), drawTreeItems, is(true)],
  SysHeader32: [is("hdr"), drawHeader, is(true)],
  "#32768": [is("menu"), drawMenu, is(true)],
};
const skinOf = (n, leaf) =>
  CLASS[n.class]?.[0](n) || (isWeb(n) ? "web" : leaf ? "self" : "frame");

// 图层记录的客户区原点（屏幕坐标），及子项平移到屏幕
const originOf = (n, rec) => [n.rect[0] + rec.off[0], n.rect[1] + rec.off[1]];
const shift = ([l, t, r, b, ...rest], [x, y]) => [
  l + x,
  t + y,
  r + x,
  b + y,
  ...rest,
];

// 一个窗口：底板 rect.win（接鼠标、带 data-hwnd，事件委托在 #map）+ 按类装饰（有绘制层则由它代替）
function drawNode(g, n, leaf) {
  const base = el(g, "rect", rectAttrs(n.rect, "win " + skinOf(n, leaf)));
  base.dataset.hwnd = n.hwnd;
  el(
    base,
    "title",
    {},
    `${n.hwnd} id=${n.id} ${n.class} "${oneLine(n.title)}" [${n.rect}]`,
  );
  if (!gdiOf(n)) {
    const rec = DETAIL.get(n.hwnd);
    const o = rec && originOf(n, rec);
    const items = rec ? rec.items.map((it) => shift(it, o)) : [];
    return (CLASS[n.class]?.[1] || drawOther)(g, n, items, leaf);
  }
  const lg = el(clipBox(g, n.rect), "g");
  GDI_G.set(n.hwnd, { g: lg, n });
  drawGdi(lg, n);
}

// 绘制层（shim gdiwatch）：每窗口一份客户区坐标的色块/文字，后画的在上。
// 推送来一层只重写该窗口的 <g>（rAF 合帧）；窗口第一次有内容才整图重画。
const GDI_G = new Map(); // hwnd → {g, n}：当前图里该窗口的绘制层容器
const SHOWN = new Set(); // 当前图里画了的 hwnd
const gdiDirty = new Set();
const gdiShown = () => $("gdiOn").checked;
const gdiOf = (n) =>
  gdiShown() && GDI.get(n.hwnd)?.items.length ? GDI.get(n.hwnd) : null;
// 绘制元素：自绘窗口（CLASS 表外）绘制层里的每段文字（按钮/页签/格子），当作该
// 窗口的子节点；hwnd = 窗口#客户区左,上（重画不变），host = 所画窗口，rect 为屏幕坐标
const elemHost = (n) => n.vis && !CLASS[n.class];
function elemsOf(n) {
  const rec = elemHost(n) && gdiOf(n);
  if (!rec) return [];
  const o = originOf(n, rec);
  const out = [];
  for (const it of rec.items)
    if (it[5]?.trim())
      out.push({
        depth: n.depth + 1,
        hwnd: `${n.hwnd}#${it[0]},${it[1]}`,
        host: n.hwnd,
        class: "绘",
        title: it[5].trim(),
        rect: shift(it, o).slice(0, 4),
        vis: 1,
        en: 1,
      });
  return out;
}
// hwnd 或元素 id → 节点
function nodeOf(id) {
  if (!id) return null;
  const host = id.split("#")[0];
  const n = NODE.get(host);
  return n && id !== host ? elemsOf(n).find((e) => e.hwnd === id) : n;
}
// COLORREF 是 0x00BBGGRR
const bgr = (c) =>
  "#" +
  [c & 255, (c >> 8) & 255, (c >> 16) & 255]
    .map((v) => v.toString(16).padStart(2, "0"))
    .join("");
function drawGdi(g, n) {
  const rec = GDI.get(n.hwnd);
  const [x, y] = originOf(n, rec);
  g.setAttribute("transform", `translate(${x},${y})`);
  let s = "";
  for (const [l, t, r, b, c, txt] of rec.items) {
    if (txt == null) {
      s += `<rect x="${l}" y="${t}" width="${r - l}" height="${b - t}" style="fill:${bgr(c)}"/>`;
      continue;
    }
    // 字框宽是客户端按原字体量的：textLength 压到同宽，位置才对得上
    s +=
      `<text x="${l}" y="${(t + b) / 2}" dominant-baseline="central" textLength="${Math.max(1, r - l)}"` +
      ` lengthAdjust="spacingAndGlyphs" style="fill:${bgr(c)};font-size:${Math.max(6, (b - t) * 0.8)}px">${esc(txt)}</text>`;
  }
  for (const [l, t, r, b, , txt] of elemHost(n) ? rec.items : [])
    if (txt?.trim())
      s += `<rect class="elem" data-hwnd="${n.hwnd}#${l},${t}" x="${l}" y="${t}" width="${r - l}" height="${b - t}"/>`;
  g.innerHTML = s;
}
// 列表跟绘制元素走：最多 500ms 标脏一次（行情文字刷得勤）
const listSoon = batch(
  () => dirty("tree"),
  (f) => setTimeout(f, 500),
);
// 布局图可见时只重写来了新层的窗口；窗口第一次有内容才整图标脏（不可见时切入即整图重画）
const gdiSoon = batch(() => {
  let full = false;
  if (shown("layout"))
    for (const h of gdiDirty) {
      const at = GDI_G.get(h);
      if (at) drawGdi(at.g, at.n);
      else if (SHOWN.has(h) && GDI.get(h).items.length) full = true;
    }
  gdiDirty.clear();
  if (full) dirty("map");
  listSoon();
}, requestAnimationFrame);

function renderMap() {
  const svg = $("map");
  svg.innerHTML = "";
  GDI_G.clear();
  SHOWN.clear();
  // 先序 + depth → 父子表；兄弟顺序即 z 序（先 = 上层）
  const kids = TREE.map(() => []);
  const roots = [];
  const stack = [];
  TREE.forEach((n, i) => {
    while (stack.length && TREE[stack.at(-1)].depth >= n.depth) stack.pop();
    (stack.length ? kids[stack.at(-1)] : roots).push(i);
    stack.push(i);
  });
  const onMap = (i) => {
    const {
      vis,
      rect: [l, t, r, b],
    } = TREE[i];
    return vis && r > l && b > t;
  };
  const vis = roots.filter(onMap);
  // 排除跑到屏外的窗口：最小化时 Win32 把窗口挪到 -32000 一带（right/bottom 为负）
  const tops = vis.filter((i) => TREE[i].rect[2] > 0 && TREE[i].rect[3] > 0);
  const ns = tops.map((i) => TREE[i]);
  if (!ns.length) {
    const msg = vis.length
      ? "主窗口已最小化 / 不在前台（切回客户端即恢复）"
      : "无可见控件（切到列表或刷新窗口树）";
    svg.setAttribute("viewBox", "0 0 340 40");
    svg.setAttribute("width", 340);
    svg.setAttribute("height", 40);
    svg.innerHTML = `<text x="6" y="22">${msg}</text>`;
    return;
  }
  const x0 = Math.min(...ns.map((n) => n.rect[0]));
  const y0 = Math.min(...ns.map((n) => n.rect[1]));
  const x1 = Math.max(...ns.map((n) => n.rect[2]));
  const y1 = Math.max(...ns.map((n) => n.rect[3]));
  const W = x1 - x0,
    H = y1 - y0;
  MAP = { x0, y0, W, H };
  svg.setAttribute("viewBox", `${x0} ${y0} ${W} ${H}`);
  applyMapSize();
  const defs = el(svg, "defs");
  const hatch = el(defs, "pattern", {
    id: "hatch",
    width: 6,
    height: 6,
    patternUnits: "userSpaceOnUse",
    patternTransform: "rotate(45)",
  });
  el(hatch, "rect", {
    width: 6,
    height: 6,
    fill: "#1c1c1c",
    stroke: "none",
  });
  el(hatch, "line", {
    x1: 0,
    y1: 0,
    x2: 0,
    y2: 6,
    stroke: "#2a2a2a",
    "stroke-width": 2,
  });
  // 画家算法：先画父，子窗口裁在父矩形内；兄弟从下层往上层画。过滤不中的只调暗自身。
  const F = filterNow();
  const draw = (i, parent) => {
    if (!onMap(i)) return; // IsWindowVisible 已含祖先，隐藏即整棵子树不可见
    const n = TREE[i];
    const sub = kids[i].filter(onMap);
    const g = el(parent, "g", F.match(n) ? {} : { opacity: 0.3 });
    drawNode(g, n, !sub.length);
    SHOWN.add(n.hwnd);
    if (!sub.length) return;
    const clip = clipBox(parent, n.rect);
    for (const k of sub.slice().reverse()) draw(k, clip);
  };
  for (const i of tops.slice().reverse()) draw(i, svg);
  el(svg, "rect", { id: "mapSel", display: "none" });
  el(svg, "rect", { id: "mapHl", display: "none" });
  mark("mapSel", SELECTED && SELECTED.hwnd);
  mark("mapHl", HOVER);
}
