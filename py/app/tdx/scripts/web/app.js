// 页面骨架：视图切换与合帧重画、shim 状态、推送连接、分栏拖拽、启动

// 上下两栏同构：pane 下 .tabbar 按钮 data-tab ↔ 视图 data-view。
// 画法 → [所在视图, 画]：别处标脏（dirty），rAF 合帧只画所在视图可见的；切入视图即画其全部画法。
const CUR = {}; // pane → 当前视图
const shown = (v) => Object.values(CUR).includes(v);
const PARTS = {
  map: ["layout", () => renderMap()],
  tree: ["list", () => renderTree()],
  rec: ["rec", () => recRender()],
  kline: ["quote", () => KCH.forEach(drawK)],
  book: ["quote", () => drawBook()],
};
const DIRTY = new Set();
const flush = batch(() => {
  for (const p of DIRTY) if (shown(PARTS[p][0])) PARTS[p][1]();
  DIRTY.clear();
}, requestAnimationFrame);
function dirty(...ps) {
  for (const p of ps) DIRTY.add(p);
  flush();
}
function tabbar(box, cur) {
  for (const b of box.children) b.classList.toggle("on", b.dataset.tab === cur);
}
function setTab(pane, v) {
  CUR[pane] = v;
  tabbar(document.querySelector(`#${pane} > .tabbar`), v);
  for (const view of document.querySelectorAll(`#${pane} > [data-view]`))
    view.style.display = view.dataset.view === v ? "flex" : "none";
  dirty(...Object.keys(PARTS).filter((p) => PARTS[p][0] === v));
  if (v === "quote") pullKline();
  syncStreams();
}

async function ping() {
  const r = await get("/api/ping");
  const st = $("status");
  st.className = r.pid ? "on" : "off";
  st.textContent = r.pid
    ? "shim 在线 · pid=" + r.pid
    : "shim 离线 — 重启 console.py 自动注入";
  if (r.pid) await setNoraise();
  return !!r.pid;
}
// 不抬窗开关（shim noraise）：无参只读状态
async function setNoraise(arg) {
  const r = await shim("noraise", ...(arg ? [arg] : []));
  if (r.err) return out("selOut", r);
  $("noraise").checked = !!r[0].on;
  $("noraiseLbl").textContent = `已拦 ${r[0].blocked} 次`;
}

// 推送连接：每条 {on 该连着, routes 路名 → 处理, opened 每次（重）连上}，es 为当前连接。
// base 常驻：tree 来 {gen} 即重拉树，gdi 来一层即记下并合帧重画，rec 录制器；
// pos（连带 ord、reb）只在持仓页或行情页可见时连，断开后服务端即停取行情。
// 标签转后台一律断开（不占浏览器对本地址的连接数），回前台重连，重连时服务端先推各路全量。
function onTree() {
  if (autoUpd()) pullTree();
}
function onGdi(r) {
  r.at = performance.now();
  GDI.set(r.hwnd, r);
  if (!gdiShown()) return;
  gdiDirty.add(r.hwnd);
  gdiSoon();
}
const STREAMS = {
  base: {
    on: () => true,
    routes: { tree: onTree, gdi: onGdi, rec: onRec },
    opened: () => (REC_RECS = []),
  },
  pos: {
    on: () => shown("pos") || shown("quote"),
    routes: { pos: onPos, ord: onOrd, reb: onReb },
  },
};
function syncStreams() {
  for (const st of Object.values(STREAMS)) {
    const on = st.on() && !document.hidden;
    if (on && !st.es) st.es = subscribe(st.routes, st.opened);
    if (!on && st.es) {
      st.es.close();
      st.es = null;
    }
  }
}

// 中间 gutter 上下拖拽，改上 pane 高度
function initGutter() {
  const wrap = document.querySelector(".wrap");
  drag(
    $("gutter"),
    (e) => {
      e.preventDefault();
      document.body.style.cursor = "row-resize";
      return { r: wrap.getBoundingClientRect() };
    },
    ({ r }, e) => {
      const px = Math.max(160, Math.min(r.height - 160, e.clientY - r.top));
      $("topPane").style.flex = `0 0 ${px}px`;
    },
    () => (document.body.style.cursor = ""),
  );
}

(async function init() {
  initGutter();
  initMapZoom();
  initMapPan();
  initMapInput();
  initTree();
  initK();
  setTab("topPane", "layout");
  setTab("botPane", "quote");
  renderSpy([]);
  if (await ping()) await refreshTree();
  syncStreams();
  document.addEventListener("visibilitychange", syncStreams);
  loop(pollTree, () => (pollFps() ? 1000 / pollFps() : 500));
  loop(
    () => ck("spyAuto") && spy("dump"),
    () => 1000,
  );
  loop(pullSoon, () => KLINE_GAP);
})();
