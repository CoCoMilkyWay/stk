// 窗口树：拉取（shim tree / detail）、过滤、列表、悬停与选中、对选中控件的操作

let TREE = []; // 最近一次窗口树
let NODE = new Map(); // hwnd → 节点（随 TREE 重建）
let SELECTED = null; // 选中节点
const COLLAPSED = new Set(); // 折叠的节点 hwnd
let HOVER = null; // 悬停节点 hwnd（重画后恢复浮框）
// 两种图层记录同形 {hwnd, off:[客户区相对窗口左上], items:[[l,t,r,b,v,text?]]}
const DETAIL = new Map(); // hwnd → shim detail（v = st 位）
const GDI = new Map(); // hwnd → shim gdiwatch（v = 色）

// force=false（轮询）：树和子项都没变就不重画
let treeKey = "";
async function refreshTree(force = true) {
  const t0 = performance.now();
  const r = await shim("tree");
  const key = JSON.stringify(r);
  const changed = key !== treeKey;
  treeKey = key;
  TREE = r.err ? [] : r;
  NODE = new Map(TREE.map((n) => [n.hwnd, n]));
  if (changed && !r.err) {
    // 已销毁窗口：只删拉树前就到的（树发出后才建的窗口不在这棵树里，其图层
    // 可能先到，删了就再也不会重推）
    const live = new Set(TREE.map((n) => n.hwnd));
    for (const [h, g] of GDI) if (!live.has(h) && g.at < t0) GDI.delete(h);
  }
  const dchanged = await pullDetails();
  if (force || changed || dchanged) dirty("map", "tree");
}

function needsDetail(n) {
  const [l, t, r, b] = n.rect;
  return n.vis && r > l && b > t && !!CLASS[n.class]?.[2](n);
}
// 列表/勾选等内容变化不发 WinEvent，靠轮询；回 true = 内容有变（丢弃时回 undefined）
let detailKey = "";
const pullDetails = drop(async () => {
  const ns = TREE.filter(needsDetail);
  const rs = await Promise.all(ns.map((n) => shim("detail", n.hwnd)));
  DETAIL.clear();
  ns.forEach((n, i) => rs[i].err || DETAIL.set(n.hwnd, rs[i][0]));
  const key = JSON.stringify(rs);
  const changed = key !== detailKey;
  detailKey = key;
  return changed;
});
// 树的三个触发源（treewatch 推送 / 轮询 / 交互后）都走这里，拉取中再触发只补拉一次
const pullTree = coalesce(() => refreshTree(false));
const autoUpd = () => ck("autoUpd");
const pollFps = () => (autoUpd() ? +$("pollFps").value : 0);
// 轮询连树一起拉：Wine 9 改标题（WM_SETTEXT）不发 WinEvent，推送收不到
const pollTree = () => pollFps() > 0 && pullTree();
// 交互后稍等客户端处理完再补拉一次，不等下个轮询周期；其间再交互并入这一次
const pollSoon = batch(
  async () => {
    await pullTree();
    await setNoraise();
  },
  (f) => setTimeout(f, 120),
);

// 列表行 ↔ 布局图浮框 同一点亮：行加类 cls、浮框 box 罩上，先清再点亮；hwnd=null 即全清 → 点亮的行
function lit(cls, box, hwnd) {
  for (const row of document.querySelectorAll(`.tnode.${cls}`))
    row.classList.remove(cls);
  mark(box, hwnd);
  const row =
    hwnd && document.querySelector(`#tree .tnode[data-hwnd="${hwnd}"]`);
  row?.classList.add(cls);
  return row;
}
// hover 与选中同一写法：悬停 hl / mapHl，选中 active / mapSel
function hover(hwnd) {
  HOVER = hwnd;
  lit("hl", "mapHl", hwnd)?.scrollIntoView({ block: "nearest" });
}
// 布局图最上层浮框（hover / 选中）：按节点矩形摆放，不被子窗口遮住
function mark(id, hwnd) {
  const box = $(id);
  if (!box) return;
  const n = nodeOf(hwnd);
  if (!n) return box.setAttribute("display", "none");
  box.removeAttribute("display");
  attrs(box, rectAttrs(n.rect));
}

// 某节点是否有子节点（下一个节点 depth 更深）
function hasChildren(i) {
  return i + 1 < TREE.length && TREE[i + 1].depth > TREE[i].depth;
}

// 过滤条件每次渲染读一次 → {text 过滤词, match(n) 词中, keep(n) 列表里留不留}
function filterNow() {
  const text = $("filter").value.trim().toLowerCase(),
    onlyVis = ck("onlyVis"),
    hideZero = ck("hideZero");
  const match = (n) =>
    !text ||
    `${n.class} ${n.title} ${n.hwnd} ${n.id}`.toLowerCase().includes(text);
  const keep = (n) => {
    const [l, t, r, b] = n.rect;
    return (
      !(onlyVis && !n.vis) && !(hideZero && (l === r || t === b)) && match(n)
    );
  };
  return { text, match, keep };
}

function renderTree() {
  const F = filterNow();
  const html = [];
  let hideDepth = null; // 折叠时跳过比它更深的节点
  for (let i = 0; i < TREE.length; i++) {
    const n = TREE[i];
    const kids = hasChildren(i);
    // 折叠：有过滤时不折叠（要能搜到深处）
    if (!F.text) {
      if (hideDepth !== null) {
        if (n.depth > hideDepth) continue;
        hideDepth = null;
      }
      if (COLLAPSED.has(n.hwnd) && kids) hideDepth = n.depth;
    }
    const els = elemsOf(n); // 绘制元素排在子窗口前，随本窗口折叠
    if (F.keep(n)) html.push(treeRow(n, kids || els.length > 0));
    if (F.text || !COLLAPSED.has(n.hwnd))
      for (const e of els) if (F.match(e)) html.push(treeRow(e));
  }
  $("tree").innerHTML = html.join("");
}
function treeRow(n, kids = false) {
  const fold = !kids ? "" : COLLAPSED.has(n.hwnd) ? "▸" : "▾";
  const cls =
    "tnode" +
    (n.vis ? "" : " invis") +
    (SELECTED?.hwnd === n.hwnd ? " active" : "");
  const ttl = n.title
    ? ` <span class="ttl">"${esc(oneLine(n.title).slice(0, 60))}"</span>`
    : "";
  const body = n.host
    ? `<span class="fold"></span><span class="cls">${n.class}</span>${ttl}` +
      ` <span class="meta">[${n.rect.join(",")}]</span>`
    : `<span class="fold">${fold}</span>` +
      `<span class="meta">${n.hwnd} id=${n.id}</span> <span class="cls">${esc(n.class)}</span>${ttl}` +
      ` <span class="meta">[${n.rect.join(",")}] v${n.vis}e${n.en}</span>`;
  return (
    `<div class="${cls}" style="padding-left:${4 + n.depth * 14}px" data-hwnd="${n.hwnd}"` +
    `${kids ? " data-kids" : ""} title="${esc(`${n.class} | ${n.title}`)}">${body}</div>`
  );
}
// 列表行事件一律委托：点 ▸/▾ 折叠、点选、双击选中并闪烁、悬停
function initTree() {
  const box = $("tree");
  const rowOf = (e) => e.target.closest(".tnode");
  // 行可能比树旧一拍（绘制元素随绘制层变），取不到节点即不理
  const nodeAt = (row) => row && nodeOf(row.dataset.hwnd);
  box.onclick = (e) => {
    const row = rowOf(e);
    if (!row) return;
    if (!e.target.classList.contains("fold")) {
      const n = nodeAt(row);
      return n && select(n);
    }
    if (!("kids" in row.dataset)) return;
    const h = row.dataset.hwnd;
    COLLAPSED.has(h) ? COLLAPSED.delete(h) : COLLAPSED.add(h);
    dirty("tree");
  };
  box.ondblclick = (e) => {
    const n = !e.target.classList.contains("fold") && nodeAt(rowOf(e));
    if (!n) return;
    select(n);
    act("highlight");
  };
  box.onmouseover = (e) => {
    const row = rowOf(e);
    if (row && row.dataset.hwnd !== HOVER) hover(row.dataset.hwnd);
  };
  box.onmouseleave = () => hover(null);
}

function select(n) {
  SELECTED = n;
  $("sel").innerHTML = n.host
    ? `绘制元素 <b>"${esc(n.title)}"</b> · 画在 <b>${n.host}</b> · rect=[${n.rect.join(",")}]`
    : `<b>${n.hwnd}</b> · id=<b>${n.id}</b> · class=<b>${esc(n.class)}</b>` +
      `<br>title=<b>${esc(oneLine(n.title)) || "(空)"}</b> · rect=[${n.rect.join(",")}] · vis=${n.vis} en=${n.en}`;
  $("mTarget").value = n.host || n.hwnd;
  lit("active", "mapSel", n.hwnd);
}

async function act(kind, ...args) {
  if (!SELECTED) return out("selOut", "先选一个控件");
  if (SELECTED.host) return out("selOut", elemAct(kind));
  out("selOut", await shim(kind, SELECTED.hwnd, ...args));
}
// 绘制元素没有自己的窗口：点击 = 往所画窗口投其中心点，文本/矩形取自绘制层，
// 闪烁即布局图上的选中框
function elemAct(kind) {
  const { host, title, rect } = SELECTED;
  const [l, t, r, b] = rect;
  if (kind === "click") {
    sendInput("clickat", host, (l + r) >> 1, (t + b) >> 1);
    return `已向 ${host} 投中心点击`;
  }
  if (kind === "gettext") return { text: title };
  if (kind === "rect") return { rect };
  if (kind === "highlight") return "";
  return `${kind} 对绘制元素无效（它没有自己的窗口）`;
}
function doSettext() {
  return act("settext", $("setval").value);
}
