// 布局图交互：鼠标 / 键盘 / 滚轮投给客户端，缩放与平移

// 交互：鼠标/键盘只给屏幕坐标，目标窗口由 shim 定（坐标下最深子窗口 / 捕获 / 菜单）。
// 所有输入串成一条队列，保证到达客户端的顺序与操作顺序一致。
let inputQ = Promise.resolve();
const interacting = () => $("interact").checked;
function sendInput(cmd, ...args) {
  inputQ = inputQ.then(async () => {
    const r = await shim(cmd, ...args);
    if (r.err) out("selOut", r);
    pollSoon();
  });
  return inputQ;
}
// Win32 MK_*：LBUTTON 1 / RBUTTON 2 / SHIFT 4 / CONTROL 8 / MBUTTON 0x10
const mk = (e) =>
  (e.buttons & 1 ? 1 : 0) |
  (e.buttons & 2 ? 2 : 0) |
  (e.shiftKey ? 4 : 0) |
  (e.ctrlKey ? 8 : 0) |
  (e.buttons & 4 ? 0x10 : 0);
function mouseAt(e, kind, ...extra) {
  const w = svgPoint(e);
  return sendInput(
    "mouse",
    kind,
    Math.round(w.x),
    Math.round(w.y),
    mk(e),
    ...extra,
  );
}
// 非可打印键 → 虚拟键码；可打印字符走 char（shim 内按 VkKeyScan 决定投键还是 WM_CHAR）
const VK = {
  Backspace: 8,
  Tab: 9,
  Enter: 13,
  Escape: 27,
  PageUp: 33,
  PageDown: 34,
  End: 35,
  Home: 36,
  ArrowLeft: 37,
  ArrowUp: 38,
  ArrowRight: 39,
  ArrowDown: 40,
  Insert: 45,
  Delete: 46,
};
for (let i = 1; i <= 12; i++) VK["F" + i] = 111 + i;

// 左/右键按下抬起分开投（可拖拽），第二下按下投双击；中键归平移。交互关时左键=选中+闪烁。
function initMapInput() {
  const wrap = $("mapWrap");
  const svg = $("map");
  const BIT = { 0: 1, 2: 2 }; // 浏览器 button → 按住位
  const LR = { 1: "l", 2: "r" };
  let held = 0;
  const moveTo = drop((e) => mouseAt(e, "move")); // 拖拽移动：上一条没回来就丢
  // 窗口底板、绘制元素命中框都带 data-hwnd，随整图 / 绘制层重写，hover 走委托
  svg.addEventListener("mouseover", (e) => {
    if (e.target.dataset?.hwnd) hover(e.target.dataset.hwnd);
  });
  svg.addEventListener("mouseout", (e) => {
    if (e.target.dataset?.hwnd) hover(null);
  });
  svg.addEventListener("mousedown", (e) => {
    const bit = BIT[e.button];
    if (!bit) return;
    wrap.focus();
    const n = e.target.dataset && nodeOf(e.target.dataset.hwnd);
    if (!interacting()) {
      if (n && bit === 1) {
        select(n);
        act("highlight");
      }
      return;
    }
    held |= bit;
    mouseAt(e, LR[bit] + (e.detail === 2 ? "dbl" : "down"));
    if (n && bit === 1) select(n);
  });
  window.addEventListener("mousemove", (e) => held && moveTo(e));
  window.addEventListener("mouseup", (e) => {
    const bit = BIT[e.button];
    if (!(held & bit)) return;
    held &= ~bit;
    mouseAt(e, LR[bit] + "up");
  });
  svg.addEventListener("contextmenu", (e) => e.preventDefault());
  wrap.addEventListener("keydown", (e) => {
    if (!interacting() || e.isComposing || e.ctrlKey || e.altKey || e.metaKey)
      return;
    if (e.key.length === 1) sendInput("char", e.key.codePointAt(0));
    else if (VK[e.key]) sendInput("key", VK[e.key]);
    else return;
    e.preventDefault();
  });
}

// 滚轮：交互开时投给客户端（WM_MOUSEWHEEL，±120/格），Ctrl+滚轮或交互关时缩放，围绕光标
function initMapZoom() {
  const wrap = $("mapWrap");
  // 上一格没回来就丢，免得触控板刷屏
  const wheelAt = drop((ev) =>
    mouseAt(ev, "wheel", ev.deltaY < 0 ? 120 : -120),
  );
  wrap.addEventListener(
    "wheel",
    (ev) => {
      ev.preventDefault();
      if (interacting() && !ev.ctrlKey) return void wheelAt(ev);
      if (mapScale === null) return; // 还没有布局图可缩放
      const r = wrap.getBoundingClientRect();
      const cx = ev.clientX - r.left,
        cy = ev.clientY - r.top;
      const contentX = wrap.scrollLeft + cx,
        contentY = wrap.scrollTop + cy;
      const old = mapScale;
      mapScale *= ev.deltaY < 0 ? 1.15 : 1 / 1.15;
      mapScale = Math.min(20, Math.max(0.15, mapScale));
      applyMapSize();
      const ratio = mapScale / old;
      wrap.scrollLeft = contentX * ratio - cx;
      wrap.scrollTop = contentY * ratio - cy;
    },
    { passive: false },
  );
}

// 布局图：中键拖拽平移；中键单击（未拖动）= 选中光标下的元素
function initMapPan() {
  const wrap = $("mapWrap");
  drag(
    wrap,
    (e) => {
      if (e.button !== 1) return null;
      e.preventDefault();
      return { button: 1, sl: wrap.scrollLeft, st: wrap.scrollTop };
    },
    (p, e, dx, dy) => {
      wrap.scrollLeft = p.sl - dx;
      wrap.scrollTop = p.st - dy;
    },
    (p, e, moved) => {
      const n =
        !moved &&
        nodeOf(document.elementFromPoint(e.clientX, e.clientY)?.dataset?.hwnd);
      if (n) select(n);
    },
  );
  // 屏蔽中键默认的自动滚动
  wrap.addEventListener("auxclick", (e) => {
    if (e.button === 1) e.preventDefault();
  });
}
