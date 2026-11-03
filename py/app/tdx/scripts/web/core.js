// 公用：取元素、后端请求与推送订阅、调度、拖拽、转义、可排序表格、悬停提示、数字格式化

const $ = (id) => document.getElementById(id);
const ck = (id) => $(id).checked;
// 涨 / 跌、买 / 卖两色以 CSS 为源（canvas 不认 var()）
const CSSV = getComputedStyle(document.documentElement);
const UP = CSSV.getPropertyValue("--up").trim(),
  DN = CSSV.getPropertyValue("--dn").trim();
// 方向 s：买 +1 / 卖 -1，与后端同号；买绿卖红（与涨红跌绿相反）；tint 为盘口量条底色
const SIDE = {
  1: { name: "买", sign: "+", color: DN, tint: "var(--dn-tint)" },
  "-1": { name: "卖", sign: "-", color: UP, tint: "var(--up-tint)" },
};

// 后端 GET / POST 两张路由表，前端同形两个入口；失败一律回 {err}
const http = async (path, opt) => (await fetch(path, opt)).json();
const get = (path) => http(path);
const post = (path, body = {}) =>
  http(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
// shim 命令统一入口：回 JSON 记录数组，失败回 {err}
const shim = (cmd, ...args) => post("/api/shim", { cmd, args });
// 出错写进提示行 id，回是否出错
function fail(id, r) {
  if (r.err) $(id).innerHTML = `<span class="off">✗ ${esc(r.err)}</span>`;
  return !!r.err;
}
// 合路长连接 /api/watch：on = {路名: 处理函数}，每条 {s, r} 按路分发，r 为 null 是心跳；
// opened 在每次（重）连上时调，服务端此时会先推各路全量
function subscribe(on, opened = () => {}) {
  const es = new EventSource("/api/watch?s=" + Object.keys(on).join(","));
  es.onopen = opened;
  es.onmessage = (e) => {
    const { s, r } = JSON.parse(e.data);
    if (r !== null) on[s](r);
  };
  return es;
}
// 定时循环：跑完 fn 再隔 gap() 毫秒跑下一轮
function loop(fn, gap) {
  (async function tick() {
    await fn();
    setTimeout(tick, gap());
  })();
}
// 拖拽：down(e) → 上下文（假即不拖；带 button 即只认该键抬起），其后 move(ctx, e, dx, dy)，
// 抬起 up(ctx, e, moved)
function drag(el, down, move, up = () => {}) {
  let ctx = null,
    x = 0,
    y = 0,
    moved = false;
  el.addEventListener("mousedown", (e) => {
    ctx = down(e);
    [x, y, moved] = [e.clientX, e.clientY, false];
  });
  window.addEventListener("mousemove", (e) => {
    if (!ctx) return;
    const dx = e.clientX - x,
      dy = e.clientY - y;
    moved ||= Math.abs(dx) + Math.abs(dy) > 3;
    move(ctx, e, dx, dy);
  });
  window.addEventListener("mouseup", (e) => {
    if (!ctx || (ctx.button != null && e.button !== ctx.button)) return;
    const c = ctx;
    ctx = null;
    up(c, e, moved);
  });
}

// 调度：drop = 上次没完就丢；coalesce = 上次没完就等它完再补跑一次；
// batch = 首次调用排一次 schedule，触发前的调用并入这一次
function drop(fn) {
  let busy = false;
  return async (...a) => {
    if (busy) return;
    busy = true;
    try {
      return await fn(...a);
    } finally {
      busy = false;
    }
  };
}
function coalesce(fn) {
  let busy = false,
    again = false;
  return async () => {
    if (busy) return void (again = true);
    busy = true;
    try {
      do {
        again = false;
        await fn();
      } while (again);
    } finally {
      busy = false;
    }
  };
}
function batch(fn, schedule) {
  let armed = false;
  return () => {
    if (armed) return;
    armed = true;
    schedule(() => {
      armed = false;
      fn();
    });
  };
}
function esc(s) {
  return (s == null ? "" : String(s)).replace(
    /[&<>"]/g,
    (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c],
  );
}
function oneLine(s) {
  return (s || "").replace(/\s+/g, " ").trim();
}

function out(id, t) {
  $(id).textContent = typeof t === "string" ? t : JSON.stringify(t, null, 1);
}
const ZH = new Intl.Collator("zh").compare;
const cmpKey = (x, y, dir) =>
  x == null
    ? +(y != null)
    : y == null
      ? -1
      : dir * (typeof x === "number" ? x - y : ZH(String(x), String(y)));
// 整表重填：cols 每列 [表头 html, 行 → 单元格 html, 行 → 排序键?, 单元格 class?, 合计 → 表尾 html?]；
// attrs(row) 为 <tr> 的属性串；给了 foot 即按各列的合计函数出表尾一行（表里须有 <tfoot>）。
// 有排序键的列点表头排序：首点降序、再点反向，空值恒在末尾；sink(row) 为真的行不参与，按原序沉在最后；
// 排序状态按表记住，重填（如行情刷新）沿用；表头变了才重写。行内事件由调用方委托在表上。
const SORT = {}; // sel → {i 列号, dir ±1, head 当前表头, redraw 按当前数据重填}
function table(
  sel,
  cols,
  rows,
  attrs = () => "",
  foot = null,
  sink = () => false,
) {
  const st = (SORT[sel] ??= { i: -1, dir: -1, head: null });
  const T = document.querySelector(sel);
  st.redraw = () => table(sel, cols, rows, attrs, foot, sink);
  const head =
    "<tr>" +
    cols
      .map(([h, , key, cls], i) => {
        const c = [key && "sortable", cls].filter(Boolean).join(" ");
        const arrow = i === st.i ? (st.dir > 0 ? " ▲" : " ▼") : "";
        return `<th class="${c}" data-i="${i}">${h}${arrow}</th>`;
      })
      .join("") +
    "</tr>";
  if (head !== st.head) {
    T.tHead.innerHTML = st.head = head;
    T.tHead.onclick = (e) => {
      const th = e.target.closest("th.sortable");
      if (!th) return;
      st.dir = st.i === +th.dataset.i ? -st.dir : -1;
      st.i = +th.dataset.i;
      st.redraw();
    };
  }
  const key = cols[st.i]?.[2];
  const list = key
    ? [...rows].sort(
        (a, b) =>
          sink(a) - sink(b) || (sink(a) ? 0 : cmpKey(key(a), key(b), st.dir)),
      )
    : rows;
  T.tBodies[0].innerHTML = list
    .map(
      (row) =>
        `<tr ${attrs(row)}>` +
        cols
          .map(([, f, , cls]) => `<td class="${cls ?? ""}">${f(row)}</td>`)
          .join("") +
        "</tr>",
    )
    .join("");
  if (foot)
    T.tFoot.innerHTML =
      "<tr>" +
      cols
        .map(
          ([, , , cls, ft]) =>
            `<td class="${cls ?? ""}">${ft?.(foot) ?? ""}</td>`,
        )
        .join("") +
      "</tr>";
  tipAt();
}
// 悬停提示：鼠标下最近的 [data-tip] 的文字浮在鼠标旁；元素整个重绘了也按鼠标位置重取，不随重绘消失
const TIP = { x: -1, y: -1 };
function tipAt() {
  const el = document.elementFromPoint(TIP.x, TIP.y)?.closest("[data-tip]");
  const t = $("tip");
  t.hidden = !el;
  if (!el) return;
  t.textContent = el.dataset.tip;
  t.style.left = Math.min(TIP.x + 12, innerWidth - t.offsetWidth - 4) + "px";
  t.style.top =
    (TIP.y + 16 + t.offsetHeight > innerHeight
      ? TIP.y - t.offsetHeight - 8
      : TIP.y + 16) + "px";
}
document.addEventListener("mousemove", (e) => {
  [TIP.x, TIP.y] = [e.clientX, e.clientY];
  tipAt();
});
document.documentElement.addEventListener("mouseleave", () => {
  TIP.x = TIP.y = -1;
  tipAt();
});
document.addEventListener("scroll", tipAt, true);
const paint = (c, s) => (c ? `<span style="color:${c}">${s}</span>` : s);

// 数字格式化：每种小数位一个缓存的格式器；缺值一律空
const NF = {};
const fmt = (v, d = 2) =>
  v == null
    ? ""
    : (NF[d] ??= new Intl.NumberFormat("zh-CN", {
        minimumFractionDigits: d,
        maximumFractionDigits: d,
      })).format(v);
const fmtPnl = (v, d) => (v == null ? "" : paint(v >= 0 ? UP : DN, fmt(v, d)));
const px3 = (v) => fmt(v, 3);
const lots = (shares) => fmt(shares / 100, 0) + "手";
// 金额一律标 W（万），两位小数
const w2 = (v) => (v == null ? "" : fmt(v / 10000) + "W");
const w2Pnl = (v) => (v == null ? "" : paint(v >= 0 ? UP : DN, w2(v)));
const wan = (n) => `${n / 10000}W`;
