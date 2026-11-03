// 录制页

// 录制器：订阅 rec 路，每推 {st 状态, recs 新记录}；页面只留尾巴，全量在 st.file
const REC_CAP = 2000;
let REC_RECS = [];
let REC_ST = null;
// kind → 显示开关 id；未列的（start/mark/err/stop）恒显
const REC_SHOW = {
  msg: "recMsg",
  req: "recReq",
  new: "recWin",
  del: "recWin",
  chg: "recWin",
  gdi: "recGdi",
};
const recWin = (n) => `${n.hwnd} ${n.class}#${n.id}「${oneLine(n.title)}」`;
const hidden = (vis) => (vis ? "" : " (隐藏)");
const recNode = (r) => `${r.kind} ${recWin(r.node)}${hidden(r.node.vis)}`;
const reqNum = (r) => `${r.op}[${r.id}]=${r.value}`;
// 请求记录按 op、录制记录按 kind 各一行的格式；表外的原样出 JSON
const REQ_FMT = {
  init: (r) => `init=${r.value}`,
  func: (r) => `func=${r.value} (0x${r.value.toString(16)})`,
  str: (r) => `str[${r.id},${r.aux}]=${JSON.stringify(r.value)}`,
  int: reqNum,
  float: reqNum,
  send: (r) => `SEND args=${JSON.stringify(r.args)}`,
};
const REC_FMT = {
  msg: (r) =>
    `msg ${r.msg} → ${recWin(r)} wp=${r.wparam} lp=${r.lparam}` +
    `${r.n > 1 ? ` ×${r.n}` : ""}${hidden(r.vis)}`,
  req: (r) => `req req=${r.obj} ${REQ_FMT[r.op](r)}`,
  new: recNode,
  del: recNode,
  chg: (r) =>
    `chg ${recWin(r.node)} ` +
    Object.entries(r.diff)
      .map(([k, [a, b]]) => `${k}: ${JSON.stringify(a)} → ${JSON.stringify(b)}`)
      .join("; "),
  gdi: (r) =>
    `gdi ${r.hwnd} ${r.cls}#${r.id} | ${oneLine(r.texts.join(" | ")).slice(0, 300)}`,
  mark: (r) => `===== ${r.text} =====`,
};
// 一条记录格式化一次，记在其上
function recLine(r) {
  const { t, kind, ...rest } = r;
  const fmt = REC_FMT[kind];
  return (r.line ??=
    `${t.toFixed(2).padStart(8)} ` +
    (fmt && (kind !== "req" || REQ_FMT[r.op])
      ? fmt(r)
      : `${kind} ${JSON.stringify(rest)}`));
}
// 状态（页签上的红点、按钮、状态行）随推送即改；日志只在录制页可见时画
function recStatus() {
  const st = REC_ST;
  $("recDot").innerHTML = st.on ? '<span class="off">●</span>' : "";
  $("recBtn").textContent = st.on ? "停止" : "开始";
  $("recSt").textContent = st.file
    ? `${st.on ? "录制中" : "已停"} · ${st.n} 条 · ${st.file}`
    : "未录制";
}
function recRender() {
  const show = Object.fromEntries(
    Object.entries(REC_SHOW).map(([k, id]) => [k, ck(id)]),
  );
  const excl = $("recExcl")
    .value.split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  const lines = REC_RECS.filter((r) => show[r.kind] ?? true)
    .map(recLine)
    .filter((s) => !excl.some((x) => s.includes(x)));
  const pre = $("recLog");
  const atEnd = pre.scrollTop + pre.clientHeight >= pre.scrollHeight - 4;
  pre.textContent = lines.join("\n");
  if (atEnd) pre.scrollTop = pre.scrollHeight;
}
function onRec(r) {
  REC_ST = r.st;
  REC_RECS = REC_RECS.concat(r.recs).slice(-REC_CAP);
  recStatus();
  dirty("rec");
}
async function recToggle() {
  fail("recSt", await post("/api/rec", { on: !REC_ST?.on }));
}
async function reqAttach() {
  const st = $("reqSt");
  if (!REC_ST?.on) return fail("reqSt", { err: "请先开始录制" });
  if (!SELECTED || SELECTED.host)
    return fail("reqSt", { err: "请在窗口树中选择闪电下单顶层窗口" });
  const off = $("reqOff").value.trim();
  const r = await shim("req", "attach", SELECTED.hwnd, ...(off ? [off] : []));
  if (fail("reqSt", r)) return;
  if (!r.some((x) => x.attached))
    return (st.textContent =
      "发现多个候选，请把对象偏移填为：" +
      r
        .filter((x) => x.candidate)
        .map((x) => x.offset)
        .join(" / "));
  st.textContent = `✓ 已接入 ${SELECTED.hwnd}，请手动买入、卖出或撤单`;
}
async function recMark() {
  const input = $("recMarkTxt");
  const r = await post("/api/recmark", {
    text: input.value.trim() || "mark",
  });
  if (!fail("recSt", r)) input.value = "";
}
