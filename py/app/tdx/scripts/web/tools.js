// 工具页：原始消息、Spy 录制

// 原始消息栏 ↔ spy 记录 {hwnd,msg,wparam,lparam}
const MSG_FIELDS = ["mTarget", "mMsg", "mWp", "mLp"];
function fillMsg(e) {
  [e.hwnd, e.msg, e.wparam, e.lparam].forEach(
    (v, i) => ($(MSG_FIELDS[i]).value = v),
  );
}
async function rawMsg(kind) {
  const args = MSG_FIELDS.map((id) => $(id).value);
  out("msgOut", await shim(kind, ...args));
}

async function spy(action) {
  const filter = $("spyFilter").value;
  const r = await shim("spy", action, ...(action === "on" ? [filter] : []));
  if (action === "dump" && !r.err) renderSpy(r);
}
const SPY_COLS = [
  ["msg", (e) => e.msg],
  ["hwnd", (e) => e.hwnd],
  ["class", (e) => esc(e.class)],
  ["wparam", (e) => e.wparam],
  ["lparam", (e) => e.lparam],
  [
    "",
    () =>
      '<button data-act="replay">回放</button> <button data-act="locate">定位</button>',
  ],
];
// 行内按钮 data-act → 动作(该行记录)
const SPY_ACT = {
  // 回放：把这条消息 post 回其 hwnd
  replay: (e) => {
    fillMsg(e);
    rawMsg("post");
  },
  // 定位：在当前树里选中该 hwnd，消息填进原始消息栏
  locate: (e) => {
    const n = NODE.get(e.hwnd);
    if (!n) return out("selOut", "当前树里没有 " + e.hwnd + "，先刷新窗口树");
    select(n);
    fillMsg(e);
  },
};
let SPY_EVENTS = [];
function renderSpy(events) {
  SPY_EVENTS = events;
  table("#spyTbl", SPY_COLS, events, (e) => `data-i="${events.indexOf(e)}"`);
}
$("spyTbl").onclick = (e) => {
  const b = e.target.closest("button[data-act]");
  if (b) SPY_ACT[b.dataset.act](SPY_EVENTS[+b.closest("tr").dataset.i]);
};
