#!/usr/bin/env python3
# console.py — 统一入口：shim 接口控制台（Web 版，stdlib http.server，无三方依赖）。
# 启动时自动接入客户端（desk.ui.attach：shim 未在线即编译 + 注入到 Wine 里的 TdxW.exe），再起服务。
# 本文件只管路由与合路推送；支撑代码都在 scripts/ 下的包里：bridge/（消息桥）、mem/（读内存明文表）、
# desk/（控制台后端各块），前端页面在 scripts/web/。
#
# 启动：python3 console.py [port]      默认 8765
# 浏览器：http://127.0.0.1:8765
#
# 提供的 API：
#   GET  /…                                  控制台页面：scripts/web/ 下的文件（/ 即 index.html）
#   GET  /api/ping                           {pid}（离线为 null）
#   GET  /api/watch?s=路,路                  SSE：所列各路合成一条连接，每条 {s 路名, r 记录}，r 为 null 是该路心跳。路：
#          tree  shim treewatch 原样转发（窗口树版本号）
#          gdi   shim gdiwatch 原样转发（窗口绘制层）
#          pos   持仓页帧（内存持仓 × 内存当日成交 × 新浪实时行情含 5 档，每只一行含盘口列；
#                另带指数名单 × 通达信行情）与刷新进度 prog，变了才推
#          ord   最近各单状态（逐步）
#          rec   录制器 {st 状态, recs 新记录}
#          reb   调仓帧（名单 / 配置 / 账户 / 试算 / 后台事务及其日志），随持仓页帧与配置重算
#   POST /api/kline      {code,kind,days}    持仓页行情里该只近 days 个交易日的 K 线（随行情的源：新浪 / 通达信，本地落盘）：
#                                            kind = min 分钟 / day 日线，
#                                            回 {per 每日根数, bars 各根带当日第几根}
#   POST /api/shim            {cmd,args}     透传 shim 命令，回 JSON 记录列表（命令表见 shim.c）
#   POST /api/clicktext {text,class?,title?} 点可见窗口上画着 text 的按钮（须唯一）
#   POST /api/refresh                        要一轮刷新（不等）：客户端依次重查当日成交、持仓、挂单、信用资产与融资负债，
#                                            各样查完即经 pos 推送，进度在帧的 prog
#   POST /api/refresh/every   {s}            定时刷新周期（秒，0 = 不定时）
#   POST /api/rec             {on}           开 / 停录制
#   POST /api/recmark         {text}         录制中打点
#   POST /api/order {code,s,size,px,credit}  持仓行闪电下单（后台跑），回该单状态；s = 1 买 / -1 卖
#   POST /api/reb/cfg {list?,unit?,pct?,px?,sells?}  改调仓配置（给了的键），回新配置；新帧经 reb 推送
#   POST /api/reb/terms                      后台拉当日两融条款（标的证券 × 担保品，客户端要卡一两分钟）
#   POST /api/reb/run                        后台按当前配置执行调仓
# 浏览器对同一地址至多 6 条 HTTP/1.1 连接，长连接一律走 /api/watch 合路，留出连接给请求。
# 任何失败（assert 等）统一回 {err}。

import os
import sys
import json
import queue
import types
import threading
import subprocess
import urllib.parse
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SCRIPTS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scripts")
sys.path.insert(0, SCRIPTS)  # desk 里按顶层名导入 mem / bridge
from scripts.desk.feed import err  # noqa: E402
from scripts.desk.hub import HUB  # noqa: E402  持仓推送
from scripts.desk.kline import KCACHE, KINDS  # noqa: E402  K 线
from scripts.desk.order import ORDERS  # noqa: E402  下单
from scripts.desk.rebal import REB  # noqa: E402  调仓
from scripts.desk.rec import REC  # noqa: E402  录制器
from scripts.desk.ui import (
    SHIM,
    attach,
    click_text,
)  # noqa: E402  消息桥（接入 / 控制通道）

WEB = os.path.join(SCRIPTS, "web")
MIME = {".html": "text/html", ".css": "text/css", ".js": "text/javascript"}


def kline(b):
    """b = {code, kind: day|min, days} → 持仓页行情里（持仓、今日清仓与指数）该只近 days 个交易日的 K 线
    {per 每个交易日根数, bars [(标号, 开, 高, 低, 收, 量, 当日第几根)]}。"""
    assert b["kind"] in KINDS, f"K 线种类 {b['kind']!r}，可选 {list(KINDS)}"
    kind, q = KINDS[b["kind"]], HUB.quote(b["code"])
    bars = KCACHE.bars(kind, q["sym"], q, int(b["days"]))
    return {"per": kind.per, "bars": [(*k, kind.slot(k[0])) for k in bars]}


# ---------- 合路推送 ----------
# 各路是独立的生成器（各自阻塞、各自心跳），每路一个线程抽取进同一队列：两路 shim 长连接原样转发，三路 Feed。
# 连接断开时置 done，各路在下一条（至迟一个心跳）时自行收尾。

WATCH = {
    "tree": lambda: SHIM.lines("treewatch"),
    "gdi": lambda: SHIM.lines("gdiwatch"),
    "pos": HUB.frames,
    "ord": ORDERS.feed.watch,
    "rec": REC.feed.watch,
    "reb": REB.frames,
}


def watch(b):
    names = b["s"].split(",")
    assert all(n in WATCH for n in names), f"订阅 {names}，可选 {list(WATCH)}"
    q = queue.Queue()
    done = threading.Event()

    def pump(name):
        try:
            with closing(WATCH[name]()) as g:
                for x in g:
                    if done.is_set():
                        return
                    q.put((name, f'{{"s": "{name}", "r": {x}}}'))
        finally:
            # 该路断了（出错或结束）：整条连接跟着断，前端自动重连
            q.put((name, None))

    for n in names:
        threading.Thread(target=pump, args=(n,), daemon=True).start()
    try:
        while True:
            name, x = q.get()
            assert x is not None, f"推送 {name} 断了"
            yield x
    finally:
        done.set()


# ---------- 路由 ----------
# /api/ 下按表分派，处理函数一律 fn(body) → 应答；应答类型决定载体：生成器=SSE、其余=JSON。
# GET 的 body 是查询串。其余 GET 都是 scripts/web/ 下的页面文件。

GET = {
    "/api/ping": lambda b: {"pid": SHIM("ping")[0]["pid"] if SHIM.online() else None},
    "/api/watch": watch,
}

POST = {
    "/api/shim": lambda b: SHIM(b["cmd"], *b["args"]),
    "/api/clicktext": click_text,
    "/api/refresh": lambda b: {"round": HUB.request()},
    "/api/refresh/every": lambda b: HUB.every(float(b["s"])),
    "/api/rec": lambda b: REC.start() if b["on"] else REC.stop(),
    "/api/recmark": lambda b: REC.mark(b["text"]),
    "/api/order": ORDERS.submit,
    "/api/kline": kline,
    "/api/reb/cfg": REB.set,
    "/api/reb/terms": REB.pull,
    "/api/reb/run": REB.run,
}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass  # 静默

    def handle_one_request(self):
        # SSE 长连接，浏览器刷新/关页会中途断连，sendall 撞 BrokenPipe；属正常，静默。
        try:
            super().handle_one_request()
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True

    def do_GET(self):
        path, _, qs = self.path.partition("?")
        if path.startswith("/api/"):
            self._route(GET, path, dict(urllib.parse.parse_qsl(qs)))
        else:
            self._file(path)

    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0))
        self._route(POST, self.path, json.loads(self.rfile.read(n)) if n else {})

    def _route(self, table, path, b):
        try:
            body = table[path](b)
        except Exception as e:  # 错误边界：任何失败都回 {err} 给前端显示
            body = {"err": err(e)}
        if isinstance(body, types.GeneratorType):
            self._stream(body)
        else:
            self._send(
                "application/json; charset=utf-8",
                json.dumps(body, ensure_ascii=False).encode(),
            )

    def _file(self, path):
        f = os.path.normpath(os.path.join(WEB, path.lstrip("/") or "index.html"))
        assert f.startswith(WEB + os.sep) and os.path.isfile(f), f"无此页面文件 {path}"
        with open(f, "rb") as fp:
            self._send(MIME[os.path.splitext(f)[1]] + "; charset=utf-8", fp.read())

    def _send(self, ctype, data):
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _stream(self, gen):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        with closing(gen):
            for x in gen:
                self.wfile.write(f"data: {x}\n\n".encode())
                self.wfile.flush()


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8765
    attach()  # 统一入口一条命令搞定
    threading.Thread(target=HUB.run, daemon=True).start()
    srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    url = f"http://127.0.0.1:{port}"
    print(f"[*] 控制台: {url}  (Ctrl+C 退出)")
    # 不用 webbrowser：它优先读 $BROWSER，IDE 终端可能把它指向内置浏览器
    subprocess.Popen(
        ["xdg-open", url], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    srv.serve_forever()


if __name__ == "__main__":
    main()
