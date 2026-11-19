"""项目研究工具的命令行入口 (给没有 MCP 客户端的 agent; 说明书见仓库 AGENT.md)

用法: python py/agent.py <工具> [参数]
  工具 = doc / scope / run / wait / cancel / unlock
  参数 = JSON 对象 (缺省 {}) / @文件 (读该文件的 JSON) / - (读标准输入)
例:
  python py/agent.py doc
  python py/agent.py run '{"calls": [{"tool": "list_tried", "args": {}}]}'
  python py/agent.py wait '{"job": 3}'

每次调用 = 一次 MCP 握手 (initialize → notifications/initialized) + 一个 tools/call; 打印结果原文.
退出码: 0 = 做成 / 1 = 工具报错 (原文写原因) / 2 = 协议错. 地址读 output/run/agent/mcp.json (程序一开即写).
"""

import json
import sys
import urllib.request
from pathlib import Path

ENDPOINT = Path(__file__).resolve().parent.parent / "output" / "run" / "agent" / "mcp.json"
TIMEOUT = 90  # 秒: 服务端 run / wait 至多等 50 秒即交回
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))  # 本机地址不走代理


def post(url, msg):
    req = urllib.request.Request(
        url, json.dumps(msg).encode(), {"Content-Type": "application/json", "Accept": "application/json, text/event-stream"}
    )
    with OPENER.open(req, timeout=TIMEOUT) as r:
        body = r.read()
    return json.loads(body) if body else None


def main():
    assert len(sys.argv) in (2, 3), __doc__
    tool, arg = sys.argv[1], sys.argv[2] if len(sys.argv) == 3 else "{}"
    text = sys.stdin.read() if arg == "-" else Path(arg[1:]).read_text() if arg.startswith("@") else arg
    args = json.loads(text)
    assert ENDPOINT.is_file(), f"没有 {ENDPOINT}: 程序没开过 (python run.py)"
    url = json.loads(ENDPOINT.read_text())["url"]

    init = {"protocolVersion": "2025-03-26", "capabilities": {}, "clientInfo": {"name": "py/agent.py", "version": "1"}}
    assert "result" in post(url, {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": init})
    post(url, {"jsonrpc": "2.0", "method": "notifications/initialized"})
    res = post(url, {"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"name": tool, "arguments": args}})
    if "error" in res:
        print(res["error"]["message"])
        sys.exit(2)
    print(res["result"]["content"][0]["text"])
    sys.exit(1 if res["result"]["isError"] else 0)


if __name__ == "__main__":
    main()
