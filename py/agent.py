"""挖因子的命令行入口: python py/agent.py [<工具> [参数]]

不带参数 = 打印入口说明 entry/agent_cli.md (外部 agent 照它走; 用法与例子全在那里)
带工具 = 一个 JSON-RPC tools/call (系统的 MCP 接口, 地址读 output/run/agent/remote.json) → 打印结果原文; 退出码 1 = 工具报错
"""

import json
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ENDPOINT = ROOT / "output" / "run" / "agent" / "remote.json"
ENTRY = ROOT / "entry" / "agent_cli.md"
TIMEOUT = 360  # 秒: 服务端 run 执行中至多等 300 秒即交回
OPENER = urllib.request.build_opener(
    urllib.request.ProxyHandler({})
)  # 本机地址不走代理


def rpc(url, method, params):
    body = json.dumps(
        {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    ).encode()
    req = urllib.request.Request(
        url,
        body,
        {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        },
    )
    with OPENER.open(req, timeout=TIMEOUT) as r:
        res = json.loads(r.read())
    assert "error" not in res, res.get("error")
    return res["result"]


def main():
    assert len(sys.argv) in (
        1,
        2,
        3,
    ), f"用法见 {ENTRY} (python py/agent.py 不带参数即打印)"
    if len(sys.argv) == 1:
        print(ENTRY.read_text())
        return
    assert ENDPOINT.is_file(), "系统没开着: 请研究员开 (python run.py)"
    url = json.loads(ENDPOINT.read_text())["url"]
    tool, arg = sys.argv[1], sys.argv[2] if len(sys.argv) == 3 else "{}"
    text = (
        sys.stdin.read()
        if arg == "-"
        else Path(arg[1:]).read_text() if arg.startswith("@") else arg
    )
    res = rpc(url, "tools/call", {"name": tool, "arguments": json.loads(text)})
    print("".join(c["text"] for c in res["content"]))
    sys.exit(1 if res["isError"] else 0)


if __name__ == "__main__":
    main()
