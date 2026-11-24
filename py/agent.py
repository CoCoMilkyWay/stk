"""外部 agent 调项目研究工具的命令行: 最小的 MCP 客户端 (说明书见仓库 AGENT.md)

用法: python py/agent.py <工具> [参数]
  工具 = 接口工具 open / doc / run, 或研究工具名 (= 只有它一个调用的 run); list = 列全部工具
  参数 = JSON 对象 (缺省 {}) / @文件 (读该文件的 JSON) / - (读标准输入)
例:
  python py/agent.py open '{"remark": "用户交代的要求原话"}'
  python py/agent.py doc
  python py/agent.py run '{"calls": [{"tool": "list_tried", "args": {}}]}'
  python py/agent.py run

一次调用 = 一个 JSON-RPC tools/call (MCP Streamable HTTP, POST) → 打印结果文本. 退出码 0 = 做成 / 1 = 工具报错 (原文写原因).
地址读 output/run/agent/remote.json (程序一开即写, 退出即删); 别的 MCP 客户端直接连这个地址.
"""

import json
import sys
import urllib.request
from pathlib import Path

ENDPOINT = (
    Path(__file__).resolve().parent.parent / "output" / "run" / "agent" / "remote.json"
)
TIMEOUT = 60  # 秒: 服务端 run 至多等 25 秒即交回
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
    assert len(sys.argv) in (2, 3), __doc__
    tool, arg = sys.argv[1], sys.argv[2] if len(sys.argv) == 3 else "{}"
    assert ENDPOINT.is_file(), f"没有 {ENDPOINT}: 程序没开过 (python run.py)"
    url = json.loads(ENDPOINT.read_text())["url"]
    if tool == "list":
        for t in rpc(url, "tools/list", {})["tools"]:
            print(f'{t["name"]}: {t["description"].splitlines()[0]}')
        return
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
