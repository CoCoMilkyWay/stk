"""外部 agent 调项目研究工具的命令行 (说明书见仓库 AGENT.md)

用法: python py/agent.py <工具> [参数]
  工具 = doc / scope / run / wait / cancel / unlock
  参数 = JSON 对象 (缺省 {}) / @文件 (读该文件的 JSON) / - (读标准输入)
例:
  python py/agent.py doc
  python py/agent.py run '{"calls": [{"tool": "list_tried", "args": {}}]}'
  python py/agent.py wait '{"job": 3}'

一次调用 = 一个 POST {"tool", "args"} → {"ok", "text"}; 打印 text. 退出码 0 = 做成 / 1 = 工具报错 (原文写原因).
地址读 output/run/agent/remote.json (程序一开即写).
"""

import json
import sys
import urllib.request
from pathlib import Path

ENDPOINT = Path(__file__).resolve().parent.parent / "output" / "run" / "agent" / "remote.json"
TIMEOUT = 90  # 秒: 服务端 run / wait 至多等 50 秒即交回
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))  # 本机地址不走代理


def main():
    assert len(sys.argv) in (2, 3), __doc__
    tool, arg = sys.argv[1], sys.argv[2] if len(sys.argv) == 3 else "{}"
    text = sys.stdin.read() if arg == "-" else Path(arg[1:]).read_text() if arg.startswith("@") else arg
    body = json.dumps({"tool": tool, "args": json.loads(text)}).encode()
    assert ENDPOINT.is_file(), f"没有 {ENDPOINT}: 程序没开过 (python run.py)"
    url = json.loads(ENDPOINT.read_text())["url"]
    req = urllib.request.Request(url, body, {"Content-Type": "application/json"})
    with OPENER.open(req, timeout=TIMEOUT) as r:  # 服务端一律 200, 成败在 ok
        res = json.loads(r.read())
    print(res["text"])
    sys.exit(0 if res["ok"] else 1)


if __name__ == "__main__":
    main()
