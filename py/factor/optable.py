"""
解析 cpp/include/factor/OpTable.hpp (真相表) → python 注册表.
仿 CMake 扫 NODE_ 的做法: 按 #define OP_<GROUP>(X) 块切分, 块内逐行抓 X(Name, "params", "描述").
"""

import re
from dataclasses import dataclass
from pathlib import Path

HEADER = Path(__file__).resolve().parents[2] / "cpp/include/factor/OpTable.hpp"

# 组 → (轴, 元数, python 模块名)
GROUPS = {
    "ELEM1": ("ELEM", 1, "elem"),
    "ELEM2": ("ELEM", 2, "elem"),
    "ELEM3": ("ELEM", 3, "elem"),
    "CUM1": ("CUM", 1, "cum"),
    "CUM2": ("CUM", 2, "cum"),
    "ROLL1": ("ROLL", 1, "roll"),
    "ROLL2": ("ROLL", 2, "roll"),
    "CS1": ("CS", 1, "cs"),
    "CS2": ("CS", 2, "cs"),
    "CS3": ("CS", 3, "cs"),
}

_DEFINE = re.compile(r"^#define OP_(\w+)\(X\)")
_ROW = re.compile(r'X\((\w+),\s*"([^"]*)",\s*"((?:[^"\\]|\\.)*)"\)')


@dataclass(frozen=True)
class Op:
    name: str
    group: str  # ELEM1 …
    axis: str  # ELEM / CUM / ROLL / CS
    arity: int
    params: tuple  # ("d",) / ("k", "k2") / ()
    desc: str

    @property
    def module(self):
        return GROUPS[self.group][2]


def load(path=HEADER):
    ops = []
    group = None
    for line in path.read_text(encoding="utf-8").splitlines():
        m = _DEFINE.match(line)
        if m:
            group = m.group(1) if m.group(1) in GROUPS else None
            continue
        if group is None:
            continue
        for name, params, desc in _ROW.findall(line):
            axis, arity, _ = GROUPS[group]
            ps = tuple(p for p in params.split(",") if p)
            ops.append(Op(name, group, axis, arity, ps, desc))
        if not line.rstrip().endswith("\\"):
            group = None  # 宏块结束
    assert ops, f"OpTable 为空: {path}"
    names = [o.name for o in ops]
    assert len(names) == len(set(names)), "OpTable 重名"
    return ops


if __name__ == "__main__":
    for o in load():
        print(f"{o.group:6} {o.name:14} {','.join(o.params):5} {o.desc}")
