#!/usr/bin/env python3
"""
跑 pool/ 下的池子 (每个文件一份配置) → config/universe/<OUTPUT_NAME>.json.
截面 / filters / 行业 / 排序键网格经 Ctx.memo 在池子间共享, 只算一次.

    python py/universe/generate.py              # 全部池子
    python py/universe/generate.py micro mega   # 只跑指定的 (pool/ 下文件名)
"""
import importlib
import sys
from pathlib import Path

from common import load_ctx, run

POOL_DIR = Path(__file__).resolve().parent / "pool"

if __name__ == "__main__":
    have = sorted(p.stem for p in POOL_DIR.glob("*.py"))
    names = sys.argv[1:] or have
    assert set(names) <= set(have), f"pool/ 下没有: {sorted(set(names) - set(have))}"
    pools = [importlib.import_module(f"pool.{s}") for s in names]
    ctx = load_ctx()
    for m in pools:
        run(ctx, m.OUTPUT_NAME, m.N, m.FREQ, m.KEY, m.SIDE, m.INDUSTRY_L1_WL)
