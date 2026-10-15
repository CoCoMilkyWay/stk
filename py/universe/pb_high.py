#!/usr/bin/env python3
"""
高 PB 400: PB(MRQ) 最高 400 只, 周频调仓 → config/universe/pb_high.json (母集 / filters / PIT 见 common/pipeline.py, 键见 common/keys.py).
行业不限.
"""
import sys

from common import HIGH, WEEKLY, filters, keys, run

# ============================================================================ 配置
OUTPUT_NAME = "pb_high"
N = 400
FREQ = WEEKLY
KEY, SIDE = keys.PB, HIGH
INDUSTRY_L1_WL = filters.SW2021_L1_ALL
# ============================================================================

if __name__ == "__main__":
    sys.exit(run(OUTPUT_NAME, N, FREQ, KEY, SIDE, INDUSTRY_L1_WL))
