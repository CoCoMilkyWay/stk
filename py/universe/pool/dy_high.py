"""
高股息 400: 股息率 (近 365 日) 最高 400 只, 逐日调仓 → config/universe/dy_high.json (母集 / filters / PIT 见 common/pipeline.py, 键见 common/keys.py).
行业不限.
"""
from common import DAILY, HIGH, filters, keys

# ============================================================================ 配置
OUTPUT_NAME = "dy_high"
N = 400
FREQ = DAILY
KEY, SIDE = keys.DY, HIGH
INDUSTRY_L1_WL = filters.SW2021_L1_ALL
# ============================================================================
