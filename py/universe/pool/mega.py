"""
巨盘 400: 市值最大 400 只, 逐日调仓 → config/universe/mega.json (母集 / filters / PIT 见 common/pipeline.py, 键见 common/keys.py).
行业不限.
"""
from common import DAILY, HIGH, filters, keys

# ============================================================================ 配置
OUTPUT_NAME = "mega"
N = 400
FREQ = DAILY
KEY, SIDE = keys.SIZE, HIGH
INDUSTRY_L1_WL = filters.SW2021_L1_ALL
# ============================================================================
