"""
高 PE 400: PE(TTM) 最高 400 只, 周频调仓 → config/universe/pe_high.json (母集 / filters / PIT 见 common/pipeline.py, 键见 common/keys.py).
行业不限.
"""
from common import HIGH, WEEKLY, filters, keys

# ============================================================================ 配置
OUTPUT_NAME = "pe_high"
N = 400
FREQ = WEEKLY
KEY, SIDE = keys.PE, HIGH
INDUSTRY_L1_WL = filters.SW2021_L1_ALL
# ============================================================================
