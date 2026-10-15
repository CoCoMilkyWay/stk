"""
低 PE 400: PE(TTM) 最低 400 只, 周频调仓 → config/universe/pe_low.json (母集 / filters / PIT 见 common/pipeline.py, 键见 common/keys.py).
行业不限.
"""
from common import LOW, WEEKLY, filters, keys

# ============================================================================ 配置
OUTPUT_NAME = "pe_low"
N = 400
FREQ = WEEKLY
KEY, SIDE = keys.PE, LOW
INDUSTRY_L1_WL = filters.SW2021_L1_ALL
# ============================================================================
