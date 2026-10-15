"""
微盘 400: 市值最小 400 只, 逐日调仓 → config/universe/micro.json (母集 / filters / PIT 见 common/pipeline.py, 键见 common/keys.py).
行业白名单与 qmt strat_0 同: 不要 农林牧渔 / 房地产 / 环保 / 钢铁 / 银行.
"""
from common import DAILY, LOW, filters, keys

# ============================================================================ 配置
OUTPUT_NAME = "micro"
N = 400
FREQ = DAILY
KEY, SIDE = keys.SIZE, LOW
INDUSTRY_L1_WL = filters.SW2021_L1_ALL - {"农林牧渔", "房地产", "环保", "钢铁", "银行"}
# ============================================================================
