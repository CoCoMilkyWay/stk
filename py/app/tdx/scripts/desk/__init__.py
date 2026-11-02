# desk — 控制台后端各块：推送源（feed）、界面锁与点按钮（ui）、新浪行情与 K 线（sina）、盘口（book）、
# 持仓页帧（board）、持仓推送（hub）、K 线缓存（kline）、下单（order）、录制器（rec）。
# 统一入口 console.py 只管路由与合路推送。

import os

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))  # data/ logs/ 所在
