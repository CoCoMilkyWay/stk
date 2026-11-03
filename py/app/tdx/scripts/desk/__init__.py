# desk — 控制台后端各块：推送源与后台事务（feed）、界面锁与点按钮（ui）、新浪行情与 K 线（sina）、盘口（book）、
# 持仓页帧（board）、持仓推送（hub）、K 线缓存（kline）、下单（order）、信用账户（credit）、一键调仓（rebal）、
# 录制器（rec）。
# 统一入口 console.py 只管路由与合路推送。

import csv
import os

ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)  # data/ logs/ 所在


def save_csv(path, head, rows):
    """整写：先写临时文件再改名，中途断掉不留半个文件。"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path + ".tmp", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(head)
        w.writerows(rows)
    os.replace(path + ".tmp", path)


def load_csv(path, head):
    """表头须为 head → 各行（字符串列表）；没有文件回 None。"""
    if not os.path.exists(path):
        return None
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.reader(f)
        assert next(r) == list(head), f"{path} 表头不对"
        return list(r)
