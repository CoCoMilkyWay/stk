#!/usr/bin/env python3
# tensor.py — 篮子 (指数 / 行业 / 概念) 日线 → CTA 张量.
# 源 = output/features/all/bar/baskets.bin (Database::BasketHistory 的账本; 布局 = BasketHistory.cpp Book::snap, 根 = Kline.hpp Candle / Bars).
# 产出 cta/data/1d/ (分钟张量同一布局, 另一目录):
#   bars.npy    float32 [T][B][F], F = open high low close amount ref (ref = 前日点位); 该篮子该日无根 = NaN
#   time.npy    int64 [T], YYYYMMDD (分钟 = YYYYMMDDHHMM)
#   assets.tsv  B 行, 与 B 轴同序: code \t name \t set \t weight
#               set = 测试集, 同 shared/Basket.hpp basket_group: idx = 全A (两权) + 各子池 (等权) / ind = 申万一级 (加权) / concept = 东财题材 (加权) / - = 不进
# 篮子线序 = shared/Basket.hpp: 线 = 切片 × 权重 (等权, 加权); 切片 = 全A, 申万一级 1..31, 子池 (all Bar L_1day 的 Pool 类 pool_* 列, 字段表序), 概念 (最近一份快照).
# 概念快照的指纹 (板块代码 + 成分全局 id 的 FNV, 同 Concept.cpp) 与账本头逐位核对: 对不上 = 账本不是这份概念表合成的.

import json
import os
import struct

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
SRC = os.path.join(ROOT, "output", "features", "all", "bar")
OUT = os.path.join(HERE, "data", "1d")

MAGIC = 0x314B534142544B53  # "SKTBASK1"
HEAD = struct.Struct(
    "<QQQIIII"
)  # magic, 库 A 轴 hash, 概念表指纹, A, 篮子数, sizeof(Candle), sizeof(Bars)
CANDLE = np.dtype(
    [
        ("o", "<f4"),
        ("h", "<f4"),
        ("l", "<f4"),
        ("c", "<f4"),
        ("v", "<f8"),
        ("a", "<f8"),
        ("date", "<i4"),
        ("ref", "<f4"),
    ]
)

# features/Method/Fundamental.hpp SW2021_L1_NAMES
SW = [
    "未知",
    "交通运输",
    "传媒",
    "公用事业",
    "农林牧渔",
    "医药生物",
    "商贸零售",
    "国防军工",
    "基础化工",
    "家用电器",
    "建筑材料",
    "建筑装饰",
    "房地产",
    "有色金属",
    "机械设备",
    "汽车",
    "煤炭",
    "环保",
    "电力设备",
    "电子",
    "石油石化",
    "社会服务",
    "纺织服饰",
    "综合",
    "美容护理",
    "计算机",
    "轻工制造",
    "通信",
    "钢铁",
    "银行",
    "非银金融",
    "食品饮料",
]

FNV_OFFSET, FNV_PRIME, M64 = 1469598103934665603, 1099511628211, (1 << 64) - 1


def fnv_append(h, s):
    for c in s.encode():
        h = ((h ^ c) * FNV_PRIME) & M64
    return (h * FNV_PRIME) & M64


def book():
    """账本 → (概念表指纹, [篮子] 日根数组 CANDLE)"""
    with open(os.path.join(SRC, "baskets.bin"), "rb") as f:
        b = f.read()
    magic, _, chash, _, nb, candle, bars = HEAD.unpack_from(b, 0)
    assert magic == MAGIC, "baskets.bin: magic 不符"
    assert (
        candle == CANDLE.itemsize
    ), f"baskets.bin: Candle {candle} 字节, 解析按 {CANDLE.itemsize}"
    o = HEAD.size

    def count():
        nonlocal o
        (n,) = struct.unpack_from("<Q", b, o)
        o += 8
        return n

    n = count()  # 已合成日
    o += 4 * n
    n = count()  # 末日点位
    o += 8 * n
    assert count() == nb, "baskets.bin: 篮子数与头不符"
    daily = []
    for _ in range(nb):
        o += 4  # Kline::last
        n = count()
        daily.append(np.frombuffer(b, CANDLE, n, o))
        o += n * candle
        n = count()
        o += n * bars
    assert o == len(b), "baskets.bin: 文件尾有多余字节"
    return chash, daily


def pools():
    """子池 (池表 [1:]) → [(池名, 中文名)]"""
    with open(os.path.join(SRC, "features.json"), encoding="utf-8") as f:
        day = sorted(json.load(f)["L_1day"], key=lambda x: x["col"])
    return [
        (x["code"][len("pool_") :], x["name_cn"])
        for x in day
        if x["type"] == "CS" and x["cat_l1"] == "Pool" and x["code"].startswith("pool_")
    ]


def concepts():
    """最近一份概念快照 → ([(板块代码, 名称)], 指纹)"""
    d = os.path.join(ROOT, "output", "concept")
    last = max(f for f in os.listdir(d) if f.endswith(".tsv"))
    with open(
        os.path.join(ROOT, "output", "fundamental", "asset_axis.json"), encoding="utf-8"
    ) as f:
        ids = {c: g for g, c in enumerate(json.load(f)["assets"])}
    h, out = FNV_OFFSET, []
    with open(os.path.join(d, last), encoding="utf-8") as f:
        for line in f:
            code, name, syms = line.rstrip("\n").split("\t")
            out.append((code, name))
            h = fnv_append(h, code)
            for s in syms.split(","):
                if s in ids:
                    h = fnv_append(h, str(ids[s]))
    return out, h


def stale():
    """张量缺或旧于账本"""
    t = os.path.join(OUT, "bars.npy")
    return not os.path.exists(t) or os.path.getmtime(t) < os.path.getmtime(
        os.path.join(SRC, "baskets.bin")
    )


def build():
    chash, daily = book()
    cl, h = concepts()
    assert (
        h == chash
    ), "概念快照与 baskets.bin 不是同一份: 先在 GUI 里让 BasketHistory 按最近快照重合成"
    # 切片: (代码, 中文名, 种类)
    cuts = (
        [("all", "全A", "all")]
        + [(f"sw{k:02d}", SW[k], "ind") for k in range(1, len(SW))]
        + [(n, cn, "pool") for n, cn in pools()]
        + [(c, n, "concept") for c, n in cl]
    )
    assert 2 * len(cuts) == len(
        daily
    ), f"切片 {len(cuts)} × 2 ≠ 篮子 {len(daily)}: 池表与账本不符"

    time = np.unique(np.concatenate([d["date"] for d in daily]))
    T, B = len(time), len(daily)
    x = np.full((T, B, 6), np.nan, np.float32)
    for b, d in enumerate(daily):
        i = np.searchsorted(time, d["date"])
        x[i, b] = np.stack([d["o"], d["h"], d["l"], d["c"], d["a"], d["ref"]], axis=1)

    rows = []
    for code, name, kind in cuts:
        for weight, sfx in (("eq", "等"), ("cap", "加")):
            s = (
                "idx"
                if kind == "all" or (kind == "pool" and weight == "eq")
                else kind if kind in ("ind", "concept") and weight == "cap" else "-"
            )
            rows.append(f"{code}.{weight}\t{name}.{sfx}\t{s}\t{weight}\n")

    os.makedirs(OUT, exist_ok=True)
    np.save(os.path.join(OUT, "bars.npy"), x)
    np.save(os.path.join(OUT, "time.npy"), time.astype(np.int64))
    with open(os.path.join(OUT, "assets.tsv"), "w", encoding="utf-8") as f:
        f.writelines(rows)
    sets = {
        s: sum(r.split("\t")[2] == s for r in rows) for s in ("idx", "ind", "concept")
    }
    print(f"[tensor] {OUT}: T = {T} ({time[0]} … {time[-1]}), B = {B}, 测试集 {sets}")


if __name__ == "__main__":
    build()
