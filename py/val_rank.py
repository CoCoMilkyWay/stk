"""子池成分的 BP 估值 band 实时排名 (新浪现价)

数据 = 主程序的 all.bar 特征库 (output/features/all/bar; L_1day 列名与字段表指纹读 lib.json 的 levels)
  T0    = 新浪快照日 (上证指数); 源行 = T0 之前末个已落盘日 (L_1day 行 D = D+1 盘前态)
  成分  = 子池在源行的选股 (同 features/Panel/PoolSelect.cpp): 在市 ∧ 切片 → 键自端头取前 N (键覆盖率 < 0.8 → 断言)
  分位  = BP 近 5 年加权分位 (同 Panel/Fund.cpp build_hist 的 pb_pct5y; 高 = 贵): 窗 = (T0 − 5 自然年, T0] 的历史点 + T0 实时点,
          实时 BP = 源行归母普通股权益 MRQ / (现价 × 源行总股本); 历史点 D = 行 D 权益 / (行 D 收盘 × 行 D−1 总股本)
  曲度  = 源行 pb_lo_curv (5 年最低档 band 价格线近 3 年二次拟合的 2c, 年⁻² · 窗内均价倍数; < 0 = 底部拐头向下)
  分    = 分位 + λ · max(0, −曲度) · 100 (曲度 ≥ 0 不加分), 低者优先; 史不满 1 年 = 无分, 排末
  底档  = 窗内贵度 0.05 加权分位的 BP 对应的价格 (VAL 主图最下一条 band); 距底档 = 现价 / 底档价 − 1

用法: python py/val_rank.py [--pool 池名] [--lam 1] [--every 10]
"""

import argparse
import datetime
import json
import math
import time
import unicodedata
import urllib.request
from pathlib import Path

import numpy as np

LIB = Path(__file__).resolve().parent.parent / "output" / "features" / "all" / "bar"
LEVEL = "L_1day"
MAGIC = 0x0000325941445446  # FeatureFileHeader::kMagic
POOL = "size.low.100.day.eq.nost.sh_main+sz_main+star+chinext.margin.nonew+nowarn"  # 小盘100B
HIST_YEARS, READY_YEARS, HIST_K, HIST_LO = 5, 1, 10.0, 0.05  # Panel/Fund.cpp kHistYears / kHistReady / kHistK / kHistLo
COVER = 0.8  # PoolSelect.hpp kCover
IND_COUNT = 32  # fund::SW2021_L1_COUNT
SINA = "http://hq.sinajs.cn/list="
SINA_HEAD = {"Referer": "https://finance.sina.com.cn", "User-Agent": "Mozilla/5.0"}
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))  # 同主程序直连

# 切片维: 池名段 → all.bar 列值 (PoolSpec.hpp 取值表); 过滤项 → 剔法 (PoolSelect.cpp kFilterDrop)
ST = {"nost": 0, "st": 1, "sst": 2, "delist": 3}
BOARD = {"sh_main": 1, "sz_main": 2, "star": 3, "chinext": 4, "bj": 5}
MARGIN = {"nonmargin": 0, "margin": 1}
DROP = {
    "nonew": lambda c: c["flt_new"] > 0.5,
    "nowarn": lambda c: c["flt_warn"] > 0.5,
    "pbpos": lambda c: ~(c["bs_eq"] > 0),
    "pepos": lambda c: ~(c["pl_npp_ttm"] > 0),
    "divstab": lambda c: c["flt_div"] > 0.5,
}
LIVE_COLS = ["close_day", "total_shares", "list_age", "delist_age", "flt_susp", "ind_l1"]
HIST_COLS = LIVE_COLS + ["bs_eq"]
BASE_COLS = HIST_COLS + ["ab_shares", "st_level", "board", "is_margin", "flt_new", "flt_warn", "pl_npp_ttm", "flt_div", "pb_lo_curv", "pb_pct5y"]


def ymd_days(x):  # YYYYMMDD → 日历天 (日越界顺延, 同 std::chrono sys_days)
    return datetime.date(x // 10000, x // 100 % 100, 1).toordinal() + x % 100 - 1


def before(x, years):  # 自然年平移 (同 VAL years_before)
    return x - years * 10000


def decode(buf, T, A):  # SparseCodec 载荷 → 行 0 [A] (L_1day 末行是哨兵; 差分列首行即原值)
    words = T * A
    nblk = (words + 63) // 64
    bits = np.unpackbits(np.frombuffer(buf, np.uint8, nblk * 8), bitorder="little")[:words].astype(bool)
    nz = int(bits.sum())
    assert len(buf) == nblk * 8 + (nz + 1) * 2, "载荷与位图不自洽"
    w = np.zeros(words, np.uint16)
    w[bits] = np.frombuffer(buf, "<u2", nz, nblk * 8)
    return w[:A].view(np.float16).astype(np.float32)


class Lib:  # all.bar 的 L_1day: 清单 + 按列读一日
    def __init__(self):
        j = json.loads((LIB / "lib.json").read_text())
        assert "levels" in j, "lib.json 无 levels: 主程序续算 / 重算 all.bar 一次即写"
        lv = j["levels"][LEVEL]
        self.codes, self.fp = j["codes"], lv["fingerprint"]
        self.col = {c: i for i, c in enumerate(lv["cols"])}
        self.F, self.A = len(lv["cols"]), len(self.codes)
        self.dates = [d for d in j["dates"] if self.path(d).is_file()]
        assert self.dates, "all.bar 未落盘"

    def path(self, d):
        return LIB / f"{d // 10000}" / f"{d // 100 % 100:02d}" / f"{d % 100:02d}" / f"features_{LEVEL}.bin"

    def read(self, d, cols):
        out = {}
        with open(self.path(d), "rb") as f:
            magic, T, F, A, _, fp = (int(x) for x in np.frombuffer(f.read(48), "<u8"))
            assert magic == MAGIC and F == self.F and A == self.A and fp == self.fp, f"{d}: 日文件与 lib.json 不符"
            span = np.frombuffer(f.read(F * 16), [("off", "<u8"), ("size", "<u4"), ("delta", "<u4")])
            for c in cols:
                s = span[self.col[c]]
                f.seek(int(s["off"]))
                out[c] = decode(f.read(int(s["size"])), T, A)
        return out


def live(c):  # 活跃 (Fund live: 已上市 ∧ 未退市 ∧ 次日不停牌 ∧ 行业已知)
    ind = c["ind_l1"]
    return (c["list_age"] >= 0) & ~(c["delist_age"] >= 0) & ~(c["flt_susp"] > 0.5) & (ind >= 1) & (ind < IND_COUNT)


def members(c, pool):  # 源行的选股 → 资产下标 (端头起)
    style, end, n, rebal, _, st, board, margin, flt = pool.split(".")
    assert style == "size" and rebal == "day", "只支持 size 风格 · 日调仓的子池"
    st_x, bd_x = c["st_level"], c["board"]
    m = live(c) & (c["close_day"] * c["total_shares"] > 0) & (st_x >= 0) & (st_x < len(ST)) & (bd_x >= 1) & (bd_x < len(BOARD) + 1)
    for seg, table, x in ((st, ST, np.floor(st_x)), (board, BOARD, np.floor(bd_x)), (margin, MARGIN, (c["is_margin"] > 0.5).astype(np.float32))):
        if seg != "all":
            m &= np.isin(x, [table[v] for v in seg.split("+")])
    if flt != "all":
        for v in flt.split("+"):
            m &= ~DROP[v](c)
    ab, px = c["ab_shares"], c["close_day"]
    key = np.where((ab > 0) & (px > 0), ab * px, np.float32(np.nan))
    idx = np.flatnonzero(m & np.isfinite(key))
    assert idx.size >= COVER * m.sum(), "键覆盖率不足, 源行不建仓"
    seq = idx[np.argsort(key[idx], kind="stable")]
    return seq[: int(n)] if end == "low" else seq[::-1][: int(n)]


class History:  # 成分的 BP 史: 行 = 落盘日 ∈ (T0 − 5 年 − 31 天, T0), 列 = 成分; NaN = 不可算
    def __init__(self, lib, t0, mem):
        lo = ymd_days(before(t0, HIST_YEARS)) - 31
        days = [d for d in lib.dates if ymd_days(d) > lo and d < t0]
        c = {k: np.empty((len(days), len(mem)), np.float32) for k in HIST_COLS}
        for r, d in enumerate(days):
            x = lib.read(d, HIST_COLS)
            for k in HIST_COLS:
                c[k][r] = x[k][mem]
        sh = np.vstack([c["total_shares"][:1], c["total_shares"][:-1]])  # 收盘口股本 = 前一行 (T ≤ D) 的总股本
        mc = c["close_day"].astype(np.float64) * sh
        with np.errstate(divide="ignore", invalid="ignore"):
            self.bp = np.where(live(c) & (mc > 0), c["bs_eq"] / mc, np.nan)
        self.ymd = np.array(days)
        self.dd = np.array([ymd_days(d) for d in days])

    def val(self, j, t0, now):  # 成分 j 在 T0 实时 BP = now 下 → (分位 %, 底档 BP); 不可算 = NaN
        bp = self.bp[:, j]
        ok = np.isfinite(bp)
        if not math.isfinite(now) or not ok.any() or self.ymd[ok][0] > before(t0, READY_YEARS):
            return math.nan, math.nan
        lo, d0 = before(t0, HIST_YEARS), ymd_days(t0)
        win = ok & (self.ymd > lo)
        v = np.append(bp[win], now)
        T = d0 - max(ymd_days(lo), ymd_days(int(self.ymd[ok][0])))
        assert T > 0
        w = 1.0 - np.exp(-HIST_K * (1.0 - np.append(d0 - self.dd[win], 0) / T))
        W = w.sum()
        order = np.argsort(-v, kind="stable")
        cw = np.cumsum(w[order])
        L = v[order[min(int(np.searchsorted(cw, HIST_LO * W)), v.size - 1)]]
        return 100.0 * w[v >= now].sum() / W, L


def sina(syms):  # 新浪快照 → {sym: (名称, 现价, 昨收, 日期)}; 现价 0 (未开盘 / 停牌) = 昨收
    req = urllib.request.Request(SINA + ",".join(syms), headers=SINA_HEAD)
    with OPENER.open(req, timeout=5) as r:
        text = r.read().decode("gbk")
    out = {}
    for line in text.splitlines():
        if "hq_str_" not in line:
            continue
        sym = line[line.find("hq_str_") + 7 : line.find("=")]
        f = line[line.find('"') + 1 : line.rfind('"')].split(",")
        assert len(f) > 31, f"sina: {sym} 无快照"
        out[sym] = (f[0], float(f[3]) or float(f[2]), float(f[2]), int(f[30].replace("-", "")))
    assert len(out) == len(syms), "sina: 快照只数不符"
    return out


def sym_of(code):  # 600000.SH → sh600000
    return code[-2:].lower() + code[:6]


def pad(s, w):  # 按显示宽度右对齐 (全角占 2)
    return " " * (w - sum(2 if unicodedata.east_asian_width(ch) in "WF" else 1 for ch in s)) + s


def num(x, fmt):
    return fmt.format(x) if math.isfinite(x) else "-"


def main():
    ap = argparse.ArgumentParser(description="子池成分的 BP 估值 band 实时排名 (新浪现价)")
    ap.add_argument("--pool", default=POOL, help="子池名 (output/config/beta/pools.json; 缺省 = 小盘100B)")
    ap.add_argument("--lam", type=float, default=1.0, help="曲度罚分系数: 分 = 分位 + lam · max(0, −曲度) · 100")
    ap.add_argument("--every", type=float, default=10.0, help="刷新间隔 (秒)")
    a = ap.parse_args()

    t0 = sina(["sh000001"])["sh000001"][3]
    lib = Lib()
    base = max(d for d in lib.dates if d < t0)
    c = lib.read(base, BASE_COLS)
    mem = members(c, a.pool)
    hist = History(lib, t0, mem)
    syms = [sym_of(lib.codes[i]) for i in mem]
    eq, sh = c["bs_eq"][mem].astype(np.float64), c["total_shares"][mem].astype(np.float64)
    curv, lib_pct = c["pb_lo_curv"][mem], c["pb_pct5y"][mem]

    heads = ["名次", "代码", "名称", "现价", "涨跌%", "分位", "昨分位", "曲度", "罚分", "分", "底档价", "距底档%"]
    widths = [4, 10, 10, 8, 7, 6, 6, 7, 6, 6, 8, 8]
    while True:
        q = sina(syms)
        assert max(x[3] for x in q.values()) == t0, "新浪快照日变了: 重开本脚本"
        rows = []
        for j, s in enumerate(syms):
            name, px, pre, _ = q[s]
            now = eq[j] / (px * sh[j]) if px > 0 and sh[j] > 0 else math.nan
            pct, L = hist.val(j, t0, now)
            pen = 100.0 * a.lam * max(0.0, -float(curv[j])) if math.isfinite(curv[j]) else 0.0
            band = px * now / L if L > 0 else math.nan
            rows.append((pct + pen, lib.codes[mem[j]], name, px, 100.0 * (px / pre - 1) if pre > 0 else math.nan, pct, float(lib_pct[j]), float(curv[j]), pen, band, 100.0 * (px / band - 1)))
        rows.sort(key=lambda r: (not math.isfinite(r[0]), r[0]))
        out = [
            f"{a.pool}  ·  {len(mem)} 只  ·  T0 {t0}  ·  源行 {base} (库末 {lib.dates[-1]})  ·  λ = {a.lam:g}  ·  {datetime.datetime.now():%H:%M:%S}",
            "".join(pad(h, w) for h, w in zip(heads, widths)),
        ]
        for k, (score, code, name, px, chg, pct, lp, cv, pen, band, gap) in enumerate(rows, 1):
            cells = [str(k), code, name, f"{px:.2f}", num(chg, "{:+.2f}"), num(pct, "{:.1f}"), num(lp, "{:.1f}"), num(cv, "{:+.3f}"), f"{pen:.1f}", num(score, "{:.1f}"), num(band, "{:.2f}"), num(gap, "{:+.1f}")]
            out.append("".join(pad(x, w) for x, w in zip(cells, widths)))
        print("\033[2J\033[H" + "\n".join(out), flush=True)
        time.sleep(a.every)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
