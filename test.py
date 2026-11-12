"""对方名单 (仓库根 *.txt, 同花顺式: 序号 / 代码 / 名称 / 星级 / "截止…为：值") ↔ 我们的选池 (features/Panel/PoolSelect.cpp).

我们这边不读子池库, 直接解码 all.bar 的 L_1day 日文件 (FeatureDay.hpp + SparseCodec.hpp 格式), 按 PoolSelect.cpp 的规则复现:
  在市 = 收盘总市值 > 0 ∧ list_age ≥ 0 ∧ delist_age 缺 ∧ 次日不停牌 ∧ 申万一级已知 ∧ st_level / board 有效
  键 = kKeyDefs (bp = bs_eq / mc, ep = pl_npp_ttm / mc, dy = dy_ttm, size = mc), 有限键按 (键, 下标) 升序, 低端 / 高端取前 N
  过滤项 pbpos / pepos = 剔 bs_eq / pl_npp_ttm ≤ 0 或缺
用法: python3 test.py [YYYY/MM/DD]   (缺省 = all.bar 最新落盘日; 行 D = D+1 盘前态, 对方 "截止 D" 对应我们行 D)

已查明 (对方 2026-11-11 名单 ↔ 我们 2026/11/09 行; 同日比时交集逐只值相对差中位 ~0.03%, 隔两日 ~1-4%, 边界名次的进出多半是这个):
  域: 对方 = 非ST ∧ 含北交所 ∧ 含次新 (PB / PE 只取正值)
  PB: 对方分母 = 归母权益 − 其他权益工具 (永续债 / 优先股); bp_adj 复现后高 / 低 PB 交集都是 195/200. 其他权益工具 > 归母权益的 (600683 / 300157 / 688520) 对方 PB 为负被剔
  MC: 对方 = A+B 股市值 (不含 H 股: 中国移动 902 亿 vs 我们 22351 亿); 我们 total_shares 含 H 股
      size_ab (A+B 占比 × 我们市值) 交集 189 → 192; 余下的差 = 光模块股 11-10 / 11-11 两日涨 15%+ (日期差) + 华虹 A 股本表滞后
      cn_stock_capital 只记股本变动, 多年未变的 (中国人寿 / 招行 / 中石油 …) 无记录 → 用 float_shares 占比近似
  PE: 对方 TTM 的上年同期项用本期报告里追溯重述后的值 (= BigQuant ttm_shift 的 net_profit_to_parent_shareholders_ttm);
      我们 pl_npp_ttm 用上年首次披露值. 雪天盐业 (同一控制合并重述) / 航天电子 / 宏桥控股 (反向收购) 差 1.5-1.9 倍即此; ep_bq 复现后 197 / 195
      次新 (宇树 / 托伦斯 / 北交所新股) 我们 pl_npp_ttm 缺 (不满 4 季), ttm_shift 有
  DY: 对方 = 报告期 ∈ (最新定期报告期 − 1 年, 最新定期报告期] 的现金分红总额 / 当前总市值 (含预案; 回购专户不参与分配)
      Fund.cpp dy 同口径 (总额 = 每股 × 公告时总股本 ⇒ 送转 / 增发后自动摊薄), 交集 179/200
      余下 21 只对方偏高: 已公告未实施的中期 / 三季度预案 (宁沪高速 / 北京银行 / 美的 / 中国重汽 / 新华保险 …);
      cn_stock_dividend 实施后才入表 (publish_date 回填为预案公告日), 只影响最近几个月. 方盛制药 / 艾罗能源 原因不明
"""

import json, re, struct, sys
from collections import Counter
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent
LIB = ROOT / "output/features/all/bar"
FUND_HPP = ROOT / "cpp/include/features/Node/Bar/TS/Fund/Fund.hpp"

# (对方文件, 我们的键, 端, 对方值 ↔ 我们键 的换算); bp_adj = 分母扣其他权益工具 (试对方口径, 不是我们的池)
LISTS = [
    ("high PB.txt", "bp", "low", lambda k: 1 / k),
    ("high PB.txt", "bp_adj", "low", lambda k: 1 / k),
    ("low PB.txt", "bp", "high", lambda k: 1 / k),
    ("low PB.txt", "bp_adj", "high", lambda k: 1 / k),
    ("high PE.txt", "ep", "low", lambda k: 1 / k),
    ("high PE.txt", "ep_bq", "low", lambda k: 1 / k),
    ("low PE.txt", "ep", "high", lambda k: 1 / k),
    ("low PE.txt", "ep_bq", "high", lambda k: 1 / k),
    ("high DY.txt", "dy", "high", lambda k: k * 100),
    ("high DY.txt", "dy_ex_date", "high", lambda k: k * 100),
    ("high DY.txt", "dy_publish_date", "high", lambda k: k * 100),
    ("high MC.txt", "size", "high", lambda k: k),
    ("high MC.txt", "size_ab", "high", lambda k: k),
]
THEIR_DATE = "2026/11/11"  # 对方名单的截止日 (分红 12 个月窗的右端)


# ---------------------------------------------------------------- all.bar L_1day 解码
def l1day_cols():
    """L_1day 列序: TradeDay(2) + Fund + KlineDay(4) + Momentum(1) + Tradable(2) + CsFund(6) + LabelBar(9) + Meta(ts_valid, cs_valid)."""
    src = FUND_HPP.read_text(encoding="utf-8")
    fund = []
    for line in src[src.index("#define FIELDS_L_1day_Fund") :].splitlines()[1:]:
        s = line.strip()
        if not s:
            break
        if m := re.match(r"X\((\w+),", s):
            fund.append(m.group(1))
        elif m := re.match(r"FUND_ROW_BS\(X, CAT1, (\w+),", s):
            fund.append(m.group(1))
        elif m := re.match(r"FUND_ROWS3\(X, CAT1, (\w+),", s):
            fund += [m.group(1) + "_q", m.group(1) + "_ttm", m.group(1) + "_lyr"]
    return (
        ["volume_day", "amount_day"]
        + fund
        + ["open_day", "high_day", "low_day", "close_day"]
    )


def load_day(date):
    lib = json.loads((LIB / "lib.json").read_text())
    codes = lib["codes"]
    A = len(codes)
    raw = (LIB / date / "features_L_1day.bin").read_bytes()
    magic, T, F, A_, _, _ = struct.unpack_from("<6Q", raw, 0)
    cols = l1day_cols()
    assert magic == 0x0000325941445446 and A_ == A and T == 2, (hex(magic), T, A_, A)
    assert F == len(cols) + 1 + 2 + 6 + 9 + 2, ("字段表变了, 改 l1day_cols", F)
    idx = {c: i for i, c in enumerate(cols)} | {"ts_valid": F - 2, "cs_valid": F - 1}
    dir_ = [struct.unpack_from("<QII", raw, 48 + 16 * f) for f in range(F)]

    def col(name):
        off, size, delta = dir_[idx[name]]
        words, nblk = T * A, (T * A + 63) // 64
        bits = np.unpackbits(
            np.frombuffer(raw, "<u8", nblk, off).view(np.uint8), bitorder="little"
        )[:words].astype(bool)
        nz = int(bits.sum())
        assert nblk * 8 + (nz + 1) * 2 == size
        out = np.zeros(words, np.uint16)
        out[bits] = np.frombuffer(raw, "<u2", nz, off + nblk * 8)
        if delta:
            for t in range(1, T):
                out[t * A : (t + 1) * A] ^= out[(t - 1) * A : t * A]
        return out[:A].view(np.float16).astype(np.float32)

    return codes, col


# ---------------------------------------------------------------- 对方名单
def theirs(name):
    L = [
        l.strip()
        for l in (ROOT / name).read_text(encoding="utf-8").splitlines()
        if l.strip()
    ]
    out, date = {}, None
    for i, l in enumerate(L):
        if l.startswith("截止"):
            date = re.search(r"(\d{4}-\d\d-\d\d)", l).group(1)
            out[L[i - 3]] = (
                L[i - 2],
                float(re.search(r"为[:：]\s*(-?[\d.]+)", l).group(1)),
            )
    return out, date


FUND = ROOT / "output/data/fundamental"


def read_tables(name, cols, months=24):
    import pyarrow as pa, pyarrow.parquet as pq

    fs = sorted(FUND.glob(f"20*/{name}.parquet"))[-months:]
    return pa.concat_tables([pq.read_table(f, columns=cols) for f in fs]).to_pandas()


def iso(date):
    return date.replace("/", "-")


def ttm_npp(date):
    """BigQuant 现成的归母净利 TTM (ttm_shift; 同期项用最新一次披露 = 追溯重述后的值), 亿元: 每资产可得日 ≤ 行日的最新报告期."""
    t = read_tables("cn_stock_financial_ttm_shift", ["date", "instrument", "report_date", "net_profit_to_parent_shareholders_ttm"])
    t = t[t.date <= iso(date)].sort_values(["report_date", "date"]).groupby("instrument").tail(1)
    return dict(zip(t.instrument.str.split(".").str[0], t.net_profit_to_parent_shareholders_ttm.values * 1e-8))


def ab_frac(date):
    """cn_stock_capital: 变动日 / 公告日 ≤ 行日的最新一条 → (A 股总数 + B 股总数) / 总股本 (表里股数常滞后, 只取占比乘我们的市值)."""
    t = read_tables("cn_stock_capital", ["instrument", "publish_date", "change_date", "total_shares", "shares_a_total", "shares_b_total"], months=999)
    t = t[(t.change_date <= iso(date)) & (t.publish_date <= iso(date))].sort_values(["change_date", "publish_date"]).groupby("instrument").tail(1)
    f = (t.shares_a_total.fillna(0) + t.shares_b_total.fillna(0)) / t.total_shares
    return dict(zip(t.instrument.str.split(".").str[0], f.values))


def div_12m(end, by):
    """每股税前现金分红, by 列 (ex_date / publish_date) 落在 (end − 365 日, end] 的累计, 元/股."""
    import pandas as pd

    t = read_tables("cn_stock_dividend", ["instrument", "report_date", "publish_date", "ex_date", "cash_before_tax"], months=30)
    e = pd.Timestamp(iso(end))
    t = t[(t[by] > e - pd.Timedelta(days=365)) & (t[by] <= e)]
    s = t.groupby("instrument").cash_before_tax.sum()
    return dict(zip(s.index.str.split(".").str[0], s.values))


def other_equity(date):
    """BigQuant 资产负债表 PIT: 每资产 date (可得日) ≤ 行日的最新一期 → 其他权益工具 / 归母权益 (永续债 + 优先股占比)."""
    import pyarrow as pa, pyarrow.parquet as pq
    y, m, d = map(int, date.split('/'))
    cols = ['date', 'instrument', 'report_date', 'total_equity_to_parent_shareholders', 'other_equity_instruments']
    fs = sorted((ROOT / 'output/data/fundamental').glob('20*/cn_stock_financial_balance_general_pit.parquet'))
    t = pa.concat_tables([pq.read_table(f, columns=cols) for f in fs[-24:]]).to_pandas()
    t = t[t.date <= f'{y:04d}-{m:02d}-{d:02d}'].sort_values(['report_date', 'date']).groupby('instrument').tail(1)
    oei = t.other_equity_instruments.fillna(0)
    return dict(zip(t.instrument.str.split('.').str[0], (oei / t.total_equity_to_parent_shareholders).values))


# ---------------------------------------------------------------- 比对
def main():
    date = (
        sys.argv[1]
        if len(sys.argv) > 1
        else sorted(
            str(p.parent.relative_to(LIB))
            for p in LIB.glob("*/*/*/features_L_1day.bin")
        )[-1]
    )
    codes, col = load_day(date)
    c6 = [c.split(".")[0] for c in codes]
    pos = {c: a for a, c in enumerate(c6)}
    v = {
        c: col(c)
        for c in [
            "close_day",
            "total_shares",
            "ind_l1",
            "list_age",
            "delist_age",
            "flt_susp",
            "st_level",
            "board",
            "flt_new",
            "flt_warn",
            "bs_eq",
            "pl_npp_ttm",
            "dy_ttm",
            "float_shares",
        ]
    }
    with np.errstate(invalid="ignore", divide="ignore"):
        px, sh = v["close_day"], v["total_shares"]
        mc = np.where((px > 0) & (sh > 0), px * sh, 0)
        st, bd, ind = v["st_level"], v["board"], v["ind_l1"]
        inmkt = (
            (mc > 0)
            & (v["list_age"] >= 0)
            & ~(v["delist_age"] >= 0)
            & ~(v["flt_susp"] > 0.5)
            & (ind >= 1)
            & (ind < 32)
            & (st >= 0)
            & (st < 4)
            & (bd >= 1)
            & (bd < 6)
        )
        keys = {
            "bp": np.where(mc > 0, v["bs_eq"] / mc, np.nan),
            "ep": np.where(mc > 0, v["pl_npp_ttm"] / mc, np.nan),
            "dy": v["dy_ttm"],
            "size": np.where(mc > 0, mc, np.nan),
        }
        pos_bp, pos_ep = v["bs_eq"] > 0, v["pl_npp_ttm"] > 0
        oe = other_equity(date)
        keys["bp_adj"] = keys["bp"] * (1 - np.array([oe.get(c, 0.0) for c in c6], np.float32))
        npp = ttm_npp(date)
        keys["ep_bq"] = np.array([npp.get(c, np.nan) for c in c6], np.float32) / np.where(mc > 0, mc, np.nan)
        fr = ab_frac(date)
        fr = np.array([fr.get(c, np.nan) for c in c6], np.float32)
        fl = np.clip(v["float_shares"] / np.where(sh > 0, sh, np.nan), 0, 1)  # 股本表无记录 (股本多年未变) → A 股流通占比近似
        keys["size_ab"] = keys["size"] * np.where((fr > 0) & (fr <= 1), fr, np.where(fl > 0, fl, 1.0))
        for by in ["ex_date", "publish_date"]:
            dv = div_12m(THEIR_DATE, by)
            keys[f"dy_{by}"] = np.array([dv.get(c, 0.0) for c in c6], np.float32) / np.where(px > 0, px, np.nan)
    print(f"我们: all.bar 行 {date} (A = {len(codes)}, 在市 {int(inmkt.sum())})")

    def top(key, end, univ, n):
        ok = np.isfinite(key) & univ
        a = np.nonzero(ok)[0]
        o = np.lexsort((a, key[a])) if end == "low" else np.lexsort((-a, -key[a]))
        return list(a[o][:n])

    for name, style, end, conv in LISTS:
        if not (ROOT / name).exists():
            continue
        th, tdate = theirs(name)
        key = keys[style]
        n = len(th)
        bj = {c for c in th if pos.get(c) is not None and bd[pos[c]] == 5}
        sts = {c for c in th if pos.get(c) is not None and st[pos[c]] > 0}
        news = {c for c in th if pos.get(c) is not None and v["flt_new"][pos[c]] > 0.5}
        absent = [c for c in th if c not in pos]
        vals = [x[1] for x in th.values()]
        print(
            f"\n===== {name}: 对方 {n} 只, 截止 {tdate}, 值 {min(vals)} .. {max(vals)} | ↔ 我们 {style}.{end}"
        )
        print(
            f"  对方含: 北交所 {len(bj)}, ST 类 {len(sts)}, 次新 {len(news)}, 不在 all.bar 轴 {absent}"
        )

        # 同值比对: 交集逐只
        both = [
            c for c in th if c in pos and np.isfinite(key[pos[c]]) and key[pos[c]] != 0
        ]
        rel = np.array([conv(key[pos[c]]) / th[c][1] - 1 for c in both])
        print(
            f"  逐只值: {len(both)} 只可比, 相对差中位 {np.median(np.abs(rel)):.4f}, >5% 的 {int((np.abs(rel) > 0.05).sum())} 只"
        )
        for c, r in sorted(zip(both, rel), key=lambda x: -abs(x[1]))[:6]:
            print(
                f"      {c} {th[c][0]}: 对方 {th[c][1]} 我们 {conv(key[pos[c]]):.2f} ({r:+.1%})"
            )

        # 口径网格: 哪种域最贴
        need_pos = {"bp": pos_bp, "bp_adj": keys["bp_adj"] > 0, "ep": pos_ep, "ep_bq": keys["ep_bq"] > 0}.get(style, np.ones(len(codes), bool))
        best = None
        for st_lab, st_m in [("含ST", np.ones(len(codes), bool)), ("非ST", st == 0)]:
            for bj_lab, bj_m in [
                ("含北交所", np.ones(len(codes), bool)),
                ("不含北交所", bd <= 4),
            ]:
                for nw_lab, nw_m in [
                    ("含次新", np.ones(len(codes), bool)),
                    ("非次新", ~(v["flt_new"] > 0.5)),
                ]:
                    univ = inmkt & st_m & bj_m & nw_m & need_pos
                    got = {c6[a] for a in top(key, end, univ, n)}
                    hit = len(got & set(th))
                    lab = f"{st_lab}/{bj_lab}/{nw_lab}"
                    print(f"  域 [{lab:<14}] 前 {n}: 交集 {hit}")
                    if best is None or hit > best[0]:
                        best = (hit, lab, univ, got)
        hit, lab, univ, got = best
        print(f"  最贴 [{lab}] 交集 {hit} — 差异逐只:")
        sel = top(key, end, univ, n)
        cut = conv(key[sel[-1]])
        print(f"    我们第 {n} 名的值 = {cut:.2f}")
        for c in sorted(set(th) - got):
            a = pos.get(c)
            if a is None:
                why = "不在 all.bar 轴"
            elif not inmkt[a]:
                why = f'不在市 (mc={mc[a]:.1f} list_age={v["list_age"][a]:.0f} delist_age={v["delist_age"][a]} susp={v["flt_susp"][a]:.0f} ind={ind[a]:.0f})'
            elif not univ[a]:
                why = f'域外 (st={st[a]:.0f} board={bd[a]:.0f} new={v["flt_new"][a]:.0f} bs_eq={v["bs_eq"][a]} npp_ttm={v["pl_npp_ttm"][a]})'
            elif not np.isfinite(key[a]):
                why = "键缺"
            else:
                why = f"我们值 {conv(key[a]):.2f}, 排不进"
            print(f"    对方多 {c} {th[c][0]}: 对方 {th[c][1]} | {why}")
        for a in sel:
            if c6[a] not in th:
                print(
                    f"    我们多 {codes[a]}: 我们 {conv(key[a]):.2f} st={st[a]:.0f} board={bd[a]:.0f}"
                )


if __name__ == "__main__":
    main()
