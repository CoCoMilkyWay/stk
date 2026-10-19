"""BigQuant DAI 数据下载脚本 (在 BigQuant AI Studio 上跑, 打包后 ssh 传回本地).

按 visible_date (因果安全的可见日) 主键, 在给定 [START_MONTH, END_MONTH] 月度区间内,
逐月逐表拉取, 主输出 parquet (高压缩), 可选切片为 JSON; 最后每表每年打一个 tar.

落盘结构 (脚本同目录下 ./data/):
    parquet (主):
        data/fundamental/_meta/<table>.parquet               # 静态/全量快照表
        data/fundamental/<yyyy>-<mm>/<table>.parquet         # 每月每表一个 parquet (= 本地 output/fundamental)
        data/fundamental/<yyyy>-<mm>/<local>/<yyyy-mm-dd>.parquet  # 分钟表: 每交易日一个 parquet (local = 本地表名)
    tar (PACK=True 时生成, 不再压缩; 成员路径相对 data/, mtime 原样保留):
        data/pack/<table>_<yyyy>.tar
        tar -xf <table>_<yyyy>.tar -C <repo>/output/  → 直接覆盖本地分片, 之后本地增量 sync (月度表 / 分钟表同)
        远端 pyarrow 的写者痕迹 (created_by / ARROW:schema) 由本地每轮开头的 renative 以本库重写抹掉
    json (可选, SPLIT_JSON=True 时同步生成):
        data/json/_meta/<table>.json                         # 静态表
        data/json/<yyyy>/<mm>/<dd>/<table>.json              # 按 visible_date 逐日切片

约定:
    - 与本地 sync 同口径 (cpp fundamental::select / Store::put): SQL = WHERE 窗口 + 分区 filters,
      结果取 arrow 原表不经 pandas; timestamp 一律 ns (本地读端按 ns 解码), 列类型限本地支持集.
    - parquet 用 zstd (level=3), pyarrow 引擎; 先写 .tmp 再改名 (中断不留半文件).
    - 月度表只拉已关月 (END_MONTH ≤ 上月): 落地即 Frozen, 开放月交给本地增量 sync.
    - JSON 用 records 数组, indent=2, 中文不转义;
      日期类列 -> YYYYMMDD 字符串, NaT/NaN -> null.

visible_date 主键的判定:
    - 大部分表的 `date` 列就是当日盘后可见 (T+0 收盘后, 或 PIT 公告日);
    - 财报派生 (PIT/ttm/notes) 的 `date` = 首次公告可见日;
    - 公告类事件 (dividend/allotment/capital/shareholder) 用 publish_date;
    - 名称变更用 end_date (该简称失效日, 之后才确知本段区间; 偏移 TBD);
    - basic_info / financial_changedate 为静态快照 (无 date 维度), 全量取.

配置见文件顶部 CONFIG 区块, 直接改变量, 不走命令行.
依赖 BigQuant AI Studio 运行时的 `dai` 模块 (与 bigtrader 同环境).
"""

import calendar
import glob
import json
import os
from collections import namedtuple

import tarfile

import pandas as pd  # pyright: ignore[reportMissingImports]
import pyarrow as pa  # pyright: ignore[reportMissingImports]
import pyarrow.compute as pc  # pyright: ignore[reportMissingImports]
import pyarrow.parquet as pq  # pyright: ignore[reportMissingImports]

import dai  # pyright: ignore[reportMissingImports]

# ============================================================================
# CONFIG  (直接改这里)
# ============================================================================
# 月度区间, 闭区间, 格式 'YYYY-MM' (整月对齐, 不支持精确到日)
#   START_MONTH = 本地 PIPELINE_START_DATE 所在月; END_MONTH 须已关月 (本地 lookback 7 天)
START_MONTH = "2015-01"
END_MONTH = "2026-09"

# 模式: "all" | "static" | "range"
MODE = "all"

# 表名子串过滤; None 表示不过滤
KEYWORD = None

# 是否同步把 parquet 数据按 visible_date 切到 yyyy/mm/dd/<table>.json
SPLIT_JSON = False

# parquet 压缩 (pyarrow 引擎)
PARQUET_COMPRESSION = "zstd"
PARQUET_COMPRESSION_LEVEL = 3  # 19 对分钟表 (单日 ~120 万行) 过慢

# 拉完后每表每年打一个 tar (data/pack/<table>_<yyyy>.tar); 成员都不比 tar 新的年份跳过
PACK = True

# ============================================================================
# 表清单 & visible_date 主键
# ============================================================================
# strategy:
#   "partition" : date 是分区列且 = visible_date, 用 filters={"date":[s,e]} 加速;
#   "where"     : visible_date 不是分区列, 用 SQL WHERE;
#   "static"    : 无 date 维度, 全量拉, 不接受时间区间.
#   "minute"    : 分钟表, date 为分钟时间戳; 按交易日逐日拉, 每日一个 parquet,
#                 已落盘日跳过 (= 增量). 只拉到昨天 (北京时间), 防止当日未入库落空文件.
# freq:
#   "day"         : 每月按 [月初, 月末] 全量拉 (默认);
#   "month_first" : 每月仅拉当月最早有记录的一天 (SQL MIN(date) 选), 用于月度快照
#                   (例: industry_component 月度快照 + industry_change 日频增量 cover).

# local: 本地表名 (= cpp SPECS 的 name); None = 同远端表名 name
Table = namedtuple("Table", "name visible_date strategy freq local", defaults=("day", None))


def _local(t):
    return t.local or t.name

TABLES = [
    # # ---- 静态快照 (优先下载, 无时间维度) ----
    # Table("cn_stock_basic_info", None, "static"),
    # # ---- 通用数据 (date = 当日) ----
    # Table("trading_days", "date", "partition"),
    # Table("holidays", "date", "partition"),
    # Table("cn_stock_instruments", "date", "partition"),
    # Table("cn_index_instruments", "date", "partition"),
    # Table("cn_fund_instruments", "date", "partition"),
    # Table("cn_future_instruments", "date", "partition"),
    # Table("cn_cbond_instruments", "date", "partition"),
    # # ---- 行业 ----
    # # industry_component: 月初快照 (低频), 月内变动靠 industry_change 增量 cover
    # Table("cn_stock_industry_component", "date", "partition", "month_first"),
    # Table("cn_stock_industry_change", "date", "partition"),
    # Table("cn_stock_industry_real_bar1d", "date", "partition"),
    # Table("cn_stock_industry_valuation", "date", "partition"),
    # # ---- 股票日度状态/行情 ----
    # Table("cn_stock_shares", "date", "partition"),
    # Table("cn_stock_status", "date", "partition"),
    # Table("cn_stock_suspend", "date", "partition"),
    Table("cn_stock_real_bar1d", "date", "partition"),
    # Table("cn_stock_limit_price", "date", "partition"),
    # Table("cn_stock_margin_trading_detail", "date", "partition"),
    # Table("cn_stock_margin_trading_market", "date", "partition"),
    # Table("cn_stock_dragon_list", "date", "partition"),
    # # ---- 事件型 (visible_date = publish_date / start_date) ----
    # Table("cn_stock_capital", "publish_date", "where"),
    # Table("cn_stock_dividend", "publish_date", "where"),
    # Table("cn_stock_allotment", "publish_date", "where"),
    # Table("cn_stock_shareholder", "publish_date", "where"),
    # Table("cn_stock_name_change", "end_date", "where"),
    # # ---- 财务 PIT (date = 首次公告可见日) ----
    # Table("cn_stock_financial_income_general_pit", "date", "partition"),
    # Table("cn_stock_financial_cashflow_general_pit", "date", "partition"),
    # Table("cn_stock_financial_balance_general_pit", "date", "partition"),
    Table("cn_stock_financial_ttm_shift", "date", "partition"),
    Table("cn_stock_financial_notes_shift", "date", "partition"),
    # # ---- 因子 (date = 当日) ----
    # Table("cn_stock_valuation", "date", "partition"),
    # ---- 分钟行情 (未复权 1m, date = 分钟时间戳) ----
    # 用截面表 _c (按月截面存储, 适合拉全市场); cn_stock_bar1m 按标的存储, 全市场单日要扫遍所有标的
    Table("cn_stock_bar1m_c", "date", "minute", local="bar1m"),
]


# ============================================================================
# 月度迭代工具
# ============================================================================
def _month_list(start_month, end_month):
    """['2024-01', '2024-02', ...] 闭区间"""
    y0, m0 = map(int, start_month.split("-"))
    y1, m1 = map(int, end_month.split("-"))
    assert (y0, m0) <= (y1, m1), f"START_MONTH > END_MONTH: {start_month} > {end_month}"
    out = []
    y, m = y0, m0
    while (y, m) <= (y1, m1):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m == 13:
            m = 1
            y += 1
    return out


def _month_bounds(ym):
    """'2024-01' -> ('2024-01-01', '2024-01-31')"""
    y, m = map(int, ym.split("-"))
    last = calendar.monthrange(y, m)[1]
    return f"{y:04d}-{m:02d}-01", f"{y:04d}-{m:02d}-{last:02d}"


# ============================================================================
# 数据拉取
# ============================================================================
def _normalize(name, tbl):
    """对齐本地落盘口径 (= Flight 返回类型):
    timestamp / date32 → timestamp[ns] (本地读端按 ns 解码); large_string → string (键列按字典读, 字典值须 string);
    其余类型须在本地支持集内."""
    fields = []
    for f in tbl.schema:
        t = f.type
        if pa.types.is_timestamp(t):
            assert t.tz is None, f"{name}.{f.name}: timestamp 带时区 {t.tz}"
            t = pa.timestamp("ns")
        elif pa.types.is_date32(t):
            t = pa.timestamp("ns")
        elif pa.types.is_large_string(t):
            t = pa.string()
        else:
            assert (
                pa.types.is_float64(t)
                or pa.types.is_float32(t)
                or pa.types.is_integer(t)
                or pa.types.is_boolean(t)
                or pa.types.is_string(t)
                or pa.types.is_null(t)
            ), f"{name}.{f.name}: 本地不支持的列类型 {t}"
        fields.append(pa.field(f.name, t))
    return tbl.cast(pa.schema(fields))


def _query(t, start=None, end=None):
    if t.strategy == "static":
        return _normalize(t.name, dai.query(f"SELECT * FROM {t.name}").arrow())
    col = t.visible_date
    rng = f"{col} >= '{start}' AND {col} <= '{end}'"
    if t.freq == "month_first":
        # 窗口内最早一天的全部行 (月初遇假期时往后顺延).
        # partition 表必须传 filters 做分区裁剪, dai 才放行.
        sql = (
            f"SELECT * FROM {t.name} "
            f"WHERE {rng} AND {col} IN ("
            f"SELECT MIN({col}) FROM {t.name} WHERE {rng}"
            f")"
        )
        return _normalize(
            t.name, dai.query(sql, filters={"date": [start, end]}).arrow()
        )
    sql = f"SELECT * FROM {t.name} WHERE {rng}"
    if t.strategy == "partition":
        # filters 只做分区裁剪, 窗口由 WHERE 精确卡住 (同本地 fundamental::select)
        return _normalize(
            t.name, dai.query(sql, filters={"date": [start, end]}).arrow()
        )
    if t.strategy == "where":
        return _normalize(t.name, dai.query(sql).arrow())
    assert False, f"unknown strategy: {t.strategy}"


def _trading_days(start, end):
    """[start, end] 内 A 股交易日, 'YYYY-MM-DD' 升序."""
    df = dai.query(
        f"SELECT date FROM all_trading_days WHERE market_code = 'CN' "
        f"AND date >= '{start}' AND date <= '{end}' ORDER BY date"
    ).df()
    return pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d").tolist()


def _query_minute_day(t, day):
    # date 是分钟时间戳: filters 只做分区裁剪 (放宽到次日), 当日边界由 WHERE 半开区间精确卡住.
    col = t.visible_date
    nxt = (pd.Timestamp(day) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    sql = f"SELECT * FROM {t.name} WHERE {col} >= '{day}' AND {col} < '{nxt}'"
    tbl = dai.query(sql, filters={"date": [day, nxt]}).arrow()
    # 截面表的存储分区列, 非数据 (select 而非 drop_columns: 远端 pyarrow 版本未知)
    tbl = tbl.select([c for c in tbl.column_names if c != "__PARTITION__"])
    tbl = _normalize(t.name, tbl)
    assert tbl.num_rows > 0, f"{t.name}: 交易日 {day} 返回 0 行"
    days = pc.unique(pc.strftime(tbl[col], format="%Y-%m-%d")).to_pylist()
    assert days == [day], f"{t.name}: {day} 含越界行 {days}"
    return tbl


# ============================================================================
# parquet 落盘
# ============================================================================
def _save_parquet(tbl, out_path):
    # 单 row group 同本地写盘; .tmp + rename: 中断不留半文件 (否则下次被当成已缓存)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    tmp = out_path + ".tmp"
    pq.write_table(
        tbl,
        tmp,
        compression=PARQUET_COMPRESSION,
        compression_level=PARQUET_COMPRESSION_LEVEL,
        row_group_size=max(tbl.num_rows, 1),
    )
    os.replace(tmp, out_path)
    return os.path.getsize(out_path)


# ============================================================================
# JSON 切片 (可选)
# ============================================================================
def _normalize_dates(df):
    """所有 datetime 列 -> YYYYMMDD 字符串; NaT -> None."""
    df = df.copy()
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            ts = df[col]
            df[col] = ts.dt.strftime("%Y%m%d").where(ts.notna(), None)
    return df


def _to_records(df):
    """df -> records list, NaN/NaT/None 一律 None."""
    obj = df.astype(object).where(df.notna(), None)
    return obj.to_dict(orient="records")


def _dump_json(records, out_path):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)


def _save_json_static(t, df, json_root):
    out_path = os.path.join(json_root, "_meta", f"{t.name}.json")
    df_n = _normalize_dates(df)
    _dump_json(_to_records(df_n), out_path)


def _save_json_day_split(t, df, json_root):
    if len(df) == 0:
        return 0
    col = t.visible_date
    key = pd.to_datetime(df[col]).dt.strftime("%Y%m%d")
    assert key.notna().all(), f"{t.name}: {col} 含 NaT, 无法切片"
    df_n = _normalize_dates(df)
    nfile = 0
    for ymd, sub in df_n.groupby(key, sort=True):
        yyyy, mm, dd = ymd[:4], ymd[4:6], ymd[6:8]
        out_path = os.path.join(json_root, yyyy, mm, dd, f"{t.name}.json")
        _dump_json(_to_records(sub), out_path)
        nfile += 1
    return nfile


# ============================================================================
# 主流程
# ============================================================================
def _select():
    assert MODE in ("all", "static", "range"), f"bad MODE: {MODE}"
    sel = TABLES
    if MODE == "static":
        sel = [t for t in sel if t.strategy == "static"]
    elif MODE == "range":
        sel = [t for t in sel if t.strategy != "static"]
    if KEYWORD:
        sel = [t for t in sel if KEYWORD in t.name]
    assert sel, "no table selected"
    return sel


def _members(data_root, t):
    """t 的全部已落盘 parquet, 按年分组 {yyyy | None (静态): [相对 data/ 的路径]}."""
    if t.strategy == "static":
        rels = [os.path.join("fundamental", "_meta", f"{t.name}.parquet")]
    else:
        # 不用 glob(root_dir=) (Python ≥ 3.10; 远端运行时偏旧)
        sub = (
            ("fundamental", "*", _local(t), "*.parquet")
            if t.strategy == "minute"
            else ("fundamental", "*", f"{t.name}.parquet")
        )
        rels = [
            os.path.relpath(p, data_root)
            for p in glob.glob(os.path.join(data_root, *sub))
        ]
    rels = [r for r in rels if os.path.exists(os.path.join(data_root, r))]
    out = {}
    for r in sorted(rels):
        # r = fundamental/<yyyy>-<mm>/...
        year = None if t.strategy == "static" else r.split(os.sep)[1][:4]
        out.setdefault(year, []).append(r)
    return out


def _pack(data_root, sel):
    """每表每年一个 tar (不压缩, 成员路径相对 data/, mtime 原样); 成员都不比 tar 新 → 跳过."""
    pack_root = os.path.join(data_root, "pack")
    os.makedirs(pack_root, exist_ok=True)
    print("\n== pack ==")
    for t in sel:
        for year, rels in _members(data_root, t).items():
            name = t.name if year is None else f"{t.name}_{year}"
            tar_path = os.path.join(pack_root, f"{name}.tar")
            newest = max(os.path.getmtime(os.path.join(data_root, r)) for r in rels)
            if os.path.exists(tar_path) and os.path.getmtime(tar_path) >= newest:
                print(f"  {name:<48} files={len(rels):>5}  cached")
                continue
            tmp = tar_path + ".tmp"
            with tarfile.open(tmp, "w") as tf:
                for r in rels:
                    tf.add(os.path.join(data_root, r), arcname=r)
            os.replace(tmp, tar_path)
            print(
                f"  {name:<48} files={len(rels):>5}  tar={os.path.getsize(tar_path)/2**20:>10.1f} MB"
            )


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    data_root = os.path.join(here, "data")
    fund_root = os.path.join(data_root, "fundamental")
    json_root = os.path.join(data_root, "json")

    sel = _select()
    static_tables = [t for t in sel if t.strategy == "static"]
    range_tables = [t for t in sel if t.strategy != "static"]
    months = _month_list(START_MONTH, END_MONTH)
    yesterday = (pd.Timestamp.now(tz="Asia/Shanghai") - pd.Timedelta(days=1)).strftime(
        "%Y-%m-%d"
    )

    print(
        f"BigQuant 数据下载  months=[{START_MONTH}..{END_MONTH}] ({len(months)} 个月)  "
        f"MODE={MODE}  SPLIT_JSON={SPLIT_JSON}\n"
        f"  static tables: {len(static_tables)}\n"
        f"  range tables : {len(range_tables)}\n"
    )

    # ---- 1. 静态表 (只拉一次) ----
    for i, t in enumerate(static_tables, 1):
        pq_path = os.path.join(fund_root, "_meta", f"{t.name}.parquet")
        cached = os.path.exists(pq_path)
        if cached:
            size = os.path.getsize(pq_path)
            tbl = pq.read_table(pq_path) if SPLIT_JSON else None
        else:
            tbl = _query(t)
            size = _save_parquet(tbl, pq_path)
        msg_json = ""
        if SPLIT_JSON:
            _save_json_static(t, tbl.to_pandas(), json_root)
            msg_json = "  +json"
        rows_str = "cached" if tbl is None else str(tbl.num_rows)
        print(
            f"[static {i}/{len(static_tables)}] {t.name:<42} "
            f"rows={rows_str:>9}  pq={size/1024:>9.1f} KB{msg_json}"
        )

    # ---- 2. 区间表: 月 × 表 ----
    for mi, ym in enumerate(months, 1):
        month_start, month_end = _month_bounds(ym)
        print(
            f"\n== [{mi}/{len(months)}] month {ym}  ({month_start} .. {month_end}) =="
        )
        days = None
        for ti, t in enumerate(range_tables, 1):
            assert t.freq in ("day", "month_first"), f"unknown freq: {t.freq}"
            if t.strategy == "minute":
                assert not SPLIT_JSON, "分钟表不支持 JSON 切片"
                last = min(month_end, yesterday)
                if month_start > last:
                    continue
                if days is None:
                    days = _trading_days(month_start, last)
                ncached = 0
                last_msg = ""
                head = f"  [{ti:>2}/{len(range_tables)}] {t.name:<42}"

                def _progress(done, status):
                    # \r 回行首 + \033[K 清行尾: 同一行原地刷新
                    print(
                        f"\r\033[K{head} days={done:>2}/{len(days)}  cached={ncached:>2}  {status}",
                        end="",
                        flush=True,
                    )

                _progress(0, "")
                for di, day in enumerate(days, 1):
                    pq_path = os.path.join(fund_root, ym, _local(t), f"{day}.parquet")
                    if os.path.exists(pq_path):
                        ncached += 1
                        _progress(di, last_msg)
                        continue
                    _progress(di - 1, f"fetching {day} ...  {last_msg}")
                    tbl = _query_minute_day(t, day)
                    size = _save_parquet(tbl, pq_path)
                    last_msg = (
                        f"last={day} rows={tbl.num_rows:>8} pq={size/2**20:>6.1f} MB"
                    )
                    _progress(di, last_msg)
                print()
                continue
            pq_path = os.path.join(fund_root, ym, f"{t.name}.parquet")
            cached = os.path.exists(pq_path)
            if cached:
                size = os.path.getsize(pq_path)
                tbl = pq.read_table(pq_path) if SPLIT_JSON else None
            else:
                tbl = _query(t, month_start, month_end)
                size = _save_parquet(tbl, pq_path)
            msg_json = ""
            if SPLIT_JSON:
                nfile = _save_json_day_split(t, tbl.to_pandas(), json_root)
                msg_json = f"  +json({nfile}d)"
            rows_str = "cached" if tbl is None else str(tbl.num_rows)
            print(
                f"  [{ti:>2}/{len(range_tables)}] {t.name:<42} "
                f"({t.strategy:<9} key={t.visible_date:<12} freq={t.freq:<11})  "
                f"rows={rows_str:>9}  pq={size/1024:>9.1f} KB{msg_json}"
            )

    # ---- 3. 打包: 每表每年一个 tar ----
    if PACK:
        _pack(data_root, sel)


if __name__ == "__main__":
    main()
