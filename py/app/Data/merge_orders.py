"""orders 迁移: orders/YYYY/MM/DD/<CODE>.<EX>.bin (+ .stat) → orders/YYYY/MM/YYYYMMDD.bin.

日文件布局与 cpp/src/shared/EncodeDayRecord.cpp 一致:
    [DayFileHeader 92B][zstd 帧 ...][EncodeDayIndexEntry 24B × n, 按 asset_id 升序]
帧原样搬 (旧 .bin 偏移 32 起就是那个帧), 不解压不重编.

每天: 写 YYYYMMDD.bin.tmp → fsync → 回读校验 → rename → fsync 目录 → 删旧日目录.
中断随时可以: 已 rename 的天下次只补删旧目录, 残留 .tmp 开跑时清掉.
坏的旧 .bin (头不自洽 / 截断 / 帧头对不上) 与轴外文件跳过并记日志, 那天记为不齐备
(complete=0), 交给增量编码补齐.

配置见 CONFIG 区块, 直接改变量.
"""

import json
import os
import shutil
import struct
import sys
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path

# ============================================================================
# CONFIG
# ============================================================================
REPO_ROOT = Path(__file__).resolve().parents[3]
CPP_RUN_DIR = REPO_ROOT / "cpp" / "projects" / \
    "main" / "build"  # config.json 里的相对路径以它为基准
CONFIG_PATH = REPO_ROOT / "config" / "config.json"
AXIS_PATH = REPO_ROOT / "output" / "fundamental" / "asset_axis.json"
LOG_PATH = REPO_ROOT / "output" / "log" / "merge_orders.log"
DAY_WORKERS = 8  # 同时在迁的天数; 每天峰值多占一天的盘 (~3GB)
FREE_MARGIN = 16 << 30  # 开一天之前盘上至少还剩 (当天体积 + 这么多)

# ============================================================================
# 盘上格式
# ============================================================================
CHECK_BITS = 13  # == L2::kCheckBitCount

# magic, version, raw_size, compressed_size, price_base, reserved
OLD_HEAD = struct.Struct("<IIQQII")
OLD_MAGIC = 0x004F324C
OLD_VERSION = 2

STAT_NAME = ".stat"
STAT_HEAD = struct.Struct(f"<IIIBBHII6I{CHECK_BITS}I")
# asset_id, order_count, orders_file_size (0 = 墓碑)
STAT_ENTRY = struct.Struct("<III")
STAT_MAGIC = 0x54415453
STAT_VERSION = 2

DAY_HEAD = struct.Struct(f"<10I{CHECK_BITS}I")
# offset, asset_id, order_count, compressed_size, price_base
DAY_ENTRY = struct.Struct("<QIIII")
DAY_MAGIC = 0x444F324C

ORDER_SIZE = 20
ZSTD_MAGIC = 0xFD2FB528
U32_MAX = 0xFFFFFFFF

assert OLD_HEAD.size == 32 and STAT_HEAD.size == 100 and DAY_HEAD.size == 92 and DAY_ENTRY.size == 24


def zstd_content_size(buf):
    """zstd 帧头里声明的解压后字节数; 不是帧 / 没写尺寸返回 None."""
    if len(buf) < 6 or struct.unpack_from("<I", buf, 0)[0] != ZSTD_MAGIC:
        return None
    fhd = buf[4]
    fcs_flag = fhd >> 6
    single_segment = (fhd >> 5) & 1
    pos = 5 + (0 if single_segment else 1) + (0, 1, 2, 4)[fhd & 3]
    fcs_size = (single_segment, 2, 4, 8)[fcs_flag]
    if fcs_size == 0 or len(buf) < pos + fcs_size:
        return None
    value = int.from_bytes(buf[pos: pos + fcs_size], "little")
    return value + 256 if fcs_size == 2 else value


# ============================================================================
# 日文件
# ============================================================================


def check_day_file(fd):
    """校验一个日文件 (头 + 目录 + 每帧帧头). 不自洽直接 assert. 返回 (头字段, 目录)."""
    size = os.fstat(fd).st_size
    assert size >= DAY_HEAD.size, "日文件短于头"
    head = DAY_HEAD.unpack(os.pread(fd, DAY_HEAD.size, 0))
    magic, header_size, entry_count, complete, total, ok, skipped, corrupt, invalid, failed = head[
        :10]
    assert magic == DAY_MAGIC and header_size == DAY_HEAD.size, "日文件头 magic / 头长不符"
    assert complete <= 1, "日文件头 complete 非 0/1"
    index_offset = size - entry_count * DAY_ENTRY.size
    assert index_offset >= DAY_HEAD.size, "日文件目录越界"
    raw = os.pread(fd, entry_count * DAY_ENTRY.size, index_offset)
    assert len(raw) == entry_count * DAY_ENTRY.size, "日文件目录读不全"
    index = [DAY_ENTRY.unpack_from(raw, i * DAY_ENTRY.size)
             for i in range(entry_count)]

    frames = 0
    prev_id = -1
    for offset, asset_id, order_count, compressed_size, price_base in index:
        assert asset_id > prev_id, "日文件目录未按 asset_id 严格升序"
        prev_id = asset_id
        if compressed_size == 0:
            assert offset == 0 and order_count == 0 and price_base == 0, "墓碑带了数据"
            continue
        assert order_count > 0, "帧的条数为 0"
        assert offset >= DAY_HEAD.size and offset + \
            compressed_size <= index_offset, "帧越出数据区"
        fcs = zstd_content_size(os.pread(fd, min(18, compressed_size), offset))
        assert fcs == 8 + order_count * ORDER_SIZE, "帧头声明的尺寸与目录条数不符"
        frames += 1
    assert ok == frames and skipped == entry_count - frames, "账目 ok / skipped 与目录不符"
    assert ok + skipped + corrupt + invalid + failed <= total, "账目处置分类之和超过分母"
    return head, index


def day_file_valid(path):
    fd = os.open(path, os.O_RDONLY)
    check_day_file(fd)
    os.close(fd)


# ============================================================================
# 旧格式
# ============================================================================


def read_old_bin(path):
    """旧 .bin → (order_count, compressed_size, price_base, 文件长); 坏文件返回 (None, 原因)."""
    fd = os.open(path, os.O_RDONLY)
    size = os.fstat(fd).st_size
    head = os.pread(fd, OLD_HEAD.size + 18, 0)
    os.close(fd)

    def bad(why):
        return None, why

    if len(head) < OLD_HEAD.size:
        return bad(f"短于 32 字节头 ({size}B)")
    magic, version, raw_size, compressed_size, price_base, _ = OLD_HEAD.unpack_from(
        head, 0)
    if magic != OLD_MAGIC or version != OLD_VERSION:
        return bad(f"magic/version 不符 ({magic:#x}/{version})")
    if raw_size < 8 or (raw_size - 8) % ORDER_SIZE != 0 or raw_size == 8:
        return bad(f"raw_size 不自洽 ({raw_size})")
    if compressed_size == 0 or size != OLD_HEAD.size + compressed_size:
        return bad(f"文件长 {size} != 32 + compressed_size {compressed_size} (截断?)")
    order_count = (raw_size - 8) // ORDER_SIZE
    if order_count > U32_MAX or compressed_size > U32_MAX:
        return bad("条数 / 帧长溢出 32 位")
    if zstd_content_size(head[OLD_HEAD.size:]) != raw_size:
        return bad("zstd 帧头与 raw_size 对不上")
    return (order_count, compressed_size, price_base, size), None


def read_old_stat(path):
    """旧 .stat → (accounted, complete, counts[6], checks, entries); 缺失 / 坏返回 None."""
    if not path.exists():
        return None
    data = path.read_bytes()
    if len(data) < STAT_HEAD.size:
        return None
    head = STAT_HEAD.unpack_from(data, 0)
    magic, version, entry_count, accounted, complete = head[:5]
    if magic != STAT_MAGIC or version != STAT_VERSION:
        return None
    if len(data) != STAT_HEAD.size + entry_count * STAT_ENTRY.size:
        return None
    counts = list(head[8:14])
    checks = list(head[14: 14 + CHECK_BITS])
    entries = [STAT_ENTRY.unpack_from(
        data, STAT_HEAD.size + i * STAT_ENTRY.size) for i in range(entry_count)]
    return accounted, complete, counts, checks, entries


# ============================================================================
# 迁一天
# ============================================================================


def fsync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    os.fsync(fd)
    os.close(fd)


def copy_range(src, dst, count, src_off, dst_off):
    while count > 0:
        n = os.copy_file_range(src, dst, count, src_off, dst_off)
        assert n > 0, "copy_file_range 提前返回 0 (源文件被截断?)"
        count -= n
        src_off += n
        dst_off += n


def merge_day(day_dir, target, axis_index):
    """返回 (状态, 搬运字节, 帧数, 墓碑数, complete, 日志行)."""
    log = []
    if target.exists():
        # 上一轮已 rename, 只差删旧目录 (或新编码器已直接编出了这天)
        day_file_valid(target)
        shutil.rmtree(day_dir)
        return "resumed", 0, 0, 0, None, log

    bins = []
    for entry in os.scandir(day_dir):
        if not entry.is_file(follow_symlinks=False):
            log.append(f"{day_dir}: 非文件条目 {entry.name}, 丢弃")
        elif entry.name.endswith(".bin"):
            bins.append(entry.name)
        elif entry.name != STAT_NAME:
            log.append(f"{day_dir}: 脏文件 {entry.name}, 丢弃")
    bins.sort()

    stat = read_old_stat(day_dir / STAT_NAME)
    if stat is None and (day_dir / STAT_NAME).exists():
        log.append(f"{day_dir}: .stat 损坏, 按无账目处理")

    # 先把旧 .bin 全部验一遍, 算出当天体积再决定开不开写
    frames = {}  # asset_id -> (文件名, order_count, compressed_size, price_base)
    day_bytes = 0
    bad_files = 0
    for name in bins:
        code = name[: -len(".bin")]
        asset_id = axis_index.get(code)
        if asset_id is None:
            log.append(f"{day_dir}/{name}: 不在 A 轴上, 丢弃")
            bad_files += 1
            continue
        info, why = read_old_bin(day_dir / name)
        if info is None:
            log.append(f"{day_dir}/{name}: 坏文件 ({why}), 丢弃")
            bad_files += 1
            continue
        order_count, compressed_size, price_base, size = info
        assert asset_id not in frames, f"{day_dir}: 同一资产两个 .bin"
        frames[asset_id] = (name, order_count, compressed_size, price_base)
        day_bytes += size

    tombs = set()
    if stat is not None:
        for asset_id, _, file_size in stat[4]:
            if file_size != 0 or asset_id in frames:
                continue
            if asset_id >= len(axis_index):
                log.append(
                    f"{day_dir}: .stat 墓碑 asset_id {asset_id} 超出 A 轴, 丢弃")
                continue
            tombs.add(asset_id)

    if not frames and not tombs:
        log.append(f"{day_dir}: 没有任何可迁的帧或墓碑, 只删旧目录")
        shutil.rmtree(day_dir)
        return "empty", 0, 0, 0, None, log

    free = shutil.disk_usage(target.parent).free
    assert free > day_bytes + \
        FREE_MARGIN, f"盘快满了: 剩 {free >> 30}GB, 这天要 {day_bytes >> 30}GB + 余量"

    # ---- 写 .tmp ----
    tmp = target.with_name(target.name + ".tmp")
    out = os.open(tmp, os.O_WRONLY | os.O_CREAT |
                  os.O_TRUNC | os.O_CLOEXEC, 0o644)
    cur = DAY_HEAD.size
    index = []
    for asset_id in sorted(frames):
        name, order_count, compressed_size, price_base = frames[asset_id]
        fd = os.open(day_dir / name, os.O_RDONLY)
        copy_range(fd, out, compressed_size, OLD_HEAD.size, cur)
        os.close(fd)
        index.append((cur, asset_id, order_count, compressed_size, price_base))
        cur += compressed_size
    for asset_id in tombs:
        index.append((0, asset_id, 0, 0, 0))
    index.sort(key=lambda e: e[1])

    # ---- 账目 ----
    n_ok, n_skip = len(frames), len(tombs)
    complete, total, corrupt, invalid, failed, checks = 0, n_ok + \
        n_skip, 0, 0, 0, [0] * CHECK_BITS
    if stat is not None and stat[0]:
        accounted_complete, (s_total, s_ok, s_skip, s_corrupt,
                             s_invalid, s_failed), s_checks = stat[1], stat[2], stat[3]
        declared = {(a, c, sz) for a, c, sz in stat[4] if sz != 0}
        on_disk = {(a, frames[a][1], OLD_HEAD.size + frames[a][2])
                   for a in frames}
        consistent = (
            bad_files == 0
            and declared == on_disk
            and s_ok == n_ok
            and s_skip == n_skip
            and s_ok + s_skip + s_corrupt + s_invalid + s_failed <= s_total
        )
        corrupt, invalid, failed, checks = s_corrupt, s_invalid, s_failed, s_checks
        total = max(s_total, n_ok + n_skip + corrupt + invalid + failed)
        complete = 1 if consistent and accounted_complete else 0
        if not consistent:
            log.append(f"{day_dir}: .stat 账目与盘上不符, 记为不齐备")

    index_bytes = b"".join(DAY_ENTRY.pack(*e) for e in index)
    written = os.pwrite(out, index_bytes, cur)
    assert written == len(index_bytes), "目录写不全 (磁盘满?)"
    head = DAY_HEAD.pack(DAY_MAGIC, DAY_HEAD.size, len(
        index), complete, total, n_ok, n_skip, corrupt, invalid, failed, *checks)
    written = os.pwrite(out, head, 0)
    assert written == len(head), "头写不全"
    os.fsync(out)
    os.close(out)

    # ---- 回读校验 → rename → 删旧 ----
    fd = os.open(tmp, os.O_RDONLY)
    back_head, back_index = check_day_file(fd)
    os.close(fd)
    assert back_head == DAY_HEAD.unpack(
        head) and back_index == index, "回读与写入不一致"

    os.replace(tmp, target)
    fsync_dir(target.parent)
    shutil.rmtree(day_dir)
    return "merged", day_bytes, n_ok, n_skip, complete, log


# ============================================================================
# main
# ============================================================================


def list_old_days(orders_dir):
    days = []
    for year in sorted(orders_dir.iterdir()):
        if not (year.is_dir() and year.name.isdigit() and len(year.name) == 4):
            continue
        for month in sorted(year.iterdir()):
            if not (month.is_dir() and month.name.isdigit() and len(month.name) == 2):
                continue
            for day in sorted(month.iterdir()):
                if day.is_dir() and day.name.isdigit() and len(day.name) == 2:
                    days.append(
                        (day, month / f"{year.name}{month.name}{day.name}.bin"))
    return days


def sweep_tmp(orders_dir):
    removed = 0
    for tmp in orders_dir.glob("[0-9][0-9][0-9][0-9]/[0-9][0-9]/*.bin.tmp"):
        tmp.unlink()
        removed += 1
    return removed


def fmt_secs(s):
    s = int(s)
    return f"{s // 3600}h{s % 3600 // 60:02d}m" if s >= 3600 else f"{s // 60}m{s % 60:02d}s"


def main():
    config = json.loads(CONFIG_PATH.read_text())
    orders_dir = (CPP_RUN_DIR / config["orders_dir"]).resolve()
    assert orders_dir.is_dir(), f"orders 目录不存在: {orders_dir}"

    axis = json.loads(AXIS_PATH.read_text())
    assert axis["count"] == len(
        axis["assets"]), "asset_axis.json count 与名单长度不符"
    axis_index = {code: i for i, code in enumerate(axis["assets"])}
    assert len(axis_index) == len(axis["assets"]), "asset_axis.json 有重复代码"

    removed = sweep_tmp(orders_dir)
    days = list_old_days(orders_dir)
    print(f"orders: {orders_dir}")
    print(f"A 轴 {len(axis_index)} 个资产; 清掉残留 .tmp {removed} 个; 待迁 {len(days)} 天")
    if not days:
        return

    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    logf = open(LOG_PATH, "a", encoding="utf-8")
    logf.write(
        f"==== {time.strftime('%Y-%m-%d %H:%M:%S')} merge {len(days)} days in {orders_dir}\n")

    done = 0
    moved = 0
    incomplete = 0
    resumed = 0
    log_lines = 0
    t0 = time.monotonic()

    def show():
        elapsed = time.monotonic() - t0
        rate = moved / elapsed / (1 << 20) if elapsed > 0 else 0.0
        eta = elapsed / done * (len(days) - done) if done else 0.0
        sys.stdout.write(
            f"\r[{done}/{len(days)}] {moved / (1 << 30):.1f}GB {rate:.0f}MB/s"
            f" | ETA {fmt_secs(eta)} | 不齐备 {incomplete} | 续删 {resumed} | 日志 {log_lines} 行   "
        )
        sys.stdout.flush()

    # 惰性提交: 在飞的永远只有 DAY_WORKERS 天. Ctrl-C 时只等这几天收尾 (每天都是原子的),
    # 不会把剩下的全跑完.
    with ThreadPoolExecutor(max_workers=DAY_WORKERS) as pool:
        pending = {}
        it = iter(days)
        while True:
            while len(pending) < DAY_WORKERS:
                nxt = next(it, None)
                if nxt is None:
                    break
                pending[pool.submit(merge_day, nxt[0],
                                    nxt[1], axis_index)] = nxt[0]
            if not pending:
                break
            finished, _ = wait(pending, timeout=1.0,
                               return_when=FIRST_COMPLETED)
            for fut in finished:
                del pending[fut]
                status, nbytes, _, _, complete, log = fut.result()
                done += 1
                moved += nbytes
                resumed += status == "resumed"
                incomplete += complete == 0
                for line in log:
                    logf.write(line + "\n")
                log_lines += len(log)
                if log:
                    logf.flush()
            show()

    logf.close()
    sys.stdout.write("\n")
    print(
        f"完成: {done} 天, {moved / (1 << 30):.1f}GB, 用时 {fmt_secs(time.monotonic() - t0)}")
    print(f"不齐备 {incomplete} 天 (增量编码会补齐); 详情见 {LOG_PATH}")


if __name__ == "__main__":
    main()
