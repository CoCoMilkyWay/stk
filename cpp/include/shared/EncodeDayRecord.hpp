#pragma once

#include "codec/L2_Validator.hpp"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

class AssetAxis;

// ============================================================================
// 一天的编码统计 — orders/YYYY/MM/DD/.stat
// ============================================================================
//
// 一天一份, 记这天的账目与每个 (资产, 当天) 的结局:
//
//   账目 (accounted/complete + 处置分类 + 判据命中数): 增量重跑用它跳过整天,
//   界面用它按日拆解原因. 分母与处置分类要拿当天归档做依据, 只有编码器知道,
//   故扫描回填时 accounted=0.
//
//   逐资产明细 (assets[]): 每个 .bin 的条数与体积, 按 asset_id 升序. 扫描读它
//   代替逐个 open+pread 文件头. order_count/orders_file_size 皆为 0 即墓碑 ——
//   该资产那天源数据不足以编码 (停牌 / 只有表头), 盘上没有 .bin.
//
// 格式: 定宽自描述头 + 定长记录数组, 与 .bin 的 L2FileHeader 同一套路
// (magic/version 挡住"不是这个文件/旧格式", 长度自洽性挡住截断). 一天五千条,
// 一次 read 全拿, 零解析 —— 文本键值对在这个量级上不合适.
//
// 明细键用 A 轴下标而非 "000001.SZ": 轴 append-only 且自校验 (见 AssetAxis),
// 下标恒定, 于是一条记录 12 字节.
//
// 采信明细的判据 (扫描与编码器共用, 见 day_index_current): 日目录 mtime 与落盘
// 时记的一致 → 直接用, 一天只花一次 stat + 一次小文件读; 变了 → readdir 与明细
// 逐一核对, 对上仍可用, 对不上 (手删过 .bin / 写了一半被截断) 扫描退回逐个读头
// 并把明细重写一遍 (顺带记下新 mtime).
inline constexpr const char *kEncodeStatName = ".stat";

// 一个 (资产, 当天) 的结局与计量.
//
// order_count / orders_file_size 同时为 0 就是墓碑: 那天这个资产的源数据不足
// 以编码 (停牌 / 文件只有表头), 盘上没有 .bin. 编出来的 .bin 至少有一条逐笔
// (空的走墓碑, 见 BinaryEncoder_L2::finish_asset), 所以这个判据不会误伤.
struct EncodeDayIndexEntry {
  uint32_t asset_id;         // A 轴下标 (== Asset::items 下标)
  uint32_t order_count;      // 与 L2FileHeader::order_count() 同一口径
  uint32_t orders_file_size; // 文件总长 = 32 + compressed_size

  bool is_tombstone() const {
    return orders_file_size == 0;
  }
};
static_assert(sizeof(EncodeDayIndexEntry) == 12, "整天明细的记录必须紧凑无填充");

// 三个来源 (编码落盘 / 增量跳过的旧产物 / 扫描回填) 都是 size_t 进、定宽出,
// 收窄的边界检查只写这一份. 32 位的余量很宽: 最活跃的标的单日逐笔是百万量级,
// 单个 .bin 是几 MB.
inline EncodeDayIndexEntry make_day_index_entry(size_t asset_id, size_t order_count,
                                                size_t orders_file_size) {
  assert(asset_id <= UINT32_MAX && "整天明细: A 轴下标溢出 32 位");
  assert(order_count <= UINT32_MAX && "整天明细: 单日逐笔条数溢出 32 位");
  assert(orders_file_size <= UINT32_MAX && "整天明细: 单个 .bin 体积溢出 32 位");
  return {static_cast<uint32_t>(asset_id), static_cast<uint32_t>(order_count),
          static_cast<uint32_t>(orders_file_size)};
}

// 墓碑: 有结局没数据.
inline EncodeDayIndexEntry make_day_tombstone(size_t asset_id) {
  return make_day_index_entry(asset_id, 0, 0);
}

struct EncodeDayRecord {
  // 账目部分是否有效.
  //
  // 明细可以由扫描独立回填 (readdir + 读头就够), 但账目不行 —— 分母与处置
  // 分类要拿当天归档做依据, 只有编码器知道. 扫描回填时这一位置 0, 于是界面
  // 不会把"只有明细的天"显示成"编过但不齐备".
  bool accounted = false;

  // 齐备 = 当天每个 (资产, 日期) 都有了结局: 落了 .bin 或记了墓碑
  bool complete = false;

  // 分母: 当天归档里落在 A 轴上、且有逐笔委托文件的资产数
  size_t assets_total = 0;

  // 处置分类, 互斥, 加起来 ≤ assets_total (取消会让一天半途而废)
  size_t assets_ok = 0;      // 落了 .bin
  size_t assets_skipped = 0; // 记了墓碑 (停牌 / 只有表头)
  size_t assets_corrupt = 0; // 源 CSV 坏行或归档流断
  size_t assets_invalid = 0; // 准入校验未过
  size_t assets_failed = 0;  // 环境错误 (磁盘满 / 压缩失败)

  // 按 L2::Check 的位记"命中这一条判据的标的数". 一个标的可能同时命中多条,
  // 所以这几列的和会大于 assets_invalid.
  size_t checks[L2::kCheckBitCount] = {};

  // 落盘那一刻日目录的 mtime (fs::file_time_type::time_since_epoch().count()).
  //
  // 明细"仍与盘上一致"的判据: 目录 mtime 没变 ⇒ 没有 .bin 增删 (rename 落盘
  // / 手删都会改它; .stat 自己是原地 trunc 写, 不改) ⇒ 明细直接采信, 连
  // readdir 都省. 变了才 readdir 逐一核对. 由 write_encode_day_stat 在打开
  // 文件之后取 (首次创建 .stat 本身也会改目录 mtime, 要取在那之后), 调用方
  // 不用管. 0 = 取不到, 永远视为"变了".
  int64_t dir_mtime = 0;

  // 每个 (资产, 当天) 一条, 按 asset_id 升序落盘.
  //
  // 注意别把它长期留在内存里: 全库 885 天 × 5200 条 = 五十多兆, 而且与
  // Asset::items[].date_info 是同一份数据. 扫描读完就把它搬空 (见
  // Asset::coro_scan_binary_database).
  std::vector<EncodeDayIndexEntry> assets;

  size_t assets_error() const {
    return assets_corrupt + assets_invalid + assets_failed;
  }
};

// 写 day_dir/.stat. 明细在里面按 asset_id 排好序; dir_mtime 在这里面取.
// 写不出去就是磁盘出了问题, 当场 assert.
void write_encode_day_stat(const std::string &day_dir, EncodeDayRecord rec);

// 读 day_dir/.stat. 文件不存在返回 false (那天从没编过, 也没被扫描回填过);
// magic/version 对不上或长度不自洽同样返回 false —— 当它不存在, 由调用方重建.
bool read_encode_day_stat(const std::string &day_dir, EncodeDayRecord &out);

// 日目录 mtime, 与 EncodeDayRecord::dir_mtime 同一口径; 取不到返回 0.
int64_t day_dir_mtime(const std::string &day_dir);

// 当天 readdir 到的一个 .bin
struct DayBin {
  size_t asset_id; // A 轴下标
  std::string path;
};

// readdir 一天的 .bin, 按 A 轴过滤 (轴外文件忽略). 目录读不动返回空.
std::vector<DayBin> list_day_bins(const std::string &day_dir, const AssetAxis &axis);

// 明细里的非墓碑条目 ↔ readdir 名单 是否严格同一批. 个数相等不够 —— 一增一删
// 会个数相同而内容错位, 所以逐个查. 墓碑不参与: 它记的正是"这个资产当天没有 .bin".
bool day_index_matches(const EncodeDayRecord &rec, const std::vector<DayBin> &bins);

// 明细是否仍与盘上一致: 目录 mtime 未变 → 是 (免 readdir); 否则 readdir 核对.
// 扫描与编码器的整天快路径共用这一条判据.
bool day_index_current(const std::string &day_dir, const EncodeDayRecord &rec, const AssetAxis &axis);
