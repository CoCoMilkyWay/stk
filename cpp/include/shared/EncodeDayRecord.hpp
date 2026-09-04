#pragma once

#include "codec/L2_Validator.hpp"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

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
// 扫描采信明细的前提是它本来就要做的那次 readdir —— 明细里的 .bin 集合与当天
// readdir 名单逐一对上才用. 对不上 (手删过 .bin / 明细比库旧 / 写了一半被截断)
// 就退回逐个读头, 并把明细重写一遍.
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

// 写 day_dir/.stat. 明细在里面按 asset_id 排好序.
// 写不出去就是磁盘出了问题, 当场 assert.
void write_encode_day_stat(const std::string &day_dir, EncodeDayRecord rec);

// 读 day_dir/.stat. 文件不存在返回 false (那天从没编过, 也没被扫描回填过);
// magic/version 对不上或长度不自洽同样返回 false —— 当它不存在, 由调用方重建.
bool read_encode_day_stat(const std::string &day_dir, EncodeDayRecord &out);
