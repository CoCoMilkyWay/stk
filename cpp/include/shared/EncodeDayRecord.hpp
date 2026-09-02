#pragma once

#include "codec/L2_Validator.hpp"

#include <cstddef>
#include <string>

// ============================================================================
// 一天的编码账目 — orders/YYYY/MM/DD/.day_complete
// ============================================================================
//
// 这个文件从前是个空标记, 语义全在"存不存在"上: 存在且不老于当天归档 ⇒ 整天
// 齐备, 增量重跑整天跳过. 代价是"这天出了什么错"只活在 encoding.log 里, 而
// 那份日志每次进程重开就被截断, Encode 页无从按日拆解原因.
//
// 现在文件里存键值对, 齐备与否只是其中一项 (complete). 增量的快路径改看这一
// 项, 不再看文件在不在 —— 出错的天照样落记录 (complete=0), 下次仍会重来, 但
// 界面在重跑之前就能说清楚那天错在哪.
//
// 没有向后兼容: 旧的空标记读出来是 complete=0 / 全零, 于是那天会被重新列举
// 一遍; 产物都还新鲜的话 producer 当场补一份完整记录, 一轮增量即自愈.
inline constexpr const char *kEncodeDayRecordName = ".day_complete";

struct EncodeDayRecord {
  // 齐备 = 当天每个 (资产, 日期) 都留下了 .bin 或 .skip 墓碑
  bool complete = false;

  // 分母: 当天归档里落在 A 轴上、且有逐笔委托文件的资产数
  size_t assets_total = 0;

  // 处置分类, 互斥, 加起来 ≤ assets_total (取消会让一天半途而废)
  size_t assets_ok = 0;      // 落了 .bin
  size_t assets_skipped = 0; // 落了 .skip (停牌 / 只有表头)
  size_t assets_corrupt = 0; // 源 CSV 坏行或归档流断
  size_t assets_invalid = 0; // 准入校验未过
  size_t assets_failed = 0;  // 环境错误 (磁盘满 / 压缩失败)

  // 按 L2::Check 的位记"命中这一条判据的标的数". 一个标的可能同时命中多条,
  // 所以这几列的和会大于 assets_invalid.
  size_t checks[L2::kCheckBitCount] = {};

  size_t assets_error() const {
    return assets_corrupt + assets_invalid + assets_failed;
  }
};

// 写 day_dir/.day_complete. 写不出去就是磁盘出了问题, 当场 assert.
void write_encode_day_record(const std::string &day_dir, const EncodeDayRecord &rec);

// 读 day_dir/.day_complete. 文件不存在返回 false (那天从没编过);
// 存在但键名不认识则 assert —— 格式对不上说明代码与盘上产物不同源.
bool read_encode_day_record(const std::string &day_dir, EncodeDayRecord &out);
