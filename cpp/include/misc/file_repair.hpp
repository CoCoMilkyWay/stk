// 归档修复 (FileRepair) ⇄ 核查 (FileCheck, misc/file_check.hpp): 重打包核查里的可修项 (Container / Solid / Structure)
//   每个归档同一条路, 顺序逐个 (单盘), 暂存在 <stage>/<日期>/:
//     解包    in/                       unrar x (RAR) / 7z x (7z / zip)
//     定根    in 恰为单个 <日期>/ → 包根 = in; 否则 in 挪成 out/<日期>/, 包根 = out
//     打包    <归档目录>/.tmp_<日期>.rar   rar a 非固实, 直接写到目标盘
//     落位    fsync → rename 成 <日期>.rar → 删源 (源即终名时不删)
//   终名只经 fsync + 原子 rename 出现: 中断后盘上要么是旧源, 要么是完整新包, 重跑 核查 → 修复 即续
#pragma once

#include "misc/file_check.hpp"

#include <filesystem>
#include <vector>

namespace FileRepair {

void repair(const std::vector<FileCheck::Finding> &findings, const std::filesystem::path &stage,
            const FileCheck::ProgressCallback &progress = nullptr);

} // namespace FileRepair
