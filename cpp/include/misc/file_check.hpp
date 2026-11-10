// 归档核查 (FileCheck) ⇄ 修复 (FileRepair, misc/file_repair.hpp): 同一份问题分类, 核查产出, 修复消费
//   合格归档: <root>/YYYY/YYYYMM/YYYYMMDD.rar, 非固实 RAR, 包内 YYYYMMDD/<资产>/*.csv
//   单盘 (单磁头机械盘) 上一切探测 / 重打包都顺序逐个: 并发 unrar lb 各自 O(条目) 次散读, 抢磁头实测慢 359 倍
#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

namespace FileCheck {

// 问题类 = 处置: 前三类重打包可修, 后两类只能人工
enum class Issue : uint8_t {
  Container, // 非 RAR 容器: .zip / .7z, 或 .rar 实为 7z / zip
  Solid,     // 固实 RAR (RAR 4.x 头可判)
  Structure, // 包内无 YYYYMMDD/ 根
  Naming,    // 目录 / 文件名不合布局
  Integrity, // 截断 / 头链损坏 / 不认识的格式
};
inline constexpr size_t kIssueCount = 5;
inline constexpr bool repairable(Issue i) { return i <= Issue::Structure; }

struct IssueMeta {
  const char *name;
  const char *desc;
  const char *fix;
};
inline constexpr std::array<IssueMeta, kIssueCount> kIssueMeta{{
    {"Container", "non-RAR container (.zip / .7z / disguised .rar)", "Repair"},
    {"Solid", "solid RAR", "Repair"},
    {"Structure", "no YYYYMMDD/ root inside", "Repair"},
    {"Naming", "not YYYY/YYYYMM/YYYYMMDD.rar", "rename or move away by hand"},
    {"Integrity", "truncated / corrupt / unknown format", "re-download"},
}};
inline const IssueMeta &meta(Issue i) { return kIssueMeta[static_cast<size_t>(i)]; }

// 容器格式 (魔数判定, 与扩展名无关)
enum class Format : uint8_t { Rar,
                              SevenZip,
                              Zip,
                              Unknown };
Format format_of(const std::string &path);

struct Finding {
  Issue issue;
  std::string path; // 绝对路径 (Naming 可能是目录)
  std::string detail;
};

struct FileCheckResult {
  bool ran = false;              // 默认构造 = 未跑
  bool dir_exists = false;       // 归档目录不在: 沿用已有 orders, 视同通过
  bool tools_ok = false;         // unrar / 7z / rar 齐
  size_t valid = 0;              // 合格归档数
  std::vector<Finding> findings; // 按路径序

  size_t count(Issue i) const;
  size_t repairable() const;
  bool passed() const { return ran && (!dir_exists || (tools_ok && findings.empty())); }
};

// (已完成, 总数, 当前路径), 逐个顺序回调
using ProgressCallback = std::function<void(size_t, size_t, const std::string &)>;

FileCheckResult check(const std::string &archive_dir, const ProgressCallback &progress = nullptr);

} // namespace FileCheck
