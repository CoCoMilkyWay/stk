#pragma once

#include "yyjson/yyjson.h"

#include <cstddef>
#include <filesystem>
#include <string>

// 通用文件系统工具: 定位 git 根、整文件读、原子写。业务无关。
namespace misc {

// 从 cwd 向上爬找含 .git 的目录 (仓库根)。结果缓存为 static, 第一次失败 assert。
std::filesystem::path git_root();

// 读整个文件到 string (二进制模式)。文件不存在不会 throw, 返回空串。
std::string read_file_all(const std::filesystem::path &path);

// tmp + rename 原子写。父目录不存在会自动 create_directories。
void atomic_write(const std::filesystem::path &path, const char *data,
                  std::size_t len);

// 序列化 yyjson_mut_doc 为 PRETTY_TWO_SPACES + atomic_write; doc 不释放 (caller 负责).
// 唯一 JSON 落盘点: output/ 下的报告元数据 (数据集本体全走 misc/parquet).
// ALLOW_INF_AND_NAN: 报告指标里 NaN 是合法值 (样本不足 / 方差为 0), 写成裸 NaN
//   字面量 — Python json.loads 原生接受, 直接得 float('nan'), 前端无需特判 null.
void atomic_write_json(const std::filesystem::path &path, yyjson_mut_doc *doc);

} // namespace misc
