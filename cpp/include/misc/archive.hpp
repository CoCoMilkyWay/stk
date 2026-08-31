// Archive 读取 — 归档内容列举 + 零落盘批量流式读取
//
// encode 的数据入口. 两条要点:
//
// 1. 列举 (list_archive): 某日 RAR 里"实际有哪些资产、每个文件多大"的唯一
//    来源. encode 不再依赖人工 universe 名单, 必须先问包. `unrar l` 只读索引,
//    单包实测 ~0.3s.
//
// 2. 批量流式读取 (stream_archive_files): 不再把 CSV 落到磁盘再读回来.
//    - 落盘往返的代价: 单日解压后 46 GB, 4 TB 压缩数据 ≈ 690 天 ⇒ 32 TB 无谓
//      写入 + 等量读回. 可用内存只有 ~15 GB, page cache 兜不住, 是真实的 SSD
//      写入与刷盘带宽.
//    - `unrar p -inul` 把文件内容原样吐到 stdout (实测字节数与 unrar l 报的
//      尺寸精确一致, 无任何附加头尾), 于是可以一次调用取多个文件, 靠已知尺寸
//      在流上切分.
//    - 一次调用摊薄 unrar 的固定开销 (进程启动 + 30k 条目包头扫描): 实测 20
//      个资产一次调用 0.390s vs 20 次单独调用 1.292s (3.3x); 200 个模式一次
//      调用吐 657 MB 用 1.96s (335 MB/s).
//    - 关键约束: unrar p 的输出顺序是**归档顺序**, 与命令行上模式的顺序无关
//      (已实测: 逆序给模式, 输出仍按归档序). 所以 paths 必须先按 ArchiveEntry
//      的归档序排好, 否则切分会全部错位.
#pragma once

#include <cstddef>
#include <functional>
#include <string>
#include <vector>

namespace misc {

struct ArchiveEntry {
  std::string path;  // 包内路径, 形如 "20260803/000001.SZ/逐笔委托.csv"
  std::size_t size;  // 解压后字节数
  std::size_t index; // 归档内序号 == unrar p 的输出顺序
};

// `<tool> l <archive>` → 全部条目, 按归档顺序 (index 递增).
// 包不存在返回空; 包损坏 (非零退出) 触发 assert.
std::vector<ArchiveEntry> list_archive(const std::string &archive_path,
                                       const std::string &archive_tool);

// 一次 `<tool> p -inul` 读取 paths 指定的文件, 按 sizes 在流上切分.
//
// paths/sizes 必须按归档序排列 (见文件头注释), 长度相等.
// on_file(i, data, size) 逐文件回调, i 为在 paths 中的下标.
// data 指向内部复用缓冲, 仅在本次回调内有效 — 调用方应在回调里就地消费
// (解析成结构体), 不要保存指针.
using FileSink = std::function<void(std::size_t, const char *, std::size_t)>;

void stream_archive_files(const std::string &archive_path,
                          const std::string &archive_tool,
                          const std::vector<std::string> &paths,
                          const std::vector<std::size_t> &sizes,
                          const FileSink &on_file);

} // namespace misc
