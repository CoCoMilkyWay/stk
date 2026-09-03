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

#include <atomic>
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

// 列举结果. Missing 与 Corrupt 必须分开, 且都不能和 "Ok + 空表" 混为一谈:
// 调用方靠"列举出多少资产"反推"这一天是否齐备", 而 Missing 下条目表恒为空,
// 混在一起就会把"没源可读"读成"没活可干" (见 encoding_producer 的完成标记).
enum class ArchiveListStatus {
  Ok,      // unrar 正常退出, entries 是这个包的真实内容
  Missing, // 包不在盘上 — 不是错误, 这一天单纯没有源
  Corrupt  // unrar 非零退出 (头链断裂/截断), entries 置空
};

// `<tool> l <archive>` → 全部条目, 按归档顺序 (index 递增), 写进 entries.
//
// 源数据损坏是数据摄入的正常结局之一, 不 assert: 编码是几小时的批处理,
// 一个坏包不该让整轮白跑. 调用方留日志并跳过这一天, 修好源文件后靠增量
// 自动补齐. 编程错误 (popen 失败) 仍然当场 assert.
ArchiveListStatus list_archive(const std::string &archive_path, const std::string &archive_tool,
                               std::vector<ArchiveEntry> &entries);

// 一次 `<tool> p -inul` 读取 paths 指定的文件, 按 sizes 在流上切分.
//
// paths/sizes 必须按归档序排列 (见文件头注释), 长度相等.
// on_file(i, data, size) 逐文件回调, i 为在 paths 中的下标.
// data 指向内部复用缓冲, 仅在本次回调内有效 — 调用方应在回调里就地消费
// (解析成结构体), 不要保存指针.
using FileSink = std::function<void(std::size_t, const char *, std::size_t)>;

// cancel 非空且置位时中途放弃: 直接关管道 (unrar 收 SIGPIPE), 不再回调,
// 也不判定完整性 — 取消路径上流本来就是不完整的 (返回 true).
//
// 返回 false = 源归档损坏: 流长度与 unrar l 报的尺寸对不上, 或 unrar 非零
// 退出 (成员 CRC 失败). 注意 CRC 是每个成员解完才校验的, 所以 false 可能在
// 全部回调都已发生之后才得知 —— 调用方要把这一批涉及的天整体判为"未完成"
// (不落完成标记), 等人修好源文件后靠增量重来. 理由同 list_archive.
bool stream_archive_files(const std::string &archive_path,
                          const std::string &archive_tool,
                          const std::vector<std::string> &paths,
                          const std::vector<std::size_t> &sizes,
                          const FileSink &on_file,
                          const std::atomic<bool> *cancel = nullptr);

} // namespace misc
