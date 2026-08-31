// Archive 内容列举 — 某日 RAR 里"实际有哪些资产"的唯一来源
//
// encode 不再依赖人工 universe 名单, 必须先知道当天包里有什么: 全市场逐日
// 遍历 A 轴 (~5500) 去盲试提取, 绝大多数是徒劳的 unrar 调用 (每次都要走一遍
// 30k 条目的头链). `unrar lb` 只读索引, 单包实测 ~120ms, 一次列举换掉几千次
// 盲试.
#pragma once

#include <string>
#include <unordered_set>

namespace misc {

// `<tool> lb <archive>` → 包内资产目录名集合, 形如 "000001.SZ".
// 条目形如 "20260803/000001.SZ/行情.csv", 取中间一段去重.
// 包不存在返回空集; 包损坏 (unrar 非零退出) 触发 assert.
std::unordered_set<std::string> list_archive_assets(const std::string &archive_path,
                                                    const std::string &archive_tool);

} // namespace misc
