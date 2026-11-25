#pragma once

// =============================================================================
// Journal — 会话目录的存档与可读日志 (只 GUI 线程写): 时间线 JSON 契约 (读写严格成对) + log.md 成文 + 存过的会话一览
// =============================================================================
//   会话目录 output/agent/<模式>/sessions/<池>/<层>/<持有期>/chat/<时间>/ (disk::AgentChat, 各文件一个节点, 一律经 disk:: 读写):
//     system.md     系统提示 (开会话时写一次)
//     session.json  整份存档 (Mine 的设置 + 时间线; 调用的结构化结果只存 Res::save 给的, 载入时在当前台面上复原, 见 Rig::restore)
//     brief.json    载入菜单的一行
//     log.md        可读研究日志: 随存档按时间线整份重写 (每步 = 发出 · 回复 · 调用, 各带其后的日志笔)
//     report.md     停下报告
// =============================================================================

#include "disk/Disk.hpp"
#include "gui/task_agent/Bridge.hpp"
#include "gui/task_agent/tools/Kit.hpp"
#include "nlohmann/json.hpp"

#include <string>
#include <vector>

namespace GUI::Agent {

void to_json(nlohmann::ordered_json &j, const Act &a);
void from_json(const nlohmann::ordered_json &j, Act &a);
void to_json(nlohmann::ordered_json &j, const Step &sp);
void from_json(const nlohmann::ordered_json &j, Step &sp);

std::string ReplyLog(int n, const Step &sp); // 一步的回复在 log.md 里的一节 (作废的回复也照这样记一笔)

class Journal {
public:
  void open(disk::Key key, const std::string &system, const std::string &head);
  void reopen(disk::Key key, std::string head);
  const std::string &dir() const { return dir_; } // 会话目录 (会话的身份: 全局记录 / 外部 agent 的等待按它认)
  const std::string &head() const { return head_; }
  std::string system() const;

  void report(const std::string &text) const;
  void save(const nlohmann::ordered_json &session, const nlohmann::ordered_json &brief, Steps timeline) const;

private:
  disk::Key key_;
  std::string dir_, head_;
};

nlohmann::ordered_json SessionOf(const disk::Key &key); // 存过的会话的整份存档

struct Saved {
  disk::Key key; // 含时刻 stamp 与模式 test
  std::string dir;
  Scope sc;
  nlohmann::ordered_json brief;
};
std::vector<Saved> SavedSessions(const Scope &sc);

} // namespace GUI::Agent
