// Live 任务的中枢: 一个 Broker (客户端适配表 live/Clients.hpp 里选一个, 点「连接」才建) + 新浪行情 + 两融条款 + 下单 / 调仓 / 拉取事务, 券商无关
//   快照: 后台改、GUI 每帧取, 一律不可变 shared_ptr (指针换了 = 换了一份): Broker 的 Snapshot, 中枢的 Inputs / Task / 最近几单;
//     持仓帧按 (Snapshot, 行情) 变了才重算, 试算按 (Snapshot, Inputs) 变了才重算 (GUI::Stage); 重算出的只是在等 (行情没齐等) 即留着上一份照显, 页面不跳
//   行情: 要的码 (持仓、今日清仓、负债、调仓名单里可买的) 每帧订阅给 QuoteHub (与 Market 同一路新浪轮询, 改了即提前一轮), 随 Live 的版本取用;
//     事务另取: 试算现取此刻的; 单笔下单现取该只; 批量下单起一路只取这批盘口的连续流 (见执行调仓). 盘口列同帧复用其 5 档.
//     快照读到后头一次取齐其各只的行情记作读时现价 (可用保证金按它校准, 见 live/Rebal.hpp). 事务里取行情一律有界 (取不到即该步失败, 不卡)
//   拉取: 两融条款 (live/Terms.hpp) 与资金流水账本 (account/Ledger.hpp) 同形 — 一件事务里 拉 → 存; 读到快照后各每天自动至多一次:
//     条款缓存缺、满 kTermsAge 天、或名单 / 持仓 / 负债里有缓存没有的代码; 账本今天还没拉过. 条款另可人点; 失败了等明天或人点
//   下单: 一单即一件「下单」事务 (调仓的各单在「执行调仓」事务里), 留最近 kOrderCap 单; 步骤 = 「行情」(按新取的盘口定委托价) + broker 的各步;
//     请求、取到的盘口、结局各记一条, 各步即本单的步骤
//   事务: 后台一次一件 (下单 / 执行调仓 / 两样拉取). 事务线程挂 live::trace::Watch + Hooks: 段 / 实时一行 / 记一条照常落交互日志,
//     记的各条另落 output/run/live/task/<时刻>.jsonl (一事务一文件, 留最近 kTaskKeep 个) 并留尾巴给页面; 进度行即其段栈.
//     执行调仓 = 检查一遍 (条款缓存还没有即先拉 → 刷新 → 还有挂单即停 (免得重复下单、冻结的钱券没算进试算) → 试算) →
//     一口气下完试算的各单 (卖 → 融资买 → 现金买, 不等成交): 其间 broker 不自起刷新 (不扫客户端), 盘口只经一路连续流
//     (后台每 0.5s 取一遍这批, 每单取最新的定扫档价; 断流超 5s 即该单现取); 钱券不够的单由闪电窗的最大可买 / 可卖封顶 → 末了刷新一轮.
//     一单没下成 (含任何意外) 只记下、接着下后面的, 末了有没下成的即事务失败、列出各单; 检查一遍里失败即停
//   错误边界 (live::guard): 各事务、各单、各帧的持仓帧 / 试算; 外部结局与应答畸形一律收成原因文本, 不越界崩进程
//   后台线程各持核心的 shared_ptr (在途的客户端操作做完即退)
#pragma once

#include "gui/util/Stage.hpp"
#include "live/Board.hpp"
#include "live/Broker.hpp"
#include "live/Clients.hpp"
#include "live/Rebal.hpp"
#include "live/Terms.hpp"
#include "misc/date.hpp"

#include <array>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

struct AssetInfo;
namespace GUI::Quote {
struct QuoteHub;
}

namespace GUI::Live {

inline constexpr size_t kOrderCap = 5;   // 留最近几单
inline constexpr int kTermsAge = 7;      // 条款缓存满几天即后台重拉 [日]
inline constexpr size_t kLogTail = 200;  // 事务日志留给页面的条数
inline constexpr size_t kTaskKeep = 500; // 事务日志留几个文件 (一事务一个)

// 万为单位, 定一位小数 (表里按位对齐); sign 带正负号
inline std::string wan(double v, bool sign = false) { return live::strf(sign ? "%+.1fW" : "%.1fW", v / 1e4); }
inline constexpr const char *kMask = "****"; // 隐私下遮住的一段 (见 LiveHub::priv)

enum class JobSt : uint8_t { Run,
                             Ok,
                             Err };
inline const char *job_name(JobSt s) { return s == JobSt::Run ? "进行中" : s == JobSt::Ok ? "成功"
                                                                                          : "失败"; }

struct OrderJob {
  int sec = -1; // 起于 (当日秒)
  JobSt st = JobSt::Run;
  live::OrderReq o;               // 委托价「行情」一步才定
  std::string label;              // 量: N股 / xW
  std::vector<std::string> steps; // 「行情」+ broker 的各步
  size_t at = 0;
  std::string detail;
  live::Placed got; // 回报; 失败 = msg 为原因
};

struct LogLine {
  double t = 0; // 距事务开头 [s]
  std::string kind, text;
};
struct Task {
  std::string name;
  int sec = -1; // 起于 (当日秒)
  JobSt st = JobSt::Run;
  std::string msg, file, live; // file = 本件事务的日志; live = 实时进度 (覆盖上一行)
  std::string step;            // 批量下单时本单在批里的位置 (如「卖出 2/5」, 左栏标签的第二级); 细到哪一步看事务线程的段栈
  std::deque<LogLine> log;
};

// 持仓帧 / 试算的输入 (行情 / 读时现价 / 条款 / 配置与名单), 改一样即整份换
struct Inputs {
  std::shared_ptr<const live::Quotes> quotes;    // 订阅的或事务现取的, 最新一份
  int quote_sec = -1;                            // 行情最新时刻 (当日秒)
  std::shared_ptr<const live::Snapshot> px_snap; // px0 是哪份快照的读时现价
  std::shared_ptr<const live::rebal::Prices> px0;
  std::shared_ptr<const live::terms::Cache> raw;   // 券商的条款 (缓存); 还没拉过 = 空
  std::shared_ptr<const live::terms::Local> local; // 本地库的个股
  std::string local_err;                           // 本地库合不了 (没有两融明细等) 的原因: 非空即不试算、不调仓
  std::shared_ptr<const live::Terms> terms;        // raw 合 local; raw 空 = 空
  live::rebal::Cfg cfg;
  std::vector<std::string> codes; // 名单
  std::string list_err;           // 名单读不出的原因
  std::vector<live::Sec> buy;     // 名单里可买的
};

// 出错 = 无帧; 在等 = 帧为上一份 (还没有即无)
struct BoardView {
  std::shared_ptr<const live::Snapshot> snap; // frame 由这份算出
  std::shared_ptr<const live::Quotes> quotes; // frame 各行的 quote 指进这份
  std::optional<live::board::Frame> frame;
  std::string wait, err;
};
// 同 BoardView: 出错 = 无试算; 在等 = 试算与「现」为上一份
struct PlanView {
  std::optional<live::rebal::Plan> plan;
  std::optional<live::rebal::Sheet> now; // 账户「现」: 试算的, 试算没出即读时的; 没读到快照 = 无
  std::string wait, err;
};

class LiveHub {
public:
  struct Core;

  LiveHub();
  ~LiveHub();
  LiveHub(const LiveHub &) = delete;
  LiveHub &operator=(const LiveHub &) = delete;

  // ---------- 以下皆 GUI 线程 ----------
  bool connected() const { return client_ != nullptr; }
  void connect(size_t client); // 按 live::clients() 的下标建 broker
  const live::Client &client() const;
  live::Broker &broker();
  // 隐私: 遮住代码、名称与账户金额 (连同其悬停提示), 比例、只数与下单照显; 落盘 output/config/live/ui.json
  bool priv() const { return priv_; }
  void set_priv(bool on);
  void peek();      // 每帧: 取快照, 不算有人在看
  void auto_pull(); // 每帧 (peek 后): 两样拉取该拉了即起事务
  // 每帧 (Board 页可见时; quote 本帧已 frame): 本地库变了即重合条款 + peek + 有人在看 + 订阅行情, 输入变了即重算持仓帧 / 试算
  void tick(const AssetInfo &info, Quote::QuoteHub &quote);

  // peek / tick 取的快照
  const std::shared_ptr<const live::Snapshot> &snapshot() const { return snap_; } // 还没成功刷过 = 空
  const live::Refresh &refresh() const { return prog_; }
  const Inputs &in() const { return *in_; }
  const std::shared_ptr<const Task> &task() const { return task_; } // 还没有事务 = 空
  const live::trace::Spot &task_at() const { return task_at_; }     // 事务线程此刻在哪 (段栈空 = 不在客户端操作里)
  const std::vector<OrderJob> &orders() const { return *jobs_; }    // 新在前
  const BoardView &board() const { return board_.val; }
  const PlanView &plan() const { return plan_.val; }
  bool busy() const { return task_ && task_->st == JobSt::Run; } // 有事务在跑

  std::vector<std::string> lists() const;
  void set_list(const std::string &name);                          // 即重读
  void edit_cfg(const std::function<void(live::rebal::Cfg &)> &f); // 改名单以外的配置

  // 持仓页 code 那只 (持仓 / 今日成交里的, 或名单里可买的只许买) 按 size 元下一单, 委托价随配置; 有事务在跑即不收
  void submit(const std::string &code, int side, double size, live::Credit credit);
  void pull_terms(); // 后台拉两融条款
  void run();        // 后台执行调仓

private:
  using PtrPair = std::pair<const void *, const void *>;

  std::shared_ptr<Core> c_;
  const live::Client *client_ = nullptr;
  bool priv_ = false;
  misc::Ymd tried_[2] = {}; // 两样拉取 (条款 / 流水, 同 kPulls 序) 上次自动拉的日子
  std::shared_ptr<const live::Snapshot> snap_;
  live::Refresh prog_;
  std::shared_ptr<const Inputs> in_;
  std::shared_ptr<const Task> task_;
  live::trace::Spot task_at_;
  std::shared_ptr<const std::vector<OrderJob>> jobs_;
  Stage<std::array<int64_t, 3>, int> info_st_; // AssetInfo 的 (pit_end, bar1m_end, 代码数): 变了即重合条款
  Stage<PtrPair, BoardView> board_;            // (Snapshot, 行情) → 持仓帧
  Stage<PtrPair, PlanView> plan_;              // (Snapshot, Inputs) → 试算
  std::vector<std::string> watch_;             // 订阅给 QuoteHub 的码 (变了才交: 交要过服务的锁)
  uint64_t live_ver_ = 0;                      // 取用过的 Live 版本
};

} // namespace GUI::Live
