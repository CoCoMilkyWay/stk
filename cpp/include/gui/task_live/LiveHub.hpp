// Live 任务的中枢: 一个 Broker (客户端适配表 live/Clients.hpp 里选一个, 点「连接」才建) + 新浪行情 + 两融条款 + 下单 / 调仓 / 拉取事务, 券商无关;
//   页面只认中枢, 不碰 Broker (刷新 / 定时 / 画面都经中枢转)
//   快照: 后台改、GUI 每帧取, 一律不可变 shared_ptr (指针换了 = 换了一份): Broker 的 Snapshot, 中枢的 Inputs / Task / 最近几单;
//     持仓帧按 (Snapshot, 行情) 变了才重算, 试算按 (Snapshot, Inputs) 变了才重算 (GUI::Stage); 重算出的只是在等 (行情没齐等) 即留着上一份照显, 页面不跳
//   每帧两个入口: update (任务级, 无论哪页) 取快照、两样拉取该拉了即起事务; tick (Board 可见时, update 之后) 盯客户端、订阅行情、重算
//   输入 Inputs 三组同形 (行情 / 两融条款 / 调仓名单: 原料 → 派生) + 调仓配置, 改一样即整份换
//   行情: 要的码 (持仓、今日清仓、负债、调仓名单里可买的) 每帧订阅给 QuoteHub (与 Market 同一路新浪轮询, 改了即提前一轮), 随 Live 的版本取用;
//     事务另取: 试算现取此刻的; 单笔下单现取该只; 批量下单起一路只取这批盘口的连续流 (见执行调仓). 三处同一个取法 (gui/task_live/Books.hpp). 盘口列同帧复用其 5 档.
//     快照读到后头一次取齐其各只的行情记作读时现价 (可用保证金按它校准, 见 live/Rebal.hpp). 事务里取行情一律有界 (取不到即该步失败, 不卡)
//   拉取 Pull: 两融条款 (live/Terms.hpp) 与资金流水账本 (live/Flows.hpp) 同形 — 一件事务里 拉 → 存, 记下拉于; 人点即拉;
//     读到快照后各每天自动至多一次: 还没拉过、拉过满 PullSpec::age 天, 或另该拉 (只条款: 名单 / 持仓 / 负债里有缓存没有的代码); 失败了等明天或人点
//   事务与其中的一单同形 (Job: 起于 · 状态 · 实时一行 · 结局原文); 最近几单之首在跑 = 事务线程正下着它
//   下单: 一单即一件「下单」事务 (调仓的各单在「执行调仓」事务里), 留最近 kOrderCap 单; 步骤 = 「行情」(按新取的盘口定委托价, 按额的单另按它折股) + broker 的各步;
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
inline constexpr size_t kLogTail = 200;  // 事务日志留给页面的条数
inline constexpr size_t kTaskKeep = 500; // 事务日志留几个文件 (一事务一个)

// 万为单位, 定一位小数 (表里按位对齐); sign 带正负号
inline std::string wan(double v, bool sign = false) { return live::strf(sign ? "%+.1fW" : "%.1fW", v / 1e4); }
inline constexpr const char *kMask = "****"; // 隐私下遮住的一段 (见 LiveHub::priv)

// 两样拉取 (同形, 见文件头), 一样一行: 事务名即按钮名 / 拉过满 age 天即后台重拉 / 拉什么 (按钮悬停) / 拉过之后另该拉的情形 (空 = 无) / 拉于 (旁注悬停)
enum class Pull : uint8_t { Terms,
                            Flows };
inline constexpr Pull kPulls[2] = {Pull::Terms, Pull::Flows};
struct PullSpec {
  const char *name;
  int age; // [日]
  const char *hint, *also, *at;
};
const PullSpec &spec(Pull p);

enum class JobSt : uint8_t { Run,
                             Ok,
                             Err };
inline const char *job_name(JobSt s) { return s == JobSt::Run ? "进行中" : s == JobSt::Ok ? "成功"
                                                                                          : "失败"; }

// 一件事 (事务 / 其中的一单): 起于 (当日秒) · 状态 · 实时一行 (覆盖上一行; 做完留着末一行) · 结局原文 (成 = 回报 / 空, 败 = 原因)
struct Job {
  int sec = -1;
  JobSt st = JobSt::Run;
  std::string live, msg;
};

struct OrderJob : Job {
  live::OrderReq o;  // 委托价 (按额的连同股数)「行情」一步才定
  std::string label; // 量: N股 / xW
  size_t at = 0;     // 走到 LiveHub::steps 的哪一步
  live::Placed got;  // 回报 (没成 = 空)
};

struct LogLine {
  double t = 0; // 距事务开头 [s]
  std::string kind, text;
};
struct Task : Job {
  std::string name;
  std::string file; // 本件事务的日志
  std::string step; // 批量下单时本单在批里的位置 (如「卖出 2/5」, 左栏标签的第二级); 细到哪一步看事务线程的段栈
  std::deque<LogLine> log;
};

// 持仓帧 / 试算的输入: 三组同形 (原料 → 派生; 合不了 / 读不出的原因) + 调仓配置
struct Inputs {
  // 行情: 订阅的或事务现取的, 最新一份 → 最新时刻 / 快照的读时现价
  struct Px {
    std::shared_ptr<const live::Quotes> quotes;
    int sec = -1;                               // 最新时刻 (当日秒)
    std::shared_ptr<const live::Snapshot> snap; // px0 是哪份快照的读时现价
    std::shared_ptr<const live::rebal::Prices> px0;
  } px;
  // 两融条款: 券商的 (缓存) 合本地库的个股 → 合并的条款
  struct Terms {
    std::shared_ptr<const live::terms::Cache> raw;   // 还没拉过 = 空
    std::shared_ptr<const live::terms::Local> local; // 本地库的个股
    std::shared_ptr<const live::Terms> merged;       // raw 空 = 空
    std::string err;                                 // 本地库合不了 (没有两融明细等): 非空即不试算、不调仓
  } terms;
  // 调仓名单: 名 → 代码 → 其中可买的 (随条款)
  struct List {
    std::string name;
    std::vector<std::string> codes;
    std::vector<live::Sec> buy;
    std::string err; // 读不出的原因
  } list;
  live::rebal::Cfg cfg;
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
  // 隐私: 遮住代码、名称与账户金额 (连同其悬停提示), 比例、只数与下单照显; 落盘 output/config/live/ui.json
  bool priv() const { return priv_; }
  void set_priv(bool on);
  void update(); // 每帧 (任务级): 取快照, 不算有人在看; 两样拉取该拉了即起事务
  // 每帧 (Board 页可见时, update 之后; quote 本帧已 frame): 本地库变了即重合条款 + 有人在看 + 订阅行情, 输入变了即重算持仓帧 / 试算
  void tick(const AssetInfo &info, Quote::QuoteHub &quote);

  // update / tick 取的快照
  const std::shared_ptr<const live::Snapshot> &snapshot() const { return snap_; } // 还没成功刷过 = 空
  const live::Refresh &refresh() const { return prog_; }
  const Inputs &in() const { return *in_; }
  misc::Ymd pulled(Pull p) const { return pulled_[static_cast<int>(p)]; } // 拉于 (0 = 还没拉过)
  const std::shared_ptr<const Task> &task() const { return task_; }       // 还没有事务 = 空
  const live::trace::Spot &task_at() const { return task_at_; }           // 事务线程此刻在哪 (段栈空 = 不在客户端操作里)
  const std::vector<OrderJob> &orders() const { return *jobs_; }          // 新在前
  const std::vector<std::string> &steps() const;                          // 下单各步: 「行情」+ broker 的各步
  const BoardView &board() const { return board_.val; }
  const PlanView &plan() const { return plan_.val; }
  bool busy() const { return task_ && task_->st == JobSt::Run; } // 有事务在跑

  // 客户端 (须已连接): 要一轮刷新 (不等) / 定时周期 [s] (0 = 不定时) / 复现画面 (不支持 = 空)
  void request();
  void every(double s);
  std::unique_ptr<live::Mirror> mirror() const;

  std::vector<std::string> lists() const;
  void set_list(const std::string &name);                          // 即重读
  void edit_cfg(const std::function<void(live::rebal::Cfg &)> &f); // 改调仓配置

  // 持仓页 code 那只 (持仓 / 今日成交里的, 或名单里可买的只许买) 按 size 元下一单, 委托价随配置; 有事务在跑即不收
  void submit(const std::string &code, int side, double size, live::Credit credit);
  void pull(Pull p); // 后台拉 p
  void run();        // 后台执行调仓

private:
  using PtrPair = std::pair<const void *, const void *>;

  void peek(); // 取核心与 broker 的快照

  std::shared_ptr<Core> c_;
  const live::Client *client_ = nullptr;
  bool priv_ = false;
  std::array<misc::Ymd, 2> tried_{};  // 两样拉取 (kPulls 序) 上次自动拉的日子
  std::array<misc::Ymd, 2> pulled_{}; // 两样拉取的拉于
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
