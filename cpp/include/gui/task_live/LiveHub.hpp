// Live 任务的中枢: 一个 Broker (客户端适配表 live/Clients.hpp 里选一个, 点「连接」才建) + 新浪行情 + 两融条款 + 下单 / 调仓事务, 券商无关
//   行情: 只此一路 (持仓、今日清仓、负债、调仓名单里可买的), 有人在看才转, 每 kQuoteEvery 取一次, 要的码多了即补取;
//     盘口列同帧复用其 5 档. 账户读到后头一次取齐其各只的行情记作读时现价 (可用保证金按它校准, 见 live/Rebal.hpp)
//   条款: 只认当日的, 落盘 output/live/terms/<日>.csv (只留当日); 跨日即换成当日落盘的; 调仓页打开时当日还没有即后台拉
//     (每天自动至多一次, 失败了等人点)
//   下单: 一次一单, 留最近 kOrderCap 单; 步骤 = 「行情」(按新取的盘口定委托价) + broker 的各步; 持仓页的单成功后 kick 定点刷新
//   调仓: 后台事务一次一件 (拉条款 / 执行), 日志落 output/run/live/rebal/<时刻>.jsonl, 留尾巴给页面.
//     执行 = 当日条款还没有即先拉 → 刷新 → 还有挂单即停 (免得重复下单、冻结的钱券没算进试算) → 试算 →
//     卖单逐只卖出 → 刷新到成交表里各只卖量到齐 → 去掉卖出重算 → 买单逐只 (融资在前) → 刷新.
//     任一步失败即停 (已下的单不撤); 卖单、买单各一整批独占界面连下, 其间不插刷新
//   GUI 线程每帧 tick 取一份快照, 持仓帧 / 试算按其输入 (State 版本 / 行情 / 配置 / 条款 / 读时现价) 变了才重算
//   后台线程各持核心的 shared_ptr, 析构只置停 (在途的客户端操作做完即退)
#pragma once

#include "live/Board.hpp"
#include "live/Broker.hpp"
#include "live/Rebal.hpp"

#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace GUI::Live {

inline constexpr size_t kOrderCap = 5;  // 留最近几单
inline constexpr size_t kLogTail = 200; // 调仓日志留给页面的条数

inline std::string wan(double v) { return live::strf("%.2fW", v / 1e4); }
inline constexpr const char *kMask = "****"; // 隐私下遮住的一段 (见 LiveHub::priv)

enum class JobSt : uint8_t { Run,
                             Ok,
                             Err };
inline const char *job_name(JobSt s) { return s == JobSt::Run ? "进行中" : s == JobSt::Ok ? "成功"
                                                                                          : "失败"; }

struct OrderJob {
  uint64_t id = 0;
  std::string t; // 起于 HH:MM:SS
  JobSt st = JobSt::Run;
  std::string msg;
  live::Sec sec;
  int side = live::kBuy;
  live::Credit credit = live::Credit::Plain;
  std::string label;              // 量: N股 / xW
  std::vector<std::string> steps; // 「行情」+ broker 的各步
  size_t at = 0;
  std::string detail;
  double qty = 0, px = 0; // 回报
};

struct LogLine {
  double t = 0; // 距事务开头 [s]
  std::string kind, text;
};
struct Task {
  std::string name, t;
  JobSt st = JobSt::Run;
  std::string msg, file, live; // live = 实时进度 (覆盖上一行)
  std::deque<LogLine> log;
};

// 帧 / 等什么 / 出错原因 三选一
struct BoardView {
  std::shared_ptr<const live::Quotes> quotes; // frame 各行的 quote 指进这份
  std::optional<live::board::Frame> frame;
  std::string wait, err;
};
struct PlanView {
  std::optional<live::rebal::Plan> plan;
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
  bool connected() const;
  void connect(size_t client); // 按 live::clients() 的下标建 broker, 起行情线程
  live::Broker &broker();
  // 隐私: 遮住代码、名称与账户金额 (连同其悬停提示), 比例、只数与下单照显; 落盘 output/config/live/ui.json
  bool priv() const { return priv_; }
  void set_priv(bool on);
  void peek(); // 每帧: 取单 / 事务 / 刷新进度 (左栏标签用), 不算有人在看
  void tick(); // 每帧 (页面可见时): peek + 有人在看 + 其余快照, 输入变了即重算持仓帧 / 试算

  // peek / tick 取的快照
  const std::shared_ptr<const live::State> &state() const { return st_; }
  const live::Refresh &refresh() const { return prog_; }
  const BoardView &board() const { return board_; }
  const PlanView &plan() const { return plan_; }
  const std::string &quote_time() const { return quote_at_; } // 行情最新时刻
  const live::rebal::Cfg &cfg() const { return cfg_; }
  const std::vector<std::string> &codes() const { return codes_; } // 当前名单
  const std::string &list_err() const { return list_err_; }        // 名单读不出的原因
  std::optional<size_t> terms() const { return terms_n_; }         // 当日条款只数; 还没拉 = 无
  const std::vector<OrderJob> &orders() const { return jobs_; }    // 新在前
  const std::optional<Task> &task() const { return task_; }
  bool ordering() const; // 有单在跑
  bool busy() const;     // 调仓事务在跑

  std::vector<std::string> lists() const;
  void set_list(const std::string &name); // 即重读
  void set_unit(double unit);
  void set_pct(double pct);
  void set_px(live::PxMode px);
  void set_sell(const std::string &code, int n); // 0 = 不卖

  // 持仓页 code 那只后台按 size 元下一单, 委托价随 cfg().px; 有单在跑即不收
  void submit(const std::string &code, int side, double size, live::Credit credit);

  void pull();      // 后台拉当日两融条款
  void run();       // 后台执行调仓
  void auto_pull(); // 调仓页打开时: 当日条款还没有、今天也还没自动拉过、没有事务在跑 → 拉

private:
  std::shared_ptr<Core> c_;
  bool priv_ = false;
  std::shared_ptr<const live::State> st_;
  live::Refresh prog_;
  BoardView board_;
  PlanView plan_;
  std::string quote_at_;
  live::rebal::Cfg cfg_;
  std::vector<std::string> codes_;
  std::string list_err_;
  std::optional<size_t> terms_n_;
  std::vector<OrderJob> jobs_;
  std::optional<Task> task_;
  uint64_t board_key_[2] = {~0ull, ~0ull};                     // State 版本, 行情版本
  uint64_t plan_key_[5] = {~0ull, ~0ull, ~0ull, ~0ull, ~0ull}; // + 配置, 条款, 读时现价
};

} // namespace GUI::Live
