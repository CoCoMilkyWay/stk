// Live 任务的中枢: 一个 Broker + 新浪行情 + 两融条款 + 串行事务道, 券商无关
//   快照: 后台改、GUI 每帧取, 不可变 shared_ptr: Broker 的 Snapshot, 中枢的 Inputs / Task / 最近几单;
//     持仓帧与调仓试算用 GUI::Stage 按键缓存, 输入在等时留着上一份照显, 页面不跳.
//   日志与进度合一: 事务线程挂 live::trace::Watch + Hooks, trace::Span / live / note 单写即同步落统一 trace/*.jsonl 并更新页面.
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

inline constexpr size_t kOrderCap = 5;  // 留最近几单
inline constexpr int kTermsAge = 7;     // 条款缓存满几天即后台重拉 [日]
inline constexpr size_t kLogTail = 200; // 事务日志留给页面的条数

inline std::string wan(double v, bool sign = false) { return live::strf(sign ? "%+.1fW" : "%.1fW", v / 1e4); }
inline constexpr const char *kMask = "****";

enum class JobSt : uint8_t { Run,
                             Ok,
                             Err };
inline const char *job_name(JobSt s) { return s == JobSt::Run ? "进行中" : s == JobSt::Ok ? "成功"
                                                                                          : "失败"; }

struct OrderJob {
  int sec = -1; // 起于 (当日秒)
  JobSt st = JobSt::Run;
  live::OrderReq o;
  std::string label;
  std::vector<std::string> steps; // 「行情」+ broker 的各步
  size_t at = 0;
  std::string detail;
  live::Placed got;
};

struct LogLine {
  double t = 0;
  std::string kind, text;
};

struct Task {
  std::string name;
  int sec = -1;
  JobSt st = JobSt::Run;
  std::string msg, file, live, step;
  std::deque<LogLine> log;
};

struct Inputs {
  std::shared_ptr<const live::Quotes> quotes;
  int quote_sec = -1;
  std::shared_ptr<const live::Snapshot> px_snap; // px0 对应哪份快照
  std::shared_ptr<const live::rebal::Prices> px0;
  std::shared_ptr<const live::terms::Cache> raw;
  std::shared_ptr<const live::terms::Local> local;
  std::shared_ptr<const live::Terms> terms;
  live::rebal::Cfg cfg;
  std::vector<std::string> codes;
  std::string list_err;
  std::vector<live::Sec> buy;
};

struct BoardView {
  std::shared_ptr<const live::Snapshot> snap;
  std::shared_ptr<const live::Quotes> quotes;
  std::optional<live::board::Frame> frame;
  std::string wait, err;
};

struct PlanView {
  std::optional<live::rebal::Plan> plan;
  std::optional<live::rebal::Sheet> now;
  std::string wait, err;
};

class LiveHub {
public:
  struct Core;

  LiveHub();
  ~LiveHub();
  LiveHub(const LiveHub &) = delete;
  LiveHub &operator=(const LiveHub &) = delete;

  // ---------- GUI 线程 ----------
  bool connected() const { return client_ != nullptr; }
  void connect(size_t client);
  const live::Client &client() const;
  live::Broker &broker();

  bool priv() const { return priv_; }
  void set_priv(bool on);
  void peek();
  void auto_pull();
  void tick(const AssetInfo &info, Quote::QuoteHub &quote);

  const std::shared_ptr<const live::Snapshot> &snapshot() const { return snap_; }
  const live::Refresh &refresh() const { return prog_; }
  const Inputs &in() const { return *in_; }
  const std::shared_ptr<const Task> &task() const { return task_; }
  const live::trace::Spot &task_at() const { return task_at_; }
  const std::vector<OrderJob> &orders() const { return *jobs_; }
  const BoardView &board() const { return board_.val; }
  const PlanView &plan() const { return plan_.val; }
  bool busy() const { return task_ && task_->st == JobSt::Run; }

  std::vector<std::string> lists() const;
  void set_list(const std::string &name);
  void edit_cfg(const std::function<void(live::rebal::Cfg &)> &f);

  void submit(const std::string &code, int side, double size, live::Credit credit);
  void pull_terms();
  void run();

private:
  using PtrPair = std::pair<const void *, const void *>;

  std::shared_ptr<Core> c_;
  const live::Client *client_ = nullptr;
  bool priv_ = false;
  misc::Ymd tried_[2] = {};
  std::shared_ptr<const live::Snapshot> snap_;
  live::Refresh prog_;
  std::shared_ptr<const Inputs> in_;
  std::shared_ptr<const Task> task_;
  live::trace::Spot task_at_;
  std::shared_ptr<const std::vector<OrderJob>> jobs_;
  Stage<std::array<int64_t, 3>, int> info_st_;
  Stage<PtrPair, BoardView> board_;
  Stage<PtrPair, PlanView> plan_;
  std::vector<std::string> watch_;
  uint64_t live_ver_ = 0;
};

} // namespace GUI::Live
