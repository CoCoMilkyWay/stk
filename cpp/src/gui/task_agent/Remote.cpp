// Remote — 见头文件
#include "gui/task_agent/Remote.hpp"
#include "gui/task_agent/Config.hpp"
#include "gui/task_agent/Prompt.hpp"
#include "gui/task_agent/Text.hpp"
#include "gui/task_agent/tools/Trial.hpp"

#include "disk/Disk.hpp"
#include "factor/Stat/Contract.hpp" // hold_name
#include "nlohmann/json.hpp"

#include <boost/asio.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <deque>
#include <functional>
#include <mutex>
#include <string_view>
#include <thread>
#include <utility>

namespace GUI::Agent {

namespace {

namespace beast = boost::beast;
namespace http = boost::beast::http;
namespace net = boost::asio;
using tcp = boost::asio::ip::tcp;
using json = nlohmann::ordered_json;
using Clock = std::chrono::steady_clock;

constexpr const char *kSession = "remote";                                   // 全局记录的会话名
constexpr const char *kNames = "doc / scope / run / wait / cancel / unlock"; // 本接口的工具
constexpr const char *kLocked = "(已为你锁上 Agent 模块: 研究员在 GUI 里解不了锁, 也开不了会话、用不了工作台, 不会干扰你, 放心做. 用完调 unlock 交还; 程序重启即解锁)\n\n";

// 一个请求的答复 (GUI 线程给, 网络线程写出)
struct Reply {
  bool error = false;
  std::string text;
};
using Answer = std::function<void(Reply)>;
struct Ask {
  std::string tool;
  json args;
  Answer answer;
};

// ---- 网络线程: POST {"tool", "args"} → {"ok", "text"} ----
class Server;
class Conn : public std::enable_shared_from_this<Conn> {
public:
  Conn(tcp::socket s, Server &srv) : s_(std::move(s)), srv_(srv) {}
  void read() {
    req_ = {};
    http::async_read(s_, buf_, req_, [self = shared_from_this()](beast::error_code ec, size_t) {
      if (!ec)
        self->handle();
    });
  }

private:
  void handle();
  // 一律 200: 成败在 body 的 ok (请求本身不合也一样, 原因写 text)
  void send(const Reply &r) {
    auto res = std::make_shared<http::response<http::string_body>>(http::status::ok, req_.version());
    res->set(http::field::content_type, "application/json");
    res->keep_alive(req_.keep_alive());
    res->body() = json{{"ok", !r.error}, {"text", r.text}}.dump();
    res->prepare_payload();
    // 写完: keep-alive 接着读下一个请求; 否则不再挂读, 连接随最后一个 shared_ptr 析构关掉
    http::async_write(s_, *res, [self = shared_from_this(), res](beast::error_code ec, size_t) {
      if (!ec && res->keep_alive())
        self->read();
    });
  }

  tcp::socket s_;
  Server &srv_;
  beast::flat_buffer buf_;
  http::request<http::string_body> req_;
};

class Server {
public:
  explicit Server(std::function<void(Ask)> on)
      : addr_(std::string("http://") + kRemoteHost + ":" + std::to_string(kRemotePort) + kRemotePath), on_(std::move(on)) {
    const tcp::endpoint ep(net::ip::make_address(kRemoteHost), static_cast<unsigned short>(kRemotePort));
    // 抛异常的重载: 端口被占 (另一个实例占着 kRemotePort) 即终止
    acc_.open(ep.protocol());
    acc_.set_option(net::socket_base::reuse_address(true));
    acc_.bind(ep);
    acc_.listen(net::socket_base::max_listen_connections);
    accept();
    th_ = std::thread([this] { ioc_.run(); });
  }
  ~Server() {
    ioc_.stop();
    th_.join();
  }
  const std::string &listen() const { return addr_; }
  // GUI 线程投回答复: 在网络线程上写出
  void post(std::function<void()> f) { net::post(ioc_, std::move(f)); }
  void call(Ask a) { on_(std::move(a)); }

private:
  void accept() {
    acc_.async_accept([this](beast::error_code ec, tcp::socket s) {
      if (ec == net::error::operation_aborted)
        return;
      if (!ec)
        std::make_shared<Conn>(std::move(s), *this)->read();
      accept();
    });
  }

  std::string addr_;
  std::function<void(Ask)> on_;
  net::io_context ioc_{1};
  tcp::acceptor acc_{ioc_};
  std::thread th_;
};

void Conn::handle() {
  if (std::string_view(req_.target().data(), req_.target().size()) != kRemotePath || req_.method() != http::verb::post)
    return send({true, std::string("只有 POST ") + kRemotePath});
  const json m = json::parse(req_.body(), nullptr, false);
  if (m.is_discarded() || !m.is_object() || !m.contains("tool") || !m["tool"].is_string() || (m.contains("args") && !m["args"].is_object()))
    return send({true, "请求须是 {\"tool\": 工具名, \"args\": {...}}"});
  srv_.call({m["tool"].get<std::string>(), m.contains("args") ? m["args"] : json::object(), [self = shared_from_this()](Reply r) {
               self->srv_.post([self, r = std::move(r)] { self->send(r); });
             }});
}

// ---- GUI 线程: 一批 = 一个作业 ----
struct Job {
  size_t next = 0;               // 下一个要开的调用
  std::optional<size_t> running; // 在跑的 (后台) 调用
  bool done = false;
  std::string text; // 做完后的原文
};
struct Waiter {
  int job = 0; // 批号 (1 起)
  double until = 0.0;
  Answer answer;
};

std::string names_of(const std::vector<Choice> &cs) {
  std::string s;
  for (const Choice &c : cs)
    s += (s.empty() ? "" : " / ") + c.name;
  return s;
}

} // namespace

struct Remote::Impl : Rig {
  Clock::time_point t0 = Clock::now();
  std::mutex mu;
  std::deque<Ask> asks;  // 网络线程入, GUI 线程出
  std::vector<Job> jobs; // [批号 − 1] (与时间线同长)
  std::vector<Waiter> waiters;
  Gate gate;
  bool gpu = false, locked = false, fresh = true; // fresh = 下个请求先重绑作用域、重读全局记录 (锁上时置)
  std::unique_ptr<Server> server;                 // 末成员: 析构先停网络线程

  double now() const { return std::chrono::duration<double>(Clock::now() - t0).count(); }
  Seat seat_at(size_t i) { return seat(i, gate, kSession, true, false, gpu); }
  bool busy() const { return (!jobs.empty() && !jobs.back().done) || lab.busy(); }

  std::string finished(int k) const {
    const Round &rd = timeline[static_cast<size_t>(k - 1)];
    return "# 第 " + std::to_string(k) + " 批 (作业 " + std::to_string(k) + ")\n\n" + ResultsText(rd.acts, k) + "# 假设板\n\n" + st.board.text();
  }
  std::string progress(int k) const {
    const Job &j = jobs[static_cast<size_t>(k - 1)];
    const Round &rd = timeline[static_cast<size_t>(k - 1)];
    const size_t i = j.running ? *j.running : j.next;
    std::string at = "第 " + std::to_string(i + 1) + "/" + std::to_string(rd.acts.size()) + " 个调用";
    if (i < rd.acts.size()) {
      const Act &a = rd.acts[i];
      at += " " + CallTitle(a, {k, static_cast<int>(i) + 1});
      if (const TrialRes *r = AsTrial(a))
        at += r->run.began() ? " (阶段 Run" : " (阶段 Search";
      else
        at += " (";
      LabLive ll;
      lab.live(ll);
      at += text::fmt(", 进度 %.0f%%)", ll.frac * 100.0);
    }
    return "作业 " + std::to_string(k) + " 还在跑: " + at + ". 结果等这批全部做完一起给; 用 wait {\"job\": " + std::to_string(k) + "} 续等, 或 cancel";
  }

  // 推进末批: 收后台结果 → 开下一个 (当场的随即做完) → 全部做完即封存原文
  void step() {
    while (std::optional<Lab::Tick> t = lab.poll()) {
      if (jobs.empty() || !jobs.back().running)
        continue;
      Job &j = jobs.back();
      const size_t i = *j.running;
      if (t->step != LabStep::End) {
        step_act(i, t->step, now());
        continue;
      }
      if (t->out.cancelled) {
        abort_act(i, now(), "你");
      } else {
        Seat s = seat_at(i);
        settle_act(i, std::move(t->out.out), s, now());
      }
      j.running.reset(), ++j.next;
    }
    flush(); // 取消掉的回测: Lab 收手后删候选
    if (jobs.empty() || jobs.back().done)
      return;
    Job &j = jobs.back();
    const std::vector<Act> &acts = timeline.back().acts;
    while (!j.running && j.next < acts.size()) {
      Seat s = seat_at(j.next);
      const Exec at = open_act(j.next, s, now());
      assert(at != Exec::Human && "外部 agent 没有 ask_human");
      if (at == Exec::Sync)
        ++j.next;
      else
        j.running = j.next;
    }
    if (!j.running && j.next == acts.size()) {
      j.done = true;
      j.text = finished(round());
    }
  }

  // 末批作废在跑的, 丢掉没开的
  void cancel_job() {
    assert(!jobs.empty() && !jobs.back().done);
    Job &j = jobs.back();
    if (j.running) {
      abort_act(*j.running, now(), "你");
      j.next = *j.running + 1, j.running.reset();
    }
    const size_t total = timeline.back().acts.size();
    timeline.back().acts.resize(j.next);
    j.done = true;
    j.text = finished(round()) + (j.next < total ? "\n(第 " + std::to_string(j.next + 1) + " 个起的 " + std::to_string(total - j.next) + " 个调用被取消, 没执行)\n" : "");
  }

  std::string ready_miss(const SharedData &data, const Scope &sc) const {
    if (!catalog::ready(data, sc, gpu))
      return "作用域 " + catalog::label(sc) + " 的数据未就绪 (资产轴未扫 / 标签库未建): 稍后再试, 或用 scope 换池 / 层";
    const std::vector<Choice> hs = catalog::holds(data, sc.level);
    if (std::none_of(hs.begin(), hs.end(), [&](const Choice &h) { return h.key == sc.hold; }))
      return "持有期 " + factor::stat::hold_name(sc.hold) + " 在本层不可搜 (可选 " + names_of(hs) + "): 用 scope 换";
    return {};
  }
  void bind(const SharedData &data, const Scope &sc) {
    lab.bind(data, sc, false);
    if (fresh || !notes.loaded(false))
      notes.load(false);
    fresh = false;
  }

  std::string scope_text(const SharedData &data, const Scope &sc) const {
    const Context &c = lab.ctx();
    return "作用域: " + catalog::label(sc) + " · 近 " + std::to_string(sc.years) + " 年 (" + c.window + ") · 特征来源 " + c.srcs + " · 开放叶 " +
           std::to_string(c.leaves.size()) + " / 算子 " + std::to_string(c.ops.size()) + " · 回测 " + (gpu ? "GPU" : "CPU") + " · 入库门槛 中位 rIC ≥ " +
           text::num(gate.ric) + "\n可选: 池 " + names_of(catalog::pools()) + "\n层 " + names_of(catalog::levels()) + "\n本层持有期 " +
           names_of(catalog::holds(data, sc.level)) + "\n";
  }

  std::string doc_text(const Scope &sc) const {
    const Context &c = lab.ctx();
    return "# 外部 agent 会话 (随作用域; 通用说明见仓库 AGENT.md)\n\n"
           "- 作用域: " +
           catalog::label(sc) + " · 近 " + std::to_string(sc.years) + " 年 (" + c.window + ") · 特征来源 " + c.srcs + " · 入库门槛 中位 rIC ≥ " + text::num(gate.ric) +
           "\n- 本会话已有 " + std::to_string(round()) + " 批调用, 假设板 " + std::to_string(st.board.all().size()) +
           " 条\n"
           "- 你经 run 交一批调用, 格式即下文 \"回复协议\" 里的 {\"calls\": [...]} (只交 calls 数组); 结果原文同下文 \"结果\", 调用编号 R<批>#<序> 沿本会话往下记, judge 的证据照样引用它\n"
           "- 与下文 GUI 会话的差别: 没有会话约束 (eval_factor 的 hyp 可不在假设板上, 回测后不强制先诊断、不强制 judge 待判的, 但研究循环照样是正确的做法); "
           "没有 ask_human 与 finish (要问研究员写 report_issue); 上下文 / 研究树不适用 (结果只在 run / wait 里给一次, 自己记); "
           "submit_factor 达门槛即真入正式库, 全局记录真写\n"
           "- 长调用 (eval_factor / 观察) " +
           std::to_string(kRemoteWait) + " 秒内没做完, run 先交回作业号, 用 wait 续取, cancel 取消; 一次只跑一批\n- Agent 模块已替你锁上, 研究员不会干扰; 用完调 unlock 交还\n\n" +
           SystemMessage(c, gate, false).text;
  }

  void handle(const SharedData &data, Scope &sc, bool human, Ask a) {
    const auto fail = [&](std::string why) { a.answer({true, std::move(why)}); };
    if (a.tool == "unlock") {
      if (!locked)
        return a.answer({false, "本来就没锁"});
      if (busy())
        return fail("作业 " + std::to_string(round()) + " 还在跑: 先 wait 做完或 cancel, 再 unlock");
      locked = false;
      return a.answer({false, "已解锁, Agent 模块交还研究员. 你的下个请求会再自动锁上 (研究员那时没在用的话); 本会话的批号与假设板照旧往下记"});
    }
    if (!locked) {
      if (human)
        return fail("研究员正在 GUI 里用 Agent 模块 (有会话开着 / 工作台在跑), 没替你锁上: 稍后再试");
      locked = fresh = true;
      a.answer = [f = std::move(a.answer)](Reply r) {
        r.text = kLocked + r.text;
        f(std::move(r));
      };
    }
    const json &x = a.args;
    if (a.tool == "wait" || a.tool == "cancel") {
      if (!x.contains("job") || !x["job"].is_number_integer())
        return fail(a.tool + " 须给整数 job");
      const int k = x["job"].get<int>();
      if (k < 1 || k > round())
        return fail("没有作业 " + std::to_string(k) + (round() ? " (已有 1.." + std::to_string(round()) + ")" : " (还没有作业)"));
      if (a.tool == "cancel") {
        if (jobs[static_cast<size_t>(k - 1)].done)
          return fail("作业 " + std::to_string(k) + " 已做完, 不用取消");
        cancel_job();
        return a.answer({false, "已取消\n\n" + jobs.back().text});
      }
      int sec = kRemoteWait;
      if (x.contains("seconds")) {
        if (!x["seconds"].is_number_integer())
          return fail("seconds 须是整数");
        sec = std::clamp(x["seconds"].get<int>(), 1, kRemoteWaitMax);
      }
      waiters.push_back({k, now() + sec, std::move(a.answer)});
      return;
    }
    if (std::string why = ready_miss(data, sc); !why.empty())
      return fail(std::move(why));
    if (!busy() && (fresh || lab.scope() != sc))
      bind(data, sc);
    if (lab.scope() != sc)
      return fail("作用域在换, 但上一批还在收尾: 稍后再试");
    if (a.tool == "doc")
      return a.answer({false, doc_text(sc)});
    if (a.tool == "scope")
      return scope(data, sc, std::move(a));
    if (a.tool == "run")
      return run(std::move(a));
    fail("没有工具 " + a.tool + " (只有 " + kNames + ")");
  }

  void scope(const SharedData &data, Scope &sc, Ask a) {
    const auto fail = [&](std::string why) { a.answer({true, std::move(why)}); };
    const json &x = a.args;
    Scope next = sc;
    // 名 → 选项 (缺 = nullptr 照旧; 不合写 why)
    std::string why;
    const auto pick = [&](const char *key, const std::vector<Choice> &cs) -> const Choice * {
      if (!x.contains(key) || !why.empty())
        return nullptr;
      const auto it = !x[key].is_string() ? cs.end() : std::find_if(cs.begin(), cs.end(), [&](const Choice &c) { return c.name == x[key].get_ref<const std::string &>(); });
      if (it == cs.end())
        why = std::string(key) + " 须是这些之一: " + names_of(cs);
      return it == cs.end() ? nullptr : &*it;
    };
    if (const Choice *p = pick("pool", catalog::pools()))
      next.pool = p->name;
    if (const Choice *l = pick("level", catalog::levels()))
      next.level = l->key;
    const std::vector<Choice> hs = catalog::holds(data, next.level);
    if (const Choice *h = pick("hold", hs))
      next.hold = h->key;
    if (!why.empty())
      return fail(std::move(why));
    if (std::none_of(hs.begin(), hs.end(), [&](const Choice &h) { return h.key == next.hold; }))
      return fail("持有期 " + factor::stat::hold_name(next.hold) + " 在该层不可搜, 一并给 hold (可选 " + names_of(hs) + ")");
    if (x.contains("years")) {
      if (!x["years"].is_number_integer() || x["years"].get<int>() < 1)
        return fail("years 须是 ≥ 1 的整数");
      next.years = x["years"].get<int>();
    }
    if (x.contains("lob")) {
      if (!x["lob"].is_boolean())
        return fail("lob 须是布尔");
      next.lob = x["lob"].get<bool>();
    }
    if (next != sc) {
      if (busy())
        return fail("作业 " + std::to_string(round()) + " 还在跑: 先 wait 做完或 cancel, 再换作用域");
      if (std::string miss = ready_miss(data, next); !miss.empty())
        return fail(std::move(miss));
      sc = next;
      bind(data, sc);
    }
    a.answer({false, scope_text(data, sc)});
  }

  void run(Ask a) {
    if (busy())
      return a.answer({true, "作业 " + std::to_string(round()) + " 还在跑: 一次只跑一批, 先 wait 或 cancel"});
    std::vector<Call> cs;
    std::string err;
    if (!a.args.contains("calls") || !ParseCalls(json{{"calls", a.args["calls"]}}.dump(), false, cs, err))
      return a.answer({true, "整批退回 (一个都没执行), 不合处:\n" + (err.empty() ? std::string("缺 calls") : err)});
    if (std::ranges::any_of(cs, [](const Call &c) { return c.tool == Tool::Finish; }))
      return a.answer({true, "整批退回 (一个都没执行): 外部 agent 会话没有 finish, 去掉它"});
    Round rd{.kind = Msg::Round};
    for (Call &c : cs)
      rd.acts.push_back({.call = std::move(c)});
    timeline.push_back(std::move(rd));
    jobs.emplace_back();
    waiters.push_back({round(), now() + kRemoteWait, std::move(a.answer)});
    step();
  }

  void answer_waiters() {
    const double t = now();
    std::erase_if(waiters, [&](Waiter &w) {
      const Job &j = jobs[static_cast<size_t>(w.job - 1)];
      if (!j.done && t < w.until)
        return false;
      w.answer({false, j.done ? j.text : progress(w.job)});
      return true;
    });
  }
};

Remote::Remote() : impl_(std::make_unique<Impl>()) {
  Impl &m = *impl_;
  m.server = std::make_unique<Server>([&m](Ask a) {
    std::lock_guard<std::mutex> lock(m.mu);
    m.asks.push_back(std::move(a));
  });
  disk::write(disk::RunAgentRemote, {}, json{{"url", m.server->listen()}}.dump(2));
}

Remote::~Remote() {
  Impl &m = *impl_;
  {
    std::lock_guard<std::mutex> lock(m.mu);
    for (Ask &a : m.asks)
      a.answer({true, "程序在退出"});
    m.asks.clear();
  }
  for (Waiter &w : m.waiters)
    w.answer({true, "程序在退出"});
  m.waiters.clear();
  m.server.reset();
}

void Remote::update(SharedData &data, Scope &sc, const Gate &gate, bool gpu, bool human) {
  Impl &m = *impl_;
  m.gate = gate, m.gpu = gpu;
  m.step();
  std::deque<Ask> got;
  {
    std::lock_guard<std::mutex> lock(m.mu);
    got.swap(m.asks);
  }
  for (Ask &a : got)
    m.handle(data, sc, human, std::move(a));
  m.answer_waiters();
}

bool Remote::locked() const { return impl_->locked; }
bool Remote::busy() const { return impl_->busy(); }
const std::string &Remote::listen() const { return impl_->server->listen(); }
const std::vector<Round> &Remote::timeline() const { return impl_->timeline; }
void Remote::live(LabLive &dst) const { impl_->lab.live(dst); }

} // namespace GUI::Agent
