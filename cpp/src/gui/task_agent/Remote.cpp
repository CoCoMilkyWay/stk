// Remote — 见头文件
#include "gui/task_agent/Remote.hpp"
#include "gui/task_agent/Config.hpp"
#include "gui/task_agent/Prompt.hpp"
#include "gui/task_agent/Text.hpp"
#include "gui/task_agent/tools/Trial.hpp"

#include "disk/Disk.hpp"
#include "nlohmann/json.hpp"

#include <boost/asio.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <limits>
#include <mutex>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace GUI::Agent {

namespace {

namespace beast = boost::beast;
namespace http = boost::beast::http;
namespace net = boost::asio;
using tcp = boost::asio::ip::tcp;
using json = nlohmann::ordered_json;
using Clock = std::chrono::steady_clock;

constexpr std::string_view kProtocols[] = {"2025-06-18", "2025-03-26", "2024-11-05"}; // 认的 MCP 版本 (首个 = 客户端要的不认时给的)
constexpr const char *kInstructions =
    "A 股量化因子挖掘系统的研究工具: 你当挖掘会话的大脑 (说明书 = 仓库 AGENT.md). 先 open 开会话 (用户交代的要求原话写进 remark) 拿首条消息, 再 doc 读系统提示; "
    "此后每批研究工具调用用 run 交, 交回的就是下一条消息 (常规为本条增量; 接上历史会话或研究员回退后自动附完整研究树), 超时未做完调无参 run 续等; 接上已有外部会话也用无参 run; 挖完 finish. "
    "规矩 (同内置模型): 只经 open / doc / run 取信息与行动 (不读仓库与 output, 不上网); 不并行调 run; 一直循环到 finish、ask_human 或会话停下 (ask_human / 停下时等研究员在 GUI 处理后再调无参 run 接着做).";

// 一个接口请求: 工具 (open / doc / run) + 参数; 答复由 GUI 线程给, 网络线程写出
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

json schema(json props, json required) {
  json s = {{"type", "object"}, {"properties", std::move(props)}};
  if (!required.empty())
    s["required"] = std::move(required);
  s["additionalProperties"] = false;
  return s;
}
json tool(const char *name, std::string desc, json input) { return {{"name", name}, {"description", std::move(desc)}, {"inputSchema", std::move(input)}}; }

// tools/list: 只有 open / doc / run (建一次); 研究工具只经 run 的 calls 交, 工具名与参数表唯一以系统提示 (doc) 为准
json tools_list() {
  json ts = json::array();
  ts.push_back(tool("open",
                    "开新挖掘会话, 你当大脑 (同研究员在 GUI 里按 New: 有会话在跑则退回, 停着的已存盘会话换下): "
                    "作用域可给 pool / level / hold / years / lob (缺省 = 研究员在 GUI 里选的), remark = 用户交代的本会话要求 (原话), test = 自检会话; "
                    "门槛 / 提问 / 会话约束 / 预算取研究员在 Config 页定的. 交回首条消息; 系统提示用 doc 取. 接上已有外部会话用无参 run",
                    schema({{"pool", {{"type", "string"}, {"description", "池名 (all 或子池)"}}},
                            {"level", {{"type", "string"}, {"description", "层: min / 5min / 15min / 30min / 60min / day"}}},
                            {"hold", {{"type", "string"}, {"description", "持有期: 30m / 60m / close / T+1C (该层能用的)"}}},
                            {"years", {{"type", "integer"}, {"minimum", 1}, {"description", "评估期年数"}}},
                            {"lob", {{"type", "boolean"}, {"description", "开放 lob 特征叶"}}},
                            {"remark", {{"type", "string"}, {"description", "本会话要求: 用户交代的原话 (只对这一段会话; 与系统提示的做法冲突处以它为准)"}}},
                            {"test", {{"type", "boolean"}, {"default", false}, {"description", "自检会话: 把每个工具调一遍, 问题记 report_issue; 候选 / 日志 / 全局记录进 test 目录, 提交只查门槛"}}}},
                           json::array())));
  ts.push_back(tool("doc", "本会话的系统提示: 角色与做法 / 评估口径 / 判优 / 研究循环 / 调用协议 / 研究工具表 (参数逐个) / 开放的特征叶 (按族) 与算子. open 后先读",
                    schema(json::object(), json::array())));
  ts.push_back(tool("run",
                    "给 calls = 交一批研究工具调用, 按序执行; 不给 calls (空参数 {}) = 接上已有外部会话 / 续等执行中的调用 / 取当前状态. "
                    "后台执行中至多等 " +
                        std::to_string(kRemoteWait) +
                        " 秒 (未完交回进度, 再调无参 run 续等); 做完即交回下一条消息 (常规为本条增量: 各调用结果 + 假设板 + 下一步; 接上有历史的会话或研究员回退后自动附完整研究树); "
                        "遇到 ask_human 或会话停下即刻交回状态 (finish 收尾那批的结果自动附一次). 不合协议或不守会话约束 → 整批退回, 交回重发要求 (isError)",
                    schema({{"calls",
                             {{"type", "array"},
                              {"minItems", 1},
                              {"maxItems", kMaxCalls},
                              {"description", "调用序列 (不给 = 接上 / 续等 / 取状态); 工具名与参数见系统提示 (doc) 的研究工具表"},
                              {"items", schema({{"tool", {{"type", "string"}}}, {"args", {{"type", "object"}}}}, json::array({"tool"}))}}}},
                           json::array())));
  return {{"tools", std::move(ts)}};
}

json initialize(const json &p) {
  const std::string want = p.is_object() && p.contains("protocolVersion") && p["protocolVersion"].is_string() ? p["protocolVersion"].get<std::string>() : std::string();
  json caps = json::object();
  caps["tools"] = {{"listChanged", false}};
  return {{"protocolVersion", std::ranges::contains(kProtocols, std::string_view(want)) ? want : std::string(kProtocols[0])},
          {"capabilities", std::move(caps)},
          {"serverInfo", {{"name", "stk-agent"}, {"version", "1"}}},
          {"instructions", kInstructions}};
}

// 本机的 Origin (scheme://host[:port]): 127.0.0.1 / localhost / [::1]
bool local_origin(std::string_view o) {
  if (const size_t p = o.find("://"); p != std::string_view::npos)
    o.remove_prefix(p + 3);
  for (const std::string_view h : {std::string_view("127.0.0.1"), std::string_view("localhost"), std::string_view("[::1]")})
    if (o.starts_with(h) && (o.size() == h.size() || o[h.size()] == ':' || o[h.size()] == '/'))
      return true;
  return false;
}

// ---- 网络线程: POST JSON-RPC → JSON-RPC ----
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
  void result(const json &id, json r) { send(http::status::ok, json{{"jsonrpc", "2.0"}, {"id", id}, {"result", std::move(r)}}.dump()); }
  void error(const json &id, int code, std::string msg) {
    send(http::status::ok, json{{"jsonrpc", "2.0"}, {"id", id}, {"error", {{"code", code}, {"message", std::move(msg)}}}}.dump());
  }
  void send(http::status st, std::string body) {
    auto res = std::make_shared<http::response<http::string_body>>(st, req_.version());
    res->keep_alive(req_.keep_alive());
    if (st == http::status::method_not_allowed)
      res->set(http::field::allow, "POST");
    if (!body.empty())
      res->set(http::field::content_type, "application/json");
    res->body() = std::move(body);
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
      : addr_(std::string("http://") + kRemoteHost + ":" + std::to_string(kRemotePort) + kRemotePath), tools_(tools_list()), on_(std::move(on)) {
    const tcp::endpoint ep(net::ip::make_address(kRemoteHost), static_cast<unsigned short>(kRemotePort));
    // 抛异常的重载: 端口被占 (另一个实例占着 kRemotePort) 即终止
    acc_.open(ep.protocol());
    acc_.set_option(net::socket_base::reuse_address(true));
    acc_.bind(ep);
    acc_.listen(net::socket_base::max_listen_connections);
    accept();
    th_ = std::thread([this] { ioc_.run(); });
  }
  ~Server() { stop(); }
  // 停网络线程但不销毁 io_context (外部持有的 Conn 可安全析构后再析构 Server)
  void stop() {
    if (!th_.joinable())
      return;
    ioc_.stop();
    th_.join();
  }
  const std::string &listen() const { return addr_; }
  const json &tools() const { return tools_; }
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
  const json tools_; // 建好即只读 (网络线程读)
  std::function<void(Ask)> on_;
  net::io_context ioc_{1};
  tcp::acceptor acc_{ioc_};
  std::thread th_;
};

void Conn::handle() {
  if (std::string_view(req_.target().data(), req_.target().size()) != kRemotePath)
    return send(http::status::not_found, {});
  if (const auto o = req_.find(http::field::origin); o != req_.end() && !local_origin(std::string_view(o->value().data(), o->value().size())))
    return send(http::status::forbidden, {});
  if (req_.method() != http::verb::post)
    return send(http::status::method_not_allowed, {});
  const json m = json::parse(req_.body(), nullptr, false);
  if (m.is_discarded())
    return error(nullptr, -32700, "不是合法的 JSON");
  if (!m.is_object() || m.value("jsonrpc", std::string()) != "2.0")
    return error(nullptr, -32600, "须是一条 JSON-RPC 2.0 消息 (不收批量)");
  if (!m.contains("id") || !m.contains("method"))
    return send(http::status::accepted, {}); // 通知 / 应答: 这里不发请求, 收下即可
  const json &id = m["id"];
  if (!m["method"].is_string())
    return error(id, -32600, "method 须是字符串");
  const std::string &method = m["method"].get_ref<const std::string &>();
  const json params = m.contains("params") ? m["params"] : json::object();
  if (method == "initialize")
    return result(id, initialize(params));
  if (method == "ping")
    return result(id, json::object());
  if (method == "tools/list")
    return result(id, srv_.tools());
  if (method != "tools/call")
    return error(id, -32601, "没有方法 " + method + " (有 initialize / ping / tools/list / tools/call)");
  if (!params.is_object() || !params.contains("name") || !params["name"].is_string() ||
      (params.contains("arguments") && !params["arguments"].is_null() && !params["arguments"].is_object()))
    return error(id, -32602, "tools/call 的 params 须是 {\"name\": 工具名, \"arguments\": {...}}");
  const json args = params.contains("arguments") && params["arguments"].is_object() ? params["arguments"] : json::object();
  srv_.call({params["name"].get<std::string>(), args, [self = shared_from_this(), id](Reply r) {
               self->srv_.post([self, id, r = std::move(r)] {
                 json content = json::array();
                 content.push_back({{"type", "text"}, {"text", r.text}});
                 self->result(id, {{"content", std::move(content)}, {"isError", r.error}});
               });
             }});
}

// ---- GUI 线程: 外部 agent 当会话的大脑 ----
// 后台调用执行中在等的请求
struct Waiter {
  std::string dir; // 哪段会话 (会话目录)
  Clock::time_point until;
  Answer answer;
};

std::string names_of(const std::vector<Choice> &cs) {
  std::string s;
  for (const Choice &c : cs)
    s += (s.empty() ? "" : " / ") + c.name;
  return s;
}

// 后台调用执行中的进度 (超时交回)
std::string progress(const Mine &m) {
  const std::vector<Act> &as = m.timeline.back().acts;
  const size_t i = m.acting();
  std::string at = "第 " + std::to_string(i + 1) + "/" + std::to_string(as.size()) + " 个调用";
  if (i < as.size()) {
    const Act &a = as[i];
    at += " " + CallTitle(a, {m.round(), static_cast<int>(i) + 1});
    const TrialRes *r = AsTrial(a);
    LabLive ll;
    m.lab.live(ll);
    if (r)
      at += std::string(r->run.began() ? " (阶段 Run" : " (阶段 Search") + text::fmt(", 进度 %.0f%%)", ll.frac * 100.0);
    else
      at += ll.frac > 0.0 ? text::fmt(" (进度 %.0f%%)", ll.frac * 100.0) : std::string(" (执行中)");
  }
  return "第 " + std::to_string(m.round()) + " 轮还在跑: " + at + ". 结果等本批全部做完一起给; 调无参 run 续等";
}

Reply choosing(const Mine &m) {
  return {false, "第 " + std::to_string(m.round()) + " 轮的 ask_human 已发到 GUI, 请研究员在 Mine 页点选并按 交答案. 答完后再调无参 run 获取答案与下一条消息."};
}

} // namespace

struct Remote::Impl {
  std::unique_ptr<Server> server; // 首成员: ~Remote 先 stop() 停线程, 再清 asks/waiters 释放 Conn, 最后销毁 io_context
  std::mutex mu;
  std::deque<Ask> asks; // 网络线程入, GUI 线程出
  std::vector<Waiter> waiters;
  struct Cursor {
    std::string dir;
    int rev = 0;
    int round = 0;
  } synced;                                // 外部 agent 已连续收到的时间线位点 (断链 / 回退时自动补完整研究树)
  std::pair<std::string, int> finish_sent; // 已交回过 finish 末批结果的 (会话目录, 轮次)

  void enqueue(const Mine &m, Answer answer) {
    waiters.push_back({m.log.dir(), Clock::now() + std::chrono::seconds(kRemoteWait), std::move(answer)});
  }

  // 停着的交回: 停下原因; finish 结束时恰好附一次末批调用的结果 (之后再调只给结束提示)
  Reply halted(const Mine &m) {
    if (!m.flags().done)
      return {false, "会话停下: " + m.reason + ". 等研究员在 GUI 处理 (继续 / 回退 / 关闭) 后再调无参 run 接着做."};
    const std::pair<std::string, int> key{m.log.dir(), m.round()};
    const bool fresh = finish_sent != key;
    finish_sent = key;
    return {false, (fresh ? ResultsText(m.timeline.back().acts, m.round()) : std::string()) + "会话结束: " + m.reason + ". 要再挖就 open 开新会话."};
  }

  // 交回待答消息: 连续推进只给本条增量, 首次接上有历史轮次的会话或研究员回退后补完整研究树; 重发要求 = isError
  Reply deliver(const Mine &m) {
    const bool continuous = synced.dir == m.log.dir() && synced.rev == m.rev() && (synced.round == m.round() - 1 || synced.round == m.round());
    synced = {m.log.dir(), m.rev(), m.round()};
    const std::string body = continuous || m.round() <= 1 ? TextOf(m.timeline.back()) : llm::Flat(TurnsOf(m.timeline));
    return {m.timeline.back().kind == Msg::Retry, body};
  }

  // 会话此刻交给外部 agent 的: 等交批 → 消息; ask_human → 等人提示; 执行中 → 进度; 停着 → 停下原因
  Reply state(const Mine &m) {
    if (m.waiting() && m.idle())
      return deliver(m);
    if (m.phase() == Mine::Phase::Choose)
      return choosing(m);
    if (m.phase() == Mine::Phase::Act)
      return {false, progress(m)};
    if (m.phase() == Mine::Phase::Pause)
      return halted(m);
    return {false, "上一批还在收尾: 稍后再试"};
  }

  // 在等的 (均为 Act): 关掉 / 换会话 / 不再是外部会话 → 报错; 离开 Act (做完 / ask_human / 停下) 或超时 → 交回此刻的状态
  void answer_waiters(const Mine &m) {
    const Clock::time_point now = Clock::now();
    std::erase_if(waiters, [&](const Waiter &w) {
      const auto done = [&](Reply r) {
        w.answer(std::move(r));
        return true;
      };
      if (!m.held() || m.log.dir() != w.dir)
        return done({true, "会话已被研究员关掉或换了一段: 要接着挖就 open"});
      if (!m.remote())
        return done({true, "会话不再由外部 agent 驱动"});
      if (m.phase() != Mine::Phase::Act || now >= w.until)
        return done(state(m));
      return false;
    });
  }

  // 交一批调用: 进后台执行则等, 其余 (当场做完 / 重发 / ask_human / 停下) 立刻交回
  void submit(Mine &m, json calls, Answer answer) {
    if (!m.waiting() || !m.idle())
      return answer({true, "这批没收下: " + state(m).text});
    m.reply(json{{"calls", std::move(calls)}}.dump());
    if (m.phase() == Mine::Phase::Act)
      return enqueue(m, std::move(answer));
    answer(state(m));
  }

  // 无参 run: 后台执行中则续等, 其余 (待答 / ask_human / 停着) 立刻交回状态
  void poll(const Mine &m, Answer answer) {
    if (m.phase() == Mine::Phase::Act)
      return enqueue(m, std::move(answer));
    answer(state(m));
  }

  // 同 GUI 的 New 开新外部会话: 有会话在跑则退回, 停着的 (已存盘) 换下
  void open(SharedData &data, Mine &m, const Mine::Setup &base, const Mine::Leg &leg, bool bench, Ask a) {
    const auto fail = [&](std::string why) { a.answer({true, std::move(why)}); };
    const json &x = a.args;
    if (m.live())
      return fail("有会话在跑 (" + catalog::label(m.setup.sc) + " · 大脑 " + m.brain() + "): " +
                  (m.remote() ? "这是外部会话, 调无参 run 接上; 要另开先 finish 或请研究员停下" : "研究员在用, 等他停下再 open"));
    if (bench)
      return fail("研究员在用工作台 (Tools 页有调用在跑): 稍后再试");
    if (!m.idle())
      return fail("上一段会话还在收尾: 稍后再试");
    Mine::Setup s = base;
    s.remote = true;
    std::string why;
    constexpr std::string_view keys[] = {"pool", "level", "hold", "years", "lob", "remark", "test"};
    for (const auto &[k, v] : x.items())
      if (!std::ranges::contains(keys, std::string_view(k)))
        why += (why.empty() ? "" : "; ") + ("没有参数 " + k);
    // 名 → 选项 (缺 = 照研究员选的)
    const auto pick = [&](const char *key, const std::vector<Choice> &cs) -> const Choice * {
      if (!x.contains(key))
        return nullptr;
      const auto it = !x[key].is_string() ? cs.end() : std::find_if(cs.begin(), cs.end(), [&](const Choice &c) { return c.name == x[key].get_ref<const std::string &>(); });
      if (it == cs.end())
        why += (why.empty() ? "" : "; ") + (std::string(key) + " 须是这些之一: " + names_of(cs));
      return it == cs.end() ? nullptr : &*it;
    };
    if (const Choice *p = pick("pool", catalog::pools()))
      s.sc.pool = p->name;
    const Choice *lv = pick("level", catalog::levels());
    if (lv)
      s.sc.level = lv->key;
    const std::vector<Choice> hs = catalog::holds(data, s.sc.level);
    if (const Choice *h = pick("hold", hs))
      s.sc.hold = h->key;
    else if (lv && !hs.empty() && std::none_of(hs.begin(), hs.end(), [&](const Choice &c) { return c.key == s.sc.hold; }))
      s.sc.hold = hs.front().key;
    if (x.contains("years")) {
      const int64_t y = x["years"].is_number_integer() ? x["years"].get<int64_t>() : 0;
      if (y < 1 || y > std::numeric_limits<int>::max())
        why += (why.empty() ? "" : "; ") + std::string("years 须是 ≥ 1 的整数");
      else
        s.sc.years = static_cast<int>(y);
    }
    const std::pair<const char *, bool *> flags[] = {{"lob", &s.sc.lob}, {"test", &s.test}};
    for (const auto &[key, to] : flags)
      if (x.contains(key)) {
        if (!x[key].is_boolean())
          why += (why.empty() ? "" : "; ") + (std::string(key) + " 须是布尔");
        else
          *to = x[key].get<bool>();
      }
    if (x.contains("remark")) {
      if (!x["remark"].is_string())
        why += (why.empty() ? "" : "; ") + std::string("remark 须是字符串");
      else
        s.remark = std::string(text::trim(x["remark"].get_ref<const std::string &>()));
    }
    if (!why.empty())
      return fail("没开成, 参数不合: " + why);
    Mine::Leg l = leg;
    l.llm = {};
    if (std::string err = m.try_open(data, std::move(s), std::move(l)); !err.empty())
      return fail(std::move(err));
    a.answer(deliver(m));
  }

  void handle(SharedData &data, Mine &m, const Mine::Setup &base, const Mine::Leg &leg, bool bench, Ask a) {
    const auto fail = [&](std::string why) { a.answer({true, std::move(why)}); };
    const json &x = a.args;
    if (a.tool == "open")
      return open(data, m, base, leg, bench, std::move(a));
    if (!m.remote())
      return fail(m.held() ? "研究员在 GUI 里有内置会话 (大脑 " + m.brain() + "): 停着时可 open 开新会话, 在跑时等他停下" : std::string("还没有外部会话: 先 open"));
    if (a.tool == "doc")
      return x.empty() ? a.answer({false, m.system}) : fail("doc 不收参数");
    if (a.tool == "run") {
      for (const auto &[k, v] : x.items())
        if (k != "calls")
          return fail("run 没有参数 " + k + " (只有可选的 calls)");
      return x.contains("calls") ? submit(m, x["calls"], std::move(a.answer)) : poll(m, std::move(a.answer));
    }
    fail("没有工具 " + a.tool + " (只有 open / doc / run; 研究工具放进 run 的 calls 交)");
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
  disk::remove(disk::RunAgentRemote);
  m.server->stop(); // 先停网络线程: 此后再无新请求入队, io_context 尚在, 下方 Conn 安全析构
  m.asks.clear();
  m.waiters.clear();
  m.server.reset();
}

void Remote::update(SharedData &data, Mine &m, const Mine::Setup &setup, const Mine::Leg &leg, bool bench) {
  Impl &r = *impl_;
  std::deque<Ask> got;
  {
    std::lock_guard<std::mutex> lock(r.mu);
    got.swap(r.asks);
  }
  for (Ask &a : got)
    r.handle(data, m, setup, leg, bench, std::move(a));
  r.answer_waiters(m);
}

const std::string &Remote::listen() const { return impl_->server->listen(); }

} // namespace GUI::Agent
