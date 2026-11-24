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

constexpr std::string_view kProtocols[] = {"2025-06-18", "2025-03-26", "2024-11-05"}; // 认的 MCP 版本 (首个 = 客户端要的不认时给的)
constexpr const char *kInstructions =
    "A 股量化因子挖掘系统的研究工具: 你当挖掘会话的大脑 (说明书 = 仓库 AGENT.md). 先 open 开会话 (用户交代的要求原话写进 remark) 拿首条消息, 再 doc 读系统提示; "
    "此后每批调用用 run 交 (或直接调某个研究工具 = 只有它一个调用的批), 交回的就是下一条消息, 没做完用 wait 续等; 挖完 finish.";
constexpr const char *kFaces = "open / doc / recap / run / wait / cancel"; // 接口工具

// 一个接口请求: 工具 (接口工具或研究工具名) + 参数; 答复由 GUI 线程给, 网络线程写出
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

// tools/list: 接口工具 + 研究工具 (只由编译期的表出, 建一次)
json tools_list() {
  const json job = {{"type", "integer"}, {"minimum", 1}, {"description", "批号 (= 交批的那一轮, 结果标题 R<批>#<序> 里的)"}};
  json ids = json::array();
  for (const Tool t : kVals<Tool>)
    ids.push_back(def(t).id);
  json ts = json::array();
  ts.push_back(tool("open",
                    "开挖掘会话, 你当大脑: 作用域可给 pool / level / hold / years / lob (缺省 = 研究员在 GUI 里选的), remark = 用户交代的本会话要求 (原话), test = 自检会话; "
                    "门槛 / 提问 / 会话约束 / 预算取研究员在 Config 页定的. 交回首条消息; 系统提示用 doc 取. "
                    "研究员已开好一段在等外部 agent 的会话时 = 接上它, 交回在等的那条消息 (参数不用)",
                    schema({{"pool", {{"type", "string"}, {"description", "池名 (all 或子池)"}}},
                            {"level", {{"type", "string"}, {"description", "层: min / 5min / 15min / 30min / 60min / day"}}},
                            {"hold", {{"type", "string"}, {"description", "持有期: 30m / 60m / close / T+1C (该层能用的)"}}},
                            {"years", {{"type", "integer"}, {"minimum", 1}, {"description", "评估期年数"}}},
                            {"lob", {{"type", "boolean"}, {"description", "开放 lob 特征叶"}}},
                            {"remark", {{"type", "string"}, {"description", "本会话要求: 用户交代的原话 (只对这一段会话; 与系统提示的做法冲突处以它为准)"}}},
                            {"test", {{"type", "boolean"}, {"default", false}, {"description", "自检会话: 把每个工具调一遍, 问题记 report_issue; 候选 / 日志 / 全局记录进 test 目录, 提交只查门槛"}}}},
                           json::array())));
  ts.push_back(tool("doc", "本会话的系统提示: 角色与做法 / 评估口径 / 判优 / 研究循环 / 回复协议 / 研究工具表 (参数逐个) / 开放的特征叶 (按族) 与算子 (约 7 万字). open 后先读",
                    schema(json::object(), json::array())));
  ts.push_back(tool("recap", "研究树: 此前各轮的假设与判定、变体结局、观察的关键数、记录 (内置会话每条消息附的同一份). 上下文丢了、或接上进行中的会话时看",
                    schema(json::object(), json::array())));
  ts.push_back(tool("run",
                    "交一批研究工具调用 (= 系统提示里的一条回复), 按序执行; 做完即交回下一条消息 (各调用结果 + 假设板 + 下一步), " + std::to_string(kRemoteWait) +
                        " 秒没做完交回进度 (用 wait 续等). 不合协议或不守会话约束 → 整批退回, 交回重发要求 (isError)",
                    schema({{"calls",
                             {{"type", "array"},
                              {"minItems", 1},
                              {"maxItems", kMaxCalls},
                              {"description", "调用序列; 各研究工具的参数见同名工具的 inputSchema"},
                              {"items", schema({{"tool", {{"type", "string"}, {"enum", ids}}}, {"args", {{"type", "object"}}}}, json::array({"tool"}))}}}},
                           json::array({"calls"}))));
  ts.push_back(tool("wait", "续等一个批: 下一条消息发出即交回它; 会话停下 / 被关掉 / 被回退即交回原因; 超时交回进度",
                    schema({{"job", job}, {"seconds", {{"type", "integer"}, {"minimum", 1}, {"maximum", kRemoteWaitMax}, {"default", kRemoteWait}, {"description", "至多等几秒"}}}},
                           json::array({"job"}))));
  ts.push_back(tool("cancel", "作废在跑的批余下的调用: 在跑的作废 (落了文件的回测删掉候选), 没开的不做; 交回下一条消息", schema({{"job", job}}, json::array({"job"}))));
  for (const Tool t : kVals<Tool>)
    ts.push_back(tool(def(t).id, std::string(def(t).cn) + ": " + ToolText(doc(t).desc) + "\n返回: " + ToolText(doc(t).out) + "\n(直接调 = 只有它一个调用的批, 同 run)", ArgsSchema(t)));
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
  ~Server() {
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
  if (!params.is_object() || !params.contains("name") || !params["name"].is_string() || (params.contains("arguments") && !params["arguments"].is_object()))
    return error(id, -32602, "tools/call 的 params 须是 {\"name\": 工具名, \"arguments\": {...}}");
  srv_.call({params["name"].get<std::string>(), params.contains("arguments") ? params["arguments"] : json::object(), [self = shared_from_this(), id](Reply r) {
               self->srv_.post([self, id, r = std::move(r)] {
                 json content = json::array();
                 content.push_back({{"type", "text"}, {"text", r.text}});
                 self->result(id, {{"content", std::move(content)}, {"isError", r.error}});
               });
             }});
}

// ---- GUI 线程: 外部 agent 当会话的大脑 ----
// 一批交出后在等下一条消息的
struct Waiter {
  std::string dir;     // 哪段会话 (会话目录)
  int job = 0;         // 批号 = 交批的那一轮
  bool paused = false; // 等起时会话就停着 (之后不因停着提前交回)
  Clock::time_point until;
  Answer answer;
};

std::string names_of(const std::vector<Choice> &cs) {
  std::string s;
  for (const Choice &c : cs)
    s += (s.empty() ? "" : " / ") + c.name;
  return s;
}

std::string job_hint(int k) { return "wait {\"job\": " + std::to_string(k) + "}"; }

// 第 k 批此刻的样子 (还没交回下一条消息时)
std::string progress(const Mine &m, int k) {
  if (m.phase() == Mine::Phase::Pause)
    return "会话停下: " + m.reason + ". 研究员按继续后, 下一条消息由 " + job_hint(k) + " 交回; 研究员也可能关掉这段会话";
  if (m.phase() != Mine::Phase::Act && m.phase() != Mine::Phase::Choose)
    return "第 " + std::to_string(k) + " 批在收尾: 用 " + job_hint(k) + " 续等";
  const std::vector<Act> &as = m.timeline.back().acts;
  const size_t i = m.acting();
  std::string at = "第 " + std::to_string(i + 1) + "/" + std::to_string(as.size()) + " 个调用";
  if (i < as.size()) {
    const Act &a = as[i];
    at += " " + CallTitle(a, {k, static_cast<int>(i) + 1});
    if (m.phase() == Mine::Phase::Choose) {
      at += " (ask_human: 等研究员在 GUI 里答)";
    } else {
      const TrialRes *r = AsTrial(a);
      LabLive ll;
      m.lab.live(ll);
      at += (r ? std::string(r->run.began() ? " (阶段 Run" : " (阶段 Search") : std::string(" (")) + text::fmt(", 进度 %.0f%%)", ll.frac * 100.0);
    }
  }
  return "第 " + std::to_string(k) + " 批还在跑: " + at + ". 结果等这批全部做完随下一条消息一起给; 用 " + job_hint(k) + " 续等, 或 cancel";
}

// 外部 agent 的会话此刻交不了批的原因
std::string busy_text(const Mine &m) {
  switch (m.phase()) {
  case Mine::Phase::Act:
  case Mine::Phase::Choose:
  case Mine::Phase::Pause:
    return progress(m, m.round());
  default:
    return "上一批还在收尾 (作废的回测在停): 稍后再试";
  }
}

} // namespace

struct Remote::Impl {
  std::mutex mu;
  std::deque<Ask> asks; // 网络线程入, GUI 线程出
  std::vector<Waiter> waiters;
  std::unique_ptr<Server> server; // 末成员: 析构先停网络线程

  void wait(const Mine &m, int k, int sec, Answer answer) {
    waiters.push_back({m.log.dir(), k, m.phase() == Mine::Phase::Pause, Clock::now() + std::chrono::seconds(sec), std::move(answer)});
  }

  // 下一条消息发出 → 交回它 (重发要求 = isError); 会话被关 / 换 / 回退 → 原因; 停下 (等起后才停的) 或超时 → 进度
  void answer_waiters(const Mine &m) {
    const Clock::time_point now = Clock::now();
    std::erase_if(waiters, [&](Waiter &w) {
      const auto done = [&](Reply r) {
        w.answer(std::move(r));
        return true;
      };
      const std::string k = std::to_string(w.job);
      if (!m.held() || m.log.dir() != w.dir)
        return done({true, "第 " + k + " 批所在的会话已被研究员关掉 (或换了一段): 要接着挖就 open"});
      if (w.job > m.round())
        return done({true, "第 " + k + " 批已被研究员回退掉 (会话回到第 " + std::to_string(m.round()) + " 轮发出前): 研究员按继续后 open 接上, 取在等的消息"});
      if (w.job < m.round())
        if (const Round &nx = m.timeline[static_cast<size_t>(w.job)]; nx.span.began())
          return done({nx.kind == Msg::Retry, TextOf(nx)});
      if ((m.phase() == Mine::Phase::Pause && !w.paused) || now >= w.until)
        return done({false, progress(m, w.job)});
      return false;
    });
  }

  void open(SharedData &data, Mine &m, Scope &sc, const Mine::Setup &base, const Mine::Leg &leg, bool bench, Ask a) {
    const auto fail = [&](std::string why) { a.answer({true, std::move(why)}); };
    const json &x = a.args;
    if (m.mcp() && m.waiting() && m.idle()) {
      std::string head = x.empty() ? std::string() : "(研究员已开好一段在等外部 agent 的会话, 接上它; 你给的参数没用上)\n";
      if (m.round() > 1)
        head += "(接上的会话已进行到第 " + std::to_string(m.round()) + " 轮: 此前的结论用 recap 看)\n";
      return a.answer({false, (head.empty() ? "" : head + "\n") + TextOf(m.timeline.back())});
    }
    const bool renew = m.mcp() && m.phase() == Mine::Phase::Pause && m.flags().done;
    if (m.held() && !renew)
      return fail(m.mcp() ? "会话已开着, 不用再 open: " + busy_text(m)
                          : "研究员在 GUI 里" + std::string(m.live() ? "开着" : "有一段停着的") + "内置会话 (大脑 " + m.brain() + "): 等他关掉再 open");
    if (bench)
      return fail("研究员在用工作台 (Tools 页有调用在跑): 稍后再试");
    if (!m.idle())
      return fail("上一段会话还在收尾: 稍后再试");
    Mine::Setup s = base;
    s.remark.clear(), s.test = false;
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
    if (const Choice *l = pick("level", catalog::levels()))
      s.sc.level = l->key;
    const std::vector<Choice> hs = catalog::holds(data, s.sc.level);
    if (const Choice *h = pick("hold", hs))
      s.sc.hold = h->key;
    if (x.contains("years")) {
      if (!x["years"].is_number_integer() || x["years"].get<int>() < 1)
        why += (why.empty() ? "" : "; ") + std::string("years 须是 ≥ 1 的整数");
      else
        s.sc.years = x["years"].get<int>();
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
    if (std::none_of(hs.begin(), hs.end(), [&](const Choice &h) { return h.key == s.sc.hold; }))
      return fail("持有期 " + factor::stat::hold_name(s.sc.hold) + " 在该层不可搜, 一并给 hold (可选 " + names_of(hs) + ")");
    if (!catalog::ready(data, s.sc, leg.gpu))
      return fail("作用域 " + catalog::label(s.sc) + " 的数据未就绪 (资产轴未扫 / 标签库未建): 稍后再试, 或换池 / 层");
    if (renew)
      m.close();
    m.lab.bind(data, s.sc, s.test);
    if (m.lab.ctx().leaves.empty() || m.lab.ctx().ops.empty())
      return fail("作用域 " + catalog::label(s.sc) + " 没有开放的" + (m.lab.ctx().leaves.empty() ? "特征叶" : "算子") + " (研究员在 Mine 页的特征 / 算子页勾): 换作用域或请研究员勾");
    Mine::Leg l = leg;
    l.mcp = true;
    m.open(data, std::move(s), std::move(l));
    sc = m.setup.sc;
    a.answer({false, TextOf(m.timeline.back())});
  }

  void handle(SharedData &data, Mine &m, Scope &sc, const Mine::Setup &base, const Mine::Leg &leg, bool bench, Ask a) {
    const auto fail = [&](std::string why) { a.answer({true, std::move(why)}); };
    const json &x = a.args;
    if (a.tool == "open")
      return open(data, m, sc, base, leg, bench, std::move(a));
    if (!m.mcp())
      return fail(m.held() ? "研究员在 GUI 里开着内置会话 (大脑 " + m.brain() + "): 等他关掉再 open" : std::string("还没有会话: 先 open"));
    if (a.tool == "doc")
      return a.answer({false, m.system});
    if (a.tool == "recap") {
      const std::string t = TreeText(m.timeline);
      return a.answer({false, t.empty() ? std::string("(还没有收进研究树的步骤)") : t});
    }
    if (a.tool == "wait" || a.tool == "cancel") {
      if (!x.contains("job") || !x["job"].is_number_integer())
        return fail(a.tool + " 须给整数 job");
      const int k = x["job"].get<int>();
      if (k < 1 || k > m.round())
        return fail("没有第 " + std::to_string(k) + " 批 (本会话到第 " + std::to_string(m.round()) + " 轮)");
      int sec = kRemoteWait;
      if (a.tool == "cancel") {
        if (k != m.round() || (m.phase() != Mine::Phase::Act && m.phase() != Mine::Phase::Choose))
          return fail("第 " + std::to_string(k) + " 批没在跑, 不用取消");
        m.drop();
      } else if (x.contains("seconds")) {
        if (!x["seconds"].is_number_integer())
          return fail("seconds 须是整数");
        sec = std::clamp(x["seconds"].get<int>(), 1, kRemoteWaitMax);
      }
      return wait(m, k, sec, std::move(a.answer));
    }
    json calls;
    if (a.tool == "run")
      calls = x.contains("calls") ? x["calls"] : json();
    else if (FindId<Tool>(a.tool))
      calls = json::array({json{{"tool", a.tool}, {"args", x}}});
    else
      return fail("没有工具 " + a.tool + " (接口工具 " + kFaces + ", 或研究工具表里的工具名)");
    if (!m.waiting() || !m.idle())
      return fail(busy_text(m));
    const int k = m.round();
    m.reply(json{{"calls", std::move(calls)}}.dump());
    wait(m, k, kRemoteWait, std::move(a.answer));
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

void Remote::update(SharedData &data, Mine &m, Scope &sc, const Mine::Setup &setup, const Mine::Leg &leg, bool bench) {
  Impl &r = *impl_;
  std::deque<Ask> got;
  {
    std::lock_guard<std::mutex> lock(r.mu);
    got.swap(r.asks);
  }
  for (Ask &a : got)
    r.handle(data, m, sc, setup, leg, bench, std::move(a));
  r.answer_waiters(m);
}

const std::string &Remote::listen() const { return impl_->server->listen(); }

} // namespace GUI::Agent
