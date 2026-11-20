#include "live/Trace.hpp"

#include "disk/Disk.hpp"
#include "live/Broker.hpp"
#include "misc/cross_platform.hpp"

#include <cassert>
#include <ctime>
#include <exception>
#include <fstream>
#include <mutex>
#include <vector>

#include <unistd.h>

namespace live {

namespace trace {

namespace {

using Steady = std::chrono::steady_clock;
using Wall = std::chrono::system_clock;

struct Frame {
  std::string name;
  Kw kw;
  Steady::time_point t0;
  Wall::time_point at;
  bool loud = true;
  bool open = false; // 已落 begin
};

// 本线程: 段栈 (已落 begin 的恒为前缀) + 静默的段数 + 待并的重复事件 + 跟随的 Track / Hooks (自段 base 起) 与实时一行、最近的 shim 命令
struct Local {
  std::vector<Frame> frames;
  int quiet = 0;
  Kw last; // 上一条落盘事件去掉 ms; null = 无可并
  long rep = 0;
  Steady::time_point first, end;
  Wall::time_point end_at;
  Track *track = nullptr;
  const Hooks *hooks = nullptr;
  size_t base = 0;
  std::string live, shim;
};
thread_local Local L;

// 跟随着即把段栈、实时一行与 shim 命令整份换进 Track
void mirror() {
  if (!L.track)
    return;
  Spot s;
  for (size_t i = L.base; i < L.frames.size(); ++i)
    s.segs.push_back({L.frames[i].name, L.frames[i].t0});
  s.live = L.live;
  s.shim = L.shim;
  L.track->set(std::move(s));
}

std::string text(Wall::time_point at) {
  const std::time_t t = Wall::to_time_t(at);
  const std::tm tm = safe_localtime(&t);
  const long ms = static_cast<long>(std::chrono::duration_cast<std::chrono::milliseconds>(at.time_since_epoch()).count() % 1000);
  return strf("%02d:%02d:%02d.%03ld", tm.tm_hour, tm.tm_min, tm.tm_sec, ms);
}

// 首次落笔才建文件, 同时删掉多出的旧文件; 逐行即刷 (断言崩了也留得下)
class Sink {
public:
  void put(const std::string &line) {
    std::lock_guard lk(mu_);
    if (!f_.is_open())
      open();
    f_ << line << '\n';
    f_.flush();
  }

private:
  void open() {
    const std::vector<disk::Key> old = disk::list(disk::RunLiveTraceLog);
    for (size_t i = 0; i + kKeep <= old.size(); ++i)
      disk::remove(disk::RunLiveTraceLog, old[i]);
    disk::ensure(disk::RunLiveTrace);
    f_.open(disk::path(disk::RunLiveTraceLog, {.stamp = disk::stamp_now()}), std::ios::app);
    assert(f_);
  }

  std::mutex mu_;
  std::ofstream f_;
};
Sink &sink() {
  static Sink s;
  return s;
}

// 一行, 段路径取前 depth 段
void write(Wall::time_point at, const char *kind, const Kw &kw, size_t depth) {
  std::string path;
  for (size_t i = 0; i < depth; ++i)
    path += (i ? "/" : "") + L.frames[i].name;
  Kw j = {{"t", text(at)}, {"th", static_cast<long>(::gettid())}, {"at", std::move(path)}, {"ev", kind}};
  for (const auto &e : kw.items())
    j[e.key()] = e.value();
  sink().put(j.dump(-1, ' ', false, Kw::error_handler_t::replace));
}

size_t opened() {
  size_t n = 0;
  while (n < L.frames.size() && L.frames[n].open)
    ++n;
  return n;
}

void flush() {
  if (L.rep)
    write(L.end_at, "rep", {{"n", L.rep}, {"ms", std::chrono::duration_cast<std::chrono::milliseconds>(L.end - L.first).count()}}, opened());
  L.rep = 0;
}

void open_all() {
  for (size_t i = opened(); i < L.frames.size(); ++i) {
    flush();
    L.last = nullptr;
    Frame &f = L.frames[i];
    write(f.at, "begin", f.kw, i + 1);
    f.open = true;
  }
}

} // namespace

bool on() { return !L.frames.empty() && !L.quiet; }

void ev(const char *kind, Kw kw) {
  if (!on())
    return;
  open_all();
  Kw key = kw;
  key.erase("ms");
  key["ev"] = kind;
  const Steady::time_point now = Steady::now();
  if (key == L.last) {
    ++L.rep;
    L.end = now;
    L.end_at = Wall::now();
    return;
  }
  flush();
  write(Wall::now(), kind, kw, L.frames.size());
  L.last = std::move(key);
  L.first = now;
}

void note(const char *kind, std::string msg, Kw kw) {
  if (!msg.empty() && !kw.contains("text"))
    kw["text"] = msg;
  if (L.hooks && L.hooks->on_note)
    L.hooks->on_note(kind, std::move(msg), kw);
  ev(kind, std::move(kw));
}

void live(std::string line) {
  if (L.hooks && L.hooks->on_live)
    L.hooks->on_live(line);
  if (!L.track)
    return;
  L.live = std::move(line);
  mirror();
}

Span::Span(std::string name, Kw kw, bool loud) : unwinding_(std::uncaught_exceptions()) {
  L.frames.push_back({std::move(name), std::move(kw), Steady::now(), Wall::now(), loud});
  L.quiet += !loud;
  L.shim.clear();
  mirror();
  if (L.hooks && L.hooks->on_span)
    L.hooks->on_span(L.frames.back().name);
}

Span::~Span() {
  assert(!L.frames.empty());
  const Frame &f = L.frames.back();
  L.quiet -= !f.loud;
  if (f.open) {
    flush();
    Kw kw = {{"ms", ms_since(f.t0)}};
    if (std::uncaught_exceptions() > unwinding_)
      kw["fail"] = true;
    write(Wall::now(), "end", kw, L.frames.size());
    L.last = nullptr;
  }
  L.frames.pop_back();
  assert(!L.track || L.frames.size() >= L.base);
  L.shim.clear();
  mirror();
}

Spot Track::now() const {
  std::lock_guard lk(mu_);
  return s_;
}

void Track::set(Spot s) {
  std::lock_guard lk(mu_);
  s_ = std::move(s);
}

Watch::Watch(Track &t, const Hooks *hooks) {
  assert(!L.track);
  L.track = &t;
  L.hooks = hooks;
  L.base = L.frames.size();
  L.live.clear();
  L.shim.clear();
  mirror();
}

Watch::~Watch() {
  L.track->set({});
  L.track = nullptr;
  L.hooks = nullptr;
  L.live.clear();
  L.shim.clear();
}

void shim_sent(std::string_view cmd) {
  if (!L.track)
    return;
  L.shim = cmd;
  mirror();
}

} // namespace trace

void fail(std::string why) {
  trace::ev("fail", {{"why", why}});
  throw Fail{std::move(why)};
}

} // namespace live
