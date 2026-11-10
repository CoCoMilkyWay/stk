#include "live/tdx/Trace.hpp"

#include "disk/Disk.hpp"
#include "misc/cross_platform.hpp"

#include "live/Broker.hpp"

#include <cassert>
#include <ctime>
#include <exception>
#include <fstream>
#include <mutex>
#include <vector>

#include <unistd.h>

namespace live::tdx {

namespace trace {

namespace {

using Steady = std::chrono::steady_clock;
using Wall = std::chrono::system_clock;

struct Frame {
  std::string name;
  Kw kw;
  Steady::time_point t0;
  Wall::time_point at;
  bool open = false; // 已落 begin
};

// 本线程: 段栈 (已落 begin 的恒为前缀) + 待并的重复事件
struct Local {
  std::vector<Frame> frames;
  Kw last; // 上一条落盘事件去掉 ms; null = 无可并
  long rep = 0;
  Steady::time_point first, end;
  Wall::time_point end_at;
};
thread_local Local L;

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
    const std::vector<disk::Key> old = disk::list(disk::RunLiveTdxLogFile);
    for (size_t i = 0; i + kKeep <= old.size(); ++i)
      disk::remove(disk::RunLiveTdxLogFile, old[i]);
    disk::ensure(disk::RunLiveTdxLog);
    f_.open(disk::path(disk::RunLiveTdxLogFile, {.stamp = disk::stamp_now()}), std::ios::app);
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
void write(Wall::time_point at, const char *ev, const Kw &kw, size_t depth) {
  std::string path;
  for (size_t i = 0; i < depth; ++i)
    path += (i ? "/" : "") + L.frames[i].name;
  Kw j = {{"t", text(at)}, {"th", static_cast<long>(::gettid())}, {"at", std::move(path)}, {"ev", ev}};
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

bool on() { return !L.frames.empty(); }

void ev(const char *ev, Kw kw) {
  if (L.frames.empty())
    return;
  open_all();
  Kw key = kw;
  key.erase("ms");
  key["ev"] = ev;
  const Steady::time_point now = Steady::now();
  if (key == L.last) {
    ++L.rep;
    L.end = now;
    L.end_at = Wall::now();
    return;
  }
  flush();
  write(Wall::now(), ev, kw, L.frames.size());
  L.last = std::move(key);
  L.first = now;
}

Span::Span(std::string name, Kw kw) : unwinding_(std::uncaught_exceptions()) {
  L.frames.push_back({std::move(name), std::move(kw), Steady::now(), Wall::now()});
}

Span::~Span() {
  assert(!L.frames.empty());
  const Frame &f = L.frames.back();
  if (f.open) {
    flush();
    Kw kw = {{"ms", ms_since(f.t0)}};
    if (std::uncaught_exceptions() > unwinding_)
      kw["fail"] = true;
    write(Wall::now(), "end", kw, L.frames.size());
    L.last = nullptr;
  }
  L.frames.pop_back();
}

} // namespace trace

void fail(std::string why) {
  trace::ev("fail", {{"why", why}});
  live::fail(std::move(why));
}

} // namespace live::tdx
