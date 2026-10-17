// StreamService — Features 页流式分析 (Dist / Transform / FeaturePreview / Corr*) 的 Worker 特化
//
// 线程模型 (骨架见 gui/util/Worker.hpp; 目标形状与发布协议见 shared/Analysis.hpp):
//   - GUI 线程: Derived::RequestCompute 解析参数快照成 Req = analysis::Request<Target::Spec>
//     (ReadScope: 月份表 + universe 子轴 + 库目录, worker 不碰 config; Spec: 目标自己的输入) → Request():
//     覆盖挂起请求 + 取消在跑
//   - worker 线程 run: FeatureRead → target.reset(scope, spec) → target.build()
//     (build 内部用 Runtime 常驻 ForkJoin 分批流式, 批末发布; 新请求/Cancel 置 cancel_, 每个任务开头检查后放弃)
//   - UI 渲染持 target.mutex 读快照; 进度走原子, 免锁
//
// 生命周期 (随任务): 进 Features 任务输入就绪即 Start(data); 切走 tab 只 RequestCancel
//   (内存与 worker 保留, 切回自动重算); 切出 Features 任务 Shutdown() = Stop + target.clear() 整体释放.
//   挂起的请求跨 Stop/Start 存活, 重进自动续算.
//
// Derived 需提供:
//   static constexpr const char *kName;             // 线程名
//   static Target &target(SharedData &);            // 目标状态 (Published 派生: Dist / Transform / ...)
#pragma once

#include "features/Backend/FeatureRead.hpp"
#include "gui/util/Worker.hpp"
#include "shared/Analysis.hpp"

#include <atomic>
#include <cassert>
#include <utility>

struct SharedData;

namespace GUI::Features {

template <class Derived, class Target>
class StreamService : public Worker<Derived, analysis::Request<typename Target::Spec>> {
public:
  using Req = analysis::Request<typename Target::Spec>;

private:
  using Base = Worker<Derived, Req>;
  friend Base;

public:
  ~StreamService() { this->Stop(); }

  void Start(SharedData &data) {
    data_ = &data;
    Base::Start();
  }
  // 任务级回收: 停 worker + 释放全部构建内存 (切出 Features 任务时调; 切 tab 不回收)
  void Shutdown() {
    this->Stop(); // join 之后 clear 无竞争
    if (data_)
      Derived::target(*data_).clear();
  }

  void Request(Req req) {
    assert(!req.scope.months.empty() && "Request 前由 RequestCompute 过滤空区间");
    Base::Request(std::move(req));
  }

private:
  static constexpr Phase kStart = Phase::Running;
  Progress &board() { return Derived::target(*data_); }
  bool run(Req &req) {
    Target &target = Derived::target(*data_);
    // reader 拿到 cancel_: 读到旧字段表写的特征文件时它删库判废并拉起取消, 构建就地收工 (不闪退).
    // reader 自持库目录拷贝, 之后 scope 可整体移交给目标
    FeatureRead reader(req.scope.lib, &this->cancel_);
    if constexpr (requires(Target &t, FeatureRead &r, const std::atomic<bool> &c) { t.build(r, r, c); }) {
      // Bar 档旁读 (全轴); All 库缺日/旧表同样删库判废 + 取消
      FeatureRead all(req.scope.bar, &this->cancel_);
      target.reset(std::move(req.scope), std::move(req.spec));
      return target.build(reader, all, this->cancel_) && !reader.stale() && !all.stale();
    } else {
      target.reset(std::move(req.scope), std::move(req.spec));
      return target.build(reader, this->cancel_) && !reader.stale();
    }
  }

  SharedData *data_ = nullptr;
};

} // namespace GUI::Features
