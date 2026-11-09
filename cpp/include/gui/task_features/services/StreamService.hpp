// StreamService — Features 页流式分析 (Distribution / Transform / Preview / Correlation*) 的 Service 特化
//
// 线程模型 (骨架见 gui/util/Service.hpp; 目标形状与发布协议见 gui/task_features/services/Analysis.hpp):
//   - GUI 线程: Derived::RequestCompute 解析参数快照成 Req = reader::Request<Target::Spec>
//     (ReadScope: 月份表 + 池轴 + 库目录, worker 不碰 config; Spec: 目标自己的输入) → Request():
//     覆盖挂起请求 + 取消在跑
//   - worker 线程 run: 池集各池 FeatureRead (LibReaders) → target.reset(scope, spec) → target.build(readers)
//     (build 内部用 Runtime 常驻 ForkJoin 分批流式, 批末发布; 新请求/Cancel 置 cancel_, 每个任务开头检查后放弃)
//   - 目标 (Published 派生: Distribution / Transform / ...) 由服务自持, UI 经 service->target 持 target.mutex 读快照;
//     进度走原子, 免锁
//
// 生命周期 (随任务): 进 Features 任务输入就绪即 Start(); 切走 tab 只 RequestCancel
//   (内存与 worker 保留, 切回自动重算); 切出 Features 任务 Shutdown() = Stop + target.clear() 整体释放.
//   挂起的请求跨 Stop/Start 存活, 重进自动续算.
//
// Derived 需提供:
//   static constexpr const char *kName;             // 线程名
#pragma once

#include "gui/util/Service.hpp"
#include "shared/ReadRequest.hpp"

#include <algorithm>
#include <cassert>
#include <utility>

namespace GUI::Features {

template <class Derived, class Target>
class StreamService : public Service<Derived, reader::Request<typename Target::Spec>> {
public:
  using Req = reader::Request<typename Target::Spec>;

private:
  using Base = Service<Derived, Req>;
  friend Base;

public:
  ~StreamService() { this->Stop(); }

  // 任务级回收: 停 worker + 释放全部构建内存 (切出 Features 任务时调; 切 tab 不回收)
  void Shutdown() {
    this->Stop(); // join 之后 clear 无竞争
    target.clear();
  }

  void Request(Req req) {
    assert(!req.scope.months.empty() && "Request 前由 RequestCompute 过滤空区间");
    Base::Request(std::move(req));
  }

private:
  static constexpr Phase kStart = Phase::Running;
  Progress &board() { return target; }
  bool run(Req &req) {
    // 池集 (主池 + 列所在池) 各一个 reader, 拿到 cancel_: 读到旧字段表写的特征文件时它删库判废并拉起取消, 构建就地收工 (不闪退).
    // reader 自持库清单拷贝, 之后 scope 可整体移交给目标
    LibReaders readers;
    readers.open(req.scope.libs, &this->cancel_);
    target.reset(std::move(req.scope), std::move(req.spec));
    const bool done = target.build(readers.ptr, this->cancel_);
    return done && !readers.stale();
  }

public:
  Target target; // GUI 线程持 target.mutex 读快照, worker 批末发布
};

} // namespace GUI::Features
