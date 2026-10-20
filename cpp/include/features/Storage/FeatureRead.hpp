#pragma once

#include "FeatureDay.hpp" // 稳定层: level_info / 日文件格式 (不依赖字段表, 增删特征不重编读端)
#include "misc/profiler.hpp"
#include "shared/FeaturePool.hpp"
#include <algorithm>
#include <atomic>
#include <cassert>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <set>
#include <string>
#include <vector>

// ============================================================================
// FEATURE READER — 特征库 (目录 + 池轴期望 + 判废), 单日读委托 FeatureDay::read
// ============================================================================
// 时间轴: 落盘形状恒为满 (rows / field_count), 消费端时间轴一律 level_valid_rows(lvl).
// A / axis_hash = 池 A 轴大小 / 指纹 (PoolAxis, 来自池清单, 见 FeaturePool.hpp):
//   读端构造时带期望池轴 (A + hash), 逐文件精确比对, 不做兼容展宽.
// 头不符分两类:
//   格式版本 / 字段表指纹 = 旧池, 已无法解释 → 就地判废: 删掉整个 base_dir_ + 置 stale() + 拉起构建的取消旗
//     (构造时可传), 在跑的构建在既有取消检查点收工, 等用户点 compute 重算. 不断言: 改字段表是常规操作, 不该闪退.
//   池轴 A / 指纹 = 本 reader 持的清单过期 (池已按新清单重算, 文件与新清单一致) → 只置 stale 空转, 不删池;
//     请求方重读清单 (新请求 / 重建服务) 即自愈.
//
// API: load_day_columns(date, cols, DayColumns) — 单日选列 [n][T][A], 只读 n 列的载荷
//      (缓冲挂在 DayColumns 里复用, 与写端 IO worker 的 FeatureDay::Scratch 对仗, 稳态零分配)
// ============================================================================

class FeatureRead {
private:
  // 池判废: 记日志 + 删池 (wipe) + 置 stale + 拉起取消旗. 幂等 (多 IO 线程共用一个 reader).
  void mark_stale(const char *why, bool wipe) const {
    if (stale_.exchange(true, std::memory_order_relaxed))
      return;
    // 不用 Logger: 它未 init 时 std::exit(1), 而这里正是"进 Features 页" (compute 还没跑过) 的路径
    std::cout << "[features] " << why << (wipe ? " → 删除特征库待重算: " : " → 本次读取作废: ") << base_dir_ << std::endl;
    if (wipe) {
      wipe_dir(base_dir_);
      set_dir_stale(base_dir_, true);
    }
    if (abort_)
      abort_->store(true, std::memory_order_relaxed); // 复用取消信号: 构建循环的既有检查点即刻收工
  }

public:
  // pool = 特征库 (档 / 期望 A 轴 / 目录, 与写端同一清单; 须已建池)
  // abort = 可选, 构建的取消旗 (StreamService::cancel_): 池判废时一并拉起, 在跑的构建就地收工
  explicit FeatureRead(const FeaturePool &pool, std::atomic<bool> *abort = nullptr)
      : tier_(pool.tier), base_dir_(pool.dir), axis_A_(pool.axis.size()), axis_hash_(pool.axis.hash), abort_(abort) {
    assert(axis_A_ > 0 && "期望池轴为空 (池未建: 先 pool.ready())");
  }

  // 删池. 多个 reader (各 service 每请求各一份, stale_ 不互通) 与 ComputeService 清池可能
  // 同时删同一目录, 并发 remove_all 会撞上对方刚删掉的文件而抛异常. 先原子 rename 到唯一
  // 墓碑名 (只有一方抢得到), 再删墓碑; rename 时目录已不在 = 别人删了.
  static void wipe_dir(const std::string &dir) {
    static std::atomic<std::uint64_t> seq{0};
    const auto ns = std::chrono::system_clock::now().time_since_epoch().count();
    const std::string trash = dir + ".trash." + std::to_string(ns) + "." + std::to_string(seq.fetch_add(1));
    std::error_code ec;
    std::filesystem::rename(dir, trash, ec);
    if (ec) {
      assert(ec == std::errc::no_such_file_or_directory && "删池 rename 失败");
      return;
    }
    std::filesystem::remove_all(trash);
  }

  // 进程级判废登记 (按池目录): reader 头不符删池时登记, ComputeService 开始重算时撤销;
  // GUI 左栏 Compute 行据此提示 stale. reader 各自的 stale_ 随请求生灭, 不能给 UI 用.
  static void set_dir_stale(const std::string &dir, bool stale) {
    auto &r = stale_registry();
    std::lock_guard<std::mutex> lock(r.mutex);
    if (stale)
      r.dirs.insert(dir);
    else
      r.dirs.erase(dir);
  }
  static bool dir_stale(const std::string &dir) {
    auto &r = stale_registry();
    std::lock_guard<std::mutex> lock(r.mutex);
    return r.dirs.count(dir) != 0;
  }

  // 字段表指纹不符 → 旧池已删, 本次构建产出无效 (调用方重算)
  bool stale() const { return stale_.load(std::memory_order_relaxed); }

  // 复活: 常驻 reader (OrderFlow) 在重算完成后重扫时调用 —— 新池已落地, 判废解除
  void revive() { stale_.store(false, std::memory_order_relaxed); }

  // ========================================================================
  // Single Day Selected Columns (GUI 单列 overlay / Transform / Dist / 因子装载)
  // ========================================================================

  // 单日选列张量 [n][T][A] (列主序 = 落盘单位: 每列解码直写进自己的 [T][A], 无交织)
  struct DayColumns {
    misc::Ymd date = 0;
    size_t level = 0;
    size_t T = 0; // = level_rows(level) (含末尾哨兵行)
    size_t A = 0;
    size_t max_features = 0;
    std::vector<size_t> fields;          // [n] 字段下标
    std::vector<feature_storage_t> data; // [n × T × A]
    FeatureDay::Scratch scratch;

    // 第 i 列 [T][A]; 时刻 t 的全资产段 = col(i) + t * A
    const feature_storage_t *col(size_t i) const {
      assert(i < fields.size());
      return data.data() + i * T * A;
    }

    inline feature_storage_t get(size_t t, size_t i, size_t a) const {
      assert(t < T && i < fields.size() && a < A);
      return data[(i * T + t) * A + a];
    }

    void preallocate(size_t A_, size_t level_, size_t max_features_) {
      A = A_;
      level = level_;
      T = level_rows(level);
      max_features = max_features_;
      data.resize(max_features * T * A);
    }
  };

  // 池已判废时空转 (out.data 保持原样): 在跑的构建靠取消旗收工, 中途读到的内容一律丢弃.
  void load_day_columns(misc::Ymd date, const std::vector<size_t> &feature_indices, DayColumns &out) const {
    Trace;
    assert(date > 0);
    assert(out.A > 0 && "Must preallocate() before load_day_columns()");
    assert(out.A == axis_A_ && "调用方 A 与 reader 期望池轴不一致 (同一池清单, 不该发生)");
    assert(has_level(tier_, out.level) && "该档没有这一层");
    assert(!feature_indices.empty() && feature_indices.size() <= out.max_features);
    out.date = date;
    out.fields = feature_indices;
    if (stale_.load(std::memory_order_relaxed))
      return;
    const FeatureDay::Status st = FeatureDay::read(feature_file(feature_day_dir(base_dir_, date), out.level), level_info(tier_, out.level),
                                                   out.fields.data(), out.fields.size(), out.data.data(), out.A, axis_hash_, out.scratch);
    // 文件缺失 = 池被外部清除: 另一 reader 判废删池, 或 ComputeService 重算
    // 前显式清池 (与读端并发; store 随即重建目录, 故 base_dir_ 是否存在不可
    // 作判据). 落盘侧 .tmp+rename 原子发布 + 末层 commit 标记 (has_date) 保证
    // 正常池不会"目录在文件缺" → 一律判废空转收工, 重扫/重算后 revive. 池轴不符 = 清单过期, 同样不删池.
    if (st == FeatureDay::Status::Missing || st == FeatureDay::Status::AxisSize || st == FeatureDay::Status::AxisHash)
      mark_stale(FeatureDay::why(st), /*wipe=*/false);
    else if (st != FeatureDay::Status::Ok)
      mark_stale(FeatureDay::why(st), /*wipe=*/true);
  }

  // ========================================================================
  // Utility Functions
  // ========================================================================

  // 该日是否有特征文件 (以档内最后一层的日文件为准: IO worker 按层序落盘, 它在则全在)
  static bool has_date(TierId tier, const std::string &base_dir, misc::Ymd date) {
    assert(date > 0);
    return std::filesystem::exists(feature_file(feature_day_dir(base_dir, date), last_level(tier)));
  }
  bool has_date(misc::Ymd date) const { return has_date(tier_, base_dir_, date); }
  TierId tier() const { return tier_; }

  // 月 m 内已齐备落盘的日, 升序
  std::vector<misc::Ymd> list_dates(misc::Ym m) const {
    std::vector<misc::Ymd> dates;
    const std::string dir = base_dir_ + "/" + misc::slash(misc::first_day(m)).substr(0, 7);

    // 目录不在 / 遍历前被并发删池 (wipe_dir) → ec 置位, 迭代器即 end
    std::error_code ec;
    for (const auto &entry : std::filesystem::directory_iterator(dir, ec)) {
      if (entry.is_directory()) {
        const std::string day = entry.path().filename().string();
        const int dd = day.size() == 2 && std::isdigit(day[0]) && std::isdigit(day[1]) ? std::stoi(day) : 0;
        const misc::Ymd date = dd ? m * 100 + dd : 0;
        if (date && has_date(date))
          dates.push_back(date);
      }
    }

    std::sort(dates.begin(), dates.end());
    return dates;
  }

  struct StaleRegistry {
    std::mutex mutex;
    std::set<std::string> dirs;
  };
  static StaleRegistry &stale_registry() {
    static StaleRegistry r;
    return r;
  }

  TierId tier_;
  std::string base_dir_;
  size_t axis_A_;
  std::uint64_t axis_hash_;
  std::atomic<bool> *abort_ = nullptr;     // 可选取消旗 (不持有)
  mutable std::atomic<bool> stale_{false}; // 池判废 (多 IO 线程共用一个 reader, 故原子)
};
