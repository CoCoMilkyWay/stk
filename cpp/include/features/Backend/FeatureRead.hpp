#pragma once

#include "FeatureLevels.hpp" // 稳定层: level_info / 文件布局 / FeatureCodec / CODEC_ENABLED (不依赖字段表, 增删特征不重编读端)
#include "misc/cross_platform.hpp"
#include "misc/profiler.hpp"
#include <algorithm>
#include <atomic>
#include <cassert>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <set>
#include <string>
#include <vector>

// ============================================================================
// FEATURE READER — 特征日文件 (一天一层一个, 内带列目录; 格式见 FeatureLevels.hpp)
// ============================================================================
// 按列选读: 一次 open 拿头 + 目录, 所请求的列按偏移升序, 首尾相接的并成一段
// (≤ kRunCap) 一次 pread, 逐列解码 (FeatureCodec) + xor_delta 层前缀 XOR 还原 +
// 交织进 [T][n][A]. 全列 = 全部存储列, 同一条路径.
//
// 形状是编译期常量: 写端永远写满 (LEVELS[lvl].rows / width), 文件头的 T/F 只做
// 校验, 不做"实际维度" —— 消费端时间轴一律 level_valid_rows(lvl).
// A / axis_hash = universe 子轴大小 / 指纹 (UniverseAxis, 见 AssetAxis.hpp):
//   列 → 资产的映射不在文件里, 靠子轴顺序 (名单 → 全局轴下标升序). 读端构造
//   时带期望子轴 (A + hash), 逐文件精确比对, 不做兼容展宽.
// 头不符 (子轴 A / 指纹 / 字段表指纹) = 旧子轴或旧字段表写的库, 已无法解释 → 就地判废:
//   删掉整个 base_dir_ + 置 stale() + 拉起构建的取消旗 (构造时可传), 在跑的构建在既有
//   取消检查点收工, 等用户点 compute 重算. 不断言: 改字段表 / 动态池名单变了 / 改回测
//   区间 (子轴 = 区间内并集) 都是常规操作, 不该闪退; 特征库是纯派生数据, 删了重跑即可.
//
// APIs (缓冲全部挂在张量结构里复用, 与写端 io_buf_/io_column_/io_dir_ 对仗, 稳态零分配):
//   1. load_day(date, DayTensor)          - GUI: 单日整层 (全部存储列)
//   2. load_day_columns(date, cols, out)  - GUI overlay / Transform: 单日, 选列 (只读 n 列的载荷)
//   3. load_month_columns()               - Dist/TimeSeries: 整月, 选列
// ============================================================================

class FeatureRead {
public:
  // 复用缓冲 (稳态零分配), 每个张量结构自带一份
  struct Scratch {
    std::vector<ColRef> dir;             // 日文件目录 [F]
    std::vector<size_t> cols;            // 本次要读的存储列 [n]
    std::vector<uint32_t> order;         // cols 下标, 按载荷偏移升序 (并段用)
    std::vector<uint8_t> zbuf;           // 一段载荷 (≤ kRunCap, 单列超限则为该列)
    std::vector<feature_storage_t> tile; // 一列解码后 [T][A], 交织前中转
  };

private:
  // 首尾相接的列载荷并成一段读, 一段最多这么大
  static constexpr size_t kRunCap = size_t(4) << 20;

  // 库判废: 记日志 + 删库 (wipe) + 置 stale + 拉起取消旗. 幂等 (多 IO 线程共用一个 reader).
  void mark_stale(const char *why, bool wipe) const {
    if (stale_.exchange(true, std::memory_order_relaxed))
      return;
    // 不用 Logger: 它未 init 时 std::exit(1), 而这里正是"进 Features 页" (compute 还没跑过) 的路径
    std::cout << "[features] " << why << " → 删除特征库待重算: " << base_dir_ << std::endl;
    if (wipe) {
      wipe_dir(base_dir_);
      set_dir_stale(base_dir_, true);
    }
    if (abort_)
      abort_->store(true, std::memory_order_relaxed); // 复用取消信号: 构建循环的既有检查点即刻收工
  }

  // 读日文件里 s.cols 那几列到 dst [T][n][A] (第 i 槽 = 存储列 s.cols[i]): 头校验
  // (子轴精确匹配) + 目录 + 并段读 + 逐列解码交织.
  // 库已判废时空转 (dst 保持原样): 在跑的构建靠取消旗收工, 中途读到的内容一律丢弃.
  void read_columns(const std::string &filepath, size_t lvl, feature_storage_t *dst, size_t A, Scratch &s) const {
    Trace;
    if (stale_.load(std::memory_order_relaxed))
      return;
    const auto &L = level_info(lvl);
    const size_t T = L.rows, F = L.width, n = s.cols.size();
    assert(A == axis_A_ && "调用方 A 与 reader 期望子轴不一致 (同一 universe_axis 推导, 不该发生)");

    const int fd = file_open_read(filepath.c_str());
    if (fd < 0) {
      // 文件缺失 = 库被外部清除: 另一 reader 判废删库, 或 ComputeService 重算
      // 前显式清库 (与读端并发; store 随即重建目录, 故 base_dir_ 是否存在不可
      // 作判据). 落盘侧 .tmp+rename 原子发布 + 末层 commit 标记 (has_date) 保证
      // 正常库不会"目录在文件缺" → 一律判废空转收工, 重扫/重算后 revive.
      mark_stale("特征库已被清除", /*wipe=*/false);
      return;
    }

    {
      TraceN("ReadHeader");
      FeatureFileHeader head{};
      const size_t got = file_pread(fd, &head, sizeof(head), 0);
      assert(got == sizeof(head) && "特征日文件短于头 (损坏)");
      (void)got;
      assert(head.magic == FeatureFileHeader::kMagic && "不是特征日文件 (magic 不符)");
      // 子轴 / 字段表与文件头不符 = 旧库: 不闪退, 删库判废, 等用户点 compute 重算 (纯派生数据)
      const char *why = head.A != axis_A_                  ? "子轴大小不符 (特征库是旧 universe 名单/回测区间写的)"
                        : head.axis_hash != axis_hash_     ? "子轴指纹不符 (特征库与当前 universe 名单/asset_axis.json 列序不一致)"
                        : head.fingerprint != L.fingerprint ? "字段表指纹不符 (特征文件是旧字段表写的)"
                                                            : nullptr;
      if (why) {
        file_close(fd);
        mark_stale(why, /*wipe=*/true);
        return;
      }
      assert(head.T == T && head.F == F && "形状不符: 落盘形状恒为满 (LEVELS[lvl].rows / width)");

      s.dir.resize(F);
      const size_t dir_got = file_pread(fd, s.dir.data(), F * sizeof(ColRef), sizeof(head));
      assert(dir_got == F * sizeof(ColRef) && "特征日文件目录读不全");
      (void)dir_got;
    }

    s.order.resize(n);
    for (size_t i = 0; i < n; ++i) {
      assert(s.cols[i] < F && "存储列越界");
      s.order[i] = static_cast<uint32_t>(i);
    }
    std::sort(s.order.begin(), s.order.end(),
              [&s](uint32_t a, uint32_t b) { return s.dir[s.cols[a]].offset < s.dir[s.cols[b]].offset; });

    const size_t raw_size = T * A * sizeof(feature_storage_t);
    s.tile.resize(T * A);
    for (size_t i = 0; i < n;) {
      const uint64_t begin = s.dir[s.cols[s.order[i]]].offset;
      uint64_t end = begin + s.dir[s.cols[s.order[i]]].size;
      size_t j = i + 1;
      while (j < n && s.dir[s.cols[s.order[j]]].offset == end && end - begin + s.dir[s.cols[s.order[j]]].size <= kRunCap)
        end += s.dir[s.cols[s.order[j++]]].size;

      {
        TraceN("ReadRun");
        const size_t len = static_cast<size_t>(end - begin);
        if (s.zbuf.size() < len)
          s.zbuf.resize(len);
        const size_t got = file_pread(fd, s.zbuf.data(), len, begin);
        assert(got == len && "特征日文件载荷越过文件末尾 (目录与文件不符)");
        (void)got;
      }

      for (; i < j; ++i) {
        const size_t slot = s.order[i];
        const ColRef &ref = s.dir[s.cols[slot]];
        const uint8_t *payload = s.zbuf.data() + (ref.offset - begin);
        if constexpr (CODEC_ENABLED) {
          TraceN("CodecDecode");
          FeatureCodec::decode(payload, ref.size, s.tile.data(), raw_size);
        } else {
          assert(ref.size == raw_size && "payload size mismatch");
          std::memcpy(s.tile.data(), payload, raw_size);
        }
        // T 轴 XOR 差分还原 (写端 write_level 对 xor_delta 层压缩前编码)
        if (L.xor_delta) {
          TraceN("XorDeltaDecode");
          xor_delta_decode(s.tile.data(), T, A);
        }
        TraceN("InterleaveColumn");
        for (size_t t = 0; t < T; ++t)
          std::memcpy(dst + (t * n + slot) * A, s.tile.data() + t * A, A * sizeof(feature_storage_t));
      }
    }
    file_close(fd);
  }

  // 装载原语: 某日某层的字段集合 → dst [T][n][A]. fields == nullptr 取全部存储列
  // (n == field_count, dst 为 [T][width][A], 宽字段各档按偏移排开).
  void load_fields(misc::Ymd date, size_t lvl, const size_t *fields, size_t n,
                   feature_storage_t *dst, size_t A, Scratch &s) const {
    const auto &L = level_info(lvl);
    assert(n <= L.field_count && (fields || n == L.field_count));
    s.cols.resize(fields ? n : L.width);
    for (size_t i = 0; i < s.cols.size(); ++i) {
      assert((!fields || L.fields[fields[i]].width == 1) && "column selection is per width-1 field");
      s.cols[i] = fields ? L.offsets[fields[i]] : i;
    }
    read_columns(feature_file(feature_day_dir(base_dir_, date), lvl), lvl, dst, A, s);
  }

public:
  // 选列区间张量 (Dist 任意日期集 / TimeSeries 整月), 布局 [N_days × T][F_selected][A];
  // 日步长恒为 LEVELS[level].rows (含末尾哨兵行), 时间轴消费用 level_valid_rows(level)
  struct MonthTensor {
    std::vector<misc::Ymd> dates;        // [N_days]
    std::vector<feature_storage_t> data; // [N_days*T × F_selected × A]
    size_t A = 0;
    size_t level = 0;
    size_t max_days = 0;
    size_t max_features = 0;
    std::vector<size_t> feature_indices;
    Scratch scratch;

    // 日 d 的时间轴起点 (定步长, 免存偏移表)
    size_t day_start(size_t d) const { return d * level_info(level).rows; }

    void preallocate(size_t A_, size_t max_days_, size_t max_features_, size_t level_) {
      A = A_;
      level = level_;
      max_days = max_days_;
      max_features = max_features_;
      data.resize(max_days * level_info(level).rows * max_features * A);
    }

    void reset() {
      dates.clear();
      feature_indices.clear();
      // data/scratch 不清, 只复用
    }
  };

  // base_dir = 该 universe 的特征库目录 (Config::FeatureUniverseDir);
  // axis_A / axis_hash = 期望子轴 (universe_axis(cfg).size() / .hash)
  // abort = 可选, 构建的取消旗 (StreamService::cancel_): 库判废时一并拉起, 在跑的构建就地收工
  FeatureRead(const std::string &base_dir, size_t axis_A, std::uint64_t axis_hash,
              std::atomic<bool> *abort = nullptr)
      : base_dir_(base_dir), axis_A_(axis_A), axis_hash_(axis_hash), abort_(abort) {
    assert(axis_A_ > 0 && "期望子轴为空");
  }

  // 删库. 多个 reader (各 service 每请求各一份, stale_ 不互通) 与 ComputeService 清库可能
  // 同时删同一目录, 并发 remove_all 会撞上对方刚删掉的文件而抛异常. 先原子 rename 到唯一
  // 墓碑名 (只有一方抢得到), 再删墓碑; rename 时目录已不在 = 别人删了.
  static void wipe_dir(const std::string &dir) {
    static std::atomic<std::uint64_t> seq{0};
    const auto ns = std::chrono::system_clock::now().time_since_epoch().count();
    const std::string trash = dir + ".trash." + std::to_string(ns) + "." + std::to_string(seq.fetch_add(1));
    std::error_code ec;
    std::filesystem::rename(dir, trash, ec);
    if (ec) {
      assert(ec == std::errc::no_such_file_or_directory && "删库 rename 失败");
      return;
    }
    std::filesystem::remove_all(trash);
  }

  // 进程级判废登记 (按库目录): reader 头不符删库时登记, ComputeService 开始重算时撤销;
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

  // 字段表指纹不符 → 旧库已删, 本次构建产出无效 (调用方重算)
  bool stale() const { return stale_.load(std::memory_order_relaxed); }

  // 复活: 常驻 reader (OrderFlow) 在重算完成后重扫时调用 —— 新库已落地, 判废解除
  void revive() { stale_.store(false, std::memory_order_relaxed); }

  // ========================================================================
  // Single Day Loading (GUI: 单日整层, 任一层)
  // ========================================================================

  // 单日单层张量 [T][F_total][A]: 一个实例 = 一层, 形状是 level_info(level) 层表常量
  struct DayTensor {
    misc::Ymd date = 0;
    size_t level = 0;
    size_t A = 0;
    std::vector<feature_storage_t> data;
    Scratch scratch;

    // 宽字段用 sub 取档内下标
    template <size_t LVL>
    inline feature_storage_t get(size_t t, size_t field, size_t a, size_t sub = 0) const {
      static_assert(LVL < LEVEL_COUNT);
      assert(LVL == level && "DayTensor level mismatch");
      const auto &L = level_info(LVL);
      assert(t < L.rows && a < A && sub < L.fields[field].width);
      return data[(t * L.width + L.offsets[field] + sub) * A + a];
    }

    void preallocate(size_t A_, size_t level_) {
      A = A_;
      level = level_;
      data.resize(level_info(level).rows * level_info(level).width * A);
    }
  };

  // Load single day, all features of out.level (for GUI)
  void load_day(misc::Ymd date, DayTensor &out) const {
    Trace;
    assert(date > 0);
    assert(out.A > 0 && "Must preallocate() before load_day()");
    out.date = date;
    load_fields(date, out.level, nullptr, level_info(out.level).field_count, out.data.data(), out.A, out.scratch);
  }

  // ========================================================================
  // Single Day Selected Columns (GUI 单列 overlay / Transform 单列拼接)
  // ========================================================================

  // 单日选列张量 [T][n][A]: load_day 的选列版, 布局与 MonthTensor 的单日切片同构.
  // 列宽必须为 1 (与 load_month_columns 同约束); 只读 n 列的载荷, dst 内存只有 [T][n][A].
  struct DayColumns {
    misc::Ymd date = 0;
    size_t level = 0;
    size_t A = 0;
    size_t max_features = 0;
    std::vector<size_t> fields;          // [n] 字段下标
    std::vector<feature_storage_t> data; // [T × n × A]
    Scratch scratch;

    inline feature_storage_t get(size_t t, size_t i, size_t a) const {
      assert(t < level_info(level).rows && i < fields.size() && a < A);
      return data[(t * fields.size() + i) * A + a];
    }

    void preallocate(size_t A_, size_t level_, size_t max_features_) {
      A = A_;
      level = level_;
      max_features = max_features_;
      data.resize(level_info(level).rows * max_features * A);
    }
  };

  // Load single day, selected width-1 fields
  void load_day_columns(misc::Ymd date, const std::vector<size_t> &feature_indices, DayColumns &out) const {
    Trace;
    assert(date > 0);
    assert(out.A > 0 && "Must preallocate() before load_day_columns()");
    assert(!feature_indices.empty() && feature_indices.size() <= out.max_features);
    out.date = date;
    out.fields = feature_indices;
    load_fields(date, out.level, out.fields.data(), out.fields.size(), out.data.data(), out.A, out.scratch);
  }

  // ========================================================================
  // Batch Monthly Loading (for Dist analysis)
  // ========================================================================

  // 单日选列载入到区间张量的第 day_idx 槽 (Dist 抽样流式: 逐日 IO, 进度/取消粒度归调用方)
  // dates/feature_indices 由调用方维护
  void load_date_columns_into(misc::Ymd date, const std::vector<size_t> &feature_indices,
                              MonthTensor &out, size_t day_idx) const {
    assert(out.A > 0 && "Must preallocate() before load_date_columns_into()");
    const size_t n = feature_indices.size();
    assert(n <= out.max_features && day_idx < out.max_days);
    load_fields(date, out.level, feature_indices.data(), n,
                out.data.data() + out.day_start(day_idx) * n * out.A, out.A, out.scratch);
  }

  void load_month_columns(
      misc::Ym month,
      const std::vector<size_t> &feature_indices,
      MonthTensor &out) const {
    Trace;

    assert(out.A > 0 && "Must preallocate() before load_month_columns()");
    assert(feature_indices.size() <= out.max_features && "Feature count exceeds preallocated");

    out.reset();
    out.feature_indices = feature_indices;

    {
      TraceN("ListDates");
      out.dates = list_dates(month);
      assert(!out.dates.empty() && "No dates found");
      assert(out.dates.size() <= out.max_days && "Day count exceeds preallocated");
    }

    for (size_t day_idx = 0; day_idx < out.dates.size(); ++day_idx) {
      TraceN("LoadDay");
      TraceTextS(misc::dense(out.dates[day_idx]).c_str());
      load_date_columns_into(out.dates[day_idx], feature_indices, out, day_idx);
    }
  }

  // ========================================================================
  // Utility Functions
  // ========================================================================

  // 该日是否有特征文件 (以最后一层的日文件为准: IO worker 按层序落盘, 它在则全在)
  static bool has_date(const std::string &base_dir, misc::Ymd date) {
    assert(date > 0);
    return std::filesystem::exists(feature_file(feature_day_dir(base_dir, date), LEVEL_COUNT - 1));
  }
  bool has_date(misc::Ymd date) const { return has_date(base_dir_, date); }

  // 月 m 内已齐备落盘的日, 升序
  std::vector<misc::Ymd> list_dates(misc::Ym m) const {
    std::vector<misc::Ymd> dates;
    const std::string dir = base_dir_ + "/" + misc::slash(misc::first_day(m)).substr(0, 7);

    // 目录不在 / 遍历前被并发删库 (wipe_dir) → ec 置位, 迭代器即 end
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

  std::string base_dir_;
  size_t axis_A_;
  std::uint64_t axis_hash_;
  std::atomic<bool> *abort_ = nullptr;     // 可选取消旗 (不持有)
  mutable std::atomic<bool> stale_{false}; // 库判废 (多 IO 线程共用一个 reader, 故原子)
};
