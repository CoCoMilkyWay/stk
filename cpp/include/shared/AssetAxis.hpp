// Asset Axis — A 轴 append-only 注册表 (列序 = asset_id 的唯一真理)
//
// 背景: 特征库文件头只存 T/F/A 三个维度, 不存资产名单 — 列 → 资产的映射
//   完全靠 A 轴顺序. 一旦轴被重排 (例: 从有序名单重新生成), 全体列下标位移,
//   历史特征文件静默作废. 因此轴必须 append-only: 新上市只追加尾部.
//
// 一致性方案 (前缀累积 hash, O(1) 校验, 热路径零开销):
//   h[0]   = FNV_OFFSET
//   h[n]   = fnv1a(h[n-1], codes[n-1])
//   注册表落盘 count + assets + h[count]; load 时重算全量比对 → 注册表自身
//   损坏立刻暴露 (O(A) 一次, 微秒级).
//   特征文件头存 (A_file, h[A_file]) — hash 只依赖前 A_file 条, 所以追加新
//   资产不改变历史文件的校验值; 读文件时 O(1) 比对即可确认"这个文件的列序与
//   当前轴的前缀一致". asset_id 仍是纯数组下标, 计算路径不受任何影响.
//
// 注: L2 二进制库 (encode cache) 按 <CODE>.<EX> 目录名寻址, 自描述, 不需要
//   轴锁定; 只有按下标寻址的特征库需要.
#pragma once

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

// ============================================================================
// AssetAxis
// ============================================================================
class AssetAxis {
public:
  // 落盘 output/fundamental/asset_axis.json — 与基本面同域, 因为轴由股票全量
  // 派生, 且必须活过两级 cache 的任何清库 (FeatureStore 构造会 remove_all
  // feature_dir; 轴丢了 append-only 保证就没了).
  static constexpr const char *kFileName = "asset_axis.json";
  static constexpr int kVersion = 1;

  // 注册表 → 内存. 文件不存在 → 空轴 (首次建库);
  // 存在 → 解析 + 重算累积 hash 与文件记录比对 (不符 = 注册表损坏, assert).
  void load();

  // 首见即追加尾部, 返回列下标; 已存在直接返回原下标 (永不重排/删除).
  std::size_t intern(const std::string &code_ex);

  // dirty 时原子写 (tmp + rename); 无变更则 no-op.
  void save() const;

  std::size_t size() const { return codes_.size(); }
  bool empty() const { return codes_.empty(); }

  // "000001.SZ"
  const std::string &code(std::size_t i) const;

  // 前 n 条的累积 hash (n ∈ [0, size()]); 特征文件头用 hash_at(A) 锁定列序.
  std::uint64_t hash_at(std::size_t n) const;

  // 查下标; 不存在返回 size() (= 无效)
  std::size_t find(const std::string &code_ex) const;

private:
  std::vector<std::string> codes_;
  std::unordered_map<std::string, std::size_t> index_;
  std::vector<std::uint64_t> prefix_hash_; // size == codes_.size() + 1
  std::string path_;
  mutable bool dirty_ = false;
};

// 进程内唯一 A 轴实例, 首次访问自动 load + 自校验.
//
// 不放进 SharedData: 轴是跨模块的全局真理 — encode 按它过滤 universe, 特征
// 计算按它定列序, FeatureRead 按它验文件指纹. 两份实例就是两套列序, 所以
// 语言层面只给一份. 读侧只调 hash_at (O(1) 数组取值), 热路径无成本.
AssetAxis &asset_axis();

// ============================================================================
// UniverseAxis — 特征管线的 A 轴 = universe 子轴
//
// 由 config.universe + 全局 AssetAxis 唯一确定: ids = 名单在全局轴上的下标
// (升序去重), hash = 按子轴顺序对码表做同一条 FNV 累积链 (fnv_append). 特征文件
// 头存 (A_sub, hash): 写读两端各自从 config 推导同一子轴, hash 不符 = 名单与
// 特征库不一致, 立刻 assert (需重算). "all" 是普通特例: ids = [0, num_assets),
// hash == asset_axis().hash_at(num_assets) (同一条链的前缀值).
//
// 整条特征管线 (张量池 / 落盘 / 读端 / Dist / Transform / OrderFlow 特征读)
// 一律用子轴下标 (sub); 需要查 items/码表/行业等全局资源时经 global() 映射.
// ============================================================================
struct UniverseAxis {
  std::vector<uint32_t> ids; // sub → global (升序去重, 非空)
  std::uint64_t hash = 0;    // 子轴码表累积 hash (特征文件头校验值)

  std::size_t size() const { return ids.size(); }
  uint32_t global(std::size_t sub) const {
    assert(sub < ids.size() && "UniverseAxis::global: 子轴下标越界");
    return ids[sub];
  }
  // global → sub (二分); 不在子轴内返回 size()
  std::size_t sub_of(uint32_t global_id) const {
    const auto it = std::lower_bound(ids.begin(), ids.end(), global_id);
    return (it != ids.end() && *it == global_id) ? static_cast<std::size_t>(it - ids.begin()) : ids.size();
  }
};

// universe 子轴构建: cfg.universe == "all" → 全轴前缀; 否则 cfg.UniverseCodes()
// (JSON 解析在 Config.cpp) 逐条按轴 find. 代码不在轴前缀 [0, num_assets) 内 →
// assert: 名单写错宁可启动即死, 不要静默算出一个残缺 universe.
// Compute (派活/落盘) 与 Dist/Transform/OrderFlow (读端) 共用, 各处永远同一子轴.
struct Config;
UniverseAxis universe_axis(const Config &cfg, std::size_t num_assets);
