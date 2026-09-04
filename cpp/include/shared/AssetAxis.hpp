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
