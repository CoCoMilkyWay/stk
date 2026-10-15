#pragma once
// universe — 动态股票池 (日频 PIT 名单), Meta 的 cs_valid 数据源; 与 Fund 吃 fund::Pool 同形.
//
//   名单由独立的 python 产出 (py/universe/<name>.py → <config_dir>/universe/<name>.json), 不与本项目耦合:
//   选池可能要用特征, 而特征此刻还没算 (鸡生蛋), 所以盘后用基本面 parquet 另算, 这里只读结果.
//
//   JSON 格式 (py/universe/common/pipeline.py 写):
//     {"YYYYMMDD": {"add": ["600000.SH", ...], "del": [...]}, ...}
//   逐日相对前一交易日的 diff, 首日 add = 全量; 无变化的日子也有 (空 add/del), 缺键 = 该日没出名单.
//   这里从空集按日回放:
//     codes(first, last) 回测区间 [first, last] 内进过池的并集 (升序去重) = 静态 A 轴 (UniverseAxis: 张量列 / 落盘 / 读端).
//                        只看张量覆盖的时间段, 不是名单全史 (名单从 2015 起, 全史并集会把轴撑大几倍).
//                        TS 特征对轴上所有标的在区间内全程计算, 区间内日后进池不用补历史
//     in_pool(code, D)   第 D 日成员位 → CoreSequential::begin_day → MetaTracker::begin_day → cs_valid 整日常量
//   PIT: 第 D 日名单由 ≤ D−1 数据定 (python 侧保证), 对本类只是 (资产, 日) 的 bool.
//
//   "all" (无名单文件): 全 A 轴恒在池, 不读文件 —— 旧语义.
#include "misc/date.hpp"

#include <cstdint>
#include <filesystem>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace universe {

class Pool {
public:
  // 读 json 并回放. 文件缺失 / 格式不对 / add 已在池 / del 不在池 / 某日池空 / 日期键非升序 → assert
  static Pool load(const std::filesystem::path &json_path);
  // "all": 恒在池, codes(...)/dates() 不得调用
  static Pool all();

  bool is_all() const { return all_; }
  // [first, last] (闭区间) 内进过池的代码并集, 升序去重; 区间内没有任何名单日 / 并集为空 → assert
  std::vector<std::string> codes(misc::Ymd first, misc::Ymd last) const;
  const std::vector<misc::Ymd> &dates() const; // 出过名单的日, 升序
  bool has_date(misc::Ymd d) const;
  // 第 D 日成员位. D 不在 dates() → assert (回测区间必须被名单覆盖, 见 ComputeService 的前置检查);
  // code 不在并集 → false (轴上但从未进过池)
  bool in_pool(std::string_view code, misc::Ymd d) const;

private:
  bool all_ = false;
  std::vector<misc::Ymd> dates_;
  std::vector<std::string> codes_;
  std::unordered_map<std::string, uint32_t> code_idx_;
  std::vector<uint8_t> member_; // [date_i * codes_.size() + code_i]
};

} // namespace universe
