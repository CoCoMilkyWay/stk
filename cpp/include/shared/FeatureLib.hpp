#pragma once

// 特征库 = 档 + A 轴 + 目录. 写端 (ComputeService 建 FeatureStore) 与读端 (FeatureRead / ReadScope / 截面旁读)
// 由同一函数从 config 推导, 两端永远同一 (档, 轴, 目录):
//   Lob  universe 子轴 (cfg.UniverseCodes 并集)  Config::FeatureUniverseDir  (features/<universe>/)
//   Bar  全轴前缀 [0, num_assets)                Config::FeatureAllDir       (features/all/)

#include "features/Backend/FeatureLevels.hpp" // TierId
#include "shared/AssetAxis.hpp"               // UniverseAxis
#include <cstddef>
#include <string>

struct Config;

struct FeatureLib {
  TierId tier = TierId::Lob;
  UniverseAxis uni; // A 轴: 张量列 / 文件列序 / 头指纹
  std::string dir;  // 库目录 (日文件落在其下)
};
FeatureLib feature_lib(const Config &cfg, std::size_t num_assets, TierId tier);
