#pragma once

#include "define/Dtype.hpp"
#include <vector>

class TechnicalAnalysis {
public:
  TechnicalAnalysis(std::vector<Table::Snapshot_3s_Record> &snapshots,
                    std::vector<Table::Bar_1m_Record> &bars);

  void update();

private:
  std::vector<Table::Snapshot_3s_Record> *snapshots_;
  std::vector<Table::Bar_1m_Record> *bars_;
};
