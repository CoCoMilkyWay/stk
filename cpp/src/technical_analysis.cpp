#include "technical_analysis.hpp"

TechnicalAnalysis::TechnicalAnalysis(std::vector<Table::Snapshot_3s_Record> &snapshots,
                                     std::vector<Table::Bar_1m_Record> &bars)
    : snapshots_(&snapshots), bars_(&bars) {
}

void TechnicalAnalysis::update() {
}
