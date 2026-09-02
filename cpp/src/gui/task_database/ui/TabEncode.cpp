// Tab Encode Implementation
#include "gui/task_database/ui/TabEncode.hpp"
#include "gui/task_database/models/SharedTypes.hpp"
#include "gui/task_database/services/EncodingService.hpp"
#include "gui/task_database/services/ScanService.hpp"
#include "imgui.h"
#include "shared/Asset.hpp"

#include <algorithm>
#include <cassert>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace GUI::Database {

namespace {

int cmp_size(size_t a, size_t b) {
  return (a > b) - (a < b);
}

// Archives / Orders 两个页签走同一段渲染, 差别只在取哪一组计数.
struct MissingDim {
  size_t Asset::AssetStats::*count;
  std::vector<std::string> Asset::AssetStats::*sample;
};
constexpr MissingDim kArchiveDim{&Asset::AssetStats::archive_missing, &Asset::AssetStats::archive_missing_sample};
constexpr MissingDim kOrderDim{&Asset::AssetStats::orders_missing, &Asset::AssetStats::orders_missing_sample};

// 重建表格视图: 只留该维度确有缺失的资产, 再排序.
void rebuild_table_view(AssetTableView &view, const Asset &asset, const MissingDim &dim,
                        ImGuiTableSortSpecs *sort_specs) {
  view.rows.clear();
  for (size_t id = 0; id < asset.asset_stats.size(); ++id) {
    if (asset.asset_stats[id].*(dim.count) > 0)
      view.rows.push_back(id);
  }

  // 默认按缺失天数降序 —— 缺得最多的排最前, 那才是要先处理的
  auto by_missing_desc = [&](size_t a, size_t b) {
    const size_t ma = asset.asset_stats[a].*(dim.count);
    const size_t mb = asset.asset_stats[b].*(dim.count);
    if (ma != mb)
      return ma > mb;
    return asset.items[a].asset_code < asset.items[b].asset_code;
  };

  if (sort_specs && sort_specs->SpecsCount > 0) {
    std::sort(view.rows.begin(), view.rows.end(), [&](size_t a, size_t b) {
      for (int n = 0; n < sort_specs->SpecsCount; n++) {
        const ImGuiTableColumnSortSpecs &spec = sort_specs->Specs[n];
        int delta = 0;
        switch (spec.ColumnIndex) {
        case 0: {
          // 代码定长 6 位, 先比代码再比交易所 == 比 "代码.交易所", 但不拼串
          const AssetItem &ia = asset.items[a];
          const AssetItem &ib = asset.items[b];
          delta = ia.asset_code.compare(ib.asset_code);
          if (delta == 0)
            delta = ia.exchange.compare(ib.exchange);
          break;
        }
        case 1:
          delta = asset.items[a].asset_name.compare(asset.items[b].asset_name);
          break;
        case 2:
          delta = cmp_size(static_cast<size_t>(GetBoardType(asset.items[a].asset_code)),
                           static_cast<size_t>(GetBoardType(asset.items[b].asset_code)));
          break;
        case 3:
          delta = cmp_size(asset.asset_stats[a].*(dim.count), asset.asset_stats[b].*(dim.count));
          break;
        }
        if (delta != 0)
          return (spec.SortDirection == ImGuiSortDirection_Ascending) ? (delta < 0) : (delta > 0);
      }
      return false;
    });
  } else {
    std::sort(view.rows.begin(), view.rows.end(), by_missing_desc);
  }

  view.built = true;
  view.generation = asset.asset_stats_generation;
}

// 视图过期就重建. 排序规则变化由 ImGui 的 SpecsDirty 告知.
void sync_table_view(AssetTableView &view, const Asset &asset, const MissingDim &dim) {
  ImGuiTableSortSpecs *sort_specs = ImGui::TableGetSortSpecs();
  const bool specs_dirty = sort_specs && sort_specs->SpecsDirty;
  if (!view.built || view.generation != asset.asset_stats_generation || specs_dirty) {
    rebuild_table_view(view, asset, dim, sort_specs);
    if (sort_specs)
      sort_specs->SpecsDirty = false;
  }
}

// 两个页签共用的缺失表. what = "archive" / "orders", 只用于文案.
void render_missing_table(const char *table_id, const Asset &asset, AssetTableView &view,
                          const MissingDim &dim, const char *what) {
  size_t assets_affected = 0;
  size_t asset_days_missing = 0;
  for (const auto &st : asset.asset_stats) {
    const size_t n = st.*(dim.count);
    if (n > 0) {
      assets_affected++;
      asset_days_missing += n;
    }
  }

  ImGui::Text("Missing:");
  ImGui::SameLine();
  if (assets_affected == 0) {
    ImGui::TextColored(ImVec4(0.3f, 0.95f, 0.4f, 1.0f), "none — every listed asset has %s for all its trading days", what);
    return;
  }
  ImGui::TextColored(ImVec4(1.0f, 0.5f, 0.0f, 1.0f), "%zu assets, %zu asset-days",
                     assets_affected, asset_days_missing);
  ImGui::TextDisabled("Trading days the asset was listed and not suspended, but has no %s", what);

  ImGui::Spacing();

  if (ImGui::BeginTable(table_id, 5,
                        ImGuiTableFlags_Borders | ImGuiTableFlags_RowBg | ImGuiTableFlags_ScrollY |
                            ImGuiTableFlags_Sortable | ImGuiTableFlags_SizingFixedFit,
                        ImVec2(0, 400))) {
    ImGui::TableSetupColumn("Asset");
    ImGui::TableSetupColumn("Name");
    ImGui::TableSetupColumn("Board");
    ImGui::TableSetupColumn("Missing", ImGuiTableColumnFlags_DefaultSort |
                                           ImGuiTableColumnFlags_PreferSortDescending);
    ImGui::TableSetupColumn("Dates", ImGuiTableColumnFlags_NoSort | ImGuiTableColumnFlags_WidthStretch);
    ImGui::TableSetupScrollFreeze(1, 1);

    ImGui::TableNextRow(ImGuiTableRowFlags_Headers);
    for (int column = 0; column < 5; column++) {
      ImGui::TableSetColumnIndex(column);
      const char *label = "";
      const char *tooltip = "";
      switch (column) {
      case 0:
        label = "Asset";
        tooltip = "资产代码\n格式: 代码.交易所 (如 600000.SH)";
        break;
      case 1:
        label = "Name";
        tooltip = "公司简称\n来源: 基本面 (cn_stock_instruments); 查不到就留空";
        break;
      case 2:
        label = "Board";
        tooltip = "板块\n由代码前缀判定: 沪主板 600/601/603/605, 深主板 000/001/002/003/004,\n"
                  "科创板 688/689, 创业板 300/301/302/309, 北交所 43/83/87/88/92";
        break;
      case 3:
        label = "Missing";
        tooltip = "回测区间内缺失的交易日数\n分母是该资产已上市未退市且未停牌的交易日";
        break;
      case 4:
        label = "Dates";
        tooltip = "缺失日期 (只列前若干个)";
        break;
      }
      ImGui::TableHeader(label);
      if (ImGui::IsItemHovered()) {
        ImGui::BeginTooltip();
        ImGui::TextUnformatted(tooltip);
        ImGui::EndTooltip();
      }
    }

    sync_table_view(view, asset, dim);

    for (const size_t id : view.rows) {
      const AssetItem &item = asset.items[id];
      const Asset::AssetStats &stats = asset.asset_stats[id];
      const size_t missing = stats.*(dim.count);
      const std::vector<std::string> &sample = stats.*(dim.sample);

      ImGui::TableNextRow();
      ImGui::TableNextColumn();
      ImGui::Text("%s.%s", item.asset_code.c_str(), item.exchange.c_str());

      ImGui::TableNextColumn();
      if (item.asset_name.empty())
        ImGui::TextDisabled("-");
      else
        ImGui::TextUnformatted(item.asset_name.c_str());

      ImGui::TableNextColumn();
      ImGui::TextUnformatted(GetBoardName(GetBoardType(item.asset_code)));

      ImGui::TableNextColumn();
      ImGui::TextColored(ImVec4(1.0f, 0.5f, 0.0f, 1.0f), "%zu / %zu", missing, stats.expected_days);

      ImGui::TableNextColumn();
      std::string dates;
      for (const auto &d : sample) {
        if (!dates.empty())
          dates += "  ";
        dates += d;
      }
      if (missing > sample.size())
        dates += "  +" + std::to_string(missing - sample.size()) + " more";
      ImGui::TextUnformatted(dates.c_str());
    }

    ImGui::EndTable();
  }
}

// ============================================================================
// By Date — 把同一批缺口按日期归堆
// ============================================================================
//
// 按资产看缺口, 一天集体出事会摊成几百行"各缺一天", 看不出那是同一件事.
// 按日期看就一目了然, 再把原因拆开就知道该去修归档还是去查源数据.

// archive 只有"整天没有归档"这一种原因; orders 的原因要从编码器留在
// orders/YYYY/MM/DD/.day_complete 里的当天账目取. 一个开关, 两张表共用代码.
enum class DateDim { Archive,
                     Orders };

struct DateRow {
  std::string date;
  size_t expected = 0; // 当天本该有产物的标的数
  size_t missing = 0;  // 其中没有的 — 排序键
  size_t no_archive = 0;
  size_t skipped = 0;
  size_t corrupt = 0;
  size_t invalid = 0;
  size_t failed = 0;
  size_t todo = 0;                      // 缺口里账目解释不掉的部分 = 还没编到
  const EncodeDayRecord *rec = nullptr; // 判据明细; 没编过这天就是 null
};

DateRow make_date_row(const Asset &asset, const std::string &date,
                      const Asset::DateGap &gap, DateDim dim) {
  DateRow row;
  row.date = date;
  row.expected = gap.expected;

  if (dim == DateDim::Archive) {
    row.missing = gap.archive_missing;
    return row;
  }

  row.missing = gap.orders_missing;
  row.no_archive = gap.archive_missing;
  if (auto it = asset.day_records.find(date); it != asset.day_records.end()) {
    row.rec = &it->second;
    row.skipped = it->second.assets_skipped;
    row.corrupt = it->second.assets_corrupt;
    row.invalid = it->second.assets_invalid;
    row.failed = it->second.assets_failed;
  }

  // 账目的分母是归档里有委托文件的资产, 缺口的分母是"已上市未退市未停牌"的
  // 日历口径 — 两者不严格相等, 所以这里是相减兜底而不是精确配平. 减不掉的
  // 那部分就是"有源、没出错、只是还没编到".
  const size_t explained =
      row.no_archive + row.skipped + row.corrupt + row.invalid + row.failed;
  row.todo = row.missing > explained ? row.missing - explained : 0;
  return row;
}

// 一列 = 一个原因. field 非空就是固定分类列, 否则按 bit 取判据命中数.
struct DateColumnSpec {
  const char *abbr;
  const char *desc;
  size_t DateRow::*field;
  size_t bit;
};

size_t column_value(const DateRow &row, const DateColumnSpec &spec) {
  if (spec.field)
    return row.*(spec.field);
  return row.rec ? row.rec->checks[spec.bit] : 0;
}

// 原因列只给"这批日期里真发生过"的出列 — 判据有十几条, 一次扫描通常只命中
// 三四条, 全列出来表宽得没法看.
std::vector<DateColumnSpec> build_date_columns(const DateTableView &view) {
  std::vector<DateColumnSpec> columns;
  auto add = [&columns](bool present, const char *abbr, const char *desc,
                        size_t DateRow::*field) {
    if (present)
      columns.push_back({abbr, desc, field, 0});
  };

  add(view.has_no_archive, "no_arc", "归档里没有这一天\n整天无源可编, 得先把包下下来",
      &DateRow::no_archive);
  add(view.has_unaccounted, "todo", "有归档、编码器也没报错, 只是还没编到\n跑一遍增量即可补齐",
      &DateRow::todo);
  add(view.has_skipped, "skip", "落了 .skip 墓碑\n源数据不足以编码 (停牌 / 文件只有表头), 不是错误",
      &DateRow::skipped);
  add(view.has_corrupt, "corrupt", "源 CSV 坏行, 或归档流中途断掉\n数据源侧的问题, 得修包",
      &DateRow::corrupt);
  add(view.has_invalid, "invalid", "准入校验未过的标的数\n右边各列是它按判据的拆解 (一个标的可命中多条)",
      &DateRow::invalid);
  add(view.has_failed, "fail", "环境错误 (磁盘满 / 压缩失败)\n重跑增量即可重试",
      &DateRow::failed);

  for (const size_t bit : view.check_columns) {
    const L2::CheckMeta &meta = L2::check_meta(bit);
    columns.push_back({meta.abbr, meta.desc, nullptr, bit});
  }
  return columns;
}

void rebuild_date_view(DateTableView &view, const Asset &asset, DateDim dim) {
  view.rows.clear();
  view.check_columns.clear();
  view.has_no_archive = false;
  view.has_unaccounted = false;
  view.has_skipped = false;
  view.has_corrupt = false;
  view.has_invalid = false;
  view.has_failed = false;

  size_t check_totals[L2::kCheckBitCount] = {};
  std::vector<std::pair<std::string, size_t>> ranked; // (日期, 缺口)

  for (const auto &[date, gap] : asset.date_gaps) {
    const DateRow row = make_date_row(asset, date, gap, dim);
    if (row.missing == 0)
      continue;
    ranked.emplace_back(date, row.missing);

    if (dim == DateDim::Archive)
      continue; // 只有"缺失"一种原因, 不出原因列

    view.has_no_archive |= row.no_archive > 0;
    view.has_unaccounted |= row.todo > 0;
    view.has_skipped |= row.skipped > 0;
    view.has_corrupt |= row.corrupt > 0;
    view.has_invalid |= row.invalid > 0;
    view.has_failed |= row.failed > 0;
    if (row.rec)
      for (size_t bit = 0; bit < L2::kCheckBitCount; ++bit)
        check_totals[bit] += row.rec->checks[bit];
  }

  for (size_t bit = 0; bit < L2::kCheckBitCount; ++bit)
    if (check_totals[bit] > 0)
      view.check_columns.push_back(bit);

  // 只有日期与缺口两列可排 —— 原因列是动态的, 列号跟判据对不上号
  std::sort(ranked.begin(), ranked.end(), [&](const auto &a, const auto &b) {
    const int delta = view.sort_column == 0 ? a.first.compare(b.first)
                                            : cmp_size(a.second, b.second);
    if (delta != 0)
      return view.sort_ascending ? delta < 0 : delta > 0;
    return a.first < b.first;
  });

  view.rows.reserve(ranked.size());
  for (auto &entry : ranked)
    view.rows.push_back(std::move(entry.first));

  view.built = true;
  view.generation = asset.asset_stats_generation;
}

void sync_date_view(DateTableView &view, const Asset &asset, DateDim dim) {
  if (!view.built || view.generation != asset.asset_stats_generation)
    rebuild_date_view(view, asset, dim);
}

void render_date_table(const char *table_id, const Asset &asset, DateTableView &view,
                       DateDim dim, const char *what) {
  // 必须先于 BeginTable —— 列数是构造参数, 而哪些原因列存在要等视图建完才知道
  sync_date_view(view, asset, dim);

  size_t asset_days = 0;
  for (const auto &date : view.rows) {
    auto it = asset.date_gaps.find(date);
    assert(it != asset.date_gaps.end() && "By Date: 行里的日期不在 date_gaps 中");
    asset_days += make_date_row(asset, date, it->second, dim).missing;
  }

  ImGui::Text("Days with gaps:");
  ImGui::SameLine();
  if (view.rows.empty()) {
    ImGui::TextColored(ImVec4(0.3f, 0.95f, 0.4f, 1.0f), "none — every trading day has %s for every listed asset", what);
    return;
  }
  ImGui::TextColored(ImVec4(1.0f, 0.5f, 0.0f, 1.0f), "%zu days, %zu asset-days",
                     view.rows.size(), asset_days);

  const std::vector<DateColumnSpec> columns = build_date_columns(view);
  const int column_count = 2 + static_cast<int>(columns.size());

  if (!ImGui::BeginTable(table_id, column_count,
                         ImGuiTableFlags_Borders | ImGuiTableFlags_RowBg | ImGuiTableFlags_ScrollY |
                             ImGuiTableFlags_Sortable | ImGuiTableFlags_SizingFixedFit,
                         ImVec2(0, 320)))
    return;

  // 默认排序只给 Miss, 且降序 —— 出事最多的那天排最前. 两列都标 DefaultSort
  // 的话 ImGui 首帧会挑列号小的那个 (Date 升序), 把默认顺序顶掉.
  ImGui::TableSetupColumn("Date", ImGuiTableColumnFlags_PreferSortAscending);
  ImGui::TableSetupColumn("Miss", ImGuiTableColumnFlags_DefaultSort |
                                      ImGuiTableColumnFlags_PreferSortDescending);
  for (const auto &spec : columns)
    ImGui::TableSetupColumn(spec.abbr, ImGuiTableColumnFlags_NoSort);
  ImGui::TableSetupScrollFreeze(1, 1);

  ImGui::TableNextRow(ImGuiTableRowFlags_Headers);
  for (int column = 0; column < column_count; column++) {
    ImGui::TableSetColumnIndex(column);
    const char *label = column == 0 ? "Date" : (column == 1 ? "Miss" : columns[column - 2].abbr);
    const char *tooltip =
        column == 0 ? "交易日 (YYYYMMDD)"
                    : (column == 1 ? "当天的缺口 / 当天本该有产物的标的数\n分母是已上市未退市、当日未停牌的标的 (北交所不计)"
                                   : columns[column - 2].desc);
    ImGui::TableHeader(label);
    if (ImGui::IsItemHovered()) {
      ImGui::BeginTooltip();
      ImGui::TextUnformatted(tooltip);
      ImGui::EndTooltip();
    }
  }

  // 排序规则只能在表内部问到; 记下来打掉视图, 下一帧连行带列一起重建
  if (ImGuiTableSortSpecs *specs = ImGui::TableGetSortSpecs()) {
    if (specs->SpecsDirty && specs->SpecsCount > 0) {
      view.sort_column = specs->Specs[0].ColumnIndex;
      view.sort_ascending = specs->Specs[0].SortDirection == ImGuiSortDirection_Ascending;
      view.built = false;
    }
    specs->SpecsDirty = false;
  }

  for (const auto &date : view.rows) {
    auto gap_it = asset.date_gaps.find(date);
    assert(gap_it != asset.date_gaps.end() && "By Date: 行里的日期不在 date_gaps 中");
    const DateRow row = make_date_row(asset, date, gap_it->second, dim);

    ImGui::TableNextRow();
    ImGui::TableNextColumn();
    ImGui::TextUnformatted(date.c_str());

    ImGui::TableNextColumn();
    ImGui::TextColored(ImVec4(1.0f, 0.5f, 0.0f, 1.0f), "%zu / %zu", row.missing, row.expected);

    for (const auto &spec : columns) {
      ImGui::TableNextColumn();
      const size_t value = column_value(row, spec);
      if (value == 0)
        ImGui::TextDisabled("-");
      else
        ImGui::Text("%zu", value);
    }
  }

  ImGui::EndTable();
}

} // namespace

void RenderTabEncode(EncodingService *encoding_service, ScanService *scan_service, EncodeState &state, Asset &asset) {
  if (!encoding_service || !scan_service) {
    ImGui::TextDisabled("Services not initialized");
    return;
  }

  // Auto-detect max cores on first run
  if (state.num_workers <= 0) {
    int max_workers = std::thread::hardware_concurrency();
    if (max_workers <= 0)
      max_workers = 8;
    state.num_workers = max_workers;
  }

  const bool is_running = encoding_service->is_running();
  const auto status = encoding_service->get_status();
  const auto progress = encoding_service->get_progress();
  // 这两个返回的是引用, 别按值接 —— 里面的缺失日期/错误文件列表是几千条
  // std::string, 逐帧整份复制就是逐帧几千次分配
  const auto &check_result = scan_service->get_last_check_result();
  const auto &file_check_result = encoding_service->get_file_check_result();
  const bool file_check_running = encoding_service->is_file_check_running();

  // ========================================================================
  // File Check (Archive Validation)
  // ========================================================================

  // 结论直接写进折叠头 —— 这一节是一次性动作, 平时不需要展开占地方
  std::string file_check_label = "File Check: ";
  if (file_check_running)
    file_check_label += "checking...";
  else if (!file_check_result.was_run())
    file_check_label += "not run";
  else if (!file_check_result.archive_dir_exists)
    file_check_label += "no archive dir (using built binaries)";
  else if (!file_check_result.commands_available)
    file_check_label += "missing commands (unrar, 7z, rar, gdb)";
  else if (file_check_result.passed)
    file_check_label += "pass — " + std::to_string(file_check_result.valid_archives) + " archives";
  else
    file_check_label += "FAILED";
  file_check_label += "###FileCheck";

  if (ImGui::CollapsingHeader(file_check_label.c_str())) {
    ImGui::Indent();

    if (file_check_running) {
      ImGui::TextDisabled("Running in background, see Terminal for progress");
    } else if (file_check_result.was_run() && file_check_result.archive_dir_exists &&
               !file_check_result.passed) {
      if (file_check_result.naming_errors > 0)
        ImGui::BulletText("Naming errors: %zu", file_check_result.naming_errors);
      if (file_check_result.format_errors > 0)
        ImGui::BulletText("Format errors: %zu (7z/solid RAR)", file_check_result.format_errors);
      if (file_check_result.structure_errors > 0)
        ImGui::BulletText("Structure errors: %zu", file_check_result.structure_errors);
      if (file_check_result.integrity_errors > 0)
        ImGui::BulletText("Integrity errors: %zu (truncated/corrupt)", file_check_result.integrity_errors);
      if (file_check_result.zip_files > 0)
        ImGui::BulletText("ZIP files: %zu (need conversion)", file_check_result.zip_files);
      ImGui::TextDisabled("(详细错误信息见下方 Terminal)");
    }

    ImGui::BeginDisabled(file_check_running);
    if (ImGui::Button(file_check_running ? "Checking..." : "Run File Check", ImVec2(150, 0))) {
      encoding_service->run_file_check(asset.archive.path);
    }
    ImGui::EndDisabled();
    ImGui::SameLine();
    ImGui::TextDisabled("Check archive format, structure and integrity");

    ImGui::Unindent();
  }

  // ========================================================================
  // Database Coverage Check
  // ========================================================================

  if (ImGui::CollapsingHeader("Database Coverage Check", ImGuiTreeNodeFlags_DefaultOpen)) {
    ImGui::Indent();

    // 状态一行说完, 解释挂在 hover 上 —— 这些说明每次都一样, 常驻会把
    // 下面的表挤下去
    if (scan_service->is_scanning()) {
      ImGui::TextColored(ImVec4(0.0f, 1.0f, 1.0f, 1.0f), "%s", scan_service->get_status_string());
    } else {
      // 每个 DatabaseStatus 都要有分支 —— 漏掉的会掉进 default 显示成
      // "没检查", 而它其实刚检查完.
      ImVec4 color(0.7f, 0.7f, 0.7f, 1.0f);
      const char *name = "";
      const char *hint = "";
      switch (check_result.status) {
      case DatabaseStatus::Unchecked:
        color = ImVec4(0.7f, 0.7f, 0.7f, 1.0f);
        name = "Not checked";
        hint = "Coverage check runs automatically after fundamental sync";
        break;
      case DatabaseStatus::Error:
        color = ImVec4(1.0f, 0.0f, 0.0f, 1.0f);
        name = "ERROR";
        hint = check_result.error_message.c_str();
        break;
      case DatabaseStatus::Pass:
        color = ImVec4(0.0f, 1.0f, 0.0f, 1.0f);
        name = "Pass";
        hint = "All required dates for backtest period are encoded";
        break;
      case DatabaseStatus::Incomplete:
        color = ImVec4(1.0f, 1.0f, 0.0f, 1.0f);
        name = "Incomplete";
        hint = "Missing dates all have archives — run encoding to fill them";
        break;
      case DatabaseStatus::NotEncoded:
        color = ImVec4(0.3f, 0.7f, 1.0f, 1.0f);
        name = "NotEncoded";
        hint = "Archives cover the backtest period, nothing encoded yet";
        break;
      case DatabaseStatus::NeedArchive:
        color = ImVec4(1.0f, 0.5f, 0.0f, 1.0f);
        name = "NeedArchive";
        hint = "Those days must be downloaded before they can be encoded";
        break;
      case DatabaseStatus::NoData:
        color = ImVec4(1.0f, 0.0f, 0.0f, 1.0f);
        name = "NoData";
        hint = check_result.error_message.c_str();
        break;
      }
      ImGui::TextColored(color, "%s", name);
      if (ImGui::IsItemHovered())
        ImGui::SetTooltip("%s", hint);

      if (!check_result.error_message.empty() &&
          (check_result.status == DatabaseStatus::NeedArchive ||
           check_result.status == DatabaseStatus::Error ||
           check_result.status == DatabaseStatus::NoData)) {
        ImGui::SameLine();
        ImGui::TextColored(color, "— %s", check_result.error_message.c_str());
      }

      // 覆盖进度与缺口明细对所有"检查跑完且有缺口"的状态都适用
      if (!check_result.missing_dates.empty() && check_result.required_dates > 0) {
        ImGui::SameLine(0.0f, 24.0f);
        ImGui::Text("Covered %zu / %zu days (%.1f%%)",
                    check_result.binary_coverage,
                    check_result.required_dates,
                    100.0 * check_result.binary_coverage / check_result.required_dates);
        ImGui::SameLine(0.0f, 24.0f);
        ImGui::Text("can encode %zu / need download %zu",
                    check_result.missing_can_encode.size(),
                    check_result.missing_no_archive.size());

        ImGui::SameLine(0.0f, 24.0f);
        ImGui::Checkbox("dates", &state.show_missing_details);
        if (state.show_missing_details) {
          ImGui::BeginChild("MissingDates", ImVec2(0, 100), true);
          for (const auto &date : check_result.missing_no_archive) {
            ImGui::TextColored(ImVec4(1.0f, 0.5f, 0.0f, 1.0f), "%s  (no archive)", date.c_str());
          }
          for (const auto &date : check_result.missing_can_encode) {
            ImGui::TextColored(ImVec4(1.0f, 1.0f, 0.0f, 1.0f), "%s  (can encode)", date.c_str());
          }
          ImGui::EndChild();
        }
      }
    }

    ImGui::Unindent();
  }

  // ========================================================================
  // Control Panel
  // ========================================================================

  if (ImGui::CollapsingHeader("Control Panel", ImGuiTreeNodeFlags_DefaultOpen)) {
    ImGui::Indent();

    // 参数与启动挤在一行 — 平时只是看一眼, 不值得占三行
    int max_workers = std::thread::hardware_concurrency();
    if (max_workers <= 0)
      max_workers = 8;
    ImGui::SetNextItemWidth(200);
    ImGui::SliderInt("workers", &state.num_workers, 1, max_workers);
    if (ImGui::IsItemHovered())
      ImGui::SetTooltip("Worker threads (max: %d)", max_workers);

    ImGui::SameLine(0.0f, 16.0f);
    ImGui::Checkbox("Skip existing", &state.skip_existing);
    ImGui::SameLine(0.0f, 16.0f);

    // Start/Stop button
    if (is_running) {
      if (ImGui::Button("Stop Encoding", ImVec2(150, 0))) {
        // Trigger stop (non-blocking)
        // service->stop_encoding() should be called from coroutine context
      }
      ImGui::SameLine();
      ImGui::TextColored(ImVec4(1.0f, 0.5f, 0.0f, 1.0f), "Encoding in progress...");
    } else {
      // File Check 不再硬性挡住入口 — 未通过时在确认弹窗里要求"风险自负"勾选
      bool file_check_ok = file_check_result.was_run() &&
                           (file_check_result.passed || !file_check_result.archive_dir_exists);

      if (ImGui::Button("Start Encoding", ImVec2(150, 0))) {
        state.skip_file_check_ack = false; // 每次打开弹窗都要重新勾
        state.show_confirm_dialog = true;
      }

      ImGui::SameLine();
      if (!file_check_result.was_run()) {
        ImGui::TextColored(ImVec4(1.0f, 1.0f, 0.0f, 1.0f), "[!] File Check Not Run");
      } else if (!file_check_result.archive_dir_exists) {
        ImGui::TextColored(ImVec4(0.7f, 0.7f, 0.7f, 1.0f), "[✓] No Archive");
      } else if (!file_check_ok) {
        ImGui::TextColored(ImVec4(1.0f, 0.0f, 0.0f, 1.0f), "[✗] File Check Failed");
      } else {
        ImGui::TextColored(ImVec4(0.0f, 1.0f, 0.0f, 1.0f), "[✓] File Check Passed");
      }

      ImGui::SameLine();
      ImGui::TextDisabled("|");
      ImGui::SameLine();

      if (status == EncodingStatus::Completed) {
        ImGui::TextColored(ImVec4(0.0f, 1.0f, 0.0f, 1.0f), "Completed");
      } else if (status == EncodingStatus::Cancelled) {
        ImGui::TextColored(ImVec4(1.0f, 1.0f, 0.0f, 1.0f), "Cancelled");
      } else if (status == EncodingStatus::Error) {
        ImGui::TextColored(ImVec4(1.0f, 0.0f, 0.0f, 1.0f), "Error");
      } else {
        ImGui::TextDisabled("Idle");
      }
    }

    ImGui::Unindent();
  }

  // ========================================================================
  // Confirmation Dialog (Fullscreen Modal)
  // ========================================================================

  if (state.show_confirm_dialog) {
    ImGui::OpenPopup("Confirm Encoding");
    ImVec2 center = ImGui::GetMainViewport()->GetCenter();
    ImGui::SetNextWindowPos(center, ImGuiCond_Appearing, ImVec2(0.5f, 0.5f));
    ImGui::SetNextWindowSize(ImVec2(600, 400));
  }

  if (ImGui::BeginPopupModal("Confirm Encoding", &state.show_confirm_dialog, ImGuiWindowFlags_NoResize)) {
    ImGui::TextColored(ImVec4(1.0f, 0.0f, 0.0f, 1.0f), "WARNING");
    ImGui::Separator();
    ImGui::Spacing();

    ImGui::PushStyleColor(ImGuiCol_Text, ImVec4(1.0f, 1.0f, 0.0f, 1.0f));
    ImGui::TextWrapped("Encoding may overwrite or corrupt existing database files. Please consider moving your database to a backup location before proceeding.");
    ImGui::Spacing();
    ImGui::TextWrapped("Encoding process takes a long time and cannot be interrupted once started. Please be ready.");
    ImGui::PopStyleColor();

    ImGui::Spacing();
    ImGui::Separator();
    ImGui::Spacing();

    // Check prerequisites
    bool archive_in_range = false;
    if (!asset.backtest.start.empty() && !asset.backtest.end.empty() &&
        !asset.archive.min_date.empty() && !asset.archive.max_date.empty()) {
      archive_in_range = (asset.archive.min_date <= asset.backtest.start &&
                          asset.backtest.end <= asset.archive.max_date);
    }

    bool file_check_ok = file_check_result.was_run() &&
                         (file_check_result.passed || !file_check_result.archive_dir_exists);

    bool can_encode = asset.archive.exists && archive_in_range &&
                      (file_check_ok || state.skip_file_check_ack);

    ImGui::Text("Prerequisites:");
    ImGui::Spacing();

    ImGui::BulletText("File Check:");
    ImGui::SameLine();
    if (!file_check_result.was_run()) {
      ImGui::TextColored(ImVec4(0.95f, 0.3f, 0.3f, 1.0f), "Not Run");
      ImGui::SameLine();
      ImGui::TextDisabled("(请先运行 File Check)");
    } else if (!file_check_result.archive_dir_exists) {
      ImGui::TextColored(ImVec4(0.7f, 0.7f, 0.7f, 1.0f), "N/A (no archive)");
    } else if (file_check_result.passed) {
      ImGui::TextColored(ImVec4(0.3f, 0.95f, 0.4f, 1.0f), "Passed");
    } else {
      ImGui::TextColored(ImVec4(0.95f, 0.3f, 0.3f, 1.0f), "Failed");
      ImGui::SameLine();
      ImGui::TextDisabled("(%zu errors)", file_check_result.naming_errors +
                                              file_check_result.format_errors +
                                              file_check_result.structure_errors +
                                              file_check_result.zip_files);
    }

    ImGui::BulletText("Archive Path Exists:");
    ImGui::SameLine();
    if (asset.archive.exists) {
      ImGui::TextColored(ImVec4(0.3f, 0.95f, 0.4f, 1.0f), "Yes");
    } else {
      ImGui::TextColored(ImVec4(0.95f, 0.3f, 0.3f, 1.0f), "No");
    }

    ImGui::BulletText("Archive In Range:");
    ImGui::SameLine();
    if (archive_in_range) {
      ImGui::TextColored(ImVec4(0.3f, 0.95f, 0.4f, 1.0f), "Yes");
    } else {
      ImGui::TextColored(ImVec4(0.95f, 0.3f, 0.3f, 1.0f), "No");
    }

    if (!file_check_ok) {
      ImGui::Spacing();
      ImGui::PushStyleColor(ImGuiCol_Text, ImVec4(0.95f, 0.3f, 0.3f, 1.0f));
      ImGui::TextWrapped(file_check_result.was_run()
                             ? "File Check 未通过: 损坏/solid/结构错误的包会让编码中途 assert 崩溃或静默产出垃圾数据。"
                             : "File Check 未运行: 无法确认归档格式与完整性, 编码可能中途失败。");
      ImGui::PopStyleColor();
      ImGui::Checkbox("跳过 File Check, 风险自负##skip_fc", &state.skip_file_check_ack);
    }

    if (!can_encode) {
      ImGui::Spacing();
      ImGui::TextColored(ImVec4(1.0f, 0.0f, 0.0f, 1.0f), "Cannot encode: Prerequisites not met!");
    }

    ImGui::Spacing();
    ImGui::Separator();
    ImGui::Spacing();

    ImGui::Text("Encoding Parameters:");
    ImGui::Spacing();
    ImGui::BulletText("Worker Threads: %d", state.num_workers);
    ImGui::BulletText("Skip Existing: %s", state.skip_existing ? "Yes" : "No");

    ImGui::Spacing();
    ImGui::Text("Paths:");
    ImGui::Spacing();
    ImGui::BulletText("Archive Path:");
    ImGui::SameLine();
    ImGui::TextColored(ImVec4(1.0f, 1.0f, 0.0f, 1.0f), "%s", asset.archive.path.c_str());

    ImGui::BulletText("Binary Path:");
    ImGui::SameLine();
    ImGui::TextColored(ImVec4(1.0f, 1.0f, 0.0f, 1.0f), "%s", asset.binary.path.c_str());

    ImGui::Spacing();
    ImGui::Text("Date Ranges:");
    ImGui::Spacing();
    ImGui::BulletText("Backtest Range: %s ~ %s",
                      asset.backtest.start.c_str(), asset.backtest.end.c_str());
    if (asset.archive.exists) {
      ImGui::BulletText("Archive Range: %s ~ %s",
                        asset.archive.min_date.c_str(), asset.archive.max_date.c_str());
    }
    if (asset.binary.exists) {
      ImGui::BulletText("Binary Range: %s ~ %s",
                        asset.binary.min_date.c_str(), asset.binary.max_date.c_str());
    }

    ImGui::Spacing();
    ImGui::Separator();
    ImGui::Spacing();

    float button_width = 120;
    float spacing = ImGui::GetStyle().ItemSpacing.x;
    float total_width = button_width * 2 + spacing;
    float offset = (ImGui::GetContentRegionAvail().x - total_width) * 0.5f;

    ImGui::SetCursorPosX(ImGui::GetCursorPosX() + offset);

    ImGui::BeginDisabled(!can_encode);
    if (ImGui::Button("Confirm and Start", ImVec2(button_width, 40))) {
      state.show_confirm_dialog = false;
      state.show_progress_fullscreen = true;
      state.trigger_start = true; // Signal TaskDatabase to start encoding
    }
    ImGui::EndDisabled();

    ImGui::SameLine();
    if (ImGui::Button("Cancel", ImVec2(button_width, 40))) {
      state.show_confirm_dialog = false;
    }

    ImGui::EndPopup();
  }

  // ========================================================================
  // Fullscreen Progress View (when encoding is running)
  // ========================================================================

  if (state.show_progress_fullscreen && is_running) {
    ImGui::SetNextWindowPos(ImVec2(0, 0));
    ImGui::SetNextWindowSize(ImGui::GetIO().DisplaySize);
    if (ImGui::Begin("Encoding Progress", nullptr, ImGuiWindowFlags_NoTitleBar | ImGuiWindowFlags_NoResize | ImGuiWindowFlags_NoMove | ImGuiWindowFlags_NoCollapse)) {

      ImVec2 window_size = ImGui::GetWindowSize();
      float center_x = window_size.x * 0.5f;
      float center_y = window_size.y * 0.5f;

      ImGui::SetCursorPosY(center_y - 150);

      // Title
      ImGui::SetCursorPosX(center_x - 100);
      ImGui::TextColored(ImVec4(0.3f, 0.7f, 1.0f, 1.0f), "Encoding in Progress...");

      ImGui::Spacing();
      ImGui::Spacing();

      // Assets Progress
      float assets_progress = progress.total_assets > 0 ? (float)progress.completed_assets / progress.total_assets : 0.0f;
      ImGui::SetCursorPosX(center_x - 200);
      ImGui::Text("Assets: %zu / %zu (%.1f%%)",
                  progress.completed_assets, progress.total_assets,
                  assets_progress * 100.0f);
      ImGui::SetCursorPosX(center_x - 200);
      ImGui::ProgressBar(assets_progress, ImVec2(400, 30));

      ImGui::Spacing();
      ImGui::Spacing();

      // Dates Progress
      float dates_progress = progress.total_dates > 0 ? (float)progress.encoded_dates / progress.total_dates : 0.0f;
      ImGui::SetCursorPosX(center_x - 200);
      ImGui::Text("Trading Days: %zu / %zu (%.1f%%)",
                  progress.encoded_dates, progress.total_dates,
                  dates_progress * 100.0f);
      ImGui::SetCursorPosX(center_x - 200);
      ImGui::ProgressBar(dates_progress, ImVec2(400, 30));

      ImGui::Spacing();
      ImGui::Spacing();
      ImGui::Spacing();

      // Statistics
      // 条数只有跑过明细扫描才有值 (见 Asset::DateInfo), 没有就不占一行
      if (progress.total_orders > 0) {
        ImGui::SetCursorPosX(center_x - 150);
        ImGui::Text("Total Orders: %zu", progress.total_orders);
      }

      ImGui::SetCursorPosX(center_x - 150);
      ImGui::Text("Elapsed Time: %.1f s", progress.elapsed_seconds);

      ImGui::SetCursorPosX(center_x - 150);
      ImGui::Text("Encoding Rate: %.2f assets/s", progress.encoding_rate);

      ImGui::Spacing();
      ImGui::Spacing();
      ImGui::Spacing();

      // Stop button
      ImGui::SetCursorPosX(center_x - 75);
      if (ImGui::Button("Stop Encoding", ImVec2(150, 40))) {
        // Trigger stop
        // service->stop_encoding() should be called from coroutine context
      }

      ImGui::End();
    }
  } else if (state.show_progress_fullscreen && !is_running) {
    // Encoding finished, close fullscreen view
    state.show_progress_fullscreen = false;
  }

  // Only show Asset Summary when not in fullscreen progress mode
  if (!state.show_progress_fullscreen) {

    // ========================================================================
    // Asset Summary
    // ========================================================================

    if (ImGui::CollapsingHeader("Asset Summary", ImGuiTreeNodeFlags_DefaultOpen)) {
      ImGui::Indent();

      // 统计由扫描末尾一次算好 (见 ScanService Phase 5)
      const bool stats_ready = asset.asset_stats.size() == asset.items.size() && !asset.items.empty();

      ImGui::Spacing();

      if (!stats_ready) {
        ImGui::TextDisabled("Waiting for database scan...");
      } else if (ImGui::BeginTabBar("AssetSummaryTabs", ImGuiTabBarFlags_None)) {

        // 分母都是回测区间内的交易日 (日历为准), 不是各自库扫出来的天数 ——
        // 后者做分母的话, 缺的那些天连同分母一起消失, 永远是 100%.
        const size_t required_days = asset.backtest.required_dates.size();

        // ========================================================================
        // Archives Tab
        // ========================================================================
        if (ImGui::BeginTabItem("Archives")) {
          ImGui::Spacing();

          size_t archive_days_in_backtest = 0;
          for (const auto &date : asset.backtest.required_dates) {
            if (asset.archive.dates.count(date))
              archive_days_in_backtest++;
          }

          ImGui::Text("Database Path:");
          ImGui::SameLine();
          ImGui::TextColored(ImVec4(0.3f, 0.95f, 0.4f, 1.0f), "%s", asset.archive.path.c_str());

          ImGui::Text("Backtest Range (Target):");
          ImGui::SameLine();
          if (required_days > 0) {
            ImGui::TextColored(ImVec4(0.3f, 0.7f, 1.0f, 1.0f), "%s ~ %s (%zu/%zu trading days, %.1f%%)",
                               asset.backtest.start.c_str(), asset.backtest.end.c_str(),
                               archive_days_in_backtest, required_days,
                               100.0 * archive_days_in_backtest / required_days);
          } else {
            ImGui::TextDisabled("Not configured");
          }

          ImGui::Spacing();
          ImGui::Separator();
          ImGui::Spacing();

          if (asset.archive.exists) {
            ImGui::Text("Range (Scanned):");
            ImGui::SameLine();
            ImGui::TextColored(ImVec4(0.3f, 0.7f, 1.0f, 1.0f), "%s ~ %s (%zu files, %.2f GB)",
                               asset.archive.min_date.c_str(), asset.archive.max_date.c_str(),
                               asset.archive.total_files, asset.archive.total_size_gb);

            ImGui::Spacing();
            ImGui::Separator();
            ImGui::Spacing();

            render_missing_table("archive_missing_table", asset, state.archive_view, kArchiveDim, "archive");
          } else {
            ImGui::TextColored(ImVec4(0.6f, 0.6f, 0.6f, 1.0f), "No archive database found");
          }

          ImGui::EndTabItem();
        }

        // ========================================================================
        // Orders Tab
        // ========================================================================
        if (ImGui::BeginTabItem("Orders")) {
          ImGui::Spacing();

          const size_t order_days_in_backtest = asset.binary.backtest_order_days;

          ImGui::Text("Database Path:");
          ImGui::SameLine();
          ImGui::TextColored(ImVec4(0.3f, 0.95f, 0.4f, 1.0f), "%s", asset.binary.path.c_str());

          ImGui::Text("Backtest Range (Target):");
          ImGui::SameLine();
          if (required_days > 0) {
            ImGui::TextColored(ImVec4(0.3f, 0.7f, 1.0f, 1.0f), "%s ~ %s (%zu/%zu trading days, %.1f%%)",
                               asset.backtest.start.c_str(), asset.backtest.end.c_str(),
                               order_days_in_backtest, required_days,
                               100.0 * order_days_in_backtest / required_days);
          } else {
            ImGui::TextDisabled("Not configured");
          }

          ImGui::Spacing();
          ImGui::Separator();
          ImGui::Spacing();

          if (asset.binary.exists) {
            ImGui::Text("Range (Scanned):");
            ImGui::SameLine();
            ImGui::TextColored(ImVec4(0.3f, 0.7f, 1.0f, 1.0f), "%s ~ %s (%zu assets, %.2f GB)",
                               asset.binary.min_date.c_str(), asset.binary.max_date.c_str(),
                               asset.binary.encoded_assets, asset.binary.orders_size_gb);

            ImGui::Spacing();
            ImGui::Separator();
            ImGui::Spacing();

            render_missing_table("order_missing_table", asset, state.order_view, kOrderDim, "orders");
          } else {
            ImGui::TextColored(ImVec4(0.6f, 0.6f, 0.6f, 1.0f), "No binary database found");
          }

          ImGui::EndTabItem();
        }

        // ========================================================================
        // By Date Tab — 同一批缺口按日期归堆, 再按原因拆开
        // ========================================================================
        if (ImGui::BeginTabItem("By Date")) {
          ImGui::Spacing();

          ImGui::SeparatorText("Archives");
          render_date_table("archive_date_table", asset, state.archive_date_view,
                            DateDim::Archive, "archives");

          ImGui::Spacing();
          ImGui::SeparatorText("Orders");
          render_date_table("order_date_table", asset, state.order_date_view,
                            DateDim::Orders, "orders");

          ImGui::EndTabItem();
        }

        ImGui::EndTabBar();
      }

      ImGui::Unindent();
    }

  } // End: if (!state.show_progress_fullscreen)
}

} // namespace GUI::Database
