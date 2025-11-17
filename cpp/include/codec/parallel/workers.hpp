#pragma once

#include <string>

namespace L2 {
namespace Parallel {

/**
 * Process a single stock's data files and encode to binary format
 * @param asset_dir Directory containing CSV files for the asset
 * @param asset_code Stock code (e.g., 600000.SH)
 * @param date_str Date string in YYYYMMDD format
 * @param OUTPUT_DIR Base output directory
 * @param compression_ratio Output parameter for compression ratio achieved
 * @return true if processing succeeded
 */
bool process_stock_data(const std::string &asset_dir,
                        const std::string &asset_code,
                        const std::string &date_str,
                        const std::string &OUTPUT_DIR,
                        double &compression_ratio);

/**
 * Decompression worker (producer) - follows exact design from workers.cpp
 * @param worker_id ID of this decompression worker thread
 */
void decompression_worker(unsigned int worker_id);

/**
 * Encoding worker (consumer) - follows exact design from workers.cpp
 * @param core_id CPU core to bind this thread to
 */
void encoding_worker(unsigned int core_id);

/**
 * Decompress 7z file using system command
 * @param archive_path Path to the 7z archive
 * @param output_dir Directory to extract to
 * @return true if decompression succeeded
 */
bool decompress_7z(const std::string &archive_path, const std::string &output_dir);

} // namespace Parallel
} // namespace L2
