#pragma once

#include <string>
#include <vector>

// Zstandard dictionary training and compression
#include "../../package/zstd-1.5.7/zstd.h"
#include "../../package/zstd-1.5.7/zdict.h"

namespace L2 {

// Dictionary compression configuration
inline constexpr size_t DICT_MAX_SIZE = 64 * 1024;  // 64KB dictionary size
inline constexpr size_t DICT_MIN_SAMPLES = 10;      // Minimum samples needed for training
inline constexpr size_t DICT_MAX_SAMPLES = 1000;    // Maximum samples to use for training

class BinaryDict_L2 {
public:
    // Dictionary training and batch compression
    static bool train_and_compress_directory(const std::string& input_dir, 
                                            const std::string& output_dir,
                                            const std::string& dict_output_path = "");
    
    // Train dictionary from existing .bin files
    static std::vector<char> train_dictionary_from_bins(const std::vector<std::string>& bin_files);
    
    // Compress single file with dictionary
    static bool compress_bin_with_dict(const std::string& input_bin_path,
                                     const std::string& output_bin_path,
                                     const std::vector<char>& dictionary);
    
    // Decompress single file with dictionary  
    static bool decompress_bin_with_dict(const std::string& input_bin_path,
                                        const std::string& output_bin_path,
                                        const std::vector<char>& dictionary);
    
    // Batch compress all .bin files in directory with dictionary
    static bool compress_directory_with_dict(const std::string& input_dir,
                                            const std::string& output_dir,
                                            const std::vector<char>& dictionary);
    
    // Utility functions
    static std::vector<std::string> find_bin_files(const std::string& directory);
    static bool save_dictionary(const std::vector<char>& dictionary, const std::string& dict_path);
    static std::vector<char> load_dictionary(const std::string& dict_path);
    
    // Analysis functions
    static void analyze_compression_results(const std::string& original_dir, 
                                          const std::string& dict_compressed_dir);

private:
    // Dictionary compression helper (extends base library)
    static bool compress_data_with_dict(const std::string& filepath, const void* data, size_t data_size,
                                       const std::vector<char>& dictionary);
    
    // Dictionary decompression helper (extends base library)
    static bool decompress_data_with_dict(const std::string& filepath, void* data, size_t expected_size,
                                         size_t& actual_size, const std::vector<char>& dictionary);
    
    // Extract raw data from compressed .bin file (for dictionary training)
    static std::vector<char> extract_raw_data_from_bin(const std::string& bin_file_path);
    
    // Sample selection for training (avoid using all files if too many)
    static std::vector<std::string> select_training_samples(const std::vector<std::string>& all_files);
};

} // namespace L2
