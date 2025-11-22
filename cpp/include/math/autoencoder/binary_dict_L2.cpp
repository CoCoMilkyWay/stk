#include "codec/binary_dict_L2.hpp"
#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iomanip>
#include <memory>
#include <random>

namespace L2 {

// Main function: Train dictionary and compress entire directory
bool BinaryDict_L2::train_and_compress_directory(const std::string& input_dir, 
                                                 const std::string& output_dir,
                                                 const std::string& dict_output_path) {
    std::cout << "=== Dictionary Training and Compression ===" << std::endl;
    std::cout << "Input directory: " << input_dir << std::endl;
    std::cout << "Output directory: " << output_dir << std::endl;
    
    // Create output directory
    std::filesystem::create_directories(output_dir);
    
    // Find all .bin files
    auto bin_files = find_bin_files(input_dir);
    if (bin_files.empty()) {
        std::cerr << "No .bin files found in " << input_dir << std::endl;
        return false;
    }
    
    std::cout << "Found " << bin_files.size() << " .bin files" << std::endl;
    
    if (bin_files.size() < DICT_MIN_SAMPLES) {
        std::cerr << "Need at least " << DICT_MIN_SAMPLES << " samples for dictionary training" << std::endl;
        return false;
    }
    
    // Train dictionary
    std::cout << "Training dictionary..." << std::endl;
    auto dictionary = train_dictionary_from_bins(bin_files);
    if (dictionary.empty()) {
        std::cerr << "Dictionary training failed" << std::endl;
        return false;
    }
    
    std::cout << "Dictionary trained successfully, size: " << dictionary.size() << " bytes" << std::endl;
    
    // Save dictionary if path provided
    if (!dict_output_path.empty()) {
        if (save_dictionary(dictionary, dict_output_path)) {
            std::cout << "Dictionary saved to: " << dict_output_path << std::endl;
        }
    }
    
    // Compress all files with dictionary
    std::cout << "Compressing files with dictionary..." << std::endl;
    return compress_directory_with_dict(input_dir, output_dir, dictionary);
}

// Train dictionary from existing .bin files
std::vector<char> BinaryDict_L2::train_dictionary_from_bins(const std::vector<std::string>& bin_files) {
    // Select training samples (avoid using too many files)
    auto training_files = select_training_samples(bin_files);
    std::cout << "Using " << training_files.size() << " files for dictionary training" << std::endl;
    
    // Extract raw data from each .bin file
    std::vector<std::vector<char>> samples;
    size_t total_sample_size = 0;
    
    for (const auto& file_path : training_files) {
        auto raw_data = extract_raw_data_from_bin(file_path);
        if (!raw_data.empty()) {
            total_sample_size += raw_data.size();
            samples.push_back(std::move(raw_data));
            std::cout << "Extracted " << samples.back().size() << " bytes from " 
                      << std::filesystem::path(file_path).filename().string() << std::endl;
        }
    }
    
    if (samples.empty()) {
        std::cerr << "No valid samples extracted for dictionary training" << std::endl;
        return {};
    }
    
    // Prepare sample data for ZDICT training
    std::vector<const char*> sample_ptrs;
    std::vector<size_t> sample_sizes;
    
    for (const auto& sample : samples) {
        sample_ptrs.push_back(sample.data());
        sample_sizes.push_back(sample.size());
    }
    
    // Train dictionary using ZDICT
    std::vector<char> dictionary(DICT_MAX_SIZE);
    size_t dict_size = ZDICT_trainFromBuffer(
        dictionary.data(), DICT_MAX_SIZE,
        sample_ptrs.data(), sample_sizes.data(), sample_ptrs.size()
    );
    
    if (ZDICT_isError(dict_size)) {
        std::cerr << "Dictionary training failed: " << ZDICT_getErrorName(dict_size) << std::endl;
        return {};
    }
    
    dictionary.resize(dict_size);
    std::cout << "Dictionary training completed. Trained size: " << dict_size << " bytes" << std::endl;
    std::cout << "Training data: " << samples.size() << " samples, " << total_sample_size << " total bytes" << std::endl;
    
    return dictionary;
}

// Compress single file with dictionary (reuses base library functions)
bool BinaryDict_L2::compress_bin_with_dict(const std::string& input_bin_path,
                                           const std::string& output_bin_path,
                                           const std::vector<char>& dictionary) {
    // Read the original compressed file and extract raw data
    auto raw_data = extract_raw_data_from_bin(input_bin_path);
    if (raw_data.empty()) {
        std::cerr << "Failed to extract raw data from " << input_bin_path << std::endl;
        return false;
    }
    
    // Compress with dictionary
    return compress_data_with_dict(output_bin_path, raw_data.data(), raw_data.size(), dictionary);
}

// Decompress single file with dictionary (reuses base library functions)
bool BinaryDict_L2::decompress_bin_with_dict(const std::string& input_bin_path,
                                            const std::string& output_bin_path,
                                            const std::vector<char>& dictionary) {
    // Get the expected size from filename (same pattern as base library)
    size_t expected_size = BinaryDecoder_L2::extract_count_from_filename(input_bin_path);
    if (expected_size == 0) {
        std::cerr << "Could not extract expected size from filename: " << input_bin_path << std::endl;
        return false;
    }
    
    // Calculate actual expected size (header + data)
    size_t header_size = sizeof(size_t);
    size_t data_size;
    if (input_bin_path.find("_snapshots_") != std::string::npos) {
        data_size = expected_size * sizeof(Snapshot);
    } else if (input_bin_path.find("_orders_") != std::string::npos) {
        data_size = expected_size * sizeof(Order);
    } else {
        std::cerr << "Unknown file type: " << input_bin_path << std::endl;
        return false;
    }
    
    size_t total_expected_size = header_size + data_size;
    
    // Allocate buffer and decompress
    auto buffer = std::make_unique<char[]>(total_expected_size);
    size_t actual_size;
    
    if (!decompress_data_with_dict(input_bin_path, buffer.get(), total_expected_size, actual_size, dictionary)) {
        return false;
    }
    
    // Write decompressed data using base library compression (standard compression)
    return BinaryEncoder_L2::compress_and_write_data(output_bin_path, buffer.get(), total_expected_size);
}

// Batch compress directory with dictionary
bool BinaryDict_L2::compress_directory_with_dict(const std::string& input_dir,
                                                 const std::string& output_dir,
                                                 const std::vector<char>& dictionary) {
    auto bin_files = find_bin_files(input_dir);
    int success_count = 0;
    int total_count = bin_files.size();
    
    for (const auto& input_file : bin_files) {
        std::filesystem::path input_path(input_file);
        std::string output_file = output_dir + "/" + input_path.filename().string();
        
        std::cout << "Compressing with dict: " << input_path.filename().string() << " -> " 
                  << std::filesystem::path(output_file).filename().string() << std::endl;
        
        if (compress_bin_with_dict(input_file, output_file, dictionary)) {
            success_count++;
        } else {
            std::cerr << "Failed to compress: " << input_file << std::endl;
        }
    }
    
    std::cout << "Dictionary compression completed: " << success_count << "/" << total_count << " files" << std::endl;
    return success_count == total_count;
}

// Find all .bin files in directory
std::vector<std::string> BinaryDict_L2::find_bin_files(const std::string& directory) {
    std::vector<std::string> bin_files;
    
    if (!std::filesystem::exists(directory)) {
        std::cerr << "Directory does not exist: " << directory << std::endl;
        return bin_files;
    }
    
    for (const auto& entry : std::filesystem::directory_iterator(directory)) {
        if (entry.is_regular_file() && entry.path().extension() == ".bin") {
            bin_files.push_back(entry.path().string());
        }
    }
    
    std::sort(bin_files.begin(), bin_files.end());
    return bin_files;
}

// Save dictionary to file
bool BinaryDict_L2::save_dictionary(const std::vector<char>& dictionary, const std::string& dict_path) {
    std::ofstream file(dict_path, std::ios::binary);
    if (!file.is_open()) {
        std::cerr << "Failed to open dictionary file for writing: " << dict_path << std::endl;
        return false;
    }
    
    file.write(dictionary.data(), dictionary.size());
    return !file.fail();
}

// Load dictionary from file
std::vector<char> BinaryDict_L2::load_dictionary(const std::string& dict_path) {
    std::ifstream file(dict_path, std::ios::binary);
    if (!file.is_open()) {
        std::cerr << "Failed to open dictionary file: " << dict_path << std::endl;
        return {};
    }
    
    file.seekg(0, std::ios::end);
    size_t size = file.tellg();
    file.seekg(0, std::ios::beg);
    
    std::vector<char> dictionary(size);
    file.read(dictionary.data(), size);
    
    if (file.fail()) {
        std::cerr << "Failed to read dictionary file: " << dict_path << std::endl;
        return {};
    }
    
    return dictionary;
}

// Analyze compression results
void BinaryDict_L2::analyze_compression_results(const std::string& original_dir, 
                                               const std::string& dict_compressed_dir) {
    auto original_files = find_bin_files(original_dir);
    auto compressed_files = find_bin_files(dict_compressed_dir);
    
    if (original_files.empty() || compressed_files.empty()) {
        std::cerr << "No files found for comparison" << std::endl;
        return;
    }
    
    std::cout << "\n=== Compression Analysis ===" << std::endl;
    std::cout << std::left << std::setw(30) << "File" 
              << std::setw(12) << "Original" 
              << std::setw(12) << "Dict Comp" 
              << std::setw(10) << "Ratio" 
              << std::setw(12) << "Savings" << std::endl;
    std::cout << std::string(76, '-') << std::endl;
    
    size_t total_original = 0;
    size_t total_compressed = 0;
    
    for (const auto& orig_file : original_files) {
        std::filesystem::path orig_path(orig_file);
        std::string filename = orig_path.filename().string();
        
        // Find corresponding compressed file
        std::string comp_file = dict_compressed_dir + "/" + filename;
        if (std::filesystem::exists(comp_file)) {
            size_t orig_size = std::filesystem::file_size(orig_file);
            size_t comp_size = std::filesystem::file_size(comp_file);
            
            double ratio = static_cast<double>(orig_size) / static_cast<double>(comp_size);
            double savings = (1.0 - static_cast<double>(comp_size) / static_cast<double>(orig_size)) * 100.0;
            
            std::cout << std::left << std::setw(30) << filename
                      << std::setw(12) << orig_size
                      << std::setw(12) << comp_size
                      << std::setw(10) << std::fixed << std::setprecision(2) << ratio << "x"
                      << std::setw(12) << std::fixed << std::setprecision(1) << savings << "%" << std::endl;
            
            total_original += orig_size;
            total_compressed += comp_size;
        }
    }
    
    std::cout << std::string(76, '-') << std::endl;
    double overall_ratio = static_cast<double>(total_original) / static_cast<double>(total_compressed);
    double overall_savings = (1.0 - static_cast<double>(total_compressed) / static_cast<double>(total_original)) * 100.0;
    
    std::cout << std::left << std::setw(30) << "TOTAL"
              << std::setw(12) << total_original
              << std::setw(12) << total_compressed
              << std::setw(10) << std::fixed << std::setprecision(2) << overall_ratio << "x"
              << std::setw(12) << std::fixed << std::setprecision(1) << overall_savings << "%" << std::endl;
}

// Private helper: Dictionary compression (extends base library)
bool BinaryDict_L2::compress_data_with_dict(const std::string& filepath, const void* data, size_t data_size,
                                           const std::vector<char>& dictionary) {
    std::ofstream file(filepath, std::ios::binary);
    if (!file.is_open()) {
        std::cerr << "Failed to open file for dictionary compression: " << filepath << std::endl;
        return false;
    }
    
    // Create compression context
    ZSTD_CCtx* cctx = ZSTD_createCCtx();
    if (!cctx) {
        std::cerr << "Failed to create compression context" << std::endl;
        return false;
    }
    
    // Set compression level and load dictionary
    ZSTD_CCtx_setParameter(cctx, ZSTD_c_compressionLevel, ZSTD_COMPRESSION_LEVEL);
    size_t dict_result = ZSTD_CCtx_loadDictionary(cctx, dictionary.data(), dictionary.size());
    if (ZSTD_isError(dict_result)) {
        std::cerr << "Failed to load dictionary: " << ZSTD_getErrorName(dict_result) << std::endl;
        ZSTD_freeCCtx(cctx);
        return false;
    }
    
    // Compress with dictionary
    size_t compressed_bound = ZSTD_compressBound(data_size);
    auto compressed_buffer = std::make_unique<char[]>(compressed_bound);
    
    size_t compressed_size = ZSTD_compress2(cctx, compressed_buffer.get(), compressed_bound, data, data_size);
    ZSTD_freeCCtx(cctx);
    
    if (ZSTD_isError(compressed_size)) {
        std::cerr << "Dictionary compression failed: " << ZSTD_getErrorName(compressed_size) << std::endl;
        return false;
    }
    
    // Write header: original size and compressed size (same format as base library)
    file.write(reinterpret_cast<const char*>(&data_size), sizeof(data_size));
    file.write(reinterpret_cast<const char*>(&compressed_size), sizeof(compressed_size));
    
    if (file.fail()) {
        std::cerr << "Failed to write compression header: " << filepath << std::endl;
        return false;
    }
    
    // Write compressed data
    file.write(compressed_buffer.get(), compressed_size);
    
    if (file.fail()) {
        std::cerr << "Failed to write compressed data: " << filepath << std::endl;
        return false;
    }
    
    // Print compression statistics
    double compression_ratio = static_cast<double>(data_size) / static_cast<double>(compressed_size);
    std::cout << "Dict compressed " << data_size << " bytes to " << compressed_size 
              << " bytes (ratio: " << std::fixed << std::setprecision(2) << compression_ratio << "x)" << std::endl;
    
    return true;
}

// Private helper: Dictionary decompression (extends base library)
bool BinaryDict_L2::decompress_data_with_dict(const std::string& filepath, void* data, size_t expected_size,
                                             size_t& actual_size, const std::vector<char>& dictionary) {
    std::ifstream file(filepath, std::ios::binary);
    if (!file.is_open()) {
        std::cerr << "Failed to open file for dictionary decompression: " << filepath << std::endl;
        return false;
    }
    
    // Read header: original size and compressed size (same format as base library)
    size_t original_size, compressed_size;
    file.read(reinterpret_cast<char*>(&original_size), sizeof(original_size));
    file.read(reinterpret_cast<char*>(&compressed_size), sizeof(compressed_size));
    
    if (file.fail()) {
        std::cerr << "Failed to read compression header: " << filepath << std::endl;
        return false;
    }
    
    // Verify expected size matches
    if (original_size != expected_size) {
        std::cerr << "Size mismatch - expected " << expected_size << " but header says " << original_size << std::endl;
        return false;
    }
    
    // Read compressed data
    auto compressed_buffer = std::make_unique<char[]>(compressed_size);
    file.read(compressed_buffer.get(), compressed_size);
    
    if (file.fail()) {
        std::cerr << "Failed to read compressed data: " << filepath << std::endl;
        return false;
    }
    
    // Create decompression context
    ZSTD_DCtx* dctx = ZSTD_createDCtx();
    if (!dctx) {
        std::cerr << "Failed to create decompression context" << std::endl;
        return false;
    }
    
    // Load dictionary
    size_t dict_result = ZSTD_DCtx_loadDictionary(dctx, dictionary.data(), dictionary.size());
    if (ZSTD_isError(dict_result)) {
        std::cerr << "Failed to load dictionary: " << ZSTD_getErrorName(dict_result) << std::endl;
        ZSTD_freeDCtx(dctx);
        return false;
    }
    
    // Decompress with dictionary
    size_t decompressed_size = ZSTD_decompressDCtx(dctx, data, expected_size, compressed_buffer.get(), compressed_size);
    ZSTD_freeDCtx(dctx);
    
    if (ZSTD_isError(decompressed_size)) {
        std::cerr << "Dictionary decompression failed: " << ZSTD_getErrorName(decompressed_size) << std::endl;
        return false;
    }
    
    if (decompressed_size != expected_size) {
        std::cerr << "Decompressed size mismatch - expected " << expected_size << " but got " << decompressed_size << std::endl;
        return false;
    }
    
    actual_size = decompressed_size;
    return true;
}

// Private helper: Extract raw data from compressed .bin file (for dictionary training)
std::vector<char> BinaryDict_L2::extract_raw_data_from_bin(const std::string& bin_file_path) {
    // Use base library to decompress and get raw data
    std::ifstream file(bin_file_path, std::ios::binary);
    if (!file.is_open()) {
        std::cerr << "Failed to open bin file: " << bin_file_path << std::endl;
        return {};
    }
    
    // Read header to get original size
    size_t original_size, compressed_size;
    file.read(reinterpret_cast<char*>(&original_size), sizeof(original_size));
    file.read(reinterpret_cast<char*>(&compressed_size), sizeof(compressed_size));
    
    if (file.fail()) {
        std::cerr << "Failed to read header from: " << bin_file_path << std::endl;
        return {};
    }
    
    // Read compressed data
    auto compressed_buffer = std::make_unique<char[]>(compressed_size);
    file.read(compressed_buffer.get(), compressed_size);
    
    if (file.fail()) {
        std::cerr << "Failed to read compressed data from: " << bin_file_path << std::endl;
        return {};
    }
    
    // Decompress using standard Zstandard (no dictionary)
    std::vector<char> raw_data(original_size);
    size_t decompressed_size = ZSTD_decompress(raw_data.data(), original_size, 
                                              compressed_buffer.get(), compressed_size);
    
    if (ZSTD_isError(decompressed_size)) {
        std::cerr << "Failed to decompress data from: " << bin_file_path 
                  << ", error: " << ZSTD_getErrorName(decompressed_size) << std::endl;
        return {};
    }
    
    if (decompressed_size != original_size) {
        std::cerr << "Size mismatch in decompression: " << bin_file_path << std::endl;
        return {};
    }
    
    return raw_data;
}

// Private helper: Select training samples (avoid using too many files)
std::vector<std::string> BinaryDict_L2::select_training_samples(const std::vector<std::string>& all_files) {
    if (all_files.size() <= DICT_MAX_SAMPLES) {
        return all_files;
    }
    
    // Randomly select DICT_MAX_SAMPLES files
    std::vector<std::string> selected_files = all_files;
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(selected_files.begin(), selected_files.end(), g);
    
    selected_files.resize(DICT_MAX_SAMPLES);
    return selected_files;
}

} // namespace L2
