#include "binary_parser.hpp"
#include <iostream>
#include <string>

int main() {
    try {
        // Load configuration from JSON file
        std::string config_file = "../config/config.json";
        std::cout << "Loading configuration from: " << config_file << "\n";
        
        BinaryParser::Config config = BinaryParser::ReadConfigFromJson(config_file);
        
        std::cout << "Configuration loaded successfully:\n";
        std::cout << "  Input directory: " << config.input_root << "\n";
        std::cout << "  Target file: " << config.target_file << "\n\n";
        std::cout << "  Output file: " << config.output_file << "\n";
        
        std::cout << "Starting binary parser...\n";
        
        BinaryParser::Parser parser(config.input_root, config.output_file);
        parser.ParseSingleFile(config.target_file);
        
        std::cout << "\nParsing completed successfully!\n";
        return 0;
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        std::cerr << "Make sure config.json exists and contains valid configuration.\n";
        return 1;
    }
}