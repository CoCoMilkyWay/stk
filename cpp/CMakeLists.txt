cmake_minimum_required(VERSION 3.20)
project(BinaryParser VERSION 1.0.0 LANGUAGES CXX C)

# Set C++ standard
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON) # for clangd syntax highlighting

# MSVC (Visual Studio) Optimizations:
# /GL - Whole program optimization
# /Gy - Function-level linking
# /Gw - Optimize global data
# /arch:AVX2 - Use AVX2 instructions
# /fp:fast - Fast floating-point model
# /LTCG - Link time code generation
# /OPT:REF - Remove unreferenced functions
# /OPT:ICF - Identical COMDAT folding

# GCC/Clang Optimizations:
# -flto - Link time optimization
# -ffast-math - Fast math operations

# Compiler-specific options
if(MSVC)
    # MSVC specific flags - Maximum optimization
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /O2 /W4 /EHsc")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /O2 /Ob2 /DNDEBUG /GL /Gy /Gw /arch:AVX2 /fp:fast")
    set(CMAKE_EXE_LINKER_FLAGS_RELEASE "${CMAKE_EXE_LINKER_FLAGS_RELEASE} /LTCG /OPT:REF /OPT:ICF")
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" OR CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    # GCC/Clang flags
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -Wall -Wextra -march=native -flto")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -DNDEBUG -ffast-math")
endif()

# Output directories
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

# Source files
set(SOURCES
    src/json_config.cpp
    src/miniz.c
    src/binary_parser.cpp
    src/technical_analysis.cpp
    src/main.cpp
)

# Set C language for miniz.c and suppress external library warnings
set_source_files_properties(src/miniz.c PROPERTIES LANGUAGE C)
if(MSVC)
    set_source_files_properties(src/miniz.c PROPERTIES COMPILE_FLAGS "/wd4132 /wd4127")
endif()

set(HEADERS
    include/misc/misc.hpp
    include/misc/print.hpp
    include/define/CBuffer.hpp
    include/json_config.hpp
    include/miniz.h
    include/binary_parser.hpp
    include/technical_analysis.hpp
)

# Create executable
add_executable(binary_parser ${SOURCES} ${HEADERS})

# Include directories
target_include_directories(binary_parser PRIVATE 
    src 
    include 
    
)

# Define MINIZ_EXPORTS for proper function export
# target_compile_definitions(binary_parser PRIVATE MINIZ_EXPORTS)

# Windows-specific settings
if(WIN32)
    # For Windows, we might need additional libraries
    target_link_libraries(binary_parser PRIVATE ws2_32)
    
    # Set subsystem to console
    set_target_properties(binary_parser PROPERTIES
        WIN32_EXECUTABLE FALSE
    )
endif()

# Installation
install(TARGETS binary_parser
    RUNTIME DESTINATION bin
)

# Custom target for running with default parameters
add_custom_target(run
    COMMAND binary_parser
    DEPENDS binary_parser
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running binary parser with default parameters"
)

# Custom target for running with custom parameters
add_custom_target(run_custom
    COMMAND binary_parser "D:/data/A_stock/A_L1_binary" "parsed_output.csv"
    DEPENDS binary_parser
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running binary parser with custom parameters"
)

# Print configuration info
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "C++ compiler: ${CMAKE_CXX_COMPILER_ID}")
message(STATUS "C++ standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "Output directory: ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}")