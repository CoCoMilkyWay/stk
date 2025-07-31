cmake_minimum_required(VERSION 3.20)
project(BinaryParser VERSION 1.0.0 LANGUAGES CXX C)

# Set C++ standard
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON) # for clangd syntax highlighting

# Compiler-specific options
if(MSVC)
    # MSVC specific flags
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /O2 /W4")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /O2 /DNDEBUG")
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" OR CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    # GCC/Clang flags
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -Wall -Wextra -march=native")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -DNDEBUG")
endif()

# Output directories
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

# Source files
set(SOURCES
    src/main.cpp
    src/binary_parser.cpp
    src/miniz.c
)

# Set C language for miniz.c and suppress external library warnings
set_source_files_properties(src/miniz.c PROPERTIES LANGUAGE C)
if(MSVC)
    set_source_files_properties(src/miniz.c PROPERTIES COMPILE_FLAGS "/wd4132 /wd4127")
endif()

set(HEADERS
    src/include/binary_parser.hpp
    src/include/miniz.h
)

# Create executable
add_executable(binary_parser ${SOURCES} ${HEADERS})

# Include directories
target_include_directories(binary_parser PRIVATE 
    src 
    src/include 
    
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