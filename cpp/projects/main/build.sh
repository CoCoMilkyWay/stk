#!/bin/bash
#
# C++ Build Script
# Compiles the project and copies compile_commands.json for clangd
#

set -e  # Exit on error

echo "========================================"
echo "  C++ Build Script"
echo "========================================"

# Compiler configuration
export CC=clang
export CXX=clang++

# Build configuration
BUILD_TYPE="Release"
CMAKE_ARGS="-S . -B build -G Ninja"
CMAKE_ARGS="$CMAKE_ARGS -DCMAKE_BUILD_TYPE=$BUILD_TYPE"
CMAKE_ARGS="$CMAKE_ARGS -DCMAKE_EXPORT_COMPILE_COMMANDS=ON"

# ThreadSanitizer mode
if [ "$TSAN_MODE" = "ON" ]; then
    echo "ThreadSanitizer mode: ENABLED"
    CMAKE_ARGS="$CMAKE_ARGS -DTSAN_MODE=ON"
else
    CMAKE_ARGS="$CMAKE_ARGS -DTSAN_MODE=OFF"
fi

# Profile mode
if [ "$PROFILE_MODE" = "ON" ]; then
    echo "Profile mode: ENABLED"
    CMAKE_ARGS="$CMAKE_ARGS -DPROFILE_MODE=ON"
else
    CMAKE_ARGS="$CMAKE_ARGS -DPROFILE_MODE=OFF"
fi

# Debug mode
if [ "$DEBUG_MODE" = "ON" ]; then
    echo "Debug mode: ENABLED"
    CMAKE_ARGS="$CMAKE_ARGS -DDEBUG_MODE=ON"
else
    CMAKE_ARGS="$CMAKE_ARGS -DDEBUG_MODE=OFF"
fi

# Configure
echo ""
echo "Configuring with CMake..."
cmake $CMAKE_ARGS

# Build
echo ""
echo "Building..."
cmake --build build --parallel

# Copy compile_commands.json for clangd
if [ -f "build/compile_commands.json" ]; then
    cp "build/compile_commands.json" "../../compile_commands.json"
    echo ""
    echo "✓ compile_commands.json copied for clangd"
fi

echo ""
echo "========================================"
echo "  Build completed successfully!"
echo "========================================"
