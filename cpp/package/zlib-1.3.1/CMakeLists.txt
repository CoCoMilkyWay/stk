cmake_minimum_required(VERSION 3.20)
project(zlib C)

# Enable POSIX definitions for Unix/Linux/macOS
if(NOT WIN32)
    add_compile_definitions(_POSIX_C_SOURCE=200112L)
endif()

# Source files
set(ZLIB_SOURCES
    adler32.c
    compress.c
    crc32.c
    deflate.c
    gzclose.c
    gzlib.c
    gzread.c
    gzwrite.c
    infback.c
    inffast.c
    inflate.c
    inftrees.c
    trees.c
    uncompr.c
    zutil.c
)

# Build static library
add_library(z STATIC ${ZLIB_SOURCES})

# Add POSIX definitions for Unix/Linux/macOS
if(NOT WIN32)
    target_compile_definitions(z PRIVATE _POSIX_C_SOURCE=200112L _DEFAULT_SOURCE)
endif()

# Public include directory
target_include_directories(z PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# Output as libz.a in source directory (consistent with zstd)
set_target_properties(z PROPERTIES
    OUTPUT_NAME z
    ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
