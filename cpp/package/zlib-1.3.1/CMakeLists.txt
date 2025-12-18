cmake_minimum_required(VERSION 3.20)
project(zlib C)

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

# Compiler flags (match parent project)
target_compile_options(z PRIVATE
    /nologo
    /O2
    -march=native
    /fp:fast
    -D_CRT_SECURE_NO_DEPRECATE
    -D_CRT_NONSTDC_NO_DEPRECATE
    -Wno-overriding-option
)

# Public include directory
target_include_directories(z PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# Output as libz.a in source directory (consistent with zstd)
set_target_properties(z PROPERTIES
    OUTPUT_NAME z
    ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
