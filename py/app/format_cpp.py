#!/usr/bin/env python3
"""
Format all C/C++ source files under cpp/ with clang-format.

Uses the same clang-format binary that clangd delegates to, so the result
matches Cursor's right-click "Format Document" (both read cpp/.clang-format).

Usage:
    python py/app/format_cpp.py            # format in place
    python py/app/format_cpp.py --dry-run  # only list files that would change
"""

import os
import sys
import shutil
import subprocess
from pathlib import Path

# ============================================================================
# Configuration
# ============================================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CPP_DIR = PROJECT_ROOT / "cpp"

# Extensions to format
EXTENSIONS = {".hpp", ".cpp", ".h", ".cc", ".hh", ".ipp", ".cxx"}

# Directories to skip (build artifacts, vendored libs, generated code)
SKIP_DIRS = {
    "build",
    ".git",
    "CMakeFiles",
    "cmake-build-debug",
    "cmake-build-release",
    # vendored / third-party packages
    "package",  # arrow, glfw, imgui, implot, microtex, nlohmann, tinyxml2,
    # tracy, utfcpp, yyjson, zlib-1.3.1, zstd-1.5.7
    "boost",  # vendored Boost headers under cpp/include/boost
}

# clang-format binary: prefer PATH, fall back to the bundled LLVM install.
CLANG_FORMAT_FALLBACK = Path(
    "/home/chuyin/work/bin/LLVM-22.1.5-Linux-X64/bin/clang-format"
)


# ============================================================================
# Helpers
# ============================================================================


def find_clang_format() -> str:
    found = shutil.which("clang-format")
    if found:
        return found
    assert CLANG_FORMAT_FALLBACK.exists(), (
        f"clang-format not found on PATH and fallback missing: "
        f"{CLANG_FORMAT_FALLBACK}"
    )
    return str(CLANG_FORMAT_FALLBACK)


def iter_sources(root: Path):
    for dirpath, dirnames, filenames in os.walk(root):
        # prune skip dirs in-place
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for name in filenames:
            if Path(name).suffix in EXTENSIONS:
                yield Path(dirpath) / name


def would_change(binary: str, path: Path) -> bool:
    # clang-format --output-replacements-xml prints <replacement> entries
    # when the file is not already formatted.
    result = subprocess.run(
        [binary, "--output-replacements-xml", str(path)],
        capture_output=True,
        check=True,
    )
    return b"<replacement " in result.stdout


# ============================================================================
# Main
# ============================================================================


def main():
    os.chdir(PROJECT_ROOT)

    dry_run = "--dry-run" in sys.argv

    assert CPP_DIR.is_dir(), f"cpp directory not found: {CPP_DIR}"

    binary = find_clang_format()
    version = subprocess.run(
        [binary, "--version"], capture_output=True, text=True, check=True
    ).stdout.strip()
    print(f"Using: {version}")

    files = list(iter_sources(CPP_DIR))
    assert files, f"No C/C++ source files found under {CPP_DIR}"
    print(f"Found {len(files)} source file(s) under {CPP_DIR}")

    changed = 0
    for i, path in enumerate(files, 1):
        rel = path.relative_to(PROJECT_ROOT)
        if dry_run:
            if would_change(binary, path):
                print(f"  [CHANGED] {rel}")
                changed += 1
        else:
            subprocess.run([binary, "-i", str(path)], check=True)
            print(f"  [{i}/{len(files)}] formatted {rel}")

    if dry_run:
        print(f"\n{changed} file(s) would be reformatted.")
    else:
        print(f"\nDone. {len(files)} file(s) processed.")


if __name__ == "__main__":
    main()
