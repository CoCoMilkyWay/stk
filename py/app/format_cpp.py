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
import json
import shutil
import hashlib
import subprocess
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

# ============================================================================
# Configuration
# ============================================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CPP_DIR = PROJECT_ROOT / "cpp"
STYLE_FILE = CPP_DIR / ".clang-format"

# Hash of each file's content after a successful format; unchanged files are skipped.
# Invalidated when clang-format version or .clang-format changes.
CACHE_FILE = Path(__file__).resolve().parent / \
    "__pycache__" / "format_cpp_cache.json"

# Extensions to format
EXTENSIONS = {".hpp", ".cpp", ".h", ".cc",
              ".hh", ".ipp", ".cxx", ".cu", ".cuh"}

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


def file_hash(path: Path) -> str:
    return hashlib.blake2b(path.read_bytes(), digest_size=16).hexdigest()


def load_cache(key: str) -> dict:
    if not CACHE_FILE.exists():
        return {}
    data = json.loads(CACHE_FILE.read_text())
    if data["key"] != key:
        return {}
    return data["files"]


def save_cache(key: str, files: dict):
    CACHE_FILE.parent.mkdir(exist_ok=True)
    CACHE_FILE.write_text(json.dumps({"key": key, "files": files}, indent=0))


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

    assert STYLE_FILE.is_file(), f"style file not found: {STYLE_FILE}"
    cache_key = version + "|" + file_hash(STYLE_FILE)
    cache = load_cache(cache_key)

    rels = [str(p.relative_to(PROJECT_ROOT)) for p in files]
    hashes = {rel: file_hash(PROJECT_ROOT / rel) for rel in rels}
    todo = [rel for rel in rels if cache.get(rel) != hashes[rel]]
    print(f"{len(rels) - len(todo)} cached, {len(todo)} to check")

    def work(rel: str):
        path = PROJECT_ROOT / rel
        if dry_run:
            return would_change(binary, path)
        subprocess.run([binary, "-i", str(path)], check=True)
        return None

    with ThreadPoolExecutor(max_workers=os.cpu_count()) as pool:
        results = list(pool.map(work, todo))

    new_cache = {rel: cache[rel] for rel in rels if rel in cache}
    changed = 0
    for rel, res in zip(todo, results):
        if dry_run:
            if res:
                print(f"  \033[1;33m[CHANGED]\033[0m {rel}")
                changed += 1
            else:
                new_cache[rel] = hashes[rel]
        else:
            new_hash = file_hash(PROJECT_ROOT / rel)
            if new_hash != hashes[rel]:
                print(f"  \033[1;33m[FORMATTED]\033[0m {rel}")
                changed += 1
            new_cache[rel] = new_hash
    save_cache(cache_key, new_cache)

    if dry_run:
        print(f"\n{changed} file(s) would be reformatted.")
    else:
        print(f"\nDone. {changed} file(s) reformatted, {len(todo)} checked.")


if __name__ == "__main__":
    main()
