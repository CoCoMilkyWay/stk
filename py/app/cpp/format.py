#!/usr/bin/env python3
"""
Format all C/C++ source files under cpp/ with clang-format.

Uses the same clang-format binary that clangd delegates to, so the result
matches Cursor's right-click "Format Document" (both read cpp/.clang-format).

Usage:
    python py/app/cpp/format.py            # format in place
    python py/app/cpp/format.py --dry-run  # only list files that would change
"""

import hashlib
import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from common import CPP_DIR, FORMAT_EXTENSIONS, PROJECT_ROOT, iter_sources, which

STYLE_FILE = CPP_DIR / ".clang-format"

# Hash of each file's content after a successful format; unchanged files are skipped.
# Invalidated when clang-format version or .clang-format changes.
CACHE_FILE = Path(__file__).resolve().parent / "__pycache__" / "format_cache.json"


# ============================================================================
# Cache
# ============================================================================


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
# Check / Apply: (binary, rel, old_hash) -> (changed, hash to cache | None)
# ============================================================================


def check(binary: str, rel: str, old_hash: str):
    # --output-replacements-xml prints <replacement> entries when the file is not already formatted.
    result = subprocess.run(
        [binary, "--output-replacements-xml", rel], capture_output=True, check=True
    )
    changed = b"<replacement " in result.stdout
    return changed, None if changed else old_hash


def apply(binary: str, rel: str, old_hash: str):
    subprocess.run([binary, "-i", rel], check=True)
    new_hash = file_hash(PROJECT_ROOT / rel)
    return new_hash != old_hash, new_hash


# ============================================================================
# Main
# ============================================================================


def main():
    os.chdir(PROJECT_ROOT)
    dry_run = "--dry-run" in sys.argv

    files = [
        str(p.relative_to(PROJECT_ROOT))
        for p in iter_sources(CPP_DIR, FORMAT_EXTENSIONS)
    ]
    assert files, f"No C/C++ source files found under {CPP_DIR}"
    print(f"Found {len(files)} source file(s) under {CPP_DIR}")

    binary = which("clang-format")
    version = subprocess.run(
        [binary, "--version"], capture_output=True, text=True, check=True
    ).stdout.strip()
    print(f"Using: {version}")

    assert STYLE_FILE.is_file(), f"style file not found: {STYLE_FILE}"
    cache_key = version + "|" + file_hash(STYLE_FILE)
    cache = load_cache(cache_key)
    hashes = {rel: file_hash(PROJECT_ROOT / rel) for rel in files}
    todo = [rel for rel in files if cache.get(rel) != hashes[rel]]
    print(f"{len(files) - len(todo)} cached, {len(todo)} to check")

    work = check if dry_run else apply
    with ThreadPoolExecutor(max_workers=os.cpu_count()) as pool:
        results = list(pool.map(lambda rel: work(binary, rel, hashes[rel]), todo))

    tag = "CHANGED" if dry_run else "FORMATTED"
    new_cache = {rel: hashes[rel] for rel in files if cache.get(rel) == hashes[rel]}
    changed = 0
    for rel, (diff, new_hash) in zip(todo, results):
        if diff:
            print(f"  \033[1;33m[{tag}]\033[0m {rel}")
            changed += 1
        if new_hash:
            new_cache[rel] = new_hash
    save_cache(cache_key, new_cache)

    verb = "would be reformatted" if dry_run else "reformatted"
    print(f"\nDone. {changed} file(s) {verb}, {len(todo)} checked.")


if __name__ == "__main__":
    main()
