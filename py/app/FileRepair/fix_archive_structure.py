#!/usr/bin/env python3
"""
Fix archive internal structure by adding missing date directory.

Converts:
    000002.SZ/行情.csv
To:
    20241119/000002.SZ/行情.csv (date extracted from archive filename)

Usage:
    python fix_archive_structure.py <archive_base_dir> <date1> [<date2> ...]

Example:
    python fix_archive_structure.py /path/to/archives 20241119 20240926
"""

import sys
import subprocess
import shutil
from pathlib import Path
from typing import Tuple, Optional

from repair_utils import (
    setup_output_dirs,
    cleanup_dirs,
    process_with_pool,
    resolve_archive_path,
    log_message,
)


def detect_incompatible_structure(archive_path: Path) -> Optional[str]:
    """Check if archive has incorrect structure. Returns first entry if invalid."""
    try:
        result = subprocess.run(
            ["unrar", "lb", str(archive_path)],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode != 0:
            return None

        lines = result.stdout.strip().split("\n")
        if not lines or not lines[0]:
            return None

        first_entry = lines[0]

        # Check if starts with YYYYMMDD/
        if (
            len(first_entry) >= 9
            and first_entry[8] == "/"
            and first_entry[:8].isdigit()
        ):
            return None  # Valid format

        return first_entry  # Invalid format

    except Exception:
        return None


def fix_single_archive(
    args: Tuple[Path, str, Path, Path], worker_idx: int
) -> Tuple[str, bool, str]:
    """Fix structure of a single archive."""
    archive_path, date_str, output_dir, temp_dir = args
    filename = archive_path.name
    stem = archive_path.stem

    log_message(worker_idx, f"=== Processing {filename} ===")

    temp_extract_dir = temp_dir / f"extract_{stem}_{worker_idx}"
    temp_repack_dir = temp_dir / f"repack_{stem}_{worker_idx}"
    output_rar_path = output_dir / filename

    try:
        # Step 1: Extract archive
        temp_extract_dir.mkdir(parents=True, exist_ok=True)
        log_message(worker_idx, f"Extracting {filename}...")
        result = subprocess.run(
            ["unrar", "x", str(archive_path), str(temp_extract_dir) + "/"],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            log_message(worker_idx, f"Extract failed: {result.stderr}")
            return (filename, False, f"Extract failed")

        # Step 2: Reorganize structure
        log_message(
            worker_idx, f"Reorganizing structure with date directory: {date_str}/"
        )
        temp_repack_dir.mkdir(parents=True, exist_ok=True)
        date_dir = temp_repack_dir / date_str
        date_dir.mkdir()

        # Move all content into date directory
        for item in temp_extract_dir.iterdir():
            shutil.move(str(item), str(date_dir / item.name))

        # Step 3: Create new RAR archive with correct structure
        log_message(worker_idx, f"Creating RAR archive...")
        result = subprocess.run(
            ["rar", "a", "-m3", "-ma5", "-r", str(output_rar_path), "."],
            cwd=temp_repack_dir,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            log_message(worker_idx, f"RAR creation failed: {result.stderr}")
            return (filename, False, f"RAR creation failed")

        # Step 4: Cleanup
        shutil.rmtree(temp_extract_dir, ignore_errors=True)
        shutil.rmtree(temp_repack_dir, ignore_errors=True)
        log_message(worker_idx, f"Success: {filename}")

        return (filename, True, "Success")

    except Exception as e:
        log_message(worker_idx, f"Exception: {str(e)}")
        return (filename, False, f"Exception: {str(e)}")
    finally:
        # Cleanup on error
        shutil.rmtree(temp_extract_dir, ignore_errors=True)
        shutil.rmtree(temp_repack_dir, ignore_errors=True)


def main():
    if len(sys.argv) < 3:
        print(
            "Usage: python fix_archive_structure.py <archive_base_dir> <date1> [<date2> ...]"
        )
        print(
            "Example: python fix_archive_structure.py /path/to/archives 20241119 20240926"
        )
        sys.exit(1)

    archive_base_dir = sys.argv[1]
    dates = sys.argv[2:]

    # Resolve archive paths and check if they need fixing
    to_fix = []
    for date in dates:
        archive_path = resolve_archive_path(archive_base_dir, date)
        if not archive_path.exists():
            print(f"✗ Archive not found: {archive_path}")
            continue
        detected = detect_incompatible_structure(archive_path)
        if not detected:
            print(f"✗ Structure already correct: {archive_path.name}")
            continue
        to_fix.append((archive_path, date, detected))

    if not to_fix:
        print("No archives need fixing.")
        return

    print(f"\nFound {len(to_fix)} archive(s) with incorrect structure:")
    for archive_path, date, detected in to_fix:
        print(f"  {archive_path.name}")
        print(f"    Current: {detected}")
        print(f"    Will add: {date}/")

    print("\nFixed RAR files will be saved to: output/fix/")
    print("Logs will be saved to: output/fix/logs/")
    print("Original files will NOT be modified.")
    print()

    response = input("Continue? (yes/no): ")
    if response.lower() != "yes":
        print("Cancelled.")
        return

    # Setup directories
    output_dir, temp_dir = setup_output_dirs()

    # Process files
    print(f"\nFixing {len(to_fix)} archive(s)...\n")
    args_list = [
        (archive_path, date, output_dir, temp_dir) for archive_path, date, _ in to_fix
    ]
    success_count, failed = process_with_pool(
        args_list, fix_single_archive, num_workers=4
    )

    # Cleanup
    cleanup_dirs(output_dir, temp_dir)

    print(f"\nFix summary:")
    print(f"  Success: {success_count}/{len(to_fix)}")
    if failed:
        print(f"  Failed: {len(failed)}")
        for name, message in failed:
            print(f"    {name}: {message}")

    print(f"\nFixed files saved to: {output_dir}/")
    print(f"Logs saved to: {output_dir}/logs/")
    print("Please manually replace original files after verification.")


if __name__ == "__main__":
    main()
