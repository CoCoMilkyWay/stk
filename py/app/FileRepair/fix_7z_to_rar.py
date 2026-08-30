#!/usr/bin/env python3
"""
Convert 7z archives (literal .7z, or disguised as .rar) to non-solid RAR format.

Scans a directory (recursively) for source files and converts all of them:
  - *.7z files (literal extension)
  - *.rar files that are actually 7z in disguise (checked via magic bytes)
Only reads matching files; never touches any other file in the directory.

Usage:
    python fix_7z_to_rar.py <dir>

Example:
    python fix_7z_to_rar.py /media/chuyin/Disk/data/L2/2026/202608
"""

import sys
import subprocess
import shutil
from pathlib import Path
from typing import Tuple

from repair_utils import (
    setup_output_dirs, cleanup_dirs,
    process_with_pool, log_message
)


def detect_7z_format(archive_path: Path) -> bool:
    """Check if file is 7z format by magic bytes."""
    try:
        with open(archive_path, 'rb') as f:
            magic = f.read(6)
            return (len(magic) == 6 and
                    magic[0] == ord('7') and magic[1] == ord('z') and
                    magic[2] == 0xbc and magic[3] == 0xaf and
                    magic[4] == 0x27 and magic[5] == 0x1c)
    except:
        return False


def convert_single_file(args: Tuple[Path, Path, Path], worker_idx: int) -> Tuple[str, bool, str]:
    """Convert a single 7z file to RAR format."""
    source_path, output_dir, temp_dir = args
    filename = source_path.name
    stem = source_path.stem

    log_message(worker_idx, f"=== Processing {filename} ===")

    temp_extract_dir = temp_dir / f"extract_{stem}_{worker_idx}"
    output_rar_path = output_dir / f"{stem}.rar"

    try:
        # Step 1: Extract 7z archive
        temp_extract_dir.mkdir(parents=True, exist_ok=True)
        log_message(worker_idx, f"Extracting {filename}...")
        result = subprocess.run(
            ['7z', 'x', str(source_path), f'-o{temp_extract_dir}/', '-y'],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            log_message(worker_idx, f"Extract failed: {result.stderr}")
            return (filename, False, f"Extract failed")

        # Step 2: Create non-solid RAR archive
        log_message(worker_idx, f"Creating RAR archive...")
        result = subprocess.run(
            ['rar', 'a', '-m3', '-ma5', '-r', str(output_rar_path), '.'],
            cwd=temp_extract_dir,
            capture_output=True, text=True
        )
        if result.returncode != 0:
            log_message(worker_idx, f"RAR creation failed: {result.stderr}")
            return (filename, False, f"RAR creation failed")

        # Step 3: Cleanup
        shutil.rmtree(temp_extract_dir)
        log_message(worker_idx, f"Success: {filename}")

        return (filename, True, "Success")

    except Exception as e:
        log_message(worker_idx, f"Exception: {str(e)}")
        return (filename, False, f"Exception: {str(e)}")
    finally:
        shutil.rmtree(temp_extract_dir, ignore_errors=True)


def main():
    if len(sys.argv) != 2:
        print("Usage: python fix_7z_to_rar.py <dir>")
        print("Example: python fix_7z_to_rar.py /media/chuyin/Disk/data/L2/2026/202608")
        sys.exit(1)

    scan_dir = Path(sys.argv[1])
    assert scan_dir.is_dir(), f"Not a directory: {scan_dir}"

    # Literal .7z files
    to_convert = sorted(scan_dir.rglob("*.7z"))

    # .rar-named files that are actually 7z in disguise (only opens .rar
    # files to peek at magic bytes; doesn't modify or move them)
    for rar_path in sorted(scan_dir.rglob("*.rar")):
        if detect_7z_format(rar_path):
            to_convert.append(rar_path)

    if not to_convert:
        print("No files to convert.")
        return

    print(f"\nFound {len(to_convert)} 7z file(s) to convert:")
    for path in to_convert:
        print(f"  {path}")

    print("\nFixed RAR files will be saved to: output/fix/")
    print("Logs will be saved to: output/fix/logs/")
    print("Original files will NOT be modified.")
    print()

    response = input("Continue? (yes/no): ")
    if response.lower() != 'yes':
        print("Cancelled.")
        return

    # Setup directories
    output_dir, temp_dir = setup_output_dirs()

    # Process files
    print(f"\nConverting {len(to_convert)} file(s)...\n")
    args_list = [(path, output_dir, temp_dir) for path in to_convert]
    success_count, failed = process_with_pool(
        args_list, convert_single_file, num_workers=4)

    # Cleanup
    cleanup_dirs(output_dir, temp_dir)

    print(f"\nConversion summary:")
    print(f"  Success: {success_count}/{len(to_convert)}")
    if failed:
        print(f"  Failed: {len(failed)}")
        for name, message in failed:
            print(f"    {name}: {message}")

    print(f"\nFixed files saved to: {output_dir}/")
    print(f"Logs saved to: {output_dir}/logs/")
    print("Please manually replace original files after verification.")


if __name__ == '__main__':
    main()
