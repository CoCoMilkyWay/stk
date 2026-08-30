#!/usr/bin/env python3
"""
Convert ZIP archives to non-solid RAR format.

Checks internal structure before conversion:
  Expected: YYYYMMDD/asset_code/*.csv

Scans a directory (recursively) for *.zip files and converts all of them.
Only reads the .zip files it finds; never touches any other file in the
directory (existing .rar files, csv, etc. are left alone).

Usage:
    python fix_zip_to_rar.py <dir>

Example:
    python fix_zip_to_rar.py /media/chuyin/Disk/data/L2/2025/202507
"""

import sys
import subprocess
import shutil
import zipfile
from pathlib import Path
from typing import Tuple, Optional

from repair_utils import (
    setup_output_dirs, cleanup_dirs,
    process_with_pool, log_message
)


def probe_zip_structure(zip_path: Path) -> Optional[str]:
    """
    Probe zip internal structure.
    Returns error message if structure is invalid, None if valid.
    Expected: YYYYMMDD/asset_code/*.csv
    """
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            namelist = zf.namelist()
            if not namelist:
                return "Empty archive"

            first_entry = namelist[0]

            # Check if starts with YYYYMMDD/
            if (len(first_entry) >= 9 and first_entry[8] == '/' and
                    first_entry[:8].isdigit()):
                return None  # Valid structure

            return f"Invalid structure: {first_entry} (expected YYYYMMDD/...)"

    except Exception as e:
        return f"Cannot read zip: {str(e)}"


def convert_single_zip(args: Tuple[Path, str, Path, Path], worker_idx: int) -> Tuple[str, bool, str]:
    """Convert a single ZIP file to non-solid RAR format."""
    zip_path, expected_date, output_dir, temp_dir = args
    filename = zip_path.stem  # Without extension

    log_message(worker_idx, f"=== Processing {filename}.zip ===")

    # Probe structure first
    log_message(worker_idx, f"Probing internal structure...")
    structure_error = probe_zip_structure(zip_path)
    if structure_error:
        log_message(worker_idx, f"Structure check failed: {structure_error}")
        return (filename, False, structure_error)

    log_message(worker_idx, f"Structure check passed")

    temp_extract_dir = temp_dir / f"extract_{filename}_{worker_idx}"
    output_rar_path = output_dir / f"{filename}.rar"

    try:
        # Step 1: Extract ZIP archive
        temp_extract_dir.mkdir(parents=True, exist_ok=True)
        log_message(worker_idx, f"Extracting {filename}.zip...")

        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(temp_extract_dir)

        log_message(worker_idx, f"Extraction completed")

        # Step 2: Verify extracted structure matches expected date
        date_dir = temp_extract_dir / expected_date
        if not date_dir.exists():
            log_message(
                worker_idx, f"Warning: Expected date directory {expected_date}/ not found")

        # Step 3: Create non-solid RAR archive
        log_message(worker_idx, f"Creating non-solid RAR archive...")
        result = subprocess.run(
            ['rar', 'a', '-m3', '-ma5', '-r', str(output_rar_path), '.'],
            cwd=temp_extract_dir,
            capture_output=True, text=True
        )
        if result.returncode != 0:
            log_message(worker_idx, f"RAR creation failed: {result.stderr}")
            return (filename, False, "RAR creation failed")

        # Step 4: Cleanup
        shutil.rmtree(temp_extract_dir)
        log_message(worker_idx, f"Success: {filename}.zip -> {filename}.rar")

        return (filename, True, "Success")

    except Exception as e:
        log_message(worker_idx, f"Exception: {str(e)}")
        return (filename, False, f"Exception: {str(e)}")
    finally:
        shutil.rmtree(temp_extract_dir, ignore_errors=True)


def main():
    if len(sys.argv) != 2:
        print("Usage: python fix_zip_to_rar.py <dir>")
        print("Example: python fix_zip_to_rar.py /media/chuyin/Disk/data/L2/2025/202507")
        sys.exit(1)

    scan_dir = Path(sys.argv[1])
    assert scan_dir.is_dir(), f"Not a directory: {scan_dir}"

    zip_files = sorted(scan_dir.rglob("*.zip"))

    # Probe structure of each zip found; only .zip files are ever read here.
    to_convert = []
    for zip_path in zip_files:
        date = zip_path.stem
        if not (len(date) == 8 and date.isdigit()):
            print(f"✗ Skip (filename not YYYYMMDD.zip): {zip_path}")
            continue

        structure_error = probe_zip_structure(zip_path)
        if structure_error:
            print(f"✗ {zip_path.name}: {structure_error}")
            continue

        to_convert.append((zip_path, date))

    if not to_convert:
        print("No valid ZIP files to convert.")
        return

    print(f"\nFound {len(to_convert)} ZIP file(s) with correct structure:")
    for zip_path, date in to_convert:
        print(f"  {zip_path.name} (internal date: {date}/)")

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
    args_list = [(zip_path, date, output_dir, temp_dir)
                 for zip_path, date in to_convert]
    success_count, failed = process_with_pool(
        args_list, convert_single_zip, num_workers=4)

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
    print("\nNote: Remember to rename .zip to .rar in the original location after replacing.")


if __name__ == '__main__':
    main()
