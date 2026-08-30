"""Common utilities for file repair scripts."""

import os
import shutil
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Callable, Tuple, Any


def get_project_root() -> Path:
    """Get project root directory."""
    current_file = Path(__file__).resolve()
    return current_file.parent.parent.parent.parent


def get_output_fix_dir() -> Path:
    """Get the output/fix directory path."""
    return get_project_root() / "output" / "fix"


def get_output_fix_temp_dir() -> Path:
    """Get the output/fix_temp directory path."""
    return get_project_root() / "output" / "fix_temp"


def setup_output_dirs() -> Tuple[Path, Path]:
    """Setup output and temp directories by cleaning and creating them."""
    output_dir = get_output_fix_dir()
    temp_dir = get_output_fix_temp_dir()
    
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    return output_dir, temp_dir


def cleanup_dirs(output_dir: Path, temp_dir: Path):
    """Clean up output and temp directories."""
    shutil.rmtree(temp_dir, ignore_errors=True)
    # Keep output_dir so user can verify fixed files


def process_with_pool(
    items: List[Any],
    process_func: Callable[[Any, int], Tuple[str, bool, str]],
    num_workers: int = 4
) -> Tuple[int, List[Tuple[str, str]]]:
    """
    Process items in parallel using ProcessPoolExecutor.
    Each worker writes to its own log file.
    
    Args:
        items: List of items to process
        process_func: Function that takes (item, worker_idx) and returns (name, success, message)
        num_workers: Number of worker processes
    
    Returns:
        Tuple of (success_count, failed_items)
    """
    success_count = 0
    failed_items = []
    total = len(items)
    completed = 0
    
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = {executor.submit(process_func, item, idx): item 
                   for idx, item in enumerate(items)}
        
        for future in as_completed(futures):
            completed += 1
            name, success, message = future.result()
            if success:
                success_count += 1
                print(f"[{completed}/{total}] ✓ {name}")
            else:
                failed_items.append((name, message))
                print(f"[{completed}/{total}] ✗ {name}: {message}")
    
    return success_count, failed_items


def resolve_archive_path(archive_base_dir: str, date: str) -> Path:
    """Resolve archive path from base dir and date (YYYYMMDD)."""
    year = date[:4]
    month = date[:6]
    filename = f"{date}.rar"
    return Path(archive_base_dir) / year / month / filename


def log_message(worker_idx: int, message: str):
    """Write message to worker's log file."""
    log_dir = get_output_fix_dir() / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"worker_{worker_idx}.log"
    with open(log_file, 'a') as f:
        f.write(message + '\n')

