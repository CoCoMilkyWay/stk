#!/usr/bin/env python3
"""
Convert archives to non-solid RAR, in place, one directory at a time.

Scans a directory (recursively) for *.zip / *.7z / *.rar, detects the REAL
format by magic bytes (a .rar may be 7z in disguise), and converts anything
that is not already a true RAR. Files of other names are never touched.

Pipeline (per-file atomic replacement, nothing waits for the whole batch):

  worker processes (CPU-bound, local disk):
      7z x <source> -> extract dir -> rar a -> local temp .rar
  replacer thread (destination-disk I/O, serialized so the disk never
  sees concurrent writes):
      copy local .rar -> <source dir>/.tmp_<stem>.rar
      os.replace() -> <stem>.rar        (atomic: same-filesystem rename)
      delete source (skip if source IS the final path, i.e. disguised .rar)

Interruption safety (Ctrl+C or any death of this python process):
  - workers are killed via pool.terminate(); their 7z/rar children carry
    PR_SET_PDEATHSIG=SIGKILL so they die with their worker, no orphans
  - the replacer is a daemon thread, it dies with the process; a half-copied
    .tmp_*.rar is swept on the next run, the final name only ever appears
    via atomic rename so it is never half-written
  - per-run temp dir output/fix_temp/run_<pid> is unique, concurrent runs
    cannot stomp on each other; dead runs' leftovers are swept at startup

Usage:
    python fix_to_rar.py <dir>
"""

import ctypes
import multiprocessing as mp
import os
import queue
import shutil
import signal
import subprocess
import sys
import threading
from pathlib import Path
from typing import Optional, Tuple

NUM_WORKERS = 4
SOURCE_EXTENSIONS = {".zip", ".7z", ".rar"}

MAGIC_TO_FORMAT = {
    b"Rar!": "rar",
    b"7z\xbc\xaf\x27\x1c": "7z",
    b"PK\x03\x04": "zip",
}

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
TEMP_BASE = PROJECT_ROOT / "output" / "fix_temp"
RUN_DIR = TEMP_BASE / f"run_{os.getpid()}"
WORK_DIR = RUN_DIR / "work"
LOG_DIR = RUN_DIR / "logs"

PR_SET_PDEATHSIG = 1


def _die_with_parent():
    """SIGKILL this process when its parent dies (Linux PDEATHSIG).

    Used both as pool worker initializer (worker dies with main) and as
    preexec_fn for 7z/rar children (tool dies with its worker), so no
    orphan ever survives this script, however it gets stopped.
    """
    ctypes.CDLL("libc.so.6", use_errno=True).prctl(PR_SET_PDEATHSIG, signal.SIGKILL)


def detect_format(path: Path) -> Optional[str]:
    with open(path, "rb") as f:
        head = f.read(6)
    for magic, fmt in MAGIC_TO_FORMAT.items():
        if head.startswith(magic):
            return fmt
    return None


def log(stem: str, message: str):
    with open(LOG_DIR / f"{stem}.log", "a") as f:
        f.write(message + "\n")


def convert_one(task: Tuple[int, Path]) -> Tuple[Path, Optional[Path], str]:
    """Extract + recompress one archive on local disk.

    Returns (source, local_rar or None, error). Runs in a worker process;
    the atomic in-place replacement is done by the replacer thread in the
    main process.
    """
    idx, source = task
    stem = source.stem
    extract_dir = WORK_DIR / f"{stem}_{idx}"
    local_rar = WORK_DIR / f"{stem}_{idx}.rar"

    try:
        extract_dir.mkdir(parents=True)
        log(stem, f"Extracting {source.name}...")
        result = subprocess.run(
            ["7z", "x", str(source), f"-o{extract_dir}/", "-y"],
            capture_output=True, text=True, preexec_fn=_die_with_parent,
        )
        if result.returncode != 0:
            log(stem, f"Extract failed: {result.stderr}")
            return (source, None, "extract failed")

        if not (extract_dir / stem).is_dir():
            log(stem, f"Warning: extracted content has no {stem}/ directory")

        log(stem, "Creating non-solid RAR...")
        result = subprocess.run(
            ["rar", "a", "-m3", "-ma5", "-r", str(local_rar), "."],
            cwd=extract_dir,
            capture_output=True, text=True, preexec_fn=_die_with_parent,
        )
        if result.returncode != 0:
            log(stem, f"RAR creation failed: {result.stderr}")
            return (source, None, "rar creation failed")

        log(stem, "Converted, queued for replacement")
        return (source, local_rar, "")

    except Exception as e:
        log(stem, f"Exception: {e}")
        return (source, None, str(e))
    finally:
        shutil.rmtree(extract_dir, ignore_errors=True)


def sweep_stale():
    """Remove leftovers of previous interrupted runs."""
    for run_dir in TEMP_BASE.glob("run_*"):
        pid = run_dir.name.split("_", 1)[1]
        if not Path(f"/proc/{pid}").exists():
            shutil.rmtree(run_dir, ignore_errors=True)
            print(f"Swept stale run dir: {run_dir}")


def collect_tasks(scan_dir: Path):
    tasks = []
    for path in sorted(scan_dir.rglob("*")):
        if path.suffix.lower() not in SOURCE_EXTENSIONS or not path.is_file():
            continue
        if path.name.startswith(".tmp_"):
            path.unlink()
            print(f"Swept stale temp file: {path}")
            continue
        if not (len(path.stem) == 8 and path.stem.isdigit()):
            print(f"✗ Skip (name not YYYYMMDD): {path}")
            continue
        fmt = detect_format(path)
        if fmt == "rar":
            continue  # already a true RAR
        if fmt is None:
            print(f"✗ Skip (unknown format): {path}")
            continue
        tasks.append((path, fmt))
    return tasks


def main():
    if len(sys.argv) != 2:
        print("Usage: python fix_to_rar.py <dir>")
        print("Example: python fix_to_rar.py /media/chuyin/Disk/data/L2/2026/202608")
        sys.exit(1)

    scan_dir = Path(sys.argv[1])
    assert scan_dir.is_dir(), f"Not a directory: {scan_dir}"

    sweep_stale()
    tasks = collect_tasks(scan_dir)
    if not tasks:
        print("Nothing to convert.")
        return

    print(f"\nFound {len(tasks)} archive(s) to convert:")
    for path, fmt in tasks:
        print(f"  [{fmt}] {path}")
    print("\nEach file is replaced IN PLACE (atomic rename) as soon as it converts;")
    print("its source is then deleted. Failed files keep their source untouched.")
    print(f"Logs: {LOG_DIR}/")
    print()

    if input("Continue? (yes/no): ").lower() != "yes":
        print("Cancelled.")
        return

    WORK_DIR.mkdir(parents=True)
    LOG_DIR.mkdir(parents=True)

    total = len(tasks)
    convert_failed = []   # (source, error)
    replace_failed = []   # (source, error)
    replaced = []         # final paths

    replace_q: "queue.Queue" = queue.Queue()

    def replace_loop():
        while True:
            item = replace_q.get()
            if item is None:
                return
            source, local_rar = item
            final = source.with_suffix(".rar")
            dest_tmp = final.parent / f".tmp_{final.name}"
            try:
                shutil.copyfile(local_rar, dest_tmp)
                os.replace(dest_tmp, final)  # atomic same-fs rename
                if source != final:
                    source.unlink()
                local_rar.unlink()
                replaced.append(final)
                print(f"[replaced {len(replaced)}/{total}] {final.name}")
            except Exception as e:
                replace_failed.append((source, str(e)))
                dest_tmp.unlink(missing_ok=True)
                print(f"[replace FAILED] {source.name}: {e}")

    # Pool before thread: fork the workers from a thread-free process.
    pool = mp.Pool(processes=NUM_WORKERS, initializer=_die_with_parent)
    replacer = threading.Thread(target=replace_loop, daemon=True)
    replacer.start()

    print(f"\nConverting {total} file(s) with {NUM_WORKERS} workers...\n")
    try:
        work = [(idx, path) for idx, (path, _fmt) in enumerate(tasks)]
        converted = 0
        for source, local_rar, err in pool.imap_unordered(convert_one, work):
            converted += 1
            if err:
                convert_failed.append((source, err))
                print(f"[converted {converted}/{total}] ✗ {source.name}: {err}")
            else:
                print(f"[converted {converted}/{total}] ✓ {source.name}")
                replace_q.put((source, local_rar))
        pool.close()
        pool.join()
        replace_q.put(None)  # sentinel: replacer exits after draining the queue
        replacer.join()
    except KeyboardInterrupt:
        pool.terminate()  # kills workers; their 7z/rar children die via PDEATHSIG
        pool.join()
        print("\nInterrupted. Unfinished files keep their original source; re-run to resume.")
        sys.exit(130)
    finally:
        shutil.rmtree(RUN_DIR, ignore_errors=True)

    print(f"\nSummary:")
    print(f"  Replaced in place: {len(replaced)}/{total}")
    for source, err in convert_failed:
        print(f"  ✗ convert failed: {source.name}: {err}")
    for source, err in replace_failed:
        print(f"  ✗ replace failed: {source.name}: {err}")


if __name__ == "__main__":
    main()
