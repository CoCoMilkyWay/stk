#!/usr/bin/env python3
"""
Convert archives to non-solid RAR, in place, one directory at a time.

Scans a directory (recursively) for *.zip / *.7z / *.rar, detects the REAL
format by magic bytes (a .rar may be 7z in disguise), and converts anything
that is not already a true RAR. Files of other names are never touched.

Pipeline, strictly one archive at a time (parallel files only fight over
the same USB bandwidth / CPU and multiply local disk usage):

      7z x <source> -> output/fix_temp/<stem>/        (local disk)
      rar a         -> <source dir>/.tmp_<stem>.rar   (written straight to
                                                        the destination disk)
      fsync, os.replace() -> <stem>.rar               (atomic rename)
      delete source (skip if source IS the final path, i.e. disguised .rar)
      rmtree extract dir

7z/rar inherit the terminal so their own in-place percentage is the live
progress; their chatter is suppressed (7z -bso0, rar -idcdn).

State machine per archive, as seen by a re-run (package-level resume):
  - source only                     -> convert
  - source + .tmp_<stem>.rar        -> sweep tmp, convert
  - source + complete <stem>.rar    -> `rar t` the rar; ok: delete source,
                                       skip; broken: convert (overwrites)
  - <stem>.rar only                 -> done, skip
  The final name only ever appears via fsync + atomic rename, so it is
  never half-written, even across power loss.

Interruption patterns:
  - Ctrl+C (SIGINT): tty delivers it to 7z/rar too; subprocess.run
    reaps/kills the child, KeyboardInterrupt unwinds, finally-blocks remove
    .tmp_ and the extract dir.
  - SIGTERM / SIGHUP (kill, closed terminal): handler raises SystemExit so
    the same unwinding happens; subprocess.run kills the child.
  - SIGKILL / OOM / power loss: nothing can run; the child carries
    PR_SET_PDEATHSIG=SIGKILL so it dies with us. Leftovers (.tmp_ on the
    destination, output/fix_temp/ locally) are swept at the next start.
  - Concurrent runs: output/fix_temp/ is flock'ed, second instance refuses
    to start.

Usage:
    python fix_to_rar.py <dir>
"""

import ctypes
import fcntl
import os
import shutil
import signal
import subprocess
import sys
from pathlib import Path
from typing import List, Optional, Tuple

SOURCE_EXTENSIONS = {".zip", ".7z", ".rar"}

MAGIC_TO_FORMAT = {
    b"Rar!": "rar",
    b"7z\xbc\xaf\x27\x1c": "7z",
    b"PK\x03\x04": "zip",
}

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
WORK_DIR = PROJECT_ROOT / "output" / "fix_temp"

PR_SET_PDEATHSIG = 1


def _die_with_parent():
    """preexec_fn for 7z/rar: SIGKILL the child when this python dies
    (Linux PDEATHSIG), so no orphan survives even a SIGKILL of the script."""
    ctypes.CDLL("libc.so.6", use_errno=True).prctl(PR_SET_PDEATHSIG, signal.SIGKILL)


def _exit_on_signal(signum, _frame):
    # Turn SIGTERM/SIGHUP into a normal unwind so finally-blocks run and
    # subprocess.run kills the active child.
    raise SystemExit(128 + signum)


def run_tool(args: List[str], cwd: Optional[Path] = None) -> int:
    """Run 7z/rar attached to the terminal (their in-place percentage is the
    live progress). Returns the exit code."""
    return subprocess.run(args, cwd=cwd, preexec_fn=_die_with_parent).returncode


def detect_format(path: Path) -> Optional[str]:
    with open(path, "rb") as f:
        head = f.read(6)
    for magic, fmt in MAGIC_TO_FORMAT.items():
        if head.startswith(magic):
            return fmt
    return None


def convert_one(tag: str, source: Path) -> Optional[str]:
    """Extract to local disk, rar straight to the destination dir as a hidden
    temp, fsync + atomic rename over <stem>.rar, delete source.

    Returns None on success, else a short error string. Any exception or
    early return leaves the source untouched and removes the temp files.
    """
    stem = source.stem
    final = source.with_suffix(".rar")
    dest_tmp = final.parent / f".tmp_{final.name}"
    extract_dir = WORK_DIR / stem

    try:
        extract_dir.mkdir(parents=True)
        print(f"{tag} {source.name}: extract")
        if (
            run_tool(
                ["7z", "x", str(source), f"-o{extract_dir}/", "-y", "-bso0", "-bsp1"]
            )
            != 0
        ):
            return "extract failed"
        if not (extract_dir / stem).is_dir():
            print(
                f"{tag} {source.name}: warning, extracted content has no {stem}/ directory"
            )

        print(f"{tag} {source.name}: rar -> {final.name}")
        if (
            run_tool(
                ["rar", "a", "-m3", "-ma5", "-r", "-idcdn", str(dest_tmp), "."],
                cwd=extract_dir,
            )
            != 0
        ):
            return "rar creation failed"

        # Durable content before the name appears: a final .rar that exists
        # after a crash is always complete.
        with open(dest_tmp, "rb") as f:
            os.fsync(f.fileno())
        os.replace(dest_tmp, final)  # atomic same-fs rename
        if source != final:
            source.unlink()
        print(f"{tag} {source.name}: done")
        return None
    finally:
        dest_tmp.unlink(missing_ok=True)  # no-op after a successful rename
        shutil.rmtree(extract_dir, ignore_errors=True)


def verify_existing(tag: str, source: Path, final: Path) -> bool:
    """A previous run renamed <stem>.rar into place but died before deleting
    the source. Test the rar; if intact, finish the job by deleting source."""
    print(f"{tag} {source.name}: {final.name} already exists, testing it")
    if run_tool(["rar", "t", "-idcdn", str(final)]) != 0:
        print(f"{tag} {source.name}: {final.name} is broken, reconverting")
        return False
    source.unlink()
    print(f"{tag} {source.name}: {final.name} intact, source removed")
    return True


def lock_work_dir() -> int:
    """flock the local work dir (refuse concurrent runs), then sweep whatever
    a dead run left in it. Returns the fd; keep it open for the whole run."""
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    fd = os.open(WORK_DIR, os.O_RDONLY)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        sys.exit(f"Another fix_to_rar.py is running (lock on {WORK_DIR})")
    for leftover in WORK_DIR.iterdir():
        shutil.rmtree(leftover) if leftover.is_dir() else leftover.unlink()
        print(f"Swept stale leftover: {leftover}")
    return fd


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
        final = path.with_suffix(".rar")
        has_final = final != path and final.exists()
        if has_final:
            assert detect_format(final) == "rar", f"{final} exists but is not a RAR"
        tasks.append((path, fmt, has_final))
    return tasks


def main():
    if len(sys.argv) != 2:
        print("Usage: python fix_to_rar.py <dir>")
        print("Example: python fix_to_rar.py /media/chuyin/Disk/data/L2/2026/202608")
        sys.exit(1)

    scan_dir = Path(sys.argv[1])
    assert scan_dir.is_dir(), f"Not a directory: {scan_dir}"

    signal.signal(signal.SIGTERM, _exit_on_signal)
    signal.signal(signal.SIGHUP, _exit_on_signal)
    lock_fd = lock_work_dir()  # held until exit

    tasks = collect_tasks(scan_dir)
    if not tasks:
        print("Nothing to convert.")
        return

    print(f"\nFound {len(tasks)} archive(s) to convert:")
    for path, fmt, has_final in tasks:
        note = (
            "  (final .rar already present, will be tested first)" if has_final else ""
        )
        print(f"  [{fmt}] {path}{note}")
    print("\nOne at a time. Each file is replaced IN PLACE (atomic rename) as soon")
    print("as it converts; its source is then deleted. Failed or interrupted files")
    print("keep their source untouched; re-run to resume.")
    print()

    if input("Continue? (yes/no): ").lower() != "yes":
        print("Cancelled.")
        return

    total = len(tasks)
    done = 0
    failed: List[Tuple[Path, str]] = []
    try:
        for idx, (source, _fmt, has_final) in enumerate(tasks, 1):
            tag = f"[{idx}/{total}]"
            if has_final and verify_existing(tag, source, source.with_suffix(".rar")):
                done += 1
                continue
            err = convert_one(tag, source)
            if err:
                failed.append((source, err))
                print(f"{tag} {source.name}: ✗ {err}")
            else:
                done += 1
    except KeyboardInterrupt:
        print(
            "\nInterrupted. Unfinished files keep their original source; re-run to resume."
        )
        sys.exit(130)
    finally:
        shutil.rmtree(WORK_DIR, ignore_errors=True)
        os.close(lock_fd)

    print(f"\nSummary:")
    print(f"  Done: {done}/{total}")
    for source, err in failed:
        print(f"  ✗ {source.name}: {err}")


if __name__ == "__main__":
    main()
