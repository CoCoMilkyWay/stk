#!/usr/bin/env python3
"""
Lint all C/C++ source files under cpp/ by driving clangd over LSP.

Collects the same diagnostics clangd reports to the IDE when a file is opened
(unused includes, unused variables, compiler warnings, ...). Errors are NOT
reported here — only Warning / Information / Hint (the yellow squiggles).

clangd is started the same way the IDE starts it (reads cpp/.clangd config,
picks up cpp/compile_commands.json automatically), so results match what you
see in Cursor.

Usage:
    python py/app/lint_cpp.py              # lint all files
    python py/app/lint_cpp.py <path>       # lint a single file / dir
    python py/app/lint_cpp.py --all        # include Error severity too
"""

import json
import os
import select
import shutil
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import quote, urlparse

# ============================================================================
# Configuration
# ============================================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CPP_DIR = PROJECT_ROOT / "cpp"

EXTENSIONS = {".hpp", ".cpp", ".h", ".cc", ".hh", ".ipp", ".cxx"}

# Directories to skip (build artifacts, vendored libs, generated code)
SKIP_DIRS = {
    "build",
    ".git",
    "CMakeFiles",
    "cmake-build-debug",
    "cmake-build-release",
    # vendored / third-party packages
    "package",
    "boost",
}

# clangd binary: prefer PATH, fall back to the bundled LLVM install.
CLANGD_FALLBACK = Path("/home/chuyin/work/bin/LLVM-22.1.5-Linux-X64/bin/clangd")

# LSP severity values
SEV_ERROR = 1
SEV_WARNING = 2
SEV_INFO = 3
SEV_HINT = 4
SEV_NAME = {1: "error", 2: "warning", 3: "info", 4: "hint"}


# ============================================================================
# Helpers
# ============================================================================


def find_clangd() -> str:
    found = shutil.which("clangd")
    if found:
        return found
    assert (
        CLANGD_FALLBACK.exists()
    ), f"clangd not found on PATH and fallback missing: {CLANGD_FALLBACK}"
    return str(CLANGD_FALLBACK)


def iter_sources(root: Path):
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for name in filenames:
            if Path(name).suffix in EXTENSIONS:
                yield Path(dirpath) / name


def file_uri(path: Path) -> str:
    # LSP file URIs are percent-encoded; clangd expects absolute paths.
    quoted = quote(str(path))
    return f"file://{quoted}"


# ============================================================================
# Minimal LSP client over stdio
# ============================================================================


class LspClient:
    def __init__(self, binary: str, root: Path):
        self.proc = subprocess.Popen(
            [
                binary,
                "--background-index=false",
                "--pch-storage=memory",
                "--log=error",
                "-j=4",
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            bufsize=0,
        )
        self._next_id = 1
        self.root = root

    def _send(self, msg: dict) -> None:
        data = json.dumps(msg).encode("utf-8")
        header = f"Content-Length: {len(data)}\r\n\r\n".encode("ascii")
        self.proc.stdin.write(header + data)
        self.proc.stdin.flush()

    def request(self, method: str, params: dict) -> int:
        msg_id = self._next_id
        self._next_id += 1
        self._send({"jsonrpc": "2.0", "id": msg_id, "method": method, "params": params})
        return msg_id

    def notify(self, method: str, params: dict) -> None:
        self._send({"jsonrpc": "2.0", "method": method, "params": params})

    def read_message(self, timeout: float = None):
        # Read headers
        headers = {}
        while True:
            line = self._readline(timeout)
            if line is None:
                return None
            if line in (b"\r\n", b"\n", b""):
                break
            if b":" in line:
                key, _, val = line.partition(b":")
                headers[key.strip().lower()] = val.strip()
        length = int(headers[b"content-length"])
        body = self._readexactly(length, timeout)
        if body is None:
            return None
        return json.loads(body.decode("utf-8"))

    def _readline(self, timeout):
        # Read until newline from stdout, with optional timeout via select.
        buf = b""
        while True:
            if timeout is not None:
                r, _, _ = select.select([self.proc.stdout], [], [], timeout)
                if not r:
                    return None if not buf else buf
            ch = self.proc.stdout.read(1)
            if not ch:
                return None if not buf else buf
            buf += ch
            if ch in (b"\n",):
                return buf

    def _readexactly(self, n, timeout):
        buf = b""
        while len(buf) < n:
            if timeout is not None:
                r, _, _ = select.select([self.proc.stdout], [], [], timeout)
                if not r:
                    return None
            chunk = self.proc.stdout.read(n - len(buf))
            if not chunk:
                return None
            buf += chunk
        return buf

    def initialize(self):
        self.request(
            "initialize",
            {
                "processId": os.getpid(),
                "rootUri": file_uri(self.root),
                "capabilities": {
                    "textDocument": {
                        "synchronization": {
                            "didOpen": True,
                            "didClose": True,
                            "change": 1,
                        },
                        "publishDiagnostics": {"relatedInformation": False},
                    },
                },
            },
        )
        # wait for initialize response
        while True:
            msg = self.read_message()
            if msg is None:
                raise RuntimeError("clangd closed during initialize")
            if msg.get("id") is not None:
                break
        self.notify("initialized", {})

    def shutdown(self):
        try:
            self.notify("shutdown", {})
            self.notify("exit", None)
        finally:
            self.proc.stdin.close()
            self.proc.wait(timeout=5)


# ============================================================================
# Linting (batched & pipelined)
# ============================================================================

# How many files to hand to clangd at once. clangd parses up to -j in parallel.
# Max wall-clock seconds to wait for clangd to report on all opened files.
# (clangd may stay silent on headers it can't build a TU for.)
DRAIN_TIMEOUT_S = 120.0

# Once clangd has reported on at least one file, a quiet window this long
# with no new diagnostics means we consider it done.
DRAIN_QUIET_S = 2.0


def status(msg: str) -> None:
    """Overwrite the current terminal line with a status string (no newline)."""
    sys.stdout.write(f"\r\033[K{msg}")
    sys.stdout.flush()


def clear_status() -> None:
    sys.stdout.write("\r\033[K")
    sys.stdout.flush()


def print_diags(rel: Path, diags: list, include_errors: bool) -> int:
    min_sev = SEV_ERROR if include_errors else SEV_WARNING
    n = sum(1 for d in diags if d.get("severity", 0) >= min_sev)
    if not n:
        return 0
    # clear the bottom status line, print the file result above it, then
    # the caller re-draws the status line.
    clear_status()
    print(f"  {rel}  ({n})", flush=True)
    return n


def lint_all(client: LspClient, paths: list, include_errors: bool) -> int:
    """didOpen every file at once, print diagnostics incrementally as they land."""
    uri_to_rel = {}
    pending = set()
    for path in paths:
        uri = file_uri(path)
        uri_to_rel[uri] = path.relative_to(PROJECT_ROOT)
        pending.add(uri)
        text = path.read_text(encoding="utf-8", errors="replace")
        client.notify(
            "textDocument/didOpen",
            {
                "textDocument": {
                    "uri": uri,
                    "languageId": "cpp",
                    "version": 1,
                    "text": text,
                }
            },
        )

    total_files = len(paths)
    total = 0
    reported = 0
    status(f"0/{total_files} files checked ...")
    deadline = time.time() + DRAIN_TIMEOUT_S
    last_recv = time.time()
    while pending and time.time() < deadline:
        msg = client.read_message(timeout=2.0)
        if msg is None:
            if reported > 0 and time.time() - last_recv > DRAIN_QUIET_S:
                break
            status(
                f"{reported}/{total_files} files checked, "
                f"{len(pending)} pending ..."
            )
            continue
        if msg.get("method") != "textDocument/publishDiagnostics":
            continue
        params = msg.get("params", {})
        uri = params.get("uri")
        if uri not in pending:
            continue
        diags = params.get("diagnostics", [])
        rel = uri_to_rel[uri]
        total += print_diags(rel, diags, include_errors)
        pending.discard(uri)
        reported += 1
        last_recv = time.time()
        status(f"{reported}/{total_files} files checked, {len(pending)} pending ...")

    clear_status()
    for uri in uri_to_rel:
        client.notify("textDocument/didClose", {"textDocument": {"uri": uri}})
    return total


def main():
    os.chdir(PROJECT_ROOT)
    # line-buffered stdout -> incremental output
    sys.stdout.reconfigure(line_buffering=True)

    include_errors = "--all" in sys.argv

    args = [a for a in sys.argv[1:] if not a.startswith("-")]
    if args:
        target = (PROJECT_ROOT / args[0]).resolve()
        assert target.exists(), f"target not found: {target}"
        files = list(iter_sources(target)) if target.is_dir() else [target]
    else:
        assert CPP_DIR.is_dir(), f"cpp directory not found: {CPP_DIR}"
        files = list(iter_sources(CPP_DIR))

    assert files, "No C/C++ source files to lint"
    print(f"Linting {len(files)} file(s) with clangd ...", flush=True)

    binary = find_clangd()
    client = LspClient(binary, CPP_DIR)
    client.initialize()

    try:
        total = lint_all(client, files, include_errors)
    finally:
        client.shutdown()

    print(f"\nDone. {total} diagnostic(s) across linted files.", flush=True)


if __name__ == "__main__":
    main()
