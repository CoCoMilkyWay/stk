#!/usr/bin/env python3
"""
Lint all C/C++ source files under cpp/ by driving clangd over LSP.

Collects the same diagnostics clangd reports to the IDE when a file is opened
(unused includes, unused variables, compiler warnings, ...). Errors are NOT
reported here — only Warning / Information / Hint (the yellow squiggles).

clangd is started the same way the IDE starts it (reads cpp/.clangd config,
picks up cpp/compile_commands.json automatically), so results match what you
see in Cursor.

A translation unit's diagnostics depend only on its own text, the files it
transitively #includes, the compile flags and clangd itself, so each file's
diagnostic counts are cached under a hash of exactly that; unchanged files are
skipped.

Usage:
    python py/app/cpp/lint.py              # lint all files
    python py/app/cpp/lint.py <path>       # lint a single file / dir
    python py/app/cpp/lint.py --all        # include Error severity too
"""

import hashlib
import json
import os
import re
import select
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path
from urllib.parse import quote

from common import (
    CPP_DIR,
    FORMAT_EXTENSIONS,
    LINT_EXTENSIONS,
    PROJECT_ROOT,
    file_hash,
    iter_sources,
    load_cache,
    save_cache,
    which,
)

CLANGD_FILE = CPP_DIR / ".clangd"
COMPILE_COMMANDS = CPP_DIR / "compile_commands.json"

# Per file: [hash of itself + its transitive project #includes, diagnostic counts by severity].
# Invalidated when clangd version, .clangd or compile_commands.json changes.
CACHE_FILE = Path(__file__).resolve().parent / "__pycache__" / "lint_cache.json"

INCLUDE_RE = re.compile(rb'^[ \t]*#[ \t]*include[ \t]*[<"]([^>"\n]+)[>"]', re.MULTILINE)

# Concurrent preamble/AST builds inside clangd. Lower if memory runs out.
CLANGD_JOBS = os.cpu_count()

# LSP severity values
SEV_ERROR = 1
SEV_WARNING = 2
SEV_COUNT = 4

# Max wall-clock seconds to wait for clangd to report on all opened files.
# (clangd may stay silent on headers it can't build a TU for.)
DRAIN_TIMEOUT_S = 120.0

# Once clangd has reported on at least one file, a quiet window this long
# with no new diagnostics means we consider it done.
DRAIN_QUIET_S = 2.0


def file_uri(path: Path) -> str:
    # LSP file URIs are percent-encoded; clangd expects absolute paths.
    quoted = quote(str(path))
    return f"file://{quoted}"


def rel_str(path: Path) -> str:
    return str(path.relative_to(PROJECT_ROOT))


# ============================================================================
# Cache key: hash of a file plus every project source it transitively includes
# ============================================================================


def dep_hashes(sources: list) -> dict:
    contents = {p: p.read_bytes() for p in sources}
    by_name = defaultdict(list)
    for p in sources:
        by_name[p.name].append(p)

    def resolve(includer: Path, inc: bytes) -> set:
        # Over-approximate (extra deps only cost a recheck): the path relative to the
        # includer plus every source whose path ends with the include spelling.
        spelled = Path(inc.decode("utf-8", errors="replace"))
        tail = "/" + "/".join(s for s in spelled.parts if s not in (".", ".."))
        found = {p for p in by_name[spelled.name] if str(p).endswith(tail)}
        relative = Path(os.path.normpath(includer.parent / spelled))
        if relative in contents:
            found.add(relative)
        return found

    direct = {
        p: set().union(*(resolve(p, inc) for inc in INCLUDE_RE.findall(text)))
        for p, text in contents.items()
    }
    own = {
        p: f"{rel_str(p)}\0{hashlib.blake2b(text, digest_size=16).hexdigest()}\n"
        for p, text in contents.items()
    }
    hashes = {}
    for p in sources:
        seen, stack = {p}, [p]
        while stack:
            for d in direct[stack.pop()] - seen:
                seen.add(d)
                stack.append(d)
        joined = "".join(own[d] for d in sorted(seen)).encode()
        hashes[rel_str(p)] = hashlib.blake2b(joined, digest_size=16).hexdigest()
    return hashes


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
                f"-j={CLANGD_JOBS}",
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

    def read_message(self, timeout: float | None = None):
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
            assert msg is not None, "clangd closed during initialize"
            if msg.get("id") is not None:
                break
        self.notify("initialized", {})

    def shutdown(self):
        self.notify("shutdown", {})
        self.notify("exit", None)
        self.proc.stdin.close()
        self.proc.wait(timeout=5)


# ============================================================================
# Linting (all files opened at once, diagnostics printed as they land)
# ============================================================================


def status(msg: str) -> None:
    """Overwrite the current terminal line with a status string (no newline)."""
    sys.stdout.write(f"\r\033[K{msg}")
    sys.stdout.flush()


def clear_status() -> None:
    sys.stdout.write("\r\033[K")
    sys.stdout.flush()


def count_by_severity(diags: list) -> list:
    counts = [0] * SEV_COUNT
    for d in diags:
        counts[d["severity"] - 1] += 1
    return counts


def print_diags(rel: str, counts: list, include_errors: bool) -> int:
    min_sev = SEV_ERROR if include_errors else SEV_WARNING
    n = sum(counts[min_sev - 1 :])
    if not n:
        return 0
    # clear the bottom status line, print the file result above it, then
    # the caller re-draws the status line.
    clear_status()
    print(f"  {rel}  ({n})", flush=True)
    return n


def lint_all(client: LspClient, paths: list, include_errors: bool) -> tuple:
    """didOpen every file at once, print diagnostics incrementally as they land.
    Returns (shown diagnostic total, {path: counts by severity} for files clangd reported on).
    """
    uri_to_path = {}
    results = {}
    pending = set()
    for path in paths:
        uri = file_uri(path)
        uri_to_path[uri] = path
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
        path = uri_to_path[uri]
        results[path] = count_by_severity(params.get("diagnostics", []))
        total += print_diags(rel_str(path), results[path], include_errors)
        pending.discard(uri)
        client.notify("textDocument/didClose", {"textDocument": {"uri": uri}})
        reported += 1
        last_recv = time.time()
        status(f"{reported}/{total_files} files checked, {len(pending)} pending ...")

    clear_status()
    for uri in pending:
        client.notify("textDocument/didClose", {"textDocument": {"uri": uri}})
    return total, results


# ============================================================================
# Main
# ============================================================================


def main():
    os.chdir(PROJECT_ROOT)
    # line-buffered stdout -> incremental output
    sys.stdout.reconfigure(line_buffering=True)

    include_errors = "--all" in sys.argv

    args = [a for a in sys.argv[1:] if not a.startswith("-")]
    target = (PROJECT_ROOT / args[0]).resolve() if args else CPP_DIR
    assert target.exists(), f"target not found: {target}"
    files = list(iter_sources(target, LINT_EXTENSIONS)) if target.is_dir() else [target]
    assert files, f"No C/C++ source files to lint under {target}"
    print(f"Linting {len(files)} file(s) with clangd ...", flush=True)

    binary = which("clangd")
    version = subprocess.run(
        [binary, "--version"], capture_output=True, text=True, check=True
    ).stdout.splitlines()[0]
    print(f"Using: {version}")

    assert CLANGD_FILE.is_file(), f"clangd config not found: {CLANGD_FILE}"
    assert COMPILE_COMMANDS.is_file(), f"compile database not found: {COMPILE_COMMANDS}"
    cache_key = "|".join([version, file_hash(CLANGD_FILE), file_hash(COMPILE_COMMANDS)])
    cache = load_cache(CACHE_FILE, cache_key)
    deps = dep_hashes(list(iter_sources(CPP_DIR, FORMAT_EXTENSIONS)))
    assert all(rel_str(p) in deps for p in files), "target outside the lintable sources"
    valid = {rel: v for rel, v in cache.items() if deps.get(rel) == v[0]}
    todo = [p for p in files if rel_str(p) not in valid]
    print(f"{len(files) - len(todo)} cached, {len(todo)} to check")

    total = sum(
        print_diags(rel_str(p), valid[rel_str(p)][1], include_errors)
        for p in files
        if rel_str(p) in valid
    )
    results = {}
    if todo:
        client = LspClient(binary, CPP_DIR)
        client.initialize()
        checked, results = lint_all(client, todo, include_errors)
        client.shutdown()
        total += checked

    for p, counts in results.items():
        valid[rel_str(p)] = [deps[rel_str(p)], counts]
    save_cache(CACHE_FILE, cache_key, valid)

    print(
        f"Done. {total} diagnostic(s) across linted files, "
        f"{len(results)}/{len(todo)} checked.",
        flush=True,
    )


if __name__ == "__main__":
    main()
