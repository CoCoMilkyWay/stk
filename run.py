"""Build & Run Pipeline (Linux)

Usage:
    Set MODE, then: python run.py
"""

import os
import subprocess
import sys
import time
from pathlib import Path

from py import build, modes

# ============================================================================
# Configuration
# ============================================================================
APP_NAME = "main"

# Build & Run mode:
#   MODE          优化  调试符号  断言  运行方式                     产出
#   "tsan"        -O1   -g        开    ThreadSanitizer, 首错即停    output/run/app/tsan_report.log
#   "debug"       -O0   -g        开    gdb 托管, 崩溃抓全线程栈     output/run/app/crash_stacks.txt
#   "profile"     -O2   -g        开    Tracy 剖析 (自动拉起 UI)     -
#   "assert"      -O2   -         开    直接运行                     -
#   "production"  -O2   -         关    直接运行 (NDEBUG)            -
MODE = "assert"

# 因子 GPU 后端 (factor/*/Gpu.cuh, nvcc): 与上面的模式正交. ON 需要完整 CUDA Toolkit (nvcc + CCCL),
# op_check 与 app_main (Alpha→Operators 页 GPU 列) 一并链 cudart; OFF 链 stub, GPU 列显示 n/a.
ENABLE_CUDA = True

# 同步 GitHub 的通道: "" 直连 (无视环境变量里的代理), 否则为代理地址, 如 "http://127.0.0.1:7890"
GIT_PROXY = ""
GIT_PROXY = "http://127.0.0.1:10808"

REPO_ROOT = Path(__file__).resolve().parent
PROJECT_DIR = REPO_ROOT / "cpp" / "projects" / APP_NAME
BUILD_DIR = PROJECT_DIR / "build"
BINARY = BUILD_DIR / "bin" / f"app_{APP_NAME}"


def _start_fetch():
    """Fetch upstream in background; overlaps network latency with cleanup/format."""
    env = os.environ | {"GIT_TERMINAL_PROMPT": "0"}
    return subprocess.Popen(["git", "-c", f"http.proxy={GIT_PROXY}", "fetch", "--quiet"], env=env)


def _git(*args):
    return subprocess.run(
        ["git", *args], capture_output=True, text=True, check=True
    ).stdout


def _git_paths(*args):
    return [p for p in _git(*args, "-z").split("\0") if p]


def _sync_remote(fetch):
    """Fast-forward to upstream if behind, then re-exec so run.py itself is fresh.
    Upstream wins on collisions: untracked files in the way are deleted, conflicting local edits are discarded.
    """
    assert fetch.wait(timeout=15) == 0, "git fetch failed"
    behind = _git("rev-list", "--count", "HEAD..@{u}").strip()
    if behind == "0":
        return
    print(f"Pulling {behind} new commit(s)...")
    for path in _git_paths("diff", "--name-only", "--diff-filter=A", "HEAD", "@{u}"):
        if os.path.lexists(path):
            print(f"  overwrite local: {path}")
            os.remove(path)

    subprocess.run(["git", "merge", "--ff-only", "--autostash", "@{u}"], check=True)
    # autostash 回放冲突时 merge 仍返回 0: 冲突文件留着冲突标记, 整份本地改动另存进 stash
    unmerged = _git_paths("diff", "--name-only", "--diff-filter=U")
    if unmerged:
        for path in unmerged:
            print(f"  overwrite local: {path}")
        subprocess.run(["git", "checkout", "HEAD", "--", *unmerged], check=True)
        subprocess.run(["git", "reset", "--quiet"], check=True)
        subprocess.run(["git", "stash", "drop", "--quiet"], check=True)
    assert not _git_paths("diff", "--name-only", "--diff-filter=U")
    os.execv(sys.executable, [sys.executable] + sys.argv)


def _prune_untracked():
    """Offer to delete files that are neither tracked nor gitignored, one by one; then drop empty untracked directories."""
    for path in _git_paths("ls-files", "--others", "--exclude-standard"):
        if input(f"  delete untracked {path}? [Y/n] ").strip() in ("", "y", "Y"):
            os.remove(path)
    for top in _git_paths("ls-files", "--others", "--exclude-standard", "--directory"):
        if not top.endswith("/"):
            continue
        for path, _, _ in os.walk(top, topdown=False):
            if not os.listdir(path):
                print(f"  delete empty {path}")
                os.rmdir(path)


def _cleanup_processes():
    """Kill old processes."""
    subprocess.run(["pkill", "-f", f"app_{APP_NAME}"], capture_output=True, check=False)
    time.sleep(0.3)


def main():
    assert MODE in modes.MODES, MODE
    os.chdir(REPO_ROOT)
    fetch = _start_fetch()

    print("Cleanup...")
    _cleanup_processes()

    print("Formatting...")
    subprocess.run([sys.executable, "py/app/cpp/format.py"], check=True)

    print("Syncing remote...")
    _sync_remote(fetch)

    print("Scanning untracked...")
    _prune_untracked()

    print("Building...")
    build.build(PROJECT_DIR, MODE, ENABLE_CUDA)

    print("Running...")
    sys.exit(modes.launch(MODE, BINARY, BUILD_DIR))


if __name__ == "__main__":
    main()
