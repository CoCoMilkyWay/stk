"""运行模式: 每模式同签名 (binary, cwd) -> 退出码; launch 统一横幅 / 计时 / 收尾"""

import os
import subprocess
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
RUN_DIR = REPO_ROOT / "output" / "run" / "app"

# ============================================================================
# tsan
# ============================================================================
TSAN_OPTIONS = (
    "history_size=3:halt_on_error=1:second_deadlock_stack=0:print_suppressions=0"
)


def _tsan(binary: Path, cwd: Path) -> int:
    report = RUN_DIR / "tsan_report.log"
    env = os.environ | {"TSAN_OPTIONS": TSAN_OPTIONS}
    print("expect 5-15x slowdown")
    with open(report, "w", encoding="utf-8") as f:
        code = subprocess.run([binary], cwd=cwd, env=env, stderr=f).returncode
    print(f"TSan report: {report} ({report.stat().st_size / 1024:.1f} KB)")
    return code


# ============================================================================
# debug
# ============================================================================
def _gdb_section(title: str, command: str) -> list[str]:
    rule = "echo ======================================\\n"
    return ["echo \\n", rule, f"echo {title}\\n", rule, "echo \\n", command]


def _debug(binary: Path, cwd: Path) -> int:
    stacks = RUN_DIR / "crash_stacks.txt"
    script = [
        "set pagination off",
        f"set logging file {stacks}",
        "set logging overwrite on",
        "set logging enabled on",
        "run",
        *_gdb_section("CRASH ANALYSIS", "bt"),
        *_gdb_section("ALL THREAD STACKS", "thread apply all bt"),
        "quit",
    ]
    args = ["gdb", "--batch"]
    for cmd in script:
        args += ["-ex", cmd]
    code = subprocess.run([*args, binary], cwd=cwd).returncode
    print(f"Crash stacks: {stacks} ({stacks.stat().st_size / 1024:.1f} KB)")
    return code


# ============================================================================
# profile: 先拉起 Tracy UI (自动连本机), 再运行 (CMake PROFILE 行开 TRACY_ENABLE)
# ============================================================================
TRACY_EXE = REPO_ROOT / "cpp" / "package" / "tracy" / "tracy-profiler"

# soname -> apt package (Ubuntu/Debian). tracy-profiler 预编译版的非核心依赖.
TRACY_DEP_TO_APT = {
    "libglfw.so.3": "libglfw3",
    "libfreetype.so.6": "libfreetype6",
    "libssl.so.3": "libssl3",
    "libcrypto.so.3": "libssl3",
    "libdbus-1.so.3": "libdbus-1-3",
    "libpng16.so.16": "libpng16-16",
    "libbrotlidec.so.1": "libbrotli1",
    "libbrotlicommon.so.1": "libbrotli1",
    "libbz2.so.1.0": "libbz2-1.0",
}


def _launch_tracy() -> None:
    assert TRACY_EXE.is_file(), f"tracy-profiler not found: {TRACY_EXE}"
    ldd = subprocess.run(
        ["ldd", TRACY_EXE], capture_output=True, text=True, check=True
    ).stdout
    missing = sorted(
        {line.split()[0] for line in ldd.splitlines() if "not found" in line}
    )
    assert not missing, (
        f"tracy-profiler missing {missing}: "
        f"sudo apt install -y {' '.join(sorted({TRACY_DEP_TO_APT.get(s, s) for s in missing}))}"
    )
    # start_new_session: Ctrl+C 只停构建/运行流水线, 不波及 Tracy UI.
    # 系统 GLFW 3.4 在 Wayland 会话下优先选 Wayland, 而 tracy-profiler 每帧调 X11 专属接口
    # (刷屏 "X11: Platform not initialized"). GLFW 按 XDG_SESSION_TYPE (+ DISPLAY) 选平台,
    # 缺 WAYLAND_DISPLAY 时 libwayland 仍默认连 wayland-0, 故须改会话类型才能与 app_main 一样走 XWayland.
    env = os.environ | {"XDG_SESSION_TYPE": "x11"}
    subprocess.Popen(
        [TRACY_EXE, "-a", "localhost"],
        cwd=TRACY_EXE.parent,
        env=env,
        start_new_session=True,
    )
    print(f"Tracy UI launched with auto-connect: {TRACY_EXE}")
    time.sleep(1.0)


def _profile(binary: Path, cwd: Path) -> int:
    _launch_tracy()
    return subprocess.run([binary], cwd=cwd).returncode


# ============================================================================
# assert / production: 直接运行, 差别全在编译期
# ============================================================================
def _direct(binary: Path, cwd: Path) -> int:
    return subprocess.run([binary], cwd=cwd).returncode


# ============================================================================
# 入口
# ============================================================================
MODES = {
    "tsan": ("ThreadSanitizer, 首错即停", _tsan),
    "debug": ("gdb 托管, 崩溃抓全线程栈", _debug),
    "profile": ("Tracy 剖析", _profile),
    "assert": ("优化 + 断言", _direct),
    "production": ("最高性能 (NDEBUG)", _direct),
}


def _banner(text: str) -> None:
    print(f"\n{'=' * 80}\n{text}\n{'=' * 80}\n")


def launch(mode: str, binary: Path, cwd: Path) -> int:
    title, run = MODES[mode]
    assert binary.is_file(), f"binary not found: {binary}"
    RUN_DIR.mkdir(parents=True, exist_ok=True)

    _banner(f"{mode} - {title}")
    start = time.time()
    code = run(binary, cwd)
    elapsed = time.time() - start
    _banner(f"{'[OK]' if code == 0 else '[X]'} {mode} exit={code} ({elapsed:.2f}s)")
    return code
