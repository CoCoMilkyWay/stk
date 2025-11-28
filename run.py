import os
import signal
import subprocess
import sys
import time

# ============================================================================
# Build & Run Pipeline
# ============================================================================
# Calling chain: run.py -> py/{APP_NAME}.py -> cpp/projects/{APP_NAME}/build.sh
#
# - run.py: orchestrates build -> run -> profiling/debugging
# - py/{APP_NAME}.py: project-specific preparation + triggers build.sh
# - build.sh: compiles C++ + copies compile_commands.json for clangd
#
# Usage:
#   1. Set ONE mode flag (ENABLE_TSAN/DEBUG/PROFILE) to True
#   2. Run: python run.py
#
# Modes:
#   - TSAN:    Detect race conditions (5-15x slower, auto-detect bugs)
#   - DEBUG:   Interactive GDB debugging (very slow, manual inspection)
#   - PROFILE: CPU profiling with gperftools (slow, performance analysis)
#   - Default: Production build (-O3, fastest)
# ============================================================================

# ============================================================================
# Configuration
# ============================================================================
APP_NAME = "main"

# Build & Run modes (set ONE to True, others to False)
# Priority: TSAN > DEBUG > PROFILE > PRODUCTION
ENABLE_TSAN = False  # ThreadSanitizer: race condition detection (-O1)
ENABLE_PROFILE = True  # gperftools: CPU profiling (-O0)
ENABLE_DEBUG = False   # GDB: interactive debugging (-O0)
# Default: production mode with -O3 optimizations

# Profiler settings
CPUPROFILE_FREQUENCY = 1000000                 # Sampling rate (Hz)
PROFILER_LIB = '/usr/lib/x86_64-linux-gnu/libprofiler.so.0'
TARGET_NAMESPACE = "Analysis"                  # Focus namespace
PPROF_PORT = 8080                              # Web GUI port
PPROF_IGNORE = "std::|__gnu_cxx::"             # Filter standard library


def _cleanup_background_processes():
    """Kill background processes that may cause memory bloat."""
    processes_to_kill = [
        f"pprof.*{PPROF_PORT}",      # Old pprof web servers
        f"app_{APP_NAME}",            # Old app instances
    ]

    for pattern in processes_to_kill:
        subprocess.run(["pkill", "-9", "-f", pattern],
                       capture_output=True, check=False)

    time.sleep(0.3)


def _cleanup_old_profiler():
    """Kill old pprof web server."""
    subprocess.run(["pkill", "-f", f"pprof.*{PPROF_PORT}"],
                   check=False, capture_output=True)
    time.sleep(0.2)


def _find_pprof_command():
    """Find pprof executable."""
    for cmd in ["pprof", "google-pprof", os.path.expanduser("~/go/bin/pprof")]:
        result = subprocess.run(
            ["which", cmd], capture_output=True, check=False)
        if result.returncode == 0:
            return cmd
    return None


def _show_profile_report(binary_path, profile_file):
    """Generate and display profiling report."""
    pprof_cmd = _find_pprof_command()
    if not pprof_cmd:
        print("pprof not found. Profile data saved to:", profile_file)
        return

    print(f"\n{'='*80}")
    print(f"SAMPLING PROFILE - Top Functions ({CPUPROFILE_FREQUENCY} Hz)")
    print(f"{'='*80}\n")

    pprof_top = subprocess.run(
        [pprof_cmd, "--top", "--cum", "--nodecount=20",
         f"--focus={TARGET_NAMESPACE}", f"--hide={PPROF_IGNORE}",
         binary_path, profile_file],
        capture_output=True, text=True, check=False
    )

    if pprof_top.returncode == 0:
        print(pprof_top.stdout)

    # Launch web server
    subprocess.Popen(
        [pprof_cmd, f"-http=:{PPROF_PORT}", binary_path, profile_file],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )

    time.sleep(1.5)
    subprocess.run(["cmd.exe", "/c", "start", f"http://localhost:{PPROF_PORT}"],
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)

    print(f"\n✓ Web GUI: http://localhost:{PPROF_PORT}")
    print(f"  To stop: pkill -f 'pprof.*{PPROF_PORT}'\n")


def run_with_profiling(binary_path, working_dir):
    """Run C++ binary with perf profiler (high-precision sampling)."""
    import threading
    
    # All profiling files in output/profile directory
    profile_dir = os.path.abspath("output/profile")
    os.makedirs(profile_dir, exist_ok=True)
    
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    perf_data = os.path.join(profile_dir, f"lob_{timestamp}.data")
    flamegraph_svg = os.path.join(profile_dir, f"flamegraph_{timestamp}.svg")
    
    # Signal file in exe working directory (build/)
    PROFILE_FLAG = os.path.join(working_dir, ".profile")
    
    # Clean up old signal file if exists
    if os.path.exists(PROFILE_FLAG):
        os.remove(PROFILE_FLAG)
    
    # Check perf availability
    if subprocess.run(["which", "perf"], capture_output=True).returncode != 0:
        print("ERROR: perf not found. Install: sudo apt install linux-tools-generic linux-tools-$(uname -r)")
        sys.exit(1)
    
    # FlameGraph tools in output/profile/tools/
    flamegraph_dir = os.path.join(profile_dir, "tools", "FlameGraph")
    if not os.path.exists(flamegraph_dir):
        print("Installing FlameGraph tools...")
        os.makedirs(os.path.dirname(flamegraph_dir), exist_ok=True)
        subprocess.run(["git", "clone", "--depth=1", 
                       "https://github.com/brendangregg/FlameGraph.git", 
                       flamegraph_dir], check=True)
    
    perf_proc = None
    
    def monitor_and_profile():
        """Background thread: monitor flag file and control perf."""
        nonlocal perf_proc
        
        print("\n[Profiler] Waiting for LOB computation to start...")
        print("[Profiler] (GUI操作不会被采样，只有点击\"开始计算\"后才会 profile)\n")
        
        # Wait for flag file
        while not os.path.exists(PROFILE_FLAG):
            time.sleep(0.1)
        
        # Read PID from flag file
        try:
            with open(PROFILE_FLAG, 'r') as f:
                pid = f.read().strip()
        except:
            return
        
        print(f"\n{'='*80}")
        print(f"[Profiler] LOB computation started! Profiling PID={pid} at MAX frequency...")
        print(f"{'='*80}\n")
        
        # Start perf with maximum precision
        perf_cmd = [
            "sudo", "perf", "record",
            "-F", "99999",           # Maximum sampling frequency (99999 Hz, ~10μs间隔)
            "-p", pid,               # Attach to process
            "-g",                    # Call graph (stack traces)
            "--call-graph", "dwarf", # DWARF unwinding (most accurate)
            "-o", perf_data,         # Output file
        ]
        
        perf_proc = subprocess.Popen(perf_cmd, 
                                     stdout=subprocess.DEVNULL, 
                                     stderr=subprocess.DEVNULL)
        
        # Wait for flag file to disappear (computation done)
        while os.path.exists(PROFILE_FLAG):
            time.sleep(0.1)
        
        # Stop perf
        if perf_proc:
            perf_proc.send_signal(signal.SIGINT)
            perf_proc.wait()
        
        print(f"\n{'='*80}")
        print(f"[Profiler] LOB computation finished! Generating flame graph...")
        print(f"{'='*80}\n")
    
    # Start profiler thread
    profiler_thread = threading.Thread(target=monitor_and_profile, daemon=True)
    profiler_thread.start()
    
    # Run the binary
    print(f"Starting GUI (PROFILE_MODE: LOB functions visible)...\n")
    start_time = time.time()
    
    try:
        subprocess.run([binary_path], cwd=working_dir, check=True)
    except KeyboardInterrupt:
        print("\n[Interrupted by user]")
    
    elapsed_time = time.time() - start_time
    
    # Wait for profiler to finish
    profiler_thread.join(timeout=5)
    
    # Generate flame graph
    if os.path.exists(perf_data):
        print("Generating flame graph...")
        
        try:
            # Convert perf.data to stacks
            perf_script = subprocess.run(
                ["sudo", "perf", "script", "-i", perf_data],
                capture_output=True, text=True, check=True
            )
            
            # Collapse stacks
            stackcollapse = subprocess.run(
                [os.path.join(flamegraph_dir, "stackcollapse-perf.pl")],
                input=perf_script.stdout, capture_output=True, text=True, check=True
            )
            
            # Generate flame graph
            with open(flamegraph_svg, 'w') as f:
                subprocess.run(
                    [os.path.join(flamegraph_dir, "flamegraph.pl"),
                     "--title", f"LOB Profile - {timestamp}",
                     "--width", "1920",
                     "--colors", "hot"],
                    input=stackcollapse.stdout, stdout=f, text=True, check=True
                )
            
            print(f"\n{'='*80}")
            print(f"✓ Profiling Complete! (Time: {elapsed_time:.2f}s)")
            print(f"{'='*80}\n")
            print(f"📊 Flame graph: {flamegraph_svg}")
            print(f"🔍 Raw data:    {perf_data}")
            print(f"\n🌐 View in browser:")
            print(f"   firefox {flamegraph_svg}")
            print(f"\n📈 Interactive analysis:")
            print(f"   sudo perf report -i {perf_data}\n")
            
            # Clean up signal file
            if os.path.exists(PROFILE_FLAG):
                os.remove(PROFILE_FLAG)
            
        except subprocess.CalledProcessError as e:
            print(f"ERROR: Failed to generate flame graph: {e}")
            # Clean up on error
            if os.path.exists(PROFILE_FLAG):
                os.remove(PROFILE_FLAG)
    else:
        print("No profiling data collected (computation may not have started)")
        # Clean up signal file
        if os.path.exists(PROFILE_FLAG):
            os.remove(PROFILE_FLAG)


def run_with_gdb_debug(binary_path, working_dir):
    """Run binary under GDB with automatic thread stack trace capture."""
    log_dir = os.path.abspath("output/log")
    os.makedirs(log_dir, exist_ok=True)

    trace_file = os.path.join(log_dir, "gdb_trace.txt")
    gdb_script = os.path.join(working_dir, "auto_debug.gdb")

    # Generate GDB script for automatic trace capture
    gdb_commands = f"""# Auto-generated GDB script
set pagination off
set logging file {trace_file}
set logging overwrite on
set logging enabled on
set confirm off

# Catch signals and crashes
catch signal SIGSEGV SIGABRT SIGFPE SIGILL SIGBUS
commands
  echo \\n=== CRASH DETECTED ===\\n
  info threads
  echo \\n=== STACK TRACES ===\\n
  thread apply all bt
  quit
end

# Run program
run

# Normal exit
echo \\n=== NORMAL EXIT ===\\n
info threads
thread apply all bt

set logging enabled off
quit
"""

    with open(gdb_script, 'w') as f:
        f.write(gdb_commands)

    print(f"Running under GDB with automatic stack trace capture...")
    print(f"Trace will be saved to: {trace_file}")
    print(f"Press Ctrl+C to capture current thread state.\n")

    start_time = time.time()

    # Run under GDB
    proc = subprocess.Popen(
        ["gdb", "-x", gdb_script, binary_path],
        cwd=working_dir,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )

    try:
        # Stream output in real-time
        if proc.stdout:
            for line in proc.stdout:
                print(line, end='')

        proc.wait()
        elapsed = time.time() - start_time

        if proc.returncode == 0:
            print(f"\n✓ Program completed successfully")
            print(f"Execution time: {elapsed:.2f}s ({elapsed/60:.2f}min)")
        else:
            print(f"\n✗ Program exited with code {proc.returncode}")

    except KeyboardInterrupt:
        print("\n\n=== Ctrl+C detected - Capturing thread state ===")

        # Send interrupt to GDB (which forwards to program)
        proc.send_signal(signal.SIGINT)
        time.sleep(0.5)

        # Send GDB commands to capture state
        capture_commands = """
echo \\n=== MANUAL INTERRUPT (Ctrl+C) ===\\n
info threads
echo \\n=== STACK TRACES ===\\n
thread apply all bt
quit
"""
        if proc.stdin:
            proc.stdin.write(capture_commands)
            proc.stdin.flush()

        # Wait for GDB to finish
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()

        elapsed = time.time() - start_time
        print(f"\n✓ Thread trace captured to: {trace_file}")
        print(f"Execution time: {elapsed:.2f}s ({elapsed/60:.2f}min)")
        print(f"Analyze with: less {trace_file}")
        sys.exit(1)


def run_with_tsan_debug(binary_path, working_dir):
    """Run binary with ThreadSanitizer for race condition detection."""
    log_dir = os.path.abspath("output/log")
    os.makedirs(log_dir, exist_ok=True)
    tsan_log = os.path.join(log_dir, "tsan_report.log")

    # Configure TSan options: halt on first error
    env = os.environ.copy()
    tsan_options = [
        "history_size=3",
        "halt_on_error=1",
        "second_deadlock_stack=0",
        "print_suppressions=0",
        "report_signal_unsafe=0",
    ]
    env['TSAN_OPTIONS'] = ':'.join(tsan_options)

    print("Running with ThreadSanitizer (expect 5-15x slowdown)...")
    print(f"TSan report will be saved to: {tsan_log}\n")

    start_time = time.time()

    # Run and capture stderr (TSan output)
    result = subprocess.run(
        [binary_path],
        cwd=working_dir,
        env=env,
        stderr=subprocess.PIPE,
        text=True
    )

    # Save stderr to file after process completes
    if result.stderr:
        with open(tsan_log, 'w') as log_file:
            log_file.write(result.stderr)

    elapsed = time.time() - start_time

    print(f"\n✓ TSan run completed")
    print(f"Execution time: {elapsed:.2f}s ({elapsed/60:.2f}min)")

    # Show summary
    if os.path.exists(tsan_log) and os.path.getsize(tsan_log) > 0:
        print(f"\n{'='*80}")
        print("ThreadSanitizer Report")
        print(f"{'='*80}\n")

        with open(tsan_log, 'r') as f:
            lines = f.readlines()
            # Show first 300 lines
            for line in lines[:300]:
                print(line, end='')

            if len(lines) > 300:
                print(f"\n... ({len(lines) - 300} more lines)")

        print(f"\n{'='*80}")
        print(f"Full report: {tsan_log}")
        print(f"{'='*80}\n")
    else:
        print(f"\n✓ No race conditions detected!")

    sys.exit(result.returncode)


def build_project(app_name, enable_tsan, enable_profile, enable_debug):
    """Trigger build via py/{app_name}.py -> build.sh."""
    py_script = f"./py/{app_name}.py"

    if not os.path.exists(py_script):
        print(f"Error: Build script not found: {py_script}")
        sys.exit(1)

    env = os.environ.copy()
    env['TSAN_MODE'] = 'ON' if enable_tsan else 'OFF'
    env['PROFILE_MODE'] = 'ON' if enable_profile else 'OFF'
    env['DEBUG_MODE'] = 'ON' if enable_debug else 'OFF'

    result = subprocess.run(["python3", py_script], env=env, check=False)

    if result.returncode != 0:
        print(f"\nBuild failed with exit code {result.returncode}")
        sys.exit(1)


def run_binary(binary_path, working_dir, enable_tsan, enable_profile, enable_debug):
    """Run binary with optional TSan/profiling/GDB."""
    if not os.path.exists(binary_path):
        print(f"Error: Binary not found: {binary_path}")
        sys.exit(1)

    if enable_tsan:
        run_with_tsan_debug(binary_path, working_dir)
    elif enable_debug:
        run_with_gdb_debug(binary_path, working_dir)
    elif enable_profile:
        run_with_profiling(binary_path, working_dir)
    else:
        start_time = time.time()
        subprocess.run([binary_path], cwd=working_dir, check=True)
        elapsed = time.time() - start_time
        print(f"\nExecution time: {elapsed:.2f}s ({elapsed/60:.2f}min)")


def main():
    # Change to script directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)

    # Cleanup background processes first
    print("Cleaning up background processes...")
    _cleanup_background_processes()

    # Build project
    build_project(APP_NAME, ENABLE_TSAN, ENABLE_PROFILE, ENABLE_DEBUG)

    # Run binary from build directory (binary expects to run from build/ for relative paths)
    build_dir = os.path.abspath(f"cpp/projects/{APP_NAME}/build")
    binary_path = os.path.join(build_dir, f"bin/app_{APP_NAME}")
    run_binary(binary_path, build_dir, ENABLE_TSAN,
               ENABLE_PROFILE, ENABLE_DEBUG)


if __name__ == "__main__":
    main()
