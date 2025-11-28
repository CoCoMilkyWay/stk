"""Debug mode: Interactive debugging with GDB"""
import os
import signal
import subprocess
import sys
import time


def run(binary_path, working_dir):
    """Run with GDB debugger."""
    log_dir = os.path.abspath("output/log")
    os.makedirs(log_dir, exist_ok=True)
    trace_file = os.path.join(log_dir, "gdb_trace.txt")
    gdb_script = os.path.join(working_dir, "auto_debug.gdb")
    
    # Generate GDB script
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
    
    # Run under GDB
    print("Running under GDB (automatic stack trace capture)...")
    print(f"Trace: {trace_file}")
    print(f"Press Ctrl+C to capture thread state\n")
    start_time = time.time()
    
    proc = subprocess.Popen(
        ["gdb", "-x", gdb_script, binary_path],
        cwd=working_dir,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )
    
    try:
        if proc.stdout:
            for line in proc.stdout:
                print(line, end='')
        
        proc.wait()
        elapsed_time = time.time() - start_time
        
        print(f"\n{'='*80}")
        if proc.returncode == 0:
            print(f"✓ Debug Complete! ({elapsed_time:.2f}s)")
        else:
            print(f"✗ Exited with code {proc.returncode} ({elapsed_time:.2f}s)")
        print(f"{'='*80}\n")
        
    except KeyboardInterrupt:
        print("\n\n=== Ctrl+C: Capturing thread state ===")
        
        proc.send_signal(signal.SIGINT)
        time.sleep(0.5)
        
        if proc.stdin:
            proc.stdin.write("""
echo \\n=== MANUAL INTERRUPT ===\\n
info threads
echo \\n=== STACK TRACES ===\\n
thread apply all bt
quit
""")
            proc.stdin.flush()
        
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        
        elapsed_time = time.time() - start_time
        print(f"\n✓ Trace captured: {trace_file} ({elapsed_time:.2f}s)")
        sys.exit(1)

