"""Debug mode: Crash dump capture with procdump + auto analysis"""
import glob
import os
import subprocess
import time


def find_latest_dump(debug_dir):
    """Find most recent dump file."""
    dumps = glob.glob(f"{debug_dir}/*.dmp")
    return max(dumps, key=os.path.getmtime) if dumps else None


def analyze_dump(dump_path):
    """Run WinDbgX to analyze dump and extract stacks."""
    output_dir = os.path.dirname(dump_path)
    stack_file = os.path.join(output_dir, "crash_stacks.txt")
    build_dir = os.path.abspath("cpp/projects/main/build/bin")
    
    wdbg_script = f""".sympath+ {build_dir}
.reload /f
.echo ======================================
.echo CRASH ANALYSIS
.echo ======================================
.echo
!analyze -v
.echo
.echo ======================================
.echo ALL THREAD STACKS (with source lines)
.echo ======================================
~* kP
.echo
.echo ======================================
.echo ANALYSIS COMPLETE
.echo ======================================
.logclose
q
"""
    
    script_path = os.path.join(output_dir, "analyze.wdbg")
    with open(script_path, 'w') as f:
        f.write(wdbg_script)
    
    print(f"Analyzing dump: {dump_path}")
    subprocess.run(
        ["WinDbgX.exe", "-c", f"$$< {script_path}", "-z", dump_path],
        capture_output=True,
        text=True
    )
    
    if os.path.exists(stack_file):
        size_mb = os.path.getsize(stack_file) / (1024 * 1024)
        print(f"Stacks saved to: {stack_file} ({size_mb:.2f} MB)")


def run(binary_path, working_dir):
    """Run with procdump to capture crash dumps."""
    debug_dir = os.path.abspath("output/debug")
    os.makedirs(debug_dir, exist_ok=True)
    
    procdump = os.path.abspath("py/app/procdump/procdump64.exe")
    assert os.path.exists(procdump), f"procdump.exe not found at {procdump}"
    
    print("Running with procdump (automatic crash dump capture)...")
    print(f"Crash dumps will be saved to: {debug_dir}")
    print(f"Press Ctrl+C to exit\n")
    start_time = time.time()
    
    proc = subprocess.Popen(
        [procdump, "-accepteula", "-ma", "-e", "-x", debug_dir, binary_path],
        cwd=working_dir
    )
    
    proc.wait()
    elapsed_time = time.time() - start_time
    
    print(f"\n{'='*80}")
    if proc.returncode == 0:
        print(f"✓ Debug Complete! ({elapsed_time:.2f}s)")
    else:
        print(f"✗ Crashed with code {proc.returncode:#x} ({elapsed_time:.2f}s)")
        dump = find_latest_dump(debug_dir)
        if dump:
            print(f"✓ Crash dump: {dump}")
            analyze_dump(dump)
    print(f"{'='*80}\n")

