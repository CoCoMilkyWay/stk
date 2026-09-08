"""Debug mode: Crash stack capture with gdb + auto analysis"""
import os
import subprocess
import time


def run(binary_path, working_dir):
    """Run binary under gdb; on crash, dump all thread stacks to file."""
    debug_dir = os.path.abspath("output/debug")
    os.makedirs(debug_dir, exist_ok=True)

    stack_file = os.path.join(debug_dir, "crash_stacks.txt")

    print("Running under gdb (automatic crash stack capture)...")
    print(f"Crash stacks will be saved to: {stack_file}")
    print("Press Ctrl+C to exit\n")

    gdb_commands = [
        "set pagination off",
        "set logging enabled on",
        f"set logging file {stack_file}",
        "set logging overwrite on",
        "run",
        "echo \\n======================================\\n",
        "echo CRASH ANALYSIS\\n",
        "echo ======================================\\n\\n",
        "bt",
        "echo \\n======================================\\n",
        "echo ALL THREAD STACKS\\n",
        "echo ======================================\\n\\n",
        "thread apply all bt",
        "echo \\n======================================\\n",
        "echo ANALYSIS COMPLETE\\n",
        "echo ======================================\\n",
        "quit",
    ]

    args = ["gdb", "--batch"]
    for cmd in gdb_commands:
        args += ["-ex", cmd]
    args.append(binary_path)

    start_time = time.time()
    proc = subprocess.run(args, cwd=working_dir)
    elapsed_time = time.time() - start_time

    print(f"\n{'='*80}")
    if proc.returncode == 0:
        print(f"✓ Debug Complete! ({elapsed_time:.2f}s)")
    else:
        print(f"✗ Crashed with code {proc.returncode:#x} ({elapsed_time:.2f}s)")
        if os.path.exists(stack_file):
            size_kb = os.path.getsize(stack_file) / 1024
            print(f"✓ Crash stacks: {stack_file} ({size_kb:.2f} KB)")
    print(f"{'='*80}\n")
