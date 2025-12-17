"""TSan mode: Race condition detection with ThreadSanitizer (Windows)"""
import os
import subprocess
import sys
import time


def run(binary_path, working_dir):
    """Run with ThreadSanitizer."""
    log_dir = os.path.abspath("output/log")
    os.makedirs(log_dir, exist_ok=True)
    tsan_log = os.path.join(log_dir, "tsan_report.log")
    
    # Configure TSan: halt on first error
    env = os.environ.copy()
    tsan_options = [
        "history_size=3",
        "halt_on_error=1",
        "second_deadlock_stack=0",
        "print_suppressions=0",
    ]
    env['TSAN_OPTIONS'] = ':'.join(tsan_options)
    
    # Run binary
    print("Running with ThreadSanitizer (expect 5-15x slowdown)...")
    print(f"Report: {tsan_log}\n")
    start_time = time.time()
    
    result = subprocess.run(
        [binary_path],
        cwd=working_dir,
        env=env,
        stderr=subprocess.PIPE,
        text=True
    )
    
    elapsed_time = time.time() - start_time
    
    # Save report
    if result.stderr:
        with open(tsan_log, 'w', encoding='utf-8') as f:
            f.write(result.stderr)
    
    # Show summary
    print(f"\n{'='*80}")
    print(f"✓ TSan Complete! ({elapsed_time:.2f}s)")
    print(f"{'='*80}\n")
    
    if os.path.exists(tsan_log) and os.path.getsize(tsan_log) > 0:
        with open(tsan_log, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            for line in lines[:300]:
                print(line, end='')
            if len(lines) > 300:
                print(f"\n... ({len(lines) - 300} more lines)")
        
        print(f"\n{'='*80}")
        print(f"Full report: {tsan_log}")
        print(f"{'='*80}\n")
    else:
        print("✓ No race conditions detected!\n")
    
    sys.exit(result.returncode)

