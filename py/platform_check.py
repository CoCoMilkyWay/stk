"""Cross-platform GUI environment detection and configuration.

Supports:
- Native Linux (X11/Wayland)
- WSL1 (requires X Server on Windows)
- WSL2 (requires X Server on Windows)
"""
import os
import socket
import sys


def _is_wsl():
    """Detect if running in WSL environment."""
    try:
        with open('/proc/version', 'r') as f:
            version = f.read().lower()
            return 'microsoft' in version or 'wsl' in version
    except:
        return False


def _get_windows_host_ip():
    """Get Windows host IP for WSL.
    
    WSL1: Uses localhost (shares network with Windows)
    WSL2: Uses nameserver from /etc/resolv.conf
    """
    import subprocess
    
    # Check kernel version to distinguish WSL1 vs WSL2
    try:
        with open('/proc/sys/kernel/osrelease', 'r') as f:
            kernel = f.read().strip()
            # WSL2 has "WSL2" in kernel version, WSL1 has just "Microsoft"
            is_wsl2 = 'WSL2' in kernel or '-microsoft-standard' in kernel.lower()
    except:
        is_wsl2 = False
    
    if is_wsl2:
        # WSL2: Get IP from nameserver (different network namespace)
        try:
            with open('/etc/resolv.conf', 'r') as f:
                for line in f:
                    if line.startswith('nameserver'):
                        return line.split()[1]
        except:
            pass
    else:
        # WSL1: Use localhost (shared network with Windows)
        return '127.0.0.1'
    
    return None


def _test_x11_connection(host, display_num=0):
    """Test if X Server is reachable on given host.
    
    Args:
        host: IP address or hostname
        display_num: X11 display number (default 0)
    
    Returns:
        True if X Server is reachable, False otherwise
    """
    port = 6000 + display_num
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(0.5)
    
    try:
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except:
        return False


def check_and_setup():
    """Check GUI environment and setup DISPLAY if needed.
    
    Returns:
        True if environment is ready, False if setup failed
    """
    is_wsl = _is_wsl()
    
    if is_wsl:
        # WSL: Auto-configure DISPLAY
        host_ip = _get_windows_host_ip()
        if not host_ip:
            print("ERROR: Cannot detect Windows host IP from /etc/resolv.conf", file=sys.stderr)
            print("", file=sys.stderr)
            print("This should not happen in WSL. Please check your network configuration.", file=sys.stderr)
            print("Exiting...", file=sys.stderr)
            return False
        
        display = f"{host_ip}:0.0"
        os.environ['DISPLAY'] = display
        
        # Test X11 connection
        if not _test_x11_connection(host_ip, 0):
            print(f"ERROR: Cannot connect to X Server at {host_ip}:0 (port 6000)", file=sys.stderr)
            print("", file=sys.stderr)
            print("VcXsrv is either not running or not configured to accept network connections.", file=sys.stderr)
            print("", file=sys.stderr)
            print("Setup Steps:", file=sys.stderr)
            print("  1. Download VcXsrv: https://github.com/marchaesen/vcxsrv/releases", file=sys.stderr)
            print("", file=sys.stderr)
            print("  2. Fix DPI scaling (High DPI displays):", file=sys.stderr)
            print("     - Find xlaunch.exe", file=sys.stderr)
            print("     - Right-click → Properties → Compatibility", file=sys.stderr)
            print("     - Click 'Change high DPI settings'", file=sys.stderr)
            print("     - ✓ 'Override high DPI scaling behavior'", file=sys.stderr)
            print("     - Scaling performed by: 'Application' → OK", file=sys.stderr)
            print("     (This prevents blurry/oversized windows)", file=sys.stderr)
            print("", file=sys.stderr)
            print("  3. Run XLaunch.exe (NOT vcxsrv.exe directly)", file=sys.stderr)
            print("     - Display settings: Display number = 0, Multiple windows → Next", file=sys.stderr)
            print("     - Client startup: Start no client → Next", file=sys.stderr)
            print("     - Extra settings:", file=sys.stderr)
            print("       * ✓ 'Disable access control'", file=sys.stderr)
            print("       * ✗ 'Native opengl' (use software rendering for WSL)", file=sys.stderr)
            print("       → Finish", file=sys.stderr)
            print("", file=sys.stderr)
            print("  4. If Windows Firewall prompts, click 'Allow access'", file=sys.stderr)
            print("", file=sys.stderr)
            print("  5. Verify connection (run in WSL):", file=sys.stderr)
            print(f"     timeout 2 bash -c 'echo > /dev/tcp/{host_ip}/6000' && echo 'OK' || echo 'Failed'", file=sys.stderr)
            print("", file=sys.stderr)
            print("  6. Re-run this program", file=sys.stderr)
            print("", file=sys.stderr)
            print("Exiting...", file=sys.stderr)
            return False
        
        print(f"✓ WSL detected, DISPLAY set to {display}")
        return True
    
    else:
        # Native Linux: Check DISPLAY is set
        display = os.environ.get('DISPLAY')
        if not display:
            print("ERROR: DISPLAY environment variable not set", file=sys.stderr)
            print("", file=sys.stderr)
            print("Make sure you are running in a graphical environment (X11 or Wayland with XWayland).", file=sys.stderr)
            print("", file=sys.stderr)
            print("Exiting...", file=sys.stderr)
            return False
        
        print(f"✓ Native Linux detected, DISPLAY={display}")
        return True

