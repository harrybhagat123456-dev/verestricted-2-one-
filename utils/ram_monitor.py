# Copyright (c) 2025 devgagan : https://github.com/devgaganin.  
# Licensed under the GNU General Public License v3.0.  
# See LICENSE file in the repository root for full license text.

"""
PERMANENT: RAM monitoring utility for Heroku console logging.

Logs detailed memory usage to console so you can monitor on Heroku.
This runs for the lifetime of the bot — NOT a test feature.

Usage:
    from utils.ram_monitor import log_ram, start_periodic_ram_log, ram_snapshot
    
    # One-time log at key moments:
    log_ram("batch_start")
    log_ram("after_prefetch", extra_info={"messages": 5000})
    
    # Periodic logging (every 60 seconds):
    await start_periodic_ram_log(interval=60)
    
    # Get a dict snapshot for programmatic use:
    info = ram_snapshot()
"""

import os
import sys
import time
import asyncio
import threading
from datetime import datetime


# ════════════════════════════════════════════════════════════
#  DYNOS — set your Heroku dyno RAM limit here
#
#  Standard-2x = 1024 MB
#  Performance-M = 2560 MB
#  Basic/eco = 512 MB
# ════════════════════════════════════════════════════════════

DYNO_RAM_LIMIT_MB = int(os.environ.get("DYNO_RAM_LIMIT_MB", "1024"))


def ram_snapshot():
    """Return a dict with current RAM usage details.
    
    Uses /proc/self/status on Linux (Heroku) for accurate RSS.
    Falls back to psutil if available.
    """
    info = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "rss_mb": 0,
        "vms_mb": 0,
        "peak_mb": 0,
        "available_mb": 0,
        "percent": 0,
        "dyno_pct": 0,       # % of dyno limit used
        "dyno_free_mb": 0,   # MB free before hitting dyno limit
    }
    
    try:
        # Method 1: /proc/self/status (Linux — most accurate for Heroku)
        if os.path.exists('/proc/self/status'):
            with open('/proc/self/status', 'r') as f:
                status = f.read()
            
            for line in status.split('\n'):
                if line.startswith('VmRSS:'):
                    # VmRSS in kB
                    info["rss_mb"] = int(line.split()[1]) / 1024
                elif line.startswith('VmSize:'):
                    # VmSize in kB
                    info["vms_mb"] = int(line.split()[1]) / 1024
                elif line.startswith('VmHWM:'):
                    # VmHWM = peak RSS in kB
                    info["peak_mb"] = int(line.split()[1]) / 1024
            
            # Get available system memory
            if os.path.exists('/proc/meminfo'):
                with open('/proc/meminfo', 'r') as f:
                    meminfo = f.read()
                for line in meminfo.split('\n'):
                    if line.startswith('MemAvailable:'):
                        info["available_mb"] = int(line.split()[1]) / 1024
                        break
                    elif line.startswith('MemTotal:'):
                        total_mb = int(line.split()[1]) / 1024
                        info["percent"] = round(info["rss_mb"] / total_mb * 100, 1) if total_mb > 0 else 0
        
        # Method 2: psutil (if available — more reliable)
        try:
            import psutil
            process = psutil.Process(os.getpid())
            mem_info = process.memory_info()
            info["rss_mb"] = round(mem_info.rss / (1024 * 1024), 1)
            info["vms_mb"] = round(mem_info.vms / (1024 * 1024), 1)
            
            # System memory
            sys_mem = psutil.virtual_memory()
            info["available_mb"] = round(sys_mem.available / (1024 * 1024), 1)
            info["percent"] = round(sys_mem.percent, 1)
            
            # Peak memory
            info["peak_mb"] = round(process.memory_info().rss / (1024 * 1024), 1)
        except ImportError:
            pass
        
        # Calculate dyno usage (against Heroku dyno limit)
        info["dyno_pct"] = round(info["rss_mb"] / DYNO_RAM_LIMIT_MB * 100, 1) if DYNO_RAM_LIMIT_MB > 0 else 0
        info["dyno_free_mb"] = round(DYNO_RAM_LIMIT_MB - info["rss_mb"], 1)
    
    except Exception as e:
        info["error"] = str(e)
    
    return info


def log_ram(label="snapshot", extra_info=None):
    """Log current RAM usage to console with a label.
    
    Shows usage out of dyno limit (e.g. 412/1024 MB) so you can
    see exactly how close you are to the Heroku memory quota.
    
    Args:
        label: A descriptive label for this log point (e.g., "batch_start", "after_prefetch")
        extra_info: Optional dict of extra context to include
    """
    info = ram_snapshot()
    
    # Build the log line
    ts = info["timestamp"]
    rss = info["rss_mb"]
    peak = info["peak_mb"]
    avail = info["available_mb"]
    pct = info["percent"]
    dyno_pct = info["dyno_pct"]
    dyno_free = info["dyno_free_mb"]
    
    # Warning indicators — based on dyno % usage
    if dyno_pct > 90:
        indicator = "🔴 CRITICAL"
    elif dyno_pct > 70:
        indicator = "🟡 HIGH"
    elif dyno_pct > 50:
        indicator = "🟠 MODERATE"
    else:
        indicator = "🟢 OK"
    
    extra_str = ""
    if extra_info:
        extra_str = " | " + " | ".join(f"{k}={v}" for k, v in extra_info.items())
    
    print(
        f"[RAM] {ts} | {indicator} | {label} | "
        f"RSS: {rss:.1f}/{DYNO_RAM_LIMIT_MB}MB ({dyno_pct}%) | "
        f"Peak: {peak:.1f}MB | Free: {dyno_free:.1f}MB | "
        f"System: {pct}%{extra_str}"
    )


async def start_periodic_ram_log(interval=60):
    """Start periodic RAM logging every `interval` seconds.
    
    Call this once after bot startup. It runs forever.
    Logs every 60 seconds by default — good for Heroku console monitoring.
    """
    print(f"[RAM] Periodic monitoring started — logging every {interval}s | Dyno limit: {DYNO_RAM_LIMIT_MB}MB")
    
    while True:
        try:
            log_ram("periodic_check")
        except Exception as e:
            print(f"[RAM] Error in periodic log: {e}")
        
        await asyncio.sleep(interval)


def log_ram_sync(label="snapshot", extra_info=None):
    """Synchronous version of log_ram for non-async contexts."""
    log_ram(label, extra_info)
