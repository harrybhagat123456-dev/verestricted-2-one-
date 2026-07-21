# Copyright (c) 2025 devgagan : https://github.com/devgaganin.  
# Licensed under the GNU General Public License v3.0.  
# See LICENSE file in the repository root for full license text.

"""
In-memory circular log buffer with timestamps.

Captures all stdout/stderr output (print statements, unhandled exceptions, etc.)
so that the /logs command can retrieve the last N hours of bot activity.

Usage:
    from utils.log_buffer import log_buffer, start_log_capture
    start_log_capture()  # Call once at startup (in main.py)
    
    # Later, to retrieve logs:
    lines = log_buffer.get_last_hours(1)  # Last 1 hour
"""

import io
import os
import sys
import time
import threading
from collections import deque
from datetime import datetime, timedelta


class LogBuffer:
    """Thread-safe circular buffer that stores timestamped log lines in memory."""
    
    def __init__(self, max_lines=50000):
        """
        Args:
            max_lines: Maximum number of log lines to keep in memory.
                       Oldest lines are discarded when the buffer is full.
        """
        self.max_lines = max_lines
        self.buffer = deque(maxlen=max_lines)
        self._lock = threading.Lock()
        self._original_stdout = sys.stdout
        self._original_stderr = sys.stderr
        self._capturing = False
    
    def add_line(self, line: str):
        """Add a timestamped line to the buffer."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self._lock:
            self.buffer.append(f"[{timestamp}] {line}")
    
    def get_last_hours(self, hours: float = 1.0):
        """Return all log lines from the last N hours.
        
        Args:
            hours: Number of hours to look back (e.g. 1.0 = last 1 hour, 0.5 = last 30 min)
        
        Returns:
            List of log line strings.
        """
        cutoff = datetime.now() - timedelta(hours=hours)
        cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")
        
        with self._lock:
            # Optimized: since deque is time-ordered, find the cutoff point
            # and return everything from there. Binary search not possible on deque,
            # but we can skip early entries efficiently.
            result = []
            started = False
            for line in self.buffer:
                if not started:
                    # Skip lines before cutoff
                    if len(line) > 21 and line.startswith("["):
                        line_ts = line[1:20]
                        if line_ts < cutoff_str:
                            continue
                    started = True
                result.append(line)
            return list(result)
    
    def write_to_file(self, file_path: str, hours: float = None):
        """Write buffered logs directly to a file — avoids huge string in memory.
        
        Args:
            file_path: Path to write the log file
            hours: If specified, only write logs from last N hours. None = all logs.
        
        Returns:
            Tuple of (lines_written, file_size_bytes)
        """
        if hours is not None:
            cutoff = datetime.now() - timedelta(hours=hours)
            cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")
        else:
            cutoff_str = None
        
        # Snapshot the buffer while holding the lock (fast — just copy references).
        # Then write to file WITHOUT holding the lock (slow I/O doesn't block add_line
        # from other threads, preventing event loop stalls).
        with self._lock:
            lines_snapshot = list(self.buffer)
        
        lines_written = 0
        with open(file_path, 'w', encoding='utf-8') as f:
            started = False if cutoff_str else True
            for line in lines_snapshot:
                if not started:
                    if len(line) > 21 and line.startswith("["):
                        line_ts = line[1:20]
                        if line_ts < cutoff_str:
                            continue
                    started = True
                f.write(line + "\n")
                lines_written += 1
        
        file_size = os.path.getsize(file_path) if lines_written > 0 else 0
        return lines_written, file_size
    
    def get_all(self):
        """Return all buffered log lines."""
        with self._lock:
            return list(self.buffer)
    
    def clear(self):
        """Clear all buffered logs."""
        with self._lock:
            self.buffer.clear()
    
    def start_capture(self):
        """Start capturing stdout and stderr into this buffer.
        
        This replaces sys.stdout and sys.stderr with TeeWriter instances
        that write to both the original stream AND this buffer.
        """
        if self._capturing:
            return
        
        self._capturing = True
        sys.stdout = _TeeWriter(self._original_stdout, self)
        sys.stderr = _TeeWriter(self._original_stderr, self)
        print("[LOG_BUFFER] Log capture started — stdout/stderr are being recorded.")
    
    def stop_capture(self):
        """Stop capturing and restore original stdout/stderr."""
        if not self._capturing:
            return
        
        self._capturing = False
        sys.stdout = self._original_stdout
        sys.stderr = self._original_stderr
        print("[LOG_BUFFER] Log capture stopped.")
    
    @property
    def is_capturing(self):
        return self._capturing
    
    @property
    def line_count(self):
        with self._lock:
            return len(self.buffer)


class _TeeWriter(io.TextIOBase):
    """Writes to both an original stream and a LogBuffer.
    
    Handles line buffering: accumulates partial lines until a newline
    is received, then adds the complete line to the LogBuffer with a timestamp.
    """
    
    def __init__(self, original_stream, log_buffer: LogBuffer):
        self._original = original_stream
        self._buffer = log_buffer
        self._partial = ""
        self._lock = threading.Lock()
    
    def write(self, text):
        if not text:
            return 0
        
        # Always write to original stream first (so console output still works)
        try:
            self._original.write(text)
            self._original.flush()
        except Exception:
            pass
        
        # Buffer the text and emit complete lines
        with self._lock:
            self._partial += text
            while '\n' in self._partial:
                line, self._partial = self._partial.split('\n', 1)
                line = line.rstrip('\r')
                if line:  # Skip empty lines
                    self._buffer.add_line(line)
        
        return len(text)
    
    def flush(self):
        try:
            self._original.flush()
        except Exception:
            pass
    
    def fileno(self):
        return self._original.fileno()
    
    def isatty(self):
        return self._original.isatty()
    
    def readable(self):
        return False
    
    def writable(self):
        return True
    
    def seekable(self):
        return False


# Global singleton instance
log_buffer = LogBuffer(max_lines=50000)


def start_log_capture():
    """Convenience function to start log capture on the global buffer."""
    log_buffer.start_capture()
