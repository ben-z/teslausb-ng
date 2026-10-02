"""Idle detection for TeslaUSB.

This module monitors USB mass storage I/O to detect when the car
has stopped writing (is idle). This prevents taking snapshots
while the car is actively recording.

The detection uses a state machine:
- UNDETERMINED: Initial state, waiting for first write
- WRITING: Active writes detected (>500KB/sec)
- IDLE: No significant writes for 5 seconds
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from threading import Event
from typing import Protocol

logger = logging.getLogger(__name__)

# Threshold for "active writing" in bytes per second
WRITE_THRESHOLD = 500_000  # 500KB

# Seconds of idle before declaring idle
IDLE_CONFIRM_SECONDS = 5

# Maximum wait time for idle
DEFAULT_TIMEOUT = 90


class IdleState(Enum):
    """State of idle detection."""

    UNDETERMINED = "undetermined"
    WRITING = "writing"
    IDLE = "idle"


@dataclass
class IdleStatus:
    """Current idle detection status."""

    state: IdleState
    bytes_written: int = 0
    burst_size: int = 0
    idle_seconds: int = 0


class IdleDetector(Protocol):
    """Protocol for idle detection."""

    def wait_for_idle(self, timeout: float = DEFAULT_TIMEOUT) -> bool:
        """Wait for the car to become idle.

        Args:
            timeout: Maximum seconds to wait

        Returns:
            True if idle detected, False if timeout
        """
        ...

    def get_status(self) -> IdleStatus:
        """Get current idle status."""
        ...


class ProcIdleDetector:
    """Idle detector using /proc filesystem.

    Monitors /proc/{pid}/io for the file-storage process to detect
    when the car has stopped writing to the USB mass storage device.
    """

    def __init__(
        self,
        proc_path: Path = Path("/proc"),
        process_name: str = "file-storage",
        stop_event: Event | None = None,
    ):
        """Initialize the idle detector.

        Args:
            proc_path: Path to /proc filesystem
            process_name: Name of the mass storage process to monitor
            stop_event: Optional threading event for interruptible waits.
                        When set, wait_for_idle returns False immediately.
        """
        self.proc_path = proc_path
        self.process_name = process_name
        self.stop_event = stop_event
        self._state = IdleState.UNDETERMINED
        self._prev_written = -1
        self._burst_size = 0
        self._idle_count = 0

    def _find_process_pid(self) -> int | None:
        """Find PID of the mass storage process.

        Returns:
            Process ID if found, None otherwise
        """
        for proc_dir in self.proc_path.iterdir():
            if not proc_dir.name.isdigit():
                continue

            try:
                comm_file = proc_dir / "comm"
                if comm_file.exists():
                    comm = comm_file.read_text().strip()
                    if comm == self.process_name:
                        return int(proc_dir.name)
            except (PermissionError, FileNotFoundError, ProcessLookupError):
                continue

        return None

    def _get_write_bytes(self, pid: int) -> int | None:
        """Get write_bytes from /proc/{pid}/io.

        Args:
            pid: Process ID

        Returns:
            Bytes written, or None if unavailable
        """
        io_path = self.proc_path / str(pid) / "io"
        try:
            content = io_path.read_text()
            match = re.search(r"write_bytes:\s*(\d+)", content)
            if match:
                return int(match.group(1))
        except (PermissionError, FileNotFoundError, ProcessLookupError):
            pass
        return None

    def wait_for_idle(self, timeout: float = DEFAULT_TIMEOUT) -> bool:
        """Wait for the car to become idle.

        Uses a state machine with three states:
        - UNDETERMINED: Initial, waiting for baseline sample
        - WRITING: Active writes detected (>500KB/sec)
        - IDLE: Below threshold; confirmed after IDLE_CONFIRM_SECONDS quiet samples

        UNDETERMINED and IDLE share identical transition logic: accumulate
        quiet samples toward the confirmation threshold, or enter WRITING
        on a large delta.

        Args:
            timeout: Maximum seconds to wait

        Returns:
            True if idle detected, False if timeout
        """
        self._state = IdleState.UNDETERMINED
        self._prev_written = -1
        self._burst_size = 0
        self._idle_count = 0

        logger.info(f"Waiting up to {timeout:.0f} seconds for idle")

        start_time = time.monotonic()
        while (time.monotonic() - start_time) < timeout:
            if self.stop_event:
                if self.stop_event.wait(timeout=1):
                    logger.info("Stop requested, aborting idle wait")
                    return False
            else:
                time.sleep(1)

            pid = self._find_process_pid()
            if pid is None:
                logger.info("Mass storage process not active, OK to proceed")
                self._state = IdleState.IDLE
                return True

            written = self._get_write_bytes(pid)
            if written is None:
                continue

            if self._prev_written < 0:
                self._prev_written = written
                continue

            delta = written - self._prev_written
            self._prev_written = written

            if self._state == IdleState.WRITING:
                if delta < WRITE_THRESHOLD:
                    logger.info(f"No longer writing, wrote {self._burst_size} bytes")
                    self._state = IdleState.IDLE
                    self._burst_size = 0
                    self._idle_count = 0
                else:
                    self._burst_size += delta

            else:
                # UNDETERMINED and IDLE share the same transition logic:
                # accumulate quiet samples, return on confirmation threshold,
                # or enter WRITING on a big delta.
                if delta > WRITE_THRESHOLD:
                    logger.info("Write in progress")
                    self._state = IdleState.WRITING
                    self._burst_size = delta
                    self._idle_count = 0
                else:
                    self._idle_count += 1
                    if self._idle_count >= IDLE_CONFIRM_SECONDS:
                        logger.info(
                            f"No writes seen in the last {IDLE_CONFIRM_SECONDS} seconds"
                        )
                        self._state = IdleState.IDLE
                        return True

        logger.warning("Couldn't determine idle interval")
        return False

    def get_status(self) -> IdleStatus:
        """Get current idle status."""
        return IdleStatus(
            state=self._state,
            bytes_written=self._prev_written if self._prev_written >= 0 else 0,
            burst_size=self._burst_size,
            idle_seconds=self._idle_count,
        )


class MockIdleDetector:
    """Mock idle detector for testing."""

    def __init__(self, always_idle: bool = True, wait_seconds: float = 0):
        """Initialize mock detector.

        Args:
            always_idle: If True, wait_for_idle always succeeds
            wait_seconds: Simulated wait time before returning
        """
        self.always_idle = always_idle
        self.wait_seconds = wait_seconds
        self._state = IdleState.IDLE if always_idle else IdleState.WRITING
        self.wait_count = 0

    def wait_for_idle(self, timeout: float = DEFAULT_TIMEOUT) -> bool:
        """Simulate waiting for idle."""
        self.wait_count += 1
        if self.wait_seconds > 0:
            time.sleep(min(self.wait_seconds, timeout))
        self._state = IdleState.IDLE if self.always_idle else IdleState.WRITING
        return self.always_idle

    def get_status(self) -> IdleStatus:
        """Get mock status."""
        return IdleStatus(state=self._state)
