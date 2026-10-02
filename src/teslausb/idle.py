"""Detect a quiet interval in the USB mass storage process's write counter."""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from threading import Event
from typing import Protocol

from .filesystem import FileNotFoundError_, Filesystem

logger = logging.getLogger(__name__)


@dataclass
class IdleConfig:
    """Timing and process identity for idle detection."""

    proc_path: Path = Path("/proc")
    process_name: str = "file-storage"
    idle_confirm_seconds: float = 5.0
    poll_interval: float = 1.0

    def __post_init__(self) -> None:
        if self.idle_confirm_seconds <= 0 or self.poll_interval <= 0:
            raise ValueError("Idle confirmation and polling intervals must be positive")


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
    idle_seconds: float = 0.0


class IdleDetector(Protocol):
    """Protocol for idle detection."""

    def wait_for_idle(self, timeout: float) -> bool:
        """Return True after a quiet interval, False on timeout or a stop request."""
        ...

    def get_status(self) -> IdleStatus:
        """Get current idle status."""
        ...


class ProcIdleDetector:
    """Observe logical writes, including metadata rewritten in already-dirty pages.

    The /proc wchar counter counts writes even when write_bytes does not increase
    because the same dirty filesystem page was updated again.
    """

    def __init__(self, fs: Filesystem, config: IdleConfig, stop_event: Event | None = None):
        self.fs = fs
        self.config = config
        self.stop_event = stop_event
        self._status = IdleStatus(IdleState.UNDETERMINED)

    def _find_process_pid(self) -> int | None:
        for name in self.fs.listdir(self.config.proc_path):
            if not name.isdigit():
                continue
            try:
                comm = self.fs.read_text(self.config.proc_path / name / "comm").strip()
            except FileNotFoundError_:
                continue  # Processes can exit between listing /proc and reading comm.
            if comm == self.config.process_name:
                return int(name)
        return None

    def _get_write_bytes(self, pid: int) -> int:
        content = self.fs.read_text(self.config.proc_path / str(pid) / "io")
        match = re.search(r"^wchar:\s*(\d+)$", content, re.MULTILINE)
        if match is None:
            raise RuntimeError(f"Missing wchar counter for mass storage process {pid}")
        return int(match.group(1))

    def wait_for_idle(self, timeout: float) -> bool:
        self._status = IdleStatus(IdleState.UNDETERMINED)
        deadline = time.monotonic() + timeout
        previous_pid: int | None = None
        previous_written: int | None = None
        quiet_since = time.monotonic()

        while time.monotonic() < deadline:
            delay = min(self.config.poll_interval, deadline - time.monotonic())
            if self.stop_event is not None:
                if self.stop_event.wait(max(0, delay)):
                    return False
            else:
                time.sleep(max(0, delay))

            pid = self._find_process_pid()
            if pid is None:
                self._status.state = IdleState.IDLE
                return True
            try:
                written = self._get_write_bytes(pid)
            except FileNotFoundError_:
                previous_pid = None
                previous_written = None
                self._status = IdleStatus(IdleState.UNDETERMINED)
                continue

            now = time.monotonic()
            self._status.bytes_written = written
            if pid != previous_pid or previous_written is None or written < previous_written:
                quiet_since = now
                self._status.state = IdleState.UNDETERMINED
                self._status.idle_seconds = 0
            elif written != previous_written:
                quiet_since = now
                self._status.state = IdleState.WRITING
                self._status.burst_size += written - previous_written
                self._status.idle_seconds = 0
            else:
                self._status.idle_seconds = now - quiet_since
                if self._status.idle_seconds >= self.config.idle_confirm_seconds:
                    self._status.state = IdleState.IDLE
                    logger.info("No disk writes for %.1f seconds", self._status.idle_seconds)
                    return True
            previous_pid = pid
            previous_written = written

        logger.warning("No confirmed idle interval within %.1f seconds", timeout)
        return False

    def get_status(self) -> IdleStatus:
        return self._status


class MockIdleDetector:
    """Mock idle detector for testing."""

    def __init__(self, always_idle: bool = True, wait_seconds: float = 0):
        self.always_idle = always_idle
        self.wait_seconds = wait_seconds
        self.wait_count = 0

    def wait_for_idle(self, timeout: float) -> bool:
        self.wait_count += 1
        if self.wait_seconds > 0:
            time.sleep(min(self.wait_seconds, timeout))
        return self.always_idle

    def get_status(self) -> IdleStatus:
        return IdleStatus(IdleState.IDLE if self.always_idle else IdleState.WRITING)
