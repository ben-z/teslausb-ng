"""Tests for idle detection."""

from threading import Event
from unittest.mock import patch

import pytest

from teslausb.idle import (
    IDLE_CONFIRM_SECONDS,
    IdleState,
    IdleStatus,
    MockIdleDetector,
    ProcIdleDetector,
)


class TestIdleStatus:
    """Tests for IdleStatus dataclass."""

    def test_default_values(self):
        """Test default status values."""
        status = IdleStatus(state=IdleState.UNDETERMINED)

        assert status.state == IdleState.UNDETERMINED
        assert status.bytes_written == 0
        assert status.burst_size == 0
        assert status.idle_seconds == 0

    def test_custom_values(self):
        """Test status with custom values."""
        status = IdleStatus(
            state=IdleState.WRITING,
            bytes_written=1000000,
            burst_size=500000,
            idle_seconds=0,
        )

        assert status.state == IdleState.WRITING
        assert status.bytes_written == 1000000
        assert status.burst_size == 500000


class TestMockIdleDetector:
    """Tests for MockIdleDetector."""

    def test_default_always_idle(self):
        """Test mock detector defaults to always idle."""
        detector = MockIdleDetector()

        result = detector.wait_for_idle(timeout=1)

        assert result is True
        assert detector.wait_count == 1

    def test_always_idle_true(self):
        """Test mock detector with always_idle=True."""
        detector = MockIdleDetector(always_idle=True)

        result = detector.wait_for_idle()
        status = detector.get_status()

        assert result is True
        assert status.state == IdleState.IDLE

    def test_always_idle_false(self):
        """Test mock detector with always_idle=False (timeout)."""
        detector = MockIdleDetector(always_idle=False)

        result = detector.wait_for_idle(timeout=1)
        status = detector.get_status()

        assert result is False
        assert status.state == IdleState.WRITING

    def test_wait_count_increments(self):
        """Test that wait_count increments on each call."""
        detector = MockIdleDetector()

        detector.wait_for_idle()
        detector.wait_for_idle()
        detector.wait_for_idle()

        assert detector.wait_count == 3


class TestProcIdleDetector:
    """Tests for ProcIdleDetector."""

    def test_init(self, tmp_path):
        """Test detector initialization."""
        detector = ProcIdleDetector(proc_path=tmp_path)

        assert detector.proc_path == tmp_path
        assert detector.process_name == "file-storage"

    def test_find_process_pid_not_found(self, tmp_path):
        """Test finding PID when process doesn't exist."""
        detector = ProcIdleDetector(proc_path=tmp_path)

        pid = detector._find_process_pid()

        assert pid is None

    def test_find_process_pid_found(self, tmp_path):
        """Test finding PID when process exists."""
        # Create fake proc entry
        proc_dir = tmp_path / "1234"
        proc_dir.mkdir()
        (proc_dir / "comm").write_text("file-storage\n")

        detector = ProcIdleDetector(proc_path=tmp_path)

        pid = detector._find_process_pid()

        assert pid == 1234

    def test_get_write_bytes(self, tmp_path):
        """Test reading write_bytes from proc."""
        proc_dir = tmp_path / "1234"
        proc_dir.mkdir()
        (proc_dir / "io").write_text(
            "read_chars: 12345\n"
            "write_chars: 67890\n"
            "read_bytes: 1000\n"
            "write_bytes: 2000\n"
        )

        detector = ProcIdleDetector(proc_path=tmp_path)

        write_bytes = detector._get_write_bytes(1234)

        assert write_bytes == 2000

    def test_get_write_bytes_not_found(self, tmp_path):
        """Test reading write_bytes when file doesn't exist."""
        detector = ProcIdleDetector(proc_path=tmp_path)

        write_bytes = detector._get_write_bytes(9999)

        assert write_bytes is None

    def test_wait_for_idle_no_process(self, tmp_path):
        """Test wait_for_idle when no mass storage process."""
        detector = ProcIdleDetector(proc_path=tmp_path)

        result = detector.wait_for_idle(timeout=2)

        assert result is True  # No process = idle

    def test_undetermined_transitions_to_idle_when_quiet(self, tmp_path):
        """Test UNDETERMINED->IDLE when no significant writes are detected.

        Previously, UNDETERMINED only transitioned to WRITING (on high delta),
        so a quiet car would waste the full 90s timeout every cycle.
        """
        proc_dir = tmp_path / "1234"
        proc_dir.mkdir()
        (proc_dir / "comm").write_text("file-storage\n")

        # write_bytes barely changes (100 bytes/sec, well below 500KB threshold)
        call_count = 0

        def fake_get_write_bytes(pid):
            nonlocal call_count
            call_count += 1
            return call_count * 100

        detector = ProcIdleDetector(proc_path=tmp_path)

        with patch.object(detector, "_get_write_bytes", fake_get_write_bytes), \
             patch("teslausb.idle.time.sleep"):
            result = detector.wait_for_idle(timeout=30)

        assert result is True
        assert detector._state == IdleState.IDLE
        # First call establishes baseline, then IDLE_CONFIRM_SECONDS quiet samples needed
        assert call_count == IDLE_CONFIRM_SECONDS + 1

    def test_undetermined_transitions_to_writing_on_high_delta(self, tmp_path):
        """Test UNDETERMINED->WRITING->IDLE: big write then quiet settles to idle."""
        proc_dir = tmp_path / "1234"
        proc_dir.mkdir()
        (proc_dir / "comm").write_text("file-storage\n")

        # Baseline, then quiet, then a big write (800KB jump), then quiet to settle
        write_values = iter([0, 100, 200, 1_000_000, 1_000_100, 1_000_200,
                            1_000_300, 1_000_400, 1_000_500, 1_000_600])

        detector = ProcIdleDetector(proc_path=tmp_path)

        with patch.object(detector, "_get_write_bytes", lambda pid: next(write_values)), \
             patch("teslausb.idle.time.sleep"):
            result = detector.wait_for_idle(timeout=30)

        assert result is True
        assert detector._state == IdleState.IDLE

    def test_stop_event_interrupts_wait(self, tmp_path):
        """Test that setting stop_event causes wait_for_idle to return False promptly."""
        proc_dir = tmp_path / "1234"
        proc_dir.mkdir()
        (proc_dir / "comm").write_text("file-storage\n")

        stop = Event()
        stop.set()  # Already signaled — should return False on first iteration

        detector = ProcIdleDetector(proc_path=tmp_path, stop_event=stop)
        result = detector.wait_for_idle(timeout=30)

        assert result is False

    def test_no_stop_event_still_works(self, tmp_path):
        """Test that ProcIdleDetector works without a stop_event (backwards compat)."""
        detector = ProcIdleDetector(proc_path=tmp_path)

        # No process → immediate idle, even without stop_event
        result = detector.wait_for_idle(timeout=2)
        assert result is True

    def test_get_status_initial(self, tmp_path):
        """Test initial status."""
        detector = ProcIdleDetector(proc_path=tmp_path)

        status = detector.get_status()

        assert status.state == IdleState.UNDETERMINED
        assert status.bytes_written == 0
