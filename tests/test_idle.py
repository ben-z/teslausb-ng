"""Tests for idle detection."""

import pytest

from teslausb.idle import (
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

    def test_get_status_initial(self, tmp_path):
        """Test initial status."""
        detector = ProcIdleDetector(proc_path=tmp_path)

        status = detector.get_status()

        assert status.state == IdleState.UNDETERMINED
        assert status.bytes_written == 0
