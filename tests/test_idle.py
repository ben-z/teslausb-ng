"""Tests for elapsed-time idle detection without filesystem or wall-clock waits."""

from pathlib import Path
from threading import Event
from unittest.mock import patch

import pytest

from teslausb.filesystem import MockFilesystem
from teslausb.idle import IdleConfig, IdleState, IdleStatus, MockIdleDetector, ProcIdleDetector


@pytest.fixture
def detector():
    fs = MockFilesystem()
    fs.mkdir(Path("/proc/1234"), parents=True)
    fs.write_text(Path("/proc/1234/comm"), "file-storage\n")
    fs.write_text(Path("/proc/1234/io"), "wchar: 2000\n")
    return ProcIdleDetector(fs, IdleConfig())


class Clock:
    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


@pytest.fixture
def clock():
    clock = Clock()
    with (
        patch("teslausb.idle.time.monotonic", clock.monotonic),
        patch("teslausb.idle.time.sleep", clock.sleep),
    ):
        yield clock


def test_status_defaults():
    status = IdleStatus(IdleState.UNDETERMINED)
    assert status.bytes_written == status.burst_size == status.idle_seconds == 0


@pytest.mark.parametrize("always_idle", [True, False])
def test_mock_idle(always_idle):
    detector = MockIdleDetector(always_idle=always_idle)
    assert detector.wait_for_idle(1) is always_idle
    assert detector.wait_count == 1
    assert detector.get_status().state == (IdleState.IDLE if always_idle else IdleState.WRITING)


def test_process_discovery(detector):
    assert detector._find_process_pid() == 1234
    assert detector._get_write_bytes(1234) == 2000


def test_missing_process_is_idle(detector, clock):
    detector.fs.remove(Path("/proc/1234/comm"))
    assert detector.wait_for_idle(2)


def test_missing_counter_fails(detector):
    detector.fs.write_text(Path("/proc/1234/io"), "read_bytes: 2000\n")
    with pytest.raises(RuntimeError, match="Missing wchar"):
        detector._get_write_bytes(1234)


def test_quiet_process_requires_real_elapsed_time(detector, clock):
    detector.config.poll_interval = 0.25
    assert detector.wait_for_idle(6)
    assert clock.now == 5.25
    assert detector.get_status().state == IdleState.IDLE
    assert detector.get_status().idle_seconds == 5


def test_even_small_metadata_writes_prevent_idle(detector, clock):
    writes = iter(range(100))
    with patch.object(detector, "_get_write_bytes", side_effect=lambda _: next(writes)):
        assert not detector.wait_for_idle(10)
    assert detector.get_status().state == IdleState.WRITING


def test_write_burst_resets_quiet_interval(detector, clock):
    with patch.object(detector, "_get_write_bytes", side_effect=[0, 0, 1, 1, 1, 1, 1, 1]):
        assert detector.wait_for_idle(9)
    assert clock.now == 8


def test_counter_reset_restarts_confirmation(detector, clock):
    with patch.object(detector, "_get_write_bytes", side_effect=[10, 10, 0, 0, 0, 0, 0, 0]):
        assert detector.wait_for_idle(9)
    assert clock.now == 8


def test_stop_event_interrupts_wait(detector):
    detector.stop_event = Event()
    detector.stop_event.set()
    assert not detector.wait_for_idle(30)


@pytest.mark.parametrize("field", ["poll_interval", "idle_confirm_seconds"])
def test_invalid_timing_rejected(field):
    with pytest.raises(ValueError, match="must be positive"):
        IdleConfig(**{field: 0})


def test_buffered_writes_block_idle_even_when_dirty_page_counter_is_constant(detector, clock):
    read_text = detector.fs.read_text
    writes = iter(range(100))

    def proc_text(path):
        if path.name == "io":
            return f"wchar: {next(writes)}\nwrite_bytes: 4096\n"
        return read_text(path)

    with patch.object(detector.fs, "read_text", side_effect=proc_text):
        assert not detector.wait_for_idle(10)
    assert detector.get_status().state == IdleState.WRITING
