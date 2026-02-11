"""Tests for coordinator behavior."""

from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from teslausb.archive import (
    ArchivedFile,
    ArchiveManager,
    ArchiveResult,
    ArchiveState,
    MockArchiveBackend,
)
from teslausb.coordinator import Coordinator, CoordinatorConfig, _backoff_intervals
from teslausb.filesystem import MockFilesystem
from teslausb.gadget import LunConfig, MockGadget
from teslausb.snapshot import SnapshotManager
from teslausb.space import SpaceManager


@contextmanager
def mock_mount(path: Path):
    """Mock mount context manager."""
    yield Path("/mnt/mock")


@pytest.fixture
def coordinator(
    mock_fs: MockFilesystem,
    snapshot_manager: SnapshotManager,
    space_manager: SpaceManager,
    mock_backend: MockArchiveBackend,
) -> Coordinator:
    """Create a Coordinator with mock components."""
    archive_manager = ArchiveManager(
        fs=mock_fs,
        snapshot_manager=snapshot_manager,
        backend=mock_backend,
    )

    config = CoordinatorConfig(mount_fn=mock_mount)

    return Coordinator(
        fs=mock_fs,
        snapshot_manager=snapshot_manager,
        archive_manager=archive_manager,
        space_manager=space_manager,
        backend=mock_backend,
        config=config,
    )


@pytest.fixture
def mock_gadget() -> MockGadget:
    """Create an initialized and enabled mock gadget."""
    gadget = MockGadget()
    gadget.initialize({0: LunConfig(disk_path=Path("/backingfiles/cam_disk.bin"))})
    gadget.enable()
    return gadget


@pytest.fixture
def coordinator_with_gadget(
    mock_fs: MockFilesystem,
    snapshot_manager: SnapshotManager,
    space_manager: SpaceManager,
    mock_backend: MockArchiveBackend,
    mock_gadget: MockGadget,
) -> Coordinator:
    """Create a Coordinator with a mock gadget."""
    archive_manager = ArchiveManager(
        fs=mock_fs,
        snapshot_manager=snapshot_manager,
        backend=mock_backend,
        cam_disk_path=Path("/backingfiles/cam_disk.bin"),
    )

    config = CoordinatorConfig(mount_fn=mock_mount, gadget=mock_gadget)

    return Coordinator(
        fs=mock_fs,
        snapshot_manager=snapshot_manager,
        archive_manager=archive_manager,
        space_manager=space_manager,
        backend=mock_backend,
        config=config,
    )


class TestCoordinatorCleanup:
    """Tests for coordinator cleanup behavior.

    These tests verify that cleanup_if_needed is always called at the end
    of an archive cycle, regardless of success or failure. This prevents
    orphaned snapshots from accumulating and filling up space.
    """

    def test_cleanup_called_on_archive_failure(self, coordinator: Coordinator):
        """Test that cleanup is called even when archive throws an exception.

        This is the critical case - if archive fails repeatedly, we must
        still clean up or orphaned snapshots will fill the disk.
        """
        coordinator.archive_manager.archive_new_snapshot = MagicMock(
            side_effect=Exception("Simulated archive failure")
        )
        coordinator.space_manager.cleanup_if_needed = MagicMock(
            wraps=coordinator.space_manager.cleanup_if_needed
        )

        result = coordinator._do_archive_cycle()

        assert result is False, "Archive cycle should return False on failure"
        coordinator.space_manager.cleanup_if_needed.assert_called()

    def test_cleanup_called_on_archive_success(self, coordinator: Coordinator):
        """Test that cleanup is called after successful archive."""
        success_result = ArchiveResult(
            snapshot_id=1,
            state=ArchiveState.COMPLETED,
            files_transferred=10,
            bytes_transferred=10000,
        )
        coordinator.archive_manager.archive_new_snapshot = MagicMock(
            return_value=success_result
        )
        coordinator.space_manager.cleanup_if_needed = MagicMock(
            wraps=coordinator.space_manager.cleanup_if_needed
        )

        result = coordinator._do_archive_cycle()

        assert result is True, "Archive cycle should return True on success"
        coordinator.space_manager.cleanup_if_needed.assert_called()


class TestGadgetCoordination:
    """Tests for USB gadget disable/enable during cam_disk cleanup.

    The gadget must be disabled before mounting cam_disk.bin read-write
    to prevent FAT filesystem corruption from concurrent access.
    """

    def test_gadget_disabled_during_deletion(
        self, coordinator_with_gadget: Coordinator, mock_gadget: MockGadget
    ):
        """Test that gadget is disabled before deleting archived files."""
        result = ArchiveResult(
            snapshot_id=1,
            state=ArchiveState.COMPLETED,
            files_transferred=5,
            archived_files={"SavedClips": [ArchivedFile("event/front.mp4", 1000)]},
        )

        # Track gadget state during mount_image call
        gadget_enabled_during_mount = None

        @contextmanager
        def tracking_mount(path, readonly=True):
            nonlocal gadget_enabled_during_mount
            gadget_enabled_during_mount = mock_gadget.is_enabled()
            yield Path("/mnt/cam")

        with patch("teslausb.mount.mount_image", tracking_mount), \
             patch("teslausb.mount.fsck_image", return_value=True):
            coordinator_with_gadget._delete_archived_files(result)

        assert gadget_enabled_during_mount is False, \
            "Gadget should be disabled during cam_disk mount"
        assert mock_gadget.is_enabled(), "Gadget should be re-enabled after cleanup"

    def test_gadget_reenabled_after_deletion_failure(
        self, coordinator_with_gadget: Coordinator, mock_gadget: MockGadget
    ):
        """Test that gadget is re-enabled even if deletion fails."""
        result = ArchiveResult(
            snapshot_id=1,
            state=ArchiveState.COMPLETED,
            files_transferred=5,
            archived_files={"SavedClips": [ArchivedFile("event/front.mp4", 1000)]},
        )

        @contextmanager
        def failing_mount(path, readonly=True):
            raise OSError("Mount failed")
            yield  # pragma: no cover

        with patch("teslausb.mount.mount_image", failing_mount), \
             patch("teslausb.mount.fsck_image", return_value=True):
            coordinator_with_gadget._delete_archived_files(result)

        assert mock_gadget.is_enabled(), "Gadget must be re-enabled after mount failure"

    def test_deletion_skipped_if_gadget_disable_fails(
        self, coordinator_with_gadget: Coordinator, mock_gadget: MockGadget
    ):
        """Test that deletion is skipped entirely if we can't disable the gadget."""
        result = ArchiveResult(
            snapshot_id=1,
            state=ArchiveState.COMPLETED,
            files_transferred=5,
            archived_files={"SavedClips": [ArchivedFile("event/front.mp4", 1000)]},
        )

        # Make gadget.disable() fail
        mock_gadget.disable = MagicMock(side_effect=Exception("Cannot disable"))

        mount_called = False

        @contextmanager
        def tracking_mount(path, readonly=True):
            nonlocal mount_called
            mount_called = True
            yield Path("/mnt/cam")

        with patch("teslausb.mount.mount_image", tracking_mount):
            coordinator_with_gadget._delete_archived_files(result)

        assert not mount_called, "Should not mount cam_disk if gadget disable fails"

    def test_deletion_skipped_if_gadget_disable_silent_failure(
        self, coordinator_with_gadget: Coordinator, mock_gadget: MockGadget
    ):
        """Test that deletion is skipped if disable() silently fails.

        UsbGadget.disable() swallows OSError, so the coordinator must verify
        with is_enabled() after calling disable() to catch silent failures.
        """
        result = ArchiveResult(
            snapshot_id=1,
            state=ArchiveState.COMPLETED,
            files_transferred=5,
            archived_files={"SavedClips": [ArchivedFile("event/front.mp4", 1000)]},
        )

        # Make disable() appear to succeed but leave gadget enabled
        mock_gadget.disable = MagicMock()  # no-op: doesn't change _enabled

        mount_called = False

        @contextmanager
        def tracking_mount(path, readonly=True):
            nonlocal mount_called
            mount_called = True
            yield Path("/mnt/cam")

        with patch("teslausb.mount.mount_image", tracking_mount):
            coordinator_with_gadget._delete_archived_files(result)

        assert not mount_called, "Should not mount cam_disk if gadget still enabled"

    def test_no_gadget_still_works(self, coordinator: Coordinator):
        """Test that deletion works without a gadget (e.g., testing without USB)."""
        coordinator.archive_manager.cam_disk_path = Path("/backingfiles/cam_disk.bin")

        result = ArchiveResult(
            snapshot_id=1,
            state=ArchiveState.COMPLETED,
            files_transferred=5,
            archived_files={"SavedClips": [ArchivedFile("event/front.mp4", 1000)]},
        )

        mount_called = False

        @contextmanager
        def tracking_mount(path, readonly=True):
            nonlocal mount_called
            mount_called = True
            yield Path("/mnt/cam")

        with patch("teslausb.mount.mount_image", tracking_mount), \
             patch("teslausb.mount.fsck_image", return_value=True):
            coordinator._delete_archived_files(result)

        assert mount_called, "Should proceed with deletion when no gadget configured"

    def test_archive_called_with_mount_fn(self, coordinator_with_gadget: Coordinator):
        """Test that archive_new_snapshot receives the configured mount function."""
        success_result = ArchiveResult(
            snapshot_id=1,
            state=ArchiveState.COMPLETED,
            files_transferred=0,
        )
        coordinator_with_gadget.archive_manager.archive_new_snapshot = MagicMock(
            return_value=success_result
        )

        coordinator_with_gadget._do_archive_cycle()

        coordinator_with_gadget.archive_manager.archive_new_snapshot.assert_called_once_with(
            mock_mount,
        )


class TestBackoffIntervals:
    """Tests for the _backoff_intervals generator."""

    def test_yields_correct_sequence(self):
        """Test backoff progression: 5, 10, 20, 40, 80, 160, 300, 300..."""
        gen = _backoff_intervals(5.0, 300.0)
        expected = [5.0, 10.0, 20.0, 40.0, 80.0, 160.0, 300.0, 300.0, 300.0]
        for exp in expected:
            assert next(gen) == exp

    def test_caps_at_maximum(self):
        """Test that intervals never exceed maximum."""
        gen = _backoff_intervals(1.0, 10.0)
        for _ in range(20):
            assert next(gen) <= 10.0

    def test_base_equals_maximum(self):
        """Test that base == maximum yields constant intervals."""
        gen = _backoff_intervals(60.0, 60.0)
        for _ in range(5):
            assert next(gen) == 60.0

    def test_base_exceeds_maximum(self):
        """Test that base > maximum is capped at maximum from the start."""
        gen = _backoff_intervals(100.0, 50.0)
        assert next(gen) == 50.0
        assert next(gen) == 50.0

    def test_rejects_non_positive_base(self):
        """Test that base <= 0 raises ValueError."""
        with pytest.raises(ValueError, match="base"):
            next(_backoff_intervals(0, 10.0))
        with pytest.raises(ValueError, match="base"):
            next(_backoff_intervals(-1.0, 10.0))

    def test_rejects_non_positive_maximum(self):
        """Test that maximum <= 0 raises ValueError."""
        with pytest.raises(ValueError, match="maximum"):
            next(_backoff_intervals(1.0, 0))
        with pytest.raises(ValueError, match="maximum"):
            next(_backoff_intervals(1.0, -5.0))


class TestWaitForArchiveReachable:
    """Tests for _wait_for_archive_reachable with exponential backoff."""

    def test_returns_immediately_when_reachable(self, coordinator: Coordinator):
        """Test that no wait occurs if archive is immediately reachable."""
        coordinator.backend.reachable = True
        assert coordinator._wait_for_archive_reachable() is True

    def test_backoff_increases_on_consecutive_failures(self, coordinator: Coordinator):
        """Test that wait intervals increase when archive stays unreachable."""
        wait_intervals: list[float] = []

        # Unreachable 4 times, then reachable
        coordinator.backend.is_reachable = MagicMock(
            side_effect=[False, False, False, False, True]
        )

        original_wait = coordinator._wait_interruptible

        def tracking_wait(seconds: float) -> bool:
            wait_intervals.append(seconds)
            return original_wait(0)  # don't actually sleep

        coordinator._wait_interruptible = tracking_wait

        assert coordinator._wait_for_archive_reachable() is True
        assert wait_intervals == [5.0, 10.0, 20.0, 40.0]

    def test_returns_false_on_stop(self, coordinator: Coordinator):
        """Test that stop event interrupts the wait."""
        coordinator.backend.reachable = False
        coordinator._stop_event.set()
        assert coordinator._wait_for_archive_reachable() is False


class TestRunLoopBackoff:
    """Tests for the run() loop's idle backoff behavior."""

    def _run_with_results(
        self, coordinator: Coordinator, results: list[ArchiveResult]
    ) -> list[float]:
        """Run the coordinator loop with canned ArchiveResults, returning wait delays.

        Each result is returned by a fake _do_archive_cycle (always returns True).
        The coordinator stops after the last result is consumed.
        """
        wait_delays: list[float] = []
        cycle = 0

        def fake_archive_cycle() -> bool:
            nonlocal cycle
            coordinator._last_archive = results[cycle]
            cycle += 1
            if cycle >= len(results):
                coordinator.stop()
            return True

        coordinator._do_archive_cycle = fake_archive_cycle
        coordinator.backend.reachable = True

        original_wait = coordinator._wait_interruptible

        def tracking_wait(seconds: float) -> bool:
            wait_delays.append(seconds)
            return original_wait(0)

        coordinator._wait_interruptible = tracking_wait

        coordinator.run()
        return wait_delays

    def test_idle_backoff_increases_then_resets(self, coordinator: Coordinator):
        """Test that run() backs off on empty cycles and resets when files transfer."""
        results = [
            ArchiveResult(snapshot_id=1, state=ArchiveState.COMPLETED, files_transferred=0),
            ArchiveResult(snapshot_id=2, state=ArchiveState.COMPLETED, files_transferred=0),
            ArchiveResult(snapshot_id=3, state=ArchiveState.COMPLETED, files_transferred=0),
            ArchiveResult(snapshot_id=4, state=ArchiveState.COMPLETED, files_transferred=5),
        ]

        wait_delays = self._run_with_results(coordinator, results)

        # First 3 waits are backoff (5, 10, 20), 4th resets to poll_interval (5)
        assert wait_delays == [5.0, 10.0, 20.0, 5.0]

    def test_no_backoff_on_failed_archive_result(self, coordinator: Coordinator):
        """Test that a FAILED ArchiveResult resets backoff instead of escalating it."""
        results = [
            ArchiveResult(snapshot_id=1, state=ArchiveState.COMPLETED, files_transferred=0),
            ArchiveResult(snapshot_id=2, state=ArchiveState.FAILED, files_transferred=0),
            ArchiveResult(snapshot_id=3, state=ArchiveState.COMPLETED, files_transferred=0),
        ]

        wait_delays = self._run_with_results(coordinator, results)

        # Cycle 1: success + 0 files -> backoff 5s
        # Cycle 2: FAILED + 0 files -> not success, so reset backoff -> poll_interval 5s
        # Cycle 3: success + 0 files -> fresh backoff (reset by cycle 2) -> 5s
        assert wait_delays == [5.0, 5.0, 5.0]
