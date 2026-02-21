"""Tests for coordinator behavior."""

import logging
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


class TestStaleSnapshotCleanup:
    """Tests for eager stale snapshot deletion at start of archive cycle."""

    def test_stale_snapshots_deleted_before_archive(self, coordinator: Coordinator):
        """Test that stale snapshots are deleted before creating a new one."""
        # Create two stale snapshots (not locked)
        coordinator.snapshot_manager.create_snapshot()
        coordinator.snapshot_manager.create_snapshot()
        assert len(coordinator.snapshot_manager.get_snapshots()) == 2

        success_result = ArchiveResult(
            snapshot_id=3,
            state=ArchiveState.COMPLETED,
            files_transferred=0,
        )
        coordinator.archive_manager.archive_new_snapshot = MagicMock(
            return_value=success_result
        )

        coordinator._do_archive_cycle()

        # Both stale snapshots should be deleted before archive_new_snapshot was called
        # (archive_new_snapshot creates snap 3, which is then deleted post-archive)
        # At the end, no snapshots should remain
        assert len(coordinator.snapshot_manager.get_snapshots()) == 0

    def test_one_stale_snapshot_logs_warning(self, coordinator: Coordinator, caplog):
        """One stale snapshot logs a warning (likely unclean shutdown)."""
        coordinator.snapshot_manager.create_snapshot()

        success_result = ArchiveResult(
            snapshot_id=2,
            state=ArchiveState.COMPLETED,
            files_transferred=0,
        )
        coordinator.archive_manager.archive_new_snapshot = MagicMock(
            return_value=success_result
        )

        with caplog.at_level(logging.WARNING, logger="teslausb.coordinator"):
            coordinator._do_archive_cycle()

        stale_records = [r for r in caplog.records if "stale snapshot" in r.message.lower()]
        assert len(stale_records) == 1
        assert stale_records[0].levelno == logging.WARNING
        assert "unclean shutdown" in stale_records[0].message

    def test_multiple_stale_snapshots_logs_error(self, coordinator: Coordinator, caplog):
        """Two+ stale snapshots logs an error (indicates a bug)."""
        coordinator.snapshot_manager.create_snapshot()
        coordinator.snapshot_manager.create_snapshot()

        success_result = ArchiveResult(
            snapshot_id=3,
            state=ArchiveState.COMPLETED,
            files_transferred=0,
        )
        coordinator.archive_manager.archive_new_snapshot = MagicMock(
            return_value=success_result
        )

        with caplog.at_level(logging.WARNING, logger="teslausb.coordinator"):
            coordinator._do_archive_cycle()

        stale_records = [r for r in caplog.records if "stale snapshot" in r.message.lower()]
        assert len(stale_records) == 1
        assert stale_records[0].levelno == logging.ERROR
        assert "bug" in stale_records[0].message

    def test_no_log_when_zero_stale_snapshots(self, coordinator: Coordinator, caplog):
        """No stale snapshot log when there are none."""
        success_result = ArchiveResult(
            snapshot_id=1,
            state=ArchiveState.COMPLETED,
            files_transferred=0,
        )
        coordinator.archive_manager.archive_new_snapshot = MagicMock(
            return_value=success_result
        )

        with caplog.at_level(logging.WARNING, logger="teslausb.coordinator"):
            coordinator._do_archive_cycle()

        assert not any("stale snapshot" in r.message.lower() for r in caplog.records)


class TestPostArchiveSnapshotDeletion:
    """Tests for snapshot deletion after archive + cam_disk cleanup."""

    def test_snapshot_deleted_after_successful_archive(self, coordinator: Coordinator):
        """Test that the archive snapshot is deleted after a successful cycle."""
        success_result = ArchiveResult(
            snapshot_id=1,
            state=ArchiveState.COMPLETED,
            files_transferred=5,
        )
        coordinator.archive_manager.archive_new_snapshot = MagicMock(
            return_value=success_result
        )
        coordinator.snapshot_manager.delete_snapshot = MagicMock()

        coordinator._do_archive_cycle()

        coordinator.snapshot_manager.delete_snapshot.assert_called_with(1)

    def test_snapshot_deletion_failure_does_not_fail_cycle(self, coordinator: Coordinator):
        """Test that cycle succeeds even if post-archive snapshot deletion fails."""
        success_result = ArchiveResult(
            snapshot_id=1,
            state=ArchiveState.COMPLETED,
            files_transferred=5,
        )
        coordinator.archive_manager.archive_new_snapshot = MagicMock(
            return_value=success_result
        )
        coordinator.snapshot_manager.delete_snapshot = MagicMock(
            side_effect=Exception("deletion failed")
        )

        result = coordinator._do_archive_cycle()
        assert result is True

    def test_snapshot_deletion_failure_logs_warning(self, coordinator: Coordinator, caplog):
        """Test that failed post-archive deletion logs a warning."""
        success_result = ArchiveResult(
            snapshot_id=1,
            state=ArchiveState.COMPLETED,
            files_transferred=5,
        )
        coordinator.archive_manager.archive_new_snapshot = MagicMock(
            return_value=success_result
        )
        coordinator.snapshot_manager.delete_snapshot = MagicMock(
            side_effect=Exception("permission denied")
        )

        with caplog.at_level(logging.WARNING, logger="teslausb.coordinator"):
            coordinator._do_archive_cycle()

        warning_records = [r for r in caplog.records if "failed to delete snapshot" in r.message.lower()]
        assert len(warning_records) == 1
        assert "permission denied" in warning_records[0].message
        assert "will retry next cycle" in warning_records[0].message

    def test_no_deletion_when_snapshot_id_none(self, coordinator: Coordinator):
        """Test that no deletion is attempted when snapshot_id is None."""
        result_no_snap = ArchiveResult(
            snapshot_id=None,
            state=ArchiveState.COMPLETED,
            files_transferred=0,
        )
        coordinator.archive_manager.archive_new_snapshot = MagicMock(
            return_value=result_no_snap
        )
        coordinator.snapshot_manager.delete_snapshot = MagicMock()

        coordinator._do_archive_cycle()

        coordinator.snapshot_manager.delete_snapshot.assert_not_called()

    def test_snapshot_still_deleted_after_failed_archive(self, coordinator: Coordinator):
        """Test that post-archive deletion runs even for FAILED archives."""
        failed_result = ArchiveResult(
            snapshot_id=1,
            state=ArchiveState.FAILED,
            files_transferred=0,
            error="rclone connection error",
        )
        coordinator.archive_manager.archive_new_snapshot = MagicMock(
            return_value=failed_result
        )
        coordinator.snapshot_manager.delete_snapshot = MagicMock()

        coordinator._do_archive_cycle()

        coordinator.snapshot_manager.delete_snapshot.assert_called_with(1)


class TestArchiveCycleFailure:
    """Tests for archive cycle failure handling."""

    def test_returns_false_on_archive_exception(self, coordinator: Coordinator):
        """Test that archive cycle returns False when archive throws."""
        coordinator.archive_manager.archive_new_snapshot = MagicMock(
            side_effect=Exception("Simulated archive failure")
        )

        result = coordinator._do_archive_cycle()
        assert result is False


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

    def test_archive_passes_delete_after_archive_false(self, coordinator_with_gadget: Coordinator):
        """Test that archive_new_snapshot is called with delete_after_archive=False."""
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
            mount_fn=mock_mount,
            delete_after_archive=False,
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


class TestStartupCamDiskSizeCheck:
    """Tests for the cam_disk size check at startup."""

    def _run_startup_only(self, coordinator: Coordinator) -> None:
        """Run coordinator but stop immediately when it reaches the main loop."""
        # Make archive unreachable and interrupt the wait to exit cleanly
        coordinator.backend.reachable = False
        original_wait = coordinator._wait_interruptible

        def stop_on_first_wait(seconds: float) -> bool:
            coordinator.stop()
            return False

        coordinator._wait_interruptible = stop_on_first_wait
        coordinator.run()

    def test_logs_error_when_cam_disk_exceeds_half(
        self, mock_fs: MockFilesystem, snapshot_manager, space_manager,
        mock_backend, caplog,
    ):
        """Test that run() logs error when cam_disk > 50% of backing store."""
        # cam_disk.bin is 21 bytes (from fixture), total space 256 GiB
        # We need cam_disk > 50% of total, so set total space small
        mock_fs.set_total_space(20)  # 20 bytes total — cam_disk (21 bytes) > 50%

        archive_manager = ArchiveManager(
            fs=mock_fs,
            snapshot_manager=snapshot_manager,
            backend=mock_backend,
            cam_disk_path=Path("/backingfiles/cam_disk.bin"),
        )

        coordinator = Coordinator(
            fs=mock_fs,
            snapshot_manager=snapshot_manager,
            archive_manager=archive_manager,
            space_manager=space_manager,
            backend=mock_backend,
            config=CoordinatorConfig(mount_fn=mock_mount),
        )

        with caplog.at_level(logging.ERROR, logger="teslausb.coordinator"):
            self._run_startup_only(coordinator)

        assert any("exceeds 50%" in r.message for r in caplog.records)

    def test_no_error_when_cam_disk_within_budget(
        self, coordinator: Coordinator, caplog,
    ):
        """Test that no error is logged when cam_disk is within budget."""
        # Default fixture: cam_disk is 21 bytes, total space 256 GiB — well within 50%
        coordinator.archive_manager.cam_disk_path = Path("/backingfiles/cam_disk.bin")

        with caplog.at_level(logging.ERROR, logger="teslausb.coordinator"):
            self._run_startup_only(coordinator)

        assert not any("exceeds 50%" in r.message for r in caplog.records)

    def test_no_error_when_no_cam_disk(self, coordinator: Coordinator, caplog):
        """Test that no error is logged when cam_disk_path is not set."""
        # archive_manager.cam_disk_path defaults to None
        with caplog.at_level(logging.ERROR, logger="teslausb.coordinator"):
            self._run_startup_only(coordinator)

        assert not any("exceeds 50%" in r.message for r in caplog.records)
