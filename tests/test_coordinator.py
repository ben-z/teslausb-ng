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
from teslausb.coordinator import Coordinator, CoordinatorConfig
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


class TestArchiveBackoff:
    """Tests for exponential backoff when nothing to archive."""

    def test_consecutive_empty_resets_on_transfer(self, coordinator: Coordinator):
        """Test that consecutive empty counter resets when files are transferred."""
        coordinator._consecutive_empty = 5

        result_with_files = ArchiveResult(
            snapshot_id=1,
            state=ArchiveState.COMPLETED,
            files_transferred=3,
        )
        coordinator._last_archive = result_with_files

        # Simulate what run() does
        if coordinator._last_archive and coordinator._last_archive.files_transferred == 0:
            coordinator._consecutive_empty += 1
        else:
            coordinator._consecutive_empty = 0

        assert coordinator._consecutive_empty == 0

    def test_consecutive_empty_increments_on_zero_files(self, coordinator: Coordinator):
        """Test that consecutive empty counter increments when no files transferred."""
        assert coordinator._consecutive_empty == 0

        empty_result = ArchiveResult(
            snapshot_id=1,
            state=ArchiveState.COMPLETED,
            files_transferred=0,
        )
        coordinator._last_archive = empty_result

        # Simulate what run() does
        if coordinator._last_archive and coordinator._last_archive.files_transferred == 0:
            coordinator._consecutive_empty += 1

        assert coordinator._consecutive_empty == 1

    def test_backoff_capped_at_max(self, coordinator: Coordinator):
        """Test that backoff is capped at max_idle_interval."""
        coordinator._consecutive_empty = 100
        max_interval = coordinator.config.max_idle_interval
        poll = coordinator.config.poll_interval

        backoff = min(poll * (2 ** 100), max_interval)
        assert backoff == max_interval

    def test_backoff_grows_exponentially(self, coordinator: Coordinator):
        """Test backoff progression: 10, 20, 40, 80, 160, 300, 300..."""
        poll = coordinator.config.poll_interval  # 5.0
        max_interval = coordinator.config.max_idle_interval  # 300.0

        expected = [10, 20, 40, 80, 160, 300, 300]
        for i, expected_backoff in enumerate(expected):
            consecutive = i + 1
            backoff = min(poll * (2 ** consecutive), max_interval)
            assert backoff == expected_backoff
