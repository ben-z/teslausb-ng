"""Tests for coordinator cleanup behavior."""

from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from teslausb.archive import ArchiveManager, ArchiveResult, ArchiveState, MockArchiveBackend
from teslausb.coordinator import Coordinator, CoordinatorConfig
from teslausb.filesystem import MockFilesystem
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
        # Mock archive_new_snapshot to raise an exception
        coordinator.archive_manager.archive_new_snapshot = MagicMock(
            side_effect=Exception("Simulated archive failure")
        )

        # Track cleanup calls
        cleanup_called = False
        original_cleanup = coordinator.space_manager.cleanup_if_needed

        def tracking_cleanup():
            nonlocal cleanup_called
            cleanup_called = True
            return original_cleanup()

        coordinator.space_manager.cleanup_if_needed = tracking_cleanup

        # Run archive cycle - should return False but still clean up
        result = coordinator._do_archive_cycle()

        assert result is False, "Archive cycle should return False on failure"
        assert cleanup_called, "Cleanup should be called after archive failure"

    def test_cleanup_called_on_archive_success(self, coordinator: Coordinator):
        """Test that cleanup is called after successful archive."""
        # Mock archive_new_snapshot to return success
        success_result = ArchiveResult(
            snapshot_id=1,
            state=ArchiveState.COMPLETED,
            files_transferred=10,
            bytes_transferred=10000,
        )
        coordinator.archive_manager.archive_new_snapshot = MagicMock(
            return_value=success_result
        )

        # Track cleanup calls
        cleanup_called = False
        original_cleanup = coordinator.space_manager.cleanup_if_needed

        def tracking_cleanup():
            nonlocal cleanup_called
            cleanup_called = True
            return original_cleanup()

        coordinator.space_manager.cleanup_if_needed = tracking_cleanup

        # Run archive cycle
        result = coordinator._do_archive_cycle()

        assert result is True, "Archive cycle should return True on success"
        assert cleanup_called, "Cleanup should be called after archive success"
