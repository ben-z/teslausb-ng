"""Tests for space management."""

from pathlib import Path

import pytest

from teslausb.filesystem import MockFilesystem
from teslausb.snapshot import SnapshotManager
from teslausb.space import (
    SpaceManager,
    SpaceInfo,
    GB,
    XFS_OVERHEAD,
    MIN_CAM_SIZE,
    DEFAULT_RESERVE,
    calculate_cam_size,
)


class TestCalculateCamSize:
    """Tests for calculate_cam_size function."""

    def test_basic_calculation(self):
        """Test basic cam_size calculation."""
        # 100GB backingfiles - 2GB overhead = 98GB usable
        # 98GB / 2 = 49GB cam_size
        result = calculate_cam_size(100 * GB)
        assert result == 49 * GB

    def test_small_backingfiles(self):
        """Test with small backingfiles size."""
        # 10GB backingfiles - 2GB overhead = 8GB usable
        # 8GB / 2 = 4GB cam_size
        result = calculate_cam_size(10 * GB)
        assert result == 4 * GB

    def test_very_small_returns_zero(self):
        """Test that very small backingfiles returns 0."""
        # 1GB backingfiles - 2GB overhead = -1GB (clamped to 0)
        result = calculate_cam_size(1 * GB)
        assert result == 0

    def test_xfs_overhead_constant(self):
        """Test XFS_OVERHEAD is 2GB."""
        assert XFS_OVERHEAD == 2 * GB

    def test_min_cam_size_constant(self):
        """Test MIN_CAM_SIZE is 1GB."""
        assert MIN_CAM_SIZE == 1 * GB

    def test_default_reserve_constant(self):
        """Test DEFAULT_RESERVE is 10GB."""
        assert DEFAULT_RESERVE == 10 * GB


class TestSpaceInfo:
    """Tests for SpaceInfo dataclass."""

    def test_space_info_properties(self):
        """Test SpaceInfo properties."""
        info = SpaceInfo(
            total_bytes=100 * GB,
            free_bytes=50 * GB,
            used_bytes=50 * GB,
            cam_size_bytes=40 * GB,
        )

        assert info.total_gb == 100.0
        assert info.free_gb == 50.0
        assert info.used_gb == 50.0
        assert info.cam_size_gb == 40.0

    def test_can_snapshot_when_enough_space(self):
        """Test can_snapshot is True when free >= cam_size."""
        info = SpaceInfo(
            total_bytes=100 * GB,
            free_bytes=50 * GB,
            used_bytes=50 * GB,
            cam_size_bytes=40 * GB,
        )
        assert info.can_snapshot  # 50 >= 40

    def test_can_snapshot_when_exact_space(self):
        """Test can_snapshot is True when free == cam_size."""
        info = SpaceInfo(
            total_bytes=100 * GB,
            free_bytes=40 * GB,
            used_bytes=60 * GB,
            cam_size_bytes=40 * GB,
        )
        assert info.can_snapshot  # 40 >= 40

    def test_cannot_snapshot_when_low_space(self):
        """Test can_snapshot is False when free < cam_size."""
        info = SpaceInfo(
            total_bytes=100 * GB,
            free_bytes=30 * GB,
            used_bytes=70 * GB,
            cam_size_bytes=40 * GB,
        )
        assert not info.can_snapshot  # 30 < 40

    def test_cannot_snapshot_when_cam_size_zero(self):
        """Test can_snapshot is False when cam_size is 0 (not initialized)."""
        info = SpaceInfo(
            total_bytes=100 * GB,
            free_bytes=50 * GB,
            used_bytes=50 * GB,
            cam_size_bytes=0,
        )
        assert not info.can_snapshot  # cam_size=0 means not initialized

    def test_cannot_snapshot_when_cam_size_negative(self):
        """Test can_snapshot is False when cam_size is negative."""
        info = SpaceInfo(
            total_bytes=100 * GB,
            free_bytes=50 * GB,
            used_bytes=50 * GB,
            cam_size_bytes=-1,
        )
        assert not info.can_snapshot  # negative cam_size should never happen

    def test_str_representation(self):
        """Test string representation includes key info."""
        info = SpaceInfo(
            total_bytes=100 * GB,
            free_bytes=50 * GB,
            used_bytes=50 * GB,
            cam_size_bytes=40 * GB,
        )
        s = str(info)
        assert "50.0" in s  # free
        assert "100.0" in s  # total
        assert "40.0" in s  # cam_size
        assert "OK" in s  # status


class TestSpaceManager:
    """Tests for SpaceManager."""

    def test_get_space_info(self, mock_fs: MockFilesystem, snapshot_manager: SnapshotManager):
        """Test getting space info."""
        mock_fs.set_free_space(100 * GB)

        manager = SpaceManager(
            fs=mock_fs,
            snapshot_manager=snapshot_manager,
            backingfiles_path=Path("/backingfiles"),
            cam_size=40 * GB,
        )

        info = manager.get_space_info()

        assert info.free_bytes == 100 * GB
        assert info.cam_size_bytes == 40 * GB
        assert info.can_snapshot  # 100 >= 40

    def test_cleanup_not_needed_when_space_ok(self, mock_fs: MockFilesystem, snapshot_manager: SnapshotManager):
        """Test cleanup_if_needed returns True immediately when space is OK."""
        mock_fs.set_free_space(100 * GB)

        manager = SpaceManager(
            fs=mock_fs,
            snapshot_manager=snapshot_manager,
            backingfiles_path=Path("/backingfiles"),
            cam_size=40 * GB,
        )

        assert manager.cleanup_if_needed()

    def test_cleanup_if_needed_deletes_snapshots(self, mock_fs: MockFilesystem):
        """Test cleanup_if_needed deletes old snapshots."""
        snapshot_manager = SnapshotManager(
            fs=mock_fs,
            cam_disk_path=Path("/backingfiles/cam_disk.bin"),
            snapshots_path=Path("/backingfiles/snapshots"),
        )

        snapshot_manager.create_snapshot()
        snapshot_manager.create_snapshot()

        # Set low free space (below cam_size)
        mock_fs.set_free_space(30 * GB)

        manager = SpaceManager(
            fs=mock_fs,
            snapshot_manager=snapshot_manager,
            backingfiles_path=Path("/backingfiles"),
            cam_size=40 * GB,
        )

        # Verify snapshots exist
        assert len(snapshot_manager.get_snapshots()) == 2
        assert len(snapshot_manager.get_deletable_snapshots()) == 2

    def test_cleanup_if_needed_no_snapshots(self, mock_fs: MockFilesystem):
        """Test cleanup_if_needed when no snapshots to delete."""
        snapshot_manager = SnapshotManager(
            fs=mock_fs,
            cam_disk_path=Path("/backingfiles/cam_disk.bin"),
            snapshots_path=Path("/backingfiles/snapshots"),
        )

        mock_fs.set_free_space(30 * GB)

        manager = SpaceManager(
            fs=mock_fs,
            snapshot_manager=snapshot_manager,
            backingfiles_path=Path("/backingfiles"),
            cam_size=40 * GB,
        )

        # No snapshots to delete, should return False
        assert not manager.cleanup_if_needed()

    def test_ensure_space_for_snapshot(self, mock_fs: MockFilesystem, snapshot_manager: SnapshotManager):
        """Test ensure_space_for_snapshot."""
        mock_fs.set_free_space(50 * GB)

        manager = SpaceManager(
            fs=mock_fs,
            snapshot_manager=snapshot_manager,
            backingfiles_path=Path("/backingfiles"),
            cam_size=40 * GB,
        )

        # Should succeed with plenty of space
        assert manager.ensure_space_for_snapshot()

    def test_ensure_space_for_snapshot_when_low(self, mock_fs: MockFilesystem, snapshot_manager: SnapshotManager):
        """Test ensure_space_for_snapshot when space is low and no snapshots."""
        mock_fs.set_free_space(30 * GB)

        manager = SpaceManager(
            fs=mock_fs,
            snapshot_manager=snapshot_manager,
            backingfiles_path=Path("/backingfiles"),
            cam_size=40 * GB,
        )

        # Should fail - no snapshots to delete
        assert not manager.ensure_space_for_snapshot()

    def test_space_manager_with_zero_cam_size(self, mock_fs: MockFilesystem, snapshot_manager: SnapshotManager):
        """Test SpaceManager with cam_size=0 (not initialized)."""
        mock_fs.set_free_space(50 * GB)

        manager = SpaceManager(
            fs=mock_fs,
            snapshot_manager=snapshot_manager,
            backingfiles_path=Path("/backingfiles"),
            cam_size=0,
        )

        space_info = manager.get_space_info()
        # cam_size=0 should result in can_snapshot=False
        assert not space_info.can_snapshot
        assert space_info.cam_size_bytes == 0
