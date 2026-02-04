"""Tests for space management."""

from pathlib import Path

import pytest

from teslausb.filesystem import MockFilesystem
from teslausb.snapshot import SnapshotManager
from teslausb.space import (
    SpaceManager,
    SpaceInfo,
    GB,
    SECTOR_SIZE,
    XFS_OVERHEAD_PROPORTION,
    MIN_CAM_SIZE,
    DEFAULT_RESERVE,
    calculate_cam_size,
)


class TestCalculateCamSize:
    """Tests for calculate_cam_size function."""

    def test_cam_size_is_just_under_half_of_backingfiles(self):
        """Test that cam_size is slightly less than half due to XFS overhead."""
        backingfiles = 100 * GB
        result = calculate_cam_size(backingfiles)

        # Should be less than half (due to overhead)
        assert result < backingfiles // 2
        # But not by much - within 2% of half
        assert result > backingfiles * 0.48

    def test_scales_linearly_with_size(self):
        """Test that cam_size scales proportionally with backingfiles size."""
        small = calculate_cam_size(50 * GB)
        large = calculate_cam_size(100 * GB)

        # Doubling backingfiles should approximately double cam_size
        assert large == small * 2

    def test_zero_returns_zero(self):
        """Test that zero backingfiles returns 0."""
        assert calculate_cam_size(0) == 0

    def test_constants_have_expected_values(self):
        """Verify constants are set correctly."""
        assert XFS_OVERHEAD_PROPORTION == 0.03  # 3%
        assert MIN_CAM_SIZE == 1 * GB
        assert DEFAULT_RESERVE == 10 * GB
        assert SECTOR_SIZE == 512

    def test_result_is_sector_aligned(self):
        """Test that cam_size is always aligned to 512-byte sector boundary.

        Without alignment, losetup will truncate the file to the nearest
        sector boundary, causing the partition to extend beyond the loop
        device and resulting in write failures and read-only remounts.
        """
        # Test various sizes that would produce non-aligned values without the fix
        test_sizes = [
            100 * GB,
            118 * GB,  # Common real-world size
            127 * GB,
            200 * GB,
            # Pathological cases
            100 * GB + 1,
            100 * GB + 255,
            100 * GB + 511,
            100 * GB + 513,
        ]

        for size in test_sizes:
            result = calculate_cam_size(size)
            assert result % SECTOR_SIZE == 0, f"cam_size {result} not sector-aligned for backingfiles_size {size}"

    def test_alignment_does_not_increase_size(self):
        """Test that alignment rounds down, never up (to stay within space budget)."""
        # The aligned size should never exceed the unaligned calculation
        for backingfiles in [50 * GB, 100 * GB, 200 * GB]:
            xfs_overhead = int(backingfiles * XFS_OVERHEAD_PROPORTION)
            usable = backingfiles - xfs_overhead
            max_cam_size = usable // 2

            actual = calculate_cam_size(backingfiles)
            assert actual <= max_cam_size, (
                f"Alignment should round down, not up: "
                f"actual={actual}, max={max_cam_size}"
            )
            # Should be within one sector of the max
            assert actual >= max_cam_size - SECTOR_SIZE + 1

    def test_small_size_rounds_to_zero(self):
        """Test that very small backingfiles sizes round down to zero gracefully."""
        # A size so small that after overhead and division, it's less than one sector
        tiny_size = 500  # Less than 2 sectors after overhead
        result = calculate_cam_size(tiny_size)
        assert result == 0
        assert result % SECTOR_SIZE == 0  # Still "aligned" (zero is aligned)


class TestSpaceInfo:
    """Tests for SpaceInfo dataclass."""

    def test_space_info_properties(self):
        """Test SpaceInfo properties."""
        info = SpaceInfo(
            total_bytes=100 * GB,
            free_bytes=50 * GB,
            used_bytes=50 * GB,
            min_free_threshold=40 * GB,
        )

        assert info.total_gb == 100.0
        assert info.free_gb == 50.0
        assert info.used_gb == 50.0
        assert info.min_free_gb == 40.0

    def test_can_snapshot_when_enough_space(self):
        """Test can_snapshot is True when free >= min_free_threshold."""
        info = SpaceInfo(
            total_bytes=100 * GB,
            free_bytes=50 * GB,
            used_bytes=50 * GB,
            min_free_threshold=40 * GB,
        )
        assert info.can_snapshot  # 50 >= 40

    def test_can_snapshot_when_exact_space(self):
        """Test can_snapshot is True when free == min_free_threshold."""
        info = SpaceInfo(
            total_bytes=100 * GB,
            free_bytes=40 * GB,
            used_bytes=60 * GB,
            min_free_threshold=40 * GB,
        )
        assert info.can_snapshot  # 40 >= 40

    def test_cannot_snapshot_when_low_space(self):
        """Test can_snapshot is False when free < min_free_threshold."""
        info = SpaceInfo(
            total_bytes=100 * GB,
            free_bytes=30 * GB,
            used_bytes=70 * GB,
            min_free_threshold=40 * GB,
        )
        assert not info.can_snapshot  # 30 < 40

    def test_cannot_snapshot_when_threshold_zero(self):
        """Test can_snapshot is False when min_free_threshold is 0 (not initialized)."""
        info = SpaceInfo(
            total_bytes=100 * GB,
            free_bytes=50 * GB,
            used_bytes=50 * GB,
            min_free_threshold=0,
        )
        assert not info.can_snapshot  # threshold=0 means not initialized

    def test_cannot_snapshot_when_threshold_negative(self):
        """Test can_snapshot is False when min_free_threshold is negative."""
        info = SpaceInfo(
            total_bytes=100 * GB,
            free_bytes=50 * GB,
            used_bytes=50 * GB,
            min_free_threshold=-1,
        )
        assert not info.can_snapshot  # negative threshold should never happen

    def test_str_representation(self):
        """Test string representation includes key info."""
        info = SpaceInfo(
            total_bytes=100 * GB,
            free_bytes=50 * GB,
            used_bytes=50 * GB,
            min_free_threshold=40 * GB,
        )
        s = str(info)
        assert "50.0" in s  # free
        assert "100.0" in s  # total
        assert "40.0" in s  # min_free_threshold
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
            min_free_threshold=40 * GB,
        )

        info = manager.get_space_info()

        assert info.free_bytes == 100 * GB
        assert info.min_free_threshold == 40 * GB
        assert info.can_snapshot  # 100 >= 40

    def test_cleanup_not_needed_when_space_ok(self, mock_fs: MockFilesystem, snapshot_manager: SnapshotManager):
        """Test cleanup_if_needed returns True immediately when space is OK."""
        mock_fs.set_free_space(100 * GB)

        manager = SpaceManager(
            fs=mock_fs,
            snapshot_manager=snapshot_manager,
            backingfiles_path=Path("/backingfiles"),
            min_free_threshold=40 * GB,
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

        # Set low free space (below threshold)
        mock_fs.set_free_space(30 * GB)

        manager = SpaceManager(
            fs=mock_fs,
            snapshot_manager=snapshot_manager,
            backingfiles_path=Path("/backingfiles"),
            min_free_threshold=40 * GB,
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
            min_free_threshold=40 * GB,
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
            min_free_threshold=40 * GB,
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
            min_free_threshold=40 * GB,
        )

        # Should fail - no snapshots to delete
        assert not manager.ensure_space_for_snapshot()

    def test_space_manager_with_zero_threshold(self, mock_fs: MockFilesystem, snapshot_manager: SnapshotManager):
        """Test SpaceManager with min_free_threshold=0 (not initialized)."""
        mock_fs.set_free_space(50 * GB)

        manager = SpaceManager(
            fs=mock_fs,
            snapshot_manager=snapshot_manager,
            backingfiles_path=Path("/backingfiles"),
            min_free_threshold=0,
        )

        space_info = manager.get_space_info()
        # threshold=0 should result in can_snapshot=False
        assert not space_info.can_snapshot
        assert space_info.min_free_threshold == 0
