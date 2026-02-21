"""Tests for space management."""

from pathlib import Path

from teslausb.filesystem import MockFilesystem
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
        )

        assert info.total_gb == 100.0
        assert info.free_gb == 50.0
        assert info.used_gb == 50.0

    def test_str_representation(self):
        """Test string representation includes key info."""
        info = SpaceInfo(
            total_bytes=100 * GB,
            free_bytes=50 * GB,
            used_bytes=50 * GB,
        )
        s = str(info)
        assert "50.0" in s  # free
        assert "100.0" in s  # total


class TestSpaceManager:
    """Tests for SpaceManager."""

    def test_get_space_info(self, mock_fs: MockFilesystem):
        """Test getting space info."""
        mock_fs.set_free_space(100 * GB)

        manager = SpaceManager(
            fs=mock_fs,
            backingfiles_path=Path("/backingfiles"),
        )

        info = manager.get_space_info()

        assert info.free_bytes == 100 * GB
        assert info.total_bytes == mock_fs.statvfs(Path("/backingfiles")).total_bytes
