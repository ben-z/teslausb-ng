"""Space management for TeslaUSB.

This module provides:
- SpaceManager: Monitors disk space
- SpaceInfo: Disk space usage information
- calculate_cam_size: Calculate cam disk size from backingfiles size

Space model:
- backingfiles.img = available_disk - RESERVE
- cam_size = (backingfiles - 3% overhead) / 2
- Snapshots use XFS reflinks (copy-on-write), so they start small but can grow
- cam_disk uses at most half the XFS volume, so proactive snapshot deletion
  after each archive cycle guarantees enough space without threshold checks
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .filesystem import Filesystem

# Constants
GB = 1024 * 1024 * 1024
SECTOR_SIZE = 512  # Disk sector size for alignment
XFS_OVERHEAD_PROPORTION = 0.03  # 3% reserved for XFS metadata (measured ~2% in practice)
MIN_CAM_SIZE = 1 * GB  # Minimum useful cam disk size
DEFAULT_RESERVE = 10 * GB  # Default space to reserve for OS


def calculate_cam_size(backingfiles_size: int) -> int:
    """Calculate cam_size from backingfiles size.

    Formula: cam_size = (backingfiles_size - xfs_overhead) / 2, aligned to sector boundary

    This ensures space for:
    - cam_disk.bin (1x cam_size)
    - 1 full snapshot worst case (1x cam_size)
    - XFS metadata overhead (~2-3% of filesystem size)

    The result is aligned down to a 512-byte sector boundary to prevent
    losetup from truncating the file. Without alignment, the partition
    table may reference sectors beyond the loop device boundary, causing
    write failures and read-only filesystem remounts.

    Args:
        backingfiles_size: Total size of backingfiles.img in bytes

    Returns:
        Recommended cam_size in bytes (sector-aligned)
    """
    xfs_overhead = int(backingfiles_size * XFS_OVERHEAD_PROPORTION)
    usable = backingfiles_size - xfs_overhead
    cam_size = usable // 2
    # Align down to sector boundary to prevent losetup truncation issues
    return max(0, (cam_size // SECTOR_SIZE) * SECTOR_SIZE)


@dataclass
class SpaceInfo:
    """Information about disk space usage."""

    total_bytes: int
    free_bytes: int
    used_bytes: int

    @property
    def total_gb(self) -> float:
        return self.total_bytes / GB

    @property
    def free_gb(self) -> float:
        return self.free_bytes / GB

    @property
    def used_gb(self) -> float:
        return self.used_bytes / GB

    def __str__(self) -> str:
        return f"Space: {self.free_gb:.1f} GiB free / {self.total_gb:.1f} GiB total"


class SpaceManager:
    """Monitors disk space on the backingfiles volume."""

    def __init__(
        self,
        fs: Filesystem,
        backingfiles_path: Path,
    ):
        """Initialize SpaceManager.

        Args:
            fs: Filesystem abstraction
            backingfiles_path: Path to backingfiles directory
        """
        self.fs = fs
        self.backingfiles_path = backingfiles_path

    def get_space_info(self) -> SpaceInfo:
        """Get current space information."""
        statvfs = self.fs.statvfs(self.backingfiles_path)

        total = statvfs.total_bytes
        free = statvfs.available_bytes
        used = total - free

        return SpaceInfo(
            total_bytes=total,
            free_bytes=free,
            used_bytes=used,
        )
