"""Space management for TeslaUSB.

This module provides:
- SpaceManager: Monitors disk space and coordinates cleanup

Space model:
- backingfiles.img = available_disk - RESERVE
- cam_size = (backingfiles - XFS_OVERHEAD) / 2
- Snapshots use XFS reflinks (copy-on-write), so they start small but can grow
- Worst case: a snapshot grows to full cam_size (if all blocks change)
- To guarantee the next snapshot succeeds, we need cam_size free space

Cleanup strategy:
- Delete oldest snapshots until free_space >= cam_size
- This ensures there's always room for one full snapshot
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from .filesystem import Filesystem
from .snapshot import SnapshotManager

logger = logging.getLogger(__name__)

# Constants
GB = 1024 * 1024 * 1024
XFS_OVERHEAD = 2 * GB  # Reserved for XFS metadata
MIN_CAM_SIZE = 1 * GB  # Minimum useful cam disk size


def calculate_cam_size(backingfiles_size: int) -> int:
    """Calculate cam_size from backingfiles size.

    Formula: cam_size = (backingfiles_size - XFS_OVERHEAD) / 2

    This ensures space for:
    - cam_disk.bin (1x cam_size)
    - 1 full snapshot worst case (1x cam_size)
    - XFS metadata overhead

    Args:
        backingfiles_size: Total size of backingfiles.img in bytes

    Returns:
        Recommended cam_size in bytes
    """
    usable = backingfiles_size - XFS_OVERHEAD
    return max(0, usable // 2)


@dataclass
class SpaceInfo:
    """Information about disk space usage."""

    total_bytes: int
    free_bytes: int
    used_bytes: int
    cam_size_bytes: int

    @property
    def total_gb(self) -> float:
        return self.total_bytes / GB

    @property
    def free_gb(self) -> float:
        return self.free_bytes / GB

    @property
    def used_gb(self) -> float:
        return self.used_bytes / GB

    @property
    def cam_size_gb(self) -> float:
        return self.cam_size_bytes / GB

    @property
    def can_snapshot(self) -> bool:
        """Whether there's enough free space for a new snapshot.

        A snapshot can grow up to cam_size in the worst case (if all blocks
        change while archiving). We need at least cam_size free to be safe.
        
        Returns False if cam_size is 0 (not initialized).
        """
        if self.cam_size_bytes == 0:
            return False
        return self.free_bytes >= self.cam_size_bytes

    def __str__(self) -> str:
        status = "OK" if self.can_snapshot else "LOW"
        return (
            f"Space: {self.free_gb:.1f} GiB free / {self.total_gb:.1f} GiB total "
            f"(need {self.cam_size_gb:.1f} GiB for snapshot) [{status}]"
        )


class SpaceManager:
    """Manages disk space and coordinates cleanup.

    The key invariant: always maintain cam_size free space so the next
    snapshot is guaranteed to succeed (worst case = full COW copy).
    """

    def __init__(
        self,
        fs: Filesystem,
        snapshot_manager: SnapshotManager,
        backingfiles_path: Path,
        cam_size: int,
    ):
        """Initialize SpaceManager.

        Args:
            fs: Filesystem abstraction
            snapshot_manager: SnapshotManager instance
            backingfiles_path: Path to backingfiles directory
            cam_size: Size of cam_disk in bytes (also the cleanup threshold)
        """
        self.fs = fs
        self.snapshot_manager = snapshot_manager
        self.backingfiles_path = backingfiles_path
        self.cam_size = cam_size

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
            cam_size_bytes=self.cam_size,
        )

    def cleanup_if_needed(self) -> bool:
        """Clean up old snapshots if space is low.

        Deletes oldest deletable snapshots until free_space >= cam_size.

        Returns:
            True if space is now sufficient (can_snapshot), False if still low
        """
        info = self.get_space_info()

        while not info.can_snapshot:
            logger.warning(f"Low space: {info}")

            deletable = self.snapshot_manager.get_deletable_snapshots()
            if not deletable:
                logger.error("Cannot free space: no deletable snapshots")
                return False

            oldest = deletable[0]
            logger.info(f"Deleting snapshot {oldest.id} to free space")

            if not self.snapshot_manager.delete_snapshot(oldest.id):
                logger.error(f"Failed to delete snapshot {oldest.id}")
                return False

            info = self.get_space_info()
            logger.info(f"After deletion: {info}")

        return True

    def ensure_space_for_snapshot(self) -> bool:
        """Ensure there's space for a new snapshot.

        Cleans up old snapshots until free_space >= cam_size.

        Returns:
            True if space is available, False if not
        """
        return self.cleanup_if_needed()
