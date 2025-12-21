"""Space management for TeslaUSB.

This module provides:
- SpaceManager: Monitors disk space and coordinates cleanup
- Clear space calculations and requirements
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
MB = 1024 * 1024


class SpaceError(Exception):
    """Base exception for space errors."""


class SpaceExhaustedError(SpaceError):
    """No space available and cannot free any."""


@dataclass
class SpaceInfo:
    """Information about disk space usage."""

    total_bytes: int
    free_bytes: int
    used_bytes: int
    reserve_bytes: int
    snapshot_budget_bytes: int

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
    def reserve_gb(self) -> float:
        return self.reserve_bytes / GB

    @property
    def snapshot_budget_gb(self) -> float:
        return self.snapshot_budget_bytes / GB

    @property
    def is_low(self) -> bool:
        """Whether free space is below reserve."""
        return self.free_bytes < self.reserve_bytes

    def __str__(self) -> str:
        return (
            f"Space: {self.free_gb:.1f} GiB free / {self.total_gb:.1f} GiB total "
            f"(reserve: {self.reserve_gb:.1f} GiB, budget: {self.snapshot_budget_gb:.1f} GiB)"
        )


class SpaceManager:
    """Manages disk space and coordinates cleanup.

    The space model:
    - Total backingfiles space contains: cam_disk, snapshots, free space
    - Reserve = 10 GiB (for filesystem overhead and safety margin)
    - Snapshot budget = free_space - reserve

    For reliable operation:
    - Recommended: total >= 2 * cam_size + reserve
    - This allows for 1 full snapshot while maintaining reserve
    """

    def __init__(
        self,
        fs: Filesystem,
        snapshot_manager: SnapshotManager,
        backingfiles_path: Path,
        cam_size: int,
        reserve: int = 10 * GB,
    ):
        """Initialize SpaceManager.

        Args:
            fs: Filesystem abstraction
            snapshot_manager: SnapshotManager instance
            backingfiles_path: Path to backingfiles directory
            cam_size: Size of cam_disk in bytes
            reserve: Reserve space in bytes (default 10GB)
        """
        self.fs = fs
        self.snapshot_manager = snapshot_manager
        self.backingfiles_path = backingfiles_path
        self.cam_size = cam_size
        self.reserve = reserve

    @property
    def reserve_bytes(self) -> int:
        """Required reserve space (minimum free space to maintain)."""
        return self.reserve

    def get_space_info(self) -> SpaceInfo:
        """Get current space information."""
        statvfs = self.fs.statvfs(self.backingfiles_path)

        total = statvfs.total_bytes
        free = statvfs.available_bytes
        used = total - free
        reserve = self.reserve_bytes
        budget = max(0, free - reserve)

        return SpaceInfo(
            total_bytes=total,
            free_bytes=free,
            used_bytes=used,
            reserve_bytes=reserve,
            snapshot_budget_bytes=budget,
        )

    def is_space_low(self) -> bool:
        """Check if free space is below reserve threshold."""
        info = self.get_space_info()
        return info.is_low

    def has_snapshot_budget(self, required_bytes: int = 0) -> bool:
        """Check if there's budget for a new snapshot.

        Args:
            required_bytes: Minimum required budget (0 = any positive budget)

        Returns:
            True if snapshot budget >= required_bytes
        """
        info = self.get_space_info()
        if required_bytes <= 0:
            return info.snapshot_budget_bytes > 0
        return info.snapshot_budget_bytes >= required_bytes

    def cleanup_if_needed(self) -> bool:
        """Clean up old snapshots if space is low.

        Deletes oldest deletable snapshots until:
        - Space is above reserve, OR
        - No more deletable snapshots exist

        Returns:
            True if space is now sufficient, False if still low
        """
        while self.is_space_low():
            info = self.get_space_info()
            logger.warning(f"Low space: {info}")

            deletable = self.snapshot_manager.get_deletable_snapshots()
            if not deletable:
                logger.error("Low space but no deletable snapshots")
                return False

            oldest = deletable[0]
            logger.info(f"Deleting snapshot {oldest.id} to free space")

            if self.snapshot_manager.delete_snapshot(oldest.id):
                new_info = self.get_space_info()
                logger.info(f"After deletion: {new_info}")
            else:
                logger.error(f"Failed to delete snapshot {oldest.id}")
                return False

        return True

    def ensure_space_for_snapshot(self) -> bool:
        """Ensure there's space for a new snapshot.

        A snapshot can grow up to cam_size in the worst case
        (if all blocks change during archiving).

        This method tries to ensure at least some snapshot budget exists,
        cleaning up old snapshots if needed.

        Returns:
            True if space is available, False if not

        Note:
            This doesn't guarantee the full cam_size is available,
            just that we have some budget and aren't critically low.
        """
        # First, ensure we're above reserve
        if not self.cleanup_if_needed():
            return False

        info = self.get_space_info()

        # Check if we have any snapshot budget
        if info.snapshot_budget_bytes <= 0:
            logger.warning("No snapshot budget available")
            return False

        # Log warning if budget is less than cam_size
        if info.snapshot_budget_bytes < self.cam_size:
            logger.warning(
                f"Snapshot budget ({info.snapshot_budget_gb:.1f} GiB) is less than cam_size "
                f"({self.cam_size / GB:.1f} GiB). Snapshot may fail if cam disk fills up."
            )

        return True

    def get_recommended_cam_size(self, total_space: int) -> int:
        """Calculate recommended maximum cam_size for given total space.

        Formula: cam_size <= (total - reserve) / 2

        This ensures space for:
        - cam_disk (1x cam_size)
        - 1 full snapshot (1x cam_size worst case)
        - reserve

        Args:
            total_space: Total available space in bytes

        Returns:
            Recommended maximum cam_size in bytes
        """
        available = total_space - self.reserve
        return int(available / 2)

    def validate_configuration(self, total_space: int) -> list[str]:
        """Validate current cam_size configuration.

        Returns:
            List of warning messages (empty if configuration is good)
        """
        warnings: list[str] = []

        recommended = self.get_recommended_cam_size(total_space)

        if self.cam_size > recommended:
            warnings.append(
                f"CAM_SIZE ({self.cam_size / GB:.1f} GiB) exceeds recommended maximum "
                f"({recommended / GB:.1f} GiB) for {total_space / GB:.1f} GiB total space. "
                f"You may experience space exhaustion errors."
            )

        # Check minimum viable space
        min_required = self.cam_size + self.reserve_bytes
        if total_space < min_required:
            warnings.append(
                f"Total space ({total_space / GB:.1f} GiB) is less than minimum required "
                f"({min_required / GB:.1f} GiB). System will not function correctly."
            )

        return warnings
