"""Snapshot management with proper state machine and reference counting.

This module provides:
- Snapshot: Represents a point-in-time copy of the camera disk
- SnapshotHandle: RAII-style handle for acquiring/releasing snapshots
- SnapshotManager: Manages snapshot lifecycle with proper locking

Design principles:
- The .toc file is the single source of truth for snapshot completion
- If .toc exists, the snapshot is complete and valid
- If .toc doesn't exist, the snapshot is incomplete and should be deleted
- State (READY vs ARCHIVING) is derived from refcount, not stored
- Only immutable facts (id, path, created_at) are persisted to metadata.json
"""

from __future__ import annotations

import json
import logging
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Iterator

from .filesystem import Filesystem

logger = logging.getLogger(__name__)


class SnapshotState(Enum):
    """State of a snapshot in its lifecycle.

    Note: These are runtime states derived from refcount.
    - READY: refcount == 0 (available for use or deletion)
    - ARCHIVING: refcount > 0 (in use, cannot be deleted)
    """

    READY = "ready"
    ARCHIVING = "archiving"


class SnapshotError(Exception):
    """Base exception for snapshot errors."""


class SnapshotNotFoundError(SnapshotError):
    """Snapshot not found."""


class SnapshotInUseError(SnapshotError):
    """Snapshot is in use and cannot be deleted."""


class SnapshotCreationError(SnapshotError):
    """Error creating snapshot."""


@dataclass
class Snapshot:
    """Represents a point-in-time copy of the camera disk.

    Attributes:
        id: Unique identifier (monotonically increasing)
        path: Path to snapshot directory
        created_at: When the snapshot was created
        refcount: Number of active references (handles) - runtime only
    """

    id: int
    path: Path
    created_at: datetime
    refcount: int = 0

    @property
    def image_path(self) -> Path:
        """Path to the snapshot image file."""
        return self.path / "snap.bin"

    @property
    def toc_path(self) -> Path:
        """Path to the table of contents file (marks snapshot as complete)."""
        return self.path / "snap.toc"

    @property
    def metadata_path(self) -> Path:
        """Path to the metadata JSON file."""
        return self.path / "metadata.json"

    @property
    def state(self) -> SnapshotState:
        """Current state, derived from refcount."""
        return SnapshotState.ARCHIVING if self.refcount > 0 else SnapshotState.READY

    @property
    def is_complete(self) -> bool:
        """Whether the snapshot creation completed successfully.

        A complete snapshot always has a .toc file. Since we only load
        complete snapshots, this always returns True for loaded snapshots.
        """
        return True  # Only complete snapshots are ever loaded

    @property
    def is_deletable(self) -> bool:
        """Whether the snapshot can be deleted."""
        return self.refcount == 0

    def to_dict(self) -> dict:
        """Serialize to dictionary.

        Only stores immutable facts. State is derived from refcount,
        which is always 0 on load (handles don't survive restart).
        """
        return {
            "id": self.id,
            "path": str(self.path),
            "created_at": self.created_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict) -> Snapshot:
        """Deserialize from dictionary."""
        return cls(
            id=data["id"],
            path=Path(data["path"]),
            created_at=datetime.fromisoformat(data["created_at"]),
            refcount=0,  # Always 0 on load
        )


class SnapshotHandle:
    """RAII-style handle for snapshot access.

    Acquiring a handle increments the snapshot's refcount.
    Releasing (or garbage collecting) decrements it.

    Use as a context manager:
        with snapshot_manager.acquire(snapshot_id) as handle:
            # use handle.snapshot
            pass
        # automatically released
    """

    def __init__(self, snapshot: Snapshot, manager: SnapshotManager):
        self._snapshot = snapshot
        self._manager = manager
        self._released = False

    @property
    def snapshot(self) -> Snapshot:
        """The acquired snapshot."""
        if self._released:
            raise SnapshotError("Handle has been released")
        return self._snapshot

    def release(self) -> None:
        """Release the handle, decrementing refcount."""
        if not self._released:
            self._manager._release_handle(self)
            self._released = True

    def __enter__(self) -> SnapshotHandle:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.release()

    def __del__(self) -> None:
        if not self._released:
            self.release()


@dataclass
class SnapshotManager:
    """Manages snapshot lifecycle with proper locking and reference counting.

    Thread-safe implementation that ensures:
    - Only one snapshot creation at a time
    - Snapshots cannot be deleted while in use (refcount > 0)
    - Incomplete snapshots are cleaned up on startup

    Crash safety:
    - The .toc file is created last, after all data is written
    - On startup, any snapshot without .toc is deleted (was incomplete)
    - Deletion removes .toc first (marks incomplete), then deletes data
    """

    fs: Filesystem
    cam_disk_path: Path
    snapshots_path: Path

    # Internal state
    _snapshots: dict[int, Snapshot] = field(default_factory=dict)
    _next_id: int = 0
    _lock: threading.RLock = field(default_factory=threading.RLock)
    _creating: bool = False

    def __post_init__(self) -> None:
        """Initialize manager and load existing snapshots."""
        self._lock = threading.RLock()
        self._load_snapshots()

    def _load_snapshots(self) -> None:
        """Load existing snapshots from disk.

        Only loads complete snapshots (those with .toc file).
        Incomplete snapshots are cleaned up.
        """
        if not self.fs.exists(self.snapshots_path):
            self.fs.mkdir(self.snapshots_path, parents=True, exist_ok=True)
            return

        for name in self.fs.listdir(self.snapshots_path):
            if not name.startswith("snap-"):
                continue

            snap_path = self.snapshots_path / name
            if not self.fs.is_dir(snap_path):
                continue

            try:
                snap_id = int(name.replace("snap-", ""))
            except ValueError:
                logger.warning(f"Invalid snapshot directory name: {name}")
                continue

            toc_path = snap_path / "snap.toc"

            # The .toc file is the source of truth for completion
            if not self.fs.exists(toc_path):
                logger.warning(f"Cleaning up incomplete snapshot {snap_id}")
                self._remove_snapshot_dir(snap_path)
                continue

            # Load snapshot metadata
            metadata_path = snap_path / "metadata.json"
            if self.fs.exists(metadata_path):
                try:
                    data = json.loads(self.fs.read_text(metadata_path))
                    snapshot = Snapshot.from_dict(data)
                except (json.JSONDecodeError, KeyError) as e:
                    logger.warning(f"Failed to load snapshot metadata {metadata_path}: {e}")
                    # Metadata corrupted but .toc exists - reconstruct from filesystem
                    snapshot = self._reconstruct_snapshot(snap_id, snap_path)
            else:
                # No metadata but .toc exists - reconstruct from filesystem
                snapshot = self._reconstruct_snapshot(snap_id, snap_path)
                self._save_metadata(snapshot)

            self._snapshots[snap_id] = snapshot
            self._next_id = max(self._next_id, snap_id + 1)

        logger.info(f"Loaded {len(self._snapshots)} existing snapshots")

    def _reconstruct_snapshot(self, snap_id: int, snap_path: Path) -> Snapshot:
        """Reconstruct snapshot metadata from filesystem."""
        image_path = snap_path / "snap.bin"
        if self.fs.exists(image_path):
            stat = self.fs.stat(image_path)
            created_at = datetime.fromtimestamp(stat.mtime)
        else:
            created_at = datetime.now()

        return Snapshot(
            id=snap_id,
            path=snap_path,
            created_at=created_at,
            refcount=0,
        )

    def _remove_snapshot_dir(self, snap_path: Path) -> None:
        """Remove a snapshot directory."""
        try:
            if self.fs.exists(snap_path):
                self.fs.rmtree(snap_path)
                logger.info(f"Removed snapshot at {snap_path}")
        except Exception as e:
            logger.error(f"Failed to remove {snap_path}: {e}")

    def _save_metadata(self, snapshot: Snapshot) -> None:
        """Save snapshot metadata to disk."""
        try:
            self.fs.write_text(
                snapshot.metadata_path,
                json.dumps(snapshot.to_dict(), indent=2)
            )
        except Exception as e:
            logger.warning(f"Failed to save snapshot metadata: {e}")

    def _snap_dir_name(self, snap_id: int) -> str:
        """Generate snapshot directory name."""
        return f"snap-{snap_id:06d}"

    def create_snapshot(self, fsck: bool = True) -> Snapshot:
        """Create a new COW snapshot of the camera disk.

        Args:
            fsck: Whether to run filesystem check on the snapshot

        Returns:
            The created snapshot

        Raises:
            SnapshotCreationError: If creation fails
        """
        with self._lock:
            if self._creating:
                raise SnapshotCreationError("Snapshot creation already in progress")
            self._creating = True

        try:
            snap_id = self._next_id
            snap_path = self.snapshots_path / self._snap_dir_name(snap_id)

            logger.info(f"Creating snapshot {snap_id} at {snap_path}")

            # Create snapshot directory
            self.fs.mkdir(snap_path, parents=True, exist_ok=True)

            # Perform COW copy
            image_path = snap_path / "snap.bin"
            try:
                self.fs.copy_reflink(self.cam_disk_path, image_path)
            except Exception as e:
                logger.error(f"Failed to create snapshot image: {e}")
                self._remove_snapshot_dir(snap_path)
                raise SnapshotCreationError(f"Failed to copy cam disk: {e}") from e

            # Create snapshot object
            snapshot = Snapshot(
                id=snap_id,
                path=snap_path,
                created_at=datetime.now(),
                refcount=0,
            )

            # Save metadata
            self._save_metadata(snapshot)

            # Create .toc file LAST - this marks the snapshot as complete
            # If we crash before this, the snapshot will be cleaned up on restart
            self.fs.write_text(snapshot.toc_path, "")

            with self._lock:
                self._snapshots[snap_id] = snapshot
                self._next_id = snap_id + 1

            logger.info(f"Snapshot {snap_id} created successfully")
            return snapshot

        finally:
            with self._lock:
                self._creating = False

    def acquire(self, snapshot_id: int) -> SnapshotHandle:
        """Acquire a reference to a snapshot.

        Increments the snapshot's refcount and returns a handle.
        The handle must be released when done (use as context manager).

        Args:
            snapshot_id: ID of snapshot to acquire

        Returns:
            SnapshotHandle for the acquired snapshot

        Raises:
            SnapshotNotFoundError: If snapshot doesn't exist
        """
        with self._lock:
            if snapshot_id not in self._snapshots:
                raise SnapshotNotFoundError(f"Snapshot {snapshot_id} not found")

            snapshot = self._snapshots[snapshot_id]
            snapshot.refcount += 1

            logger.debug(f"Acquired snapshot {snapshot_id}, refcount={snapshot.refcount}")
            return SnapshotHandle(snapshot, self)

    def _release_handle(self, handle: SnapshotHandle) -> None:
        """Release a snapshot handle (called by SnapshotHandle)."""
        with self._lock:
            snapshot = handle._snapshot

            if snapshot.id not in self._snapshots:
                logger.warning(f"Releasing handle for deleted snapshot {snapshot.id}")
                return

            snapshot.refcount = max(0, snapshot.refcount - 1)
            logger.debug(f"Released snapshot {snapshot.id}, refcount={snapshot.refcount}")

    def get_snapshot(self, snapshot_id: int) -> Snapshot | None:
        """Get a snapshot by ID."""
        with self._lock:
            return self._snapshots.get(snapshot_id)

    def get_snapshots(self) -> list[Snapshot]:
        """Get all snapshots, ordered by creation time (oldest first)."""
        with self._lock:
            return sorted(self._snapshots.values(), key=lambda s: s.created_at)

    def get_deletable_snapshots(self) -> list[Snapshot]:
        """Get snapshots that can be deleted (refcount == 0)."""
        with self._lock:
            return [s for s in self.get_snapshots() if s.is_deletable]

    def delete_snapshot(self, snapshot_id: int) -> bool:
        """Delete a snapshot if it's not in use.

        Deletion is crash-safe: .toc is deleted first (marks incomplete),
        then the rest of the directory is removed. If we crash mid-deletion,
        the snapshot will be cleaned up on next startup.

        Args:
            snapshot_id: ID of snapshot to delete

        Returns:
            True if deleted, False if not found

        Raises:
            SnapshotInUseError: If snapshot has references
        """
        with self._lock:
            if snapshot_id not in self._snapshots:
                logger.warning(f"Snapshot {snapshot_id} not found for deletion")
                return False

            snapshot = self._snapshots[snapshot_id]

            if snapshot.refcount > 0:
                raise SnapshotInUseError(
                    f"Snapshot {snapshot_id} has {snapshot.refcount} active references"
                )

            # Remove from tracking first
            del self._snapshots[snapshot_id]

        # Perform deletion outside lock
        logger.info(f"Deleting snapshot {snapshot_id}")

        # Delete .toc first - this marks snapshot as incomplete
        # If we crash after this, cleanup will remove the rest on restart
        try:
            if self.fs.exists(snapshot.toc_path):
                self.fs.remove(snapshot.toc_path)
        except Exception as e:
            logger.warning(f"Failed to remove .toc file: {e}")

        # Now delete the rest
        self._remove_snapshot_dir(snapshot.path)

        logger.info(f"Snapshot {snapshot_id} deleted")
        return True

    def delete_oldest_if_deletable(self) -> bool:
        """Delete the oldest deletable snapshot.

        Returns:
            True if a snapshot was deleted, False otherwise
        """
        deletable = self.get_deletable_snapshots()
        if not deletable:
            return False

        oldest = deletable[0]
        try:
            return self.delete_snapshot(oldest.id)
        except SnapshotInUseError:
            # Race condition - snapshot was acquired between check and delete
            return False

    @contextmanager
    def snapshot_session(self, fsck: bool = True) -> Iterator[SnapshotHandle]:
        """Context manager that creates a snapshot and acquires it.

        Creates a new snapshot, acquires a handle to it, and ensures
        the handle is released when the context exits.

        Usage:
            with snapshot_manager.snapshot_session() as handle:
                # use handle.snapshot
                archive_files(handle.snapshot)
            # handle automatically released

        Args:
            fsck: Whether to run filesystem check on the snapshot

        Yields:
            SnapshotHandle for the new snapshot
        """
        snapshot = self.create_snapshot(fsck=fsck)
        handle = self.acquire(snapshot.id)
        try:
            yield handle
        finally:
            handle.release()
