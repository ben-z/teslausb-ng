"""Archive management for TeslaUSB.

This module provides:
- ArchiveBackend: Abstract base class for archive backends
- RcloneBackend: Archive using rclone (supports 40+ cloud providers)
- ArchiveManager: Coordinates archiving from snapshots
"""

from __future__ import annotations

import logging
import subprocess
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from threading import Event
from typing import Callable, Iterator

from .filesystem import Filesystem
from .snapshot import SnapshotHandle, SnapshotManager

logger = logging.getLogger(__name__)


class ArchiveError(Exception):
    """Base exception for archive errors."""


class ArchiveConnectionError(ArchiveError):
    """Failed to connect to archive destination."""


class ArchiveState(Enum):
    """State of an archive operation."""

    PENDING = "pending"
    CONNECTING = "connecting"
    ARCHIVING = "archiving"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class ArchiveResult:
    """Result of an archive operation."""

    snapshot_id: int
    state: ArchiveState
    files_total: int = 0
    files_archived: int = 0
    files_skipped: int = 0
    files_failed: int = 0
    bytes_transferred: int = 0
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error: str | None = None

    @property
    def success(self) -> bool:
        return self.state == ArchiveState.COMPLETED

    @property
    def duration_seconds(self) -> float | None:
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None


class ArchiveBackend(ABC):
    """Abstract base class for archive backends."""

    @abstractmethod
    def is_reachable(self) -> bool:
        """Check if archive destination is reachable.

        Returns:
            True if destination can be reached
        """

    @abstractmethod
    def connect(self) -> None:
        """Connect to archive destination.

        Raises:
            ArchiveConnectionError: If connection fails
        """

    @abstractmethod
    def disconnect(self) -> None:
        """Disconnect from archive destination."""

    @abstractmethod
    def archive_file(self, src: Path, dst_relative: Path) -> bool:
        """Archive a single file.

        Args:
            src: Source file path (absolute)
            dst_relative: Destination path relative to archive root

        Returns:
            True if successful, False otherwise
        """

    @abstractmethod
    def file_exists(self, dst_relative: Path) -> bool:
        """Check if file already exists in archive.

        Args:
            dst_relative: Destination path relative to archive root

        Returns:
            True if file exists
        """

    @abstractmethod
    def get_file_size(self, dst_relative: Path) -> int | None:
        """Get size of file in archive.

        Args:
            dst_relative: Destination path relative to archive root

        Returns:
            File size in bytes, or None if file doesn't exist
        """


class MockArchiveBackend(ArchiveBackend):
    """Mock archive backend for testing."""

    def __init__(self, reachable: bool = True, fail_files: set[str] | None = None):
        self.reachable = reachable
        self.connected = False
        self.fail_files = fail_files or set()
        self.archived_files: dict[Path, bytes] = {}

    def is_reachable(self) -> bool:
        return self.reachable

    def connect(self) -> None:
        if not self.reachable:
            raise ArchiveConnectionError("Archive not reachable")
        self.connected = True

    def disconnect(self) -> None:
        self.connected = False

    def archive_file(self, src: Path, dst_relative: Path) -> bool:
        if str(dst_relative) in self.fail_files:
            return False
        # In mock, just record that the file was "archived"
        self.archived_files[dst_relative] = b"mock content"
        return True

    def file_exists(self, dst_relative: Path) -> bool:
        return dst_relative in self.archived_files

    def get_file_size(self, dst_relative: Path) -> int | None:
        if dst_relative in self.archived_files:
            return len(self.archived_files[dst_relative])
        return None


class RcloneBackend(ArchiveBackend):
    """Archive backend using rclone.

    Rclone supports 40+ cloud storage providers including Google Drive,
    Dropbox, S3, etc. Configure rclone first using `rclone config`.
    """

    def __init__(
        self,
        remote: str,
        path: str = "",
        flags: list[str] | None = None,
        timeout: int = 300,
        stop_event: Event | None = None,
    ):
        """Initialize rclone backend.

        Args:
            remote: Rclone remote name (e.g., "gdrive", "s3", "dropbox")
            path: Path within the remote (e.g., "TeslaCam/archive")
            flags: Additional rclone flags (e.g., ["--fast-list"])
            timeout: Timeout per file operation in seconds
            stop_event: Optional event to signal shutdown (for interruptible operations)
        """
        self.remote = remote
        self.path = path.strip("/")
        self.flags = flags or []
        self.timeout = timeout
        self.stop_event = stop_event

    def _dest(self, dst_relative: Path | str = "") -> str:
        """Build rclone destination path."""
        if self.path and dst_relative:
            return f"{self.remote}:{self.path}/{dst_relative}"
        elif self.path:
            return f"{self.remote}:{self.path}"
        elif dst_relative:
            return f"{self.remote}:{dst_relative}"
        else:
            return f"{self.remote}:"

    def is_reachable(self) -> bool:
        """Check if rclone remote is reachable.

        Uses polling with short intervals so stop events can be checked.
        """
        proc = None
        try:
            proc = subprocess.Popen(
                ["rclone", "lsf", f"{self.remote}:", "--max-depth", "1"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            # Poll with short intervals to allow stop event to interrupt
            for _ in range(300):  # 30 seconds total (300 * 0.1s)
                # Check if we should stop
                if self.stop_event and self.stop_event.is_set():
                    return False

                returncode = proc.poll()
                if returncode is not None:
                    # Log any output for debugging
                    stdout, stderr = proc.communicate()
                    if stderr:
                        for line in stderr.decode().splitlines():
                            logger.debug(f"rclone: {line}")
                    return returncode == 0
                time.sleep(0.1)
            # Timeout
            return False
        except (OSError, FileNotFoundError):
            return False
        finally:
            if proc is not None:
                proc.kill()  # No-op if already exited
                proc.wait()  # Reap zombie, returns immediately if already done

    def connect(self) -> None:
        """Verify rclone remote is accessible."""
        if not self.is_reachable():
            raise ArchiveConnectionError(f"Cannot reach rclone remote: {self.remote}")

    def disconnect(self) -> None:
        """No-op for rclone (stateless)."""
        pass

    def archive_file(self, src: Path, dst_relative: Path) -> bool:
        """Archive a single file using rclone copyto."""
        dest = self._dest(dst_relative)
        cmd = ["rclone", "copyto", str(src), dest] + self.flags

        try:
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=self.timeout,
                check=False,
            )
            if result.stderr:
                for line in result.stderr.decode().splitlines():
                    logger.debug(f"rclone: {line}")
            if result.returncode != 0:
                logger.warning(f"rclone copyto failed for {src}")
            return result.returncode == 0
        except subprocess.TimeoutExpired:
            logger.error(f"rclone timeout archiving {src}")
            return False
        except (OSError, FileNotFoundError) as e:
            logger.error(f"rclone error: {e}")
            return False

    def file_exists(self, dst_relative: Path) -> bool:
        """Check if file exists in remote."""
        dest = self._dest(dst_relative)
        try:
            result = subprocess.run(
                ["rclone", "lsf", dest],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=30,
                check=False,
            )
            if result.stderr:
                for line in result.stderr.decode().splitlines():
                    logger.debug(f"rclone: {line}")
            # lsf returns empty output if file doesn't exist
            return result.returncode == 0 and bool(result.stdout.strip())
        except (subprocess.TimeoutExpired, OSError, FileNotFoundError):
            return False

    def get_file_size(self, dst_relative: Path) -> int | None:
        """Get size of file in remote."""
        dest = self._dest(dst_relative)
        try:
            result = subprocess.run(
                ["rclone", "size", dest, "--json"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=30,
                check=False,
            )
            if result.stderr:
                for line in result.stderr.decode().splitlines():
                    logger.debug(f"rclone: {line}")
            if result.returncode == 0:
                import json
                data = json.loads(result.stdout)
                return data.get("bytes")
        except (subprocess.TimeoutExpired, OSError, FileNotFoundError, ValueError):
            pass
        return None


@dataclass
class FileToArchive:
    """Represents a file that needs to be archived."""

    src_path: Path
    dst_relative: Path
    size: int


class ArchiveManager:
    """Manages archiving footage from snapshots.

    Coordinates with SnapshotManager to:
    1. Acquire snapshot (locks it from deletion)
    2. Find files to archive
    3. Archive files via backend
    4. Release snapshot
    """

    def __init__(
        self,
        fs: Filesystem,
        snapshot_manager: SnapshotManager,
        backend: ArchiveBackend,
        archive_recent: bool = False,
        archive_saved: bool = True,
        archive_sentry: bool = True,
        archive_track: bool = True,
        min_file_size: int = 100_000,  # 100KB minimum (skip short recordings)
        progress_callback: Callable[[int, int], None] | None = None,
    ):
        """Initialize ArchiveManager.

        Args:
            fs: Filesystem abstraction
            snapshot_manager: SnapshotManager instance
            backend: Archive backend to use
            archive_recent: Whether to archive RecentClips
            archive_saved: Whether to archive SavedClips
            archive_sentry: Whether to archive SentryClips
            archive_track: Whether to archive TrackMode clips
            min_file_size: Minimum file size to archive (skip smaller files)
            progress_callback: Optional callback(files_done, files_total)
        """
        self.fs = fs
        self.snapshot_manager = snapshot_manager
        self.backend = backend
        self.archive_recent = archive_recent
        self.archive_saved = archive_saved
        self.archive_sentry = archive_sentry
        self.archive_track = archive_track
        self.min_file_size = min_file_size
        self.progress_callback = progress_callback

        # Track previously archived files to avoid re-archiving
        self._archived_files: set[str] = set()

    def _find_files_to_archive(self, snapshot_mount: Path) -> list[FileToArchive]:
        """Find all files that need to be archived from a snapshot.

        Args:
            snapshot_mount: Path where snapshot is mounted

        Returns:
            List of files to archive
        """
        files: list[FileToArchive] = []

        # Define directories to scan based on settings
        dirs_to_scan: list[tuple[Path, str]] = []

        if self.archive_saved:
            dirs_to_scan.append((snapshot_mount / "TeslaCam" / "SavedClips", "SavedClips"))
        if self.archive_sentry:
            dirs_to_scan.append((snapshot_mount / "TeslaCam" / "SentryClips", "SentryClips"))
        if self.archive_recent:
            dirs_to_scan.append((snapshot_mount / "TeslaCam" / "RecentClips", "RecentClips"))
        if self.archive_track:
            dirs_to_scan.append((snapshot_mount / "TeslaTrackMode", "TrackMode"))

        for scan_dir, archive_prefix in dirs_to_scan:
            if not self.fs.exists(scan_dir):
                continue

            for dirpath, _, filenames in self.fs.walk(scan_dir):
                for filename in filenames:
                    if not filename.lower().endswith(".mp4"):
                        continue

                    src_path = dirpath / filename
                    try:
                        stat = self.fs.stat(src_path)
                    except Exception as e:
                        logger.warning(f"Failed to stat {src_path}: {e}")
                        continue

                    # Skip small files (likely incomplete recordings)
                    if stat.size < self.min_file_size:
                        continue

                    # Calculate relative path for archive
                    rel_to_scan = src_path.relative_to(scan_dir)
                    dst_relative = Path(archive_prefix) / rel_to_scan

                    # Skip already archived files
                    archive_key = str(dst_relative)
                    if archive_key in self._archived_files:
                        continue

                    files.append(FileToArchive(
                        src_path=src_path,
                        dst_relative=dst_relative,
                        size=stat.size,
                    ))

        # Sort by path for deterministic ordering
        files.sort(key=lambda f: str(f.dst_relative))
        return files

    def archive_snapshot(self, handle: SnapshotHandle, mount_path: Path) -> ArchiveResult:
        """Archive all files from an acquired snapshot.

        The snapshot must already be acquired (handle obtained from SnapshotManager).
        This ensures the snapshot cannot be deleted during archiving.

        Args:
            handle: Acquired snapshot handle
            mount_path: Path where snapshot filesystem is mounted

        Returns:
            ArchiveResult with details of the operation
        """
        snapshot = handle.snapshot
        result = ArchiveResult(
            snapshot_id=snapshot.id,
            state=ArchiveState.PENDING,
            started_at=datetime.now(),
        )

        logger.info(f"Starting archive of snapshot {snapshot.id} from {mount_path}")

        # Connect to backend
        result.state = ArchiveState.CONNECTING
        try:
            self.backend.connect()
        except ArchiveConnectionError as e:
            logger.error(f"Failed to connect to archive: {e}")
            result.state = ArchiveState.FAILED
            result.error = str(e)
            result.completed_at = datetime.now()
            return result

        try:
            result.state = ArchiveState.ARCHIVING
            files = self._find_files_to_archive(mount_path)
            result.files_total = len(files)

            logger.info(f"Found {len(files)} files to archive")

            # Archive each file
            for i, file_info in enumerate(files):
                if self.progress_callback:
                    self.progress_callback(i, len(files))

                # Check if already exists in archive (same size)
                existing_size = self.backend.get_file_size(file_info.dst_relative)
                if existing_size == file_info.size:
                    logger.debug(f"Skipping {file_info.dst_relative} (already archived)")
                    result.files_skipped += 1
                    self._archived_files.add(str(file_info.dst_relative))
                    continue

                # Archive the file
                logger.debug(f"Archiving {file_info.src_path} -> {file_info.dst_relative}")
                success = self.backend.archive_file(file_info.src_path, file_info.dst_relative)

                if success:
                    result.files_archived += 1
                    result.bytes_transferred += file_info.size
                    self._archived_files.add(str(file_info.dst_relative))
                else:
                    logger.warning(f"Failed to archive {file_info.src_path}")
                    result.files_failed += 1

            if self.progress_callback:
                self.progress_callback(len(files), len(files))

            result.state = ArchiveState.COMPLETED
            logger.info(
                f"Archive complete: {result.files_archived} archived, "
                f"{result.files_skipped} skipped, {result.files_failed} failed"
            )

        except Exception as e:
            logger.error(f"Archive failed: {e}")
            result.state = ArchiveState.FAILED
            result.error = str(e)

        finally:
            try:
                self.backend.disconnect()
            except Exception as e:
                logger.warning(f"Failed to disconnect from archive: {e}")

        result.completed_at = datetime.now()
        return result

    def archive_new_snapshot(
        self,
        mount_fn: Callable[[Path], Iterator[Path]] | None = None,
    ) -> ArchiveResult:
        """Create a new snapshot, mount it, and archive.

        This is a convenience method that:
        1. Creates a new snapshot
        2. Acquires it
        3. Mounts it (if mount_fn provided)
        4. Archives all files
        5. Unmounts and releases

        Args:
            mount_fn: Context manager function that mounts an image and yields mount path.
                      If None, uses snapshot.path / "mnt" (for testing with mock filesystem).

        Returns:
            ArchiveResult with details of the operation
        """
        snapshot = self.snapshot_manager.create_snapshot()
        handle = self.snapshot_manager.acquire(snapshot.id)

        try:
            if mount_fn:
                # Production: mount the snapshot image
                with mount_fn(snapshot.image_path) as mount_path:
                    return self.archive_snapshot(handle, mount_path)
            else:
                # Testing: assume files are at snapshot.path / "mnt"
                mount_path = snapshot.path / "mnt"
                return self.archive_snapshot(handle, mount_path)
        finally:
            handle.release()

    def clear_archived_cache(self) -> None:
        """Clear the cache of previously archived files.

        Call this if you want to re-archive files that were already archived.
        """
        self._archived_files.clear()
