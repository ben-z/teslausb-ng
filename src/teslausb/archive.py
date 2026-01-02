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
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from threading import Event
from typing import Callable, Iterator

from .filesystem import Filesystem, FilesystemError, RealFilesystem
from .snapshot import SnapshotHandle, SnapshotManager

logger = logging.getLogger(__name__)


class ArchiveState(Enum):
    """State of an archive operation."""

    PENDING = "pending"
    CONNECTING = "connecting"
    ARCHIVING = "archiving"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class ArchivedFile:
    """Information about an archived file for later deletion."""

    relative_path: str  # Path relative to clip directory (e.g., "2024-01-01_12-00-00/front.mp4")
    size: int  # File size in bytes at time of archive


@dataclass
class ArchiveResult:
    """Result of an archive operation."""

    snapshot_id: int
    state: ArchiveState
    files_transferred: int = 0
    bytes_transferred: int = 0
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error: str | None = None
    # Archived files by directory name (e.g., "SavedClips" -> [ArchivedFile, ...])
    archived_files: dict[str, list[ArchivedFile]] = field(default_factory=dict)

    @property
    def success(self) -> bool:
        return self.state == ArchiveState.COMPLETED

    @property
    def duration_seconds(self) -> float | None:
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None


@dataclass
class CopyResult:
    """Result of a directory copy operation."""

    success: bool
    files_transferred: int = 0
    bytes_transferred: int = 0
    error: str | None = None
    # Files that were archived (with sizes for verification before deletion)
    archived_files: list[ArchivedFile] = field(default_factory=list)


class ArchiveBackend(ABC):
    """Abstract base class for archive backends."""

    @abstractmethod
    def is_reachable(self) -> bool:
        """Check if archive destination is reachable."""

    @abstractmethod
    def copy_directory(self, src: Path, dst_name: str) -> CopyResult:
        """Copy a directory to the archive.

        Args:
            src: Source directory path (absolute)
            dst_name: Destination directory name in archive

        Returns:
            CopyResult with transfer details
        """


class MockArchiveBackend(ArchiveBackend):
    """Mock archive backend for testing."""

    def __init__(
        self,
        reachable: bool = True,
        fail_dirs: set[str] | None = None,
    ):
        self.reachable = reachable
        self.fail_dirs = fail_dirs or set()
        self.copied_dirs: list[tuple[Path, str]] = []

    def is_reachable(self) -> bool:
        return self.reachable

    def copy_directory(self, src: Path, dst_name: str) -> CopyResult:
        if dst_name in self.fail_dirs:
            return CopyResult(success=False, error=f"Mock failure for {dst_name}")
        self.copied_dirs.append((src, dst_name))
        return CopyResult(success=True, files_transferred=10, bytes_transferred=1000000)


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
        timeout: int = 3600,
        stop_event: Event | None = None,
        fs: Filesystem | None = None,
    ):
        """Initialize rclone backend.

        Args:
            remote: Rclone remote name (e.g., "gdrive", "s3", "dropbox")
            path: Path within the remote (e.g., "TeslaCam/archive")
            flags: Additional rclone flags (e.g., ["--fast-list"])
            timeout: Timeout for copy operations in seconds
            stop_event: Optional event to signal shutdown
            fs: Filesystem abstraction (for scanning source directories)
        """
        self.remote = remote
        self.path = path.strip("/")
        self.flags = flags or []
        self.timeout = timeout
        self.stop_event = stop_event
        self.fs = fs or RealFilesystem()

    def _dest(self, subpath: str = "") -> str:
        """Build rclone destination path."""
        parts = [p for p in [self.path, subpath] if p]
        path_str = "/".join(parts)
        if path_str:
            return f"{self.remote}:{path_str}"
        return f"{self.remote}:"

    def is_reachable(self) -> bool:
        """Check if rclone remote is reachable."""
        proc = None
        try:
            proc = subprocess.Popen(
                ["rclone", "lsf", f"{self.remote}:", "--max-depth", "1"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            for _ in range(300):  # 30 seconds total
                if self.stop_event and self.stop_event.is_set():
                    return False
                returncode = proc.poll()
                if returncode is not None:
                    stdout, stderr = proc.communicate()
                    if stderr:
                        for line in stderr.decode().splitlines():
                            logger.debug(f"rclone: {line}")
                    return returncode == 0
                time.sleep(0.1)
            return False
        except (OSError, FileNotFoundError):
            return False
        finally:
            if proc is not None:
                proc.kill()
                proc.wait()

    def _scan_directory(self, src: Path) -> list[ArchivedFile]:
        """Scan a directory and collect file info for later deletion verification.

        Args:
            src: Source directory to scan

        Returns:
            List of ArchivedFile with relative paths and sizes
        """
        files: list[ArchivedFile] = []
        try:
            for dirpath, _, filenames in self.fs.walk(src):
                for filename in filenames:
                    full_path = Path(dirpath) / filename
                    try:
                        size = self.fs.stat(full_path).size
                        rel_path = str(full_path.relative_to(src))
                        files.append(ArchivedFile(relative_path=rel_path, size=size))
                    except (OSError, FilesystemError) as e:
                        logger.warning(f"Could not stat {full_path}: {e}")
        except (OSError, FilesystemError) as e:
            logger.warning(f"Could not scan directory {src}: {e}")
        return files

    def copy_directory(self, src: Path, dst_name: str) -> CopyResult:
        """Copy a directory using rclone copy.

        Scans the source directory first to collect file info for later
        deletion verification.
        """
        # Scan files before copying (for deletion verification later)
        archived_files = self._scan_directory(src)
        logger.debug(f"Scanned {len(archived_files)} files in {src}")

        dest = self._dest(dst_name)
        cmd = [
            "rclone", "copy",
            str(src),
            dest,
            "--stats-one-line",
            "-v",
        ] + self.flags

        logger.info(f"Running: {' '.join(cmd)}")

        try:
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=self.timeout,
                check=False,
            )

            # Parse output for stats
            files_transferred = 0
            bytes_transferred = 0
            output = result.stderr.decode() if result.stderr else ""

            for line in output.splitlines():
                logger.debug(f"rclone: {line}")
                # Look for transfer stats in output
                if "Transferred:" in line:
                    # Parse lines like "Transferred: 5 / 5, 100%, 1.234 MiB/s"
                    try:
                        parts = line.split("Transferred:")[1].strip().split(",")
                        if "/" in parts[0]:
                            files_transferred = int(parts[0].split("/")[0].strip())
                    except (IndexError, ValueError):
                        pass

            if result.returncode != 0:
                error_msg = output.strip().split("\n")[-1] if output else "Unknown error"
                logger.error(f"rclone copy failed: {error_msg}")
                return CopyResult(success=False, error=error_msg)

            return CopyResult(
                success=True,
                files_transferred=files_transferred,
                bytes_transferred=bytes_transferred,
                archived_files=archived_files,
            )

        except subprocess.TimeoutExpired:
            logger.error(f"rclone timeout copying {src}")
            return CopyResult(success=False, error="Timeout")
        except (OSError, FileNotFoundError) as e:
            logger.error(f"rclone error: {e}")
            return CopyResult(success=False, error=str(e))


class ArchiveManager:
    """Manages archiving footage from snapshots.

    Coordinates with SnapshotManager to:
    1. Acquire snapshot (locks it from deletion)
    2. Archive clip directories
    3. Delete archived files from live cam_disk
    4. Release snapshot
    """

    # Directory name to TeslaCam path mapping
    DIR_TO_PATH: dict[str, str] = {
        "SavedClips": "TeslaCam/SavedClips",
        "SentryClips": "TeslaCam/SentryClips",
        "RecentClips": "TeslaCam/RecentClips",
        "TrackMode": "TeslaTrackMode",
    }

    def __init__(
        self,
        fs: Filesystem,
        snapshot_manager: SnapshotManager,
        backend: ArchiveBackend,
        cam_disk_path: Path | None = None,
        archive_recent: bool = False,
        archive_saved: bool = True,
        archive_sentry: bool = True,
        archive_track: bool = True,
    ):
        """Initialize ArchiveManager.

        Args:
            fs: Filesystem abstraction
            snapshot_manager: SnapshotManager instance
            backend: Archive backend to use
            cam_disk_path: Path to cam_disk.bin (for deleting archived files)
            archive_recent: Whether to archive RecentClips
            archive_saved: Whether to archive SavedClips
            archive_sentry: Whether to archive SentryClips
            archive_track: Whether to archive TrackMode clips
        """
        self.fs = fs
        self.snapshot_manager = snapshot_manager
        self.backend = backend
        self.cam_disk_path = cam_disk_path
        self.archive_recent = archive_recent
        self.archive_saved = archive_saved
        self.archive_sentry = archive_sentry
        self.archive_track = archive_track

    def _get_dirs_to_archive(self, snapshot_mount: Path) -> list[tuple[Path, str]]:
        """Get list of directories to archive.

        Args:
            snapshot_mount: Path where snapshot is mounted

        Returns:
            List of (source_path, dest_name) tuples
        """
        dirs: list[tuple[Path, str]] = []

        if self.archive_saved:
            path = snapshot_mount / "TeslaCam" / "SavedClips"
            if self.fs.exists(path):
                dirs.append((path, "SavedClips"))

        if self.archive_sentry:
            path = snapshot_mount / "TeslaCam" / "SentryClips"
            if self.fs.exists(path):
                dirs.append((path, "SentryClips"))

        if self.archive_recent:
            path = snapshot_mount / "TeslaCam" / "RecentClips"
            if self.fs.exists(path):
                dirs.append((path, "RecentClips"))

        if self.archive_track:
            path = snapshot_mount / "TeslaTrackMode"
            if self.fs.exists(path):
                dirs.append((path, "TrackMode"))

        return dirs

    def archive_snapshot(self, handle: SnapshotHandle, mount_path: Path) -> ArchiveResult:
        """Archive all clip directories from a snapshot.

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

        # Check reachability
        result.state = ArchiveState.CONNECTING
        if not self.backend.is_reachable():
            logger.error("Archive backend not reachable")
            result.state = ArchiveState.FAILED
            result.error = "Archive not reachable"
            result.completed_at = datetime.now()
            return result

        result.state = ArchiveState.ARCHIVING
        dirs_to_archive = self._get_dirs_to_archive(mount_path)

        if not dirs_to_archive:
            logger.info("No directories to archive")
            result.state = ArchiveState.COMPLETED
            result.completed_at = datetime.now()
            return result

        logger.info(f"Archiving {len(dirs_to_archive)} directories")

        total_files = 0
        total_bytes = 0
        errors: list[str] = []

        for src_path, dst_name in dirs_to_archive:
            logger.info(f"Archiving {dst_name}...")
            copy_result = self.backend.copy_directory(src_path, dst_name)

            if copy_result.success:
                total_files += copy_result.files_transferred
                total_bytes += copy_result.bytes_transferred
                # Track archived files for deletion (only for successful directories)
                if copy_result.archived_files:
                    result.archived_files[dst_name] = copy_result.archived_files
                logger.info(f"  {dst_name}: {copy_result.files_transferred} files")
            else:
                errors.append(f"{dst_name}: {copy_result.error}")
                logger.error(f"  {dst_name}: failed - {copy_result.error}")

        result.files_transferred = total_files
        result.bytes_transferred = total_bytes
        result.completed_at = datetime.now()

        if errors:
            result.state = ArchiveState.FAILED
            result.error = "; ".join(errors)
        else:
            result.state = ArchiveState.COMPLETED

        logger.info(f"Archive complete: {total_files} files transferred")

        return result

    def delete_archived_files(
        self,
        result: ArchiveResult,
        cam_disk_mount: Path,
    ) -> tuple[int, int]:
        """Delete archived files from the live cam_disk.

        Before deleting each file, verifies that the file size matches what was
        archived. This catches edge cases where files might have been modified
        (e.g., if Tesla's behavior changes).

        Args:
            result: ArchiveResult containing the list of archived files
            cam_disk_mount: Path where cam_disk is mounted (read-write)

        Returns:
            Tuple of (files_deleted, files_skipped)
        """
        deleted = 0
        skipped = 0

        for dir_name, files in result.archived_files.items():
            # Map directory name to path on disk
            dir_path = self.DIR_TO_PATH.get(dir_name)
            if not dir_path:
                logger.warning(f"Unknown directory name: {dir_name}")
                continue

            base_path = cam_disk_mount / dir_path

            for archived_file in files:
                file_path = base_path / archived_file.relative_path

                # Check if file exists
                if not self.fs.exists(file_path):
                    logger.debug(f"File already deleted: {file_path}")
                    skipped += 1
                    continue

                # Verify file size matches (safety check)
                try:
                    current_size = self.fs.stat(file_path).size
                    if current_size != archived_file.size:
                        logger.warning(
                            f"File size mismatch for {file_path}: "
                            f"archived={archived_file.size}, current={current_size}. "
                            f"Skipping deletion."
                        )
                        skipped += 1
                        continue
                except (OSError, FilesystemError) as e:
                    logger.warning(f"Could not stat {file_path}: {e}")
                    skipped += 1
                    continue

                # Delete the file
                try:
                    self.fs.remove(file_path)
                    deleted += 1
                    logger.debug(f"Deleted: {file_path}")
                except (OSError, FilesystemError) as e:
                    logger.warning(f"Could not delete {file_path}: {e}")
                    skipped += 1

            # Clean up empty directories
            self._cleanup_empty_dirs(base_path)

        logger.info(f"Deleted {deleted} files, skipped {skipped}")
        return deleted, skipped

    def _cleanup_empty_dirs(self, base_path: Path) -> None:
        """Remove empty directories under base_path.

        Walks the directory tree bottom-up and removes empty directories.
        """
        if not self.fs.exists(base_path):
            return

        # Collect all directories, then sort by depth (deepest first)
        dirs_to_check: list[Path] = []
        try:
            for dirpath, dirnames, filenames in self.fs.walk(base_path):
                for dirname in dirnames:
                    dirs_to_check.append(Path(dirpath) / dirname)
        except (OSError, FilesystemError):
            return

        # Sort by path length descending (deepest first)
        dirs_to_check.sort(key=lambda p: len(p.parts), reverse=True)

        for dir_path in dirs_to_check:
            try:
                # Check if directory is empty
                if self.fs.exists(dir_path) and not any(self.fs.listdir(dir_path)):
                    self.fs.rmdir(dir_path)
                    logger.debug(f"Removed empty directory: {dir_path}")
            except (OSError, FilesystemError):
                pass  # Directory not empty or other error, skip

    def archive_new_snapshot(
        self,
        mount_fn: Callable[[Path], Iterator[Path]],
        delete_after_archive: bool = True,
    ) -> ArchiveResult:
        """Create a new snapshot, mount it, archive, and optionally delete archived files.

        Args:
            mount_fn: Context manager function that mounts an image and yields mount path.
            delete_after_archive: If True and cam_disk_path is set, delete archived
                files from cam_disk after successful archive.

        Returns:
            ArchiveResult with details of the operation
        """
        from .mount import mount_image

        snapshot = self.snapshot_manager.create_snapshot()
        handle = self.snapshot_manager.acquire(snapshot.id)

        try:
            with mount_fn(snapshot.image_path) as mount_path:
                result = self.archive_snapshot(handle, mount_path)

            # Delete archived files from cam_disk if configured and archive succeeded
            if (
                delete_after_archive
                and self.cam_disk_path
                and result.success
                and result.archived_files
            ):
                logger.info("Deleting archived files from cam_disk...")
                try:
                    with mount_image(self.cam_disk_path, readonly=False) as cam_mount:
                        deleted, skipped = self.delete_archived_files(result, cam_mount)
                        logger.info(f"Cleanup complete: {deleted} deleted, {skipped} skipped")
                except Exception as e:
                    # Don't fail the archive if cleanup fails - files will be re-archived next time
                    logger.error(f"Failed to delete archived files: {e}")

            return result
        finally:
            handle.release()
