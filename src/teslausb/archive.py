"""Archive management for TeslaUSB.

This module provides:
- ArchiveBackend: Abstract base class for archive backends
- RcloneBackend: Archive using rclone (supports 40+ cloud providers)
- ArchiveManager: Coordinates archiving from snapshots
"""

from __future__ import annotations

import json
import logging
import subprocess
import tempfile
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from threading import Event

from .filesystem import Filesystem, FilesystemError, RealFilesystem
from .snapshot import SnapshotHandle, SnapshotManager

logger = logging.getLogger(__name__)


def format_size(num_bytes: int | float) -> str:
    """Format a byte count as a human-readable string (e.g. '2.3 GiB')."""
    value = float(num_bytes)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if abs(value) < 1024 or unit == "GiB":
            precision = 0 if value % 1 == 0 else 1
            return f"{value:.{precision}f} {unit}"
        value /= 1024
    # Unreachable: the loop always returns at "GiB"
    return f"{value:.1f} GiB"


class ArchiveCommandInterruptedError(Exception):
    """A stopped command with the output captured before its child was reaped."""

    def __init__(self, reason: str, stdout: bytes, stderr: bytes) -> None:
        super().__init__(reason)
        self.stdout = stdout
        self.stderr = stderr


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
    mtime: float  # Modification time as reported by the mounted snapshot


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
class EventObservation:
    """An event's immutable file signature and the time it was first observed."""

    files: tuple[tuple[str, int, float], ...]
    first_seen: float


@dataclass
class CopyResult:
    """Result of a directory copy operation."""

    success: bool
    files_transferred: int = 0
    bytes_transferred: int = 0
    error: str | None = None
    # Files confirmed present in the archive and safe to consider for deletion.
    # On failed copies this may be a partial list parsed from rclone's structured output.
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

    def _remote_with_colon(self) -> str:
        """Get remote name with exactly one trailing colon."""
        if self.remote.endswith(":"):
            return self.remote
        return f"{self.remote}:"

    def _dest(self, subpath: str = "") -> str:
        """Build rclone destination path."""
        parts = [p for p in [self.path, subpath] if p]
        path_str = "/".join(parts)
        remote = self._remote_with_colon()
        if path_str:
            return f"{remote}{path_str}"
        return remote

    def _run_command(
        self, cmd: list[str], timeout: float, input_data: bytes | None
    ) -> subprocess.CompletedProcess[bytes]:
        """Drain subprocess streams while honoring cancellation and the deadline."""
        if self.stop_event and self.stop_event.is_set():
            raise ArchiveCommandInterruptedError("Stopped", b"", b"")
        # communicate retries cannot resume an unfinished stdin pipe write.
        with tempfile.TemporaryFile() as input_stream:
            if input_data is not None:
                input_stream.write(input_data)
                input_stream.seek(0)
            with subprocess.Popen(
                cmd, stdin=input_stream, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            ) as proc:
                deadline = time.monotonic() + timeout
                try:
                    while True:
                        remaining = deadline - time.monotonic()
                        stopped = self.stop_event is not None and self.stop_event.is_set()
                        if stopped or remaining <= 0:
                            proc.kill()
                            stdout, stderr = proc.communicate()
                            reason = "Stopped" if stopped else "Timeout"
                            raise ArchiveCommandInterruptedError(reason, stdout, stderr)
                        try:
                            stdout, stderr = proc.communicate(timeout=min(0.1, remaining))
                            return subprocess.CompletedProcess(cmd, proc.returncode, stdout, stderr)
                        except subprocess.TimeoutExpired:
                            continue
                finally:
                    if proc.poll() is None:
                        proc.kill()
                        proc.communicate()

    def is_reachable(self) -> bool:
        """Check the archive root while draining output and honoring stop requests."""
        try:
            result = self._run_command(
                ["rclone", "lsf", self._remote_with_colon(), "--max-depth", "1"],
                timeout=30,
                input_data=None,
            )
        except ArchiveCommandInterruptedError as error:
            logger.warning("Archive reachability check interrupted: %s", error)
            return False
        if result.returncode != 0:
            logger.warning(
                "Archive reachability check failed: %s",
                result.stderr.decode(errors="replace").strip(),
            )
        return result.returncode == 0

    def _scan_directory(self, src: Path) -> list[ArchivedFile]:
        """Scan a directory and collect file info for later deletion verification.

        Args:
            src: Source directory to scan

        Returns:
            List of ArchivedFile with relative paths and sizes
        """
        files: list[ArchivedFile] = []
        for dirpath, _, filenames in self.fs.walk(src):
            for filename in filenames:
                full_path = Path(dirpath) / filename
                stat = self.fs.stat(full_path)
                rel_path = str(full_path.relative_to(src))
                files.append(ArchivedFile(relative_path=rel_path, size=stat.size, mtime=stat.mtime))
        return files

    def _decode_output(self, output: bytes | str | None) -> str:
        """Decode subprocess output."""
        if output is None:
            return ""
        if isinstance(output, str):
            return output
        return output.decode(errors="replace")

    def _combined_output(self, *outputs: bytes | str | None) -> str:
        """Combine subprocess output streams into one log string."""
        return "\n".join(text for output in outputs if (text := self._decode_output(output)))

    def _rclone_json_records(self, output: str) -> Iterator[dict[str, object]]:
        """Yield structured rclone log records from line-delimited JSON output."""
        for raw_line in output.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                logger.debug(f"Ignoring non-JSON rclone log line: {line}")
                continue
            if isinstance(record, dict):
                yield record

    def _parse_rclone_paths(self, output: str, message_prefixes: tuple[str, ...]) -> set[str]:
        """Parse relative file paths from structured rclone log records.

        With --use-json-log, rclone emits one JSON record per line. File-level
        records include fields like:
            {"object": "event/front.mp4", "msg": "Copied (new)"}
            {"object": "event/front.mp4", "msg": "Unchanged skipping"}

        Only objects whose message starts with one of the supplied prefixes are
        returned.
        """
        paths: set[str] = set()

        for record in self._rclone_json_records(output):
            message = record.get("msg")
            rel_path = record.get("object")
            if not isinstance(message, str) or not isinstance(rel_path, str):
                continue
            if not any(message.startswith(prefix) for prefix in message_prefixes):
                continue
            rel_path = rel_path.strip().lstrip("/")
            if rel_path:
                paths.add(rel_path)

        return paths

    def _rclone_error_message(self, output: str) -> str:
        """Extract a useful error message from rclone JSON logs."""
        fallback = output.strip().splitlines()[-1] if output.strip() else "Unknown error"
        last_message: str | None = None
        last_error: str | None = None

        for record in self._rclone_json_records(output):
            message = record.get("msg")
            if not isinstance(message, str) or not message.strip():
                continue
            clean_message = " ".join(message.split())
            last_message = clean_message
            level = record.get("level")
            if isinstance(level, str) and level.lower() in {"error", "fatal"}:
                last_error = clean_message

        return last_error or last_message or fallback

    def _select_archived_files(
        self,
        files: list[ArchivedFile],
        relative_paths: set[str],
    ) -> list[ArchivedFile]:
        """Select scanned files whose relative paths were confirmed by rclone."""
        if not relative_paths:
            return []

        by_path = {file.relative_path: file for file in files}
        selected: list[ArchivedFile] = []
        for rel_path in sorted(relative_paths):
            archived_file = by_path.get(rel_path)
            if archived_file:
                selected.append(archived_file)
            else:
                logger.debug(f"rclone reported unknown archived file: {rel_path}")
        return selected

    def copy_directory(self, src: Path, dst_name: str) -> CopyResult:
        """Archive RecentClips by recording date and other directories as-is."""
        files = self._scan_directory(src)
        if dst_name != "RecentClips":
            return self._copy_files(src, dst_name, files)

        batches: dict[str, list[ArchivedFile]] = {}
        for file in files:
            name = file.relative_path
            if name in {"thumb.png", "event.json"}:
                batches.setdefault("metadata", []).append(file)
                continue
            try:
                recording_date = date.fromisoformat(name[:10])
                if len(name) < 12 or name[10] != "_" or "/" in name:
                    raise ValueError("expected YYYY-MM-DD_<clip name>")
            except ValueError as error:
                raise ValueError(f"Cannot date-partition RecentClips file {name!r}") from error
            batches.setdefault(recording_date.isoformat(), []).append(file)

        result = CopyResult(success=True)
        errors: list[str] = []
        for day, batch in sorted(batches.items()):
            copied = self._copy_files(src, f"RecentClips/{day}", batch)
            result.files_transferred += copied.files_transferred
            result.bytes_transferred += copied.bytes_transferred
            result.archived_files.extend(copied.archived_files)
            if not copied.success:
                errors.append(f"{day}: {copied.error}")
                if self.stop_event and self.stop_event.is_set():
                    break
        if errors:
            result.success = False
            result.error = "; ".join(errors)
        return result

    def _copy_files(
        self, src: Path, dst_name: str, archived_files: list[ArchivedFile]
    ) -> CopyResult:
        """Copy the scanned files and retain only positive archive confirmations."""
        if not archived_files:
            return CopyResult(success=True)
        if any("\n" in file.relative_path or "\r" in file.relative_path for file in archived_files):
            raise ValueError(f"Cannot archive filenames containing line breaks in {src}")
        dest = self._dest(dst_name)
        cmd = [
            "rclone",
            "copy",
            str(src),
            dest,
            *self.flags,
            "--stats-one-line",
            "--use-json-log",
            "--log-level",
            "DEBUG",
            "--files-from-raw",
            "-",
            "--no-traverse",
        ]

        logger.info(f"Running: {' '.join(cmd)}")

        error: str | None = None
        try:
            result = self._run_command(
                cmd,
                timeout=self.timeout,
                input_data="\n".join(file.relative_path for file in archived_files).encode(),
            )
            output = self._combined_output(result.stdout, result.stderr)
            if result.returncode != 0:
                error = self._rclone_error_message(output)
        except ArchiveCommandInterruptedError as interrupted:
            output = self._combined_output(interrupted.stdout, interrupted.stderr)
            error = str(interrupted)
        except OSError as exception:
            logger.error("rclone error: %s", exception)
            return CopyResult(success=False, error=str(exception))

        for line in output.splitlines():
            logger.debug("rclone: %s", line)
        copied_files = self._select_archived_files(
            archived_files, self._parse_rclone_paths(output, ("Copied",))
        )
        confirmed_files = self._select_archived_files(
            archived_files, self._parse_rclone_paths(output, ("Copied", "Unchanged skipping"))
        )
        if error:
            logger.error("rclone copy failed for %s: %s", src, error)
        return CopyResult(
            success=error is None,
            files_transferred=len(copied_files),
            bytes_transferred=sum(file.size for file in copied_files),
            error=error,
            archived_files=confirmed_files,
        )


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
        "Photobooth": "TeslaCam/Photobooth",
        "TrackMode": "TeslaTrackMode",
    }

    def __init__(
        self,
        fs: Filesystem,
        snapshot_manager: SnapshotManager,
        backend: ArchiveBackend,
        event_stability_seconds: float,
        cam_disk_path: Path | None = None,
        archive_recent: bool = False,
        archive_saved: bool = True,
        archive_sentry: bool = True,
        archive_track: bool = True,
        archive_photobooth: bool = True,
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
            archive_photobooth: Whether to archive Photobooth selfies
        """
        self.fs = fs
        self.snapshot_manager = snapshot_manager
        self.backend = backend
        self.event_stability_seconds = event_stability_seconds
        self._event_observations: dict[tuple[str, str], EventObservation] = {}
        self.cam_disk_path = cam_disk_path
        self.archive_recent = archive_recent
        self.archive_saved = archive_saved
        self.archive_sentry = archive_sentry
        self.archive_track = archive_track
        self.archive_photobooth = archive_photobooth

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

        if self.archive_photobooth:
            path = snapshot_mount / "TeslaCam" / "Photobooth"
            if self.fs.exists(path):
                dirs.append((path, "Photobooth"))

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
            total_files += copy_result.files_transferred
            total_bytes += copy_result.bytes_transferred
            deletable = self._deletable_files(src_path, dst_name, copy_result.archived_files)
            if deletable:
                result.archived_files[dst_name] = deletable

            if copy_result.success:
                logger.info(
                    f"  {dst_name}: transferred {copy_result.files_transferred} files"
                    f" ({format_size(copy_result.bytes_transferred)})"
                )
            else:
                if copy_result.archived_files:
                    logger.info(
                        f"  {dst_name}: confirmed {len(copy_result.archived_files)} "
                        "files before failure"
                    )
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

        logger.info(f"Archive complete: {total_files} files, {format_size(total_bytes)}")

        return result

    def _deletable_files(
        self, src: Path, directory: str, files: list[ArchivedFile]
    ) -> list[ArchivedFile]:
        """Remove only complete events observed unchanged across archive cycles.

        FAT timestamps use the car's local time. Elapsed stability is measured
        with the host's monotonic clock, independent of either clock's timezone.
        """
        if directory == "RecentClips":
            return []
        if directory not in {"SavedClips", "SentryClips"}:
            return files

        events: dict[str, list[tuple[str, int, float]]] = {}
        for parent, _, names in self.fs.walk(src):
            for name in names:
                path = parent / name
                relative = str(path.relative_to(src))
                stat = self.fs.stat(path)
                event = relative.split("/", 1)[0]
                events.setdefault(event, []).append((relative, stat.size, stat.mtime))

        for key in list(self._event_observations):
            if key[0] == directory and key[1] not in events:
                del self._event_observations[key]

        confirmed = {file.relative_path: file for file in files}
        deletable: list[ArchivedFile] = []
        now = time.monotonic()
        for event, entries in events.items():
            signature = tuple(sorted(entries))
            key = (directory, event)
            observation = self._event_observations.get(key)
            if observation is None or observation.files != signature:
                observation = EventObservation(signature, now)
                self._event_observations[key] = observation
            if now - observation.first_seen < self.event_stability_seconds:
                continue
            if not any(
                Path(path).suffix.lower() == ".mp4" and size > 0 for path, size, _ in entries
            ):
                logger.warning("Preserving %s/%s: event has no video", directory, event)
                continue
            if all(path in confirmed and confirmed[path].size == size for path, size, _ in entries):
                deletable.extend(confirmed[path] for path, _, _ in entries)
        return deletable

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
            if dir_name == "RecentClips":
                skipped += len(files)
                continue
            # Map directory name to path on disk
            dir_path = self.DIR_TO_PATH.get(dir_name)
            if not dir_path:
                logger.warning(f"Unknown directory name: {dir_name}")
                continue

            base_path = cam_disk_mount / dir_path
            if dir_name in {"SavedClips", "SentryClips"}:
                events: dict[str, list[ArchivedFile]] = {}
                for file in files:
                    events.setdefault(file.relative_path.split("/", 1)[0], []).append(file)
                files = []
                for event, event_files in events.items():
                    expected = sorted(
                        (file.relative_path, file.size, file.mtime) for file in event_files
                    )
                    current = []
                    for parent, _, names in self.fs.walk(base_path / event):
                        for name in names:
                            path = parent / name
                            stat = self.fs.stat(path)
                            current.append(
                                (str(path.relative_to(base_path)), stat.size, stat.mtime)
                            )
                    has_video = any(
                        Path(file.relative_path).suffix.lower() == ".mp4" and file.size > 0
                        for file in event_files
                    )
                    if sorted(current) != expected or not has_video:
                        logger.info(
                            "Preserving %s/%s: event changed or has no video", dir_name, event
                        )
                        skipped += len(event_files)
                        continue
                    files.extend(event_files)

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
            for dirpath, dirnames, _filenames in self.fs.walk(base_path):
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
        mount_fn: Callable[[Path], AbstractContextManager[Path]],
    ) -> ArchiveResult:
        """Create a snapshot and archive its contents.

        The coordinator owns deletion from the live disk while the USB gadget
        is disconnected.
        """
        snapshot = self.snapshot_manager.create_snapshot()
        with (
            self.snapshot_manager.acquire(snapshot.id) as handle,
            mount_fn(snapshot.image_path) as mount_path,
        ):
            return self.archive_snapshot(handle, mount_path)
