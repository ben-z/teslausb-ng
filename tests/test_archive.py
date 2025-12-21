"""Tests for archive management."""

from pathlib import Path

import pytest

from teslausb.archive import (
    ArchiveManager,
    ArchiveResult,
    ArchiveState,
    MockArchiveBackend,
    RcloneBackend,
    FileToArchive,
)
from teslausb.filesystem import MockFilesystem
from teslausb.snapshot import SnapshotManager


class TestMockArchiveBackend:
    """Tests for MockArchiveBackend."""

    def test_is_reachable(self):
        """Test reachability check."""
        backend = MockArchiveBackend(reachable=True)
        assert backend.is_reachable()

        backend = MockArchiveBackend(reachable=False)
        assert not backend.is_reachable()

    def test_connect_when_reachable(self):
        """Test connecting when reachable."""
        backend = MockArchiveBackend(reachable=True)

        backend.connect()
        assert backend.connected

    def test_connect_when_unreachable(self):
        """Test connecting when unreachable raises error."""
        from teslausb.archive import ArchiveConnectionError

        backend = MockArchiveBackend(reachable=False)

        with pytest.raises(ArchiveConnectionError):
            backend.connect()

    def test_disconnect(self):
        """Test disconnecting."""
        backend = MockArchiveBackend(reachable=True)
        backend.connect()
        assert backend.connected

        backend.disconnect()
        assert not backend.connected

    def test_archive_file(self):
        """Test archiving a file."""
        backend = MockArchiveBackend(reachable=True)
        backend.connect()

        success = backend.archive_file(
            src=Path("/some/file.mp4"),
            dst_relative=Path("SavedClips/event/file.mp4"),
        )

        assert success
        assert backend.file_exists(Path("SavedClips/event/file.mp4"))

    def test_archive_file_fails(self):
        """Test archiving a file that should fail."""
        backend = MockArchiveBackend(
            reachable=True,
            fail_files={"SavedClips/bad.mp4"},
        )
        backend.connect()

        success = backend.archive_file(
            src=Path("/some/bad.mp4"),
            dst_relative=Path("SavedClips/bad.mp4"),
        )

        assert not success

    def test_file_exists(self):
        """Test checking file existence."""
        backend = MockArchiveBackend(reachable=True)
        backend.connect()

        assert not backend.file_exists(Path("nonexistent.mp4"))

        backend.archive_file(Path("/src"), Path("test.mp4"))
        assert backend.file_exists(Path("test.mp4"))

    def test_get_file_size(self):
        """Test getting file size."""
        backend = MockArchiveBackend(reachable=True)
        backend.connect()

        assert backend.get_file_size(Path("nonexistent.mp4")) is None

        backend.archive_file(Path("/src"), Path("test.mp4"))
        size = backend.get_file_size(Path("test.mp4"))
        assert size is not None
        assert size > 0


class TestArchiveManager:
    """Tests for ArchiveManager."""

    def test_find_files_to_archive(self, mock_fs_with_teslacam: MockFilesystem):
        """Test finding files to archive."""
        snapshot_manager = SnapshotManager(
            fs=mock_fs_with_teslacam,
            cam_disk_path=Path("/backingfiles/cam_disk.bin"),
            snapshots_path=Path("/backingfiles/snapshots"),
        )

        backend = MockArchiveBackend(reachable=True)

        manager = ArchiveManager(
            fs=mock_fs_with_teslacam,
            snapshot_manager=snapshot_manager,
            backend=backend,
            min_file_size=100_000,  # 100KB
        )

        # Find files in the pre-created snapshot structure
        snapshot_mount = Path("/backingfiles/snapshots/snap-000000/mnt")
        files = manager._find_files_to_archive(snapshot_mount)

        # Should find 3 files (2 in SavedClips, 1 in SentryClips)
        # The small file should be skipped
        assert len(files) == 3

        # Check file paths
        paths = [str(f.dst_relative) for f in files]
        assert any("SavedClips" in p for p in paths)
        assert any("SentryClips" in p for p in paths)

    def test_find_files_respects_settings(self, mock_fs_with_teslacam: MockFilesystem):
        """Test that file finding respects archive settings."""
        snapshot_manager = SnapshotManager(
            fs=mock_fs_with_teslacam,
            cam_disk_path=Path("/backingfiles/cam_disk.bin"),
            snapshots_path=Path("/backingfiles/snapshots"),
        )

        backend = MockArchiveBackend(reachable=True)

        # Only archive SavedClips
        manager = ArchiveManager(
            fs=mock_fs_with_teslacam,
            snapshot_manager=snapshot_manager,
            backend=backend,
            archive_saved=True,
            archive_sentry=False,
            archive_recent=False,
            archive_track=False,
        )

        snapshot_mount = Path("/backingfiles/snapshots/snap-000000/mnt")
        files = manager._find_files_to_archive(snapshot_mount)

        # Should only find SavedClips files
        assert len(files) == 2  # 2 large files in SavedClips
        assert all("SavedClips" in str(f.dst_relative) for f in files)

    def test_archive_snapshot(self, mock_fs_with_teslacam: MockFilesystem):
        """Test archiving a snapshot."""
        snapshot_manager = SnapshotManager(
            fs=mock_fs_with_teslacam,
            cam_disk_path=Path("/backingfiles/cam_disk.bin"),
            snapshots_path=Path("/backingfiles/snapshots"),
        )

        # Create a snapshot that matches the pre-created structure
        mock_fs_with_teslacam.mkdir(Path("/backingfiles/snapshots/snap-000000"), exist_ok=True)
        mock_fs_with_teslacam.write_text(
            Path("/backingfiles/snapshots/snap-000000/snap.bin"),
            "mock"
        )
        mock_fs_with_teslacam.write_text(
            Path("/backingfiles/snapshots/snap-000000/snap.toc"),
            ""
        )

        # Reload to pick up the snapshot
        snapshot_manager = SnapshotManager(
            fs=mock_fs_with_teslacam,
            cam_disk_path=Path("/backingfiles/cam_disk.bin"),
            snapshots_path=Path("/backingfiles/snapshots"),
        )

        backend = MockArchiveBackend(reachable=True)

        manager = ArchiveManager(
            fs=mock_fs_with_teslacam,
            snapshot_manager=snapshot_manager,
            backend=backend,
            min_file_size=100_000,
        )

        # Get the snapshot and acquire it
        snapshots = snapshot_manager.get_snapshots()
        assert len(snapshots) == 1

        handle = snapshot_manager.acquire(snapshots[0].id)

        try:
            mount_path = Path("/backingfiles/snapshots/snap-000000/mnt")
            result = manager.archive_snapshot(handle, mount_path)

            assert result.state == ArchiveState.COMPLETED
            assert result.files_archived == 3
            assert result.files_failed == 0
        finally:
            handle.release()

    def test_archive_snapshot_skips_existing(self, mock_fs_with_teslacam: MockFilesystem):
        """Test that archive skips files that already exist with same size."""
        snapshot_manager = SnapshotManager(
            fs=mock_fs_with_teslacam,
            cam_disk_path=Path("/backingfiles/cam_disk.bin"),
            snapshots_path=Path("/backingfiles/snapshots"),
        )

        # Set up snapshot
        mock_fs_with_teslacam.mkdir(Path("/backingfiles/snapshots/snap-000000"), exist_ok=True)
        mock_fs_with_teslacam.write_text(
            Path("/backingfiles/snapshots/snap-000000/snap.bin"),
            "mock"
        )
        mock_fs_with_teslacam.write_text(
            Path("/backingfiles/snapshots/snap-000000/snap.toc"),
            ""
        )

        snapshot_manager = SnapshotManager(
            fs=mock_fs_with_teslacam,
            cam_disk_path=Path("/backingfiles/cam_disk.bin"),
            snapshots_path=Path("/backingfiles/snapshots"),
        )

        backend = MockArchiveBackend(reachable=True)

        # Pre-archive one file (simulating it already being in archive)
        # The file has 500000 bytes
        backend.archived_files[Path("SavedClips/2024-01-15_10-30-00/2024-01-15_10-30-00-front.mp4")] = b"x" * 500_000

        manager = ArchiveManager(
            fs=mock_fs_with_teslacam,
            snapshot_manager=snapshot_manager,
            backend=backend,
            min_file_size=100_000,
        )

        snapshots = snapshot_manager.get_snapshots()
        handle = snapshot_manager.acquire(snapshots[0].id)

        try:
            mount_path = Path("/backingfiles/snapshots/snap-000000/mnt")
            result = manager.archive_snapshot(handle, mount_path)

            assert result.state == ArchiveState.COMPLETED
            # 2 archived + 1 skipped = 3 total
            assert result.files_skipped == 1
            assert result.files_archived == 2
        finally:
            handle.release()

    def test_archive_handles_backend_failure(self, mock_fs_with_teslacam: MockFilesystem):
        """Test archive handles file transfer failures."""
        snapshot_manager = SnapshotManager(
            fs=mock_fs_with_teslacam,
            cam_disk_path=Path("/backingfiles/cam_disk.bin"),
            snapshots_path=Path("/backingfiles/snapshots"),
        )

        # Set up snapshot
        mock_fs_with_teslacam.mkdir(Path("/backingfiles/snapshots/snap-000000"), exist_ok=True)
        mock_fs_with_teslacam.write_text(
            Path("/backingfiles/snapshots/snap-000000/snap.bin"),
            "mock"
        )
        mock_fs_with_teslacam.write_text(
            Path("/backingfiles/snapshots/snap-000000/snap.toc"),
            ""
        )

        snapshot_manager = SnapshotManager(
            fs=mock_fs_with_teslacam,
            cam_disk_path=Path("/backingfiles/cam_disk.bin"),
            snapshots_path=Path("/backingfiles/snapshots"),
        )

        # Backend that fails on specific files
        backend = MockArchiveBackend(
            reachable=True,
            fail_files={"SavedClips/2024-01-15_10-30-00/2024-01-15_10-30-00-front.mp4"},
        )

        manager = ArchiveManager(
            fs=mock_fs_with_teslacam,
            snapshot_manager=snapshot_manager,
            backend=backend,
            min_file_size=100_000,
        )

        snapshots = snapshot_manager.get_snapshots()
        handle = snapshot_manager.acquire(snapshots[0].id)

        try:
            mount_path = Path("/backingfiles/snapshots/snap-000000/mnt")
            result = manager.archive_snapshot(handle, mount_path)

            # Should complete but with one failure
            assert result.state == ArchiveState.COMPLETED
            assert result.files_failed == 1
            assert result.files_archived == 2
        finally:
            handle.release()

    def test_clear_archived_cache(self, mock_fs: MockFilesystem):
        """Test clearing the archived files cache."""
        snapshot_manager = SnapshotManager(
            fs=mock_fs,
            cam_disk_path=Path("/backingfiles/cam_disk.bin"),
            snapshots_path=Path("/backingfiles/snapshots"),
        )

        backend = MockArchiveBackend(reachable=True)

        manager = ArchiveManager(
            fs=mock_fs,
            snapshot_manager=snapshot_manager,
            backend=backend,
        )

        # Add some entries to the cache
        manager._archived_files.add("file1.mp4")
        manager._archived_files.add("file2.mp4")

        assert len(manager._archived_files) == 2

        manager.clear_archived_cache()

        assert len(manager._archived_files) == 0


class TestArchiveResult:
    """Tests for ArchiveResult."""

    def test_success_property(self):
        """Test success property."""
        result = ArchiveResult(
            snapshot_id=1,
            state=ArchiveState.COMPLETED,
        )
        assert result.success

        result = ArchiveResult(
            snapshot_id=1,
            state=ArchiveState.FAILED,
        )
        assert not result.success

    def test_duration(self):
        """Test duration calculation."""
        from datetime import datetime, timedelta

        start = datetime.now()
        end = start + timedelta(seconds=120)

        result = ArchiveResult(
            snapshot_id=1,
            state=ArchiveState.COMPLETED,
            started_at=start,
            completed_at=end,
        )

        assert result.duration_seconds == 120.0

    def test_duration_none_when_incomplete(self):
        """Test duration is None when not completed."""
        result = ArchiveResult(
            snapshot_id=1,
            state=ArchiveState.ARCHIVING,
        )

        assert result.duration_seconds is None


class TestRcloneBackend:
    """Tests for RcloneBackend destination path building."""

    def test_dest_remote_only(self):
        """Test destination with remote name only."""
        backend = RcloneBackend(remote="gdrive")
        assert backend._dest() == "gdrive:"
        assert backend._dest("") == "gdrive:"

    def test_dest_with_path(self):
        """Test destination with remote and path."""
        backend = RcloneBackend(remote="gdrive", path="TeslaCam/archive")
        assert backend._dest() == "gdrive:TeslaCam/archive"
        assert backend._dest("") == "gdrive:TeslaCam/archive"

    def test_dest_with_relative_file(self):
        """Test destination with relative file path."""
        backend = RcloneBackend(remote="gdrive", path="TeslaCam")
        assert backend._dest(Path("SavedClips/event/file.mp4")) == "gdrive:TeslaCam/SavedClips/event/file.mp4"

    def test_dest_no_base_path(self):
        """Test destination without base path."""
        backend = RcloneBackend(remote="s3")
        assert backend._dest(Path("SavedClips/file.mp4")) == "s3:SavedClips/file.mp4"

    def test_dest_strips_slashes_from_path(self):
        """Test that leading/trailing slashes are stripped from path."""
        backend = RcloneBackend(remote="gdrive", path="/TeslaCam/archive/")
        assert backend._dest() == "gdrive:TeslaCam/archive"
