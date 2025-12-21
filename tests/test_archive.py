"""Tests for archive management."""

from pathlib import Path

import pytest

from teslausb.archive import (
    ArchiveManager,
    ArchiveResult,
    ArchiveState,
    CopyResult,
    MockArchiveBackend,
    RcloneBackend,
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

    def test_copy_directory_success(self):
        """Test copying a directory."""
        backend = MockArchiveBackend(reachable=True)

        result = backend.copy_directory(
            src=Path("/some/SavedClips"),
            dst_name="SavedClips",
        )

        assert result.success
        assert result.files_transferred > 0
        assert len(backend.copied_dirs) == 1
        assert backend.copied_dirs[0] == (Path("/some/SavedClips"), "SavedClips")

    def test_copy_directory_fails(self):
        """Test copying a directory that should fail."""
        backend = MockArchiveBackend(
            reachable=True,
            fail_dirs={"SavedClips"},
        )

        result = backend.copy_directory(
            src=Path("/some/SavedClips"),
            dst_name="SavedClips",
        )

        assert not result.success
        assert result.error is not None


class TestArchiveManager:
    """Tests for ArchiveManager."""

    def test_get_dirs_to_archive(self, mock_fs_with_teslacam: MockFilesystem):
        """Test finding directories to archive."""
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
        )

        snapshot_mount = Path("/backingfiles/snapshots/snap-000000/mnt")
        dirs = manager._get_dirs_to_archive(snapshot_mount)

        # Should find SavedClips and SentryClips (RecentClips exists but not enabled by default)
        assert len(dirs) == 2
        dir_names = [d[1] for d in dirs]
        assert "SavedClips" in dir_names
        assert "SentryClips" in dir_names

    def test_get_dirs_respects_settings(self, mock_fs_with_teslacam: MockFilesystem):
        """Test that directory selection respects archive settings."""
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
        dirs = manager._get_dirs_to_archive(snapshot_mount)

        assert len(dirs) == 1
        assert dirs[0][1] == "SavedClips"

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
        )

        # Get the snapshot and acquire it
        snapshots = snapshot_manager.get_snapshots()
        assert len(snapshots) == 1

        handle = snapshot_manager.acquire(snapshots[0].id)

        try:
            mount_path = Path("/backingfiles/snapshots/snap-000000/mnt")
            result = manager.archive_snapshot(handle, mount_path)

            assert result.state == ArchiveState.COMPLETED
            assert result.files_transferred > 0
            # Should have copied SavedClips and SentryClips
            assert len(backend.copied_dirs) == 2
        finally:
            handle.release()

    def test_archive_snapshot_handles_failure(self, mock_fs_with_teslacam: MockFilesystem):
        """Test archive handles directory copy failures."""
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

        # Backend that fails on SavedClips
        backend = MockArchiveBackend(
            reachable=True,
            fail_dirs={"SavedClips"},
        )

        manager = ArchiveManager(
            fs=mock_fs_with_teslacam,
            snapshot_manager=snapshot_manager,
            backend=backend,
        )

        snapshots = snapshot_manager.get_snapshots()
        handle = snapshot_manager.acquire(snapshots[0].id)

        try:
            mount_path = Path("/backingfiles/snapshots/snap-000000/mnt")
            result = manager.archive_snapshot(handle, mount_path)

            # Should fail because SavedClips failed
            assert result.state == ArchiveState.FAILED
            assert result.error is not None
            assert "SavedClips" in result.error
        finally:
            handle.release()

    def test_archive_when_unreachable(self, mock_fs_with_teslacam: MockFilesystem):
        """Test archive fails gracefully when backend unreachable."""
        snapshot_manager = SnapshotManager(
            fs=mock_fs_with_teslacam,
            cam_disk_path=Path("/backingfiles/cam_disk.bin"),
            snapshots_path=Path("/backingfiles/snapshots"),
        )

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

        backend = MockArchiveBackend(reachable=False)

        manager = ArchiveManager(
            fs=mock_fs_with_teslacam,
            snapshot_manager=snapshot_manager,
            backend=backend,
        )

        snapshots = snapshot_manager.get_snapshots()
        handle = snapshot_manager.acquire(snapshots[0].id)

        try:
            mount_path = Path("/backingfiles/snapshots/snap-000000/mnt")
            result = manager.archive_snapshot(handle, mount_path)

            assert result.state == ArchiveState.FAILED
            assert "not reachable" in result.error
        finally:
            handle.release()


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


class TestCopyResult:
    """Tests for CopyResult."""

    def test_success_result(self):
        """Test successful copy result."""
        result = CopyResult(success=True, files_transferred=10, bytes_transferred=1000000)
        assert result.success
        assert result.files_transferred == 10
        assert result.error is None

    def test_failed_result(self):
        """Test failed copy result."""
        result = CopyResult(success=False, error="Connection failed")
        assert not result.success
        assert result.error == "Connection failed"


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

    def test_dest_with_subpath(self):
        """Test destination with subpath."""
        backend = RcloneBackend(remote="gdrive", path="TeslaCam")
        assert backend._dest("SavedClips") == "gdrive:TeslaCam/SavedClips"

    def test_dest_no_base_path(self):
        """Test destination without base path."""
        backend = RcloneBackend(remote="s3")
        assert backend._dest("SavedClips") == "s3:SavedClips"

    def test_dest_strips_slashes_from_path(self):
        """Test that leading/trailing slashes are stripped from path."""
        backend = RcloneBackend(remote="gdrive", path="/TeslaCam/archive/")
        assert backend._dest() == "gdrive:TeslaCam/archive"
