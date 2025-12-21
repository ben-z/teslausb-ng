"""Pytest fixtures for TeslaUSB tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from teslausb.archive import MockArchiveBackend
from teslausb.filesystem import MockFilesystem
from teslausb.snapshot import SnapshotManager
from teslausb.space import SpaceManager

GB = 1024 * 1024 * 1024
MB = 1024 * 1024


@pytest.fixture
def mock_fs() -> MockFilesystem:
    """Create a mock filesystem with standard structure."""
    fs = MockFilesystem()

    # Create standard directory structure
    fs.mkdir(Path("/backingfiles"), parents=True)
    fs.mkdir(Path("/backingfiles/snapshots"), parents=True)
    fs.mkdir(Path("/mutable"), parents=True)
    fs.mkdir(Path("/mutable/TeslaCam"), parents=True)

    # Create a mock cam_disk.bin (just a marker file)
    fs.write_bytes(Path("/backingfiles/cam_disk.bin"), b"mock cam disk content")

    # Set total space to 256GB
    fs.set_total_space(256 * GB)

    return fs


@pytest.fixture
def snapshot_manager(mock_fs: MockFilesystem) -> SnapshotManager:
    """Create a SnapshotManager with mock filesystem."""
    return SnapshotManager(
        fs=mock_fs,
        cam_disk_path=Path("/backingfiles/cam_disk.bin"),
        snapshots_path=Path("/backingfiles/snapshots"),
    )


@pytest.fixture
def space_manager(mock_fs: MockFilesystem, snapshot_manager: SnapshotManager) -> SpaceManager:
    """Create a SpaceManager with mock filesystem."""
    return SpaceManager(
        fs=mock_fs,
        snapshot_manager=snapshot_manager,
        backingfiles_path=Path("/backingfiles"),
        cam_size=40 * GB,
    )


@pytest.fixture
def mock_backend() -> MockArchiveBackend:
    """Create a mock archive backend."""
    return MockArchiveBackend(reachable=True)


@pytest.fixture
def mock_fs_with_teslacam(mock_fs: MockFilesystem) -> MockFilesystem:
    """Create a mock filesystem with TeslaCam folder structure and files."""
    fs = mock_fs
    snap_dir = Path("/backingfiles/snapshots/snap-000000")

    # Create snapshot directory with required files
    fs.mkdir(snap_dir, parents=True)
    fs.write_bytes(snap_dir / "snap.bin", b"snapshot data")
    fs.write_text(snap_dir / "snap.toc", "")  # .toc marks snapshot as complete
    fs.write_text(
        snap_dir / "metadata.json",
        '{"id": 0, "path": "/backingfiles/snapshots/snap-000000", "created_at": "2024-01-15T10:00:00"}'
    )

    # Create TeslaCam structure inside the mount point
    base = snap_dir / "mnt" / "TeslaCam"
    fs.mkdir(base / "SavedClips", parents=True)
    fs.mkdir(base / "SentryClips", parents=True)
    fs.mkdir(base / "RecentClips", parents=True)

    # Create some event folders with video files
    event1 = base / "SavedClips" / "2024-01-15_10-30-00"
    fs.mkdir(event1, parents=True)
    fs.write_bytes(event1 / "2024-01-15_10-30-00-front.mp4", b"x" * 500_000)
    fs.write_bytes(event1 / "2024-01-15_10-30-00-back.mp4", b"x" * 500_000)

    event2 = base / "SentryClips" / "2024-01-15_11-00-00"
    fs.mkdir(event2, parents=True)
    fs.write_bytes(event2 / "2024-01-15_11-00-00-front.mp4", b"x" * 600_000)

    return fs
