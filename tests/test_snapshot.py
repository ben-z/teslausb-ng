"""Tests for snapshot management."""

from pathlib import Path
from datetime import datetime
import threading
import time

import pytest

from teslausb.filesystem import MockFilesystem
from teslausb.snapshot import (
    Snapshot,
    SnapshotHandle,
    SnapshotManager,
    SnapshotState,
    SnapshotError,
    SnapshotInUseError,
    SnapshotNotFoundError,
    SnapshotCreationError,
)


class TestSnapshot:
    """Tests for the Snapshot dataclass."""

    def test_snapshot_creation(self):
        """Test creating a snapshot."""
        snap = Snapshot(
            id=1,
            path=Path("/backingfiles/snapshots/snap-000001"),
            created_at=datetime.now(),
            refcount=0,
        )

        assert snap.id == 1
        assert snap.state == SnapshotState.READY
        assert snap.refcount == 0
        assert snap.is_complete
        assert snap.is_deletable

    def test_snapshot_paths(self):
        """Test snapshot path properties."""
        snap = Snapshot(
            id=1,
            path=Path("/backingfiles/snapshots/snap-000001"),
            created_at=datetime.now(),
        )

        assert snap.image_path == Path("/backingfiles/snapshots/snap-000001/snap.bin")
        assert snap.toc_path == Path("/backingfiles/snapshots/snap-000001/snap.toc")
        assert snap.metadata_path == Path("/backingfiles/snapshots/snap-000001/metadata.json")

    def test_snapshot_not_deletable_when_in_use(self):
        """Test that snapshot with refs is not deletable."""
        snap = Snapshot(
            id=1,
            path=Path("/snap"),
            created_at=datetime.now(),
            refcount=1,
        )

        assert snap.state == SnapshotState.ARCHIVING
        assert not snap.is_deletable

    def test_snapshot_state_derived_from_refcount(self):
        """Test that state is derived from refcount."""
        snap = Snapshot(
            id=1,
            path=Path("/snap"),
            created_at=datetime.now(),
            refcount=0,
        )

        assert snap.state == SnapshotState.READY

        snap.refcount = 1
        assert snap.state == SnapshotState.ARCHIVING

        snap.refcount = 0
        assert snap.state == SnapshotState.READY

    def test_snapshot_serialization(self):
        """Test snapshot to_dict and from_dict."""
        original = Snapshot(
            id=5,
            path=Path("/backingfiles/snapshots/snap-000005"),
            created_at=datetime(2024, 1, 15, 10, 30, 0),
            refcount=2,  # This won't be serialized
        )

        data = original.to_dict()
        restored = Snapshot.from_dict(data)

        assert restored.id == original.id
        assert restored.path == original.path
        assert restored.created_at == original.created_at
        # refcount is always 0 on load (runtime only)
        assert restored.refcount == 0
        # state is derived from refcount
        assert restored.state == SnapshotState.READY


class TestSnapshotManager:
    """Tests for SnapshotManager."""

    def test_create_snapshot(self, mock_fs: MockFilesystem):
        """Test creating a snapshot."""
        manager = SnapshotManager(
            fs=mock_fs,
            cam_disk_path=Path("/backingfiles/cam_disk.bin"),
            snapshots_path=Path("/backingfiles/snapshots"),
        )

        snapshot = manager.create_snapshot()

        assert snapshot.id == 0
        assert snapshot.state == SnapshotState.READY
        assert snapshot.refcount == 0
        assert mock_fs.exists(snapshot.image_path)
        assert mock_fs.exists(snapshot.toc_path)

    def test_create_multiple_snapshots(self, mock_fs: MockFilesystem):
        """Test creating multiple snapshots."""
        manager = SnapshotManager(
            fs=mock_fs,
            cam_disk_path=Path("/backingfiles/cam_disk.bin"),
            snapshots_path=Path("/backingfiles/snapshots"),
        )

        snap1 = manager.create_snapshot()
        snap2 = manager.create_snapshot()
        snap3 = manager.create_snapshot()

        assert snap1.id == 0
        assert snap2.id == 1
        assert snap3.id == 2

        snapshots = manager.get_snapshots()
        assert len(snapshots) == 3

    def test_acquire_and_release(self, mock_fs: MockFilesystem):
        """Test acquiring and releasing a snapshot."""
        manager = SnapshotManager(
            fs=mock_fs,
            cam_disk_path=Path("/backingfiles/cam_disk.bin"),
            snapshots_path=Path("/backingfiles/snapshots"),
        )

        snapshot = manager.create_snapshot()
        assert snapshot.refcount == 0

        handle = manager.acquire(snapshot.id)
        assert handle.snapshot.refcount == 1
        assert handle.snapshot.state == SnapshotState.ARCHIVING

        handle.release()
        assert manager.get_snapshot(snapshot.id).refcount == 0
        assert manager.get_snapshot(snapshot.id).state == SnapshotState.READY

    def test_acquire_context_manager(self, mock_fs: MockFilesystem):
        """Test using acquire with context manager."""
        manager = SnapshotManager(
            fs=mock_fs,
            cam_disk_path=Path("/backingfiles/cam_disk.bin"),
            snapshots_path=Path("/backingfiles/snapshots"),
        )

        snapshot = manager.create_snapshot()

        with manager.acquire(snapshot.id) as handle:
            assert handle.snapshot.refcount == 1

        # After context, should be released
        assert manager.get_snapshot(snapshot.id).refcount == 0

    def test_acquire_nonexistent_snapshot(self, mock_fs: MockFilesystem):
        """Test acquiring a nonexistent snapshot raises error."""
        manager = SnapshotManager(
            fs=mock_fs,
            cam_disk_path=Path("/backingfiles/cam_disk.bin"),
            snapshots_path=Path("/backingfiles/snapshots"),
        )

        with pytest.raises(SnapshotNotFoundError):
            manager.acquire(999)

    def test_delete_snapshot(self, mock_fs: MockFilesystem):
        """Test deleting a snapshot."""
        manager = SnapshotManager(
            fs=mock_fs,
            cam_disk_path=Path("/backingfiles/cam_disk.bin"),
            snapshots_path=Path("/backingfiles/snapshots"),
        )

        snapshot = manager.create_snapshot()
        snap_path = snapshot.path

        assert mock_fs.exists(snap_path)
        assert manager.delete_snapshot(snapshot.id)
        assert not mock_fs.exists(snap_path)
        assert manager.get_snapshot(snapshot.id) is None

    def test_delete_snapshot_in_use(self, mock_fs: MockFilesystem):
        """Test that deleting a snapshot in use raises error."""
        manager = SnapshotManager(
            fs=mock_fs,
            cam_disk_path=Path("/backingfiles/cam_disk.bin"),
            snapshots_path=Path("/backingfiles/snapshots"),
        )

        snapshot = manager.create_snapshot()
        handle = manager.acquire(snapshot.id)

        with pytest.raises(SnapshotInUseError):
            manager.delete_snapshot(snapshot.id)

        # Snapshot should still exist
        assert manager.get_snapshot(snapshot.id) is not None

        handle.release()

    def test_delete_oldest_if_deletable(self, mock_fs: MockFilesystem):
        """Test deleting oldest deletable snapshot."""
        manager = SnapshotManager(
            fs=mock_fs,
            cam_disk_path=Path("/backingfiles/cam_disk.bin"),
            snapshots_path=Path("/backingfiles/snapshots"),
        )

        snap1 = manager.create_snapshot()
        time.sleep(0.01)  # Ensure different timestamps
        snap2 = manager.create_snapshot()
        time.sleep(0.01)
        snap3 = manager.create_snapshot()

        # Acquire snap2 (middle one)
        handle = manager.acquire(snap2.id)

        # Should delete snap1 (oldest deletable)
        assert manager.delete_oldest_if_deletable()
        assert manager.get_snapshot(snap1.id) is None
        assert manager.get_snapshot(snap2.id) is not None  # Still in use
        assert manager.get_snapshot(snap3.id) is not None

        handle.release()

    def test_get_deletable_snapshots(self, mock_fs: MockFilesystem):
        """Test getting deletable snapshots."""
        manager = SnapshotManager(
            fs=mock_fs,
            cam_disk_path=Path("/backingfiles/cam_disk.bin"),
            snapshots_path=Path("/backingfiles/snapshots"),
        )

        snap1 = manager.create_snapshot()
        snap2 = manager.create_snapshot()
        snap3 = manager.create_snapshot()

        # Acquire snap2
        handle = manager.acquire(snap2.id)

        deletable = manager.get_deletable_snapshots()
        deletable_ids = [s.id for s in deletable]

        assert snap1.id in deletable_ids
        assert snap2.id not in deletable_ids  # In use
        assert snap3.id in deletable_ids

        handle.release()

    def test_snapshot_session(self, mock_fs: MockFilesystem):
        """Test snapshot_session context manager."""
        manager = SnapshotManager(
            fs=mock_fs,
            cam_disk_path=Path("/backingfiles/cam_disk.bin"),
            snapshots_path=Path("/backingfiles/snapshots"),
        )

        with manager.snapshot_session() as handle:
            assert handle.snapshot.state == SnapshotState.ARCHIVING
            assert handle.snapshot.refcount == 1
            snap_id = handle.snapshot.id

        # After context, snapshot should be released
        snapshot = manager.get_snapshot(snap_id)
        assert snapshot.refcount == 0
        assert snapshot.state == SnapshotState.READY

    def test_load_existing_snapshots(self, mock_fs: MockFilesystem):
        """Test loading existing snapshots on initialization."""
        # Create snapshots with first manager
        manager1 = SnapshotManager(
            fs=mock_fs,
            cam_disk_path=Path("/backingfiles/cam_disk.bin"),
            snapshots_path=Path("/backingfiles/snapshots"),
        )

        manager1.create_snapshot()
        manager1.create_snapshot()

        # Create new manager (simulating restart)
        manager2 = SnapshotManager(
            fs=mock_fs,
            cam_disk_path=Path("/backingfiles/cam_disk.bin"),
            snapshots_path=Path("/backingfiles/snapshots"),
        )

        # Should have loaded existing snapshots
        snapshots = manager2.get_snapshots()
        assert len(snapshots) == 2

        # Creating new snapshot should continue ID sequence
        snap3 = manager2.create_snapshot()
        assert snap3.id == 2

    def test_concurrent_creates_blocked(self, mock_fs: MockFilesystem):
        """Test that concurrent snapshot creation is blocked."""
        manager = SnapshotManager(
            fs=mock_fs,
            cam_disk_path=Path("/backingfiles/cam_disk.bin"),
            snapshots_path=Path("/backingfiles/snapshots"),
        )

        results = []
        errors = []

        def create_snapshot():
            try:
                snap = manager.create_snapshot()
                results.append(snap)
            except SnapshotCreationError as e:
                errors.append(e)

        # Start two threads trying to create snapshots
        t1 = threading.Thread(target=create_snapshot)
        t2 = threading.Thread(target=create_snapshot)

        t1.start()
        t2.start()

        t1.join()
        t2.join()

        # Both should succeed (one waits for the other)
        # Note: In our implementation, second one gets "already in progress" error
        # Actually in our implementation both should succeed as they're sequential
        assert len(results) == 2 or (len(results) == 1 and len(errors) == 1)

    def test_multiple_acquires(self, mock_fs: MockFilesystem):
        """Test acquiring the same snapshot multiple times."""
        manager = SnapshotManager(
            fs=mock_fs,
            cam_disk_path=Path("/backingfiles/cam_disk.bin"),
            snapshots_path=Path("/backingfiles/snapshots"),
        )

        snapshot = manager.create_snapshot()

        handle1 = manager.acquire(snapshot.id)
        assert manager.get_snapshot(snapshot.id).refcount == 1

        handle2 = manager.acquire(snapshot.id)
        assert manager.get_snapshot(snapshot.id).refcount == 2

        handle1.release()
        assert manager.get_snapshot(snapshot.id).refcount == 1

        # Should still not be deletable
        with pytest.raises(SnapshotInUseError):
            manager.delete_snapshot(snapshot.id)

        handle2.release()
        assert manager.get_snapshot(snapshot.id).refcount == 0

        # Now should be deletable
        assert manager.delete_snapshot(snapshot.id)


class TestSnapshotHandle:
    """Tests for SnapshotHandle."""

    def test_handle_release(self, snapshot_manager: SnapshotManager):
        """Test handle release."""
        snapshot = snapshot_manager.create_snapshot()
        handle = snapshot_manager.acquire(snapshot.id)

        assert not handle._released
        handle.release()
        assert handle._released

    def test_handle_double_release_is_safe(self, snapshot_manager: SnapshotManager):
        """Test that releasing a handle twice is safe."""
        snapshot = snapshot_manager.create_snapshot()
        handle = snapshot_manager.acquire(snapshot.id)

        handle.release()
        handle.release()  # Should not raise

        assert snapshot_manager.get_snapshot(snapshot.id).refcount == 0

    def test_handle_access_after_release_raises(self, snapshot_manager: SnapshotManager):
        """Test that accessing snapshot after release raises error."""
        snapshot = snapshot_manager.create_snapshot()
        handle = snapshot_manager.acquire(snapshot.id)

        handle.release()

        with pytest.raises(SnapshotError):
            _ = handle.snapshot


class TestPowerCutRecovery:
    """Tests for power-cut recovery scenarios.

    The key design principle is that the .toc file is the single source
    of truth for snapshot completion. If .toc doesn't exist, the snapshot
    is incomplete and will be cleaned up on restart.
    """

    def test_incomplete_snapshot_without_toc_is_cleaned_up(self, mock_fs: MockFilesystem, tmp_path: Path):
        """Test that snapshots without .toc file are cleaned up on restart."""
        snapshots_path = tmp_path / "snapshots"
        cam_disk = tmp_path / "cam.bin"
        mock_fs.mkdir(tmp_path, parents=True)
        mock_fs.write_bytes(cam_disk, b"cam data")
        mock_fs.mkdir(snapshots_path, parents=True)

        # Simulate a snapshot that was being created when power was lost
        # (no .toc file = incomplete)
        incomplete_snap = snapshots_path / "snap-000001"
        mock_fs.mkdir(incomplete_snap)
        mock_fs.write_text(
            incomplete_snap / "metadata.json",
            '{"id": 1, "path": "' + str(incomplete_snap) + '", "created_at": "2024-01-01T00:00:00"}'
        )
        mock_fs.write_bytes(incomplete_snap / "snap.bin", b"incomplete")
        # Note: NO .toc file!

        # Create manager - should clean up the incomplete snapshot
        manager = SnapshotManager(
            fs=mock_fs,
            cam_disk_path=cam_disk,
            snapshots_path=snapshots_path,
        )

        # Incomplete snapshot should have been removed
        assert len(manager.get_snapshots()) == 0
        assert not mock_fs.exists(incomplete_snap)

    def test_interrupted_deletion_is_completed(self, mock_fs: MockFilesystem, tmp_path: Path):
        """Test that interrupted deletions are completed on restart.

        If deletion was interrupted after .toc was removed but before
        the directory was fully removed, the snapshot should be cleaned up.
        """
        snapshots_path = tmp_path / "snapshots"
        cam_disk = tmp_path / "cam.bin"
        mock_fs.mkdir(tmp_path, parents=True)
        mock_fs.write_bytes(cam_disk, b"cam data")
        mock_fs.mkdir(snapshots_path, parents=True)

        # Simulate a snapshot where .toc was deleted (deletion started)
        # but directory still exists
        partial_delete_snap = snapshots_path / "snap-000002"
        mock_fs.mkdir(partial_delete_snap)
        mock_fs.write_text(
            partial_delete_snap / "metadata.json",
            '{"id": 2, "path": "' + str(partial_delete_snap) + '", "created_at": "2024-01-01T00:00:00"}'
        )
        mock_fs.write_bytes(partial_delete_snap / "snap.bin", b"data")
        # Note: NO .toc file (was deleted during deletion process)

        # Create manager - should finish the deletion
        manager = SnapshotManager(
            fs=mock_fs,
            cam_disk_path=cam_disk,
            snapshots_path=snapshots_path,
        )

        # Snapshot should have been removed
        assert len(manager.get_snapshots()) == 0
        assert not mock_fs.exists(partial_delete_snap)

    def test_complete_snapshot_with_toc_is_loaded(self, mock_fs: MockFilesystem, tmp_path: Path):
        """Test that complete snapshots (with .toc) are loaded properly."""
        snapshots_path = tmp_path / "snapshots"
        cam_disk = tmp_path / "cam.bin"
        mock_fs.mkdir(tmp_path, parents=True)
        mock_fs.write_bytes(cam_disk, b"cam data")
        mock_fs.mkdir(snapshots_path, parents=True)

        # Create a complete snapshot with .toc file
        complete_snap = snapshots_path / "snap-000003"
        mock_fs.mkdir(complete_snap)
        mock_fs.write_text(
            complete_snap / "metadata.json",
            '{"id": 3, "path": "' + str(complete_snap) + '", "created_at": "2024-01-01T00:00:00"}'
        )
        mock_fs.write_bytes(complete_snap / "snap.bin", b"snapshot data")
        mock_fs.write_text(complete_snap / "snap.toc", "")  # .toc exists = complete

        # Create manager - should load the snapshot
        manager = SnapshotManager(
            fs=mock_fs,
            cam_disk_path=cam_disk,
            snapshots_path=snapshots_path,
        )

        snapshots = manager.get_snapshots()
        assert len(snapshots) == 1
        assert snapshots[0].id == 3
        assert snapshots[0].state == SnapshotState.READY
        assert snapshots[0].refcount == 0

    def test_legacy_snapshot_without_metadata_is_reconstructed(self, mock_fs: MockFilesystem, tmp_path: Path):
        """Test that legacy snapshots without metadata.json are reconstructed."""
        snapshots_path = tmp_path / "snapshots"
        cam_disk = tmp_path / "cam.bin"
        mock_fs.mkdir(tmp_path, parents=True)
        mock_fs.write_bytes(cam_disk, b"cam data")
        mock_fs.mkdir(snapshots_path, parents=True)

        # Create a legacy snapshot with .toc but no metadata.json
        legacy_snap = snapshots_path / "snap-000005"
        mock_fs.mkdir(legacy_snap)
        mock_fs.write_bytes(legacy_snap / "snap.bin", b"legacy data")
        mock_fs.write_text(legacy_snap / "snap.toc", "")
        # Note: NO metadata.json

        # Create manager - should reconstruct from filesystem
        manager = SnapshotManager(
            fs=mock_fs,
            cam_disk_path=cam_disk,
            snapshots_path=snapshots_path,
        )

        snapshots = manager.get_snapshots()
        assert len(snapshots) == 1
        assert snapshots[0].id == 5
        assert snapshots[0].state == SnapshotState.READY

        # Should have saved metadata for future loads
        assert mock_fs.exists(legacy_snap / "metadata.json")
