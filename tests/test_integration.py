from __future__ import annotations

import shutil
from contextlib import contextmanager
from pathlib import Path

from teslausb.archive import ArchiveManager, MockArchiveBackend
from teslausb.coordinator import Coordinator, CoordinatorConfig
from teslausb.filesystem import RealFilesystem
from teslausb.snapshot import SnapshotManager
from teslausb.space import SpaceManager


def test_end_to_end_archive_cycle(tmp_path: Path) -> None:
    fs = RealFilesystem()

    backingfiles_path = tmp_path / "backingfiles"
    snapshots_path = backingfiles_path / "snapshots"
    cam_disk_path = backingfiles_path / "cam_disk.bin"

    backingfiles_path.mkdir(parents=True)
    cam_disk_path.write_bytes(b"x" * 2_000_000)

    mount_source = tmp_path / "cam-mount-source"
    saved_event = mount_source / "TeslaCam" / "SavedClips" / "2024-01-01_12-00-00"
    saved_event.mkdir(parents=True)
    (saved_event / "front.mp4").write_bytes(b"f" * 150_000)
    (saved_event / "back.mp4").write_bytes(b"b" * 150_000)

    sentry_event = mount_source / "TeslaCam" / "SentryClips" / "2024-01-01_13-00-00"
    sentry_event.mkdir(parents=True)
    (sentry_event / "sentry.mp4").write_bytes(b"s" * 200_000)

    snapshot_manager = SnapshotManager(
        fs=fs,
        cam_disk_path=cam_disk_path,
        snapshots_path=snapshots_path,
    )

    space_manager = SpaceManager(
        fs=fs,
        snapshot_manager=snapshot_manager,
        backingfiles_path=backingfiles_path,
        cam_size=2_000_000,
        reserve=10 * 1024 * 1024,
    )

    backend = MockArchiveBackend(reachable=True)
    archive_manager = ArchiveManager(
        fs=fs,
        snapshot_manager=snapshot_manager,
        backend=backend,
        archive_recent=False,
        archive_track=False,
    )

    @contextmanager
    def fake_mount(image_path: Path):
        mount_path = image_path.parent / "mnt"
        if mount_path.exists():
            shutil.rmtree(mount_path)
        shutil.copytree(mount_source, mount_path)
        try:
            yield mount_path
        finally:
            if mount_path.exists():
                shutil.rmtree(mount_path)

    coordinator = Coordinator(
        fs=fs,
        snapshot_manager=snapshot_manager,
        archive_manager=archive_manager,
        space_manager=space_manager,
        backend=backend,
        config=CoordinatorConfig(
            mount_fn=fake_mount,
            fsck_on_snapshot=False,
            wait_for_idle=False,
            poll_interval=0.01,
            disconnect_poll_interval=0.01,
        ),
    )

    assert coordinator.run_once()

    archived_paths = {str(path) for path in backend.archived_files}
    assert archived_paths == {
        "SavedClips/2024-01-01_12-00-00/back.mp4",
        "SavedClips/2024-01-01_12-00-00/front.mp4",
        "SentryClips/2024-01-01_13-00-00/sentry.mp4",
    }

    snapshots = snapshot_manager.get_snapshots()
    assert len(snapshots) == 1
    snapshot = snapshots[0]
    assert fs.exists(snapshot.toc_path)
    assert snapshot.refcount == 0
