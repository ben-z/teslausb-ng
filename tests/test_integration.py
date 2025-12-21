from __future__ import annotations

import shutil
from contextlib import contextmanager
from functools import partial
from pathlib import Path
from typing import Iterator

from teslausb.archive import ArchiveManager, MockArchiveBackend
from teslausb.coordinator import Coordinator, CoordinatorConfig
from teslausb.filesystem import RealFilesystem
from teslausb.snapshot import SnapshotManager
from teslausb.space import SpaceManager

CAM_DISK_SIZE_BYTES = 2_000_000
SAVED_CLIP_SIZE_BYTES = 150_000
SENTRY_CLIP_SIZE_BYTES = 200_000
RESERVE_BYTES = 10 * 1024 * 1024
SAVED_EVENT_ID = "2024-01-01_12-00-00"
SENTRY_EVENT_ID = "2024-01-01_13-00-00"


@contextmanager
def mount_from_source(image_path: Path, mount_source: Path) -> Iterator[Path]:
    mount_path = image_path.parent / "mnt"
    if mount_path.exists():
        shutil.rmtree(mount_path)
    shutil.copytree(mount_source, mount_path)
    try:
        yield mount_path
    finally:
        if mount_path.exists():
            shutil.rmtree(mount_path)


def test_end_to_end_archive_cycle(tmp_path: Path) -> None:
    fs = RealFilesystem()

    backingfiles_path = tmp_path / "backingfiles"
    snapshots_path = backingfiles_path / "snapshots"
    cam_disk_path = backingfiles_path / "cam_disk.bin"

    backingfiles_path.mkdir(parents=True)
    cam_disk_path.write_bytes(b"x" * CAM_DISK_SIZE_BYTES)

    mount_source = tmp_path / "cam-mount-source"
    saved_event = mount_source / "TeslaCam" / "SavedClips" / SAVED_EVENT_ID
    saved_event.mkdir(parents=True)
    (saved_event / "front.mp4").write_bytes(b"f" * SAVED_CLIP_SIZE_BYTES)
    (saved_event / "back.mp4").write_bytes(b"b" * SAVED_CLIP_SIZE_BYTES)

    sentry_event = mount_source / "TeslaCam" / "SentryClips" / SENTRY_EVENT_ID
    sentry_event.mkdir(parents=True)
    (sentry_event / "sentry.mp4").write_bytes(b"s" * SENTRY_CLIP_SIZE_BYTES)

    snapshot_manager = SnapshotManager(
        fs=fs,
        cam_disk_path=cam_disk_path,
        snapshots_path=snapshots_path,
    )

    space_manager = SpaceManager(
        fs=fs,
        snapshot_manager=snapshot_manager,
        backingfiles_path=backingfiles_path,
        cam_size=CAM_DISK_SIZE_BYTES,
        reserve=RESERVE_BYTES,
    )

    backend = MockArchiveBackend(reachable=True)
    archive_manager = ArchiveManager(
        fs=fs,
        snapshot_manager=snapshot_manager,
        backend=backend,
        archive_saved=True,
        archive_sentry=True,
        archive_recent=False,
        archive_track=False,
    )

    mount_fn = partial(mount_from_source, mount_source=mount_source)

    coordinator = Coordinator(
        fs=fs,
        snapshot_manager=snapshot_manager,
        archive_manager=archive_manager,
        space_manager=space_manager,
        backend=backend,
        config=CoordinatorConfig(
            mount_fn=mount_fn,
            fsck_on_snapshot=False,
            wait_for_idle=False,
            poll_interval=0.01,
            disconnect_poll_interval=0.01,
        ),
    )

    assert coordinator.run_once()

    archived_paths = set(backend.archived_files.keys())
    assert archived_paths == {
        Path(f"SavedClips/{SAVED_EVENT_ID}/back.mp4"),
        Path(f"SavedClips/{SAVED_EVENT_ID}/front.mp4"),
        Path(f"SentryClips/{SENTRY_EVENT_ID}/sentry.mp4"),
    }

    snapshots = snapshot_manager.get_snapshots()
    assert len(snapshots) == 1
    snapshot = snapshots[0]
    assert fs.exists(snapshot.toc_path)
    assert snapshot.refcount == 0
