"""Stress tests for snapshot space management on real XFS.

These tests verify the core space invariant: since cam_disk.bin uses at most
half the XFS volume, eagerly deleting stale snapshots before creating a new
one guarantees that we never run out of space — even under worst-case COW
divergence from car writes.

The tests create an XFS volume and exercise the snapshot lifecycle with real
reflink copies and real file overwrites to ensure no ENOSPC errors occur.
"""

from __future__ import annotations

import ctypes
import os
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

MB = 1024 * 1024


def _run(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, **kwargs)


@pytest.fixture
def xfs_volume(tmp_path: Path):
    """Create an XFS volume with reflink support.

    Yields (mount_path, cam_disk_path, cam_size) where cam_size is set to
    exactly half the actual usable space (measured after mount, not estimated).
    """
    img = tmp_path / "backing.img"
    mount_path = tmp_path / "xfs"
    mount_path.mkdir()

    # 200 MB — large enough for meaningful COW testing, small enough to be fast
    total_bytes = 200 * MB

    _run(["truncate", "-s", str(total_bytes), str(img)])
    result = _run(["mkfs.xfs", "-f", str(img)])
    assert result.returncode == 0, f"mkfs.xfs failed: {result.stderr}"

    result = _run(["mount", "-o", "loop", str(img), str(mount_path)])
    assert result.returncode == 0, f"mount failed: {result.stderr}"

    try:
        # Measure actual free space (XFS overhead varies by volume size)
        st = os.statvfs(mount_path)
        actual_free = st.f_bavail * st.f_frsize

        # cam_disk = 45% of actual free space.  On the real system,
        # calculate_cam_size uses (total * 0.97) / 2 ≈ 48.5%.  We use 45%
        # to leave margin for block-level overhead on small test volumes.
        cam_size = (int(actual_free * 0.45) // 512) * 512

        # Create fully-allocated cam_disk using fallocate
        cam_disk = mount_path / "cam_disk.bin"
        result = _run(["fallocate", "-l", str(cam_size), str(cam_disk)])
        assert result.returncode == 0, f"fallocate failed: {result.stderr}"

        snapshots_dir = mount_path / "snapshots"
        snapshots_dir.mkdir()

        yield mount_path, cam_disk, cam_size
    finally:
        _run(["umount", "-l", str(mount_path)])


def _df_free_bytes(path: Path) -> int:
    """Get free bytes from statvfs."""
    st = os.statvfs(path)
    return st.f_bavail * st.f_frsize


def _syncfs(path: Path) -> None:
    """Flush the filesystem journal via syncfs(2).

    This ensures XFS commits deferred block frees so that a subsequent
    statvfs() sees accurate free space.  This is the same approach used
    in production code (filesystem.py).
    """
    fd = os.open(str(path), os.O_RDONLY)
    try:
        ctypes.CDLL("libc.so.6", use_errno=True).syncfs(fd)
    finally:
        os.close(fd)


def _overwrite_chunk(path: Path, offset: int, size: int) -> None:
    """Overwrite a chunk of a file with random data.

    Simulates the car writing to cam_disk.bin via the USB gadget, causing
    COW block allocation when a reflink snapshot exists.
    """
    data = os.urandom(size)
    fd = os.open(str(path), os.O_WRONLY)
    try:
        os.lseek(fd, offset, os.SEEK_SET)
        os.write(fd, data)
    finally:
        os.close(fd)


class TestSnapshotSpaceInvariant:
    """Verify that eager snapshot deletion keeps space usage safe."""

    def test_repeated_snapshot_cycles_no_enospc(self, xfs_volume):
        """Snapshot, overwrite cam_disk, delete snapshot — repeat 10 times.

        Each cycle:
        1. Delete any stale snapshot (eager cleanup)
        2. Create reflink snapshot of cam_disk
        3. Overwrite 25% of cam_disk (simulates car writes causing COW)
        4. Sync to flush XFS journal
        5. Assert no ENOSPC
        """
        mount_path, cam_disk, cam_size = xfs_volume
        snapshots_dir = mount_path / "snapshots"

        # Write initial data so the file isn't sparse
        chunk = min(cam_size, 1 * MB)
        _overwrite_chunk(cam_disk, 0, chunk)
        subprocess.run(["sync"], check=True)

        free_before = _df_free_bytes(mount_path)

        prev_snap: Path | None = None

        for cycle in range(10):
            # 1. Eager cleanup: delete previous snapshot
            if prev_snap and prev_snap.exists():
                prev_snap.unlink()
                subprocess.run(["sync"], check=True)

            # 2. Take reflink snapshot
            snap_path = snapshots_dir / f"snap-{cycle:06d}.bin"
            result = _run(["cp", "--reflink=always", str(cam_disk), str(snap_path)])
            assert result.returncode == 0, (
                f"Cycle {cycle}: reflink failed: {result.stderr}"
            )

            # 3. Overwrite ~25% of cam_disk (COW divergence)
            write_size = cam_size // 4
            offset = (cycle * write_size) % max(1, cam_size - write_size)
            _overwrite_chunk(cam_disk, offset, write_size)

            # 4. Sync
            subprocess.run(["sync"], check=True)

            # 5. Must have space left
            free_now = _df_free_bytes(mount_path)
            assert free_now > 0, (
                f"Cycle {cycle}: no free space ({free_now} bytes free)"
            )

            prev_snap = snap_path

        # Final cleanup
        if prev_snap and prev_snap.exists():
            prev_snap.unlink()
            subprocess.run(["sync"], check=True)

        # After all snapshots deleted, free space should recover to near the
        # pre-cycle level (within one snapshot's worth of COW overhead)
        free_after = _df_free_bytes(mount_path)
        assert free_after >= free_before * 0.70, (
            f"Space not recovered: {free_after} bytes free "
            f"(was {free_before} before cycles)"
        )

    def test_full_cow_divergence_still_fits(self, xfs_volume):
        """Overwrite 100% of cam_disk while snapshot exists — must not ENOSPC.

        This is the worst case: every block diverges from the snapshot.
        Since cam_disk is half of actual free space, the COW copy should
        fit in the other half.
        """
        mount_path, cam_disk, cam_size = xfs_volume
        snapshots_dir = mount_path / "snapshots"

        # Fully write cam_disk with known data
        chunk = 1 * MB
        for off in range(0, cam_size, chunk):
            size = min(chunk, cam_size - off)
            _overwrite_chunk(cam_disk, off, size)
        subprocess.run(["sync"], check=True)

        # Take snapshot
        snap_path = snapshots_dir / "snap.bin"
        result = _run(["cp", "--reflink=always", str(cam_disk), str(snap_path)])
        assert result.returncode == 0, f"reflink failed: {result.stderr}"

        # Overwrite 100% of cam_disk — worst-case COW, every block diverges
        for off in range(0, cam_size, chunk):
            size = min(chunk, cam_size - off)
            _overwrite_chunk(cam_disk, off, size)

        subprocess.run(["sync"], check=True)

        free_after_cow = _df_free_bytes(mount_path)
        assert free_after_cow > 0, (
            f"Full COW divergence exhausted space: {free_after_cow} bytes free"
        )

        # Delete snapshot — space should recover
        snap_path.unlink()
        _syncfs(mount_path)

        free_recovered = _df_free_bytes(mount_path)
        assert free_recovered > free_after_cow, (
            f"Space not recovered after snapshot deletion: "
            f"{free_recovered} vs {free_after_cow}"
        )

    def test_stale_snapshots_cause_enospc_without_cleanup(self, xfs_volume):
        """Without eager deletion, snapshots accumulate and fill the disk.

        This demonstrates the failure mode we're preventing.
        """
        mount_path, cam_disk, cam_size = xfs_volume
        snapshots_dir = mount_path / "snapshots"

        # Fully write cam_disk
        chunk = 1 * MB
        for off in range(0, cam_size, chunk):
            size = min(chunk, cam_size - off)
            _overwrite_chunk(cam_disk, off, size)
        subprocess.run(["sync"], check=True)

        # Keep creating snapshots WITHOUT deleting — COW blocks pile up
        hit_pressure = False
        snap_paths: list[Path] = []

        for cycle in range(5):
            snap_path = snapshots_dir / f"snap-{cycle:06d}.bin"
            result = _run(["cp", "--reflink=always", str(cam_disk), str(snap_path)])
            if result.returncode != 0:
                hit_pressure = True
                break
            snap_paths.append(snap_path)

            # Overwrite cam_disk to cause COW divergence
            try:
                for off in range(0, cam_size, chunk):
                    size = min(chunk, cam_size - off)
                    _overwrite_chunk(cam_disk, off, size)
            except OSError:
                hit_pressure = True
                break

            subprocess.run(["sync"], check=True)
            if _df_free_bytes(mount_path) < 1 * MB:
                hit_pressure = True
                break

        assert hit_pressure, (
            "Expected disk pressure from accumulating snapshots"
        )

        # Clean up for fixture teardown
        for p in snap_paths:
            if p.exists():
                p.unlink()
