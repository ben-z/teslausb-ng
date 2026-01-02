"""Disk image mounting utilities.

Simple, focused module for mounting disk images via loop devices.
"""

from __future__ import annotations

import logging
import subprocess
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

logger = logging.getLogger(__name__)


class MountError(Exception):
    """Error during mount operations."""


def _run(cmd: list[str], timeout: int = 30) -> subprocess.CompletedProcess:
    """Run command and return result.

    Captures output and logs stderr for visibility.
    """
    logger.debug(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout, check=False)
    if result.stderr:
        for line in result.stderr.decode().splitlines():
            logger.debug(f"{cmd[0]}: {line}")
    return result


@contextmanager
def mount_image(image_path: Path, readonly: bool = True) -> Iterator[Path]:
    """Mount a disk image and yield the mount path.

    Creates a loop device with partition scanning, mounts the first partition,
    and cleans up on exit.

    Args:
        image_path: Path to disk image file (e.g., snap.bin or cam_disk.bin)
        readonly: If True, mount read-only (default). If False, mount read-write.

    Yields:
        Path to mounted filesystem

    Raises:
        MountError: If mounting fails

    Example:
        with mount_image(Path("/backingfiles/snapshots/snap-000000/snap.bin")) as mnt:
            for f in (mnt / "TeslaCam" / "SavedClips").iterdir():
                print(f)

        # Mount read-write for deletion:
        with mount_image(Path("/backingfiles/cam_disk.bin"), readonly=False) as mnt:
            (mnt / "TeslaCam" / "SavedClips" / "old_event").unlink()
    """
    loop_dev: str | None = None
    mount_point: Path | None = None

    try:
        # Create loop device with partition scanning
        result = _run(["losetup", "-Pf", "--show", str(image_path)])
        if result.returncode != 0:
            raise MountError("losetup failed")

        loop_dev = result.stdout.decode().strip()
        partition = f"{loop_dev}p1"

        # Wait briefly for partition device to appear
        for _ in range(10):
            if Path(partition).exists():
                break
            time.sleep(0.1)
        else:
            raise MountError(f"Partition device {partition} not found")

        # Create mount point
        mount_point = Path(tempfile.mkdtemp(prefix="teslausb-mount-"))

        # Mount the partition (read-only or read-write)
        mount_opts = "ro" if readonly else "rw"
        result = _run(["mount", "-o", mount_opts, partition, str(mount_point)])
        if result.returncode != 0:
            raise MountError("mount failed")

        mode = "read-only" if readonly else "read-write"
        logger.info(f"Mounted {image_path} at {mount_point} ({mode})")
        yield mount_point

    finally:
        # Cleanup: unmount and detach
        if mount_point and mount_point.exists():
            # Sync before unmount if read-write
            if not readonly:
                _run(["sync"])
            result = _run(["umount", str(mount_point)])
            if result.returncode != 0:
                logger.warning("umount failed")
            try:
                mount_point.rmdir()
            except OSError:
                pass

        if loop_dev:
            result = _run(["losetup", "-d", loop_dev])
            if result.returncode != 0:
                logger.warning("losetup -d failed")

        logger.debug(f"Cleaned up mount for {image_path}")
