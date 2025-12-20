"""Disk image mounting utilities.

Simple, focused module for mounting disk images via loop devices.
"""

from __future__ import annotations

import logging
import os
import subprocess
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

logger = logging.getLogger(__name__)


class MountError(Exception):
    """Error during mount operations."""


def _run(cmd: list[str], timeout: int = 30) -> subprocess.CompletedProcess:
    """Run command and return result."""
    logger.debug(f"Running: {' '.join(cmd)}")
    return subprocess.run(cmd, capture_output=True, timeout=timeout, check=False)


@contextmanager
def mount_image(image_path: Path) -> Iterator[Path]:
    """Mount a disk image and yield the mount path.

    Creates a loop device with partition scanning, mounts the first partition,
    and cleans up on exit.

    Args:
        image_path: Path to disk image file (e.g., snap.bin)

    Yields:
        Path to mounted filesystem

    Raises:
        MountError: If mounting fails

    Example:
        with mount_image(Path("/backingfiles/snapshots/snap-000000/snap.bin")) as mnt:
            for f in (mnt / "TeslaCam" / "SavedClips").iterdir():
                print(f)
    """
    loop_dev: str | None = None
    mount_point: Path | None = None

    try:
        # Create loop device with partition scanning
        result = _run(["losetup", "-Pf", "--show", str(image_path)])
        if result.returncode != 0:
            raise MountError(f"losetup failed: {result.stderr.decode()}")

        loop_dev = result.stdout.decode().strip()
        partition = f"{loop_dev}p1"

        # Wait briefly for partition device to appear
        for _ in range(10):
            if Path(partition).exists():
                break
            import time
            time.sleep(0.1)
        else:
            raise MountError(f"Partition device {partition} not found")

        # Create mount point
        mount_point = Path(tempfile.mkdtemp(prefix="teslausb-mount-"))

        # Mount the partition
        result = _run(["mount", "-o", "ro", partition, str(mount_point)])
        if result.returncode != 0:
            raise MountError(f"mount failed: {result.stderr.decode()}")

        logger.info(f"Mounted {image_path} at {mount_point}")
        yield mount_point

    finally:
        # Cleanup: unmount and detach
        if mount_point and mount_point.exists():
            result = _run(["umount", str(mount_point)])
            if result.returncode != 0:
                logger.warning(f"umount failed: {result.stderr.decode()}")
            try:
                mount_point.rmdir()
            except OSError:
                pass

        if loop_dev:
            result = _run(["losetup", "-d", loop_dev])
            if result.returncode != 0:
                logger.warning(f"losetup -d failed: {result.stderr.decode()}")

        logger.debug(f"Cleaned up mount for {image_path}")
