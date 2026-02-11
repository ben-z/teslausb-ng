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
    result = subprocess.run(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout, check=False
    )
    if result.stderr:
        for line in result.stderr.decode().splitlines():
            logger.debug(f"{cmd[0]}: {line}")
    return result


def _setup_loop_device(image_path: Path) -> tuple[str, str] | None:
    """Create a loop device with partition scanning and wait for partition.

    Args:
        image_path: Path to disk image file

    Returns:
        Tuple of (loop_device, partition_device) on success, None on failure
    """
    result = _run(["losetup", "-Pf", "--show", str(image_path)])
    if result.returncode != 0:
        return None

    loop_dev = result.stdout.decode().strip()
    partition = f"{loop_dev}p1"

    for _ in range(10):
        if Path(partition).exists():
            return loop_dev, partition
        time.sleep(0.1)

    # Partition didn't appear -- detach loop device before returning
    _run(["losetup", "-d", loop_dev])
    return None


def _detach_loop_device(loop_dev: str) -> None:
    """Detach a loop device."""
    result = _run(["losetup", "-d", loop_dev])
    if result.returncode != 0:
        logger.warning("losetup -d failed")


def fsck_image(image_path: Path) -> bool:
    """Run filesystem check on a disk image.

    Creates a temporary loop device, runs fsck -p on the first partition
    to auto-repair errors, then detaches. This should be run after the USB
    gadget is disabled and before mounting read-write, since the car may
    have been mid-write when disconnected.

    Args:
        image_path: Path to disk image file (e.g., cam_disk.bin)

    Returns:
        True if fsck succeeded (or made repairs), False on failure
    """
    devices = _setup_loop_device(image_path)
    if not devices:
        logger.error("fsck: failed to set up loop device")
        return False

    loop_dev, partition = devices

    try:
        logger.info(f"Running fsck on {image_path}")
        result = _run(["fsck", "-p", partition], timeout=120)

        # fsck exit codes: 0 = clean, 1 = errors corrected, 2+ = errors remain
        if result.returncode == 0:
            logger.info("fsck: filesystem clean")
        elif result.returncode == 1:
            logger.info("fsck: errors corrected")
        else:
            logger.warning(f"fsck: exited with code {result.returncode}")
            return False

        return True

    finally:
        _detach_loop_device(loop_dev)


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
    devices = _setup_loop_device(image_path)
    if not devices:
        raise MountError(f"Failed to set up loop device for {image_path}")

    loop_dev, partition = devices
    mount_point: Path | None = None

    try:
        mount_point = Path(tempfile.mkdtemp(prefix="teslausb-mount-"))

        mount_opts = "ro" if readonly else "rw"
        result = _run(["mount", "-o", mount_opts, partition, str(mount_point)])
        if result.returncode != 0:
            raise MountError("mount failed")

        mode = "read-only" if readonly else "read-write"
        logger.info(f"Mounted {image_path} at {mount_point} ({mode})")
        yield mount_point

    finally:
        if mount_point and mount_point.exists():
            if not readonly:
                _run(["sync"])
            result = _run(["umount", str(mount_point)])
            if result.returncode != 0:
                logger.warning("umount failed")
            try:
                mount_point.rmdir()
            except OSError:
                pass

        _detach_loop_device(loop_dev)
        logger.debug(f"Cleaned up mount for {image_path}")
