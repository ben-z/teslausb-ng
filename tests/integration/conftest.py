"""Fixtures for integration tests.

These tests are designed to run in a Docker container with --privileged flag.
They require root access, XFS support, and rclone.

Run with: docker compose -f docker-compose.test.yml up --build
"""

from __future__ import annotations

import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Generator, Tuple

import pytest


@dataclass
class IntegrationTestEnv:
    """Test environment with isolated paths."""

    root: Path
    mutable_path: Path
    backingfiles_path: Path
    config_path: Path

    @property
    def backingfiles_img(self) -> Path:
        return self.mutable_path / "backingfiles.img"

    @property
    def cam_disk_path(self) -> Path:
        return self.backingfiles_path / "cam_disk.bin"

    @property
    def snapshots_path(self) -> Path:
        return self.backingfiles_path / "snapshots"


def _cleanup_loop_devices(path_pattern: str | None = None) -> None:
    """Clean up loop devices, optionally filtering by path pattern."""
    # First, remove any kpartx mappings
    result = subprocess.run(["losetup", "-a"], capture_output=True, text=True)
    for line in result.stdout.splitlines():
        if ":" not in line:
            continue
        loop_dev = line.split(":")[0]
        if path_pattern is None or path_pattern in line:
            # Remove kpartx mappings first
            subprocess.run(["kpartx", "-d", loop_dev], capture_output=True)
            subprocess.run(["losetup", "-d", loop_dev], capture_output=True)

    # Also clean up any stale device mapper entries
    dm_path = Path("/dev/mapper")
    if dm_path.exists():
        for entry in dm_path.iterdir():
            if entry.name.startswith("loop"):
                subprocess.run(["dmsetup", "remove", str(entry)], capture_output=True)


def _is_mounted(path: Path) -> bool:
    """Check if path is a mount point."""
    result = subprocess.run(
        ["mountpoint", "-q", str(path)],
        capture_output=True,
    )
    return result.returncode == 0


def _cleanup_mounts_and_devices(env: IntegrationTestEnv) -> None:
    """Full cleanup of mounts and loop devices for a test environment."""
    # Unmount backingfiles if mounted
    if _is_mounted(env.backingfiles_path):
        subprocess.run(["umount", "-l", str(env.backingfiles_path)], check=False)

    # Unmount temp mount point
    tmp_mount = Path("/tmp/teslausb-setup-mount")
    if tmp_mount.exists() and _is_mounted(tmp_mount):
        subprocess.run(["umount", "-l", str(tmp_mount)], check=False)

    # Clean up loop devices for this test's files
    _cleanup_loop_devices(str(env.root))

    # Give system time to settle
    time.sleep(0.2)


def mount_cam_disk(disk_path: Path, mount_point: Path) -> tuple[str, str, bool]:
    """Mount a cam disk image, handling Docker kpartx fallback.

    Args:
        disk_path: Path to the disk image file
        mount_point: Directory to mount the partition

    Returns:
        Tuple of (loop_device, partition_device, kpartx_used)
    """
    # Clean up any stale loop devices for this disk
    result = subprocess.run(["losetup", "-j", str(disk_path)], capture_output=True, text=True)
    for line in result.stdout.splitlines():
        if ":" in line:
            old_loop = line.split(":")[0]
            subprocess.run(["kpartx", "-d", old_loop], capture_output=True)
            subprocess.run(["losetup", "-d", old_loop], capture_output=True)

    # Create loop device
    result = subprocess.run(
        ["losetup", "-f", "--show", str(disk_path)],
        capture_output=True,
        text=True,
    )
    loop_dev = result.stdout.strip()
    if result.returncode != 0 or not loop_dev:
        error_msg = result.stderr.strip() if result.stderr else "losetup failed with no error message"
        raise RuntimeError(f"Failed to set up loop device for {disk_path}: {error_msg}")
    partition = f"{loop_dev}p1"
    kpartx_used = False

    # Try to get kernel to recognize partition table
    subprocess.run(["blockdev", "--rereadpt", loop_dev], capture_output=True)

    # Wait for partition device to appear
    for _ in range(20):
        if Path(partition).exists():
            break
        time.sleep(0.1)

    # If partition not found, use kpartx (needed in Docker Desktop)
    if not Path(partition).exists():
        result = subprocess.run(["kpartx", "-av", loop_dev], capture_output=True)
        if result.returncode == 0:
            kpartx_used = True
            loop_name = Path(loop_dev).name
            partition = f"/dev/mapper/{loop_name}p1"
            for _ in range(20):
                if Path(partition).exists():
                    break
                time.sleep(0.1)

    subprocess.run(["mount", partition, str(mount_point)], check=True)
    return loop_dev, partition, kpartx_used


def unmount_cam_disk(mount_point: Path, loop_dev: str, kpartx_used: bool) -> None:
    """Unmount a cam disk and clean up loop devices."""
    subprocess.run(["umount", str(mount_point)], check=False)
    if kpartx_used:
        subprocess.run(["kpartx", "-d", loop_dev], check=False)
    subprocess.run(["losetup", "-d", loop_dev], check=False)


def create_test_footage(cam_mount: Path, event_name: str = "2024-01-15_10-30-00") -> None:
    """Create test TeslaCam footage structure."""
    saved = cam_mount / "TeslaCam" / "SavedClips"
    saved.mkdir(parents=True, exist_ok=True)

    event_dir = saved / event_name
    event_dir.mkdir(exist_ok=True)

    # Create fake video files
    for cam in ["front", "back", "left_repeater", "right_repeater"]:
        video = event_dir / f"{event_name}-{cam}.mp4"
        video.write_bytes(b"fake video content " * 100)

    # Create event.json
    event_json = event_dir / "event.json"
    event_json.write_text('{"timestamp": "2024-01-15T10:30:00"}')


@pytest.fixture
def test_env(tmp_path: Path) -> Generator[IntegrationTestEnv, None, None]:
    """Create an isolated test environment.

    Creates a temporary directory structure for testing:
    - {tmp}/mutable/       - Where backingfiles.img is created
    - {tmp}/backingfiles/  - Mount point for backingfiles.img
    - {tmp}/config         - Config file
    """
    # Clean up any leftover devices from previous test runs
    _cleanup_loop_devices()

    mutable_path = tmp_path / "mutable"
    backingfiles_path = tmp_path / "backingfiles"
    config_path = tmp_path / "teslausb.conf"

    mutable_path.mkdir()
    backingfiles_path.mkdir()

    # Create config file with test settings
    # Use 1G to meet minimum CAM_SIZE requirement
    config_content = f"""
CAM_SIZE=1G
MUTABLE_PATH={mutable_path}
BACKINGFILES_PATH={backingfiles_path}
ARCHIVE_SYSTEM=rclone
RCLONE_DRIVE=:memory:
RCLONE_PATH=/test
"""
    config_path.write_text(config_content.strip())

    env = IntegrationTestEnv(
        root=tmp_path,
        mutable_path=mutable_path,
        backingfiles_path=backingfiles_path,
        config_path=config_path,
    )

    yield env

    # Full cleanup after test
    _cleanup_mounts_and_devices(env)


@pytest.fixture
def cli_runner(test_env: IntegrationTestEnv):
    """Return a function to run CLI commands with the test config."""

    def run(*args: str, check: bool = True) -> subprocess.CompletedProcess:
        """Run teslausb CLI command with test config.

        Args:
            *args: Command arguments (e.g., "init", "status", "--json")
            check: If True, raise on non-zero exit code

        Returns:
            CompletedProcess with stdout/stderr captured
        """
        cmd = [
            "python3", "-m", "teslausb.cli",
            "-c", str(test_env.config_path),
            *args,
        ]
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
        )
        if check and result.returncode != 0:
            # Print output for debugging before raising
            print(f"Command failed: {' '.join(cmd)}")
            print(f"stdout: {result.stdout}")
            print(f"stderr: {result.stderr}")
            raise subprocess.CalledProcessError(
                result.returncode,
                cmd,
                result.stdout,
                result.stderr,
            )
        return result

    return run


@pytest.fixture
def initialized_env(test_env: IntegrationTestEnv, cli_runner) -> IntegrationTestEnv:
    """Test environment with init already run."""
    cli_runner("init")
    return test_env


@pytest.fixture
def cam_mount(initialized_env: IntegrationTestEnv) -> Generator[Path, None, None]:
    """Mount the cam_disk and yield the mount point."""
    mount_point = initialized_env.root / "cam_mount"
    mount_point.mkdir()

    loop_dev, partition, kpartx_used = mount_cam_disk(
        initialized_env.cam_disk_path, mount_point
    )

    yield mount_point

    unmount_cam_disk(mount_point, loop_dev, kpartx_used)
