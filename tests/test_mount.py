"""Verify mount cleanup never hides an attached writable filesystem."""

import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from teslausb.mount import MountError, fsck_image, mount_image


def test_failed_unmount_does_not_detach_loop_or_remove_mount_directory():
    commands = []

    def run(cmd):
        commands.append(cmd)
        return subprocess.CompletedProcess(cmd, 1 if cmd[0] == "umount" else 0, b"", b"")

    with (
        patch("teslausb.mount._setup_loop_device", return_value=("/dev/loop1", "/dev/loop1p1")),
        patch("teslausb.mount.tempfile.mkdtemp", return_value="/mount/test"),
        patch("teslausb.mount._run", side_effect=run),
        patch.object(Path, "rmdir") as remove,
        pytest.raises(MountError, match="Failed to unmount"),
        mount_image(Path("/disk.bin"), readonly=False),
    ):
        pass
    assert [cmd[0] for cmd in commands] == ["mount", "umount"]
    remove.assert_not_called()


def test_failed_mount_detaches_loop_without_unmounting():
    commands = []

    def run(cmd):
        commands.append(cmd)
        return subprocess.CompletedProcess(cmd, 1 if cmd[0] == "mount" else 0, b"", b"")

    with (
        patch("teslausb.mount._setup_loop_device", return_value=("/dev/loop1", "/dev/loop1p1")),
        patch("teslausb.mount.tempfile.mkdtemp", return_value="/mount/test"),
        patch("teslausb.mount._run", side_effect=run),
        patch.object(Path, "rmdir"),
        pytest.raises(MountError, match="mount failed"),
        mount_image(Path("/disk.bin"), readonly=False),
    ):
        pass
    assert commands[-1] == ["losetup", "-d", "/dev/loop1"]
    assert all(cmd[0] != "umount" for cmd in commands)


def test_fsck_logs_repair_details(caplog):
    caplog.set_level("INFO")
    result = subprocess.CompletedProcess([], 1, b"Bad long filename repaired\n", b"")
    with (
        patch("teslausb.mount._setup_loop_device", return_value=("/dev/loop1", "/dev/loop1p1")),
        patch(
            "teslausb.mount._run",
            side_effect=[result, subprocess.CompletedProcess([], 0, b"", b"")],
        ),
        patch("teslausb.mount._detach_loop_device"),
    ):
        assert fsck_image(Path("/disk.bin"))
    assert "Bad long filename repaired" in caplog.text


def test_fsck_repair_requires_clean_verification():
    repaired = subprocess.CompletedProcess([], 1, b"Repaired errors\n", b"")
    inconsistent = subprocess.CompletedProcess([], 1, b"Bad long filename\n", b"")
    with (
        patch("teslausb.mount._setup_loop_device", return_value=("/dev/loop1", "/dev/loop1p1")),
        patch("teslausb.mount._run", side_effect=[repaired, inconsistent]),
        patch("teslausb.mount._detach_loop_device"),
    ):
        assert not fsck_image(Path("/disk.bin"))


def test_fsck_refuses_an_already_attached_image():
    attached = subprocess.CompletedProcess([], 0, b"/dev/loop1\n", b"")
    with (
        patch("teslausb.mount._run", return_value=attached) as run,
        pytest.raises(MountError, match="already attached"),
    ):
        fsck_image(Path("/disk.bin"))
    run.assert_called_once()
