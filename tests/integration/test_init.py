"""Integration tests for the init command."""

from __future__ import annotations

import subprocess


import pytest

from .conftest import IntegrationTestEnv

pytestmark = pytest.mark.integration


class TestInitCommand:
    """Tests for teslausb init."""

    def test_init_creates_backingfiles_image(
        self, test_env: IntegrationTestEnv, cli_runner
    ):
        """Init should create the backingfiles.img XFS image."""
        result = cli_runner("init")

        assert test_env.backingfiles_img.exists()
        assert "Initialization complete" in result.stdout

    def test_init_mounts_backingfiles(
        self, test_env: IntegrationTestEnv, cli_runner
    ):
        """Init should mount backingfiles.img."""
        cli_runner("init")

        # Check mount point is active
        result = subprocess.run(
            ["mountpoint", "-q", str(test_env.backingfiles_path)],
        )
        assert result.returncode == 0, "backingfiles should be mounted"

    def test_init_creates_xfs_filesystem(
        self, test_env: IntegrationTestEnv, cli_runner
    ):
        """Init should create XFS filesystem (required for reflinks)."""
        cli_runner("init")

        # Check filesystem type
        result = subprocess.run(
            ["stat", "-f", "-c", "%T", str(test_env.backingfiles_path)],
            capture_output=True,
            text=True,
        )
        assert result.stdout.strip() == "xfs"

    def test_init_creates_cam_disk(
        self, test_env: IntegrationTestEnv, cli_runner
    ):
        """Init should create cam_disk.bin with FAT32 partition."""
        cli_runner("init")

        assert test_env.cam_disk_path.exists()

        # Check it has a partition table
        result = subprocess.run(
            ["parted", "-s", str(test_env.cam_disk_path), "print"],
            capture_output=True,
            text=True,
        )
        assert "fat32" in result.stdout.lower()

    def test_init_creates_snapshots_directory(
        self, test_env: IntegrationTestEnv, cli_runner
    ):
        """Init should create snapshots directory."""
        cli_runner("init")

        assert test_env.snapshots_path.exists()
        assert test_env.snapshots_path.is_dir()

    def test_init_fails_if_already_initialized(
        self, test_env: IntegrationTestEnv, cli_runner
    ):
        """Init should fail if backingfiles.img already exists."""
        cli_runner("init")

        # Second init should fail
        result = cli_runner("init", check=False)
        assert result.returncode != 0
        assert "already exists" in result.stdout

    def test_init_shows_next_steps(
        self, test_env: IntegrationTestEnv, cli_runner
    ):
        """Init should show helpful next steps."""
        result = cli_runner("init")

        assert "Next steps" in result.stdout
        assert "gadget on" in result.stdout
        assert "run" in result.stdout


class TestXfsReflinks:
    """Tests for XFS reflink functionality."""

    def test_reflinks_work(self, initialized_env: IntegrationTestEnv):
        """Verify reflinks work on the XFS filesystem."""
        # Create a test file
        test_file = initialized_env.backingfiles_path / "test.bin"
        test_file.write_bytes(b"x" * 1024)

        # Create a reflink copy
        copy_file = initialized_env.backingfiles_path / "test_copy.bin"
        result = subprocess.run(
            ["cp", "--reflink=always", str(test_file), str(copy_file)],
            capture_output=True,
        )

        assert result.returncode == 0, "reflink copy should succeed"
        assert copy_file.exists()
        assert copy_file.read_bytes() == test_file.read_bytes()

    def test_cam_disk_reflink_copy(self, initialized_env: IntegrationTestEnv):
        """Verify cam_disk.bin can be reflink copied (snapshot mechanism)."""
        snap_dir = initialized_env.snapshots_path / "snap-test"
        snap_dir.mkdir()

        snap_bin = snap_dir / "snap.bin"
        result = subprocess.run(
            [
                "cp", "--reflink=always",
                str(initialized_env.cam_disk_path),
                str(snap_bin),
            ],
            capture_output=True,
        )

        assert result.returncode == 0, "reflink copy of cam_disk should work"
        assert snap_bin.exists()


class TestDeinitCommand:
    """Tests for teslausb deinit."""

    def test_deinit_removes_backingfiles(
        self, initialized_env: IntegrationTestEnv, cli_runner
    ):
        """Deinit should remove backingfiles.img."""
        result = cli_runner("deinit", "--yes")

        assert not initialized_env.backingfiles_img.exists()
        assert "Deinitialization complete" in result.stdout

    def test_deinit_unmounts_first(
        self, initialized_env: IntegrationTestEnv, cli_runner
    ):
        """Deinit should unmount backingfiles before removing."""
        # Verify it's mounted
        result = subprocess.run(
            ["mountpoint", "-q", str(initialized_env.backingfiles_path)],
        )
        assert result.returncode == 0, "should be mounted before deinit"

        cli_runner("deinit", "--yes")

        # Verify it's unmounted
        result = subprocess.run(
            ["mountpoint", "-q", str(initialized_env.backingfiles_path)],
        )
        assert result.returncode != 0, "should be unmounted after deinit"

    def test_deinit_noop_if_not_initialized(
        self, test_env: IntegrationTestEnv, cli_runner
    ):
        """Deinit should be a no-op if not initialized."""
        result = cli_runner("deinit", "--yes")

        assert result.returncode == 0
        assert "Nothing to do" in result.stdout
