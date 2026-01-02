"""Integration tests for the archive cycle."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from .conftest import IntegrationTestEnv, mount_cam_disk, unmount_cam_disk

pytestmark = pytest.mark.integration


def _create_test_footage(cam_mount: Path, event_name: str = "2024-01-15_10-30-00") -> None:
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


class TestArchiveCycle:
    """Tests for the full archive cycle."""

    def test_archive_creates_snapshot(
        self, initialized_env: IntegrationTestEnv, cli_runner, cam_mount: Path
    ):
        """Archive should create a snapshot."""
        _create_test_footage(cam_mount)

        # Unmount cam_disk so archive can create snapshot
        subprocess.run(["umount", str(cam_mount)], check=True)

        # Run archive
        cli_runner("archive", check=False)

        # Check snapshot was created
        snapshots = list(initialized_env.snapshots_path.glob("snap-*"))
        assert len(snapshots) == 1, f"Expected 1 snapshot, got {len(snapshots)}"

    def test_archive_snapshot_has_toc(
        self, initialized_env: IntegrationTestEnv, cli_runner, cam_mount: Path
    ):
        """Archive snapshot should have .toc file (completion marker)."""
        _create_test_footage(cam_mount)
        subprocess.run(["umount", str(cam_mount)], check=True)

        cli_runner("archive", check=False)

        snapshots = list(initialized_env.snapshots_path.glob("snap-*"))
        assert len(snapshots) == 1

        toc_file = snapshots[0] / "snap.toc"
        assert toc_file.exists(), "Snapshot should have .toc file"

    def test_archive_snapshot_has_metadata(
        self, initialized_env: IntegrationTestEnv, cli_runner, cam_mount: Path
    ):
        """Archive snapshot should have metadata.json."""
        _create_test_footage(cam_mount)
        subprocess.run(["umount", str(cam_mount)], check=True)

        cli_runner("archive", check=False)

        snapshots = list(initialized_env.snapshots_path.glob("snap-*"))
        assert len(snapshots) == 1

        metadata_file = snapshots[0] / "metadata.json"
        assert metadata_file.exists(), "Snapshot should have metadata.json"

        metadata = json.loads(metadata_file.read_text())
        assert "id" in metadata
        assert "path" in metadata
        assert "created_at" in metadata

    def test_snapshots_command_shows_snapshot(
        self, initialized_env: IntegrationTestEnv, cli_runner, cam_mount: Path
    ):
        """After archive, snapshots command should list the snapshot."""
        _create_test_footage(cam_mount)
        subprocess.run(["umount", str(cam_mount)], check=True)

        cli_runner("archive", check=False)

        result = cli_runner("snapshots")
        assert "snap-" in result.stdout

    def test_snapshots_json_has_snapshot(
        self, initialized_env: IntegrationTestEnv, cli_runner, cam_mount: Path
    ):
        """After archive, snapshots --json should include snapshot data."""
        _create_test_footage(cam_mount)
        subprocess.run(["umount", str(cam_mount)], check=True)

        cli_runner("archive", check=False)

        result = cli_runner("snapshots", "--json")
        data = json.loads(result.stdout)

        assert len(data) == 1
        assert "id" in data[0]
        assert "path" in data[0]
        assert "created_at" in data[0]

    def test_status_shows_snapshot_count(
        self, initialized_env: IntegrationTestEnv, cli_runner, cam_mount: Path
    ):
        """After archive, status should show snapshot count."""
        _create_test_footage(cam_mount)
        subprocess.run(["umount", str(cam_mount)], check=True)

        cli_runner("archive", check=False)

        result = cli_runner("status", "--json")
        data = json.loads(result.stdout)

        assert data["snapshots"]["count"] == 1


class TestMultipleArchiveCycles:
    """Tests for multiple archive cycles."""

    def test_multiple_archives_create_multiple_snapshots(
        self, initialized_env: IntegrationTestEnv, cli_runner, cam_mount: Path
    ):
        """Multiple archive cycles should create multiple snapshots."""
        # First archive
        _create_test_footage(cam_mount, "event1")
        subprocess.run(["umount", str(cam_mount)], check=True)
        cli_runner("archive", check=False)

        # Remount and add more footage
        loop_dev, partition, kpartx_used = mount_cam_disk(
            initialized_env.cam_disk_path, cam_mount
        )

        _create_test_footage(cam_mount, "event2")
        unmount_cam_disk(cam_mount, loop_dev, kpartx_used)

        # Second archive
        cli_runner("archive", check=False)

        # Check we have 2 snapshots
        snapshots = list(initialized_env.snapshots_path.glob("snap-*"))
        assert len(snapshots) == 2, f"Expected 2 snapshots, got {len(snapshots)}"

    def test_snapshot_ids_increment(
        self, initialized_env: IntegrationTestEnv, cli_runner, cam_mount: Path
    ):
        """Snapshot IDs should increment monotonically."""
        # First archive
        _create_test_footage(cam_mount, "event1")
        subprocess.run(["umount", str(cam_mount)], check=True)
        cli_runner("archive", check=False)

        # Remount and second archive
        loop_dev, partition, kpartx_used = mount_cam_disk(
            initialized_env.cam_disk_path, cam_mount
        )

        _create_test_footage(cam_mount, "event2")
        unmount_cam_disk(cam_mount, loop_dev, kpartx_used)

        cli_runner("archive", check=False)

        # Check IDs
        result = cli_runner("snapshots", "--json")
        data = json.loads(result.stdout)

        ids = sorted([s["id"] for s in data])
        assert ids == [0, 1], f"Expected IDs [0, 1], got {ids}"
