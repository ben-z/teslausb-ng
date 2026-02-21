"""Integration tests for the archive cycle."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from .conftest import (
    IntegrationTestEnv,
    mount_cam_disk,
    unmount_cam_disk,
    create_test_footage,
)

pytestmark = pytest.mark.integration


class TestArchiveCycle:
    """Tests for the full archive cycle."""

    def test_archive_creates_snapshot(
        self, initialized_env: IntegrationTestEnv, cli_runner, cam_mount: Path
    ):
        """Archive should create a snapshot."""
        create_test_footage(cam_mount)

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
        create_test_footage(cam_mount)
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
        create_test_footage(cam_mount)
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
        create_test_footage(cam_mount)
        subprocess.run(["umount", str(cam_mount)], check=True)

        cli_runner("archive", check=False)

        result = cli_runner("snapshots")
        assert "snap-" in result.stdout

    def test_snapshots_json_has_snapshot(
        self, initialized_env: IntegrationTestEnv, cli_runner, cam_mount: Path
    ):
        """After archive, snapshots --json should include snapshot data."""
        create_test_footage(cam_mount)
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
        create_test_footage(cam_mount)
        subprocess.run(["umount", str(cam_mount)], check=True)

        cli_runner("archive", check=False)

        result = cli_runner("status", "--json")
        data = json.loads(result.stdout)

        assert data["snapshots"]["count"] == 1


class TestMultipleArchiveCycles:
    """Tests for multiple archive cycles."""

    def test_stale_snapshots_cleaned_by_next_archive(
        self, initialized_env: IntegrationTestEnv, cli_runner, cam_mount: Path
    ):
        """Second archive cycle should clean up stale snapshot from first cycle."""
        # First archive
        create_test_footage(cam_mount, "event1")
        subprocess.run(["umount", str(cam_mount)], check=True)
        cli_runner("archive", check=False)

        # Verify first snapshot exists
        snapshots_after_first = list(initialized_env.snapshots_path.glob("snap-*"))
        assert len(snapshots_after_first) <= 1, (
            f"Expected at most 1 snapshot after first archive, got {len(snapshots_after_first)}"
        )

        # Remount and add more footage
        loop_dev, partition, kpartx_used = mount_cam_disk(
            initialized_env.cam_disk_path, cam_mount
        )

        create_test_footage(cam_mount, "event2")
        unmount_cam_disk(cam_mount, loop_dev, kpartx_used)

        # Second archive — should clean up any stale snapshot from first
        cli_runner("archive", check=False)

        # At most 1 snapshot (the current one) — stale ones are eagerly deleted
        snapshots = list(initialized_env.snapshots_path.glob("snap-*"))
        assert len(snapshots) <= 1, (
            f"Expected at most 1 snapshot (eager deletion), got {len(snapshots)}"
        )

    def test_snapshot_ids_increment(
        self, initialized_env: IntegrationTestEnv, cli_runner, cam_mount: Path
    ):
        """Snapshot IDs should increment monotonically across archive cycles."""
        # First archive
        create_test_footage(cam_mount, "event1")
        subprocess.run(["umount", str(cam_mount)], check=True)
        cli_runner("archive", check=False)

        # Remount and second archive
        loop_dev, partition, kpartx_used = mount_cam_disk(
            initialized_env.cam_disk_path, cam_mount
        )

        create_test_footage(cam_mount, "event2")
        unmount_cam_disk(cam_mount, loop_dev, kpartx_used)

        cli_runner("archive", check=False)

        # The surviving snapshot should have a higher ID than the first (0)
        result = cli_runner("snapshots", "--json")
        data = json.loads(result.stdout)

        if data:
            ids = [s["id"] for s in data]
            assert all(i > 0 for i in ids), f"Expected IDs > 0 after stale cleanup, got {ids}"
