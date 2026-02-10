"""Integration tests for status and info commands."""

from __future__ import annotations

import json
import subprocess

import pytest

from .conftest import IntegrationTestEnv, create_test_footage, mount_cam_disk

pytestmark = pytest.mark.integration


class TestStatusCommand:
    """Tests for teslausb status."""

    def test_status_before_init(self, test_env: IntegrationTestEnv, cli_runner):
        """Status should show warning when not initialized."""
        result = cli_runner("status", check=False)

        # Should succeed but show warnings
        assert result.returncode == 0
        assert "warning" in result.stdout.lower()
        assert "not mounted" in result.stdout.lower()

    def test_status_after_init(self, initialized_env: IntegrationTestEnv, cli_runner):
        """Status should show space and snapshot info after init."""
        result = cli_runner("status")

        assert "Space:" in result.stdout
        assert "Snapshots:" in result.stdout
        assert "Archive:" in result.stdout

    def test_status_shows_space_info(
        self, initialized_env: IntegrationTestEnv, cli_runner
    ):
        """Status should show disk space information."""
        result = cli_runner("status")

        assert "Total:" in result.stdout
        assert "Free:" in result.stdout
        assert "Min free:" in result.stdout
        assert "Can snapshot:" in result.stdout

    def test_status_json_output(
        self, initialized_env: IntegrationTestEnv, cli_runner
    ):
        """Status --json should return valid JSON."""
        result = cli_runner("status", "--json")

        data = json.loads(result.stdout)
        assert "space" in data
        assert "snapshots" in data
        assert "archive" in data
        assert "warnings" in data

    def test_status_json_space_fields(
        self, initialized_env: IntegrationTestEnv, cli_runner
    ):
        """Status JSON should have correct space fields."""
        result = cli_runner("status", "--json")
        data = json.loads(result.stdout)

        space = data["space"]
        assert "total_gb" in space
        assert "free_gb" in space
        assert "min_free_gb" in space
        assert "can_snapshot" in space

    def test_status_shows_archive_system(
        self, initialized_env: IntegrationTestEnv, cli_runner
    ):
        """Status should show configured archive system."""
        result = cli_runner("status")

        assert "rclone" in result.stdout.lower()


class TestSnapshotsCommand:
    """Tests for teslausb snapshots."""

    def test_snapshots_empty(self, initialized_env: IntegrationTestEnv, cli_runner):
        """Snapshots should show empty message when no snapshots."""
        result = cli_runner("snapshots")

        assert "No snapshots" in result.stdout

    def test_snapshots_json_empty(
        self, initialized_env: IntegrationTestEnv, cli_runner
    ):
        """Snapshots --json should return empty array when no snapshots."""
        result = cli_runner("snapshots", "--json")

        data = json.loads(result.stdout)
        assert data == []


class TestCleanCommand:
    """Tests for teslausb clean."""

    def test_clean_dry_run_no_snapshots(
        self, initialized_env: IntegrationTestEnv, cli_runner
    ):
        """Clean --dry-run should work with no snapshots."""
        result = cli_runner("clean", "--dry-run")

        assert "No deletable snapshots" in result.stdout

    def test_clean_no_snapshots(
        self, initialized_env: IntegrationTestEnv, cli_runner
    ):
        """Clean should succeed with no snapshots to delete."""
        result = cli_runner("clean")

        assert result.returncode == 0

    def test_clean_all_with_deletable_snapshots(
        self, initialized_env: IntegrationTestEnv, cli_runner, cam_mount
    ):
        """Clean --all should delete all deletable snapshots."""
        # Create multiple snapshots
        for i, event_name in enumerate(["event1", "event2", "event3"]):
            create_test_footage(cam_mount, event_name)
            subprocess.run(["umount", str(cam_mount)], check=True)
            cli_runner("archive", check=False)

            # Remount for next iteration (except the last one)
            if i < 2:
                loop_dev, partition, kpartx_used = mount_cam_disk(
                    initialized_env.cam_disk_path, cam_mount
                )

        # Verify snapshots were created
        snapshots_before = list(initialized_env.snapshots_path.glob("snap-*"))
        assert len(snapshots_before) == 3, f"Expected 3 snapshots, got {len(snapshots_before)}"

        # Run clean --all
        result = cli_runner("clean", "--all")

        # Verify all snapshots were deleted
        assert "Deleted 3 snapshots" in result.stdout
        snapshots_after = list(initialized_env.snapshots_path.glob("snap-*"))
        assert len(snapshots_after) == 0, f"Expected 0 snapshots after clean, got {len(snapshots_after)}"

    def test_clean_all_dry_run_with_deletable_snapshots(
        self, initialized_env: IntegrationTestEnv, cli_runner, cam_mount
    ):
        """Clean --all --dry-run should show what would be deleted."""
        # Create multiple snapshots
        for i, event_name in enumerate(["event1", "event2"]):
            create_test_footage(cam_mount, event_name)
            subprocess.run(["umount", str(cam_mount)], check=True)
            cli_runner("archive", check=False)

            # Remount for next iteration (except the last one)
            if i < 1:
                loop_dev, partition, kpartx_used = mount_cam_disk(
                    initialized_env.cam_disk_path, cam_mount
                )

        # Verify snapshots were created
        snapshots_before = list(initialized_env.snapshots_path.glob("snap-*"))
        assert len(snapshots_before) == 2, f"Expected 2 snapshots, got {len(snapshots_before)}"

        # Run clean --all --dry-run
        result = cli_runner("clean", "--all", "--dry-run")

        # Verify output shows what would be deleted
        assert "Would delete 2 snapshots:" in result.stdout
        assert "snap-" in result.stdout

        # Verify snapshots were NOT actually deleted
        snapshots_after = list(initialized_env.snapshots_path.glob("snap-*"))
        assert len(snapshots_after) == 2, f"Expected 2 snapshots after dry-run, got {len(snapshots_after)}"

    def test_clean_all_deletes_even_when_space_sufficient(
        self, initialized_env: IntegrationTestEnv, cli_runner, cam_mount
    ):
        """Clean --all should delete snapshots even when space is sufficient."""
        # Create a single snapshot (space should be sufficient)
        create_test_footage(cam_mount, "event1")
        subprocess.run(["umount", str(cam_mount)], check=True)
        cli_runner("archive", check=False)

        # Verify snapshot was created
        snapshots_before = list(initialized_env.snapshots_path.glob("snap-*"))
        assert len(snapshots_before) == 1, f"Expected 1 snapshot, got {len(snapshots_before)}"

        # Verify space is sufficient (status should not indicate low space)
        status_result = cli_runner("status", "--json")
        status_data = json.loads(status_result.stdout)
        assert status_data["space"]["can_snapshot"], "Space should be sufficient"

        # Run clean --all (should still delete despite sufficient space)
        result = cli_runner("clean", "--all")

        # Verify snapshot was deleted
        assert "Deleted 1 snapshot" in result.stdout
        snapshots_after = list(initialized_env.snapshots_path.glob("snap-*"))
        assert len(snapshots_after) == 0, f"Expected 0 snapshots after clean, got {len(snapshots_after)}"
