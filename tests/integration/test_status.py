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
        assert "Used:" in result.stdout

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
        assert "used_gb" in space

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

    def test_clean_with_deletable_snapshot(
        self, initialized_env: IntegrationTestEnv, cli_runner, cam_mount
    ):
        """Clean should delete all deletable snapshots."""
        # Create a snapshot via archive
        create_test_footage(cam_mount, "event1")
        subprocess.run(["umount", str(cam_mount)], check=True)
        cli_runner("archive", check=False)

        # With eager deletion, at most 1 snapshot survives each archive cycle
        snapshots_before = list(initialized_env.snapshots_path.glob("snap-*"))
        if not snapshots_before:
            pytest.skip("No snapshot survived archive (post-archive deletion succeeded)")

        # Run clean
        result = cli_runner("clean")

        # Verify snapshots were deleted
        assert "Deleted" in result.stdout
        snapshots_after = list(initialized_env.snapshots_path.glob("snap-*"))
        assert len(snapshots_after) == 0, f"Expected 0 snapshots after clean, got {len(snapshots_after)}"

    def test_clean_dry_run_with_deletable_snapshot(
        self, initialized_env: IntegrationTestEnv, cli_runner, cam_mount
    ):
        """Clean --dry-run should show what would be deleted without deleting."""
        # Create a snapshot via archive
        create_test_footage(cam_mount, "event1")
        subprocess.run(["umount", str(cam_mount)], check=True)
        cli_runner("archive", check=False)

        # With eager deletion, at most 1 snapshot survives
        snapshots_before = list(initialized_env.snapshots_path.glob("snap-*"))
        if not snapshots_before:
            pytest.skip("No snapshot survived archive (post-archive deletion succeeded)")

        n = len(snapshots_before)

        # Run clean --dry-run
        result = cli_runner("clean", "--dry-run")

        # Verify output shows what would be deleted
        assert "Would delete" in result.stdout
        assert "snap-" in result.stdout

        # Verify snapshots were NOT actually deleted
        snapshots_after = list(initialized_env.snapshots_path.glob("snap-*"))
        assert len(snapshots_after) == n, (
            f"Expected {n} snapshots after dry-run, got {len(snapshots_after)}"
        )
