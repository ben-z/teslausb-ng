"""Main coordinator for TeslaUSB.

This module provides the Coordinator class that orchestrates:
- Waiting for archive connectivity
- Taking snapshots
- Archiving footage
- Managing disk space
- Cleanup
- LED status indication
- Temperature monitoring
- Idle detection

This replaces the bash archiveloop script with a cleaner implementation.
"""

from __future__ import annotations

import logging
import signal
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from threading import Event
from typing import Any, Callable, Iterator

from .archive import ArchiveBackend, ArchiveManager, ArchiveResult, ArchiveState
from .filesystem import Filesystem
from .idle import IdleDetector
from .led import LedController, LedPattern
from .snapshot import SnapshotManager
from .space import GB, SpaceManager
from .temperature import TemperatureMonitor

logger = logging.getLogger(__name__)


def _backoff_intervals(base: float, maximum: float) -> Iterator[float]:
    """Yield exponentially increasing intervals: base, 2*base, 4*base, ..., capped at maximum.

    Both base and maximum must be positive.
    """
    if base <= 0:
        raise ValueError("base backoff interval must be positive")
    if maximum <= 0:
        raise ValueError("maximum backoff interval must be positive")
    interval = min(base, maximum)
    while True:
        yield interval
        interval = min(interval * 2, maximum)


class CoordinatorState(Enum):
    """State of the coordinator."""

    STARTING = "starting"
    WAITING_FOR_ARCHIVE = "waiting_for_archive"
    ARCHIVING = "archiving"
    STOPPED = "stopped"
    ERROR = "error"


@dataclass
class CoordinatorConfig:
    """Configuration for the Coordinator."""

    # Required - function to mount snapshot images
    mount_fn: Callable[[Path], Iterator[Path]]

    # Timing
    poll_interval: float = 5.0  # Seconds between archive reachability checks
    idle_timeout: float = 90.0  # Seconds to wait for idle before snapshot

    # Optional components (None = disabled)
    led_controller: LedController | None = None
    idle_detector: IdleDetector | None = None  # If set, waits for car to stop writing before snapshot
    temperature_monitor: TemperatureMonitor | None = None
    gadget: Any | None = None  # USB gadget (enable/disable/is_enabled) - disabled during cam_disk cleanup

    # Backoff
    max_idle_interval: float = 300.0  # Cap for both idle-cycle backoff and archive reachability retry backoff

    # Callbacks (optional)
    on_state_change: Callable[[CoordinatorState], None] | None = None
    on_archive_start: Callable[[], None] | None = None
    on_archive_complete: Callable[[ArchiveResult], None] | None = None
    on_error: Callable[[str], None] | None = None


class Coordinator:
    """Main coordinator that orchestrates TeslaUSB operations.

    The main loop:
    1. Wait for archive to become reachable (WiFi connected, server up)
    2. Wait for car to stop writing (idle detection)
    3. Take snapshot and archive
    4. Disable gadget, delete archived files from cam_disk, re-enable gadget
    5. Clean up old snapshots if needed
    6. Repeat

    This design ensures:
    - Snapshots are only taken when we're about to archive
    - Snapshots are locked during archiving (can't be deleted)
    - Space cleanup only deletes unreferenced snapshots
    - cam_disk.bin is never mounted while the gadget is active
    - Clean shutdown on SIGTERM/SIGINT
    """

    def __init__(
        self,
        fs: Filesystem,
        snapshot_manager: SnapshotManager,
        archive_manager: ArchiveManager,
        space_manager: SpaceManager,
        backend: ArchiveBackend,
        config: CoordinatorConfig,
    ):
        """Initialize the Coordinator.

        Args:
            fs: Filesystem abstraction
            snapshot_manager: SnapshotManager instance
            archive_manager: ArchiveManager instance
            space_manager: SpaceManager instance
            backend: Archive backend (for reachability checks)
            config: Coordinator configuration
        """
        self.fs = fs
        self.snapshot_manager = snapshot_manager
        self.archive_manager = archive_manager
        self.space_manager = space_manager
        self.backend = backend
        self.config = config

        self._state = CoordinatorState.STOPPED
        self._stop_event = Event()
        self._last_archive: ArchiveResult | None = None
        self._archive_count = 0
        self._error_count = 0

        # Share stop event with backend if it supports it (for interruptible operations)
        if hasattr(self.backend, 'stop_event'):
            self.backend.stop_event = self._stop_event

    @property
    def state(self) -> CoordinatorState:
        """Current coordinator state."""
        return self._state

    def _set_state(self, new_state: CoordinatorState) -> None:
        """Update state and notify callback."""
        old_state = self._state
        self._state = new_state
        logger.info(f"State: {old_state.value} -> {new_state.value}")

        # Update LED pattern based on state
        self._update_led_for_state(new_state)

        if self.config.on_state_change:
            try:
                self.config.on_state_change(new_state)
            except Exception as e:
                logger.warning(f"State change callback error: {e}")

    def _update_led_for_state(self, state: CoordinatorState) -> None:
        """Update LED pattern based on coordinator state."""
        if not self.config.led_controller:
            return

        led = self.config.led_controller
        if state == CoordinatorState.WAITING_FOR_ARCHIVE:
            led.set_pattern(LedPattern.SLOW_BLINK)
        elif state == CoordinatorState.ARCHIVING:
            led.set_pattern(LedPattern.FAST_BLINK)
        elif state == CoordinatorState.STOPPED:
            led.set_pattern(LedPattern.OFF)

    def _wait_interruptible(self, seconds: float) -> bool:
        """Wait for specified seconds, or until stop event.

        Returns:
            True if wait completed, False if interrupted by stop
        """
        return not self._stop_event.wait(timeout=seconds)

    def _wait_for_archive_reachable(self) -> bool:
        """Wait for archive destination to become reachable.

        Uses exponential backoff to avoid log spam during long WiFi outages.

        Returns:
            True if reachable, False if stopped
        """
        self._set_state(CoordinatorState.WAITING_FOR_ARCHIVE)

        for interval in _backoff_intervals(self.config.poll_interval, self.config.max_idle_interval):
            if self.backend.is_reachable():
                logger.info("Archive is reachable")
                return True

            logger.info(f"Archive not reachable, retrying in {interval:.0f}s")
            if not self._wait_interruptible(interval):
                return False

        return False  # unreachable: _backoff_intervals is infinite, but mypy needs this

    def _do_archive_cycle(self) -> bool:
        """Perform one archive cycle.

        1. Delete all stale snapshots from previous runs
        2. Wait for idle (car stops writing)
        3. Take snapshot and archive (safe - reads from snapshot only)
        4. Disable gadget, delete archived files from cam_disk, re-enable gadget
        5. Delete the snapshot

        Returns:
            True if successful, False on error or stop
        """
        self._set_state(CoordinatorState.ARCHIVING)

        # Delete all stale snapshots before creating a new one.
        # Snapshots pin COW blocks — the car keeps writing via the USB gadget,
        # and old snapshots prevent XFS from reclaiming space.
        stale = 0
        while self.snapshot_manager.delete_oldest_if_deletable():
            stale += 1
        if stale == 1:
            # One stale snapshot is expected after an unclean shutdown —
            # the post-archive deletion didn't run.
            logger.warning(
                "Deleted 1 stale snapshot (likely unclean shutdown)"
            )
        elif stale > 1:
            # With eager deletion, at most 1 snapshot should ever exist.
            # Multiple stale snapshots indicate a bug or first run after
            # upgrading from the old threshold-based cleanup.
            logger.error(
                f"Deleted {stale} stale snapshots — expected at most 1. "
                f"This may indicate a bug in snapshot lifecycle management."
            )

        # Wait for car to stop writing (if idle detector configured)
        if self.config.idle_detector:
            logger.info("Waiting for car to become idle...")
            if not self.config.idle_detector.wait_for_idle(self.config.idle_timeout):
                logger.warning("Timeout waiting for idle, proceeding anyway")

        # Notify archive start
        if self.config.on_archive_start:
            try:
                self.config.on_archive_start()
            except Exception as e:
                logger.warning(f"Archive start callback error: {e}")

        # Create snapshot and archive (deletion handled separately below)
        try:
            result = self.archive_manager.archive_new_snapshot(
                mount_fn=self.config.mount_fn,
                delete_after_archive=False,
            )
            self._last_archive = result
            self._archive_count += 1

            if result.state == ArchiveState.COMPLETED:
                logger.info(
                    f"Archive cycle {self._archive_count} complete: "
                    f"{result.files_transferred} files transferred"
                )
                # Delete archived files with gadget disabled to prevent
                # FAT corruption from concurrent access to cam_disk.bin
                if result.archived_files and self.archive_manager.cam_disk_path:
                    self._delete_archived_files(result)
            else:
                logger.warning(f"Archive cycle {self._archive_count} had issues: {result.error}")
                self._error_count += 1

            # Delete the snapshot now that archiving and cam_disk cleanup are done.
            # If this fails, start-of-cycle cleanup will handle it next time.
            if result.snapshot_id is not None:
                try:
                    self.snapshot_manager.delete_snapshot(result.snapshot_id)
                except Exception as e:
                    logger.warning(
                        f"Failed to delete snapshot {result.snapshot_id} "
                        f"after archive: {e} (will retry next cycle)"
                    )

            # Notify archive complete
            if self.config.on_archive_complete:
                try:
                    self.config.on_archive_complete(result)
                except Exception as e:
                    logger.warning(f"Archive complete callback error: {e}")

        except Exception as e:
            logger.error(f"Archive cycle failed: {e}")
            self._error_count += 1
            if self.config.on_error:
                self.config.on_error(str(e))
            return False

        return True

    def _delete_archived_files(self, result: ArchiveResult) -> None:
        """Delete archived files from cam_disk, disabling gadget during the operation.

        The USB gadget exposes cam_disk.bin to the car as a block device.
        Mounting it simultaneously via loop device for deletion causes FAT
        filesystem corruption from dual writer access. The gadget must be
        disabled first - this causes a brief USB disconnect that the car
        handles gracefully (removable media). The original teslausb project
        uses the same approach.

        After disabling the gadget, we run fsck to repair any FAT errors
        from the car's abrupt disconnection, then mount and delete files.
        """
        from .mount import fsck_image, mount_image

        gadget = self.config.gadget
        gadget_was_enabled = False

        # Disable gadget to prevent concurrent access to cam_disk.bin
        if gadget and gadget.is_enabled():
            logger.info("Disabling USB gadget for cam_disk cleanup")
            try:
                gadget.disable()
            except Exception as e:
                logger.error(f"Failed to disable gadget, skipping file deletion: {e}")
                return
            # Verify disable succeeded - UsbGadget.disable() may silently fail
            if gadget.is_enabled():
                logger.error("Gadget still enabled after disable, skipping file deletion")
                return
            gadget_was_enabled = True

        try:
            # Repair any FAT errors from the car's abrupt disconnection
            cam_disk = self.archive_manager.cam_disk_path
            if not fsck_image(cam_disk):
                logger.warning("fsck failed, proceeding with mount anyway")

            with mount_image(cam_disk, readonly=False) as cam_mount:
                deleted, skipped = self.archive_manager.delete_archived_files(result, cam_mount)
                logger.info(f"Cleanup complete: {deleted} deleted, {skipped} skipped")
        except Exception as e:
            logger.error(f"Failed to delete archived files: {e}")
        finally:
            if gadget_was_enabled:
                try:
                    gadget.enable()
                    logger.info("USB gadget re-enabled")
                except Exception as e:
                    logger.error(f"Failed to re-enable gadget: {e}")

    def run_once(self) -> bool:
        """Run a single archive cycle.

        Useful for testing or manual triggers.

        Returns:
            True if archive was successful
        """
        if not self.backend.is_reachable():
            logger.error("Archive not reachable")
            return False

        return self._do_archive_cycle()

    def run(self) -> None:
        """Run the main coordinator loop.

        This runs until stop() is called or a signal is received.
        """
        self._stop_event.clear()
        self._set_state(CoordinatorState.STARTING)

        # Set up signal handlers
        def handle_signal(signum, frame):
            logger.info(f"Received signal {signum}, stopping...")
            self.stop()

        old_sigterm = signal.signal(signal.SIGTERM, handle_signal)
        old_sigint = signal.signal(signal.SIGINT, handle_signal)

        # Start temperature monitoring
        if self.config.temperature_monitor:
            self.config.temperature_monitor.start()

        try:
            logger.info("Coordinator starting")

            cam_disk = self.archive_manager.cam_disk_path
            if cam_disk and self.fs.exists(cam_disk):
                cam_size = self.fs.stat(cam_disk).size
                space = self.space_manager.get_space_info()
                if cam_size > space.total_bytes * 0.50:
                    logger.error(
                        f"cam_disk.bin ({cam_size / GB:.1f} GiB) exceeds 50% of "
                        f"backing store ({space.total_gb:.1f} GiB) — snapshot COW "
                        f"may exhaust XFS space"
                    )

            def new_idle_backoff() -> Iterator[float]:
                return _backoff_intervals(self.config.poll_interval, self.config.max_idle_interval)

            idle_backoff = new_idle_backoff()

            while not self._stop_event.is_set():
                # Wait for archive to be reachable
                if not self._wait_for_archive_reachable():
                    break

                # Do archive cycle (waits for idle, then archives)
                if not self._do_archive_cycle():
                    # On error, reset backoff and wait before retrying
                    idle_backoff = new_idle_backoff()
                    if not self._wait_interruptible(30):
                        break
                    continue

                # Backoff when nothing to archive to avoid hot-looping.
                # Only back off on successful cycles with zero transfers (truly idle).
                # Failed cycles are handled by the error path above.
                if (self._last_archive
                        and self._last_archive.success
                        and self._last_archive.files_transferred == 0):
                    delay = next(idle_backoff)
                    logger.info(f"No files to archive, waiting {delay:.0f}s before next cycle")
                else:
                    idle_backoff = new_idle_backoff()
                    delay = self.config.poll_interval

                if not self._wait_interruptible(delay):
                    break

        finally:
            # Stop temperature monitoring
            if self.config.temperature_monitor:
                self.config.temperature_monitor.stop()

            # Restore signal handlers
            signal.signal(signal.SIGTERM, old_sigterm)
            signal.signal(signal.SIGINT, old_sigint)

            self._set_state(CoordinatorState.STOPPED)
            logger.info(
                f"Coordinator stopped. Archives: {self._archive_count}, Errors: {self._error_count}"
            )

    def stop(self) -> None:
        """Stop the coordinator gracefully."""
        logger.info("Stop requested")
        self._stop_event.set()

    def get_status(self) -> dict:
        """Get current status information."""
        space_info = self.space_manager.get_space_info()
        snapshots = self.snapshot_manager.get_snapshots()

        status = {
            "state": self._state.value,
            "archive_count": self._archive_count,
            "error_count": self._error_count,
            "last_archive": self._last_archive.snapshot_id if self._last_archive else None,
            "space": {
                "free_gb": space_info.free_gb,
                "total_gb": space_info.total_gb,
            },
            "snapshots": {
                "count": len(snapshots),
                "deletable": len(self.snapshot_manager.get_deletable_snapshots()),
                "ids": [s.id for s in snapshots],
            },
            "archive_reachable": self.backend.is_reachable(),
        }

        # Add temperature info if monitoring is enabled
        if self.config.temperature_monitor:
            temp_status = self.config.temperature_monitor.get_status()
            status["temperature"] = {
                "current_celsius": temp_status.current.celsius if temp_status.current else None,
                "peak_celsius": temp_status.peak.celsius if temp_status.peak else None,
                "warning_triggered": temp_status.warning_triggered,
                "caution_triggered": temp_status.caution_triggered,
            }

        # Add LED info if controller is enabled
        if self.config.led_controller:
            status["led_pattern"] = self.config.led_controller.get_pattern().value

        return status
