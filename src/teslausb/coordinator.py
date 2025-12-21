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
from typing import Callable, Iterator

from .archive import ArchiveBackend, ArchiveManager, ArchiveResult, ArchiveState
from .filesystem import Filesystem
from .idle import IdleDetector
from .led import LedController, LedPattern
from .mount import mount_image
from .snapshot import SnapshotManager
from .space import SpaceManager
from .temperature import TemperatureMonitor

logger = logging.getLogger(__name__)


class CoordinatorState(Enum):
    """State of the coordinator."""

    STARTING = "starting"
    WAITING_FOR_ARCHIVE = "waiting_for_archive"
    ARCHIVING = "archiving"
    CLEANING = "cleaning"
    STOPPED = "stopped"
    ERROR = "error"


@dataclass
class CoordinatorConfig:
    """Configuration for the Coordinator."""

    # Timing
    poll_interval: float = 5.0  # Seconds between archive reachability checks
    idle_timeout: float = 90.0  # Seconds to wait for idle before snapshot

    # Archive settings
    mount_fn: Callable[[Path], Iterator[Path]] | None = None  # None = use mock path for testing
    wait_for_idle: bool = True  # Wait for car to stop writing before snapshot

    # Optional components (None = disabled)
    led_controller: LedController | None = None
    idle_detector: IdleDetector | None = None
    temperature_monitor: TemperatureMonitor | None = None

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
    4. Clean up old snapshots if needed
    5. Repeat

    This design ensures:
    - Snapshots are only taken when we're about to archive
    - Snapshots are locked during archiving (can't be deleted)
    - Space cleanup only deletes unreferenced snapshots
    - Clean shutdown on SIGTERM/SIGINT
    """

    def __init__(
        self,
        fs: Filesystem,
        snapshot_manager: SnapshotManager,
        archive_manager: ArchiveManager,
        space_manager: SpaceManager,
        backend: ArchiveBackend,
        config: CoordinatorConfig | None = None,
    ):
        """Initialize the Coordinator.

        Args:
            fs: Filesystem abstraction
            snapshot_manager: SnapshotManager instance
            archive_manager: ArchiveManager instance
            space_manager: SpaceManager instance
            backend: Archive backend (for reachability checks)
            config: Optional configuration
        """
        self.fs = fs
        self.snapshot_manager = snapshot_manager
        self.archive_manager = archive_manager
        self.space_manager = space_manager
        self.backend = backend
        self.config = config or CoordinatorConfig()

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

        Returns:
            True if reachable, False if stopped
        """
        self._set_state(CoordinatorState.WAITING_FOR_ARCHIVE)

        while not self._stop_event.is_set():
            if self.backend.is_reachable():
                logger.info("Archive is reachable")
                return True

            logger.debug("Archive not reachable, waiting...")
            if not self._wait_interruptible(self.config.poll_interval):
                return False

        return False

    def _do_archive_cycle(self) -> bool:
        """Perform one archive cycle.

        1. Wait for idle (car stops writing)
        2. Ensure space is available
        3. Take snapshot
        4. Archive snapshot
        5. Clean up if needed

        Returns:
            True if successful, False on error or stop
        """
        self._set_state(CoordinatorState.ARCHIVING)

        # Wait for car to stop writing (if enabled)
        if self.config.wait_for_idle and self.config.idle_detector:
            logger.info("Waiting for car to become idle...")
            if not self.config.idle_detector.wait_for_idle(self.config.idle_timeout):
                logger.warning("Timeout waiting for idle, proceeding anyway")

        # Ensure space for snapshot
        if not self.space_manager.ensure_space_for_snapshot():
            logger.error("Cannot ensure space for snapshot")
            if self.config.on_error:
                self.config.on_error("Cannot ensure space for snapshot")
            return False

        # Notify archive start
        if self.config.on_archive_start:
            try:
                self.config.on_archive_start()
            except Exception as e:
                logger.warning(f"Archive start callback error: {e}")

        # Create snapshot and archive
        try:
            result = self.archive_manager.archive_new_snapshot(
                mount_fn=self.config.mount_fn,
            )
            self._last_archive = result
            self._archive_count += 1

            if result.state == ArchiveState.COMPLETED:
                logger.info(
                    f"Archive cycle {self._archive_count} complete: "
                    f"{result.files_transferred} files transferred"
                )
            else:
                logger.warning(f"Archive cycle {self._archive_count} had issues: {result.error}")
                self._error_count += 1

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

        # Clean up old snapshots
        self._set_state(CoordinatorState.CLEANING)
        self.space_manager.cleanup_if_needed()

        return True

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

            while not self._stop_event.is_set():
                # Wait for archive to be reachable
                if not self._wait_for_archive_reachable():
                    break

                # Do archive cycle (waits for idle, then archives)
                if not self._do_archive_cycle():
                    # On error, wait before retrying
                    if not self._wait_interruptible(30):
                        break
                    continue

                # Brief pause before next cycle
                if not self._wait_interruptible(self.config.poll_interval):
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
                "reserve_gb": space_info.reserve_gb,
                "is_low": space_info.is_low,
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


def create_coordinator(
    backingfiles_path: Path = Path("/backingfiles"),
    cam_disk_path: Path = Path("/backingfiles/cam_disk.bin"),
    snapshots_path: Path = Path("/backingfiles/snapshots"),
    cam_size: int = 40 * 1024 * 1024 * 1024,  # 40GB default
    backend: ArchiveBackend | None = None,
    config: CoordinatorConfig | None = None,
) -> Coordinator:
    """Factory function to create a fully configured Coordinator.

    Args:
        backingfiles_path: Path to backingfiles directory
        cam_disk_path: Path to cam_disk.bin
        snapshots_path: Path to snapshots directory
        cam_size: Size of cam disk in bytes
        backend: Archive backend (required for production)
        config: Optional coordinator configuration

    Returns:
        Configured Coordinator instance
    """
    from .filesystem import RealFilesystem
    from .archive import MockArchiveBackend

    fs = RealFilesystem()

    snapshot_manager = SnapshotManager(
        fs=fs,
        cam_disk_path=cam_disk_path,
        snapshots_path=snapshots_path,
    )

    space_manager = SpaceManager(
        fs=fs,
        snapshot_manager=snapshot_manager,
        backingfiles_path=backingfiles_path,
        cam_size=cam_size,
    )

    if backend is None:
        logger.warning("No backend provided, using mock backend")
        backend = MockArchiveBackend()

    archive_manager = ArchiveManager(
        fs=fs,
        snapshot_manager=snapshot_manager,
        backend=backend,
    )

    # Default to real mount function for production
    if config is None:
        config = CoordinatorConfig(mount_fn=mount_image)
    elif config.mount_fn is None:
        config.mount_fn = mount_image

    return Coordinator(
        fs=fs,
        snapshot_manager=snapshot_manager,
        archive_manager=archive_manager,
        space_manager=space_manager,
        backend=backend,
        config=config,
    )
