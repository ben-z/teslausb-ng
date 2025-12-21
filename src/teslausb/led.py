"""LED indicator control for TeslaUSB.

This module controls status LEDs on the Raspberry Pi to indicate
the current state of the archiving process.

LED patterns:
- slow_blink: Waiting for archive (900ms off, 100ms on)
- fast_blink: Archiving in progress (150ms off, 50ms on)
- heartbeat: Rhythmic pulse pattern
- off: LED disabled
"""

from __future__ import annotations

import logging
from enum import Enum
from pathlib import Path
from typing import Protocol

logger = logging.getLogger(__name__)

# Common LED paths on Raspberry Pi
LED_PATHS = [
    "/sys/class/leds/led0",  # Pi Zero, Pi 3
    "/sys/class/leds/ACT",  # Pi 4, Pi 5
    "/sys/class/leds/status",
    "/sys/class/leds/user-led2",  # Some boards
    "/sys/class/leds/radxa-zero:green",  # Radxa Zero
]


class LedPattern(Enum):
    """LED display patterns."""

    OFF = "off"
    SLOW_BLINK = "slow_blink"  # Waiting for archive
    FAST_BLINK = "fast_blink"  # Archiving
    HEARTBEAT = "heartbeat"  # Archive complete


class LedController(Protocol):
    """Protocol for LED control."""

    def set_pattern(self, pattern: LedPattern) -> None:
        """Set the LED pattern."""
        ...

    def get_pattern(self) -> LedPattern:
        """Get current LED pattern."""
        ...


class SysfsLedController:
    """LED controller using Linux sysfs interface.

    Controls LEDs via /sys/class/leds/{led}/trigger and timing files.
    Supports timer and heartbeat triggers.
    """

    def __init__(self, led_path: Path | None = None):
        """Initialize LED controller.

        Args:
            led_path: Path to LED in sysfs. If None, auto-detects.
        """
        self._led_path = led_path or self._find_led()
        self._pattern = LedPattern.OFF
        self._available_triggers: set[str] = set()

        if self._led_path:
            self._load_triggers()
            logger.info(f"Using LED: {self._led_path}")
        else:
            logger.warning("No LED found, LED control disabled")

    def _find_led(self) -> Path | None:
        """Find a usable status LED.

        Returns:
            Path to LED directory, or None if not found
        """
        for led_str in LED_PATHS:
            led_path = Path(led_str)
            if led_path.exists():
                return led_path
        return None

    def _load_triggers(self) -> None:
        """Load available LED triggers."""
        if not self._led_path:
            return

        trigger_file = self._led_path / "trigger"
        try:
            content = trigger_file.read_text()
            # Triggers are listed with current in [brackets]
            self._available_triggers = set(content.replace("[", "").replace("]", "").split())
        except (PermissionError, FileNotFoundError) as e:
            logger.warning(f"Cannot read LED triggers: {e}")

    def _write_file(self, name: str, value: str) -> bool:
        """Write to an LED control file.

        Args:
            name: File name (e.g., "trigger", "delay_on")
            value: Value to write

        Returns:
            True if successful
        """
        if not self._led_path:
            return False

        file_path = self._led_path / name
        try:
            file_path.write_text(value)
            return True
        except (PermissionError, FileNotFoundError, OSError) as e:
            logger.debug(f"Cannot write {file_path}: {e}")
            return False

    def set_pattern(self, pattern: LedPattern) -> None:
        """Set the LED pattern.

        Args:
            pattern: Pattern to display
        """
        if not self._led_path:
            self._pattern = pattern
            return

        self._pattern = pattern

        if pattern == LedPattern.OFF:
            self._write_file("trigger", "none")
            self._write_file("brightness", "0")

        elif pattern == LedPattern.SLOW_BLINK:
            if "timer" not in self._available_triggers:
                logger.debug("Timer trigger not available")
                return
            self._write_file("trigger", "timer")
            self._write_file("delay_off", "900")
            self._write_file("delay_on", "100")

        elif pattern == LedPattern.FAST_BLINK:
            if "timer" not in self._available_triggers:
                logger.debug("Timer trigger not available")
                return
            self._write_file("trigger", "timer")
            self._write_file("delay_off", "150")
            self._write_file("delay_on", "50")

        elif pattern == LedPattern.HEARTBEAT:
            if "heartbeat" not in self._available_triggers:
                logger.debug("Heartbeat trigger not available")
                return
            self._write_file("trigger", "heartbeat")
            self._write_file("invert", "0")

        logger.debug(f"LED pattern set to {pattern.value}")

    def get_pattern(self) -> LedPattern:
        """Get current LED pattern."""
        return self._pattern


class MockLedController:
    """Mock LED controller for testing."""

    def __init__(self):
        """Initialize mock controller."""
        self._pattern = LedPattern.OFF
        self.pattern_history: list[LedPattern] = []

    def set_pattern(self, pattern: LedPattern) -> None:
        """Record pattern change."""
        self._pattern = pattern
        self.pattern_history.append(pattern)

    def get_pattern(self) -> LedPattern:
        """Get current pattern."""
        return self._pattern
