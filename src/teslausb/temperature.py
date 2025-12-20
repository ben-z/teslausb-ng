"""Temperature monitoring for TeslaUSB.

This module monitors CPU temperature on the Raspberry Pi and
provides alerts when thresholds are exceeded. This is important
because the Pi can overheat in a hot car.

Features:
- Warning and caution thresholds with hysteresis
- Peak temperature tracking
- Configurable monitoring intervals
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable, Protocol

logger = logging.getLogger(__name__)

# Default thermal zone path on Linux
THERMAL_ZONE_PATH = Path("/sys/class/thermal/thermal_zone0/temp")

# Hysteresis to prevent alert flapping (5°C)
HYSTERESIS_MILLIDEGREES = 5000


@dataclass
class TemperatureReading:
    """A temperature reading."""

    millidegrees: int
    timestamp: datetime = field(default_factory=datetime.now)

    @property
    def celsius(self) -> float:
        """Temperature in degrees Celsius."""
        return self.millidegrees / 1000.0

    @property
    def fahrenheit(self) -> float:
        """Temperature in degrees Fahrenheit."""
        return (self.celsius * 9 / 5) + 32

    def __str__(self) -> str:
        return f"{self.celsius:.1f}°C"


@dataclass
class TemperatureStatus:
    """Current temperature monitoring status."""

    current: TemperatureReading | None
    peak: TemperatureReading | None
    warning_triggered: bool = False
    caution_triggered: bool = False


@dataclass
class TemperatureConfig:
    """Configuration for temperature monitoring."""

    # Thresholds in millidegrees (None = disabled)
    warning_threshold: int | None = None  # e.g., 80000 = 80°C
    caution_threshold: int | None = None  # e.g., 70000 = 70°C

    # How often to check temperature (seconds)
    poll_interval: float = 60.0

    # Callbacks for alerts
    on_warning: Callable[[TemperatureReading], None] | None = None
    on_caution: Callable[[TemperatureReading], None] | None = None
    on_reading: Callable[[TemperatureReading], None] | None = None


class TemperatureMonitor(Protocol):
    """Protocol for temperature monitoring."""

    def get_temperature(self) -> TemperatureReading | None:
        """Get current temperature reading."""
        ...

    def get_status(self) -> TemperatureStatus:
        """Get current monitoring status."""
        ...

    def start(self) -> None:
        """Start background monitoring."""
        ...

    def stop(self) -> None:
        """Stop background monitoring."""
        ...


class SysfsTemperatureMonitor:
    """Temperature monitor using Linux sysfs thermal zone.

    Reads temperature from /sys/class/thermal/thermal_zone0/temp
    and provides threshold-based alerts with hysteresis.
    """

    def __init__(
        self,
        thermal_path: Path = THERMAL_ZONE_PATH,
        config: TemperatureConfig | None = None,
    ):
        """Initialize temperature monitor.

        Args:
            thermal_path: Path to thermal zone temperature file
            config: Monitoring configuration
        """
        self.thermal_path = thermal_path
        self.config = config or TemperatureConfig()

        self._current: TemperatureReading | None = None
        self._peak: TemperatureReading | None = None
        self._warning_triggered = False
        self._caution_triggered = False

        self._running = False
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    def is_available(self) -> bool:
        """Check if temperature monitoring is available."""
        return self.thermal_path.exists()

    def get_temperature(self) -> TemperatureReading | None:
        """Get current temperature reading.

        Returns:
            TemperatureReading, or None if unavailable
        """
        try:
            content = self.thermal_path.read_text().strip()
            millidegrees = int(content)
            return TemperatureReading(millidegrees=millidegrees)
        except (FileNotFoundError, PermissionError, ValueError) as e:
            logger.debug(f"Cannot read temperature: {e}")
            return None

    def get_status(self) -> TemperatureStatus:
        """Get current monitoring status."""
        return TemperatureStatus(
            current=self._current,
            peak=self._peak,
            warning_triggered=self._warning_triggered,
            caution_triggered=self._caution_triggered,
        )

    def _check_thresholds(self, reading: TemperatureReading) -> None:
        """Check temperature against thresholds and trigger alerts.

        Uses hysteresis to prevent alert flapping: threshold must be
        exceeded to trigger, but temp must drop 5°C below threshold
        to clear.
        """
        temp = reading.millidegrees

        # Check warning threshold
        if self.config.warning_threshold is not None:
            clear_threshold = self.config.warning_threshold - HYSTERESIS_MILLIDEGREES

            if temp < clear_threshold:
                self._warning_triggered = False
            elif temp > self.config.warning_threshold and not self._warning_triggered:
                self._warning_triggered = True
                logger.warning(f"Temperature WARNING: {reading}")
                if self.config.on_warning:
                    try:
                        self.config.on_warning(reading)
                    except Exception as e:
                        logger.error(f"Warning callback failed: {e}")

        # Check caution threshold
        if self.config.caution_threshold is not None:
            clear_threshold = self.config.caution_threshold - HYSTERESIS_MILLIDEGREES

            if temp < clear_threshold:
                self._caution_triggered = False
            elif temp > self.config.caution_threshold and not self._caution_triggered:
                self._caution_triggered = True
                logger.warning(f"Temperature CAUTION: {reading}")
                if self.config.on_caution:
                    try:
                        self.config.on_caution(reading)
                    except Exception as e:
                        logger.error(f"Caution callback failed: {e}")

    def _update(self) -> None:
        """Take a temperature reading and update state."""
        reading = self.get_temperature()
        if reading is None:
            return

        self._current = reading

        # Track peak temperature
        if self._peak is None or reading.millidegrees > self._peak.millidegrees:
            self._peak = reading

        # Check thresholds
        self._check_thresholds(reading)

        # Notify callback
        if self.config.on_reading:
            try:
                self.config.on_reading(reading)
            except Exception as e:
                logger.error(f"Reading callback failed: {e}")

    def _monitor_loop(self) -> None:
        """Background monitoring loop."""
        logger.info("Temperature monitor started")

        while not self._stop_event.is_set():
            self._update()
            self._stop_event.wait(timeout=self.config.poll_interval)

        logger.info("Temperature monitor stopped")

    def start(self) -> None:
        """Start background temperature monitoring."""
        if self._running:
            return

        if not self.is_available():
            logger.warning("Temperature monitoring not available on this device")
            return

        self._stop_event.clear()
        self._running = True
        self._thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop background temperature monitoring."""
        if not self._running:
            return

        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
        self._running = False

    def reset_peak(self) -> None:
        """Reset peak temperature tracking."""
        self._peak = None


class MockTemperatureMonitor:
    """Mock temperature monitor for testing."""

    def __init__(self, temperature: int = 45000):
        """Initialize mock monitor.

        Args:
            temperature: Simulated temperature in millidegrees
        """
        self._temperature = temperature
        self._peak = temperature
        self._running = False
        self.reading_count = 0

    def set_temperature(self, millidegrees: int) -> None:
        """Set simulated temperature."""
        self._temperature = millidegrees
        if millidegrees > self._peak:
            self._peak = millidegrees

    def get_temperature(self) -> TemperatureReading | None:
        """Get simulated temperature."""
        self.reading_count += 1
        return TemperatureReading(millidegrees=self._temperature)

    def get_status(self) -> TemperatureStatus:
        """Get mock status."""
        return TemperatureStatus(
            current=TemperatureReading(millidegrees=self._temperature),
            peak=TemperatureReading(millidegrees=self._peak),
        )

    def start(self) -> None:
        """Start mock monitoring."""
        self._running = True

    def stop(self) -> None:
        """Stop mock monitoring."""
        self._running = False

    def is_available(self) -> bool:
        """Mock always available."""
        return True

    def reset_peak(self) -> None:
        """Reset mock peak."""
        self._peak = self._temperature
