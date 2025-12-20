"""Tests for temperature monitoring."""

from datetime import datetime
from pathlib import Path

import pytest

from teslausb.temperature import (
    MockTemperatureMonitor,
    SysfsTemperatureMonitor,
    TemperatureConfig,
    TemperatureReading,
    TemperatureStatus,
)


class TestTemperatureReading:
    """Tests for TemperatureReading dataclass."""

    def test_celsius(self):
        """Test celsius conversion."""
        reading = TemperatureReading(millidegrees=45000)

        assert reading.celsius == 45.0

    def test_fahrenheit(self):
        """Test fahrenheit conversion."""
        reading = TemperatureReading(millidegrees=37000)  # 37°C

        # 37°C = 98.6°F
        assert abs(reading.fahrenheit - 98.6) < 0.1

    def test_str(self):
        """Test string representation."""
        reading = TemperatureReading(millidegrees=65500)

        assert str(reading) == "65.5°C"

    def test_timestamp(self):
        """Test timestamp is set."""
        before = datetime.now()
        reading = TemperatureReading(millidegrees=45000)
        after = datetime.now()

        assert before <= reading.timestamp <= after


class TestTemperatureConfig:
    """Tests for TemperatureConfig dataclass."""

    def test_default_values(self):
        """Test default configuration."""
        config = TemperatureConfig()

        assert config.warning_threshold is None
        assert config.caution_threshold is None
        assert config.poll_interval == 60.0
        assert config.on_warning is None
        assert config.on_caution is None

    def test_custom_values(self):
        """Test custom configuration."""
        callback = lambda r: None

        config = TemperatureConfig(
            warning_threshold=80000,
            caution_threshold=70000,
            poll_interval=30.0,
            on_warning=callback,
        )

        assert config.warning_threshold == 80000
        assert config.caution_threshold == 70000
        assert config.poll_interval == 30.0
        assert config.on_warning is callback


class TestMockTemperatureMonitor:
    """Tests for MockTemperatureMonitor."""

    def test_default_temperature(self):
        """Test default mock temperature."""
        monitor = MockTemperatureMonitor()

        reading = monitor.get_temperature()

        assert reading is not None
        assert reading.celsius == 45.0

    def test_custom_temperature(self):
        """Test custom mock temperature."""
        monitor = MockTemperatureMonitor(temperature=60000)

        reading = monitor.get_temperature()

        assert reading.celsius == 60.0

    def test_set_temperature(self):
        """Test setting temperature dynamically."""
        monitor = MockTemperatureMonitor()

        monitor.set_temperature(75000)
        reading = monitor.get_temperature()

        assert reading.celsius == 75.0

    def test_peak_tracking(self):
        """Test peak temperature tracking."""
        monitor = MockTemperatureMonitor(temperature=40000)

        monitor.set_temperature(60000)
        monitor.set_temperature(50000)  # Lower

        status = monitor.get_status()

        assert status.current.celsius == 50.0
        assert status.peak.celsius == 60.0

    def test_reset_peak(self):
        """Test resetting peak temperature."""
        monitor = MockTemperatureMonitor(temperature=40000)
        monitor.set_temperature(70000)
        monitor.set_temperature(50000)

        monitor.reset_peak()
        status = monitor.get_status()

        assert status.peak.celsius == 50.0

    def test_reading_count(self):
        """Test reading count increments."""
        monitor = MockTemperatureMonitor()

        monitor.get_temperature()
        monitor.get_temperature()
        monitor.get_temperature()

        assert monitor.reading_count == 3

    def test_is_available(self):
        """Test mock always available."""
        monitor = MockTemperatureMonitor()

        assert monitor.is_available() is True

    def test_start_stop(self):
        """Test start/stop (no-op in mock)."""
        monitor = MockTemperatureMonitor()

        monitor.start()
        assert monitor._running is True

        monitor.stop()
        assert monitor._running is False


class TestSysfsTemperatureMonitor:
    """Tests for SysfsTemperatureMonitor."""

    def test_is_available_true(self, tmp_path):
        """Test is_available when thermal zone exists."""
        thermal_file = tmp_path / "temp"
        thermal_file.write_text("45000")

        monitor = SysfsTemperatureMonitor(thermal_path=thermal_file)

        assert monitor.is_available() is True

    def test_is_available_false(self, tmp_path):
        """Test is_available when thermal zone doesn't exist."""
        monitor = SysfsTemperatureMonitor(thermal_path=tmp_path / "nonexistent")

        assert monitor.is_available() is False

    def test_get_temperature(self, tmp_path):
        """Test reading temperature from file."""
        thermal_file = tmp_path / "temp"
        thermal_file.write_text("65500")

        monitor = SysfsTemperatureMonitor(thermal_path=thermal_file)

        reading = monitor.get_temperature()

        assert reading is not None
        assert reading.celsius == 65.5

    def test_get_temperature_file_not_found(self, tmp_path):
        """Test reading when file doesn't exist."""
        monitor = SysfsTemperatureMonitor(thermal_path=tmp_path / "nonexistent")

        reading = monitor.get_temperature()

        assert reading is None

    def test_warning_threshold(self, tmp_path):
        """Test warning threshold triggers callback."""
        thermal_file = tmp_path / "temp"
        thermal_file.write_text("85000")

        warnings = []
        config = TemperatureConfig(
            warning_threshold=80000,
            on_warning=lambda r: warnings.append(r),
        )

        monitor = SysfsTemperatureMonitor(thermal_path=thermal_file, config=config)
        monitor._update()

        assert len(warnings) == 1
        assert warnings[0].celsius == 85.0

    def test_warning_hysteresis(self, tmp_path):
        """Test warning doesn't re-trigger due to hysteresis."""
        thermal_file = tmp_path / "temp"
        thermal_file.write_text("85000")

        warnings = []
        config = TemperatureConfig(
            warning_threshold=80000,
            on_warning=lambda r: warnings.append(r),
        )

        monitor = SysfsTemperatureMonitor(thermal_path=thermal_file, config=config)

        # First trigger
        monitor._update()
        assert len(warnings) == 1

        # Should not re-trigger (still above threshold)
        thermal_file.write_text("82000")
        monitor._update()
        assert len(warnings) == 1

        # Should clear after dropping below hysteresis
        thermal_file.write_text("74000")  # Below 80000 - 5000
        monitor._update()
        assert len(warnings) == 1

        # Should trigger again
        thermal_file.write_text("85000")
        monitor._update()
        assert len(warnings) == 2

    def test_caution_threshold(self, tmp_path):
        """Test caution threshold triggers callback."""
        thermal_file = tmp_path / "temp"
        thermal_file.write_text("72000")

        cautions = []
        config = TemperatureConfig(
            caution_threshold=70000,
            on_caution=lambda r: cautions.append(r),
        )

        monitor = SysfsTemperatureMonitor(thermal_path=thermal_file, config=config)
        monitor._update()

        assert len(cautions) == 1
        assert cautions[0].celsius == 72.0

    def test_peak_temperature(self, tmp_path):
        """Test peak temperature tracking."""
        thermal_file = tmp_path / "temp"
        thermal_file.write_text("50000")

        monitor = SysfsTemperatureMonitor(thermal_path=thermal_file)

        monitor._update()
        thermal_file.write_text("70000")
        monitor._update()
        thermal_file.write_text("60000")
        monitor._update()

        status = monitor.get_status()

        assert status.current.celsius == 60.0
        assert status.peak.celsius == 70.0

    def test_on_reading_callback(self, tmp_path):
        """Test on_reading callback."""
        thermal_file = tmp_path / "temp"
        thermal_file.write_text("45000")

        readings = []
        config = TemperatureConfig(
            on_reading=lambda r: readings.append(r),
        )

        monitor = SysfsTemperatureMonitor(thermal_path=thermal_file, config=config)
        monitor._update()
        monitor._update()

        assert len(readings) == 2

    def test_get_status(self, tmp_path):
        """Test get_status returns current state."""
        thermal_file = tmp_path / "temp"
        thermal_file.write_text("45000")

        config = TemperatureConfig(warning_threshold=80000)
        monitor = SysfsTemperatureMonitor(thermal_path=thermal_file, config=config)
        monitor._update()

        status = monitor.get_status()

        assert status.current.celsius == 45.0
        assert status.warning_triggered is False
        assert status.caution_triggered is False
