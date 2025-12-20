"""Tests for LED indicator control."""

from pathlib import Path

import pytest

from teslausb.led import (
    LedPattern,
    MockLedController,
    SysfsLedController,
)


class TestLedPattern:
    """Tests for LedPattern enum."""

    def test_patterns_exist(self):
        """Test all expected patterns exist."""
        assert LedPattern.OFF.value == "off"
        assert LedPattern.SLOW_BLINK.value == "slow_blink"
        assert LedPattern.FAST_BLINK.value == "fast_blink"
        assert LedPattern.HEARTBEAT.value == "heartbeat"


class TestMockLedController:
    """Tests for MockLedController."""

    def test_initial_state(self):
        """Test mock controller starts with LED off."""
        controller = MockLedController()

        assert controller.get_pattern() == LedPattern.OFF
        assert controller.pattern_history == []

    def test_set_pattern(self):
        """Test setting LED pattern."""
        controller = MockLedController()

        controller.set_pattern(LedPattern.FAST_BLINK)

        assert controller.get_pattern() == LedPattern.FAST_BLINK

    def test_pattern_history(self):
        """Test pattern history is recorded."""
        controller = MockLedController()

        controller.set_pattern(LedPattern.SLOW_BLINK)
        controller.set_pattern(LedPattern.FAST_BLINK)
        controller.set_pattern(LedPattern.HEARTBEAT)
        controller.set_pattern(LedPattern.OFF)

        assert controller.pattern_history == [
            LedPattern.SLOW_BLINK,
            LedPattern.FAST_BLINK,
            LedPattern.HEARTBEAT,
            LedPattern.OFF,
        ]

    def test_all_patterns(self):
        """Test all patterns can be set."""
        controller = MockLedController()

        for pattern in LedPattern:
            controller.set_pattern(pattern)
            assert controller.get_pattern() == pattern


class TestSysfsLedController:
    """Tests for SysfsLedController."""

    def test_init_no_led(self, tmp_path):
        """Test initialization when no LED exists."""
        controller = SysfsLedController(led_path=None)

        # Should gracefully handle missing LED
        assert controller.get_pattern() == LedPattern.OFF

    def test_init_with_led(self, tmp_path):
        """Test initialization with LED path."""
        led_path = tmp_path / "led0"
        led_path.mkdir()
        (led_path / "trigger").write_text("[none] timer heartbeat")

        controller = SysfsLedController(led_path=led_path)

        assert controller._led_path == led_path
        assert "timer" in controller._available_triggers
        assert "heartbeat" in controller._available_triggers

    def test_set_pattern_off(self, tmp_path):
        """Test setting LED to off."""
        led_path = tmp_path / "led0"
        led_path.mkdir()
        (led_path / "trigger").write_text("[none] timer heartbeat")
        (led_path / "brightness").write_text("1")

        controller = SysfsLedController(led_path=led_path)
        controller.set_pattern(LedPattern.OFF)

        assert (led_path / "trigger").read_text() == "none"
        assert (led_path / "brightness").read_text() == "0"

    def test_set_pattern_slow_blink(self, tmp_path):
        """Test setting slow blink pattern."""
        led_path = tmp_path / "led0"
        led_path.mkdir()
        (led_path / "trigger").write_text("[none] timer heartbeat")
        (led_path / "delay_off").write_text("")
        (led_path / "delay_on").write_text("")

        controller = SysfsLedController(led_path=led_path)
        controller.set_pattern(LedPattern.SLOW_BLINK)

        assert (led_path / "trigger").read_text() == "timer"
        assert (led_path / "delay_off").read_text() == "900"
        assert (led_path / "delay_on").read_text() == "100"

    def test_set_pattern_fast_blink(self, tmp_path):
        """Test setting fast blink pattern."""
        led_path = tmp_path / "led0"
        led_path.mkdir()
        (led_path / "trigger").write_text("[none] timer heartbeat")
        (led_path / "delay_off").write_text("")
        (led_path / "delay_on").write_text("")

        controller = SysfsLedController(led_path=led_path)
        controller.set_pattern(LedPattern.FAST_BLINK)

        assert (led_path / "trigger").read_text() == "timer"
        assert (led_path / "delay_off").read_text() == "150"
        assert (led_path / "delay_on").read_text() == "50"

    def test_set_pattern_heartbeat(self, tmp_path):
        """Test setting heartbeat pattern."""
        led_path = tmp_path / "led0"
        led_path.mkdir()
        (led_path / "trigger").write_text("[none] timer heartbeat")
        (led_path / "invert").write_text("")

        controller = SysfsLedController(led_path=led_path)
        controller.set_pattern(LedPattern.HEARTBEAT)

        assert (led_path / "trigger").read_text() == "heartbeat"
        assert (led_path / "invert").read_text() == "0"

    def test_set_pattern_no_led(self):
        """Test setting pattern when no LED available."""
        controller = SysfsLedController(led_path=None)

        # Should not raise
        controller.set_pattern(LedPattern.FAST_BLINK)

        assert controller.get_pattern() == LedPattern.FAST_BLINK

    def test_timer_trigger_unavailable(self, tmp_path):
        """Test graceful handling when timer trigger unavailable."""
        led_path = tmp_path / "led0"
        led_path.mkdir()
        (led_path / "trigger").write_text("[none]")  # No timer

        controller = SysfsLedController(led_path=led_path)
        controller.set_pattern(LedPattern.SLOW_BLINK)

        # Should not raise, just log
        assert controller.get_pattern() == LedPattern.SLOW_BLINK
