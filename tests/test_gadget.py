"""Tests for USB gadget management."""

from pathlib import Path

import pytest

from teslausb.gadget import (
    GadgetError,
    LunConfig,
    MockGadget,
    UsbGadget,
)


class TestLunConfig:
    """Tests for LunConfig dataclass."""

    def test_default_values(self):
        """Test default LUN configuration."""
        config = LunConfig(disk_path=Path("/test/disk.bin"))

        assert config.disk_path == Path("/test/disk.bin")
        assert config.removable is True
        assert config.readonly is False
        assert config.cdrom is False

    def test_custom_values(self):
        """Test custom LUN configuration."""
        config = LunConfig(
            disk_path=Path("/test/iso.bin"),
            removable=False,
            readonly=True,
            cdrom=True,
        )

        assert config.removable is False
        assert config.readonly is True
        assert config.cdrom is True


class TestMockGadget:
    """Tests for MockGadget."""

    def test_initial_state(self):
        """Test mock gadget starts in correct state."""
        gadget = MockGadget()

        assert not gadget.is_initialized()
        assert not gadget.is_enabled()
        assert gadget.luns == {}

    def test_initialize(self):
        """Test initializing mock gadget."""
        gadget = MockGadget()
        luns = {
            0: LunConfig(disk_path=Path("/cam.bin")),
            1: LunConfig(disk_path=Path("/music.bin")),
        }

        gadget.initialize(luns)

        assert gadget.is_initialized()
        assert len(gadget.luns) == 2
        assert gadget.luns[0].disk_path == Path("/cam.bin")

    def test_initialize_empty_luns_raises(self):
        """Test that initializing with no LUNs raises error."""
        gadget = MockGadget()

        with pytest.raises(GadgetError):
            gadget.initialize({})

    def test_enable(self):
        """Test enabling mock gadget."""
        gadget = MockGadget()
        gadget.initialize({0: LunConfig(disk_path=Path("/disk.bin"))})

        gadget.enable()

        assert gadget.is_enabled()
        assert gadget.enable_count == 1

    def test_enable_without_init_raises(self):
        """Test that enabling without being initialized raises error."""
        gadget = MockGadget()

        with pytest.raises(GadgetError):
            gadget.enable()

    def test_disable(self):
        """Test disabling mock gadget."""
        gadget = MockGadget()
        gadget.initialize({0: LunConfig(disk_path=Path("/disk.bin"))})
        gadget.enable()

        gadget.disable()

        assert not gadget.is_enabled()
        assert gadget.disable_count == 1

    def test_disable_when_not_enabled(self):
        """Test disabling when not enabled is safe."""
        gadget = MockGadget()
        gadget.initialize({0: LunConfig(disk_path=Path("/disk.bin"))})

        gadget.disable()  # Should not raise

        assert gadget.disable_count == 0  # Wasn't actually enabled

    def test_remove(self):
        """Test removing mock gadget."""
        gadget = MockGadget()
        gadget.initialize({0: LunConfig(disk_path=Path("/disk.bin"))})
        gadget.enable()

        gadget.remove()

        assert not gadget.is_initialized()
        assert not gadget.is_enabled()
        assert gadget.luns == {}

    def test_get_status(self):
        """Test getting mock gadget status."""
        gadget = MockGadget()
        gadget.initialize({
            0: LunConfig(disk_path=Path("/cam.bin")),
            1: LunConfig(disk_path=Path("/music.bin"), readonly=True),
        })
        gadget.enable()

        status = gadget.get_status()

        assert status["initialized"] is True
        assert status["enabled"] is True
        assert status["udc"] == "mock-udc"
        assert len(status["luns"]) == 2
        assert status["luns"][0]["file"] == "/cam.bin"
        assert status["luns"][1]["readonly"] is True


class TestUsbGadget:
    """Tests for UsbGadget.

    These tests use a temporary directory to simulate configfs.
    Note: Real configfs behavior can't be fully simulated, but we can
    test the file operations.
    """

    def test_init(self):
        """Test UsbGadget initialization."""
        gadget = UsbGadget(name="test", configfs=Path("/tmp/fake"))

        assert gadget.name == "test"
        assert gadget.path == Path("/tmp/fake/test")

    def test_is_initialized_false_when_not_exists(self, tmp_path):
        """Test is_initialized returns False when gadget doesn't exist."""
        gadget = UsbGadget(name="test", configfs=tmp_path)

        assert not gadget.is_initialized()

    def test_is_initialized_true_when_exists(self, tmp_path):
        """Test is_initialized returns True when gadget directory exists."""
        gadget = UsbGadget(name="test", configfs=tmp_path)
        (tmp_path / "test").mkdir()

        assert gadget.is_initialized()

    def test_is_enabled_false_when_not_initialized(self, tmp_path):
        """Test is_enabled returns False when not initialized."""
        gadget = UsbGadget(name="test", configfs=tmp_path)

        assert not gadget.is_enabled()

    def test_is_enabled_reads_udc_file(self, tmp_path):
        """Test is_enabled reads from UDC file."""
        gadget = UsbGadget(name="test", configfs=tmp_path)
        gadget_path = tmp_path / "test"
        gadget_path.mkdir()

        # Empty UDC = not enabled
        (gadget_path / "UDC").write_text("")
        assert not gadget.is_enabled()

        # Non-empty UDC = enabled
        (gadget_path / "UDC").write_text("fe980000.usb\n")
        assert gadget.is_enabled()

    def test_enable_without_init_raises(self, tmp_path):
        """Test enable raises when gadget not initialized."""
        gadget = UsbGadget(name="test", configfs=tmp_path)

        with pytest.raises(GadgetError, match="not initialized"):
            gadget.enable()

    def test_get_status_not_initialized(self, tmp_path):
        """Test get_status when gadget not initialized."""
        gadget = UsbGadget(name="test", configfs=tmp_path)

        status = gadget.get_status()

        assert status["name"] == "test"
        assert status["initialized"] is False
        assert status["enabled"] is False
        assert status["luns"] == {}

    def test_initialize_empty_luns_raises(self, tmp_path):
        """Test that initializing with empty LUNs raises error."""
        gadget = UsbGadget(name="test", configfs=tmp_path)

        with pytest.raises(GadgetError, match="At least one LUN"):
            gadget.initialize({})
