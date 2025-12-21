"""USB mass storage gadget management.

This module manages the Linux USB gadget subsystem to present disk images
as USB mass storage devices to the Tesla vehicle.

The gadget is configured via configfs at /sys/kernel/config/usb_gadget/.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

logger = logging.getLogger(__name__)


class GadgetError(Exception):
    """Error during gadget operations."""


@dataclass
class LunConfig:
    """Configuration for a single LUN (logical unit).

    Args:
        disk_path: Path to the disk image file
        removable: Whether the device appears as removable media
        readonly: Whether to expose as read-only
        cdrom: Whether to emulate a CD-ROM drive
    """

    disk_path: Path
    removable: bool = True
    readonly: bool = False
    cdrom: bool = False


class Gadget(Protocol):
    """Protocol for USB gadget operations."""

    def setup(self, luns: dict[int, LunConfig]) -> None:
        """Create and configure the gadget."""
        ...

    def teardown(self) -> None:
        """Remove the gadget configuration."""
        ...

    def enable(self) -> None:
        """Bind gadget to UDC - host sees the drives."""
        ...

    def disable(self) -> None:
        """Unbind gadget from UDC - drives disappear."""
        ...

    def is_enabled(self) -> bool:
        """Check if gadget is currently bound to UDC."""
        ...

    def is_setup(self) -> bool:
        """Check if gadget structure exists."""
        ...


class UsbGadget:
    """USB mass storage gadget using Linux configfs.

    This class manages the USB gadget subsystem to present disk images
    as USB mass storage devices. It requires:
    - configfs mounted at /sys/kernel/config
    - libcomposite kernel module loaded
    - dwc2 (or similar) USB gadget controller

    Example:
        gadget = UsbGadget()
        gadget.setup({
            0: LunConfig(disk_path=Path("/backingfiles/cam_disk.bin")),
        })
        gadget.enable()
    """

    # USB IDs for Linux Foundation composite gadget
    VENDOR_ID = "0x1d6b"
    PRODUCT_ID = "0x0104"

    def __init__(
        self,
        name: str = "teslausb",
        configfs: Path = Path("/sys/kernel/config/usb_gadget"),
    ):
        """Initialize UsbGadget.

        Args:
            name: Gadget name (creates directory under configfs)
            configfs: Path to USB gadget configfs mount point
        """
        self.name = name
        self.configfs = configfs
        self.path = configfs / name
        self._udc_path = Path("/sys/class/udc")

    def _write(self, path: Path, value: str) -> None:
        """Write value to a configfs file."""
        logger.debug(f"Writing '{value}' to {path}")
        path.write_text(value)

    def _read(self, path: Path) -> str:
        """Read value from a configfs file."""
        return path.read_text().strip()

    def _get_udc(self) -> str:
        """Get available USB Device Controller name.

        Returns:
            Name of the first available UDC

        Raises:
            GadgetError: If no UDC is available
        """
        if not self._udc_path.exists():
            raise GadgetError(f"UDC path {self._udc_path} does not exist")

        udcs = list(self._udc_path.iterdir())
        if not udcs:
            raise GadgetError("No USB Device Controller found")

        return udcs[0].name

    def setup(self, luns: dict[int, LunConfig]) -> None:
        """Create gadget structure in configfs.

        Args:
            luns: Mapping of LUN number to configuration.
                  LUN 0 is typically the camera disk, LUN 1 is music, etc.

        Raises:
            GadgetError: If setup fails
        """
        if self.is_setup():
            logger.info(f"Gadget {self.name} already set up")
            return

        if not luns:
            raise GadgetError("At least one LUN must be configured")

        # Check prerequisites
        if not self.configfs.exists():
            configfs_base = Path("/sys/kernel/config")
            if not configfs_base.exists():
                raise GadgetError(
                    f"configfs not mounted. "
                    "Run: sudo mount -t configfs none /sys/kernel/config"
                )
            else:
                raise GadgetError(
                    f"USB gadget configfs not available at {self.configfs}. "
                    "Run: sudo modprobe libcomposite"
                )

        # Check that cam_disk exists
        for lun_id, config in luns.items():
            if not config.disk_path.exists():
                raise GadgetError(f"Disk image not found: {config.disk_path}")

        logger.info(f"Setting up gadget {self.name} with {len(luns)} LUN(s)")

        try:
            # Create gadget directory
            self.path.mkdir(parents=True)

            # Set USB IDs
            self._write(self.path / "idVendor", self.VENDOR_ID)
            self._write(self.path / "idProduct", self.PRODUCT_ID)
            self._write(self.path / "bcdDevice", "0x0100")
            self._write(self.path / "bcdUSB", "0x0200")

            # Create strings (English)
            strings = self.path / "strings" / "0x409"
            strings.mkdir(parents=True)
            self._write(strings / "manufacturer", "TeslaUSB")
            self._write(strings / "product", "Tesla USB Drive")
            self._write(strings / "serialnumber", "fedcba9876543210")

            # Create mass storage function
            func = self.path / "functions" / "mass_storage.0"
            func.mkdir(parents=True)

            # Configure each LUN
            for lun_id, config in sorted(luns.items()):
                self._setup_lun(func, lun_id, config)

            # Create configuration
            cfg = self.path / "configs" / "c.1"
            cfg.mkdir(parents=True)

            cfg_strings = cfg / "strings" / "0x409"
            cfg_strings.mkdir(parents=True)
            self._write(cfg_strings / "configuration", "Config 1: Mass Storage")

            self._write(cfg / "MaxPower", "250")

            # Link function to configuration
            link = cfg / "mass_storage.0"
            if not link.exists():
                link.symlink_to(func)

            logger.info(f"Gadget {self.name} setup complete")

        except OSError as e:
            logger.error(f"Failed to set up gadget: {e}")
            # Try to clean up partial setup
            self._cleanup_partial()
            raise GadgetError(f"Failed to set up gadget: {e}") from e

    def _setup_lun(self, func_path: Path, lun_id: int, config: LunConfig) -> None:
        """Configure a single LUN.

        Args:
            func_path: Path to the mass_storage function
            lun_id: LUN number (0, 1, 2, ...)
            config: LUN configuration
        """
        lun = func_path / f"lun.{lun_id}"

        # LUN 0 is created automatically, others need mkdir
        if lun_id > 0:
            lun.mkdir(exist_ok=True)

        logger.debug(f"Configuring LUN {lun_id}: {config.disk_path}")

        self._write(lun / "removable", "1" if config.removable else "0")
        self._write(lun / "ro", "1" if config.readonly else "0")
        self._write(lun / "cdrom", "1" if config.cdrom else "0")
        self._write(lun / "file", str(config.disk_path))

    def _cleanup_partial(self) -> None:
        """Clean up a partial gadget setup after failure."""
        try:
            if self.path.exists():
                import shutil

                shutil.rmtree(self.path)
        except OSError:
            pass

    def teardown(self) -> None:
        """Remove gadget from configfs.

        This will disable the gadget first if it's enabled.
        """
        if not self.is_setup():
            return

        logger.info(f"Tearing down gadget {self.name}")

        # Must disable first
        self.disable()

        try:
            # Remove symlink from config
            cfg_link = self.path / "configs" / "c.1" / "mass_storage.0"
            if cfg_link.is_symlink():
                cfg_link.unlink()

            # Remove config strings
            cfg_strings = self.path / "configs" / "c.1" / "strings" / "0x409"
            if cfg_strings.exists():
                cfg_strings.rmdir()

            # Remove config
            cfg = self.path / "configs" / "c.1"
            if cfg.exists():
                cfg.rmdir()

            # Remove LUNs (except lun.0 which is automatic)
            func = self.path / "functions" / "mass_storage.0"
            if func.exists():
                for lun in func.iterdir():
                    if lun.is_dir() and lun.name != "lun.0":
                        lun.rmdir()
                func.rmdir()

            # Remove strings
            strings = self.path / "strings" / "0x409"
            if strings.exists():
                strings.rmdir()

            # Remove gadget
            self.path.rmdir()

            logger.info(f"Gadget {self.name} removed")

        except OSError as e:
            logger.error(f"Failed to teardown gadget: {e}")
            raise GadgetError(f"Failed to teardown gadget: {e}") from e

    def enable(self) -> None:
        """Bind gadget to UDC - Tesla sees the drives.

        Raises:
            GadgetError: If gadget is not setup or binding fails
        """
        if not self.is_setup():
            raise GadgetError("Gadget not set up")

        if self.is_enabled():
            logger.debug("Gadget already enabled")
            return

        udc = self._get_udc()
        logger.info(f"Enabling gadget {self.name} on {udc}")

        try:
            self._write(self.path / "UDC", udc)
        except OSError as e:
            raise GadgetError(f"Failed to enable gadget: {e}") from e

    def disable(self) -> None:
        """Unbind gadget from UDC - drives disappear from Tesla."""
        if not self.is_enabled():
            return

        logger.info(f"Disabling gadget {self.name}")

        try:
            self._write(self.path / "UDC", "")
        except OSError as e:
            logger.warning(f"Failed to disable gadget: {e}")

    def is_enabled(self) -> bool:
        """Check if gadget is bound to UDC.

        Returns:
            True if gadget is bound and visible to host
        """
        udc_file = self.path / "UDC"
        if not udc_file.exists():
            return False
        return bool(self._read(udc_file))

    def is_setup(self) -> bool:
        """Check if gadget structure exists in configfs.

        Returns:
            True if gadget directory exists
        """
        return self.path.exists()

    def get_status(self) -> dict:
        """Get current gadget status.

        Returns:
            Dictionary with status information
        """
        status = {
            "name": self.name,
            "setup": self.is_setup(),
            "enabled": False,
            "udc": None,
            "luns": {},
        }

        if not self.is_setup():
            return status

        # Get UDC
        udc_file = self.path / "UDC"
        if udc_file.exists():
            udc = self._read(udc_file)
            status["enabled"] = bool(udc)
            status["udc"] = udc if udc else None

        # Get LUN info
        func = self.path / "functions" / "mass_storage.0"
        if func.exists():
            for lun_dir in sorted(func.iterdir()):
                if lun_dir.is_dir() and lun_dir.name.startswith("lun."):
                    lun_id = int(lun_dir.name.split(".")[1])
                    file_path = lun_dir / "file"
                    status["luns"][lun_id] = {
                        "file": self._read(file_path) if file_path.exists() else None,
                        "readonly": self._read(lun_dir / "ro") == "1",
                    }

        return status


class MockGadget:
    """Mock gadget for testing.

    Tracks state in memory without touching the filesystem.
    """

    def __init__(self):
        self._setup = False
        self._enabled = False
        self.luns: dict[int, LunConfig] = {}
        self.enable_count = 0
        self.disable_count = 0

    def setup(self, luns: dict[int, LunConfig]) -> None:
        """Record gadget setup."""
        if not luns:
            raise GadgetError("At least one LUN must be configured")
        self.luns = {k: v for k, v in luns.items()}
        self._setup = True

    def teardown(self) -> None:
        """Record gadget teardown."""
        self._enabled = False
        self._setup = False
        self.luns.clear()

    def enable(self) -> None:
        """Record gadget enable."""
        if not self._setup:
            raise GadgetError("Gadget not setup")
        self._enabled = True
        self.enable_count += 1

    def disable(self) -> None:
        """Record gadget disable."""
        if self._enabled:
            self._enabled = False
            self.disable_count += 1

    def is_enabled(self) -> bool:
        """Check if mock gadget is enabled."""
        return self._enabled

    def is_setup(self) -> bool:
        """Check if mock gadget is set up."""
        return self._setup

    def get_status(self) -> dict:
        """Get mock gadget status."""
        return {
            "name": "mock",
            "setup": self._setup,
            "enabled": self._enabled,
            "udc": "mock-udc" if self._enabled else None,
            "luns": {
                k: {"file": str(v.disk_path), "readonly": v.readonly}
                for k, v in self.luns.items()
            },
        }
