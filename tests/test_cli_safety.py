"""Validate USB exposure checks before attaching a camera disk."""

import argparse
from unittest.mock import MagicMock, patch

from teslausb.cli import cmd_gadget
from teslausb.config import Config


def test_active_gadget_is_not_checked_or_reinitialized():
    gadget = MagicMock()
    gadget.is_enabled.return_value = True
    with (
        patch("teslausb.cli.UsbGadget", return_value=gadget),
        patch("teslausb.cli.fsck_image") as fsck,
    ):
        assert cmd_gadget(argparse.Namespace(gadget_command="on")) == 0
    fsck.assert_not_called()
    gadget.initialize.assert_not_called()


def test_inconsistent_camera_disk_is_never_exposed():
    gadget = MagicMock()
    gadget.is_enabled.return_value = False
    with (
        patch("teslausb.cli.UsbGadget", return_value=gadget),
        patch("teslausb.cli.load_config", return_value=Config()),
        patch("teslausb.cli.fsck_image", return_value=False),
    ):
        assert cmd_gadget(argparse.Namespace(gadget_command="on")) == 1
    gadget.initialize.assert_not_called()
    gadget.enable.assert_not_called()


def test_missing_or_empty_camera_disk_fails_before_creating_components():
    from pathlib import Path

    import pytest

    from teslausb.cli import create_components
    from teslausb.config import ConfigError
    from teslausb.filesystem import MockFilesystem

    fs = MockFilesystem()
    fs.mkdir(Path("/backingfiles"))
    with patch("teslausb.cli.RealFilesystem", return_value=fs):
        with pytest.raises(ConfigError, match="not found"):
            create_components(Config())
        fs.write_text(Path("/backingfiles/cam_disk.bin"), "")
        with pytest.raises(ConfigError, match="empty"):
            create_components(Config())
