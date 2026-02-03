"""Configuration handling for TeslaUSB.

This module provides:
- Config dataclass with all configuration options
- Loading from environment variables
- Loading from config file
- Validation
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path

GB = 1024 * 1024 * 1024
MB = 1024 * 1024


class ConfigError(Exception):
    """Configuration error."""


def parse_size(size_str: str) -> int:
    """Parse a size string like '40G' or '500M' to bytes.

    Args:
        size_str: Size string (e.g., '40G', '500M', '1024K', '1000000')

    Returns:
        Size in bytes

    Raises:
        ConfigError: If size string is invalid
    """
    if isinstance(size_str, int):
        return size_str

    size_str = str(size_str).strip().upper()

    # Check for percentage (not supported here)
    if size_str.endswith("%"):
        raise ConfigError(f"Percentage sizes not supported: {size_str}")

    # Parse number and optional suffix
    match = re.match(r"^(\d+(?:\.\d+)?)\s*([KMGT]?B?)?$", size_str)
    if not match:
        raise ConfigError(f"Invalid size string: {size_str}")

    value = float(match.group(1))
    suffix = match.group(2) or ""

    # Remove trailing 'B' if present
    suffix = suffix.rstrip("B")

    multipliers = {
        "": 1,
        "K": 1024,
        "M": 1024 * 1024,
        "G": 1024 * 1024 * 1024,
        "T": 1024 * 1024 * 1024 * 1024,
    }

    if suffix not in multipliers:
        raise ConfigError(f"Invalid size suffix: {suffix}")

    return int(value * multipliers[suffix])


@dataclass
class ArchiveConfig:
    """Archive-specific configuration."""

    system: str = "none"  # rclone or none

    # rclone settings
    rclone_drive: str = ""
    rclone_path: str = ""
    rclone_flags: list[str] = field(default_factory=list)

    # What to archive
    archive_recent: bool = False
    archive_saved: bool = True
    archive_sentry: bool = True
    archive_track: bool = True


@dataclass
class Config:
    """Main configuration for TeslaUSB."""

    # Paths
    backingfiles_path: Path = Path("/backingfiles")
    mutable_path: Path = Path("/mutable")

    # Archive
    archive: ArchiveConfig = field(default_factory=ArchiveConfig)

    # Space management
    snapshot_space_proportion: float = 0.5  # Fraction of cam_size needed for snapshot

    # Derived paths
    @property
    def cam_disk_path(self) -> Path:
        return self.backingfiles_path / "cam_disk.bin"

    @property
    def snapshots_path(self) -> Path:
        return self.backingfiles_path / "snapshots"

    def validate(self) -> list[str]:
        """Validate configuration.

        Returns:
            List of warning/error messages (empty if valid)
        """
        warnings: list[str] = []

        if self.archive.system not in ("rclone", "none"):
            warnings.append(f"Unknown archive system: {self.archive.system}")

        return warnings


def load_from_env() -> Config:
    """Load configuration from environment variables.

    Reads environment variables:
    - MUTABLE_PATH, BACKINGFILES_PATH (optional path overrides)
    - ARCHIVE_SYSTEM (rclone, none)
    - RCLONE_DRIVE, RCLONE_PATH

    Returns:
        Config instance
    """
    config = Config()
    archive = ArchiveConfig()

    # Optional path overrides
    if path := os.environ.get("MUTABLE_PATH"):
        config.mutable_path = Path(path)
    if path := os.environ.get("BACKINGFILES_PATH"):
        config.backingfiles_path = Path(path)

    # Archive system
    archive.system = os.environ.get("ARCHIVE_SYSTEM", "none").lower()

    # rclone settings
    archive.rclone_drive = os.environ.get("RCLONE_DRIVE", "")
    archive.rclone_path = os.environ.get("RCLONE_PATH", "")

    # What to archive
    archive.archive_recent = os.environ.get("ARCHIVE_RECENTCLIPS", "false").lower() == "true"
    archive.archive_saved = os.environ.get("ARCHIVE_SAVEDCLIPS", "true").lower() != "false"
    archive.archive_sentry = os.environ.get("ARCHIVE_SENTRYCLIPS", "true").lower() != "false"
    archive.archive_track = os.environ.get("ARCHIVE_TRACKMODECLIPS", "true").lower() != "false"

    config.archive = archive

    # Space management
    if proportion := os.environ.get("SNAPSHOT_SPACE_PROPORTION"):
        config.snapshot_space_proportion = float(proportion)

    return config


def load_from_file(path: Path) -> Config:
    """Load configuration from a shell-style config file.

    Parses files like teslausb_setup_variables.conf that use
    export VAR=value or VAR=value syntax.

    Args:
        path: Path to config file

    Returns:
        Config instance
    """
    if not path.exists():
        raise ConfigError(f"Config file not found: {path}")

    # Parse the file and set environment variables
    env_vars: dict[str, str] = {}

    with open(path) as f:
        for line in f:
            line = line.strip()

            # Skip comments and empty lines
            if not line or line.startswith("#"):
                continue

            # Handle export statements
            if line.startswith("export "):
                line = line[7:]

            # Parse VAR=value
            if "=" in line:
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip()

                # Remove quotes
                if (value.startswith('"') and value.endswith('"')) or \
                   (value.startswith("'") and value.endswith("'")):
                    value = value[1:-1]

                env_vars[key] = value

    # Temporarily set environment variables and load
    old_env = dict(os.environ)
    try:
        os.environ.update(env_vars)
        return load_from_env()
    finally:
        os.environ.clear()
        os.environ.update(old_env)
