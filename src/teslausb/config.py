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
    archive_photobooth: bool = True


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

        if not (0 < self.snapshot_space_proportion <= 1):
            warnings.append(
                f"snapshot_space_proportion must be between 0 and 1, "
                f"got {self.snapshot_space_proportion}"
            )

        return warnings


def _load_from_dict(env: dict[str, str]) -> Config:
    """Build a Config from a string dictionary (shared by load_from_env and load_from_file).

    Args:
        env: Dictionary mapping variable names to values

    Returns:
        Config instance
    """
    rclone_flags = env.get("RCLONE_FLAGS", "").split() if env.get("RCLONE_FLAGS") else []

    archive = ArchiveConfig(
        system=env.get("ARCHIVE_SYSTEM", "none").lower(),
        rclone_drive=env.get("RCLONE_DRIVE", ""),
        rclone_path=env.get("RCLONE_PATH", ""),
        rclone_flags=rclone_flags,
        archive_recent=env.get("ARCHIVE_RECENTCLIPS", "false").lower() == "true",
        archive_saved=env.get("ARCHIVE_SAVEDCLIPS", "true").lower() != "false",
        archive_sentry=env.get("ARCHIVE_SENTRYCLIPS", "true").lower() != "false",
        archive_track=env.get("ARCHIVE_TRACKMODECLIPS", "true").lower() != "false",
        archive_photobooth=env.get("ARCHIVE_PHOTOBOOTH", "true").lower() != "false",
    )

    config = Config(
        backingfiles_path=Path(env.get("BACKINGFILES_PATH", "/backingfiles")),
        mutable_path=Path(env.get("MUTABLE_PATH", "/mutable")),
        archive=archive,
    )

    if proportion := env.get("SNAPSHOT_SPACE_PROPORTION"):
        config.snapshot_space_proportion = float(proportion)

    return config


def load_from_env() -> Config:
    """Load configuration from environment variables.

    Reads environment variables:
    - MUTABLE_PATH, BACKINGFILES_PATH (optional path overrides)
    - ARCHIVE_SYSTEM (rclone, none)
    - RCLONE_DRIVE, RCLONE_PATH, RCLONE_FLAGS (space-separated)

    Returns:
        Config instance
    """
    return _load_from_dict(dict(os.environ))


def load_from_file(path: Path) -> Config:
    """Load configuration from a shell-style config file.

    Parses files like teslausb_setup_variables.conf that use
    export VAR=value or VAR=value syntax.

    File values are used directly without mutating os.environ.

    Args:
        path: Path to config file

    Returns:
        Config instance
    """
    if not path.exists():
        raise ConfigError(f"Config file not found: {path}")

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

                # Remove surrounding quotes
                if len(value) >= 2 and value[0] in ("'", '"') and value[0] == value[-1]:
                    value = value[1:-1]

                env_vars[key] = value

    return _load_from_dict(env_vars)
