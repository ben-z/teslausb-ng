"""Configuration handling for TeslaUSB.

This module provides:
- Config dataclass with all configuration options
- Loading from environment variables
- Loading from config file
- Validation
"""

from __future__ import annotations

import math
import os
import re
import shlex
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
    event_stability_seconds: float = 600.0


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
    config = Config()
    archive = config.archive
    for key, attribute in (
        ("BACKINGFILES_PATH", "backingfiles_path"),
        ("MUTABLE_PATH", "mutable_path"),
    ):
        if key in env:
            setattr(config, attribute, Path(env[key]))
    for key, attribute in (
        ("ARCHIVE_SYSTEM", "system"),
        ("RCLONE_DRIVE", "rclone_drive"),
        ("RCLONE_PATH", "rclone_path"),
    ):
        if key in env:
            setattr(archive, attribute, env[key])
    archive.system = archive.system.lower()
    if "RCLONE_FLAGS" in env:
        archive.rclone_flags = shlex.split(env["RCLONE_FLAGS"])
    for key, attribute in (
        ("ARCHIVE_RECENTCLIPS", "archive_recent"),
        ("ARCHIVE_SAVEDCLIPS", "archive_saved"),
        ("ARCHIVE_SENTRYCLIPS", "archive_sentry"),
        ("ARCHIVE_TRACKMODECLIPS", "archive_track"),
        ("ARCHIVE_PHOTOBOOTH", "archive_photobooth"),
    ):
        if key in env:
            if env[key].lower() not in {"true", "false"}:
                raise ConfigError(f"{key} must be true or false")
            setattr(archive, attribute, env[key].lower() == "true")

    if "EVENT_STABILITY_SECONDS" in env:
        config.archive.event_stability_seconds = float(env["EVENT_STABILITY_SECONDS"])
        if (
            not math.isfinite(config.archive.event_stability_seconds)
            or config.archive.event_stability_seconds < 0
        ):
            raise ConfigError("EVENT_STABILITY_SECONDS must be nonnegative")

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
