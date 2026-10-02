"""Tests for configuration handling."""

import os
from pathlib import Path
import tempfile

import pytest

from teslausb.config import (
    Config,
    ArchiveConfig,
    ConfigError,
    parse_size,
    load_from_env,
    load_from_file,
    GB,
    MB,
)


class TestParseSize:
    """Tests for parse_size function."""

    def test_parse_bytes(self):
        """Test parsing plain bytes."""
        assert parse_size("1000") == 1000
        assert parse_size("0") == 0

    def test_parse_kilobytes(self):
        """Test parsing kilobytes."""
        assert parse_size("1K") == 1024
        assert parse_size("1KB") == 1024
        assert parse_size("10k") == 10 * 1024

    def test_parse_megabytes(self):
        """Test parsing megabytes."""
        assert parse_size("1M") == 1024 * 1024
        assert parse_size("1MB") == 1024 * 1024
        assert parse_size("500m") == 500 * 1024 * 1024

    def test_parse_gigabytes(self):
        """Test parsing gigabytes."""
        assert parse_size("1G") == 1024 * 1024 * 1024
        assert parse_size("1GB") == 1024 * 1024 * 1024
        assert parse_size("40g") == 40 * 1024 * 1024 * 1024

    def test_parse_terabytes(self):
        """Test parsing terabytes."""
        assert parse_size("1T") == 1024 * 1024 * 1024 * 1024
        assert parse_size("1TB") == 1024 * 1024 * 1024 * 1024

    def test_parse_with_spaces(self):
        """Test parsing with spaces."""
        assert parse_size("  40G  ") == 40 * GB
        assert parse_size("1 G") == 1 * GB

    def test_parse_decimal(self):
        """Test parsing decimal values."""
        assert parse_size("1.5G") == int(1.5 * GB)
        assert parse_size("0.5M") == int(0.5 * MB)

    def test_parse_int_passthrough(self):
        """Test that int values pass through."""
        assert parse_size(1000) == 1000

    def test_parse_invalid(self):
        """Test parsing invalid values raises error."""
        with pytest.raises(ConfigError):
            parse_size("invalid")

        with pytest.raises(ConfigError):
            parse_size("40X")  # Invalid suffix

    def test_parse_percentage_not_supported(self):
        """Test that percentage is not supported."""
        with pytest.raises(ConfigError):
            parse_size("50%")


class TestConfig:
    """Tests for Config dataclass."""

    def test_default_config(self):
        """Test default configuration values."""
        config = Config()

        assert config.backingfiles_path == Path("/backingfiles")
        assert config.mutable_path == Path("/mutable")

    def test_derived_paths(self):
        """Test derived path properties."""
        config = Config(backingfiles_path=Path("/test"))

        assert config.cam_disk_path == Path("/test/cam_disk.bin")
        assert config.snapshots_path == Path("/test/snapshots")

    def test_validate_good_config(self):
        """Test validation of good config."""
        config = Config()

        warnings = config.validate()
        assert len(warnings) == 0

    def test_validate_invalid_archive_system(self):
        """Test validation catches invalid archive system."""
        config = Config()
        config.archive = ArchiveConfig(system="invalid")

        warnings = config.validate()
        assert any("Unknown archive system" in w for w in warnings)

    def test_validate_snapshot_space_proportion_out_of_range(self):
        """Test validation catches out-of-range snapshot_space_proportion."""
        for bad_value in [0, -0.5, 1.5, 2.0]:
            config = Config(snapshot_space_proportion=bad_value)
            warnings = config.validate()
            assert any("snapshot_space_proportion" in w for w in warnings), \
                f"Expected warning for proportion={bad_value}"

    def test_validate_snapshot_space_proportion_valid(self):
        """Test validation accepts valid snapshot_space_proportion values."""
        for good_value in [0.1, 0.5, 1.0]:
            config = Config(snapshot_space_proportion=good_value)
            warnings = config.validate()
            assert warnings == [], f"Unexpected warning for proportion={good_value}"


class TestArchiveConfig:
    """Tests for ArchiveConfig dataclass."""

    def test_default_archive_config(self):
        """Test default archive config."""
        config = ArchiveConfig()

        assert config.system == "none"
        assert config.archive_saved is True
        assert config.archive_sentry is True
        assert config.archive_recent is False
        assert config.archive_photobooth is True


class TestLoadFromEnv:
    """Tests for load_from_env function."""

    def test_load_default_env(self):
        """Test loading with no environment variables set."""
        # Clear relevant env vars
        env_vars = [
            "ARCHIVE_SYSTEM", "ARCHIVE_RECENTCLIPS",
        ]
        old_values = {k: os.environ.pop(k, None) for k in env_vars}

        try:
            config = load_from_env()

            assert config.archive.system == "none"
        finally:
            for k, v in old_values.items():
                if v is not None:
                    os.environ[k] = v

    def test_load_archive_system(self):
        """Test loading ARCHIVE_SYSTEM from env."""
        old_value = os.environ.get("ARCHIVE_SYSTEM")

        try:
            os.environ["ARCHIVE_SYSTEM"] = "rclone"
            config = load_from_env()

            assert config.archive.system == "rclone"
        finally:
            if old_value is not None:
                os.environ["ARCHIVE_SYSTEM"] = old_value
            else:
                os.environ.pop("ARCHIVE_SYSTEM", None)

    def test_load_rclone_flags(self):
        """Test loading RCLONE_FLAGS from env (space-separated)."""
        old_value = os.environ.get("RCLONE_FLAGS")

        try:
            os.environ["RCLONE_FLAGS"] = "--fast-list --transfers 4"
            config = load_from_env()

            assert config.archive.rclone_flags == ["--fast-list", "--transfers", "4"]
        finally:
            if old_value is not None:
                os.environ["RCLONE_FLAGS"] = old_value
            else:
                os.environ.pop("RCLONE_FLAGS", None)

    def test_load_rclone_flags_unset(self):
        """Test that rclone_flags defaults to empty list when unset."""
        old_value = os.environ.pop("RCLONE_FLAGS", None)

        try:
            config = load_from_env()
            assert config.archive.rclone_flags == []
        finally:
            if old_value is not None:
                os.environ["RCLONE_FLAGS"] = old_value


class TestLoadFromFile:
    """Tests for load_from_file function."""

    def test_load_simple_config(self):
        """Test loading a simple config file."""
        config_content = """
ARCHIVE_SYSTEM=rclone
RCLONE_DRIVE=gdrive
RCLONE_PATH=/TeslaCam
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".conf", delete=False) as f:
            f.write(config_content)
            config_path = Path(f.name)

        try:
            config = load_from_file(config_path)

            assert config.archive.system == "rclone"
            assert config.archive.rclone_drive == "gdrive"
            assert config.archive.rclone_path == "/TeslaCam"
        finally:
            config_path.unlink()

    def test_load_config_with_exports(self):
        """Test loading config with export statements."""
        config_content = """
export ARCHIVE_SYSTEM=rclone
export RCLONE_DRIVE=s3
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".conf", delete=False) as f:
            f.write(config_content)
            config_path = Path(f.name)

        try:
            config = load_from_file(config_path)

            assert config.archive.system == "rclone"
            assert config.archive.rclone_drive == "s3"
        finally:
            config_path.unlink()

    def test_load_config_with_comments(self):
        """Test loading config with comments."""
        config_content = """
# This is a comment
ARCHIVE_SYSTEM=none
# Another comment
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".conf", delete=False) as f:
            f.write(config_content)
            config_path = Path(f.name)

        try:
            config = load_from_file(config_path)

            assert config.archive.system == "none"
        finally:
            config_path.unlink()

    def test_load_config_with_quotes(self):
        """Test loading config with quoted values."""
        config_content = """
RCLONE_DRIVE='gdrive'
RCLONE_PATH="/My Drive/TeslaCam"
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".conf", delete=False) as f:
            f.write(config_content)
            config_path = Path(f.name)

        try:
            config = load_from_file(config_path)

            assert config.archive.rclone_drive == "gdrive"
            assert config.archive.rclone_path == "/My Drive/TeslaCam"
        finally:
            config_path.unlink()

    def test_load_nonexistent_file(self):
        """Test loading nonexistent file raises error."""
        with pytest.raises(ConfigError):
            load_from_file(Path("/nonexistent/config.conf"))

    def test_load_archive_flags(self):
        """Test loading archive clip flags."""
        config_content = """
ARCHIVE_RECENTCLIPS=true
ARCHIVE_SAVEDCLIPS=false
ARCHIVE_SENTRYCLIPS=true
ARCHIVE_TRACKMODECLIPS=false
ARCHIVE_PHOTOBOOTH=false
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".conf", delete=False) as f:
            f.write(config_content)
            config_path = Path(f.name)

        try:
            config = load_from_file(config_path)

            assert config.archive.archive_recent is True
            assert config.archive.archive_saved is False
            assert config.archive.archive_sentry is True
            assert config.archive.archive_track is False
            assert config.archive.archive_photobooth is False
        finally:
            config_path.unlink()
