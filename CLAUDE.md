# CLAUDE.md

This file provides guidance for AI assistants working with the teslausb-ng codebase.

## Project Overview

teslausb-ng is a Python rewrite of [TeslaUSB](https://github.com/marcone/teslausb) — a system that archives Tesla dashcam footage to cloud storage. It emulates a USB mass storage device, takes crash-safe snapshots via XFS reflinks, and uploads clips via rclone. See [DESIGN.md](DESIGN.md) for architecture details.

## Quick Reference

```bash
# Run unit tests (no root needed)
pytest tests/ --ignore=tests/integration -v

# Run integration tests (requires Docker + privileged mode)
docker compose -f docker-compose.test.yml up --build

# Type checking
mypy src/

# Lint and format check
ruff check src/ tests/
ruff format --check src/ tests/
```

## Code Style and Conventions

- **Python 3.9+** — all code must be compatible with 3.9
- **Type hints everywhere** — strict mypy is enforced (`strict = true`)
- **Zero production dependencies** — only stdlib; dev deps are pytest, pytest-cov, mypy, ruff
- **Line length**: 100 characters (ruff)
- **Ruff rules**: E, F, I, N, W, UP, B, C4, SIM
- **Dataclasses** for data structures, **Protocols** for abstractions
- **Each module has one clear purpose** — see module table below

### Grammar Convention

In prose (comments, docstrings, error messages): use **verb form** (two words — "set up", "clean up", "shut down"). In identifiers (functions, variables, commands): **noun form** (one word) is fine ("setup", "cleanup", "shutdown").

CLI commands use verb forms: `init`, `run`, `clean`, `validate`, `enable`, `disable`, `remove`.

## Repository Structure

```
src/teslausb/          # All source code
  cli.py               # Command-line interface (entry point: main())
  coordinator.py       # Main orchestration loop
  snapshot.py          # Snapshot lifecycle with refcounting
  archive.py           # Archive via rclone
  space.py             # Disk space management
  filesystem.py        # Filesystem abstraction (Protocol + Real + Mock)
  config.py            # Configuration from env/file
  gadget.py            # USB mass storage gadget
  mount.py             # Loop device mounting
  idle.py              # Detect when car stops writing
  led.py               # Status LED control
  temperature.py       # CPU temperature monitoring

tests/                 # Unit tests (MockFilesystem, no real I/O)
  conftest.py          # Shared fixtures: mock_fs, snapshot_manager, etc.
  test_*.py            # One test file per module

tests/integration/     # Integration tests (require Docker + root)
  conftest.py          # Real filesystem fixtures, loop devices
  test_*.py            # End-to-end tests with real XFS/FAT32
```

## Build and Dependencies

- **Build system**: Hatchling (`pyproject.toml`)
- **Package manager**: uv (lock file: `uv.lock`)
- **Entry point**: `teslausb = "teslausb.cli:main"`
- **Version**: 2.0.0

Install for development:
```bash
pip install -e ".[dev]"
```

## Testing

### Unit Tests

```bash
pytest tests/ --ignore=tests/integration -v
```

- Use `MockFilesystem` — never real filesystem operations
- Key fixtures in `tests/conftest.py`: `mock_fs`, `snapshot_manager`, `space_manager`, `mock_backend`, `mock_fs_with_teslacam`

### Integration Tests

```bash
docker run --privileged \
  -e CAM_SIZE=256M \
  -e ARCHIVE_SYSTEM=rclone \
  -e RCLONE_DRIVE=:memory: \
  -e RCLONE_PATH=/test \
  teslausb-test \
  pytest tests/integration -v --tb=short
```

- Require `--privileged` Docker (real loop devices, XFS, partition tables)
- Marked with `@pytest.mark.integration`

### CI

GitHub Actions runs on push/PR to `main`:
- Unit tests on Python 3.11 and 3.12
- Integration tests in Docker (privileged)

## Architecture Essentials

- **Coordinator** orchestrates: SnapshotManager, ArchiveManager, SpaceManager, UsbGadget
- **Filesystem Protocol** enables testing: `RealFilesystem` (production) / `MockFilesystem` (tests)
- **Crash safety**: `.toc` file is single source of truth — no `.toc` = incomplete = auto-delete
- **Reference counting**: RAII-style handles via context managers prevent deletion during archiving
- **Storage**: XFS reflinks for instant snapshots; FAT32 cam_disk presented to Tesla via USB gadget
- **Space formula**: `cam_size = (backingfiles_size - 3% overhead) / 2` — half for cam, half for snapshot headroom

## Common Tasks

**Adding a new archive backend:**
1. Implement `ArchiveBackend` in `archive.py`
2. Add creation logic in `cli.py:create_components()`
3. Add tests in `test_archive.py`

**Modifying snapshot behavior:**
1. Core logic in `snapshot.py`
2. `.toc` handling is critical for crash safety
3. Test with `MockFilesystem`

**Changing space management:**
1. Logic in `space.py`
2. Reserve is fixed at 10 GB
3. Formula: `cam_size = (backingfiles_size - 3% overhead) / 2`

**Adding a new CLI command:**
1. Add subparser in `cli.py`
2. Implement handler function in `cli.py`
3. Add integration tests if it touches real filesystems

## Commit Message Style

- Present tense, descriptive: "Add X feature", "Fix X behavior", "Replace X with Y"
- Reference issues with `(#N)` when applicable
- Keep the first line concise and specific
