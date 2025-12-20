# Agent Guidelines

## Project Overview

teslausb-ng is a Python rewrite of TeslaUSB's dashcam archiving system. It runs on a Raspberry Pi connected to a Tesla vehicle via USB, presenting as a mass storage device and archiving footage to cloud storage.

## Architecture

```
Coordinator → SnapshotManager → Filesystem (protocol)
            → ArchiveManager  → RcloneBackend
            → SpaceManager
```

All I/O goes through protocol abstractions (`Filesystem`, `ArchiveBackend`, etc.) with mock implementations for testing.

## Key Design Decisions

1. **Derived state**: Snapshot state (READY/ARCHIVING) is computed from refcount, not stored
2. **Single source of truth**: The `.toc` file marks snapshot completion
3. **Reference counting**: Prevents deletion of in-use snapshots
4. **On-demand snapshots**: Only taken when WiFi available (no timer-based accumulation)

## Code Style

- Python 3.11+, type hints everywhere
- Dataclasses for data, protocols for abstractions
- Each module has one clear purpose
- Tests use mock implementations, not real filesystem

## Testing

```bash
PYTHONPATH=src pytest tests/ -v
```

All 181 tests should pass. When modifying code, ensure tests still pass.

## Common Tasks

### Adding a new archive backend

1. Implement `ArchiveBackend` protocol in `archive.py`
2. Add backend creation logic in `cli.py:create_components()`
3. Add tests in `test_archive.py`

### Modifying snapshot behavior

1. Core logic is in `snapshot.py`
2. `.toc` file handling is critical for crash safety
3. Always test with `MockFilesystem`

### Changing space management

1. Logic is in `space.py`
2. Reserve is fixed at 10GB
3. Formula: `recommended_cam_size = (total - other_drives - reserve) / 2`
