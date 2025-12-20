# teslausb-ng

A Python rewrite of TeslaUSB's dashcam archiving system. Focused on correctness, simplicity, and testability.

## Features

- **On-demand snapshots**: Only when WiFi is available (no disk-full errors)
- **Reference counting**: Prevents race conditions between archiving and cleanup
- **Crash-safe**: Uses `.toc` file as single source of truth
- **rclone support**: Archive to 40+ cloud providers (Google Drive, S3, Dropbox, etc.)

## Quick Start

```bash
# Install
pip install ./teslausb-ng

# Configure (same env vars as original TeslaUSB)
export CAM_SIZE=40G
export ARCHIVE_SYSTEM=rclone
export RCLONE_DRIVE=gdrive
export RCLONE_PATH=/TeslaCam

# Run
teslausb run
```

## Commands

```
teslausb run        # Main loop: wait for WiFi, snapshot, archive, repeat
teslausb archive    # Single archive cycle
teslausb status     # Show space and snapshot info
teslausb snapshots  # List snapshots
teslausb cleanup    # Delete old snapshots
teslausb validate   # Check configuration
teslausb gadget     # Manage USB mass storage
```

## Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `CAM_SIZE` | Camera disk size | `40G` |
| `ARCHIVE_SYSTEM` | `rclone` or `none` | `none` |
| `RCLONE_DRIVE` | rclone remote name | |
| `RCLONE_PATH` | Path within remote | |
| `ARCHIVE_DELAY` | Seconds before archiving | `20` |

## Development

```bash
# Run tests
PYTHONPATH=src pytest tests/ -v

# Install in dev mode
pip install -e .
```

## License

MIT
