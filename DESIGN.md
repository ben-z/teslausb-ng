# TeslaUSB Core - Design Document

## Overview

A clean Python rewrite of TeslaUSB's snapshot/archive system. Focused on correctness, simplicity, and testability.

## Problems with the Original Bash Design

1. **Race Conditions**: `freespacemanager` can delete snapshots while `archiveloop` reads them
2. **No Reference Counting**: Snapshots have no concept of "in use"
3. **Unbounded Snapshots**: Timer-based snapshots accumulate, causing disk full errors
4. **Complex State**: State spread across filesystem markers with no clear model

## Design Principles

1. **Single Source of Truth**: The `.toc` file indicates snapshot completion
2. **Derived State**: Snapshot state is computed from refcount, not stored
3. **Reference Counting**: Snapshots cannot be deleted while in use
4. **On-Demand Snapshots**: Only taken when WiFi available (no background accumulation)
5. **Protocol-Based Abstractions**: All I/O abstracted for testability

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Coordinator                             │
│  (Orchestrates snapshot creation, archiving, space management)  │
└─────────────────────┬───────────────────────────────────────────┘
                      │
        ┌─────────────┼─────────────┬─────────────┐
        │             │             │             │
        ▼             ▼             ▼             ▼
┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
│  Snapshot   │ │   Archive   │ │    Space    │ │   Gadget    │
│  Manager    │ │   Manager   │ │   Manager   │ │  (USB MSC)  │
└──────┬──────┘ └──────┬──────┘ └──────┬──────┘ └─────────────┘
       │               │               │
       └───────────────┴───────────────┘
                       │
                       ▼
           ┌───────────────────────┐
           │  Filesystem Protocol  │
           └───────────┬───────────┘
                       │
           ┌───────────┴───────────┐
           ▼                       ▼
   ┌───────────────┐       ┌───────────────┐
   │ RealFilesystem│       │ MockFilesystem│
   └───────────────┘       └───────────────┘

Optional Components:
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│   Idle      │  │    LED      │  │ Temperature │
│  Detector   │  │ Controller  │  │   Monitor   │
└─────────────┘  └─────────────┘  └─────────────┘
```

## Snapshot Design

### State Model

State is **derived from refcount**, not stored:

```python
class SnapshotState(Enum):
    READY = "ready"        # refcount == 0
    ARCHIVING = "archiving"  # refcount > 0

@dataclass
class Snapshot:
    id: int
    path: Path
    created_at: datetime
    refcount: int = 0  # Runtime only, always 0 on load

    @property
    def state(self) -> SnapshotState:
        return SnapshotState.ARCHIVING if self.refcount > 0 else SnapshotState.READY
```

### Crash Safety

The `.toc` file is the single source of truth for completion:

- **Create**: Write data first, create `.toc` last. Crash before `.toc` = incomplete = auto cleanup.
- **Delete**: Delete `.toc` first, then delete directory. Crash mid-delete = auto cleanup.
- **Load**: Any directory without `.toc` is deleted on startup.

```
On Startup:
  for each snapshot directory:
    if .toc exists → load as READY
    if .toc missing → delete (incomplete)
```

### Reference Counting

```python
# Acquire increments refcount
handle = snapshot_manager.acquire(snapshot_id)
assert snapshot.refcount == 1
assert snapshot.state == SnapshotState.ARCHIVING

# Release decrements refcount
handle.release()
assert snapshot.refcount == 0
assert snapshot.state == SnapshotState.READY

# Cannot delete while in use
with manager.acquire(snap_id) as handle:
    manager.delete_snapshot(snap_id)  # Raises SnapshotInUseError
```

## Main Loop

```python
while running:
    # 1. Wait for WiFi/archive server
    wait_for_archive_reachable()

    # 2. Wait for settling + idle
    sleep(archive_delay)
    wait_for_idle()  # Optional: wait for car to stop writing

    # 3. Ensure space, take snapshot, archive
    space_manager.ensure_space_for_snapshot()
    with snapshot_manager.snapshot_session() as handle:
        archive_manager.archive_snapshot(handle, mount_path)

    # 4. Cleanup old snapshots
    space_manager.cleanup_if_needed()

    # 5. Wait for car to drive away
    wait_for_archive_unreachable()
```

Key difference from original: **no timer-based background snapshots**. This prevents snapshot accumulation and disk full errors.

## Space Management

```
Total Space
├── cam_disk.bin (CAM_SIZE)
├── music_disk.bin (MUSIC_SIZE, optional)
├── snapshots/ (variable, up to CAM_SIZE per snapshot)
└── free space (must maintain >= reserve)

Reserve = 10GB (filesystem overhead + safety margin)

Recommended: total >= 2 * CAM_SIZE + other_drives + reserve
```

## Modules

| Module | Purpose |
|--------|---------|
| `snapshot.py` | Snapshot lifecycle with refcounting |
| `archive.py` | Archive to rclone backend |
| `space.py` | Disk space management |
| `coordinator.py` | Main orchestration loop |
| `filesystem.py` | Filesystem abstraction |
| `config.py` | Configuration from env/file |
| `mount.py` | Loop device mounting |
| `gadget.py` | USB mass storage gadget |
| `idle.py` | Detect when car stops writing |
| `led.py` | Status LED control |
| `temperature.py` | CPU temperature monitoring |
| `cli.py` | Command-line interface |

## Configuration

Environment variables (compatible with original):
- `CAM_SIZE` - Camera disk size (e.g., "40G")
- `ARCHIVE_SYSTEM` - "rclone" or "none"
- `RCLONE_DRIVE` - rclone remote name
- `RCLONE_PATH` - Path within remote

## Testing

All I/O goes through protocols, enabling pure unit tests:

```python
def test_snapshot_creation(mock_fs):
    manager = SnapshotManager(fs=mock_fs, ...)
    snap = manager.create_snapshot()
    assert mock_fs.exists(snap.toc_path)
```

Run tests: `PYTHONPATH=src pytest tests/ -v`

## File Structure

```
teslausb-core/
├── src/teslausb/
│   ├── __init__.py
│   ├── archive.py
│   ├── cli.py
│   ├── config.py
│   ├── coordinator.py
│   ├── filesystem.py
│   ├── gadget.py
│   ├── idle.py
│   ├── led.py
│   ├── mount.py
│   ├── snapshot.py
│   ├── space.py
│   └── temperature.py
├── tests/
│   ├── conftest.py
│   ├── test_*.py
├── pyproject.toml
└── DESIGN.md
```
