# Design

## Why Rewrite?

The original bash implementation had:

1. **Race conditions**: `freespacemanager` deletes snapshots while `archiveloop` reads them
2. **No reference counting**: Snapshots have no concept of "in use"
3. **Unbounded snapshots**: Timer-based snapshots accumulate, causing disk-full errors
4. **Complex state**: Spread across filesystem markers with no clear model

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Coordinator                             │
│  (Orchestrates snapshot creation, archiving, space management)  │
└─────────────────────┬───────────────────────────────────────────┘
                      │
        ┌─────────────┼─────────────┬─────────────┐
        ▼             ▼             ▼             ▼
   Snapshot      Archive       Space         Gadget
   Manager       Manager       Manager      (USB MSC)
        │             │             │
        └─────────────┴─────────────┘
                      │
                      ▼
             Filesystem Protocol
                      │
           ┌──────────┴──────────┐
           ▼                     ▼
     RealFilesystem       MockFilesystem
```

Optional: IdleDetector, LedController, TemperatureMonitor

## Snapshot Design

### State Model

State is **derived from refcount**, not stored:

```python
class SnapshotState(Enum):
    READY = "ready"        # refcount == 0
    ARCHIVING = "archiving"  # refcount > 0

@property
def state(self) -> SnapshotState:
    return SnapshotState.ARCHIVING if self.refcount > 0 else SnapshotState.READY
```

### Crash Safety

The `.toc` file is the single source of truth:

| Operation | Order | Crash Recovery |
|-----------|-------|----------------|
| Create | Write data, then `.toc` | No `.toc` = incomplete = auto-delete |
| Delete | Delete `.toc`, then data | No `.toc` = incomplete = auto-delete |
| Load | Check `.toc` exists | Missing `.toc` = delete directory |

### Reference Counting

```python
with snapshot_manager.acquire(snap_id) as handle:
    # refcount=1, cannot be deleted
    archive_files(handle.snapshot)
# refcount=0, can be deleted
```

## Main Loop

```python
while running:
    wait_for_archive_reachable()
    wait_for_idle()

    space_manager.ensure_space_for_snapshot()
    with snapshot_manager.snapshot_session() as handle:
        archive_manager.archive_snapshot(handle, mount_path)

    space_manager.cleanup_if_needed()
    wait_for_archive_unreachable()
```

Key: **no timer-based snapshots** - only when WiFi available.

## Space Management

```
Total Space
├── cam_disk.bin (CAM_SIZE)
├── music_disk.bin (optional)
├── snapshots/
└── free space (>= 10GB reserve)

Recommended: total >= 2 * CAM_SIZE + other_drives + 10GB
```
