# Design

## Why Rewrite?

The [original TeslaUSB](https://github.com/marcone/teslausb) bash implementation had:

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
```

Archives continuously while WiFi is available. Idle detection gates each cycle.

## Storage Architecture

```
/mutable/backingfiles.img (XFS, sparse)
    └── mounted at /backingfiles
        ├── cam_disk.bin (FAT32, sparse, CAM_SIZE)
        │   └── TeslaCam/
        └── snapshots/
            └── <id>/
                ├── image.bin (reflink copy of cam_disk.bin)
                └── .toc
```

**Why XFS?** Reflinks (copy-on-write) enable instant, space-efficient snapshots. A 40 GiB cam disk can be "copied" in milliseconds, using no extra space until Tesla writes new data.

**Why a disk image?** The backingfiles.img allows teslausb to work on any root filesystem (ext4, etc.) while still getting XFS benefits for the snapshot directory.

## Space Management

**Simple model**: User sets `RESERVE` (space for OS), everything else is automatic.

```
available_disk - RESERVE = backingfiles.img size
backingfiles.img - 3% XFS overhead = usable space
usable space / 2 = cam_size (half for cam_disk, half for snapshot)
```

Example with 128 GiB SD card:
- RESERVE = 10 GiB (for OS)
- backingfiles.img = 118 GiB
- XFS overhead = 3.5 GiB (3%)
- cam_size = 57 GiB
- min_free_threshold = 28.5 GiB (50% of cam_size by default)

**Key invariant**: Always maintain at least `min_free_threshold` (default 50% of `cam_size`) free space so the next snapshot is guaranteed to succeed.

- Snapshots use XFS reflinks (copy-on-write), so they start small
- Worst case: snapshot grows to full `cam_size` if all blocks change during archiving
- The threshold is configurable via `SNAPSHOT_SPACE_PROPORTION` (default 0.5) because worst-case 100% COW copy is unrealistic
- Cleanup deletes oldest snapshots until `free_space >= min_free_threshold`
