# Design

## Goal

teslausb-ng is an appliance-style command-line tool for Tesla dashcam archiving.
It should be simple to deploy, predictable after power loss, and conservative
around mounted filesystems and USB gadget state.

The implementation is Rust, but the design remains Unix-oriented: specialized
system tools do filesystem, partition, USB, and cloud-storage work. The binary
coordinates them and owns the safety invariants.

## Architecture

```text
CLI
 ├── Config
 ├── SnapshotManager
 ├── ArchiveManager
 ├── Coordinator
 ├── UsbGadget
 ├── MountedImage / LoopDevice guards
 └── CommandRunner
```

| Module | Purpose |
| --- | --- |
| `cli.rs` | Command-line interface and systemd installation |
| `command.rs` | Subprocess execution with captured output and timeouts |
| `config.rs` | Environment and config-file loading |
| `filesystem.rs` | Filesystem abstraction and test mock |
| `snapshot.rs` | Snapshot lifecycle and refcounting |
| `archive.rs` | rclone archiving and post-archive deletion checks |
| `mount.rs` | Loop device and mount RAII guards |
| `dependencies.rs` | Startup and doctor checks for external commands and versions |
| `gadget.rs` | Linux USB gadget configfs management |
| `idle.rs` | USB mass-storage write-idle detection |
| `led.rs` | Sysfs status LED patterns |
| `temperature.rs` | Sysfs CPU temperature monitoring |
| `coordinator.rs` | Main archive loop |
| `space.rs` | Disk sizing and space reporting |

## Crash Safety

The snapshot completion marker is the source of truth.

| Operation | Order | Recovery |
| --- | --- | --- |
| Create | reflink `snap.bin`, write metadata, atomically write `snap.toc` | no `snap.toc` means remove during explicit recovery |
| Delete | remove `snap.toc`, sync directory, remove snapshot directory | no `snap.toc` means remove during explicit recovery |
| Load | scan `snap-*` directories and require `snap.toc` | inspection preserves incomplete directories; runtime recovery removes them |

Metadata is useful but not trusted. If metadata is missing or stale, the snapshot
can be reconstructed from the directory name and `snap.bin` mtime.

## Resource Safety

Rust destructors are used for resources that must be unwound:

- `LoopDevice` detaches `losetup` and removes `kpartx` mappings
- `MountedImage` unmounts and removes the temporary mount directory
- `GadgetDisableGuard` restores the gadget only after explicit checked completion; failures leave it disabled
- `SnapshotHandle` decrements snapshot refcounts on drop
- `TemperatureMonitorGuard` stops the background temperature thread on drop

This does not make `SIGKILL` or power loss graceful, so the `.toc` recovery model
still matters.

## Archive Cycle

```text
wait until archive is reachable
set status LED to slow blink
set status LED to fast blink while the archive cycle runs
delete all stale/deletable snapshots
wait for USB writes to become idle; skip on timeout
create reflink snapshot
recover ext4 journal on a private reflink copy, then verify it
mount snapshot through a read-only loop device
copy enabled clip directories with rclone JSON confirmations
select complete, stable Saved/Sentry events for cleanup
wait for USB writes to become idle again; skip cleanup on timeout
set status LED to heartbeat while cleaning up
disable USB gadget if it is enabled
repair and independently verify the cam filesystem
mount cam disk read-write
revalidate whole event file sets, sizes, and modification times
delete only confirmed files in unchanged events
check cam disk unmount succeeds
verify cam filesystem and re-enable USB gadget
delete snapshot
set status LED to heartbeat after a successful cycle
repeat
```

Archive readers require an ext4 snapshot partition and reject other formats.
A private reflink copy receives journal-only replay followed by a forced
read-only filesystem check. A successful check permits a read-only loop and
`ro,noload` mount, so mounting cannot replay the journal or modify the raw
snapshot.

The durable `snapshots/recovery.state` record names the source snapshot, image
size, and phase: pending, ready, or failed. Pending state is written before
creating `snapshots/recovery/`. The raw snapshot is preserved there as `raw.bin`;
journal recovery runs on a separate `recovered.bin` reflink copy. Ready state is
written after successful recovery, verification, and image synchronization.
Observed preparation or mount failures are recorded as failed.

Startup reconciles this workspace before removing incomplete or stale snapshots.
Pending preparation is rebuilt from the preserved raw image, or from the named
complete snapshot if the raw link was not yet created. A ready workspace is
released only after its images have no loop owners. The state record remains
until workspace removal finishes, so an interrupted cleanup can resume.
Failed or malformed state and unmarked recovery directories require inspection;
their evidence is preserved. New snapshots are blocked while either recovery
path exists, maintaining the single-snapshot space bound. Normal archive cleanup
releases a successful workspace after checked unmount and loop detach.

The live camera disk is never mounted read-write while it is exposed to the car
through the USB gadget.

RecentClips uploads are partitioned by date, with the known metadata files in a
separate metadata directory. Local RecentClips files remain owned by the car.
Saved/Sentry events must remain unchanged across snapshots for the configured
grace period and every event file must have a positive rclone confirmation.
Metadata-only events remain on the camera disk.

A catalog lock serializes snapshot creation, inspection, recovery, and deletion.
Per-snapshot locks remain held through use or deletion, and a persistent ID
counter prevents stale processes from reusing snapshot IDs. A separate archive
lock prevents concurrent processes from maintaining the live camera disk.

## Storage Model

```text
/mutable/backingfiles.img  (XFS, reflink-capable)
  mounted at /backingfiles
    cam_disk.bin           (ext4 disk image exposed to Tesla)
    snapshots/
      snap-000000/
        snap.bin           (reflink copy of cam_disk.bin)
        metadata.json
        snap.toc
```

The camera disk size is calculated as:

```text
cam_size = (backingfiles_size - 3% XFS overhead) / 2
```

This reserves half the XFS volume for the worst case where every camera-disk block
diverges while a snapshot exists.

## Deployment

Deployment is a binary copy:

```bash
cargo build --release
sudo install -m 0755 target/release/teslausb /usr/local/bin/teslausb
```

The systemd unit is generated by `teslausb service install` using the actual
`current_exe()` path, avoiding `pip`, virtual environments, and `sudo PATH`
ambiguity.
