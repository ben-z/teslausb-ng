# teslausb-ng

A Rust implementation of TeslaUSB-style dashcam archiving.

The tool presents a FAT32 camera disk to a Tesla over Linux USB gadget mode, takes
crash-safe XFS reflink snapshots, archives clips with `rclone`, then removes only
files that were verified as copied.

## Why Rust

The original Python version was intentionally small, but deployment still depended
on Python packaging and target-board `pip` behavior. The Rust path keeps the same
Unix-tool design while producing a single CLI binary.

Rust is used here for:

- one deployable `teslausb` binary
- RAII guards for mounts, loop devices, and USB gadget disable/enable scopes
- atomic snapshot marker writes
- central subprocess execution with timeouts
- USB write-idle detection before taking snapshots
- best-effort sysfs LED and CPU temperature monitoring during `run`
- explicit state and error propagation

External tools remain external: `rclone`, coreutils, `mount`, `losetup`, `fsck`,
`mkfs.xfs`, `mkfs.vfat`, `parted`, `kpartx`, `modprobe`, and `systemctl`.

## Requirements

- Linux board with USB OTG/peripheral support
- USB gadget support through configfs
- XFS reflink support
- `rclone` configured for the archive destination
- Optional sysfs LED, `/proc`, and thermal-zone support for status, write-idle,
  and temperature monitoring

Install system packages:

```bash
sudo apt update
sudo apt install -y rclone xfsprogs parted dosfstools kpartx util-linux
```

## Build And Install

Build on the target board:

```bash
cargo build --release
sudo install -m 0755 target/release/teslausb /usr/local/bin/teslausb
```

Or build elsewhere for the board target and install the resulting binary.

Check the target:

```bash
sudo teslausb doctor
```

`doctor` checks required commands, reports versions, enforces minimum versions
where TeslaUSB relies on specific behavior, and verifies that GNU `cp` advertises
`--reflink` support. The generated systemd unit runs `teslausb doctor --startup`
before it mounts images or enables the USB gadget.

Current minimum gates are `rclone >= 1.50.0`, `mkfs.xfs >= 4.9.0`, and
GNU `cp >= 8.23.0`.

## Configure

Configure `rclone` as the same user that the service will run as. The systemd
service runs as root, so root needs the `rclone` remote unless you customize the
unit.

```bash
sudo rclone config
```

Create `/etc/teslausb.conf`:

```bash
ARCHIVE_SYSTEM=rclone
RCLONE_DRIVE=gdrive
RCLONE_PATH=/TeslaCam
```

Supported variables:

| Variable | Description | Default |
| --- | --- | --- |
| `ARCHIVE_SYSTEM` | `rclone` or `none` | `none` |
| `RCLONE_DRIVE` | rclone remote name | |
| `RCLONE_PATH` | Path within remote | |
| `RCLONE_FLAGS` | Extra rclone flags split on whitespace | |
| `ARCHIVE_SAVEDCLIPS` | Archive SavedClips | `true` |
| `ARCHIVE_SENTRYCLIPS` | Archive SentryClips | `true` |
| `ARCHIVE_RECENTCLIPS` | Archive RecentClips | `false` |
| `ARCHIVE_TRACKMODECLIPS` | Archive TrackMode clips | `true` |
| `ARCHIVE_PHOTOBOOTH` | Archive Photobooth files | `true` |
| `MUTABLE_PATH` | Path for `backingfiles.img` | `/mutable` |
| `BACKINGFILES_PATH` | Mount point for backing files | `/backingfiles` |

## Initialize

Create the XFS backing image and FAT32 camera disk:

```bash
sudo teslausb init --reserve 10G
```

This creates:

- `/mutable/backingfiles.img`
- `/backingfiles/cam_disk.bin`
- `/backingfiles/snapshots/`

Sizing is automatic:

```text
backingfiles_size = available_disk - reserve
cam_size = (backingfiles_size - 3% XFS overhead) / 2
```

Half the XFS volume is reserved for worst-case snapshot COW growth.

## Run

Install and start the systemd service:

```bash
sudo teslausb service install
sudo systemctl start teslausb
```

The generated service runs:

```text
teslausb mount
teslausb gadget on
teslausb run
teslausb gadget off
```

Manual run:

```bash
sudo teslausb mount
sudo teslausb gadget on
sudo teslausb run
```

## Commands

| Command | Description |
| --- | --- |
| `teslausb init [--reserve SIZE]` | Initialize disk images |
| `teslausb deinit [-y]` | Remove disk images and clean up |
| `teslausb mount` | Mount `backingfiles.img` |
| `teslausb run` | Run the archive loop |
| `teslausb archive` | Run one archive cycle |
| `teslausb status [--json]` | Show status |
| `teslausb snapshots [--json]` | List snapshots |
| `teslausb clean [--dry-run]` | Delete deletable snapshots |
| `teslausb gadget on/off/status` | Manage USB gadget mode |
| `teslausb service install/uninstall/status` | Manage systemd |
| `teslausb doctor [--startup]` | Check external dependencies and versions |

## Board Notes

### Rock 5C

Enable the USB peripheral overlay in `/boot/armbianEnv.txt`:

```text
overlays=rk3588-dwc3-peripheral
```

Then reboot and verify:

```bash
ls /sys/class/udc/
```

### Raspberry Pi

Ensure `/boot/config.txt` contains:

```text
dtoverlay=dwc2
```

Ensure `/boot/cmdline.txt` includes `modules-load=dwc2` after `rootwait`.

## Development

```bash
cargo fmt
cargo test
cargo llvm-cov --locked --summary-only --fail-under-lines 85
cargo build --release
scripts/run-linux-integration.sh
```

The Rust tests use `MockFileSystem` for snapshot and archive behavior.
`tests/offline_cli.rs` runs the compiled binary against fake Unix tools so init,
mount, `run`, archive, clean, status, doctor, and service flows are validated
without root or Linux block devices. Tests that need real loop devices and
filesystems live in `tests/linux_integration.rs` and are run by
`scripts/run-linux-integration.sh` on a privileged Linux host, VM, or QEMU guest.
The offline tests use `TESLAUSB_LED_PATH`, `TESLAUSB_THERMAL_PATH`,
`TESLAUSB_PROC_PATH`, and `TESLAUSB_IDLE_TIMEOUT_SECS` to point runtime monitors
at fixtures instead of host sysfs/proc paths.
Install coverage locally with `cargo install cargo-llvm-cov --locked` if needed.

## Safety Model

- A snapshot is complete only after `snap.toc` exists.
- Snapshot creation writes data and metadata before atomically writing `snap.toc`.
- Snapshot deletion removes `snap.toc` first, then removes data.
- On start up, any snapshot directory without `snap.toc` is cleaned up.
- Cleaning up the live camera disk disables the USB gadget before mounting read-write.
- Each deletion checks that the current file size still matches the archived file.

## License

MIT
