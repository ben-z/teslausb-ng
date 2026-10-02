# teslausb-ng

A Rust implementation of [TeslaUSB](https://github.com/marcone/teslausb)'s dashcam
archiving system. It presents a FAT32 or ext4 camera disk over USB, copies XFS reflink
snapshots to an rclone remote, and removes saved events only after their files
have been verified in the archive.

## Requirements

- Linux board with a USB peripheral port and configfs gadget support
- XFS with reflink support, plus the system tools listed below
- Network access to an rclone archive destination
- Readable `/proc`, a CPU temperature sensor, and a status LED with timer and
  heartbeat triggers in sysfs
- Rust 1.89 or newer with Cargo to build the binary

The installed program is one binary. It uses `serde_json` for JSON and external
Linux tools for filesystems, USB devices, and transfers.

## Quick Start

1. [Connect to WiFi](#set-up-wifi).
2. [Build and install](#build-and-install).
3. [Enable the board's USB peripheral mode](#board-configuration).
4. [Configure the archive](#configure).
5. [Initialize new disk images](#initialize) and [start the service](#run).

## Set Up WiFi

Use the network manager configured by your Linux image.

### NetworkManager (nmcli)

```bash
# List available networks
sudo nmcli device wifi list

# Connect to a network
sudo nmcli --ask device wifi connect "YourNetworkName"

# Verify connection
nmcli connection show --active
```

#### Multiple Networks

You can save multiple WiFi networks (home, work, mobile hotspot):

```bash
sudo nmcli --ask device wifi connect "WorkWiFi"
sudo nmcli --ask device wifi connect "iPhone"
```

The device will automatically connect to whichever saved network is available.

### Netplan

For images using Netplan with `systemd-networkd`, replace `wlan0` with your
wireless interface name from `ip link`. Edit the existing WiFi configuration
under `/etc/netplan/` with `sudoedit`, using this structure:

```yaml
network:
  version: 2
  renderer: networkd
  wifis:
    wlan0:
      dhcp4: true
      dhcp6: true
      access-points:
        "YourSSID":
          password: "YourPassword"
```

Protect the file containing your password, validate it, and try the configuration:

```sh
sudo chmod 600 /etc/netplan/20-wifi.yaml  # Use the filename you edited
sudo netplan generate
sudo netplan try
networkctl status wlan0
```


## Build and Install

On a Debian or Ubuntu board, install the system dependencies:

```bash
sudo apt update
sudo apt install -y git rclone xfsprogs parted dosfstools e2fsprogs kpartx util-linux kmod
```

With Rust and Cargo installed, build on the board:

```bash
git clone https://github.com/ben-z/teslausb-ng.git
cd teslausb-ng
cargo build --release --locked
sudo install -m 0755 target/release/teslausb /usr/local/bin/teslausb
```

A binary built elsewhere must target the board's Linux architecture.

### Update an Existing Installation

The Python and Rust implementations use the same disk image paths and
`/etc/teslausb.conf`. Keep those images and the root user's rclone configuration
when updating. Existing snapshots with `snap.toc` remain readable.

From the source checkout:

```bash
git pull --ff-only
cargo build --release --locked
sudo systemctl stop teslausb
sudo install -m 0755 target/release/teslausb /usr/local/bin/teslausb
sudo teslausb service install --force
sudo systemctl start teslausb
sudo systemctl is-active teslausb
sudo journalctl -u teslausb -n 100 --no-pager
```

The forced service installation updates the executable path and lifecycle
commands. Initialization is for new installations; `deinit` permanently deletes
local recordings.

## Board Configuration

### Rock 5C

For Armbian images providing the `rk3588-dwc3-peripheral` overlay, add its name to
the `overlays` line in `/boot/armbianEnv.txt`, preserving other entries. If there
is no `overlays` line, add:

```text
overlays=rk3588-dwc3-peripheral
```

Reboot and check that a USB Device Controller is listed:

```bash
ls /sys/class/udc/
```

The peripheral port connects to the car as a USB device. Use another port for
USB host peripherals.

### Raspberry Pi

Enable `dtoverlay=dwc2` in the boot configuration used by your image, commonly
`/boot/firmware/config.txt` or `/boot/config.txt`. Add `modules-load=dwc2` to the
corresponding `cmdline.txt`, keeping its contents on one line. Reboot and verify
`/sys/class/udc/` contains a controller.

## Configure

The systemd service runs as root. Configure its rclone remote as root:

```bash
sudo rclone config
```

For browser authorization on a headless device, follow
[rclone's headless instructions](https://rclone.org/remote_setup/). Answer `n`
when asked to authenticate with a local browser, then run the displayed
`rclone authorize` command on a computer with a browser.

Create `/etc/teslausb.conf` with `sudoedit`:

```text
ARCHIVE_SYSTEM=rclone
RCLONE_DRIVE=gdrive
RCLONE_PATH=/TeslaCam
```

Use the remote name you created and verify access:

```bash
sudo rclone lsd gdrive:
sudo teslausb doctor
```

`doctor` checks required commands, versions, and GNU `cp` reflink support.
Required minimum versions are rclone 1.50.0, XFS tools 4.9.0, and GNU coreutils
8.23.0.

| Variable | Description | Default |
| --- | --- | --- |
| `ARCHIVE_SYSTEM` | `rclone` or `none` | `none` |
| `RCLONE_DRIVE` | rclone remote name | |
| `RCLONE_PATH` | Path within the remote | |
| `RCLONE_FLAGS` | Extra rclone flags, separated by whitespace | |
| `ARCHIVE_SAVEDCLIPS` | Archive SavedClips | `true` |
| `ARCHIVE_SENTRYCLIPS` | Archive SentryClips | `true` |
| `ARCHIVE_RECENTCLIPS` | Archive the car's rolling buffer | `false` |
| `ARCHIVE_TRACKMODECLIPS` | Archive TrackMode clips | `true` |
| `ARCHIVE_PHOTOBOOTH` | Archive Photobooth files | `true` |
| `CAM_FILESYSTEM` | Filesystem for a new camera image: `fat32` or `ext4` | `fat32` |
| `MUTABLE_PATH` | Directory containing `backingfiles.img` | `/mutable` |
| `BACKINGFILES_PATH` | Mount point for the backing filesystem | `/backingfiles` |

Boolean values must be `true` or `false`. File settings take precedence over
environment variables. To install a service using another configuration file,
use `sudo teslausb --config /absolute/path/teslausb.conf service install --force`.

## Initialize

For a new installation, create the XFS backing image and camera disk:

```bash
sudo teslausb init --reserve 10G
```

Set `CAM_FILESYSTEM=ext4` in the configuration before initialization to use an
ext4 camera disk. Initialization never converts or reformats an existing image.
Archiving detects the filesystem stored in the image independently of this
setting. The XFS backing filesystem remains the same for either choice.

Ext4 journals metadata to support recovery after interrupted writes. It cannot
recover video that the car has not written or made durable. Camera images use
an explicit feature set without newer ext4 extensions such as metadata checksums,
fast commits, or orphan files; compatibility still requires validation with the
car's firmware.

This creates `/mutable/backingfiles.img`, `/backingfiles/cam_disk.bin`, and
`/backingfiles/snapshots/`. Sizing uses the free space available when initialized:

```text
backingfiles_size = available_disk - reserve
cam_size = (backingfiles_size - 3% XFS overhead) / 2
```

The remaining XFS space accommodates snapshot copy-on-write growth. Camera
images are aligned to 512-byte sectors.

## Run

Install the service, start it, and check the result:

```bash
sudo teslausb service install
sudo systemctl start teslausb
sudo systemctl is-active teslausb
sudo journalctl -u teslausb -n 100 --no-pager
```

The service starts at boot, checks dependencies, mounts the backing image, and
enables the USB gadget. It archives when the destination is reachable and
restarts after failures. Stopping the service turns off the USB gadget.

To run manually while the service is stopped:

```bash
sudo teslausb mount
sudo teslausb gadget on
sudo teslausb run
```

CPU temperature logs a caution above 70°C and a warning above 80°C. A missing or
unreadable sensor is reported as an error. The default sensor is
`/sys/class/thermal/thermal_zone0/temp`; set `TESLAUSB_THERMAL_PATH` in the process
environment or systemd unit when the board exposes its CPU sensor elsewhere.

The status LED blinks slowly while waiting, quickly while archiving, and uses
the heartbeat trigger during cleanup and after a successful cycle. Set
`TESLAUSB_LED_PATH` to the board's LED directory when its name is not recognized.
Missing LED capabilities and failed writes are reported as errors.

## Archive and Storage Behavior

Saved and Sentry event directories must remain unchanged across snapshots for
at least ten minutes before removal. Every file must have positive rclone
confirmation, and live files must still match the archived sizes and modification
times. Incomplete events and events containing only metadata stay on the camera
disk.

When enabled, RecentClips videos go into `RecentClips/YYYY-MM-DD/`; the known
`thumb.png` and `event.json` files go into `RecentClips/metadata/`. The car
manages the local RecentClips buffer. TeslaUSB retains those local files so
saving a recent event can still use them.

If an existing flat `RecentClips` archive has reached the provider's directory
limit, it cannot accept the new date subfolders. Stop the service and rename the
old archive folder to `RecentClips-before-YYYY-MM-DD`, using the migration date
and preserving its files. Verify that the destination name is unused before
moving it. Restart the service to create a new `RecentClips` tree beside the
preserved archive. TeslaCam Replay recognizes both the dated folders and
preserved archives with this name.

The camera filesystem and XFS backing filesystem have separate space
limits. Deleting snapshots frees XFS space. Removing confirmed saved events
frees camera filesystem space.

## Commands

Run device commands as root.

| Command | Description |
| --- | --- |
| `teslausb init [--reserve SIZE]` | Initialize new disk images |
| `teslausb deinit [-y]` | Permanently delete disk images |
| `teslausb mount` | Mount `backingfiles.img` |
| `teslausb run` | Run the archive loop |
| `teslausb archive` | Run one archive cycle |
| `teslausb status [--json]` | Show space, snapshots, and archive reachability |
| `teslausb snapshots [--json]` | List complete snapshots |
| `teslausb clean [--dry-run]` | Delete snapshots that are not in use |
| `teslausb gadget on/off/status` | Manage USB gadget mode |
| `teslausb service install/uninstall/status` | Manage the systemd service |
| `teslausb doctor [--startup]` | Check external dependencies |

## Troubleshooting

Check service logs, kernel errors, and both storage layers:

```bash
sudo teslausb status
sudo teslausb gadget status
sudo journalctl -u teslausb -n 100 --no-pager
sudo journalctl -k -n 100 --no-pager
df -h /mutable /backingfiles
```

For archive failures, test the configured remote with the root user's rclone
configuration. Cloud quota or directory limits must be resolved before those
uploads can succeed.

For XFS space occupied by unused snapshots, inspect before deleting:

```bash
sudo teslausb clean --dry-run
sudo teslausb clean
```

For filesystem errors or missing Saved/Sentry recordings, preserve the disk image and
inspect the logs before attempting repair. Keep the service stopped and the
USB gadget disabled during manual filesystem repair. Mounting the live camera
filesystem read-write while the car is connected can corrupt recordings.

If the car cannot see the drive, check the gadget status and
`/sys/class/udc/`, then verify the board's peripheral configuration.

## Development

```bash
cargo fmt --check
cargo test --locked
cargo clippy --locked -- -D warnings
cargo llvm-cov --locked --summary-only --fail-under-lines 85
cargo build --release --locked
scripts/run-linux-integration.sh
```

Install the coverage tool with `cargo install cargo-llvm-cov --locked`.
Snapshot and archive unit tests use `MockFileSystem`. Offline CLI tests use fake
Unix tools. Linux integration tests exercise real loop devices, XFS, FAT32, and ext4
on a privileged Linux host or VM; the integration script requires those
capabilities. The test fixtures use `TESLAUSB_LED_PATH`, `TESLAUSB_THERMAL_PATH`,
`TESLAUSB_PROC_PATH`, and `TESLAUSB_IDLE_TIMEOUT_SECS` for monitor inputs.

Filesystem failure experiments require a disposable Linux VM with root access,
loop devices, FAT32, ext4, Python 3, and the `dm-log-writes` kernel target. Build
`replay-log` from [log-writes](https://github.com/josefbacik/log-writes) at commit
`7b70d8a6863c5de30933d42a7672d35d01d2dc6c` and install it on `PATH`. Run:

```bash
export TESLAUSB_RUN_FILESYSTEM_FAILURES=1
export TESLAUSB_FAILURE_ARTIFACT_DIR=/var/tmp/teslausb-filesystem-artifacts
cargo test --locked --test filesystem_failures -- --ignored --test-threads=1
for promotion in rename copy; do
    python3 scripts/filesystem-replay.py --filesystem fat32 --promotion "$promotion"
    python3 scripts/filesystem-replay.py --filesystem ext4 --promotion "$promotion"
done
```

These experiments retain raw images, write logs, file hashes, and JSON reports.
They cover premature clip deletion, disk exhaustion, every recorded write prefix,
and first-sector-only persistence of multi-sector writes. File contents and both
affected directories are synced before each acknowledged save. Both rename-based
and copy-based promotions are tested. Zero-write,
fully-written, and acknowledged-save controls must preserve exact bytes.
Experiment completion means every cut was evaluated; `summary.json` records
losses, read errors, and rejected recovery separately. This model does not emulate
the car's firmware, USB cache behavior, or every possible storage failure.

## Safety Model

- A snapshot becomes complete when `snap.toc` is written after its data.
- Snapshot creation, recovery, acquisition, and deletion share a catalog lock.
- Snapshot handles keep an exclusive lock until their final release.
- Deletion keeps both locks, removes `snap.toc` first, then removes the data.
- Runtime recovery removes incomplete snapshots; status and dry-run inspection
  preserve them.
- Snapshot loops are read-only. Ext4 journals are replayed and independently
  checked on a private reflink copy before that copy is mounted read-only.
- A failed ext4 recovery retains `raw.bin` and `recovered.bin` in the `snapshots/recovery/`
  directory. Archiving stops until retained evidence is inspected and this directory
  is explicitly removed. Normal snapshot cleanup preserves it.
- Live camera cleanup disables the gadget, checks its filesystem, mounts the image,
  removes verified files, and unmounts it before reconnecting.

See [DESIGN.md](DESIGN.md) for architecture and [AGENTS.md](AGENTS.md) for coding
guidelines. [teslacam-replay](https://github.com/ben-z/teslacam-replay) displays
archived footage with synchronized camera views.

## License

MIT
