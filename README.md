# teslausb-ng

A Python rewrite of [TeslaUSB](https://github.com/marcone/teslausb)'s dashcam archiving system.

## Features

- **On-demand snapshots**: Only when WiFi is available (no disk-full errors)
- **Reference counting**: Prevents race conditions between archiving and cleanup
- **Crash-safe**: Uses `.toc` file as single source of truth
- **rclone support**: Archive to 40+ cloud providers
- **LED status indicators**: Visual feedback during operation
- **Temperature monitoring**: Protects against overheating in hot vehicles

## Requirements

- Single-board computer with USB OTG support (Raspberry Pi, Rock Pi, etc.)
- Linux with USB gadget support (dwc2 or similar)
- rclone configured with your cloud provider

## Quick Start

1. [Connect to WiFi](#set-up-wifi)
2. [Install teslausb-ng](#installation)
3. [Configure rclone](#rclone-configuration)
4. [Create config file](#configuration)
5. [Initialize and start](#running)

---

## Set Up WiFi

Configure WiFi using NetworkManager:

```bash
# List available networks
sudo nmcli device wifi list

# Connect to a network
sudo nmcli device wifi connect "YourNetworkName" password "YourPassword"

# Verify connection
nmcli connection show
```

### Multiple Networks

You can save multiple WiFi networks (home, work, mobile hotspot):

```bash
sudo nmcli device wifi connect "WorkWiFi" password "WorkPassword"
sudo nmcli device wifi connect "iPhone" password "HotspotPassword"
```

The device will automatically connect to whichever saved network is available.

---

## Installation

```bash
# Install system dependencies
sudo apt update
sudo apt install -y python3-pip rclone xfsprogs parted dosfstools

# Install teslausb-ng
pip install git+https://github.com/ben-z/teslausb-ng.git
```

---

## rclone Configuration

Configure [rclone](https://rclone.org/) with your cloud provider:

```bash
rclone config
```

When running headless, rclone provides a URL you can open on another device to authorize.

---

## Configuration

Create `/etc/teslausb.conf`:

```bash
CAM_SIZE=40G
ARCHIVE_SYSTEM=rclone
RCLONE_DRIVE=gdrive
RCLONE_PATH=/TeslaCam
```

Or export environment variables directly.

| Variable | Description | Default |
|----------|-------------|---------|
| `CAM_SIZE` | Camera disk size | `40G` |
| `ARCHIVE_SYSTEM` | `rclone` or `none` | `none` |
| `RCLONE_DRIVE` | rclone remote name | |
| `RCLONE_PATH` | Path within remote | |
| `ARCHIVE_SAVEDCLIPS` | Archive SavedClips | `true` |
| `ARCHIVE_SENTRYCLIPS` | Archive SentryClips | `true` |
| `ARCHIVE_RECENTCLIPS` | Archive RecentClips (rolling buffer) | `false` |
| `ARCHIVE_TRACKMODECLIPS` | Archive TrackMode clips | `true` |

---

## Running

### Initialize

First, create the disk images and directory structure:

```bash
sudo teslausb init
```

This creates:
- `/mutable/backingfiles.img` - XFS disk image (for reflink snapshots)
- `/backingfiles/cam_disk.bin` - FAT32 disk image (presented to Tesla via USB)

### Install as a Service (Recommended)

```bash
sudo teslausb service install
sudo systemctl start teslausb
```

The service will:
- Start automatically on boot
- Enable the USB gadget before running
- Archive footage whenever WiFi is available
- Restart automatically if it crashes

### Manual Running

```bash
# Mount /backingfiles
sudo teslausb mount
# Enable USB gadget
sudo teslausb gadget on

# Run the archiver
sudo teslausb run
```

---

## Commands

| Command | Description |
|---------|-------------|
| `teslausb init` | Initialize disk images and directories |
| `teslausb deinit` | Remove disk images and clean up |
| `teslausb run` | Main loop: wait for WiFi, snapshot, archive, repeat |
| `teslausb archive` | Single archive cycle |
| `teslausb status` | Show status (space, snapshots, config warnings) |
| `teslausb snapshots` | List snapshots |
| `teslausb clean` | Clean up old snapshots |
| `teslausb gadget on` | Initialize and enable USB gadget |
| `teslausb gadget off` | Disable and remove USB gadget |
| `teslausb gadget status` | Show USB gadget status |
| `teslausb service install` | Install systemd service |
| `teslausb service uninstall` | Remove systemd service |
| `teslausb service status` | Show service status |

---

## Tailscale (Optional)

[Tailscale](https://tailscale.com/) provides secure remote access for debugging and monitoring.

---

## LED Status Indicators

If your board has a controllable status LED, teslausb uses it to show current state:

| Pattern | Meaning |
|---------|---------|
| Slow blink (0.9s off, 0.1s on) | Waiting for WiFi/archive |
| Fast blink (150ms off, 50ms on) | Archiving in progress |
| Off | Service stopped or idle |

---

## Temperature Monitoring

teslausb monitors CPU temperature and logs warnings when thresholds are exceeded:

- **Caution** at 70°C
- **Warning** at 80°C

Warnings appear in the service logs:

```bash
sudo journalctl -u teslausb -f
```

---

## Troubleshooting

### Tesla doesn't see the USB drive

1. Verify USB gadget is enabled:
   ```bash
   teslausb gadget status
   ```

2. Try reinitializing:
   ```bash
   sudo teslausb gadget off
   sudo teslausb gadget on
   ```

### Archive not working

1. Check WiFi connectivity:
   ```bash
   ping -c 3 google.com
   ```

2. Test rclone configuration:
   ```bash
   rclone lsd gdrive:
   ```

3. Check service status:
   ```bash
   sudo systemctl status teslausb
   sudo journalctl -u teslausb -f
   ```

### Disk full errors

1. Check space status:
   ```bash
   teslausb status
   ```

2. Manually clean old snapshots:
   ```bash
   teslausb clean --dry-run  # See what would be deleted
   teslausb clean            # Actually delete
   ```

3. If space is consistently low, reduce `CAM_SIZE` in your config to leave more room for snapshots.

### Service won't start

1. Check for configuration errors:
   ```bash
   teslausb status
   ```

2. Verify config file syntax:
   ```bash
   cat /etc/teslausb.conf
   ```

3. Check logs for specific errors:
   ```bash
   sudo journalctl -u teslausb --no-pager | tail -50
   ```

---

## Documentation

- [DESIGN.md](DESIGN.md) - Architecture and design decisions
- [AGENTS.md](AGENTS.md) - Guidelines for AI assistants

## Development

```bash
git clone https://github.com/ben-z/teslausb-ng.git
cd teslausb-ng
pip install -e .
pytest tests/ -v
```

## License

MIT
