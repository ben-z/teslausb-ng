# teslausb-ng

A Python rewrite of [TeslaUSB](https://github.com/marcone/teslausb)'s dashcam archiving system.

## Features

- **On-demand snapshots**: Archive when the destination is reachable
- **Reference counting**: Prevents race conditions between archiving and cleanup
- **Crash-safe**: Uses `.toc` file as single source of truth
- **rclone support**: Archive to 40+ cloud providers
- **LED status indicators**: Visual feedback during operation
- **Temperature monitoring**: Warns when the CPU is hot

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

---

## Installation

```bash
# Install system dependencies
sudo apt update
sudo apt install -y git python3-venv rclone xfsprogs parted dosfstools kpartx

# Install teslausb-ng
sudo python3 -m venv /opt/teslausb
sudo /opt/teslausb/bin/pip install git+https://github.com/ben-z/teslausb-ng.git
sudo ln -s /opt/teslausb/bin/teslausb /usr/local/bin/teslausb
```

### Updating

To update to the latest version:

```bash
sudo systemctl stop teslausb
sudo /opt/teslausb/bin/pip install --force-reinstall git+https://github.com/ben-z/teslausb-ng.git

# If running as a service, reinstall it to pick up any service file changes
sudo teslausb service install --force
sudo systemctl start teslausb
```

---

## Board-Specific Setup

### Rock 5C (RK3588S)

The Rock 5C requires a device tree overlay to enable USB gadget mode. Without this, you'll see `No USB Device Controller found` when running `teslausb gadget on`.

**Enable the USB peripheral overlay:**

1. Edit `/boot/armbianEnv.txt`:
   ```bash
   sudo nano /boot/armbianEnv.txt
   ```

2. Add `rk3588-dwc3-peripheral` to the existing `overlays` line, preserving other
   entries. If there is no `overlays` line, add:
   ```
   overlays=rk3588-dwc3-peripheral
   ```

3. Reboot:
   ```bash
   sudo reboot
   ```

4. Verify the UDC is available:
   ```bash
   ls /sys/class/udc/
   # Should show: fc000000.usb
   ```

**Important:** Once peripheral mode is enabled, the USB-C port used for gadget mode will **only** work as a device port (connecting to Tesla). It will no longer work as a USB host port. Ensure you have another way to connect peripherals if needed.

### Raspberry Pi

Raspberry Pi boards typically work out of the box with the `dwc2` overlay. If you encounter issues, ensure your `/boot/config.txt` contains:

```
dtoverlay=dwc2
```

And `/boot/cmdline.txt` includes `modules-load=dwc2` after `rootwait`.

---

## rclone Configuration

The service runs as root. Configure [rclone](https://rclone.org/) as root so the
service can access its remote:

```bash
sudo rclone config
```

For a provider that needs browser authorization on a headless device, follow
[rclone's headless instructions](https://rclone.org/remote_setup/). Answer `n`
when asked to authenticate with a local browser, then run the displayed
`rclone authorize` command on a computer with a browser.

---

## Configuration

Create `/etc/teslausb.conf` with `sudoedit`:

```bash
ARCHIVE_SYSTEM=rclone
RCLONE_DRIVE=gdrive
RCLONE_PATH=/TeslaCam
```

Or export environment variables directly.

| Variable | Description | Default |
|----------|-------------|---------|
| `ARCHIVE_SYSTEM` | `rclone` or `none` | `none` |
| `RCLONE_DRIVE` | rclone remote name | |
| `RCLONE_PATH` | Path within remote | |
| `ARCHIVE_SAVEDCLIPS` | Archive SavedClips | `true` |
| `ARCHIVE_SENTRYCLIPS` | Archive SentryClips | `true` |
| `ARCHIVE_RECENTCLIPS` | Archive RecentClips (rolling buffer) | `false` |
| `ARCHIVE_TRACKMODECLIPS` | Archive TrackMode clips | `true` |
| `ARCHIVE_PHOTOBOOTH` | Archive Photobooth selfies | `true` |

---

## Running

### Initialize

First, create the disk images and directory structure:

```bash
sudo teslausb init
```

You'll be prompted to specify how much space to reserve for the OS (default: 10 GiB).
Alternatively, use the `--reserve` flag:

```bash
sudo teslausb init --reserve 10G
```

The cam disk size is **automatically calculated** from available disk space:

```
available_disk - reserve = backingfiles size
backingfiles - 3% (XFS overhead) = usable space
usable space / 2 = cam_size
```

For example, with 128 GiB of free disk space and 10 GiB reserved:
- backingfiles.img = 118 GiB
- cam_size = approximately 57.2 GiB (half for cam disk, half for snapshots)

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
| `teslausb init [--reserve SIZE]` | Initialize disk images and directories |
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

### No USB Device Controller found

If you see this error when running `teslausb gadget on`:

```
Failed to enable gadget: No USB Device Controller found
```

This means the USB gadget driver isn't loaded or the device tree isn't configured for peripheral/OTG mode.

**Diagnose:**

```bash
# Check if any UDC exists
ls /sys/class/udc/

# Check current USB mode (if available)
cat /sys/firmware/devicetree/base/usbdrd3_0/usb@fc000000/dr_mode 2>/dev/null

# Check loaded USB modules
lsmod | grep -E 'dwc|gadget'
```

**Solutions by board:**

- **Rock 5C / RK3588**: See [Board-Specific Setup](#rock-5c-rk3588s) - requires `overlays=rk3588-dwc3-peripheral` in `/boot/armbianEnv.txt`
- **Raspberry Pi**: Ensure `dtoverlay=dwc2` is in `/boot/config.txt`
- **Other boards**: Check your board's documentation for enabling USB gadget/OTG mode. You may need to enable a device tree overlay or load kernel modules.

**After making changes**, reboot and verify:

```bash
ls /sys/class/udc/  # Should show a device like fc000000.usb or musb-hdrc.0
sudo teslausb gadget on
```

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
   sudo rclone lsd gdrive:
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
   sudo teslausb clean --dry-run  # See what would be deleted
   sudo teslausb clean            # Actually delete
   ```

3. Check the archive logs for failed uploads or camera filesystem errors:
   ```bash
   sudo journalctl -u teslausb -n 100 --no-pager
   sudo journalctl -k -n 100 --no-pager
   df -h /mutable /backingfiles
   ```

The camera's FAT32 filesystem and its XFS backing filesystem have separate space
limits. Deleting snapshots frees XFS space; the archive cycle removes confirmed
uploads from the camera filesystem. `deinit` permanently deletes all local
recordings, so preserve footage before rebuilding the disk images.

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

## Related Projects

- [teslacam-replay](https://github.com/ben-z/teslacam-replay) — Web viewer for archived footage with synchronized multi-camera playback

## Documentation

- [DESIGN.md](DESIGN.md) - Architecture and design decisions
- [AGENTS.md](AGENTS.md) - Guidelines for AI assistants

## Development

```bash
git clone https://github.com/ben-z/teslausb-ng.git
cd teslausb-ng
python3 -m venv .venv
. .venv/bin/activate
pip install -e '.[test]'
pytest tests/ -v
```

## License

MIT
