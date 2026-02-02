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

1. [Connect to WiFi](#wifi-setup)
2. [Install teslausb-ng](#installation)
3. [Configure rclone](#rclone-configuration)
4. [Create config file](#configuration)
5. [Initialize and start](#running)

---

## WiFi Setup

Configure WiFi using NetworkManager:

```bash
# List available networks
sudo nmcli device wifi list

# Connect to a network
sudo nmcli device wifi connect "YourNetworkName" password "YourPassword"

# Verify connection
nmcli connection show
```

Alternatively, configure WiFi using netplan:

```sh
# List available networks
iw wlan0 scan

# Add networks to netplan settings
cat > /etc/netplan/20-wifi.yaml << 'EOF'
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
EOF

# Apply the configuration
netplan apply

# Show wifi status
iw wlan0 link
# or
networkctl status wlan0
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
sudo apt install -y python3-pip rclone xfsprogs parted dosfstools kpartx

# Install teslausb-ng
pip install git+https://github.com/ben-z/teslausb-ng.git
```

### Updating

To update to the latest version:

```bash
pip install --force-reinstall git+https://github.com/ben-z/teslausb-ng.git

# If running as a service, reinstall it to pick up any service file changes
sudo teslausb service uninstall
sudo teslausb service install
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

2. Add this line:
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

**Note:** The first boot after enabling the overlay may fail with "gave up waiting for root file system device". Simply reboot again and it should work. This is a one-time timing issue during initial overlay application.

**Important:** Once peripheral mode is enabled, the USB-C port used for gadget mode will **only** work as a device port (connecting to Tesla). It will no longer work as a USB host port. Ensure you have another way to connect peripherals if needed.

### Raspberry Pi

Raspberry Pi boards typically work out of the box with the `dwc2` overlay. If you encounter issues, ensure your `/boot/config.txt` contains:

```
dtoverlay=dwc2
```

And `/boot/cmdline.txt` includes `modules-load=dwc2` after `rootwait`.

---

## rclone Configuration

Configure [rclone](https://rclone.org/) with your cloud provider:

```bash
rclone config
```

For headless setup, rclone provides a URL to authorize on another device.

---

## Configuration

Create `/etc/teslausb.conf`:

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
backingfiles - 2 GiB (XFS overhead) = usable space
usable space / 2 = cam_size
```

For example, on a 128 GiB SD card with 10 GiB reserved:
- backingfiles.img = 118 GiB
- cam_size = 58 GiB (half for cam disk, half for snapshots)

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

3. If space is consistently low, reinitialize with a larger reserve to leave more room for the OS (this reduces the cam disk size):
   ```bash
   sudo teslausb deinit
   sudo teslausb init --reserve 20G
   ```

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
