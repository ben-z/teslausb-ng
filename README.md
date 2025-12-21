# teslausb-ng

A Python rewrite of [TeslaUSB](https://github.com/marcone/teslausb)'s dashcam archiving system.

## Features

- **On-demand snapshots**: Only when WiFi is available (no disk-full errors)
- **Reference counting**: Prevents race conditions between archiving and cleanup
- **Crash-safe**: Uses `.toc` file as single source of truth
- **rclone support**: Archive to 40+ cloud providers

## Requirements

- Raspberry Pi (Zero 2 W, 3, 4, or 5) with USB OTG support
- Raspberry Pi OS Lite (64-bit recommended)
- rclone configured with your cloud provider

## Installation

```bash
# Install system dependencies
sudo apt update
sudo apt install -y python3-pip rclone xfsprogs parted dosfstools

# Configure rclone (follow prompts)
rclone config

# Install teslausb-ng
pip install git+https://github.com/ben-z/teslausb-ng.git
```

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

## Running

### Initialize

First, create the disk images and directory structure:

```bash
sudo teslausb init
```

This creates:
- `/mutable/backingfiles.img` - XFS disk image (for reflink snapshots)
- `/backingfiles/cam_disk.bin` - FAT32 disk image (presented to Tesla via USB)

### USB Gadget

Set up the USB mass storage gadget so the Tesla sees the Pi as a USB drive:

```bash
teslausb gadget init --enable
```

This binds the disk image to the USB controller.

### Manual

```bash
teslausb run
```

### As a systemd service

```bash
sudo tee /etc/systemd/system/teslausb.service << EOF
[Unit]
Description=TeslaUSB Archiver
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStartPre=/usr/local/bin/teslausb mount
ExecStartPre=/usr/local/bin/teslausb gadget init --enable
ExecStart=/usr/local/bin/teslausb run
ExecStopPost=/usr/local/bin/teslausb gadget remove
EnvironmentFile=/etc/teslausb.conf
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl enable teslausb
sudo systemctl start teslausb
```

## Commands

| Command | Description |
|---------|-------------|
| `teslausb init` | Initialize disk images and directories |
| `teslausb mount` | Mount the backingfiles image |
| `teslausb run` | Main loop: wait for WiFi, snapshot, archive, repeat |
| `teslausb archive` | Single archive cycle |
| `teslausb status` | Show space and snapshot info |
| `teslausb snapshots` | List snapshots |
| `teslausb clean` | Clean up old snapshots |
| `teslausb validate` | Check configuration |
| `teslausb gadget` | Manage USB mass storage |

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
