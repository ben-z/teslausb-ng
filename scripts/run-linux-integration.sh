#!/usr/bin/env sh
set -eu

if [ "$(uname -s)" != "Linux" ]; then
    echo "Linux integration tests require Linux; run this inside a Linux host, VM, or QEMU guest." >&2
    exit 0
fi

missing=''
for command in blockdev cp df fsck kpartx losetup mkfs.vfat mkfs.xfs modprobe mount mountpoint parted stat sync truncate umount; do
    if ! command -v "$command" >/dev/null 2>&1; then
        missing="$missing $command"
    fi
done

if [ -n "$missing" ]; then
    echo "Missing Linux integration dependencies:$missing" >&2
    echo "On Debian/Ubuntu: sudo apt-get install -y xfsprogs dosfstools kpartx parted util-linux" >&2
    exit 1
fi

if [ "$(id -u)" != "0" ]; then
    if command -v sudo >/dev/null 2>&1; then
        exec sudo -E env \
            PATH="$PATH" \
            CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/teslausb-linux-integration-target}" \
            TESLAUSB_RUN_LINUX_INTEGRATION=1 \
            "$0" "$@"
    fi
    echo "Linux integration tests require root for losetup and mount." >&2
    exit 1
fi

export TESLAUSB_RUN_LINUX_INTEGRATION=1
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/teslausb-linux-integration-target}"

cargo test --locked --test linux_integration -- --ignored --test-threads=1 --nocapture "$@"
