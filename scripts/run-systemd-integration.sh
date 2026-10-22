#!/usr/bin/env sh
set -eu

if [ "$(uname -s)" != Linux ]; then
    echo 'Systemd integration tests require Linux and a running systemd manager.' >&2
    exit 1
fi

for command in cargo systemctl journalctl id dirname cat sleep; do
    if ! command -v "$command" >/dev/null 2>&1; then
        echo "Missing systemd integration dependency: $command" >&2
        exit 1
    fi
done

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/teslausb-systemd-integration-target}"
if [ "$(id -u)" != 0 ]; then
    exec sudo -E env PATH="$PATH" \
        CARGO_TARGET_DIR="$CARGO_TARGET_DIR" \
        "$0" "$@"
fi

if [ ! -d /run/systemd/system ]; then
    echo 'Systemd integration tests require a running systemd manager.' >&2
    exit 1
fi
systemctl show --property=Version --value >/dev/null

cargo test --locked --test systemd_integration -- --ignored --test-threads=1 --nocapture "$@"
