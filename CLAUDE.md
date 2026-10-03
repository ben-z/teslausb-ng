# CLAUDE.md

This file provides guidance for AI assistants working with teslausb-ng.

teslausb-ng is a Rust CLI. It coordinates Linux system tools to expose a
Tesla camera disk over USB gadget mode, take XFS reflink snapshots, archive clips
with `rclone`, and clean up copied files conservatively.

## Quick Reference

```bash
cargo fmt
cargo test
cargo llvm-cov --locked --summary-only --fail-under-lines 85
cargo build --release
scripts/run-linux-integration.sh
```

## Project Shape

```text
src/
  main.rs
  cli.rs
  command.rs
  config.rs
  dependencies.rs
  filesystem.rs
  snapshot.rs
  archive.rs
  mount.rs
  gadget.rs
  idle.rs
  led.rs
  coordinator.rs
  space.rs
  temperature.rs
```

## Important Invariants

- `snap.toc` is the snapshot completion marker.
- No `snap.toc` means the snapshot is incomplete; explicit runtime recovery removes it.
- Delete `snap.toc` before deleting snapshot data.
- Disable the USB gadget before mounting `cam_disk.bin` read-write.
- Require ext4 camera partitions; reject other formats without repair or conversion.
- Re-enable the USB gadget only after checked unmount and successful filesystem verification.
- Require positive rclone confirmations and revalidate the whole Saved/Sentry event file set, sizes, and modification times before deletion.
- Require five consecutive samples with no USB `wchar` writes before snapshots and cleanup; skip the cycle on timeout.
- Preserve the car-owned RecentClips buffer and metadata-only Saved/Sentry events.
- Require Saved/Sentry event stability across snapshots for the configured grace period before cleanup.
- Surface missing or failing LED and temperature capabilities as errors.
- Keep external tools external; this binary coordinates them.

## Testing Guidance

Use `MockFileSystem` for snapshot and archive behavior tests.
`tests/offline_cli.rs` validates the compiled binary against fake Unix tools for
offline coverage of init, mount, `run`, archive, clean, status, doctor, and
service flows. `tests/linux_integration.rs` validates real loop-device, XFS,
ext4, mount, reflink, archive cleanup, and `run` signal handling through
`scripts/run-linux-integration.sh` on a privileged Linux host, VM, or QEMU guest.
Add hardware-specific tests for configfs and USB gadget behavior.

## Deployment

The deployment artifact is `target/release/teslausb`.

```bash
sudo install -m 0755 target/release/teslausb /usr/local/bin/teslausb
sudo teslausb service install --force
```
