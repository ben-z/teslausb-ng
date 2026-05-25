# Agent Guidelines

teslausb-ng is a Rust implementation of TeslaUSB-style dashcam archiving. See
[DESIGN.md](DESIGN.md) for architecture and safety invariants.

## Code Style

- Rust 2021
- Prefer standard library APIs and small local helpers
- Keep modules focused on one purpose
- Use `Result<T>` with contextual errors instead of panics in production paths
- Use RAII guards for resources that must be cleaned up
- Tests that touch snapshot/archive behavior should use `MockFileSystem`, not the
  real filesystem

## Grammar

In prose (comments, error messages, documentation), use the verb form (two words).
In identifiers (function names, command names, variables), the noun form (one word)
is fine.

| Noun (one word) | Verb (two words) |
| --- | --- |
| setup | set up |
| teardown | tear down |
| cleanup | clean up |
| startup | start up |
| shutdown | shut down |
| backup | back up |
| login | log in |
| logout | log out |

Examples:

- `cleanup_empty_dirs` - function name (noun form OK)
- `"failed to set up loop device"` - error message (use verb form)
- `teslausb clean` - command name
- `"Clean up old snapshots"` - help text

## Modules

| Module | Purpose |
| --- | --- |
| `cli.rs` | Command-line interface and systemd service management |
| `command.rs` | Subprocess execution with timeout handling |
| `config.rs` | Configuration from env/file |
| `dependencies.rs` | Dependency and version preflight checks |
| `filesystem.rs` | Filesystem abstraction and mock |
| `snapshot.rs` | Snapshot lifecycle with refcounting |
| `archive.rs` | Archive via rclone and verified deletion |
| `mount.rs` | Loop device and mount guards |
| `gadget.rs` | USB mass storage gadget |
| `idle.rs` | USB write-idle detection |
| `led.rs` | Sysfs status LED control |
| `temperature.rs` | Sysfs CPU temperature monitoring |
| `coordinator.rs` | Main archive loop |
| `space.rs` | Disk space management |

## Testing

```bash
cargo fmt
cargo test
cargo llvm-cov --locked --summary-only --fail-under-lines 85
scripts/run-linux-integration.sh
```

`cargo test` includes offline CLI integration tests with fake Unix tools. Keep
those tests current when changing deployment, service, mount, `run`, or archive
flows. `scripts/run-linux-integration.sh` runs ignored Linux tests that use real
loop devices, XFS, FAT32, and mounts; use a privileged Linux host, VM, or QEMU
guest.

## Common Tasks

**Adding a new archive backend:**

1. Extend `ArchiveBackend` in `archive.rs`
2. Add construction logic in `cli.rs:create_components`
3. Add focused unit tests with `MockFileSystem`

**Modifying snapshot behavior:**

1. Update `snapshot.rs`
2. Preserve `.toc` ordering; it is critical for crash safety
3. Test with `MockFileSystem`

**Changing space management:**

1. Update `space.rs`
2. Preserve 512-byte sector alignment
3. Formula: `cam_size = (backingfiles_size - 3% overhead) / 2`
