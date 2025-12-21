# Agent Guidelines

teslausb-ng is a Python rewrite of [TeslaUSB](https://github.com/marcone/teslausb). See [DESIGN.md](DESIGN.md) for architecture and design decisions.

## Code Style

- Python 3.9+, type hints everywhere
- Dataclasses for data, protocols for abstractions
- Each module has one clear purpose
- Tests use `MockFilesystem`, not real filesystem

## Grammar

In prose (comments, docstrings, error messages, documentation), use the verb form (two words). In identifiers (function names, command names, variables), the noun form (one word) is fine.

| Noun (one word) | Verb (two words) |
|-----------------|------------------|
| setup | set up |
| teardown | tear down |
| cleanup | clean up |
| startup | start up |
| shutdown | shut down |
| backup | back up |
| login | log in |
| logout | log out |

Examples:
- `gadget.setup()` - method name (noun form OK)
- `"Failed to set up gadget"` - error message (use verb form)
- `teslausb clean` - command uses verb form
- `"Clean up old snapshots"` - help text (use verb form)

CLI commands use verb forms: `init`, `run`, `clean`, `validate`, `enable`, `disable`, `remove`.

## Modules

| Module | Purpose |
|--------|---------|
| `coordinator.py` | Main orchestration loop |
| `snapshot.py` | Snapshot lifecycle with refcounting |
| `archive.py` | Archive via rclone |
| `space.py` | Disk space management |
| `filesystem.py` | Filesystem abstraction (protocol + real + mock) |
| `config.py` | Configuration from env/file |
| `gadget.py` | USB mass storage gadget |
| `mount.py` | Loop device mounting |
| `idle.py` | Detect when car stops writing |
| `led.py` | Status LED control |
| `temperature.py` | CPU temperature monitoring |
| `cli.py` | Command-line interface |

## Testing

```bash
pytest tests/ -v
```

## Common Tasks

**Adding a new archive backend:**
1. Implement `ArchiveBackend` in `archive.py`
2. Add creation logic in `cli.py:create_components()`
3. Add tests in `test_archive.py`

**Modifying snapshot behavior:**
1. Core logic in `snapshot.py`
2. `.toc` handling is critical for crash safety
3. Test with `MockFilesystem`

**Changing space management:**
1. Logic in `space.py`
2. Reserve is fixed at 10GB
3. Formula: `recommended_cam_size = (total - 10GB) / 2`
