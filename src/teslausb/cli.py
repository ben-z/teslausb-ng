"""Command-line interface for TeslaUSB.

Usage:
    teslausb init          # Create disk images and directory structure
    teslausb deinit        # Remove disk images and clean up
    teslausb run           # Run the main coordinator loop
    teslausb archive       # Run a single archive cycle
    teslausb status        # Show status (space, snapshots, config)
    teslausb snapshots     # List snapshots
    teslausb clean         # Clean up old snapshots
    teslausb gadget        # Manage USB mass storage gadget
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import time
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from .archive import ArchiveManager, MockArchiveBackend, RcloneBackend
from .config import Config, ConfigError, load_from_env, load_from_file
from .coordinator import Coordinator, CoordinatorConfig
from .filesystem import RealFilesystem
from .mount import mount_image
from .gadget import GadgetError, LunConfig, UsbGadget
from .snapshot import SnapshotManager
from .space import SpaceManager, GB

logger = logging.getLogger(__name__)

# ANSI escape codes for dim text
DIM = "\033[2m"
RESET = "\033[0m"


def _run_cmd(cmd: list[str], capture_stdout: bool = False) -> subprocess.CompletedProcess:
    """Run a command with stderr output shown in dim text.

    Args:
        cmd: Command and arguments to run
        capture_stdout: If True, capture stdout for parsing; otherwise pass through

    Returns:
        CompletedProcess result
    """
    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE if capture_stdout else None,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.stderr:
        for line in result.stderr.decode().splitlines():
            print(f"{DIM}    {cmd[0]}: {line}{RESET}", file=sys.stderr)
    return result


def _get_version() -> str:
    """Get package version, with fallback for development."""
    try:
        return version("teslausb")
    except PackageNotFoundError:
        return "dev"


def configure_logging(verbose: bool = False, debug: bool = False) -> None:
    """Configure logging."""
    if debug:
        level = logging.DEBUG
    elif verbose:
        level = logging.INFO
    else:
        level = logging.WARNING

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def load_config(args: argparse.Namespace) -> Config:
    """Load configuration from file or environment."""
    if args.config:
        return load_from_file(Path(args.config))
    else:
        return load_from_env()


def create_components(config: Config) -> tuple[
    RealFilesystem, SnapshotManager, SpaceManager, ArchiveManager, MockArchiveBackend | RcloneBackend
]:
    """Create all components from configuration."""
    fs = RealFilesystem()

    snapshot_manager = SnapshotManager(
        fs=fs,
        cam_disk_path=config.cam_disk_path,
        snapshots_path=config.snapshots_path,
    )

    space_manager = SpaceManager(
        fs=fs,
        snapshot_manager=snapshot_manager,
        backingfiles_path=config.backingfiles_path,
        cam_size=config.cam_size,
        reserve=config.reserve,
    )

    # Create backend based on archive system
    if config.archive.system == "rclone":
        backend = RcloneBackend(
            remote=config.archive.rclone_drive,
            path=config.archive.rclone_path,
            flags=config.archive.rclone_flags,
        )
    else:
        # Default to mock for testing
        backend = MockArchiveBackend(reachable=True)

    archive_manager = ArchiveManager(
        fs=fs,
        snapshot_manager=snapshot_manager,
        backend=backend,
        cam_disk_path=config.cam_disk_path,
        archive_recent=config.archive.archive_recent,
        archive_saved=config.archive.archive_saved,
        archive_sentry=config.archive.archive_sentry,
        archive_track=config.archive.archive_track,
    )

    return fs, snapshot_manager, space_manager, archive_manager, backend


def _is_mounted(path: Path) -> bool:
    """Check if a path is a mount point."""
    result = _run_cmd(["mountpoint", "-q", str(path)])
    return result.returncode == 0


def _get_fstype(path: Path) -> str | None:
    """Get filesystem type of a mounted path."""
    result = _run_cmd(["stat", "-f", "-c", "%T", str(path)], capture_stdout=True)
    if result.returncode == 0:
        return result.stdout.decode().strip()
    return None


def _create_backingfiles_image(image_path: Path, size: int) -> bool:
    """Create and format an XFS disk image for backingfiles."""
    print(f"  Creating {size / GB:.1f} GiB XFS image at {image_path}...")

    result = _run_cmd(["truncate", "-s", str(size), str(image_path)])
    if result.returncode != 0:
        print(f"  Failed to create image file")
        return False

    result = _run_cmd(["mkfs.xfs", "-f", str(image_path)])
    if result.returncode != 0:
        print(f"  Failed to format as XFS (is xfsprogs installed?)")
        return False

    return True


def _mount_backingfiles(image_path: Path, mount_path: Path) -> bool:
    """Mount the backingfiles image."""
    mount_path.mkdir(parents=True, exist_ok=True)

    if _is_mounted(mount_path):
        return True

    print(f"  Mounting {image_path} at {mount_path}...")
    result = _run_cmd(["mount", "-o", "loop", str(image_path), str(mount_path)])
    if result.returncode != 0:
        print(f"  Failed to mount image")
        return False

    return True


def _ensure_mounted(config: Config) -> bool:
    """Ensure backingfiles image is mounted.

    Returns:
        True if mounted successfully, False on error.
    """
    backingfiles_img = config.mutable_path / "backingfiles.img"

    if not backingfiles_img.exists():
        print(f"Error: {backingfiles_img} does not exist")
        print(f"Run 'teslausb init' first to create the backingfiles image")
        return False

    if not _mount_backingfiles(backingfiles_img, config.backingfiles_path):
        return False

    # Verify it's XFS (required for reflinks)
    fstype = _get_fstype(config.backingfiles_path)
    if fstype != "xfs":
        print(f"Error: {config.backingfiles_path} is {fstype}, not xfs")
        return False

    return True


def _create_cam_disk(cam_disk_path: Path, cam_size: int) -> bool:
    """Create the FAT32 cam disk image."""
    print(f"  Creating {cam_size / GB:.1f} GiB cam disk (sparse)...")

    result = _run_cmd(["truncate", "-s", str(cam_size), str(cam_disk_path)])
    if result.returncode != 0:
        print(f"  Failed to create disk image")
        return False

    print(f"  Creating partition table...")
    result = _run_cmd(["parted", "-s", str(cam_disk_path), "mklabel", "msdos"])
    if result.returncode != 0:
        print(f"  Failed to create partition table")
        return False

    result = _run_cmd(["parted", "-s", str(cam_disk_path), "mkpart", "primary", "fat32", "0%", "100%"])
    if result.returncode != 0:
        print(f"  Failed to create partition")
        return False

    print(f"  Formatting cam disk as FAT32...")
    loop_dev = None
    try:
        result = _run_cmd(["losetup", "-Pf", "--show", str(cam_disk_path)], capture_stdout=True)
        if result.returncode != 0:
            print(f"  Failed to create loop device")
            return False
        loop_dev = result.stdout.decode().strip()
        partition = f"{loop_dev}p1"

        # Wait for partition device to appear
        for _ in range(20):
            if Path(partition).exists():
                break
            time.sleep(0.1)
        else:
            print(f"  Partition device {partition} not found")
            return False

        result = _run_cmd(["mkfs.vfat", "-F", "32", "-n", "TESLAUSB", partition])
        if result.returncode != 0:
            print(f"  Failed to format partition")
            return False

        # Create TeslaCam directory structure
        mount_point = Path("/tmp/teslausb-setup-mount")
        mount_point.mkdir(exist_ok=True)

        result = _run_cmd(["mount", partition, str(mount_point)])
        if result.returncode != 0:
            print(f"  Failed to mount partition")
            return False

        try:
            (mount_point / "TeslaCam").mkdir()
            print(f"  Created TeslaCam directory")
        finally:
            _run_cmd(["umount", str(mount_point)])
            try:
                mount_point.rmdir()
            except OSError:
                pass

        return True

    finally:
        if loop_dev:
            _run_cmd(["losetup", "-d", loop_dev])


def cmd_init(args: argparse.Namespace) -> int:
    """Initialize TeslaUSB disk images and directory structure."""
    config = load_config(args)
    backingfiles_img = config.mutable_path / "backingfiles.img"

    if backingfiles_img.exists():
        print(f"Error: {backingfiles_img} already exists")
        print(f"Run 'teslausb deinit' to remove it first")
        return 1

    print(f"Initializing TeslaUSB...")
    print(f"  Cam disk size: {config.cam_size / GB:.1f} GiB")

    # Create XFS backingfiles image
    # size: cam_disk + one full snapshot + reserve
    backingfiles_size = config.cam_size * 2 + config.reserve
    config.mutable_path.mkdir(parents=True, exist_ok=True)
    if not _create_backingfiles_image(backingfiles_img, backingfiles_size):
        return 1

    # Mount backingfiles
    if not _mount_backingfiles(backingfiles_img, config.backingfiles_path):
        return 1

    # Verify it's XFS (required for reflinks)
    fstype = _get_fstype(config.backingfiles_path)
    if fstype != "xfs":
        print(f"  Error: {config.backingfiles_path} is {fstype}, not xfs")
        print(f"  Reflinks require XFS. Delete {backingfiles_img} and re-run init.")
        return 1

    # Create snapshots directory and cam disk
    config.snapshots_path.mkdir(parents=True, exist_ok=True)

    if not _create_cam_disk(config.cam_disk_path, config.cam_size):
        return 1

    print(f"\nInitialization complete!")
    print(f"  Backingfiles image: {backingfiles_img}")
    print(f"  Cam disk: {config.cam_disk_path}")
    print(f"\nNext steps:")
    print(f"  1. Configure archiving in /etc/teslausb.conf")
    print(f"  2. Enable USB gadget: teslausb gadget on")
    print(f"  3. Start archiving: teslausb run")

    return 0


def cmd_deinit(args: argparse.Namespace) -> int:
    """Remove TeslaUSB disk images and clean up."""
    config = load_config(args)
    backingfiles_img = config.mutable_path / "backingfiles.img"

    if not backingfiles_img.exists():
        print(f"Nothing to do: {backingfiles_img} does not exist")
        return 0

    # Confirm unless --yes flag is provided
    if not args.yes:
        print(f"This will permanently delete:")
        print(f"  {backingfiles_img}")
        print(f"  All snapshots and cam disk data")
        print()
        response = input("Are you sure? [y/N] ")
        if response.lower() not in ("y", "yes"):
            print("Aborted")
            return 1

    print(f"Deinitializing TeslaUSB...")

    # Unmount backingfiles if mounted
    if _is_mounted(config.backingfiles_path):
        print(f"  Unmounting {config.backingfiles_path}...")
        result = _run_cmd(["umount", str(config.backingfiles_path)])
        if result.returncode != 0:
            print(f"  Failed to unmount {config.backingfiles_path}")
            print(f"  Make sure no processes are using files in {config.backingfiles_path}")
            return 1

    # Remove backingfiles image
    print(f"  Removing {backingfiles_img}...")
    backingfiles_img.unlink()

    # Remove mount directory if empty
    if config.backingfiles_path.exists():
        try:
            config.backingfiles_path.rmdir()
            print(f"  Removed {config.backingfiles_path}")
        except OSError:
            # Directory not empty or other issue, that's fine
            pass

    print(f"Deinitialization complete")
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    """Run the main coordinator loop."""
    config = load_config(args)

    # Auto-mount backingfiles
    if not _ensure_mounted(config):
        return 1

    warnings = config.validate()
    for warning in warnings:
        logger.warning(f"Config warning: {warning}")

    fs, snapshot_manager, space_manager, archive_manager, backend = create_components(config)

    coordinator = Coordinator(
        fs=fs,
        snapshot_manager=snapshot_manager,
        archive_manager=archive_manager,
        space_manager=space_manager,
        backend=backend,
        config=CoordinatorConfig(mount_fn=mount_image),
    )

    logger.info("Starting TeslaUSB coordinator")
    coordinator.run()
    return 0


def cmd_archive(args: argparse.Namespace) -> int:
    """Run a single archive cycle."""
    config = load_config(args)

    # Auto-mount backingfiles
    if not _ensure_mounted(config):
        return 1

    fs, snapshot_manager, space_manager, archive_manager, backend = create_components(config)

    coordinator = Coordinator(
        fs=fs,
        snapshot_manager=snapshot_manager,
        archive_manager=archive_manager,
        space_manager=space_manager,
        backend=backend,
        config=CoordinatorConfig(mount_fn=mount_image),
    )

    success = coordinator.run_once()
    return 0 if success else 1


def cmd_status(args: argparse.Namespace) -> int:
    """Show current status including config validation."""
    config = load_config(args)

    # Collect validation warnings
    warnings = config.validate()

    fs = RealFilesystem()
    snapshot_manager = SnapshotManager(
        fs=fs,
        cam_disk_path=config.cam_disk_path,
        snapshots_path=config.snapshots_path,
    )

    # Check if backingfiles is mounted
    backingfiles_mounted = _is_mounted(config.backingfiles_path)

    # Get space info if mounted
    space_data = None
    if backingfiles_mounted:
        try:
            space_manager = SpaceManager(
                fs=fs,
                snapshot_manager=snapshot_manager,
                backingfiles_path=config.backingfiles_path,
                cam_size=config.cam_size,
                reserve=config.reserve,
            )
            space_info = space_manager.get_space_info()
            space_data = {
                "total_gb": round(space_info.total_gb, 2),
                "free_gb": round(space_info.free_gb, 2),
                "used_gb": round(space_info.used_gb, 2),
                "reserve_gb": round(space_info.reserve_gb, 2),
                "snapshot_budget_gb": round(space_info.snapshot_budget_gb, 2),
                "is_low": space_info.is_low,
            }

            # Add space validation warnings
            space_warnings = space_manager.validate_configuration(
                total_space=int(space_info.total_gb * GB),
            )
            warnings.extend(space_warnings)
        except Exception as e:
            warnings.append(f"Could not get space info: {e}")
    else:
        warnings.append("Backingfiles not mounted (run 'teslausb run' to auto-mount)")

    # Get snapshots if mounted
    snapshots = []
    deletable_count = 0
    if backingfiles_mounted:
        try:
            snapshots = snapshot_manager.get_snapshots()
            deletable_count = len(snapshot_manager.get_deletable_snapshots())
        except Exception:
            pass

    # Get archive backend status
    if config.archive.system == "rclone":
        backend = RcloneBackend(
            remote=config.archive.rclone_drive,
            path=config.archive.rclone_path,
            flags=config.archive.rclone_flags,
        )
    else:
        backend = MockArchiveBackend(reachable=True)

    archive_reachable = backend.is_reachable()

    # Build status dict
    status = {
        "warnings": warnings,
        "space": space_data,
        "snapshots": {
            "count": len(snapshots),
            "deletable": deletable_count,
        },
        "archive": {
            "system": config.archive.system,
            "reachable": archive_reachable,
        },
        "config": {
            "cam_size_gb": round(config.cam_size / GB, 2),
        },
    }

    if args.json:
        print(json.dumps(status, indent=2))
    else:
        # Show warnings first
        if warnings:
            print("Warnings:")
            for w in warnings:
                print(f"  - {w}")
            print()

        # Space
        print("Space:")
        if space_data:
            print(f"  Total: {space_data['total_gb']} GiB")
            print(f"  Free: {space_data['free_gb']} GiB")
            print(f"  Reserve: {space_data['reserve_gb']} GiB")
            print(f"  Snapshot budget: {space_data['snapshot_budget_gb']} GiB")
            print(f"  Low space: {'YES' if space_data['is_low'] else 'No'}")
        else:
            print("  (not available)")
        print()

        # Snapshots
        print("Snapshots:")
        print(f"  Count: {status['snapshots']['count']}")
        print(f"  Deletable: {status['snapshots']['deletable']}")
        print()

        # Archive
        print("Archive:")
        print(f"  System: {status['archive']['system']}")
        print(f"  Reachable: {'Yes' if status['archive']['reachable'] else 'No'}")

    return 0


def cmd_snapshots(args: argparse.Namespace) -> int:
    """List snapshots."""
    config = load_config(args)

    # Auto-mount backingfiles
    if not _ensure_mounted(config):
        return 1

    fs = RealFilesystem()
    snapshot_manager = SnapshotManager(
        fs=fs,
        cam_disk_path=config.cam_disk_path,
        snapshots_path=config.snapshots_path,
    )

    snapshots = snapshot_manager.get_snapshots()

    if args.json:
        print(json.dumps([s.to_dict() for s in snapshots], indent=2, default=str))
    else:
        if not snapshots:
            print("No snapshots")
            return 0

        print(f"{'ID':>6}  {'State':<10}  {'Refs':>4}  {'Created':<20}  Path")
        print("-" * 80)
        for snap in snapshots:
            created = snap.created_at.strftime("%Y-%m-%d %H:%M:%S")
            print(f"{snap.id:>6}  {snap.state.value:<10}  {snap.refcount:>4}  {created:<20}  {snap.path}")

    return 0


def cmd_clean(args: argparse.Namespace) -> int:
    """Clean up old snapshots."""
    config = load_config(args)

    # Auto-mount backingfiles
    if not _ensure_mounted(config):
        return 1

    fs, snapshot_manager, space_manager, _, _ = create_components(config)

    if args.dry_run:
        deletable = snapshot_manager.get_deletable_snapshots()
        print(f"Would delete {len(deletable)} snapshots:")
        for snap in deletable:
            print(f"  {snap.id}: {snap.path}")
        return 0

    success = space_manager.cleanup_if_needed()

    if success:
        print("Cleanup complete, space is sufficient")
        return 0
    else:
        print("Cleanup complete, but space is still low")
        return 1


def cmd_gadget(args: argparse.Namespace) -> int:
    """Manage USB gadget."""
    if args.gadget_command is None:
        args.gadget_parser.print_help()
        return 1

    gadget = UsbGadget()

    if args.gadget_command == "on":
        config = load_config(args)
        luns = {0: LunConfig(disk_path=config.cam_disk_path)}

        try:
            gadget.initialize(luns)
            gadget.enable()
            print("Gadget enabled")
            return 0
        except GadgetError as e:
            print(f"Failed to enable gadget: {e}")
            return 1

    elif args.gadget_command == "off":
        try:
            gadget.remove()
            print("Gadget disabled")
            return 0
        except GadgetError as e:
            print(f"Failed to disable gadget: {e}")
            return 1

    elif args.gadget_command == "status":
        status = gadget.get_status()

        if args.json:
            print(json.dumps(status, indent=2))
        else:
            print(f"Gadget: {status['name']}")
            print(f"  Initialized: {'Yes' if status['initialized'] else 'No'}")
            print(f"  Enabled: {'Yes' if status['enabled'] else 'No'}")
            if status['udc']:
                print(f"  UDC: {status['udc']}")
            if status['luns']:
                print(f"  LUNs:")
                for lun_id, lun_info in sorted(status['luns'].items()):
                    ro = " (read-only)" if lun_info.get('readonly') else ""
                    print(f"    {lun_id}: {lun_info.get('file', 'N/A')}{ro}")
        return 0

    return 1


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="TeslaUSB - Dashcam footage archiving for Tesla vehicles",
        prog="teslausb",
    )
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {_get_version()}"
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose output"
    )
    parser.add_argument(
        "--debug", action="store_true", help="Enable debug output"
    )
    parser.add_argument(
        "-c", "--config", type=str, help="Path to config file"
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # init command
    subparsers.add_parser("init", help="Initialize disk images and directory structure")

    # deinit command
    deinit_parser = subparsers.add_parser("deinit", help="Remove disk images and clean up")
    deinit_parser.add_argument(
        "-y", "--yes", action="store_true", help="Skip confirmation prompt"
    )

    # run command
    subparsers.add_parser("run", help="Run the main coordinator loop")

    # archive command
    subparsers.add_parser("archive", help="Run a single archive cycle")

    # status command
    status_parser = subparsers.add_parser("status", help="Show status (space, snapshots, config)")
    status_parser.add_argument("--json", action="store_true", help="Output as JSON")

    # snapshots command
    snap_parser = subparsers.add_parser("snapshots", help="List snapshots")
    snap_parser.add_argument("--json", action="store_true", help="Output as JSON")

    # clean command
    clean_parser = subparsers.add_parser("clean", help="Clean up old snapshots")
    clean_parser.add_argument(
        "--dry-run", action="store_true", help="Show what would be deleted"
    )

    # gadget command with subcommands
    gadget_parser = subparsers.add_parser("gadget", help="Manage USB gadget")
    gadget_parser.set_defaults(gadget_parser=gadget_parser)
    gadget_subparsers = gadget_parser.add_subparsers(dest="gadget_command", help="Gadget command")

    gadget_subparsers.add_parser("on", help="Initialize and enable USB gadget")
    gadget_subparsers.add_parser("off", help="Disable and remove USB gadget")
    gadget_status = gadget_subparsers.add_parser("status", help="Show gadget status")
    gadget_status.add_argument("--json", action="store_true", help="Output as JSON")

    args = parser.parse_args()

    configure_logging(verbose=args.verbose, debug=args.debug)

    if args.command is None:
        parser.print_help()
        return 1

    commands = {
        "init": cmd_init,
        "deinit": cmd_deinit,
        "run": cmd_run,
        "archive": cmd_archive,
        "status": cmd_status,
        "snapshots": cmd_snapshots,
        "clean": cmd_clean,
        "gadget": cmd_gadget,
    }

    try:
        return commands[args.command](args)
    except ConfigError as e:
        logger.error(f"Configuration error: {e}")
        return 1
    except KeyboardInterrupt:
        logger.info("Interrupted")
        return 130
    except Exception as e:
        logger.exception(f"Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
