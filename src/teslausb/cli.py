"""Command-line interface for TeslaUSB.

Usage:
    teslausb setup         # Create disk images and directory structure
    teslausb run           # Run the main coordinator loop
    teslausb archive       # Run a single archive cycle
    teslausb status        # Show current status
    teslausb snapshots     # List snapshots
    teslausb cleanup       # Clean up old snapshots
    teslausb validate      # Validate configuration
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
from .coordinator import Coordinator
from .filesystem import RealFilesystem
from .gadget import GadgetError, LunConfig, UsbGadget
from .snapshot import SnapshotManager
from .space import SpaceManager, GB

logger = logging.getLogger(__name__)


def _get_version() -> str:
    """Get package version, with fallback for development."""
    try:
        return version("teslausb")
    except PackageNotFoundError:
        return "dev"


def setup_logging(verbose: bool = False, debug: bool = False) -> None:
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
        archive_recent=config.archive.archive_recent,
        archive_saved=config.archive.archive_saved,
        archive_sentry=config.archive.archive_sentry,
        archive_track=config.archive.archive_track,
    )

    return fs, snapshot_manager, space_manager, archive_manager, backend


def cmd_setup(args: argparse.Namespace) -> int:
    """Set up TeslaUSB disk images and directory structure."""
    config = load_config(args)

    print(f"Setting up TeslaUSB...")
    print(f"  Backingfiles path: {config.backingfiles_path}")
    print(f"  Cam disk size: {config.cam_size / GB:.1f} GB")

    # Create directories
    config.backingfiles_path.mkdir(parents=True, exist_ok=True)
    config.snapshots_path.mkdir(parents=True, exist_ok=True)
    print(f"  Created directories")

    # Check if cam_disk already exists
    if config.cam_disk_path.exists():
        if not args.force:
            print(f"  Cam disk already exists: {config.cam_disk_path}")
            print(f"  Use --force to recreate")
            return 0
        else:
            print(f"  Removing existing cam disk...")
            config.cam_disk_path.unlink()

    # Create sparse disk image
    print(f"  Creating {config.cam_size / GB:.1f} GB disk image (sparse)...")
    try:
        subprocess.run(
            ["fallocate", "-l", str(config.cam_size), str(config.cam_disk_path)],
            check=True,
            capture_output=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        # fallocate may not be available or supported, fall back to truncate
        try:
            subprocess.run(
                ["truncate", "-s", str(config.cam_size), str(config.cam_disk_path)],
                check=True,
                capture_output=True,
            )
        except subprocess.CalledProcessError as e:
            print(f"  Failed to create disk image: {e.stderr.decode()}")
            return 1
        except FileNotFoundError:
            print(f"  Neither 'fallocate' nor 'truncate' found")
            return 1

    # Create partition table and filesystem
    print(f"  Creating partition table...")
    try:
        # Create MBR partition table with single FAT32 partition
        subprocess.run(
            ["parted", "-s", str(config.cam_disk_path), "mklabel", "msdos"],
            check=True,
            capture_output=True,
        )
        subprocess.run(
            ["parted", "-s", str(config.cam_disk_path), "mkpart", "primary", "fat32", "0%", "100%"],
            check=True,
            capture_output=True,
        )
    except subprocess.CalledProcessError as e:
        print(f"  Failed to create partition table: {e.stderr.decode()}")
        return 1
    except FileNotFoundError:
        print(f"  'parted' not found. Install it with: apt install parted")
        return 1

    # Set up loop device and format
    print(f"  Formatting as FAT32...")
    loop_dev = None
    try:
        # Create loop device with partition scanning
        result = subprocess.run(
            ["losetup", "-Pf", "--show", str(config.cam_disk_path)],
            check=True,
            capture_output=True,
        )
        loop_dev = result.stdout.decode().strip()
        partition = f"{loop_dev}p1"

        # Wait for partition device to appear
        for _ in range(20):
            if Path(partition).exists():
                break
            time.sleep(0.1)
        else:
            print(f"  Partition device {partition} not found")
            return 1

        # Format as FAT32
        subprocess.run(
            ["mkfs.vfat", "-F", "32", "-n", "TESLAUSB", partition],
            check=True,
            capture_output=True,
        )

        # Create TeslaCam directory structure
        mount_point = Path("/tmp/teslausb-setup-mount")
        mount_point.mkdir(exist_ok=True)

        subprocess.run(
            ["mount", partition, str(mount_point)],
            check=True,
            capture_output=True,
        )

        try:
            (mount_point / "TeslaCam").mkdir()
            print(f"  Created TeslaCam directory")
        finally:
            subprocess.run(["umount", str(mount_point)], capture_output=True)
            try:
                mount_point.rmdir()
            except OSError:
                pass

    except subprocess.CalledProcessError as e:
        print(f"  Failed to format disk: {e.stderr.decode() if e.stderr else e}")
        return 1
    except FileNotFoundError as e:
        print(f"  Required tool not found: {e}")
        return 1
    finally:
        # Always clean up loop device
        if loop_dev:
            subprocess.run(["losetup", "-d", loop_dev], capture_output=True)

    print(f"\nSetup complete!")
    print(f"  Disk image: {config.cam_disk_path}")
    print(f"\nNext steps:")
    print(f"  1. Configure archiving in /etc/teslausb.conf (optional)")
    print(f"  2. Run: teslausb gadget setup --enable")
    print(f"  3. Run: teslausb run")

    return 0


def cmd_run(args: argparse.Namespace) -> int:
    """Run the main coordinator loop."""
    config = load_config(args)

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
    )

    logger.info("Starting TeslaUSB coordinator")
    coordinator.run()
    return 0


def cmd_archive(args: argparse.Namespace) -> int:
    """Run a single archive cycle."""
    config = load_config(args)
    fs, snapshot_manager, space_manager, archive_manager, backend = create_components(config)

    coordinator = Coordinator(
        fs=fs,
        snapshot_manager=snapshot_manager,
        archive_manager=archive_manager,
        space_manager=space_manager,
        backend=backend,
    )

    success = coordinator.run_once()
    return 0 if success else 1


def cmd_status(args: argparse.Namespace) -> int:
    """Show current status."""
    config = load_config(args)
    fs, snapshot_manager, space_manager, archive_manager, backend = create_components(config)

    # Get space info
    space_info = space_manager.get_space_info()

    # Get snapshots
    snapshots = snapshot_manager.get_snapshots()

    status = {
        "space": {
            "total_gb": round(space_info.total_gb, 2),
            "free_gb": round(space_info.free_gb, 2),
            "used_gb": round(space_info.used_gb, 2),
            "reserve_gb": round(space_info.reserve_gb, 2),
            "snapshot_budget_gb": round(space_info.snapshot_budget_gb, 2),
            "is_low": space_info.is_low,
        },
        "snapshots": {
            "count": len(snapshots),
            "deletable": len(snapshot_manager.get_deletable_snapshots()),
        },
        "archive": {
            "system": config.archive.system,
            "reachable": backend.is_reachable(),
        },
        "config": {
            "cam_size_gb": round(config.cam_size / GB, 2),
        },
    }

    if args.json:
        print(json.dumps(status, indent=2))
    else:
        print(f"Space:")
        print(f"  Total: {status['space']['total_gb']} GB")
        print(f"  Free: {status['space']['free_gb']} GB")
        print(f"  Reserve: {status['space']['reserve_gb']} GB")
        print(f"  Snapshot budget: {status['space']['snapshot_budget_gb']} GB")
        print(f"  Low space: {'YES' if status['space']['is_low'] else 'No'}")
        print()
        print(f"Snapshots:")
        print(f"  Count: {status['snapshots']['count']}")
        print(f"  Deletable: {status['snapshots']['deletable']}")
        print()
        print(f"Archive:")
        print(f"  System: {status['archive']['system']}")
        print(f"  Reachable: {'Yes' if status['archive']['reachable'] else 'No'}")

    return 0


def cmd_snapshots(args: argparse.Namespace) -> int:
    """List snapshots."""
    config = load_config(args)
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


def cmd_cleanup(args: argparse.Namespace) -> int:
    """Clean up old snapshots."""
    config = load_config(args)
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


def cmd_validate(args: argparse.Namespace) -> int:
    """Validate configuration."""
    try:
        config = load_config(args)
    except ConfigError as e:
        print(f"Configuration error: {e}")
        return 1

    warnings = config.validate()

    # Also check space requirements if possible
    fs = RealFilesystem()
    if fs.exists(config.backingfiles_path):
        try:
            statvfs = fs.statvfs(config.backingfiles_path)
            total_space = statvfs.total_bytes

            space_manager = SpaceManager(
                fs=fs,
                snapshot_manager=None,  # type: ignore
                backingfiles_path=config.backingfiles_path,
                cam_size=config.cam_size,
            )

            space_warnings = space_manager.validate_configuration(
                total_space=total_space,
            )
            warnings.extend(space_warnings)
        except Exception as e:
            warnings.append(f"Could not validate space: {e}")

    if warnings:
        print("Configuration warnings:")
        for warning in warnings:
            print(f"  - {warning}")
        return 1
    else:
        print("Configuration is valid")
        return 0


def cmd_gadget(args: argparse.Namespace) -> int:
    """Manage USB gadget."""
    gadget = UsbGadget()

    if args.gadget_command == "setup":
        config = load_config(args)

        luns = {0: LunConfig(disk_path=config.cam_disk_path)}

        try:
            gadget.setup(luns)
            if args.enable:
                gadget.enable()
            print(f"Gadget setup complete with {len(luns)} LUN(s)")
            return 0
        except GadgetError as e:
            print(f"Failed to setup gadget: {e}")
            return 1

    elif args.gadget_command == "enable":
        try:
            gadget.enable()
            print("Gadget enabled")
            return 0
        except GadgetError as e:
            print(f"Failed to enable gadget: {e}")
            return 1

    elif args.gadget_command == "disable":
        try:
            gadget.disable()
            print("Gadget disabled")
            return 0
        except GadgetError as e:
            print(f"Failed to disable gadget: {e}")
            return 1

    elif args.gadget_command == "teardown":
        try:
            gadget.teardown()
            print("Gadget removed")
            return 0
        except GadgetError as e:
            print(f"Failed to teardown gadget: {e}")
            return 1

    elif args.gadget_command == "status":
        status = gadget.get_status()

        if args.json:
            print(json.dumps(status, indent=2))
        else:
            print(f"Gadget: {status['name']}")
            print(f"  Setup: {'Yes' if status['setup'] else 'No'}")
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

    # setup command
    setup_parser = subparsers.add_parser("setup", help="Create disk images and directory structure")
    setup_parser.add_argument(
        "--force", action="store_true", help="Recreate disk image if it exists"
    )

    # run command
    run_parser = subparsers.add_parser("run", help="Run the main coordinator loop")

    # archive command
    archive_parser = subparsers.add_parser("archive", help="Run a single archive cycle")

    # status command
    status_parser = subparsers.add_parser("status", help="Show current status")
    status_parser.add_argument("--json", action="store_true", help="Output as JSON")

    # snapshots command
    snap_parser = subparsers.add_parser("snapshots", help="List snapshots")
    snap_parser.add_argument("--json", action="store_true", help="Output as JSON")

    # cleanup command
    cleanup_parser = subparsers.add_parser("cleanup", help="Clean up old snapshots")
    cleanup_parser.add_argument(
        "--dry-run", action="store_true", help="Show what would be deleted"
    )

    # validate command
    validate_parser = subparsers.add_parser("validate", help="Validate configuration")

    # gadget command with subcommands
    gadget_parser = subparsers.add_parser("gadget", help="Manage USB gadget")
    gadget_subparsers = gadget_parser.add_subparsers(dest="gadget_command", help="Gadget command")

    gadget_setup = gadget_subparsers.add_parser("setup", help="Setup USB gadget")
    gadget_setup.add_argument("--enable", action="store_true", help="Enable after setup")

    gadget_subparsers.add_parser("enable", help="Enable USB gadget")
    gadget_subparsers.add_parser("disable", help="Disable USB gadget")
    gadget_subparsers.add_parser("teardown", help="Remove USB gadget")

    gadget_status = gadget_subparsers.add_parser("status", help="Show gadget status")
    gadget_status.add_argument("--json", action="store_true", help="Output as JSON")

    args = parser.parse_args()

    setup_logging(verbose=args.verbose, debug=args.debug)

    if args.command is None:
        parser.print_help()
        return 1

    commands = {
        "setup": cmd_setup,
        "run": cmd_run,
        "archive": cmd_archive,
        "status": cmd_status,
        "snapshots": cmd_snapshots,
        "cleanup": cmd_cleanup,
        "validate": cmd_validate,
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
