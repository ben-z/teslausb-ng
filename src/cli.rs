use std::env;
use std::fs;
use std::io::{self, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::archive::{ArchiveBackend, ArchiveManager};
use crate::command::CommandRunner;
use crate::config::{load_config, parse_size, Config, GB};
use crate::coordinator::Coordinator;
use crate::dependencies::{
    check_dependencies, dependency_detail, ensure_dependencies, DependencySet,
};
use crate::error::{Error, Result};
use crate::filesystem::RealFileSystem;
use crate::gadget::{LunConfig, UsbGadget};
use crate::idle::ProcIdleDetector;
use crate::led::SysfsLedController;
use crate::mount::setup_loop_device;
use crate::snapshot::SnapshotManager;
use crate::space::{calculate_cam_size, disk_space, DEFAULT_RESERVE, MIN_CAM_SIZE};
use crate::temperature::{SysfsTemperatureMonitor, TemperatureConfig};

pub fn main() -> i32 {
    match run(env::args().collect()) {
        Ok(code) => code,
        Err(err) => {
            eprintln!("error: {}", err);
            1
        }
    }
}

#[derive(Debug, Clone)]
struct GlobalArgs {
    config_path: Option<PathBuf>,
    command: String,
    args: Vec<String>,
}

fn run(argv: Vec<String>) -> Result<i32> {
    let parsed = parse_global_args(argv)?;
    match parsed.command.as_str() {
        "init" => cmd_init(&parsed),
        "deinit" => cmd_deinit(&parsed),
        "mount" => cmd_mount(&parsed),
        "run" => cmd_run(&parsed),
        "archive" => cmd_archive(&parsed),
        "status" => cmd_status(&parsed),
        "snapshots" => cmd_snapshots(&parsed),
        "clean" => cmd_clean(&parsed),
        "gadget" => cmd_gadget(&parsed),
        "service" => cmd_service(&parsed),
        "doctor" => cmd_doctor(&parsed),
        "help" | "--help" | "-h" => {
            print_help();
            Ok(0)
        }
        "--version" | "-V" => {
            println!("teslausb {}", env!("CARGO_PKG_VERSION"));
            Ok(0)
        }
        unknown => Err(Error::new(format!("unknown command: {}", unknown))),
    }
}

fn parse_global_args(argv: Vec<String>) -> Result<GlobalArgs> {
    let mut iter = argv.into_iter();
    let _program = iter.next();
    let mut config_path = None;
    let mut rest = Vec::new();

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "-c" | "--config" => {
                let value = iter
                    .next()
                    .ok_or_else(|| Error::new("--config requires a path"))?;
                config_path = Some(PathBuf::from(value));
            }
            "-l" | "--log-level" => {
                let _ = iter.next();
            }
            "--version" | "-V" | "--help" | "-h" => {
                rest.push(arg);
                rest.extend(iter);
                break;
            }
            _ => {
                rest.push(arg);
                rest.extend(iter);
                break;
            }
        }
    }

    if rest.is_empty() {
        rest.push("help".to_string());
    }
    let command = rest.remove(0);
    Ok(GlobalArgs {
        config_path,
        command,
        args: rest,
    })
}

fn config(args: &GlobalArgs) -> Result<Config> {
    load_config(args.config_path.as_deref())
}

fn create_components(
    config: &Config,
) -> Result<(
    SnapshotManager<RealFileSystem>,
    ArchiveManager<RealFileSystem>,
)> {
    let fs = RealFileSystem;
    let snapshot_manager =
        SnapshotManager::new(fs, config.cam_disk_path(), config.snapshots_path())?;
    let backend = ArchiveBackend::from_config(&config.archive, fs);
    let archive_manager = ArchiveManager::new(
        fs,
        snapshot_manager.clone(),
        backend,
        config.cam_disk_path(),
        &config.archive,
    );
    Ok((snapshot_manager, archive_manager))
}

fn cmd_init(args: &GlobalArgs) -> Result<i32> {
    let config = config(args)?;
    let reserve = parse_optional_value(&args.args, "--reserve")?;
    let backingfiles_img = config.backingfiles_image_path();
    if backingfiles_img.exists() {
        eprintln!("Error: {} already exists", backingfiles_img.display());
        eprintln!("Run 'teslausb deinit' to remove it first");
        return Ok(1);
    }
    ensure_dependencies(&config, DependencySet::Init)?;

    fs::create_dir_all(&config.mutable_path)?;
    let space = disk_space(&config.mutable_path)?;
    let available_space = space.free_bytes;
    let reserve = if let Some(reserve) = reserve {
        parse_size(&reserve)?
    } else {
        prompt_reserve(available_space)?
    };

    let backingfiles_size = available_space.saturating_sub(reserve);
    if backingfiles_size == 0 || reserve >= available_space {
        eprintln!("Error: reserve leaves no room for TeslaUSB backing files");
        eprintln!("  Available: {:.1} GiB", available_space as f64 / GB as f64);
        eprintln!("  Reserve: {:.1} GiB", reserve as f64 / GB as f64);
        return Ok(1);
    }

    let cam_size = calculate_cam_size(backingfiles_size);
    if cam_size < MIN_CAM_SIZE {
        eprintln!("Error: not enough disk space");
        eprintln!("  Cam disk would be {:.1} GiB", cam_size as f64 / GB as f64);
        eprintln!(
            "  Minimum cam disk size is {:.1} GiB",
            MIN_CAM_SIZE as f64 / GB as f64
        );
        return Ok(1);
    }

    println!("Initializing TeslaUSB...");
    println!(
        "  Available space: {:.1} GiB",
        available_space as f64 / GB as f64
    );
    println!("  Reserve for OS: {:.1} GiB", reserve as f64 / GB as f64);
    println!(
        "  Backingfiles size: {:.1} GiB",
        backingfiles_size as f64 / GB as f64
    );
    println!("  Cam disk size: {:.1} GiB", cam_size as f64 / GB as f64);

    create_backingfiles_image(&backingfiles_img, backingfiles_size)?;
    mount_backingfiles(&backingfiles_img, &config.backingfiles_path)?;
    verify_xfs(&config.backingfiles_path)?;
    fs::create_dir_all(config.snapshots_path())?;
    create_cam_disk(&config.cam_disk_path(), cam_size)?;

    println!("\nInitialization complete");
    println!("  Backingfiles image: {}", backingfiles_img.display());
    println!("  Cam disk: {}", config.cam_disk_path().display());
    Ok(0)
}

fn cmd_deinit(args: &GlobalArgs) -> Result<i32> {
    let yes = has_flag(&args.args, "--yes") || has_flag(&args.args, "-y");
    let config = config(args)?;
    let backingfiles_img = config.backingfiles_image_path();
    if !backingfiles_img.exists() {
        println!(
            "Nothing to do: {} does not exist",
            backingfiles_img.display()
        );
        return Ok(0);
    }
    if !yes && !confirm("This will permanently delete all TeslaUSB disk images. Continue? [y/N] ")?
    {
        println!("Aborted");
        return Ok(1);
    }
    if is_mounted(&config.backingfiles_path) {
        CommandRunner.check(
            "umount",
            [config.backingfiles_path.display().to_string().as_str()],
            Some(Duration::from_secs(30)),
        )?;
    }
    fs::remove_file(&backingfiles_img)?;
    let _ = fs::remove_dir(&config.backingfiles_path);
    println!("Deinitialization complete");
    Ok(0)
}

fn cmd_mount(args: &GlobalArgs) -> Result<i32> {
    let config = config(args)?;
    ensure_dependencies(&config, DependencySet::Mount)?;
    ensure_mounted(&config)?;
    println!(
        "Backingfiles mounted at {}",
        config.backingfiles_path.display()
    );
    Ok(0)
}

fn cmd_run(args: &GlobalArgs) -> Result<i32> {
    let config = config(args)?;
    ensure_dependencies(&config, DependencySet::Runtime)?;
    ensure_mounted(&config)?;
    for warning in config.warnings() {
        eprintln!("warning: {}", warning);
    }
    let (snapshot_manager, archive_manager) = create_components(&config)?;
    let temperature_monitor = SysfsTemperatureMonitor::default_sysfs(TemperatureConfig {
        warning_threshold: Some(80_000),
        caution_threshold: Some(70_000),
        poll_interval: Duration::from_secs(60),
    });
    let _temperature_guard = temperature_monitor.start();
    let mut coordinator = Coordinator::new(
        snapshot_manager,
        archive_manager,
        Some(UsbGadget::default()),
    )
    .with_led(SysfsLedController::auto_detect())
    .with_idle_detector(ProcIdleDetector::default());
    coordinator.run()?;
    Ok(0)
}

fn cmd_archive(args: &GlobalArgs) -> Result<i32> {
    let config = config(args)?;
    ensure_dependencies(&config, DependencySet::Runtime)?;
    ensure_mounted(&config)?;
    let (snapshot_manager, archive_manager) = create_components(&config)?;
    let mut coordinator = Coordinator::new(
        snapshot_manager,
        archive_manager,
        Some(UsbGadget::default()),
    )
    .with_idle_detector(ProcIdleDetector::default());
    Ok(if coordinator.run_once()? { 0 } else { 1 })
}

fn cmd_status(args: &GlobalArgs) -> Result<i32> {
    let json = has_flag(&args.args, "--json");
    let config = config(args)?;
    let mut warnings = config.warnings();
    let mounted = is_mounted(&config.backingfiles_path);
    let archive_reachable =
        ArchiveBackend::from_config(&config.archive, RealFileSystem).is_reachable();
    let snapshots = if mounted {
        SnapshotManager::new(
            RealFileSystem,
            config.cam_disk_path(),
            config.snapshots_path(),
        )
        .map(|manager| manager.get_snapshots())
        .unwrap_or_default()
    } else {
        Vec::new()
    };
    let deletable_count = if mounted {
        snapshots
            .iter()
            .filter(|snapshot| snapshot.is_deletable())
            .count()
    } else {
        warnings.push("Backingfiles not mounted (run 'teslausb mount' or 'teslausb run')".into());
        0
    };
    if deletable_count > 0 {
        warnings.push(format!(
            "{} stale snapshot(s) found (run 'teslausb clean' or wait for the next archive cycle)",
            deletable_count
        ));
    }
    let space = if mounted {
        disk_space(&config.backingfiles_path).ok()
    } else {
        None
    };

    if json {
        println!(
            "{{\n  \"backingfiles_mounted\": {},\n  \"warnings\": [{}],\n  \"space\": {},\n  \"snapshots\": {{ \"count\": {}, \"deletable\": {} }},\n  \"archive\": {{ \"system\": \"{}\", \"reachable\": {} }}\n}}",
            mounted,
            warnings
                .iter()
                .map(|warning| format!("\"{}\"", json_escape(warning)))
                .collect::<Vec<_>>()
                .join(", "),
            space_json(space),
            snapshots.len(),
            deletable_count,
            json_escape(&config.archive.system),
            archive_reachable
        );
    } else {
        if warnings.is_empty() {
            println!("Warnings: none");
        } else {
            println!("Warnings:");
            for warning in warnings {
                println!("  - {}", warning);
            }
        }
        println!(
            "\nBackingfiles mounted: {}",
            if mounted { "Yes" } else { "No" }
        );
        println!("Snapshots: {}", snapshots.len());
        println!("Deletable snapshots: {}", deletable_count);
        if let Some(space) = space {
            println!(
                "Space: {:.1} GiB free / {:.1} GiB total",
                space.free_gib(),
                space.total_gib()
            );
        }
        println!(
            "Archive: {} ({})",
            config.archive.system,
            if archive_reachable {
                "reachable"
            } else {
                "not reachable"
            }
        );
    }
    Ok(0)
}

fn cmd_snapshots(args: &GlobalArgs) -> Result<i32> {
    let json = has_flag(&args.args, "--json");
    let config = config(args)?;
    ensure_mounted(&config)?;
    let manager = SnapshotManager::new(
        RealFileSystem,
        config.cam_disk_path(),
        config.snapshots_path(),
    )?;
    let snapshots = manager.get_snapshots();
    if json {
        println!(
            "[{}]",
            snapshots
                .iter()
                .map(|snapshot| format!(
                    "{{\"id\":{},\"state\":\"{}\",\"refs\":{},\"created_at_unix\":{},\"path\":\"{}\"}}",
                    snapshot.id,
                    match snapshot.state() {
                        crate::snapshot::SnapshotState::Ready => "ready",
                        crate::snapshot::SnapshotState::Archiving => "archiving",
                    },
                    snapshot.refcount,
                    snapshot.created_secs,
                    json_escape(&snapshot.path.display().to_string())
                ))
                .collect::<Vec<_>>()
                .join(",")
        );
    } else if snapshots.is_empty() {
        println!("No snapshots");
    } else {
        println!(
            "{:>6}  {:<10}  {:>4}  {:>12}  Path",
            "ID", "State", "Refs", "Created"
        );
        println!("{}", "-".repeat(80));
        for snapshot in snapshots {
            let state = match snapshot.state() {
                crate::snapshot::SnapshotState::Ready => "ready",
                crate::snapshot::SnapshotState::Archiving => "archiving",
            };
            println!(
                "{:>6}  {:<10}  {:>4}  {:>12}  {}",
                snapshot.id,
                state,
                snapshot.refcount,
                snapshot.created_secs,
                snapshot.path.display()
            );
        }
    }
    Ok(0)
}

fn cmd_clean(args: &GlobalArgs) -> Result<i32> {
    let dry_run = has_flag(&args.args, "--dry-run");
    let config = config(args)?;
    ensure_mounted(&config)?;
    let manager = SnapshotManager::new(
        RealFileSystem,
        config.cam_disk_path(),
        config.snapshots_path(),
    )?;
    let deletable = manager.get_deletable_snapshots();
    if deletable.is_empty() {
        println!("No deletable snapshots");
        return Ok(0);
    }
    if dry_run {
        println!("Would delete {} snapshot(s):", deletable.len());
        for snapshot in deletable {
            println!("  {}: {}", snapshot.id, snapshot.path.display());
        }
        return Ok(0);
    }
    let mut deleted = 0;
    for snapshot in deletable {
        if manager.delete_snapshot(snapshot.id)? {
            deleted += 1;
            println!("Deleted snapshot {}", snapshot.id);
        }
    }
    println!("Deleted {} snapshot(s)", deleted);
    Ok(0)
}

fn cmd_gadget(args: &GlobalArgs) -> Result<i32> {
    let Some(command) = args.args.first() else {
        eprintln!("usage: teslausb gadget <on|off|status>");
        return Ok(1);
    };
    let gadget = UsbGadget::default();
    match command.as_str() {
        "on" => {
            let config = config(args)?;
            ensure_dependencies(&config, DependencySet::Gadget)?;
            gadget.initialize(&[(0, LunConfig::new(config.cam_disk_path()))])?;
            gadget.enable()?;
            println!("Gadget enabled");
            Ok(0)
        }
        "off" => {
            gadget.remove()?;
            println!("Gadget disabled");
            Ok(0)
        }
        "status" => {
            if has_flag(&args.args, "--json") {
                print!("{}", gadget.status_json());
            } else {
                println!("Gadget: {}", gadget.name());
                println!(
                    "  Initialized: {}",
                    if gadget.is_initialized() { "Yes" } else { "No" }
                );
                println!(
                    "  Enabled: {}",
                    if gadget.is_enabled() { "Yes" } else { "No" }
                );
            }
            Ok(0)
        }
        _ => Err(Error::new(format!("unknown gadget command: {}", command))),
    }
}

fn cmd_service(args: &GlobalArgs) -> Result<i32> {
    let Some(command) = args.args.first() else {
        eprintln!("usage: teslausb service <install|uninstall|status>");
        return Ok(1);
    };
    match command.as_str() {
        "install" => {
            let force = has_flag(&args.args, "--force");
            let config = config(args)?;
            ensure_dependencies(&config, DependencySet::Full)?;
            install_service(force)?;
            Ok(0)
        }
        "uninstall" => {
            let config = config(args)?;
            ensure_dependencies(&config, DependencySet::Service)?;
            uninstall_service()?;
            Ok(0)
        }
        "status" => {
            let config = config(args)?;
            ensure_dependencies(&config, DependencySet::Service)?;
            let output = CommandRunner.run(
                "systemctl",
                ["status", "teslausb.service"],
                Some(Duration::from_secs(30)),
            )?;
            print!("{}", output.stdout);
            eprint!("{}", output.stderr);
            Ok(output.code.unwrap_or(1))
        }
        _ => Err(Error::new(format!("unknown service command: {}", command))),
    }
}

fn cmd_doctor(args: &GlobalArgs) -> Result<i32> {
    let config = config(args)?;
    let set = if has_flag(&args.args, "--startup") {
        DependencySet::Startup
    } else {
        DependencySet::Full
    };
    let mut failed = false;
    println!(
        "{:<12} {:<8} {:<12} Detail",
        "Dependency", "Status", "Version"
    );
    println!("{}", "-".repeat(64));
    for report in check_dependencies(&config, set) {
        let version = report
            .version
            .map(|version| version.to_string())
            .unwrap_or_else(|| "-".to_string());
        println!(
            "{:<12} {:<8} {:<12} {}",
            report.name,
            if report.ok { "ok" } else { "failed" },
            version,
            dependency_detail(&report)
        );
        failed |= !report.ok;
    }
    for warning in config.warnings() {
        println!("warning: {}", warning);
        failed = true;
    }
    println!(
        "USB gadget UDC: {}",
        if Path::new("/sys/class/udc")
            .read_dir()
            .map(|mut it| it.next().is_some())
            .unwrap_or(false)
        {
            "present"
        } else {
            "missing"
        }
    );
    Ok(if failed { 1 } else { 0 })
}

fn prompt_reserve(available_space: u64) -> Result<u64> {
    if !io::stdin().is_terminal() {
        return Err(Error::new(
            "--reserve is required when running non-interactively",
        ));
    }
    println!(
        "Available disk space: {:.1} GiB",
        available_space as f64 / GB as f64
    );
    print!("Reserve size [{}G]: ", DEFAULT_RESERVE / GB);
    io::stdout().flush()?;
    let mut line = String::new();
    io::stdin().read_line(&mut line)?;
    let line = line.trim();
    if line.is_empty() {
        Ok(DEFAULT_RESERVE)
    } else {
        parse_size(line)
    }
}

fn create_backingfiles_image(path: &Path, size: u64) -> Result<()> {
    println!("  Creating {:.1} GiB XFS image...", size as f64 / GB as f64);
    let size_s = size.to_string();
    CommandRunner.check(
        "truncate",
        ["-s", size_s.as_str(), &path.display().to_string()],
        Some(Duration::from_secs(300)),
    )?;
    CommandRunner.check(
        "mkfs.xfs",
        ["-f", &path.display().to_string()],
        Some(Duration::from_secs(300)),
    )?;
    Ok(())
}

fn mount_backingfiles(image_path: &Path, mount_path: &Path) -> Result<()> {
    fs::create_dir_all(mount_path)?;
    if is_mounted(mount_path) {
        return Ok(());
    }
    CommandRunner.check(
        "mount",
        [
            "-o",
            "loop",
            &image_path.display().to_string(),
            &mount_path.display().to_string(),
        ],
        Some(Duration::from_secs(60)),
    )?;
    Ok(())
}

fn ensure_mounted(config: &Config) -> Result<()> {
    let image = config.backingfiles_image_path();
    if !image.exists() {
        return Err(Error::new(format!(
            "{} does not exist; run 'teslausb init' first",
            image.display()
        )));
    }
    mount_backingfiles(&image, &config.backingfiles_path)?;
    verify_xfs(&config.backingfiles_path)?;
    Ok(())
}

fn verify_xfs(path: &Path) -> Result<()> {
    let fstype = filesystem_type(path)?;
    if fstype != "xfs" {
        return Err(Error::new(format!(
            "{} is {}, not xfs",
            path.display(),
            fstype
        )));
    }
    Ok(())
}

fn create_cam_disk(cam_disk_path: &Path, cam_size: u64) -> Result<()> {
    println!(
        "  Creating {:.1} GiB cam disk...",
        cam_size as f64 / GB as f64
    );
    let cam_size_s = cam_size.to_string();
    CommandRunner.check(
        "truncate",
        [
            "-s",
            cam_size_s.as_str(),
            &cam_disk_path.display().to_string(),
        ],
        Some(Duration::from_secs(300)),
    )?;
    CommandRunner.check(
        "parted",
        [
            "-s",
            &cam_disk_path.display().to_string(),
            "mklabel",
            "msdos",
        ],
        Some(Duration::from_secs(60)),
    )?;
    CommandRunner.check(
        "parted",
        [
            "-s",
            &cam_disk_path.display().to_string(),
            "mkpart",
            "primary",
            "fat32",
            "0%",
            "100%",
        ],
        Some(Duration::from_secs(60)),
    )?;

    let loop_device = setup_loop_device(cam_disk_path)?;
    CommandRunner.check(
        "mkfs.vfat",
        ["-F", "32", "-n", "TESLAUSB", loop_device.partition()],
        Some(Duration::from_secs(300)),
    )?;

    let mount_point =
        std::env::temp_dir().join(format!("teslausb-cam-mount-{}", std::process::id()));
    fs::create_dir_all(&mount_point)?;
    CommandRunner.check(
        "mount",
        [loop_device.partition(), &mount_point.display().to_string()],
        Some(Duration::from_secs(60)),
    )?;
    let create_result = fs::create_dir_all(mount_point.join("TeslaCam"));
    let umount_result = CommandRunner.run(
        "umount",
        [&mount_point.display().to_string()],
        Some(Duration::from_secs(60)),
    );
    let _ = fs::remove_dir(&mount_point);
    create_result?;
    if !umount_result?.success() {
        return Err(Error::new("failed to unmount temporary cam disk mount"));
    }
    Ok(())
}

fn is_mounted(path: &Path) -> bool {
    CommandRunner
        .run(
            "mountpoint",
            ["-q", &path.display().to_string()],
            Some(Duration::from_secs(10)),
        )
        .map(|output| output.success())
        .unwrap_or(false)
}

fn filesystem_type(path: &Path) -> Result<String> {
    let output = CommandRunner.check(
        "stat",
        ["-f", "-c", "%T", &path.display().to_string()],
        Some(Duration::from_secs(10)),
    )?;
    Ok(output.stdout.trim().to_string())
}

const SYSTEMD_SERVICE_PATH: &str = "/etc/systemd/system/teslausb.service";

fn systemd_service_path() -> PathBuf {
    env::var_os("TESLAUSB_SYSTEMD_SERVICE_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(SYSTEMD_SERVICE_PATH))
}

fn install_service(force: bool) -> Result<()> {
    let path = systemd_service_path();
    if path.exists() && !force {
        return Err(Error::new(format!(
            "service already installed at {}; use --force to overwrite",
            path.display()
        )));
    }
    let exe = env::current_exe()?;
    let service = format!(
        "[Unit]\n\
Description=TeslaUSB Archiver\n\
After=local-fs.target network-online.target\n\
Wants=local-fs.target network-online.target\n\
\n\
[Service]\n\
Type=simple\n\
ExecStartPre={exe} doctor --startup\n\
ExecStartPre={exe} mount\n\
ExecStartPre={exe} gadget on\n\
ExecStart={exe} run\n\
ExecStop={exe} gadget off\n\
TimeoutStartSec=120\n\
Restart=always\n\
RestartSec=10\n\
\n\
[Install]\n\
WantedBy=multi-user.target\n",
        exe = exe.display()
    );
    fs::write(&path, service)?;
    CommandRunner.check(
        "systemctl",
        ["daemon-reload"],
        Some(Duration::from_secs(30)),
    )?;
    CommandRunner.check(
        "systemctl",
        ["enable", "teslausb.service"],
        Some(Duration::from_secs(30)),
    )?;
    println!("Service installed at {}", path.display());
    Ok(())
}

fn uninstall_service() -> Result<()> {
    let path = systemd_service_path();
    if !path.exists() {
        println!("Service is not installed");
        return Ok(());
    }
    let _ = CommandRunner.run(
        "systemctl",
        ["stop", "teslausb.service"],
        Some(Duration::from_secs(30)),
    );
    let _ = CommandRunner.run(
        "systemctl",
        ["disable", "teslausb.service"],
        Some(Duration::from_secs(30)),
    );
    fs::remove_file(&path)?;
    let _ = CommandRunner.run(
        "systemctl",
        ["daemon-reload"],
        Some(Duration::from_secs(30)),
    );
    println!("Service uninstalled");
    Ok(())
}

fn parse_optional_value(args: &[String], name: &str) -> Result<Option<String>> {
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        if arg == name {
            return iter
                .next()
                .cloned()
                .map(Some)
                .ok_or_else(|| Error::new(format!("{} requires a value", name)));
        }
    }
    Ok(None)
}

fn has_flag(args: &[String], flag: &str) -> bool {
    args.iter().any(|arg| arg == flag)
}

fn confirm(prompt: &str) -> Result<bool> {
    if !io::stdin().is_terminal() {
        return Ok(false);
    }
    print!("{}", prompt);
    io::stdout().flush()?;
    let mut line = String::new();
    io::stdin().read_line(&mut line)?;
    Ok(matches!(
        line.trim().to_ascii_lowercase().as_str(),
        "y" | "yes"
    ))
}

fn space_json(space: Option<crate::space::SpaceInfo>) -> String {
    match space {
        Some(space) => format!(
            "{{ \"total_gb\": {:.2}, \"free_gb\": {:.2}, \"used_gb\": {:.2} }}",
            space.total_gib(),
            space.free_gib(),
            space.used_gib()
        ),
        None => "null".to_string(),
    }
}

fn json_escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn print_help() {
    println!(
        "TeslaUSB\n\n\
Usage: teslausb [--config PATH] <command> [options]\n\n\
Commands:\n\
  init [--reserve SIZE]      Initialize disk images and directories\n\
  deinit [-y|--yes]          Remove disk images and clean up\n\
  mount                      Mount backingfiles image\n\
  run                        Run archive loop\n\
  archive                    Run one archive cycle\n\
  status [--json]            Show status\n\
  snapshots [--json]         List snapshots\n\
  clean [--dry-run]          Delete deletable snapshots\n\
  gadget <on|off|status>     Manage USB mass storage gadget\n\
  service <install|uninstall|status>\n\
  doctor [--startup]         Check external dependencies\n"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    fn temp_config(name: &str, backingfiles_exists: bool) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "teslausb-cli-{name}-{}-{suffix}",
            std::process::id()
        ));
        let mutable = root.join("mutable");
        let backingfiles = root.join("backingfiles");
        fs::create_dir_all(&mutable).unwrap();
        fs::create_dir_all(&backingfiles).unwrap();
        if backingfiles_exists {
            fs::write(mutable.join("backingfiles.img"), b"exists").unwrap();
        }
        let config = root.join("teslausb.conf");
        fs::write(
            &config,
            format!(
                "MUTABLE_PATH={}\nBACKINGFILES_PATH={}\nARCHIVE_SYSTEM=none\n",
                mutable.display(),
                backingfiles.display()
            ),
        )
        .unwrap();
        config
    }

    #[test]
    fn parse_global_args_defaults_to_help() {
        let parsed = parse_global_args(strings(&["teslausb"])).unwrap();
        assert_eq!(parsed.command, "help");
        assert!(parsed.args.is_empty());
        assert!(parsed.config_path.is_none());
    }

    #[test]
    fn parse_global_args_extracts_config_and_command() {
        let parsed = parse_global_args(strings(&[
            "teslausb",
            "--config",
            "/tmp/conf",
            "status",
            "--json",
        ]))
        .unwrap();

        assert_eq!(parsed.config_path, Some(PathBuf::from("/tmp/conf")));
        assert_eq!(parsed.command, "status");
        assert_eq!(parsed.args, ["--json"]);
    }

    #[test]
    fn parse_global_args_accepts_short_config_and_ignores_log_level() {
        let parsed = parse_global_args(strings(&[
            "teslausb",
            "-l",
            "debug",
            "-c",
            "/tmp/conf",
            "clean",
            "--dry-run",
        ]))
        .unwrap();

        assert_eq!(parsed.config_path, Some(PathBuf::from("/tmp/conf")));
        assert_eq!(parsed.command, "clean");
        assert_eq!(parsed.args, ["--dry-run"]);
    }

    #[test]
    fn parse_global_args_rejects_missing_config_value() {
        assert!(parse_global_args(strings(&["teslausb", "--config"])).is_err());
    }

    #[test]
    fn parse_optional_value_reads_values_and_rejects_missing_values() {
        let args = strings(&["--reserve", "10G", "--other"]);
        assert_eq!(
            parse_optional_value(&args, "--reserve").unwrap(),
            Some("10G".into())
        );
        assert_eq!(parse_optional_value(&args, "--missing").unwrap(), None);

        let missing = strings(&["--reserve"]);
        assert!(parse_optional_value(&missing, "--reserve").is_err());
    }

    #[test]
    fn has_flag_matches_exact_arguments() {
        let args = strings(&["--dry-run", "--jsonish"]);
        assert!(has_flag(&args, "--dry-run"));
        assert!(!has_flag(&args, "--json"));
    }

    #[test]
    fn json_helpers_escape_strings_and_format_optional_space() {
        assert_eq!(json_escape(r#"a"b\c"#), r#"a\"b\\c"#);
        assert_eq!(space_json(None), "null");
        assert_eq!(
            space_json(Some(crate::space::SpaceInfo {
                total_bytes: 2 * GB,
                free_bytes: GB,
                used_bytes: GB,
            })),
            r#"{ "total_gb": 2.00, "free_gb": 1.00, "used_gb": 1.00 }"#
        );
    }

    #[test]
    fn run_handles_help_version_and_unknown_without_external_tools() {
        assert_eq!(run(strings(&["teslausb", "--help"])).unwrap(), 0);
        assert_eq!(run(strings(&["teslausb", "--version"])).unwrap(), 0);
        assert!(run(strings(&["teslausb", "unknown"])).is_err());
    }

    #[test]
    fn gadget_and_service_require_subcommands() {
        assert_eq!(run(strings(&["teslausb", "gadget"])).unwrap(), 1);
        assert_eq!(run(strings(&["teslausb", "service"])).unwrap(), 1);
        assert!(run(strings(&["teslausb", "gadget", "bogus"])).is_err());
        assert!(run(strings(&["teslausb", "service", "bogus"])).is_err());
    }

    #[test]
    fn status_and_gadget_status_are_non_privileged_smoke_tests() {
        let config = temp_config("status", false);
        let config_s = config.display().to_string();

        assert_eq!(
            run(strings(&["teslausb", "--config", &config_s, "status"])).unwrap(),
            0
        );
        assert_eq!(
            run(strings(&[
                "teslausb", "--config", &config_s, "status", "--json"
            ]))
            .unwrap(),
            0
        );
        assert_eq!(run(strings(&["teslausb", "gadget", "status"])).unwrap(), 0);
        assert_eq!(
            run(strings(&["teslausb", "gadget", "status", "--json"])).unwrap(),
            0
        );

        let _ = fs::remove_dir_all(config.parent().unwrap());
    }

    #[test]
    fn init_and_deinit_handle_already_initialized_boundaries() {
        let config = temp_config("already-init", true);
        let config_s = config.display().to_string();

        assert_eq!(
            run(strings(&[
                "teslausb",
                "--config",
                &config_s,
                "init",
                "--reserve",
                "1G"
            ]))
            .unwrap(),
            1
        );
        assert_eq!(
            run(strings(&[
                "teslausb", "--config", &config_s, "deinit", "--yes"
            ]))
            .unwrap(),
            0
        );

        let _ = fs::remove_dir_all(config.parent().unwrap());
    }

    #[test]
    fn deinit_is_noop_when_not_initialized() {
        let config = temp_config("deinit-noop", false);
        let config_s = config.display().to_string();

        assert_eq!(
            run(strings(&[
                "teslausb", "--config", &config_s, "deinit", "--yes"
            ]))
            .unwrap(),
            0
        );

        let _ = fs::remove_dir_all(config.parent().unwrap());
    }
}
