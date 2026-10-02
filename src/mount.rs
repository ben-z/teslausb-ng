use std::fs;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::command::{CommandOutput, CommandRunner};
use crate::config::CameraFilesystem;
use crate::error::{Error, Result};
use crate::snapshot::RECOVERY_DIRECTORY;

static MOUNT_COUNTER: Mutex<u64> = Mutex::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LoopState {
    Attached,
    Detaching,
    Detached,
}

#[derive(Debug)]
pub struct LoopDevice {
    loop_dev: String,
    partition: String,
    image: PathBuf,
    kpartx_used: bool,
    state: LoopState,
}

impl LoopDevice {
    pub fn partition(&self) -> &str {
        &self.partition
    }

    pub fn detach(&mut self) -> Result<()> {
        if self.state == LoopState::Detached {
            return Ok(());
        }
        if self.state == LoopState::Attached {
            if self.kpartx_used {
                CommandRunner.check(
                    "kpartx",
                    ["-d", self.loop_dev.as_str()],
                    Some(Duration::from_secs(30)),
                )?;
                self.kpartx_used = false;
            }
            CommandRunner.check(
                "losetup",
                ["-d", self.loop_dev.as_str()],
                Some(Duration::from_secs(30)),
            )?;
            self.state = LoopState::Detaching;
        }
        self.wait_detached()?;
        self.state = LoopState::Detached;
        Ok(())
    }

    fn wait_detached(&self) -> Result<()> {
        #[cfg(target_os = "linux")]
        {
            let name = Path::new(&self.loop_dev)
                .file_name()
                .ok_or_else(|| Error::new("invalid loop device"))?;
            let backing = Path::new("/sys/class/block")
                .join(name)
                .join("loop/backing_file");
            let started = std::time::Instant::now();
            loop {
                match fs::read_to_string(&backing) {
                    Ok(path) if loop_backs_image(&path, &self.image) => {}
                    Ok(_) => return Ok(()),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                    Err(error) => return Err(error.into()),
                }
                if started.elapsed() >= Duration::from_secs(30) {
                    return Err(Error::new(format!(
                        "loop device {} still holds {} after detach",
                        self.loop_dev,
                        self.image.display()
                    )));
                }
                thread::sleep(Duration::from_millis(100));
            }
        }
        #[cfg(not(target_os = "linux"))]
        Ok(())
    }
}

#[cfg(any(target_os = "linux", test))]
fn loop_backs_image(backing: &str, image: &Path) -> bool {
    let backing = backing
        .trim_end_matches('\n')
        .replace("\\040", " ")
        .replace("\\011", "\t")
        .replace("\\012", "\n")
        .replace("\\134", "\\");
    Path::new(&backing) == image
        || backing
            .strip_suffix(" (deleted)")
            .is_some_and(|path| Path::new(path) == image)
}

impl Drop for LoopDevice {
    fn drop(&mut self) {
        if let Err(error) = self.detach() {
            eprintln!(
                "error: failed to detach {} from {}: {error}",
                self.loop_dev,
                self.image.display()
            );
        }
    }
}

#[derive(Debug)]
pub struct MountedImage {
    mount_point: PathBuf,
    readonly: bool,
    mounted: bool,
    loop_device: Option<LoopDevice>,
    recovery_dir: Option<PathBuf>,
}

impl MountedImage {
    pub fn path(&self) -> &Path {
        &self.mount_point
    }

    pub fn unmount(mut self) -> Result<()> {
        self.unmount_inner()
    }

    fn unmount_inner(&mut self) -> Result<()> {
        if self.mounted {
            if !self.readonly {
                CommandRunner.check(
                    "sync",
                    std::iter::empty::<&str>(),
                    Some(Duration::from_secs(30)),
                )?;
            }
            CommandRunner.check(
                "umount",
                [self.mount_point.display().to_string()],
                Some(Duration::from_secs(30)),
            )?;
            self.mounted = false;
        }
        if let Some(device) = self.loop_device.as_mut() {
            device.detach()?;
            self.loop_device.take();
        }
        match fs::remove_dir(&self.mount_point) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        if let Some(path) = &self.recovery_dir {
            fs::remove_file(path.join("recovered.bin"))?;
            fs::remove_file(path.join("raw.bin"))?;
            fs::remove_dir(path)?;
            self.recovery_dir = None;
        }
        Ok(())
    }
}

impl Drop for MountedImage {
    fn drop(&mut self) {
        if let Err(error) = self.unmount_inner() {
            eprintln!(
                "error: failed to release mount {}: {error}; recovery files retained at {:?}",
                self.mount_point.display(),
                self.recovery_dir
            );
            // Keep the loop mapping attached when the filesystem is still mounted.
            if self.mounted {
                if let Some(device) = self.loop_device.take() {
                    std::mem::forget(device);
                }
            }
        }
    }
}

pub fn setup_loop_device(image_path: &Path, readonly: bool) -> Result<LoopDevice> {
    let image = fs::canonicalize(image_path)?;
    let mut args = vec!["-Pf", "--show"];
    if readonly {
        args.push("--read-only");
    }
    let image_string = image.display().to_string();
    args.push(&image_string);
    let output = CommandRunner.check("losetup", args, Some(Duration::from_secs(30)))?;
    let loop_dev = output.stdout.trim().to_string();
    if loop_dev.is_empty() {
        return Err(Error::new("losetup did not print a loop device"));
    }
    let mut device = LoopDevice {
        partition: format!("{}p1", loop_dev),
        loop_dev,
        image,
        kpartx_used: false,
        state: LoopState::Attached,
    };
    if wait_for_path(Path::new(&device.partition), Duration::from_secs(2)) {
        return Ok(device);
    }
    CommandRunner.check(
        "kpartx",
        [
            if readonly { "-arv" } else { "-av" },
            device.loop_dev.as_str(),
        ],
        Some(Duration::from_secs(30)),
    )?;
    device.kpartx_used = true;
    let loop_name = Path::new(&device.loop_dev)
        .file_name()
        .ok_or_else(|| Error::new("invalid loop-device path"))?
        .to_string_lossy();
    device.partition = format!("/dev/mapper/{}p1", loop_name);
    if wait_for_path(Path::new(&device.partition), Duration::from_secs(2)) {
        return Ok(device);
    }
    Err(Error::new(format!(
        "partition device did not appear for {}",
        image_path.display()
    )))
}

fn parse_filesystem(value: &str) -> Result<CameraFilesystem> {
    match value.trim() {
        "vfat" => Ok(CameraFilesystem::Fat32),
        "ext4" => Ok(CameraFilesystem::Ext4),
        value => Err(Error::new(format!(
            "unsupported camera filesystem {value:?}; expected vfat or ext4"
        ))),
    }
}

fn detect_filesystem(partition: &str) -> Result<CameraFilesystem> {
    let output = CommandRunner.check(
        "blkid",
        ["-p", "-s", "TYPE", "-o", "value", partition],
        Some(Duration::from_secs(30)),
    )?;
    parse_filesystem(&output.stdout)
}

fn check_partition(program: &str, args: &[&str], repair: bool) -> Result<()> {
    let output = CommandRunner.run(program, args, Some(Duration::from_secs(120)))?;
    assess_check(program, &output, repair)
}

fn assess_check(program: &str, output: &CommandOutput, repair: bool) -> Result<()> {
    let details = format!("{}\n{}", output.stdout.trim(), output.stderr.trim());
    if !details.trim().is_empty() {
        eprintln!("{program}: {}", details.trim());
    }
    if output.timed_out || !(output.code == Some(0) || repair && output.code == Some(1)) {
        let reason = if repair {
            "filesystem check failed"
        } else {
            "filesystem remains inconsistent after repair"
        };
        return Err(Error::new(format!(
            "{reason} (exit {:?}, timed out {}): {}",
            output.code,
            output.timed_out,
            details.trim()
        )));
    }
    Ok(())
}

pub fn fsck_image(image_path: &Path) -> Result<()> {
    #[cfg(target_os = "linux")]
    crate::gadget::ensure_image_unmounted(
        image_path,
        Path::new("/sys/class/block"),
        Path::new("/proc/self/mountinfo"),
    )?;
    let mut device = setup_loop_device(image_path, false)?;
    match detect_filesystem(device.partition())? {
        CameraFilesystem::Fat32 => {
            check_partition("fsck", &["-p", device.partition()], true)?;
            check_partition("fsck", &["-n", device.partition()], false)?;
        }
        CameraFilesystem::Ext4 => {
            check_partition("e2fsck", &["-p", device.partition()], true)?;
            check_partition("e2fsck", &["-f", "-n", device.partition()], false)?;
        }
    }
    device.detach()
}

pub fn mount_image(image_path: &Path, readonly: bool) -> Result<MountedImage> {
    let device = setup_loop_device(image_path, readonly)?;
    let filesystem = detect_filesystem(device.partition())?;
    if readonly && filesystem == CameraFilesystem::Ext4 {
        return Err(Error::new(
            "ext4 read-only mounts require a recovered snapshot",
        ));
    }
    mount_partition(device, filesystem, readonly)
}

fn mount_partition(
    device: LoopDevice,
    filesystem: CameraFilesystem,
    readonly: bool,
) -> Result<MountedImage> {
    let mount_point = temp_mount_point(SystemTime::now())?;
    fs::create_dir(&mount_point)?;
    let opts = match (filesystem, readonly) {
        (CameraFilesystem::Ext4, true) => "ro,noload",
        (_, true) => "ro",
        (_, false) => "rw",
    };
    let partition = device.partition().to_string();
    let mut mounted = MountedImage {
        mount_point,
        readonly,
        mounted: false,
        loop_device: Some(device),
        recovery_dir: None,
    };
    if let Err(error) = CommandRunner.check(
        "mount",
        [
            "-o",
            opts,
            &partition,
            &mounted.mount_point.display().to_string(),
        ],
        Some(Duration::from_secs(30)),
    ) {
        let probe = CommandRunner
            .run(
                "mountpoint",
                ["-q", &mounted.mount_point.display().to_string()],
                Some(Duration::from_secs(30)),
            )
            .and_then(|output| {
                if !output.timed_out {
                    match output.code {
                        Some(0) => return Ok(true),
                        Some(32) => return Ok(false),
                        _ => {}
                    }
                }
                Err(Error::new(format!(
                    "mountpoint probe failed: {}",
                    output.last_error_line()
                )))
            });
        match probe {
            Ok(is_mounted) => mounted.mounted = is_mounted,
            Err(probe_error) => {
                let path = mounted.mount_point.display().to_string();
                // Unknown mount ownership must retain the loop and backing image.
                std::mem::forget(mounted);
                return Err(Error::new(format!(
                    "{error}; {probe_error}; mount and loop retained at {path}"
                )));
            }
        }
        mounted.unmount_inner().map_err(|cleanup| {
            Error::new(format!(
                "{error}; failed to release partial mount: {cleanup}"
            ))
        })?;
        return Err(error.context("mount failed"));
    }
    mounted.mounted = true;
    Ok(mounted)
}

pub fn mount_snapshot(image_path: &Path) -> Result<MountedImage> {
    let mut device = setup_loop_device(image_path, true)?;
    let filesystem = detect_filesystem(device.partition())?;
    if filesystem == CameraFilesystem::Fat32 {
        return mount_partition(device, filesystem, true);
    }
    device.detach()?;
    let snapshots = image_path
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| Error::new("snapshot image has no catalog directory"))?;
    let recovery = snapshots.join(RECOVERY_DIRECTORY);
    fs::DirBuilder::new()
        .mode(0o700)
        .create(&recovery)
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                Error::new(format!(
                    "snapshot recovery evidence already exists at {}; inspect retained files before archiving again",
                    recovery.display()
                ))
            } else {
                Error::new(format!(
                    "failed to create recovery directory {}: {error}",
                    recovery.display()
                ))
            }
        })?;
    let recovered = recovery.join("recovered.bin");
    let result = (|| -> Result<MountedImage> {
        fs::hard_link(image_path, recovery.join("raw.bin"))?;
        fs::File::open(&recovery)?.sync_all()?;
        fs::File::open(snapshots)?.sync_all()?;
        fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&recovered)?;
        CommandRunner.check(
            "cp",
            [
                "--reflink=always",
                "--",
                &image_path.display().to_string(),
                &recovered.display().to_string(),
            ],
            Some(Duration::from_secs(120)),
        )?;
        let mut writable = setup_loop_device(&recovered, false)?;
        check_partition(
            "e2fsck",
            &["-p", "-E", "journal_only", writable.partition()],
            true,
        )?;
        check_partition("e2fsck", &["-f", "-n", writable.partition()], false)?;
        writable.detach()?;
        fs::File::open(&recovered)?.sync_all()?;
        let readonly = setup_loop_device(&recovered, true)?;
        mount_partition(readonly, CameraFilesystem::Ext4, true)
    })();
    match result {
        Ok(mut mounted) => {
            mounted.recovery_dir = Some(recovery);
            Ok(mounted)
        }
        Err(error) => Err(error.context(format!(
            "snapshot recovery failed; raw input and recovery copy retained at {}",
            recovery.display()
        ))),
    }
}

fn wait_for_path(path: &Path, timeout: Duration) -> bool {
    let started = std::time::Instant::now();
    while started.elapsed() < timeout {
        if path.exists() {
            return true;
        }
        thread::sleep(Duration::from_millis(100));
    }
    path.exists()
}

fn temp_mount_point(timestamp: SystemTime) -> Result<PathBuf> {
    let suffix = timestamp
        .duration_since(UNIX_EPOCH)
        .map_err(|error| Error::new(format!("invalid temporary mount timestamp: {error}")))?
        .as_nanos();
    let mut counter = MOUNT_COUNTER
        .lock()
        .map_err(|_| Error::new("temporary mount counter poisoned"))?;
    let id = *counter;
    *counter = counter
        .checked_add(1)
        .ok_or_else(|| Error::new("temporary mount identifiers exhausted"))?;
    Ok(std::env::temp_dir().join(format!(
        "teslausb-mount-{}-{suffix}-{id}",
        std::process::id()
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loop_ownership_handles_escaped_paths_and_deleted_backing_files() {
        let image = Path::new("/snapshots/event \t\n\\040 /recovered.bin");
        let encoded = "/snapshots/event\\040\\011\\012\\134040\\040/recovered.bin";
        assert!(loop_backs_image(&format!("{encoded}\n"), image));
        assert!(loop_backs_image(&format!("{encoded} (deleted)\n"), image));
        assert!(!loop_backs_image(&format!("{encoded}.other\n"), image));
        assert!(loop_backs_image(
            "/path/with space \n",
            Path::new("/path/with space ")
        ));
        assert!(loop_backs_image(
            "/path/a (deleted)\n",
            Path::new("/path/a (deleted)")
        ));
    }

    #[test]
    fn filesystem_probe_rejects_unknown_and_ambiguous_types() {
        assert_eq!(parse_filesystem("vfat\n").unwrap(), CameraFilesystem::Fat32);
        assert_eq!(parse_filesystem("ext4\n").unwrap(), CameraFilesystem::Ext4);
        for value in ["", "exfat", "ext3", "vfat\next4"] {
            assert!(parse_filesystem(value).is_err());
        }
    }

    #[test]
    fn repair_and_verification_fail_closed_on_fsck_exit_flags() {
        for code in [0, 1, 2, 3, 4, 5, 8, 16, 32, 128] {
            let output = CommandOutput {
                code: Some(code),
                stdout: "specific filesystem problem".into(),
                stderr: String::new(),
                timed_out: false,
            };
            assert_eq!(
                assess_check("e2fsck", &output, true).is_ok(),
                matches!(code, 0 | 1)
            );
            assert_eq!(assess_check("e2fsck", &output, false).is_ok(), code == 0);
            if code > 1 {
                assert!(assess_check("e2fsck", &output, true)
                    .unwrap_err()
                    .to_string()
                    .contains("specific filesystem problem"));
            }
        }
        let timeout = CommandOutput {
            code: Some(0),
            stdout: String::new(),
            stderr: String::new(),
            timed_out: true,
        };
        assert!(assess_check("e2fsck", &timeout, true).is_err());
        assert!(assess_check("e2fsck", &timeout, false).is_err());
    }

    #[test]
    fn loop_device_returns_partition_path() {
        let device = LoopDevice {
            loop_dev: "/dev/null".into(),
            partition: "/dev/nullp1".into(),
            kpartx_used: false,
            state: LoopState::Detached,
            image: PathBuf::from("/dev/null"),
        };
        assert_eq!(device.partition(), "/dev/nullp1");
    }

    #[test]
    fn confirmed_detach_never_rechecks_a_reused_device() {
        let mut device = LoopDevice {
            // Neither a detach command nor a sysfs probe can use this device path.
            loop_dev: "/".into(),
            partition: String::new(),
            kpartx_used: true,
            state: LoopState::Detached,
            image: PathBuf::from("/missing/recovered.bin"),
        };
        device.detach().unwrap();
        device.detach().unwrap();
        assert_eq!(device.state, LoopState::Detached);
    }

    #[test]
    fn wait_for_path_detects_existing_and_missing_paths() {
        let existing = std::env::temp_dir().join(format!(
            "teslausb-wait-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::write(&existing, b"x").unwrap();

        assert!(wait_for_path(&existing, Duration::from_millis(1)));
        let _ = fs::remove_file(&existing);
        assert!(!wait_for_path(&existing, Duration::from_millis(1)));
    }

    #[test]
    fn temp_mount_points_are_under_temp_and_unique() {
        let timestamp = UNIX_EPOCH + Duration::from_secs(1);
        let mut paths = std::collections::HashSet::new();
        for _ in 0..256 {
            let path = temp_mount_point(timestamp).unwrap();
            assert_eq!(path.parent().unwrap(), std::env::temp_dir());
            assert!(paths.insert(path));
        }
        let workers = (0..8)
            .map(|_| {
                thread::spawn(move || {
                    (0..256)
                        .map(|_| temp_mount_point(timestamp).unwrap())
                        .collect::<Vec<_>>()
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            for path in worker.join().unwrap() {
                assert_eq!(path.parent().unwrap(), std::env::temp_dir());
                assert!(paths.insert(path));
            }
        }
        assert_eq!(paths.len(), 2304);
        assert!(temp_mount_point(UNIX_EPOCH - Duration::from_secs(1)).is_err());
    }
}
