use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
#[cfg(any(target_os = "linux", test))]
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::command::{CommandOutput, CommandRunner};
use crate::error::{Error, Result};
use crate::filesystem::RealFileSystem;
use crate::snapshot::{
    begin_recovery, latch_recovery_failure, prepare_recovery, release_recovery, RECOVERY_DIRECTORY,
};

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
    image: PathBuf,
    state: LoopState,
}

impl LoopDevice {
    pub fn partition(&self) -> &str {
        &self.loop_dev
    }

    pub fn detach(&mut self) -> Result<()> {
        if self.state == LoopState::Detached {
            return Ok(());
        }
        if self.state == LoopState::Attached {
            self.state = LoopState::Detaching;
            CommandRunner.check(
                "losetup",
                ["-d", self.loop_dev.as_str()],
                Some(Duration::from_secs(30)),
            )?;
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
            release_recovery(
                &RealFileSystem,
                path.parent()
                    .ok_or_else(|| Error::new("recovery directory has no catalog"))?,
            )?;
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
    let image_string = image.display().to_string();
    let table = CommandRunner.check(
        "parted",
        ["-sm", &image_string, "unit", "B", "print"],
        Some(Duration::from_secs(30)),
    )?;
    let (offset, size) = first_partition_range(&table.stdout, fs::metadata(&image)?.len())?;
    let offset = offset.to_string();
    let size = size.to_string();
    // Expose only the filesystem, so detach never rescans kernel partitions
    // while udev may still be probing them.
    let mut args = vec![
        "--find",
        "--show",
        "--offset",
        &offset,
        "--sizelimit",
        &size,
    ];
    if readonly {
        args.push("--read-only");
    }
    args.push(&image_string);
    let output = CommandRunner.check("losetup", args, Some(Duration::from_secs(30)))?;
    let loop_dev = output.stdout.trim().to_string();
    if loop_dev.is_empty() {
        return Err(Error::new("losetup did not print a loop device"));
    }
    Ok(LoopDevice {
        loop_dev,
        image,
        state: LoopState::Attached,
    })
}

fn first_partition_range(table: &str, image_size: u64) -> Result<(u64, u64)> {
    let mut lines = table.lines();
    if lines.next() != Some("BYT;") || lines.next().is_none() {
        return Err(Error::new(
            "parted did not report a byte-based partition table",
        ));
    }
    let mut partitions = lines.filter(|line| line.starts_with("1:"));
    let partition = partitions
        .next()
        .ok_or_else(|| Error::new("camera image has no first partition"))?;
    if partitions.next().is_some() {
        return Err(Error::new("camera image has multiple first partitions"));
    }
    let mut fields = partition.split(':').skip(1);
    let mut byte_field = || -> Result<u64> {
        fields
            .next()
            .and_then(|field| field.strip_suffix('B'))
            .ok_or_else(|| Error::new("invalid byte range for camera image first partition"))?
            .parse()
            .map_err(|error| Error::new(format!("invalid camera partition byte value: {error}")))
    };
    let offset = byte_field()?;
    let end = byte_field()?;
    let size = byte_field()?;
    let limit = offset
        .checked_add(size)
        .ok_or_else(|| Error::new("camera image first partition byte range overflows"))?;
    if offset == 0
        || size == 0
        || offset % 512 != 0
        || size % 512 != 0
        || limit > image_size
        || limit - 1 != end
    {
        return Err(Error::new(
            "camera image first partition has an invalid byte range",
        ));
    }
    Ok((offset, size))
}

fn validate_filesystem(value: &str) -> Result<()> {
    match value.trim() {
        "ext4" => Ok(()),
        value => Err(Error::new(format!(
            "unsupported camera filesystem {value:?}; ext4 is required; preserve the existing image and migrate it before using TeslaUSB"
        ))),
    }
}

fn ensure_ext4(partition: &str) -> Result<()> {
    let output = CommandRunner.check(
        "blkid",
        ["-p", "-s", "TYPE", "-o", "value", partition],
        Some(Duration::from_secs(30)),
    )?;
    validate_filesystem(&output.stdout)
}

fn check_partition(args: &[&str], repair: bool) -> Result<()> {
    let output = CommandRunner.run("e2fsck", args, Some(Duration::from_secs(120)))?;
    assess_check(&output, repair)
}

fn assess_check(output: &CommandOutput, repair: bool) -> Result<()> {
    let details = format!("{}\n{}", output.stdout.trim(), output.stderr.trim());
    if !details.trim().is_empty() {
        eprintln!("e2fsck: {}", details.trim());
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
    ensure_ext4(device.partition())?;
    check_partition(&["-p", device.partition()], true)?;
    check_partition(&["-f", "-n", device.partition()], false)?;
    device.detach()
}

pub fn mount_image(image_path: &Path) -> Result<MountedImage> {
    let device = setup_loop_device(image_path, false)?;
    ensure_ext4(device.partition())?;
    mount_partition(device, false)
}

fn mount_partition(device: LoopDevice, readonly: bool) -> Result<MountedImage> {
    let mount_point = temp_mount_point(SystemTime::now())?;
    fs::create_dir(&mount_point)?;
    let opts = if readonly { "ro,noload" } else { "rw" };
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
    ensure_ext4(device.partition())?;
    device.detach()?;
    let snapshots = image_path
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| Error::new("snapshot image has no catalog directory"))?;
    let recovery = snapshots.join(RECOVERY_DIRECTORY);
    let mut record = begin_recovery(&RealFileSystem, image_path)?;
    prepare_recovery(&RealFileSystem, snapshots, &mut record)?;
    let result = (|| -> Result<MountedImage> {
        let readonly = setup_loop_device(&recovery.join("recovered.bin"), true)?;
        mount_partition(readonly, true)
    })();
    match result {
        Ok(mut mounted) => {
            mounted.recovery_dir = Some(recovery);
            Ok(mounted)
        }
        Err(error) => Err(latch_recovery_failure(
            &RealFileSystem,
            snapshots,
            &mut record,
            error,
        )),
    }
}

pub(crate) fn recover_ext4_image(image_path: &Path) -> Result<()> {
    let mut writable = setup_loop_device(image_path, false)?;
    ensure_ext4(writable.partition())?;
    check_partition(&["-p", "-E", "journal_only", writable.partition()], true)?;
    check_partition(&["-f", "-n", writable.partition()], false)?;
    writable.detach()?;
    fs::File::open(image_path)?.sync_all()?;
    Ok(())
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
    fn filesystem_probe_requires_ext4_and_rejects_ambiguous_types() {
        validate_filesystem("ext4\n").unwrap();
        for value in [
            "",
            "vfat",
            "fat32",
            "exfat",
            "ext3",
            "vfat\next4",
            "ext4\nvfat",
        ] {
            let error = validate_filesystem(value).unwrap_err().to_string();
            assert!(error.contains("ext4 is required"));
            assert!(error.contains("preserve the existing image"));
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
            assert_eq!(assess_check(&output, true).is_ok(), matches!(code, 0 | 1));
            assert_eq!(assess_check(&output, false).is_ok(), code == 0);
            if code > 1 {
                assert!(assess_check(&output, true)
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
        assert!(assess_check(&timeout, true).is_err());
        assert!(assess_check(&timeout, false).is_err());
    }

    #[test]
    fn loop_device_exposes_only_the_partition_byte_range() {
        let device = LoopDevice {
            loop_dev: "/dev/null".into(),
            state: LoopState::Detached,
            image: PathBuf::from("/dev/null"),
        };
        assert_eq!(device.partition(), "/dev/null");
    }

    #[test]
    fn confirmed_detach_never_rechecks_a_reused_device() {
        let mut device = LoopDevice {
            // Neither a detach command nor a sysfs probe can use this device path.
            loop_dev: "/".into(),
            state: LoopState::Detached,
            image: PathBuf::from("/missing/recovered.bin"),
        };
        device.detach().unwrap();
        device.detach().unwrap();
        assert_eq!(device.state, LoopState::Detached);
    }

    #[test]
    fn partition_ranges_support_mbr_and_gpt_without_kernel_partition_devices() {
        for label in ["msdos", "gpt"] {
            let table =
                format!("BYT;\n/disk:4096B:file:512:512:{label}::;\n1:512B:3583B:3072B:ext4::;\n");
            assert_eq!(first_partition_range(&table, 4096).unwrap(), (512, 3072));
        }
    }

    #[test]
    fn partition_ranges_reject_missing_ambiguous_or_invalid_ranges() {
        let header = "BYT;\n/disk:4096B:file:512:512:msdos::;\n";
        for row in [
            "",
            "2:512B:1023B:512B:::;\n",
            "1:512B:1023B:512B:::;\n1:1024B:1535B:512B:::;\n",
            "1:512s:1023B:512B:::;\n",
            "1:invalidB:1023B:512B:::;\n",
            "1:0B:511B:512B:::;\n",
            "1:512B:511B:0B:::;\n",
            "1:513B:1024B:512B:::;\n",
            "1:512B:1024B:513B:::;\n",
            "1:512B:1024B:512B:::;\n",
            "1:512B:4607B:4096B:::;\n",
            "1:18446744073709551104B:0B:512B:::;\n",
        ] {
            assert!(
                first_partition_range(&format!("{header}{row}"), 4096).is_err(),
                "accepted {row:?}"
            );
        }
        for table in ["", "BYT;", "CHS;\n/disk\n1:512B:1023B:512B:::;\n"] {
            assert!(first_partition_range(table, 4096).is_err());
        }
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
