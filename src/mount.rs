use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::command::CommandRunner;
use crate::error::{Error, Result};

static MOUNT_COUNTER: Mutex<u64> = Mutex::new(0);

#[derive(Debug)]
pub struct LoopDevice {
    loop_dev: String,
    partition: String,
    kpartx_used: bool,
}

impl LoopDevice {
    pub fn partition(&self) -> &str {
        &self.partition
    }
}

impl Drop for LoopDevice {
    fn drop(&mut self) {
        let runner = CommandRunner;
        if self.kpartx_used {
            let _ = runner.run(
                "kpartx",
                ["-d", self.loop_dev.as_str()],
                Some(Duration::from_secs(30)),
            );
        }
        let output = runner.run(
            "losetup",
            ["-d", self.loop_dev.as_str()],
            Some(Duration::from_secs(30)),
        );
        if let Ok(output) = output {
            if !output.success() {
                eprintln!(
                    "warning: losetup -d {} failed: {}",
                    self.loop_dev,
                    output.last_error_line()
                );
            }
        }
    }
}

#[derive(Debug)]
pub struct MountedImage {
    mount_point: PathBuf,
    readonly: bool,
    loop_device: Option<LoopDevice>,
}

impl MountedImage {
    pub fn path(&self) -> &Path {
        &self.mount_point
    }

    pub fn unmount(mut self) -> Result<()> {
        self.unmount_inner()
    }

    fn unmount_inner(&mut self) -> Result<()> {
        if self.loop_device.is_none() {
            return Ok(());
        }
        if !self.readonly {
            CommandRunner.check(
                "sync",
                std::iter::empty::<&str>(),
                Some(Duration::from_secs(30)),
            )?;
        }
        CommandRunner.check(
            "umount",
            [self.mount_point.display().to_string().as_str()],
            Some(Duration::from_secs(30)),
        )?;
        drop(self.loop_device.take());
        if self.mount_point.exists() {
            fs::remove_dir(&self.mount_point)?;
        }
        Ok(())
    }
}

impl Drop for MountedImage {
    fn drop(&mut self) {
        if let Err(err) = self.unmount_inner() {
            eprintln!(
                "error: failed to unmount {}: {}",
                self.mount_point.display(),
                err
            );
            // Keep the loop mapping attached when the filesystem is still mounted.
            if let Some(device) = self.loop_device.take() {
                std::mem::forget(device);
            }
        }
    }
}

pub fn setup_loop_device(image_path: &Path) -> Result<LoopDevice> {
    let runner = CommandRunner;
    let image = image_path.display().to_string();
    let output = runner.check(
        "losetup",
        ["-Pf", "--show", image.as_str()],
        Some(Duration::from_secs(30)),
    )?;
    let loop_dev = output.stdout.trim().to_string();
    if loop_dev.is_empty() {
        return Err(Error::new("losetup did not print a loop device"));
    }

    let mut device = LoopDevice {
        partition: format!("{}p1", loop_dev),
        loop_dev,
        kpartx_used: false,
    };
    let _ = runner.run(
        "blockdev",
        ["--rereadpt", device.loop_dev.as_str()],
        Some(Duration::from_secs(10)),
    );
    if wait_for_path(Path::new(&device.partition), Duration::from_secs(2)) {
        return Ok(device);
    }
    runner.check(
        "kpartx",
        ["-av", device.loop_dev.as_str()],
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

pub fn fsck_image(image_path: &Path) -> Result<()> {
    #[cfg(target_os = "linux")]
    crate::gadget::ensure_image_unmounted(
        image_path,
        Path::new("/sys/class/block"),
        Path::new("/proc/self/mountinfo"),
    )?;
    let loop_device = setup_loop_device(image_path)?;
    let output = CommandRunner.run(
        "fsck",
        ["-p", loop_device.partition()],
        Some(Duration::from_secs(120)),
    )?;
    if !output.stdout.trim().is_empty() {
        eprintln!("fsck repair: {}", output.stdout.trim());
    }
    if !output.stderr.trim().is_empty() {
        eprintln!("fsck repair: {}", output.stderr.trim());
    }
    if !matches!(output.code, Some(0 | 1)) || output.timed_out {
        return Err(Error::new(format!(
            "filesystem check failed for {} (exit {:?}, timed out {}): {}",
            image_path.display(),
            output.code,
            output.timed_out,
            output.last_error_line()
        )));
    }
    let verification = CommandRunner.run(
        "fsck",
        ["-n", loop_device.partition()],
        Some(Duration::from_secs(120)),
    )?;
    if !verification.stdout.trim().is_empty() {
        eprintln!("fsck verification: {}", verification.stdout.trim());
    }
    if !verification.stderr.trim().is_empty() {
        eprintln!("fsck verification: {}", verification.stderr.trim());
    }
    if !verification.success() {
        return Err(Error::new(format!(
            "filesystem remains inconsistent after repair for {}: {}",
            image_path.display(),
            verification.last_error_line()
        )));
    }
    Ok(())
}

pub fn mount_image(image_path: &Path, readonly: bool) -> Result<MountedImage> {
    let loop_device = setup_loop_device(image_path)?;
    let mount_point = temp_mount_point(SystemTime::now())?;
    fs::create_dir(&mount_point)?;
    let opts = if readonly { "ro" } else { "rw" };
    let output = CommandRunner.run(
        "mount",
        [
            "-o",
            opts,
            loop_device.partition(),
            &mount_point.display().to_string(),
        ],
        Some(Duration::from_secs(30)),
    )?;
    if !output.success() {
        let _ = fs::remove_dir(&mount_point);
        return Err(Error::new(format!(
            "mount failed: {}",
            output.last_error_line()
        )));
    }
    Ok(MountedImage {
        mount_point,
        readonly,
        loop_device: Some(loop_device),
    })
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
    fn loop_device_returns_partition_path() {
        let device = LoopDevice {
            loop_dev: "/dev/null".into(),
            partition: "/dev/nullp1".into(),
            kpartx_used: false,
        };
        assert_eq!(device.partition(), "/dev/nullp1");
        std::mem::forget(device);
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
