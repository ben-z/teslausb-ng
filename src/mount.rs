use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::command::CommandRunner;
use crate::error::{Error, Result};

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
    _loop_device: LoopDevice,
}

impl MountedImage {
    pub fn path(&self) -> &Path {
        &self.mount_point
    }
}

impl Drop for MountedImage {
    fn drop(&mut self) {
        let runner = CommandRunner;
        if !self.readonly {
            let _ = runner.run(
                "sync",
                std::iter::empty::<&str>(),
                Some(Duration::from_secs(30)),
            );
        }
        let output = runner.run(
            "umount",
            [self.mount_point.display().to_string().as_str()],
            Some(Duration::from_secs(30)),
        );
        if let Ok(output) = output {
            if !output.success() {
                eprintln!(
                    "warning: umount {} failed: {}",
                    self.mount_point.display(),
                    output.last_error_line()
                );
            }
        }
        let _ = fs::remove_dir(&self.mount_point);
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

    let direct_partition = format!("{}p1", loop_dev);
    let _ = runner.run(
        "blockdev",
        ["--rereadpt", loop_dev.as_str()],
        Some(Duration::from_secs(10)),
    );
    if wait_for_path(Path::new(&direct_partition), Duration::from_secs(2)) {
        return Ok(LoopDevice {
            loop_dev,
            partition: direct_partition,
            kpartx_used: false,
        });
    }

    let output = runner.run(
        "kpartx",
        ["-av", loop_dev.as_str()],
        Some(Duration::from_secs(30)),
    )?;
    if output.success() {
        let loop_name = Path::new(&loop_dev)
            .file_name()
            .map(|name| name.to_string_lossy().to_string())
            .unwrap_or_else(|| loop_dev.trim_start_matches("/dev/").to_string());
        let mapper_partition = format!("/dev/mapper/{}p1", loop_name);
        if wait_for_path(Path::new(&mapper_partition), Duration::from_secs(2)) {
            return Ok(LoopDevice {
                loop_dev,
                partition: mapper_partition,
                kpartx_used: true,
            });
        }
    }

    let _ = runner.run(
        "losetup",
        ["-d", loop_dev.as_str()],
        Some(Duration::from_secs(30)),
    );
    Err(Error::new(format!(
        "partition device did not appear for {}",
        image_path.display()
    )))
}

pub fn fsck_image(image_path: &Path) -> Result<bool> {
    let loop_device = setup_loop_device(image_path)?;
    let output = CommandRunner.run(
        "fsck",
        ["-p", loop_device.partition()],
        Some(Duration::from_secs(120)),
    )?;
    Ok(matches!(output.code, Some(0 | 1)) && !output.timed_out)
}

pub fn mount_image(image_path: &Path, readonly: bool) -> Result<MountedImage> {
    let loop_device = setup_loop_device(image_path)?;
    let mount_point = temp_mount_point();
    fs::create_dir_all(&mount_point)?;
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
        _loop_device: loop_device,
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

fn temp_mount_point() -> PathBuf {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    std::env::temp_dir().join(format!("teslausb-mount-{}-{}", std::process::id(), suffix))
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
        let first = temp_mount_point();
        let second = temp_mount_point();
        assert!(first.starts_with(std::env::temp_dir()));
        assert_ne!(first, second);
    }
}
