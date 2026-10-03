#![cfg(unix)]

use std::env;
use std::ffi::OsString;
use std::fs;
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct Harness {
    root: PathBuf,
    bin: PathBuf,
    state: PathBuf,
    mutable: PathBuf,
    backingfiles: PathBuf,
    cam_source: PathBuf,
    config: PathBuf,
    service_path: PathBuf,
    old_path: OsString,
}

impl Harness {
    fn new(archive_system: &str) -> Self {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let counter = COUNTER.fetch_add(1, Ordering::SeqCst);
        let root = env::temp_dir().join(format!(
            "teslausb-offline-{}-{counter}-{suffix}",
            std::process::id()
        ));
        let bin = root.join("bin");
        let state = root.join("state");
        let mutable = root.join("mutable");
        let backingfiles = root.join("backingfiles");
        let cam_source = root.join("cam-source");
        let config = root.join("teslausb.conf");
        let service_path = root.join("systemd/teslausb.service");

        fs::create_dir_all(&bin).unwrap();
        fs::create_dir_all(&state).unwrap();
        fs::create_dir_all(root.join("proc")).unwrap();
        fs::create_dir_all(&mutable).unwrap();
        fs::create_dir_all(&backingfiles).unwrap();
        fs::create_dir_all(service_path.parent().unwrap()).unwrap();
        write_fake_tools(&bin);
        write_cam_fixture(&cam_source);

        let mut config_content = format!(
            "MUTABLE_PATH={}\nBACKINGFILES_PATH={}\nARCHIVE_SYSTEM={archive_system}\nEVENT_STABILITY_SECONDS=0\n",
            mutable.display(),
            backingfiles.display()
        );
        if archive_system == "rclone" {
            config_content.push_str(
                "RCLONE_DRIVE=fake\nRCLONE_PATH=TeslaArchive\nRCLONE_FLAGS=--fast-list\n",
            );
        }
        fs::write(&config, config_content).unwrap();

        Self {
            root,
            bin,
            state,
            mutable,
            backingfiles,
            cam_source,
            config,
            service_path,
            old_path: env::var_os("PATH").unwrap_or_default(),
        }
    }

    fn run(&self, args: &[&str]) -> Output {
        self.run_with_env(args, &[])
    }

    fn run_with_env(&self, args: &[&str], extra_env: &[(&str, &str)]) -> Output {
        let mut path = OsString::from(&self.bin);
        path.push(":");
        path.push(&self.old_path);

        let mut command = Command::new(env!("CARGO_BIN_EXE_teslausb"));
        command
            .args(args)
            .env_remove("CAM_FILESYSTEM")
            .env("PATH", path)
            .env("TESLAUSB_FAKE_STATE", &self.state)
            .env("TESLAUSB_PROC_PATH", self.root.join("proc"))
            .env("TESLAUSB_FAKE_CAM_SOURCE", &self.cam_source)
            .env("TESLAUSB_SYSTEMD_SERVICE_PATH", &self.service_path)
            .stdin(Stdio::null());
        for (key, value) in extra_env {
            command.env(key, value);
        }
        command.output().unwrap()
    }

    fn spawn_with_env(&self, args: &[&str], extra_env: &[(&str, &str)]) -> Child {
        let mut path = OsString::from(&self.bin);
        path.push(":");
        path.push(&self.old_path);

        let mut command = Command::new(env!("CARGO_BIN_EXE_teslausb"));
        command
            .args(args)
            .env_remove("CAM_FILESYSTEM")
            .env("PATH", path)
            .env("TESLAUSB_FAKE_STATE", &self.state)
            .env("TESLAUSB_PROC_PATH", self.root.join("proc"))
            .env("TESLAUSB_FAKE_CAM_SOURCE", &self.cam_source)
            .env("TESLAUSB_SYSTEMD_SERVICE_PATH", &self.service_path)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for (key, value) in extra_env {
            command.env(key, value);
        }
        command.spawn().unwrap()
    }

    fn config_arg(&self) -> String {
        self.config.display().to_string()
    }

    fn command_log(&self) -> String {
        fs::read_to_string(self.state.join("commands.log")).unwrap_or_default()
    }

    fn archive_path(&self, relative: &str) -> PathBuf {
        self.state.join("archive/fake:TeslaArchive").join(relative)
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

#[test]
fn offline_init_mount_status_doctor_and_deinit() {
    let harness = Harness::new("none");
    let config = harness.config_arg();

    assert_success(&harness.run(&["--config", &config, "init", "--reserve", "20G"]));
    assert!(harness.mutable.join("backingfiles.img").is_file());
    assert!(harness.backingfiles.join("cam_disk.bin").is_file());
    assert!(harness.backingfiles.join("snapshots").is_dir());

    assert_success(&harness.run(&["--config", &config, "mount"]));

    let status = harness.run(&["--config", &config, "status", "--json"]);
    assert_success(&status);
    let status_json = stdout(&status);
    assert!(status_json.contains("\"backingfiles_mounted\": true"));
    assert!(status_json.contains("\"snapshots\": { \"count\": 0, \"deletable\": 0 }"));
    assert!(status_json.contains("\"system\": \"none\""));

    let doctor = harness.run(&["--config", &config, "doctor"]);
    assert_success(&doctor);
    assert!(stdout(&doctor).contains("mkfs.xfs"));
    assert!(stdout(&doctor).contains("6.1.0"));
    assert!(stdout(&doctor).contains("supports --reflink"));

    let deinit = harness.run(&["--config", &config, "deinit", "--yes"]);
    assert_success(&deinit);
    assert!(!harness.mutable.join("backingfiles.img").exists());

    let log = harness.command_log();
    for expected in [
        "df\t-Pk",
        "truncate\t-s",
        "mkfs.xfs\t-f",
        "parted\t-s",
        "losetup\t-Pf\t--show",
        "mkfs.ext4\t-F\t-b\t4096\t-I\t256\t-m\t0",
        "mount\t-o\tloop",
        "stat\t-f\t-c\t%T",
        "umount",
    ] {
        assert!(
            log.contains(expected),
            "missing command log entry {expected:?}\n{log}"
        );
    }
}

#[test]
fn offline_archive_recovers_ext4_privately_before_readonly_mount() {
    let harness = Harness::new("rclone");
    let config = harness.config_arg();
    assert_success(&harness.run(&["--config", &config, "init", "--reserve", "20G"]));
    assert!(harness.command_log().contains("mkfs.ext4\t-F"));
    fs::write(harness.state.join("commands.log"), "").unwrap();
    assert_success(&harness.run(&["--config", &config, "archive"]));
    let log = harness.command_log();
    for command in [
        "blkid\t-p\t-s\tTYPE\t-o\tvalue",
        "e2fsck\t-p\t-E\tjournal_only",
        "e2fsck\t-f\t-n",
        "losetup\t-Pf\t--show\t--read-only",
        "mount\t-o\tro,noload",
    ] {
        assert!(log.contains(command), "missing {command:?}\n{log}");
    }
    assert!(!log.contains("mkfs.ext4\t-F"));
    assert!(!log.contains("mkfs.vfat\t-F"));
    assert!(harness.archive_path("SavedClips/event/front.mp4").is_file());
    assert!(harness
        .archive_path("SentryClips/sentry/rear.mp4")
        .is_file());
    assert!(fs::read_dir(harness.backingfiles.join("snapshots"))
        .unwrap()
        .all(|entry| { !entry.unwrap().file_name().to_string_lossy().eq("recovery") }));
}

#[test]
fn offline_ext4_recovery_failure_prevents_upload_and_preserves_evidence() {
    for (variable, exit_code) in [
        ("TESLAUSB_FAKE_FSCK_EXIT", "4"),
        ("TESLAUSB_FAKE_FSCK_VERIFY_EXIT", "4"),
    ] {
        let harness = Harness::new("rclone");
        let config = harness.config_arg();
        assert_success(&harness.run(&["--config", &config, "init", "--reserve", "20G"]));
        fs::write(harness.state.join("commands.log"), "").unwrap();
        let archive =
            harness.run_with_env(&["--config", &config, "archive"], &[(variable, exit_code)]);
        assert!(!archive.status.success(), "{}", describe(&archive));
        let log = harness.command_log();
        assert!(!log.contains("rclone\tcopy"));
        assert!(!log.contains("mount\t-o\trw"));
        let recovery: Vec<_> = fs::read_dir(harness.backingfiles.join("snapshots"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.file_name().unwrap().to_string_lossy().eq("recovery"))
            .collect();
        assert_eq!(recovery.len(), 1, "{}", describe(&archive));
        assert!(recovery[0].join("raw.bin").is_file());
        assert!(recovery[0].join("recovered.bin").is_file());
        let retained_files = recovery_files(&recovery[0]);
        let state_path = harness.backingfiles.join("snapshots/recovery.state");
        let failed_state = fs::read(&state_path).unwrap();
        let state: serde_json::Value = serde_json::from_slice(&failed_state).unwrap();
        assert_eq!(state["phase"], "failed");
        let copies_before = harness
            .command_log()
            .lines()
            .filter(|line| line.starts_with("cp\t--reflink=always"))
            .count();
        let retry = harness.run(&["--config", &config, "archive"]);
        let copies_after = harness
            .command_log()
            .lines()
            .filter(|line| line.starts_with("cp\t--reflink=always"))
            .count();
        assert_eq!(
            copies_before, copies_after,
            "retry pinned another camera snapshot"
        );
        assert!(!retry.status.success(), "{}", describe(&retry));
        assert!(
            stderr(&retry).contains("recovery evidence already exists"),
            "{}",
            describe(&retry)
        );
        assert_eq!(recovery_files(&recovery[0]), retained_files);
        assert_eq!(fs::read(&state_path).unwrap(), failed_state);
        assert_eq!(
            fs::read(recovery[0].join("raw.bin")).unwrap(),
            fs::read(harness.backingfiles.join("cam_disk.bin")).unwrap()
        );
        assert_success(&harness.run(&["--config", &config, "clean"]));
        assert!(
            recovery[0].join("raw.bin").is_file(),
            "normal cleanup erased failed recovery evidence"
        );
        assert_eq!(fs::read(&state_path).unwrap(), failed_state);
    }
}

#[test]
fn offline_abrupt_archive_death_recovers_after_kernel_resources_are_released() {
    let harness = Harness::new("rclone");
    let config = harness.config_arg();
    assert_success(&harness.run(&["--config", &config, "init", "--reserve", "20G"]));
    let camera = harness.backingfiles.join("cam_disk.bin");
    let before = fs::read(&camera).unwrap();
    let mut child = harness.spawn_with_env(
        &["--config", &config, "archive"],
        &[("TESLAUSB_FAKE_RCLONE_SLEEP", "true")],
    );
    let pid_file = harness.state.join("rclone.pid");
    if !wait_until(
        || {
            pid_file.is_file()
                && fs::read_to_string(&pid_file)
                    .unwrap()
                    .trim()
                    .parse::<i32>()
                    .is_ok()
        },
        Duration::from_secs(10),
    ) {
        terminate_child(&mut child);
        panic!(
            "copy did not start: {}",
            describe(&child.wait_with_output().unwrap())
        );
    }
    let copy_pid: i32 = fs::read_to_string(&pid_file)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    child.kill().unwrap();
    unsafe extern "C" {
        fn kill(pid: i32, signal: i32) -> i32;
    }
    assert_eq!(unsafe { kill(-copy_pid, 9) }, 0);
    assert!(!child.wait_with_output().unwrap().status.success());

    let recovery = harness.backingfiles.join("snapshots/recovery");
    assert!(recovery.join("raw.bin").is_file());
    assert!(recovery.join("recovered.bin").is_file());
    let state_path = harness.backingfiles.join("snapshots/recovery.state");
    let state: serde_json::Value = serde_json::from_slice(&fs::read(&state_path).unwrap()).unwrap();
    assert_eq!(state["phase"], "ready");
    assert_eq!(before, fs::read(&camera).unwrap());
    assert!(harness.state.join("attached-image").is_file());
    let log = harness.command_log();
    let mount = log
        .lines()
        .find(|line| line.starts_with("mount\t-o\tro,noload\t"))
        .unwrap()
        .split('\t')
        .next_back()
        .unwrap();
    let released = Command::new(harness.bin.join("umount"))
        .arg(mount)
        .env("TESLAUSB_FAKE_STATE", &harness.state)
        .output()
        .unwrap();
    assert_success(&released);
    let detached = Command::new(harness.bin.join("losetup"))
        .args(["-d", harness.state.join("loop0").to_str().unwrap()])
        .env("TESLAUSB_FAKE_STATE", &harness.state)
        .output()
        .unwrap();
    assert_success(&detached);

    let retry = harness.run(&["--config", &config, "archive"]);
    assert_success(&retry);
    assert!(harness.archive_path("SavedClips/event/front.mp4").is_file());
    assert!(harness
        .archive_path("SentryClips/sentry/rear.mp4")
        .is_file());
    assert!(!recovery.exists());
    assert!(!state_path.exists());
    assert_eq!(before, fs::read(&camera).unwrap());
}

fn recovery_files(path: &Path) -> std::collections::BTreeMap<OsString, Vec<u8>> {
    fs::read_dir(path)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            (entry.file_name(), fs::read(entry.path()).unwrap())
        })
        .collect()
}

#[test]
fn offline_unsupported_camera_filesystem_stops_before_mount_or_upload() {
    for filesystem in ["vfat", "ntfs"] {
        let harness = Harness::new("rclone");
        let config = harness.config_arg();
        assert_success(&harness.run(&["--config", &config, "init", "--reserve", "20G"]));
        let image = harness.backingfiles.join("cam_disk.bin");
        let before = fs::read(&image).unwrap();
        fs::write(harness.state.join("commands.log"), "").unwrap();
        let archive = harness.run_with_env(
            &["--config", &config, "archive"],
            &[("TESLAUSB_FAKE_FILESYSTEM", filesystem)],
        );
        assert!(!archive.status.success(), "{}", describe(&archive));
        assert!(
            stderr(&archive).contains(&format!("unsupported camera filesystem {filesystem:?}")),
            "{}",
            describe(&archive)
        );
        assert!(stderr(&archive).contains("ext4 is required"));
        let log = harness.command_log();
        for forbidden in [
            "rclone\tcopy",
            "mount\t-o\tro",
            "mount\t-o\trw",
            "e2fsck\t-p",
            "e2fsck\t-f",
            "mkfs.ext4\t-F",
        ] {
            assert!(!log.contains(forbidden), "unexpected {forbidden:?}\n{log}");
        }
        assert_eq!(before, fs::read(&image).unwrap());
        assert!(!harness.backingfiles.join("snapshots/recovery").exists());
    }
}

#[test]
fn offline_obsolete_filesystem_configuration_fails_before_modification() {
    for value in ["ext4", "fat32", ""] {
        for in_file in [false, true] {
            let harness = Harness::new("none");
            let config = harness.config_arg();
            let output = if in_file {
                let contents = fs::read_to_string(&harness.config).unwrap();
                fs::write(
                    &harness.config,
                    format!("{contents}CAM_FILESYSTEM={value}\n"),
                )
                .unwrap();
                harness.run(&["--config", &config, "init", "--reserve", "20G"])
            } else {
                harness.run_with_env(
                    &["--config", &config, "init", "--reserve", "20G"],
                    &[("CAM_FILESYSTEM", value)],
                )
            };
            assert!(!output.status.success(), "{}", describe(&output));
            assert!(
                stderr(&output).contains("CAM_FILESYSTEM is no longer supported"),
                "{}",
                describe(&output)
            );
            assert!(harness.command_log().is_empty());
            assert!(!harness.mutable.join("backingfiles.img").exists());
            assert!(!harness.backingfiles.join("cam_disk.bin").exists());
        }
    }

    let harness = Harness::new("none");
    let mut path = OsString::from(&harness.bin);
    path.push(":");
    path.push(&harness.old_path);
    let output = Command::new(env!("CARGO_BIN_EXE_teslausb"))
        .args(["--config", &harness.config_arg(), "init"])
        .env("PATH", path)
        .env("TESLAUSB_FAKE_STATE", &harness.state)
        .env("CAM_FILESYSTEM", OsString::from_vec(vec![0xff]))
        .output()
        .unwrap();
    assert!(!output.status.success(), "{}", describe(&output));
    assert!(stderr(&output).contains("CAM_FILESYSTEM is no longer supported"));
    assert!(harness.command_log().is_empty());
    assert!(!harness.mutable.join("backingfiles.img").exists());
}

#[test]
fn offline_init_never_reformats_existing_camera() {
    let harness = Harness::new("none");
    let config = harness.config_arg();
    assert_success(&harness.run(&["--config", &config, "init", "--reserve", "20G"]));
    fs::write(harness.state.join("commands.log"), "").unwrap();
    let before = fs::read(harness.backingfiles.join("cam_disk.bin")).unwrap();
    let init = harness.run(&["--config", &config, "init", "--reserve", "20G"]);
    assert_eq!(init.status.code(), Some(1), "{}", describe(&init));
    assert!(stderr(&init).contains("already exists"));
    let log = harness.command_log();
    assert!(!log.contains("mkfs.ext4\t-F"));
    assert!(!log.contains("mkfs.vfat\t-F"));
    assert_eq!(
        before,
        fs::read(harness.backingfiles.join("cam_disk.bin")).unwrap()
    );
}

#[test]
fn offline_mount_failure_after_effect_unmounts_and_stops_archive() {
    let harness = Harness::new("rclone");
    let config = harness.config_arg();
    assert_success(&harness.run(&["--config", &config, "init", "--reserve", "20G"]));
    let mounts_before = fs::read_dir(harness.state.join("mounted")).unwrap().count();
    fs::write(harness.state.join("commands.log"), "").unwrap();
    let archive = harness.run_with_env(
        &["--config", &config, "archive"],
        &[("TESLAUSB_FAKE_MOUNT_FAIL_AFTER_EFFECT", "true")],
    );
    assert!(!archive.status.success(), "{}", describe(&archive));
    assert!(
        stderr(&archive).contains("injected failure after mount took effect"),
        "{}",
        describe(&archive)
    );
    let log = harness.command_log();
    assert!(!log.contains("rclone\tcopy"));
    assert!(
        log.lines()
            .any(|line| line.starts_with("umount\t") && line.contains("teslausb-mount-")),
        "{log}"
    );
    assert_eq!(
        fs::read_dir(harness.state.join("mounted")).unwrap().count(),
        mounts_before
    );
}

#[test]
fn offline_uncertain_loop_detach_is_not_issued_again_during_drop() {
    let harness = Harness::new("none");
    let config = harness.config_arg();
    let init = harness.run_with_env(
        &["--config", &config, "init", "--reserve", "20G"],
        &[("TESLAUSB_FAKE_DETACH_FAIL_AFTER_EFFECT", "true")],
    );
    assert!(!init.status.success(), "{}", describe(&init));
    assert!(stderr(&init).contains("injected failure after detach took effect"));
    let log = harness.command_log();
    assert_eq!(
        log.lines()
            .filter(|line| line.starts_with("losetup\t-d\t"))
            .count(),
        1,
        "{log}"
    );
    assert!(!log.contains("mount\t-o\trw"));
}

#[test]
fn offline_mount_probe_errors_preserve_the_disk_image() {
    let harness = Harness::new("none");
    let config = harness.config_arg();
    assert_success(&harness.run(&["--config", &config, "init", "--reserve", "20G"]));
    let image = harness.mutable.join("backingfiles.img");
    let log_before = harness.command_log();

    for (extra_env, expected_error) in [
        (("TESLAUSB_FAKE_MOUNTPOINT_EXIT", "1"), "mountpoint failed"),
        (("PATH", ""), "failed to run mountpoint"),
    ] {
        for args in [
            vec!["--config", &config, "deinit", "--yes"],
            vec!["--config", &config, "status", "--json"],
        ] {
            let output = harness.run_with_env(&args, &[extra_env]);
            assert!(!output.status.success(), "{}", describe(&output));
            assert!(
                stderr(&output).contains(expected_error),
                "{}",
                describe(&output)
            );
            assert!(
                image.is_file(),
                "an unverified mount probe removed the disk image"
            );
        }
    }
    let log_after = harness.command_log();
    assert!(!log_after[log_before.len()..].contains("umount"));
}

#[test]
fn offline_status_before_init_warns_when_not_mounted() {
    let harness = Harness::new("none");
    let config = harness.config_arg();

    let status = harness.run(&["--config", &config, "status", "--json"]);

    let text_status = harness.run(&["--config", &config, "status"]);
    assert_success(&text_status);
    assert!(stdout(&text_status).contains("Backingfiles not mounted"));

    assert_success(&status);
    let json = stdout(&status);
    assert!(json.contains("\"backingfiles_mounted\": false"), "{json}");
    assert!(json.contains("Backingfiles not mounted"), "{json}");
    assert!(json.contains("\"snapshots\": { \"count\": 0, \"deletable\": 0 }"));
}

#[test]
fn offline_archive_snapshots_and_clean_with_fake_rclone() {
    let harness = Harness::new("rclone");
    let config = harness.config_arg();

    assert_success(&harness.run(&["--config", &config, "init", "--reserve", "20G"]));
    let archive = harness.run_with_env(
        &["--config", &config, "archive"],
        &[("TESLAUSB_FAKE_RCLONE_STARTUP_NOTICE", "true")],
    );
    assert_success(&archive);
    assert!(stderr(&archive).contains("fake rclone startup diagnostic"));
    for expected in [
        "SavedClips: transferred 1 files (11 B)",
        "SentryClips: transferred 1 files (11 B)",
        "TrackMode: transferred 1 files (11 B)",
        "Photobooth: transferred 1 files (5 B)",
        "archive complete: 4 files transferred, 38 B",
    ] {
        assert!(
            stderr(&archive).contains(expected),
            "{}",
            describe(&archive)
        );
    }
    assert!(stderr(&archive).contains("clean up complete"));

    assert_eq!(
        fs::read_to_string(harness.archive_path("SavedClips/event/front.mp4")).unwrap(),
        "saved-front"
    );
    assert_eq!(
        fs::read_to_string(harness.archive_path("SentryClips/sentry/rear.mp4")).unwrap(),
        "sentry-rear"
    );
    assert_eq!(
        fs::read_to_string(harness.archive_path("TrackMode/lap/video.mp4")).unwrap(),
        "track-video"
    );
    assert_eq!(
        fs::read_to_string(harness.archive_path("Photobooth/photo.jpg")).unwrap(),
        "photo"
    );
    assert!(!harness.archive_path("RecentClips/recent/skip.mp4").exists());

    let snapshots = harness.run(&["--config", &config, "snapshots", "--json"]);
    assert_success(&snapshots);
    assert_eq!(stdout(&snapshots).trim(), "[]");

    let clean = harness.run(&["--config", &config, "clean", "--dry-run"]);
    assert_success(&clean);
    assert!(stdout(&clean).contains("No deletable snapshots"));

    let log = harness.command_log();
    assert!(log.contains("rclone\tlsf\tfake:"));
    assert!(log.contains("rclone\tcopy"));
    assert!(log.contains("fake:TeslaArchive/SavedClips"));
    assert!(log.contains("fake:TeslaArchive/SentryClips"));
    assert!(log.contains("fake:TeslaArchive/TrackMode"));
    assert!(log.contains("fake:TeslaArchive/Photobooth"));
    assert!(!log.contains("fake:TeslaArchive/RecentClips"));
}

#[test]
fn offline_archive_failure_returns_nonzero_without_privileged_tools() {
    let harness = Harness::new("rclone");
    let config = harness.config_arg();

    assert_success(&harness.run(&["--config", &config, "init", "--reserve", "20G"]));
    for level in ["error", "fatal"] {
        let archive = harness.run_with_env(
            &["--config", &config, "archive"],
            &[
                ("TESLAUSB_FAKE_RCLONE_FAIL", "SavedClips"),
                ("TESLAUSB_FAKE_RCLONE_FAILURE_LEVEL", level),
            ],
        );
        assert!(!archive.status.success(), "{}", describe(&archive));
        assert!(stderr(&archive).contains("warning: archive finished with issues"));
        assert!(
            stderr(&archive).contains("SavedClips: failed after transferring 0 files (0 B)"),
            "{}",
            describe(&archive)
        );
        assert!(
            stderr(&archive).contains("injected rclone failure for fake:TeslaArchive/SavedClips"),
            "{}",
            describe(&archive)
        );
    }
}

#[test]
fn offline_run_loop_archives_updates_monitors_and_stops_on_sigterm() {
    let harness = Harness::new("rclone");
    let config = harness.config_arg();
    let led_path = harness.root.join("led");
    let thermal_path = harness.root.join("thermal/temp");
    let proc_path = harness.root.join("proc");
    write_led_fixture(&led_path);
    write_file(thermal_path.clone(), "85000");
    fs::create_dir_all(&proc_path).unwrap();

    assert_success(&harness.run(&["--config", &config, "init", "--reserve", "20G"]));

    let led_path_s = led_path.display().to_string();
    let thermal_path_s = thermal_path.display().to_string();
    let proc_path_s = proc_path.display().to_string();
    let mut child = harness.spawn_with_env(
        &["--config", &config, "run"],
        &[
            ("TESLAUSB_LED_PATH", led_path_s.as_str()),
            ("TESLAUSB_THERMAL_PATH", thermal_path_s.as_str()),
            ("TESLAUSB_PROC_PATH", proc_path_s.as_str()),
            ("TESLAUSB_IDLE_TIMEOUT_SECS", "1"),
            ("TESLAUSB_FAKE_CHECK_CLEANUP_LED", "1"),
        ],
    );

    if !wait_until(
        || {
            harness.archive_path("Photobooth/photo.jpg").exists()
                && fs::read_dir(harness.backingfiles.join("snapshots"))
                    .unwrap()
                    .all(|entry| {
                        !entry
                            .unwrap()
                            .file_name()
                            .to_string_lossy()
                            .starts_with("snap-")
                    })
        },
        Duration::from_secs(10),
    ) {
        terminate_child(&mut child);
        let output = child.wait_with_output().unwrap();
        panic!("run loop did not archive in time\n{}", describe(&output));
    }

    terminate_child(&mut child);
    let output = child.wait_with_output().unwrap();
    assert_success(&output);

    let stderr = stderr(&output);
    assert!(stderr.contains("waiting up to 1s for USB writes to become idle"));
    assert!(stderr.contains("temperature warning: 85.0 C"), "{stderr}");
    assert!(stderr.contains("temperature caution: 85.0 C"), "{stderr}");
    assert!(stderr.contains("archive complete"), "{stderr}");
    assert!(harness.command_log().contains("cleanup-led\theartbeat"));

    assert_eq!(
        fs::read_to_string(led_path.join("trigger")).unwrap(),
        "none"
    );
    assert_eq!(
        fs::read_to_string(led_path.join("brightness")).unwrap(),
        "0"
    );
    assert_eq!(
        fs::read_to_string(led_path.join("delay_off")).unwrap(),
        "150"
    );
    assert_eq!(fs::read_to_string(led_path.join("delay_on")).unwrap(), "50");
    assert_eq!(fs::read_to_string(led_path.join("invert")).unwrap(), "0");

    let log = harness.command_log();
    assert!(log.contains("rclone\tcopy"), "{log}");
    assert!(log.contains("live-camera-fsck"), "{log}");
}

#[test]
fn offline_recent_uploads_are_partitioned_by_date_without_cleaning_the_live_buffer() {
    let harness = Harness::new("rclone");
    let config = harness.config_arg();
    assert_success(&harness.run(&["--config", &config, "init", "--reserve", "20G"]));
    fs::remove_dir_all(&harness.cam_source).unwrap();
    for date in ["2026-10-01", "2026-10-02"] {
        write_file(
            harness
                .cam_source
                .join(format!("TeslaCam/RecentClips/{date}_12-00-00-front.mp4")),
            "video",
        );
    }
    write_file(
        harness.cam_source.join("TeslaCam/RecentClips/thumb.png"),
        "thumbnail",
    );
    let mut content = fs::read_to_string(&harness.config).unwrap();
    content.push_str("ARCHIVE_RECENTCLIPS=true\n");
    fs::write(&harness.config, content).unwrap();
    let output = harness.run(&["--config", &config, "archive"]);
    assert_success(&output);
    for date in ["2026-10-01", "2026-10-02"] {
        assert!(harness
            .archive_path(&format!("RecentClips/{date}/{date}_12-00-00-front.mp4"))
            .exists());
    }
    assert!(harness
        .archive_path("RecentClips/metadata/thumb.png")
        .exists());
    let log = harness.command_log();
    assert!(log.contains("--no-traverse"));
    assert!(log.contains("--files-from-raw"));
    assert!(!log.contains("live-camera-fsck"));
}

#[test]
fn offline_fsck_failure_prevents_writable_mount() {
    let harness = Harness::new("rclone");
    let config = harness.config_arg();
    assert_success(&harness.run(&["--config", &config, "init", "--reserve", "20G"]));
    fs::write(harness.state.join("commands.log"), "").unwrap();
    let output = harness.run_with_env(
        &["--config", &config, "archive"],
        &[("TESLAUSB_FAKE_LIVE_FSCK_EXIT", "4")],
    );
    assert!(!output.status.success(), "{}", describe(&output));
    assert!(harness.command_log().contains("rclone\tcopy"));
    assert!(stderr(&output).contains("filesystem check failed"));
    assert!(!harness.command_log().contains("mount\t-o\trw"));
}

#[test]
fn offline_incomplete_fsck_repair_prevents_writable_mount() {
    let harness = Harness::new("rclone");
    let config = harness.config_arg();
    assert_success(&harness.run(&["--config", &config, "init", "--reserve", "20G"]));
    fs::write(harness.state.join("commands.log"), "").unwrap();
    let output = harness.run_with_env(
        &["--config", &config, "archive"],
        &[("TESLAUSB_FAKE_LIVE_FSCK_VERIFY_EXIT", "4")],
    );
    assert!(!output.status.success(), "{}", describe(&output));
    assert!(harness.command_log().contains("rclone\tcopy"));
    assert!(stderr(&output).contains("filesystem remains inconsistent"));
    assert!(!harness.command_log().contains("mount\t-o\trw"));
}

#[test]
fn offline_writable_unmount_failure_fails_the_archive_cycle() {
    let harness = Harness::new("rclone");
    let config = harness.config_arg();
    assert_success(&harness.run(&["--config", &config, "init", "--reserve", "20G"]));
    let output = harness.run_with_env(
        &["--config", &config, "archive"],
        &[("TESLAUSB_FAKE_UMOUNT_FAIL", "rw")],
    );
    assert!(!output.status.success());
    assert!(stderr(&output).contains("injected unmount failure"));
    assert!(!stderr(&output).contains("clean up complete"));
}

#[test]
fn offline_stop_interrupts_an_active_copy_without_starting_cleanup() {
    let harness = Harness::new("rclone");
    let config = harness.config_arg();
    let led = harness.root.join("led");
    let thermal = harness.root.join("thermal");
    write_led_fixture(&led);
    fs::write(&thermal, "45000").unwrap();
    assert_success(&harness.run(&["--config", &config, "init", "--reserve", "20G"]));
    let mut child = harness.spawn_with_env(
        &["--config", &config, "run"],
        &[
            ("TESLAUSB_LED_PATH", led.to_str().unwrap()),
            ("TESLAUSB_THERMAL_PATH", thermal.to_str().unwrap()),
            ("TESLAUSB_FAKE_RCLONE_SLEEP", "true"),
        ],
    );
    if !wait_until(
        || harness.command_log().contains("rclone\tcopy"),
        Duration::from_secs(10),
    ) {
        terminate_child(&mut child);
        panic!(
            "copy did not start: {}",
            describe(&child.wait_with_output().unwrap())
        );
    }
    let started = Instant::now();
    terminate_child(&mut child);
    let output = child.wait_with_output().unwrap();
    assert_success(&output);
    assert!(started.elapsed() < Duration::from_secs(3));
    assert!(!harness.command_log().contains("live-camera-fsck"));
}

#[test]
fn offline_gadget_changes_refuse_an_occupied_camera_lock() {
    let harness = Harness::new("none");
    let config = harness.config_arg();
    let lock = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(harness.backingfiles.join("archive.lock"))
        .unwrap();
    unsafe extern "C" {
        fn flock(fd: i32, operation: i32) -> i32;
    }
    assert_eq!(unsafe { flock(lock.as_raw_fd(), 2 | 4) }, 0);

    for action in ["on", "off"] {
        let output = harness.run(&["--config", &config, "gadget", action]);
        assert!(!output.status.success(), "{action}: {}", describe(&output));
        assert!(
            stderr(&output).contains("another TeslaUSB process is using the camera disk"),
            "{action}: {}",
            describe(&output)
        );
    }
    assert!(harness.command_log().is_empty());
}

#[test]
fn offline_manual_archive_sigterm_reaps_copy_and_unmounts_snapshot() {
    let harness = Harness::new("rclone");
    let config = harness.config_arg();
    assert_success(&harness.run(&["--config", &config, "init", "--reserve", "20G"]));
    fs::write(harness.state.join("commands.log"), "").unwrap();
    let mut child = harness.spawn_with_env(
        &["--config", &config, "archive"],
        &[("TESLAUSB_FAKE_RCLONE_SLEEP", "true")],
    );
    let pid_file = harness.state.join("rclone.pid");
    if !wait_until(|| pid_file.exists(), Duration::from_secs(10)) {
        terminate_child(&mut child);
        panic!(
            "copy did not start: {}",
            describe(&child.wait_with_output().unwrap())
        );
    }
    let copy_pid: i32 = fs::read_to_string(pid_file)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    terminate_child(&mut child);
    let stopped = wait_until(
        || child.try_wait().unwrap().is_some(),
        Duration::from_secs(3),
    );

    unsafe extern "C" {
        fn kill(pid: i32, signal: i32) -> i32;
    }
    let copy_still_running = unsafe { kill(-copy_pid, 0) } == 0;
    if copy_still_running {
        assert_eq!(unsafe { kill(-copy_pid, 9) }, 0);
    }
    if !stopped {
        child.kill().unwrap();
    }
    let output = child.wait_with_output().unwrap();
    assert!(stopped, "{}", describe(&output));
    assert!(!copy_still_running, "copy process group survived shutdown");
    assert_eq!(output.status.code(), Some(1), "{}", describe(&output));
    assert!(stderr(&output).contains("stopped by shutdown request"));
    let log = harness.command_log();
    assert!(log
        .lines()
        .any(|line| line.starts_with("umount\t") && line.contains("teslausb-mount-")));
    assert!(!log.contains("live-camera-fsck"));
    assert!(!log.contains("mount\t-o\trw"));
    let log_paths: Vec<_> = log
        .lines()
        .filter(|line| line.starts_with("rclone\tcopy\t"))
        .filter_map(|line| line.split_once("\t--log-file\t"))
        .map(|(_, args)| Path::new(args.split('\t').next().unwrap()))
        .collect();
    assert!(
        !log_paths.is_empty(),
        "copy did not receive a dedicated JSON log file"
    );
    for path in log_paths {
        assert!(
            !path.exists(),
            "cancelled copy retained its log at {}",
            path.display()
        );
    }
    assert_success(&harness.run(&["--config", &config, "archive"]));
}

#[test]
fn offline_malformed_rclone_log_prevents_live_cleanup() {
    let harness = Harness::new("rclone");
    let config = harness.config_arg();
    assert_success(&harness.run(&["--config", &config, "init", "--reserve", "20G"]));
    fs::write(harness.state.join("commands.log"), "").unwrap();
    let output = harness.run_with_env(
        &["--config", &config, "archive"],
        &[("TESLAUSB_FAKE_RCLONE_MALFORMED_LOG", "true")],
    );
    assert!(!output.status.success(), "{}", describe(&output));
    assert!(
        stderr(&output).contains("invalid rclone JSON log record"),
        "{}",
        describe(&output)
    );
    assert!(harness.archive_path("SavedClips/event/front.mp4").is_file());
    assert!(harness
        .archive_path("SentryClips/sentry/rear.mp4")
        .is_file());
    assert!(harness
        .cam_source
        .join("TeslaCam/SavedClips/event/front.mp4")
        .is_file());
    assert!(harness
        .cam_source
        .join("TeslaCam/SentryClips/sentry/rear.mp4")
        .is_file());
    let log = harness.command_log();
    assert!(!log.contains("live-camera-fsck"));
    assert!(!log.contains("mount\t-o\trw"));
}

#[test]
fn offline_startup_check_rejects_old_rclone_before_archive() {
    let harness = Harness::new("rclone");
    let config = harness.config_arg();

    assert_success(&harness.run(&["--config", &config, "init", "--reserve", "20G"]));
    let archive = harness.run_with_env(
        &["--config", &config, "archive"],
        &[("TESLAUSB_FAKE_RCLONE_VERSION", "1.49.0")],
    );

    assert!(!archive.status.success(), "{}", describe(&archive));
    assert!(stderr(&archive).contains("dependency check failed"));
    assert!(stderr(&archive).contains("rclone"));
    assert!(stderr(&archive).contains("requires >= 1.50.0"));

    let log = harness.command_log();
    assert!(log.contains("rclone\tversion"));
    assert!(!log.contains("rclone\tcopy"));
}

#[test]
fn offline_invalid_camera_image_blocks_mutations_and_reports_status() {
    for condition in ["missing", "empty", "directory"] {
        let harness = Harness::new("none");
        let config = harness.config_arg();
        assert_success(&harness.run(&["--config", &config, "init", "--reserve", "20G"]));
        let image = harness.backingfiles.join("cam_disk.bin");
        fs::remove_file(&image).unwrap();
        let expected = match condition {
            "empty" => {
                fs::write(&image, b"").unwrap();
                "camera disk is empty"
            }
            "directory" => {
                fs::create_dir(&image).unwrap();
                "camera disk is a directory"
            }
            _ => "camera disk not found",
        };
        let incomplete = harness.backingfiles.join("snapshots/snap-000123");
        write_file(incomplete.join("snap.bin"), "preserve incomplete snapshot");
        fs::write(harness.state.join("commands.log"), "").unwrap();

        for command in ["run", "archive", "clean"] {
            let output = harness.run(&["--config", &config, command]);
            assert_eq!(output.status.code(), Some(1), "{}", describe(&output));
            assert!(stderr(&output).contains(expected), "{}", describe(&output));
            assert!(incomplete.join("snap.bin").exists());
        }
        assert!(!harness.command_log().contains("cp\t--reflink=always"));

        let status = harness.run(&["--config", &config, "status", "--json"]);
        assert_success(&status);
        let data: serde_json::Value = serde_json::from_str(&stdout(&status)).unwrap();
        assert!(data["warnings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|warning| { warning.as_str().unwrap().contains(expected) }));
        assert!(incomplete.join("snap.bin").exists());
    }
}

#[test]
fn offline_service_status_preserves_systemctl_exit_code() {
    let harness = Harness::new("none");
    for code in ["0", "3", "4"] {
        let output = harness.run_with_env(
            &["service", "status"],
            &[("TESLAUSB_FAKE_SYSTEMCTL_STATUS_EXIT", code)],
        );
        assert_eq!(output.status.code(), Some(code.parse().unwrap()));
        assert!(stdout(&output).contains("teslausb.service fake active"));
    }
}

#[test]
fn offline_service_install_status_and_uninstall() {
    let harness = Harness::new("none");

    let install = harness.run(&["service", "install", "--force"]);
    assert_success(&install);
    let service = fs::read_to_string(&harness.service_path).unwrap();
    assert!(service.contains("ExecStartPre="));
    assert!(service.contains("User=root\n"));
    assert!(service.contains(" doctor --startup\n"));
    assert!(service.contains(" mount\n"));
    assert!(service.contains(" gadget on\n"));
    assert!(service.contains("ExecStart="));
    assert!(service.contains("KillMode=mixed\n"));
    assert!(service.contains("TimeoutStopSec=900\n"));
    assert!(!service.contains("network-online.target"));
    assert!(service.contains(" run\n"));
    assert!(service.contains("ExecStopPost="));
    assert!(!service.lines().any(|line| line.starts_with("ExecStop=")));
    assert!(service.contains(" gadget off\n"));

    assert_success(&harness.run(&["service", "status"]));
    assert_success(&harness.run(&["service", "uninstall"]));
    assert!(!harness.service_path.exists());

    let log = harness.command_log();
    assert!(log.contains("systemctl\tdaemon-reload"));
    assert!(log.contains("systemctl\tenable\tteslausb.service"));
    assert!(log.contains("systemctl\tstatus\tteslausb.service"));
    assert!(log.contains("systemctl\tstop\tteslausb.service"));
    assert!(log.contains("systemctl\tdisable\tteslausb.service"));
}

fn write_cam_fixture(root: &Path) {
    write_file(
        root.join("TeslaCam/SavedClips/event/front.mp4"),
        "saved-front",
    );
    write_file(
        root.join("TeslaCam/SentryClips/sentry/rear.mp4"),
        "sentry-rear",
    );
    write_file(
        root.join("TeslaCam/RecentClips/recent/skip.mp4"),
        "recent-skip",
    );
    write_file(root.join("TeslaCam/Photobooth/photo.jpg"), "photo");
    write_file(root.join("TeslaTrackMode/lap/video.mp4"), "track-video");
}

fn write_file(path: PathBuf, content: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, content).unwrap();
}

fn write_led_fixture(path: &Path) {
    fs::create_dir_all(path).unwrap();
    fs::write(path.join("trigger"), "[none] timer heartbeat").unwrap();
    fs::write(path.join("brightness"), "1").unwrap();
    fs::write(path.join("delay_off"), "").unwrap();
    fs::write(path.join("delay_on"), "").unwrap();
    fs::write(path.join("invert"), "").unwrap();
}

fn wait_until(mut predicate: impl FnMut() -> bool, timeout: Duration) -> bool {
    let started = Instant::now();
    while started.elapsed() < timeout {
        if predicate() {
            return true;
        }
        thread::sleep(Duration::from_millis(50));
    }
    predicate()
}

fn terminate_child(child: &mut Child) {
    if child.try_wait().unwrap().is_some() {
        return;
    }
    let status = Command::new("kill")
        .arg("-TERM")
        .arg(child.id().to_string())
        .status()
        .unwrap();
    assert!(status.success(), "failed to send SIGTERM to run loop");
}

fn write_fake_tools(bin: &Path) {
    let script = r#"#!/bin/sh
set -eu
tool=$(basename "$0")
state="${TESLAUSB_FAKE_STATE:?}"
mkdir -p "$state"
log="$state/commands.log"
{
    printf '%s' "$tool"
    for arg in "$@"; do
        printf '\t%s' "$arg"
    done
    printf '\n'
} >> "$log"

key_for() {
    printf '%s' "$1" | sed 's#[^A-Za-z0-9_.-]#_#g'
}

mark_mounted() {
    mkdir -p "$state/mounted"
    : > "$state/mounted/$(key_for "$1")"
}

unmark_mounted() {
    rm -f "$state/mounted/$(key_for "$1")"
}

last_arg() {
    last=''
    for arg in "$@"; do
        last="$arg"
    done
    printf '%s' "$last"
}

case "$tool" in
    rclone)
        if [ "${1:-}" = "version" ]; then
            printf 'rclone v%s\n' "${TESLAUSB_FAKE_RCLONE_VERSION:-1.65.0}"
            exit 0
        fi
        ;;
    mkfs.xfs)
        if [ "${1:-}" = "-V" ] || [ "${1:-}" = "--version" ]; then
            printf 'mkfs.xfs version %s\n' "${TESLAUSB_FAKE_MKFS_XFS_VERSION:-6.1.0}"
            exit 0
        fi
        ;;
    kpartx)
        if [ "${1:-}" = "-V" ] || [ "${1:-}" = "--version" ]; then
            printf 'kpartx version %s\n' "${TESLAUSB_FAKE_KPARTX_VERSION:-0.8.8}"
            exit 0
        fi
        ;;
    cp|df|stat|sync|truncate)
        if [ "${1:-}" = "--version" ] || [ "${1:-}" = "-V" ]; then
            printf '%s (GNU coreutils) %s\n' "$tool" "${TESLAUSB_FAKE_COREUTILS_VERSION:-9.1.0}"
            exit 0
        fi
        if [ "$tool" = "cp" ] && [ "${1:-}" = "--help" ]; then
            printf 'Usage: cp [OPTION] SOURCE DEST\n'
            printf '      --reflink[=WHEN] control clone/CoW copies\n'
            exit 0
        fi
        ;;
    mount|mountpoint|umount|losetup|blockdev|blkid)
        if [ "${1:-}" = "--version" ] || [ "${1:-}" = "-V" ]; then
            printf '%s from util-linux %s\n' "$tool" "${TESLAUSB_FAKE_UTIL_LINUX_VERSION:-2.38.1}"
            exit 0
        fi
        ;;
    parted)
        if [ "${1:-}" = "--version" ] || [ "${1:-}" = "-V" ]; then
            printf 'parted (GNU parted) %s\n' "${TESLAUSB_FAKE_PARTED_VERSION:-3.5}"
            exit 0
        fi
        ;;
    mkfs.ext4|e2fsck)
        if [ "${1:-}" = "-V" ]; then
            printf '%s 1.47.0 (5-Feb-2023)\n' "$tool" >&2
            exit 0
        fi
        ;;
    modprobe)
        if [ "${1:-}" = "--version" ] || [ "${1:-}" = "-V" ]; then
            printf 'kmod version %s\n' "${TESLAUSB_FAKE_KMOD_VERSION:-30}"
            exit 0
        fi
        ;;
    systemctl)
        if [ "${1:-}" = "--version" ] || [ "${1:-}" = "-V" ]; then
            printf 'systemd %s\n' "${TESLAUSB_FAKE_SYSTEMD_VERSION:-252}"
            exit 0
        fi
        ;;
esac

case "$tool" in
    sync|mkfs.xfs|parted|blockdev|modprobe)
        exit 0
        ;;
    mkfs.ext4)
        printf 'ext4\n' > "$state/cam-filesystem"
        exit 0
        ;;
    blkid)
        if [ -n "${TESLAUSB_FAKE_FILESYSTEM:-}" ]; then
            printf '%s\n' "$TESLAUSB_FAKE_FILESYSTEM"
        else
            cat "$state/cam-filesystem"
        fi
        exit 0
        ;;
    e2fsck)
        image=$(tail -n 1 "$state/partition-map.tsv" | cut -f 2)
        case "$image" in
            */cam_disk.bin)
                printf 'live-camera-fsck\n' >> "$log"
                if [ "${1:-}" = "-p" ] && [ "${TESLAUSB_FAKE_CHECK_CLEANUP_LED:-}" = "1" ]; then
                    trigger=$(cat "${TESLAUSB_LED_PATH:?}/trigger")
                    if [ "$trigger" != "heartbeat" ]; then
                        printf 'expected cleanup heartbeat before e2fsck, got %s\n' "$trigger" >&2
                        exit 4
                    fi
                    printf 'cleanup-led\theartbeat\n' >> "$log"
                fi
                if [ "${1:-}" = "-f" ]; then exit "${TESLAUSB_FAKE_LIVE_FSCK_VERIFY_EXIT:-0}"; fi
                exit "${TESLAUSB_FAKE_LIVE_FSCK_EXIT:-0}"
                ;;
        esac
        if [ "${1:-}" = "-f" ]; then exit "${TESLAUSB_FAKE_FSCK_VERIFY_EXIT:-0}"; fi
        exit "${TESLAUSB_FAKE_FSCK_EXIT:-0}"
        ;;
    df)
        mount_path="${2:-/}"
        printf 'Filesystem 1024-blocks Used Available Capacity Mounted on\n'
        printf 'fakefs 209715200 52428800 157286400 25%% %s\n' "$mount_path"
        exit 0
        ;;
    truncate)
        if [ "${1:-}" = "-s" ]; then
            path="$3"
        else
            path=$(last_arg "$@")
        fi
        mkdir -p "$(dirname "$path")"
        printf 'fake disk image\n' > "$path"
        exit 0
        ;;
    losetup)
        if [ "${1:-}" = "--associated" ]; then
            if [ -f "$state/attached-image" ] && [ "$(cat "$state/attached-image")" = "$2" ]; then
                printf '%s\n' "$state/loop0"
            fi
            exit 0
        fi
        if [ "${1:-}" = "-d" ]; then
            rm -f "$state/attached-image"
            if [ "${TESLAUSB_FAKE_DETACH_FAIL_AFTER_EFFECT:-}" = "true" ]; then
                printf 'injected failure after detach took effect\n' >&2
                exit 9
            fi
            exit 0
        fi
        image=$(last_arg "$@")
        loop="$state/loop0"
        partition="${loop}p1"
        printf '%s\n' "$image" > "$state/attached-image"
        : > "$loop"
        : > "$partition"
        printf '%s\t%s\n' "$partition" "$image" >> "$state/partition-map.tsv"
        printf '%s\n' "$loop"
        exit 0
        ;;
    kpartx)
        if [ "${1:-}" = "-d" ]; then
            exit 0
        fi
        exit 0
        ;;
    mountpoint)
        if [ -n "${TESLAUSB_FAKE_MOUNTPOINT_EXIT:-}" ]; then
            exit "$TESLAUSB_FAKE_MOUNTPOINT_EXIT"
        fi
        target=$(last_arg "$@")
        if [ -f "$state/mounted/$(key_for "$target")" ]; then
            exit 0
        fi
        exit 32
        ;;
    mount)
        if [ "${1:-}" = "-o" ]; then
            src="$3"
            target="$4"
        else
            src="$1"
            target="$2"
        fi
        mkdir -p "$target"
        mark_mounted "$target"
        if [ "${2:-}" = "rw" ]; then
            : > "$state/rw-$(key_for "$target")"
        fi
        case "$src" in
            *p1)
                if [ -n "${TESLAUSB_FAKE_CAM_SOURCE:-}" ] && [ -d "$TESLAUSB_FAKE_CAM_SOURCE" ]; then
                    /bin/cp -Rp "$TESLAUSB_FAKE_CAM_SOURCE"/. "$target"/
                fi
                ;;
        esac
        if [ "${TESLAUSB_FAKE_MOUNT_FAIL_AFTER_EFFECT:-}" = "true" ]; then
            printf 'injected failure after mount took effect\n' >&2
            exit 9
        fi
        exit 0
        ;;
    umount)
        target=$(last_arg "$@")
        if [ "${TESLAUSB_FAKE_UMOUNT_FAIL:-}" = "rw" ] && [ -f "$state/rw-$(key_for "$target")" ]; then
            printf 'injected unmount failure\n' >&2
            exit 1
        fi
        unmark_mounted "$target"
        case "$(basename "$target")" in
            teslausb-mount-*|teslausb-cam-mount-*)
                rm -rf "$target"
                ;;
        esac
        exit 0
        ;;
    stat)
        printf 'xfs\n'
        exit 0
        ;;
    cp)
        src=''
        dst=''
        for arg in "$@"; do
            case "$arg" in
                --*) ;;
                *) src="$dst"; dst="$arg" ;;
            esac
        done
        if [ -z "$src" ] || [ -z "$dst" ]; then
            printf 'bad cp arguments\n' >&2
            exit 2
        fi
        mkdir -p "$(dirname "$dst")"
        /bin/cp "$src" "$dst"
        exit 0
        ;;
    rclone)
        if [ "${1:-}" = "lsf" ]; then
            exit 0
        fi
        if [ "${1:-}" = "copy" ]; then
            src="$2"
            dst="$3"
            if [ "${TESLAUSB_FAKE_RCLONE_STARTUP_NOTICE:-}" = "true" ]; then
                printf 'fake rclone startup diagnostic\n' >&2
            fi
            log_file=''
            previous=''
            for arg in "$@"; do
                if [ "$previous" = "--log-file" ]; then log_file="$arg"; fi
                previous="$arg"
            done
            : "${log_file:?rclone copy requires a JSON log file}"
            exec 2>"$log_file"
            if [ "${TESLAUSB_FAKE_RCLONE_SLEEP:-}" = "true" ]; then
                printf '%s\n' "$$" > "$state/rclone.pid"
                sleep 30
            fi
            fail="${TESLAUSB_FAKE_RCLONE_FAIL:-}"
            if [ -n "$fail" ]; then
                case "$dst" in
                    *"$fail"*)
                        printf '{"msg":"injected rclone failure for %s","level":"%s"}\n' "$dst" "${TESLAUSB_FAKE_RCLONE_FAILURE_LEVEL:-error}" >&2
                        exit 9
                        ;;
                esac
            fi
            dest="$state/archive/$dst"
            mkdir -p "$dest"
            list=''
            previous=''
            for arg in "$@"; do
                if [ "$previous" = "--files-from-raw" ]; then list="$arg"; fi
                previous="$arg"
            done
            if [ -z "$list" ]; then
                list="$state/current-files"
                find "$src" -type f | while IFS= read -r file; do printf '%s\n' "${file#"$src"/}"; done > "$list"
            fi
            while IFS= read -r relative; do
                mkdir -p "$(dirname "$dest/$relative")"
                /bin/cp "$src/$relative" "$dest/$relative"
                printf '{"object":"%s","msg":"Copied (new)"}\n' "$relative" >&2
            done < "$list"
            if [ "${TESLAUSB_FAKE_RCLONE_MALFORMED_LOG:-}" = "true" ]; then
                printf 'invalid JSON log record\n' >&2
            fi
            exit 0
        fi
        exit 0
        ;;
    systemctl)
        if [ "${1:-}" = "status" ]; then
            printf 'teslausb.service fake active\n'
            exit "${TESLAUSB_FAKE_SYSTEMCTL_STATUS_EXIT:-0}"
        fi
        exit 0
        ;;
esac

printf 'unexpected fake tool: %s\n' "$tool" >&2
exit 127
"#;

    for tool in [
        "blkid",
        "blockdev",
        "cp",
        "df",
        "e2fsck",
        "kpartx",
        "losetup",
        "mkfs.ext4",
        "mkfs.xfs",
        "modprobe",
        "mount",
        "mountpoint",
        "parted",
        "rclone",
        "stat",
        "sync",
        "systemctl",
        "truncate",
        "umount",
    ] {
        let path = bin.join(tool);
        fs::write(&path, script).unwrap();
        let mut permissions = fs::metadata(&path).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).unwrap();
    }
}

fn assert_success(output: &Output) {
    assert!(output.status.success(), "{}", describe(output));
    for error in ["error: failed to detach", "error: failed to release mount"] {
        assert!(!stderr(output).contains(error), "{}", describe(output));
    }
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn describe(output: &Output) -> String {
    format!(
        "status: {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        stdout(output),
        stderr(output)
    )
}
