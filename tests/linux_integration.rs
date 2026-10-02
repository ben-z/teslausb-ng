#![cfg(target_os = "linux")]

use std::env;
use std::ffi::{CString, OsStr, OsString};
use std::fs;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

static COUNTER: AtomicU64 = AtomicU64::new(0);
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(10);

unsafe extern "C" {
    fn umount(target: *const std::ffi::c_char) -> std::ffi::c_int;
}

struct Harness {
    root: PathBuf,
    fake_bin: PathBuf,
    mutable: PathBuf,
    backingfiles: PathBuf,
    mutable_mount: MountGuard,
    cleaned: bool,
    config: PathBuf,
    old_path: OsString,
}

impl Harness {
    fn new(archive_system: &str) -> Self {
        require_linux_integration();

        let root = temp_path("teslausb-linux");
        fs::create_dir_all(&root).unwrap();
        let root = fs::canonicalize(root).unwrap();
        let fake_bin = root.join("bin");
        let mutable = root.join("mutable");
        let backingfiles = root.join("backingfiles");
        let config = root.join("teslausb.conf");
        fs::create_dir_all(&fake_bin).unwrap();
        fs::create_dir_all(&mutable).unwrap();
        fs::create_dir_all(&backingfiles).unwrap();

        let mutable_mount = MountGuard::xfs_loop(root.join("mutable-volume.img"), &mutable, "3G");
        let mut config_content = format!(
            "MUTABLE_PATH={}\nBACKINGFILES_PATH={}\nARCHIVE_SYSTEM={archive_system}\nEVENT_STABILITY_SECONDS=0\n",
            mutable.display(),
            backingfiles.display()
        );
        if archive_system == "rclone" {
            config_content.push_str(
                "RCLONE_DRIVE=fake\nRCLONE_PATH=TeslaArchive\nRCLONE_FLAGS=--fast-list\n",
            );
            write_fake_rclone(&fake_bin.join("rclone"));
        }
        fs::write(&config, config_content).unwrap();

        Self {
            root,
            fake_bin,
            mutable,
            backingfiles,
            mutable_mount,
            cleaned: false,
            config,
            old_path: env::var_os("PATH").unwrap_or_default(),
        }
    }

    fn run(&self, args: &[&str]) -> Output {
        self.run_with_env(args, &[])
    }

    fn command(&self, args: &[&str]) -> Command {
        let mut path = OsString::from(&self.fake_bin);
        path.push(":");
        path.push(&self.old_path);
        let mut command = Command::new(env!("CARGO_BIN_EXE_teslausb"));
        command.args(args).env("PATH", path).stdin(Stdio::null());
        command
    }

    fn run_with_env(&self, args: &[&str], extra_env: &[(&str, &Path)]) -> Output {
        let mut command = self.command(args);
        for (key, value) in extra_env {
            command.env(key, value);
        }
        command.output().unwrap()
    }

    fn spawn_with_env(&self, args: &[&str], extra_env: &[(&str, &Path)]) -> Child {
        let mut command = self.command(args);
        command.stdout(Stdio::piped()).stderr(Stdio::piped());
        for (key, value) in extra_env {
            command.env(key, value);
        }
        command.spawn().unwrap()
    }

    fn config_arg(&self) -> String {
        self.config.display().to_string()
    }

    fn cam_disk(&self) -> PathBuf {
        self.backingfiles.join("cam_disk.bin")
    }

    fn clean_up(&mut self) -> Result<(), String> {
        if self.cleaned {
            return Ok(());
        }
        unmount_fixture(&self.backingfiles)?;
        self.mutable_mount.clean_up()?;
        fs::remove_dir_all(&self.root)
            .map_err(|err| format!("remove {}: {err}", self.root.display()))?;
        self.cleaned = true;
        Ok(())
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        let result = self.clean_up();
        report_cleanup_result(&self.root, result);
    }
}

#[test]
#[ignore = "requires root, Linux loop devices, XFS, FAT32, and mount support"]
fn linux_fixture_cleanup_waits_for_open_loop_references() {
    let harness = Harness::new("none");
    let root = harness.root.clone();
    let config = harness.config_arg();
    assert_success(&harness.run(&["--config", &config, "init", "--reserve", "512M"]));

    let cam = PartitionMount::mount(&harness.cam_disk(), &root.join("cam-held"), "rw");
    let loop_reference = fs::File::open(&cam.loop_dev).unwrap();
    let loop_name = Path::new(&cam.loop_dev).file_name().unwrap();
    let autoclear = Path::new("/sys/block")
        .join(loop_name)
        .join("loop/autoclear");
    let cleanup = thread::spawn(move || drop(cam));
    let detach_requested = wait_until(
        || fs::read_to_string(&autoclear).unwrap().trim() == "1",
        Duration::from_secs(5),
    );
    let cleanup_waited = !wait_until(|| cleanup.is_finished(), Duration::from_millis(200));
    drop(loop_reference);
    cleanup.join().unwrap();

    assert!(detach_requested, "cleanup must request loop detach");
    assert!(
        cleanup_waited,
        "cleanup returned while the loop image was still held open"
    );
    assert!(loops_backed_under(&harness.cam_disk()).unwrap().is_empty());
    drop(harness);
    assert!(!root.exists(), "fully detached fixture should be removed");
}

#[test]
#[ignore = "requires root, Linux loop devices, XFS, FAT32, and mount support"]
fn linux_fixture_cleanup_retains_data_when_unmount_fails() {
    let mut harness = Harness::new("none");
    let root = harness.root.clone();
    let config = harness.config_arg();
    assert_success(&harness.run(&["--config", &config, "init", "--reserve", "512M"]));
    let open_image = fs::File::open(harness.cam_disk()).unwrap();

    let error = harness.clean_up().unwrap_err();

    assert!(error.contains("umount"), "{error}");
    assert!(
        harness.cam_disk().is_file(),
        "busy fixture must retain its camera image"
    );
    assert!(harness.mutable.join("backingfiles.img").is_file());
    assert_success(&run(
        "mountpoint",
        ["-q", harness.backingfiles.to_str().unwrap()],
    ));
    let release = thread::spawn(move || {
        thread::sleep(Duration::from_millis(200));
        drop(open_image);
    });
    harness.clean_up().unwrap();
    release.join().unwrap();
    assert!(
        !root.exists(),
        "fixture should be removed once it can be unmounted"
    );
}

#[test]
#[ignore = "requires root, Linux loop devices, XFS, FAT32, and mount support"]
fn linux_init_mount_status_and_deinit_with_real_images() {
    linux_init_mount_status_and_deinit_with_real_images_for("fat32");
}

#[test]
#[ignore = "requires root, Linux loop devices, XFS, ext4, and mount support"]
fn linux_init_mount_status_and_deinit_with_real_images_ext4() {
    linux_init_mount_status_and_deinit_with_real_images_for("ext4");
}

fn linux_init_mount_status_and_deinit_with_real_images_for(filesystem: &str) {
    let harness = Harness::new("none");
    let mut contents = fs::read_to_string(&harness.config).unwrap();
    contents.push_str(&format!("CAM_FILESYSTEM={filesystem}\n"));
    fs::write(&harness.config, contents).unwrap();
    let config = harness.config_arg();

    assert_success(&harness.run(&["--config", &config, "init", "--reserve", "512M"]));
    assert!(harness.mutable.join("backingfiles.img").is_file());
    assert!(harness.backingfiles.join("cam_disk.bin").is_file());
    assert!(harness.backingfiles.join("snapshots").is_dir());

    {
        let cam =
            PartitionMount::mount(&harness.cam_disk(), &harness.root.join("cam-format"), "rw");
        let source = run(
            "findmnt",
            [
                "-n",
                "-o",
                "SOURCE",
                "--target",
                cam.path().to_str().unwrap(),
            ],
        );
        assert_success(&source);
        let partition = stdout(&source);
        let actual = run(
            "blkid",
            ["-p", "-s", "TYPE", "-o", "value", partition.trim()],
        );
        assert_success(&actual);
        assert_eq!(
            stdout(&actual).trim(),
            if filesystem == "fat32" {
                "vfat"
            } else {
                "ext4"
            }
        );
        if filesystem == "ext4" {
            let nonroot = Command::new("mkdir")
                .arg(cam.path().join("TeslaCam/SavedClips"))
                .uid(65534)
                .gid(65534)
                .output()
                .unwrap();
            assert_success(&nonroot);
            let superblock = run("dumpe2fs", ["-h", partition.trim()]);
            assert_success(&superblock);
            let header = stdout(&superblock);
            assert!(header.contains("has_journal"));
            for feature in ["metadata_csum", "orphan_file", "fast_commit", "64bit"] {
                assert!(
                    !header.contains(feature),
                    "unexpected compatibility feature {feature}"
                );
            }
            let reserved = header
                .lines()
                .find(|line| line.starts_with("Reserved block count:"))
                .unwrap();
            assert_eq!(reserved.split(':').nth(1).unwrap().trim(), "0");
        }
    }

    let status = harness.run(&["--config", &config, "status", "--json"]);
    assert_success(&status);
    assert!(stdout(&status).contains("\"backingfiles_mounted\": true"));

    assert_success(&harness.run(&["--config", &config, "mount"]));

    let deinit = harness.run(&["--config", &config, "deinit", "--yes"]);
    assert_success(&deinit);
    assert!(!harness.mutable.join("backingfiles.img").exists());
}

#[test]
#[ignore = "requires root, Linux loop devices, XFS, FAT32, and mount support"]
fn linux_doctor_startup_reports_real_dependency_versions() {
    let harness = Harness::new("rclone");
    let config = harness.config_arg();

    let doctor = harness.run(&["--config", &config, "doctor", "--startup"]);
    assert_success(&doctor);

    let stdout = stdout(&doctor);
    for expected in [
        "Dependency",
        "rclone",
        "mkfs.xfs",
        "mkfs.vfat",
        "mount",
        "cp",
    ] {
        assert!(stdout.contains(expected), "missing {expected:?}\n{stdout}");
    }
    assert!(
        stdout.contains("supports --reflink"),
        "doctor should verify cp reflink support\n{stdout}"
    );
}

#[test]
#[ignore = "requires root, Linux loop devices and XFS mount support"]
fn linux_init_dependency_failure_stops_before_creating_images() {
    let harness = Harness::new("none");
    let config = harness.config_arg();
    write_fake_old_mkfs_xfs(&harness.fake_bin.join("mkfs.xfs"));

    let init = harness.run(&["--config", &config, "init", "--reserve", "512M"]);

    assert!(!init.status.success(), "{}", describe(&init));
    assert!(stderr(&init).contains("dependency check failed"));
    assert!(stderr(&init).contains("mkfs.xfs"));
    assert!(stderr(&init).contains("requires >= 4.9.0"));
    assert!(
        !harness.mutable.join("backingfiles.img").exists(),
        "init should fail before creating backingfiles.img"
    );
}

#[test]
#[ignore = "requires root, Linux loop devices, XFS reflinks, FAT32, and mount support"]
fn linux_archive_cycle_uses_real_loop_mounts_and_cleans_cam_disk() {
    linux_archive_cycle_uses_real_loop_mounts_and_cleans_cam_disk_for("fat32");
}

#[test]
#[ignore = "requires root, Linux loop devices, XFS, ext4, and mount support"]
fn linux_archive_cycle_uses_real_loop_mounts_and_cleans_cam_disk_ext4() {
    linux_archive_cycle_uses_real_loop_mounts_and_cleans_cam_disk_for("ext4");
}

fn linux_archive_cycle_uses_real_loop_mounts_and_cleans_cam_disk_for(filesystem: &str) {
    let harness = Harness::new("rclone");
    let mut contents = fs::read_to_string(&harness.config).unwrap();
    contents.push_str(&format!("CAM_FILESYSTEM={filesystem}\n"));
    fs::write(&harness.config, contents).unwrap();
    let config = harness.config_arg();
    let archive_root = harness.root.join("archive");

    assert_success(&harness.run(&["--config", &config, "init", "--reserve", "512M"]));
    {
        let cam = PartitionMount::mount(&harness.cam_disk(), &harness.root.join("cam-write"), "rw");
        write_cam_fixture(cam.path());
    }

    let archive = harness.run_with_env(
        &["--config", &config, "archive"],
        &[("TESLAUSB_FAKE_RCLONE_ARCHIVE", &archive_root)],
    );
    assert_success(&archive);
    assert!(stderr(&archive).contains("archive complete"));
    assert!(stderr(&archive).contains("clean up complete"));

    assert_archive_contains_fixture(&archive_root.join("fake:TeslaArchive"));
    assert!(!archive_root
        .join("fake:TeslaArchive/RecentClips/recent/skip.mp4")
        .exists());

    {
        let cam =
            PartitionMount::mount(&harness.cam_disk(), &harness.root.join("cam-verify"), "ro");
        assert_archived_files_removed_from_cam(cam.path());
    }

    let snapshots = harness.run(&["--config", &config, "snapshots", "--json"]);
    assert_success(&snapshots);
    assert_eq!(stdout(&snapshots).trim(), "[]");
}

#[test]
#[ignore = "requires root, real rclone, Linux loop devices, XFS reflinks, and FAT32 mounts"]
fn linux_real_rclone_confirms_copies_and_preserves_unconfirmed_files() {
    linux_real_rclone_confirms_copies_and_preserves_unconfirmed_files_for("fat32");
}

#[test]
#[ignore = "requires root, Linux loop devices, XFS, ext4, and mount support"]
fn linux_real_rclone_confirms_copies_and_preserves_unconfirmed_files_ext4() {
    linux_real_rclone_confirms_copies_and_preserves_unconfirmed_files_for("ext4");
}

fn linux_real_rclone_confirms_copies_and_preserves_unconfirmed_files_for(filesystem: &str) {
    let harness = Harness::new("rclone");
    let mut contents = fs::read_to_string(&harness.config).unwrap();
    contents.push_str(&format!("CAM_FILESYSTEM={filesystem}\n"));
    fs::write(&harness.config, contents).unwrap();
    let config = harness.config_arg();
    let real_rclone = run_shell("command -v rclone");
    assert_success(&real_rclone);
    let rclone_binary = stdout(&real_rclone);
    fs::remove_file(harness.fake_bin.join("rclone")).unwrap();
    std::os::unix::fs::symlink(rclone_binary.trim(), harness.fake_bin.join("rclone")).unwrap();
    let version = run(rclone_binary.trim(), ["version"]);
    assert_success(&version);
    eprintln!("real rclone integration version:\n{}", stdout(&version));

    let archive_root = harness.root.join("archive-real");
    fs::create_dir_all(&archive_root).unwrap();
    let config_home = harness.root.join("config");
    fs::create_dir_all(config_home.join("rclone")).unwrap();
    fs::write(
        config_home.join("rclone/rclone.conf"),
        format!(
            "[fake]\ntype = alias\nremote = {}\n",
            archive_root.display()
        ),
    )
    .unwrap();
    let archive_destination = archive_root.join("TeslaArchive");
    let config_template = fs::read_to_string(&harness.config).unwrap();
    let set_flags = |flags: &str| {
        fs::write(
            &harness.config,
            config_template.replace("RCLONE_FLAGS=--fast-list", &format!("RCLONE_FLAGS={flags}")),
        )
        .unwrap();
    };
    let archive_command = || {
        let mut command = harness.command(&["--config", &config, "archive"]);
        command
            .env("XDG_CONFIG_HOME", &config_home)
            .env_remove("RCLONE_CONFIG")
            .env("HOME", "/root")
            .env_remove("XDG_CACHE_HOME");
        command
    };
    let archive = || archive_command().output().unwrap();
    assert_success(&harness.run(&["--config", &config, "init", "--reserve", "512M"]));
    {
        let cam = PartitionMount::mount(&harness.cam_disk(), &harness.root.join("cam-real"), "rw");
        for path in [
            "TeslaCam/SavedClips",
            "TeslaCam/SentryClips",
            "TeslaCam/Photobooth",
            "TeslaTrackMode",
        ] {
            fs::create_dir_all(cam.path().join(path)).unwrap();
        }
    }
    let empty = archive();
    assert_success(&empty);
    assert!(
        stderr(&empty).contains("archive complete: 0 files"),
        "{}",
        describe(&empty)
    );

    for copied in [true, false] {
        {
            let cam =
                PartitionMount::mount(&harness.cam_disk(), &harness.root.join("cam-real"), "rw");
            write_cam_fixture(cam.path());
        }
        set_flags("--checksum");
        let output = archive();
        assert_success(&output);
        let expected = if copied {
            "archive complete: 4 files"
        } else {
            "archive complete: 0 files"
        };
        assert!(stderr(&output).contains(expected), "{}", describe(&output));
        assert_archive_contains_fixture(&archive_destination);
        let cam = PartitionMount::mount(&harness.cam_disk(), &harness.root.join("cam-real"), "ro");
        assert_archived_files_removed_from_cam(cam.path());
    }

    let unconfirmed = "TeslaCam/SavedClips/unconfirmed";
    {
        let cam = PartitionMount::mount(&harness.cam_disk(), &harness.root.join("cam-real"), "rw");
        write_file(
            cam.path().join(unconfirmed).join("front.mp4"),
            "unconfirmed-video",
        );
        write_file(
            cam.path().join(unconfirmed).join("event.json"),
            "event-metadata",
        );
    }
    set_flags("--dry-run");
    let dry_run = archive();
    assert_success(&dry_run);
    assert!(
        stderr(&dry_run).contains("archive complete: 0 files"),
        "{}",
        describe(&dry_run)
    );
    assert!(!archive_destination
        .join("SavedClips/unconfirmed/front.mp4")
        .exists());
    assert!(!archive_destination
        .join("SavedClips/unconfirmed/event.json")
        .exists());
    {
        let cam = PartitionMount::mount(&harness.cam_disk(), &harness.root.join("cam-real"), "ro");
        assert_eq!(
            fs::read_to_string(cam.path().join(unconfirmed).join("front.mp4")).unwrap(),
            "unconfirmed-video"
        );
        assert_eq!(
            fs::read_to_string(cam.path().join(unconfirmed).join("event.json")).unwrap(),
            "event-metadata"
        );
    }

    set_flags("--exclude *.json");
    let excluded = archive();
    assert_success(&excluded);
    assert!(
        stderr(&excluded).contains("archive complete: 1 files"),
        "{}",
        describe(&excluded)
    );
    assert_eq!(
        fs::read_to_string(archive_destination.join("SavedClips/unconfirmed/front.mp4")).unwrap(),
        "unconfirmed-video"
    );
    assert!(!archive_destination
        .join("SavedClips/unconfirmed/event.json")
        .exists());
    {
        let cam = PartitionMount::mount(&harness.cam_disk(), &harness.root.join("cam-real"), "ro");
        assert_eq!(
            fs::read_to_string(cam.path().join(unconfirmed).join("front.mp4")).unwrap(),
            "unconfirmed-video"
        );
        assert_eq!(
            fs::read_to_string(cam.path().join(unconfirmed).join("event.json")).unwrap(),
            "event-metadata"
        );
    }

    set_flags("--checksum");
    let configured = archive_command()
        .env("RCLONE_CONFIG", config_home.join("rclone/rclone.conf"))
        .output()
        .unwrap();
    assert_success(&configured);
    assert!(
        stderr(&configured).contains("archive complete: 1 files"),
        "{}",
        describe(&configured)
    );
    assert_eq!(
        fs::read_to_string(archive_destination.join("SavedClips/unconfirmed/event.json")).unwrap(),
        "event-metadata"
    );
    let cam = PartitionMount::mount(&harness.cam_disk(), &harness.root.join("cam-real"), "ro");
    assert!(!cam.path().join(unconfirmed).exists());
}

#[test]
#[ignore = "requires root, Linux loop devices, XFS reflinks, FAT32, and mount support"]
fn linux_archive_refuses_to_repair_an_already_mounted_camera_image() {
    let harness = Harness::new("rclone");
    let config = harness.config_arg();
    let archive_root = harness.root.join("archive-mounted");
    let fsck_marker = harness.root.join("unexpected-fsck");

    assert_success(&harness.run(&["--config", &config, "init", "--reserve", "512M"]));
    let cam = PartitionMount::mount(
        &harness.cam_disk(),
        &harness.root.join("cam-still-mounted"),
        "rw",
    );
    write_cam_fixture(cam.path());
    assert_success(&run("sync", std::iter::empty::<&str>()));

    let fsck = harness.fake_bin.join("fsck");
    fs::write(
        &fsck,
        r#"#!/bin/sh
set -eu
case "${1:-}" in
    -p|-n)
        : > "$TESLAUSB_FSCK_MARKER"
        printf 'fsck must not run while the camera image is mounted\n' >&2
        exit 97
        ;;
esac
exec /usr/sbin/fsck "$@"
"#,
    )
    .unwrap();
    fs::set_permissions(&fsck, fs::Permissions::from_mode(0o755)).unwrap();

    let archive = harness.run_with_env(
        &["--config", &config, "archive"],
        &[
            ("TESLAUSB_FAKE_RCLONE_ARCHIVE", &archive_root),
            ("TESLAUSB_FSCK_MARKER", &fsck_marker),
        ],
    );
    assert!(!archive.status.success(), "{}", describe(&archive));
    assert!(
        stderr(&archive).contains("still mounted locally"),
        "{}",
        describe(&archive)
    );
    assert!(
        !fsck_marker.exists(),
        "fsck must be rejected before its process starts"
    );
    assert_archive_contains_fixture(&archive_root.join("fake:TeslaArchive"));
    assert_success(&run(
        "mountpoint",
        [OsStr::new("-q"), cam.path().as_os_str()],
    ));
    assert_eq!(
        fs::read_to_string(cam.path().join("TeslaCam/SavedClips/event/front.mp4")).unwrap(),
        "saved-front"
    );
}

#[test]
#[ignore = "requires root, Linux loop devices, XFS reflinks, FAT32, and mount support"]
fn linux_failed_archive_cleans_files_confirmed_before_rclone_error() {
    let harness = Harness::new("rclone");
    let config = harness.config_arg();
    let archive_root = harness.root.join("archive-failed");
    let fail_after_copy = harness.root.join("SentryClips");

    assert_success(&harness.run(&["--config", &config, "init", "--reserve", "512M"]));
    {
        let cam = PartitionMount::mount(
            &harness.cam_disk(),
            &harness.root.join("cam-fail-write"),
            "rw",
        );
        write_cam_fixture(cam.path());
    }

    let archive = harness.run_with_env(
        &["--config", &config, "archive"],
        &[
            ("TESLAUSB_FAKE_RCLONE_ARCHIVE", &archive_root),
            ("TESLAUSB_FAKE_RCLONE_FAIL_AFTER_COPY", &fail_after_copy),
        ],
    );
    assert!(!archive.status.success(), "{}", describe(&archive));
    assert!(stderr(&archive).contains("warning: archive finished with issues"));
    assert!(stderr(&archive).contains("SentryClips"));
    assert!(stderr(&archive).contains("clean up complete"));

    assert_archive_contains_fixture(&archive_root.join("fake:TeslaArchive"));
    {
        let cam = PartitionMount::mount(
            &harness.cam_disk(),
            &harness.root.join("cam-fail-verify"),
            "ro",
        );
        assert_archived_files_removed_from_cam(cam.path());
    }

    let snapshots = harness.run(&["--config", &config, "snapshots", "--json"]);
    assert_success(&snapshots);
    assert_eq!(stdout(&snapshots).trim(), "[]");
}

#[test]
#[ignore = "requires root, Linux loop devices, XFS reflinks, FAT32, and mount support"]
fn linux_run_loop_uses_real_mounts_and_stops_cleanly_on_sigterm() {
    let harness = Harness::new("rclone");
    let config = harness.config_arg();
    let archive_root = harness.root.join("archive-run");

    assert_success(&harness.run(&["--config", &config, "init", "--reserve", "512M"]));
    {
        let cam = PartitionMount::mount(
            &harness.cam_disk(),
            &harness.root.join("cam-run-write"),
            "rw",
        );
        write_cam_fixture(cam.path());
    }

    let thermal_path = harness.root.join("thermal");
    fs::write(&thermal_path, "45000").unwrap();
    let led_path = harness.root.join("led");
    fs::create_dir_all(&led_path).unwrap();
    for (name, content) in [
        ("trigger", "[none] timer heartbeat"),
        ("brightness", "0"),
        ("delay_on", "0"),
        ("delay_off", "0"),
        ("invert", "0"),
    ] {
        fs::write(led_path.join(name), content).unwrap();
    }
    let mut child = harness.spawn_with_env(
        &["--config", &config, "run"],
        &[
            ("TESLAUSB_FAKE_RCLONE_ARCHIVE", &archive_root),
            ("TESLAUSB_THERMAL_PATH", &thermal_path),
            ("TESLAUSB_LED_PATH", &led_path),
        ],
    );

    if !wait_until(
        || {
            archive_root
                .join("fake:TeslaArchive/Photobooth/photo.jpg")
                .exists()
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
        Duration::from_secs(30),
    ) {
        terminate_child(&mut child);
        let output = child.wait_with_output().unwrap();
        panic!("run loop did not archive in time\n{}", describe(&output));
    }

    terminate_child(&mut child);
    let output = child.wait_with_output().unwrap();
    assert_success(&output);
    assert!(stderr(&output).contains("archive complete"));

    assert_archive_contains_fixture(&archive_root.join("fake:TeslaArchive"));
    {
        let cam = PartitionMount::mount(
            &harness.cam_disk(),
            &harness.root.join("cam-run-verify"),
            "ro",
        );
        assert_archived_files_removed_from_cam(cam.path());
    }
}

#[test]
#[ignore = "requires root, Linux loop devices, XFS, FAT32, and mount support"]
fn linux_snapshot_inspection_preserves_incomplete_data_until_recovery() {
    let harness = Harness::new("none");
    let config = harness.config_arg();

    assert_success(&harness.run(&["--config", &config, "init", "--reserve", "512M"]));
    let incomplete = harness.backingfiles.join("snapshots/snap-000123");
    fs::create_dir_all(&incomplete).unwrap();
    fs::write(incomplete.join("snap.bin"), b"incomplete snapshot").unwrap();
    assert!(incomplete.exists());

    let snapshots = harness.run(&["--config", &config, "snapshots", "--json"]);

    assert_success(&snapshots);
    assert_eq!(stdout(&snapshots).trim(), "[]");
    assert!(
        incomplete.exists(),
        "snapshot inspection must preserve incomplete data"
    );
    assert_success(&harness.run(&["--config", &config, "archive"]));
    assert!(
        !incomplete.exists(),
        "archive start should recover incomplete snapshots"
    );
}

#[test]
#[ignore = "requires root, Linux loop devices, XFS reflinks, ext4, and mount support"]
fn linux_dirty_ext4_snapshot_replays_privately_and_preserves_camera_bytes() {
    let harness = Harness::new("rclone");
    let config = harness.config_arg();
    let mut contents = fs::read_to_string(&harness.config).unwrap();
    contents.push_str("CAM_FILESYSTEM=ext4\nARCHIVE_RECENTCLIPS=true\n");
    fs::write(&harness.config, contents).unwrap();
    assert_success(&harness.run(&["--config", &config, "init", "--reserve", "512M"]));
    let dirty = harness.backingfiles.join("dirty-fixture.bin");
    {
        let cam = PartitionMount::mount(&harness.cam_disk(), &harness.root.join("cam-dirty"), "rw");
        let recent = cam.path().join("TeslaCam/RecentClips");
        fs::create_dir_all(&recent).unwrap();
        for camera in [
            "front",
            "back",
            "left_repeater",
            "right_repeater",
            "left_pillar",
            "right_pillar",
        ] {
            let path = recent.join(format!("2026-10-02_12-34-56-{camera}.mp4"));
            fs::write(&path, format!("durable-{camera}")).unwrap();
            fs::File::open(path).unwrap().sync_all().unwrap();
        }
        fs::File::open(&recent).unwrap().sync_all().unwrap();
        fs::File::open(cam.path().join("TeslaCam"))
            .unwrap()
            .sync_all()
            .unwrap();
        assert_success(&run(
            "cp",
            [
                OsStr::new("--reflink=always"),
                harness.cam_disk().as_os_str(),
                dirty.as_os_str(),
            ],
        ));
    }
    // The source was copied while mounted. Its journal still requires recovery,
    // unlike the original camera image after the checked unmount above.
    assert_success(&run(
        "cp",
        [
            OsStr::new("--reflink=always"),
            dirty.as_os_str(),
            harness.cam_disk().as_os_str(),
        ],
    ));
    let before = run("sha256sum", [harness.cam_disk().as_os_str()]);
    assert_success(&before);
    let archive_root = harness.root.join("archive-dirty");
    let archive = harness
        .command(&["--config", &config, "archive"])
        .env("TESLAUSB_FAKE_RCLONE_ARCHIVE", &archive_root)
        .env(
            "TESLAUSB_RAW_SNAPSHOT_SHA256",
            stdout(&before).split_whitespace().next().unwrap(),
        )
        .env(
            "TESLAUSB_SNAPSHOT_CATALOG",
            harness.backingfiles.join("snapshots"),
        )
        .output()
        .unwrap();
    assert_success(&archive);
    assert!(
        stderr(&archive).contains("recovering journal"),
        "{}",
        describe(&archive)
    );
    let after = run("sha256sum", [harness.cam_disk().as_os_str()]);
    assert_success(&after);
    assert_eq!(
        stdout(&before),
        stdout(&after),
        "archive changed dirty camera bytes"
    );
    for camera in [
        "front",
        "back",
        "left_repeater",
        "right_repeater",
        "left_pillar",
        "right_pillar",
    ] {
        let path = archive_root.join(format!(
            "fake:TeslaArchive/RecentClips/2026-10-02/2026-10-02_12-34-56-{camera}.mp4"
        ));
        assert_eq!(
            fs::read_to_string(path).unwrap(),
            format!("durable-{camera}")
        );
    }
    assert!(fs::read_dir(harness.backingfiles.join("snapshots"))
        .unwrap()
        .all(|entry| { !entry.unwrap().file_name().to_string_lossy().eq("recovery") }));
}

#[test]
#[ignore = "requires root, Linux loop devices, XFS reflinks, ext4, and mount support"]
fn linux_ext4_corruption_stops_archive_and_preserves_recovery_evidence() {
    let harness = Harness::new("rclone");
    let config = harness.config_arg();
    let mut contents = fs::read_to_string(&harness.config).unwrap();
    contents.push_str("CAM_FILESYSTEM=ext4\n");
    fs::write(&harness.config, contents).unwrap();
    assert_success(&harness.run(&["--config", &config, "init", "--reserve", "512M"]));
    {
        let cam =
            PartitionMount::mount(&harness.cam_disk(), &harness.root.join("cam-corrupt"), "rw");
        write_cam_fixture(cam.path());
        let source = run(
            "findmnt",
            [
                "-n",
                "-o",
                "SOURCE",
                "--target",
                cam.path().to_str().unwrap(),
            ],
        );
        assert_success(&source);
        assert_success(&run("umount", [cam.path().as_os_str()]));
        assert_success(&run(
            "debugfs",
            [
                "-w",
                "-R",
                "set_inode_field /TeslaCam/SavedClips/event/front.mp4 links_count 2",
                stdout(&source).trim(),
            ],
        ));
    }
    let before = run("sha256sum", [harness.cam_disk().as_os_str()]);
    assert_success(&before);
    let archive_root = harness.root.join("archive-corrupt");
    let archive = harness.run_with_env(
        &["--config", &config, "archive"],
        &[("TESLAUSB_FAKE_RCLONE_ARCHIVE", &archive_root)],
    );
    assert!(!archive.status.success(), "{}", describe(&archive));
    assert!(
        stderr(&archive).contains("filesystem remains inconsistent"),
        "{}",
        describe(&archive)
    );
    assert!(!archive_root.exists());
    let after = run("sha256sum", [harness.cam_disk().as_os_str()]);
    assert_success(&after);
    assert_eq!(stdout(&before), stdout(&after));
    let recovery: Vec<_> = fs::read_dir(harness.backingfiles.join("snapshots"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.file_name().unwrap().to_string_lossy().eq("recovery"))
        .collect();
    assert_eq!(recovery.len(), 1);
    let raw = run("sha256sum", [recovery[0].join("raw.bin").as_os_str()]);
    assert_success(&raw);
    assert_eq!(
        stdout(&raw).split_whitespace().next(),
        stdout(&before).split_whitespace().next()
    );
    assert_success(&harness.run(&["--config", &config, "clean"]));
    assert!(recovery[0].join("raw.bin").is_file());
    assert!(recovery[0].join("recovered.bin").is_file());
    assert!(loops_backed_under(&harness.backingfiles)
        .unwrap()
        .is_empty());
}

struct MountGuard {
    image: PathBuf,
    mount_point: PathBuf,
    loop_dev: Option<String>,
    cleaned: bool,
}

impl MountGuard {
    fn xfs_loop(image: PathBuf, mount_point: &Path, size: &str) -> Self {
        assert_success(&run(
            "truncate",
            [OsStr::new("-s"), OsStr::new(size), image.as_os_str()],
        ));
        assert_success(&run("mkfs.xfs", [OsStr::new("-f"), image.as_os_str()]));
        fs::create_dir_all(mount_point).unwrap();
        let output = run(
            "losetup",
            [OsStr::new("-f"), OsStr::new("--show"), image.as_os_str()],
        );
        assert_success(&output);
        let loop_dev = stdout(&output).trim().to_string();
        assert!(!loop_dev.is_empty(), "losetup produced no loop device");
        let guard = Self {
            image,
            mount_point: mount_point.to_path_buf(),
            loop_dev: Some(loop_dev),
            cleaned: false,
        };
        assert_success(&run(
            "mount",
            [
                OsStr::new(guard.loop_dev.as_ref().unwrap()),
                mount_point.as_os_str(),
            ],
        ));
        guard
    }

    fn clean_up(&mut self) -> Result<(), String> {
        if self.cleaned {
            return Ok(());
        }
        unmount_fixture(&self.mount_point)?;
        if let Some(loop_dev) = &self.loop_dev {
            checked_run("losetup", ["-d", loop_dev])?;
            self.loop_dev = None;
        }
        wait_for_detached_loops(&self.image)?;
        self.cleaned = true;
        Ok(())
    }
}

impl Drop for MountGuard {
    fn drop(&mut self) {
        let result = self.clean_up();
        report_cleanup_result(&self.image, result);
    }
}

struct PartitionMount {
    image: PathBuf,
    loop_dev: String,
    mount_point: PathBuf,
    kpartx_used: bool,
}

impl PartitionMount {
    fn mount(image: &Path, mount_point: &Path, mode: &str) -> Self {
        fs::create_dir_all(mount_point).unwrap();
        let output = run(
            "losetup",
            [OsStr::new("-Pf"), OsStr::new("--show"), image.as_os_str()],
        );
        assert_success(&output);
        let loop_dev = stdout(&output).trim().to_string();
        assert!(!loop_dev.is_empty(), "losetup produced no loop device");

        let _ = run_status(
            "blockdev",
            [OsStr::new("--rereadpt"), OsStr::new(&loop_dev)],
        );
        let direct_partition = format!("{loop_dev}p1");
        let (partition, kpartx_used) = if wait_for_path(Path::new(&direct_partition)) {
            (direct_partition, false)
        } else {
            assert_success(&run("kpartx", [OsStr::new("-av"), OsStr::new(&loop_dev)]));
            let loop_name = Path::new(&loop_dev)
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string();
            let mapper = format!("/dev/mapper/{loop_name}p1");
            assert!(
                wait_for_path(Path::new(&mapper)),
                "partition device did not appear for {image:?}"
            );
            (mapper, true)
        };

        assert_success(&run(
            "mount",
            [
                OsStr::new("-o"),
                OsStr::new(mode),
                OsStr::new(&partition),
                mount_point.as_os_str(),
            ],
        ));

        Self {
            image: image.to_path_buf(),
            loop_dev,
            mount_point: mount_point.to_path_buf(),
            kpartx_used,
        }
    }

    fn path(&self) -> &Path {
        &self.mount_point
    }

    fn clean_up(&self) -> Result<(), String> {
        unmount_fixture(&self.mount_point)?;
        if self.kpartx_used {
            checked_run("kpartx", ["-d", &self.loop_dev])?;
        }
        checked_run("losetup", ["-d", &self.loop_dev])?;
        wait_for_detached_loops(&self.image)
    }
}

impl Drop for PartitionMount {
    fn drop(&mut self) {
        report_cleanup_result(&self.image, self.clean_up());
    }
}

fn report_cleanup_result(fixture: &Path, result: Result<(), String>) {
    if let Err(err) = result {
        let message = format!(
            "failed to clean up {}; fixture retained: {err}",
            fixture.display()
        );
        if thread::panicking() {
            eprintln!("{message}");
        } else {
            panic!("{message}");
        }
    }
}

fn unmount_fixture(mount_point: &Path) -> Result<(), String> {
    // Inner loops can keep this filesystem busy after their own mounts are gone.
    wait_for_detached_loops(mount_point)?;
    if !mount_point
        .try_exists()
        .map_err(|err| format!("stat {}: {err}", mount_point.display()))?
    {
        return Ok(());
    }
    let output = checked_output("mountpoint", [OsStr::new("-q"), mount_point.as_os_str()])?;
    match output.status.code() {
        Some(0) => unmount_when_unused(mount_point),
        Some(32) => Ok(()),
        _ => Err(format!(
            "mountpoint {}: {}",
            mount_point.display(),
            describe(&output)
        )),
    }
}

fn unmount_when_unused(mount_point: &Path) -> Result<(), String> {
    let target = CString::new(mount_point.as_os_str().as_bytes())
        .map_err(|err| format!("invalid mount path {}: {err}", mount_point.display()))?;
    let started = Instant::now();
    loop {
        // The CString is alive for the call; umount does not retain its pointer.
        if unsafe { umount(target.as_ptr()) } == 0 {
            return Ok(());
        }
        let err = std::io::Error::last_os_error();
        if err.kind() != std::io::ErrorKind::ResourceBusy || started.elapsed() >= CLEANUP_TIMEOUT {
            return Err(format!("umount {}: {err}", mount_point.display()));
        }
        // Loop sysfs removal can precede the kernel's final backing-file release.
        thread::sleep(Duration::from_millis(20));
    }
}

fn wait_for_detached_loops(scope: &Path) -> Result<(), String> {
    let started = Instant::now();
    loop {
        let attached = loops_backed_under(scope)?;
        if attached.is_empty() {
            return Ok(());
        }
        if started.elapsed() >= CLEANUP_TIMEOUT {
            return Err(format!(
                "loop devices still hold {}: {}",
                scope.display(),
                attached.join(", ")
            ));
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn loops_backed_under(scope: &Path) -> Result<Vec<String>, String> {
    let mut attached = Vec::new();
    for entry in fs::read_dir("/sys/block").map_err(|err| format!("read /sys/block: {err}"))? {
        let entry = entry.map_err(|err| format!("read /sys/block entry: {err}"))?;
        if !entry.file_name().as_encoded_bytes().starts_with(b"loop") {
            continue;
        }
        let path = entry.path().join("loop/backing_file");
        let backing = match fs::read_to_string(&path) {
            Ok(backing) => backing,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(format!("read {}: {err}", path.display())),
        };
        let backing = backing
            .trim()
            .replace("\\040", " ")
            .replace("\\011", "\t")
            .replace("\\134", "\\");
        let backing = backing.strip_suffix(" (deleted)").unwrap_or(&backing);
        if Path::new(backing).starts_with(scope) {
            attached.push(entry.file_name().to_string_lossy().into_owned());
        }
    }
    Ok(attached)
}

fn checked_output<I, S>(program: &str, args: I) -> Result<Output, String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    Command::new(program)
        .args(args)
        .stdin(Stdio::null())
        .output()
        .map_err(|err| format!("run {program}: {err}"))
}

fn checked_run<I, S>(program: &str, args: I) -> Result<(), String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = checked_output(program, args)?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!("{program}: {}", describe(&output)))
    }
}

fn require_linux_integration() {
    if env::var_os("TESLAUSB_RUN_LINUX_INTEGRATION").is_none() {
        panic!("set TESLAUSB_RUN_LINUX_INTEGRATION=1 or use scripts/run-linux-integration.sh");
    }

    let uid = run("id", [OsStr::new("-u")]);
    assert_success(&uid);
    assert_eq!(
        stdout(&uid).trim(),
        "0",
        "Linux integration tests must run as root"
    );

    for command in [
        "blkid",
        "blockdev",
        "cp",
        "debugfs",
        "df",
        "dumpe2fs",
        "findmnt",
        "fsck",
        "fsck.fat",
        "e2fsck",
        "kpartx",
        "losetup",
        "mkfs.ext4",
        "mkfs.vfat",
        "mkfs.xfs",
        "mount",
        "mountpoint",
        "modprobe",
        "parted",
        "rclone",
        "sha256sum",
        "stat",
        "sync",
        "truncate",
        "umount",
    ] {
        assert_success(&run_shell(&format!("command -v {command}")));
    }
}

fn run_shell(script: &str) -> Output {
    Command::new("sh")
        .arg("-c")
        .arg(script)
        .stdin(Stdio::null())
        .output()
        .unwrap_or_else(|err| panic!("failed to run shell command {script:?}: {err}"))
}

fn temp_path(prefix: &str) -> PathBuf {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let counter = COUNTER.fetch_add(1, Ordering::SeqCst);
    env::temp_dir().join(format!(
        "{prefix}-{}-{counter}-{suffix}",
        std::process::id()
    ))
}

fn wait_for_path(path: &Path) -> bool {
    for _ in 0..50 {
        if path.exists() {
            return true;
        }
        thread::sleep(Duration::from_millis(100));
    }
    path.exists()
}

fn wait_until(mut predicate: impl FnMut() -> bool, timeout: Duration) -> bool {
    let started = Instant::now();
    while started.elapsed() < timeout {
        if predicate() {
            return true;
        }
        thread::sleep(Duration::from_millis(100));
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

fn write_file(path: PathBuf, content: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, content).unwrap();
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

fn assert_archive_contains_fixture(archive_root: &Path) {
    assert_eq!(
        fs::read_to_string(archive_root.join("SavedClips/event/front.mp4")).unwrap(),
        "saved-front"
    );
    assert_eq!(
        fs::read_to_string(archive_root.join("SentryClips/sentry/rear.mp4")).unwrap(),
        "sentry-rear"
    );
    assert_eq!(
        fs::read_to_string(archive_root.join("TrackMode/lap/video.mp4")).unwrap(),
        "track-video"
    );
    assert_eq!(
        fs::read_to_string(archive_root.join("Photobooth/photo.jpg")).unwrap(),
        "photo"
    );
}

fn assert_archived_files_removed_from_cam(cam_root: &Path) {
    assert!(!cam_root
        .join("TeslaCam/SavedClips/event/front.mp4")
        .exists());
    assert!(!cam_root
        .join("TeslaCam/SentryClips/sentry/rear.mp4")
        .exists());
    assert!(!cam_root.join("TeslaCam/Photobooth/photo.jpg").exists());
    assert!(!cam_root.join("TeslaTrackMode/lap/video.mp4").exists());
    assert!(cam_root
        .join("TeslaCam/RecentClips/recent/skip.mp4")
        .exists());
}

fn write_fake_old_mkfs_xfs(path: &Path) {
    let script = r#"#!/bin/sh
set -eu
if [ "${1:-}" = "-V" ] || [ "${1:-}" = "--version" ]; then
    printf 'mkfs.xfs version 4.8.0\n'
    exit 0
fi
printf 'old mkfs.xfs should not be used for formatting\n' >&2
exit 42
"#;
    fs::write(path, script).unwrap();
    let mut permissions = fs::metadata(path).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).unwrap();
}

fn write_fake_rclone(path: &Path) {
    let script = r#"#!/bin/sh
set -eu
case "${1:-}" in
    version)
        printf 'rclone v1.65.0\n'
        exit 0
        ;;
    lsf)
        exit 0
        ;;
    copy)
        src="$2"
        dst="$3"
        device=$(findmnt -n -o SOURCE --target "$src")
        test "$(blockdev --getro "$device")" = 1
        if [ -n "${TESLAUSB_RAW_SNAPSHOT_SHA256:-}" ]; then
            for snapshot in "$TESLAUSB_SNAPSHOT_CATALOG"/snap-*/snap.bin; do
                actual=$(sha256sum "$snapshot")
                test "${actual%% *}" = "$TESLAUSB_RAW_SNAPSHOT_SHA256"
            done
        fi

        log_file=''
        previous=''
        for arg in "$@"; do
            if [ "$previous" = "--log-file" ]; then log_file="$arg"; fi
            previous="$arg"
        done
        : "${log_file:?rclone copy requires a JSON log file}"
        exec 2>"$log_file"
        archive="${TESLAUSB_FAKE_RCLONE_ARCHIVE:?}"
        mkdir -p "$archive/$dst"
        /bin/cp -R "$src"/. "$archive/$dst"/
        (cd "$src" && find . -type f) | while IFS= read -r file; do
            file=${file#./}
            relative="${file#"$src"/}"
                printf '{"object":"%s","msg":"Copied (new)"}\n' "$relative" >&2
        done
        fail_after="${TESLAUSB_FAKE_RCLONE_FAIL_AFTER_COPY:-}"
        if [ -n "$fail_after" ]; then
            fail_name=$(basename "$fail_after")
            case "$dst" in
                *"$fail_name"*)
                    printf '{"msg":"injected rclone failure after copying %s","level":"error"}\n' "$dst" >&2
                    exit 9
                    ;;
            esac
        fi
        exit 0
        ;;
esac
printf 'unexpected rclone invocation\n' >&2
exit 2
"#;
    fs::write(path, script).unwrap();
    let mut permissions = fs::metadata(path).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).unwrap();
}

fn run<I, S>(program: &str, args: I) -> Output
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    Command::new(program)
        .args(args)
        .stdin(Stdio::null())
        .output()
        .unwrap_or_else(|err| panic!("failed to run {program}: {err}"))
}

fn run_status<I, S>(program: &str, args: I) -> std::io::Result<std::process::ExitStatus>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    Command::new(program)
        .args(args)
        .stdin(Stdio::null())
        .status()
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
