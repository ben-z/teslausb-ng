#![cfg(target_os = "linux")]

use std::env;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct Harness {
    root: PathBuf,
    fake_bin: PathBuf,
    mutable: PathBuf,
    backingfiles: PathBuf,
    _mutable_mount: MountGuard,
    config: PathBuf,
    old_path: OsString,
}

impl Harness {
    fn new(archive_system: &str) -> Self {
        require_linux_integration();

        let root = temp_path("teslausb-linux");
        let fake_bin = root.join("bin");
        let mutable = root.join("mutable");
        let backingfiles = root.join("backingfiles");
        let config = root.join("teslausb.conf");
        fs::create_dir_all(&fake_bin).unwrap();
        fs::create_dir_all(&mutable).unwrap();
        fs::create_dir_all(&backingfiles).unwrap();

        let mutable_mount = MountGuard::xfs_loop(root.join("mutable-volume.img"), &mutable, "3G");
        let mut config_content = format!(
            "MUTABLE_PATH={}\nBACKINGFILES_PATH={}\nARCHIVE_SYSTEM={archive_system}\n",
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
            _mutable_mount: mutable_mount,
            config,
            old_path: env::var_os("PATH").unwrap_or_default(),
        }
    }

    fn run(&self, args: &[&str]) -> Output {
        self.run_with_env(args, &[])
    }

    fn run_with_env(&self, args: &[&str], extra_env: &[(&str, &Path)]) -> Output {
        let mut path = OsString::from(&self.fake_bin);
        path.push(":");
        path.push(&self.old_path);

        let mut command = Command::new(env!("CARGO_BIN_EXE_teslausb"));
        command.args(args).env("PATH", path).stdin(Stdio::null());
        for (key, value) in extra_env {
            command.env(key, value);
        }
        command.output().unwrap()
    }

    fn spawn_with_env(&self, args: &[&str], extra_env: &[(&str, &Path)]) -> Child {
        let mut path = OsString::from(&self.fake_bin);
        path.push(":");
        path.push(&self.old_path);

        let mut command = Command::new(env!("CARGO_BIN_EXE_teslausb"));
        command
            .args(args)
            .env("PATH", path)
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

    fn cam_disk(&self) -> PathBuf {
        self.backingfiles.join("cam_disk.bin")
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ = run_status("umount", [&self.backingfiles]);
        let _ = fs::remove_dir_all(&self.root);
    }
}

#[test]
#[ignore = "requires root, Linux loop devices, XFS, FAT32, and mount support"]
fn linux_init_mount_status_and_deinit_with_real_images() {
    let harness = Harness::new("none");
    let config = harness.config_arg();

    assert_success(&harness.run(&["--config", &config, "init", "--reserve", "512M"]));
    assert!(harness.mutable.join("backingfiles.img").is_file());
    assert!(harness.backingfiles.join("cam_disk.bin").is_file());
    assert!(harness.backingfiles.join("snapshots").is_dir());

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
    let harness = Harness::new("rclone");
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

    assert_archive_contains_fixture(&archive_root);
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

    assert_archive_contains_fixture(&archive_root);
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

    let mut child = harness.spawn_with_env(
        &["--config", &config, "run"],
        &[("TESLAUSB_FAKE_RCLONE_ARCHIVE", &archive_root)],
    );

    if !wait_until(
        || {
            archive_root
                .join("fake:TeslaArchive/SavedClips/event/front.mp4")
                .exists()
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

    assert_archive_contains_fixture(&archive_root);
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
fn linux_incomplete_snapshot_directory_is_cleaned_on_load() {
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
        !incomplete.exists(),
        "snapshot load should remove directories without snap.toc"
    );
}

struct MountGuard {
    mount_point: PathBuf,
}

impl MountGuard {
    fn xfs_loop(image: PathBuf, mount_point: &Path, size: &str) -> Self {
        assert_success(&run(
            "truncate",
            [OsStr::new("-s"), OsStr::new(size), image.as_os_str()],
        ));
        assert_success(&run("mkfs.xfs", [OsStr::new("-f"), image.as_os_str()]));
        fs::create_dir_all(mount_point).unwrap();
        assert_success(&run(
            "mount",
            [
                OsStr::new("-o"),
                OsStr::new("loop"),
                image.as_os_str(),
                mount_point.as_os_str(),
            ],
        ));
        Self {
            mount_point: mount_point.to_path_buf(),
        }
    }
}

impl Drop for MountGuard {
    fn drop(&mut self) {
        let _ = run_status("umount", [&self.mount_point]);
    }
}

struct PartitionMount {
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
            loop_dev,
            mount_point: mount_point.to_path_buf(),
            kpartx_used,
        }
    }

    fn path(&self) -> &Path {
        &self.mount_point
    }
}

impl Drop for PartitionMount {
    fn drop(&mut self) {
        let _ = run_status("sync", std::iter::empty::<&OsStr>());
        let _ = run_status("umount", [&self.mount_point]);
        if self.kpartx_used {
            let _ = run_status("kpartx", [OsStr::new("-d"), OsStr::new(&self.loop_dev)]);
        }
        let _ = run_status("losetup", [OsStr::new("-d"), OsStr::new(&self.loop_dev)]);
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
        "blockdev",
        "cp",
        "df",
        "fsck",
        "kpartx",
        "losetup",
        "mkfs.vfat",
        "mkfs.xfs",
        "mount",
        "mountpoint",
        "modprobe",
        "parted",
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
        fs::read_to_string(archive_root.join("fake:TeslaArchive/SavedClips/event/front.mp4"))
            .unwrap(),
        "saved-front"
    );
    assert_eq!(
        fs::read_to_string(archive_root.join("fake:TeslaArchive/SentryClips/sentry/rear.mp4"))
            .unwrap(),
        "sentry-rear"
    );
    assert_eq!(
        fs::read_to_string(archive_root.join("fake:TeslaArchive/TrackMode/lap/video.mp4")).unwrap(),
        "track-video"
    );
    assert_eq!(
        fs::read_to_string(archive_root.join("fake:TeslaArchive/Photobooth/photo.jpg")).unwrap(),
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
        archive="${TESLAUSB_FAKE_RCLONE_ARCHIVE:?}"
        mkdir -p "$archive/$dst"
        /bin/cp -R "$src"/. "$archive/$dst"/
        (cd "$src" && find . -type f) | while IFS= read -r file; do
            file=${file#./}
            printf '%s: Copied (new)\n' "$file" >&2
        done
        fail_after="${TESLAUSB_FAKE_RCLONE_FAIL_AFTER_COPY:-}"
        if [ -n "$fail_after" ]; then
            fail_name=$(basename "$fail_after")
            case "$dst" in
                *"$fail_name"*)
                    printf 'injected rclone failure after copying %s\n' "$dst" >&2
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
