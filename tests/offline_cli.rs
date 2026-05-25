#![cfg(unix)]

use std::env;
use std::ffi::OsString;
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
        fs::create_dir_all(&mutable).unwrap();
        fs::create_dir_all(&backingfiles).unwrap();
        fs::create_dir_all(service_path.parent().unwrap()).unwrap();
        write_fake_tools(&bin);
        write_cam_fixture(&cam_source);

        let mut config_content = format!(
            "MUTABLE_PATH={}\nBACKINGFILES_PATH={}\nARCHIVE_SYSTEM={archive_system}\n",
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
            .env("PATH", path)
            .env("TESLAUSB_FAKE_STATE", &self.state)
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
            .env("PATH", path)
            .env("TESLAUSB_FAKE_STATE", &self.state)
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
        "mkfs.vfat\t-F\t32",
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
fn offline_status_before_init_warns_when_not_mounted() {
    let harness = Harness::new("none");
    let config = harness.config_arg();

    let status = harness.run(&["--config", &config, "status", "--json"]);

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
    let archive = harness.run(&["--config", &config, "archive"]);
    assert_success(&archive);
    assert!(stderr(&archive).contains("archive complete"));
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
    let archive = harness.run_with_env(
        &["--config", &config, "archive"],
        &[("TESLAUSB_FAKE_RCLONE_FAIL", "SavedClips")],
    );

    assert!(!archive.status.success(), "{}", describe(&archive));
    assert!(stderr(&archive).contains("warning: archive finished with issues"));
    assert!(stderr(&archive).contains("SavedClips"));
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
        ],
    );

    if !wait_until(
        || harness.archive_path("SavedClips/event/front.mp4").exists(),
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
    assert!(log.contains("fsck\t-p"), "{log}");
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
fn offline_service_install_status_and_uninstall() {
    let harness = Harness::new("none");

    let install = harness.run(&["service", "install", "--force"]);
    assert_success(&install);
    let service = fs::read_to_string(&harness.service_path).unwrap();
    assert!(service.contains("ExecStartPre="));
    assert!(service.contains(" doctor --startup\n"));
    assert!(service.contains(" mount\n"));
    assert!(service.contains(" gadget on\n"));
    assert!(service.contains("ExecStart="));
    assert!(service.contains(" run\n"));
    assert!(service.contains("ExecStop="));
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
    mount|mountpoint|umount|losetup|blockdev|fsck)
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
    mkfs.vfat)
        if [ "${1:-}" = "--version" ] || [ "${1:-}" = "-V" ]; then
            printf 'mkfs.fat %s (2021-01-31)\n' "${TESLAUSB_FAKE_DOSFSTOOLS_VERSION:-4.2}"
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
    sync|mkfs.xfs|parted|blockdev|mkfs.vfat|fsck|modprobe)
        exit 0
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
        : > "$path"
        exit 0
        ;;
    losetup)
        if [ "${1:-}" = "-d" ]; then
            exit 0
        fi
        image=$(last_arg "$@")
        loop="$state/loop0"
        partition="${loop}p1"
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
        target=$(last_arg "$@")
        if [ -f "$state/mounted/$(key_for "$target")" ]; then
            exit 0
        fi
        exit 1
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
        case "$src" in
            *p1)
                if [ -n "${TESLAUSB_FAKE_CAM_SOURCE:-}" ] && [ -d "$TESLAUSB_FAKE_CAM_SOURCE" ]; then
                    /bin/cp -R "$TESLAUSB_FAKE_CAM_SOURCE"/. "$target"/
                fi
                ;;
        esac
        exit 0
        ;;
    umount)
        target=$(last_arg "$@")
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
            fail="${TESLAUSB_FAKE_RCLONE_FAIL:-}"
            if [ -n "$fail" ]; then
                case "$dst" in
                    *"$fail"*)
                        printf 'injected rclone failure for %s\n' "$dst" >&2
                        exit 9
                        ;;
                esac
            fi
            dest="$state/archive/$dst"
            mkdir -p "$dest"
            /bin/cp -R "$src"/. "$dest"/
            find "$src" -type f | while IFS= read -r file; do
                printf '%s: Copied (new)\n' "$file" >&2
            done
            exit 0
        fi
        exit 0
        ;;
    systemctl)
        if [ "${1:-}" = "status" ]; then
            printf 'teslausb.service fake active\n'
        fi
        exit 0
        ;;
esac

printf 'unexpected fake tool: %s\n' "$tool" >&2
exit 127
"#;

    for tool in [
        "blockdev",
        "cp",
        "df",
        "fsck",
        "kpartx",
        "losetup",
        "mkfs.vfat",
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
    assert!(output.status.success(), "{}", describe(&output));
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
