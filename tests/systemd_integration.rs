#![cfg(target_os = "linux")]

use std::env;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const WAIT_TIMEOUT: Duration = Duration::from_secs(30);

struct Harness {
    root: PathBuf,
    unit: String,
    unit_path: PathBuf,
    config: PathBuf,
    installed: bool,
    cleanup_failed: bool,
}

impl Harness {
    fn new() -> Self {
        assert_eq!(output("id", &["-u"]).trim(), "0", "requires root");
        assert!(
            Path::new("/run/systemd/system").is_dir(),
            "requires a running systemd manager"
        );
        systemctl(&["show", "--property=Version", "--value"]).unwrap();
        let id = format!(
            "{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let root = env::temp_dir().join(format!("teslausb-systemd-{id}"));
        fs::create_dir(&root).unwrap();
        let root = fs::canonicalize(root).unwrap();
        let unit = format!("teslausb-systemd-test-{id}.service");
        let config = root.join("config %n ${EVIL} \"double\" 'single' \\slash.conf");
        let mut harness = Self {
            unit_path: Path::new("/run/systemd/system").join(&unit),
            root,
            unit,
            config,
            installed: false,
            cleanup_failed: false,
        };
        let bin = harness.root.join("tools");
        fs::create_dir(&bin).unwrap();
        for tool in [
            "blkid",
            "df",
            "truncate",
            "mkfs.xfs",
            "parted",
            "losetup",
            "blockdev",
            "mkfs.ext4",
            "mount",
            "mountpoint",
            "umount",
            "stat",
            "cp",
            "sync",
            "e2fsck",
            "modprobe",
            "systemctl",
        ] {
            write_executable(&bin.join(tool), "#!/bin/sh\nprintf '99.0.0 --reflink\\n'\n");
        }
        let executable_dir = harness.root.join("exe space %n $LITERAL ${EVIL}");
        fs::create_dir(&executable_dir).unwrap();
        let executable = executable_dir.join("teslausb");
        fs::copy(env!("CARGO_BIN_EXE_teslausb"), &executable).unwrap();
        fs::write(&harness.config, "ARCHIVE_SYSTEM=none\n").unwrap();
        let install = Command::new(&executable)
            .args([
                "--config",
                harness.config.to_str().unwrap(),
                "service",
                "install",
            ])
            .env_clear()
            .env(
                "PATH",
                format!("{}:{}", bin.display(), env::var("PATH").unwrap()),
            )
            .env("TESLAUSB_SYSTEMD_SERVICE_PATH", &harness.unit_path)
            .output()
            .unwrap();
        harness.installed = harness.unit_path.exists();
        assert_success(&install);
        assert!(harness.installed);
        write_executable(&executable, FAKE_EXECUTABLE);
        systemctl(&["daemon-reload"]).unwrap();
        harness
    }

    fn property(&self, name: &str) -> String {
        systemctl(&["show", &self.unit, "--property", name, "--value"])
            .unwrap()
            .trim()
            .to_string()
    }

    fn wait(&self, predicate: impl Fn() -> bool) {
        assert!(wait_until(predicate), "{}", self.diagnostics());
    }

    fn reset(&self, mode: &str) {
        assert_eq!(self.property("ActiveState"), "inactive");
        for (name, contents) in [
            ("mode", mode),
            ("usb", "on"),
            ("events", ""),
            ("run-count", "0"),
        ] {
            fs::write(self.root.join(name), contents).unwrap();
        }
        for entry in fs::read_dir(&self.root).unwrap() {
            let path = entry.unwrap().path();
            if path
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("ready-")
            {
                fs::remove_file(path).unwrap();
            }
        }
    }

    fn start(&self) {
        systemctl(&["start", "--no-block", &self.unit]).unwrap();
    }

    fn stop(&self) {
        systemctl(&["stop", "--no-block", &self.unit]).unwrap();
        self.wait(|| self.property("ActiveState") == "inactive");
    }

    fn actions(&self) -> Vec<Vec<String>> {
        let bytes = fs::read(self.root.join("events")).unwrap();
        let mut fields = bytes.split(|byte| *byte == 0).peekable();
        let mut actions = Vec::new();
        while let Some(count) = fields.next() {
            if count.is_empty() {
                assert!(fields.peek().is_none(), "invalid empty argument count");
                break;
            }
            let count: usize = std::str::from_utf8(count).unwrap().parse().unwrap();
            let args: Vec<_> = (0..count)
                .map(|_| String::from_utf8(fields.next().unwrap().to_vec()).unwrap())
                .collect();
            assert_eq!(args[0], "--config");
            assert_eq!(Path::new(&args[1]), self.config);
            actions.push(args[2..].to_vec());
        }
        actions
    }

    fn off_count(&self) -> usize {
        self.actions()
            .iter()
            .filter(|args| args.as_slice() == ["gadget", "off"])
            .count()
    }

    fn usb(&self) -> String {
        fs::read_to_string(self.root.join("usb")).unwrap()
    }

    fn diagnostics(&self) -> String {
        let journal = Command::new("journalctl")
            .args(["--unit", &self.unit, "--no-pager", "--lines=40"])
            .output()
            .unwrap();
        format!(
            "fixture {}: {}\n{}",
            self.root.display(),
            self.property("SubState"),
            String::from_utf8_lossy(&journal.stdout)
        )
    }

    fn clean_up(&mut self) -> Result<(), String> {
        let result = self.clean_up_inner();
        self.cleanup_failed |= result.is_err();
        result
    }

    fn clean_up_inner(&mut self) -> Result<(), String> {
        if self.installed {
            let stop_result = systemctl(&["stop", "--no-block", &self.unit]);
            let mut forced = false;
            if !wait_until(|| {
                matches!(self.property("ActiveState").as_str(), "inactive" | "failed")
            }) {
                // Only this test's uniquely named unit may be killed during fixture cleanup.
                systemctl(&["kill", "--kill-who=all", "--signal=KILL", &self.unit])?;
                if !wait_until(|| {
                    matches!(self.property("ActiveState").as_str(), "inactive" | "failed")
                }) {
                    return Err(format!("test service did not stop: {}", self.diagnostics()));
                }
                forced = true;
            }
            fs::remove_file(&self.unit_path).map_err(|error| error.to_string())?;
            systemctl(&["daemon-reload"])?;
            self.installed = false;
            stop_result?;
            if forced {
                return Err(format!(
                    "test service required SIGKILL; evidence retained at {}",
                    self.root.display()
                ));
            }
        }
        fs::remove_dir_all(&self.root).map_err(|error| error.to_string())
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        if self.cleanup_failed {
            eprintln!(
                "systemd test fixture retained after cleanup failure: {}",
                self.root.display()
            );
            return;
        }
        if self.root.exists() {
            if let Err(error) = self.clean_up() {
                eprintln!("failed to clean up systemd test fixture: {error}");
                if !thread::panicking() {
                    panic!("systemd test fixture cleanup failed: {error}");
                }
            }
        }
    }
}

#[test]
#[ignore = "requires root and a running Linux systemd manager"]
fn failed_cleanup_preserves_evidence_after_the_service_is_unloaded() {
    let mut harness = Harness::new();
    let root = harness.root.clone();
    fs::write(
        &harness.unit_path,
        "[Service]\nExecStart=\"/invalid'path\"\n",
    )
    .unwrap();
    systemctl(&["daemon-reload"]).unwrap();

    let error = harness.clean_up().unwrap_err();
    assert!(error.contains("systemctl"), "{error}");
    assert!(!harness.installed);
    assert!(!harness.unit_path.exists());
    drop(harness);

    assert!(root.exists(), "Drop removed retained cleanup evidence");
    fs::remove_dir_all(root).unwrap();
}

#[test]
#[ignore = "requires root and a running Linux systemd manager"]
fn generated_service_preserves_recording_after_failures_and_quotes_literal_paths() {
    let mut harness = Harness::new();
    for mode in ["startup-fail", "runtime-fail", "maintenance-fail"] {
        harness.reset(mode);
        harness.start();
        harness.wait(|| harness.property("SubState") == "auto-restart");
        assert_eq!(harness.property("Result"), "exit-code");
        assert_eq!(
            harness.usb(),
            if mode == "maintenance-fail" {
                "off"
            } else {
                "on"
            }
        );
        assert_eq!(harness.off_count(), 0);
        harness.stop();
        // Cancelling a scheduled restart does not run a second stop hook.
        assert_eq!(harness.off_count(), 0);
    }

    harness.reset("retry-first");
    harness.start();
    harness.wait(|| harness.root.join("ready-2").exists());
    assert_eq!(harness.property("NRestarts"), "1");
    assert_eq!(harness.off_count(), 0);
    assert_eq!(harness.usb(), "on");
    harness.stop();
    assert_eq!(harness.off_count(), 1);
    assert_eq!(harness.usb(), "off");

    harness.reset("hold");
    harness.start();
    harness.wait(|| harness.root.join("ready-1").exists());
    systemctl(&["restart", "--no-block", &harness.unit]).unwrap();
    harness.wait(|| harness.root.join("ready-2").exists());
    assert_eq!(harness.off_count(), 1);
    assert_eq!(harness.usb(), "on");
    harness.stop();
    assert_eq!(harness.off_count(), 2);
    assert_eq!(harness.usb(), "off");
    harness.clean_up().unwrap();
}

fn wait_until(predicate: impl Fn() -> bool) -> bool {
    let started = Instant::now();
    while started.elapsed() < WAIT_TIMEOUT {
        if predicate() {
            return true;
        }
        thread::sleep(Duration::from_millis(50));
    }
    predicate()
}

fn write_executable(path: &Path, text: &str) {
    fs::write(path, text).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

fn output(program: &str, args: &[&str]) -> String {
    let result = Command::new(program).args(args).output().unwrap();
    assert_success(&result);
    String::from_utf8(result.stdout).unwrap()
}

fn systemctl(args: &[&str]) -> Result<String, String> {
    let result = Command::new("systemctl")
        .args(args)
        .output()
        .map_err(|error| error.to_string())?;
    if !result.status.success() {
        return Err(format!("systemctl {args:?}: {result:?}"));
    }
    String::from_utf8(result.stdout).map_err(|error| error.to_string())
}

fn assert_success(result: &Output) {
    assert!(result.status.success(), "{result:?}");
}

const FAKE_EXECUTABLE: &str = r#"#!/bin/sh
set -eu
root=$(dirname "$(dirname "$0")")
printf '%s\0' "$#" "$@" >> "$root/events"
shift 2
mode=$(cat "$root/mode")
case "$*" in
    'doctor --startup')
        if [ "$mode" = startup-fail ]; then exit 23; fi
        ;;
    mount) ;;
    'gadget on') printf on > "$root/usb" ;;
    'gadget off') printf off > "$root/usb" ;;
    run)
        count=$(cat "$root/run-count")
        count=$((count + 1))
        printf '%s' "$count" > "$root/run-count"
        if [ "$mode" = maintenance-fail ]; then
            printf off > "$root/usb"
            exit 23
        fi
        if [ "$mode" = runtime-fail ] || { [ "$mode" = retry-first ] && [ "$count" = 1 ]; }; then
            exit 23
        fi
        trap 'exit 0' TERM
        : > "$root/ready-$count"
        while :; do sleep 0.05; done
        ;;
    *) printf 'unexpected command: %s\n' "$*" >&2; exit 98 ;;
esac
"#;
