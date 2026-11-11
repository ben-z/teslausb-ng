#![cfg(target_os = "linux")]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

struct Harness {
    root: PathBuf,
    unit: String,
    unit_path: PathBuf,
    dropin_dir: PathBuf,
    config: PathBuf,
    supplicant_config: PathBuf,
    installed: bool,
}

impl Harness {
    fn new() -> Self {
        assert_eq!(run("id", &["-u"]).trim(), "0", "requires root");
        assert!(Path::new("/run/systemd/system").is_dir());
        run("systemctl", &["show", "--property=Version", "--value"]);
        let id = format!(
            "{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let root = std::env::temp_dir().join(format!("teslausb-wifi-systemd-{id}"));
        fs::create_dir(&root).unwrap();
        let unit = format!("teslausb-wifi-test-{id}.service");
        let mut harness = Self {
            unit_path: Path::new("/run/systemd/system").join(&unit),
            dropin_dir: Path::new("/run/systemd/system").join(format!("{unit}.d")),
            config: root.join("private.conf"),
            supplicant_config: root.join("netplan/run/netplan/wpa-wlan0.conf"),
            root,
            unit,
            installed: false,
        };
        fs::write(
            &harness.config,
            "ARCHIVE_SYSTEM=none\nARCHIVE_PAUSED_SSIDS=[\"ExampleMobile\"]\nWIFI_DEFAULT_PRIORITY=100\nWIFI_PAUSED_PRIORITY=0\n",
        )
        .unwrap();
        fs::set_permissions(&harness.config, fs::Permissions::from_mode(0o600)).unwrap();
        let netplan_dir = harness.root.join("netplan/etc/netplan");
        fs::create_dir_all(&netplan_dir).unwrap();
        let yaml = netplan_dir.join("50-fixture.yaml");
        fs::write(
            &yaml,
            "network:\n  version: 2\n  renderer: networkd\n  wifis:\n    wlan0:\n      dhcp4: true\n      access-points:\n        ExampleHome:\n          password: fixture-home-password\n        ExampleMobile:\n          password: fixture-mobile-password\n          hidden: true\n",
        )
        .unwrap();
        fs::set_permissions(yaml, fs::Permissions::from_mode(0o600)).unwrap();
        harness.generate();

        let supplicant = harness.root.join("supplicant");
        write_executable(
            &supplicant,
            &format!(
                "#!/bin/sh\nset -eu\ncp {} {}\nprintf 'start\\n' >> {}\ntouch {}\nexec /bin/sleep infinity\n",
                shell_quote(&harness.supplicant_config),
                shell_quote(&harness.root.join("start-observed.conf")),
                shell_quote(&harness.root.join("events")),
                shell_quote(&harness.root.join("start-ready")),
            ),
        );
        let wpa_cli = harness.root.join("wpa_cli");
        write_executable(
            &wpa_cli,
            &format!(
                "#!/bin/sh\nset -eu\n[ \"$#\" -eq 3 ]\n[ \"$1\" = -i ]\n[ \"$2\" = wlan0 ]\n[ \"$3\" = reconfigure ]\ncp {} {}\nprintf 'reload\\n' >> {}\n",
                shell_quote(&harness.supplicant_config),
                shell_quote(&harness.root.join("reload-observed.conf")),
                shell_quote(&harness.root.join("events")),
            ),
        );
        let legacy_reload = harness.root.join("legacy-reload");
        write_executable(
            &legacy_reload,
            &format!(
                "#!/bin/sh\ntouch {}\n",
                shell_quote(&harness.root.join("legacy-ran"))
            ),
        );
        let dropin = fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("deploy/systemd/netplan-wpa-wlan0.service.d/50-teslausb-priorities.conf"),
        )
        .unwrap();
        for fixed_path in [
            "/usr/local/bin/teslausb",
            "/etc/teslausb.conf",
            "/run/netplan/wpa-wlan0.conf",
            "/sbin/wpa_cli",
        ] {
            assert!(dropin.contains(fixed_path), "drop-in lacks {fixed_path}");
        }
        let dropin = dropin
            .replace(
                "ExecStartPre=/usr/local/bin/teslausb",
                &format!(
                    "ExecStartPre={} --config {}",
                    env!("CARGO_BIN_EXE_teslausb"),
                    harness.config.display()
                ),
            )
            .replace(
                "ExecReload=/usr/local/bin/teslausb",
                &format!(
                    "ExecReload={} --config {}",
                    env!("CARGO_BIN_EXE_teslausb"),
                    harness.config.display()
                ),
            )
            .replace("/usr/local/bin/teslausb", env!("CARGO_BIN_EXE_teslausb"))
            .replace("/etc/teslausb.conf", harness.config.to_str().unwrap())
            .replace(
                "/run/netplan/wpa-wlan0.conf",
                harness.supplicant_config.to_str().unwrap(),
            )
            .replace("/sbin/wpa_cli", wpa_cli.to_str().unwrap());
        fs::write(
            &harness.unit_path,
            format!(
                "[Service]\nType=simple\nExecStart={}\nExecReload={}\nTimeoutStartSec=10\nTimeoutStopSec=5\n",
                supplicant.display(),
                legacy_reload.display()
            ),
        )
        .unwrap();
        harness.installed = true;
        fs::create_dir(&harness.dropin_dir).unwrap();
        fs::write(
            harness.dropin_dir.join("50-teslausb-priorities.conf"),
            dropin,
        )
        .unwrap();
        run("systemctl", &["daemon-reload"]);
        harness
    }

    fn generate(&self) {
        run(
            "netplan",
            &[
                "generate",
                "--root-dir",
                self.root.join("netplan").to_str().unwrap(),
            ],
        );
        assert_eq!(
            fs::metadata(&self.supplicant_config)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert!(!fs::read_to_string(&self.supplicant_config)
            .unwrap()
            .contains("priority="));
    }

    fn action(&self, action: &str) {
        run("systemctl", &[action, &self.unit]);
    }

    fn wait_for_start(&self) {
        let started = Instant::now();
        while !self.root.join("start-ready").is_file() {
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "{}",
                self.journal()
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn journal(&self) -> String {
        run(
            "journalctl",
            &["--unit", &self.unit, "--no-pager", "--lines=30"],
        )
    }

    fn assert_observed(&self, name: &str, generated: &str) {
        let observed = fs::read_to_string(self.root.join(name)).unwrap();
        assert_eq!(without_priorities(&observed), generated);
        for (ssid, priority) in [("ExampleHome", "100"), ("ExampleMobile", "0")] {
            let blocks: Vec<_> = observed
                .split("network={")
                .skip(1)
                .filter(|block| {
                    block.lines().any(|line| {
                        line.trim() == format!("ssid=\"{ssid}\"")
                            || line.trim() == format!("ssid=P\"{ssid}\"")
                    })
                })
                .collect();
            assert_eq!(blocks.len(), 1);
            let priorities: Vec<_> = blocks[0]
                .split('}')
                .next()
                .unwrap()
                .lines()
                .filter_map(|line| line.trim().strip_prefix("priority="))
                .collect();
            assert_eq!(priorities, [priority], "{ssid}");
        }
        assert_eq!(
            fs::metadata(&self.supplicant_config)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }

    fn clean_up(&mut self, retain_evidence: bool) -> Result<(), String> {
        if self.installed {
            checked_run("systemctl", &["stop", &self.unit])?;
            fs::remove_file(&self.unit_path).map_err(|error| error.to_string())?;
            fs::remove_dir_all(&self.dropin_dir).map_err(|error| error.to_string())?;
            self.installed = false;
            checked_run("systemctl", &["daemon-reload"])?;
        }
        if retain_evidence {
            eprintln!("Wi-Fi systemd fixture retained at {}", self.root.display());
        } else {
            fs::remove_dir_all(&self.root).map_err(|error| error.to_string())?;
        }
        Ok(())
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        if self.root.exists() {
            if let Err(error) = self.clean_up(thread::panicking()) {
                eprintln!("failed to clean up Wi-Fi systemd fixture: {error}");
                if !thread::panicking() {
                    panic!("Wi-Fi systemd fixture cleanup failed: {error}");
                }
            }
        }
    }
}

#[test]
#[ignore = "requires root, netplan, and a running Linux systemd manager"]
fn netplan_dropin_sets_priorities_before_start_restart_and_reload() {
    let harness = Harness::new();
    let generated = fs::read_to_string(&harness.supplicant_config).unwrap();
    harness.action("start");
    harness.wait_for_start();
    harness.assert_observed("start-observed.conf", &generated);

    harness.generate();
    fs::remove_file(harness.root.join("start-ready")).unwrap();
    harness.action("restart");
    harness.wait_for_start();
    harness.assert_observed("start-observed.conf", &generated);

    harness.generate();
    harness.action("reload");
    harness.assert_observed("reload-observed.conf", &generated);
    assert!(!harness.root.join("legacy-ran").exists());
    assert_eq!(
        fs::read_to_string(harness.root.join("events")).unwrap(),
        "start\nstart\nreload\n"
    );
}

#[test]
#[ignore = "requires root, netplan, and a running Linux systemd manager"]
fn netplan_dropin_refuses_to_start_without_valid_private_configuration() {
    for malformed in [false, true] {
        let harness = Harness::new();
        let generated = fs::read_to_string(&harness.supplicant_config).unwrap();
        if malformed {
            fs::write(&harness.config, "ARCHIVE_PAUSED_SSIDS=[\n").unwrap();
        } else {
            fs::remove_file(&harness.config).unwrap();
        }
        let result = Command::new("systemctl")
            .args(["start", &harness.unit])
            .output()
            .unwrap();
        assert!(!result.status.success(), "{result:?}");
        assert!(!harness.root.join("start-observed.conf").exists());
        assert!(!harness.root.join("events").exists());
        let journal = harness.journal();
        assert!(
            journal.contains(if malformed {
                "ARCHIVE_PAUSED_SSIDS"
            } else {
                "config file not found or unreadable"
            }),
            "{journal}"
        );
        assert_eq!(
            fs::read_to_string(&harness.supplicant_config).unwrap(),
            generated
        );
        assert_eq!(
            run(
                "systemctl",
                &["show", &harness.unit, "--property=Result", "--value"]
            )
            .trim(),
            "exit-code"
        );
    }
}

#[test]
#[ignore = "requires root, netplan, and a running Linux systemd manager"]
fn netplan_dropin_refuses_to_reconfigure_when_priority_configuration_is_invalid() {
    let harness = Harness::new();
    harness.action("start");
    harness.wait_for_start();
    harness.generate();
    let generated = fs::read_to_string(&harness.supplicant_config).unwrap();
    fs::write(&harness.config, "ARCHIVE_PAUSED_SSIDS=[\n").unwrap();
    let result = Command::new("systemctl")
        .args(["reload", &harness.unit])
        .output()
        .unwrap();
    assert!(!result.status.success(), "{result:?}");
    assert!(!harness.root.join("reload-observed.conf").exists());
    assert_eq!(
        fs::read_to_string(&harness.supplicant_config).unwrap(),
        generated
    );
    assert_eq!(
        run(
            "systemctl",
            &["show", &harness.unit, "--property=ActiveState", "--value"]
        )
        .trim(),
        "active"
    );
    assert_eq!(
        fs::read_to_string(harness.root.join("events")).unwrap(),
        "start\n"
    );
}

fn without_priorities(text: &str) -> String {
    text.split_inclusive('\n')
        .filter(|line| !line.trim_start().starts_with("priority="))
        .collect()
}

fn shell_quote(path: &Path) -> String {
    format!("'{}'", path.to_str().unwrap().replace('\'', "'\\''"))
}

fn write_executable(path: &Path, contents: &str) {
    fs::write(path, contents).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

fn run(program: &str, args: &[&str]) -> String {
    checked_run(program, args).unwrap()
}

fn checked_run(program: &str, args: &[&str]) -> Result<String, String> {
    let result: Output = Command::new(program)
        .args(args)
        .output()
        .map_err(|error| error.to_string())?;
    if !result.status.success() {
        return Err(format!(
            "{program} {args:?}: {}\nstdout:\n{}\nstderr:\n{}",
            result.status,
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        ));
    }
    String::from_utf8(result.stdout).map_err(|error| error.to_string())
}
